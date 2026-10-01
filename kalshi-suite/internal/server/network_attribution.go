package server

// network_attribution.go extends the existing latency sampler with a small, non-authorizing
// receipt that answers a practical operator question: is a red venue feed probably the local
// hotspot, or only that venue?  It deliberately stores no SSID, BSSID, adapter address, public
// IP, or command output.

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	networkPathSampleEvery = time.Minute
	wlanCommandTimeout     = 1500 * time.Millisecond
	internetProbeTimeout   = 4 * time.Second
)

type wlanLinkSample struct {
	Available, Connected, SignalKnown bool
	SignalPct                         int
	ReceiveMbps, TransmitMbps         float64
	Error                             string
}

type internetPathSample struct {
	Known, OK bool
	RTTMS     float64
	Error     string
}

type networkAttributionInput struct {
	Link       wlanLinkSample
	Internet   internetPathSample
	VenueSlow  map[string]bool
	VenueKnown map[string]bool
}

type networkPathReceipt struct {
	At                                time.Time
	Classification, State, Reason     string
	InternetKnown, InternetOK         bool
	InternetRTTMS                     float64
	WiFiAvailable, WiFiConnected      bool
	WiFiSignalKnown                   bool
	WiFiSignalPct                     int
	WiFiReceiveMbps, WiFiTransmitMbps float64
	VenueSlow                         []string
}

// parseWLANInterfaces extracts only link-health fields. Exact-key matching matters: SSID/BSSID
// lines must never accidentally enter the receipt or logs.
func parseWLANInterfaces(raw []byte) wlanLinkSample {
	var out wlanLinkSample
	for _, line := range strings.Split(strings.ReplaceAll(string(raw), "\r\n", "\n"), "\n") {
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.ToLower(strings.TrimSpace(parts[0]))
		value := strings.TrimSpace(parts[1])
		switch key {
		case "state":
			out.Available = true
			if strings.EqualFold(value, "connected") {
				out.Connected = true
			}
		case "signal":
			v := strings.TrimSuffix(value, "%")
			if n, err := strconv.Atoi(strings.TrimSpace(v)); err == nil && n >= 0 && n <= 100 {
				out.SignalKnown = true
				if n > out.SignalPct {
					out.SignalPct = n
				}
			}
		case "receive rate (mbps)":
			if n, err := strconv.ParseFloat(value, 64); err == nil && n >= 0 && n > out.ReceiveMbps {
				out.ReceiveMbps = n
			}
		case "transmit rate (mbps)":
			if n, err := strconv.ParseFloat(value, 64); err == nil && n >= 0 && n > out.TransmitMbps {
				out.TransmitMbps = n
			}
		}
	}
	if !out.Available {
		out.Error = "Wi-Fi link telemetry unavailable"
	}
	return out
}

type wlanCommandRunner func(context.Context) ([]byte, error)

// queryWLANLink is hard-bounded because diagnostic telemetry may never delay venue collection.
func queryWLANLink(parent context.Context, timeout time.Duration, run wlanCommandRunner) wlanLinkSample {
	if timeout <= 0 {
		timeout = wlanCommandTimeout
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	raw, err := run(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return wlanLinkSample{Error: "Wi-Fi link telemetry timed out"}
		}
		return wlanLinkSample{Error: "Wi-Fi link telemetry unavailable"}
	}
	return parseWLANInterfaces(raw)
}

func probeIndependentInternet(parent context.Context) internetPathSample {
	ctx, cancel := context.WithTimeout(parent, internetProbeTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		"https://connectivitycheck.gstatic.com/generate_204", nil)
	if err != nil {
		return internetPathSample{Known: true, Error: "connectivity probe setup failed"}
	}
	t0 := time.Now()
	resp, err := http.DefaultClient.Do(req)
	rtt := float64(time.Since(t0).Milliseconds())
	if err != nil {
		return internetPathSample{Known: true, RTTMS: rtt, Error: "independent internet path unreachable"}
	}
	resp.Body.Close()
	ok := resp.StatusCode >= 200 && resp.StatusCode < 400
	out := internetPathSample{Known: true, OK: ok, RTTMS: rtt}
	if !ok {
		out.Error = fmt.Sprintf("independent internet probe HTTP %d", resp.StatusCode)
	}
	return out
}

func classifyNetworkPath(in networkAttributionInput) (classification, state, reason string, slow []string) {
	for _, venue := range []string{"kalshi", "polyus", "polyint"} {
		if in.VenueKnown[venue] && in.VenueSlow[venue] {
			slow = append(slow, venue)
		}
	}
	weakWiFi := in.Link.Available && in.Link.Connected && in.Link.SignalKnown && in.Link.SignalPct <= 25
	switch {
	case in.Internet.Known && !in.Internet.OK && ((in.Link.Available && !in.Link.Connected) || weakWiFi || len(slow) >= 2):
		return "local_network", "degraded", "independent internet and multiple local-path signals are failing", slow
	case in.Internet.Known && in.Internet.OK && len(slow) == 1:
		return "single_venue", "degraded", slow[0] + " is slow while the independent internet path works", slow
	case in.Internet.Known && in.Internet.OK && len(slow) == 0 && weakWiFi:
		return "local_network", "warning", "Wi-Fi signal is weak, but internet and venue probes still answer", slow
	case in.Internet.Known && in.Internet.OK && len(slow) == 0:
		return "healthy", "healthy", "independent internet and all measured venue paths are responding", slow
	case in.Internet.Known && in.Internet.OK && len(slow) >= 2:
		return "unknown", "degraded", "multiple venue paths are slow while the independent internet path works", slow
	case !in.Internet.Known:
		return "unknown", "unknown", "independent internet sample is not available yet", slow
	default:
		return "unknown", "degraded", "internet reachability is degraded, but the current evidence cannot isolate hotspot versus upstream route", slow
	}
}

func (m *latMon) networkSampleDue(now time.Time) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.network.At.IsZero() || now.Sub(m.network.At) >= networkPathSampleEvery
}

func (m *latMon) recordNetworkPath(now time.Time, link wlanLinkSample, internet internetPathSample, restWarnMS float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	in := networkAttributionInput{Link: link, Internet: internet,
		VenueSlow: map[string]bool{}, VenueKnown: map[string]bool{}}
	for key, venue := range map[string]string{"kalshi_rest": "kalshi", "polyus_rest": "polyus", "polyint_rest": "polyint"} {
		if ring := m.rings[key]; ring != nil && ring.n > 0 {
			in.VenueKnown[venue] = true
			in.VenueSlow[venue] = ring.buf[(ring.i-1+latRingCap)%latRingCap].MS >= restWarnMS
		}
	}
	class, state, reason, slow := classifyNetworkPath(in)
	m.network = networkPathReceipt{At: now, Classification: class, State: state, Reason: reason,
		InternetKnown: internet.Known, InternetOK: internet.OK, InternetRTTMS: internet.RTTMS,
		WiFiAvailable: link.Available, WiFiConnected: link.Connected, WiFiSignalKnown: link.SignalKnown,
		WiFiSignalPct: link.SignalPct, WiFiReceiveMbps: link.ReceiveMbps, WiFiTransmitMbps: link.TransmitMbps,
		VenueSlow: slow}
}

func (m *latMon) networkPathSnapshot() (networkPathReceipt, bool) {
	if m == nil {
		return networkPathReceipt{}, false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.network, !m.network.At.IsZero()
}

func networkPathDetail(v networkPathReceipt, now time.Time) string {
	if v.At.IsZero() {
		return "warming; first bounded network-path sample pending"
	}
	detail := fmt.Sprintf("%s: %s; sample %.0fs old", v.Classification, v.Reason, now.Sub(v.At).Seconds())
	if v.WiFiSignalKnown {
		detail += fmt.Sprintf("; Wi-Fi %d%%", v.WiFiSignalPct)
	}
	if v.InternetKnown {
		detail += fmt.Sprintf("; independent internet %.0fms ok=%v", v.InternetRTTMS, v.InternetOK)
	}
	return detail
}
