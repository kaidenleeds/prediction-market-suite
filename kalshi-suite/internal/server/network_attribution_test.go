package server

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestNetworkAttributionParsesOnlySafeWLANHealth(t *testing.T) {
	raw := []byte(`
    Name                   : Wi-Fi
    State                  : connected
    SSID                   : private-hotspot-name
    BSSID                  : 11:22:33:44:55:66
    Signal                 : 37%
    Receive rate (Mbps)    : 144.4
    Transmit rate (Mbps)   : 72.2
`)
	got := parseWLANInterfaces(raw)
	if !got.Available || !got.Connected || !got.SignalKnown || got.SignalPct != 37 ||
		got.ReceiveMbps != 144.4 || got.TransmitMbps != 72.2 {
		t.Fatalf("bad safe link parse: %+v", got)
	}
	if strings.Contains(strings.ToLower(got.Error), "private-hotspot") || strings.Contains(got.Error, "11:22") {
		t.Fatalf("SSID/BSSID escaped the parser: %+v", got)
	}
}

func TestNetworkAttributionClassification(t *testing.T) {
	base := networkAttributionInput{Link: wlanLinkSample{Available: true, Connected: true, SignalKnown: true, SignalPct: 70},
		Internet:   internetPathSample{Known: true, OK: true},
		VenueKnown: map[string]bool{"kalshi": true, "polyus": true, "polyint": true},
		VenueSlow:  map[string]bool{}}
	if c, _, _, _ := classifyNetworkPath(base); c != "healthy" {
		t.Fatalf("healthy classification=%q", c)
	}
	base.VenueSlow["polyus"] = true
	if c, _, _, _ := classifyNetworkPath(base); c != "single_venue" {
		t.Fatalf("single venue classification=%q", c)
	}
	base.Internet.OK = false
	base.VenueSlow["kalshi"] = true
	if c, _, _, _ := classifyNetworkPath(base); c != "local_network" {
		t.Fatalf("local network classification=%q", c)
	}
}

func TestNetworkAttributionWLANCommandTimeout(t *testing.T) {
	started := time.Now()
	got := queryWLANLink(context.Background(), 20*time.Millisecond, func(ctx context.Context) ([]byte, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	})
	if time.Since(started) > 250*time.Millisecond {
		t.Fatalf("bounded WLAN sample took %s", time.Since(started))
	}
	if !strings.Contains(strings.ToLower(got.Error), "timed out") {
		t.Fatalf("timeout not classified safely: %+v", got)
	}
}
