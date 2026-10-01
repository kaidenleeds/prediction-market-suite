package server

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"
)

// signalProducerReceipt proves that one complete producer scan reached the end of its signal
// insertion attempts. A scan with zero candidates is healthy; a scan with any storage error is
// not. The receipt is deliberately separate from the producer's throttle timestamp so starting a
// scan can never paint a failed or canceled scan green.
type signalProducerReceipt struct {
	Completed time.Time
	Attempts  int
	Errors    int
	Reason    string
}

func (s *Server) finishSignalProducer(name string, completed time.Time, attempts, errors int) {
	s.finishSignalProducerWithReason(name, completed, attempts, errors, "")
}

// finishSignalProducerWithReason keeps the exact fail-closed reason in the readiness receipt and
// durably audits unhealthy/recovered transitions. The transition guard prevents a one-second
// producer from flooding SQLite with the same failure every tick.
func (s *Server) finishSignalProducerWithReason(name string, completed time.Time,
	attempts, errors int, reason string) {
	if s == nil || strings.TrimSpace(name) == "" || completed.IsZero() {
		return
	}
	name = strings.TrimSpace(name)
	reason = strings.TrimSpace(reason)
	var (
		audit       bool
		auditLevel  string
		auditState  string
		auditDetail string
	)
	s.signalProducerMu.Lock()
	if s.signalProducerReceipts == nil {
		s.signalProducerReceipts = map[string]signalProducerReceipt{}
	}
	prior, hadPrior := s.signalProducerReceipts[name]
	s.signalProducerReceipts[name] = signalProducerReceipt{
		Completed: completed.UTC(), Attempts: attempts, Errors: errors, Reason: reason,
	}
	if errors > 0 && reason != "" &&
		(!hadPrior || prior.Errors == 0 || prior.Reason != reason) {
		audit, auditLevel, auditState, auditDetail = true, "warn", "unhealthy", reason
	} else if errors == 0 && hadPrior && prior.Errors > 0 && prior.Reason != "" {
		audit, auditLevel, auditState, auditDetail = true, "info", "recovered", prior.Reason
	}
	s.signalProducerMu.Unlock()
	if !audit {
		return
	}
	message := fmt.Sprintf("%s signal producer %s", name, auditState)
	if s.log != nil {
		if auditLevel == "warn" {
			s.log.Warn(message, "reason", auditDetail, "attempts", attempts, "errors", errors)
		} else {
			s.log.Info(message, "prior_reason", auditDetail, "attempts", attempts)
		}
	}
	if s.store != nil {
		auditCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		_ = s.store.Audit(auditCtx, auditLevel, "signal-producer", message, auditDetail)
		cancel()
	}
}

// signalProducerSnapshotAt returns a clock captured while the receipt lock is still held. A
// one-second producer can otherwise publish after a slow readiness handler captured its earlier
// `now`, making a genuinely fresh receipt look several seconds future-dated.
func (s *Server) signalProducerSnapshotAt() (map[string]signalProducerReceipt, time.Time) {
	out := map[string]signalProducerReceipt{}
	if s == nil {
		return out, time.Now()
	}
	s.signalProducerMu.Lock()
	for name, receipt := range s.signalProducerReceipts {
		out[name] = receipt
	}
	snapshotAt := time.Now()
	s.signalProducerMu.Unlock()
	return out, snapshotAt
}

func (s *Server) signalProducerSnapshot() map[string]signalProducerReceipt {
	out, _ := s.signalProducerSnapshotAt()
	return out
}

// signalProducerRequirementsForRoutes replaces the coarse Kalshi venue heartbeat with the exact
// Spot-lag heartbeat only when Spot-lag is the sole Kalshi LIVE family. Mixed or non-Spot-lag
// authority keeps the established venue receipts because those producers still share that
// contract. Never let an exact family receipt hide a second venue.
func signalProducerRequirementsForRoutes(routes, fallbackVenues []string) []string {
	fallback := append([]string(nil), fallbackVenues...)
	if len(fallback) != 1 || fallback[0] != "kalshi" || len(routes) == 0 {
		return fallback
	}
	for _, route := range routes {
		fields := strings.Split(route, "|")
		if len(fields) != 4 || fields[0] != "kalshi" || fields[1] != "spotlag" {
			return fallback
		}
	}
	return []string{"spotlag"}
}

func (s *Server) requiredLiveSignalProducers(fallbackVenues []string) []string {
	fallback := append([]string(nil), fallbackVenues...)
	if s == nil {
		return fallback
	}
	cfg := s.cfg()
	if !cfg.Risk.LiveProspectiveAllocation ||
		cfg.Risk.LiveNewMLKalshi || cfg.Risk.LiveNewMLPolyUS {
		return fallback
	}
	_, canonical, why := parseLiveSystemAllowlist(cfg.Risk.LiveSystemAllowlist)
	if why != "" {
		return fallback
	}
	enabled := make([]string, 0, len(canonical))
	for _, route := range canonical {
		fields := strings.Split(route, "|")
		if len(fields) != 4 || !s.liveSystemVenueEnabled(fields[0]) {
			continue
		}
		enabled = append(enabled, route)
	}
	return signalProducerRequirementsForRoutes(enabled, fallback)
}

// signalProducersClassify requires every exact family or execution-venue producer named by the
// caller. Research-only Polymarket Global has its own nonblocking receipt. This prevents one
// unrelated healthy timer from masking the producer that can actually spend LIVE cash.
func signalProducersClassify(now time.Time, maxAge time.Duration, required []string,
	receipts map[string]signalProducerReceipt) (bool, string) {
	if maxAge <= 0 {
		maxAge = 3 * time.Minute
	}
	required = append([]string(nil), required...)
	sort.Strings(required)
	ok := true
	details := make([]string, 0, len(required))
	for _, name := range required {
		r, have := receipts[name]
		if !have || r.Completed.IsZero() {
			ok = false
			details = append(details, name+"=warming (first completed scan pending)")
			continue
		}
		age := now.Sub(r.Completed)
		state := fmt.Sprintf("%s=%s ago, tried=%d, errors=%d", name,
			age.Truncate(time.Second), r.Attempts, r.Errors)
		if strings.TrimSpace(r.Reason) != "" {
			state += ", reason=" + strings.TrimSpace(r.Reason)
		}
		if age < 0 || age > maxAge || r.Errors > 0 {
			ok = false
			state += " (unhealthy)"
		}
		details = append(details, state)
	}
	if len(details) == 0 {
		return false, "no required execution-venue signal producers configured"
	}
	return ok, strings.Join(details, " | ") + " | zero candidates is healthy"
}

// signalFamilyCadence is reserved for configured, graded families whose producer contract says
// they must emit a row on a clock. Conditional opportunity detectors do not belong in this list:
// their completed-scan heartbeat is the liveness proof when the correct result is zero rows.
type signalFamilyCadence struct {
	fam string
	bar time.Duration
}

// signalFamineClassify is pure so a quiet market cannot be confused with a dead producer. The
// global newest signal row is deliberately not an authority: it may be old while a conditional
// producer is completing healthy zero-opportunity scans. Only the producer heartbeat and explicit
// configured-family cadence contracts can fail this component.
func signalFamineClassify(now time.Time, producersOK bool, producerDetail, newest string,
	byFam map[string]string, queryErr string, cadences []signalFamilyCadence) (bool, string) {
	prefix := "producers: " + producerDetail
	if !producersOK {
		return false, prefix
	}
	if len(cadences) == 0 {
		return true, prefix + " | no configured clock-graded families; zero opportunities is healthy"
	}
	if strings.TrimSpace(queryErr) != "" {
		return false, prefix + " | cadence query failed: " + queryErr
	}

	newestText := "none"
	if t, err := time.Parse(time.RFC3339Nano, newest); err == nil {
		age := now.Sub(t)
		if age >= 0 {
			newestText = age.Truncate(time.Second).String() + " ago"
		}
	}
	ages := make([]string, 0, len(cadences))
	lag := make([]string, 0, len(cadences))
	for _, fc := range cadences {
		ts, have := byFam[fc.fam]
		if !have {
			ages = append(ages, fmt.Sprintf("%s=missing/%s", fc.fam, fc.bar))
			lag = append(lag, fc.fam)
			continue
		}
		t, err := time.Parse(time.RFC3339Nano, ts)
		if err != nil {
			ages = append(ages, fmt.Sprintf("%s=invalid/%s", fc.fam, fc.bar))
			lag = append(lag, fc.fam)
			continue
		}
		age := now.Sub(t)
		ages = append(ages, fmt.Sprintf("%s=%s/%s", fc.fam, age.Truncate(time.Minute), fc.bar))
		if age < 0 || age > fc.bar {
			lag = append(lag, fc.fam)
		}
	}
	detail := prefix + " | newest signal row=" + newestText + " (informational only) | " + strings.Join(ages, " ")
	if len(lag) > 0 {
		return false, detail + " | over-cadence: " + strings.Join(lag, ",")
	}
	return true, detail
}
