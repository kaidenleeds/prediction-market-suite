package server

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func r163CanaryAdmissionServer(t *testing.T, family string) *Server {
	t.Helper()
	s := testServer(t)
	identity := "kalshi|" + family + "|YES|taker"
	cfg := *s.cfg()
	cfg.Risk.LiveProspectiveAllocation = true
	cfg.Risk.LiveSystemKalshi = true
	cfg.Risk.LiveSystemAllowlist = identity
	cfg.Risk.LiveSystemCanaryAllowlist = identity
	s.cfgP.Store(&cfg)
	s.liveMu.Lock()
	s.liveArmed, s.liveAuto = true, true
	s.liveMu.Unlock()
	return s
}

// r163PreparedAdmissionCanary models the immutable output of the canonical LIVE-first worker.
// It deliberately does not call enqueue: rejection tests can corrupt one provenance field at a
// time and prove the queue boundary fails before the older Paper observation fallback runs.
func r163PreparedAdmissionCanary(t *testing.T, s *Server,
	sig storage.Signal) liveMirrorCandidate {
	t.Helper()
	now := time.Now()
	c := liveMirrorCandidate{
		Platform:        strings.ToLower(strings.TrimSpace(sig.Platform)),
		Ticker:          strings.TrimSpace(sig.Ticker),
		Title:           sig.Title,
		Side:            strings.ToUpper(strings.TrimSpace(sig.Side)),
		Source:          "auto-cons-" + strings.TrimSpace(sig.SignalType),
		Price:           sig.EntryPrice,
		At:              now,
		InputTopology:   r147SignalInputTopology(sig),
		InputObservedAt: r147SignalInputObservedAt(sig),
	}
	bound, _, why := r147BindSignalContract(c, "taker")
	if why != "" {
		t.Fatalf("bind canonical test signal: %q signal=%+v", why, sig)
	}
	c = bound
	basis := "one-contract-canary:UNPROVEN:" + liveSystemIdentityKey(c, "taker") +
		":point-source=exact-paper-model-cell:test"
	c.Canary = true
	c.CanaryBasis = basis
	c.ProspectiveQty = 1
	c.ProspectiveRequestedFee = .01
	c.ProspectiveRequestedFeePC = .01
	c.ProspectiveFeeSource = "kalshi:test-q1"
	c.ProspectiveTimeInForce = liveProspectiveIOC
	c.ProspectivePlanReason = "test canonical q1 IOC"
	c.ArbitrationQuote = liveMirrorQuote{
		Price: .40, Depth: 1, Tick: .01, MinQty: 1, MinQtyKnown: true,
		BookSource: "kalshi_ws_full_orderbook:test",
	}
	c.ArbitrationBasis = basis
	c.ArbitrationMean = .03
	c.ArbitrationLower = -.02
	c.ArbitrationFee = .01
	c.ArbitrationAt = now
	c.LiveFirstUnitPersisted = true
	s.liveMirrorMu.Lock()
	if s.liveAllocationSignal == nil {
		s.liveAllocationSignal = map[string]time.Time{}
	}
	s.liveAllocationSignal[liveAllocationSignalKey(c)] = now
	s.liveMirrorMu.Unlock()
	return c
}

func r163QueuedCanary(t *testing.T, s *Server) liveMirrorCandidate {
	t.Helper()
	s.liveMirrorMu.Lock()
	defer s.liveMirrorMu.Unlock()
	if len(s.liveMirrorQ) != 1 {
		t.Fatalf("LIVE canary queue len=%d want=1", len(s.liveMirrorQ))
	}
	return s.liveMirrorQ[0]
}

func TestR163LegacyAutoConsKalshiPaperAliasCannotEnterCanary(t *testing.T) {
	s := r163CanaryAdmissionServer(t, "kalshi-flow")
	c := liveMirrorCandidate{
		Platform: "kalshi", Ticker: "R163-LEGACY-PAPER", Title: "legacy Paper alias",
		Side: "YES", Source: "auto-cons-kalshi", Price: .40, At: time.Now(),
		InputTopology: "K",
	}
	if why := s.liveConfiguredCanaryAdmissionReason(c, "taker", time.Now()); why != "one-contract-canary-requires-canonical-live-first-source" {
		t.Fatalf("legacy alias reason=%q", why)
	}
	if s.enqueueLiveMirrorCandidate(c) {
		t.Fatal("legacy auto-cons-kalshi Paper alias entered the LIVE canary queue")
	}
	s.liveMirrorMu.Lock()
	queued, minted := len(s.liveMirrorQ), len(s.liveAllocationSignal)
	s.liveMirrorMu.Unlock()
	if queued != 0 || minted != 0 {
		t.Fatalf("legacy Paper alias minted LIVE authority: queued=%d receipts=%d", queued, minted)
	}
}

func TestR163ForgedCanonicalSourceWithoutLiveFirstPreflightCannotEnterCanary(t *testing.T) {
	s := r163CanaryAdmissionServer(t, "kalshi-flow")
	observed := time.Now()
	c := liveMirrorCandidate{
		Platform: "kalshi", Ticker: "R163-FORGED-FLOW", Title: "forged canonical source",
		Side: "YES", Source: "auto-cons-kalshi-flow", Price: .40, At: observed,
		InputTopology: "K", InputObservedAt: observed,
	}
	if why := s.liveConfiguredCanaryAdmissionReason(c, "taker", time.Now()); why != "one-contract-canary-requires-completed-live-first-preflight" {
		t.Fatalf("forged canonical source reason=%q", why)
	}
	if s.enqueueLiveMirrorCandidate(c) {
		t.Fatal("canonical-looking source without completed LIVE-first preflight entered LIVE")
	}
	s.liveMirrorMu.Lock()
	queued, minted := len(s.liveMirrorQ), len(s.liveAllocationSignal)
	s.liveMirrorMu.Unlock()
	if queued != 0 || minted != 0 {
		t.Fatalf("forged source minted LIVE authority: queued=%d receipts=%d", queued, minted)
	}
}

func TestR163CanonicalKalshiFlowEventCanaryRemainsQ1IOC(t *testing.T) {
	s := r163CanaryAdmissionServer(t, "kalshi-flow")
	observed := time.Now().Add(-25 * time.Millisecond)
	sig := storage.Signal{
		Platform: "kalshi", Ticker: "R163-EVENT-FLOW", Title: "event Flow",
		Side: "YES", SignalType: "kalshi-flow", EntryPrice: .40,
		ExecExpr: r147InputReceiptExpr("K", observed),
	}
	c := r163PreparedAdmissionCanary(t, s, sig)
	if why := s.liveConfiguredCanaryAdmissionReason(c, "taker", time.Now()); why != "" {
		t.Fatalf("canonical event Flow canary refused: %q", why)
	}
	if !s.enqueueLiveMirrorCandidate(c) {
		t.Fatal("canonical event Flow canary did not enter LIVE")
	}
	got := r163QueuedCanary(t, s)
	if !got.Canary || got.ProspectiveQty != 1 ||
		got.ProspectiveTimeInForce != liveProspectiveIOC ||
		got.Source != "auto-cons-kalshi-flow" || got.InputTopology != "K" ||
		!got.InputObservedAt.Equal(observed) {
		t.Fatalf("canonical event Flow lost exact q1 IOC provenance: %+v", got)
	}
}

func TestR163PeriodicKalshiFlowKeepsLoggingButCannotEnterLive(t *testing.T) {
	s := testServer(t)
	periodic := storage.Signal{
		Platform: "kalshi", Ticker: "R163-PERIODIC-FLOW", Title: "periodic Flow backstop",
		Side: "YES", SignalType: "kalshi-flow", EntryPrice: .40,
	}
	if err := s.insertSignal(context.Background(), periodic); err != nil {
		t.Fatalf("periodic Flow logging failed: %v", err)
	}
	var loggedExpr string
	if err := s.store.DBForTest().QueryRow(
		`SELECT exec_expr FROM signal_log WHERE ticker=? AND signal_type='kalshi-flow'`,
		periodic.Ticker).Scan(&loggedExpr); err != nil {
		t.Fatalf("periodic Flow row was not retained: %v", err)
	}
	if loggedExpr != "input=K" {
		t.Fatalf("periodic Flow logger changed shape: exec_expr=%q want clockless input=K", loggedExpr)
	}

	identity := "kalshi|kalshi-flow|YES|taker"
	cfg := *s.cfg()
	cfg.Risk.LiveProspectiveAllocation = true
	cfg.Risk.LiveSystemKalshi = true
	cfg.Risk.LiveSystemAllowlist = identity
	cfg.Risk.LiveSystemCanaryAllowlist = identity
	s.cfgP.Store(&cfg)
	s.liveMu.Lock()
	s.liveArmed, s.liveAuto = true, true
	s.liveMu.Unlock()
	periodic.ExecExpr = loggedExpr
	c := r163PreparedAdmissionCanary(t, s, periodic)
	if why := s.liveConfiguredCanaryAdmissionReason(c, "taker", time.Now()); why != "one-contract-canary-origin-clock-missing-or-stale" {
		t.Fatalf("clockless periodic Flow reason=%q", why)
	}
	if s.enqueueLiveMirrorCandidate(c) {
		t.Fatal("clockless periodic Flow backstop entered the LIVE canary queue")
	}
	s.liveMirrorMu.Lock()
	queued := len(s.liveMirrorQ)
	s.liveMirrorMu.Unlock()
	if queued != 0 {
		t.Fatalf("periodic Flow created %d LIVE candidate(s)", queued)
	}
	var logged int
	if err := s.store.DBForTest().QueryRow(
		`SELECT COUNT(*) FROM signal_log WHERE ticker=? AND signal_type='kalshi-flow'`,
		periodic.Ticker).Scan(&logged); err != nil || logged != 1 {
		t.Fatalf("LIVE refusal damaged periodic logging: rows=%d err=%v", logged, err)
	}
}

func TestR163SpotlagDetectorCanaryRemainsQ1IOC(t *testing.T) {
	s := r163CanaryAdmissionServer(t, "spotlag")
	observed := time.Now().Add(-30 * time.Millisecond)
	sig, emitted := spotlagSignalFromInputs(
		"R163-SPOTLAG", "Spot-lag detector", .40, 64000, .031, .01, .02, true, observed)
	if !emitted {
		t.Fatal("valid Spot-lag detector fixture did not emit")
	}
	c := r163PreparedAdmissionCanary(t, s, sig)
	if why := s.liveConfiguredCanaryAdmissionReason(c, "taker", time.Now()); why != "" {
		t.Fatalf("canonical Spot-lag canary refused: %q", why)
	}
	if !s.enqueueLiveMirrorCandidate(c) {
		t.Fatal("canonical Spot-lag canary did not enter LIVE")
	}
	got := r163QueuedCanary(t, s)
	if !got.Canary || got.ProspectiveQty != 1 ||
		got.ProspectiveTimeInForce != liveProspectiveIOC ||
		got.Source != "auto-cons-spotlag" || got.InputTopology != "K-SPOT" ||
		!got.InputObservedAt.Equal(observed) {
		t.Fatalf("canonical Spot-lag lost exact q1 IOC provenance: %+v", got)
	}
}
