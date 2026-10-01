package server

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func r153ArmAuto(s *Server) {
	s.liveMu.Lock()
	s.liveArmed, s.liveAuto = true, true
	s.liveMu.Unlock()
}

func r153CandidateDropRows(t *testing.T, s *Server) []map[string]any {
	t.Helper()
	flushCtx, flushCancel := context.WithTimeout(context.Background(), time.Second)
	defer flushCancel()
	if err := s.flushLiveCandidateAudit(flushCtx); err != nil {
		t.Fatalf("flush LIVE candidate audit: %v", err)
	}
	rows, err := s.store.ListAudit(context.Background(), 100)
	if err != nil {
		t.Fatal(err)
	}
	out := []map[string]any{}
	for _, row := range rows {
		if row.Category != "live-candidate-drop" {
			continue
		}
		var detail map[string]any
		if err := json.Unmarshal([]byte(row.Detail), &detail); err != nil {
			t.Fatalf("durable rejection detail is not JSON: %v (%q)", err, row.Detail)
		}
		detail["audit_message"] = row.Message
		out = append(out, detail)
	}
	return out
}

func TestR153LiveFirstSpotlagDropPersistsEveryExactAudit(t *testing.T) {
	s := testServer(t)
	r153ArmAuto(s)
	sig := storage.Signal{Platform: "kalshi", Ticker: "KXBTC15M-26JUL161700-T100000",
		Side: "NO", SignalType: "spotlag", EntryPrice: .43}
	const reason = "current-executable-price-or-fee-erases-point-edge"

	if s.rejectLiveSignalIntent(sig, reason) {
		t.Fatal("a rejected LIVE-first signal returned accepted")
	}
	// Identical real attempts are separate economic observations. Neither is deduplicated.
	s.rejectLiveSignalIntent(sig, reason)

	rows := r153CandidateDropRows(t, s)
	if len(rows) != 2 {
		t.Fatalf("durable identical spotlag rejection rows=%d, want every 2 rows", len(rows))
	}
	for _, detail := range rows {
		want := map[string]string{
			"event": "AUTO-LIVE-SIGNAL-DROP", "stage": "live-first-preflight",
			"venue": "kalshi", "system": "spotlag", "source": "auto-cons-spotlag",
			"ticker": sig.Ticker, "side": "NO", "reason": reason,
			"audit_message": "AUTO-LIVE-SIGNAL-DROP spotlag",
		}
		for key, value := range want {
			if fmt.Sprint(detail[key]) != value {
				t.Fatalf("durable rejection %s=%q, want %q; detail=%v", key, detail[key], value, detail)
			}
		}
		if detail["candidate_at"] == "" || detail["logged_at"] == "" || detail["age_ms"] == nil || detail["ttl_ms"] == nil {
			t.Fatalf("durable rejection lost timing: %v", detail)
		}
	}
}

func TestR153CandidateDropDoesNotPersistRawSignalWhileOperatorOff(t *testing.T) {
	s := testServer(t)
	c := liveMirrorCandidate{Platform: "kalshi", Ticker: "KXOFF", Side: "YES",
		Family: "spotlag", Source: "auto-cons-spotlag", Price: .40, At: time.Now()}
	if s.recordLiveCandidateDrop(c, "AUTO-LIVE-SIGNAL-DROP", "signal-selection", "test-off", time.Now(), nil) {
		t.Fatal("operator-OFF signal was persisted")
	}
	if rows := r153CandidateDropRows(t, s); len(rows) != 0 {
		t.Fatalf("operator-OFF durable rows=%d want=0", len(rows))
	}
	s.liveMu.Lock()
	defer s.liveMu.Unlock()
	if len(s.liveLog) != 0 {
		t.Fatalf("operator-OFF raw signal entered memory log: %v", s.liveLog)
	}
}

func TestR153SignalAndMirrorCoalescingAreDurableRejections(t *testing.T) {
	s := testServer(t)
	// This fixture has no MonitorLiveAuto consumer. Model the production dispatcher taking
	// ownership of the accepted money intent before asking the lower-priority audit writer to
	// flush; otherwise the intentional cash-first scheduler correctly waits forever on the
	// hermetic channel. The quiet-window policy is covered independently by the R159 priority
	// tests and is not part of this durable-coalescing assertion.
	s.liveDispatchShadowQuietDelay = -1
	cfg := *s.cfg()
	cfg.Risk.LiveProspectiveAllocation = true
	cfg.Risk.LiveSystemKalshi = true
	cfg.Risk.LiveSystemAllowlist = "kalshi|kalshi-flow|YES|taker"
	s.cfgP.Store(&cfg)
	r153ArmAuto(s)
	sig := storage.Signal{Platform: "kalshi", Ticker: "KXR153-COALESCE", Side: "YES",
		SignalType: "kalshi-flow", EntryPrice: .40}
	if !s.queueLiveSignalIntent(sig, .05) || s.queueLiveSignalIntent(sig, .05) {
		t.Fatal("signal burst did not accept first/coalesce second")
	}
	select {
	case <-s.liveSignalIntentChannel():
	default:
		t.Fatal("accepted signal intent was not available to the production dispatcher")
	}

	now := time.Now()
	c := liveMirrorCandidate{Platform: "kalshi", Ticker: "KXR153-MIRROR-COALESCE", Side: "YES",
		Action: "BUY", Family: "kalshi-flow", Source: "auto-cons-kalshi-flow", Price: .40, At: now,
		InputTopology: "K", SignalContractID: "r153-fixture"}
	// Preseed the exact bounded enqueue identity so the test does not need venue I/O.
	s.liveMirrorMu.Lock()
	s.liveMirrorSeen = map[string]time.Time{c.key(): now}
	s.liveMirrorMu.Unlock()
	if s.enqueueLiveMirrorCandidate(c) {
		t.Fatal("duplicate mirror candidate unexpectedly entered the queue")
	}

	rows := r153CandidateDropRows(t, s)
	reasons := map[string]bool{}
	for _, row := range rows {
		reasons[fmt.Sprint(row["reason"])] = true
	}
	for _, reason := range []string{"duplicate-coalesced-live-signal-intent", "duplicate-coalesced-live-candidate"} {
		if !reasons[reason] {
			t.Fatalf("missing durable coalescing reason %q: %v", reason, rows)
		}
	}
}

func TestR153ExpiryAndFinalDispatchDropUseCentralDurableReceipt(t *testing.T) {
	s := testServer(t)
	r153ArmAuto(s)
	c := liveMirrorCandidate{Platform: "kalshi", Ticker: "KXR153-EXPIRED", Side: "NO",
		Action: "BUY", Family: "spotlag", Source: "auto-cons-spotlag", Price: .42,
		At: time.Now().Add(-liveMirrorTTL - time.Second)}
	s.logLiveMirrorExpiry(c, time.Now(), "before-dispatch")
	s.logLiveMirrorArbitrationDrop(c, "final-dispatch", "duplicate-or-unpersisted-claim")
	rows := r153CandidateDropRows(t, s)
	if len(rows) != 2 {
		t.Fatalf("central durable expiry/dispatch rows=%d want=2: %v", len(rows), rows)
	}
	seen := map[string]string{}
	for _, row := range rows {
		seen[fmt.Sprint(row["stage"])] = fmt.Sprint(row["reason"])
	}
	if seen["before-dispatch"] != "expired-before-dispatch" ||
		seen["final-dispatch"] != "duplicate-or-unpersisted-claim" {
		t.Fatalf("central durable stage/reasons=%v", seen)
	}
}

func TestR153FastDispatchRequiresNoUnresolvedDurableMoneyState(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	if safe, why := s.liveFastIntentDispatchSafe(ctx); !safe || why != "no-unresolved-durable-money-state" {
		t.Fatalf("empty durable ledgers safe=%v why=%q", safe, why)
	}
	r148RiskTestReservation(t, s.store, "r153-fast-risk")
	if safe, why := s.liveFastIntentDispatchSafe(ctx); safe || !strings.Contains(why, "pending-risk-active:1") {
		t.Fatalf("active durable money risk safe=%v why=%q", safe, why)
	}
}

func TestR153CandidateAuditRecordNeverWaitsForStalledStore(t *testing.T) {
	s := testServer(t)
	r153ArmAuto(s)
	entered := make(chan struct{})
	var once sync.Once
	s.liveCandidateAuditWrite = func(ctx context.Context, _ []storage.AuditEntry) error {
		once.Do(func() { close(entered) })
		<-ctx.Done()
		return ctx.Err()
	}
	c := liveMirrorCandidate{Platform: "kalshi", Ticker: "KXR153-NONBLOCKING", Side: "YES",
		Action: "BUY", Family: "kalshi-flow", Source: "auto-cons-kalshi-flow", At: time.Now()}

	started := time.Now()
	if !s.recordLiveCandidateDrop(c, "AUTO-LIVE-SIGNAL-DROP", "preflight", "fixture", time.Now(), nil) {
		t.Fatal("receipt did not enter an empty audit queue")
	}
	if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
		t.Fatalf("hot record waited %s for the stalled persistence worker", elapsed)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("audit worker did not reach the stalled sink")
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := s.stopLiveCandidateAudit(stopCtx); err == nil {
		t.Fatal("bounded shutdown unexpectedly reported a completed stalled write")
	}
	select {
	case <-s.liveCandidateAuditDone:
	case <-time.After(time.Second):
		t.Fatal("canceled stalled audit worker leaked")
	}
}

func TestR153CandidateAuditFlushPersistsEveryQueuedReceipt(t *testing.T) {
	s := testServer(t)
	r153ArmAuto(s)
	for i := 0; i < 5; i++ {
		c := liveMirrorCandidate{Platform: "kalshi", Ticker: fmt.Sprintf("KXR153-FLUSH-%d", i),
			Side: "NO", Action: "BUY", Family: "spotlag", Source: "auto-cons-spotlag", At: time.Now()}
		if !s.recordLiveCandidateDrop(c, "AUTO-LIVE-SIGNAL-DROP", "preflight",
			fmt.Sprintf("fixture-%d", i), time.Now(), nil) {
			t.Fatalf("receipt %d did not enter an empty audit queue", i)
		}
	}
	rows := r153CandidateDropRows(t, s)
	if len(rows) != 5 {
		t.Fatalf("flushed exact receipts=%d want=5: %v", len(rows), rows)
	}
	seen := map[string]bool{}
	for _, row := range rows {
		seen[fmt.Sprint(row["ticker"])] = true
	}
	for i := 0; i < 5; i++ {
		if !seen[fmt.Sprintf("KXR153-FLUSH-%d", i)] {
			t.Fatalf("flush lost exact candidate %d: %v", i, rows)
		}
	}
}

func TestR163CandidateAuditCoalescesDuplicateFloodWithoutCrowdingFinalDecision(t *testing.T) {
	s := testServer(t)
	s.liveDispatchShadowQuietDelay = -1
	s.liveCandidateAuditCapacity = 1
	r153ArmAuto(s)

	const duplicates = 10000
	for i := 0; i < duplicates; i++ {
		c := liveMirrorCandidate{
			Platform: "kalshi", Ticker: fmt.Sprintf("KXR163-DUP-%d", i%2), Side: "YES",
			Action: "BUY", Family: "kalshi-flow", Source: "auto-cons-kalshi-flow",
			At: time.Now(),
		}
		if !s.recordLiveCandidateDrop(c, "AUTO-LIVE-SIGNAL-DROP", "signal-queue",
			"duplicate-coalesced-live-signal-intent", time.Now(),
			map[string]any{"coalesced_with_age_ms": i % 500}) {
			t.Fatalf("duplicate %d did not enter the counted summary", i)
		}
	}

	final := liveMirrorCandidate{
		Platform: "kalshi", Ticker: "KXR163-FINAL", Side: "NO", Action: "BUY",
		Family: "spotlag", Source: "auto-cons-spotlag", At: time.Now(),
	}
	if !s.recordLiveCandidateDrop(final, "AUTO-LIVE-MIRROR-DROP", "final-dispatch",
		"unique-final-economic-refusal", time.Now(), nil) {
		t.Fatal("a unique final decision was crowded out by duplicate telemetry")
	}

	rows := r153CandidateDropRows(t, s)
	summaryCount, finalCount := 0, 0
	for _, row := range rows {
		switch fmt.Sprint(row["reason"]) {
		case "duplicate-coalesced-live-signal-intent":
			if fmt.Sprint(row["receipt_kind"]) != "duplicate-coalesced-summary" {
				t.Fatalf("duplicate persisted as a raw row instead of a counted summary: %+v", row)
			}
			n, ok := row["occurrence_count"].(float64)
			if !ok || n <= 0 {
				t.Fatalf("duplicate summary has no occurrence count: %+v", row)
			}
			summaryCount += int(n)
			if fmt.Sprint(row["meaning"]) == "" {
				t.Fatalf("duplicate summary did not explain its semantics: %+v", row)
			}
		case "unique-final-economic-refusal":
			finalCount++
			if row["receipt_kind"] != nil {
				t.Fatalf("unique final decision was aggregated: %+v", row)
			}
			if fmt.Sprint(row["ticker"]) != final.Ticker ||
				fmt.Sprint(row["stage"]) != "final-dispatch" {
				t.Fatalf("unique final decision lost exact identity: %+v", row)
			}
		}
	}
	if summaryCount != duplicates || finalCount != 1 {
		t.Fatalf("durable duplicate/final counts=%d/%d want=%d/1; rows=%+v",
			summaryCount, finalCount, duplicates, rows)
	}
	if dropped := s.liveCandidateAuditDropped.Load(); dropped != 0 {
		t.Fatalf("counted duplicates still overflowed the exact queue: dropped=%d", dropped)
	}

	s.liveMu.Lock()
	defer s.liveMu.Unlock()
	for _, row := range s.liveLog {
		if row["reason"] == "duplicate-coalesced-live-signal-intent" &&
			row["receipt_kind"] != "duplicate-coalesced-summary" {
			t.Fatalf("raw duplicates still flooded the bounded operator ring: %+v", row)
		}
	}
}

func TestR153CandidateAuditOverflowIsExplicitInMemoryAndDurable(t *testing.T) {
	s := testServer(t)
	s.liveCandidateAuditCapacity = 1
	r153ArmAuto(s)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	s.liveCandidateAuditWrite = func(ctx context.Context, entries []storage.AuditEntry) error {
		first := false
		once.Do(func() {
			first = true
			close(entered)
		})
		if first {
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return s.store.AuditBatch(ctx, entries)
	}
	record := func(ticker string) bool {
		return s.recordLiveCandidateDrop(liveMirrorCandidate{Platform: "kalshi", Ticker: ticker,
			Side: "YES", Action: "BUY", Family: "kalshi-flow", Source: "auto-cons-kalshi-flow",
			At: time.Now()}, "AUTO-LIVE-SIGNAL-DROP", "preflight", "fixture", time.Now(), nil)
	}
	if !record("KXR153-OVERFLOW-1") {
		t.Fatal("first receipt did not enter queue")
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("worker did not reach blocked first write")
	}
	if !record("KXR153-OVERFLOW-2") {
		t.Fatal("second receipt did not fill the bounded queue")
	}
	if record("KXR153-OVERFLOW-3") {
		t.Fatal("third receipt unexpectedly entered a full capacity-one queue")
	}
	s.liveMu.Lock()
	memoryOverflow := false
	for _, row := range s.liveLog {
		if row["event"] == "AUTO-LIVE-CANDIDATE-AUDIT-OVERFLOW" && row["ticker"] == "KXR153-OVERFLOW-3" {
			memoryOverflow = true
		}
	}
	s.liveMu.Unlock()
	if !memoryOverflow {
		t.Fatal("full queue did not leave exact in-memory overflow telemetry")
	}
	close(release)
	flushCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := s.flushLiveCandidateAudit(flushCtx); err != nil {
		t.Fatalf("flush after store recovery: %v", err)
	}
	rows, err := s.store.ListAudit(context.Background(), 100)
	if err != nil {
		t.Fatal(err)
	}
	drops, overflow := 0, 0
	for _, row := range rows {
		switch row.Category {
		case "live-candidate-drop":
			drops++
		case "live-candidate-audit-overflow":
			overflow++
		}
	}
	if drops != 2 || overflow != 1 {
		t.Fatalf("durable exact/overflow rows=%d/%d want=2/1: %+v", drops, overflow, rows)
	}
}
