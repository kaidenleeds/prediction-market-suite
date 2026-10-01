package server

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func r166SpotlagSignal(ticker string) storage.Signal {
	return storage.Signal{
		Platform: "kalshi", Ticker: ticker, Title: "R166 exact route",
		Side: "YES", SignalType: "spotlag", EntryPrice: .40, ResolveHours: 1,
		ExecExpr: r147InputReceiptExpr("K-SPOT", time.Now().UTC()),
	}
}

func TestR166FirstEverStaticRouteTracksWithoutVerdictCache(t *testing.T) {
	s := testServer(t)
	s.verdMu.Lock()
	s.verdCache, s.verdAt = nil, time.Time{}
	s.verdMu.Unlock()

	sig := r166SpotlagSignal("KXR166-FIRST")
	registered, why := s.gfRegisteredRouteForSignal(sig)
	if why != "" || !registered.Static || registered.Contract.InputTopology != "K-SPOT" ||
		registered.Contract.SystemID != "spotlag" {
		t.Fatalf("first-ever exact route was not registered: route=%+v reason=%q", registered, why)
	}

	paperCh := r163InstallPaperCapture(s, 1, false)
	t.Cleanup(func() { r163RemovePaperCapture(s) })
	s.genfollowTriggeredSystems(context.Background(), sig)
	select {
	case intent := <-paperCh:
		found := false
		for _, queued := range intent.Signals {
			found = found || (queued.Signal.SignalType == "spotlag" && queued.Signal.Ticker == sig.Ticker)
		}
		if !found {
			t.Fatalf("first-ever direct route missing from realistic Paper batch: %+v", intent.Signals)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("first-ever static route disappeared before the realistic Paper worker")
	}
}

func TestR166InsertSignalPreservesTriggerClockWithoutInventingSourceClock(t *testing.T) {
	s := testServer(t)
	paperCh := r163InstallPaperCapture(s, 2, false)
	t.Cleanup(func() { r163RemovePaperCapture(s) })
	before := time.Now().UTC()
	sig := storage.Signal{Platform: "kalshi", Ticker: "KXR166-TRIGGER-CLOCK",
		Title: "R166 trigger clock", Side: "YES", SignalType: "kalshi-flow",
		EntryPrice: .40, ResolveHours: 1}
	if err := s.insertSignal(context.Background(), sig); err != nil {
		t.Fatal(err)
	}
	after := time.Now().UTC()
	var queued genfollowPaperSignal
	select {
	case intent := <-paperCh:
		if len(intent.Signals) != 1 {
			t.Fatalf("trigger-clock Paper batch=%+v", intent.Signals)
		}
		queued = intent.Signals[0]
	case <-time.After(250 * time.Millisecond):
		t.Fatal("trigger-clock signal never reached realistic Paper")
	}
	inputAt := r147SignalInputObservedAt(queued.Signal)
	if queued.SignalAt.Before(before) || queued.SignalAt.After(after) || !inputAt.IsZero() {
		t.Fatalf("trigger clock was minted late or a source clock was invented: before=%s signal=%s input=%s after=%s",
			before, queued.SignalAt, inputAt, after)
	}
	var storedExpr string
	if err := s.store.DBForTest().QueryRow(`SELECT exec_expr FROM signal_log
WHERE ticker=? AND signal_type=? ORDER BY id DESC LIMIT 1`, sig.Ticker, sig.SignalType).Scan(&storedExpr); err != nil {
		t.Fatal(err)
	}
	stored := storage.Signal{ExecExpr: storedExpr}
	if got := r147SignalInputObservedAt(stored); !got.IsZero() || r147SignalInputTopology(stored) != "K" {
		t.Fatalf("durable clock/topology invented or lost: input_at=%s topology=%q expr=%q",
			got, r147SignalInputTopology(stored), storedExpr)
	}
}

func TestR166InsertSignalDoesNotCollapseDistinctInputTopologies(t *testing.T) {
	s := testServer(t)
	base := storage.Signal{Platform: "kalshi", Ticker: "KXR166-TOPOLOGY-DEDUP",
		Title: "R166 topology dedup", Side: "YES", SignalType: "arb", EntryPrice: .4,
		ResolveHours: 1}
	for _, topology := range []string{"K-PINT", "K-PUS"} {
		sig := base
		sig.ExecExpr = r147InputReceiptExpr(topology, time.Now().UTC())
		if err := s.insertSignal(context.Background(), sig); err != nil {
			t.Fatalf("insert %s: %v", topology, err)
		}
	}
	var n int
	if err := s.store.DBForTest().QueryRow(`SELECT COUNT(*) FROM signal_log
WHERE platform='kalshi' AND ticker=? AND signal_type='arb'`, base.Ticker).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("distinct topologies collapsed into %d signal row(s), want 2", n)
	}
}

func TestR166MissingOrStaleVerdictCannotEraseStaticRoute(t *testing.T) {
	s := testServer(t)
	sig := r166SpotlagSignal("KXR166-STALE")

	s.verdMu.Lock()
	s.verdCache = []verdictEnt{{
		Family: "taker:spotlag@kalshi", SourceFamily: "spotlag", Platform: "kalshi",
		OriginLayer: "model", Route: "taker", Side: "YES", Group: "taker",
		N: 20, Mean: -.50, ProfitEvidence: true, FillConditioned: true,
		EvidenceTier: "authenticated_live_fill",
	}}
	s.verdAt = time.Now().Add(-swStale - time.Second)
	s.verdMu.Unlock()
	if route, why := s.gfRegisteredRouteForSignal(sig); why != "" || !route.Static {
		t.Fatalf("stale verdict erased static route: route=%+v reason=%q", route, why)
	}

	s.verdMu.Lock()
	s.verdAt = time.Now()
	s.verdMu.Unlock()
	if _, why := s.gfRegisteredRouteForSignal(sig); why != "fresh-authenticated-exchange-profit-evidence-nonpositive" {
		t.Fatalf("fresh authenticated negative evidence did not defeat only its exact route: %q", why)
	}
}

func TestR166FirstEverStaticRouteCanReachRealisticDelayedFill(t *testing.T) {
	const ticker = "KXR166-FIRST-FILL"
	s, _, quoteFor := r154PaperTakerFixture(t, ticker)
	key := gfRosterKey("kalshi-flow", "kalshi", "YES", "taker")
	s.verdMu.Lock()
	s.verdCache, s.verdAt = nil, time.Time{}
	s.verdMu.Unlock()
	s.swMu.Lock()
	s.swTable = nil
	s.swMu.Unlock()
	s.completeBookSignalFn = func(in storage.Signal) (storage.Signal, string, bool) {
		return quoteFor(in, .38, .40, 20)
	}
	sig := storage.Signal{Platform: "kalshi", Ticker: ticker, Title: "R166 first fill",
		Side: "YES", SignalType: "kalshi-flow", EntryPrice: .40, ResolveHours: 1,
		ExecExpr: r147InputReceiptExpr("K", time.Now().UTC())}
	s.genfollowConsider(context.Background(), sig)

	s.gfBookMu.Lock()
	book := s.gfLoadLocked().Subs[key]
	open := 0
	if book != nil {
		open = len(book.Open)
	}
	s.gfBookMu.Unlock()
	if open != 1 {
		var detail string
		_ = s.store.DBForTest().QueryRow(`SELECT detail FROM audit_log
WHERE category='system-route' AND message LIKE ? ORDER BY id DESC LIMIT 1`,
			"%"+ticker+"%").Scan(&detail)
		t.Fatalf("first-ever static route created %d realistic delayed fills; want 1; terminal=%s", open, detail)
	}
}

func TestR166StaticRouteBindingIsSideAndTopologyExact(t *testing.T) {
	s := testServer(t)
	base := storage.Signal{Platform: "kalshi", Ticker: "KXR166-ARB", Title: "R166 arb",
		Side: "YES", SignalType: "arb", EntryPrice: .40, ResolveHours: 1}
	if _, why := s.gfRegisteredRouteForSignal(base); why != "prospective-allocation-signal-topology-ambiguous" {
		t.Fatalf("multi-topology route without source identity reason=%q", why)
	}
	base.ExecExpr = r147InputReceiptExpr("K-PUS", time.Now().UTC())
	route, why := s.gfRegisteredRouteForSignal(base)
	if why != "" || route.Contract.InputTopology != "K-PUS" {
		t.Fatalf("exact K-PUS route binding=%+v reason=%q", route, why)
	}
	if got := gfRosterKey("arb", "kalshi", "YES", "taker", gfRosterTopology(route.Contract)); !strings.Contains(got, ":k-pus") || !strings.HasSuffix(got, "-k") {
		t.Fatalf("multi-topology Paper ledger key pooled input sources: %q", got)
	}
	wrongSide := base
	wrongSide.Side = "NO"
	if _, why := s.gfRegisteredRouteForSignal(wrongSide); why != "exact-static-signal-contract-absent" {
		t.Fatalf("wrong side borrowed YES contract: %q", why)
	}
}

func TestR166UnbindableExactOpportunityGetsDurablePaperExclusion(t *testing.T) {
	s := testServer(t)
	sig := storage.Signal{Platform: "kalshi", Ticker: "KXR166-TERMINAL", Title: "R166 terminal",
		Side: "YES", SignalType: "arb", EntryPrice: .40, ResolveHours: 1}
	s.genfollowConsider(context.Background(), sig)
	s.genfollowConsider(context.Background(), sig)
	var terminals int
	if err := s.store.DBForTest().QueryRow(`SELECT COUNT(*) FROM audit_log
WHERE category='system-route' AND message LIKE ?`, "REJECTED%"+sig.Ticker+"%").Scan(&terminals); err != nil {
		t.Fatal(err)
	}
	if terminals != 2 {
		t.Fatalf("exact opportunities received %d durable terminals; want 2", terminals)
	}

	var detail string
	if err := s.store.DBForTest().QueryRow(`SELECT detail FROM audit_log
WHERE category='system-route' AND message LIKE ? ORDER BY id DESC LIMIT 1`,
		"REJECTED%"+sig.Ticker+"%").Scan(&detail); err != nil {
		t.Fatal(err)
	}
	var receipt map[string]any
	if err := json.Unmarshal([]byte(detail), &receipt); err != nil {
		t.Fatal(err)
	}
	if receipt["reason"] != "prospective-allocation-signal-topology-ambiguous" ||
		receipt["order_result"] != "REJECTED" {
		t.Fatalf("exact opportunity terminal=%+v", receipt)
	}
}

func TestR166SpecialistTakerRefusesToInventAmbiguousTopology(t *testing.T) {
	s := testServer(t)
	sig := storage.Signal{Platform: "kalshi", Ticker: "KXR166-SPECIALIST-AMBIG",
		Title: "R166 ambiguous specialist", Side: "YES", EntryPrice: .18, ResolveHours: 1}

	if topology, why := r166SpecialistInputTopology(sig, "cheapband", ""); topology != "" || why != "specialist-input-topology-ambiguous" {
		t.Fatalf("ambiguous specialist topology=%q reason=%q", topology, why)
	}
	if topology, why := r166SpecialistInputTopology(sig, "rawflow", ""); topology != "K" || why != "" {
		t.Fatalf("single-source specialist topology=%q reason=%q", topology, why)
	}

	s.enqueueR166SpecialistTaker(context.Background(), sig, "cheapband", "",
		"r166-test:ambiguous-specialist")
	var detail string
	if err := s.store.DBForTest().QueryRow(`SELECT detail FROM audit_log
WHERE category='system-route' AND message LIKE ? ORDER BY id DESC LIMIT 1`,
		"PAPER-NOT-OBSERVED cheapband%"+sig.Ticker+"%").Scan(&detail); err != nil {
		t.Fatal(err)
	}
	var receipt map[string]any
	if err := json.Unmarshal([]byte(detail), &receipt); err != nil {
		t.Fatal(err)
	}
	if receipt["reason"] != "specialist-input-topology-ambiguous" ||
		receipt["order_result"] != "PAPER-NOT-OBSERVED" {
		t.Fatalf("ambiguous specialist terminal=%+v", receipt)
	}
}

func TestR166PaperQueueAdmissionDropReceiptsEveryExactSignal(t *testing.T) {
	s := testServer(t)
	at := time.Now().UTC()
	s.genfollowPaperWorkerMu.Lock()
	s.genfollowPaperWorkerStopping = true
	s.genfollowPaperWorkerMu.Unlock()
	batch := []genfollowPaperSignal{
		{Signal: storage.Signal{Platform: "kalshi", Ticker: "KXR166-DROP-A", Side: "YES",
			SignalType: "spotlag", ExecExpr: r147InputReceiptExpr("K-SPOT", at)}, SignalAt: at},
		{Signal: storage.Signal{Platform: "kalshi", Ticker: "KXR166-DROP-B", Side: "NO",
			SignalType: "spotlag", ExecExpr: r147InputReceiptExpr("K-SPOT", at)}, SignalAt: at},
	}
	if s.enqueueGenfollowPaperIntent(batch) {
		t.Fatal("stopping Paper queue accepted exact signal batch")
	}

	var detail string
	if err := s.store.DBForTest().QueryRow(`SELECT detail FROM audit_log
WHERE category='paper-taker-attempt' ORDER BY id DESC LIMIT 1`).Scan(&detail); err != nil {
		t.Fatal(err)
	}
	var receipt struct {
		Reason      string           `json:"reason"`
		OrderResult string           `json:"order_result"`
		SignalCount int              `json:"signal_count"`
		Signals     []map[string]any `json:"signals"`
	}
	if err := json.Unmarshal([]byte(detail), &receipt); err != nil {
		t.Fatal(err)
	}
	if receipt.Reason != "paper-worker-stopping" || receipt.OrderResult != "PAPER-NOT-OBSERVED" ||
		receipt.SignalCount != 2 || len(receipt.Signals) != 2 {
		t.Fatalf("queue terminal receipt=%+v", receipt)
	}
	for i, row := range receipt.Signals {
		if row["ticker"] != batch[i].Signal.Ticker || row["input_topology"] != "K-SPOT" ||
			row["signal_contract"] == "" || row["state"] != "PAPER-NOT-OBSERVED" {
			t.Fatalf("queue terminal signal[%d]=%+v", i, row)
		}
	}
}

func TestR167EveryRealisticContractCanMintItsExactCanonicalOpportunity(t *testing.T) {
	s := testServer(t)
	triggerAt := time.Date(2026, 7, 22, 12, 0, 0, 123456000, time.UTC)
	realistic, logOnly := 0, 0
	for i, contract := range r147SystemVariantSignalContracts() {
		executionClass, isRealistic := r147ContractPaperExecution(contract)
		if !isRealistic {
			logOnly++
			if !strings.HasPrefix(executionClass, "log_only_") {
				t.Fatalf("contract %s has neither realistic nor explicit log-only tracking: %q",
					r147SignalContractIdentity(contract), executionClass)
			}
			continue
		}
		realistic++
		if contract.ExecutionDerivative {
			in := storage.ProperMomentumSellIntent{IntentID: fmt.Sprintf("derivative-%03d", i),
				SourceID: fmt.Sprintf("r139ms:derivative-%03d", i), Created: triggerAt,
				Candidate: storage.ProperMomentumSellCandidate{Observed: triggerAt,
					Cohort: "proper-score-v2|transform=" + strings.ToLower(strings.TrimPrefix(
						contract.InputTopology, "K-PROPER-")), Ticker: fmt.Sprintf("R167-SELL-%03d", i),
					OwnedSide: contract.Side, ObservedSellPrice: .60, ObservedSellFee: .01}}
			candidate := s.properMomentumCanonicalCandidate(in)
			if candidate.Action != "SELL" || candidate.ProspectiveTimeInForce != liveProspectiveFOK ||
				candidate.SignalContractID != r147SignalContractIdentity(contract) {
				t.Fatalf("SELL derivative borrowed generic BUY/IOC producer: contract=%+v candidate=%+v",
					contract, candidate)
			}
			if _, _, ok := s.executionShadowBuildCandidate(candidate,
				"registered proper momentum derivative", candidate.ShadowAttemptID); !ok {
				t.Fatalf("SELL derivative cannot mint canonical attempt: %+v", contract)
			}
			continue
		}
		sig := storage.Signal{
			Platform:     contract.ExecutionVenue,
			Ticker:       fmt.Sprintf("R167-CANONICAL-%03d", i),
			Title:        "R167 all-system canonical opportunity",
			Side:         contract.Side,
			SignalType:   contract.SystemID,
			EntryPrice:   .40,
			ResolveHours: 1,
			ExecExpr:     r147InputReceiptExpr(contract.InputTopology, triggerAt),
		}
		candidate := liveMirrorCandidate{Platform: sig.Platform, Ticker: sig.Ticker,
			Side: sig.Side, Family: sig.SignalType, At: triggerAt}
		bound, why, declared := bindStaticTakerSignalCandidate(candidate, sig)
		if !declared || why != "" || bound.SignalContractID != r147SignalContractIdentity(contract) ||
			bound.InputTopology != contract.InputTopology {
			t.Fatalf("realistic contract cannot bind exact opportunity: contract=%+v bound=%+v declared=%t reason=%q",
				contract, bound, declared, why)
		}
		if attemptID := s.executionShadowSignalAttemptID(sig, 0, triggerAt); attemptID == "" {
			t.Fatalf("realistic contract cannot mint canonical attempt: %+v", contract)
		}
	}
	if realistic != 354 || logOnly != 31 || realistic+logOnly != 385 {
		t.Fatalf("all-system execution coverage changed: realistic=%d log_only=%d total=%d",
			realistic, logOnly, realistic+logOnly)
	}
}
