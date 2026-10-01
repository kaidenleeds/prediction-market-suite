package server

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func registerRelationFixture(t *testing.T, s *Server, eventID string, tickers ...string) {
	t.Helper()
	event := storage.CanonicalEventSpec{EventID: eventID, EventType: "sports-game", Domain: "test",
		Title: eventID, SourceArtifact: "fixture", OutcomeSetStatus: "complete", EvidenceJSON: `{}`}
	payoffs := make([]storage.CanonicalPayoffSpec, 0, len(tickers))
	instruments := make([]storage.CanonicalInstrumentSpec, 0, len(tickers))
	for _, ticker := range tickers {
		payoff := eventID + "|" + ticker
		payoffs = append(payoffs, storage.CanonicalPayoffSpec{PayoffID: payoff, EventID: eventID,
			Label: ticker, PredicateJSON: `{}`, SourceArtifact: "fixture", IdentityStatus: "verified",
			PayoutFloor: 0, PayoutCeiling: 1, EvidenceJSON: `{}`})
		instruments = append(instruments, storage.CanonicalInstrumentSpec{Venue: "kalshi", Ticker: ticker,
			EventID: eventID, PayoffID: payoff, NativeSide: "YES", Orientation: "same", MarketKind: "binary",
			RulesArtifact: "fixture", IdentityStatus: "verified", EvidenceJSON: `{}`})
	}
	catalog := make([]storage.CatalogRow, 0, len(tickers))
	for _, ticker := range tickers {
		// The canonical fixture deliberately groups related subbets into one broad game. Kalshi's
		// native event_ticker is narrower: each spread/total/prop header is distinct.
		catalog = append(catalog, storage.CatalogRow{Venue: "kalshi", Ticker: ticker,
			EventKey: eventID + "|header|" + ticker, Kind: "binary", Title: ticker})
	}
	if err := s.store.UpsertMarketCatalog(context.Background(), catalog); err != nil {
		t.Fatal(err)
	}
	if s.kmkts == nil {
		s.kmkts = map[string]kalshi.Market{}
	}
	if s.kmktsAt == nil {
		s.kmktsAt = map[string]time.Time{}
	}
	for _, row := range catalog {
		s.kmkts[row.Ticker] = kalshi.Market{Ticker: row.Ticker, EventTicker: row.EventKey}
		s.kmktsAt[row.Ticker] = time.Now()
	}
	if _, err := s.store.RegisterCanonicalBatch(context.Background(), []storage.CanonicalEventSpec{event}, payoffs, instruments); err != nil {
		t.Fatal(err)
	}
}

func TestFundedRelationDecisionMatchesOpenCanonicalEvent(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	registerRelationFixture(t, s, "game-1", "SPREAD", "TOTAL")
	registerRelationFixture(t, s, "game-2", "OTHER")

	first := s.fundedSingleRelation(ctx, "kalshi", "SPREAD", "YES", "system-a", "taker")
	if first.state != "independent" {
		t.Fatalf("first state=%s", first.state)
	}
	firstID, err := first.allowBeforePlacement("fixture-pass", 1, .4, .01)
	if err != nil {
		t.Fatal(err)
	}
	second := s.fundedSingleRelation(ctx, "kalshi", "TOTAL", "NO", "system-b", "taker")
	if second.state != "dependent" || len(second.matchedPositions) != 1 || len(second.legs) != 1 || second.legs[0].RelationState != "dependent" {
		t.Fatalf("second=%+v", second)
	}
	if second.matchedPositions[0] == "" || firstID == "" {
		t.Fatal("missing stable match identity")
	}
	other := s.fundedSingleRelation(ctx, "kalshi", "OTHER", "YES", "system-c", "taker")
	if other.state != "independent" {
		t.Fatalf("other state=%s", other.state)
	}
	unknown := s.fundedSingleRelation(ctx, "kalshi", "MISSING", "YES", "system-u", "taker")
	if unknown.state != "unknown" || unknown.legs[0].RelationState != "unknown" {
		t.Fatalf("unknown=%+v", unknown)
	}
}

func TestGenfollowBootReconcilesOnlyExactRetainedSpecialistOutcome(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	registerRelationFixture(t, s, "game-genfollow-race", "GF-RACE")
	decision := s.fundedSingleRelation(ctx, "kalshi", "GF-RACE", "YES",
		"poly-pred-kalshi:yes:taker-k", "taker")
	receiptID, err := decision.allowBeforePlacement("fixture-pass", 20, .50, .35)
	if err != nil {
		t.Fatal(err)
	}
	settled := time.Now().UTC().Add(-time.Hour)
	if inserted, err := s.store.RecordFundedRelationOutcome(ctx, receiptID, settled,
		4.825, "settled-win"); err != nil || !inserted {
		t.Fatalf("wrong first writer inserted=%v err=%v", inserted, err)
	}
	lot := kfPos{TS: settled.Add(-time.Hour).Format(time.RFC3339Nano), Platform: "kalshi",
		Ticker: "GF-RACE", Side: "YES", Price: .50, Contracts: 20, Fee: .35,
		RelationReceiptID: receiptID}
	s.gfBook = &gfBookState{Subs: map[string]*kfBook{
		"gf:poly-pred-kalshi:yes:taker-k": {Closed: []kfClosed{{
			kfPos: lot, Payout: 1, PnL: 9.65, Won: true,
			SettledTS: settled.Format(time.RFC3339Nano), Reason: "settled",
		}}},
	}}
	if err := s.reconcileGenfollowFundedRelationOutcomes(ctx); err != nil {
		t.Fatal(err)
	}
	var original, corrected float64
	var source, reason string
	if err := s.store.DBForTest().QueryRow(`SELECT pnl_dollars FROM funded_relation_outcomes
WHERE receipt_id=?`, receiptID).Scan(&original); err != nil || original != 4.825 {
		t.Fatalf("original outcome=%v err=%v", original, err)
	}
	if err := s.store.DBForTest().QueryRow(`SELECT corrected_pnl_dollars,corrected_result_source,reason
FROM funded_relation_outcome_corrections WHERE receipt_id=?`, receiptID).
		Scan(&corrected, &source, &reason); err != nil {
		t.Fatal(err)
	}
	if corrected != 9.65 || source != "genfollow-settlement" ||
		reason != "specialist-position-outcome-supersedes-contract-allocation-v1" {
		t.Fatalf("correction pnl=%v source=%q reason=%q", corrected, source, reason)
	}
	// Repeated boot reconciliation is idempotent and keeps one append-only correction.
	if err := s.reconcileGenfollowFundedRelationOutcomes(ctx); err != nil {
		t.Fatal(err)
	}
	var corrections int
	if err := s.store.DBForTest().QueryRow(`SELECT COUNT(*) FROM funded_relation_outcome_corrections
WHERE receipt_id=?`, receiptID).Scan(&corrections); err != nil || corrections != 1 {
		t.Fatalf("corrections=%d err=%v", corrections, err)
	}
	rows, err := s.store.FundedSystemPerformanceRows(ctx, time.Now().UTC())
	if err != nil || len(rows) != 1 || rows[0].TotalRealizedProfitDollars != 9.65 ||
		rows[0].ReconciledReceipts != 1 {
		t.Fatalf("funded rows=%+v err=%v", rows, err)
	}
}

func TestFundedRelationMaterializesKnownCatalogEventBeforeBackgroundSweep(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	if err := s.store.UpsertMarketCatalog(ctx, []storage.CatalogRow{
		{Venue: "kalshi", Ticker: "COLD-SPREAD", EventKey: "KX-COLD-GAME", Kind: "spread", Title: "Cold game spread"},
		{Venue: "kalshi", Ticker: "COLD-TOTAL", EventKey: "KX-COLD-GAME", Kind: "total", Title: "Cold game total"},
	}); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := s.store.CurrentCanonicalInstrument(ctx, "kalshi", "COLD-SPREAD"); err != nil || ok {
		t.Fatalf("fixture unexpectedly pre-materialized: ok=%v err=%v", ok, err)
	}
	s.kmkts = map[string]kalshi.Market{
		"COLD-SPREAD": {Ticker: "COLD-SPREAD", EventTicker: "KX-COLD-SPREAD"},
		"COLD-TOTAL":  {Ticker: "COLD-TOTAL", EventTicker: "KX-COLD-TOTAL"},
	}
	s.kmktsAt = map[string]time.Time{}
	first := s.fundedSingleRelation(ctx, "kalshi", "COLD-SPREAD", "YES", "cold-a", "taker")
	if first.state != "independent" || !first.executionIdentity {
		t.Fatalf("first catalog leg=%+v", first)
	}
	if _, err := first.allowBeforePlacement("fixture-pass", 1, .4, .01); err != nil {
		t.Fatal(err)
	}
	second := s.fundedSingleRelation(ctx, "kalshi", "COLD-TOTAL", "NO", "cold-b", "taker")
	if second.state != "dependent" || !second.executionIdentity || len(second.matchedPositions) != 1 {
		t.Fatalf("catalog siblings were not related before the background sweep: %+v", second)
	}
}

func TestFundedPaperAdmissionBlocksAddedGhibaudoAcrossLedgers(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	totalTicker := "KXATPCHALLENGERTOTAL-26JUL16ADDGHI-3"
	if err := s.store.UpsertMarketCatalog(ctx, []storage.CatalogRow{
		{Venue: "kalshi", Ticker: addedTicker, EventKey: addedGhibaudoEvent, Kind: "winner", Title: "Added wins"},
		{Venue: "kalshi", Ticker: ghibaudoTicker, EventKey: addedGhibaudoEvent, Kind: "winner", Title: "Ghibaudo wins"},
		{Venue: "kalshi", Ticker: totalTicker, EventKey: "KXATPCHALLENGERTOTAL-26JUL16ADDGHI", Kind: "total", Title: "Total sets"},
	}); err != nil {
		t.Fatal(err)
	}
	s.kmkts = map[string]kalshi.Market{
		addedTicker:    {Ticker: addedTicker, EventTicker: addedGhibaudoEvent},
		ghibaudoTicker: {Ticker: ghibaudoTicker, EventTicker: addedGhibaudoEvent},
		totalTicker:    {Ticker: totalTicker, EventTicker: "KXATPCHALLENGERTOTAL-26JUL16ADDGHI"},
	}
	s.kmktsAt = map[string]time.Time{}

	first := s.fundedSingleRelation(ctx, "kalshi", addedTicker, "YES", "generic-first", "taker")
	if _, err := first.allowBeforePlacement("passed", 13, .54, .12); err != nil {
		t.Fatalf("first outcome refused: %v", err)
	}

	// A specialist ledger is not allowed to bypass the generic ledger's open event header.
	specialist := kfPos{Ticker: ghibaudoTicker, Side: "YES", Price: .52, Contracts: 13, Fee: .12, Platform: "kalshi"}
	if s.stampFundedKFRelation(ctx, &specialist, "specialist-sibling", "taker") {
		t.Fatal("specialist ledger admitted Ghibaudo while Added was already funded")
	}
	if specialist.RelationReceiptID != "" {
		t.Fatal("rejected specialist lot received an accepted relation id")
	}

	sibling := s.fundedSingleRelation(ctx, "kalshi", ghibaudoTicker, "YES", "generic-sibling", "taker")
	if _, err := sibling.allowBeforePlacement("passed", 13, .52, .12); err == nil ||
		!strings.Contains(err.Error(), "kalshi-event-header-already-funded") {
		t.Fatalf("generic sibling error=%v, want exact event-header refusal", err)
	}

	// A different venue event_ticker under the same real match remains tradable; broader related
	// exposure is handled by the existing relation/cluster controls, not this hard sibling gate.
	distinct := s.fundedSingleRelation(ctx, "kalshi", totalTicker, "YES", "generic-total", "taker")
	if _, err := distinct.allowBeforePlacement("passed", 1, .40, .01); err != nil {
		t.Fatalf("distinct total event_ticker refused: %v", err)
	}

	var rejected int
	if err := s.store.DBForTest().QueryRow(`SELECT COUNT(*) FROM funded_relation_receipts
WHERE allowed=0 AND reason='kalshi-event-header-already-funded'
  AND system_id IN ('specialist-sibling','generic-sibling')`).Scan(&rejected); err != nil {
		t.Fatal(err)
	}
	if rejected != 2 {
		t.Fatalf("durable sibling rejections=%d, want 2", rejected)
	}
}

func TestFundedRelationRejectedHotLoopDedupesByMinute(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	registerRelationFixture(t, s, "game-3", "REJECT")
	one := s.fundedSingleRelation(ctx, "kalshi", "REJECT", "YES", "system-r", "candidate")
	one.reject("stale-book")
	two := s.fundedSingleRelation(ctx, "kalshi", "REJECT", "YES", "system-r", "candidate")
	two.decision = one.decision
	two.reject("stale-book")
	var count int
	if err := s.store.DBForTest().QueryRow(`SELECT COUNT(*) FROM funded_relation_receipts WHERE system_id='system-r'`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("count=%d err=%v", count, err)
	}
}

func TestFundedRelationResearchOutcomeRejectUsesUnclassifiedDurableLane(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	one := s.fundedSingleRelation(ctx, "polymarket", "0xdown", "Down", "auto-cons-pcrypto", "candidate")
	if one.executionIdentity || one.state != "unknown" || len(one.legs) != 1 || one.legs[0].RelationState != "unknown" {
		t.Fatalf("research outcome was given funded relation truth: %+v", one)
	}
	one.reject("track-only")
	two := s.fundedSingleRelation(ctx, "polymarket", "0xdown", "Down", "auto-cons-pcrypto", "candidate")
	two.decision = one.decision
	two.reject("track-only")
	var rawRejects, fundedReceipts, relationErrors int
	if err := s.store.DBForTest().QueryRow(`SELECT COUNT(*) FROM funded_relation_identity_rejections
WHERE system_id='auto-cons-pcrypto' AND candidate_venue='polymarket' AND candidate_ticker='0xdown'
 AND candidate_side='DOWN' AND identity_issue='nonbinary-side,nonfunded-venue'`).Scan(&rawRejects); err != nil {
		t.Fatal(err)
	}
	if err := s.store.DBForTest().QueryRow(`SELECT COUNT(*) FROM funded_relation_receipts
WHERE system_id='auto-cons-pcrypto'`).Scan(&fundedReceipts); err != nil {
		t.Fatal(err)
	}
	if err := s.store.DBForTest().QueryRow(`SELECT COUNT(*) FROM audit_log
WHERE message='funded rejection relation receipt failed'`).Scan(&relationErrors); err != nil {
		t.Fatal(err)
	}
	if rawRejects != 1 || fundedReceipts != 0 || relationErrors != 0 {
		t.Fatalf("raw=%d funded=%d errors=%d", rawRejects, fundedReceipts, relationErrors)
	}
}

func TestFundedRelationBinaryResearchVenueStillUsesRawRejectionLane(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	d := s.beginFundedRelation(ctx, "kalshi", "binary-research", "candidate",
		[]fundedRelationCandidateLeg{{Venue: "polymarket", Ticker: "0xyes", Side: "YES"}})
	if d.executionIdentity || d.state != "unknown" || len(d.legs) != 1 || d.legs[0].RelationState != "unknown" {
		t.Fatalf("binary research venue was treated as funded identity: %+v", d)
	}
	d.reject("track-only")
	var rawRejects, fundedReceipts int
	if err := s.store.DBForTest().QueryRow(`SELECT COUNT(*) FROM funded_relation_identity_rejections
WHERE system_id='binary-research' AND candidate_venue='polymarket' AND candidate_ticker='0xyes'
 AND candidate_side='YES' AND identity_issue='nonfunded-venue'`).Scan(&rawRejects); err != nil {
		t.Fatal(err)
	}
	if err := s.store.DBForTest().QueryRow(`SELECT COUNT(*) FROM funded_relation_receipts
WHERE system_id='binary-research'`).Scan(&fundedReceipts); err != nil {
		t.Fatal(err)
	}
	if rawRejects != 1 || fundedReceipts != 0 {
		t.Fatalf("raw=%d funded=%d", rawRejects, fundedReceipts)
	}
}
