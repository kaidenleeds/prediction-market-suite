package storage

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"
)

func insertAttentionFixture(t *testing.T, st *Store, opportunity string, observed time.Time) int64 {
	t.Helper()
	row := r138EvidenceFixture("control", false)
	row.Observed = observed
	row.SystemID = "attention-spillover-graph"
	row.OpportunityID = opportunity
	row.Cohort = "structural-parent-child-60s-300s"
	row.Blocker = "prospective timed control"
	row.EventVersion, row.CanonicalPayoffID, row.PayoffVersion = registerR138EvidenceInstrument(t, st,
		row.CanonicalEventID+"-"+opportunity, row.Venue, row.Ticker+"-"+opportunity)
	row.CanonicalEventID += "-" + opportunity
	row.Ticker += "-" + opportunity
	id, inserted, err := st.InsertResearchSystemObservation(context.Background(), row)
	if err != nil || !inserted {
		t.Fatalf("attention fixture inserted=%v id=%d err=%v", inserted, id, err)
	}
	return id
}

func TestConcreteAttentionFixedHorizonsStoreExactMarksAndExplicitMisses(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Millisecond)
	id := insertAttentionFixture(t, st, "due", now.Add(-65*time.Second))
	due, err := st.PendingAttentionMarkouts(ctx, now, 20)
	if err != nil || len(due) != 1 || due[0].ObservationID != id || due[0].Horizon != 60 {
		t.Fatalf("pending=%+v err=%v", due, err)
	}
	mark := AttentionMarkout{ObservationID: id, Horizon: 60, Target: due[0].Target,
		Captured: now, Venue: due[0].Venue, Ticker: due[0].Ticker, Side: due[0].Side,
		ExitBid: .45, ExitDepth: 8, Tick: .01, QuoteAge: .1, ExitFee: .01,
		EntryAllIn: due[0].EntryCost + due[0].EntryFee,
		BookSource: "kalshi_book_ws_full", FeeSource: "kalshi:fixture",
		SourceClockID: "kalshi-book:g1:sid2:seq3", Evidence: map[string]any{"fixed_horizon": true}}
	mark.Markout = mark.ExitBid - mark.ExitFee - mark.EntryAllIn
	inserted, err := st.InsertAttentionMarkout(ctx, mark)
	if err != nil || !inserted {
		t.Fatalf("mark inserted=%v err=%v", inserted, err)
	}
	if inserted, err = st.InsertAttentionMarkout(ctx, mark); err != nil || inserted {
		t.Fatalf("duplicate mark inserted=%v err=%v", inserted, err)
	}
	due, err = st.PendingAttentionMarkouts(ctx, now, 20)
	if err != nil || len(due) != 0 {
		t.Fatalf("completed horizon remained due: %+v err=%v", due, err)
	}

	missID := insertAttentionFixture(t, st, "miss", now.Add(-6*time.Minute))
	due, err = st.PendingAttentionMarkouts(ctx, now, 20)
	if err != nil || len(due) != 2 {
		t.Fatalf("late pending=%+v err=%v", due, err)
	}
	for _, pending := range due {
		if pending.ObservationID != missID {
			t.Fatalf("unexpected late observation %+v", pending)
		}
		if inserted, err = st.InsertAttentionMarkoutMiss(ctx, pending, now,
			"fixed_horizon_expired_before_an_exact_fee_complete_exit_was_captured"); err != nil || !inserted {
			t.Fatalf("miss inserted=%v err=%v", inserted, err)
		}
	}
	var marks, misses, authority int
	if err = st.db.QueryRow(`SELECT (SELECT COUNT(*) FROM research_attention_markouts),
(SELECT COUNT(*) FROM research_attention_markout_misses),
(SELECT COALESCE(SUM(funded+paper_authority+live_authority),0) FROM research_attention_markouts)+
(SELECT COALESCE(SUM(funded+paper_authority+live_authority),0) FROM research_attention_markout_misses)`).Scan(&marks, &misses, &authority); err != nil {
		t.Fatal(err)
	}
	if marks != 1 || misses != 2 || authority != 0 {
		t.Fatalf("marks/misses/authority=%d/%d/%d", marks, misses, authority)
	}
	if _, err = st.db.Exec(`UPDATE research_attention_markouts SET executable_markout=9 WHERE observation_id=?`, id); err == nil {
		t.Fatal("append-only attention markout allowed update")
	}
}

func TestConcreteCarryFreezeIsPriorOnlyConservativeAndImmutable(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Millisecond)
	row, inserted, err := st.FreezeCarryInput(ctx, CarryFreezeRequest{FreezeID: "carry-ready",
		Frozen: now, SignalID: 11, Venue: "kalshi", Ticker: "KXCARRY", Side: "YES",
		EntryAllIn: .95, ExpectedHoldDays: .5, Evidence: map[string]any{"prospective": true}})
	if err != nil || !inserted || row.InputState != "ready" || row.VoidPriorN != 0 ||
		math.Abs(row.VoidProbabilityUpper-1) > 1e-12 || math.Abs(row.VoidReserve-.95) > 1e-12 ||
		row.BenchmarkKind != "nominal_cash_floor" || row.BenchmarkRate != 0 || row.BenchmarkReserve != 0 ||
		!strings.Contains(row.BenchmarkJustification, "zero-dollar return floor") {
		t.Fatalf("ready freeze inserted=%v row=%+v err=%v", inserted, row, err)
	}
	originalHash := row.SpecHash
	row, inserted, err = st.FreezeCarryInput(ctx, CarryFreezeRequest{FreezeID: "carry-ready",
		Frozen: now.Add(time.Hour), SignalID: 11, Venue: "kalshi", Ticker: "KXCARRY", Side: "YES",
		EntryAllIn: .50, ExpectedHoldDays: 2})
	if err != nil || inserted || row.SpecHash != originalHash || row.EntryAllIn != .95 || row.ExpectedHoldDays != .5 {
		t.Fatalf("immutable reload inserted=%v row=%+v err=%v", inserted, row, err)
	}
	if _, err = st.db.Exec(`UPDATE research_carry_frozen_inputs SET expected_hold_days=2 WHERE freeze_id='carry-ready'`); err == nil {
		t.Fatal("immutable carry freeze allowed update")
	}

	blocked, inserted, err := st.FreezeCarryInput(ctx, CarryFreezeRequest{FreezeID: "carry-blocked",
		Frozen: now, SignalID: 12, Venue: "polyus", Ticker: "carry-slug", Side: "NO", EntryAllIn: .92})
	if err != nil || !inserted || blocked.InputState != "blocked" || blocked.Blocker != "resolve_horizon_unknown_at_freeze" {
		t.Fatalf("blocked freeze inserted=%v row=%+v err=%v", inserted, blocked, err)
	}
	blocked, inserted, err = st.FreezeCarryInput(ctx, CarryFreezeRequest{FreezeID: "carry-blocked",
		Frozen: now.Add(time.Hour), SignalID: 12, Venue: "polyus", Ticker: "carry-slug", Side: "NO",
		EntryAllIn: .92, ExpectedHoldDays: 1})
	if err != nil || inserted || blocked.InputState != "blocked" || blocked.ExpectedHoldDays != 0 {
		t.Fatalf("later horizon backfilled blocked receipt: inserted=%v row=%+v err=%v", inserted, blocked, err)
	}
}

func TestWilsonUpperUsesConservativeNoHistoryAndFinitePrior(t *testing.T) {
	if got := wilsonUpper(0, 0); got != 1 {
		t.Fatalf("no-history upper=%v want=1", got)
	}
	if got := wilsonUpper(0, 100); !(got > 0 && got < .05) {
		t.Fatalf("zero-in-100 upper=%v", got)
	}
	if got := wilsonUpper(10, 100); !(got > .10 && got < .20) {
		t.Fatalf("ten-in-100 upper=%v", got)
	}
}
