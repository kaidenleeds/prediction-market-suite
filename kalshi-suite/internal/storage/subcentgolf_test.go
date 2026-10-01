package storage

import (
	"context"
	"math"
	"testing"
	"time"
)

func TestSubcentGolfCohortRouteLifecycleAndSettlement(t *testing.T) {
	ctx := context.Background()
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	q := 17.0
	base := SubcentGolfTrial{
		Slot: "2026-07-11T12", Platform: "kalshi", Ticker: "KXPGATOUR-TEST-PLAYER",
		EventTicker: "KXPGATOUR-TEST", Title: "Will Test Player win?", Player: "Test Player",
		Side: "YES", Phase: "live", PhaseEvidence: "ticker-embedded event start",
		PlayerStatus: "unknown", StatusEvidence: "no authoritative player-status field",
		MarketType: "winner", BookSource: "kalshi-ws-depth", QuoteAgeS: .2,
		TickSize: .001, BidPx: .001, AskPx: .002, BidDepth: q, AskDepth: 25,
	}
	taker := base
	taker.Route, taker.FillState, taker.FillPrice, taker.FeePC = "taker", "filled", .002, .01
	if _, inserted, err := st.InsertSubcentGolfTrial(ctx, taker); err != nil || !inserted {
		t.Fatalf("insert taker: inserted=%v err=%v", inserted, err)
	}
	if _, inserted, err := st.InsertSubcentGolfTrial(ctx, taker); err != nil || inserted {
		t.Fatalf("hourly duplicate: inserted=%v err=%v", inserted, err)
	}
	maker := base
	maker.Route, maker.FillState = "maker", "resting"
	makerID, inserted, err := st.InsertSubcentGolfTrial(ctx, maker)
	if err != nil || !inserted {
		t.Fatalf("insert maker: inserted=%v err=%v", inserted, err)
	}
	if err := st.AttachSubcentGolfMaker(ctx, makerID, 77, &q); err != nil {
		t.Fatal(err)
	}
	if err := st.MarkSubcentGolfMakerFilled(ctx, 77, .001, -.0002, "rebate-test", "queue-through"); err != nil {
		t.Fatal(err)
	}
	if err := st.ResolveSubcentGolfTicker(ctx, base.Ticker, 1); err != nil {
		t.Fatal(err)
	}
	report, err := st.SubcentGolfReport(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if report.Total != 2 || report.Settled != 2 || report.OpenMaker != 0 || !report.ResearchOnly {
		t.Fatalf("report totals: %+v", report)
	}
	for _, g := range report.Groups {
		if g.PlayerStatus != "unknown" || g.TerminalStatus != "active" {
			t.Fatalf("group mixed observation and terminal status: %+v", g)
		}
	}
	var takerNet, makerNet float64
	for _, r := range report.Recent {
		if r.PlayerStatus != "unknown" || r.TerminalStatus != "active" || !r.Settled || r.NetPC == nil {
			t.Fatalf("settlement/status missing: %+v", r)
		}
		if r.Route == "taker" {
			takerNet = *r.NetPC
		} else {
			makerNet = *r.NetPC
			if r.QueueAhead == nil || *r.QueueAhead != q || r.RebatePC != .0002 {
				t.Fatalf("maker queue/rebate missing: %+v", r)
			}
		}
	}
	if math.Abs(takerNet-.988) > 1e-9 || math.Abs(makerNet-.9992) > 1e-9 {
		t.Fatalf("net taker/maker = %.6f/%.6f", takerNet, makerNet)
	}
}

func TestSubcentGolfOrphanMakerCanceledNotFilled(t *testing.T) {
	ctx := context.Background()
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	trial := SubcentGolfTrial{Slot: "old", Ticker: "KXGOLF-OLD", Side: "YES", Phase: "unknown",
		PlayerStatus: "unknown", MarketType: "prop", Route: "maker", BookSource: "kalshi-ws-depth",
		TickSize: .001, AskPx: .003, FillState: "resting"}
	id, inserted, err := st.InsertSubcentGolfTrial(ctx, trial)
	if err != nil || !inserted {
		t.Fatalf("insert: %v %v", inserted, err)
	}
	old := time.Now().UTC().Add(-time.Hour).Format(time.RFC3339)
	if _, err := st.db.ExecContext(ctx, `UPDATE subcent_golf_trials SET observed_ts=? WHERE id=?`, old, id); err != nil {
		t.Fatal(err)
	}
	if n, err := st.ExpireSubcentGolfOrphans(ctx, 15*time.Minute); err != nil || n != 1 {
		t.Fatalf("expire n=%d err=%v", n, err)
	}
	var state, reason string
	if err := st.db.QueryRowContext(ctx, `SELECT fill_state,cancel_reason FROM subcent_golf_trials WHERE id=?`, id).Scan(&state, &reason); err != nil {
		t.Fatal(err)
	}
	if state != "canceled" || reason != "orphan-boot" {
		t.Fatalf("state/reason = %s/%s", state, reason)
	}
}
