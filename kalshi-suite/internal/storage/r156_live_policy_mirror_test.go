package storage

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"
)

func r156MirrorFilledRow(now time.Time) LivePolicyMirrorRow {
	return LivePolicyMirrorRow{
		CandidateID:     "candidate-filled",
		IntentKey:       "intent-filled",
		ObservedAt:      now,
		ProcessedAt:     now,
		Venue:           "kalshi",
		Ticker:          "KXR156-FILL",
		Title:           "R156 fill",
		Side:            "YES",
		SystemID:        "kalshi-flow",
		Route:           "taker",
		ModelVersion:    "live-policy-mirror-r156-delayed-ws-ioc-v1",
		State:           LivePolicyMirrorFilled,
		SignalPrice:     0.39,
		LimitPrice:      0.40,
		RequestedQty:    2,
		FilledQty:       2,
		FillPrice:       0.40,
		TouchDepth:      4,
		FeeUSD:          0.10,
		FeeSource:       "kalshi:test",
		CostUSD:         0.90,
		ProofMean:       0.08,
		ProofLower:      0.04,
		ProofFeePC:      0.05,
		SizingBankroll:  LivePolicyMirrorSeedUSD,
		SizingTargetUSD: 5,
		EventKey:        "KXR156",
		ClusterKey:      "kalshi:event:KXR156",
		DelayMS:         750,
		WireDelayMS:     100,
		BookSource:      "kalshi_ws_full_orderbook:test",
	}
}

func TestR166LivePolicyMirrorResetPreservesHistoryButIsolatesCurrentCash(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	now := time.Now().UTC().Add(time.Millisecond)
	old := r156MirrorFilledRow(now)
	old.CandidateID, old.IntentKey, old.Ticker = "r166-old", "r166-old-intent", "KXR166-OLD"
	oldMeta, err := st.LivePolicyMirrorMeta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	id, err := st.InsertLivePolicyMirror(ctx, old)
	if err != nil {
		t.Fatal(err)
	}
	epoch := now.Add(time.Second)
	receipt, err := st.ResetLivePolicyMirrorPortfolio(ctx, epoch)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.ExcludedRows != 1 || receipt.ExcludedOpen != 1 || receipt.SeedUSD != 400 {
		t.Fatalf("reset receipt=%+v", receipt)
	}
	if err := st.UpdateLivePolicyMirrorPeak(ctx, 999, epoch.Add(time.Second), oldMeta.EpochAt); err != nil {
		t.Fatal(err)
	}
	metaAfterStalePeak, err := st.LivePolicyMirrorMeta(ctx)
	if err != nil || metaAfterStalePeak.PeakNAVUSD != LivePolicyMirrorSeedUSD {
		t.Fatalf("stale pre-reset peak changed new epoch: meta=%+v err=%v", metaAfterStalePeak, err)
	}
	currentOpen, err := st.OpenLivePolicyMirrorCurrent(ctx)
	if err != nil || len(currentOpen) != 0 {
		t.Fatalf("current open after reset=%d err=%v", len(currentOpen), err)
	}
	allOpen, err := st.OpenLivePolicyMirror(ctx)
	if err != nil || len(allOpen) != 1 {
		t.Fatalf("historical open after reset=%d err=%v", len(allOpen), err)
	}
	if exists, err := st.LivePolicyMirrorIntentExistsSince(ctx, old.IntentKey, now.Add(-time.Hour)); err != nil || exists {
		t.Fatalf("old intent survived current epoch: exists=%v err=%v", exists, err)
	}
	if changed, err := st.SettleLivePolicyMirror(ctx, id, 1, epoch.Add(time.Hour), "venue:test", "old-hash"); err != nil || !changed {
		t.Fatalf("historical settlement changed=%v err=%v", changed, err)
	}
	rollup, err := st.LivePolicyMirrorRollup(ctx)
	if err != nil || len(rollup.StateCounts) != 0 || rollup.SettledNet != 0 {
		t.Fatalf("historical settlement contaminated current rollup=%+v err=%v", rollup, err)
	}

	fresh := r156MirrorFilledRow(epoch.Add(time.Second))
	fresh.CandidateID, fresh.IntentKey, fresh.Ticker = "r166-current", "r166-current-intent", "KXR166-CURRENT"
	if _, err = st.InsertLivePolicyMirror(ctx, fresh); err != nil {
		t.Fatal(err)
	}
	currentOpen, err = st.OpenLivePolicyMirrorCurrent(ctx)
	if err != nil || len(currentOpen) != 1 || currentOpen[0].CandidateID != fresh.CandidateID {
		t.Fatalf("current open=%+v err=%v", currentOpen, err)
	}
	currentRows, err := st.ListLivePolicyMirrorCurrent(ctx, 10)
	if err != nil || len(currentRows) != 1 || currentRows[0].CandidateID != fresh.CandidateID {
		t.Fatalf("current rows=%+v err=%v", currentRows, err)
	}
	meta, err := st.LivePolicyMirrorMeta(ctx)
	if err != nil || math.Abs(meta.DayTurnover-fresh.CostUSD) > 1e-12 || !meta.EpochAt.Equal(epoch) {
		t.Fatalf("current meta=%+v err=%v", meta, err)
	}
	allRows, err := st.ListLivePolicyMirror(ctx, 10)
	if err != nil || len(allRows) != 2 {
		t.Fatalf("append-only history rows=%d err=%v", len(allRows), err)
	}
}

func TestR169LivePolicyMirrorPeakAcceptsEquivalentEpochFormatting(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	// SQLite's bootstrap timestamp keeps fixed millisecond precision. Go's
	// RFC3339Nano formatter removes this trailing zero, but both strings denote
	// the same reset epoch and must pass the stale-epoch guard.
	const storedEpoch = "2026-07-23T12:34:56.120Z"
	if _, err = st.db.ExecContext(ctx, `UPDATE live_policy_mirror_meta
SET epoch_ts=?,peak_nav_usd=? WHERE id=1`, storedEpoch, LivePolicyMirrorSeedUSD); err != nil {
		t.Fatal(err)
	}
	meta, err := st.LivePolicyMirrorMeta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := meta.EpochAt.Format(time.RFC3339Nano); got != "2026-07-23T12:34:56.12Z" {
		t.Fatalf("test setup did not produce alternate formatting: %q", got)
	}
	if err = st.UpdateLivePolicyMirrorPeak(ctx, 412.25, time.Now().UTC(), meta.EpochAt); err != nil {
		t.Fatal(err)
	}
	meta, err = st.LivePolicyMirrorMeta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs(meta.PeakNAVUSD-412.25) > 1e-12 {
		t.Fatalf("equivalent epoch formatting rejected peak update: %+v", meta)
	}
}

func TestR156LivePolicyMirrorIsolatedLedgerAndOneWaySettlement(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	now := time.Now().UTC()

	meta, err := st.LivePolicyMirrorMeta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if LivePolicyMirrorSeedUSD != 400 || meta.SeedUSD != 400 || meta.PeakNAVUSD != 400 {
		t.Fatalf("mirror seed/peak = %v/%v (constant %v), want exact $400 epoch",
			meta.SeedUSD, meta.PeakNAVUSD, LivePolicyMirrorSeedUSD)
	}

	rejected := r156MirrorFilledRow(now)
	rejected.CandidateID, rejected.IntentKey, rejected.Ticker = "candidate-rejected", "intent-rejected", "KXR156-REJECT"
	rejected.State, rejected.Reason = LivePolicyMirrorRejected, "current-book-unavailable"
	rejected.RequestedQty, rejected.FilledQty, rejected.FillPrice = 0, 0, 0
	rejected.FeeUSD, rejected.FeeSource, rejected.CostUSD = 0, "", 0
	rejectedID, err := st.InsertLivePolicyMirror(ctx, rejected)
	if err != nil {
		t.Fatalf("insert rejection: %v", err)
	}
	rejectedRetryID, err := st.InsertLivePolicyMirror(ctx, rejected)
	if err != nil || rejectedRetryID != rejectedID {
		t.Fatalf("identical rejection retry id=%d err=%v, want original id=%d",
			rejectedRetryID, err, rejectedID)
	}
	rejectedConflict := rejected
	rejectedConflict.Reason = "different-terminal-reason"
	if _, err = st.InsertLivePolicyMirror(ctx, rejectedConflict); err == nil {
		t.Fatal("conflicting rejection identity reuse must fail")
	}
	if claimed, claimErr := st.LivePolicyMirrorIntentExistsSince(ctx, rejected.IntentKey,
		now.Add(-time.Hour)); claimErr != nil || claimed {
		t.Fatalf("rejection claimed durable dedup=%v err=%v", claimed, claimErr)
	}
	zero := rejected
	zero.CandidateID, zero.IntentKey, zero.Ticker = "candidate-zero", "intent-zero", "KXR156-ZERO"
	zero.State, zero.Reason = LivePolicyMirrorZeroFill, "newer-book-frame-unavailable"
	if _, err = st.InsertLivePolicyMirror(ctx, zero); err != nil {
		t.Fatalf("insert zero fill: %v", err)
	}
	if claimed, claimErr := st.LivePolicyMirrorIntentExistsSince(ctx, zero.IntentKey,
		now.Add(-time.Hour)); claimErr != nil || claimed {
		t.Fatalf("zero fill claimed durable dedup=%v err=%v", claimed, claimErr)
	}

	filled := r156MirrorFilledRow(now)
	badCost := filled
	badCost.CandidateID = "candidate-bad-cost"
	badCost.CostUSD += .01
	if _, badErr := st.InsertLivePolicyMirror(ctx, badCost); badErr == nil {
		t.Fatal("filled cost must equal quantity*price+fee")
	}
	id, err := st.InsertLivePolicyMirror(ctx, filled)
	if err != nil {
		t.Fatalf("insert fill: %v", err)
	}
	if claimed, claimErr := st.LivePolicyMirrorIntentExistsSince(ctx, filled.IntentKey,
		now.Add(-time.Hour)); claimErr != nil || !claimed {
		t.Fatalf("open fill claimed durable dedup=%v err=%v", claimed, claimErr)
	}
	duplicateID, err := st.InsertLivePolicyMirror(ctx, filled)
	if err != nil || duplicateID != id {
		t.Fatalf("identical terminal retry id=%d err=%v, want original id=%d", duplicateID, err, id)
	}
	conflict := filled
	conflict.LimitPrice = 0.41
	if _, err = st.InsertLivePolicyMirror(ctx, conflict); err == nil {
		t.Fatal("conflicting candidate identity reuse must fail")
	}
	meta, err = st.LivePolicyMirrorMeta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs(meta.DayTurnover-0.90) > 1e-12 {
		t.Fatalf("accepted-cost turnover=%v want 0.90", meta.DayTurnover)
	}

	var paperRows int
	if err = st.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM paper_fills`).Scan(&paperRows); err != nil {
		t.Fatal(err)
	}
	if paperRows != 0 {
		t.Fatalf("mirror contaminated paper_fills with %d rows", paperRows)
	}

	if _, err = st.db.ExecContext(ctx, `UPDATE live_policy_mirror SET ticker='MUTATED' WHERE id=?`, id); err == nil {
		t.Fatal("captured mirror identity must be immutable")
	}
	if _, err = st.db.ExecContext(ctx, `UPDATE live_policy_mirror SET state='zero_fill' WHERE id=?`, id); err == nil {
		t.Fatal("filled mirror state must only move to settled")
	}
	if _, err = st.db.ExecContext(ctx, `UPDATE live_policy_mirror SET state='settled',
settlement_known=1,settlement_value=1,settled_ts=?,settlement_source='tamper',
settlement_hash='tamper',realized_net=999 WHERE id=?`, now.Format(time.RFC3339Nano), id); err == nil {
		t.Fatal("settlement transition must enforce fee-inclusive realized P&L")
	}
	if _, err = st.db.ExecContext(ctx, `DELETE FROM live_policy_mirror WHERE id=?`, id); err == nil {
		t.Fatal("mirror evidence must be append-preserved")
	}

	settledAt := now.Add(time.Hour)
	changed, err := st.SettleLivePolicyMirror(ctx, id, 1, settledAt, "venue_settlement:test", "hash-1")
	if err != nil || !changed {
		t.Fatalf("settle changed=%v err=%v", changed, err)
	}
	changed, err = st.SettleLivePolicyMirror(ctx, id, 1, settledAt, "venue_settlement:test", "hash-1")
	if err != nil || changed {
		t.Fatalf("identical settlement must be idempotent: changed=%v err=%v", changed, err)
	}
	if _, err = st.SettleLivePolicyMirror(ctx, id, 0, settledAt, "venue_settlement:test", "hash-2"); err == nil {
		t.Fatal("authoritative settlement cannot be rewritten")
	}
	duplicateID, err = st.InsertLivePolicyMirror(ctx, filled)
	if err != nil || duplicateID != id {
		t.Fatalf("original fill retry after settlement id=%d err=%v, want id=%d",
			duplicateID, err, id)
	}
	meta, err = st.LivePolicyMirrorMeta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs(meta.DayTurnover-0.90) > 1e-12 {
		t.Fatalf("post-settlement retry changed turnover=%v want 0.90", meta.DayTurnover)
	}
	rows, err := st.ListLivePolicyMirror(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	var got LivePolicyMirrorRow
	for _, row := range rows {
		if row.ID == id {
			got = row
			break
		}
	}
	if got.State != LivePolicyMirrorSettled || !got.SettlementKnown ||
		math.Abs(got.RealizedNet-1.10) > 1e-12 {
		t.Fatalf("settled row state=%q known=%v net=%v, want settled/true/1.10",
			got.State, got.SettlementKnown, got.RealizedNet)
	}
	if claimed, claimErr := st.LivePolicyMirrorIntentExistsSince(ctx, filled.IntentKey,
		now.Add(-time.Hour)); claimErr != nil || !claimed {
		t.Fatalf("settled fill claimed durable dedup=%v err=%v", claimed, claimErr)
	}
	rollup, err := st.LivePolicyMirrorRollup(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rollup.StateCounts[LivePolicyMirrorRejected] != 1 ||
		rollup.StateCounts[LivePolicyMirrorZeroFill] != 1 ||
		rollup.StateCounts[LivePolicyMirrorSettled] != 1 ||
		rollup.ReasonCounts["current-book-unavailable"] != 1 ||
		math.Abs(rollup.SettledNet-1.10) > 1e-12 ||
		math.Abs(rollup.FeesUSD-0.10) > 1e-12 {
		t.Fatalf("full-epoch rollup = %+v", rollup)
	}
}

func TestR156MirrorFullEpochRollupAndOpenSetIgnoreRecentDisplayCap(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	now := time.Now().UTC()
	for i := 0; i < 125; i++ {
		row := r156MirrorFilledRow(now.Add(time.Duration(i) * time.Millisecond))
		row.CandidateID = fmt.Sprintf("rejected-%03d", i)
		row.IntentKey = fmt.Sprintf("rejected-intent-%03d", i)
		row.Ticker = fmt.Sprintf("KXR156-REJECT-%03d", i)
		row.State, row.Reason = LivePolicyMirrorRejected, "test-rejection"
		row.RequestedQty, row.FilledQty, row.FillPrice = 0, 0, 0
		row.FeeUSD, row.FeeSource, row.CostUSD = 0, "", 0
		if _, err = st.InsertLivePolicyMirror(ctx, row); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 105; i++ {
		row := r156MirrorFilledRow(now.Add(time.Duration(i+200) * time.Millisecond))
		row.CandidateID = fmt.Sprintf("open-%03d", i)
		row.IntentKey = fmt.Sprintf("open-intent-%03d", i)
		row.Ticker = fmt.Sprintf("KXR156-OPEN-%03d", i)
		if _, err = st.InsertLivePolicyMirror(ctx, row); err != nil {
			t.Fatal(err)
		}
	}
	recent, err := st.ListLivePolicyMirror(ctx, 100)
	if err != nil || len(recent) != 100 {
		t.Fatalf("recent display rows=%d err=%v, want cap 100", len(recent), err)
	}
	open, err := st.OpenLivePolicyMirror(ctx)
	if err != nil || len(open) != 105 {
		t.Fatalf("uncapped open rows=%d err=%v, want 105", len(open), err)
	}
	rollup, err := st.LivePolicyMirrorRollup(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rollup.StateCounts[LivePolicyMirrorRejected] != 125 ||
		rollup.StateCounts[LivePolicyMirrorFilled] != 105 ||
		rollup.ReasonCounts["test-rejection"] != 125 {
		t.Fatalf("full-epoch rollup truncated: %+v", rollup)
	}
}
