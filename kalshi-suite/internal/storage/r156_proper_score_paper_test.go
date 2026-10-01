package storage

import (
	"context"
	"math"
	"testing"
	"time"
)

func properScorePaperAttemptFixture(key, lane, ticker, event string,
	reservation float64) ProperScorePaperAttempt {
	now := time.Date(2026, 7, 17, 12, 0, 0, 0, time.UTC)
	return ProperScorePaperAttempt{
		AttemptKey: key, Lane: lane, Transform: lane, Slot: now.Format(time.RFC3339),
		Platform: "kalshi", Ticker: ticker, EventKey: event, Title: "fixture", Side: "YES",
		ObservedAt: now, ExecuteAfter: now.Add(750 * time.Millisecond),
		ForecastYes: .70, NormalizedWeight: 1, VectorBudgetUSD: 20,
		CoordinateBudgetUSD: 20, LimitPrice: .50, RequestedQty: 10,
		ReservationUSD: reservation, FeeSource: "fixture-fee",
		BookSource: "fixture-book", SourceClockID: "fixture-clock",
		ExecutionShadowAttemptID: "exec-fixture-" + lane + "-" + key, DelayMS: 750,
	}
}

func TestProperScorePaperCashConflictAndLaneIsolation(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	first := properScorePaperAttemptFixture("b-1", "brier", "KX-A", "EVENT-A", 20)
	id, claimed, reason, err := st.ClaimProperScorePaperAttempt(ctx, first)
	if err != nil || !claimed || id <= 0 || reason != "" {
		t.Fatalf("first claim id=%d claimed=%v reason=%q err=%v", id, claimed, reason, err)
	}
	conflict := properScorePaperAttemptFixture("b-2", "brier", "KX-B", "EVENT-A", 20)
	_, claimed, reason, err = st.ClaimProperScorePaperAttempt(ctx, conflict)
	if err != nil || claimed || reason != "same-event-position-or-reservation" {
		t.Fatalf("same-lane conflict claimed=%v reason=%q err=%v", claimed, reason, err)
	}
	otherLane := properScorePaperAttemptFixture("l-1", "log", "KX-B", "EVENT-A", 20)
	if _, claimed, reason, err = st.ClaimProperScorePaperAttempt(ctx, otherLane); err != nil || !claimed || reason != "" {
		t.Fatalf("other lane inherited Brier conflict claimed=%v reason=%q err=%v", claimed, reason, err)
	}
	// Four $20 reservations exactly consume the 20% ($80) open-exposure cap; a fifth cannot
	// turn the eight-coordinate vector into 8x5% exposure.
	for i := 2; i <= 4; i++ {
		a := properScorePaperAttemptFixture("b-cap-"+string(rune('0'+i)), "brier",
			"KX-CAP-"+string(rune('0'+i)), "EVENT-CAP-"+string(rune('0'+i)), 20)
		if _, ok, why, claimErr := st.ClaimProperScorePaperAttempt(ctx, a); claimErr != nil || !ok || why != "" {
			t.Fatalf("cap claim %d ok=%v why=%q err=%v", i, ok, why, claimErr)
		}
	}
	fifth := properScorePaperAttemptFixture("b-cap-5", "brier", "KX-CAP-5", "EVENT-CAP-5", 20)
	if _, claimed, reason, err = st.ClaimProperScorePaperAttempt(ctx, fifth); err != nil || claimed || reason != "isolated-lane-20pct-open-exposure-cap" {
		t.Fatalf("fifth cap claim=%v reason=%q err=%v", claimed, reason, err)
	}
	var liveAuthority, realCapability int
	if err = st.DBForTest().QueryRow(`SELECT COALESCE(SUM(live_authority),0),
COALESCE(SUM(real_order_capability),0) FROM proper_score_paper_attempts`).Scan(
		&liveAuthority, &realCapability); err != nil {
		t.Fatal(err)
	}
	if liveAuthority != 0 || realCapability != 0 {
		t.Fatalf("Paper attempts acquired authority live=%d real=%d", liveAuthority, realCapability)
	}
}

func TestProperScorePaperSettlementIsExactAndCannotRewrite(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	a := properScorePaperAttemptFixture("settle-1", "spherical", "KX-SETTLE", "EVENT-S", 10)
	id, claimed, _, err := st.ClaimProperScorePaperAttempt(ctx, a)
	if err != nil || !claimed {
		t.Fatal(err)
	}
	if err = st.CompleteProperScorePaperAttempt(ctx, ProperScorePaperOutcome{
		AttemptID: id, ProcessedAt: a.ExecuteAfter, State: "filled", FilledQty: 10,
		FillPrice: .49, CostUSD: 4.90, FeeUSD: .10, FeeSource: "exact-fee",
		BookSource: "fresh-book", SourceClockID: "fresh-clock",
	}); err != nil {
		t.Fatal(err)
	}
	closed := "2026-07-17T15:00:00Z"
	ok, err := st.settleProperScorePaperAttempt(ctx, id, 1, closed, "fixture", "hash-a")
	if err != nil || !ok {
		t.Fatalf("settle ok=%v err=%v", ok, err)
	}
	ok, err = st.settleProperScorePaperAttempt(ctx, id, 1, closed, "fixture", "hash-a")
	if err != nil || ok {
		t.Fatalf("idempotent settle ok=%v err=%v", ok, err)
	}
	if _, err = st.settleProperScorePaperAttempt(ctx, id, 0, closed, "fixture", "hash-b"); err == nil {
		t.Fatal("conflicting settlement rewrite was accepted")
	}
	if _, err = st.DBForTest().Exec(`UPDATE proper_score_paper_outcomes SET reason='rewritten'
WHERE attempt_id=?`, id); err == nil {
		t.Fatal("terminal outcome trigger allowed arbitrary rewrite")
	}
	report, err := st.ProperScorePaperPortfolioReport(ctx)
	if err != nil {
		t.Fatal(err)
	}
	row := report["spherical"].(map[string]any)
	if row["settled_positions"].(int) != 1 || row["open_positions"].(int) != 0 ||
		math.Abs(row["realized_net_usd"].(float64)-5) > 1e-9 ||
		math.Abs(row["cash_usd"].(float64)-405) > 1e-9 ||
		math.Abs(row["entry_mark_equity_usd"].(float64)-405) > 1e-9 {
		t.Fatalf("settled Paper money truth=%+v", row)
	}
}

func TestProperScorePaperCrashRecoveryReleasesOnlyExpiredAttempts(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	old := properScorePaperAttemptFixture("old", "brier", "KX-OLD", "EVENT-OLD", 20)
	old.ObservedAt = time.Now().UTC().Add(-time.Minute)
	old.ExecuteAfter = old.ObservedAt.Add(750 * time.Millisecond)
	if _, claimed, _, err := st.ClaimProperScorePaperAttempt(ctx, old); err != nil || !claimed {
		t.Fatal(err)
	}
	fresh := properScorePaperAttemptFixture("fresh", "log", "KX-FRESH", "EVENT-FRESH", 20)
	fresh.ObservedAt = time.Now().UTC()
	fresh.ExecuteAfter = fresh.ObservedAt.Add(750 * time.Millisecond)
	if _, claimed, _, err := st.ClaimProperScorePaperAttempt(ctx, fresh); err != nil || !claimed {
		t.Fatal(err)
	}
	n, err := st.RecoverProperScorePaperAttempts(ctx, time.Now().UTC())
	if err != nil || n != 1 {
		t.Fatalf("recovered=%d err=%v", n, err)
	}
	var oldState string
	if err = st.DBForTest().QueryRow(`SELECT o.state FROM proper_score_paper_attempts a
JOIN proper_score_paper_outcomes o ON o.attempt_id=a.id WHERE a.attempt_key='old'`).Scan(&oldState); err != nil || oldState != "zero_fill" {
		t.Fatalf("old state=%q err=%v", oldState, err)
	}
	var freshOutcomes int
	if err = st.DBForTest().QueryRow(`SELECT COUNT(*) FROM proper_score_paper_attempts a
JOIN proper_score_paper_outcomes o ON o.attempt_id=a.id WHERE a.attempt_key='fresh'`).Scan(&freshOutcomes); err != nil || freshOutcomes != 0 {
		t.Fatalf("fresh outcomes=%d err=%v", freshOutcomes, err)
	}
}

func TestProperScorePaperStorageStartupRecoversStaleReservation(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx, now := context.Background(), time.Now().UTC()
	old := properScorePaperAttemptFixture("startup-old", "log", "KX-STARTUP", "EVENT-STARTUP", 20)
	old.ObservedAt = now.Add(-time.Minute)
	old.ExecuteAfter = old.ObservedAt.Add(750 * time.Millisecond)
	id, claimed, _, err := st.ClaimProperScorePaperAttempt(ctx, old)
	if err != nil || !claimed {
		_ = st.Close()
		t.Fatalf("claim id=%d claimed=%v err=%v", id, claimed, err)
	}
	if err = st.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	var state, reason string
	if err = reopened.DBForTest().QueryRow(`SELECT state,reason
FROM proper_score_paper_outcomes WHERE attempt_id=?`, id).Scan(&state, &reason); err != nil {
		t.Fatal(err)
	}
	if state != "zero_fill" || reason != "process-ended-before-delayed-IOC-check" {
		t.Fatalf("startup recovery state=%q reason=%q", state, reason)
	}
}

func TestProperScorePaperResetStartsCleanEpochAndPreservesHistory(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	oldEpochs := map[string]int64{}
	for _, lane := range properScorePaperLanes {
		attempt := properScorePaperAttemptFixture("reset-reuse-"+lane, lane,
			"KX-RESET-"+lane, "EVENT-RESET-"+lane, 20)
		id, claimed, reason, claimErr := st.ClaimProperScorePaperAttempt(ctx, attempt)
		if claimErr != nil || !claimed || reason != "" {
			t.Fatalf("%s old claim id=%d claimed=%v reason=%q err=%v", lane, id, claimed, reason, claimErr)
		}
		if err = st.CompleteProperScorePaperAttempt(ctx, ProperScorePaperOutcome{
			AttemptID: id, ProcessedAt: attempt.ExecuteAfter, State: "filled", FilledQty: 10,
			FillPrice: .50, CostUSD: 5, FeeUSD: 0, FeeSource: "exact-fee",
			BookSource: "fresh-book", SourceClockID: "fresh-clock",
		}); err != nil {
			t.Fatal(err)
		}
		if ok, settleErr := st.settleProperScorePaperAttempt(ctx, id, 1,
			"2026-07-21T12:00:00Z", "fixture", "hash-"+lane); settleErr != nil || !ok {
			t.Fatalf("%s settle ok=%v err=%v", lane, ok, settleErr)
		}
	}
	before, err := st.ProperScorePaperPortfolioReport(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, lane := range properScorePaperLanes {
		row := before[lane].(map[string]any)
		oldEpochs[lane] = row["epoch_id"].(int64)
		if math.Abs(row["cash_usd"].(float64)-405) > 1e-9 || row["settled_positions"].(int) != 1 {
			t.Fatalf("%s pre-reset report=%+v", lane, row)
		}
	}

	resetAt := time.Date(2026, 7, 21, 13, 0, 0, 123000000, time.UTC)
	epochs, err := st.ResetProperScorePaperPortfolios(ctx, resetAt)
	if err != nil {
		t.Fatal(err)
	}
	after, err := st.ProperScorePaperPortfolioReport(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, lane := range properScorePaperLanes {
		row := after[lane].(map[string]any)
		if epochs[lane] <= oldEpochs[lane] || row["epoch_id"].(int64) != epochs[lane] ||
			row["epoch_reset_at"].(string) != resetAt.Format(time.RFC3339Nano) {
			t.Fatalf("%s reset boundary old=%d new=%d report=%+v", lane, oldEpochs[lane], epochs[lane], row)
		}
		if row["seed_usd"].(float64) != 400 || row["cash_usd"].(float64) != 400 ||
			row["entry_mark_equity_usd"].(float64) != 400 || row["net_profit_usd"].(float64) != 0 ||
			row["attempts"].(int) != 0 || row["fills"].(int) != 0 ||
			row["open_positions"].(int) != 0 || row["settled_positions"].(int) != 0 {
			t.Fatalf("%s post-reset report not clean: %+v", lane, row)
		}
		cash, equity, sizingErr := st.ProperScorePaperLaneSizing(ctx, lane)
		if sizingErr != nil || cash != 400 || equity != 400 {
			t.Fatalf("%s post-reset sizing cash=%v equity=%v err=%v", lane, cash, equity, sizingErr)
		}
		// The same attempt identity, ticker, and event belong to the archived epoch and cannot
		// poison duplicate/conflict checks in the fresh one.
		retry := properScorePaperAttemptFixture("reset-reuse-"+lane, lane,
			"KX-RESET-"+lane, "EVENT-RESET-"+lane, 20)
		retry.ExecutionShadowAttemptID += "-new-epoch"
		newID, claimed, reason, claimErr := st.ClaimProperScorePaperAttempt(ctx, retry)
		if claimErr != nil || !claimed || reason != "" || newID <= 0 {
			t.Fatalf("%s fresh claim id=%d claimed=%v reason=%q err=%v", lane, newID, claimed, reason, claimErr)
		}
		dupID, claimed, reason, claimErr := st.ClaimProperScorePaperAttempt(ctx, retry)
		if claimErr != nil || claimed || reason != "duplicate-attempt" || dupID != newID {
			t.Fatalf("%s current duplicate id=%d/%d claimed=%v reason=%q err=%v",
				lane, dupID, newID, claimed, reason, claimErr)
		}
	}
	var attempts, outcomes, mappings, epochsN int
	if err = st.DBForTest().QueryRow(`SELECT COUNT(*) FROM proper_score_paper_attempts`).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if err = st.DBForTest().QueryRow(`SELECT COUNT(*) FROM proper_score_paper_outcomes`).Scan(&outcomes); err != nil {
		t.Fatal(err)
	}
	if err = st.DBForTest().QueryRow(`SELECT COUNT(*) FROM proper_score_paper_attempt_epoch`).Scan(&mappings); err != nil {
		t.Fatal(err)
	}
	if err = st.DBForTest().QueryRow(`SELECT COUNT(*) FROM proper_score_paper_epochs`).Scan(&epochsN); err != nil {
		t.Fatal(err)
	}
	if attempts != 6 || outcomes != 3 || mappings != 6 || epochsN != 6 {
		t.Fatalf("append history attempts=%d outcomes=%d mappings=%d epochs=%d", attempts, outcomes, mappings, epochsN)
	}
}
