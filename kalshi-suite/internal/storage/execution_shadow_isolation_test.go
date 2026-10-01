package storage

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func isolatedShadowAttempt(id string, at time.Time) ExecutionShadowAttempt {
	return ExecutionShadowAttempt{
		AttemptID: id, SignalDecisionID: "decision-" + id,
		ObservedAt: at, TriggerUnixMS: at.UnixMilli(),
		Venue: "kalshi", Ticker: "KX-" + strings.ToUpper(id),
		Title: "isolated shadow", Side: "YES", Action: "BUY",
		SystemID: "spotlag", Route: "taker", SignalSource: "spotlag",
		SignalPrice: .40, QualificationBasis: "isolation-test",
	}
}

func isolatedShadowEvent(id, attemptID string, trigger time.Time, offset time.Duration) ExecutionShadowEvent {
	at := trigger.Add(offset)
	return ExecutionShadowEvent{
		EventID: id, AttemptID: attemptID, At: at,
		ElapsedFromTriggerMS: at.UnixMilli() - trigger.UnixMilli(),
		Stage:                "isolation-test", Outcome: "recorded",
	}
}

func TestExecutionShadowSustainsRateWhileCashDatabaseWriterIsLocked(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	mainConn, err := st.DBForTest().Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer mainConn.Close()
	if _, err = mainConn.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		t.Fatal(err)
	}
	defer mainConn.ExecContext(context.Background(), `ROLLBACK`)

	const totalRecords = 4200
	trigger := time.Now().UTC().Truncate(time.Millisecond)
	attempt := isolatedShadowAttempt("isolation-rate", trigger)
	writes := make([]ExecutionShadowWrite, 0, totalRecords)
	writes = append(writes, ExecutionShadowWrite{Attempt: &attempt})
	for i := 1; i < totalRecords; i++ {
		event := isolatedShadowEvent(fmt.Sprintf("rate-event-%05d", i), attempt.AttemptID,
			trigger, time.Duration(i)*time.Millisecond)
		writes = append(writes, ExecutionShadowWrite{Event: &event})
	}
	started := time.Now()
	for start := 0; start < len(writes); start += 128 {
		end := min(start+128, len(writes))
		orphans, persistErr := st.PersistExecutionShadowBatch(ctx, writes[start:end])
		if persistErr != nil || len(orphans) != 0 {
			t.Fatalf("isolated batch %d:%d orphans=%v err=%v", start, end, orphans, persistErr)
		}
	}
	elapsed := time.Since(started)
	rate := float64(totalRecords) / elapsed.Seconds()
	if rate <= 42 {
		t.Fatalf("isolated shadow rate %.2f rows/s <= required 42 (elapsed=%s)", rate, elapsed)
	}
	var attempts, events int
	if err = st.ExecutionShadowDBForTest().QueryRow(
		`SELECT COUNT(*) FROM execution_shadow_attempts`).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if err = st.ExecutionShadowDBForTest().QueryRow(
		`SELECT COUNT(*) FROM execution_shadow_events`).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if attempts != 1 || events != totalRecords-1 {
		t.Fatalf("isolated counts attempts=%d events=%d", attempts, events)
	}
	var cashAttempts, cashEvents int
	if err = mainConn.QueryRowContext(ctx,
		`SELECT COUNT(*),(SELECT COUNT(*) FROM execution_shadow_events)
FROM execution_shadow_attempts`).Scan(&cashAttempts, &cashEvents); err != nil {
		t.Fatal(err)
	}
	if cashAttempts != 0 || cashEvents != 0 {
		t.Fatalf("shadow records leaked into cash DB: attempts=%d events=%d",
			cashAttempts, cashEvents)
	}
}

func TestExecutionShadowBatchDerivesSentinelElapsedFromExactParent(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	ctx := context.Background()
	trigger := time.Now().UTC().Truncate(time.Millisecond)
	attempt := isolatedShadowAttempt("sentinel-parent", trigger)
	sameBatch := isolatedShadowEvent("sentinel-same-batch", attempt.AttemptID,
		trigger, 17*time.Millisecond)
	sameBatch.ElapsedFromTriggerMS = -1
	orphan := isolatedShadowEvent("sentinel-orphan", "missing-parent",
		trigger, 19*time.Millisecond)
	orphan.ElapsedFromTriggerMS = -1
	orphans, err := st.PersistExecutionShadowBatch(ctx, []ExecutionShadowWrite{
		{Event: &sameBatch},
		{Attempt: &attempt},
		{Event: &orphan},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(orphans) != 1 || orphans[0] != 2 {
		t.Fatalf("sentinel orphan indexes=%v want [2]", orphans)
	}

	durableParent := isolatedShadowAttempt("sentinel-durable", trigger)
	if inserted, insertErr := st.InsertExecutionShadowAttempt(ctx, durableParent); insertErr != nil || !inserted {
		t.Fatalf("insert durable parent inserted=%t err=%v", inserted, insertErr)
	}
	durableEvent := isolatedShadowEvent("sentinel-durable-event", durableParent.AttemptID,
		trigger, 23*time.Millisecond)
	durableEvent.ElapsedFromTriggerMS = -1
	if orphans, persistErr := st.PersistExecutionShadowBatch(ctx,
		[]ExecutionShadowWrite{{Event: &durableEvent}}); persistErr != nil || len(orphans) != 0 {
		t.Fatalf("persist durable sentinel orphans=%v err=%v", orphans, persistErr)
	}

	for eventID, wantElapsed := range map[string]int64{
		sameBatch.EventID:    17,
		durableEvent.EventID: 23,
	} {
		var elapsed int64
		if err = st.ExecutionShadowDBForTest().QueryRow(
			`SELECT elapsed_from_trigger_ms FROM execution_shadow_events WHERE event_id=?`,
			eventID).Scan(&elapsed); err != nil {
			t.Fatal(err)
		}
		if elapsed != wantElapsed {
			t.Fatalf("%s elapsed=%d want %d", eventID, elapsed, wantElapsed)
		}
	}
	arbitraryNegative := isolatedShadowEvent("arbitrary-negative",
		durableParent.AttemptID, trigger, 24*time.Millisecond)
	arbitraryNegative.ElapsedFromTriggerMS = -2
	if _, persistErr := st.PersistExecutionShadowBatch(ctx,
		[]ExecutionShadowWrite{{Event: &arbitraryNegative}}); persistErr == nil {
		t.Fatal("arbitrary negative elapsed was silently repaired as an unknown sentinel")
	}
}

func TestExecutionShadowSettlementRetryKeepsFirstObservationClock(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	trigger := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	attempt := isolatedShadowAttempt("settlement-retry", trigger)
	if inserted, insertErr := st.InsertExecutionShadowAttempt(ctx, attempt); insertErr != nil || !inserted {
		t.Fatalf("insert attempt inserted=%t err=%v", inserted, insertErr)
	}
	value, net := 1.0, .59
	firstAt := trigger.Add(time.Minute)
	first := ExecutionShadowEvent{
		EventID: "settlement-retry-event", AttemptID: attempt.AttemptID,
		At: firstAt, ElapsedFromTriggerMS: firstAt.UnixMilli() - trigger.UnixMilli(),
		Stage: "settlement", Outcome: "settled", SettlementKnown: true,
		SettlementValue: &value, SettledAt: trigger.Add(-time.Hour),
		SettlementSource: "official-history", SettlementHash: "immutable-hash",
		ShadowNet: &net,
	}
	if inserted, appendErr := st.AppendExecutionShadowEvent(ctx, first); appendErr != nil || !inserted {
		t.Fatalf("append first inserted=%t err=%v", inserted, appendErr)
	}
	retry := first
	retry.At = firstAt.Add(time.Minute)
	retry.ElapsedFromTriggerMS = retry.At.UnixMilli() - trigger.UnixMilli()
	if inserted, appendErr := st.AppendExecutionShadowEvent(ctx, retry); appendErr != nil || inserted {
		t.Fatalf("identical settlement retry inserted=%t err=%v", inserted, appendErr)
	}
	changed := retry
	changed.SettlementHash = "changed-hash"
	if _, appendErr := st.AppendExecutionShadowEvent(ctx, changed); appendErr == nil {
		t.Fatal("changed settlement facts reused the deterministic event id")
	}
	var storedAt string
	if err = st.ExecutionShadowDBForTest().QueryRow(
		`SELECT event_ts FROM execution_shadow_events WHERE event_id=?`, first.EventID).
		Scan(&storedAt); err != nil {
		t.Fatal(err)
	}
	if storedAt != firstAt.Format(time.RFC3339Nano) {
		t.Fatalf("settlement retry changed first observation clock=%q want %q",
			storedAt, firstAt.Format(time.RFC3339Nano))
	}
}

func TestExecutionShadowCommittedRowsReopenAndUncleanTailIsVisible(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	trigger := time.Now().UTC().Truncate(time.Millisecond)
	attempt := isolatedShadowAttempt("reopen", trigger)
	event := isolatedShadowEvent("reopen-event", attempt.AttemptID, trigger, time.Millisecond)
	if orphans, persistErr := st.PersistExecutionShadowBatch(context.Background(),
		[]ExecutionShadowWrite{{Attempt: &attempt}, {Event: &event}}); persistErr != nil || len(orphans) != 0 {
		t.Fatalf("persist reopen fixture: orphans=%v err=%v", orphans, persistErr)
	}
	if err = st.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	rows, err := reopened.ListExecutionShadowAttempts(context.Background(), 10)
	if err != nil || len(rows) != 1 || len(rows[0].Events) != 1 {
		t.Fatalf("committed rows did not reopen: rows=%+v err=%v", rows, err)
	}
	status := reopened.ExecutionShadowStatus(context.Background())
	if status.PriorUncleanRuns != 1 || status.LatestClean {
		t.Fatalf("hard-close tail risk hidden: %+v", status)
	}
	next := isolatedShadowAttempt("clean-run", trigger.Add(time.Second))
	if _, err = reopened.PersistExecutionShadowBatch(context.Background(),
		[]ExecutionShadowWrite{{Attempt: &next}}); err != nil {
		t.Fatal(err)
	}
	if err = reopened.FinishExecutionShadowSession(context.Background(), 0); err != nil {
		t.Fatal(err)
	}
	status = reopened.ExecutionShadowStatus(context.Background())
	if !status.LatestClean || status.PriorUncleanRuns != 1 {
		t.Fatalf("clean drain marker incorrect: %+v", status)
	}
}

func TestExecutionShadowLegacyMigrationIsIdempotentAndConflictsFailBoot(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	trigger := time.Now().UTC().Truncate(time.Millisecond)
	attempt := isolatedShadowAttempt("legacy-copy", trigger)
	event := isolatedShadowEvent("legacy-copy-event", attempt.AttemptID, trigger, time.Millisecond)
	if _, err = st.InsertExecutionShadowAttempt(context.Background(), attempt); err != nil {
		t.Fatal(err)
	}
	if _, err = st.AppendExecutionShadowEvent(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	mainConn, err := st.DBForTest().Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	path := strings.ReplaceAll(filepath.Join(dir, ExecutionShadowFileName), "'", "''")
	if _, err = mainConn.ExecContext(context.Background(),
		"ATTACH DATABASE '"+path+"' AS isolated_shadow_copy"); err != nil {
		mainConn.Close()
		t.Fatal(err)
	}
	if _, err = mainConn.ExecContext(context.Background(),
		`INSERT INTO execution_shadow_attempts SELECT * FROM isolated_shadow_copy.execution_shadow_attempts`); err != nil {
		t.Fatal(err)
	}
	if _, err = mainConn.ExecContext(context.Background(),
		`INSERT INTO execution_shadow_events SELECT * FROM isolated_shadow_copy.execution_shadow_events`); err != nil {
		t.Fatal(err)
	}
	_, _ = mainConn.ExecContext(context.Background(), `DETACH DATABASE isolated_shadow_copy`)
	_ = mainConn.Close()
	if err = st.Close(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		st, err = Open(dir)
		if err != nil {
			t.Fatalf("idempotent reopen %d: %v", i, err)
		}
		var attempts, events int
		_ = st.ExecutionShadowDBForTest().QueryRow(
			`SELECT COUNT(*) FROM execution_shadow_attempts`).Scan(&attempts)
		_ = st.ExecutionShadowDBForTest().QueryRow(
			`SELECT COUNT(*) FROM execution_shadow_events`).Scan(&events)
		if attempts != 1 || events != 1 {
			t.Fatalf("migration duplicated rows on reopen %d: attempts=%d events=%d",
				i, attempts, events)
		}
		if err = st.Close(); err != nil {
			t.Fatal(err)
		}
	}

	st, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	conflict := isolatedShadowAttempt("legacy-conflict", trigger.Add(2*time.Second))
	if _, err = st.InsertExecutionShadowAttempt(context.Background(), conflict); err != nil {
		t.Fatal(err)
	}
	mainConn, err = st.DBForTest().Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = mainConn.ExecContext(context.Background(),
		"ATTACH DATABASE '"+path+"' AS isolated_shadow_copy"); err != nil {
		t.Fatal(err)
	}
	conflictInsert := `INSERT INTO execution_shadow_attempts(` + executionShadowAttemptColumns + `)
SELECT attempt_id,signal_decision_id,observed_ts,trigger_unix_ms,venue,ticker,
'different legacy title',side,action,system_id,route,signal_source,input_topology,signal_contract,
input_observed_ts,signal_price,qualification_basis,created_ts
FROM isolated_shadow_copy.execution_shadow_attempts WHERE attempt_id=?`
	if _, err = mainConn.ExecContext(context.Background(), conflictInsert, conflict.AttemptID); err != nil {
		t.Fatal(err)
	}
	_, _ = mainConn.ExecContext(context.Background(), `DETACH DATABASE isolated_shadow_copy`)
	_ = mainConn.Close()
	if err = st.Close(); err != nil {
		t.Fatal(err)
	}
	if reopenedConflict, openErr := Open(dir); openErr == nil {
		_ = reopenedConflict.Close()
		t.Fatal("same attempt id with different legacy evidence did not fail boot")
	} else if !strings.Contains(openErr.Error(), "reused with different identity") {
		t.Fatalf("wrong migration conflict error: %v", openErr)
	}
}
