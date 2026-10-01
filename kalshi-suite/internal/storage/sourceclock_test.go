package storage

import (
	"context"
	"testing"
	"time"
)

func sourceClockFixture() SourceClockSpec {
	return SourceClockSpec{SourceID: "test-noaa-nbm", DisplayName: "NOAA NBM forecast publication test fixture",
		AuthorityURL: "https://vlab.noaa.gov/web/mdl/nbm", SchemaVersion: "nbm-v1",
		SchemaHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		ClockKind:  "source_timestamp_sequence", TimestampField: "forecast_generation_time",
		SequenceField: "message_number", Timezone: "UTC", ExpectedCadence: time.Minute,
		GapTolerance: 2 * time.Minute, MaxLag: 5 * time.Minute, PublicationLag: 10 * time.Second,
		RevisionPolicy:          "new model run appends; corrections retain original publication time",
		SettlementCompatibility: "forecast evidence only; NOAA observation source required for settlement",
		CachePolicy:             "cache by model run and station"}
}

func TestSourceClockSpecsVersionInsteadOfRewriting(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	spec := sourceClockFixture()
	version, inserted, err := st.RegisterSourceClockSpec(ctx, spec)
	if err != nil || version != 1 || !inserted {
		t.Fatalf("v=%d inserted=%v err=%v", version, inserted, err)
	}
	version, inserted, err = st.RegisterSourceClockSpec(ctx, spec)
	if err != nil || version != 1 || inserted {
		t.Fatalf("repeat v=%d inserted=%v err=%v", version, inserted, err)
	}
	drift := spec
	drift.Version, drift.SchemaVersion, drift.SchemaHash = 1, "nbm-v2", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	if _, _, err := st.RegisterSourceClockSpec(ctx, drift); err == nil {
		t.Fatal("semantic drift rewrote pinned source-clock version")
	}
	drift.Version = 0
	version, inserted, err = st.RegisterSourceClockSpec(ctx, drift)
	if err != nil || version != 2 || !inserted {
		t.Fatalf("new version=%d inserted=%v err=%v", version, inserted, err)
	}
	if _, err := st.db.ExecContext(ctx, `UPDATE research_source_clock_specs SET schema_version='tampered' WHERE source_id='test-noaa-nbm'`); err == nil {
		t.Fatal("immutable source-clock spec allowed UPDATE")
	}
	if _, err := st.db.ExecContext(ctx, `DELETE FROM research_source_clock_specs WHERE source_id='test-noaa-nbm'`); err == nil {
		t.Fatal("immutable source-clock spec allowed DELETE")
	}
}

func TestSourceClockReceiptsExposeGapRegressionSchemaDriftAndMissingClock(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	spec := sourceClockFixture()
	if _, _, err := st.RegisterSourceClockSpec(ctx, spec); err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC().Truncate(time.Second)
	seq := func(a, b int64) (*int64, *int64) { return &a, &b }
	a, b := seq(1, 10)
	first, err := st.RecordSourceClock(ctx, SourceClockReceipt{SourceID: spec.SourceID,
		SchemaVersion: spec.SchemaVersion, SchemaHash: spec.SchemaHash,
		Watermark: base, Received: base.Add(15 * time.Second), SequenceStart: a, SequenceEnd: b, RowsSeen: 10})
	if err != nil || first.Status != "healthy" || first.Alert {
		t.Fatalf("first=%+v err=%v", first, err)
	}
	a, b = seq(11, 20)
	second, err := st.RecordSourceClock(ctx, SourceClockReceipt{SourceID: spec.SourceID,
		SchemaVersion: spec.SchemaVersion, SchemaHash: spec.SchemaHash,
		Watermark: base.Add(time.Minute), Received: base.Add(time.Minute + 15*time.Second),
		SequenceStart: a, SequenceEnd: b, RowsSeen: 10})
	if err != nil || second.Status != "healthy" {
		t.Fatalf("second=%+v err=%v", second, err)
	}
	a, b = seq(25, 30)
	gap, err := st.RecordSourceClock(ctx, SourceClockReceipt{SourceID: spec.SourceID,
		SchemaVersion: spec.SchemaVersion, SchemaHash: spec.SchemaHash,
		Watermark: base.Add(5 * time.Minute), Received: base.Add(5*time.Minute + 15*time.Second),
		SequenceStart: a, SequenceEnd: b, RowsSeen: 6, WatermarkGapObservable: true})
	if err != nil || gap.Status != "gap" || gap.GapSeconds != 4*60 || gap.SequenceGap != 4 || !gap.Alert {
		t.Fatalf("gap=%+v err=%v", gap, err)
	}
	a, b = seq(19, 22)
	regression, err := st.RecordSourceClock(ctx, SourceClockReceipt{SourceID: spec.SourceID,
		SchemaVersion: spec.SchemaVersion, SchemaHash: spec.SchemaHash,
		Watermark: base.Add(4 * time.Minute), Received: base.Add(5*time.Minute + 20*time.Second),
		SequenceStart: a, SequenceEnd: b, RowsSeen: 4})
	if err != nil || regression.Status != "regression" || regression.RegressionSeconds != 60 {
		t.Fatalf("regression=%+v err=%v", regression, err)
	}
	a, b = seq(31, 40)
	drift, err := st.RecordSourceClock(ctx, SourceClockReceipt{SourceID: spec.SourceID,
		SchemaVersion: "nbm-v2-unregistered", SchemaHash: "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
		Watermark: base.Add(6 * time.Minute), Received: base.Add(6*time.Minute + 15*time.Second),
		SequenceStart: a, SequenceEnd: b, RowsSeen: 10})
	if err != nil || drift.Status != "schema_drift" || !drift.SchemaDrift {
		t.Fatalf("drift=%+v err=%v", drift, err)
	}
	blocked, err := st.RecordSourceClock(ctx, SourceClockReceipt{SourceID: spec.SourceID,
		SchemaVersion: spec.SchemaVersion, SchemaHash: spec.SchemaHash,
		Watermark: base.Add(7 * time.Minute), Received: base.Add(7*time.Minute + 15*time.Second), RowsSeen: 0})
	if err != nil || blocked.Status != "blocked" {
		t.Fatalf("blocked=%+v err=%v", blocked, err)
	}
	var receipts int
	if err := st.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM research_source_clock_receipts WHERE source_id=?`, spec.SourceID).Scan(&receipts); err != nil || receipts != 6 {
		t.Fatalf("receipts=%d err=%v", receipts, err)
	}
	if _, err := st.db.ExecContext(ctx, `UPDATE research_source_clock_receipts SET status='healthy' WHERE source_id=?`, spec.SourceID); err == nil {
		t.Fatal("append-only clock receipt allowed UPDATE")
	}
	report, err := st.SourceClockReport(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rows := report["sources"].([]SourceClockLiveness)
	var got *SourceClockLiveness
	for i := range rows {
		if rows[i].SourceID == spec.SourceID {
			got = &rows[i]
			break
		}
	}
	if got == nil || !got.Alert || got.LifetimeReceipts != 6 || got.Gaps != 1 ||
		got.Regressions != 1 || got.SchemaDrifts != 1 || got.Funded || got.LiveAuthority {
		t.Fatalf("source=%+v all=%+v", got, rows)
	}
}

func TestSourceClockArrivalOnlyDoesNotInventSourceTimestamp(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	spec := SourceClockSpec{SourceID: "polyus-lite-arrival", DisplayName: "PolyUS LITE arrival clock",
		SchemaVersion: "lite-v1", SchemaHash: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
		ClockKind: "arrival_only", ExpectedCadence: 5 * time.Second, GapTolerance: 15 * time.Second,
		MaxLag: 30 * time.Second, RevisionPolicy: "arrival receipts are immutable",
		SettlementCompatibility: "not a settlement source"}
	if _, _, err := st.RegisterSourceClockSpec(context.Background(), spec); err != nil {
		t.Fatal(err)
	}
	result, err := st.RecordSourceClock(context.Background(), SourceClockReceipt{SourceID: spec.SourceID,
		SchemaVersion: spec.SchemaVersion, SchemaHash: spec.SchemaHash, RowsSeen: 1})
	if err != nil || result.Status != "healthy" || result.WatermarkTS != "" {
		t.Fatalf("arrival-only result=%+v err=%v", result, err)
	}
}

func TestSourceClockPeriodicObserverDoesNotInventBusyStreamGapOrStaleness(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	spec := SourceClockSpec{SourceID: "busy-stream", DisplayName: "busy timestamped stream",
		SchemaVersion: "stream-v1", SchemaHash: "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
		ClockKind: "source_timestamp", TimestampField: "timestamp", ExpectedCadence: time.Second,
		GapTolerance: 15 * time.Second, MaxLag: 5 * time.Minute,
		RevisionPolicy: "append every event", SettlementCompatibility: "research price source only"}
	if _, _, err := st.RegisterSourceClockSpec(t.Context(), spec); err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC().Add(-2 * time.Minute).Truncate(time.Second)
	for i, watermark := range []time.Time{base, base.Add(time.Minute)} {
		result, err := st.RecordSourceClock(t.Context(), SourceClockReceipt{SourceID: spec.SourceID,
			SchemaVersion: spec.SchemaVersion, SchemaHash: spec.SchemaHash, Watermark: watermark,
			Received: watermark.Add(time.Second), RowsSeen: (i + 1) * 100})
		if err != nil || result.Status != "healthy" || result.GapSeconds != 0 {
			t.Fatalf("periodic stream sample %d invented a gap: result=%+v err=%v", i, result, err)
		}
	}
	report, err := st.SourceClockReport(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range report["sources"].([]SourceClockLiveness) {
		if row.SourceID == spec.SourceID {
			if row.Alert || row.Status != "healthy" || row.ReceiptLagS < 3 {
				t.Fatalf("one-minute observer was judged against one-second source cadence: %+v", row)
			}
			return
		}
	}
	t.Fatal("busy stream source missing from report")
}

func TestSourceClockExplicitAdjacentTruthNeverSynthesizesPeriodicRanges(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	spec := sourceClockFixture()
	if _, _, err := st.RegisterSourceClockSpec(t.Context(), spec); err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC().Truncate(time.Second)
	record := func(current int64, prior *int64, gap int64, offset time.Duration) SourceClockReceiptResult {
		start, end, gapCopy := current, current, gap
		result, recordErr := st.RecordSourceClock(t.Context(), SourceClockReceipt{SourceID: spec.SourceID,
			SchemaVersion: spec.SchemaVersion, SchemaHash: spec.SchemaHash, Watermark: base.Add(offset),
			Received: base.Add(offset + 15*time.Second), SequenceStart: &start, SequenceEnd: &end,
			SequencePrior: prior, SequenceGap: &gapCopy, RowsSeen: 1})
		if recordErr != nil {
			t.Fatal(recordErr)
		}
		return result
	}
	prior9 := int64(9)
	if got := record(10, &prior9, 0, 0); got.Status != "healthy" {
		t.Fatalf("first=%+v", got)
	}
	// The periodic observer skipped receipt numbers 11..19, but the source decoder actually saw
	// frame 19 immediately before 20. Explicit adjacent truth must not manufacture a nine-frame gap.
	prior19 := int64(19)
	if got := record(20, &prior19, 0, time.Minute); got.Status != "healthy" || got.SequenceGap != 0 {
		t.Fatalf("periodic sample invented gap: %+v", got)
	}
	prior20 := int64(20)
	if got := record(25, &prior20, 4, 2*time.Minute); got.Status != "gap" || got.SequenceGap != 4 {
		t.Fatalf("real adjacent gap lost: %+v", got)
	}
	wrong := int64(3)
	start, end := int64(30), int64(30)
	if _, err := st.RecordSourceClock(t.Context(), SourceClockReceipt{SourceID: spec.SourceID,
		SchemaVersion: spec.SchemaVersion, SchemaHash: spec.SchemaHash, SequenceStart: &start,
		SequenceEnd: &end, SequencePrior: &prior20, SequenceGap: &wrong, RowsSeen: 1}); err == nil {
		t.Fatal("contradictory explicit gap accepted")
	}
}

func TestR138SourceClockBlueprintsCarrySemanticContracts(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ids := R138SourceClockIDs()
	if len(ids) != 8 {
		t.Fatalf("ids=%v", ids)
	}
	var n, timestamped, sequenced, settlementBound int
	err = st.db.QueryRow(`SELECT COUNT(*),
COALESCE(SUM(timestamp_field<>''),0),COALESCE(SUM(sequence_field<>''),0),
COALESCE(SUM(settlement_compatibility<>''),0)
FROM research_source_clock_specs WHERE version=1`).Scan(&n, &timestamped, &sequenced, &settlementBound)
	if err != nil {
		t.Fatal(err)
	}
	if n != 8 || timestamped < 5 || sequenced < 1 || settlementBound != 8 {
		t.Fatalf("n=%d timestamped=%d sequenced=%d settlement=%d", n, timestamped, sequenced, settlementBound)
	}
	report, err := st.SourceClockReport(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report["active"].(int) != 8 || report["alerts"].(int) != 8 {
		t.Fatalf("new source clocks must be visibly never-ran: %#v", report)
	}
}

func TestSourceClockReportUsesOneConnectionWithoutNPlusOne(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	var indexed int
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_rsource_clock_spec_latest'`).Scan(&indexed); err != nil || indexed != 1 {
		t.Fatalf("source/version/latest report index missing: n=%d err=%v", indexed, err)
	}

	// SourceClockReport used to hold its result rows open and then issue one aggregate query per
	// source. With a one-connection pool that pattern blocks on itself until context cancellation.
	// The report now obtains latest receipts and lifetime aggregates in its single SQL statement.
	st.db.SetMaxOpenConns(1)
	st.db.SetMaxIdleConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	started := time.Now()
	report, err := st.SourceClockReport(ctx)
	if err != nil {
		t.Fatalf("single-connection SourceClockReport exposed nested query: %v", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("single-statement report unexpectedly slow: %v", elapsed)
	}
	if report["active"].(int) != len(R138SourceClockIDs()) ||
		len(report["sources"].([]SourceClockLiveness)) != len(R138SourceClockIDs()) {
		t.Fatalf("single-query report lost source rows: %#v", report)
	}
}
