package storage

import (
	"context"
	"strings"
	"testing"
	"time"
)

func riskTestStore(t *testing.T) *Store {
	t.Helper()
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func riskTestSingle(now time.Time) (LivePendingRiskIntent, []LivePendingRiskLeg, []LivePendingRiskCluster) {
	in := LivePendingRiskIntent{
		ReservationID: "risk-single-1", Created: now, BaselineObserved: now.Add(-time.Second),
		Product: "single", DispatchSource: "live-auto", SystemID: "spotlag", Route: "taker",
		PrincipalUSD: 1.20, FeeUSD: .03, CostUSD: 1.23,
		SourceIntentID: "promotion-1", RequestHash: strings.Repeat("a", 64),
		ProofJSON: `{"lower":0.02}`, BaselineReceiptJSON: `{"position":0,"orders":[]}`,
	}
	legs := []LivePendingRiskLeg{{Index: 0, Venue: "kalshi", Ticker: "KXTEST-1",
		Side: "YES", Action: "BUY", ClientOrderID: "client-1", Quantity: 2, LimitPrice: .60,
		BaselinePositionQty: 0, ExpectedPositionQty: 2, BaselineRestingRiskUSD: 0}}
	clusters := []LivePendingRiskCluster{{Index: 0, Venue: "kalshi", ClusterKey: "event:test",
		MappingVersion: "canonical-v1", ReservedUSD: 1.23}}
	return in, legs, clusters
}

func riskEvent(in LivePendingRiskIntent, at time.Time, typ string, leg int) LivePendingRiskEvent {
	return LivePendingRiskEvent{ReservationID: in.ReservationID, Observed: at, EventType: typ,
		LegIndex: &leg, AttemptKey: "entry", Action: "BUY", ClientOrderID: "client-1",
		ReceiptSource: "test", Reason: typ, EvidenceJSON: `{}`}
}

func TestR148LivePendingRiskLifecycleAndImmutableRetry(t *testing.T) {
	st := riskTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	in, legs, clusters := riskTestSingle(now)
	inserted, err := st.InsertLivePendingRisk(ctx, in, legs, clusters)
	if err != nil || !inserted {
		t.Fatalf("insert=%v err=%v", inserted, err)
	}
	inserted, err = st.InsertLivePendingRisk(ctx, in, legs, clusters)
	if err != nil || inserted {
		t.Fatalf("identical retry insert=%v err=%v", inserted, err)
	}
	changed := in
	changed.PrincipalUSD += .01
	changed.CostUSD += .01
	if _, err := st.InsertLivePendingRisk(ctx, changed, legs, clusters); err == nil {
		t.Fatal("changed immutable money fields reused reservation/hash")
	}
	active, err := st.ActiveLivePendingRisk(ctx)
	if err != nil || len(active) != 1 || len(active[0].Events) != 1 ||
		active[0].Events[0].EventType != LivePendingRiskReserved {
		t.Fatalf("active=%+v err=%v", active, err)
	}

	leg := 0
	submit := riskEvent(in, now.Add(time.Second), LivePendingRiskSubmitStarted, leg)
	if ok, err := st.AppendLivePendingRiskEvent(ctx, submit); err != nil || !ok {
		t.Fatalf("submit ok=%v err=%v", ok, err)
	}
	// A retry is idempotent, while a changed singleton receipt is rejected.
	if ok, err := st.AppendLivePendingRiskEvent(ctx, submit); err != nil || ok {
		t.Fatalf("submit retry ok=%v err=%v", ok, err)
	}
	changedSubmit := submit
	changedSubmit.Reason = "different"
	if _, err := st.AppendLivePendingRiskEvent(ctx, changedSubmit); err == nil {
		t.Fatal("changed singleton event retry was accepted")
	}

	ack := riskEvent(in, now.Add(2*time.Second), LivePendingRiskAck, leg)
	ack.OrderID = "order-1"
	if ok, err := st.AppendLivePendingRiskEvent(ctx, ack); err != nil || !ok {
		t.Fatalf("ack ok=%v err=%v", ok, err)
	}
	resting := riskEvent(in, now.Add(2500*time.Millisecond), LivePendingRiskRestingVisible, leg)
	resting.OrderID, resting.AveragePrice = "order-1", .60
	if ok, err := st.AppendLivePendingRiskEvent(ctx, resting); err != nil || !ok {
		t.Fatalf("resting ok=%v err=%v", ok, err)
	}
	restingReplay := resting
	restingReplay.ReceiptSource, restingReplay.Reason, restingReplay.EvidenceJSON = "push", "same order via push", `{"push":true}`
	if ok, err := st.AppendLivePendingRiskEvent(ctx, restingReplay); err != nil || ok {
		t.Fatalf("semantic resting replay ok=%v err=%v", ok, err)
	}
	fill := riskEvent(in, now.Add(3*time.Second), LivePendingRiskFillSeen, leg)
	fill.OrderID, fill.FilledQty, fill.AveragePrice, fill.FeeTotal = "order-1", 2, .60, .03
	if ok, err := st.AppendLivePendingRiskEvent(ctx, fill); err != nil || !ok {
		t.Fatalf("fill ok=%v err=%v", ok, err)
	}
	fillReplay := fill
	fillReplay.ReceiptSource, fillReplay.Reason, fillReplay.EvidenceJSON = "poll", "same cumulative fill via poll", `{"poll":true}`
	if ok, err := st.AppendLivePendingRiskEvent(ctx, fillReplay); err != nil || ok {
		t.Fatalf("semantic cumulative fill replay ok=%v err=%v", ok, err)
	}
	changedFill := fillReplay
	changedFill.FeeTotal = .04
	if _, err := st.AppendLivePendingRiskEvent(ctx, changedFill); err == nil {
		t.Fatal("same cumulative fill quantity accepted changed fee economics")
	}
	visible := riskEvent(in, now.Add(4*time.Second), LivePendingRiskAccountVisible, leg)
	visible.OrderID, visible.FilledQty, visible.AveragePrice, visible.FeeTotal = "order-1", 2, .60, .03
	visible.AccountObserved = now.Add(3500 * time.Millisecond)
	if ok, err := st.AppendLivePendingRiskEvent(ctx, visible); err != nil || !ok {
		t.Fatalf("visible ok=%v err=%v", ok, err)
	}
	release := LivePendingRiskEvent{ReservationID: in.ReservationID, Observed: now.Add(5 * time.Second),
		EventType: LivePendingRiskReleased, ReceiptSource: "account-reconciler",
		Reason: "exact fill and expected signed position are account-visible", EvidenceJSON: `{}`}
	if ok, err := st.AppendLivePendingRiskEvent(ctx, release); err != nil || !ok {
		t.Fatalf("release ok=%v err=%v", ok, err)
	}
	active, err = st.ActiveLivePendingRisk(ctx)
	if err != nil || len(active) != 0 {
		t.Fatalf("released reservation remained active: %+v err=%v", active, err)
	}
	got, found, err := st.LivePendingRiskByID(ctx, in.ReservationID)
	if err != nil || !found || len(got.Events) != 7 {
		t.Fatalf("got=%+v found=%v err=%v", got, found, err)
	}
}

func TestR151KalshiScopedFillRefinesTruncatedCreateAverageFee(t *testing.T) {
	st := riskTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	in, legs, clusters := riskTestSingle(now)
	legs[0].Quantity, legs[0].LimitPrice, legs[0].ExpectedPositionQty = 12, .48, 12
	in.PrincipalUSD, in.FeeUSD, in.CostUSD = 5.76, .21, 5.97
	clusters[0].ReservedUSD = in.CostUSD
	if ok, err := st.InsertLivePendingRisk(ctx, in, legs, clusters); err != nil || !ok {
		t.Fatalf("insert=%v err=%v", ok, err)
	}
	leg := 0
	submit := riskEvent(in, now.Add(time.Second), LivePendingRiskSubmitStarted, leg)
	if ok, err := st.AppendLivePendingRiskEvent(ctx, submit); err != nil || !ok {
		t.Fatalf("submit=%v err=%v", ok, err)
	}
	ack := riskEvent(in, now.Add(2*time.Second), LivePendingRiskAck, leg)
	ack.OrderID = "order-12"
	if ok, err := st.AppendLivePendingRiskEvent(ctx, ack); err != nil || !ok {
		t.Fatalf("ack=%v err=%v", ok, err)
	}

	// This is the exact production shape that tripped: create-order exposes a truncated average
	// fee (0.0174 * 12 = 0.2088), while scoped fill fee_cost receipts sum to 0.2097.
	create := riskEvent(in, now.Add(3*time.Second), LivePendingRiskFillSeen, leg)
	create.OrderID, create.FilledQty, create.AveragePrice, create.FeeTotal = "order-12", 12, .48, .2088
	create.ReceiptSource = "kalshi-create-order-receipt"
	create.EvidenceJSON = `{"response":{"fill_count":"12","average_fee_paid":"0.0174"}}`
	if ok, err := st.AppendLivePendingRiskEvent(ctx, create); err != nil || !ok {
		t.Fatalf("create fill=%v err=%v", ok, err)
	}
	scoped := create
	scoped.Observed, scoped.FeeTotal = now.Add(4*time.Second), .2097
	scoped.ReceiptSource = "kalshi-get-order+order-scoped-fills"
	scoped.Reason = "authoritative scoped fill aggregation"
	scoped.EvidenceJSON = `{"fills":[{"fee_cost":"0.2097"}]}`
	if ok, err := st.AppendLivePendingRiskEvent(ctx, scoped); err != nil || !ok {
		t.Fatalf("scoped precision refinement=%v err=%v", ok, err)
	}
	if ok, err := st.AppendLivePendingRiskEvent(ctx, scoped); err != nil || ok {
		t.Fatalf("exact scoped replay=%v err=%v", ok, err)
	}

	changed := scoped
	changed.FeeTotal = .2110 // outside the 0.0174..<0.0175 per-contract evidence envelope
	if _, err := st.AppendLivePendingRiskEvent(ctx, changed); err == nil {
		t.Fatal("changed authoritative scoped fee was accepted")
	}
	row, found, err := st.LivePendingRiskByID(ctx, in.ReservationID)
	if err != nil || !found {
		t.Fatalf("load found=%v err=%v", found, err)
	}
	var fills []LivePendingRiskEvent
	for _, event := range row.Events {
		if event.EventType == LivePendingRiskFillSeen {
			fills = append(fills, event)
		}
	}
	if len(fills) != 2 || fills[1].FeeTotal != .2097 || fills[1].ReceiptSource != scoped.ReceiptSource {
		t.Fatalf("append-only precision truth=%+v", fills)
	}
}

func TestR151KalshiAckScopedFillAlsoRefinesTruncatedCreateAverageFee(t *testing.T) {
	prior := LivePendingRiskEvent{EventType: LivePendingRiskFillSeen, AttemptKey: "entry",
		Action: "BUY", ClientOrderID: "client-1", OrderID: "order-1", FilledQty: 12,
		AveragePrice: .48, FeeTotal: .2088, ReceiptSource: "kalshi-create-order-receipt",
		EvidenceJSON: `{"response":{"fill_count":"12","average_fee_paid":"0.0174"}}`}
	next := prior
	next.FeeTotal = .2097
	next.ReceiptSource = "kalshi-create-ack+order-scoped-fills"
	next.EvidenceJSON = `{"fills":[{"fee_cost":"0.2097"}]}`
	if !kalshiScopedFeeRefinesCreateAverage(prior, next) {
		t.Fatal("ack+scoped-fill source did not refine the venue's truncated create fee")
	}
	next.FeeTotal = .2110
	if kalshiScopedFeeRefinesCreateAverage(prior, next) {
		t.Fatal("ack+scoped-fill source accepted a fee outside the exact create envelope")
	}
}

func TestR148LivePendingRiskSubmitMustMatchImmutableLegAction(t *testing.T) {
	st := riskTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	in, legs, clusters := riskTestSingle(now)
	if ok, err := st.InsertLivePendingRisk(ctx, in, legs, clusters); err != nil || !ok {
		t.Fatal(ok, err)
	}
	event := riskEvent(in, now.Add(time.Second), LivePendingRiskSubmitStarted, 0)
	event.Action = "SELL"
	if _, err := st.AppendLivePendingRiskEvent(ctx, event); err == nil ||
		!strings.Contains(err.Error(), "immutable leg action/client identity") {
		t.Fatalf("mismatched submit action err=%v", err)
	}
}

func TestR148LivePendingRiskAmbiguityCannotRelease(t *testing.T) {
	st := riskTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	in, legs, clusters := riskTestSingle(now)
	if ok, err := st.InsertLivePendingRisk(ctx, in, legs, clusters); err != nil || !ok {
		t.Fatal(ok, err)
	}
	leg := 0
	if ok, err := st.AppendLivePendingRiskEvent(ctx,
		riskEvent(in, now.Add(time.Second), LivePendingRiskSubmitStarted, leg)); err != nil || !ok {
		t.Fatal(ok, err)
	}
	ambiguous := riskEvent(in, now.Add(2*time.Second), LivePendingRiskAmbiguous, leg)
	ambiguous.Reason = "transport outcome unknown"
	if ok, err := st.AppendLivePendingRiskEvent(ctx, ambiguous); err != nil || !ok {
		t.Fatal(ok, err)
	}
	release := LivePendingRiskEvent{ReservationID: in.ReservationID, Observed: now.Add(3 * time.Second),
		EventType: LivePendingRiskReleased, ReceiptSource: "bad-reconciler", Reason: "timeout", EvidenceJSON: `{}`}
	if _, err := st.AppendLivePendingRiskEvent(ctx, release); err == nil {
		t.Fatal("ambiguous submit was released without terminal/account truth")
	}
	active, err := st.ActiveLivePendingRisk(ctx)
	if err != nil || len(active) != 1 {
		t.Fatalf("ambiguous reservation not retained: len=%d err=%v", len(active), err)
	}
}

func TestR148LivePendingRiskMultiLegMultiCluster(t *testing.T) {
	st := riskTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	in := LivePendingRiskIntent{ReservationID: "risk-staged-1", Created: now,
		BaselineObserved: now.Add(-time.Second), Product: "staged", DispatchSource: "staged-runtime",
		SystemID: "event-basket-lock", Route: "staged_fok", PrincipalUSD: .70, FeeUSD: .02,
		CostUSD: .72, BundleID: "bundle-1", RequestHash: strings.Repeat("b", 64),
		ProofJSON: `{"sealed":true}`, BaselineReceiptJSON: `{"positions":[],"orders":[]}`}
	legs := []LivePendingRiskLeg{
		{Index: 0, Venue: "kalshi", Ticker: "KXA", Side: "YES", Action: "BUY",
			ClientOrderID: "stg-a", Quantity: 1, LimitPrice: .40, ExpectedPositionQty: 1},
		{Index: 1, Venue: "kalshi", Ticker: "KXB", Side: "NO", Action: "BUY",
			ClientOrderID: "stg-b", Quantity: 1, LimitPrice: .30, ExpectedPositionQty: -1},
	}
	clusters := []LivePendingRiskCluster{
		{Index: 0, Venue: "kalshi", ClusterKey: "event:a", MappingVersion: "canonical-v1", ReservedUSD: .72},
		{Index: 1, Venue: "kalshi", ClusterKey: "event:b", MappingVersion: "canonical-v1", ReservedUSD: .72},
	}
	if ok, err := st.InsertLivePendingRisk(ctx, in, legs, clusters); err != nil || !ok {
		t.Fatalf("insert=%v err=%v", ok, err)
	}
	got, found, err := st.LivePendingRiskByID(ctx, in.ReservationID)
	if err != nil || !found || len(got.Legs) != 2 || len(got.Clusters) != 2 ||
		got.Clusters[1].ClusterKey != "event:b" {
		t.Fatalf("got=%+v found=%v err=%v", got, found, err)
	}
}

func TestR148LivePendingRiskInsertRollsBackOnDBFailure(t *testing.T) {
	st := riskTestStore(t)
	ctx := context.Background()
	if _, err := st.db.Exec(`CREATE TRIGGER r148_force_leg_failure BEFORE INSERT ON live_pending_risk_legs
BEGIN SELECT RAISE(ABORT,'forced leg insert failure'); END`); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	in, legs, clusters := riskTestSingle(now)
	if _, err := st.InsertLivePendingRisk(ctx, in, legs, clusters); err == nil {
		t.Fatal("forced DB failure was swallowed")
	}
	var n int
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM live_pending_risk_intents WHERE reservation_id=?`,
		in.ReservationID).Scan(&n); err != nil || n != 0 {
		t.Fatalf("partial intent survived failed atomic insert: n=%d err=%v", n, err)
	}
}

func TestR148LivePendingRiskRejectsConcurrentActiveTickerClaim(t *testing.T) {
	st := riskTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	in, legs, clusters := riskTestSingle(now)
	if ok, err := st.InsertLivePendingRisk(ctx, in, legs, clusters); err != nil || !ok {
		t.Fatal(ok, err)
	}
	second := in
	second.ReservationID = "risk-single-2"
	second.RequestHash = strings.Repeat("e", 64)
	second.SourceIntentID = "promotion-2"
	if _, err := st.InsertLivePendingRisk(ctx, second, legs, clusters); err == nil ||
		!strings.Contains(err.Error(), "already owns kalshi:KXTEST-1") {
		t.Fatalf("second active ticker claim err=%v", err)
	}
	// Risk-reducing orders remain admissible; the same-ticker rail must never prevent an exit.
	reduce := second
	reduce.ReservationID = "risk-reduce-1"
	reduce.RequestHash = strings.Repeat("f", 64)
	reduce.Product, reduce.Route, reduce.PrincipalUSD, reduce.CostUSD = "reduce", "reduce_only_fok", 0, .03
	reduceLegs := append([]LivePendingRiskLeg(nil), legs...)
	reduceLegs[0].Action = "SELL"
	reduceClusters := append([]LivePendingRiskCluster(nil), clusters...)
	reduceClusters[0].ReservedUSD = .03
	if ok, err := st.InsertLivePendingRisk(ctx, reduce, reduceLegs, reduceClusters); err != nil || !ok {
		t.Fatalf("reduce-only same-ticker claim was blocked: ok=%v err=%v", ok, err)
	}
	secondReduce := reduce
	secondReduce.ReservationID = "risk-reduce-2"
	secondReduce.RequestHash = strings.Repeat("1", 64)
	secondReduce.SourceIntentID = "promotion-reduce-2"
	if _, err := st.InsertLivePendingRisk(ctx, secondReduce, reduceLegs, reduceClusters); err == nil ||
		!strings.Contains(err.Error(), "already owns kalshi:KXTEST-1") {
		t.Fatalf("second active reduce claim err=%v", err)
	}
}

func TestR148LivePendingRiskTablesAreImmutableAndCorruptionFailsClosed(t *testing.T) {
	st := riskTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	in, legs, clusters := riskTestSingle(now)
	if ok, err := st.InsertLivePendingRisk(ctx, in, legs, clusters); err != nil || !ok {
		t.Fatal(ok, err)
	}
	for _, stmt := range []string{
		`UPDATE live_pending_risk_intents SET cost_usd=0 WHERE reservation_id='risk-single-1'`,
		`UPDATE live_pending_risk_legs SET quantity=3 WHERE reservation_id='risk-single-1'`,
		`UPDATE live_pending_risk_clusters SET reserved_usd=0 WHERE reservation_id='risk-single-1'`,
		`UPDATE live_pending_risk_events SET reason='changed' WHERE reservation_id='risk-single-1'`,
		`DELETE FROM live_pending_risk_events WHERE reservation_id='risk-single-1'`,
	} {
		if _, err := st.db.Exec(stmt); err == nil {
			t.Fatalf("immutable statement succeeded: %s", stmt)
		}
	}
	// Direct SQL can satisfy SQLite's broad shape while violating the loaded reservation's exact
	// leg cardinality. The reader must report corruption rather than silently drop the reservation.
	_, err := st.db.Exec(`INSERT INTO live_pending_risk_events(
reservation_id,event_seq,observed_ts,event_type,leg_index,attempt_key,action,client_order_id,
order_id,filled_qty,average_price,fee_total,account_observed_ts,receipt_source,reason,evidence_json)
VALUES(?,2,?,'submit_started',5,'bad-leg','BUY','','',0,0,0,'','direct','corrupt','{}')`,
		in.ReservationID, now.Add(time.Second).Format(time.RFC3339Nano))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.ActiveLivePendingRisk(ctx); err == nil {
		t.Fatal("corrupt active reservation was treated as usable exposure truth")
	}
}

func TestR148ActivePendingRiskProductIsolationKeepsStagedRecoveryExact(t *testing.T) {
	st := riskTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	in, legs, clusters := riskTestSingle(now)
	if ok, err := st.InsertLivePendingRisk(ctx, in, legs, clusters); err != nil || !ok {
		t.Fatalf("insert single ok=%v err=%v", ok, err)
	}
	// Hold the same immediate transaction lock used by startup cleanup writers.  The staged-only
	// empty-ledger probe is an autocommit WAL read and must not wait behind this unrelated writer.
	writer, err := st.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	probeCtx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	present, presenceErr := st.HasActiveLivePendingRiskProduct(probeCtx, "staged")
	staged, probeErr := st.ActiveLivePendingRiskProduct(probeCtx, "staged")
	cancel()
	_ = writer.Rollback()
	if presenceErr != nil || present || probeErr != nil || len(staged) != 0 {
		t.Fatalf("empty staged probe waited behind unrelated writer: present=%v presence_err=%v rows=%+v err=%v",
			present, presenceErr, staged, probeErr)
	}

	// Corrupt an active SINGLE reservation in a way the immutable loader must reject.  Staged
	// recovery must not scan or trip over a different product's ledger; that product's own recovery
	// remains responsible for failing closed on the malformed row.
	_, err = st.db.Exec(`INSERT INTO live_pending_risk_events(
reservation_id,event_seq,observed_ts,event_type,leg_index,attempt_key,action,client_order_id,
order_id,filled_qty,average_price,fee_total,account_observed_ts,receipt_source,reason,evidence_json)
VALUES(?,2,?,'submit_started',5,'bad-leg','BUY','','',0,0,0,'','direct','corrupt','{}')`,
		in.ReservationID, now.Add(time.Second).Format(time.RFC3339Nano))
	if err != nil {
		t.Fatal(err)
	}
	staged, err = st.ActiveLivePendingRiskProduct(ctx, "staged")
	if err != nil || len(staged) != 0 {
		t.Fatalf("unrelated corrupt single contaminated staged recovery: rows=%+v err=%v", staged, err)
	}
	if _, err := st.ActiveLivePendingRiskProduct(ctx, "single"); err == nil {
		t.Fatal("corrupt single reservation did not fail closed in its owning product reader")
	}
	if _, err := st.ActiveLivePendingRiskProduct(ctx, "unknown"); err == nil {
		t.Fatal("unknown product filter was accepted")
	}
}

func TestR148ActiveStagedPendingRiskStillLoadsAndCorruptionFailsClosed(t *testing.T) {
	st := riskTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	in, legs, clusters := riskTestSingle(now)
	in.ReservationID = "risk-staged-1"
	in.Product = "staged"
	in.Route = "staged_fok"
	in.SourceIntentID = "staged-execution-1"
	in.BundleID = "staged-bundle-1"
	in.RequestHash = strings.Repeat("b", 64)
	legs = append(legs, LivePendingRiskLeg{Index: 1, Venue: "kalshi", Ticker: "KXTEST-2",
		Side: "NO", Action: "BUY", ClientOrderID: "client-2", Quantity: 2, LimitPrice: .40,
		BaselinePositionQty: 0, ExpectedPositionQty: -2})
	clusters[0].ReservedUSD = in.CostUSD
	if ok, err := st.InsertLivePendingRisk(ctx, in, legs, clusters); err != nil || !ok {
		t.Fatalf("insert staged ok=%v err=%v", ok, err)
	}
	rows, err := st.ActiveLivePendingRiskProduct(ctx, "staged")
	if err != nil || len(rows) != 1 || rows[0].Intent.ReservationID != in.ReservationID {
		t.Fatalf("active staged rows=%+v err=%v", rows, err)
	}
	if present, err := st.HasActiveLivePendingRiskProduct(ctx, "staged"); err != nil || !present {
		t.Fatalf("active staged presence=%v err=%v", present, err)
	}

	// A matching staged row never receives the empty-ledger shortcut.  If its immutable journal is
	// malformed, the staged reader must surface the error so runtime recovery freezes money.
	_, err = st.db.Exec(`INSERT INTO live_pending_risk_events(
reservation_id,event_seq,observed_ts,event_type,leg_index,attempt_key,action,client_order_id,
order_id,filled_qty,average_price,fee_total,account_observed_ts,receipt_source,reason,evidence_json)
VALUES(?,2,?,'submit_started',5,'bad-leg','BUY','','',0,0,0,'','direct','corrupt','{}')`,
		in.ReservationID, now.Add(time.Second).Format(time.RFC3339Nano))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.ActiveLivePendingRiskProduct(ctx, "staged"); err == nil {
		t.Fatal("corrupt active staged reservation bypassed fail-closed loading")
	}
}
