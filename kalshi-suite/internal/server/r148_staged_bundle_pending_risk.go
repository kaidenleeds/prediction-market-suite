package server

// Durable pending-risk integration for sequential staged bundles. A single immutable reservation
// owns every leg and canonical cluster. Per-attempt events are appended before/after each venue
// mutation, while ambiguous or frozen packages remain charged until an authoritative account
// snapshot proves the complete expected state vector.

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

var errR148StagedRiskReservationMissing = errors.New(
	"staged terminal state lacks pending-risk reservation")

func r148StagedRiskID(executionID string) string {
	return "risk-staged-" + r148Hash(map[string]string{"execution_id": strings.TrimSpace(executionID)})[:32]
}

func r148StagedRiskAttempt(executionID string, legIndex int, action string) string {
	return r148Hash(map[string]any{"execution": executionID, "leg": legIndex, "action": strings.ToUpper(action)})
}

func r148StagedRiskClientID(executionID string, legIndex int, action string) string {
	return idemKey("stg-", r148StagedRiskAttempt(executionID, legIndex, action))
}

func r148StagedRiskHas(row storage.LivePendingRiskReservation, eventType, attempt string) bool {
	for _, event := range row.Events {
		if event.EventType == eventType && (attempt == "" || event.AttemptKey == attempt) {
			return true
		}
	}
	return false
}

func (b *r148ServerStagedBroker) stagedRiskBaselines(ctx context.Context,
	bundle storage.ResearchRouteBundle) (map[string]r148LiveRiskBaseline, error) {
	if b.riskBaselineFn != nil {
		return b.riskBaselineFn(ctx, bundle)
	}
	if b.s == nil || b.s.kal == nil {
		return nil, errors.New("authenticated Kalshi account baseline is unavailable")
	}
	for _, leg := range bundle.Legs {
		if leg.Venue != "kalshi" {
			return nil, errors.New("staged pending-risk baseline supports Kalshi only")
		}
	}
	positions, err := b.s.kal.GetPositions(ctx)
	if err != nil {
		return nil, fmt.Errorf("complete Kalshi positions baseline: %w", err)
	}
	orders, err := b.s.kal.GetOrders(ctx)
	if err != nil {
		return nil, fmt.Errorf("complete Kalshi orders baseline: %w", err)
	}
	positionByTicker := map[string]float64{}
	for _, position := range positions {
		positionByTicker[position.Ticker] = position.PositionQty()
	}
	restingByTicker := map[string]float64{}
	orderIDsByTicker := map[string][]string{}
	for _, order := range orders {
		if order.RemainFP.Float() <= 0 {
			continue
		}
		risk, known := kalshiRestingRisk(order)
		if !known {
			return nil, errors.New("complete Kalshi resting-order risk is unreadable")
		}
		restingByTicker[order.Ticker] += risk
		if id := strings.TrimSpace(order.OrderID); id != "" {
			orderIDsByTicker[order.Ticker] = append(orderIDsByTicker[order.Ticker], id)
		}
	}
	observed := b.clock()
	out := make(map[string]r148LiveRiskBaseline, len(bundle.Legs))
	for _, leg := range bundle.Legs {
		ids := append([]string(nil), orderIDsByTicker[leg.Ticker]...)
		sort.Strings(ids)
		out[leg.Venue+"\x00"+leg.Ticker] = r148LiveRiskBaseline{
			Observed: observed, PositionQty: positionByTicker[leg.Ticker],
			RestingRiskUSD: restingByTicker[leg.Ticker],
			ReceiptJSON: r148LiveRiskEvidence(map[string]any{
				"source": "kalshi-complete-positions+orders", "observed_at": observed,
				"ticker": leg.Ticker, "position_qty": positionByTicker[leg.Ticker],
				"resting_risk_usd": restingByTicker[leg.Ticker], "resting_order_ids": ids,
			}),
		}
	}
	return out, nil
}

func (b *r148ServerStagedBroker) ReserveStagedPackage(ctx context.Context, executionID string,
	bundle storage.ResearchRouteBundle, quotes map[int]r148StagedQuote) error {
	if b.s == nil || b.s.store == nil || strings.TrimSpace(executionID) == "" {
		return errors.New("staged pending-risk store is unavailable")
	}
	reservationID := r148StagedRiskID(executionID)
	if existing, ok, err := b.s.store.LivePendingRiskByID(ctx, reservationID); err != nil {
		return err
	} else if ok {
		if existing.Intent.Product != "staged" || existing.Intent.Route != "staged_fok" ||
			existing.Intent.SourceIntentID != executionID || existing.Intent.BundleID != bundle.BundleID ||
			existing.Intent.SystemID != bundle.SystemID || r148StagedRiskHas(existing, storage.LivePendingRiskReleased, "") {
			return errors.New("existing staged pending-risk reservation has a different or released identity")
		}
		return nil
	}
	if len(bundle.Legs) < 2 || len(bundle.Legs) > 6 || len(quotes) != len(bundle.Legs) {
		return errors.New("staged pending-risk reservation lacks every bounded leg quote")
	}
	baselines, err := b.stagedRiskBaselines(ctx, bundle)
	if err != nil {
		return err
	}
	// Compute the complete package position vector once. Repeated tickers are still represented by
	// one final expected quantity rather than allowing the first partial leg to look account-visible.
	deltaByTicker := map[string]float64{}
	for _, leg := range bundle.Legs {
		delta := leg.Quantity
		if leg.Side == "NO" {
			delta = -delta
		} else if leg.Side != "YES" {
			return errors.New("staged pending-risk leg has no concrete outcome side")
		}
		deltaByTicker[leg.Venue+"\x00"+leg.Ticker] += delta
	}
	legs := make([]storage.LivePendingRiskLeg, len(bundle.Legs))
	principal, fee := 0.0, 0.0
	baselineEvidence := make([]any, 0, len(bundle.Legs))
	quoteEvidence := make([]any, 0, len(bundle.Legs))
	for i, leg := range bundle.Legs {
		if leg.Index != i {
			return errors.New("staged pending-risk legs are not in canonical index order")
		}
		quote, ok := quotes[i]
		baseline, baselineOK := baselines[leg.Venue+"\x00"+leg.Ticker]
		if !ok || !baselineOK || quote.Price <= 0 || quote.Price >= 1 || quote.Fee < 0 ||
			quote.Available+1e-9 < leg.Quantity || baseline.Observed.IsZero() {
			return fmt.Errorf("staged pending-risk leg %d lacks a current quote/account baseline", i)
		}
		principal += quote.Price * leg.Quantity
		fee += quote.Fee
		legs[i] = storage.LivePendingRiskLeg{
			ReservationID: reservationID, Index: i, Venue: leg.Venue, Ticker: leg.Ticker,
			Side: leg.Side, Action: "BUY", ClientOrderID: r148StagedRiskClientID(executionID, i, "BUY"),
			Quantity: leg.Quantity, LimitPrice: quote.Price, BaselinePositionQty: baseline.PositionQty,
			ExpectedPositionQty:    baseline.PositionQty + deltaByTicker[leg.Venue+"\x00"+leg.Ticker],
			BaselineRestingRiskUSD: baseline.RestingRiskUSD,
		}
		baselineEvidence = append(baselineEvidence, baseline.ReceiptJSON)
		quoteEvidence = append(quoteEvidence, map[string]any{"leg": i, "price": quote.Price,
			"quantity": leg.Quantity, "fee": quote.Fee, "tick": quote.Tick, "source": quote.Source})
	}
	cost := principal + fee
	if cost <= 0 || math.IsNaN(cost) || math.IsInf(cost, 0) {
		return errors.New("staged pending-risk cost is invalid")
	}
	clusterSet := map[string]storage.LivePendingRiskCluster{}
	for _, leg := range bundle.Legs {
		cluster := b.s.liveMirrorClusterKey(leg.Venue, leg.Ticker, "")
		key := leg.Venue + "\x00" + cluster
		clusterSet[key] = storage.LivePendingRiskCluster{Venue: leg.Venue, ClusterKey: cluster,
			MappingVersion: r148LiveRiskMappingVersion, ReservedUSD: cost}
	}
	clusterKeys := make([]string, 0, len(clusterSet))
	for key := range clusterSet {
		clusterKeys = append(clusterKeys, key)
	}
	sort.Strings(clusterKeys)
	clusters := make([]storage.LivePendingRiskCluster, 0, len(clusterKeys))
	for i, key := range clusterKeys {
		cluster := clusterSet[key]
		cluster.ReservationID, cluster.Index = reservationID, i
		clusters = append(clusters, cluster)
	}
	proofJSON := r148LiveRiskEvidence(map[string]any{
		"execution_id": executionID, "bundle_hash": r148BundleIdentityHash(bundle),
		"certificate_hash": bundle.CertificateHash, "event_version": bundle.EventVersion,
		"quotes": quoteEvidence,
	})
	baselineJSON := r148LiveRiskEvidence(map[string]any{
		"source": "complete-staged-account-vector", "legs": baselineEvidence,
	})
	requestHash, err := r148LiveRiskHash(map[string]any{
		"execution_id": executionID, "bundle_id": bundle.BundleID, "system_id": bundle.SystemID,
		"principal": principal, "fee": fee, "legs": legs, "clusters": clusters,
		"proof": proofJSON, "baseline": baselineJSON,
	})
	if err != nil {
		return err
	}
	observed := b.clock()
	for _, baseline := range baselines {
		if baseline.Observed.After(observed) {
			observed = baseline.Observed
		}
	}
	_, err = b.s.store.InsertLivePendingRisk(ctx, storage.LivePendingRiskIntent{
		ReservationID: reservationID, Created: b.clock(), BaselineObserved: observed,
		Product: "staged", DispatchSource: "r148-staged-bundle-coordinator", SystemID: bundle.SystemID,
		Route: "staged_fok", PrincipalUSD: principal, FeeUSD: fee, CostUSD: cost,
		SourceIntentID: executionID, BundleID: bundle.BundleID, RequestHash: requestHash,
		ProofJSON: proofJSON, BaselineReceiptJSON: baselineJSON,
	}, legs, clusters)
	return err
}

func (b *r148ServerStagedBroker) stagedRiskForBundle(ctx context.Context,
	bundle storage.ResearchRouteBundle) (storage.StagedBundleExecutionIntent, storage.LivePendingRiskReservation, bool, error) {
	var risk storage.LivePendingRiskReservation
	if b.s == nil || b.s.store == nil {
		return storage.StagedBundleExecutionIntent{}, risk, false, nil
	}
	in, ok, err := b.s.store.StagedBundleExecutionIntentByBundleID(ctx, bundle.BundleID)
	if err != nil || !ok {
		// Direct broker unit tests intentionally have no coordinator intent. Production writes are
		// reachable only through the coordinator and therefore always have one.
		return in, risk, false, err
	}
	risk, found, err := b.s.store.LivePendingRiskByID(ctx, r148StagedRiskID(in.ExecutionID))
	if err != nil {
		return in, risk, true, err
	}
	if !found || risk.Intent.SourceIntentID != in.ExecutionID || risk.Intent.BundleID != bundle.BundleID ||
		risk.Intent.SystemID != bundle.SystemID || risk.Intent.Product != "staged" {
		return in, risk, true, errors.New("coordinated staged order lacks its whole-package pending-risk reservation")
	}
	return in, risk, true, nil
}

func (b *r148ServerStagedBroker) appendStagedRiskAttempt(ctx context.Context, executionID string,
	leg storage.ResearchRouteBundleLeg, action, eventType, clientOrderID, orderID string,
	receipt r148StagedReceipt, reason string, accountObserved time.Time) error {
	idx := leg.Index
	_, err := b.s.store.AppendLivePendingRiskEvent(ctx, storage.LivePendingRiskEvent{
		ReservationID: r148StagedRiskID(executionID), Observed: b.clock(), EventType: eventType,
		LegIndex: &idx, AttemptKey: r148StagedRiskAttempt(executionID, leg.Index, action), Action: action,
		ClientOrderID: clientOrderID, OrderID: orderID, FilledQty: receipt.FilledQty,
		AveragePrice: receipt.AveragePrice, FeeTotal: receipt.FeeTotal,
		AccountObserved: accountObserved, ReceiptSource: firstNonEmpty(receipt.Source, "staged-risk-ledger"),
		Reason: firstNonEmpty(reason, "staged risk transition"), EvidenceJSON: r148LiveRiskEvidence(receipt),
	})
	return err
}

func (b *r148ServerStagedBroker) startStagedRiskAttempt(ctx context.Context,
	bundle storage.ResearchRouteBundle, leg storage.ResearchRouteBundleLeg, action, clientOrderID string) (string, bool, error) {
	in, _, coordinated, err := b.stagedRiskForBundle(ctx, bundle)
	if err != nil || !coordinated {
		return "", coordinated, err
	}
	return b.startKnownStagedRiskAttempt(ctx, in.ExecutionID, true, leg, action, clientOrderID)
}

// startKnownStagedRiskAttempt appends the required pre-send durability event without repeating the
// execution/reservation database lookup while the final venue fence is held. The immutable
// execution id and coordinated flag were proved during the outside-fence preflight.
func (b *r148ServerStagedBroker) startKnownStagedRiskAttempt(ctx context.Context,
	executionID string, coordinated bool, leg storage.ResearchRouteBundleLeg,
	action, clientOrderID string) (string, bool, error) {
	if !coordinated {
		return "", false, nil
	}
	err := b.appendStagedRiskAttempt(ctx, executionID, leg, action, storage.LivePendingRiskSubmitStarted,
		clientOrderID, "", r148StagedReceipt{Source: "staged-pre-send-risk"},
		"durable submit attempt before venue mutation", time.Time{})
	return executionID, true, err
}

func r163StagedKnownNoSendSource(source string) bool {
	switch source {
	case r163StagedMoneyPolicyNoSendSource,
		r163StagedWireAuthorityNoSendSource,
		r163StagedWireBookNoSendSource:
		return true
	default:
		return false
	}
}

func (b *r148ServerStagedBroker) RecordStagedRiskReceipt(ctx context.Context, executionID string,
	leg storage.ResearchRouteBundleLeg, action string, receipt r148StagedReceipt) error {
	if b.s == nil || b.s.store == nil {
		return nil
	}
	risk, ok, err := b.s.store.LivePendingRiskByID(ctx, r148StagedRiskID(executionID))
	if err != nil || !ok {
		if err != nil {
			return err
		}
		return errors.New("staged risk receipt has no immutable package reservation")
	}
	attempt := r148StagedRiskAttempt(executionID, leg.Index, action)
	clientOrderID := r148StagedRiskClientID(executionID, leg.Index, action)
	if !r148StagedRiskHas(risk, storage.LivePendingRiskSubmitStarted, attempt) {
		return errors.New("staged risk receipt precedes its durable submit-started event")
	}
	if receipt.OrderID != "" && !r148StagedRiskHas(risk, storage.LivePendingRiskAck, attempt) {
		if err := b.appendStagedRiskAttempt(ctx, executionID, leg, action, storage.LivePendingRiskAck,
			clientOrderID, receipt.OrderID, receipt, "venue acknowledgement joined to staged attempt", time.Time{}); err != nil {
			return err
		}
	}
	eventType := ""
	switch receipt.State {
	case r148ReceiptFilled:
		eventType = storage.LivePendingRiskFillSeen
	case r148ReceiptUnfilled:
		eventType = storage.LivePendingRiskTerminalUnfilled
		if receipt.OrderID == "" && r163StagedKnownNoSendSource(receipt.Source) {
			eventType = storage.LivePendingRiskCleanRejected
		}
	case r148ReceiptAmbiguous:
		eventType = storage.LivePendingRiskAmbiguous
	case r148ReceiptPending:
		return nil
	default:
		return errors.New("unknown staged risk receipt state")
	}
	return b.appendStagedRiskAttempt(ctx, executionID, leg, action, eventType, clientOrderID,
		receipt.OrderID, receipt, receipt.Reason, time.Time{})
}

func (b *r148ServerStagedBroker) stagedRiskAttemptsTerminal(row storage.LivePendingRiskReservation) bool {
	started, terminal := map[string]bool{}, map[string]bool{}
	for _, event := range row.Events {
		switch event.EventType {
		case storage.LivePendingRiskSubmitStarted:
			started[event.AttemptKey] = true
		case storage.LivePendingRiskCleanRejected, storage.LivePendingRiskTerminalUnfilled,
			storage.LivePendingRiskAccountVisible:
			terminal[event.AttemptKey] = true
		}
	}
	for attempt := range started {
		if !terminal[attempt] {
			return false
		}
	}
	return true
}

func (b *r148ServerStagedBroker) reconcileStagedRiskAccount(ctx context.Context,
	row storage.LivePendingRiskReservation) error {
	if b.s == nil || b.s.kal == nil || r148StagedRiskHas(row, storage.LivePendingRiskAmbiguous, "") ||
		r148StagedRiskHas(row, storage.LivePendingRiskFrozen, "") {
		return nil
	}
	positions, err := b.s.kal.GetPositions(ctx)
	if err != nil {
		// Snapshot lag/outage is not proof of visibility. Keep the conservative reservation and let
		// the next AUTO recovery sweep retry; never weaken the money rail because a read failed.
		return nil
	}
	positionByTicker := map[string]float64{}
	for _, position := range positions {
		positionByTicker[position.Ticker] = position.PositionQty()
	}
	type attemptState struct {
		fill      *storage.LivePendingRiskEvent
		terminal  bool
		hasUnwind bool
	}
	states := map[string]*attemptState{}
	for i := range row.Events {
		event := &row.Events[i]
		if event.AttemptKey == "" {
			continue
		}
		state := states[event.AttemptKey]
		if state == nil {
			state = &attemptState{}
			states[event.AttemptKey] = state
		}
		if event.EventType == storage.LivePendingRiskFillSeen {
			state.fill = event
		}
		if event.EventType == storage.LivePendingRiskAccountVisible ||
			event.EventType == storage.LivePendingRiskTerminalUnfilled ||
			event.EventType == storage.LivePendingRiskCleanRejected {
			state.terminal = true
		}
	}
	for attempt, state := range states {
		if state.terminal || state.fill == nil || state.fill.LegIndex == nil {
			continue
		}
		idx := *state.fill.LegIndex
		if idx < 0 || idx >= len(row.Legs) {
			return errors.New("staged pending-risk fill points outside immutable legs")
		}
		leg := row.Legs[idx]
		target := leg.ExpectedPositionQty
		if state.fill.Action == "SELL" {
			target = leg.BaselinePositionQty
		}
		current := positionByTicker[leg.Ticker]
		if math.Abs(current-target) > 1e-8 {
			// An authoritative later unwind plus a flat account vector also proves the earlier BUY
			// is represented, even if the account endpoint never exposed the transient partial bundle.
			if state.fill.Action != "BUY" || math.Abs(current-leg.BaselinePositionQty) > 1e-8 {
				continue
			}
			unwindSeen := false
			for _, other := range row.Events {
				unwindSeen = unwindSeen || (other.EventType == storage.LivePendingRiskFillSeen &&
					other.LegIndex != nil && *other.LegIndex == idx && other.Action == "SELL")
			}
			if !unwindSeen {
				continue
			}
		}
		clientOrderID := r148StagedRiskClientID(row.Intent.SourceIntentID, idx, state.fill.Action)
		if err := b.appendStagedRiskAttempt(ctx, row.Intent.SourceIntentID,
			storage.ResearchRouteBundleLeg{Index: idx, Venue: leg.Venue, Ticker: leg.Ticker,
				Side: leg.Side, Quantity: leg.Quantity}, state.fill.Action,
			storage.LivePendingRiskAccountVisible, clientOrderID, state.fill.OrderID,
			r148StagedReceipt{OrderID: state.fill.OrderID, State: r148ReceiptFilled,
				FilledQty: state.fill.FilledQty, AveragePrice: state.fill.AveragePrice,
				FeeTotal: state.fill.FeeTotal, Authoritative: true, Source: "kalshi-complete-positions"},
			"authenticated account position reached the staged expected state", b.clock()); err != nil {
			return fmt.Errorf("attempt %s account visibility: %w", attempt, err)
		}
	}
	return nil
}

func (b *r148ServerStagedBroker) ReleaseStagedRisk(ctx context.Context, executionID, reason string, evidence any) error {
	if b.s == nil || b.s.store == nil {
		return nil
	}
	id := r148StagedRiskID(executionID)
	row, ok, err := b.s.store.LivePendingRiskByID(ctx, id)
	if err != nil || !ok {
		if err != nil {
			return err
		}
		return errR148StagedRiskReservationMissing
	}
	if r148StagedRiskHas(row, storage.LivePendingRiskReleased, "") ||
		r148StagedRiskHas(row, storage.LivePendingRiskAmbiguous, "") ||
		r148StagedRiskHas(row, storage.LivePendingRiskFrozen, "") {
		return nil
	}
	if err := b.reconcileStagedRiskAccount(ctx, row); err != nil {
		return err
	}
	row, ok, err = b.s.store.LivePendingRiskByID(ctx, id)
	if err != nil || !ok {
		if err != nil {
			return err
		}
		return errors.New("staged pending-risk reservation disappeared during release")
	}
	if !b.stagedRiskAttemptsTerminal(row) {
		return nil
	}
	return b.s.r148ReleaseRisk(ctx, id, "staged-package-reconciler", reason, evidence)
}

func (b *r148ServerStagedBroker) FreezeStagedRisk(ctx context.Context, executionID, reason string, evidence any) error {
	if b.s == nil || b.s.store == nil {
		return nil
	}
	id := r148StagedRiskID(executionID)
	row, ok, err := b.s.store.LivePendingRiskByID(ctx, id)
	if err != nil || !ok || r148StagedRiskHas(row, storage.LivePendingRiskReleased, "") {
		// A clean pre-reservation failure has no pending money to freeze.
		return err
	}
	_, err = b.s.store.AppendLivePendingRiskEvent(ctx, storage.LivePendingRiskEvent{
		ReservationID: id, Observed: b.clock(), EventType: storage.LivePendingRiskFrozen,
		ReceiptSource: "staged-package-coordinator", Reason: reason,
		EvidenceJSON: r148LiveRiskEvidence(evidence),
	})
	return err
}

// reconcileActiveStagedRisks runs independently of execution terminality. This matters when a
// fast FOK fill is durable in the order/fill ledger before the authenticated positions endpoint
// catches up; the reservation stays active until a later sweep sees the exact state vector.
func (b *r148ServerStagedBroker) reconcileActiveStagedRisks(ctx context.Context) error {
	if b.s == nil || b.s.store == nil {
		return nil
	}
	// Read only this coordinator's product.  In the common empty-staged-ledger case storage uses
	// an autocommit EXISTS probe, avoiding the _txlock=immediate transaction that previously sat
	// behind unrelated startup writes until this recovery context expired and permanently tripped
	// the global kill switch.
	rows, err := b.s.store.ActiveLivePendingRiskProduct(ctx, "staged")
	if err != nil {
		return err
	}
	for _, row := range rows {
		if row.Intent.Product != "staged" {
			continue
		}
		if err := b.reconcileStagedRiskAccount(ctx, row); err != nil {
			return err
		}
		events, err := b.s.store.StagedBundleExecutionEvents(ctx, row.Intent.SourceIntentID)
		if err != nil || !r148Terminal(events) {
			continue
		}
		if err := b.ReleaseStagedRisk(ctx, row.Intent.SourceIntentID,
			"terminal staged package is represented by the authenticated account", map[string]any{"terminal": true}); err != nil {
			return err
		}
	}
	return nil
}
