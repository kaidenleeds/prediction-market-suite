package server

// QUARANTINED SUPPORT/TEST PROTOTYPE — NOT LIVE AUTHORITY.
//
// PolyUS Retail deliberately has no client-order-id. These helpers test how far three independent
// receipts can narrow an intent-only recovery problem: a complete account baseline, the official
// synchronousExecution response when it arrives, and complete post-crash account snapshots.
// They cannot prove a unique order when the submit response is lost and another account mutation
// overlaps. The production staged runtime therefore hard-blocks every PolyUS leg before intent;
// no money path calls this adapter. Keep it as executable specification/research plumbing until
// the venue supplies a durable client identity or atomic basket primitive.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/polymarketus"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

const (
	r148PUSBaselinePrefix    = "r148_pus_staged_baseline_"
	r148PUSResponsePrefix    = "r148_pus_staged_response_"
	r148PUSObservationPrefix = "r148_pus_staged_observation_"
	r148PUSSyntheticPrefix   = "pus-intent:"
	r148PUSNoFillFinality    = 15 * time.Second // maxBlockTime=10s plus a bounded account-ledger margin
)

var errR148PUSRecoveryPending = errors.New("PolyUS intent-only recovery is awaiting a second stable complete account snapshot")

type r148PUSBaseline struct {
	Version       int                      `json:"version"`
	Idempotency   string                   `json:"idempotency"`
	BundleID      string                   `json:"bundle_id"`
	SystemID      string                   `json:"system_id"`
	Ticker        string                   `json:"ticker"`
	Side          string                   `json:"side"`
	Action        string                   `json:"action"`
	Quantity      float64                  `json:"quantity"`
	Limit         float64                  `json:"limit"`
	Captured      time.Time                `json:"captured"`
	Position      polymarketus.PUSPosition `json:"position"`
	PositionFound bool                     `json:"position_found"`
	ActivityIDs   []string                 `json:"activity_ids"`
	OpenOrderIDs  []string                 `json:"open_order_ids"`
	FullPositions string                   `json:"full_positions_fingerprint"`
	FullActivity  string                   `json:"full_activity_fingerprint"`
	FullOrders    string                   `json:"full_orders_fingerprint"`
}

type r148PUSObservation struct {
	Fingerprint string    `json:"fingerprint"`
	Count       int       `json:"count"`
	Observed    time.Time `json:"observed"`
}

func r148PUSKey(prefix, idem string) string {
	return prefix + r148Hash(strings.TrimSpace(idem))[:40]
}

func r148SortPUSAccount(positions []polymarketus.PUSPosition, activities []polymarketus.PUSActivity,
	orders []polymarketus.PUSOrder) {
	sort.Slice(positions, func(i, j int) bool { return positions[i].Slug < positions[j].Slug })
	sort.Slice(activities, func(i, j int) bool {
		if activities[i].ID != activities[j].ID {
			return activities[i].ID < activities[j].ID
		}
		if activities[i].Slug != activities[j].Slug {
			return activities[i].Slug < activities[j].Slug
		}
		return activities[i].Time < activities[j].Time
	})
	sort.Slice(orders, func(i, j int) bool { return orders[i].ID < orders[j].ID })
}

func r148PUSPositionFor(rows []polymarketus.PUSPosition, slug string) (polymarketus.PUSPosition, bool, error) {
	var out polymarketus.PUSPosition
	found := false
	for _, row := range rows {
		if row.Slug != slug {
			continue
		}
		if found {
			return out, false, errors.New("PolyUS complete positions receipt contains duplicate market identity")
		}
		out, found = row, true
	}
	return out, found, nil
}

func (b *r148ServerStagedBroker) pusPositions(ctx context.Context) ([]polymarketus.PUSPosition, error) {
	if b.pusPosFn != nil {
		return b.pusPosFn(ctx)
	}
	if b.s == nil || b.s.polyUSAuth == nil {
		return nil, errors.New("authenticated PolyUS positions client is unavailable")
	}
	return b.s.polyUSAuth.LivePositions(ctx)
}

func (b *r148ServerStagedBroker) pusActivities(ctx context.Context) ([]polymarketus.PUSActivity, error) {
	if b.pusActFn != nil {
		return b.pusActFn(ctx)
	}
	if b.s == nil || b.s.polyUSAuth == nil {
		return nil, errors.New("authenticated PolyUS activities client is unavailable")
	}
	return b.s.polyUSAuth.Activities(ctx)
}

func (b *r148ServerStagedBroker) pusOpenOrders(ctx context.Context) ([]polymarketus.PUSOrder, error) {
	if b.pusOpenFn != nil {
		return b.pusOpenFn(ctx)
	}
	if b.s == nil || b.s.polyUSAuth == nil {
		return nil, errors.New("authenticated PolyUS open-orders client is unavailable")
	}
	return b.s.polyUSAuth.OpenOrders(ctx)
}

func (b *r148ServerStagedBroker) pusOrder(ctx context.Context, id string) (polymarketus.OrderState, error) {
	if b.pusOrderFn != nil {
		return b.pusOrderFn(ctx, id)
	}
	if b.s == nil || b.s.polyUSAuth == nil {
		return polymarketus.OrderState{}, errors.New("authenticated PolyUS order client is unavailable")
	}
	return b.s.polyUSAuth.GetOrderState(ctx, id)
}

func (b *r148ServerStagedBroker) pusMeta(ctx context.Context, slug string) (polymarketus.MarketMeta, error) {
	if b.pusMetaFn != nil {
		return b.pusMetaFn(ctx, slug)
	}
	if b.s == nil || b.s.polyUSAuth == nil {
		return polymarketus.MarketMeta{}, errors.New("authenticated PolyUS market client is unavailable")
	}
	return b.s.polyUSAuth.GetMarketMeta(ctx, slug)
}

func (b *r148ServerStagedBroker) pusAccountSnapshot(ctx context.Context) ([]polymarketus.PUSPosition,
	[]polymarketus.PUSActivity, []polymarketus.PUSOrder, error) {
	var (
		positions  []polymarketus.PUSPosition
		activities []polymarketus.PUSActivity
		orders     []polymarketus.PUSOrder
		posErr     error
		actErr     error
		ordErr     error
		wg         sync.WaitGroup
	)
	wg.Add(3)
	go func() { defer wg.Done(); positions, posErr = b.pusPositions(ctx) }()
	go func() { defer wg.Done(); activities, actErr = b.pusActivities(ctx) }()
	go func() { defer wg.Done(); orders, ordErr = b.pusOpenOrders(ctx) }()
	wg.Wait()
	if posErr != nil || actErr != nil || ordErr != nil {
		return nil, nil, nil, fmt.Errorf("complete PolyUS account baseline unavailable: positions=%v activities=%v orders=%v",
			posErr, actErr, ordErr)
	}
	r148SortPUSAccount(positions, activities, orders)
	return positions, activities, orders, nil
}

func (b *r148ServerStagedBroker) persistPUSImmutable(ctx context.Context, key string, value any) (string, error) {
	if b.s == nil || b.s.store == nil {
		return "", errors.New("durable PolyUS recovery store is unavailable")
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	text := string(raw)
	if prior, ok := b.s.store.KVGet(ctx, key); ok {
		if prior != text {
			return "", errors.New("durable PolyUS recovery receipt conflicts with an existing immutable value")
		}
		return prior, nil
	}
	if err := b.s.store.KVSet(ctx, key, text); err != nil {
		return "", err
	}
	verified, ok := b.s.store.KVGet(ctx, key)
	if !ok || verified != text {
		return "", errors.New("durable PolyUS recovery receipt failed read-after-write verification")
	}
	return verified, nil
}

func (b *r148ServerStagedBroker) capturePUSBaseline(ctx context.Context, bundle storage.ResearchRouteBundle,
	leg storage.ResearchRouteBundleLeg, action string, quantity, limit float64, idem string) (r148PUSBaseline, error) {
	key := r148PUSKey(r148PUSBaselinePrefix, idem)
	if raw, exists := b.s.store.KVGet(ctx, key); exists {
		var prior r148PUSBaseline
		if json.Unmarshal([]byte(raw), &prior) != nil || prior.Idempotency != idem {
			return r148PUSBaseline{}, errors.New("existing PolyUS pre-send baseline is unreadable")
		}
		return prior, errors.New("PolyUS pre-send baseline already exists; refusing a possible duplicate submit")
	}
	positions, activities, orders, err := b.pusAccountSnapshot(ctx)
	if err != nil {
		return r148PUSBaseline{}, err
	}
	position, found, err := r148PUSPositionFor(positions, leg.Ticker)
	if err != nil {
		return r148PUSBaseline{}, err
	}
	activityIDs := make([]string, 0, len(activities))
	for _, row := range activities {
		if strings.TrimSpace(row.ID) != "" {
			activityIDs = append(activityIDs, strings.TrimSpace(row.ID))
		}
	}
	orderIDs := make([]string, 0, len(orders))
	for _, row := range orders {
		if strings.TrimSpace(row.ID) != "" {
			orderIDs = append(orderIDs, strings.TrimSpace(row.ID))
		}
	}
	baseline := r148PUSBaseline{Version: 1, Idempotency: idem, BundleID: bundle.BundleID,
		SystemID: bundle.SystemID, Ticker: leg.Ticker, Side: leg.Side, Action: action,
		Quantity: quantity, Limit: limit, Captured: b.clock(), Position: position, PositionFound: found,
		ActivityIDs: activityIDs, OpenOrderIDs: orderIDs, FullPositions: r148Hash(positions),
		FullActivity: r148Hash(activities), FullOrders: r148Hash(orders)}
	if _, err := b.persistPUSImmutable(ctx, key, baseline); err != nil {
		return r148PUSBaseline{}, err
	}
	return baseline, nil
}

func r148PUSOrderReceipt(state polymarketus.OrderState, leg storage.ResearchRouteBundleLeg,
	action string) r148StagedReceipt {
	r := r148StagedReceipt{OrderID: state.ID, State: r148ReceiptPending, Source: "polyus-get-order"}
	if state.ID == "" || state.MarketSlug != leg.Ticker || state.Outcome != leg.Side || state.Action != action ||
		math.Abs(state.OrderQty-leg.Quantity) > 1e-8 || state.TIF != "TIME_IN_FORCE_FILL_OR_KILL" {
		r.State, r.Authoritative, r.Reason = r148ReceiptAmbiguous, false, "PolyUS order identity differs from staged intent"
		return r
	}
	r.FilledQty, r.AveragePrice, r.FeeTotal = state.Cum, state.AvgPx, state.CommissionTotalUSD
	if state.Cum > 0 && (!state.CommissionTotalKnown || state.AvgPx <= 0 || state.AvgPx >= 1) {
		r.State, r.Reason = r148ReceiptAmbiguous, "filled PolyUS order lacks exact average price or commission receipt"
		return r
	}
	if math.Abs(state.Cum-leg.Quantity) <= 1e-8 {
		r.State, r.Authoritative, r.Reason = r148ReceiptFilled, true, "authoritative PolyUS full FOK fill"
		return r
	}
	if state.Cum > 1e-9 {
		r.State, r.Reason = r148ReceiptAmbiguous, "PolyUS FOK returned a partial fill"
		return r
	}
	if state.Terminal() {
		r.State, r.Authoritative, r.Reason = r148ReceiptUnfilled, true, "authoritative terminal PolyUS FOK without fills"
		return r
	}
	r.Reason = "PolyUS order remains nonterminal"
	return r
}

func r148PUSResponseReceipt(result polymarketus.LiveOrderResult, leg storage.ResearchRouteBundleLeg,
	action string) r148StagedReceipt {
	r := r148StagedReceipt{OrderID: result.OrderID, State: r148ReceiptPending,
		Source: "polyus-synchronous-executions", Reason: "synchronous response has no complete execution yet"}
	qty, gross, fee := 0.0, 0.0, 0.0
	seenExecution, seenTrade := map[string]string{}, map[string]string{}
	for _, row := range result.Executions {
		if row.OrderID != result.OrderID || row.MarketSlug != leg.Ticker || row.Outcome != leg.Side ||
			row.Action != action || math.Abs(row.OrderQty-leg.Quantity) > 1e-8 ||
			row.TIF != "TIME_IN_FORCE_FILL_OR_KILL" {
			r.State, r.Reason = r148ReceiptAmbiguous, "synchronous PolyUS execution has incomplete or conflicting identity/economics"
			return r
		}
		typeName := strings.ToUpper(strings.TrimSpace(row.Type))
		switch typeName {
		case "EXECUTION_TYPE_NEW", "EXECUTION_TYPE_CANCELED", "EXECUTION_TYPE_REPLACE",
			"EXECUTION_TYPE_REJECTED", "EXECUTION_TYPE_EXPIRED", "EXECUTION_TYPE_DONE_FOR_DAY":
			continue
		case "EXECUTION_TYPE_PARTIAL_FILL", "EXECUTION_TYPE_FILL":
		default:
			r.State, r.Reason = r148ReceiptAmbiguous, "synchronous PolyUS response contains an unknown execution type"
			return r
		}
		if strings.TrimSpace(row.ID) == "" || strings.TrimSpace(row.TradeID) == "" || row.LastQty <= 0 ||
			row.LastPrice <= 0 || row.LastPrice >= 1 || !row.CommissionKnown {
			r.State, r.Reason = r148ReceiptAmbiguous, "PolyUS fill execution lacks stable ids, quantity, price, or commission"
			return r
		}
		digest := r148Hash(row)
		if prior, exists := seenExecution[row.ID]; exists {
			if prior != digest {
				r.State, r.Reason = r148ReceiptAmbiguous, "PolyUS repeated one execution id with conflicting economics"
				return r
			}
			continue
		}
		if prior, exists := seenTrade[row.TradeID]; exists {
			if prior != digest {
				r.State, r.Reason = r148ReceiptAmbiguous, "PolyUS repeated one trade id with conflicting economics"
				return r
			}
			continue
		}
		seenExecution[row.ID], seenTrade[row.TradeID] = digest, digest
		qty, gross, fee = qty+row.LastQty, gross+row.LastQty*row.LastPrice, fee+row.CommissionUSD
	}
	r.FilledQty, r.FeeTotal = qty, fee
	if qty > 0 {
		r.AveragePrice = gross / qty
	}
	if math.Abs(qty-leg.Quantity) <= 1e-8 {
		r.State, r.Authoritative, r.Reason = r148ReceiptFilled, true, "exact synchronous PolyUS executions prove full FOK fill"
	} else if qty > 1e-9 {
		r.State, r.Reason = r148ReceiptAmbiguous, "synchronous PolyUS executions prove a partial FOK fill"
	}
	return r
}

func r148PUSSignedDelta(side, action string, quantity float64) float64 {
	positive := (side == "YES" && action == "BUY") || (side == "NO" && action == "SELL")
	if positive {
		return quantity
	}
	return -quantity
}

func (b *r148ServerStagedBroker) readPUSBaseline(ctx context.Context, idem string) (r148PUSBaseline, error) {
	raw, ok := b.s.store.KVGet(ctx, r148PUSKey(r148PUSBaselinePrefix, idem))
	var baseline r148PUSBaseline
	if !ok || json.Unmarshal([]byte(raw), &baseline) != nil || baseline.Version != 1 || baseline.Idempotency != idem {
		return baseline, errors.New("durable PolyUS pre-send account baseline is absent or unreadable")
	}
	return baseline, nil
}

func (b *r148ServerStagedBroker) reconcilePUSIntent(ctx context.Context, leg storage.ResearchRouteBundleLeg,
	action, idem string) (r148StagedReceipt, error) {
	baseline, err := b.readPUSBaseline(ctx, idem)
	if err != nil {
		return r148StagedReceipt{}, err
	}
	if baseline.Ticker != leg.Ticker || baseline.Side != leg.Side || baseline.Action != action ||
		math.Abs(baseline.Quantity-leg.Quantity) > 1e-8 {
		return r148StagedReceipt{State: r148ReceiptAmbiguous, Source: "polyus-account-recovery",
			Reason: "durable PolyUS baseline identity conflicts with staged leg"}, nil
	}
	if raw, ok := b.s.store.KVGet(ctx, r148PUSKey(r148PUSResponsePrefix, idem)); ok {
		var result polymarketus.LiveOrderResult
		if json.Unmarshal([]byte(raw), &result) != nil || result.OrderID == "" {
			return r148StagedReceipt{State: r148ReceiptAmbiguous, Source: "polyus-response-recovery",
				Reason: "durable synchronous PolyUS response is unreadable"}, nil
		}
		r := r148PUSResponseReceipt(result, leg, action)
		if r.State != r148ReceiptPending {
			return r, nil
		}
		state, lookupErr := b.pusOrder(ctx, result.OrderID)
		if lookupErr != nil {
			return r, nil
		}
		return r148PUSOrderReceipt(state, leg, action), nil
	}
	positions, activities, orders, err := b.pusAccountSnapshot(ctx)
	if err != nil {
		return r148StagedReceipt{}, err
	}
	// Forensics only: Activities has no orderId/outcomeSide/action and the three account endpoints
	// are not an atomic snapshot. A matching trade/position delta could be manual or another suite
	// order, while absence is not documented finality. Never infer fill/no-fill and never resubmit.
	forensic := map[string]any{"observed": b.clock(), "positions": r148Hash(positions),
		"activities": r148Hash(activities), "open_orders": r148Hash(orders),
		"baseline_positions": baseline.FullPositions, "baseline_activities": baseline.FullActivity,
		"baseline_open_orders": baseline.FullOrders}
	raw, _ := json.Marshal(forensic)
	_ = b.s.store.KVSet(ctx, r148PUSKey(r148PUSObservationPrefix, idem), string(raw))
	return r148StagedReceipt{OrderID: r148PUSSyntheticPrefix + idem, State: r148ReceiptAmbiguous,
		Source: "polyus-complete-account-forensics",
		Reason: "lost PolyUS response cannot be uniquely joined to an order: Activities omits orderId/side/action; no retry allowed"}, nil

	/* Unreachable historical sketch retained temporarily below; the official schema cannot make it authoritative.
	position, found, err := r148PUSPositionFor(positions, leg.Ticker)
	if err != nil {
		return r148StagedReceipt{}, err
	}
	beforeNet, afterNet := 0.0, 0.0
	if baseline.PositionFound {
		beforeNet = baseline.Position.Net
	}
	if found {
		afterNet = position.Net
	}
	delta := afterNet - beforeNet
	baseActivity := make(map[string]bool, len(baseline.ActivityIDs))
	for _, id := range baseline.ActivityIDs {
		baseActivity[id] = true
	}
	newTrades := make([]polymarketus.PUSActivity, 0, 2)
	for _, row := range activities {
		if row.Type == "TRADE" && row.Slug == leg.Ticker && (row.ID == "" || !baseActivity[row.ID]) {
			newTrades = append(newTrades, row)
		}
	}
	baseOrders := make(map[string]bool, len(baseline.OpenOrderIDs))
	for _, id := range baseline.OpenOrderIDs {
		baseOrders[id] = true
	}
	newOrders := make([]polymarketus.PUSOrder, 0, 1)
	for _, row := range orders {
		if row.Slug == leg.Ticker && !baseOrders[row.ID] {
			newOrders = append(newOrders, row)
		}
	}
	synthetic := r148PUSSyntheticPrefix + idem
	if len(newOrders) > 0 || len(newTrades) > 1 {
		return r148StagedReceipt{OrderID: synthetic, State: r148ReceiptAmbiguous, Source: "polyus-account-recovery",
			Reason: "manual/conflicting PolyUS activity makes the send-before-response outcome non-unique"}, nil
	}
	if len(newTrades) == 1 {
		trade := newTrades[0]
		price := trade.Price
		if leg.Side == "NO" && price > 0 && price < 1 {
			price = 1 - price
		}
		priceOK := price > 0 && price < 1
		if action == "BUY" {
			priceOK = priceOK && price <= baseline.Limit+1e-8
		} else {
			priceOK = priceOK && price+1e-8 >= baseline.Limit
		}
		if trade.ID == "" || !trade.CommissionKnown || math.Abs(trade.Qty-baseline.Quantity) > 1e-8 ||
			math.Abs(delta-r148PUSSignedDelta(leg.Side, action, baseline.Quantity)) > 1e-8 || !priceOK {
			return r148StagedReceipt{OrderID: synthetic, State: r148ReceiptAmbiguous, Source: "polyus-account-recovery",
				Reason: "PolyUS activity/position delta does not uniquely match the staged intent"}, nil
		}
		return r148StagedReceipt{OrderID: "pus-activity:" + trade.ID, State: r148ReceiptFilled,
			FilledQty: trade.Qty, AveragePrice: price, FeeTotal: trade.CommissionUSD, Authoritative: true,
			Source: "polyus-complete-activity+position-delta", Reason: "unique account delta proves the crash-window fill"}, nil
	}
	if math.Abs(delta) > 1e-8 {
		return r148StagedReceipt{OrderID: synthetic, State: r148ReceiptAmbiguous, Source: "polyus-account-recovery",
			Reason: "PolyUS position changed without an attributable activity receipt"}, nil
	}
	if b.clock().Sub(baseline.Captured) < r148PUSNoFillFinality {
		return r148StagedReceipt{}, errR148PUSRecoveryPending
	}
	// A FOK cannot remain open. Two identical, complete target-market observations after the
	// synchronous window prove a finalized no-fill without using absence from a single page.
	targetActivities := make([]polymarketus.PUSActivity, 0)
	targetOrders := make([]polymarketus.PUSOrder, 0)
	for _, row := range activities {
		if row.Slug == leg.Ticker {
			targetActivities = append(targetActivities, row)
		}
	}
	for _, row := range orders {
		if row.Slug == leg.Ticker {
			targetOrders = append(targetOrders, row)
		}
	}
	fingerprint := r148Hash(map[string]any{"position": position, "found": found,
		"activities": targetActivities, "orders": targetOrders})
	observation := r148PUSObservation{Fingerprint: fingerprint, Count: 1, Observed: b.clock()}
	key := r148PUSKey(r148PUSObservationPrefix, idem)
	if raw, ok := b.s.store.KVGet(ctx, key); ok {
		var prior r148PUSObservation
		if json.Unmarshal([]byte(raw), &prior) != nil {
			return r148StagedReceipt{OrderID: synthetic, State: r148ReceiptAmbiguous, Source: "polyus-account-recovery",
				Reason: "durable PolyUS no-fill observation is unreadable"}, nil
		}
		if prior.Fingerprint == fingerprint {
			observation.Count = prior.Count + 1
		}
	}
	raw, _ := json.Marshal(observation)
	if err := b.s.store.KVSet(ctx, key, string(raw)); err != nil {
		return r148StagedReceipt{}, err
	}
	if observation.Count < 2 {
		return r148StagedReceipt{}, errR148PUSRecoveryPending
	}
	return r148StagedReceipt{OrderID: synthetic, State: r148ReceiptUnfilled, Authoritative: true,
		Source: "polyus-two-stable-complete-account-snapshots", Reason: "finalized PolyUS FOK no-fill"}, nil
	*/
}

func (b *r148ServerStagedBroker) ReconcileIntent(ctx context.Context, bundle storage.ResearchRouteBundle,
	leg storage.ResearchRouteBundleLeg, action, idempotencyKey string, intentObserved time.Time) (r148StagedReceipt, error) {
	if leg.Venue == "kalshi" {
		clientOrderID := idemKey("stg-", idempotencyKey)
		if intentObserved.IsZero() {
			return r148StagedReceipt{}, errors.New("Kalshi staged intent timestamp is unavailable")
		}
		// Five minutes of clock/append margin is conservative while still bounding the history
		// read to this intent. The venue has no direct client_order_id filter.
		orders, err := b.clientOrders(ctx, leg.Ticker, clientOrderID, intentObserved.Add(-5*time.Minute))
		if err != nil {
			return r148StagedReceipt{}, fmt.Errorf("complete Kalshi client-order history failed: %w", err)
		}
		if len(orders) != 1 {
			return r148StagedReceipt{}, fmt.Errorf("complete Kalshi client-order history matched %d orders; want exactly one", len(orders))
		}
		if err := r148ValidateKalshiOrder(orders[0], leg, action, clientOrderID); err != nil {
			return r148StagedReceipt{}, err
		}
		fills, err := b.fills(ctx, orders[0].OrderID)
		if err != nil {
			return r148StagedReceipt{}, fmt.Errorf("Kalshi recovered order fills failed: %w", err)
		}
		return r148KalshiReceipt(orders[0], fills, leg, action), nil
	}
	if leg.Venue != "polyus" {
		return r148StagedReceipt{}, errors.New("intent-only recovery is unavailable for this venue")
	}
	baseline, err := b.readPUSBaseline(ctx, idempotencyKey)
	if err != nil {
		return r148StagedReceipt{}, err
	}
	if baseline.BundleID != bundle.BundleID || baseline.SystemID != bundle.SystemID {
		return r148StagedReceipt{State: r148ReceiptAmbiguous, Source: "polyus-account-recovery",
			Reason: "durable PolyUS baseline belongs to another staged bundle"}, nil
	}
	return b.reconcilePUSIntent(ctx, leg, action, idempotencyKey)
}
