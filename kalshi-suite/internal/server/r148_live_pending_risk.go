package server

// R148 durable pending-risk overlay.
//
// A venue acknowledgement and its authenticated account snapshot are not atomic.  Without this
// ledger, a second order can briefly size against money that the venue accepted but has not yet
// exposed through positions/resting orders.  Every production mutation therefore owns an
// append-only reservation written before the network call.  Account readers add the unrepresented
// reservation to both venue and event-cluster exposure until exact order/fill/position truth lands.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/polymarketus"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

const r148LiveRiskMappingVersion = "leg-event-key-v1"

type r148LiveRiskBaseline struct {
	Observed       time.Time
	PositionQty    float64
	RestingRiskUSD float64
	ReceiptJSON    string
}

const r151RiskBaselineClock = "venue-account-watermark-v1"

// errR156KalshiAdmissionSnapshotNotCurrent is deliberately typed so the single-order handler can
// distinguish one proven pre-insert account-epoch race from storage failures and every error that
// can occur after a durable reservation exists. It is the only r148 reservation error eligible
// for the bounded pre-reservation account refresh in handleLivePlace.
var errR156KalshiAdmissionSnapshotNotCurrent = errors.New("carried Kalshi admission snapshot is no longer current")

func r151AccountWatermark(raw string) (time.Time, bool) {
	raw = strings.TrimSpace(raw)
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
		if at, err := time.Parse(layout, raw); err == nil &&
			!at.After(time.Now().UTC().Add(5*time.Minute)) {
			return at.UTC(), true
		}
	}
	return time.Time{}, false
}

func r148LiveRiskExpectedPosition(prior, qty float64, side, action string) (float64, error) {
	if qty <= 0 || math.IsNaN(qty) || math.IsInf(qty, 0) {
		return 0, errors.New("invalid reservation quantity")
	}
	sign := 1.0
	if strings.EqualFold(side, "NO") {
		sign = -1
	} else if !strings.EqualFold(side, "YES") && !strings.EqualFold(side, "BUNDLE") {
		return 0, errors.New("invalid reservation side")
	}
	if strings.EqualFold(action, "SELL") {
		sign *= -1
	} else if !strings.EqualFold(action, "BUY") {
		return 0, errors.New("invalid reservation action")
	}
	return prior + sign*qty, nil
}

func r148LiveRiskHash(v any) (string, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:]), nil
}

func r148LiveRiskEvidence(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return `{}`
	}
	return string(b)
}

func (s *Server) r148KalshiRiskBaseline(ctx context.Context, ticker string) (r148LiveRiskBaseline, error) {
	var out r148LiveRiskBaseline
	if s == nil || strings.TrimSpace(ticker) == "" {
		return out, errors.New("Kalshi risk baseline unavailable")
	}
	if carried, ok := r154KalshiAdmissionSnapshotFromContext(ctx); ok {
		cache := s.r154KalshiAdmissionCache()
		if cache == nil || !cache.CurrentForExecution(carried, time.Now()) {
			return out, errR156KalshiAdmissionSnapshotNotCurrent
		}
		return r148KalshiRiskBaselineFromAccount(ticker, carried.ObservedAt(),
			"kalshi-carried-admission-snapshot", carried.Positions(), carried.Orders())
	}
	if s.kal == nil {
		return out, errors.New("Kalshi risk baseline unavailable")
	}
	// Freshness must remain in the venue's clock domain.  The old code stamped time.Now after
	// both REST calls; a fast fill could consequently receive a venue last_updated_ts before that
	// local end-time (ordinary clock skew), leaving an exact fill permanently reserved.  Preserve
	// the authenticated pre-submit position watermark instead.  requestStarted is only a non-zero
	// schema fallback when an empty account provides no venue timestamp; target absence is recorded
	// separately and is itself stronger evidence than a cross-clock comparison.
	requestStarted := time.Now().UTC()
	positions, err := s.kal.GetPositions(ctx)
	if err != nil {
		return out, fmt.Errorf("Kalshi baseline positions: %w", err)
	}
	orders, err := s.kal.GetOrders(ctx)
	if err != nil {
		return out, fmt.Errorf("Kalshi baseline orders: %w", err)
	}
	return r148KalshiRiskBaselineFromAccount(ticker, requestStarted,
		"kalshi-complete-positions+orders", positions, orders)
}

func r148KalshiRiskBaselineFromAccount(ticker string, requestStarted time.Time, source string,
	positions []kalshi.MarketPosition, orders []kalshi.Order) (r148LiveRiskBaseline, error) {
	var out r148LiveRiskBaseline
	if strings.TrimSpace(ticker) == "" || requestStarted.IsZero() {
		return out, errors.New("Kalshi risk baseline account snapshot unavailable")
	}
	positionFound := false
	venueWatermark := time.Time{}
	orderIDs := make([]string, 0)
	for _, p := range positions {
		if at, ok := r151AccountWatermark(p.LastUpdatedTS); ok && at.After(venueWatermark) {
			venueWatermark = at
		}
		if strings.EqualFold(strings.TrimSpace(p.Ticker), strings.TrimSpace(ticker)) {
			out.PositionQty = p.PositionQty()
			positionFound = true
			if at, ok := r151AccountWatermark(p.LastUpdatedTS); ok {
				out.Observed = at
			}
		}
	}
	for _, o := range orders {
		if strings.EqualFold(strings.TrimSpace(o.Ticker), strings.TrimSpace(ticker)) && o.RemainFP.Float() > 0 {
			risk, known := kalshiRestingRisk(o)
			if !known {
				return out, errors.New("Kalshi baseline resting order omitted current direction or price truth")
			}
			out.RestingRiskUSD += risk
			if id := strings.TrimSpace(o.OrderID); id != "" {
				orderIDs = append(orderIDs, id)
			}
		}
	}
	sort.Strings(orderIDs)
	if out.Observed.IsZero() {
		out.Observed = venueWatermark
	}
	if out.Observed.IsZero() {
		out.Observed = requestStarted
	}
	out.ReceiptJSON = r148LiveRiskEvidence(map[string]any{
		"source": source, "observed_at": out.Observed,
		"baseline_clock": r151RiskBaselineClock, "request_started_at": requestStarted,
		"baseline_target_position_found": positionFound,
		"ticker":                         ticker, "position_qty": out.PositionQty,
		"resting_risk_usd": out.RestingRiskUSD, "resting_order_ids": orderIDs,
	})
	return out, nil
}

func (s *Server) r148PolyUSRiskBaseline(ctx context.Context, slug string) (r148LiveRiskBaseline, error) {
	var out r148LiveRiskBaseline
	if s == nil || s.polyUSAuth == nil || strings.TrimSpace(slug) == "" {
		return out, errors.New("PolyUS risk baseline unavailable")
	}
	requestStarted := time.Now().UTC()
	positions, err := s.polyUSAuth.LivePositions(ctx)
	if err != nil {
		return out, fmt.Errorf("PolyUS baseline positions: %w", err)
	}
	orders, err := s.polyUSAuth.OpenOrders(ctx)
	if err != nil {
		return out, fmt.Errorf("PolyUS baseline orders: %w", err)
	}
	orderIDs := make([]string, 0)
	positionFound := false
	venueWatermark := time.Time{}
	for _, p := range positions {
		if at, ok := r151AccountWatermark(p.Updated); ok && at.After(venueWatermark) {
			venueWatermark = at
		}
		if strings.EqualFold(strings.TrimSpace(p.Slug), strings.TrimSpace(slug)) && !p.Expired {
			out.PositionQty = p.Net
			positionFound = true
			if at, ok := r151AccountWatermark(p.Updated); ok {
				out.Observed = at
			}
		}
	}
	for _, o := range orders {
		if !strings.EqualFold(strings.TrimSpace(o.Slug), strings.TrimSpace(slug)) ||
			!strings.EqualFold(o.Action, "BUY") || o.Leaves <= 0 {
			continue
		}
		out.RestingRiskUSD += o.Leaves * o.Price
		if id := strings.TrimSpace(o.ID); id != "" {
			orderIDs = append(orderIDs, id)
		}
	}
	sort.Strings(orderIDs)
	if out.Observed.IsZero() {
		out.Observed = venueWatermark
	}
	if out.Observed.IsZero() {
		out.Observed = requestStarted
	}
	out.ReceiptJSON = r148LiveRiskEvidence(map[string]any{
		"source": "polyus-complete-positions+open-orders", "observed_at": out.Observed,
		"baseline_clock": r151RiskBaselineClock, "request_started_at": requestStarted,
		"baseline_target_position_found": positionFound,
		"ticker":                         slug, "position_qty": out.PositionQty,
		"resting_risk_usd": out.RestingRiskUSD, "resting_order_ids": orderIDs,
	})
	return out, nil
}

type r148SingleRiskRequest struct {
	Product, Venue, Ticker, Side, Action, Route, DispatchSource, SystemID string
	Quantity, LimitPrice, PrincipalUSD, FeeUSD                            float64
	ClientOrderID, SourceIntentID, AttemptNonce                           string
	ExecutionShadowAttemptID                                              string
	RequireExecutionShadowAttemptID                                       bool
	Proof                                                                 any
}

func r148LiveRiskEvidenceWithAttempt(v any, attemptID string) (string, error) {
	attemptID = strings.TrimSpace(attemptID)
	encoded := r148LiveRiskEvidence(v)
	if attemptID == "" {
		return encoded, nil
	}
	var decoded any
	if err := json.Unmarshal([]byte(encoded), &decoded); err != nil {
		return "", errors.New("pending-risk proof JSON is invalid")
	}
	var evidence map[string]any
	switch value := decoded.(type) {
	case nil:
		evidence = map[string]any{}
	case map[string]any:
		evidence = value
	default:
		return "", errors.New("opportunity-linked pending-risk proof must be a JSON object")
	}
	if prior, exists := evidence["execution_shadow_attempt_id"]; exists {
		priorID, stringID := prior.(string)
		if !stringID || strings.TrimSpace(priorID) != attemptID {
			return "", errors.New("pending-risk proof names a different execution-shadow attempt")
		}
	}
	evidence["execution_shadow_attempt_id"] = attemptID
	b, err := json.Marshal(evidence)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func r148SingleRiskRequestIdentity(in r148SingleRiskRequest) (attemptID, proofJSON, hash string, err error) {
	venue := strings.ToLower(strings.TrimSpace(in.Venue))
	product := strings.ToLower(strings.TrimSpace(in.Product))
	attemptID = strings.TrimSpace(in.ExecutionShadowAttemptID)
	if in.RequireExecutionShadowAttemptID && attemptID == "" {
		return "", "", "", errors.New("automatic/system pending-risk request lacks canonical opportunity id")
	}
	proofJSON, err = r148LiveRiskEvidenceWithAttempt(in.Proof, attemptID)
	if err != nil {
		return "", "", "", err
	}
	hash, err = r148LiveRiskHash(map[string]any{
		"product": product, "venue": venue, "ticker": in.Ticker,
		"side": strings.ToUpper(in.Side), "action": strings.ToUpper(in.Action), "route": in.Route,
		"quantity": in.Quantity, "limit": in.LimitPrice, "principal": in.PrincipalUSD,
		"fee": in.FeeUSD, "client_order_id": in.ClientOrderID, "source_intent": in.SourceIntentID,
		"attempt_nonce": in.AttemptNonce, "execution_shadow_attempt_id": attemptID,
		"proof": proofJSON,
	})
	return attemptID, proofJSON, hash, err
}

func (s *Server) r148ReserveSingleRisk(ctx context.Context, in r148SingleRiskRequest) (string, error) {
	if s == nil || s.store == nil || in.PrincipalUSD < 0 || in.FeeUSD < 0 ||
		math.IsNaN(in.PrincipalUSD) || math.IsInf(in.PrincipalUSD, 0) || math.IsNaN(in.FeeUSD) || math.IsInf(in.FeeUSD, 0) {
		return "", errors.New("invalid single-order pending-risk request")
	}
	venue := strings.ToLower(strings.TrimSpace(in.Venue))
	product := strings.ToLower(strings.TrimSpace(in.Product))
	if (product != "reduce" && in.PrincipalUSD <= 0) || (venue == "polyus" && strings.TrimSpace(in.AttemptNonce) == "") {
		return "", errors.New("single-order pending-risk request lacks principal or durable attempt identity")
	}
	// Idempotency is derived only from the immutable dispatch and wire request. The authenticated
	// baseline is deliberately NOT part of this hash: its timestamp changes on every retry, and
	// including it would turn one timeout/restart into a second reservation.
	attemptID, proofJSON, hash, err := r148SingleRiskRequestIdentity(in)
	if err != nil {
		return "", err
	}
	reservationID := "risk-" + hash[:32]
	if existing, found, readErr := s.store.LivePendingRiskByID(ctx, reservationID); readErr != nil {
		return "", readErr
	} else if found {
		if existing.Intent.RequestHash != hash {
			return "", errors.New("pending-risk reservation id collision")
		}
		return reservationID, nil
	}
	var baseline r148LiveRiskBaseline
	switch venue {
	case "kalshi":
		baseline, err = s.r148KalshiRiskBaseline(ctx, in.Ticker)
	case "polyus":
		baseline, err = s.r148PolyUSRiskBaseline(ctx, in.Ticker)
	default:
		err = errors.New("unsupported pending-risk venue")
	}
	if err != nil {
		return "", err
	}
	expected, err := r148LiveRiskExpectedPosition(baseline.PositionQty, in.Quantity, in.Side, in.Action)
	if err != nil {
		return "", err
	}
	cluster := s.liveMirrorClusterKey(venue, in.Ticker, "")
	now := time.Now().UTC()
	inserted, err := s.store.InsertLivePendingRisk(ctx, storage.LivePendingRiskIntent{
		ReservationID: reservationID, Created: now, BaselineObserved: baseline.Observed,
		Product: product, DispatchSource: in.DispatchSource, SystemID: in.SystemID, Route: in.Route,
		PrincipalUSD: in.PrincipalUSD, FeeUSD: in.FeeUSD, CostUSD: in.PrincipalUSD + in.FeeUSD,
		SourceIntentID: in.SourceIntentID, ExecutionShadowAttemptID: attemptID,
		RequestHash: hash, ProofJSON: proofJSON,
		BaselineReceiptJSON: baseline.ReceiptJSON,
	}, []storage.LivePendingRiskLeg{{
		ReservationID: reservationID, Index: 0, Venue: venue, Ticker: in.Ticker,
		Side: strings.ToUpper(in.Side), Action: strings.ToUpper(in.Action), ClientOrderID: in.ClientOrderID,
		Quantity: in.Quantity, LimitPrice: in.LimitPrice, BaselinePositionQty: baseline.PositionQty,
		ExpectedPositionQty: expected, BaselineRestingRiskUSD: baseline.RestingRiskUSD,
	}}, []storage.LivePendingRiskCluster{{
		ReservationID: reservationID, Index: 0, Venue: venue, ClusterKey: cluster,
		MappingVersion: r148LiveRiskMappingVersion, ReservedUSD: in.PrincipalUSD + in.FeeUSD,
	}})
	if err != nil {
		return "", err
	}
	if !inserted {
		row, found, readErr := s.store.LivePendingRiskByID(ctx, reservationID)
		if readErr != nil || !found || len(row.Events) == 0 {
			return "", errors.New("pending-risk retry could not reload its immutable reservation")
		}
	}
	return reservationID, nil
}

func (s *Server) r148AppendRiskEvent(ctx context.Context, reservationID, eventType, attemptKey,
	action, clientOrderID, orderID, source, reason string, legIndex *int, filled, avg, fee float64, evidence any) error {
	if s == nil || s.store == nil || strings.TrimSpace(reservationID) == "" {
		return errors.New("pending-risk store unavailable")
	}
	now := time.Now().UTC()
	accountObserved := time.Time{}
	if eventType == storage.LivePendingRiskAccountVisible {
		accountObserved = now
	}
	_, err := s.store.AppendLivePendingRiskEvent(ctx, storage.LivePendingRiskEvent{
		ReservationID: reservationID, Observed: now, AccountObserved: accountObserved, EventType: eventType,
		LegIndex: legIndex, AttemptKey: attemptKey, Action: strings.ToUpper(action),
		ClientOrderID: clientOrderID, OrderID: orderID, FilledQty: filled,
		AveragePrice: avg, FeeTotal: fee, ReceiptSource: source, Reason: reason,
		EvidenceJSON: r148LiveRiskEvidence(evidence),
	})
	if err == nil && (eventType == storage.LivePendingRiskAccountVisible ||
		eventType == storage.LivePendingRiskReleased) {
		// AccountVisible removes a single's pending-risk overlay immediately; Released removes every
		// remaining reservation overlay. Fence any pre-ACK account prewarm before that protection
		// disappears, including an idempotent replay of either transition.
		s.invalidateR154KalshiAdmissionSnapshot()
	}
	if err == nil && eventType == storage.LivePendingRiskSubmitStarted {
		// The durable submit boundary is the exact point at which venue exposure can become unknown.
		// Pause every later write now; the current serialized handler continues its already-journaled
		// mutation, while its final exact release resumes the queue.
		s.setLiveAutoSafetyPause(liveSafetyPausePendingRisk,
			"an exact venue mutation crossed submit_started and awaits authoritative reconciliation")
	} else if err == nil && eventType == storage.LivePendingRiskAck {
		// A stable venue order id plus the immutable maximum-cost reservation makes exposure
		// conservatively identified even before order-scoped fee/account truth catches up. Clear
		// only this temporary global pause; the reservation, duplicate ticker guard, cluster
		// attribution, and total-exposure overlay all remain active until exact reconciliation.
		s.r151RefreshPendingRiskSafetyPause(ctx,
			"every submitted mutation has a stable venue identity and remains fully reserved")
	}
	return err
}

func r148RiskHasEvent(row storage.LivePendingRiskReservation, eventTypes ...string) bool {
	for _, event := range row.Events {
		for _, want := range eventTypes {
			if strings.EqualFold(event.EventType, want) {
				return true
			}
		}
	}
	return false
}

func r148RiskAttemptHasEvent(row storage.LivePendingRiskReservation, attemptKey string, eventTypes ...string) bool {
	for _, event := range row.Events {
		if event.AttemptKey != attemptKey {
			continue
		}
		for _, want := range eventTypes {
			if event.EventType == want {
				return true
			}
		}
	}
	return false
}

// r148PendingRiskTickerConflict is the restart-safe counterpart to kalPlaced/pus placement
// latches. Those maps deliberately die with the process; the immutable reservation does not. Any
// unreleased claim on this exact venue contract blocks another entry, even if authenticated account
// state has not caught up yet or the process restarted between submit and acknowledgement.
func (s *Server) r148PendingRiskTickerConflict(ctx context.Context, venue, ticker string) string {
	if s == nil || s.store == nil {
		return "durable-pending-risk-unavailable"
	}
	venue, ticker = strings.ToLower(strings.TrimSpace(venue)), strings.TrimSpace(ticker)
	rows, err := s.store.ActiveLivePendingRisk(ctx)
	if err != nil {
		return "durable-pending-risk-unreadable"
	}
	for _, row := range rows {
		for _, leg := range row.Legs {
			if strings.EqualFold(leg.Venue, venue) && strings.EqualFold(leg.Ticker, ticker) {
				return "existing-durable-pending-order"
			}
		}
	}
	return ""
}

// r148PendingRiskOverlay returns durable risk not yet proven visible in the authenticated account.
// Exact resting order ids are already counted by the normal account reader, so only their
// unrepresented remainder stays in the overlay. Multi-leg packages remain fully reserved until
// their coordinator proves the whole expected state vector.
func (s *Server) r148PendingRiskOverlay(ctx context.Context, venue string,
	restingByID map[string]float64) (float64, map[string]float64, error) {
	if s == nil || s.store == nil {
		return 0, map[string]float64{}, errors.New("pending-risk store unavailable")
	}
	rows, err := s.store.ActiveLivePendingRisk(ctx)
	if err != nil {
		return 0, map[string]float64{}, err
	}
	return r148PendingRiskOverlayRows(rows, venue, restingByID)
}

// r148PendingRiskOverlayRows lets a final account scan reuse one durable-risk read for total,
// cluster, and asset-class accounting. It is byte-for-byte the same overlay contract as the
// storage-loading wrapper above; callers cannot observe a different pending snapshot per rail.
func r148PendingRiskOverlayRows(rows []storage.LivePendingRiskReservation, venue string,
	restingByID map[string]float64) (float64, map[string]float64, error) {
	clusters := map[string]float64{}
	total := 0.0
	for _, row := range rows {
		if r148RiskHasEvent(row, "released") {
			continue
		}
		venueMatch := false
		for _, leg := range row.Legs {
			if strings.EqualFold(leg.Venue, venue) {
				venueMatch = true
				break
			}
		}
		if !venueMatch {
			continue
		}
		// A multi-leg package remains reserved at its whole-package maximum until its coordinator
		// appends a reservation-level release. One visible leg must never zero the other legs.
		accountVisible := len(row.Legs) == 1 && r148RiskHasEvent(row, storage.LivePendingRiskAccountVisible)
		represented := 0.0
		if len(row.Legs) == 1 && !accountVisible {
			for i := len(row.Events) - 1; i >= 0; i-- {
				e := row.Events[i]
				if e.EventType == storage.LivePendingRiskAck {
					id := strings.TrimSpace(e.OrderID)
					represented = math.Max(0, restingByID[id])
					break
				}
			}
		}
		unrepresented := row.Intent.CostUSD
		if accountVisible {
			unrepresented = 0
		} else if represented > 0 {
			unrepresented = math.Max(0, row.Intent.CostUSD-represented)
		}
		total += unrepresented
		// Combined-market positions lose the underlying event mapping. Keep their immutable cluster
		// attribution through settlement even after venue exposure becomes visible.
		keepComboAttribution := strings.EqualFold(row.Intent.Product, "combo")
		for _, cluster := range row.Clusters {
			if !strings.EqualFold(cluster.Venue, venue) {
				continue
			}
			v := unrepresented
			if keepComboAttribution && accountVisible {
				v = cluster.ReservedUSD
			} else if cluster.ReservedUSD < v {
				v = cluster.ReservedUSD
			}
			clusters[cluster.ClusterKey] += math.Max(0, v)
		}
	}
	if math.IsNaN(total) || math.IsInf(total, 0) {
		return 0, nil, errors.New("pending-risk overlay is non-finite")
	}
	return total, clusters, nil
}

func kalshiRestingRiskByID(rows []kalshi.Order) (map[string]float64, error) {
	out := make(map[string]float64, len(rows))
	for _, row := range rows {
		if row.RemainFP.Float() <= 0 {
			continue
		}
		risk, known := kalshiRestingRisk(row)
		if !known {
			return nil, fmt.Errorf("resting Kalshi order %q omitted current direction or price truth", strings.TrimSpace(row.OrderID))
		}
		if id := strings.TrimSpace(row.OrderID); id != "" {
			out[id] = risk
		}
	}
	return out, nil
}

type r148RiskAttemptIdentity struct {
	AttemptKey, Action, ClientOrderID, OrderID string
	LegIndex                                   int
	Acked                                      bool
	Fill                                       storage.LivePendingRiskEvent
	FillKnown                                  bool
}

// r148RiskAttemptIdentityFor binds every post-submit event back to the exact immutable submit.
// An order id can discover its attempt after a restart; an attempt key can identify a clean reject
// before the venue ever returned an order id. Multiple matches are corruption and fail closed.
func r148RiskAttemptIdentityFor(row storage.LivePendingRiskReservation, attemptKey, orderID string) (r148RiskAttemptIdentity, error) {
	attemptKey, orderID = strings.TrimSpace(attemptKey), strings.TrimSpace(orderID)
	if attemptKey == "" && orderID != "" {
		for _, event := range row.Events {
			if event.EventType != storage.LivePendingRiskAck || event.OrderID != orderID {
				continue
			}
			if attemptKey != "" && attemptKey != event.AttemptKey {
				return r148RiskAttemptIdentity{}, errors.New("order id belongs to multiple pending-risk attempts")
			}
			attemptKey = event.AttemptKey
		}
	}
	if attemptKey == "" {
		return r148RiskAttemptIdentity{}, errors.New("pending-risk attempt identity is unavailable")
	}
	var out r148RiskAttemptIdentity
	foundSubmit := false
	for _, event := range row.Events {
		if event.AttemptKey != attemptKey {
			continue
		}
		switch event.EventType {
		case storage.LivePendingRiskSubmitStarted:
			if foundSubmit || event.LegIndex == nil {
				return out, errors.New("pending-risk attempt has invalid submit identity")
			}
			foundSubmit = true
			out = r148RiskAttemptIdentity{AttemptKey: attemptKey, LegIndex: *event.LegIndex,
				Action: event.Action, ClientOrderID: event.ClientOrderID}
		case storage.LivePendingRiskAck:
			if !foundSubmit || event.LegIndex == nil || *event.LegIndex != out.LegIndex ||
				event.Action != out.Action || event.ClientOrderID != out.ClientOrderID || event.OrderID == "" {
				return out, errors.New("pending-risk acknowledgement differs from submit identity")
			}
			out.Acked, out.OrderID = true, event.OrderID
		case storage.LivePendingRiskFillSeen:
			if !out.Acked || event.OrderID != out.OrderID {
				return out, errors.New("pending-risk fill differs from acknowledgement identity")
			}
			out.Fill, out.FillKnown = event, true
		}
	}
	if !foundSubmit {
		return out, errors.New("pending-risk submit event is unavailable")
	}
	if orderID != "" && (!out.Acked || out.OrderID != orderID) {
		return out, errors.New("pending-risk order id differs from exact acknowledgement")
	}
	return out, nil
}

func (s *Server) r148RiskReservation(ctx context.Context, id string) (storage.LivePendingRiskReservation, error) {
	if s == nil || s.store == nil {
		return storage.LivePendingRiskReservation{}, errors.New("pending-risk store unavailable")
	}
	row, found, err := s.store.LivePendingRiskByID(ctx, strings.TrimSpace(id))
	if err != nil {
		return row, err
	}
	if !found {
		return row, errors.New("pending-risk reservation not found")
	}
	return row, nil
}

func (s *Server) r148ReleaseRisk(ctx context.Context, id, source, reason string, evidence any) error {
	if err := s.r148AppendRiskEvent(ctx, id, storage.LivePendingRiskReleased, "", "", "", "", source, reason,
		nil, 0, 0, 0, evidence); err != nil {
		return err
	}
	// Pending-risk reservations are the durable pause condition. Only the final exact release clears
	// it; a second unresolved order keeps all new money paused without latching the manual stop.
	s.r151RefreshPendingRiskSafetyPause(ctx,
		"all durable venue mutations have exact terminal/account-visible receipts")
	return nil
}

func (s *Server) r151RefreshPendingRiskSafetyPause(ctx context.Context, proof string) {
	if s == nil || s.store == nil {
		return
	}
	if rows, err := s.store.ActiveLivePendingRisk(ctx); err == nil && !r151HasUnidentifiedPendingRisk(rows) {
		s.clearLiveAutoSafetyPause(liveSafetyPausePendingRisk, proof)
	}
}

func r151HasUnidentifiedPendingRisk(rows []storage.LivePendingRiskReservation) bool {
	for _, row := range rows {
		if r148RiskHasEvent(row, storage.LivePendingRiskReleased) {
			continue
		}
		// An explicit ambiguity/freeze is never covered by ordinary maximum-cost bookkeeping.
		if r148RiskHasEvent(row, storage.LivePendingRiskAmbiguous, storage.LivePendingRiskFrozen) {
			return true
		}
		// A combo whose exact venue position is already visible stays in the ledger only to preserve
		// event-cluster attribution through settlement; it is known exposure, not a reason to stop
		// unrelated singles.
		if strings.EqualFold(row.Intent.Product, "combo") &&
			r148RiskHasEvent(row, storage.LivePendingRiskAccountVisible) {
			continue
		}
		if (strings.EqualFold(row.Intent.Product, "single") || strings.EqualFold(row.Intent.Product, "reduce")) &&
			r148RiskHasEvent(row, storage.LivePendingRiskRestingVisible, storage.LivePendingRiskAccountVisible) {
			continue
		}
		// For a single/reduce, one exact submit followed by one internally consistent ACK is also
		// identified enough to let unrelated orders continue: the durable overlay still charges the
		// full immutable principal+fee maximum until exact fills/account state release it. A missing
		// or malformed ACK remains a global pause because the venue mutation cannot be named.
		if strings.EqualFold(row.Intent.Product, "single") || strings.EqualFold(row.Intent.Product, "reduce") {
			submits := make([]storage.LivePendingRiskEvent, 0, 1)
			for _, event := range row.Events {
				if event.EventType == storage.LivePendingRiskSubmitStarted {
					submits = append(submits, event)
				}
			}
			if len(submits) == 1 {
				identity, err := r148RiskAttemptIdentityFor(row, submits[0].AttemptKey, "")
				if err == nil && identity.Acked && strings.TrimSpace(identity.OrderID) != "" &&
					len(row.Legs) == 1 && row.Intent.CostUSD > 0 {
					continue
				}
			}
		}
		return true
	}
	return false
}

func (s *Server) r148RiskCleanRejected(ctx context.Context, id, attempt, source, reason string, evidence any) error {
	row, err := s.r148RiskReservation(ctx, id)
	if err != nil {
		return err
	}
	identity, err := r148RiskAttemptIdentityFor(row, attempt, "")
	if err != nil {
		return err
	}
	leg := identity.LegIndex
	if err := s.r148AppendRiskEvent(ctx, id, storage.LivePendingRiskCleanRejected, identity.AttemptKey,
		identity.Action, identity.ClientOrderID, "", source, reason, &leg, 0, 0, 0, evidence); err != nil {
		return err
	}
	return s.r148ReleaseRisk(ctx, id, source, "venue proved the mutation rejected before exposure", evidence)
}

func (s *Server) r148RiskTerminalUnfilled(ctx context.Context, id, attempt, orderID, source, reason string, evidence any) error {
	row, err := s.r148RiskReservation(ctx, id)
	if err != nil {
		return err
	}
	identity, err := r148RiskAttemptIdentityFor(row, attempt, orderID)
	if err != nil || !identity.Acked {
		return firstNonNilError(err, errors.New("terminal-unfilled pending-risk order lacks acknowledgement"))
	}
	leg := identity.LegIndex
	if err := s.r148AppendRiskEvent(ctx, id, storage.LivePendingRiskTerminalUnfilled, identity.AttemptKey,
		identity.Action, identity.ClientOrderID, identity.OrderID, source, reason, &leg, 0, 0, 0, evidence); err != nil {
		return err
	}
	return s.r148ReleaseRisk(ctx, id, source, "exact venue receipt proved terminal zero fill", evidence)
}

func (s *Server) r148RiskAmbiguous(ctx context.Context, id, attempt, orderID, source, reason string, evidence any) {
	persistCtx := context.WithoutCancel(ctx)
	var appendErr error
	if strings.TrimSpace(attempt) == "" {
		appendErr = s.r148AppendRiskEvent(persistCtx, id, storage.LivePendingRiskAmbiguous, "", "", "", orderID,
			source, reason, nil, 0, 0, 0, evidence)
	} else if row, err := s.r148RiskReservation(persistCtx, id); err != nil {
		appendErr = err
	} else if identity, err := r148RiskAttemptIdentityFor(row, attempt, ""); err != nil {
		appendErr = err
	} else {
		eventOrderID := ""
		if identity.Acked {
			if strings.TrimSpace(orderID) != "" && identity.OrderID != strings.TrimSpace(orderID) {
				appendErr = errors.New("ambiguous order id differs from durable acknowledgement")
			} else {
				eventOrderID = identity.OrderID
			}
		}
		leg := identity.LegIndex
		if appendErr == nil {
			appendErr = s.r148AppendRiskEvent(persistCtx, id, storage.LivePendingRiskAmbiguous,
				identity.AttemptKey, identity.Action, identity.ClientOrderID, eventOrderID,
				source, reason, &leg, 0, 0, 0, evidence)
		}
	}
	if appendErr != nil {
		reason += "; durable ambiguity event failed: " + appendErr.Error()
	}
	s.setLiveAutoSafetyPause(liveSafetyPausePendingRisk,
		"venue outcome is unidentified and remains fully risk-reserved: "+reason)
}

func firstNonNilError(a, fallback error) error {
	if a != nil {
		return a
	}
	return fallback
}

func r148AccountUpdateAfter(raw string, baseline time.Time) (time.Time, bool) {
	if at, ok := r151AccountWatermark(raw); ok && at.After(baseline) {
		return at, true
	}
	return time.Time{}, false
}

type r151RiskBaselineReceipt struct {
	Clock               string `json:"baseline_clock"`
	TargetPositionFound *bool  `json:"baseline_target_position_found"`
}

func r151ExactScopedFillSource(venue, source string) bool {
	switch strings.ToLower(strings.TrimSpace(venue)) {
	case "kalshi":
		switch strings.ToLower(strings.TrimSpace(source)) {
		case "kalshi-get-order+order-scoped-fills", "kalshi-create-ack+order-scoped-fills":
			return true
		default:
			return false
		}
	case "polyus":
		return strings.EqualFold(strings.TrimSpace(source), "polyus-get-order")
	default:
		return false
	}
}

// r151RiskPositionFresh compares venue timestamps only with a venue-derived baseline.  Rows made
// before R151 lack that marker; a narrowly bounded compatibility path lets an exact, order-scoped
// fill tolerate at most five seconds of local/venue clock skew.  It cannot release from an
// acknowledgement, an estimated fill, or an arbitrarily old matching aggregate position.
func r151RiskPositionFresh(row storage.LivePendingRiskReservation, identity r148RiskAttemptIdentity,
	leg storage.LivePendingRiskLeg, raw string) (time.Time, bool) {
	at, parsed := r151AccountWatermark(raw)
	if !parsed {
		return time.Time{}, false
	}
	if at.After(row.Intent.BaselineObserved) {
		return at, true
	}
	if !identity.FillKnown || !r151ExactScopedFillSource(leg.Venue, identity.Fill.ReceiptSource) {
		return time.Time{}, false
	}
	var receipt r151RiskBaselineReceipt
	if err := json.Unmarshal([]byte(row.Intent.BaselineReceiptJSON), &receipt); err != nil {
		return time.Time{}, false
	}
	if receipt.Clock == r151RiskBaselineClock {
		// A complete authenticated pre-submit snapshot that lacked this target is direct proof that
		// the now-visible expected position is new; no cross-clock comparison is needed.
		if receipt.TargetPositionFound != nil && !*receipt.TargetPositionFound {
			return at, true
		}
		return time.Time{}, false
	}
	if strings.TrimSpace(receipt.Clock) != "" {
		return time.Time{}, false
	}
	skew := row.Intent.BaselineObserved.Sub(at)
	if skew >= 0 && skew <= 5*time.Second {
		return at, true
	}
	return time.Time{}, false
}

// r148RiskAccountVisible releases a terminal single only after the exact filled quantity is present
// in a fresh authenticated account read. A terminal partial maker fill is a valid final exposure;
// requiring the originally requested quantity would strand that reservation forever. Open partials,
// external offsets and account lag remain fully reserved because callers invoke this only after the
// order is terminal (or the requested quantity is completely filled).
func (s *Server) r148RiskAccountVisible(ctx context.Context, id, orderID, source string) bool {
	row, err := s.r148RiskReservation(ctx, id)
	if err != nil || len(row.Legs) != 1 {
		return false
	}
	identity, err := r148RiskAttemptIdentityFor(row, "", orderID)
	if err != nil || !identity.Acked || !identity.FillKnown {
		return false
	}
	leg := row.Legs[0]
	if identity.LegIndex != leg.Index || identity.Fill.FilledQty <= 0 ||
		identity.Fill.FilledQty > leg.Quantity+1e-8 ||
		identity.Fill.AveragePrice <= 0 || identity.Fill.AveragePrice >= 1 {
		return false
	}
	expectedPosition, err := r148LiveRiskExpectedPosition(leg.BaselinePositionQty,
		identity.Fill.FilledQty, leg.Side, leg.Action)
	if err != nil {
		return false
	}
	position := 0.0
	positionFound := false
	positionUpdated := time.Time{}
	positionUpdatedRaw := ""
	switch leg.Venue {
	case "kalshi":
		positions, readErr := s.kal.GetPositions(ctx)
		if readErr != nil {
			return false
		}
		for _, p := range positions {
			if strings.EqualFold(p.Ticker, leg.Ticker) {
				position = p.PositionQty()
				positionFound = true
				positionUpdatedRaw = p.LastUpdatedTS
				break
			}
		}
	case "polyus":
		positions, readErr := s.polyUSAuth.LivePositions(ctx)
		if readErr != nil {
			return false
		}
		for _, p := range positions {
			if strings.EqualFold(p.Slug, leg.Ticker) && !p.Expired {
				position = p.Net
				positionFound = true
				positionUpdatedRaw = p.Updated
				break
			}
		}
	default:
		return false
	}
	if math.Abs(position-expectedPosition) > 1e-8 {
		return false
	}
	// For a non-flat target, require the account row itself to have advanced after the immutable
	// pre-send baseline. A stale aggregate position that happens to equal the target cannot release
	// money. A flat target may be represented by complete absence from venues that omit zero rows.
	if math.Abs(expectedPosition) > 1e-8 {
		if !positionFound {
			return false
		}
		if positionUpdated, _ = r151RiskPositionFresh(row, identity, leg, positionUpdatedRaw); positionUpdated.IsZero() {
			return false
		}
	}
	evidence := map[string]any{"ticker": leg.Ticker, "expected_position": expectedPosition,
		"observed_position": position, "baseline_position": leg.BaselinePositionQty, "order_id": orderID,
		"account_position_found": positionFound, "account_position_updated": positionUpdated}
	legIndex := identity.LegIndex
	if err := s.r148AppendRiskEvent(context.WithoutCancel(ctx), id, storage.LivePendingRiskAccountVisible,
		identity.AttemptKey, identity.Action, identity.ClientOrderID, identity.OrderID, source,
		"exact fully-filled order and expected signed position are visible", &legIndex,
		identity.Fill.FilledQty, identity.Fill.AveragePrice, identity.Fill.FeeTotal, evidence); err != nil {
		return false
	}
	return s.r148ReleaseRisk(context.WithoutCancel(ctx), id, source,
		"exact filled single is represented by authenticated position exposure", evidence) == nil
}

func (s *Server) r148PollRiskAccountVisible(id, orderID, source string) {
	s.runGuarded("r148PendingRiskReconcile", func() {
		for _, wait := range []time.Duration{250 * time.Millisecond, time.Second, 2 * time.Second, 5 * time.Second} {
			time.Sleep(wait)
			ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
			done := s.r148RiskAccountVisible(ctx, id, orderID, source)
			cancel()
			if done {
				return
			}
		}
	})
}

// r151KalshiSettlementProof is the small, exported-field-free view needed to prove that a filled
// single has completed its whole account lifecycle after the venue removes the settled position.
// This is not a substitute for position visibility while the market remains open.
type r151KalshiSettlementProof struct {
	Ticker, Result, SettledTime                 string
	YesCount, NoCount, YesCost, NoCost, Revenue float64
	Fee                                         float64
	FeeKnown                                    bool
}

func r151KalshiSettlementView(st kalshi.Settlement) r151KalshiSettlementProof {
	return r151KalshiSettlementProof{Ticker: st.Ticker, Result: strings.ToLower(strings.TrimSpace(st.MarketResult)),
		SettledTime: st.SettledTime, YesCount: st.YesCount.Float(), NoCount: st.NoCount.Float(),
		YesCost: st.YesTotalCost.Float(), NoCost: st.NoTotalCost.Float(), Revenue: st.RevenueUSD(),
		Fee: st.FeeUSD(), FeeKnown: st.FeeKnown()}
}

// r151SettledSingleMatchesRisk is intentionally exact and narrow. It covers a BUY single whose
// pre-send baseline was flat and whose order-scoped fills are authoritative. The settlement row
// must show exactly that one side/quantity/cost/fee and its mathematically implied revenue. Any
// manual same-market activity, old aggregate position, scalar result, missing fee, or clock
// disagreement keeps the reservation active for investigation.
func r151SettledSingleMatchesRisk(row storage.LivePendingRiskReservation,
	identity r148RiskAttemptIdentity, proof r151KalshiSettlementProof) bool {
	if row.Intent.Product != "single" || len(row.Legs) != 1 || !identity.FillKnown {
		return false
	}
	leg := row.Legs[0]
	if leg.Venue != "kalshi" || leg.Action != "BUY" || math.Abs(leg.BaselinePositionQty) > 1e-9 ||
		!r151ExactScopedFillSource(leg.Venue, identity.Fill.ReceiptSource) ||
		!strings.EqualFold(proof.Ticker, leg.Ticker) || !proof.FeeKnown ||
		identity.Fill.FilledQty <= 0 || math.Abs(identity.Fill.FilledQty-leg.Quantity) > 1e-9 {
		return false
	}
	settledAt, ok := r151AccountWatermark(proof.SettledTime)
	if !ok || settledAt.Before(identity.Fill.Observed) {
		return false
	}
	qty, price, fee := identity.Fill.FilledQty, identity.Fill.AveragePrice, identity.Fill.FeeTotal
	if math.Abs(proof.Fee-fee) > 1e-8 {
		return false
	}
	won := false
	switch strings.ToUpper(leg.Side) {
	case "YES":
		if math.Abs(proof.YesCount-qty) > 1e-8 || math.Abs(proof.NoCount) > 1e-8 ||
			math.Abs(proof.YesCost-qty*price) > 1e-8 || math.Abs(proof.NoCost) > 1e-8 {
			return false
		}
		won = proof.Result == "yes"
	case "NO":
		if math.Abs(proof.NoCount-qty) > 1e-8 || math.Abs(proof.YesCount) > 1e-8 ||
			math.Abs(proof.NoCost-qty*price) > 1e-8 || math.Abs(proof.YesCost) > 1e-8 {
			return false
		}
		won = proof.Result == "no"
	default:
		return false
	}
	if proof.Result != "yes" && proof.Result != "no" {
		return false
	}
	expectedRevenue := 0.0
	if won {
		expectedRevenue = qty
	}
	return math.Abs(proof.Revenue-expectedRevenue) <= 1e-8
}

// r151ReleaseSettledSingleRisks runs on every authenticated settlement snapshot, including rows
// already present in the journal watermark. A process can be down between fill and account-visible
// proof and restart only after the venue has removed the settled position; exact settlement then
// supplies the terminal account proof rather than stranding the reservation forever.
func (s *Server) r151ReleaseSettledSingleRisks(ctx context.Context, settlements []kalshi.Settlement) {
	if s == nil || s.store == nil || len(settlements) == 0 {
		return
	}
	byTicker := make(map[string]r151KalshiSettlementProof, len(settlements))
	for _, settlement := range settlements {
		if ticker := strings.TrimSpace(settlement.Ticker); ticker != "" {
			byTicker[strings.ToUpper(ticker)] = r151KalshiSettlementView(settlement)
		}
	}
	rows, err := s.store.ActiveLivePendingRisk(ctx)
	if err != nil {
		s.setLiveAutoSafetyPause(liveSafetyPausePendingRisk,
			"durable pending-risk ledger is temporarily unreadable; retrying automatically")
		return
	}
	if r151HasUnidentifiedPendingRisk(rows) {
		s.setLiveAutoSafetyPause(liveSafetyPausePendingRisk,
			"a venue mutation is awaiting exact order/fill/account reconciliation")
	} else {
		s.clearLiveAutoSafetyPause(liveSafetyPausePendingRisk,
			"no unidentified venue mutation remains")
	}
	for _, row := range rows {
		if row.Intent.Product != "single" || len(row.Legs) != 1 || row.Legs[0].Venue != "kalshi" {
			continue
		}
		proof, ok := byTicker[strings.ToUpper(strings.TrimSpace(row.Legs[0].Ticker))]
		if !ok {
			continue
		}
		attempt := ""
		for _, event := range row.Events {
			if event.EventType != storage.LivePendingRiskSubmitStarted {
				continue
			}
			if attempt != "" { // more than one submit is ambiguous and cannot use this terminal shortcut
				attempt = ""
				break
			}
			attempt = event.AttemptKey
		}
		identity, identityErr := r148RiskAttemptIdentityFor(row, attempt, "")
		if identityErr != nil || !r151SettledSingleMatchesRisk(row, identity, proof) {
			continue
		}
		legIndex := identity.LegIndex
		evidence := map[string]any{"ticker": proof.Ticker, "market_result": proof.Result,
			"yes_count": proof.YesCount, "no_count": proof.NoCount, "yes_cost": proof.YesCost,
			"no_cost": proof.NoCost, "revenue": proof.Revenue, "fee": proof.Fee,
			"fee_known": proof.FeeKnown, "settled_time": proof.SettledTime,
			"order_id": identity.OrderID, "terminal_account_proof": "authenticated-settlement"}
		if err := s.r148AppendRiskEvent(context.WithoutCancel(ctx), row.Intent.ReservationID,
			storage.LivePendingRiskAccountVisible, identity.AttemptKey, identity.Action,
			identity.ClientOrderID, identity.OrderID, "kalshi-authenticated-settlement",
			"exact order-scoped fill and authenticated settlement prove the completed account exposure lifecycle",
			&legIndex, identity.Fill.FilledQty, identity.Fill.AveragePrice, identity.Fill.FeeTotal, evidence); err != nil {
			continue
		}
		_ = s.r148ReleaseRisk(context.WithoutCancel(ctx), row.Intent.ReservationID,
			"kalshi-authenticated-settlement", "exact filled single reached authoritative settlement", evidence)
	}
}

func r148SingleRiskLeg(row storage.LivePendingRiskReservation) (storage.LivePendingRiskLeg, error) {
	if len(row.Legs) != 1 || row.Legs[0].Index != 0 {
		return storage.LivePendingRiskLeg{}, errors.New("single pending-risk reservation does not have exactly one immutable leg")
	}
	return row.Legs[0], nil
}

func r148ValidateSingleKalshiOrder(order kalshi.Order, leg storage.LivePendingRiskLeg,
	identity r148RiskAttemptIdentity) error {
	bundleLeg := storage.ResearchRouteBundleLeg{Index: leg.Index, Venue: leg.Venue, Ticker: leg.Ticker,
		Side: leg.Side, Quantity: leg.Quantity}
	if strings.TrimSpace(order.OrderID) != identity.OrderID {
		return errors.New("Kalshi order lookup returned a different order id")
	}
	if err := r148ValidateKalshiOrder(order, bundleLeg, identity.Action, identity.ClientOrderID); err != nil {
		return err
	}
	limit := order.YesPrice()
	if strings.EqualFold(leg.Side, "NO") {
		limit = order.NoPrice()
	}
	if limit <= 0 || limit >= 1 || math.Abs(limit-leg.LimitPrice) > 1e-8 {
		return errors.New("Kalshi recovered order price differs from the immutable single-order intent")
	}
	return nil
}

func r148SingleKalshiReceipt(order kalshi.Order, fills []kalshi.Fill, leg storage.LivePendingRiskLeg,
	identity r148RiskAttemptIdentity) (r148StagedReceipt, error) {
	if err := r148ValidateSingleKalshiOrder(order, leg, identity); err != nil {
		return r148StagedReceipt{}, err
	}
	bundleLeg := storage.ResearchRouteBundleLeg{Index: leg.Index, Venue: leg.Venue, Ticker: leg.Ticker,
		Side: leg.Side, Quantity: leg.Quantity}
	receipt := r148KalshiReceipt(order, fills, bundleLeg, identity.Action)
	if receipt.State == r148ReceiptAmbiguous || !receipt.Authoritative {
		// A stable, validated order can become visible before its order-scoped fills. The complete
		// immutable max cost remains reserved and the exact ticker remains claimed, so this bounded
		// endpoint-convergence state is pending/retryable rather than unidentified venue risk.
		// Malformed fills, changed identity/direction/price, and every other contradiction still
		// return their ordinary non-transient error and pause new money.
		if receipt.Reason == "order and fill ledgers disagree" ||
			receipt.Reason == "executed FOK has no authoritative scoped fills" {
			return receipt, fmt.Errorf("%w: %s", errKalshiScopedFillsConverging, receipt.Reason)
		}
		return receipt, errors.New(receipt.Reason)
	}
	return receipt, nil
}

var errKalshiScopedFillsConverging = errors.New(
	"stable Kalshi order is awaiting complete order-scoped fills")

const kalshiScopedFillConvergenceGrace = 30 * time.Second

func kalshiScopedFillConvergencePending(err error, created, now time.Time) bool {
	age := now.Sub(created)
	return errors.Is(err, errKalshiScopedFillsConverging) &&
		age >= 0 && age <= kalshiScopedFillConvergenceGrace
}

// r151SingleKalshiAckFillReceipt covers a production IOC/FOK response shape observed on 2026-07-16:
// POST returned an exact order id, while the point lookup immediately returned not_found even
// though the order-scoped fills endpoint and authenticated position already contained the fill.
// The acknowledgement is durable before this path is entered.  Therefore a complete, uniquely
// identified set of scoped fills can prove the entire requested quantity without inventing a
// terminal order state.  A zero/partial/duplicate/wrong fill remains ambiguous and fails closed.
func r151SingleKalshiAckFillReceipt(fills []kalshi.Fill, leg storage.LivePendingRiskLeg,
	identity r148RiskAttemptIdentity, route string) (r148StagedReceipt, error) {
	const source = "kalshi-create-ack+order-scoped-fills"
	r := r148StagedReceipt{OrderID: identity.OrderID, State: r148ReceiptAmbiguous,
		Source: source, Reason: "acknowledgement and scoped fills do not prove the complete order"}
	if !identity.Acked || strings.TrimSpace(identity.OrderID) == "" ||
		strings.TrimSpace(identity.ClientOrderID) == "" || identity.LegIndex != leg.Index ||
		!strings.EqualFold(strings.TrimSpace(identity.Action), strings.TrimSpace(leg.Action)) ||
		!strings.EqualFold(strings.TrimSpace(route), "taker") {
		return r, errors.New("Kalshi fill-only recovery lacks the immutable acknowledgement identity")
	}
	expectedOutcome, _ := r148ExpectedKalshiDirection(storage.ResearchRouteBundleLeg{
		Index: leg.Index, Venue: leg.Venue, Ticker: leg.Ticker, Side: leg.Side, Quantity: leg.Quantity,
	}, identity.Action)
	qty, gross, fee := 0.0, 0.0, 0.0
	seen := make(map[string]struct{}, len(fills))
	for _, fill := range fills {
		fillID := strings.TrimSpace(fill.FillID)
		q := fill.Qty()
		price := fill.YesPriceUSD()
		if strings.EqualFold(expectedOutcome, "NO") {
			price = fill.NoPriceUSD()
		}
		_, duplicate := seen[fillID]
		if fillID == "" || duplicate || strings.TrimSpace(fill.OrderID) != identity.OrderID ||
			!strings.EqualFold(strings.TrimSpace(fill.Ticker), strings.TrimSpace(leg.Ticker)) ||
			!strings.EqualFold(fill.SideYesNo(), expectedOutcome) || q <= 0 || price <= 0 || price >= 1 ||
			!fill.IsTaker || !fill.FeeKnown() || fill.FeeUSD() < 0 {
			return r, errors.New("Kalshi fill-only recovery found a wrong, duplicate, or economically incomplete scoped fill")
		}
		// A BUY may improve below its limit and a SELL may improve above it; crossing the immutable
		// limit in the adverse direction means the receipt cannot belong to this intended order.
		if (strings.EqualFold(identity.Action, "BUY") && price > leg.LimitPrice+1e-9) ||
			(strings.EqualFold(identity.Action, "SELL") && price+1e-9 < leg.LimitPrice) {
			return r, errors.New("Kalshi fill-only recovery price violates the immutable order limit")
		}
		seen[fillID] = struct{}{}
		qty, gross, fee = qty+q, gross+q*price, fee+fill.FeeUSD()
		if qty > leg.Quantity+1e-9 {
			return r, errors.New("Kalshi fill-only recovery exceeds the immutable order quantity")
		}
	}
	if qty <= 0 || math.Abs(qty-leg.Quantity) > 1e-9 {
		return r, fmt.Errorf("%w: Kalshi fill-only recovery does not contain the complete immutable order quantity",
			errKalshiScopedFillsConverging)
	}
	r.State, r.Authoritative = r148ReceiptFilled, true
	r.FilledQty, r.AveragePrice, r.FeeTotal = qty, gross/qty, fee
	r.Reason = "exact create acknowledgement and complete order-scoped IOC fills"
	return r, nil
}

func r151KalshiPointOrderNotFound(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "status 404") &&
		(strings.Contains(msg, "not_found") || strings.Contains(msg, "not found"))
}

func r148ExpectedPUSTIF(route string) string {
	if strings.EqualFold(strings.TrimSpace(route), "maker") {
		return "TIME_IN_FORCE_GOOD_TILL_CANCEL"
	}
	return "TIME_IN_FORCE_IMMEDIATE_OR_CANCEL"
}

func r148SinglePUSReceipt(state polymarketus.OrderState, row storage.LivePendingRiskReservation,
	leg storage.LivePendingRiskLeg, identity r148RiskAttemptIdentity) (r148StagedReceipt, error) {
	r := r148StagedReceipt{OrderID: state.ID, State: r148ReceiptPending, Source: "polyus-get-order",
		Authoritative: true, Reason: "authenticated order remains open"}
	price := state.YesPx
	if strings.EqualFold(leg.Side, "NO") && price > 0 && price < 1 {
		price = 1 - price
	}
	if state.ID != identity.OrderID || !strings.EqualFold(state.MarketSlug, leg.Ticker) ||
		!strings.EqualFold(state.Outcome, leg.Side) || !strings.EqualFold(state.Action, identity.Action) ||
		math.Abs(state.OrderQty-leg.Quantity) > 1e-8 || state.Cum < 0 || state.Leaves < 0 ||
		state.Cum > leg.Quantity+1e-8 || !strings.EqualFold(state.TIF, r148ExpectedPUSTIF(row.Intent.Route)) ||
		price <= 0 || price >= 1 || math.Abs(price-leg.LimitPrice) > 1e-8 {
		return r148StagedReceipt{}, errors.New("PolyUS recovered order differs from the immutable single-order intent")
	}
	terminal := state.Terminal()
	if terminal && state.Leaves > 1e-8 {
		return r148StagedReceipt{}, errors.New("terminal PolyUS order still reports resting quantity")
	}
	if !terminal && math.Abs(state.Cum+state.Leaves-leg.Quantity) > 1e-8 {
		return r148StagedReceipt{}, errors.New("open PolyUS order quantities do not reconcile")
	}
	if state.Cum > 0 && (!state.CommissionTotalKnown || state.AvgPx <= 0 || state.AvgPx >= 1) {
		return r148StagedReceipt{}, errors.New("filled PolyUS order lacks exact average price or cumulative commission")
	}
	r.FilledQty, r.AveragePrice, r.FeeTotal = state.Cum, state.AvgPx, state.CommissionTotalUSD
	if strings.EqualFold(state.State, "ORDER_STATE_FILLED") && math.Abs(state.Cum-leg.Quantity) > 1e-8 {
		return r148StagedReceipt{}, errors.New("PolyUS FILLED state does not contain the immutable requested quantity")
	}
	if math.Abs(state.Cum-leg.Quantity) <= 1e-8 {
		r.State, r.Reason = r148ReceiptFilled, "authenticated PolyUS order is completely filled"
		return r, nil
	}
	if terminal {
		if state.Cum <= 1e-9 {
			r.State, r.Reason = r148ReceiptUnfilled, "authenticated PolyUS order is terminal without a fill"
		} else {
			r.State, r.Reason = r148ReceiptFilled, "authenticated PolyUS order is terminal with a partial fill"
		}
		return r, nil
	}
	switch strings.ToUpper(strings.TrimSpace(state.State)) {
	case "ORDER_STATE_NEW", "ORDER_STATE_PARTIALLY_FILLED", "ORDER_STATE_PENDING", "ORDER_STATE_OPEN":
		return r, nil
	default:
		return r148StagedReceipt{}, errors.New("PolyUS order has an unknown nonterminal state")
	}
}

func (s *Server) r148PersistSingleReceipt(ctx context.Context, row storage.LivePendingRiskReservation,
	identity r148RiskAttemptIdentity, receipt r148StagedReceipt, terminal bool, evidence any) error {
	leg, err := r148SingleRiskLeg(row)
	if err != nil {
		return err
	}
	legIndex := leg.Index
	if receipt.State == r148ReceiptUnfilled {
		if err := s.r148RiskTerminalUnfilled(ctx, row.Intent.ReservationID, identity.AttemptKey,
			identity.OrderID, receipt.Source, receipt.Reason, evidence); err != nil {
			return err
		}
		// This is background recovery, not the LIVE order response path. Reload the just-appended
		// authoritative terminal receipt and invalidate funded Paper only when an earlier durable
		// event carries the exact same detector attempt and sequence-stamped execution book.
		latest, found, err := s.store.LivePendingRiskByID(ctx, row.Intent.ReservationID)
		if err != nil {
			return err
		}
		if found {
			s.rememberFundedPaperLiveZeroFillFromRisk(latest)
		}
		return nil
	}
	if receipt.FilledQty > 0 {
		if err := s.r148AppendRiskEvent(ctx, row.Intent.ReservationID, storage.LivePendingRiskFillSeen,
			identity.AttemptKey, identity.Action, identity.ClientOrderID, identity.OrderID,
			receipt.Source, receipt.Reason, &legIndex, receipt.FilledQty, receipt.AveragePrice,
			receipt.FeeTotal, evidence); err != nil {
			return err
		}
	}
	if receipt.State == r148ReceiptPending {
		if !r148RiskAttemptHasEvent(row, identity.AttemptKey, storage.LivePendingRiskRestingVisible) {
			if err := s.r148AppendRiskEvent(ctx, row.Intent.ReservationID, storage.LivePendingRiskRestingVisible,
				identity.AttemptKey, identity.Action, identity.ClientOrderID, identity.OrderID,
				receipt.Source, "exact order remains active at the venue", &legIndex,
				receipt.FilledQty, receipt.AveragePrice, receipt.FeeTotal, evidence); err != nil {
				return err
			}
		}
		s.r151RefreshPendingRiskSafetyPause(ctx,
			"every active mutation is now exact and visible as venue resting/account exposure")
		return nil
	}
	if receipt.State != r148ReceiptFilled {
		return errors.New("single-order reconciler received an unsupported receipt state")
	}
	if terminal || receipt.FilledQty+1e-8 >= leg.Quantity {
		// A failed visibility read is normal account lag. Keep the reservation and retry on the next
		// durable sweep; only exact position truth releases it.
		_ = s.r148RiskAccountVisible(ctx, row.Intent.ReservationID, identity.OrderID,
			receipt.Source+"+authenticated-positions")
	}
	return nil
}

func (s *Server) r148ReconcileKalshiSingle(ctx context.Context, row storage.LivePendingRiskReservation,
	identity r148RiskAttemptIdentity) error {
	leg, err := r148SingleRiskLeg(row)
	if err != nil {
		return err
	}
	if s.kal == nil {
		return errors.New("authenticated Kalshi recovery client is unavailable")
	}
	if !identity.Acked {
		orders, err := s.kal.GetOrdersByClientOrderID(ctx, leg.Ticker, identity.ClientOrderID,
			row.Intent.Created.Add(-5*time.Second))
		if err != nil {
			return err
		}
		switch len(orders) {
		case 0:
			// Give the venue's history more than enough time to converge. After that, the complete
			// ticker/time cursor crawl is authoritative proof that this exact deterministic id never
			// became an order, so the pre-send claim can be released without guessing.
			if time.Since(row.Intent.Created) >= 2*time.Minute {
				return s.r148RiskCleanRejected(ctx, row.Intent.ReservationID, identity.AttemptKey,
					"kalshi-complete-client-order-history", "complete order history proves the submitted client id never became venue exposure",
					map[string]any{"ticker": leg.Ticker, "client_order_id": identity.ClientOrderID})
			}
			return nil
		case 1:
			candidate := orders[0]
			if strings.TrimSpace(candidate.OrderID) == "" {
				return errors.New("Kalshi client-order recovery returned an order without order_id")
			}
			probe := identity
			probe.OrderID, probe.Acked = candidate.OrderID, true
			if err := r148ValidateSingleKalshiOrder(candidate, leg, probe); err != nil {
				return err
			}
			legIndex := identity.LegIndex
			if err := s.r148AppendRiskEvent(ctx, row.Intent.ReservationID, storage.LivePendingRiskAck,
				identity.AttemptKey, identity.Action, identity.ClientOrderID, candidate.OrderID,
				"kalshi-complete-client-order-history", "recovered exact venue acknowledgement after restart",
				&legIndex, 0, leg.LimitPrice, 0, map[string]any{"order": candidate}); err != nil {
				return err
			}
			row, err = s.r148RiskReservation(ctx, row.Intent.ReservationID)
			if err != nil {
				return err
			}
			identity, err = r148RiskAttemptIdentityFor(row, identity.AttemptKey, candidate.OrderID)
			if err != nil {
				return err
			}
		default:
			return errors.New("Kalshi deterministic client order id matched multiple venue orders")
		}
	}
	order, err := s.kal.GetOrder(ctx, identity.OrderID)
	pointLookupMissing := r151KalshiPointOrderNotFound(err)
	if err != nil && !pointLookupMissing {
		return err
	}
	fills, err := s.kal.GetFillsForOrder(ctx, identity.OrderID)
	if err != nil {
		return err
	}
	if pointLookupMissing {
		receipt, receiptErr := r151SingleKalshiAckFillReceipt(fills, leg, identity, row.Intent.Route)
		if receiptErr != nil {
			return receiptErr
		}
		evidence := map[string]any{"order_lookup": "not_found", "order_id": identity.OrderID,
			"client_order_id": identity.ClientOrderID, "fills": fills}
		return s.r148PersistSingleReceipt(ctx, row, identity, receipt, true, evidence)
	}
	receipt, err := r148SingleKalshiReceipt(order, fills, leg, identity)
	if err != nil {
		return err
	}
	return s.r148PersistSingleReceipt(ctx, row, identity, receipt,
		r148KalshiTerminalStatus(order.Status), map[string]any{"order": order, "fills": fills})
}

func (s *Server) r148ReconcilePUSSingle(ctx context.Context, row storage.LivePendingRiskReservation,
	identity r148RiskAttemptIdentity) error {
	leg, err := r148SingleRiskLeg(row)
	if err != nil {
		return err
	}
	if s.polyUSAuth == nil {
		return errors.New("authenticated PolyUS recovery client is unavailable")
	}
	if !identity.Acked {
		if time.Since(row.Intent.Created) < 30*time.Second {
			return nil
		}
		return errors.New("PolyUS submit crossed the network boundary without a recoverable order id")
	}
	state, err := s.polyUSAuth.GetOrderState(ctx, identity.OrderID)
	if err != nil {
		return err
	}
	receipt, err := r148SinglePUSReceipt(state, row, leg, identity)
	if err != nil {
		return err
	}
	return s.r148PersistSingleReceipt(ctx, row, identity, receipt, state.Terminal(), map[string]any{"order_state": state})
}

// reconcileR148SinglePendingRisks is intentionally driven by the append-only ledger, never the
// liveOrders/liveMirrorPUSDispatches maps. It therefore continues late maker fill/cancel/account
// reconciliation after a restart and also clears exact terminal zero-fill reservations.
func (s *Server) reconcileR148SinglePendingRisks(ctx context.Context) {
	if s == nil || s.store == nil {
		return
	}
	s.livePendingRiskReconcileMu.Lock()
	defer s.livePendingRiskReconcileMu.Unlock()
	rows, err := s.store.ActiveLivePendingRisk(ctx)
	if err != nil {
		// This reader is the authority for whether a prior venue mutation is still unidentified.
		// A transient read failure must pause new AUTO money for this cycle, while preserving the
		// operator's ARM/AUTO choices and manual kill-switch state. The next clean read below either
		// keeps the pause for a real unresolved mutation or clears it automatically.
		s.setLiveAutoSafetyPause(liveSafetyPausePendingRisk,
			"durable pending-risk ledger is temporarily unreadable; retrying automatically")
		return
	}
	if r151HasUnidentifiedPendingRisk(rows) {
		s.setLiveAutoSafetyPause(liveSafetyPausePendingRisk,
			"a venue mutation is awaiting exact order/fill/account reconciliation")
	} else {
		s.clearLiveAutoSafetyPause(liveSafetyPausePendingRisk,
			"durable pending-risk ledger is readable and contains no unidentified venue mutation")
	}
	for _, row := range rows {
		if row.Intent.Product != "single" && row.Intent.Product != "reduce" {
			continue
		}
		var submits []storage.LivePendingRiskEvent
		for _, event := range row.Events {
			if event.EventType == storage.LivePendingRiskSubmitStarted {
				submits = append(submits, event)
			}
		}
		if len(submits) == 0 {
			// Do not race the handler between reservation and submit_started. Once stale, absence of
			// that durable boundary proves the venue mutation could not have been called.
			if time.Since(row.Intent.Created) >= 2*time.Minute {
				_ = s.r148ReleaseRisk(ctx, row.Intent.ReservationID, "pending-risk-recovery",
					"stale reservation has no submit_started event, proving no venue mutation began", nil)
			}
			continue
		}
		if len(submits) != 1 {
			if !r148RiskHasEvent(row, storage.LivePendingRiskAmbiguous, storage.LivePendingRiskFrozen) {
				s.r148RiskAmbiguous(ctx, row.Intent.ReservationID, "", "", "pending-risk-recovery",
					"single-order reservation contains multiple submit attempts", map[string]any{"submits": len(submits)})
			}
			continue
		}
		attempt := submits[0].AttemptKey
		if r148RiskAttemptHasEvent(row, attempt, storage.LivePendingRiskCleanRejected,
			storage.LivePendingRiskTerminalUnfilled, storage.LivePendingRiskAccountVisible) {
			// The terminal receipt and reservation-level release are two append-only commits. A
			// process can die between them; finish that exact, already-proven transition on restart.
			_ = s.r148ReleaseRisk(ctx, row.Intent.ReservationID, "pending-risk-recovery",
				"recovered terminal single-order receipt was durable before its release", nil)
			continue
		}
		identity, err := r148RiskAttemptIdentityFor(row, attempt, "")
		if err == nil {
			switch row.Legs[0].Venue {
			case "kalshi":
				err = s.r148ReconcileKalshiSingle(ctx, row, identity)
			case "polyus":
				err = s.r148ReconcilePUSSingle(ctx, row, identity)
			default:
				err = errors.New("single-order pending risk has an unsupported venue")
			}
		}
		if err != nil {
			if latest, found, readErr := s.store.LivePendingRiskByID(ctx, row.Intent.ReservationID); readErr == nil && found {
				if r148RiskHasEvent(latest, storage.LivePendingRiskReleased) {
					continue
				}
				row = latest
			}
		}
		if err != nil && !r148RiskHasEvent(row, storage.LivePendingRiskAmbiguous, storage.LivePendingRiskFrozen) {
			// Transient authenticated reads retain the claim and retry. Identity/schema conflicts or
			// an unrecoverable PolyUS pre-ACK mutation retain the money reservation and pause new AUTO
			// dispatch; they never latch the operator-only kill switch.
			msg := strings.ToLower(err.Error())
			transient := ctx.Err() != nil || errors.Is(err, context.Canceled) ||
				errors.Is(err, context.DeadlineExceeded) || strings.Contains(msg, "timeout") ||
				strings.Contains(msg, "context deadline") || strings.Contains(msg, "context canceled") ||
				strings.Contains(msg, "connection") || strings.Contains(msg, "temporar") ||
				strings.Contains(msg, "status 429") || strings.Contains(msg, "status 5") ||
				strings.Contains(msg, "client is unavailable") ||
				kalshiScopedFillConvergencePending(err, row.Intent.Created, time.Now())
			if !transient {
				s.r148RiskAmbiguous(ctx, row.Intent.ReservationID, attempt, identity.OrderID,
					"pending-risk-recovery", err.Error(), map[string]any{"error": err.Error()})
			}
		}
	}
}

func (s *Server) livePendingRiskWakeChannel() chan struct{} {
	s.livePendingRiskWakeOnce.Do(func() {
		s.livePendingRiskWakeCh = make(chan struct{}, 1)
	})
	return s.livePendingRiskWakeCh
}

// scheduleLivePendingRiskReconcile is nonblocking and coalesced. It is called immediately after a
// stable venue acknowledgement so exact order-scoped fills/account visibility begin while the
// urgent coordinator can rank the next signal. No reconciliation call is allowed to hold the
// real-money placement mutex.
func (s *Server) scheduleLivePendingRiskReconcile() {
	select {
	case s.livePendingRiskWakeChannel() <- struct{}{}:
	default:
	}
}

func (s *Server) monitorLivePendingRiskReconcile(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.livePendingRiskWakeChannel():
			reconcileCtx, cancel := context.WithTimeout(ctx, 4*time.Second)
			s.reconcileR148SinglePendingRisks(reconcileCtx)
			cancel()
			s.wakeLiveMirrorDispatch()
		}
	}
}

// r148PreArmPendingRiskReason runs while handleLiveArm holds livePlaceMu and both write gates are
// deliberately disarmed. Reservation-only rows are then provably pre-network leftovers and can be
// released. Submitted singles get one exact venue reconciliation pass. Anything still unresolved
// blocks ARM; the only exception is an account-visible combo whose reservation intentionally keeps
// canonical cluster attribution until settlement.
func (s *Server) r148PreArmPendingRiskReason(ctx context.Context) string {
	if s == nil || s.store == nil {
		return "durable pending-risk ledger is unavailable"
	}
	rows, err := s.store.ActiveLivePendingRisk(ctx)
	if err != nil {
		return "durable pending-risk ledger is unreadable: " + err.Error()
	}
	for _, row := range rows {
		if r148RiskHasEvent(row, storage.LivePendingRiskSubmitStarted) {
			continue
		}
		if err := s.r148ReleaseRisk(ctx, row.Intent.ReservationID, "pre-arm-pending-risk",
			"disarmed pre-ARM sweep proves reservation never crossed submit_started", nil); err != nil {
			return "could not release a pre-network pending-risk reservation: " + err.Error()
		}
	}
	s.reconcileR148SinglePendingRisks(ctx)
	rows, err = s.store.ActiveLivePendingRisk(ctx)
	if err != nil {
		return "durable pending-risk ledger is unreadable after recovery: " + err.Error()
	}
	blocked := make([]string, 0, 4)
	for _, row := range rows {
		comboVisible := row.Intent.Product == "combo" &&
			r148RiskHasEvent(row, storage.LivePendingRiskAccountVisible) &&
			!r148RiskHasEvent(row, storage.LivePendingRiskAmbiguous, storage.LivePendingRiskFrozen)
		if comboVisible {
			continue
		}
		state := "submitted"
		if r148RiskHasEvent(row, storage.LivePendingRiskFrozen) {
			state = "frozen"
		} else if r148RiskHasEvent(row, storage.LivePendingRiskAmbiguous) {
			state = "ambiguous"
		} else if !r148RiskHasEvent(row, storage.LivePendingRiskSubmitStarted) {
			state = "reservation-only"
		}
		blocked = append(blocked, fmt.Sprintf("%s(%s:%s)", row.Intent.ReservationID, row.Intent.Product, state))
		if len(blocked) == 4 {
			break
		}
	}
	if len(blocked) > 0 {
		return "unresolved durable venue mutation blocks ARM: " + strings.Join(blocked, ", ")
	}
	return ""
}
