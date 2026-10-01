package server

import (
	"context"
	"errors"
	"math"
	"strings"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

type r148ComboRiskRequest struct {
	MarketTicker, RFQID, QuoteID, Collection, SystemID, SourceIntentID, BundleID string
	UnderlyingTickers                                                            []string
	Quantity, Price, Fee                                                         float64
	Proof                                                                        any
}

func (s *Server) r148ActiveComboRisk(ctx context.Context, rfqID, quoteID string) (storage.LivePendingRiskReservation, bool, error) {
	if s == nil || s.store == nil {
		return storage.LivePendingRiskReservation{}, false, errors.New("combo pending-risk store unavailable")
	}
	rows, err := s.store.ActiveLivePendingRisk(ctx)
	if err != nil {
		return storage.LivePendingRiskReservation{}, false, err
	}
	var found storage.LivePendingRiskReservation
	hits := 0
	for _, row := range rows {
		if row.Intent.Product != "combo" || row.Intent.RFQID != strings.TrimSpace(rfqID) ||
			row.Intent.QuoteID != strings.TrimSpace(quoteID) {
			continue
		}
		found, hits = row, hits+1
	}
	if hits > 1 {
		return storage.LivePendingRiskReservation{}, false, errors.New("multiple active combo reservations share one RFQ quote")
	}
	return found, hits == 1, nil
}

func r148ComboRiskOrderID(row storage.LivePendingRiskReservation) string {
	for i := len(row.Events) - 1; i >= 0; i-- {
		if row.Events[i].EventType == storage.LivePendingRiskAck && row.Events[i].AttemptKey == "accept" {
			return strings.TrimSpace(row.Events[i].OrderID)
		}
	}
	return ""
}

// r148ComboEnsureAck binds the RFQ accept attempt to Kalshi's exact creator order id. An RFQ
// acceptance without this id remains fully reserved; it is never assigned a synthetic identity.
func (s *Server) r148ComboEnsureAck(ctx context.Context, row storage.LivePendingRiskReservation,
	creatorOrderID, source string) (storage.LivePendingRiskReservation, error) {
	creatorOrderID = strings.TrimSpace(creatorOrderID)
	if creatorOrderID == "" || len(row.Legs) != 1 {
		return row, errors.New("combo acknowledgement lacks exact creator order identity")
	}
	if prior := r148ComboRiskOrderID(row); prior != "" {
		if prior != creatorOrderID {
			return row, errors.New("combo creator order identity changed")
		}
		return row, nil
	}
	leg := row.Legs[0]
	legIndex := leg.Index
	if err := s.r148AppendRiskEvent(context.WithoutCancel(ctx), row.Intent.ReservationID,
		storage.LivePendingRiskAck, "accept", "BUY", "", creatorOrderID, source,
		"RFQ-scoped quote exposed the exact creator order id", &legIndex, 0, leg.LimitPrice, 0,
		map[string]any{"rfq_id": row.Intent.RFQID, "quote_id": row.Intent.QuoteID,
			"creator_order_id": creatorOrderID}); err != nil {
		return row, err
	}
	reloaded, err := s.r148RiskReservation(context.WithoutCancel(ctx), row.Intent.ReservationID)
	return reloaded, err
}

// r148ComboRecordFillAndVisibility removes only the venue-level double count after the exact MVE
// position is visible. It intentionally does not release the reservation: immutable underlying
// cluster attribution remains active until authoritative settlement.
func (s *Server) r148ComboRecordFillAndVisibility(ctx context.Context,
	row storage.LivePendingRiskReservation, creatorOrderID string, filled, avg, fee, position float64,
	source string, evidence any) error {
	var err error
	row, err = s.r148ComboEnsureAck(ctx, row, creatorOrderID, source)
	if err != nil || len(row.Legs) != 1 {
		return firstNonNilError(err, errors.New("combo risk row lacks its exact execution leg"))
	}
	if r148RiskHasEvent(row, storage.LivePendingRiskAccountVisible) {
		return nil
	}
	leg := row.Legs[0]
	if filled <= 0 || filled > leg.Quantity+1e-9 || avg <= 0 || avg >= 1 || fee < 0 {
		return errors.New("combo fill receipt is not exact and bounded")
	}
	identity, err := r148RiskAttemptIdentityFor(row, "accept", creatorOrderID)
	if err != nil {
		return err
	}
	legIndex := leg.Index
	if err := s.r148AppendRiskEvent(context.WithoutCancel(ctx), row.Intent.ReservationID,
		storage.LivePendingRiskFillSeen, identity.AttemptKey, identity.Action, identity.ClientOrderID,
		identity.OrderID, source, "exact RFQ creator-order fill observed", &legIndex,
		filled, avg, fee, evidence); err != nil {
		return err
	}
	if filled+1e-9 < leg.Quantity || math.Abs(position-leg.ExpectedPositionQty) > 1e-8 {
		return nil
	}
	return s.r148AppendRiskEvent(context.WithoutCancel(ctx), row.Intent.ReservationID,
		storage.LivePendingRiskAccountVisible, identity.AttemptKey, identity.Action, identity.ClientOrderID,
		identity.OrderID, source, "exact combined-market position is visible; underlying clusters stay reserved until settlement",
		&legIndex, filled, avg, fee, map[string]any{"position_qty": position,
			"expected_position_qty": leg.ExpectedPositionQty, "fill": evidence})
}

func (s *Server) r148ComboTerminalUnfilledRisk(ctx context.Context,
	row storage.LivePendingRiskReservation, creatorOrderID, source, reason string, evidence any) error {
	creatorOrderID = strings.TrimSpace(creatorOrderID)
	if creatorOrderID == "" {
		// A terminal RFQ-scoped quote with no creator order proves that no order was created.
		return s.r148RiskCleanRejected(context.WithoutCancel(ctx), row.Intent.ReservationID,
			"accept", source, reason, evidence)
	}
	var err error
	row, err = s.r148ComboEnsureAck(ctx, row, creatorOrderID, source)
	if err != nil {
		return err
	}
	return s.r148RiskTerminalUnfilled(context.WithoutCancel(ctx), row.Intent.ReservationID,
		"accept", creatorOrderID, source, reason, evidence)
}

func (s *Server) r148ReleaseSettledComboRisk(ctx context.Context, rfqID, quoteID, source string, evidence any) error {
	row, found, err := s.r148ActiveComboRisk(ctx, rfqID, quoteID)
	if err != nil || !found {
		return firstNonNilError(err, errors.New("settled combo has no active durable risk reservation"))
	}
	if !r148RiskHasEvent(row, storage.LivePendingRiskAccountVisible) {
		// A process can be down for the entire position lifetime and restart only after settlement,
		// when the open-position quantity has correctly returned to zero. An exact creator-order fill
		// plus the authenticated final settlement is then the account-visible terminal proof.
		identity, identityErr := r148RiskAttemptIdentityFor(row, "accept", r148ComboRiskOrderID(row))
		if identityErr != nil || !identity.FillKnown || len(row.Legs) != 1 ||
			identity.Fill.FilledQty+1e-9 < row.Legs[0].Quantity {
			return firstNonNilError(identityErr, errors.New("settled combo lacks its exact full-fill receipt"))
		}
		legIndex := row.Legs[0].Index
		if err := s.r148AppendRiskEvent(context.WithoutCancel(ctx), row.Intent.ReservationID,
			storage.LivePendingRiskAccountVisible, identity.AttemptKey, identity.Action,
			identity.ClientOrderID, identity.OrderID, source,
			"exact creator-order fill and authenticated final settlement prove the account exposure lifecycle",
			&legIndex, identity.Fill.FilledQty, identity.Fill.AveragePrice, identity.Fill.FeeTotal,
			map[string]any{"settlement_terminal_proof": evidence}); err != nil {
			return err
		}
	}
	return s.r148ReleaseRisk(context.WithoutCancel(ctx), row.Intent.ReservationID, source,
		"authoritative combined-market settlement ended the underlying cluster exposure", evidence)
}

// r148ReconcileActiveComboRisks is intentionally independent of live_combos. If Kalshi accepts
// the quote and the legacy position-row insert then fails, this immutable pre-send ledger still
// has enough RFQ identity to recover the order, fill, account visibility, and eventual release.
func (s *Server) r148ReconcileActiveComboRisks(ctx context.Context, limit int) {
	if s == nil || s.store == nil || s.kal == nil || limit <= 0 {
		return
	}
	rows, err := s.store.ActiveLivePendingRisk(ctx)
	if err != nil {
		return
	}
	seen := 0
	for _, row := range rows {
		if row.Intent.Product != "combo" || seen >= limit || len(row.Legs) != 1 {
			continue
		}
		seen++
		quote, err := s.kal.GetRFQQuote(ctx, row.Intent.RFQID, row.Intent.QuoteID)
		if err != nil {
			continue
		}
		leg := row.Legs[0]
		if quote.ID != row.Intent.QuoteID || quote.RFQID != row.Intent.RFQID ||
			(quote.MarketTicker != "" && !strings.EqualFold(quote.MarketTicker, leg.Ticker)) {
			s.setLiveAutoSafetyPause(liveSafetyPausePendingRisk,
				"RFQ identity changed while reconciling durable combo risk")
			continue
		}
		status := strings.ToLower(strings.TrimSpace(quote.Status))
		terminalQuote := status == "cancelled" || status == "canceled" || status == "expired" || status == "rejected"
		creatorOrderID := strings.TrimSpace(quote.RFQCreatorOrderID)
		if creatorOrderID == "" {
			creatorOrderID = r148ComboRiskOrderID(row)
		}
		if creatorOrderID == "" {
			if terminalQuote {
				_ = s.r148ComboTerminalUnfilledRisk(ctx, row, "", "kalshi-rfq-risk-reconcile",
					"terminal RFQ quote exposed no creator order", map[string]any{"quote_status": status})
			}
			continue
		}
		row, err = s.r148ComboEnsureAck(ctx, row, creatorOrderID, "kalshi-rfq-risk-reconcile")
		if err != nil {
			s.setLiveAutoSafetyPause(liveSafetyPausePendingRisk,
				"combo pending-risk acknowledgement reconciliation failed: "+err.Error())
			continue
		}
		fills, err := s.kal.GetFillsForOrder(ctx, creatorOrderID)
		if err != nil {
			continue
		}
		qty, avg, fee, fillFeeKnown, ids := liveComboFillAggregate(fills, leg.Ticker, creatorOrderID)
		if qty <= 0 {
			if !terminalQuote {
				continue
			}
			order, orderErr := s.kal.GetOrder(ctx, creatorOrderID)
			orderStatus := strings.ToLower(strings.TrimSpace(order.Status))
			terminalOrder := orderStatus == "cancelled" || orderStatus == "canceled" ||
				orderStatus == "expired" || orderStatus == "rejected"
			if orderErr == nil && terminalOrder && order.Filled() == 0 && order.Remaining() == 0 {
				_ = s.r148ComboTerminalUnfilledRisk(ctx, row, creatorOrderID,
					"kalshi-rfq-order-risk-reconcile", "terminal exact creator order has zero fills",
					map[string]any{"quote_status": status, "order_status": orderStatus})
			}
			continue
		}
		expectedFee, feeKnown, feeSource := s.kalFeeExact(leg.Ticker, false, qty, avg)
		if !fillFeeKnown || !feeKnown || math.Abs(expectedFee-fee) > .010000001 {
			continue
		}
		position := 0.0
		positions, positionErr := s.kal.GetPositions(ctx)
		if positionErr == nil {
			for _, p := range positions {
				if strings.EqualFold(p.Ticker, leg.Ticker) {
					position = p.PositionQty()
					break
				}
			}
		}
		evidence := map[string]any{"rfq_id": row.Intent.RFQID, "quote_id": row.Intent.QuoteID,
			"creator_order_id": creatorOrderID, "fill_ids": ids, "fee_source": feeSource,
			"quote_status": status, "position_read_ok": positionErr == nil}
		if err := s.r148ComboRecordFillAndVisibility(ctx, row, creatorOrderID, qty, avg, fee,
			position, "kalshi-rfq-risk-reconcile", evidence); err != nil {
			continue
		}
		row, _ = s.r148RiskReservation(ctx, row.Intent.ReservationID)
		if !r148RiskHasEvent(row, storage.LivePendingRiskAccountVisible) {
			continue
		}
		if market, marketErr := s.kal.GetMarket(ctx, leg.Ticker); marketErr == nil && market.SettledYes() >= 0 {
			_ = s.r148ReleaseSettledComboRisk(context.WithoutCancel(ctx), row.Intent.RFQID,
				row.Intent.QuoteID, "kalshi-rfq-risk-reconcile",
				map[string]any{"market": leg.Ticker, "settled_yes": market.SettledYes()})
		}
	}
}

func (s *Server) r148ReserveComboRisk(ctx context.Context, in r148ComboRiskRequest) (string, error) {
	if s == nil || s.store == nil || strings.TrimSpace(in.MarketTicker) == "" ||
		strings.TrimSpace(in.RFQID) == "" || strings.TrimSpace(in.QuoteID) == "" ||
		in.Quantity <= 0 || in.Price <= 0 || in.Price >= 1 || in.Fee < 0 {
		return "", errors.New("invalid combo pending-risk request")
	}
	baseline, err := s.r148KalshiRiskBaseline(ctx, in.MarketTicker)
	if err != nil {
		return "", err
	}
	expected, err := r148LiveRiskExpectedPosition(baseline.PositionQty, in.Quantity, "YES", "BUY")
	if err != nil {
		return "", err
	}
	principal := in.Quantity * in.Price
	proofJSON := r148LiveRiskEvidence(in.Proof)
	hash, err := r148LiveRiskHash(map[string]any{
		"product": "combo", "market": in.MarketTicker, "rfq_id": in.RFQID, "quote_id": in.QuoteID,
		"collection": in.Collection, "quantity": in.Quantity, "price": in.Price, "fee": in.Fee,
		"source_intent": in.SourceIntentID, "bundle_id": in.BundleID, "proof": proofJSON,
	})
	if err != nil {
		return "", err
	}
	id := "risk-" + hash[:32]
	if existing, found, readErr := s.store.LivePendingRiskByID(ctx, id); readErr != nil {
		return "", readErr
	} else if found {
		if existing.Intent.RequestHash != hash {
			return "", errors.New("combo pending-risk identity collision")
		}
		return id, nil
	}
	clusters := make([]storage.LivePendingRiskCluster, 0, len(in.UnderlyingTickers))
	seen := map[string]bool{}
	for _, ticker := range in.UnderlyingTickers {
		key := s.liveMirrorClusterKey("kalshi", ticker, "")
		if key == "" || seen[key] {
			continue
		}
		seen[key] = true
		clusters = append(clusters, storage.LivePendingRiskCluster{ReservationID: id, Index: len(clusters),
			Venue: "kalshi", ClusterKey: key, MappingVersion: r148LiveRiskMappingVersion,
			ReservedUSD: principal + in.Fee})
	}
	if len(clusters) == 0 {
		return "", errors.New("combo has no durable underlying risk clusters")
	}
	systemID := strings.TrimSpace(in.SystemID)
	if systemID == "" {
		systemID = "combo"
	}
	inserted, err := s.store.InsertLivePendingRisk(ctx, storage.LivePendingRiskIntent{
		ReservationID: id, Created: baseline.Observed, BaselineObserved: baseline.Observed,
		Product: "combo", DispatchSource: "handleLiveComboPlace", SystemID: systemID, Route: "rfq",
		PrincipalUSD: principal, FeeUSD: in.Fee, CostUSD: principal + in.Fee,
		RFQID: in.RFQID, QuoteID: in.QuoteID, SourceIntentID: in.SourceIntentID, BundleID: in.BundleID,
		RequestHash: hash, ProofJSON: proofJSON, BaselineReceiptJSON: baseline.ReceiptJSON,
	}, []storage.LivePendingRiskLeg{{ReservationID: id, Index: 0, Venue: "kalshi",
		Ticker: in.MarketTicker, Side: "YES", Action: "BUY", Quantity: in.Quantity,
		LimitPrice: in.Price, BaselinePositionQty: baseline.PositionQty,
		ExpectedPositionQty: expected, BaselineRestingRiskUSD: baseline.RestingRiskUSD}}, clusters)
	if err != nil {
		return "", err
	}
	if !inserted {
		return "", errors.New("combo risk reservation was not inserted or reloaded")
	}
	return id, nil
}
