package server

import (
	"context"
	"errors"
	"math"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

const fundedPaperCorrectedExecutionGenerationV1 = "funded-paper-delayed-two-touch-wire-ioc-v5-canonical-shadow-kalshi-5s-gated"

// This policy is fixed before the joined cohort is reviewed. The version belongs in every report
// that consumes these proofs; changing the age ceiling requires a new policy/version rather than a
// retrospective setting change on already-settled outcomes.
const (
	fundedPaperImmutableReviewPolicyV1   = "immutable-execution-ledger-join-v1"
	fundedPaperImmutableMaxBookAgeMSV1   = 5000.0
	fundedPaperImmutableFloatToleranceV1 = 1e-9
)

type fundedPaperImmutableProof struct {
	Attempt       storage.ExecutionShadowAttempt
	PaperTerminal storage.ExecutionShadowEvent
	LiveTerminal  storage.ExecutionShadowEvent
	LiveOrderID   string
}

type fundedPaperImmutableProofMap map[string]fundedPaperImmutableProof

func finiteFundedPaperValue(v float64) bool {
	return !math.IsNaN(v) && !math.IsInf(v, 0)
}

func sameFundedPaperValue(a, b float64) bool {
	return finiteFundedPaperValue(a) && finiteFundedPaperValue(b) &&
		math.Abs(a-b) <= fundedPaperImmutableFloatToleranceV1
}

func fundedPaperLiveTerminalPriority(event storage.ExecutionShadowEvent) (int, bool) {
	if strings.TrimSpace(event.LiveState) == "" {
		return 0, false
	}
	switch event.Stage {
	case "live-exact-reconcile":
		return 0, true
	case "live-terminal":
		return 1, true
	}
	if !event.VenueAttempted && !event.LiveAuthoritative &&
		strings.EqualFold(strings.TrimSpace(event.LiveState), "not-sent") &&
		event.Stage != "combo-candidate-queue" && event.Stage != "combo-candidate-read" {
		if event.Stage == "queue-clear" &&
			strings.EqualFold(strings.TrimSpace(stringEvidence(event.Evidence, "queue_branch")),
				"live-mirror-combo") {
			return 0, false
		}
		return 2, true
	}
	return 0, false
}

func stringEvidence(evidence map[string]any, key string) string {
	if evidence == nil {
		return ""
	}
	v, _ := evidence[key].(string)
	return v
}

func fundedPaperCanonicalLiveTerminal(events []storage.ExecutionShadowEvent) (
	storage.ExecutionShadowEvent, bool) {
	var selected storage.ExecutionShadowEvent
	selectedPriority := 0
	found := false
	for _, event := range events {
		priority, eligible := fundedPaperLiveTerminalPriority(event)
		if !eligible {
			continue
		}
		if !found || priority < selectedPriority || priority == selectedPriority && event.ID > selected.ID {
			selected, selectedPriority, found = event, priority, true
		}
	}
	return selected, found
}

func completeFundedPaperTerminal(event storage.ExecutionShadowEvent) bool {
	if event.Stage != "funded-paper-terminal" ||
		!strings.EqualFold(strings.TrimSpace(event.Outcome), "paper-filled") ||
		!strings.EqualFold(strings.TrimSpace(event.PaperState), "PAPER-FILLED") ||
		strings.TrimSpace(event.PaperAttemptID) == "" ||
		event.PaperFilledQty == nil || event.PaperFillPrice == nil || event.PaperFee == nil ||
		strings.TrimSpace(event.PaperFeeSource) == "" ||
		strings.TrimSpace(event.PaperBookSource) == "" ||
		strings.TrimSpace(event.BookSource) == "" ||
		event.BookAgeMS == nil || event.SideBid == nil || event.SideAsk == nil ||
		event.VisibleDepth == nil || event.RequestedQty == nil {
		return false
	}
	qty, price, fee := *event.PaperFilledQty, *event.PaperFillPrice, *event.PaperFee
	bid, ask, depth, age := *event.SideBid, *event.SideAsk, *event.VisibleDepth, *event.BookAgeMS
	requested := *event.RequestedQty
	return finiteFundedPaperValue(qty) && qty > 0 &&
		finiteFundedPaperValue(requested) && requested+fundedPaperImmutableFloatToleranceV1 >= qty &&
		finiteFundedPaperValue(price) && price > 0 && price < 1 &&
		finiteFundedPaperValue(fee) && fee >= 0 &&
		finiteFundedPaperValue(bid) && bid > 0 && bid < 1 &&
		finiteFundedPaperValue(ask) && ask > 0 && ask < 1 && bid <= ask &&
		sameFundedPaperValue(ask, price) &&
		finiteFundedPaperValue(depth) && depth+fundedPaperImmutableFloatToleranceV1 >= qty &&
		finiteFundedPaperValue(age) && age >= 0 && age <= fundedPaperImmutableMaxBookAgeMSV1 &&
		strings.TrimSpace(event.BookSource) == strings.TrimSpace(event.PaperBookSource)
}

func completeFundedPaperLiveFill(event storage.ExecutionShadowEvent) bool {
	state := strings.ToLower(strings.TrimSpace(event.LiveState))
	return event.VenueAttempted && event.LiveAuthoritative &&
		(state == "full" || state == "filled" || state == "partial") &&
		strings.TrimSpace(event.LiveReservationID) != "" &&
		event.LiveFilledQty != nil && finiteFundedPaperValue(*event.LiveFilledQty) && *event.LiveFilledQty > 0 &&
		event.LiveFillPrice != nil && finiteFundedPaperValue(*event.LiveFillPrice) &&
		*event.LiveFillPrice > 0 && *event.LiveFillPrice < 1 &&
		event.LiveFee != nil && finiteFundedPaperValue(*event.LiveFee) && *event.LiveFee >= 0 &&
		strings.TrimSpace(event.LiveFeeSource) != "" &&
		strings.TrimSpace(event.LiveReceiptSource) != ""
}

func fundedPaperLiveOrderID(events []storage.ExecutionShadowEvent,
	terminal storage.ExecutionShadowEvent) string {
	if orderID := strings.TrimSpace(terminal.VenueOrderID); orderID != "" {
		return orderID
	}
	// Exact reconciliation owns fill/fee truth but older rows did not repeat the order ID. The
	// immutable handler terminal under the same reservation supplies that identity; no ticker or
	// price fallback is allowed.
	var orderID string
	var latest int64
	for _, event := range events {
		if !event.VenueAttempted || strings.TrimSpace(event.LiveReceiptSource) == "" ||
			strings.TrimSpace(event.LiveReservationID) != strings.TrimSpace(terminal.LiveReservationID) ||
			strings.TrimSpace(event.VenueOrderID) == "" || event.ID < latest {
			continue
		}
		orderID, latest = strings.TrimSpace(event.VenueOrderID), event.ID
	}
	return orderID
}

func fundedPaperImmutableProofFromView(view storage.ExecutionShadowAttemptView) (
	fundedPaperImmutableProof, bool) {
	attempt := view.Attempt
	if strings.TrimSpace(attempt.AttemptID) == "" || attempt.TriggerUnixMS <= 0 ||
		strings.TrimSpace(attempt.Venue) == "" || strings.TrimSpace(attempt.Ticker) == "" ||
		strings.TrimSpace(attempt.SystemID) == "" ||
		!strings.EqualFold(strings.TrimSpace(attempt.Action), "BUY") ||
		!strings.EqualFold(strings.TrimSpace(attempt.Route), "taker") {
		return fundedPaperImmutableProof{}, false
	}
	var paper storage.ExecutionShadowEvent
	paperTerminals := 0
	for _, event := range view.Events {
		if event.Stage != "funded-paper-terminal" {
			continue
		}
		paperTerminals++
		paper = event
	}
	if paperTerminals != 1 || !completeFundedPaperTerminal(paper) ||
		paper.At.UnixMilli() < attempt.TriggerUnixMS {
		return fundedPaperImmutableProof{}, false
	}
	live, found := fundedPaperCanonicalLiveTerminal(view.Events)
	if !found || !completeFundedPaperLiveFill(live) || live.At.UnixMilli() < attempt.TriggerUnixMS {
		return fundedPaperImmutableProof{}, false
	}
	orderID := fundedPaperLiveOrderID(view.Events, live)
	if orderID == "" {
		return fundedPaperImmutableProof{}, false
	}
	return fundedPaperImmutableProof{Attempt: attempt, PaperTerminal: paper,
		LiveTerminal: live, LiveOrderID: orderID}, true
}

func fundedPaperLotSystem(p kfPos) string {
	for _, field := range strings.Fields(p.RouteReason) {
		if system, ok := strings.CutPrefix(field, "fam:"); ok {
			return strings.TrimSpace(system)
		}
	}
	return ""
}

func fundedPaperLotTriggerUnixMS(p kfPos) (int64, bool) {
	raw := strings.TrimSpace(p.SignalTS)
	if raw == "" {
		return 0, false
	}
	at, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return 0, false
	}
	return at.UnixMilli(), true
}

func (proof fundedPaperImmutableProof) accepts(p kfPos) bool {
	triggerMS, triggerKnown := fundedPaperLotTriggerUnixMS(p)
	paper := proof.PaperTerminal
	return strings.TrimSpace(p.ExecutionShadowAttemptID) == proof.Attempt.AttemptID &&
		strings.EqualFold(strings.TrimSpace(fiLotPlatform(p)), strings.TrimSpace(proof.Attempt.Venue)) &&
		strings.TrimSpace(p.Ticker) == strings.TrimSpace(proof.Attempt.Ticker) &&
		strings.EqualFold(strings.TrimSpace(p.Side), strings.TrimSpace(proof.Attempt.Side)) &&
		fundedPaperLotSystem(p) == strings.TrimSpace(proof.Attempt.SystemID) &&
		triggerKnown && triggerMS == proof.Attempt.TriggerUnixMS &&
		strings.EqualFold(strings.TrimSpace(p.FillKind), "taker") &&
		p.FeeKnown && strings.TrimSpace(p.FeeSource) != "" &&
		finiteFundedPaperValue(p.Contracts) && p.Contracts > 0 &&
		finiteFundedPaperValue(p.Price) && p.Price > 0 && p.Price < 1 &&
		finiteFundedPaperValue(p.Fee) && p.Fee >= 0 &&
		sameFundedPaperValue(p.Contracts, *paper.PaperFilledQty) &&
		sameFundedPaperValue(p.Price, *paper.PaperFillPrice) &&
		sameFundedPaperValue(p.Fee, *paper.PaperFee) &&
		strings.TrimSpace(p.FeeSource) == strings.TrimSpace(paper.PaperFeeSource) &&
		strings.TrimSpace(p.ExecutionBookSource) == strings.TrimSpace(paper.PaperBookSource)
}

func (proofs fundedPaperImmutableProofMap) accepts(p kfPos) bool {
	proof, ok := proofs[strings.TrimSpace(p.ExecutionShadowAttemptID)]
	return ok && proof.accepts(p)
}

func (s *Server) fundedPaperImmutableProofs(ctx context.Context,
	lots []kfPos) (fundedPaperImmutableProofMap, error) {
	out := make(fundedPaperImmutableProofMap)
	if s == nil || s.store == nil {
		return out, errors.New("immutable execution ledger unavailable")
	}
	ids := make([]string, 0, len(lots))
	for _, lot := range lots {
		if id := strings.TrimSpace(lot.ExecutionShadowAttemptID); id != "" {
			ids = append(ids, id)
		}
	}
	views, err := s.store.ExecutionShadowAttemptsByIDs(ctx, ids)
	if err != nil {
		return out, err
	}
	for attemptID, view := range views {
		if proof, ok := fundedPaperImmutableProofFromView(view); ok {
			out[attemptID] = proof
		}
	}
	return out, nil
}

func fundedPaperClosedSnapshot(book *kfBook) []kfClosed {
	if book == nil || len(book.Closed) == 0 {
		return nil
	}
	return append([]kfClosed(nil), book.Closed...)
}

func fundedPaperLots(closed ...[]kfClosed) []kfPos {
	count := 0
	for _, rows := range closed {
		count += len(rows)
	}
	out := make([]kfPos, 0, count)
	for _, rows := range closed {
		for _, row := range rows {
			out = append(out, row.kfPos)
		}
	}
	return out
}

func fundedPaperBookLots(books ...*kfBook) []kfPos {
	count := 0
	for _, book := range books {
		if book != nil {
			count += len(book.Open) + len(book.Closed)
		}
	}
	out := make([]kfPos, 0, count)
	for _, book := range books {
		if book == nil {
			continue
		}
		out = append(out, book.Open...)
		for _, row := range book.Closed {
			out = append(out, row.kfPos)
		}
	}
	return out
}
