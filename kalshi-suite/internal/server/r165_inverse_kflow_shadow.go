package server

// Forward, money-free Kalshi-flow YES -> buy NO experiment.
//
// This lane is intentionally outside LIVE, canary, Paper allocation, proof history, balance,
// positions, and venue submission. It records the original public-trade millisecond clock, two
// sequence-proven full WebSocket books, a q1 exact fee, modeled IOC fill/zero-fill, and settlement
// in the isolated execution-shadow ledger.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

const (
	inverseKflowShadowIdentity       = "kalshi|invert:kalshi-flow|NO|taker"
	inverseKflowShadowExperimentKind = "inverse-kalshi-flow-no-q1-ioc"
	inverseKflowShadowModelVersion   = "inverse-kalshi-flow-shadow-r165-v1"
	inverseKflowShadowSignalSource   = "shadow-only:original-kalshi-flow-YES:" + inverseKflowShadowModelVersion
	inverseKflowShadowQualification  = "shadow-only-prior-free-original-kalshi-flow-YES:" + inverseKflowShadowModelVersion
)

func parseLiveSystemShadowAllowlist(raw string) (string, string) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", ""
	}
	parts := strings.FieldsFunc(raw, func(r rune) bool {
		return r == ',' || r == ';' || r == '\n' || r == '\r'
	})
	if len(parts) != 1 || !strings.EqualFold(strings.TrimSpace(parts[0]), inverseKflowShadowIdentity) {
		return "", "live-system-shadow-allowlist-only-accepts-" + inverseKflowShadowIdentity
	}
	return inverseKflowShadowIdentity, ""
}

func (s *Server) inverseKflowShadowEnabled() bool {
	if s == nil {
		return false
	}
	canonical, why := parseLiveSystemShadowAllowlist(s.cfg().Risk.LiveSystemShadowAllowlist)
	return why == "" && canonical == inverseKflowShadowIdentity
}

func inverseKflowShadowSignal(original storage.Signal) (storage.Signal, bool) {
	if !strings.EqualFold(strings.TrimSpace(original.Platform), "kalshi") ||
		!strings.EqualFold(strings.TrimSpace(original.SignalType), "kalshi-flow") ||
		!strings.EqualFold(strings.TrimSpace(original.Side), "YES") {
		return storage.Signal{}, false
	}
	inverse, ok := independentInverseSignalIdentity(original)
	if !ok || inverse.SignalType != "invert:kalshi-flow" || inverse.Side != "NO" {
		return storage.Signal{}, false
	}
	return inverse, true
}

func (s *Server) claimInverseKflowShadowEpisode(ticker string, now time.Time) bool {
	key := "shadow-only\x00invert:kalshi-flow\x00" + strings.TrimSpace(ticker)
	s.livePolicyMirrorMu.Lock()
	defer s.livePolicyMirrorMu.Unlock()
	if s.livePolicyMirrorSeen == nil {
		s.livePolicyMirrorSeen = make(map[string]time.Time)
	}
	if seen, ok := s.livePolicyMirrorSeen[key]; ok && now.Sub(seen) >= 0 &&
		now.Sub(seen) <= liveMirrorDedup {
		return false
	}
	s.livePolicyMirrorSeen[key] = now
	for oldKey, seen := range s.livePolicyMirrorSeen {
		if strings.HasPrefix(oldKey, "shadow-only\x00") && now.Sub(seen) > 2*liveMirrorDedup {
			delete(s.livePolicyMirrorSeen, oldKey)
		}
	}
	return true
}

func (s *Server) rollbackInverseKflowShadowEpisode(ticker string) {
	key := "shadow-only\x00invert:kalshi-flow\x00" + strings.TrimSpace(ticker)
	s.livePolicyMirrorMu.Lock()
	delete(s.livePolicyMirrorSeen, key)
	s.livePolicyMirrorMu.Unlock()
}

// queueInverseKflowShadow is only a bounded handoff into the ordinary independently quoted inverse
// producer. R170 retired the second execution-shadow producer: it minted a different attempt id
// for the same original Kalshi-flow trigger and could then suppress the richer generic
// Detector/Paper/LIVE fanout. The direct LIVE publication, when any, is performed before this
// function is called.
func (s *Server) queueInverseKflowShadow(original storage.Signal, signalAt time.Time) bool {
	if s == nil || s.store == nil || !s.inverseKflowShadowEnabled() {
		return false
	}
	inverse, ok := inverseKflowShadowSignal(original)
	if !ok {
		return false
	}
	if signalAt.IsZero() {
		signalAt = time.Now()
	}
	if !s.claimInverseKflowShadowEpisode(inverse.Ticker, time.Now()) {
		return false
	}
	c := liveMirrorCandidate{
		Platform: "kalshi", Ticker: inverse.Ticker, Title: inverse.Title,
		Side: "NO", Action: "BUY", Family: "invert:kalshi-flow",
		Source: inverseKflowShadowSignalSource, At: signalAt,
		InputTopology:   r147SignalInputTopology(inverse),
		InputObservedAt: r147SignalInputObservedAt(inverse),
	}
	work := livePolicyMirrorWork{
		// Preserve the original YES detector receipt. The bounded worker below obtains and stores
		// the real opposite-side book before it lets the existing generic fanout mint an id.
		Signal: original, Point: original.EntryPrice, SignalAt: signalAt, Candidate: c,
		ExperimentKind: inverseKflowShadowExperimentKind, ExecutionOnly: true,
	}
	work.DueAt = signalAt
	if s.enqueueLivePolicyMirrorWork(work) {
		return true
	}
	s.rollbackInverseKflowShadowEpisode(inverse.Ticker)
	return false
}

// runInverseKflowCanonicalFanout performs the slower opposite-book/storage work only after the
// event path has handed off. Success enters the same generic fanout used by periodic collection,
// so whichever producer arrives first owns one canonical id and every branch stays attached to
// that id. There is deliberately no dedicated execution-shadow attempt anymore.
func (s *Server) runInverseKflowCanonicalFanout(ctx context.Context, work livePolicyMirrorWork) {
	original := work.Signal
	inverse, ok := s.collectAndNoteIndependentInverse(ctx, original, work.SignalAt)
	if !ok {
		return
	}
	s.genfollowTriggeredSystemsAt(ctx, inverse, work.SignalAt)
}

func (s *Server) beginInverseKflowShadowAttempt(work livePolicyMirrorWork) (liveMirrorCandidate, bool) {
	c := work.Candidate
	c.At = work.SignalAt
	c.Price = 0 // actual NO ask belongs to the first full-book touch, never 1-original YES price.
	bound, bindWhy, declared := bindStaticTakerSignalCandidate(c, work.Signal)
	if !declared || bindWhy != "" {
		return c, false
	}
	c = bound
	var attempt storage.ExecutionShadowAttempt
	var ok bool
	c, attempt, ok = s.executionShadowBuildCandidate(c, inverseKflowShadowQualification, "")
	if !ok {
		return c, false
	}
	attempt.SignalSource = inverseKflowShadowSignalSource
	attempt.SystemID = "invert:kalshi-flow"
	attempt.Side, attempt.Action, attempt.Route = "NO", "BUY", "taker"
	s.executionShadowRememberTrigger(c.ShadowAttemptID, attempt.TriggerUnixMS)
	sourceAt := r147SignalInputObservedAt(work.Signal)
	sourceToDecisionMS := int64(-1)
	sourceUnixMS := int64(0)
	if !sourceAt.IsZero() {
		sourceToDecisionMS = work.SignalAt.Sub(sourceAt).Milliseconds()
		sourceUnixMS = sourceAt.UnixMilli()
	}
	event := s.executionShadowEvent(c.ShadowAttemptID, "inverse-kflow-shadow-detector",
		"eligible", "original Kalshi-flow emitted YES; inverse NO entered money-free shadow only", work.SignalAt)
	event.Evidence = map[string]any{
		"experiment_kind":         inverseKflowShadowExperimentKind,
		"model_version":           inverseKflowShadowModelVersion,
		"execution_model_version": inverseKflowShadowModelVersion,
		"forward_generation_id":   r168ForwardGenerationID,
		"signal_contract_id":      attempt.SignalContract,
		"cohort":                  "forward raw event-driven Kalshi-flow YES; distinct from the historical cash-selected 316",
		"economic_episode_ms":     liveMirrorDedup.Milliseconds(),
		"original_family":         "kalshi-flow", "original_emitted_side": "YES",
		"original_discovery_price": work.Point,
		"inverse_family":           "invert:kalshi-flow", "inverse_side": "NO",
		"original_trigger_at":           optionalExecutionShadowTimeText(work.SignalAt),
		"original_trigger_unix_ms":      work.SignalAt.UnixMilli(),
		"decision_emitted_at":           optionalExecutionShadowTimeText(work.SignalAt),
		"decision_emitted_unix_ms":      work.SignalAt.UnixMilli(),
		"source_trade_received_at":      optionalExecutionShadowTimeText(sourceAt),
		"source_trade_received_unix_ms": sourceUnixMS,
		"source_to_decision_ms":         sourceToDecisionMS,
		"signal_exec_expr":              work.Signal.ExecExpr,
		"real_money_authority":          false, "cash_path_blocking": false,
		"live_allowlist_consulted": false, "canary_allowlist_consulted": false,
		"paper_portfolio_consulted": false, "history_admission_consulted": false,
		"venue_post_possible": false,
	}
	// The original signal already carries the cached full-book transport receipt. Reuse only that
	// in-memory provenance here; the later shadow executor independently captures both NO books.
	event.BookSource = strings.TrimSpace(work.Signal.BookSource)
	event.BookGeneration, event.BookSubscriptionID, event.BookSequence =
		executionShadowQuoteReceipt(liveMirrorQuote{BookSource: event.BookSource})
	s.r168StampForwardDetectorEvent(&event, attempt, inverseKflowShadowModelVersion)
	liveNoSend := s.executionShadowEvent(c.ShadowAttemptID, "live-selection", "not-sent",
		"money-free inverse experiment has no LIVE cash authority", work.SignalAt)
	liveNoSend.LiveState = "not-sent"
	liveNoSend.Evidence = map[string]any{
		"experiment_kind": inverseKflowShadowExperimentKind,
		"model_version":   inverseKflowShadowModelVersion,
		"cash_authority":  false, "venue_post_possible": false,
		"paper_portfolio_admission": false,
	}
	// This experiment intentionally has no funded-Paper authority. Record that decision as an
	// explicit branch plus terminal on the same canonical attempt instead of leaving absence to
	// look like lost telemetry. Stable ids and the trigger clock make writer retry/replay
	// idempotent; neither event creates a Paper attempt, position, fee, fill, or P&L.
	paperBranch := inverseKflowPaperNonParticipationEvent(s, c.ShadowAttemptID,
		"funded-paper-branch", "not-offered", work.SignalAt)
	paperTerminal := inverseKflowPaperNonParticipationEvent(s, c.ShadowAttemptID,
		"funded-paper-terminal", "not_observed", work.SignalAt)
	paperTerminal.PaperState = "PAPER-NOT-OBSERVED"
	if !s.enqueueExecutionShadowWriteGroup([]executionShadowWrite{
		{attempt: &attempt}, {event: &event}, {event: &liveNoSend},
		{event: &paperBranch}, {event: &paperTerminal},
	}, false) {
		return c, false
	}
	return c, true
}

func inverseKflowPaperNonParticipationEvent(s *Server, attemptID, stage, outcome string,
	at time.Time) storage.ExecutionShadowEvent {
	reason := "shadow-only inverse experiment has no funded Paper authority"
	event := s.executionShadowEvent(attemptID, stage, outcome, reason, at)
	stable := sha256.Sum256([]byte(strings.Join([]string{
		attemptID, stage, inverseKflowShadowModelVersion,
	}, "|")))
	event.EventID = "execev-stable-" + hex.EncodeToString(stable[:16])
	event.Evidence = map[string]any{
		"experiment_kind":           inverseKflowShadowExperimentKind,
		"model_version":             inverseKflowShadowModelVersion,
		"funded_paper_authority":    false,
		"paper_portfolio_admission": false,
		"paper_attempt_created":     false,
		"real_money_authority":      false,
		"cash_path_blocking":        false,
		"market_outcome":            false,
	}
	return event
}

type inverseKflowShadowBook struct {
	quote     liveMirrorQuote
	fee       float64
	feeKnown  bool
	feeSource string
	evidence  map[string]any
}

func orderbookLevelsEvidence(levels []kalshi.OrderbookLevel, invert bool) []map[string]any {
	out := make([]map[string]any, 0, len(levels))
	for _, level := range levels {
		price := level.Price
		if invert {
			price = 1 - price
		}
		out = append(out, map[string]any{"price": price, "depth": level.Size})
	}
	return out
}

// fullKalshiBookEvidence is a reusable, money-inert logging hook. It records both economic sides,
// every ladder level returned by the full WS cache, top depth, source/receive/check clocks, age,
// and the reconnect-safe sequence domain. Funded Paper can reuse this helper later without this
// experiment reaching into or changing its executor.
func fullKalshiBookEvidence(book *kalshi.Orderbook, provenance kalshi.BookProvenance,
	checkedAt time.Time) map[string]any {
	ageMS := -1.0
	if !provenance.ReceivedAt.IsZero() {
		ageMS = float64(checkedAt.Sub(provenance.ReceivedAt)) / float64(time.Millisecond)
	}
	yesBid, yesAsk, noBid, noAsk := 0.0, 0.0, 0.0, 0.0
	yesBidDepth, yesAskDepth, noBidDepth, noAskDepth := 0.0, 0.0, 0.0, 0.0
	var yesBids, yesAsks []kalshi.OrderbookLevel
	if book != nil && len(book.YesBids) > 0 && len(book.YesAsks) > 0 {
		yesBids, yesAsks = book.YesBids, book.YesAsks
		yesBid, yesBidDepth = book.YesBids[0].Price, book.YesBids[0].Size
		yesAsk, yesAskDepth = book.YesAsks[0].Price, book.YesAsks[0].Size
		noBid, noBidDepth = 1-yesAsk, yesAskDepth
		noAsk, noAskDepth = 1-yesBid, yesBidDepth
	}
	return map[string]any{
		"book_kind": "sequence-proven-full-ws-ladder",
		"channel":   provenance.Channel, "generation": provenance.Generation,
		"subscription_id": provenance.SubscriptionID, "sequence": provenance.Sequence,
		"source_at":   optionalExecutionShadowTimeText(provenance.SourceAt),
		"received_at": optionalExecutionShadowTimeText(provenance.ReceivedAt),
		"checked_at":  optionalExecutionShadowTimeText(checkedAt), "age_ms": ageMS,
		"yes_top": map[string]any{"bid": yesBid, "bid_depth": yesBidDepth,
			"ask": yesAsk, "ask_depth": yesAskDepth},
		"no_top": map[string]any{"bid": noBid, "bid_depth": noBidDepth,
			"ask": noAsk, "ask_depth": noAskDepth},
		"yes_bid_levels": orderbookLevelsEvidence(yesBids, false),
		"yes_ask_levels": orderbookLevelsEvidence(yesAsks, false),
		// Kalshi's canonical ladder stores YES bids and YES asks. These derived arrays make the
		// opposite executable side explicit without pretending a midpoint/complement fill.
		"no_bid_levels": orderbookLevelsEvidence(yesAsks, true),
		"no_ask_levels": orderbookLevelsEvidence(yesBids, true),
	}
}

func (s *Server) captureInverseKflowShadowBook(ticker string) (inverseKflowShadowBook, string) {
	var out inverseKflowShadowBook
	if s == nil || s.kal == nil {
		return out, "inverse-shadow-kalshi-client-unavailable"
	}
	market, ok := s.kmkt(ticker)
	if !ok || market.Result != "" ||
		(!strings.EqualFold(market.Status, "active") && !strings.EqualFold(market.Status, "open")) {
		return out, "inverse-shadow-current-market-lifecycle-unavailable"
	}
	book, _, provenance, ok := s.kal.LiveBookWithProvenance(ticker, livePolicyMirrorBookMaxAge)
	checkedAt := time.Now().UTC()
	if !ok || book == nil {
		return out, "inverse-shadow-fresh-full-ws-book-unavailable"
	}
	noBid, _, noAsk, noAskDepth, sidesOK := kalshiFullBookSides(book, "NO")
	if !sidesOK || noBid <= 0 || noAsk <= 0 || noAsk >= 1 || noAskDepth < 1 {
		return out, "inverse-shadow-fresh-two-sided-executable-touch-unavailable"
	}
	tick, tickKnown := market.TickForKnown(noAsk)
	if !tickKnown || tick <= 0 || tick > 1 {
		return out, "inverse-shadow-current-market-tick-unavailable"
	}
	if provenance.Generation == 0 || provenance.SubscriptionID <= 0 || provenance.Sequence <= 0 ||
		provenance.ReceivedAt.IsZero() {
		return out, "inverse-shadow-book-provenance-unavailable"
	}
	source := fmt.Sprintf("kalshi_ws_full_orderbook:g%d:s%d:q%d",
		provenance.Generation, provenance.SubscriptionID, provenance.Sequence)
	out.quote = liveMirrorQuote{Price: noAsk, Depth: noAskDepth, Tick: tick, MinQty: 1,
		MinQtyKnown: true, SpreadCents: math.Max(0, (noAsk-noBid)*100), Maker: false,
		Route: "taker:inverse-kalshi-flow-shadow", BookSource: source,
		SourceAt: provenance.SourceAt, ObservedAt: provenance.ReceivedAt, CheckedAt: checkedAt}
	out.evidence = fullKalshiBookEvidence(book, provenance, checkedAt)
	out.fee, out.feeKnown, out.feeSource = s.kalFeeExactActionResident(ticker, false, 1, noAsk, true)
	if out.feeKnown {
		out.feeSource = "kalshi-exact-resident:" + strings.TrimSpace(out.feeSource)
	}
	return out, ""
}

func (s *Server) recordInverseKflowShadowBook(c liveMirrorCandidate, stage, outcome,
	reason string, book inverseKflowShadowBook) {
	event := s.executionShadowEvent(c.ShadowAttemptID, stage, outcome, reason, time.Now().UTC())
	if book.quote.Price > 0 {
		executionShadowApplyQuote(&event, book.quote, book.quote.CheckedAt)
		event.OriginalLimit = executionShadowPricePtr(book.quote.Price)
		event.RequestedQty = floatPtrShadow(1)
	}
	if book.feeKnown && strings.TrimSpace(book.feeSource) != "" {
		event.FeeQuote, event.FeeQuoteSource = executionShadowNonnegativePtr(book.fee), book.feeSource
	}
	event.Evidence = map[string]any{
		"model_version": inverseKflowShadowModelVersion,
		"selected_side": "NO", "requested_quantity": 1,
		"exact_q1_fee": book.fee, "exact_q1_fee_known": book.feeKnown,
		"exact_q1_fee_source": book.feeSource, "full_book": book.evidence,
		"real_money_authority": false, "cash_path_blocking": false,
	}
	s.enqueueExecutionShadowWrite(executionShadowWrite{event: &event})
}

func (s *Server) recordInverseKflowShadowTerminal(c liveMirrorCandidate, initial,
	final inverseKflowShadowBook, state, reason string, filled bool) {
	now := time.Now().UTC()
	event := s.executionShadowEvent(c.ShadowAttemptID, "counterfactual-execution-terminal",
		state, reason, now)
	stable := sha256.Sum256([]byte(strings.Join([]string{
		c.ShadowAttemptID, "counterfactual-execution-terminal", inverseKflowShadowModelVersion,
	}, "|")))
	event.EventID = "execev-stable-" + hex.EncodeToString(stable[:16])
	selected := final
	if selected.quote.Price <= 0 {
		selected = initial
	}
	if selected.quote.Price > 0 {
		executionShadowApplyQuote(&event, selected.quote, now)
	}
	if initial.quote.Price > 0 {
		event.OriginalLimit = executionShadowPricePtr(initial.quote.Price)
	}
	event.RequestedQty = floatPtrShadow(1)
	event.ShadowState, event.ShadowReason = state, reason
	if filled {
		event.ShadowFilledQty = floatPtrShadow(1)
		event.ShadowFillPrice = executionShadowPricePtr(final.quote.Price)
		if final.feeKnown && strings.TrimSpace(final.feeSource) != "" {
			event.ShadowFee, event.ShadowFeeSource = executionShadowNonnegativePtr(final.fee), final.feeSource
		}
	}
	event.Evidence = map[string]any{
		"experiment_kind":      inverseKflowShadowExperimentKind,
		"model_version":        inverseKflowShadowModelVersion,
		"cohort":               "forward raw event-driven Kalshi-flow YES; distinct from the historical cash-selected 316",
		"economic_episode_ms":  liveMirrorDedup.Milliseconds(),
		"scope":                "forward prior-free q1 two-touch IOC simulation only",
		"real_money_authority": false, "cash_path_blocking": false,
		"paper_portfolio_admission": false, "venue_post_possible": false,
		"original_emitted_side": "YES", "selected_inverse_side": "NO",
		"time_in_force": liveProspectiveIOC, "requested_quantity": 1,
		"limit_policy":             "first-current-full-ws-NO-ask-no-chase",
		"wire_delay_ms":            s.livePolicyMirrorFinalWireDelay().Milliseconds(),
		"trigger_at":               optionalExecutionShadowTimeText(c.At),
		"decision_emitted_at":      optionalExecutionShadowTimeText(c.At),
		"source_trade_received_at": optionalExecutionShadowTimeText(c.InputObservedAt),
		"initial_book":             initial.evidence, "final_book": final.evidence,
		"initial_exact_q1_fee": initial.fee, "initial_fee_known": initial.feeKnown,
		"initial_fee_source": initial.feeSource,
		"final_exact_q1_fee": final.fee, "final_fee_known": final.feeKnown,
		"final_fee_source": final.feeSource,
	}
	s.enqueueExecutionShadowWrite(executionShadowWrite{event: &event})
}

func (s *Server) runInverseKflowShadowExperiment(ctx context.Context, work livePolicyMirrorWork) {
	if !s.inverseKflowShadowEnabled() || work.ExperimentKind != inverseKflowShadowExperimentKind {
		return
	}
	// Restart-safe one-attempt-per-ticker episode. This query hits only execution_shadow.db.
	recent, err := s.store.HasRecentExecutionShadowAttempt(ctx, "kalshi", work.Signal.Ticker,
		"NO", "invert:kalshi-flow", inverseKflowShadowSignalSource,
		inverseKflowShadowQualification, work.SignalAt.Add(-liveMirrorDedup))
	if err == nil && recent {
		return
	}
	c, ok := s.beginInverseKflowShadowAttempt(work)
	if !ok {
		return
	}
	if err != nil {
		s.recordInverseKflowShadowTerminal(c, inverseKflowShadowBook{}, inverseKflowShadowBook{},
			livePolicyMirrorNotObserved, "inverse-shadow-durable-episode-check-unavailable", false)
		return
	}
	if !paperEntryHorizonValueKnown(work.Signal.ResolveHours) ||
		!s.paperEntryHorizonOK(work.Signal.ResolveHours, work.Signal.Ticker, work.Signal.Title) {
		s.recordInverseKflowShadowTerminal(c, inverseKflowShadowBook{}, inverseKflowShadowBook{},
			livePolicyMirrorNotObserved, "inverse-shadow-current-4h-or-crypto-2h-entry-window-failed", false)
		return
	}
	if !s.waitForLivePolicyMirrorExecutionTurn(ctx, work.DueAt) {
		s.recordInverseKflowShadowTerminal(c, inverseKflowShadowBook{}, inverseKflowShadowBook{},
			livePolicyMirrorNotObserved, "inverse-shadow-cash-priority-window-unavailable", false)
		return
	}
	if time.Since(work.DueAt) > livePolicyMirrorStartLateMax {
		s.recordInverseKflowShadowTerminal(c, inverseKflowShadowBook{}, inverseKflowShadowBook{},
			livePolicyMirrorNotObserved, "inverse-shadow-first-touch-started-over-500ms-late", false)
		return
	}
	initial, why := s.captureInverseKflowShadowBook(work.Signal.Ticker)
	if why != "" {
		s.recordInverseKflowShadowBook(c, "inverse-kflow-shadow-book-first", "unavailable", why, initial)
		s.recordInverseKflowShadowTerminal(c, initial, inverseKflowShadowBook{},
			livePolicyMirrorNotObserved, why, false)
		return
	}
	s.recordInverseKflowShadowBook(c, "inverse-kflow-shadow-book-first", "observed", "", initial)
	if !initial.feeKnown || strings.TrimSpace(initial.feeSource) == "" {
		s.recordInverseKflowShadowTerminal(c, initial, inverseKflowShadowBook{},
			livePolicyMirrorNotObserved, "inverse-shadow-exact-q1-fee-unavailable", false)
		return
	}
	if !waitLivePolicyMirror(ctx, s.livePolicyMirrorFinalWireDelay()) {
		s.recordInverseKflowShadowTerminal(c, initial, inverseKflowShadowBook{},
			livePolicyMirrorNotObserved, "inverse-shadow-wire-delay-canceled", false)
		return
	}
	final, why := s.captureInverseKflowShadowBook(work.Signal.Ticker)
	if why != "" {
		s.recordInverseKflowShadowBook(c, "inverse-kflow-shadow-book-second", "unavailable", why, final)
		s.recordInverseKflowShadowTerminal(c, initial, final, livePolicyMirrorNotObserved, why, false)
		return
	}
	s.recordInverseKflowShadowBook(c, "inverse-kflow-shadow-book-second", "observed", "", final)
	if !livePolicyMirrorBookUsableAtWire(initial.quote, final.quote) {
		s.recordInverseKflowShadowTerminal(c, initial, final, livePolicyMirrorNotObserved,
			"inverse-shadow-wire-book-provenance-regressed", false)
		return
	}
	filled, why := livePolicyMirrorIOC(initial.quote.Price, 1, final.quote)
	if why != "" || filled < 1 {
		s.recordInverseKflowShadowTerminal(c, initial, final, livePolicyMirrorModeledZeroFill,
			"inverse-shadow:"+why, false)
		return
	}
	if !final.feeKnown || strings.TrimSpace(final.feeSource) == "" {
		s.recordInverseKflowShadowTerminal(c, initial, final, livePolicyMirrorNotObserved,
			"inverse-shadow-final-exact-q1-fee-unavailable", false)
		return
	}
	s.recordInverseKflowShadowTerminal(c, initial, final, livePolicyMirrorModeledFill, "", true)
}
