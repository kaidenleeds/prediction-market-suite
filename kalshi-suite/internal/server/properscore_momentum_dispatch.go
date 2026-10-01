package server

// Sealed Paper -> armed LIVE parity for one Kalshi proper-score momentum reduction. Only a single
// one-contract SELL is supported. Crosses and PolyUS are structurally refused because the verified
// retail routes cannot provide an atomic, per-leg, all-or-none receipt.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/paper"
	"github.com/kalshi-suite/kalshi-suite/internal/properbetting"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

const properMomentumLiveDisabledReason = "momentum-sell-live-disabled-pending-immutable-trigger-lease"

// properMomentumLiveSellEnabled keeps the already-written dispatch path compile- and test-visible
// while the immutable trigger lease remains unfinished. A function gate avoids unreachable code;
// changing LIVE authority still requires an explicit source edit, test pass, build and ARM cycle.
func properMomentumLiveSellEnabled() bool { return false }

func (s *Server) properMomentumPaperSnapshot(ctx context.Context, in storage.ProperMomentumSellIntent) (
	lastFillID int64, title, openSource, blocker string) {
	fills, err := s.store.ListPaperFills(ctx)
	if err != nil {
		return 0, "", "", "paper-position-read-unavailable"
	}
	open, exact := storage.ExactProperMomentumPaperOpen(fills, in.Candidate.Ticker, in.Candidate.OwnedSide)
	lastFillID, title, openSource = open.LastFillID, open.Title, open.Source
	if !exact || math.Abs(math.Abs(in.Candidate.RequiredPrior)-1) > 1e-8 ||
		((strings.EqualFold(in.Candidate.OwnedSide, "YES")) != (in.Candidate.RequiredPrior > 0)) {
		return lastFillID, title, openSource, "paper-position-does-not-exactly-match-one-promoted-lot"
	}
	opened, accepted := s.researchPromotionAccepted(ctx, openSource)
	if !accepted || opened.Proof.SystemID != "proper-score-executor" ||
		opened.Candidate.Venue != "kalshi" || opened.Candidate.Ticker != in.Candidate.Ticker ||
		!strings.EqualFold(opened.Candidate.Side, in.Candidate.OwnedSide) {
		return lastFillID, title, openSource, "paper-position-is-not-an-accepted-proper-score-open"
	}
	return lastFillID, title, openSource, ""
}

func (s *Server) properMomentumCurrentSale(ctx context.Context, ticker, ownedSide string) (
	price, fee, depth, tick, age float64, feeSource, bookSource, sourceClock string,
	sourceClockAt time.Time, fills []properbetting.Fill, blocker string) {
	book, ok := s.properScoreCurrentBook(ctx, "kalshi", ticker)
	if !ok || !book.completeBinarySet() || strings.TrimSpace(book.sourceClock) == "" {
		blocker = "current-complete-kalshi-book-clock-unavailable"
		return
	}
	levels := book.yesBids
	forecastSide := .5
	if strings.EqualFold(ownedSide, "NO") {
		levels = book.noBids
	}
	if len(levels) == 0 {
		blocker = "current-owned-side-bid-depth-unavailable"
		return
	}
	feeFn, source, known := s.properScoreCurveFeeAction("kalshi", ticker, "taker", levels[0].Price, false)
	if !known {
		blocker = "current-exact-kalshi-sell-fee-unavailable:" + source
		return
	}
	walk, err := properbetting.WalkSaleDepth(levels, 1, forecastSide, book.tick, book.lot, feeFn)
	if err != nil || !walk.FullFill || math.Abs(walk.Filled-1) > 1e-8 || walk.Cancelled > 1e-8 {
		blocker = "current-sell-route-is-not-full-one-share"
		return
	}
	for _, level := range levels {
		depth += level.Quantity
	}
	return walk.IntegratedValue, walk.Fee, depth, walk.MinimumTick, book.age, source, book.source,
		book.sourceClock, book.sourceClockAt, append([]properbetting.Fill(nil), walk.Fills...), ""
}

func properMomentumPaperBookIsNewer(candidate storage.ProperMomentumSellCandidate,
	clock string, clockAt time.Time) bool {
	return strings.TrimSpace(candidate.SourceClockID) != "" && strings.TrimSpace(clock) != "" &&
		clock != candidate.SourceClockID && !candidate.Observed.IsZero() && !clockAt.IsZero() &&
		clockAt.After(candidate.Observed)
}

func properMomentumExecutionBookIsNewer(in storage.ProperMomentumSellIntent,
	clock string, clockAt time.Time) bool {
	return strings.TrimSpace(in.Preprice.SourceClockID) != "" && strings.TrimSpace(clock) != "" &&
		clock != in.Preprice.SourceClockID && !in.Preprice.SourceClockAt.IsZero() &&
		!clockAt.IsZero() && clockAt.After(in.Preprice.SourceClockAt)
}

func properMomentumInputTopology(candidate storage.ProperMomentumSellCandidate) (string, bool) {
	transform, ok := properScoreTransformFromCohort(candidate.Cohort)
	if !ok {
		return "K-PROPER-TRANSFORM-AMBIGUOUS", false
	}
	return "K-PROPER-" + strings.ToUpper(transform), true
}

func properMomentumCanonicalAt(in storage.ProperMomentumSellIntent) time.Time {
	if !in.Candidate.Observed.IsZero() {
		return in.Candidate.Observed.UTC()
	}
	if !in.Created.IsZero() {
		return in.Created.UTC()
	}
	// Test-only malformed fixtures still need a stable, valid clock. Production intents always
	// persist Created, and recovered intents rehydrate the immutable research observation clock.
	return time.Unix(1, 0).UTC()
}

func properMomentumCanonicalAttemptID(in storage.ProperMomentumSellIntent) string {
	at := properMomentumCanonicalAt(in)
	sum := sha256.Sum256([]byte(strings.Join([]string{
		in.IntentID, "proper-score-momentum", "kalshi", in.Candidate.Ticker,
		strings.ToUpper(in.Candidate.OwnedSide), "SELL", "taker", "reduce_only_fok",
	}, "|")))
	return fmt.Sprintf("exec-%d-%s", at.UnixNano(), hex.EncodeToString(sum[:8]))
}

func properMomentumStableEventID(attemptID, stage string) string {
	sum := sha256.Sum256([]byte(strings.Join([]string{
		attemptID, stage, "proper-score-momentum-sell-v1",
	}, "|")))
	return "execev-stable-" + hex.EncodeToString(sum[:16])
}

func (s *Server) properMomentumCanonicalCandidate(in storage.ProperMomentumSellIntent) liveMirrorCandidate {
	at := properMomentumCanonicalAt(in)
	topology, _ := properMomentumInputTopology(in.Candidate)
	contract := ""
	if registered, ok := r147ProperMomentumSignalContract(in.Candidate.OwnedSide, topology); ok {
		contract = r147SignalContractIdentity(registered)
	}
	price := in.Candidate.ObservedSellPrice
	if price <= 0 || price >= 1 {
		price = in.PaperSellPrice
	}
	return liveMirrorCandidate{
		Platform: "kalshi", Ticker: in.Candidate.Ticker, Title: in.Candidate.Title,
		Side: in.Candidate.OwnedSide, Action: "SELL", Family: "proper-score-momentum",
		Source: in.SourceID, Price: price, At: at,
		ShadowAttemptID: properMomentumCanonicalAttemptID(in), InputTopology: topology,
		SignalContractID: contract, InputObservedAt: at, ProspectiveQty: 1,
		ProspectiveTimeInForce: liveProspectiveFOK,
		ProspectivePlanReason:  "typed one-contract reduce-only SELL must fill all or none",
	}
}

// ensureProperMomentumCanonicalAttempt creates one immutable SELL opportunity. The observed book
// is the research candidate's owned-side bid; it is never relabelled as a BUY ask. Stable attempt
// and event ids make crash reconciliation idempotent rather than counting one intent twice.
func (s *Server) ensureProperMomentumCanonicalAttempt(in storage.ProperMomentumSellIntent) liveMirrorCandidate {
	c := s.properMomentumCanonicalCandidate(in)
	qualification := "typed proper-score momentum SELL with immutable action and transform cohort"
	if _, exact := properMomentumInputTopology(in.Candidate); !exact {
		qualification = "typed proper-score momentum SELL; transform cohort ambiguous; log-only"
	}
	c, attempt, ok := s.executionShadowBuildCandidate(c, qualification, c.ShadowAttemptID)
	if !ok {
		return c
	}
	s.executionShadowRememberTrigger(c.ShadowAttemptID, attempt.TriggerUnixMS)
	s.noteR147SignalContractRuntime("proper-score-momentum", "kalshi", c.Side, "taker",
		c.InputTopology, c.Ticker, "proper-score-momentum-typed-sell", "signal", "", "",
		c.At, true, "")
	event := s.executionShadowEvent(c.ShadowAttemptID, "detector-branch", "qualified",
		qualification, c.At)
	event.EventID = properMomentumStableEventID(c.ShadowAttemptID, "detector-branch")
	if strings.TrimSpace(in.Candidate.SourceClockID) != "" {
		event.BookSource = "sealed-proper-score-bid:" + in.Candidate.SourceClockID
	}
	event.BookReceivedAt = c.At
	event.SideBid = executionShadowPricePtr(c.Price)
	event.VisibleDepth = floatPtrShadow(1) // candidate SQL proves visible_capacity >= 1
	event.OriginalLimit = executionShadowPricePtr(c.Price)
	event.RequestedQty = floatPtrShadow(1)
	if fee := executionShadowNonnegativePtr(in.Candidate.ObservedSellFee); fee != nil {
		event.FeeQuote = fee
		event.FeeQuoteSource = "sealed-proper-score-observation-exact-sell-fee"
	}
	event.Evidence = map[string]any{
		"action": "SELL", "route": "taker", "time_in_force": "fill_or_kill",
		"reduce_only": true, "owned_side_bid": true, "visible_depth_lower_bound": 1,
		"source_clock_id": in.Candidate.SourceClockID, "candidate_cohort": in.Candidate.Cohort,
		"real_money_authority": false,
	}
	s.r168StampForwardDetectorEvent(&event, attempt,
		r168ForwardDetectorModelVersion+"/proper-score-momentum-sell-fok")
	s.enqueueExecutionShadowWriteGroup([]executionShadowWrite{{attempt: &attempt}, {event: &event}}, true)
	return c
}

func (s *Server) recordProperMomentumFundedPaperBranch(c liveMirrorCandidate, at time.Time) {
	event := s.executionShadowEvent(c.ShadowAttemptID, "funded-paper-branch", "offered",
		"typed SELL offered to Paper only after a newer complete owned-side bid book", at)
	event.EventID = properMomentumStableEventID(c.ShadowAttemptID, "funded-paper-branch")
	event.Evidence = map[string]any{"action": "SELL", "reduce_only": true,
		"time_in_force": "fill_or_kill", "cash_path_blocking": false}
	s.enqueueExecutionShadowWrite(executionShadowWrite{event: &event})
}

func (s *Server) recordProperMomentumPaperTerminal(c liveMirrorCandidate,
	state storage.ProperMomentumSellPaperState, receipt *storage.ProperMomentumSellBookReceipt,
	prepriceClock string, prepricePrice float64, delayMS int64, reason string, at time.Time) {
	paperState, outcome := "PAPER-REJECTED", "rejected"
	if state.State == "filled" {
		paperState, outcome = "PAPER-FILLED", "filled"
	}
	event := s.executionShadowEvent(c.ShadowAttemptID, "funded-paper-terminal", outcome, reason, at)
	event.EventID = properMomentumStableEventID(c.ShadowAttemptID, "funded-paper-terminal")
	event.PaperAttemptID, event.PaperState = c.Source, paperState
	event.RequestedQty = floatPtrShadow(1)
	event.OriginalLimit = executionShadowPricePtr(prepricePrice)
	if event.OriginalLimit == nil {
		event.OriginalLimit = executionShadowPricePtr(c.Price)
	}
	if receipt != nil {
		event.PaperBookSource, event.BookSource = receipt.BookSource, receipt.BookSource
		event.BookSourceAt, event.BookReceivedAt = receipt.SourceClockAt, receipt.ObservedAt
		event.SideBid = executionShadowPricePtr(receipt.Price)
		event.VisibleDepth = executionShadowNonnegativePtr(receipt.VisibleDepth)
		if receipt.TickSize > 0 {
			event.TickSize = floatPtrShadow(receipt.TickSize)
		}
		if fee := executionShadowNonnegativePtr(receipt.Fee); fee != nil && receipt.FeeSource != "" {
			event.FeeQuote, event.FeeQuoteSource = fee, receipt.FeeSource
		}
	}
	if state.State == "filled" {
		event.PaperFilledQty = floatPtrShadow(1)
		event.PaperFillPrice = executionShadowPricePtr(state.SellPrice)
		event.PaperFee = executionShadowNonnegativePtr(state.Fee)
		if event.PaperFee != nil && receipt != nil && receipt.FeeSource != "" {
			event.PaperFeeSource = receipt.FeeSource
		}
	}
	event.Evidence = map[string]any{"action": "SELL", "reduce_only": true,
		"time_in_force": "fill_or_kill", "same_snapshot_execution_allowed": false,
		"execution_receipt_available": receipt != nil, "configured_delay_ms": delayMS}
	if receipt != nil {
		event.Evidence["source_clock_id"] = receipt.SourceClockID
		event.Evidence["preprice_source_clock_id"] = prepriceClock
	}
	s.enqueueExecutionShadowWrite(executionShadowWrite{event: &event})
}

func (s *Server) recordProperMomentumShadowUnavailable(c liveMirrorCandidate, at time.Time) {
	const reason = "independent reduce-only SELL/FOK bid-route shadow is not implemented; BUY/IOC shadow is inapplicable"
	event := s.executionShadowEvent(c.ShadowAttemptID, "counterfactual-execution-terminal",
		"not_observed", reason, at)
	event.EventID = properMomentumStableEventID(c.ShadowAttemptID, "counterfactual-execution-terminal")
	event.ShadowState, event.ShadowReason = "not_observed", reason
	event.OriginalLimit = executionShadowPricePtr(c.Price)
	event.RequestedQty = floatPtrShadow(1)
	event.Evidence = map[string]any{"action": "SELL", "reduce_only": true,
		"time_in_force": "fill_or_kill", "real_money_authority": false,
		"cash_path_blocking": false, "buy_ioc_simulator_used": false}
	s.enqueueExecutionShadowWrite(executionShadowWrite{event: &event})
}

func (s *Server) recordProperMomentumLiveNoSend(c liveMirrorCandidate, reason string, at time.Time) {
	event := s.executionShadowEvent(c.ShadowAttemptID, "live-selection", "rejected", reason, at)
	event.EventID = properMomentumStableEventID(c.ShadowAttemptID, "live-selection")
	event.LiveState, event.VenueAttempted, event.LiveAuthoritative = "not-sent", false, false
	event.OriginalLimit = executionShadowPricePtr(c.Price)
	event.RequestedQty = floatPtrShadow(1)
	event.Evidence = map[string]any{"action": "SELL", "reduce_only": true,
		"time_in_force": "fill_or_kill", "venue_called": false}
	s.enqueueExecutionShadowWrite(executionShadowWrite{event: &event})
}

func (s *Server) executeProperMomentumPaperSell(ctx context.Context,
	in storage.ProperMomentumSellIntent) (bool, string, *storage.ProperMomentumSellBookReceipt) {
	if in.ExecutionDelayMS <= 0 || in.ExecuteAfter.IsZero() {
		return false, "paper-sell-preprice-delay-provenance-unavailable", nil
	}
	if time.Now().UTC().Before(in.ExecuteAfter) {
		return false, "paper-sell-configured-delay-not-elapsed", nil
	}
	lastID, title, openSource, blocker := s.properMomentumPaperSnapshot(ctx, in)
	if blocker != "" {
		return false, blocker, nil
	}
	price, fee, depth, tick, _, feeSource, bookSource, clock, clockAt, _, blocker := s.properMomentumCurrentSale(ctx,
		in.Candidate.Ticker, in.Candidate.OwnedSide)
	if blocker != "" {
		return false, blocker, nil
	}
	receipt := &storage.ProperMomentumSellBookReceipt{ObservedAt: time.Now().UTC(),
		SourceClockAt: clockAt, BookSource: bookSource, SourceClockID: clock,
		FeeSource: feeSource, VisibleDepth: depth, TickSize: tick, Price: price, Fee: fee,
		RequestedQty: 1, State: "rejected"}
	if !properMomentumExecutionBookIsNewer(in, clock, clockAt) {
		receipt.Reason = "execution-book-is-not-newer-than-persisted-preprice-receipt"
		return false, receipt.Reason, receipt
	}
	if (1-price)+fee > in.MaxSyntheticAllIn+1e-9 {
		receipt.Reason = "current-paper-sell-synthetic-all-in-erases-sealed-edge"
		return false, receipt.Reason, receipt
	}
	receipt.State, receipt.Reason = "filled", "newer complete bid book filled one reduce-only SELL"
	_, inserted, err := s.store.InsertProperMomentumPaperSellWithReceipt(ctx, paper.Fill{Platform: "kalshi",
		Ticker: in.Candidate.Ticker, Title: firstNonEmpty(title, in.Candidate.Title),
		Side: in.Candidate.OwnedSide, Action: "SELL", Price: price, Contracts: 1, Fee: fee,
		Source: in.SourceID, Note: "sealed proper-score momentum reduce-only SELL @ " + clock,
		FillKind: "taker", RouteReason: "sealed-proper-score-momentum-reduce-only"},
		in.Candidate.RequiredPrior, lastID, openSource, *receipt)
	if err != nil {
		return false, "paper-sell-storage-error", nil
	}
	if !inserted {
		receipt.State, receipt.Reason = "rejected", "paper-position-changed-before-atomic-sell"
		return false, receipt.Reason, receipt
	}
	return true, "", receipt
}

func (s *Server) properMomentumLiveCandidate(in storage.ProperMomentumSellIntent,
	state storage.ProperMomentumSellPaperState, now time.Time) liveMirrorCandidate {
	c := s.properMomentumCanonicalCandidate(in)
	c.Source, c.Price = in.SourceID, state.SellPrice
	c.LiveIntentGeneration = s.liveSignalIntentGeneration.Load()
	c.LiveIntentGenerationBound = true
	return c
}

func (s *Server) reconcileProperMomentumSellIntent(ctx context.Context, now time.Time,
	in storage.ProperMomentumSellIntent, receipt *storage.CollectorReceipt) {
	state, err := s.store.ProperMomentumSellPaperState(ctx, in, in.Created.Add(-time.Second))
	if err != nil {
		receipt.Exclusions["momentum_paper_receipt_error"]++
		return
	}
	c := s.ensureProperMomentumCanonicalAttempt(in)
	s.recordProperMomentumFundedPaperBranch(c, in.Created)
	var executionReceipt *storage.ProperMomentumSellBookReceipt
	if state.State == "missing" {
		if in.ExecutionDelayMS <= 0 || in.ExecuteAfter.IsZero() {
			if now.Sub(in.Created) < 30*time.Second {
				receipt.Metrics["momentum_legacy_intent_waiting_fail_closed"] =
					metricInt(receipt.Metrics["momentum_legacy_intent_waiting_fail_closed"]) + 1
				return
			}
			why := "legacy-intent-lacks-persisted-preprice-clock-and-configured-delay"
			_ = s.store.AppendProperMomentumPaperRejected(context.WithoutCancel(ctx), in, 0, 0, why, nil)
			state.State, state.Reason = "terminal_rejected", why
		} else if now.Before(in.ExecuteAfter) {
			receipt.Metrics["momentum_paper_configured_delay_warming"] =
				metricInt(receipt.Metrics["momentum_paper_configured_delay_warming"]) + 1
			return
		} else if placed, why, observed := s.executeProperMomentumPaperSell(ctx, in); !placed {
			executionReceipt = observed
			price, fee := 0.0, 0.0
			if observed != nil {
				price, fee = observed.Price, observed.Fee
			}
			if err := s.store.AppendProperMomentumPaperRejected(context.WithoutCancel(ctx), in,
				price, fee, why, observed); err != nil {
				receipt.Exclusions["momentum_paper_reject_persistence_error"]++
				return
			}
			state = storage.ProperMomentumSellPaperState{State: "terminal_rejected",
				Reason: why, SellPrice: price, Fee: fee}
		} else {
			executionReceipt = observed
			state, err = s.store.ProperMomentumSellPaperState(ctx, in, in.Created.Add(-time.Second))
			if err != nil || state.State != "filled" {
				receipt.Exclusions["momentum_paper_fill_receipt_missing"]++
				return
			}
		}
	}
	if executionReceipt == nil {
		if stored, found, readErr := s.store.ProperMomentumSellExecutionReceipt(ctx,
			in.IntentID); readErr == nil && found {
			executionReceipt = &stored
		}
	}
	switch state.State {
	case "filled":
		if err := s.store.AppendProperMomentumSellEvent(context.WithoutCancel(ctx), in, "paper_accepted",
			state.SellPrice, state.Fee, "atomic Paper position check accepted identical one-share SELL"); err != nil {
			receipt.Exclusions["momentum_paper_accept_rejected"]++
			return
		}
		liveCandidate := s.properMomentumLiveCandidate(in, state, now)
		if !properMomentumLiveSellEnabled() {
			_ = s.store.AppendProperMomentumSellEvent(context.WithoutCancel(ctx), in, "live_rejected",
				0, 0, properMomentumLiveDisabledReason)
		} else {
			s.enqueueLiveMirrorCandidate(liveCandidate)
		}
		receipt.Inserted++
	case "terminal_rejected":
		_ = s.store.AppendProperMomentumSellEvent(context.WithoutCancel(ctx), in, "live_rejected",
			0, 0, "Paper SELL rejected before LIVE handoff")
		receipt.Exclusions["momentum_paper_terminal_rejected"]++
	}
	if durable, stateErr := s.store.ProperMomentumSellEventState(ctx, in.IntentID); stateErr == nil {
		s.replayProperMomentumCanonicalIntent(in, durable, executionReceipt, now)
	} else {
		receipt.Exclusions["momentum_canonical_durable_event_read_error"]++
	}
}

func properMomentumCanonicalViewComplete(view storage.ExecutionShadowAttemptView) bool {
	want := map[string]bool{"detector-branch": false, "funded-paper-branch": false,
		"funded-paper-terminal": false, "counterfactual-execution-terminal": false,
		"live-selection": false}
	for _, event := range view.Events {
		if _, ok := want[event.Stage]; ok {
			want[event.Stage] = true
		}
	}
	for _, seen := range want {
		if !seen {
			return false
		}
	}
	return view.Attempt.AttemptID != ""
}

func (s *Server) replayProperMomentumCanonicalIntent(in storage.ProperMomentumSellIntent,
	state storage.ProperMomentumSellEventState, executionReceipt *storage.ProperMomentumSellBookReceipt,
	_ time.Time) {
	c := s.ensureProperMomentumCanonicalAttempt(in)
	s.recordProperMomentumFundedPaperBranch(c, in.Created)
	paperAt := state.PaperAt
	if paperAt.IsZero() && executionReceipt != nil {
		paperAt = executionReceipt.ObservedAt
	}
	if paperAt.IsZero() {
		paperAt = in.ExecuteAfter
		if paperAt.IsZero() {
			paperAt = in.Created
		}
		if paperAt.IsZero() {
			paperAt = c.At
		}
	}
	paperState := storage.ProperMomentumSellPaperState{State: "terminal_rejected",
		Reason: state.PaperReason, SellPrice: state.PaperPrice, Fee: state.PaperFee}
	if state.PaperEvent == "paper_accepted" {
		paperState.State = "filled"
	}
	s.recordProperMomentumPaperTerminal(c, paperState, executionReceipt,
		in.Preprice.SourceClockID, in.Preprice.Price, in.ExecutionDelayMS,
		firstNonEmpty(state.PaperReason, state.PaperEvent), paperAt)
	s.recordProperMomentumShadowUnavailable(c, paperAt)
	if state.LiveEvent == "live_rejected" || (state.LiveEvent == "" && !properMomentumLiveSellEnabled()) {
		liveAt := state.LiveAt
		if liveAt.IsZero() {
			liveAt = paperAt
		}
		s.recordProperMomentumLiveNoSend(c, firstNonEmpty(state.LiveReason,
			properMomentumLiveDisabledReason), liveAt)
	}
}

// recoverProperMomentumCanonicalOutbox treats the main strategy database as the durable outbox.
// Stable attempt/event ids make replay harmless. A crash after the Paper/live event but before the
// isolated async writer commits can therefore reconstruct every branch on the next collector pass.
func (s *Server) recoverProperMomentumCanonicalOutbox(ctx context.Context, now time.Time,
	receipt *storage.CollectorReceipt) {
	rows, err := s.store.CompletedProperMomentumSellIntents(ctx)
	if err != nil {
		receipt.Exclusions["momentum_canonical_recovery_read_error"]++
		return
	}
	ids := make([]string, len(rows))
	for i := range rows {
		ids[i] = properMomentumCanonicalAttemptID(rows[i])
	}
	views := make(map[string]storage.ExecutionShadowAttemptView, len(ids))
	for start := 0; start < len(ids); start += storage.ExecutionShadowExactIDLookupMax {
		end := min(start+storage.ExecutionShadowExactIDLookupMax, len(ids))
		batch, readErr := s.store.ExecutionShadowAttemptsByIDs(ctx, ids[start:end])
		if readErr != nil {
			receipt.Exclusions["momentum_canonical_shadow_read_error"]++
			return
		}
		for id, view := range batch {
			views[id] = view
		}
	}
	replayed := 0
	for i, in := range rows {
		if properMomentumCanonicalViewComplete(views[ids[i]]) {
			continue
		}
		state, stateErr := s.store.ProperMomentumSellEventState(ctx, in.IntentID)
		if stateErr != nil || state.PaperEvent == "" {
			continue
		}
		var executionReceipt *storage.ProperMomentumSellBookReceipt
		if stored, found, readErr := s.store.ProperMomentumSellExecutionReceipt(ctx,
			in.IntentID); readErr == nil && found {
			executionReceipt = &stored
		}
		s.replayProperMomentumCanonicalIntent(in, state, executionReceipt, now)
		replayed++
	}
	receipt.Metrics["momentum_canonical_replayed"] = replayed
}

func (s *Server) reconcileProperMomentumSellIntents(ctx context.Context, now time.Time,
	receipt *storage.CollectorReceipt) {
	rows, err := s.store.UnresolvedProperMomentumSellIntents(ctx, 100)
	if err != nil {
		receipt.Exclusions["momentum_intent_storage_error"]++
		return
	}
	receipt.Metrics["momentum_unresolved_intents"] = len(rows)
	for _, in := range rows {
		s.reconcileProperMomentumSellIntent(ctx, now, in, receipt)
	}
	pendingLive, err := s.store.PendingProperMomentumLiveSellIntents(ctx, 100)
	if err != nil {
		receipt.Exclusions["momentum_live_handoff_storage_error"]++
		return
	}
	receipt.Metrics["momentum_pending_live_handoffs"] = len(pendingLive)
	for _, in := range pendingLive {
		state, stateErr := s.store.ProperMomentumSellPaperState(ctx, in, in.Created.Add(-time.Second))
		if stateErr != nil || state.State != "filled" {
			receipt.Exclusions["momentum_accepted_paper_receipt_unavailable"]++
			continue
		}
		var executionReceipt *storage.ProperMomentumSellBookReceipt
		if stored, found, readErr := s.store.ProperMomentumSellExecutionReceipt(ctx,
			in.IntentID); readErr == nil && found {
			executionReceipt = &stored
		}
		liveCandidate := s.properMomentumLiveCandidate(in, state, now)
		if !properMomentumLiveSellEnabled() {
			_ = s.store.AppendProperMomentumSellEvent(context.WithoutCancel(ctx), in, "live_rejected",
				0, 0, properMomentumLiveDisabledReason)
		} else {
			s.enqueueLiveMirrorCandidate(liveCandidate)
		}
		if durable, stateErr := s.store.ProperMomentumSellEventState(ctx, in.IntentID); stateErr == nil {
			s.replayProperMomentumCanonicalIntent(in, durable, executionReceipt, now)
		}
	}
	s.recoverProperMomentumCanonicalOutbox(ctx, now, receipt)
}

func (s *Server) tryProperMomentumSellPromotion(ctx context.Context, now time.Time,
	proof storage.ResearchPromotionProof, floor float64, receipt *storage.CollectorReceipt) {
	candidate, ok, err := s.store.LatestProperMomentumSellCandidate(ctx, proof, now.Add(-20*time.Second))
	if err != nil || !ok {
		receipt.Exclusions["no_fresh_typed_momentum_sell_candidate"]++
		return
	}
	if _, _, _, blocker := s.properMomentumPaperSnapshot(ctx,
		storage.ProperMomentumSellIntent{Candidate: candidate}); blocker != "" {
		receipt.Exclusions["momentum_exact_paper_position_unavailable"]++
		return
	}
	// Price the exact Paper sale before freezing the intent. Paper execution rechecks again and the
	// storage transaction rechecks the unchanged position/fill boundary.
	price, fee, depth, tick, _, feeSource, bookSource, clock, clockAt, _, blocker := s.properMomentumCurrentSale(ctx,
		candidate.Ticker, candidate.OwnedSide)
	if blocker != "" {
		receipt.Exclusions["momentum_sell_money_truth_unavailable"]++
		return
	}
	// The research observation's book selected the action. Paper may mutate P&L only after a
	// genuinely later complete bid-book receipt; re-reading the same cached frame is not execution.
	if !properMomentumPaperBookIsNewer(candidate, clock, clockAt) {
		receipt.Exclusions["momentum_sell_waiting_newer_complete_bid_book"]++
		return
	}
	delay := s.genfollowPaperTakerDelay()
	preprice := storage.ProperMomentumSellBookReceipt{ObservedAt: now,
		SourceClockAt: clockAt, BookSource: bookSource, SourceClockID: clock,
		FeeSource: feeSource, VisibleDepth: depth, TickSize: tick, Price: price, Fee: fee,
		RequestedQty: 1, State: "rejected", Reason: "persisted preprice; not an execution receipt"}
	in, inserted, err := s.store.InsertProperMomentumSellIntent(ctx, proof, candidate,
		preprice, floor, delay)
	if err != nil {
		receipt.Exclusions["momentum_sell_sealed_contract_rejected"]++
		return
	}
	if !inserted {
		receipt.Duplicates++
		return
	}
	receipt.Attempted++
	// The durable intent is the delay queue. Reconciliation may execute it only at ExecuteAfter
	// against a source-clock-strictly-newer complete bid frame.
	s.reconcileProperMomentumSellIntent(ctx, now, in, receipt)
}

func (s *Server) exactKalshiMomentumLivePosition(ctx context.Context,
	in storage.ProperMomentumSellIntent) (string, float64) {
	if s.kal == nil {
		return "kalshi-account-client-unavailable", 0
	}
	var positions []kalshi.MarketPosition
	var orders []kalshi.Order
	var posErr, orderErr error
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); positions, posErr = s.kal.GetPositions(ctx) }()
	go func() { defer wg.Done(); orders, orderErr = s.kal.GetOrders(ctx) }()
	wg.Wait()
	if posErr != nil {
		return "kalshi-position-read-unavailable", 0
	}
	if orderErr != nil {
		return "kalshi-order-read-unavailable", 0
	}
	return properMomentumKalshiPositionBlocker(in, positions, orders)
}

func properMomentumKalshiPositionBlocker(in storage.ProperMomentumSellIntent,
	positions []kalshi.MarketPosition, orders []kalshi.Order) (string, float64) {
	if math.Abs(math.Abs(in.Candidate.RequiredPrior)-1) > 1e-8 ||
		((in.Candidate.OwnedSide == "YES") != (in.Candidate.RequiredPrior > 0)) {
		return "invalid-live-reduce-only-prior", 0
	}
	for _, order := range orders {
		if strings.EqualFold(order.Ticker, in.Candidate.Ticker) {
			return "existing-kalshi-resting-order", 0
		}
	}
	position := 0.0
	for _, row := range positions {
		if strings.EqualFold(row.Ticker, in.Candidate.Ticker) {
			position = row.PositionQty()
			break
		}
	}
	if math.Abs(position-in.Candidate.RequiredPrior) > 1e-8 {
		return "live-position-does-not-exactly-match-typed-prior", position
	}
	return "", position
}

func properMomentumKalshiFOKRequest(in storage.ProperMomentumSellIntent, salePrice float64) (kalshi.OrderRequest, error) {
	if (in.Candidate.OwnedSide != "YES" && in.Candidate.OwnedSide != "NO") || salePrice <= 0 || salePrice >= 1 ||
		math.Abs(math.Abs(in.Candidate.RequiredPrior)-1) > 1e-8 ||
		((in.Candidate.OwnedSide == "YES") != (in.Candidate.RequiredPrior > 0)) {
		return kalshi.OrderRequest{}, fmt.Errorf("invalid momentum SELL wire contract")
	}
	bookSide, wirePrice := "ask", salePrice
	if in.Candidate.OwnedSide == "NO" {
		bookSide, wirePrice = "bid", 1-salePrice
	}
	return kalshi.OrderRequest{Ticker: in.Candidate.Ticker,
		ClientOrderID: idemKey("psms-", in.IntentID), Side: bookSide, Count: "1",
		Price: kalshi.FmtPx(wirePrice), TimeInForce: "fill_or_kill",
		SelfTradePreventionType: "taker_at_cross", ReduceOnly: true, CancelOrderOnPause: true}, nil
}

func properMomentumFOKReceiptState(res *kalshi.CreateOrderResult) (
	state string, authoritative bool, filled, remaining float64) {
	if res == nil {
		return "ambiguous", false, 0, 0
	}
	filled, fillErr := strconv.ParseFloat(strings.TrimSpace(res.FillCount), 64)
	remaining, remainErr := strconv.ParseFloat(strings.TrimSpace(res.RemainingCount), 64)
	state, authoritative = liveKalshiCreateReceiptState(
		liveProspectiveFOK, 1, filled, remaining, fillErr == nil && remainErr == nil)
	return state, authoritative, filled, remaining
}

func properMomentumFullFOKReceipt(res *kalshi.CreateOrderResult) bool {
	state, authoritative, filled, remaining := properMomentumFOKReceiptState(res)
	return state == "full" && authoritative &&
		math.Abs(filled-1) <= 1e-8 && math.Abs(remaining) <= 1e-8
}

func properMomentumExpectedPost(prior float64, ownedSide string) float64 {
	if strings.EqualFold(ownedSide, "YES") {
		return prior - 1
	}
	return prior + 1
}

func properMomentumResidentSaleQuote(book r159KalshiExecutableBook, bookWhy string) (
	liveMirrorQuote, string) {
	if bookWhy != "" {
		return liveMirrorQuote{}, bookWhy
	}
	if book.MakerPrice <= 0 || book.MakerPrice >= 1 || book.MakerDepth <= 0 ||
		book.MakerTick <= 0 || strings.TrimSpace(book.Source) == "" ||
		book.ReceivedAt.IsZero() {
		return liveMirrorQuote{}, "momentum-sell-final-resident-book-incomplete"
	}
	return liveMirrorQuote{
		Price: book.MakerPrice, Depth: book.MakerDepth, Tick: book.MakerTick,
		Route: "reduce_only_fok", BookSource: book.Source,
		SourceAt: book.SourceAt, ObservedAt: book.ReceivedAt, CheckedAt: time.Now(),
	}, ""
}

func (s *Server) rejectProperMomentumNoSend(ctx context.Context,
	in storage.ProperMomentumSellIntent, riskID, claimKey, source, reason, code string,
	evidence any) (bool, string) {
	if err := s.r148RiskCleanRejected(context.WithoutCancel(ctx), riskID, "reduce",
		source, reason, evidence); err != nil {
		s.setLiveAutoSafetyPause(liveSafetyPausePendingRisk,
			"proper momentum known no-send could not release durable risk: "+err.Error())
		_ = s.store.AppendProperMomentumSellEvent(context.WithoutCancel(ctx), in,
			"live_ambiguous", 0, 0, reason+"; durable risk close failed: "+err.Error())
		return false, code + ":risk-release-failed"
	}
	if err := s.store.KVDel(context.WithoutCancel(ctx), claimKey); err != nil {
		_ = s.store.AppendProperMomentumSellEvent(context.WithoutCancel(ctx), in,
			"live_rejected", 0, 0, reason+"; durable claim cleanup failed: "+err.Error())
		return false, code + ":claim-release-failed"
	}
	_ = s.store.AppendProperMomentumSellEvent(context.WithoutCancel(ctx), in,
		"live_rejected", 0, 0, reason)
	return false, code
}

func (s *Server) dispatchProperMomentumLiveSell(ctx context.Context, c liveMirrorCandidate,
	in storage.ProperMomentumSellIntent) (dispatched bool, dispatchReason string) {
	shadowQuote := liveMirrorQuote{Price: c.Price, Route: "reduce_only_fok"}
	shadowRiskID, shadowOrderID := "", ""
	shadowState, shadowReceiptSource := "rejected-before-venue", "proper-momentum-dispatch"
	shadowFilled, shadowFillPrice, shadowFee := 0.0, 0.0, 0.0
	shadowFeeKnown, shadowAuthoritative, shadowVenueAttempted := false, true, false
	defer func() {
		s.executionShadowRecordLiveResult(c, shadowQuote, shadowRiskID, shadowOrderID,
			shadowState, shadowReceiptSource, 1, shadowFilled, shadowFillPrice, shadowFee,
			shadowFeeKnown, shadowAuthoritative, shadowVenueAttempted, dispatchReason,
			map[string]any{"action": "SELL", "reduce_only": true,
				"time_in_force": "fill_or_kill", "proper_momentum_intent_id": in.IntentID})
	}()
	// Paper keeps collecting this typed reduction. LIVE has no authority until the trigger's
	// original clock and AUTO generation are frozen once, then carried unchanged through retries.
	// Keep this refusal before live-state, account, risk, claim, book, or venue work.
	if !properMomentumLiveSellEnabled() {
		return false, properMomentumLiveDisabledReason
	}

	// The implementation below stays parked for evidence review and a future immutable-lease
	// enablement. The explicit false gate above is the only current LIVE authority decision.
	if !s.liveMirrorEnabled() || c.Action != "SELL" || c.Platform != "kalshi" ||
		!strings.EqualFold(c.Side, in.Candidate.OwnedSide) || time.Since(c.At) > liveMirrorTTL {
		return false, "momentum-sell-live-contract-mismatch"
	}
	if why := s.liveIntentGenerationGateReason(true, c.LiveIntentGeneration,
		c.LiveIntentGenerationBound, true); why != "" {
		return false, why
	}
	if why := r159LiveSubmitBoundaryReason(ctx, true, c.At.UnixMilli(), time.Now()); why != "" {
		return false, why
	}
	s.livePlaceMu.Lock()
	defer s.livePlaceMu.Unlock()
	if why, _ := s.exactKalshiMomentumLivePosition(ctx, in); why != "" {
		return false, why
	}
	price, fee, depth, tick, _, feeSource, bookSource, clock, _, _, blocker := s.properMomentumCurrentSale(ctx,
		in.Candidate.Ticker, in.Candidate.OwnedSide)
	if blocker != "" || depth < 1 || tick <= 0 || feeSource == "" || bookSource == "" || clock == "" {
		return false, firstNonEmpty(blocker, "momentum-sell-current-money-truth-incomplete")
	}
	observedAt, _ := time.Parse(time.RFC3339Nano, clock)
	shadowQuote = liveMirrorQuote{Price: price, Depth: depth, Tick: tick,
		Route: "reduce_only_fok", BookSource: bookSource,
		SourceAt: observedAt, ObservedAt: observedAt}
	if (1-price)+fee > in.MaxSyntheticAllIn+1e-9 {
		return false, "momentum-sell-current-synthetic-all-in-erases-sealed-edge"
	}
	if !s.liveMirrorEnabled() { // recheck after account/book I/O; toggles and kill switch are live state.
		return false, "auto-live-off-before-momentum-sell-claim"
	}
	if why := s.liveIntentGenerationGateReason(true, c.LiveIntentGeneration,
		c.LiveIntentGenerationBound, true); why != "" {
		return false, why
	}
	if why := r159LiveSubmitBoundaryReason(ctx, true, c.At.UnixMilli(), time.Now()); why != "" {
		return false, why
	}
	req, reqErr := properMomentumKalshiFOKRequest(in, price)
	if reqErr != nil {
		_ = s.store.AppendProperMomentumSellEvent(context.WithoutCancel(ctx), in, "live_rejected", 0, 0,
			"typed reduce-only FOK wire contract invalid")
		return false, "momentum-sell-wire-contract-invalid"
	}
	if why := s.liveContractLimitReason(1); why != "" {
		_ = s.store.AppendProperMomentumSellEvent(context.WithoutCancel(ctx), in, "live_rejected", 0, 0, why)
		return false, "momentum-sell-contract-cap"
	}
	riskID, riskErr := s.r148ReserveSingleRisk(ctx, r148SingleRiskRequest{
		Product: "reduce", Venue: "kalshi", Ticker: in.Candidate.Ticker,
		Side: in.Candidate.OwnedSide, Action: "SELL", Route: "reduce_only_fok",
		DispatchSource: "dispatchProperMomentumLiveSell", SystemID: "proper-score-momentum",
		Quantity: 1, LimitPrice: price, PrincipalUSD: 0, FeeUSD: fee,
		ClientOrderID: req.ClientOrderID, SourceIntentID: in.IntentID, AttemptNonce: req.ClientOrderID,
		ExecutionShadowAttemptID: c.ShadowAttemptID, RequireExecutionShadowAttemptID: true,
		Proof: map[string]any{"max_synthetic_all_in": in.MaxSyntheticAllIn,
			"required_prior": in.Candidate.RequiredPrior, "source_clock": clock},
	})
	if riskErr != nil {
		return false, "momentum-sell-risk-reservation-failed:" + riskErr.Error()
	}
	shadowRiskID = riskID
	claimKey := "proper_momentum_live_claim:" + in.IntentID
	if _, exists := s.store.KVGet(ctx, claimKey); exists {
		_ = s.r148ReleaseRisk(context.WithoutCancel(ctx), riskID, "proper-momentum-pre-submit",
			"durable submit claim already exists; venue was not called", map[string]any{"claim_key": claimKey})
		_ = s.store.AppendProperMomentumSellEvent(context.WithoutCancel(ctx), in, "live_ambiguous", 0, 0,
			"durable submit claim exists without a LIVE receipt; execution state is unknowable after interruption")
		return false, "duplicate-or-unpersisted-momentum-sell-claim"
	}
	if err := s.store.KVSet(ctx, claimKey, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		_ = s.r148ReleaseRisk(context.WithoutCancel(ctx), riskID, "proper-momentum-pre-submit",
			"durable claim persistence failed; venue was not called", map[string]any{"error": err.Error()})
		return false, "momentum-sell-claim-persistence-failed"
	}
	if !s.liveMirrorEnabled() {
		_ = s.r148ReleaseRisk(context.WithoutCancel(ctx), riskID, "proper-momentum-pre-submit",
			"LIVE AUTO disabled after durable claim; venue was not called", nil)
		_ = s.store.KVDel(context.WithoutCancel(ctx), claimKey)
		_ = s.store.AppendProperMomentumSellEvent(context.WithoutCancel(ctx), in, "live_rejected", 0, 0,
			"LIVE AUTO disabled after durable claim but before order submit")
		return false, "auto-live-off-after-momentum-sell-claim"
	}
	s.liveWriteFence.RLock()
	if why := s.liveWriteBoundaryReason(true); why != "" {
		s.liveWriteFence.RUnlock()
		_ = s.r148ReleaseRisk(context.WithoutCancel(ctx), riskID, "proper-momentum-pre-submit",
			"LIVE state changed before submit_started; venue was not called", map[string]any{"reason": why})
		_ = s.store.KVDel(context.WithoutCancel(ctx), claimKey)
		_ = s.store.AppendProperMomentumSellEvent(context.WithoutCancel(ctx), in, "live_rejected", 0, 0, why)
		return false, "momentum-sell-live-state-changed"
	}
	legIndex := 0
	if err := s.r148AppendRiskEvent(context.WithoutCancel(ctx), riskID, storage.LivePendingRiskSubmitStarted,
		"reduce", "SELL", req.ClientOrderID, "", "kalshi-proper-momentum-fok",
		"reduce-only FOK network mutation begins", &legIndex, 0, price, fee,
		map[string]any{"request": req, "execution_shadow_attempt_id": c.ShadowAttemptID}); err != nil {
		s.liveWriteFence.RUnlock()
		_ = s.r148ReleaseRisk(context.WithoutCancel(ctx), riskID, "proper-momentum-pre-submit",
			"submit_started could not persist; venue was not called", map[string]any{"error": err.Error()})
		_ = s.store.KVDel(context.WithoutCancel(ctx), claimKey)
		return false, "momentum-sell-submit-boundary-failed"
	}
	defer s.scheduleLivePendingRiskReconcile()
	if why := s.liveIntentGenerationGateReason(true, c.LiveIntentGeneration,
		c.LiveIntentGenerationBound, true); why != "" {
		s.liveWriteFence.RUnlock()
		return s.rejectProperMomentumNoSend(ctx, in, riskID, claimKey,
			"proper-momentum-wire-generation",
			"AUTO generation changed before reduce-only FOK; venue was not called",
			"momentum-sell-wire-generation-changed",
			map[string]any{"reason": why, "candidate_generation": c.LiveIntentGeneration,
				"current_generation": s.liveSignalIntentGeneration.Load(),
				"generation_bound":   c.LiveIntentGenerationBound})
	}
	if why := r159LiveSubmitBoundaryReason(ctx, true, c.At.UnixMilli(), time.Now()); why != "" {
		s.liveWriteFence.RUnlock()
		return s.rejectProperMomentumNoSend(ctx, in, riskID, claimKey,
			"proper-momentum-wire-signal-deadline",
			"signal deadline elapsed before reduce-only FOK; venue was not called",
			"momentum-sell-wire-signal-deadline",
			map[string]any{"reason": why, "trigger_unix_ms": c.At.UnixMilli(),
				"ttl_ms": liveMirrorTTL.Milliseconds()})
	}
	readBook := s.r159KalshiWSExecutableBook
	if s.kalshiExecutableBookFn != nil {
		readBook = s.kalshiExecutableBookFn
	}
	finalBook, finalBookWhy := readBook(c)
	finalQuote, finalBookWhy := properMomentumResidentSaleQuote(finalBook, finalBookWhy)
	if finalBookWhy == "" {
		finalBookWhy = liveMirrorHandlerQuoteChangeReasonForTIF(
			shadowQuote, finalQuote, 1, true)
	}
	if finalBookWhy != "" {
		checkedAt := time.Now()
		s.liveWriteFence.RUnlock()
		return s.rejectProperMomentumNoSend(ctx, in, riskID, claimKey,
			"proper-momentum-wire-book",
			"final resident full book changed before reduce-only FOK; venue was not called",
			"momentum-sell-wire-book-changed",
			map[string]any{"reason": finalBookWhy,
				"prior_quote": liveMirrorQuoteEvidence(shadowQuote, checkedAt),
				"wire_quote":  liveMirrorQuoteEvidence(finalQuote, checkedAt)})
	}
	shadowQuote = finalQuote
	s.invalidateR154KalshiAdmissionSnapshot()
	shadowVenueAttempted, shadowAuthoritative, shadowState =
		true, false, "ambiguous"
	res, err := s.kal.CreateOrder(ctx, req)
	s.invalidateR154KalshiAdmissionSnapshot()
	s.liveWriteFence.RUnlock()
	if err != nil {
		eventType := "live_rejected"
		if venueOutcomeAmbiguous(err) {
			eventType = "live_ambiguous"
			shadowState = "ambiguous"
			s.r148RiskAmbiguous(context.WithoutCancel(ctx), riskID, "reduce", "",
				"kalshi-proper-momentum-fok", "reduce-only FOK outcome is transport-ambiguous",
				map[string]any{"error": err.Error(), "request": req})
			_ = s.store.Audit(context.WithoutCancel(ctx), "critical", "live",
				"proper-score momentum SELL outcome ambiguous; intent remains permanently claimed",
				in.Candidate.Ticker+": "+err.Error())
		} else if riskEventErr := s.r148RiskCleanRejected(context.WithoutCancel(ctx), riskID, "reduce",
			"kalshi-proper-momentum-fok", "Kalshi cleanly rejected reduce-only FOK",
			map[string]any{"error": err.Error()}); riskEventErr != nil {
			s.setLiveAutoSafetyPause(liveSafetyPausePendingRisk,
				"proper momentum clean rejection could not release risk: "+riskEventErr.Error())
		} else {
			shadowState, shadowAuthoritative = "clean_rejected", true
		}
		_ = s.store.AppendProperMomentumSellEvent(context.WithoutCancel(ctx), in, eventType, 0, 0,
			"kalshi-fok-submit:"+err.Error())
		return false, "momentum-sell-live-handler:" + err.Error()
	}
	if res == nil || strings.TrimSpace(res.OrderID) == "" {
		shadowState = "ambiguous"
		s.r148RiskAmbiguous(context.WithoutCancel(ctx), riskID, "reduce", "",
			"kalshi-proper-momentum-fok", "reduce-only FOK acknowledgement lacks exact order id",
			map[string]any{"response": res})
		return false, "momentum-sell-order-id-unavailable"
	}
	shadowOrderID = res.OrderID
	if ackErr := s.r148AppendRiskEvent(context.WithoutCancel(ctx), riskID, storage.LivePendingRiskAck,
		"reduce", "SELL", req.ClientOrderID, res.OrderID, "kalshi-proper-momentum-fok",
		"exact reduce-only FOK acknowledgement", &legIndex, 0, price, 0,
		map[string]any{"response": res}); ackErr != nil {
		s.r148RiskAmbiguous(context.WithoutCancel(ctx), riskID, "reduce", "",
			"kalshi-proper-momentum-fok", "reduce-only FOK ACK could not persist",
			map[string]any{"error": ackErr.Error(), "response": res})
		return false, "momentum-sell-ack-persistence-failed"
	}
	s.refreshR154KalshiAdmissionSnapshotAsync()
	receiptState, receiptAuthoritative, filled, remaining := properMomentumFOKReceiptState(res)
	if receiptState != "full" || !receiptAuthoritative {
		if receiptState == "unfilled" && receiptAuthoritative &&
			liveKalshiTerminalZeroFill(receiptState, receiptAuthoritative, filled, remaining) {
			shadowState, shadowAuthoritative = "unfilled", true
			if riskErr := s.r148RiskTerminalUnfilled(context.WithoutCancel(ctx), riskID, "reduce",
				res.OrderID, "kalshi-proper-momentum-fok", "reduce-only FOK expired with zero fill",
				map[string]any{"response": res}); riskErr != nil {
				s.setLiveAutoSafetyPause(liveSafetyPausePendingRisk,
					"proper momentum zero-fill receipt could not release risk: "+riskErr.Error())
			}
			_ = s.store.AppendProperMomentumSellEvent(context.WithoutCancel(ctx), in, "live_rejected", 0, 0,
				"fill-or-kill returned an authoritative zero-fill receipt")
			return false, "momentum-sell-fok-unfilled"
		}
		partialWhy := liveProspectiveFOKPartialReason(
			liveProspectiveFOK, receiptState, filled, 1)
		if partialWhy != "" {
			s.setLiveAutoSafetyPause(liveSafetyPausePendingRisk,
				"proper-score momentum "+partialWhy)
		}
		shadowState, shadowAuthoritative = "ambiguous", false
		s.r148RiskAmbiguous(context.WithoutCancel(ctx), riskID, "reduce", res.OrderID,
			"kalshi-proper-momentum-fok", "reduce-only FOK returned a non-atomic receipt",
			map[string]any{"response": res, "classified_state": receiptState,
				"classified_authoritative": receiptAuthoritative, "partial_reason": partialWhy})
		_ = s.store.Audit(context.WithoutCancel(ctx), "critical", "live",
			"proper-score momentum FOK returned non-full receipt; route halted and intent remains claimed",
			fmt.Sprintf("ticker=%s fill=%q remaining=%q", in.Candidate.Ticker, res.FillCount, res.RemainingCount))
		_ = s.store.AppendProperMomentumSellEvent(context.WithoutCancel(ctx), in, "live_ambiguous", 0, 0,
			"fill-or-kill returned non-full receipt")
		return false, "momentum-sell-fok-non-full-receipt"
	}
	actualWire, err := strconv.ParseFloat(strings.TrimSpace(res.AverageFillPrice), 64)
	if err != nil || actualWire <= 0 || actualWire >= 1 {
		actualWire, _ = strconv.ParseFloat(req.Price, 64)
	}
	actualSale := actualWire
	if in.Candidate.OwnedSide == "NO" {
		actualSale = 1 - actualWire
	}
	shadowFilled, shadowFillPrice = 1, actualSale
	// The create receipt is the authoritative fee actually charged. AverageFeePaid is per filled
	// contract; this route is exactly one contract, so it is also the exact total fee.
	actualFee, feeErr := strconv.ParseFloat(strings.TrimSpace(res.AverageFeePaid), 64)
	if feeErr != nil || actualFee < 0 || math.IsNaN(actualFee) || math.IsInf(actualFee, 0) {
		shadowState, shadowAuthoritative = "filled_fee_unknown", false
		s.r148RiskAmbiguous(context.WithoutCancel(ctx), riskID, "reduce", res.OrderID,
			"kalshi-proper-momentum-fok", "filled reduce-only FOK lacks exact fee",
			map[string]any{"response": res})
		_ = s.store.AppendProperMomentumSellEvent(context.WithoutCancel(ctx), in, "live_ambiguous",
			actualSale, 0, "authoritative create-order average_fee_paid unavailable")
		return false, "momentum-sell-post-fill-fee-unavailable"
	}
	shadowFee, shadowFeeKnown = actualFee, true
	shadowState, shadowAuthoritative = "filled", true
	if fillEventErr := s.r148AppendRiskEvent(context.WithoutCancel(ctx), riskID,
		storage.LivePendingRiskFillSeen, "reduce", "SELL", req.ClientOrderID, res.OrderID,
		"kalshi-proper-momentum-fok", "authoritative full FOK fill observed", &legIndex,
		1, actualSale, actualFee, map[string]any{"response": res}); fillEventErr != nil {
		s.r148RiskAmbiguous(context.WithoutCancel(ctx), riskID, "reduce", res.OrderID,
			"kalshi-proper-momentum-fok", "full reduce-only FOK fill could not persist",
			map[string]any{"error": fillEventErr.Error(), "response": res})
		return false, "momentum-sell-fill-persistence-failed"
	}
	postRows, postErr := s.kal.GetPositions(ctx)
	postPosition := 0.0
	if postErr == nil {
		for _, row := range postRows {
			if strings.EqualFold(row.Ticker, in.Candidate.Ticker) {
				postPosition = row.PositionQty()
				break
			}
		}
	}
	wantPost := properMomentumExpectedPost(in.Candidate.RequiredPrior, in.Candidate.OwnedSide)
	if postErr != nil || math.Abs(postPosition-wantPost) > 1e-8 {
		s.r148RiskAmbiguous(context.WithoutCancel(ctx), riskID, "reduce", res.OrderID,
			"kalshi-proper-momentum-position", "filled reduce-only FOK is not visible in exact position state",
			map[string]any{"expected": wantPost, "observed": postPosition, "error": fmt.Sprint(postErr)})
		_ = s.store.Audit(context.WithoutCancel(ctx), "critical", "live",
			"proper-score momentum SELL filled but post-position could not be reconciled",
			fmt.Sprintf("ticker=%s want=%.8f got=%.8f err=%v", in.Candidate.Ticker, wantPost, postPosition, postErr))
		_ = s.store.AppendProperMomentumSellEvent(context.WithoutCancel(ctx), in, "live_ambiguous",
			actualSale, actualFee, "FOK fill receipt landed but exact post-position did not reconcile")
		return false, "momentum-sell-post-position-unreconciled"
	}
	if !s.r148RiskAccountVisible(ctx, riskID, res.OrderID, "kalshi-proper-momentum-position") {
		s.r148RiskAmbiguous(context.WithoutCancel(ctx), riskID, "reduce", res.OrderID,
			"kalshi-proper-momentum-position", "exact reduce-only fill could not close durable pending risk",
			map[string]any{"expected": wantPost, "observed": postPosition})
		return false, "momentum-sell-risk-release-failed"
	}
	if (1-actualSale)+actualFee > in.MaxSyntheticAllIn+1e-9 {
		_ = s.store.AppendProperMomentumSellEvent(context.WithoutCancel(ctx), in, "live_ambiguous",
			actualSale, actualFee, "venue fill exceeded sealed synthetic all-in ceiling")
		return false, "momentum-sell-post-fill-ceiling-breach"
	}
	s.liveRiskBook(math.Max(0, actualFee), "kalshi")
	_ = s.store.AppendProperMomentumSellEvent(context.WithoutCancel(ctx), in, "live_dispatched",
		actualSale, actualFee, "armed LIVE AUTO filled identical reduce-only FOK SELL")
	s.liveLogAdd(map[string]any{"event": "AUTO-LIVE-MOMENTUM-SELL", "venue": "kalshi",
		"ticker": in.Candidate.Ticker, "owned_side": in.Candidate.OwnedSide, "action": "SELL",
		"price_dollars": actualSale, "fee": actualFee, "count": 1, "reduce_only": true,
		"fee_source": "kalshi_create_order_average_fee_paid", "time_in_force": "fill_or_kill",
		"source_clock": clock, "proof": "sealed-paper-accepted"})
	s.liveInvalidate()
	return true, ""
}
