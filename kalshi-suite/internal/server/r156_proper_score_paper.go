package server

// The Proper Betting Paper portfolios are the funded-simulation twins of the existing
// Brier/log/spherical research vectors. They reuse those already-evaluated forecasts and never
// create a prediction, score, paper_fills row, LIVE candidate, or venue order of their own.

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/properbetting"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

const (
	properScorePaperVectorPC = 0.05
)

type properScorePaperVector struct {
	transform  string
	rows       []properScorePrepared
	candidates []properScoreCandidate
	reasons    []string
}

type properScorePaperPlannedAttempt struct {
	id        int64
	row       properScorePrepared
	candidate properScoreCandidate
	quantity  float64
	limit     float64
	reserved  float64
	shadowID  string
	signal    storage.Signal
	executeAt time.Time
}

type properScorePaperDueMarket struct {
	book       properScoreBook
	capturedAt time.Time
	reason     string
	fee        properbetting.FeeFunc
	feeSource  string
	feeKnown   bool
}

type properScorePaperPreparedExecution struct {
	attempt properScorePaperPlannedAttempt
	outcome storage.ProperScorePaperOutcome
	book    storage.Signal
}

func properScorePaperInputTopology(platform, transform string) string {
	prefix := r147VenueCode(platform)
	transform = strings.ToUpper(strings.TrimSpace(transform))
	if (prefix != "K" && prefix != "PUS") ||
		(transform != "BRIER" && transform != "LOG" && transform != "SPHERICAL") {
		return ""
	}
	return prefix + "-PROPER-" + transform
}

// properScorePaperCanonicalSignal freezes the exact side book which admitted one funded
// coordinate. The scoring transform is part of the static input topology, so simultaneous Brier,
// log and spherical choices on the same market cannot deduplicate each other.
func (s *Server) properScorePaperCanonicalSignal(row properScorePrepared,
	c properScoreCandidate, transform string) (storage.Signal, bool) {
	topology := properScorePaperInputTopology(row.platform, transform)
	if topology == "" || (c.side != "YES" && c.side != "NO") ||
		strings.TrimSpace(row.book.sourceClock) == "" || row.book.sourceClockAt.IsZero() {
		return storage.Signal{}, false
	}
	bid, ask := row.book.yesBid, row.book.yesAsk
	bidDepth, askDepth := row.book.yesBidDepth, row.book.yesAskDepth
	if c.side == "NO" {
		bid, ask = row.book.noBid, row.book.noAsk
		bidDepth, askDepth = row.book.noBidDepth, row.book.noAskDepth
	}
	if bid <= 0 || ask <= bid || ask >= 1 || bidDepth <= 0 || askDepth <= 0 ||
		row.book.tick <= 0 || row.book.age < 0 {
		return storage.Signal{}, false
	}
	age, tick := row.book.age, row.book.tick
	sig := storage.Signal{Platform: row.platform, Ticker: row.forecast.Ticker,
		Title: row.forecast.Title, Side: c.side, SignalType: "proper-score-executor",
		EntryPrice: ask, ResolveHours: row.forecast.ResolveHours,
		SpreadCents: (ask - bid) * 100, BookDepth: askDepth,
		BookBid: &bid, BookAsk: &ask, BookBidDepth: &bidDepth, BookAskDepth: &askDepth,
		BookQuoteAgeS: &age, BookTakerTick: &tick, BookSource: row.book.source,
		ExecExpr: r147InputReceiptExpr(topology, row.book.sourceClockAt) +
			"/proper_transform=" + strings.ToLower(strings.TrimSpace(transform)),
		LabelVersion:   fundedPaperCorrectedExecutionGenerationV1,
		PricingVersion: fundedPaperCorrectedExecutionGenerationV1}
	if fee, _, known := s.fillFeeReceipt(row.platform, row.forecast.Ticker,
		false, 1, ask); known && fee >= 0 {
		sig.FeePC = &fee
	}
	return sig, true
}

func (s *Server) publishProperScorePaperOpportunity(sig storage.Signal, observed time.Time,
	shadowID string, requested, limit float64) {
	selectionReason := "proper-betting-funded-paper-lane-has-no-live-cash-authority"
	s.executionShadowPublishSignalDisposition(sig, 0, observed, shadowID,
		true, false, selectionReason)
	c := liveCandidateFromSignalIntent(liveSignalIntent{Signal: sig, At: observed,
		ShadowAttemptID: shadowID})
	if bound, why, declared := bindStaticTakerSignalCandidate(c, sig); declared && why == "" {
		c = bound
	}
	c.ProspectiveQty, c.ProspectiveTimeInForce = requested, liveProspectiveIOC
	event := s.executionShadowEvent(shadowID, "live-selection", "not-sent",
		selectionReason, observed)
	stable := sha256.Sum256([]byte(shadowID + "|live-selection|proper-score-paper-v1"))
	event.EventID = fmt.Sprintf("execev-stable-%x", stable[:16])
	event.LiveState, event.VenueAttempted, event.LiveAuthoritative = "not-sent", false, false
	event.RequestedQty, event.OriginalLimit = floatPtrShadow(requested), executionShadowPricePtr(limit)
	executionShadowApplySignalBook(&event, sig, observed)
	event.Evidence = map[string]any{"cash_authority": false,
		"venue_post_possible": false, "paper_only_portfolio": true,
		"proper_transform": strings.TrimPrefix(r147SignalInputTopology(sig),
			r147VenueCode(sig.Platform)+"-PROPER-")}
	s.enqueueExecutionShadowWrite(executionShadowWrite{event: &event})
	s.executionShadowRecordPaperFanout(shadowID, time.Now().UTC(), observed, sig)
	s.enqueueCanonicalSystemExecutionShadow(sig, observed, shadowID)
}

func (s *Server) recordProperScorePaperTerminal(attempt properScorePaperPlannedAttempt,
	outcome storage.ProperScorePaperOutcome, book storage.Signal) {
	state := "PAPER-" + strings.ToUpper(strings.ReplaceAll(outcome.State, "_", "-"))
	if outcome.State == "rejected" {
		state = "PAPER-REJECTED"
	}
	s.executionShadowRecordPaperAttempt(attempt.shadowID, state, outcome.Reason,
		fmt.Sprintf("proper-score-paper:%d", attempt.id), outcome.ProcessedAt,
		attempt.quantity, attempt.limit, outcome.FilledQty, outcome.FillPrice,
		outcome.FeeUSD, outcome.FeeSource, book)
}

func properScorePaperAttemptKey(slot, transform string, row properScorePrepared,
	side, forecastSource, forecastVersion string) string {
	raw := strings.Join([]string{"proper-betting-paper-v1", slot, transform, row.platform,
		row.forecast.Ticker, side, forecastSource, forecastVersion}, "\x00")
	return fmt.Sprintf("%x", sha256.Sum256([]byte(raw)))
}

func properScorePaperSideLevels(row properScorePrepared, side string) ([]properbetting.Level, float64) {
	if strings.EqualFold(side, "NO") {
		return append([]properbetting.Level(nil), row.book.noAsks...), 1 - row.pYes
	}
	return append([]properbetting.Level(nil), row.book.yesAsks...), row.pYes
}

// properScorePaperSizedBuy converts one already-normalized scoring coordinate into whole venue
// lots under its dollar share of the vector budget. Full depth and the venue's aggregate-order fee
// are priced together; a large order is reduced until both the dollar rail and positive net edge
// hold.
func (s *Server) properScorePaperSizedBuy(row properScorePrepared, c properScoreCandidate,
	budget, requestedCap, limit float64) (properbetting.DepthEvaluation, string, bool) {
	var zero properbetting.DepthEvaluation
	levels, forecastSide := properScorePaperSideLevels(row, c.side)
	if len(levels) == 0 || budget <= 0 || row.book.lot <= 0 || row.book.tick <= 0 {
		return zero, "", false
	}
	probe := levels[0].Price
	feeFn, feeSource, known := s.properScoreCurveFeeAction(row.platform,
		row.forecast.Ticker, "taker", probe, true)
	return properScorePaperSizedBuyWithFee(row, levels, forecastSide, budget, requestedCap,
		limit, feeFn, feeSource, known)
}

func properScorePaperSizedBuyWithFee(row properScorePrepared, levels []properbetting.Level,
	forecastSide, budget, requestedCap, limit float64,
	feeFn properbetting.FeeFunc, feeSource string, feeKnown bool,
) (properbetting.DepthEvaluation, string, bool) {
	var zero properbetting.DepthEvaluation
	if len(levels) == 0 || budget <= 0 || row.book.lot <= 0 || row.book.tick <= 0 ||
		!feeKnown || feeFn == nil || strings.TrimSpace(feeSource) == "" {
		return zero, feeSource, false
	}
	levels = append([]properbetting.Level(nil), levels...)
	if limit > 0 {
		kept := levels[:0]
		for _, level := range levels {
			if level.Price <= limit+1e-9 {
				kept = append(kept, level)
			}
		}
		levels = kept
		if len(levels) == 0 {
			return zero, "", false
		}
	}
	probe := levels[0].Price
	quantity := math.Floor((budget/probe+1e-12)/row.book.lot) * row.book.lot
	if requestedCap > 0 {
		quantity = math.Min(quantity, requestedCap)
		quantity = math.Floor((quantity+1e-12)/row.book.lot) * row.book.lot
	}
	for attempt := 0; attempt < 16 && quantity >= row.book.lot-1e-9; attempt++ {
		fill, err := properbetting.WalkDepth(levels, quantity, forecastSide,
			row.book.tick, row.book.lot, feeFn)
		if err == nil && fill.Filled > 0 && fill.IntegratedCost+fill.Fee <= budget+1e-9 &&
			fill.ExpectedNet > 0 {
			return fill, feeSource, true
		}
		next := quantity - row.book.lot
		if err == nil && fill.IntegratedCost+fill.Fee > budget && fill.IntegratedCost+fill.Fee > 0 {
			next = math.Floor((quantity*budget/(fill.IntegratedCost+fill.Fee)+1e-12)/
				row.book.lot) * row.book.lot
			if next >= quantity-1e-9 {
				next = quantity - row.book.lot
			}
		} else if err == nil && fill.ExpectedNet <= 0 {
			next = math.Floor((quantity/2+1e-12)/row.book.lot) * row.book.lot
		}
		quantity = next
	}
	return zero, feeSource, false
}

func properScorePaperLimit(fill properbetting.DepthEvaluation) float64 {
	limit := 0.0
	for _, level := range fill.Fills {
		limit = math.Max(limit, level.Price)
	}
	return limit
}

// A delayed Paper IOC cannot claim liquidity from the exact snapshot that created the decision.
// Without a newer venue receipt, the suite does not know whether that displayed size survived the
// wire delay. Failing closed here makes the portfolio conservative instead of inventing a fill.
func properScorePaperExecutionBookNewer(decision, execution properScoreBook) bool {
	return strings.TrimSpace(decision.sourceClock) != "" &&
		strings.TrimSpace(execution.sourceClock) != "" &&
		!decision.sourceClockAt.IsZero() &&
		!execution.sourceClockAt.IsZero() &&
		execution.sourceClockAt.After(decision.sourceClockAt)
}

func cloneProperScorePaperBook(book properScoreBook) properScoreBook {
	book.yesBids = append([]properbetting.Level(nil), book.yesBids...)
	book.yesAsks = append([]properbetting.Level(nil), book.yesAsks...)
	book.noBids = append([]properbetting.Level(nil), book.noBids...)
	book.noAsks = append([]properbetting.Level(nil), book.noAsks...)
	return book
}

func properScorePaperDueMarketKey(attempt properScorePaperPlannedAttempt) string {
	return strings.Join([]string{
		strings.ToLower(strings.TrimSpace(attempt.row.platform)),
		strings.TrimSpace(attempt.row.forecast.Ticker),
		attempt.executeAt.UTC().Format(time.RFC3339Nano),
	}, "\x00")
}

func (s *Server) claimProperScorePaperVector(ctx context.Context, observed time.Time, slot string,
	vector properScorePaperVector, forecastSource, forecastVersion string,
	delay time.Duration) ([]properScorePaperPlannedAttempt, error) {
	if len(vector.rows) != len(vector.candidates) || len(vector.rows) != len(vector.reasons) {
		return nil, fmt.Errorf("Proper Betting Paper %s vector dimension mismatch", vector.transform)
	}
	type ranked struct {
		index int
		score float64
	}
	var order []ranked
	for i, c := range vector.candidates {
		if vector.reasons[i] == "" && c.normalized > 0 && c.expected > 0 &&
			(c.side == "YES" || c.side == "NO") {
			order = append(order, ranked{index: i, score: c.normalized * c.expected})
		}
	}
	sort.SliceStable(order, func(i, j int) bool {
		if math.Abs(order[i].score-order[j].score) > 1e-12 {
			return order[i].score > order[j].score
		}
		left, right := vector.rows[order[i].index], vector.rows[order[j].index]
		if left.platform != right.platform {
			return left.platform < right.platform
		}
		return left.forecast.Ticker < right.forecast.Ticker
	})
	_, laneEquity, err := s.store.ProperScorePaperLaneSizing(ctx, vector.transform)
	if err != nil {
		return nil, err
	}
	vectorBudget := properScorePaperVectorPC * laneEquity
	if vectorBudget <= 0 {
		return nil, nil
	}
	planned := make([]properScorePaperPlannedAttempt, 0, len(order))
	for _, rank := range order {
		i := rank.index
		row, c := vector.rows[i], vector.candidates[i]
		coordinateBudget := vectorBudget * c.normalized
		decisionFill, feeSource, ok := s.properScorePaperSizedBuy(row, c,
			coordinateBudget, 0, 0)
		if !ok || decisionFill.Filled <= 0 {
			continue
		}
		limit := properScorePaperLimit(decisionFill)
		if limit <= 0 {
			continue
		}
		identity, identityOK, err := s.store.CurrentCanonicalInstrument(ctx,
			row.platform, row.forecast.Ticker)
		if err != nil {
			return planned, err
		}
		if !identityOK || strings.TrimSpace(identity.EventID) == "" {
			continue
		}
		sig, signalOK := s.properScorePaperCanonicalSignal(row, c, vector.transform)
		if !signalOK || !s.claimCanonicalSignalOpportunity(sig, observed) {
			continue
		}
		shadowID := s.executionShadowSignalAttemptID(sig, 0, observed)
		if shadowID == "" {
			continue
		}
		reserved := decisionFill.IntegratedCost + decisionFill.Fee
		attemptID, claimed, claimReason, err := s.store.ClaimProperScorePaperAttempt(ctx,
			storage.ProperScorePaperAttempt{
				AttemptKey: properScorePaperAttemptKey(slot, vector.transform, row, c.side,
					forecastSource, forecastVersion),
				Lane: vector.transform, Transform: vector.transform, Slot: slot,
				Platform: row.platform, Ticker: row.forecast.Ticker,
				EventKey: identity.EventID, Title: row.forecast.Title, Side: c.side,
				ObservedAt: observed, ExecuteAfter: observed.Add(delay),
				ForecastYes: row.pYes, NormalizedWeight: c.normalized,
				VectorBudgetUSD: vectorBudget, CoordinateBudgetUSD: coordinateBudget,
				LimitPrice: limit, RequestedQty: decisionFill.Filled,
				ReservationUSD: reserved, FeeSource: feeSource,
				BookSource: row.book.source, SourceClockID: row.book.sourceClock,
				ExecutionShadowAttemptID: shadowID,
				DelayMS:                  int(delay / time.Millisecond),
			})
		if err != nil {
			return planned, err
		}
		attempt := properScorePaperPlannedAttempt{id: attemptID, row: row, candidate: c,
			quantity: decisionFill.Filled, limit: limit, reserved: reserved,
			shadowID: shadowID, signal: sig, executeAt: observed.Add(delay)}
		if claimed {
			s.publishProperScorePaperOpportunity(sig, observed, shadowID,
				decisionFill.Filled, limit)
			planned = append(planned, attempt)
		} else if claimReason == "duplicate-attempt" {
			continue
		} else if outcome, found, outcomeErr := s.store.ProperScorePaperOutcomeForAttempt(ctx,
			attemptID); outcomeErr != nil {
			return planned, outcomeErr
		} else if found {
			// A portfolio conflict/cap refusal is still an exact detector opportunity. Duplicate
			// attempts have no newly inserted outcome and therefore do not mint a second sample.
			s.publishProperScorePaperOpportunity(sig, observed, shadowID,
				decisionFill.Filled, limit)
			s.recordProperScorePaperTerminal(attempt, outcome, sig)
		}
	}
	return planned, nil
}

func (s *Server) completeProperScorePaperZero(ctx context.Context,
	attemptID int64, reason string) error {
	return s.store.CompleteProperScorePaperAttempt(ctx, storage.ProperScorePaperOutcome{
		AttemptID: attemptID, ProcessedAt: time.Now().UTC(), State: "zero_fill", Reason: reason,
	})
}

// prepareProperScorePaperDueExecutions freezes one due-time market receipt per
// venue/ticker/schedule before any portfolio outcome is written. Brier, log and spherical can
// retain independent attempts, quantities and fees without letting their fixed processing order
// choose three different books. All fill economics are also computed here, before sequential
// persistence can make a later lane miss the 500 ms start window.
func (s *Server) prepareProperScorePaperDueExecutions(ctx context.Context,
	planned []properScorePaperPlannedAttempt,
	now func() time.Time,
	bookLookup func(context.Context, string, string) (properScoreBook, bool),
	horizonCheck func(context.Context, string, string, string) bool,
) []properScorePaperPreparedExecution {
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	if bookLookup == nil {
		bookLookup = s.properScoreCurrentBook
	}
	if horizonCheck == nil {
		horizonCheck = s.paperEntryHorizonNow
	}

	dueMarkets := make(map[string]properScorePaperDueMarket, len(planned))
	for _, attempt := range planned {
		key := properScorePaperDueMarketKey(attempt)
		if _, found := dueMarkets[key]; found {
			continue
		}
		capturedAt := now().UTC()
		receipt := properScorePaperDueMarket{capturedAt: capturedAt}
		switch {
		case properScorePaperExecutionStartedLate(attempt.executeAt, capturedAt):
			receipt.reason = "proper-scheduled-touch-started-over-500ms-late"
		case !horizonCheck(ctx, attempt.row.platform, attempt.row.forecast.Ticker,
			attempt.row.forecast.Title):
			receipt.reason = "execution-time-remaining-outside-entry-horizon"
		default:
			book, ok := bookLookup(ctx, attempt.row.platform, attempt.row.forecast.Ticker)
			if !ok || !book.completeBinarySet() || strings.TrimSpace(book.sourceClock) == "" {
				receipt.reason = "execution-book-incomplete-or-stale-after-delay"
			} else {
				receipt.book = cloneProperScorePaperBook(book)
				receipt.fee, receipt.feeSource, receipt.feeKnown =
					s.properScoreCurveFeeAction(attempt.row.platform,
						attempt.row.forecast.Ticker, "taker", receipt.book.yesAsk, true)
			}
		}
		dueMarkets[key] = receipt
	}

	prepared := make([]properScorePaperPreparedExecution, 0, len(planned))
	for _, attempt := range planned {
		receipt := dueMarkets[properScorePaperDueMarketKey(attempt)]
		outcome := storage.ProperScorePaperOutcome{
			AttemptID: attempt.id, ProcessedAt: receipt.capturedAt,
			State: "zero_fill", Reason: receipt.reason,
		}
		executionSignal := attempt.signal
		if outcome.Reason == "" && !properScorePaperExecutionBookNewer(
			attempt.row.book, receipt.book) {
			outcome.Reason = "execution-book-has-no-newer-venue-receipt-after-delay"
		}
		if outcome.Reason == "" {
			row := attempt.row
			row.book = cloneProperScorePaperBook(receipt.book)
			levels, forecastSide := properScorePaperSideLevels(row, attempt.candidate.side)
			fill, feeSource, ok := properScorePaperSizedBuyWithFee(row, levels, forecastSide,
				attempt.reserved, attempt.quantity, attempt.limit,
				receipt.fee, receipt.feeSource, receipt.feeKnown)
			switch {
			case !ok || fill.Filled <= 0:
				outcome.Reason = "execution-no-positive-fill-at-original-limit-after-delay"
			case fill.IntegratedCost+fill.Fee > attempt.reserved+1e-9:
				outcome.Reason = "execution-fee-inclusive-reservation-exceeded"
			default:
				outcome.State, outcome.Reason = "filled", ""
				outcome.FilledQty, outcome.FillPrice = fill.Filled, fill.AveragePrice
				outcome.CostUSD, outcome.FeeUSD = fill.IntegratedCost, fill.Fee
				outcome.FeeSource = feeSource
				outcome.BookSource, outcome.SourceClockID =
					receipt.book.source, receipt.book.sourceClock
				transform := strings.TrimPrefix(r147SignalInputTopology(attempt.signal),
					r147VenueCode(row.platform)+"-PROPER-")
				if sig, signalOK := s.properScorePaperCanonicalSignal(
					row, attempt.candidate, transform); signalOK {
					executionSignal = sig
				}
			}
		}
		prepared = append(prepared, properScorePaperPreparedExecution{
			attempt: attempt, outcome: outcome, book: executionSignal,
		})
	}
	return prepared
}

func (s *Server) completeProperScorePaperPreparedExecution(ctx context.Context,
	prepared properScorePaperPreparedExecution) error {
	if err := s.store.CompleteProperScorePaperAttempt(ctx, prepared.outcome); err != nil {
		return err
	}
	s.recordProperScorePaperTerminal(prepared.attempt, prepared.outcome, prepared.book)
	return nil
}

func properScorePaperExecutionStartedLate(executeAt, startedAt time.Time) bool {
	return !executeAt.IsZero() && !startedAt.IsZero() &&
		startedAt.After(executeAt.Add(livePolicyMirrorStartLateMax))
}

// executeProperScorePaperBatch drains every claimed reservation even if one attempt encounters a
// persistence/check error. The first error remains the telemetry headline; cleanup failures are
// joined behind it. A detached, bounded terminal write prevents caller cancellation from leaving
// the current attempt pending, and later attempts are still executed or terminalized in order.
func executeProperScorePaperBatch(ctx context.Context, planned []properScorePaperPlannedAttempt,
	execute func(context.Context, properScorePaperPlannedAttempt) error,
	terminalize func(context.Context, int64, string) error) error {
	var firstErr error
	var cleanupErrs []error
	for _, attempt := range planned {
		reason := ""
		if err := ctx.Err(); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			reason = "execution-context-ended-before-durable-outcome"
		} else if err := execute(ctx, attempt); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			reason = "execution-check-failed-before-durable-outcome"
		}
		if reason == "" {
			continue
		}
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		err := terminalize(cleanupCtx, attempt.id, reason)
		cancel()
		if err != nil {
			cleanupErrs = append(cleanupErrs,
				fmt.Errorf("terminalize Proper Betting Paper attempt %d: %w", attempt.id, err))
		}
	}
	if firstErr == nil {
		return errors.Join(cleanupErrs...)
	}
	return errors.Join(append([]error{firstErr}, cleanupErrs...)...)
}

// runProperScorePaperVectors claims all three isolated lanes from one completed immutable vector,
// waits once, freezes one due-time book per market, and computes every lane's economics before any
// outcome is persisted. Attempts, cash, positions, conflicts and history remain lane-local.
func (s *Server) runProperScorePaperVectors(ctx context.Context, observed time.Time, slot string,
	vectors []properScorePaperVector, forecastSource, forecastVersion string) error {
	// Sizing, three-lane claims, the delayed execution check, and an operator reset must observe
	// one epoch. Without this lock a reset between sizing and claim could attach an old budget to
	// a fresh epoch even though SQLite correctly isolated the stored rows.
	s.properScorePaperMu.Lock()
	defer s.properScorePaperMu.Unlock()
	if _, err := s.store.RecoverProperScorePaperAttempts(ctx, observed); err != nil {
		return err
	}
	if len(vectors) == 0 {
		return nil
	}
	delay := s.genfollowPaperTakerDelay()
	var planned []properScorePaperPlannedAttempt
	for _, vector := range vectors {
		rows, err := s.claimProperScorePaperVector(ctx, observed, slot, vector,
			forecastSource, forecastVersion, delay)
		if err != nil {
			return err
		}
		planned = append(planned, rows...)
	}
	if len(planned) == 0 {
		return nil
	}
	byID := make(map[int64]properScorePaperPlannedAttempt, len(planned))
	for _, attempt := range planned {
		byID[attempt.id] = attempt
	}
	terminalize := func(cleanupCtx context.Context, attemptID int64, reason string) error {
		if err := s.store.TerminalizeProperScorePaperAttemptFailure(cleanupCtx,
			attemptID, reason); err != nil {
			return err
		}
		outcome, found, err := s.store.ProperScorePaperOutcomeForAttempt(cleanupCtx, attemptID)
		if err != nil || !found {
			return err
		}
		if attempt, ok := byID[attemptID]; ok {
			s.recordProperScorePaperTerminal(attempt, outcome, attempt.signal)
		}
		return nil
	}
	if !waitForGenfollowPaperDue(ctx, observed.Add(delay)) {
		return executeProperScorePaperBatch(ctx, planned,
			func(context.Context, properScorePaperPlannedAttempt) error {
				return ctx.Err()
			}, terminalize)
	}
	prepared := s.prepareProperScorePaperDueExecutions(ctx, planned, nil, nil, nil)
	preparedByID := make(map[int64]properScorePaperPreparedExecution, len(prepared))
	for _, execution := range prepared {
		preparedByID[execution.attempt.id] = execution
	}
	return executeProperScorePaperBatch(ctx, planned,
		func(executionCtx context.Context, attempt properScorePaperPlannedAttempt) error {
			execution, ok := preparedByID[attempt.id]
			if !ok {
				return fmt.Errorf("Proper Betting Paper due-time execution missing for attempt %d",
					attempt.id)
			}
			return s.completeProperScorePaperPreparedExecution(executionCtx, execution)
		}, terminalize)
}
