package server

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

const fundedPaperLiveTruthContractV1 = "durable-live-terminal-v1"

const (
	fundedPaperLiveTerminalNoSend    = "no-send"
	fundedPaperLiveTerminalFill      = "fill"
	fundedPaperLiveTerminalZeroFill  = "zero-fill"
	fundedPaperLiveTerminalAmbiguous = "ambiguous"
)

// fundedPaperLiveTerminalReceipt is the durable centralized LIVE outcome which authorizes a
// selected funded-Paper counterfactual. No-send and a real fill may be compared; a venue zero-fill
// or ambiguous outcome must fail closed before a Paper lot can exist.
type fundedPaperLiveTerminalReceipt struct {
	State, Kind, EventID string
	At                   time.Time
}

func fundedPaperLiveTerminalReceiptFromEvent(
	event storage.ExecutionShadowEvent) fundedPaperLiveTerminalReceipt {
	state := strings.ToLower(strings.TrimSpace(event.LiveState))
	out := fundedPaperLiveTerminalReceipt{
		State: state, Kind: fundedPaperLiveTerminalAmbiguous,
		EventID: strings.TrimSpace(event.EventID), At: event.At,
	}
	filled := 0.0
	if event.LiveFilledQty != nil {
		filled = *event.LiveFilledQty
	}
	switch {
	case state == "ambiguous" || state == "pending" || state == "unknown":
		// Quantity without authoritative price/fee/order identity is still ambiguous. Never let an
		// optimistic nonzero field turn an unresolved venue result into a Paper authorization.
	case !event.VenueAttempted &&
		(state == "not-sent" || state == "rejected" || state == "rejected-before-venue"):
		out.Kind = fundedPaperLiveTerminalNoSend
	case event.LiveAuthoritative && filled > 0 &&
		(state == "full" || state == "filled" || state == "partial"):
		out.Kind = fundedPaperLiveTerminalFill
	case event.VenueAttempted &&
		(state == "rejected" || state == "clean_rejected"):
		// A clean venue rejection cannot later become a fill. It is economically the same terminal
		// zero as an empty IOC, but remains labelled separately in State.
		out.Kind = fundedPaperLiveTerminalZeroFill
	case event.VenueAttempted && event.LiveAuthoritative &&
		(state == "unfilled" || state == "terminal_unfilled" ||
			state == "cancelled" || state == "canceled"):
		out.Kind = fundedPaperLiveTerminalZeroFill
	}
	return out
}

func (s *Server) fundedPaperLiveTerminalForAttempt(ctx context.Context,
	attemptID string) (fundedPaperLiveTerminalReceipt, bool, error) {
	if s == nil || s.store == nil || strings.TrimSpace(attemptID) == "" {
		return fundedPaperLiveTerminalReceipt{}, false, nil
	}
	event, found, err := s.store.ExecutionShadowLiveTerminal(ctx, attemptID)
	if err != nil || !found {
		return fundedPaperLiveTerminalReceipt{}, false, err
	}
	return fundedPaperLiveTerminalReceiptFromEvent(event), true, nil
}

// fundedPaperLiveZeroFillBook is venue truth about one exact selected-outcome touch. It is
// deliberately session-memory only: the funded Paper follower belongs to the same running
// detector/execution experiment, and a restart cannot have delayed work from the prior process.
type fundedPaperLiveZeroFillBook struct {
	AttemptID  string
	Ticker     string
	Side       string
	Price      float64
	Receipt    livePolicyMirrorBookReceipt
	BookSource string
	ObservedAt time.Time
	RecordedAt time.Time
	Reconciled bool
}

func fundedPaperLiveZeroFillKey(attemptID, ticker, side string, price float64) (string, bool) {
	attemptID = strings.TrimSpace(attemptID)
	ticker = strings.ToUpper(strings.TrimSpace(ticker))
	side = strings.ToUpper(strings.TrimSpace(side))
	if attemptID == "" || ticker == "" || (side != "YES" && side != "NO") ||
		price <= 0 || price >= 1 || math.IsNaN(price) || math.IsInf(price, 0) {
		return "", false
	}
	// Kalshi prices are decimal ticks. A micro-dollar key is intentionally tighter than every
	// current tick while remaining stable across the JSON/string/float round trip.
	priceMicros := int64(math.Round(price * 1_000_000))
	return fmt.Sprintf("%s\x00%s\x00%s\x00%d", attemptID, ticker, side, priceMicros), true
}

// rememberFundedPaperLiveZeroFillBook publishes only after the direct Kalshi response proves an
// immediate order terminal and empty. It performs no storage or network work and never runs before
// CreateOrder. The short lock can contend only with Paper's final in-memory append boundary. Any
// Paper lot that won that boundary just before the venue reply is removed by a separately scheduled
// reconciler; this function never takes the Paper-book lock or waits on a queue.
func (s *Server) rememberFundedPaperLiveZeroFillBook(attemptID, ticker, side, timeInForce, state string,
	authoritative bool, filled, remaining, price float64, quote liveMirrorQuote) bool {
	return s.publishFundedPaperLiveZeroFillBook(attemptID, ticker, side, timeInForce, state,
		authoritative, filled, remaining, price, quote, true)
}

func (s *Server) publishFundedPaperLiveZeroFillBook(
	attemptID, ticker, side, timeInForce, state string,
	authoritative bool, filled, remaining, price float64, quote liveMirrorQuote,
	schedule bool) bool {
	if s == nil || !liveKalshiImmediateTimeInForce(timeInForce) ||
		!liveKalshiTerminalZeroFill(state, authoritative, filled, remaining) ||
		math.Abs(quote.Price-price) > 1e-9 {
		return false
	}
	key, ok := fundedPaperLiveZeroFillKey(attemptID, ticker, side, price)
	receipt := livePolicyMirrorBookReceiptFrom(quote.BookSource)
	if !ok || !receipt.ok {
		return false
	}
	row := fundedPaperLiveZeroFillBook{
		AttemptID: strings.TrimSpace(attemptID),
		Ticker:    strings.ToUpper(strings.TrimSpace(ticker)),
		Side:      strings.ToUpper(strings.TrimSpace(side)), Price: price,
		Receipt: receipt, BookSource: strings.TrimSpace(quote.BookSource),
		ObservedAt: quote.ObservedAt, RecordedAt: time.Now().UTC(),
	}
	s.fundedPaperBookTruthMu.Lock()
	if s.fundedPaperLiveZeroFillBook == nil {
		s.fundedPaperLiveZeroFillBook = make(map[string]fundedPaperLiveZeroFillBook)
	}
	s.fundedPaperLiveZeroFillBook[key] = row
	s.fundedPaperBookTruthGeneration.Add(1)
	s.fundedPaperBookTruthMu.Unlock()
	if schedule {
		s.scheduleFundedPaperLiveZeroFillReconcile()
	}
	return true
}

// fundedPaperLiveZeroFillReasonLocked requires fundedPaperBookTruthMu to be held for reading. A
// sequence in the same subscription domain must advance past the venue-disproved receipt. A new
// generation/subscription is usable only when its per-ticker observation is demonstrably newer.
func (s *Server) fundedPaperLiveZeroFillReasonLocked(attemptID, ticker, side string, price float64,
	quote liveMirrorQuote) string {
	key, ok := fundedPaperLiveZeroFillKey(attemptID, ticker, side, price)
	current := livePolicyMirrorBookReceiptFrom(quote.BookSource)
	if !ok || !current.ok || s.fundedPaperLiveZeroFillBook == nil {
		return ""
	}
	blocked, found := s.fundedPaperLiveZeroFillBook[key]
	if !found {
		return ""
	}
	postZeroObservation := !quote.ObservedAt.IsZero() &&
		quote.ObservedAt.After(blocked.RecordedAt)
	newer := false
	if current.generation == blocked.Receipt.generation &&
		current.subID == blocked.Receipt.subID {
		newer = current.sequence > blocked.Receipt.sequence && postZeroObservation
	} else if current.generation > blocked.Receipt.generation {
		newer = postZeroObservation
	}
	if newer {
		return ""
	}
	return "execution-book-disproved-by-authoritative-live-zero-fill"
}

func (s *Server) fundedPaperLiveZeroFillReason(attemptID, ticker, side string, price float64,
	quote liveMirrorQuote) string {
	if s == nil {
		return ""
	}
	s.fundedPaperBookTruthMu.RLock()
	reason := s.fundedPaperLiveZeroFillReasonLocked(attemptID, ticker, side, price, quote)
	s.fundedPaperBookTruthMu.RUnlock()
	return reason
}

// fundedPaperLotCloseBlockReasonLocked is the settlement/stop linearization check. The caller holds
// fundedPaperBookTruthMu for reading and the funded-Paper book mutex for the exact open lot.
// Legacy lots declare no contract and retain their historical behavior, but any exact tombstone
// still blocks them. New contract-v1 lots cannot close until their persisted LIVE terminal is
// complete and economically safe.
func (s *Server) fundedPaperLotCloseBlockReasonLocked(lot kfPos) string {
	if lot.ExecutionTruthContract != "" {
		if lot.ExecutionTruthContract != fundedPaperLiveTruthContractV1 ||
			strings.TrimSpace(lot.ExecutionShadowAttemptID) == "" ||
			strings.TrimSpace(lot.ExecutionLiveTerminalID) == "" ||
			strings.TrimSpace(lot.ExecutionLiveTerminal) == "" {
			return "durable-live-terminal-contract-incomplete"
		}
		if lot.ExecutionLiveTerminalKind != fundedPaperLiveTerminalNoSend &&
			lot.ExecutionLiveTerminalKind != fundedPaperLiveTerminalFill {
			return "durable-live-terminal-not-safe-for-paper-settlement"
		}
	}
	key, ok := fundedPaperLiveZeroFillKey(lot.ExecutionShadowAttemptID,
		lot.Ticker, lot.Side, lot.Price)
	if !ok || s.fundedPaperLiveZeroFillBook == nil {
		return ""
	}
	tombstone, found := s.fundedPaperLiveZeroFillBook[key]
	if found && sameFundedPaperBookReceipt(lot.ExecutionBookSource, tombstone.Receipt) {
		return "execution-book-disproved-by-authoritative-live-zero-fill"
	}
	return ""
}

type fundedPaperLiveZeroFillCorrection struct {
	Tombstone fundedPaperLiveZeroFillBook
	Lot       kfPos
}

func sameFundedPaperBookReceipt(source string, want livePolicyMirrorBookReceipt) bool {
	got := livePolicyMirrorBookReceiptFrom(source)
	return got.ok && want.ok && got.generation == want.generation &&
		got.subID == want.subID && got.sequence == want.sequence
}

// scheduleFundedPaperLiveZeroFillReconcile starts at most one asynchronous reconciler. The LIVE
// response path performs only one atomic operation and a goroutine schedule; Paper locks, file
// persistence, SQLite audit and comparison telemetry all remain off that path.
func (s *Server) scheduleFundedPaperLiveZeroFillReconcile() {
	if s == nil || !s.fundedPaperBookTruthReconcileRunning.CompareAndSwap(false, true) {
		return
	}
	go s.runFundedPaperLiveZeroFillReconcile()
}

func (s *Server) waitFundedPaperLiveZeroFillReconcile(ctx context.Context) error {
	if s == nil {
		return nil
	}
	for s.fundedPaperBookTruthReconcileRunning.Load() {
		timer := time.NewTimer(2 * time.Millisecond)
		select {
		case <-timer.C:
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return ctx.Err()
		}
	}
	return nil
}

func (s *Server) pendingFundedPaperLiveZeroFills() map[string]fundedPaperLiveZeroFillBook {
	s.fundedPaperBookTruthMu.RLock()
	defer s.fundedPaperBookTruthMu.RUnlock()
	out := make(map[string]fundedPaperLiveZeroFillBook)
	for key, row := range s.fundedPaperLiveZeroFillBook {
		if !row.Reconciled {
			out[key] = row
		}
	}
	return out
}

func (s *Server) markFundedPaperLiveZeroFillsReconciled(
	rows map[string]fundedPaperLiveZeroFillBook) {
	s.fundedPaperBookTruthMu.Lock()
	for key, checked := range rows {
		current, ok := s.fundedPaperLiveZeroFillBook[key]
		if !ok || !current.RecordedAt.Equal(checked.RecordedAt) ||
			!sameFundedPaperBookReceipt(current.BookSource, checked.Receipt) {
			continue
		}
		current.Reconciled = true
		s.fundedPaperLiveZeroFillBook[key] = current
	}
	s.fundedPaperBookTruthMu.Unlock()
}

// reconcileFundedPaperLiveZeroFills removes only a lot carrying the same immutable detector
// attempt, ticker, outcome, price and sequence-stamped book as the authoritative LIVE zero-fill.
// The truth map remains after cleanup, so a delayed Paper append cannot resurrect the lot.
func (s *Server) reconcileFundedPaperLiveZeroFills() ([]fundedPaperLiveZeroFillCorrection, error) {
	pending := s.pendingFundedPaperLiveZeroFills()
	if len(pending) == 0 {
		return nil, nil
	}
	corrections := make([]fundedPaperLiveZeroFillCorrection, 0)
	s.gfBookMu.Lock()
	st := s.gfLoadLocked()
	for _, book := range st.Subs {
		if book == nil || len(book.Open) == 0 {
			continue
		}
		kept := book.Open[:0]
		for _, lot := range book.Open {
			key, ok := fundedPaperLiveZeroFillKey(
				lot.ExecutionShadowAttemptID, lot.Ticker, lot.Side, lot.Price)
			tombstone, found := pending[key]
			if ok && found && sameFundedPaperBookReceipt(
				lot.ExecutionBookSource, tombstone.Receipt) {
				corrections = append(corrections, fundedPaperLiveZeroFillCorrection{
					Tombstone: tombstone, Lot: lot,
				})
				continue
			}
			kept = append(kept, lot)
		}
		clear(book.Open[len(kept):])
		book.Open = kept
	}
	if len(corrections) > 0 {
		s.gfBookDirty = true
	}
	s.gfBookMu.Unlock()
	// Always call the idempotent flush. If an earlier flush failed after removing a lot from
	// memory, gfBookDirty remains true even though this retry no longer finds that lot.
	if err := s.genfollowFlush(); err != nil {
		return corrections, err
	}
	s.markFundedPaperLiveZeroFillsReconciled(pending)
	return corrections, nil
}

func (s *Server) auditFundedPaperLiveZeroFillCorrections(
	corrections []fundedPaperLiveZeroFillCorrection) {
	for _, correction := range corrections {
		row, lot := correction.Tombstone, correction.Lot
		event := s.executionShadowEvent(row.AttemptID, "funded-paper-correction", "invalidated",
			"authoritative LIVE IOC/FOK zero-fill invalidated the exact earlier Paper fill",
			time.Now().UTC())
		event.PaperState = "PAPER-INVALIDATED"
		event.PaperBookSource = lot.ExecutionBookSource
		event.BookSource = row.BookSource
		event.OriginalLimit = executionShadowPricePtr(row.Price)
		event.Evidence = map[string]any{
			"ticker": row.Ticker, "side": row.Side,
			"paper_book_source":          lot.ExecutionBookSource,
			"live_zero_fill_book_source": row.BookSource,
		}
		s.enqueueExecutionShadowWrite(executionShadowWrite{event: &event})
		if s.store == nil {
			continue
		}
		detail, _ := json.Marshal(map[string]any{
			"execution_shadow_attempt_id": row.AttemptID,
			"ticker":                      row.Ticker, "side": row.Side, "price": row.Price,
			"book_source": row.BookSource,
			"reason":      "authoritative-live-zero-fill-after-paper-append",
		})
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		_ = s.store.Audit(ctx, "warn", "genfollow",
			"PAPER-CORRECTION removed an exact simulated fill disproved by LIVE zero-fill",
			string(detail))
		cancel()
	}
}

func (s *Server) runFundedPaperLiveZeroFillReconcile() {
	const maxFlushAttempts = 3
	for {
		generation := s.fundedPaperBookTruthGeneration.Load()
		var (
			correctionsByKey = make(map[string]fundedPaperLiveZeroFillCorrection)
			err              error
		)
		for attempt := 0; attempt < maxFlushAttempts; attempt++ {
			var corrections []fundedPaperLiveZeroFillCorrection
			corrections, err = s.reconcileFundedPaperLiveZeroFills()
			for _, correction := range corrections {
				key, _ := fundedPaperLiveZeroFillKey(correction.Tombstone.AttemptID,
					correction.Lot.Ticker, correction.Lot.Side, correction.Lot.Price)
				correctionsByKey[key+"\x00"+correction.Lot.TS] = correction
			}
			if err == nil {
				break
			}
			time.Sleep(time.Duration(attempt+1) * 50 * time.Millisecond)
		}
		if err != nil {
			s.gfBookPoisoned.Store(true)
			if s.store != nil {
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				_ = s.store.Audit(ctx, "error", "genfollow",
					"Paper zero-fill correction could not be persisted; follower ledger poisoned",
					err.Error())
				cancel()
			}
		} else {
			corrections := make([]fundedPaperLiveZeroFillCorrection, 0, len(correctionsByKey))
			for _, correction := range correctionsByKey {
				corrections = append(corrections, correction)
			}
			s.auditFundedPaperLiveZeroFillCorrections(corrections)
		}
		if s.fundedPaperBookTruthGeneration.Load() != generation {
			continue
		}
		s.fundedPaperBookTruthReconcileRunning.Store(false)
		if s.fundedPaperBookTruthGeneration.Load() == generation ||
			!s.fundedPaperBookTruthReconcileRunning.CompareAndSwap(false, true) {
			return
		}
	}
}

type fundedPaperLiveZeroFillEvidence struct {
	ExecutionShadowAttemptID string  `json:"execution_shadow_attempt_id"`
	ExecutionBookSource      string  `json:"execution_book_source"`
	ExecutionBookObservedAt  string  `json:"execution_book_observed_at"`
	ExecutionLimitPrice      float64 `json:"execution_limit_price"`
	TimeInForce              string  `json:"time_in_force"`
	Response                 *struct {
		FillCount      string `json:"fill_count"`
		RemainingCount string `json:"remaining_count"`
	} `json:"response"`
}

func fundedPaperRiskZeroFillProof(row storage.LivePendingRiskReservation,
	attemptID string) (fundedPaperLiveZeroFillBook, string, float64, bool) {
	if row.Intent.Product != "single" || len(row.Legs) != 1 ||
		!strings.EqualFold(row.Legs[0].Venue, "kalshi") {
		return fundedPaperLiveZeroFillBook{}, "", 0, false
	}
	leg := row.Legs[0]
	var (
		exact        fundedPaperLiveZeroFillEvidence
		exactFound   bool
		terminalSeen bool
		directZero   bool
	)
	for _, event := range row.Events {
		if event.AttemptKey == "entry" &&
			event.EventType == storage.LivePendingRiskTerminalUnfilled {
			terminalSeen = true
		}
		var evidence fundedPaperLiveZeroFillEvidence
		if json.Unmarshal([]byte(event.EvidenceJSON), &evidence) != nil ||
			strings.TrimSpace(evidence.ExecutionShadowAttemptID) != strings.TrimSpace(attemptID) {
			continue
		}
		if evidence.ExecutionLimitPrice <= 0 || evidence.ExecutionLimitPrice >= 1 ||
			math.Abs(evidence.ExecutionLimitPrice-leg.LimitPrice) > 1e-9 ||
			!livePolicyMirrorBookReceiptFrom(evidence.ExecutionBookSource).ok ||
			!liveKalshiImmediateTimeInForce(evidence.TimeInForce) {
			continue
		}
		exact, exactFound = evidence, true
		if evidence.Response != nil {
			filled, fillErr := strconv.ParseFloat(strings.TrimSpace(evidence.Response.FillCount), 64)
			remaining, remainErr := strconv.ParseFloat(
				strings.TrimSpace(evidence.Response.RemainingCount), 64)
			state, authoritative := liveKalshiCreateReceiptState(evidence.TimeInForce,
				leg.Quantity, filled, remaining, fillErr == nil && remainErr == nil)
			if liveKalshiTerminalZeroFill(state, authoritative, filled, remaining) {
				directZero = true
			}
		}
	}
	if !exactFound || (!terminalSeen && !directZero) {
		return fundedPaperLiveZeroFillBook{}, "", 0, false
	}
	observedAt, _ := time.Parse(time.RFC3339Nano,
		strings.TrimSpace(exact.ExecutionBookObservedAt))
	return fundedPaperLiveZeroFillBook{
		AttemptID: strings.TrimSpace(attemptID),
		Ticker:    strings.ToUpper(strings.TrimSpace(leg.Ticker)),
		Side:      strings.ToUpper(strings.TrimSpace(leg.Side)),
		Price:     leg.LimitPrice, BookSource: strings.TrimSpace(exact.ExecutionBookSource),
		ObservedAt: observedAt,
	}, exact.TimeInForce, leg.Quantity, true
}

func fundedPaperRiskAttemptIDs(row storage.LivePendingRiskReservation) []string {
	seen := make(map[string]struct{})
	for _, event := range row.Events {
		var evidence fundedPaperLiveZeroFillEvidence
		if json.Unmarshal([]byte(event.EvidenceJSON), &evidence) != nil {
			continue
		}
		if id := strings.TrimSpace(evidence.ExecutionShadowAttemptID); id != "" {
			seen[id] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for id := range seen {
		out = append(out, id)
	}
	return out
}

func (s *Server) rememberFundedPaperLiveZeroFillFromRisk(
	row storage.LivePendingRiskReservation) bool {
	published := false
	for _, attemptID := range fundedPaperRiskAttemptIDs(row) {
		proof, timeInForce, quantity, ok := fundedPaperRiskZeroFillProof(row, attemptID)
		if !ok {
			continue
		}
		quote := liveMirrorQuote{
			Price: proof.Price, BookSource: proof.BookSource, ObservedAt: proof.ObservedAt,
		}
		if s.rememberFundedPaperLiveZeroFillBook(attemptID, proof.Ticker, proof.Side,
			timeInForce, "unfilled", true, 0, quantity, proof.Price, quote) {
			published = true
		}
	}
	return published
}

// recoverFundedPaperLiveZeroFillsAtBoot repairs the crash window in which the venue's direct
// acknowledgement was durable but the asynchronous Paper JSON correction had not yet flushed.
// It queries only attempt ids currently present in open Paper lots and runs before settlement.
func (s *Server) recoverFundedPaperLiveZeroFillsAtBoot(ctx context.Context) (int, error) {
	if s == nil || s.store == nil {
		return 0, nil
	}
	s.gfBookMu.Lock()
	state := s.gfLoadLocked()
	attemptSet := make(map[string]struct{})
	for _, book := range state.Subs {
		if book == nil {
			continue
		}
		for _, lot := range book.Open {
			if id := strings.TrimSpace(lot.ExecutionShadowAttemptID); id != "" {
				attemptSet[id] = struct{}{}
			}
		}
	}
	s.gfBookMu.Unlock()
	if len(attemptSet) == 0 {
		return 0, nil
	}
	attempts := make([]string, 0, len(attemptSet))
	for id := range attemptSet {
		attempts = append(attempts, id)
	}
	rows, err := s.store.LivePendingRisksByExecutionShadowAttempts(ctx, attempts)
	if err != nil {
		return 0, err
	}
	for _, row := range rows {
		for _, attemptID := range fundedPaperRiskAttemptIDs(row) {
			proof, timeInForce, quantity, ok := fundedPaperRiskZeroFillProof(row, attemptID)
			if !ok {
				continue
			}
			quote := liveMirrorQuote{
				Price: proof.Price, BookSource: proof.BookSource, ObservedAt: proof.ObservedAt,
			}
			s.publishFundedPaperLiveZeroFillBook(attemptID, proof.Ticker, proof.Side,
				timeInForce, "unfilled", true, 0, quantity, proof.Price, quote, false)
		}
	}
	corrections, err := s.reconcileFundedPaperLiveZeroFills()
	if err != nil {
		return len(corrections), err
	}
	s.auditFundedPaperLiveZeroFillCorrections(corrections)
	return len(corrections), nil
}
