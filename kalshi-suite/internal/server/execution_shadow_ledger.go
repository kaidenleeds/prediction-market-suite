package server

// Unified execution-shadow ledger.
//
// A detector emits one immutable attempt id before LIVE and Paper branch. The hot path only copies
// evidence into memory; it wakes one background writer after the LIVE branch is published and
// never waits for SQLite or a network request. LIVE keeps first use of the captured candidate/book.
// The worker appends the same id's LIVE gates, funded-Paper result, realistic mirror IOC result,
// venue receipt, and settlement.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

const (
	executionShadowBatchMax   = storage.ExecutionShadowBatchMax
	executionShadowPendingMax = 32768
	executionShadowOrphanMax  = 8
	executionShadowBatchDelay = 50 * time.Millisecond
	// The isolated FULL-synchronous database can wait through its own checkpoint without holding
	// the cash database's writer lock. The producer remains nonblocking and memory-bounded.
	executionShadowWriteTimeout = 5 * time.Second

	executionShadowSettlementBatch                   = 500
	executionShadowFundedSettlementPriorityMax       = 512
	executionShadowSettlementCursorMetaKey           = "r169:settlement-sequence-cursor-v1"
	executionShadowElapsedUnknownMS            int64 = -1
	executionShadowElapsedInvalidMS            int64 = -1 << 63
	executionShadowPrePOSTSchema                     = "r172-prepost-v1"
)

type executionShadowSettlementCursor struct {
	AfterSequence   int64 `json:"after_sequence"`
	ThroughSequence int64 `json:"through_sequence"`
}

type executionShadowWrite struct {
	attempt       *storage.ExecutionShadowAttempt
	event         *storage.ExecutionShadowEvent
	kalshiTrade   *storage.KalshiTradeReconciliationEvent
	orphanRetries uint8
}

// executionShadowIngressNode is the rare-path, lock-free handoff used whenever the writer owns
// executionShadowMu. One node contains one complete producer group, so an attempt plus its first
// event can never be partly admitted. The worker exchanges and reverses the stack before appending
// it to its private pending slice.
type executionShadowIngressNode struct {
	writes []executionShadowWrite
	next   *executionShadowIngressNode
}

type executionShadowWakeHandle struct {
	ch chan struct{}
}

type executionShadowSignalRef struct {
	AttemptID string
	At        time.Time
}

func (s *Server) executionShadowRememberTrigger(attemptID string, triggerUnixMS int64) {
	if s == nil || strings.TrimSpace(attemptID) == "" || triggerUnixMS <= 0 {
		return
	}
	// Native attempt ids carry their immutable trigger clock. They do not need a process-lifetime
	// map entry, which keeps the diagnostic lineage bounded even during a long-running suite.
	if parsed, ok := executionShadowTriggerFromAttemptID(attemptID); ok && parsed == triggerUnixMS {
		return
	}
	s.executionShadowSignalMu.Lock()
	if s.executionShadowTriggers == nil {
		s.executionShadowTriggers = make(map[string]int64)
	}
	s.executionShadowTriggers[attemptID] = triggerUnixMS
	s.executionShadowSignalMu.Unlock()
}

func (s *Server) executionShadowTrigger(attemptID string) (int64, bool) {
	if s == nil || strings.TrimSpace(attemptID) == "" {
		return 0, false
	}
	if trigger, ok := executionShadowTriggerFromAttemptID(attemptID); ok {
		return trigger, true
	}
	s.executionShadowSignalMu.Lock()
	trigger, ok := s.executionShadowTriggers[attemptID]
	s.executionShadowSignalMu.Unlock()
	return trigger, ok && trigger > 0
}

func executionShadowTriggerFromAttemptID(attemptID string) (int64, bool) {
	attemptID = strings.TrimSpace(attemptID)
	if !strings.HasPrefix(attemptID, "exec-") {
		return 0, false
	}
	rest := strings.TrimPrefix(attemptID, "exec-")
	cut := strings.IndexByte(rest, '-')
	if cut <= 0 {
		return 0, false
	}
	nanos, err := strconv.ParseInt(rest[:cut], 10, 64)
	if err != nil || nanos <= 0 {
		return 0, false
	}
	return time.Unix(0, nanos).UnixMilli(), true
}

func executionShadowSignalKey(sig storage.Signal) string {
	inputObserved := r147SignalInputObservedAt(sig)
	return strings.Join([]string{
		strings.ToLower(strings.TrimSpace(sig.Platform)),
		strings.TrimSpace(sig.Ticker),
		strings.ToUpper(strings.TrimSpace(sig.Side)),
		strings.ToLower(strings.TrimSpace(sig.SignalType)),
		strconv.Itoa(sig.Episode),
		strings.TrimSpace(sig.ExecExpr),
		fmt.Sprintf("%.9f", sig.EntryPrice),
		inputObserved.UTC().Format(time.RFC3339Nano),
	}, "\x00")
}

func (s *Server) executionShadowRememberSignal(sig storage.Signal, attemptID string, at time.Time) {
	if s == nil || strings.TrimSpace(attemptID) == "" {
		return
	}
	now := time.Now()
	s.executionShadowSignalMu.Lock()
	if s.executionShadowSignalRefs == nil {
		s.executionShadowSignalRefs = make(map[string]executionShadowSignalRef)
	}
	s.executionShadowSignalRefs[executionShadowSignalKey(sig)] =
		executionShadowSignalRef{AttemptID: attemptID, At: at}
	if len(s.executionShadowSignalRefs) > 4096 {
		for key, ref := range s.executionShadowSignalRefs {
			if now.Sub(ref.At) > liveMirrorTTL*2 {
				delete(s.executionShadowSignalRefs, key)
			}
		}
	}
	s.executionShadowSignalMu.Unlock()
}

func (s *Server) executionShadowAttemptForSignal(sig storage.Signal) string {
	if s == nil {
		return ""
	}
	now := time.Now()
	key := executionShadowSignalKey(sig)
	s.executionShadowSignalMu.Lock()
	ref, ok := s.executionShadowSignalRefs[key]
	if ok && now.Sub(ref.At) > liveMirrorTTL*2 {
		delete(s.executionShadowSignalRefs, key)
		ok = false
	}
	s.executionShadowSignalMu.Unlock()
	if !ok {
		return ""
	}
	return ref.AttemptID
}

func executionShadowCandidateDecisionID(c liveMirrorCandidate) string {
	at := c.At.UTC().Format(time.RFC3339Nano)
	inputAt := c.InputObservedAt.UTC().Format(time.RFC3339Nano)
	sum := sha256.Sum256([]byte(strings.Join([]string{
		c.key(), strings.ToLower(strings.TrimSpace(c.Source)),
		strings.TrimSpace(c.InputTopology), strings.TrimSpace(c.SignalContractID),
		inputAt, at,
	}, "\x00")))
	return "decision-" + hex.EncodeToString(sum[:16])
}

func executionShadowRedactedSource(source string) string {
	source = strings.TrimSpace(source)
	if isNewMLV2LiveSource(source) {
		return newMLV2LiveSourcePrefix + "[redacted]"
	}
	return source
}

func executionShadowRedactedEvidence(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		switch typed := value.(type) {
		case string:
			out[key] = executionShadowRedactedSource(typed)
		case map[string]any:
			out[key] = executionShadowRedactedEvidence(typed)
		case []any:
			copied := make([]any, len(typed))
			for i, item := range typed {
				if text, ok := item.(string); ok {
					copied[i] = executionShadowRedactedSource(text)
				} else {
					copied[i] = item
				}
			}
			out[key] = copied
		default:
			out[key] = value
		}
	}
	return out
}

func (s *Server) executionShadowBuildCandidate(c liveMirrorCandidate, qualification,
	attemptID string) (liveMirrorCandidate, storage.ExecutionShadowAttempt, bool) {
	var attempt storage.ExecutionShadowAttempt
	if s == nil || s.store == nil {
		return c, attempt, false
	}
	if c.At.IsZero() {
		c.At = time.Now()
	}
	c.Platform = strings.ToLower(strings.TrimSpace(c.Platform))
	c.Ticker = strings.TrimSpace(c.Ticker)
	c.Side = strings.ToUpper(strings.TrimSpace(c.Side))
	c.Action = strings.ToUpper(strings.TrimSpace(c.Action))
	if c.Action == "" {
		c.Action = "BUY"
	}
	if (c.Platform != "kalshi" && c.Platform != "polyus") || c.Ticker == "" ||
		(c.Side != "YES" && c.Side != "NO") || (c.Action != "BUY" && c.Action != "SELL") {
		return c, attempt, false
	}
	family := liveCandidateDropSystem(c)
	route := "taker"
	if c.ArbitrationQuote.Maker {
		route = "maker"
	}
	decisionID := executionShadowCandidateDecisionID(c)
	attemptID = strings.TrimSpace(attemptID)
	if attemptID == "" {
		seq := s.executionShadowSequence.Add(1)
		sum := sha256.Sum256([]byte(fmt.Sprintf("%s|%d", decisionID, seq)))
		attemptID = fmt.Sprintf("exec-%d-%s", c.At.UnixNano(), hex.EncodeToString(sum[:8]))
	}
	c.ShadowAttemptID = attemptID
	if strings.TrimSpace(qualification) == "" {
		qualification = "qualified-candidate"
	}
	signalPrice := c.Price
	if math.IsNaN(signalPrice) || math.IsInf(signalPrice, 0) ||
		signalPrice < 0 || signalPrice >= 1 {
		signalPrice = 0
	}
	attempt = storage.ExecutionShadowAttempt{
		AttemptID: c.ShadowAttemptID, SignalDecisionID: decisionID, ObservedAt: c.At.UTC(),
		TriggerUnixMS: c.At.UnixMilli(),
		Venue:         c.Platform, Ticker: c.Ticker, Title: c.Title, Side: c.Side, Action: c.Action,
		SystemID: family, Route: route,
		SignalSource:  executionShadowRedactedSource(firstNonEmpty(c.Source, family)),
		InputTopology: c.InputTopology, SignalContract: c.SignalContractID,
		InputObservedAt: c.InputObservedAt, SignalPrice: signalPrice,
		QualificationBasis: qualification,
	}
	return c, attempt, true
}

func (s *Server) executionShadowEnsureCandidate(c liveMirrorCandidate,
	qualification string) liveMirrorCandidate {
	if s == nil || s.store == nil || strings.TrimSpace(c.ShadowAttemptID) != "" {
		return c
	}
	var attempt storage.ExecutionShadowAttempt
	var ok bool
	if c, attempt, ok = s.executionShadowBuildCandidate(c, qualification, ""); !ok {
		return c
	}
	s.executionShadowRememberTrigger(c.ShadowAttemptID, attempt.TriggerUnixMS)
	writes := []executionShadowWrite{{attempt: &attempt}}
	if !strings.HasPrefix(qualification, "shared-detector-") {
		event := s.executionShadowEvent(c.ShadowAttemptID, "candidate-origin", "qualified",
			qualification, c.At)
		writes = append(writes, executionShadowWrite{event: &event})
	}
	s.enqueueExecutionShadowWriteGroup(writes, false)
	return c
}

// executionShadowSignalAttemptID creates only the immutable lineage token. It performs no shared
// queue or map work, so callers can put the real LIVE intent on its bounded channel first.
func (s *Server) executionShadowSignalAttemptID(sig storage.Signal, measuredPoint float64,
	signalAt time.Time) string {
	if signalAt.IsZero() {
		signalAt = time.Now()
	}
	c := liveMirrorCandidate{
		Platform: sig.Platform, Ticker: sig.Ticker, Title: sig.Title, Side: sig.Side,
		Family: sig.SignalType, Source: "auto-cons-" + sig.SignalType, Price: sig.EntryPrice,
		At: signalAt, InputTopology: r147SignalInputTopology(sig),
		InputObservedAt: r147SignalInputObservedAt(sig),
	}
	if bound, why, declared := bindStaticTakerSignalCandidate(c, sig); declared {
		if why != "" {
			return ""
		}
		c = bound
	}
	c, _, ok := s.executionShadowBuildCandidate(c,
		fmt.Sprintf("shared-detector-positive-route-point=%.9f", measuredPoint), "")
	if !ok {
		return ""
	}
	return c.ShadowAttemptID
}

// executionShadowPublishSignal runs only after the LIVE intent publication attempt. The attempt
// and its shared opportunity receipt enter the bounded telemetry queue as one ordered group.
// The comparison ledger therefore records the same immutable detector opportunity and millisecond
// trigger clock for both money lanes without putting funded-Paper book, balance, or main-ledger
// work ahead of LIVE. paperFanout means funded Paper execution was also scheduled at this boundary;
// false means only its opportunity was recorded here and execution remains gated on the later LIVE
// preflight.
func (s *Server) executionShadowPublishSignal(sig storage.Signal, measuredPoint float64,
	signalAt time.Time, attemptID string, paperFanout bool) {
	s.executionShadowPublishSignalDisposition(sig, measuredPoint, signalAt, attemptID,
		paperFanout, true, "")
}

// executionShadowPublishSignalDisposition is the pre-selection canonical detector receipt.
// liveSelected=false is not a missing LIVE branch: the same opportunity receives a later durable
// not-sent terminal naming the exact selection reason. This lets Paper-only research remain
// joinable without pretending it had cash authority.
func (s *Server) executionShadowPublishSignalDisposition(sig storage.Signal, measuredPoint float64,
	signalAt time.Time, attemptID string, paperFanout, liveSelected bool, selectionReason string) {
	if strings.TrimSpace(attemptID) == "" {
		return
	}
	if signalAt.IsZero() {
		signalAt = time.Now()
	}
	c := liveMirrorCandidate{
		Platform: sig.Platform, Ticker: sig.Ticker, Title: sig.Title, Side: sig.Side,
		Family: sig.SignalType, Source: "auto-cons-" + sig.SignalType, Price: sig.EntryPrice,
		At: signalAt, InputTopology: r147SignalInputTopology(sig),
		InputObservedAt: r147SignalInputObservedAt(sig),
	}
	if bound, why, declared := bindStaticTakerSignalCandidate(c, sig); declared {
		if why != "" {
			return
		}
		c = bound
	}
	var attempt storage.ExecutionShadowAttempt
	var ok bool
	c, attempt, ok = s.executionShadowBuildCandidate(c,
		fmt.Sprintf("shared-detector-positive-route-point=%.9f", measuredPoint), attemptID)
	if !ok {
		return
	}
	s.executionShadowRememberTrigger(c.ShadowAttemptID, attempt.TriggerUnixMS)
	s.executionShadowRememberSignal(sig, c.ShadowAttemptID, signalAt)
	reason, paperBranch, paperExecutionBoundary :=
		"shared detector recorded one identical LIVE/Paper opportunity after publishing LIVE first",
		"opportunity-recorded; funded execution waits for LIVE preflight",
		"after-live-preflight"
	if !liveSelected {
		reason = "shared detector recorded one research opportunity before explicit LIVE non-selection and Paper/shadow fanout"
		paperBranch = "opportunity-recorded; funded execution follows the explicit LIVE not-selected terminal"
		paperExecutionBoundary = "after-live-selection-terminal"
	}
	if paperFanout {
		if liveSelected {
			reason = "shared detector recorded one identical LIVE/Paper opportunity after publishing LIVE first and before funded-Paper execution fanout"
		} else {
			reason = "shared detector recorded one research opportunity before explicit LIVE non-selection and funded-Paper execution fanout"
		}
		paperBranch = "opportunity-recorded; bounded delayed funded-Paper worker follows"
		if liveSelected {
			paperExecutionBoundary = "detector-boundary-after-live-publication"
		} else {
			paperExecutionBoundary = "detector-boundary-after-live-non-selection"
		}
	}
	event := s.executionShadowEvent(c.ShadowAttemptID, "detector-branch", "qualified",
		reason, signalAt)
	event.Evidence = map[string]any{
		"measured_point":                      measuredPoint,
		"live_published_first":                liveSelected,
		"live_selected":                       liveSelected,
		"live_selection_reason":               strings.TrimSpace(selectionReason),
		"shared_opportunity_receipt":          true,
		"live_detector_opportunity_recorded":  true,
		"paper_detector_opportunity_recorded": true,
		"paper_branch":                        paperBranch,
		"paper_fanout":                        paperFanout,
		"paper_execution_scheduled":           paperFanout,
		"paper_execution_boundary":            paperExecutionBoundary,
		"signal_book_source":                  sig.BookSource,
	}
	executionShadowApplySignalBook(&event, sig, signalAt)
	s.r168StampForwardDetectorEvent(&event, attempt, r168ForwardModelVersion(sig))
	event.Evidence["signal_pricing_version"] = strings.TrimSpace(sig.PricingVersion)
	event.Evidence["signal_label_version"] = strings.TrimSpace(sig.LabelVersion)
	s.enqueueExecutionShadowWriteGroup([]executionShadowWrite{
		{attempt: &attempt}, {event: &event},
	}, true)
}

// executionShadowRecordPaperFanout marks the later pass-only branch boundary for event-driven
// signals. The detector receipt remains truthful about publishing only LIVE; this same attempt id
// records when the cash candidate has passed preflight and is offered to funded Paper.
func (s *Server) executionShadowRecordPaperFanout(attemptID string, at, signalAt time.Time,
	sig storage.Signal) {
	if strings.TrimSpace(attemptID) == "" {
		return
	}
	event := s.executionShadowEvent(attemptID, "funded-paper-branch", "offered",
		"LIVE-first preflight passed; identical signal offered to bounded funded-Paper worker", at)
	event.Evidence = map[string]any{
		"paper_fanout":         true,
		"fanout_boundary":      "after-live-preflight-and-candidate-queue",
		"cash_candidate_first": true,
		"signal_at":            optionalExecutionShadowTimeText(signalAt),
	}
	// The retained writer marshals evidence later. Never hand it a struct that may contain a
	// non-finite research feature: json.Marshal would then fail forever and pin the whole retry
	// queue. Store the already-validated JSON as an inert string; recovery decodes that string.
	if payload, err := json.Marshal(sig); err == nil {
		event.Evidence["paper_signal_json"] = string(payload)
		event.Evidence["paper_signal_available"] = true
	} else {
		event.Evidence["paper_signal_available"] = false
		event.Evidence["paper_signal_error"] = "signal-payload-not-finite-json"
	}
	s.enqueueExecutionShadowWrite(executionShadowWrite{event: &event})
}

// executionShadowBeginSignal remains the non-money helper used by tests and mirror-only callers.
// Money callers use SignalAttemptID -> publish LIVE -> PublishSignal.
func (s *Server) executionShadowBeginSignal(sig storage.Signal, measuredPoint float64,
	signalAt time.Time) string {
	attemptID := s.executionShadowSignalAttemptID(sig, measuredPoint, signalAt)
	s.executionShadowPublishSignal(sig, measuredPoint, signalAt, attemptID, false)
	return attemptID
}

func (s *Server) executionShadowEvent(attemptID, stage, outcome, reason string,
	at time.Time) storage.ExecutionShadowEvent {
	if at.IsZero() {
		at = time.Now().UTC()
	}
	seq := s.executionShadowSequence.Add(1)
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s|%s|%s|%d|%d",
		attemptID, stage, outcome, at.UnixNano(), seq)))
	triggerUnixMS, ok := s.executionShadowTrigger(attemptID)
	elapsedFromTriggerMS := executionShadowElapsedUnknownMS
	if ok {
		elapsedFromTriggerMS = at.UnixMilli() - triggerUnixMS
		if elapsedFromTriggerMS < 0 {
			// Keep "parent not in memory yet" distinct from a causally impossible event clock.
			// The former is derived from the exact parent by the isolated writer; the latter must
			// be refused before it can poison every later row in a retained batch.
			elapsedFromTriggerMS = executionShadowElapsedInvalidMS
		}
	}
	return storage.ExecutionShadowEvent{
		EventID:   fmt.Sprintf("execev-%d-%s", at.UnixNano(), hex.EncodeToString(sum[:8])),
		AttemptID: attemptID, At: at.UTC(), ElapsedFromTriggerMS: elapsedFromTriggerMS,
		Stage: stage, Outcome: outcome, Reason: reason,
	}
}

func floatPtrShadow(v float64) *float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return nil
	}
	out := v
	return &out
}

func executionShadowPricePtr(v float64) *float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) || v <= 0 || v >= 1 {
		return nil
	}
	return floatPtrShadow(v)
}

func executionShadowNonnegativePtr(v float64) *float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) || v < 0 {
		return nil
	}
	return floatPtrShadow(v)
}

func intPtrShadow(v int64) *int64 {
	out := v
	return &out
}

func executionShadowApplySignalBook(event *storage.ExecutionShadowEvent, sig storage.Signal,
	checkedAt time.Time) {
	if event == nil {
		return
	}
	event.BookSource = strings.TrimSpace(sig.BookSource)
	event.BookGeneration, event.BookSubscriptionID, event.BookSequence =
		executionShadowQuoteReceipt(liveMirrorQuote{BookSource: event.BookSource})
	if sig.BookBid != nil {
		event.SideBid = executionShadowPricePtr(*sig.BookBid)
	}
	if sig.BookAsk != nil {
		event.SideAsk = executionShadowPricePtr(*sig.BookAsk)
	}
	event.SpreadCents = executionShadowNonnegativePtr(sig.SpreadCents)
	if sig.BookAskDepth != nil {
		event.VisibleDepth = executionShadowNonnegativePtr(*sig.BookAskDepth)
	}
	if sig.BookTakerTick != nil && *sig.BookTakerTick > 0 && *sig.BookTakerTick <= 1 {
		event.TickSize = floatPtrShadow(*sig.BookTakerTick)
	}
	if sig.BookQuoteAgeS != nil && *sig.BookQuoteAgeS >= 0 {
		age := *sig.BookQuoteAgeS * 1000
		event.BookAgeMS = &age
		event.BookReceivedAt = checkedAt.Add(-time.Duration(age * float64(time.Millisecond))).UTC()
	}
}

func executionShadowQuoteReceipt(q liveMirrorQuote) (generation, subscription, sequence *int64) {
	var g uint64
	var sid, seq int64
	n, err := fmt.Sscanf(strings.TrimSpace(q.BookSource),
		"kalshi_ws_full_orderbook:g%d:s%d:q%d", &g, &sid, &seq)
	if err != nil || n != 3 || g == 0 || sid <= 0 || seq <= 0 {
		return nil, nil, nil
	}
	return intPtrShadow(int64(g)), intPtrShadow(sid), intPtrShadow(seq)
}

func executionShadowApplyQuote(event *storage.ExecutionShadowEvent, q liveMirrorQuote,
	checkedAt time.Time) {
	if event == nil {
		return
	}
	event.BookSource = strings.TrimSpace(q.BookSource)
	event.BookGeneration, event.BookSubscriptionID, event.BookSequence =
		executionShadowQuoteReceipt(q)
	event.BookSourceAt = q.SourceAt.UTC()
	event.BookReceivedAt = q.ObservedAt.UTC()
	if !q.ObservedAt.IsZero() && !checkedAt.IsZero() {
		age := float64(checkedAt.Sub(q.ObservedAt)) / float64(time.Millisecond)
		if age >= 0 {
			event.BookAgeMS = &age
		}
	}
	spread := math.Max(0, q.SpreadCents)
	event.SpreadCents = executionShadowNonnegativePtr(spread)
	bid, ask := q.Price-spread/100, q.Price
	if q.Maker {
		bid, ask = q.Price, q.Price+spread/100
	}
	if bid > 0 && bid < 1 {
		event.SideBid = &bid
	}
	if ask > 0 && ask < 1 {
		event.SideAsk = &ask
	}
	if q.Depth >= 0 {
		event.VisibleDepth = floatPtrShadow(q.Depth)
	}
	if q.Tick > 0 && q.Tick <= 1 && !math.IsNaN(q.Tick) && !math.IsInf(q.Tick, 0) {
		event.TickSize = floatPtrShadow(q.Tick)
	}
}

func executionShadowEvidenceNumber(value any) (float64, bool) {
	number, ok := value.(float64)
	return number, ok && !math.IsNaN(number) && !math.IsInf(number, 0)
}

func executionShadowEvidenceTime(value any) time.Time {
	text, ok := value.(string)
	if !ok {
		return time.Time{}
	}
	at, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(text))
	if err != nil {
		return time.Time{}
	}
	return at.UTC()
}

// executionShadowApplyDurablePrePOST projects the immutable pre-transport snapshot into the typed
// columns used by the profit funnel. Keeping the same facts only inside opaque JSON would preserve
// forensic detail while still making crash-recovered book and fee evidence look unavailable.
func executionShadowApplyDurablePrePOST(event *storage.ExecutionShadowEvent,
	evidence map[string]any) {
	if event == nil || evidence == nil ||
		fmt.Sprint(evidence["pre_post_schema"]) != executionShadowPrePOSTSchema {
		return
	}
	book, ok := evidence["pre_post_book"].(map[string]any)
	if !ok {
		return
	}
	q := liveMirrorQuote{}
	q.Price, _ = executionShadowEvidenceNumber(book["price"])
	q.Depth, _ = executionShadowEvidenceNumber(book["depth"])
	q.Tick, _ = executionShadowEvidenceNumber(book["tick"])
	q.SpreadCents, _ = executionShadowEvidenceNumber(book["spread_cents"])
	q.BookSource, _ = book["book_source"].(string)
	q.Maker, _ = book["maker"].(bool)
	q.SourceAt = executionShadowEvidenceTime(book["source_at"])
	q.ObservedAt = executionShadowEvidenceTime(book["observed_at"])
	executionShadowApplyQuote(event, q, executionShadowEvidenceTime(book["capture_at"]))
	for key, destination := range map[string]**int64{
		"book_generation":      &event.BookGeneration,
		"book_subscription_id": &event.BookSubscriptionID,
		"book_sequence":        &event.BookSequence,
	} {
		if number, valid := executionShadowEvidenceNumber(book[key]); valid &&
			number > 0 && math.Trunc(number) == number {
			*destination = intPtrShadow(int64(number))
		}
	}
	if number, valid := executionShadowEvidenceNumber(evidence["pre_post_limit_price"]); valid {
		event.OriginalLimit = executionShadowPricePtr(number)
	}
	if number, valid := executionShadowEvidenceNumber(evidence["pre_post_requested_qty"]); valid &&
		number > 0 {
		event.RequestedQty = floatPtrShadow(number)
	}
	if number, valid := executionShadowEvidenceNumber(evidence["pre_post_fee_total"]); valid &&
		number >= 0 {
		if source, ok := evidence["pre_post_fee_source"].(string); ok &&
			strings.TrimSpace(source) != "" {
			event.FeeQuote = floatPtrShadow(number)
			event.FeeQuoteSource = strings.TrimSpace(source)
		}
	}
}

func (s *Server) executionShadowRecordQuote(c liveMirrorCandidate, stage, outcome, reason string,
	q liveMirrorQuote, fee float64, feeSource string, requestedQty float64) {
	s.executionShadowRecordQuoteWithLimit(c, stage, outcome, reason, q, q.Price, fee,
		feeSource, requestedQty)
}

func (s *Server) executionShadowRecordQuoteWithLimit(c liveMirrorCandidate,
	stage, outcome, reason string, q liveMirrorQuote, originalLimit, fee float64,
	feeSource string, requestedQty float64) {
	if strings.TrimSpace(c.ShadowAttemptID) == "" {
		return
	}
	now := time.Now()
	event := s.executionShadowEvent(c.ShadowAttemptID, stage, outcome, reason, now)
	executionShadowApplyQuote(&event, q, now)
	if originalLimit > 0 {
		event.OriginalLimit = executionShadowPricePtr(originalLimit)
	}
	if requestedQty > 0 {
		event.RequestedQty = floatPtrShadow(requestedQty)
	}
	if feeValue := executionShadowNonnegativePtr(fee); feeValue != nil &&
		strings.TrimSpace(feeSource) != "" {
		event.FeeQuote, event.FeeQuoteSource = feeValue, strings.TrimSpace(feeSource)
	}
	s.enqueueExecutionShadowWrite(executionShadowWrite{event: &event})
}

func (s *Server) executionShadowRecordDrop(c liveMirrorCandidate, stage, reason string,
	at time.Time, extra map[string]any) {
	if s == nil || s.store == nil {
		return
	}
	if c.ShadowAttemptID == "" {
		c = s.executionShadowEnsureCandidate(c, "qualified-live-candidate")
	}
	if c.ShadowAttemptID == "" {
		return
	}
	event := s.executionShadowEvent(c.ShadowAttemptID, stage, "rejected", reason, at)
	// This is the centralized durable terminal for a candidate that never crossed the venue
	// boundary. Funded Paper may retain it only as an explicitly labelled counterfactual; it must
	// never confuse this local no-send with an authoritative venue zero-fill.
	event.LiveState = "not-sent"
	event.VenueAttempted = false
	event.LiveAuthoritative = false
	event.Evidence = executionShadowRedactedEvidence(extra)
	if event.Evidence == nil {
		event.Evidence = make(map[string]any)
	}
	// A local no-send is still a complete comparison terminal. Preserve the exact frozen order
	// plan so restart recovery does not silently fall back to a fake quantity or TIF.
	if c.ProspectiveQty > 0 && !math.IsNaN(c.ProspectiveQty) &&
		!math.IsInf(c.ProspectiveQty, 0) {
		event.RequestedQty = floatPtrShadow(c.ProspectiveQty)
	}
	if tif := strings.TrimSpace(c.ProspectiveTimeInForce); tif != "" {
		event.Evidence["time_in_force"] = tif
	}
	if planReason := strings.TrimSpace(c.ProspectivePlanReason); planReason != "" {
		event.Evidence["time_in_force_reason"] = planReason
	}
	if !c.ArbitrationAt.IsZero() && c.ArbitrationQuote.Price > 0 {
		executionShadowApplyQuote(&event, c.ArbitrationQuote, at)
		event.OriginalLimit = floatPtrShadow(c.ArbitrationQuote.Price)
		if c.ArbitrationFee >= 0 && strings.TrimSpace(c.ArbitrationBasis) != "" {
			event.FeeQuote = executionShadowNonnegativePtr(c.ArbitrationFee)
			if event.FeeQuote == nil {
				event.FeeQuoteSource = ""
			} else {
				event.FeeQuoteSource = "strategy-proof:" + c.ArbitrationBasis
			}
		}
	}
	if !s.enqueueExecutionShadowWrite(executionShadowWrite{event: &event}) {
		return
	}
	s.enqueueGenfollowPaperAfterLiveTerminal(event)
	if c.ProspectiveQty >= 1 &&
		(c.ProspectiveTimeInForce == liveProspectiveIOC ||
			c.ProspectiveTimeInForce == liveProspectiveFOK) {
		s.enqueueLivePolicyMirrorCandidateAfterTerminal(c, c.ArbitrationQuote, at,
			"not-sent", false)
	}
}

// executionShadowRecordHousekeeping preserves queue/cache lifecycle evidence without claiming a
// second economic LIVE outcome. A candidate may remain in the combo-history queue after its
// dispatch handler has already produced the one cash terminal; expiry, replacement, or queue
// cleanup of that copy must therefore never populate the LIVE terminal columns.
func (s *Server) executionShadowRecordHousekeeping(c liveMirrorCandidate, stage, reason string,
	at time.Time, extra map[string]any) {
	if s == nil || s.store == nil {
		return
	}
	if c.ShadowAttemptID == "" {
		c = s.executionShadowEnsureCandidate(c, "qualified-live-candidate")
	}
	if c.ShadowAttemptID == "" {
		return
	}
	event := s.executionShadowEvent(c.ShadowAttemptID, stage, "housekeeping", reason, at)
	event.Evidence = executionShadowRedactedEvidence(extra)
	if event.Evidence == nil {
		event.Evidence = make(map[string]any)
	}
	event.Evidence["economic_terminal"] = false
	s.enqueueExecutionShadowWrite(executionShadowWrite{event: &event})
}

func (s *Server) executionShadowRecordPaperAttempt(attemptID, state, reason, paperAttemptID string,
	at time.Time, attemptedQty, limit, filledQty, fillPrice, fee float64, feeSource string,
	book storage.Signal) {
	if attemptID == "" {
		return
	}
	event := s.executionShadowEvent(attemptID, "funded-paper-terminal",
		strings.ToLower(strings.TrimSpace(state)), reason, at)
	stable := sha256.Sum256([]byte(strings.Join(
		[]string{attemptID, "funded-paper-terminal", "r164"}, "|")))
	event.EventID = "execev-stable-" + hex.EncodeToString(stable[:16])
	event.PaperAttemptID, event.PaperState = paperAttemptID, state
	if attemptedQty > 0 {
		event.RequestedQty = floatPtrShadow(attemptedQty)
	}
	if limit > 0 {
		event.OriginalLimit = executionShadowPricePtr(limit)
	}
	if filledQty > 0 {
		event.PaperFilledQty = floatPtrShadow(filledQty)
		event.PaperFillPrice = floatPtrShadow(fillPrice)
		if fee >= 0 && feeSource != "" {
			if feeValue := executionShadowNonnegativePtr(fee); feeValue != nil {
				event.PaperFee, event.PaperFeeSource = feeValue, feeSource
			}
		}
	}
	event.PaperBookSource = book.BookSource
	executionShadowApplySignalBook(&event, book, at)
	// A successful in-memory admission is retained and retried by the isolated writer. Keep the
	// claim for this process so a recovery sweep cannot manufacture the same stable ID with a
	// later timestamp before the queued row commits. If admission itself failed, release the
	// claim so durable recovery can try again.
	if s.enqueueExecutionShadowWrite(executionShadowWrite{event: &event}) {
		s.completeGenfollowPaperScheduled(attemptID)
	} else {
		s.releaseGenfollowPaperScheduled(attemptID)
	}
}

// executionShadowRecordPaperScheduler is deliberately not a funded-paper terminal. Queue pressure,
// a busy cash lane, or shutdown says only that the fake portfolio has not run yet; the isolated
// offered receipt remains a durable replay spool.
func (s *Server) executionShadowRecordPaperScheduler(attemptID, reason string, at time.Time) {
	attemptID, reason = strings.TrimSpace(attemptID), strings.TrimSpace(reason)
	if attemptID == "" {
		return
	}
	if _, alreadyRecorded := s.genfollowPaperSchedulerSeen.LoadOrStore(
		attemptID+"\x00"+reason, struct{}{}); alreadyRecorded {
		return
	}
	event := s.executionShadowEvent(attemptID, "funded-paper-scheduler", "deferred",
		reason, at)
	event.PaperState = "PAPER-DEFERRED"
	event.Evidence = map[string]any{
		"retryable": true, "market_outcome": false,
		"real_money_authority": false, "cash_path_blocking": false,
	}
	s.enqueueExecutionShadowWrite(executionShadowWrite{event: &event})
}

func (s *Server) executionShadowRecordMirror(c liveMirrorCandidate,
	row storage.LivePolicyMirrorRow) {
	if c.ShadowAttemptID == "" {
		return
	}
	outcome := row.State
	if outcome == "" {
		outcome = "unknown"
	}
	event := s.executionShadowEvent(c.ShadowAttemptID, "counterfactual-terminal",
		outcome, row.Reason, row.ProcessedAt)
	event.BookSource = row.BookSource
	event.OriginalLimit = executionShadowPricePtr(row.LimitPrice)
	if row.RequestedQty > 0 {
		event.RequestedQty = floatPtrShadow(row.RequestedQty)
	}
	event.VisibleDepth = executionShadowNonnegativePtr(row.TouchDepth)
	event.ShadowState, event.ShadowReason = row.State, row.Reason
	if row.FilledQty > 0 {
		event.ShadowFilledQty = floatPtrShadow(row.FilledQty)
		event.ShadowFillPrice = executionShadowPricePtr(row.FillPrice)
		if feeValue := executionShadowNonnegativePtr(row.FeeUSD); feeValue != nil {
			event.ShadowFee = feeValue
			event.ShadowFeeSource = firstNonEmpty(row.FeeSource, "mirror-exact-route-fee")
		}
	}
	event.Evidence = map[string]any{
		"model_version": row.ModelVersion, "signal_price": row.SignalPrice,
		"proof_mean": row.ProofMean, "proof_lower": row.ProofLower,
		"proof_fee_pc": row.ProofFeePC, "sizing_bankroll": row.SizingBankroll,
		"sizing_target_usd": row.SizingTargetUSD, "wire_delay_ms": row.WireDelayMS,
	}
	s.enqueueExecutionShadowWrite(executionShadowWrite{event: &event})
}

// recordLivePolicyMirrorExecution is the R164 execution-only comparison terminal. Unlike the
// fake-portfolio terminal above, this row is created before portfolio admission and is therefore
// the canonical Shadow fill lane for new attempts. The $400 portfolio retains its own durable
// table and counterfactual-terminal reason, but cannot overwrite these fill economics.
func (s *Server) recordLivePolicyMirrorExecution(c liveMirrorCandidate,
	initial, finalQ liveMirrorQuote, meta livePolicyMirrorExecutionMeta, startedAt time.Time,
	limit, requested, filled, fillPrice, fee float64, feeKnown bool,
	feeSource, state, reason string) {
	if strings.TrimSpace(c.ShadowAttemptID) == "" {
		return
	}
	now := time.Now().UTC()
	outcome := strings.TrimSpace(state)
	if outcome == "" {
		outcome = "unknown"
	}
	event := s.executionShadowEvent(c.ShadowAttemptID,
		"counterfactual-execution-terminal", outcome, strings.TrimSpace(reason), now)
	// One attempt has exactly one execution-only comparison terminal. A queue replay or racing
	// recovery sweep therefore becomes an idempotent INSERT OR IGNORE instead of a second result.
	stable := sha256.Sum256([]byte(strings.Join([]string{
		c.ShadowAttemptID, "counterfactual-execution-terminal", livePolicyMirrorModelVersion,
	}, "|")))
	event.EventID = "execev-stable-" + hex.EncodeToString(stable[:16])
	if finalQ.Price > 0 {
		executionShadowApplyQuote(&event, finalQ, now)
	} else if initial.Price > 0 {
		executionShadowApplyQuote(&event, initial, now)
	}
	event.OriginalLimit = executionShadowPricePtr(limit)
	if requested > 0 {
		event.RequestedQty = floatPtrShadow(requested)
	}
	event.ShadowState, event.ShadowReason = outcome, strings.TrimSpace(reason)
	if filled > 0 {
		event.ShadowFilledQty = floatPtrShadow(filled)
		event.ShadowFillPrice = executionShadowPricePtr(fillPrice)
		if feeValue := executionShadowNonnegativePtr(fee); feeKnown && feeValue != nil &&
			strings.TrimSpace(feeSource) != "" {
			event.ShadowFee, event.ShadowFeeSource = feeValue, strings.TrimSpace(feeSource)
		}
	}
	event.Evidence = map[string]any{
		"model_version":            livePolicyMirrorModelVersion,
		"scope":                    "delayed execution mechanics only; no fake portfolio admission or money authority",
		"real_money_authority":     false,
		"cash_path_blocking":       false,
		"fake_portfolio_admission": false,
		"live_terminal_observed":   meta.LiveReceiptSeen,
		"live_terminal_state":      meta.LiveState,
		"live_venue_attempted":     meta.VenueAttempted,
		"live_original_limit":      meta.LiveLimit,
		"delayed_decision_limit":   limit,
		"limit_policy":             "delayed_first_touch",
		"requested_quantity":       requested,
		"time_in_force":            c.ProspectiveTimeInForce,
		"fee_known":                feeKnown,
		"wire_delay_ms":            s.livePolicyMirrorFinalWireDelay().Milliseconds(),
		"trigger_at":               optionalExecutionShadowTimeText(c.At),
		"arbitration_at":           optionalExecutionShadowTimeText(c.ArbitrationAt),
		"live_terminal_at":         optionalExecutionShadowTimeText(meta.LiveTerminalAt),
		"scheduled_at":             optionalExecutionShadowTimeText(meta.ScheduledAt),
		"started_at":               optionalExecutionShadowTimeText(startedAt),
		"completed_at":             optionalExecutionShadowTimeText(now),
		"initial_quote":            liveMirrorQuoteEvidence(initial, startedAt),
		"final_quote":              liveMirrorQuoteEvidence(finalQ, now),
	}
	if !s.enqueueExecutionShadowWrite(executionShadowWrite{event: &event}) {
		// No retained terminal exists, so let the durable gap sweep retry admission. A successful
		// enqueue keeps the process-lifetime claim while the isolated writer handles persistence.
		s.forgetLivePolicyMirrorExecutionScheduled(c.ShadowAttemptID)
	}
}

func (s *Server) executionShadowRecordLiveResult(c liveMirrorCandidate, q liveMirrorQuote,
	reservationID, orderID, state, receiptSource string, requested, filled, fillPrice float64,
	fee float64, feeKnown, authoritative, venueAttempted bool, reason string,
	evidence map[string]any) {
	if c.ShadowAttemptID == "" {
		return
	}
	now := time.Now()
	outcome := state
	if outcome == "" {
		outcome = "rejected-before-venue"
	}
	event := s.executionShadowEvent(c.ShadowAttemptID, "live-terminal", outcome, reason, now)
	executionShadowApplyQuote(&event, q, now)
	event.OriginalLimit = executionShadowPricePtr(q.Price)
	if requested > 0 {
		event.RequestedQty = floatPtrShadow(requested)
	}
	event.LiveReservationID, event.VenueAttempted = reservationID, venueAttempted
	event.VenueOrderID, event.VenueAck = orderID, orderID != ""
	terminalState := state
	if strings.TrimSpace(terminalState) == "" && !venueAttempted {
		terminalState = "not-sent"
	}
	event.LiveState, event.LiveAuthoritative = terminalState, authoritative
	event.LiveReceiptSource = receiptSource
	if filled >= 0 {
		event.LiveFilledQty = floatPtrShadow(filled)
	}
	if filled > 0 && fillPrice > 0 {
		event.LiveFillPrice = executionShadowPricePtr(fillPrice)
		if feeKnown {
			if feeValue := executionShadowNonnegativePtr(fee); feeValue != nil {
				event.LiveFee, event.LiveFeeSource = feeValue,
					firstNonEmpty(receiptSource, "venue-execution-receipt")
			}
		}
	}
	event.Evidence = executionShadowRedactedEvidence(evidence)
	if event.Evidence == nil {
		event.Evidence = make(map[string]any)
	}
	if tif := strings.TrimSpace(c.ProspectiveTimeInForce); tif != "" {
		if _, present := event.Evidence["time_in_force"]; !present {
			event.Evidence["time_in_force"] = tif
		}
	}
	if !s.enqueueExecutionShadowWrite(executionShadowWrite{event: &event}) {
		return
	}
	s.enqueueGenfollowPaperAfterLiveTerminal(event)
	if strings.TrimSpace(event.LiveState) != "" {
		s.executionShadowHandlerTerminals.Store(c.ShadowAttemptID, event.EventID)
	}
	// The realistic comparison is strictly downstream of the exact cash terminal. It consumes
	// only resident full-book snapshots and never delays this order path. Transport-ambiguous or
	// still-pending receipts must wait for a later exact reconciliation event.
	if !executionShadowLiveTerminalComparable(event) {
		return
	}
	comparisonCandidate := c
	if requested > 0 {
		comparisonCandidate.ProspectiveQty = requested
	}
	s.enqueueLivePolicyMirrorCandidateAfterTerminal(
		comparisonCandidate, q, now, terminalState, venueAttempted)
}

func (s *Server) executionShadowHandlerTerminalSeen(attemptID string) bool {
	if s == nil || strings.TrimSpace(attemptID) == "" {
		return false
	}
	_, seen := s.executionShadowHandlerTerminals.Load(strings.TrimSpace(attemptID))
	return seen
}

// executionShadowRecordQueueClear gives callers that remove queued work one exact terminal
// receipt. It deliberately accepts the immutable candidate rather than a ticker lookup.
func (s *Server) executionShadowRecordQueueClear(c liveMirrorCandidate, branch, reason string) {
	if strings.TrimSpace(reason) == "" {
		reason = "live-queue-cleared"
	}
	s.executionShadowRecordDrop(c, "queue-clear", reason, time.Now().UTC(),
		map[string]any{"queue_branch": strings.TrimSpace(branch)})
}

func (s *Server) executionShadowRecordQueueCleanup(c liveMirrorCandidate, branch, reason string) {
	if strings.TrimSpace(reason) == "" {
		reason = "live-queue-cleared"
	}
	s.executionShadowRecordHousekeeping(c, "queue-clear", reason, time.Now().UTC(),
		map[string]any{"queue_branch": strings.TrimSpace(branch)})
}

func (s *Server) executionShadowRecordIntentQueueClear(intent liveSignalIntent, reason string) {
	s.executionShadowRecordQueueClear(liveCandidateFromSignalIntent(intent),
		"live-signal-intent", reason)
}

func (s *Server) enqueueExecutionShadowWrite(write executionShadowWrite) bool {
	return s.enqueueExecutionShadowWriteGroup([]executionShadowWrite{write}, true)
}

func (s *Server) enqueueExecutionShadowWriteNoWake(write executionShadowWrite) {
	s.enqueueExecutionShadowWriteMode(write, false)
}

func (s *Server) enqueueExecutionShadowWriteMode(write executionShadowWrite, wakeNow bool) {
	s.enqueueExecutionShadowWriteGroup([]executionShadowWrite{write}, wakeNow)
}

func executionShadowWriteValid(write executionShadowWrite) bool {
	payloads := 0
	if write.attempt != nil {
		payloads++
	}
	if write.event != nil {
		payloads++
	}
	if write.kalshiTrade != nil {
		payloads++
	}
	if payloads != 1 {
		return false
	}
	return write.event == nil ||
		write.event.ElapsedFromTriggerMS >= 0 ||
		write.event.ElapsedFromTriggerMS == executionShadowElapsedUnknownMS
}

func (s *Server) executionShadowCapacity() int64 {
	capacity := executionShadowPendingMax
	if s.executionShadowPendingCap > 0 {
		capacity = s.executionShadowPendingCap
	}
	return int64(capacity)
}

func (s *Server) executionShadowReserve(records int) bool {
	n := int64(records)
	if n <= 0 {
		return false
	}
	for {
		if s.executionShadowStoppingAtomic.Load() {
			s.executionShadowDropped.Add(uint64(records))
			s.executionShadowDropStopping.Add(uint64(records))
			return false
		}
		current := s.executionShadowOccupancy.Load()
		if current+n > s.executionShadowCapacity() {
			s.executionShadowDropped.Add(uint64(records))
			s.executionShadowDropCapacity.Add(uint64(records))
			return false
		}
		if !s.executionShadowOccupancy.CompareAndSwap(current, current+n) {
			continue
		}
		// Close admission has one atomic boundary. If shutdown won immediately before our
		// reservation, give the slots back; otherwise this group is pre-boundary work and shutdown
		// will wait for it even if its pointer publication follows by a few instructions.
		if s.executionShadowStoppingAtomic.Load() {
			s.executionShadowOccupancy.Add(-n)
			s.executionShadowDropped.Add(uint64(records))
			s.executionShadowDropStopping.Add(uint64(records))
			return false
		}
		return true
	}
}

func (s *Server) executionShadowPushFallback(writes []executionShadowWrite, wakeNow bool) {
	node := &executionShadowIngressNode{writes: writes}
	for {
		head := s.executionShadowFallback.Load()
		node.next = head
		if s.executionShadowFallback.CompareAndSwap(head, node) {
			break
		}
	}
	s.executionShadowRerouted.Add(uint64(len(writes)))
	if wake := s.executionShadowWakeRef.Load(); wake != nil {
		if wakeNow {
			select {
			case wake.ch <- struct{}{}:
			default:
			}
		}
		return
	}
	if wakeNow {
		s.executionShadowWakeRequested.Store(true)
	}
	s.executionShadowScheduleWriterStart()
}

// executionShadowScheduleWriterStart is the cold-start half of the nonblocking ingress. Multiple
// producers and an explicit wake may race here; one helper publishes the worker channel, then
// consumes any wake request that arrived while the mutex was unavailable.
func (s *Server) executionShadowScheduleWriterStart() {
	// A cold bare Server can be contended before its first ordinary enqueue. Recovery happens on
	// another goroutine; the producer never waits for initialization or the worker mutex.
	if s.executionShadowWakeScheduled.CompareAndSwap(false, true) {
		go func() {
			defer s.executionShadowWakeScheduled.Store(false)
			s.executionShadowMu.Lock()
			wake := s.executionShadowStartWriterLocked()
			s.executionShadowMu.Unlock()
			if wake != nil && s.executionShadowWakeRequested.Swap(false) {
				select {
				case wake <- struct{}{}:
				default:
				}
			}
		}()
	}
}

func (s *Server) executionShadowStartWriterLocked() chan struct{} {
	if s.executionShadowWake != nil {
		if s.executionShadowWakeRef.Load() == nil {
			s.executionShadowWakeRef.Store(&executionShadowWakeHandle{
				ch: s.executionShadowWake,
			})
		}
		return s.executionShadowWake
	}
	s.executionShadowWake = make(chan struct{}, 1)
	s.executionShadowDone = make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	s.executionShadowCancel = cancel
	s.executionShadowWakeRef.Store(&executionShadowWakeHandle{ch: s.executionShadowWake})
	go s.runGuarded("execution-shadow-writer", func() {
		s.runExecutionShadowWriter(ctx)
	})
	return s.executionShadowWake
}

// executionShadowWriteGroup preserves attempt/event ordering as one all-or-nothing admission.
// Producers never wait for the worker mutex. The common path appends directly; a collision uses
// the bounded lock-free ingress, so a diagnostics reader or writer batch cannot erase evidence or
// add latency to the real-money path.
func (s *Server) enqueueExecutionShadowWriteGroup(writes []executionShadowWrite,
	wakeNow bool) bool {
	if s == nil || s.store == nil || len(writes) == 0 {
		return false
	}
	// Resolve an unknown elapsed sentinel from a parent carried by this same producer group before
	// admission. If the two clocks are causally impossible, refusing the whole group here preserves
	// attempt+first-event atomicity. Event-only rows whose parent is already durable are resolved by
	// the background writer without adding a database read to the producer path.
	groupTriggers := make(map[string]int64)
	for _, write := range writes {
		if write.attempt != nil {
			groupTriggers[strings.TrimSpace(write.attempt.AttemptID)] =
				write.attempt.TriggerUnixMS
		}
	}
	for i, write := range writes {
		if write.event == nil ||
			write.event.ElapsedFromTriggerMS != executionShadowElapsedUnknownMS {
			continue
		}
		triggerUnixMS, found := groupTriggers[strings.TrimSpace(write.event.AttemptID)]
		if !found {
			continue
		}
		event := *write.event
		if event.At.IsZero() {
			event.At = time.Now().UTC()
		}
		event.ElapsedFromTriggerMS = event.At.UTC().UnixMilli() - triggerUnixMS
		if event.ElapsedFromTriggerMS < 0 {
			event.ElapsedFromTriggerMS = executionShadowElapsedInvalidMS
		}
		writes[i].event = &event
	}
	for _, write := range writes {
		if !executionShadowWriteValid(write) {
			s.executionShadowDropped.Add(uint64(len(writes)))
			s.executionShadowDropInvalid.Add(uint64(len(writes)))
			if s.log != nil {
				attemptID, eventID, stage := "", "", ""
				elapsed := int64(0)
				if write.attempt != nil {
					attemptID = write.attempt.AttemptID
				}
				if write.event != nil {
					attemptID = write.event.AttemptID
					eventID = write.event.EventID
					stage = write.event.Stage
					elapsed = write.event.ElapsedFromTriggerMS
				}
				s.log.Warn("execution-shadow malformed producer group refused before queue",
					"attempt_id", attemptID, "event_id", eventID, "stage", stage,
					"elapsed_from_trigger_ms", elapsed, "group_records", len(writes))
			}
			return false
		}
	}
	if !s.executionShadowReserve(len(writes)) {
		return false
	}
	if !s.executionShadowMu.TryLock() {
		s.executionShadowPushFallback(writes, wakeNow)
		return true
	}
	if s.executionShadowStopping {
		s.executionShadowMu.Unlock()
		s.executionShadowOccupancy.Add(-int64(len(writes)))
		s.executionShadowDropped.Add(uint64(len(writes)))
		s.executionShadowDropStopping.Add(uint64(len(writes)))
		return false
	}
	s.executionShadowStartWriterLocked()
	s.executionShadowPending = append(s.executionShadowPending, writes...)
	wake := s.executionShadowWake
	s.executionShadowMu.Unlock()
	if wakeNow {
		select {
		case wake <- struct{}{}:
		default:
		}
	}
	return true
}

func (s *Server) wakeExecutionShadowWriter() {
	if s == nil {
		return
	}
	if wake := s.executionShadowWakeRef.Load(); wake != nil {
		select {
		case wake.ch <- struct{}{}:
		default:
		}
		return
	}
	if !s.executionShadowMu.TryLock() {
		// Do not lose the only explicit wake while cold initialization owns or waits on the mutex.
		// The scheduled initializer publishes wakeRef first and then consumes this request.
		s.executionShadowWakeRequested.Store(true)
		s.executionShadowScheduleWriterStart()
		return
	}
	wake := s.executionShadowWake
	if wake == nil && s.executionShadowOccupancy.Load() > 0 {
		wake = s.executionShadowStartWriterLocked()
	} else if wake != nil {
		s.executionShadowWakeRef.Store(&executionShadowWakeHandle{ch: wake})
	}
	s.executionShadowMu.Unlock()
	if wake == nil {
		return
	}
	select {
	case wake <- struct{}{}:
	default:
	}
}

func (s *Server) executionShadowDrainFallbackLocked() {
	head := s.executionShadowFallback.Swap(nil)
	if head == nil {
		return
	}
	var fifo *executionShadowIngressNode
	for head != nil {
		next := head.next
		head.next = fifo
		fifo = head
		head = next
	}
	for fifo != nil {
		s.executionShadowPending = append(s.executionShadowPending, fifo.writes...)
		fifo = fifo.next
	}
}

func (s *Server) executionShadowPopBatch() []executionShadowWrite {
	s.executionShadowMu.Lock()
	defer s.executionShadowMu.Unlock()
	s.executionShadowDrainFallbackLocked()
	n := len(s.executionShadowPending)
	if n > executionShadowBatchMax {
		n = executionShadowBatchMax
	}
	if n == 0 {
		return nil
	}
	out := append([]executionShadowWrite(nil), s.executionShadowPending[:n]...)
	clear(s.executionShadowPending[:n])
	s.executionShadowPending = s.executionShadowPending[n:]
	if len(s.executionShadowPending) == 0 {
		s.executionShadowPending = nil
	}
	s.executionShadowInFlight += n
	s.executionShadowInFlightCount.Add(int64(n))
	return out
}

func (s *Server) executionShadowCompleteAndRequeue(n int, front,
	back []executionShadowWrite) {
	if n <= 0 && len(front) == 0 && len(back) == 0 {
		return
	}
	s.executionShadowMu.Lock()
	s.executionShadowInFlight -= n
	if s.executionShadowInFlight < 0 {
		s.executionShadowInFlight = 0
	}
	s.executionShadowInFlightCount.Add(-int64(n))
	retryCount := len(front) + len(back)
	s.executionShadowOccupancy.Add(-int64(n - retryCount))
	pending := make([]executionShadowWrite, 0,
		len(front)+len(s.executionShadowPending)+len(back))
	pending = append(pending, front...)
	pending = append(pending, s.executionShadowPending...)
	pending = append(pending, back...)
	s.executionShadowPending = pending
	s.executionShadowMu.Unlock()
}

func (s *Server) executionShadowCompleteBatch(n int) {
	s.executionShadowCompleteAndRequeue(n, nil, nil)
}

func (s *Server) persistExecutionShadowWrite(ctx context.Context, write executionShadowWrite) error {
	if s == nil || s.store == nil {
		return fmt.Errorf("execution-shadow store unavailable")
	}
	if s.executionShadowStoppingAtomic.Load() {
		return fmt.Errorf("execution-shadow admission is fenced")
	}
	orphanIndexes, invalidIndexes, err := s.persistExecutionShadowBatch(ctx,
		[]executionShadowWrite{write})
	if err != nil {
		return err
	}
	if len(invalidIndexes) > 0 {
		return fmt.Errorf("causally invalid execution-shadow event")
	}
	if len(orphanIndexes) > 0 {
		return fmt.Errorf("execution-shadow parent attempt not committed")
	}
	return nil
}

func (s *Server) persistExecutionShadowBatch(ctx context.Context,
	batch []executionShadowWrite) (map[int]struct{}, map[int]struct{}, error) {
	writeCtx, cancel := context.WithTimeout(ctx, executionShadowWriteTimeout)
	defer cancel()
	batchTriggers := make(map[string]int64)
	for _, write := range batch {
		if write.attempt != nil {
			batchTriggers[strings.TrimSpace(write.attempt.AttemptID)] =
				write.attempt.TriggerUnixMS
		}
	}
	records := make([]storage.ExecutionShadowWrite, 0, len(batch))
	originalIndexes := make([]int, 0, len(batch))
	invalids := make(map[int]struct{})
	for i, write := range batch {
		if write.event != nil &&
			write.event.ElapsedFromTriggerMS == executionShadowElapsedUnknownMS {
			triggerUnixMS, found := batchTriggers[strings.TrimSpace(write.event.AttemptID)]
			if !found {
				parent, parentFound, parentErr := s.store.ExecutionShadowAttemptByID(
					writeCtx, write.event.AttemptID)
				if parentErr != nil {
					return nil, nil, parentErr
				}
				if parentFound {
					triggerUnixMS, found = parent.TriggerUnixMS, true
				}
			}
			if found {
				event := *write.event
				if event.At.IsZero() {
					event.At = time.Now().UTC()
				}
				event.ElapsedFromTriggerMS = event.At.UTC().UnixMilli() - triggerUnixMS
				if event.ElapsedFromTriggerMS < 0 {
					invalids[i] = struct{}{}
					continue
				}
				write.event = &event
			}
		}
		records = append(records,
			storage.ExecutionShadowWrite{Attempt: write.attempt, Event: write.event,
				KalshiTrade: write.kalshiTrade})
		originalIndexes = append(originalIndexes, i)
	}
	if len(records) == 0 {
		return map[int]struct{}{}, invalids, nil
	}
	orphanIndexes, err := s.store.PersistExecutionShadowBatch(writeCtx, records)
	if err != nil {
		return nil, nil, err
	}
	orphans := make(map[int]struct{}, len(orphanIndexes))
	for _, index := range orphanIndexes {
		if index >= 0 && index < len(originalIndexes) {
			orphans[originalIndexes[index]] = struct{}{}
		}
	}
	return orphans, invalids, nil
}

func (s *Server) waitExecutionShadowBatchWindow(ctx context.Context) bool {
	timer := time.NewTimer(executionShadowBatchDelay)
	defer timer.Stop()
	for {
		pending := int(s.executionShadowOccupancy.Load() -
			s.executionShadowInFlightCount.Load())
		wakeRef := s.executionShadowWakeRef.Load()
		if pending >= executionShadowBatchMax {
			return true
		}
		if wakeRef == nil {
			return false
		}
		select {
		case <-ctx.Done():
			return false
		case <-timer.C:
			return true
		case <-wakeRef.ch:
			// A new producer arrived. Keep coalescing until the original 50 ms deadline or max.
		}
	}
}

func (s *Server) runExecutionShadowWriter(ctx context.Context) {
	defer close(s.executionShadowDone)
	backoff := 25 * time.Millisecond
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.executionShadowWake:
		}
		if !s.waitExecutionShadowBatchWindow(ctx) {
			return
		}
		for {
			batch := s.executionShadowPopBatch()
			if len(batch) == 0 {
				break
			}
			orphanIndexes, invalidIndexes, persistErr := s.persistExecutionShadowBatch(ctx, batch)
			if persistErr != nil {
				s.executionShadowCompleteAndRequeue(len(batch), batch, nil)
				if s.log != nil {
					s.log.Warn("execution-shadow writer delayed; exact rows retained",
						"queued", len(batch), "err", persistErr)
				}
				timer := time.NewTimer(backoff)
				select {
				case <-timer.C:
				case <-ctx.Done():
					if !timer.Stop() {
						select {
						case <-timer.C:
						default:
						}
					}
					return
				}
				if backoff < time.Second {
					backoff *= 2
				}
				select {
				case s.executionShadowWake <- struct{}{}:
				default:
				}
				break
			}
			for i := range invalidIndexes {
				write := batch[i]
				total := s.executionShadowDropped.Add(1)
				s.executionShadowDropInvalid.Add(1)
				if s.log != nil && write.event != nil {
					s.log.Warn("execution-shadow derived negative elapsed row refused",
						"attempt_id", write.event.AttemptID,
						"event_id", write.event.EventID,
						"stage", write.event.Stage,
						"dropped_total", total)
				}
			}
			orphans := make([]executionShadowWrite, 0, len(batch))
			for i, write := range batch {
				if _, orphan := orphanIndexes[i]; !orphan {
					continue
				}
				// A LIVE receipt may win the queue race before its detector parent. The isolated
				// transaction commits every ready row and returns only these exact orphans.
				if write.orphanRetries >= executionShadowOrphanMax {
					total := s.executionShadowDropped.Add(1)
					s.executionShadowDropOrphan.Add(1)
					if s.log != nil {
						s.log.Warn("execution-shadow orphan exhausted bounded retries",
							"attempt_id", write.event.AttemptID,
							"event_id", write.event.EventID, "dropped_total", total)
					}
					continue
				}
				write.orphanRetries++
				orphans = append(orphans, write)
			}
			if len(orphans) > 0 {
				s.executionShadowCompleteAndRequeue(len(batch), nil, orphans)
				timer := time.NewTimer(backoff)
				select {
				case <-timer.C:
				case <-ctx.Done():
					if !timer.Stop() {
						select {
						case <-timer.C:
						default:
						}
					}
					return
				}
				if backoff < time.Second {
					backoff *= 2
				}
				select {
				case s.executionShadowWake <- struct{}{}:
				default:
				}
				break
			}
			s.executionShadowCompleteBatch(len(batch))
			backoff = 25 * time.Millisecond
		}
	}
}

// FenceExecutionShadowForBackup permanently closes execution-shadow admission, drains every
// record accepted before that boundary, stops the writer, and marks the writer session clean.
// Call it immediately before the final backup while the Store is still open. The fence is
// intentionally irreversible: a producer racing or arriving after it receives an explicit
// stopping drop and can never extend or enter the snapshot tail. The operation is idempotent, so
// ordinary Server.Shutdown may safely call it again after the backup has been published.
func (s *Server) FenceExecutionShadowForBackup(ctx context.Context) error {
	return s.stopExecutionShadowWriter(ctx)
}

func executionShadowWorkerStopped(done <-chan struct{}) bool {
	if done == nil {
		return false
	}
	select {
	case <-done:
		return true
	default:
		return false
	}
}

func (s *Server) stopExecutionShadowWriter(ctx context.Context) error {
	if s == nil {
		return nil
	}
	s.executionShadowStoppingAtomic.Store(true)
	for {
		s.executionShadowMu.Lock()
		// Close admission before inspecting or draining the queue. Upstream comparison producers
		// are stopped first during normal shutdown; anything racing this boundary must receive an
		// explicit stopping drop instead of extending the drain or entering a queue whose worker
		// may be cancelled when the caller's shutdown deadline expires.
		s.executionShadowStopping = true
		occupancy := s.executionShadowOccupancy.Load()
		// A short earlier fence deadline cancels the worker but deliberately retains every accepted
		// row in memory. Once that worker has published its done receipt, a later fence attempt must
		// replace its dead wake channel and drain the retained tail; otherwise every retry only sends
		// to a channel with no receiver and can never complete.
		if occupancy > 0 && executionShadowWorkerStopped(s.executionShadowDone) {
			s.executionShadowWakeRef.Store(nil)
			s.executionShadowWake = nil
			s.executionShadowDone = nil
			s.executionShadowCancel = nil
		}
		if s.executionShadowWake == nil && occupancy > 0 {
			s.executionShadowStartWriterLocked()
		}
		if s.executionShadowWake == nil {
			s.executionShadowMu.Unlock()
			// A direct isolated-store caller or an older recovered path may have opened a writer
			// session without ever starting the in-memory queue worker. Closing that empty session
			// is still required; otherwise a clean shutdown is reported as an unclean run.
			if s.store == nil {
				return nil
			}
			return s.store.FinishExecutionShadowSession(ctx, s.executionShadowDropped.Load())
		}
		done, cancel, wake := s.executionShadowDone, s.executionShadowCancel, s.executionShadowWake
		if occupancy == 0 {
			s.executionShadowMu.Unlock()
			if cancel != nil {
				cancel()
			}
			select {
			case <-done:
				if s.store == nil {
					return nil
				}
				return s.store.FinishExecutionShadowSession(ctx, s.executionShadowDropped.Load())
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		s.executionShadowMu.Unlock()
		select {
		case wake <- struct{}{}:
		default:
		}
		select {
		case <-time.After(10 * time.Millisecond):
		case <-ctx.Done():
			if cancel != nil {
				cancel()
			}
			return ctx.Err()
		}
	}
}

func executionShadowLatestFills(events []storage.ExecutionShadowEvent) (liveQty, livePrice,
	liveFee float64, liveKnown bool, paperQty, paperPrice, paperFee float64, paperKnown bool,
	shadowQty, shadowPrice, shadowFee float64, shadowKnown bool) {
	hasExecutionOnlyShadow := false
	for _, event := range events {
		if event.Stage == "counterfactual-execution-terminal" {
			hasExecutionOnlyShadow = true
			break
		}
	}
	for _, event := range events {
		if event.LiveFilledQty != nil && *event.LiveFilledQty > 0 && event.LiveFillPrice != nil &&
			event.LiveFee != nil {
			liveQty, livePrice, liveFee, liveKnown =
				*event.LiveFilledQty, *event.LiveFillPrice, *event.LiveFee, true
		}
		if event.PaperFilledQty != nil && *event.PaperFilledQty > 0 && event.PaperFillPrice != nil &&
			event.PaperFee != nil {
			paperQty, paperPrice, paperFee, paperKnown =
				*event.PaperFilledQty, *event.PaperFillPrice, *event.PaperFee, true
		}
		if (!hasExecutionOnlyShadow || event.Stage == "counterfactual-execution-terminal") &&
			event.ShadowFilledQty != nil && *event.ShadowFilledQty > 0 && event.ShadowFillPrice != nil &&
			event.ShadowFee != nil {
			shadowQty, shadowPrice, shadowFee, shadowKnown =
				*event.ShadowFilledQty, *event.ShadowFillPrice, *event.ShadowFee, true
		}
	}
	return
}

// executionShadowLatestZeroFills recognizes only explicit terminal zero-fill evidence. A local
// no-send, Paper rejection, or missing final book is not a zero-dollar observation. The current
// execution-only Shadow terminal supersedes the older optional portfolio mirror just as it does
// in executionShadowLatestFills.
func executionShadowLatestZeroFills(events []storage.ExecutionShadowEvent) (
	liveZero, paperZero, shadowZero bool) {
	hasExecutionOnlyShadow := false
	for _, event := range events {
		if event.Stage == "counterfactual-execution-terminal" {
			hasExecutionOnlyShadow = true
			break
		}
	}
	for _, event := range events {
		if event.LiveAuthoritative && event.LiveFilledQty != nil &&
			strings.TrimSpace(event.LiveState) != "" {
			liveZero = *event.LiveFilledQty == 0
		}
		if event.Stage == "funded-paper-terminal" {
			paperZero = strings.EqualFold(strings.TrimSpace(event.PaperState), "PAPER-ZERO-FILL")
		}
		if (hasExecutionOnlyShadow && event.Stage == "counterfactual-execution-terminal") ||
			(!hasExecutionOnlyShadow && event.Stage == "counterfactual-terminal") {
			state := strings.ToLower(strings.TrimSpace(event.ShadowState))
			if state == "" {
				state = strings.ToLower(strings.TrimSpace(event.Outcome))
			}
			shadowZero = state == "modeled_zero_fill" || state == "zero_fill" ||
				state == "zero-fill" || state == "unfilled"
		}
	}
	return
}

func executionShadowSettlementNet(action, side string, yesValue, qty, price, fee float64) float64 {
	payout := yesValue
	if strings.EqualFold(side, "NO") {
		payout = 1 - yesValue
	}
	if strings.EqualFold(action, "SELL") {
		return qty*(price-payout) - fee
	}
	return qty*(payout-price) - fee
}

func executionShadowSettlementCoverage(events []storage.ExecutionShadowEvent) (
	settled, liveNet, paperNet, shadowNet bool) {
	executionTerminalIndex := -1
	for i, event := range events {
		if event.Stage == "counterfactual-execution-terminal" {
			executionTerminalIndex = i
		}
	}
	for i, event := range events {
		if !event.SettlementKnown {
			continue
		}
		settled = true
		liveNet = liveNet || event.LiveNet != nil
		paperNet = paperNet || event.PaperNet != nil
		// A new execution-only terminal supersedes older fake-portfolio economics. Only a
		// settlement appended after that terminal can cover the canonical comparison lane.
		shadowNet = shadowNet || event.ShadowNet != nil &&
			(executionTerminalIndex < 0 || i > executionTerminalIndex)
	}
	return
}

func executionShadowSettlementEventID(attemptID, settlementHash, outcome string,
	liveNet, paperNet, shadowNet *float64) string {
	netText := func(value *float64) string {
		if value == nil {
			return "unknown"
		}
		return strconv.FormatFloat(*value, 'g', 17, 64)
	}
	sum := sha256.Sum256([]byte(strings.Join([]string{
		strings.TrimSpace(attemptID), strings.TrimSpace(settlementHash),
		strings.TrimSpace(outcome), netText(liveNet), netText(paperNet), netText(shadowNet),
	}, "|")))
	return "execev-settlement-" + hex.EncodeToString(sum[:16])
}

func executionShadowComparisonPending(row storage.ExecutionShadowAttemptView) bool {
	if !strings.EqualFold(row.Attempt.Venue, "kalshi") ||
		!strings.EqualFold(row.Attempt.Action, "BUY") ||
		!strings.EqualFold(row.Attempt.Route, "taker") {
		return false
	}
	passed, liveTerminal, executionTerminal := false, false, false
	for _, event := range row.Events {
		passed = passed || event.Stage == "live-first-preflight" && event.Outcome == "passed"
		liveTerminal = liveTerminal || strings.TrimSpace(event.LiveState) != ""
		executionTerminal = executionTerminal ||
			event.Stage == "counterfactual-execution-terminal"
	}
	return passed && liveTerminal && !executionTerminal
}

// priorityExecutionShadowFundedSettlementAttempts keeps the shared proof ledger current with the
// funded Paper portfolio instead of making a newly closed lot wait behind the entire historical
// open-attempt settlement scan. The JSON close is already durable and carries the immutable shared
// attempt id. Only genuine market settlements qualify; a mark-based stop is portfolio economics,
// not an authoritative market outcome.
func (s *Server) priorityExecutionShadowFundedSettlementAttempts() []string {
	if s == nil {
		return nil
	}
	type candidate struct {
		attemptID string
		settledAt time.Time
	}
	var candidates []candidate
	s.gfBookMu.Lock()
	for _, book := range s.gfLoadLocked().Subs {
		for _, closed := range book.Closed {
			attemptID := strings.TrimSpace(closed.ExecutionShadowAttemptID)
			if attemptID == "" || !strings.EqualFold(strings.TrimSpace(closed.Reason), "settled") {
				continue
			}
			settledAt, err := time.Parse(time.RFC3339Nano, closed.SettledTS)
			if err != nil {
				settledAt, _ = time.Parse(time.RFC3339, closed.SettledTS)
			}
			candidates = append(candidates, candidate{attemptID: attemptID, settledAt: settledAt})
		}
	}
	s.gfBookMu.Unlock()
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].settledAt.Equal(candidates[j].settledAt) {
			return candidates[i].attemptID < candidates[j].attemptID
		}
		return candidates[i].settledAt.After(candidates[j].settledAt)
	})
	out := make([]string, 0, min(len(candidates), executionShadowFundedSettlementPriorityMax))
	seen := make(map[string]struct{}, len(candidates))
	for _, value := range candidates {
		if _, exists := seen[value.attemptID]; exists {
			continue
		}
		seen[value.attemptID] = struct{}{}
		out = append(out, value.attemptID)
		if len(out) == executionShadowFundedSettlementPriorityMax {
			break
		}
	}
	return out
}

func (s *Server) enqueueExecutionShadowSettlements(
	ctx context.Context,
	rows []storage.ExecutionShadowAttemptView,
) {
	byVenue := make(map[string][]string)
	for _, row := range rows {
		byVenue[row.Attempt.Venue] = append(byVenue[row.Attempt.Venue], row.Attempt.Ticker)
	}
	receipts := make(map[string]storage.VenueSettlementReceipt)
	for venue, tickers := range byVenue {
		batch, readErr := s.store.VenueSettlementsForTickers(ctx, venue, tickers)
		if readErr != nil {
			continue
		}
		for ticker, receipt := range batch {
			receipts[venue+"\x00"+ticker] = receipt
		}
	}
	for _, row := range rows {
		// Recovery is asynchronous and runs earlier in this same settlement pass. Do not let an
		// older fake-portfolio fill settle into the canonical Shadow P&L while its execution-only
		// terminal is still queued.
		if executionShadowComparisonPending(row) {
			continue
		}
		receipt, ok := receipts[row.Attempt.Venue+"\x00"+row.Attempt.Ticker]
		if !ok {
			continue
		}
		lq, lp, lf, lk, pq, pp, pf, pk, sq, sp, sf, sk :=
			executionShadowLatestFills(row.Events)
		liveZero, paperZero, shadowZero := executionShadowLatestZeroFills(row.Events)
		alreadySettled, liveNetKnown, paperNetKnown, shadowNetKnown :=
			executionShadowSettlementCoverage(row.Events)
		s.executionShadowRememberTrigger(row.Attempt.AttemptID, row.Attempt.TriggerUnixMS)
		outcome, reason := "settled", ""
		if alreadySettled {
			outcome = "net-backfill"
			reason = "late exact lane fee completed settlement economics"
		}
		// Event.At is when this process observed and joined the authoritative result. The venue's
		// earlier resolution clock belongs in SettledAt. Backfilled detector attempts can be newer
		// than ResolvedAt; using that historical venue clock as the ledger event clock produced a
		// negative trigger-relative duration and pinned the entire retained-writer batch forever.
		observedAt := time.Now().UTC()
		event := s.executionShadowEvent(row.Attempt.AttemptID, "settlement",
			outcome, reason, observedAt)
		event.SettlementKnown = true
		event.SettlementValue = floatPtrShadow(receipt.YesValue)
		event.SettledAt, event.SettlementSource, event.SettlementHash =
			receipt.ResolvedAt, receipt.SourceArtifact, receipt.Hash
		if lk && !liveNetKnown {
			event.LiveNet = floatPtrShadow(executionShadowSettlementNet(
				row.Attempt.Action, row.Attempt.Side, receipt.YesValue, lq, lp, lf))
		} else if liveZero && !liveNetKnown {
			event.LiveNet = floatPtrShadow(0)
		}
		if pk && !paperNetKnown {
			event.PaperNet = floatPtrShadow(executionShadowSettlementNet(
				row.Attempt.Action, row.Attempt.Side, receipt.YesValue, pq, pp, pf))
		} else if paperZero && !paperNetKnown {
			event.PaperNet = floatPtrShadow(0)
		}
		if sk && !shadowNetKnown {
			event.ShadowNet = floatPtrShadow(executionShadowSettlementNet(
				row.Attempt.Action, row.Attempt.Side, receipt.YesValue, sq, sp, sf))
		} else if shadowZero && !shadowNetKnown {
			event.ShadowNet = floatPtrShadow(0)
		}
		if alreadySettled && event.LiveNet == nil && event.PaperNet == nil && event.ShadowNet == nil {
			continue
		}
		if !alreadySettled && event.LiveNet == nil && event.PaperNet == nil && event.ShadowNet == nil {
			event.Reason = "authoritative outcome joined to terminal attempt; unobserved lane economics remain unknown"
		}
		event.EventID = executionShadowSettlementEventID(row.Attempt.AttemptID,
			receipt.Hash, event.Outcome, event.LiveNet, event.PaperNet, event.ShadowNet)
		// Settlement backfills use the retained isolated writer too. Direct writes here previously
		// bypassed retry/accounting and could leave the writer session falsely unclean at shutdown.
		if !s.enqueueExecutionShadowWrite(executionShadowWrite{event: &event}) && s.log != nil {
			s.log.Warn("execution-shadow settlement enqueue dropped; fair scan will retry next cycle",
				"attempt_id", row.Attempt.AttemptID, "sequence", row.Sequence)
		}
	}
}

func executionShadowStableRiskEventID(attemptID, reservationID, stage, suffix string) string {
	sum := sha256.Sum256([]byte(strings.Join([]string{
		strings.TrimSpace(attemptID), strings.TrimSpace(reservationID),
		strings.TrimSpace(stage), strings.TrimSpace(suffix),
	}, "|")))
	return "execev-risk-" + hex.EncodeToString(sum[:16])
}

// executionShadowRiskLineageEvents reconstructs only immutable joins. It never guesses by ticker
// or clock: the attempt ID comes from the normalized pre-send reservation, while submit identity
// comes from that reservation's append-only submit_started receipt.
func executionShadowRiskLineageEvents(row storage.LivePendingRiskReservation) []storage.ExecutionShadowEvent {
	attemptID := strings.TrimSpace(row.Intent.ExecutionShadowAttemptID)
	reservationID := strings.TrimSpace(row.Intent.ReservationID)
	if attemptID == "" || reservationID == "" || row.Intent.Created.IsZero() {
		return nil
	}
	baseEvidence := map[string]any{
		"execution_shadow_attempt_id": attemptID,
		"live_reservation_id":         reservationID,
		"dispatch_source":             row.Intent.DispatchSource,
		"system_id":                   row.Intent.SystemID,
		"route":                       row.Intent.Route,
		"recovery_join":               "normalized-live-pending-risk-intent",
	}
	reservation := storage.ExecutionShadowEvent{
		EventID: executionShadowStableRiskEventID(attemptID, reservationID,
			"live-risk-reservation", "v1"),
		AttemptID: attemptID, At: row.Intent.Created.UTC(), ElapsedFromTriggerMS: -1,
		Stage: "live-risk-reservation", Outcome: "reserved",
		Reason:            "durable pre-send money reservation names the exact detector opportunity",
		LiveReservationID: reservationID, LiveReceiptSource: "live_pending_risk_intents",
		Evidence: baseEvidence,
	}
	if len(row.Legs) == 1 {
		reservation.OriginalLimit = executionShadowPricePtr(row.Legs[0].LimitPrice)
		reservation.RequestedQty = executionShadowNonnegativePtr(row.Legs[0].Quantity)
	}
	out := []storage.ExecutionShadowEvent{reservation}
	for _, riskEvent := range row.Events {
		if riskEvent.EventType != storage.LivePendingRiskSubmitStarted || riskEvent.Observed.IsZero() {
			continue
		}
		evidence := make(map[string]any, len(baseEvidence)+8)
		eventVariant := riskEvent.AttemptKey
		enrichedPrePOST := false
		if raw := strings.TrimSpace(riskEvent.EvidenceJSON); raw != "" {
			var persisted map[string]any
			if err := json.Unmarshal([]byte(raw), &persisted); err == nil &&
				fmt.Sprint(persisted["pre_post_schema"]) == executionShadowPrePOSTSchema {
				for key, value := range executionShadowRedactedEvidence(persisted) {
					evidence[key] = value
				}
				eventVariant += "|" + executionShadowPrePOSTSchema
				enrichedPrePOST = true
			}
		}
		// Durable reservation identity wins over any same-named field in the evidence payload.
		for key, value := range baseEvidence {
			evidence[key] = value
		}
		evidence["pending_risk_attempt_key"] = riskEvent.AttemptKey
		evidence["pending_risk_event_id"] = riskEvent.ID
		evidence["pending_risk_event_sequence"] = riskEvent.Sequence
		reason := "durable venue-mutation boundary rejoins the exact detector opportunity"
		venueAttempted := true
		if enrichedPrePOST {
			evidence["venue_attempt_state"] = "possible-unconfirmed"
			reason = "durable pre-transport boundary rejoins the exact detector opportunity; " +
				"venue receipt is not yet confirmed"
			venueAttempted = false
		}
		submit := storage.ExecutionShadowEvent{
			EventID: executionShadowStableRiskEventID(attemptID, reservationID,
				"live-risk-submit", eventVariant),
			AttemptID: attemptID, At: riskEvent.Observed.UTC(), ElapsedFromTriggerMS: -1,
			Stage: "live-risk-submit", Outcome: "submit-started",
			Reason:            reason,
			LiveReservationID: reservationID, VenueAttempted: venueAttempted,
			LiveReceiptSource: "live_pending_risk_events", Evidence: evidence,
		}
		executionShadowApplyDurablePrePOST(&submit, evidence)
		if len(row.Legs) == 1 {
			if submit.OriginalLimit == nil {
				submit.OriginalLimit = executionShadowPricePtr(row.Legs[0].LimitPrice)
			}
			if submit.RequestedQty == nil {
				submit.RequestedQty = executionShadowNonnegativePtr(row.Legs[0].Quantity)
			}
		}
		out = append(out, submit)
	}
	return out
}

func executionShadowRiskReceiptAt(row storage.LivePendingRiskReservation, state string,
	qty, price, fee float64) time.Time {
	for i := len(row.Events) - 1; i >= 0; i-- {
		event := row.Events[i]
		switch state {
		case "terminal_unfilled":
			if event.EventType == storage.LivePendingRiskTerminalUnfilled {
				return event.Observed
			}
		case "clean_rejected":
			if event.EventType == storage.LivePendingRiskCleanRejected {
				return event.Observed
			}
		case "filled":
			if event.EventType == storage.LivePendingRiskFillSeen &&
				math.Abs(event.FilledQty-qty) <= 1e-9 && math.Abs(event.AveragePrice-price) <= 1e-9 &&
				math.Abs(event.FeeTotal-fee) <= 1e-9 {
				return event.Observed
			}
		}
	}
	return row.Intent.Created
}

func executionShadowExactRiskEvent(attemptID string, row storage.LivePendingRiskReservation) (
	storage.ExecutionShadowEvent, bool) {
	state, qty, price, fee, known := livePendingExecution(row)
	if !known || strings.TrimSpace(attemptID) == "" {
		return storage.ExecutionShadowEvent{}, false
	}
	receiptAt := executionShadowRiskReceiptAt(row, state, qty, price, fee)
	suffix := fmt.Sprintf("%s|%.9f|%.9f|%.9f", state, qty, price, fee)
	event := storage.ExecutionShadowEvent{
		EventID: executionShadowStableRiskEventID(attemptID, row.Intent.ReservationID,
			"live-exact-reconcile", suffix),
		AttemptID: attemptID, At: receiptAt.UTC(), ElapsedFromTriggerMS: -1,
		Stage: "live-exact-reconcile", Outcome: state,
		Reason:            "durable order-scoped terminal execution and cumulative fee receipt",
		LiveReservationID: row.Intent.ReservationID, VenueAttempted: true,
		LiveState: state, LiveAuthoritative: true,
		LiveFilledQty: floatPtrShadow(qty), LiveFee: floatPtrShadow(fee),
		LiveFeeSource:     "durable-pending-risk-order-scoped-terminal",
		LiveReceiptSource: "live_pending_risk_events",
		Evidence: map[string]any{
			"execution_shadow_attempt_id": attemptID,
			"live_reservation_id":         row.Intent.ReservationID,
			"restart_join":                "normalized-live-pending-risk-intent",
		},
	}
	if qty > 0 {
		event.LiveFillPrice = floatPtrShadow(price)
	}
	return event, true
}

func (s *Server) appendExecutionShadowRecoveryEvent(ctx context.Context,
	event storage.ExecutionShadowEvent) (bool, bool) {
	inserted, err := s.store.AppendExecutionShadowEvent(ctx, event)
	if err == nil {
		return inserted, true
	}
	// The detector parent may still be in the isolated writer queue during a live process. Retain
	// the recovery row there; after a real restart its parent is already durable and the direct path
	// above is normally used.
	return false, s.enqueueExecutionShadowWrite(executionShadowWrite{event: &event})
}

func (s *Server) reconcileExecutionShadowLiveReceipts(ctx context.Context) {
	lineageRows, lineageErr := s.store.ListLivePendingRisksWithExecutionShadowAttempts(ctx, 1000)
	if lineageErr == nil {
		for _, risk := range lineageRows {
			attempt, found, attemptErr := s.store.ExecutionShadowAttemptByID(
				ctx, risk.Intent.ExecutionShadowAttemptID)
			if attemptErr != nil || !found {
				continue
			}
			s.executionShadowRememberTrigger(attempt.AttemptID, attempt.TriggerUnixMS)
			for _, event := range executionShadowRiskLineageEvents(risk) {
				event.ElapsedFromTriggerMS = event.At.UnixMilli() - attempt.TriggerUnixMS
				if event.ElapsedFromTriggerMS < 0 {
					continue
				}
				_, _ = s.appendExecutionShadowRecoveryEvent(ctx, event)
			}
		}
	}
	rows, err := s.store.ListExecutionShadowLiveReceiptPending(ctx, 256)
	if err != nil || len(rows) == 0 {
		return
	}
	ids := make([]string, 0, len(rows))
	byAttempt := make(map[string]string, len(rows))
	for _, row := range rows {
		for _, event := range row.Events {
			if event.LiveReservationID != "" {
				byAttempt[row.Attempt.AttemptID] = event.LiveReservationID
			}
		}
	}
	for _, id := range byAttempt {
		ids = append(ids, id)
	}
	risks, err := s.store.LivePendingRiskExecutionsByID(ctx, ids)
	if err != nil {
		return
	}
	for _, row := range rows {
		riskID := byAttempt[row.Attempt.AttemptID]
		risk, ok := risks[riskID]
		if !ok {
			continue
		}
		// The bounded execution projection intentionally omits the full intent. Restore only the two
		// immutable join keys already proved by this exact attempt->reservation lookup.
		risk.Intent.ReservationID = riskID
		risk.Intent.ExecutionShadowAttemptID = row.Attempt.AttemptID
		event, known := executionShadowExactRiskEvent(row.Attempt.AttemptID, risk)
		if !known {
			continue
		}
		event.ElapsedFromTriggerMS = event.At.UnixMilli() - row.Attempt.TriggerUnixMS
		if event.ElapsedFromTriggerMS < 0 {
			continue
		}
		inserted, retained := s.appendExecutionShadowRecoveryEvent(ctx, event)
		if inserted {
			s.enqueueGenfollowPaperAfterLiveTerminal(event)
		} else if retained {
			// A queued retry is durable-in-memory but its insertion result is not yet known. The funded
			// Paper recovery sweep will consume it after persistence, avoiding duplicate scheduling.
			continue
		}
	}
}

func (s *Server) loadExecutionShadowSettlementCursor(
	ctx context.Context,
	maxSequence int64,
) executionShadowSettlementCursor {
	var state executionShadowSettlementCursor
	raw, ok, err := s.store.ExecutionShadowMeta(ctx, executionShadowSettlementCursorMetaKey)
	if err != nil {
		if s.log != nil {
			s.log.Warn("execution-shadow settlement cursor read failed", "err", err)
		}
		return state
	}
	if ok && json.Unmarshal([]byte(raw), &state) != nil {
		if s.log != nil {
			s.log.Warn("execution-shadow settlement cursor was invalid; restarting fair scan")
		}
		return executionShadowSettlementCursor{}
	}
	if state.AfterSequence < 0 || state.ThroughSequence < 0 ||
		state.AfterSequence > state.ThroughSequence ||
		state.ThroughSequence > maxSequence {
		if s.log != nil {
			s.log.Warn("execution-shadow settlement cursor was outside the restored ledger; restarting fair scan",
				"after_sequence", state.AfterSequence,
				"through_sequence", state.ThroughSequence,
				"current_max_sequence", maxSequence)
		}
		return executionShadowSettlementCursor{}
	}
	return state
}

func (s *Server) persistExecutionShadowSettlementCursor(
	ctx context.Context,
	state executionShadowSettlementCursor,
) {
	raw, err := json.Marshal(state)
	if err != nil {
		return
	}
	writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	if err = s.store.SetExecutionShadowMeta(writeCtx,
		executionShadowSettlementCursorMetaKey, string(raw)); err != nil && s.log != nil {
		s.log.Warn("execution-shadow settlement cursor write failed", "err", err)
	}
}

func (s *Server) settleExecutionShadowLedger(ctx context.Context) {
	if s == nil || s.store == nil {
		return
	}
	s.executionShadowSettleMu.Lock()
	defer s.executionShadowSettleMu.Unlock()
	if !s.waitForLiveDispatchShadowQuiet(ctx) {
		return
	}
	s.reconcileExecutionShadowLiveReceipts(ctx)
	// Funded Paper closes are the operator-facing portfolio truth. Join their exact shared attempt
	// ids first so a fresh realized loss cannot remain absent from the research ledger while the
	// fair historical cursor works through tens of thousands of older unresolved attempts.
	priorityIDs := s.priorityExecutionShadowFundedSettlementAttempts()
	if len(priorityIDs) > 0 {
		pendingIDs, pendingErr :=
			s.store.ExecutionShadowFundedSettlementPendingIDs(ctx, priorityIDs)
		if pendingErr == nil && len(pendingIDs) > 0 {
			priorityRows, priorityErr := s.store.ExecutionShadowAttemptsByIDs(ctx, pendingIDs)
			if priorityErr == nil {
				ordered := make([]storage.ExecutionShadowAttemptView, 0, len(pendingIDs))
				for _, attemptID := range pendingIDs {
					if row, ok := priorityRows[attemptID]; ok {
						ordered = append(ordered, row)
					}
				}
				s.enqueueExecutionShadowSettlements(ctx, ordered)
			}
		}
	}
	watermark, err := s.store.CaptureExecutionShadowWatermark(ctx)
	if err != nil || watermark.AttemptSequence == 0 {
		return
	}
	cursor := s.loadExecutionShadowSettlementCursor(ctx, watermark.AttemptSequence)
	if cursor.ThroughSequence == 0 || cursor.AfterSequence >= cursor.ThroughSequence {
		cursor = executionShadowSettlementCursor{ThroughSequence: watermark.AttemptSequence}
	}
	rows, err := s.store.ListOpenExecutionShadowAttemptsBySequence(ctx,
		cursor.AfterSequence, cursor.ThroughSequence, executionShadowSettlementBatch)
	if err != nil || len(rows) == 0 {
		if err == nil {
			// This frozen prefix is complete. New attempts and any old attempt that becomes
			// settlement-eligible after its turn are picked up from sequence zero next cycle.
			cursor.AfterSequence = cursor.ThroughSequence
			s.persistExecutionShadowSettlementCursor(ctx, cursor)
		}
		return
	}
	s.enqueueExecutionShadowSettlements(ctx, rows)
	cursor.AfterSequence = rows[len(rows)-1].Sequence
	if len(rows) < executionShadowSettlementBatch {
		// The sequence query exhausted every currently eligible row through the frozen watermark,
		// even when closed/noneligible sequence numbers existed between returned rows.
		cursor.AfterSequence = cursor.ThroughSequence
	}
	s.persistExecutionShadowSettlementCursor(ctx, cursor)
}

func executionShadowSummary(rows []storage.ExecutionShadowAttemptView) map[string]any {
	summary := map[string]any{
		"attempts": len(rows), "live_attempted": 0, "live_filled": 0,
		"paper_filled": 0, "counterfactual_filled": 0, "settled": 0,
		"live_net_usd": 0.0, "paper_net_usd": 0.0, "counterfactual_net_usd": 0.0,
		"execution_comparison_modeled_fill": 0, "execution_comparison_modeled_zero_fill": 0,
		"execution_comparison_not_observed": 0, "portfolio_mirror_filled": 0,
	}
	for _, row := range rows {
		liveAttempted, liveFilled, paperFilled, shadowFilled, settled := false, false, false, false, false
		hasExecutionOnlyShadow := false
		executionTerminalIndex := -1
		executionFill, executionZero, executionMissing, portfolioFilled :=
			false, false, false, false
		for i, event := range row.Events {
			hasExecutionOnlyShadow = hasExecutionOnlyShadow ||
				event.Stage == "counterfactual-execution-terminal"
			if event.Stage == "counterfactual-execution-terminal" {
				executionTerminalIndex = i
				executionFill = executionFill || event.Outcome == livePolicyMirrorModeledFill
				executionZero = executionZero || event.Outcome == livePolicyMirrorModeledZeroFill
				executionMissing = executionMissing || event.Outcome == livePolicyMirrorNotObserved
			}
			if event.Stage == "counterfactual-terminal" {
				portfolioFilled = portfolioFilled ||
					event.ShadowFilledQty != nil && *event.ShadowFilledQty > 0
			}
		}
		for i, event := range row.Events {
			liveAttempted = liveAttempted || event.VenueAttempted
			liveFilled = liveFilled || event.LiveFilledQty != nil && *event.LiveFilledQty > 0
			paperFilled = paperFilled || event.PaperFilledQty != nil && *event.PaperFilledQty > 0
			shadowFilled = shadowFilled ||
				(!hasExecutionOnlyShadow || event.Stage == "counterfactual-execution-terminal") &&
					event.ShadowFilledQty != nil && *event.ShadowFilledQty > 0
			if event.SettlementKnown {
				settled = true
				if event.LiveNet != nil {
					summary["live_net_usd"] = summary["live_net_usd"].(float64) + *event.LiveNet
				}
				if event.PaperNet != nil {
					summary["paper_net_usd"] = summary["paper_net_usd"].(float64) + *event.PaperNet
				}
				if event.ShadowNet != nil &&
					(executionTerminalIndex < 0 || i > executionTerminalIndex) {
					summary["counterfactual_net_usd"] =
						summary["counterfactual_net_usd"].(float64) + *event.ShadowNet
				}
			}
		}
		for key, value := range map[string]bool{
			"live_attempted": liveAttempted, "live_filled": liveFilled,
			"paper_filled": paperFilled, "counterfactual_filled": shadowFilled,
			"settled": settled, "execution_comparison_modeled_fill": executionFill,
			"execution_comparison_modeled_zero_fill": executionZero,
			"execution_comparison_not_observed":      executionMissing,
			"portfolio_mirror_filled":                portfolioFilled,
		} {
			if value {
				summary[key] = summary[key].(int) + 1
			}
		}
	}
	return summary
}

func (s *Server) handleExecutionShadow(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()
	rows, err := s.store.ListExecutionShadowAttempts(ctx, limit)
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": err.Error()})
		return
	}
	// Historical rows may predate source redaction. The diagnostic API is never a capability
	// recovery surface, so redact again at the response boundary as defense in depth.
	for i := range rows {
		rows[i].Attempt.SignalSource =
			executionShadowRedactedSource(rows[i].Attempt.SignalSource)
		for j := range rows[i].Events {
			rows[i].Events[j].Evidence =
				executionShadowRedactedEvidence(rows[i].Events[j].Evidence)
		}
	}
	occupancy := s.executionShadowOccupancy.Load()
	inFlight := s.executionShadowInFlightCount.Load()
	pending := max(occupancy-inFlight, 0)
	capacity := s.executionShadowCapacity()
	stopping := s.executionShadowStoppingAtomic.Load()
	storeStatus := s.store.ExecutionShadowStatus(ctx)
	writeJSON(w, http.StatusOK, map[string]any{
		"summary": executionShadowSummary(rows), "attempts": rows,
		"generated_at":      time.Now().UTC().Format(time.RFC3339Nano),
		"telemetry_dropped": s.executionShadowDropped.Load(),
		"telemetry_dropped_by_reason": map[string]uint64{
			"invalid":    s.executionShadowDropInvalid.Load(),
			"contention": s.executionShadowDropContention.Load(),
			"stopping":   s.executionShadowDropStopping.Load(),
			"capacity":   s.executionShadowDropCapacity.Load(),
			"orphan":     s.executionShadowDropOrphan.Load(),
		},
		"telemetry_pending": pending, "telemetry_in_flight": inFlight,
		"telemetry_capacity": capacity, "telemetry_stopping": stopping,
		"telemetry_contention_rerouted": s.executionShadowRerouted.Load(),
		"storage":                       storeStatus,
		"join_contract":                 "attempt.signal_decision_id + attempt.attempt_id join the shared detector branch to LIVE, funded Paper, counterfactual IOC, and settlement; ticker-only joins are forbidden",
	})
}
