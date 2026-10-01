package server

// Durable LIVE-candidate rejection receipts.
//
// SQLite can legitimately wait behind a research writer. A real-money signal/preflight path must
// not inherit that delay, so it only appends to the operator ring and offers one immutable receipt
// to this Server's bounded audit queue. One worker batches those rows into a single transaction.
// Economic and final-dispatch decisions are never deduplicated. The two known scheduler
// coalescing reasons are housekeeping rather than independent order decisions, so they are
// retained as counted summaries instead of being allowed to crowd exact decisions out.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

const (
	liveCandidateAuditQueueCapacity = 4096
	liveCandidateAuditBatchMax      = 64
	liveCandidateAuditAggregateFor  = time.Second
	liveCandidateAuditSampleMax     = 8
)

type liveCandidateAuditReceipt struct {
	row    map[string]any
	event  string
	system string
	logged time.Time
}

type liveCandidateAuditQueueItem struct {
	receipt *liveCandidateAuditReceipt
	barrier chan error
	stop    bool
}

type liveCandidateAuditAggregate struct {
	row           map[string]any
	event         string
	system        string
	first         time.Time
	last          time.Time
	count         uint64
	tickers       map[string]struct{}
	sampleTickers []string
}

func liveCandidateAuditAggregateReason(reason string) bool {
	switch strings.TrimSpace(reason) {
	case "duplicate-coalesced-live-signal-intent",
		"duplicate-coalesced-live-candidate":
		return true
	default:
		return false
	}
}

func liveCandidateAuditAggregateKey(receipt liveCandidateAuditReceipt) string {
	return strings.Join([]string{
		receipt.event,
		fmt.Sprint(receipt.row["stage"]),
		fmt.Sprint(receipt.row["reason"]),
		fmt.Sprint(receipt.row["venue"]),
		receipt.system,
		fmt.Sprint(receipt.row["action"]),
		fmt.Sprint(receipt.row["side"]),
	}, "\x1f")
}

func cloneLiveCandidateAuditRow(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func (s *Server) liveOperatorAutoOn() bool {
	if s == nil {
		return false
	}
	s.liveMu.Lock()
	on := s.liveArmed && s.liveAuto
	s.liveMu.Unlock()
	return on
}

func liveCandidateDropSystem(c liveMirrorCandidate) string {
	if family := liveMirrorFamily(c); family != "" {
		return family
	}
	if family := strings.ToLower(strings.TrimSpace(c.Family)); family != "" {
		return family
	}
	source := strings.ToLower(strings.TrimSpace(c.Source))
	for _, prefix := range []string{"auto-cons-", "book:", "auto-"} {
		if strings.HasPrefix(source, prefix) {
			source = strings.TrimPrefix(source, prefix)
			break
		}
	}
	if source == "" {
		return "unknown-system"
	}
	return source
}

func (s *Server) startLiveCandidateAuditLocked() bool {
	if s.liveCandidateAuditCh != nil {
		return true
	}
	if s.liveCandidateAuditStopping.Load() || (s.store == nil && s.liveCandidateAuditWrite == nil) {
		return false
	}
	capacity := s.liveCandidateAuditCapacity
	if capacity <= 0 {
		capacity = liveCandidateAuditQueueCapacity
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.liveCandidateAuditCh = make(chan liveCandidateAuditQueueItem, capacity)
	s.liveCandidateAuditOverflowWake = make(chan struct{}, 1)
	s.liveCandidateAuditDone = make(chan struct{})
	s.liveCandidateAuditCancel = cancel
	go s.runLiveCandidateAuditWorker(ctx)
	return true
}

func (s *Server) enqueueLiveCandidateAudit(receipt liveCandidateAuditReceipt) bool {
	s.liveCandidateAuditStateMu.Lock()
	defer s.liveCandidateAuditStateMu.Unlock()
	if s.liveCandidateAuditStopping.Load() || !s.startLiveCandidateAuditLocked() {
		return false
	}
	select {
	case s.liveCandidateAuditCh <- liveCandidateAuditQueueItem{receipt: &receipt}:
		return true
	default:
		return false
	}
}

// aggregateLiveCandidateAudit retains the volume and timing of scheduler housekeeping without
// putting one queue item, execution-shadow event, and operator-ring row behind every duplicate.
// It holds only in-memory locks and runs exclusively on an already-rejected duplicate path; it
// performs no disk, SQLite, network, account, book, or order work.
func (s *Server) aggregateLiveCandidateAudit(receipt liveCandidateAuditReceipt) bool {
	s.liveCandidateAuditStateMu.Lock()
	if s.liveCandidateAuditStopping.Load() || !s.startLiveCandidateAuditLocked() {
		s.liveCandidateAuditStateMu.Unlock()
		return false
	}
	key := liveCandidateAuditAggregateKey(receipt)
	ticker := strings.TrimSpace(fmt.Sprint(receipt.row["ticker"]))
	s.liveCandidateAuditAggregateMu.Lock()
	aggregate := s.liveCandidateAuditAggregates[key]
	if aggregate == nil {
		aggregate = &liveCandidateAuditAggregate{
			row: cloneLiveCandidateAuditRow(receipt.row), event: receipt.event,
			system: receipt.system, first: receipt.logged, last: receipt.logged,
			tickers: map[string]struct{}{},
		}
		if s.liveCandidateAuditAggregates == nil {
			s.liveCandidateAuditAggregates = map[string]*liveCandidateAuditAggregate{}
		}
		s.liveCandidateAuditAggregates[key] = aggregate
	}
	aggregate.count++
	if receipt.logged.Before(aggregate.first) {
		aggregate.first = receipt.logged
	}
	if receipt.logged.After(aggregate.last) {
		aggregate.last = receipt.logged
		aggregate.row["logged_at"] = receipt.logged.UTC().Format(time.RFC3339Nano)
		aggregate.row["candidate_at"] = receipt.row["candidate_at"]
		aggregate.row["age_ms"] = receipt.row["age_ms"]
	}
	if ticker != "" {
		if _, seen := aggregate.tickers[ticker]; !seen {
			aggregate.tickers[ticker] = struct{}{}
			if len(aggregate.sampleTickers) < liveCandidateAuditSampleMax {
				aggregate.sampleTickers = append(aggregate.sampleTickers, ticker)
			}
		}
	}
	s.liveCandidateAuditAggregateMu.Unlock()
	s.liveCandidateAuditStateMu.Unlock()
	return true
}

func (s *Server) drainLiveCandidateAuditAggregates() []liveCandidateAuditReceipt {
	s.liveCandidateAuditAggregateMu.Lock()
	aggregates := s.liveCandidateAuditAggregates
	s.liveCandidateAuditAggregates = nil
	s.liveCandidateAuditAggregateMu.Unlock()
	if len(aggregates) == 0 {
		return nil
	}
	keys := make([]string, 0, len(aggregates))
	for key := range aggregates {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]liveCandidateAuditReceipt, 0, len(keys))
	for _, key := range keys {
		aggregate := aggregates[key]
		row := cloneLiveCandidateAuditRow(aggregate.row)
		row["receipt_kind"] = "duplicate-coalesced-summary"
		row["occurrence_count"] = aggregate.count
		row["distinct_ticker_count"] = len(aggregate.tickers)
		row["sample_tickers"] = append([]string(nil), aggregate.sampleTickers...)
		row["first_logged_at"] = aggregate.first.UTC().Format(time.RFC3339Nano)
		row["last_logged_at"] = aggregate.last.UTC().Format(time.RFC3339Nano)
		row["logged_at"] = aggregate.last.UTC().Format(time.RFC3339Nano)
		row["meaning"] = "counted scheduler duplicates; not independent executable order decisions"
		// A summary spans more than one ticker, so never let the representative ticker be mistaken
		// for the whole cohort. Exact bounded examples remain available in sample_tickers.
		row["ticker"] = "[coalesced-summary]"
		receipt := liveCandidateAuditReceipt{
			row: row, event: aggregate.event, system: aggregate.system, logged: aggregate.last,
		}
		out = append(out, receipt)
		s.liveLogAdd(row)
	}
	return out
}

func (s *Server) writeLiveCandidateAuditBatch(ctx context.Context, entries []storage.AuditEntry) error {
	if len(entries) == 0 {
		return nil
	}
	if s.liveCandidateAuditWrite != nil {
		return s.liveCandidateAuditWrite(ctx, entries)
	}
	if s.store == nil {
		return errors.New("live-candidate audit store unavailable")
	}
	return s.store.AuditBatch(ctx, entries)
}

func liveCandidateAuditEntries(receipts []liveCandidateAuditReceipt) []storage.AuditEntry {
	entries := make([]storage.AuditEntry, 0, len(receipts))
	for _, receipt := range receipts {
		detail, err := json.Marshal(receipt.row)
		if err != nil {
			// Caller extras should be JSON-shaped, but a bad diagnostic value must not wedge the
			// whole queue. Preserve identity plus the serialization failure as an explicit row.
			detail, _ = json.Marshal(map[string]any{
				"event": receipt.event, "system": receipt.system,
				"error": "candidate receipt JSON: " + err.Error(),
			})
		}
		entries = append(entries, storage.AuditEntry{
			TS: receipt.logged.UTC().Format(time.RFC3339Nano), Level: "info",
			Category: "live-candidate-drop", Message: receipt.event + " " + receipt.system,
			Detail: string(detail),
		})
	}
	return entries
}

// persistLiveCandidateAudit retries a full all-or-nothing batch until SQLite recovers or shutdown
// cancels the worker. The hot producer never waits for any of these retries.
func (s *Server) persistLiveCandidateAudit(ctx context.Context, entries []storage.AuditEntry) error {
	if len(entries) == 0 {
		return nil
	}
	backoff := 25 * time.Millisecond
	loggedFailure := false
	for {
		// Candidate diagnostics share SQLite with the account/risk ledgers. Recheck before every
		// batch attempt so an urgent preflight or order that begins after a prior retry receives the
		// next writer opportunity. The producer remains nonblocking and the exact rows stay queued.
		if !s.waitForLiveDispatchShadowQuiet(ctx) {
			if err := ctx.Err(); err != nil {
				return err
			}
			return context.DeadlineExceeded
		}
		err := s.writeLiveCandidateAuditBatch(ctx, entries)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !loggedFailure {
			loggedFailure = true
			s.liveLogAdd(map[string]any{
				"event": "AUTO-LIVE-CANDIDATE-AUDIT-RETRY", "queued_rows": len(entries),
				"error": err.Error(),
			})
			if s.log != nil {
				s.log.Warn("LIVE candidate audit writer blocked; exact queued receipts retained for retry",
					"rows", len(entries), "err", err)
			}
		}
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		if backoff < time.Second {
			backoff *= 2
			if backoff > time.Second {
				backoff = time.Second
			}
		}
	}
}

func (s *Server) liveCandidateAuditOverflowEntry(total, stored uint64) storage.AuditEntry {
	detail, _ := json.Marshal(map[string]any{
		"event":                      "AUTO-LIVE-CANDIDATE-AUDIT-OVERFLOW",
		"dropped_since_last_receipt": total - stored,
		"dropped_total":              total,
		"queue_capacity":             cap(s.liveCandidateAuditCh),
		"meaning":                    "exact candidate rows could not enter the bounded audit queue; in-memory identity receipts remain visible",
	})
	return storage.AuditEntry{Level: "error", Category: "live-candidate-audit-overflow",
		Message: "LIVE candidate audit queue overflow", Detail: string(detail)}
}

func (s *Server) persistLiveCandidateAuditOverflow(ctx context.Context) error {
	total := s.liveCandidateAuditDropped.Load()
	stored := s.liveCandidateAuditOverflowStored.Load()
	if total <= stored {
		return nil
	}
	if err := s.persistLiveCandidateAudit(ctx,
		[]storage.AuditEntry{s.liveCandidateAuditOverflowEntry(total, stored)}); err != nil {
		return err
	}
	s.liveCandidateAuditOverflowStored.Store(total)
	return nil
}

func (s *Server) runLiveCandidateAuditWorker(ctx context.Context) {
	defer close(s.liveCandidateAuditDone)
	aggregateTicker := time.NewTicker(liveCandidateAuditAggregateFor)
	defer aggregateTicker.Stop()
	for {
		var first liveCandidateAuditQueueItem
		select {
		case <-ctx.Done():
			return
		case <-aggregateTicker.C:
			summaries := s.drainLiveCandidateAuditAggregates()
			if err := s.persistLiveCandidateAudit(ctx, liveCandidateAuditEntries(summaries)); err != nil {
				return
			}
			continue
		case <-s.liveCandidateAuditOverflowWake:
			if err := s.persistLiveCandidateAuditOverflow(ctx); err != nil {
				return
			}
			continue
		case first = <-s.liveCandidateAuditCh:
		}

		batch := make([]liveCandidateAuditReceipt, 0, liveCandidateAuditBatchMax)
		var barrier chan error
		stop := false
		consume := func(item liveCandidateAuditQueueItem) {
			if item.receipt != nil {
				batch = append(batch, *item.receipt)
			}
			if item.barrier != nil {
				batch = append(batch, s.drainLiveCandidateAuditAggregates()...)
				barrier, stop = item.barrier, item.stop
			}
		}
		consume(first)
		for len(batch) < liveCandidateAuditBatchMax && barrier == nil {
			select {
			case item := <-s.liveCandidateAuditCh:
				consume(item)
			default:
				goto drained
			}
		}
	drained:
		err := s.persistLiveCandidateAudit(ctx, liveCandidateAuditEntries(batch))
		if err == nil {
			err = s.persistLiveCandidateAuditOverflow(ctx)
		}
		if barrier != nil {
			barrier <- err
			close(barrier)
			if stop {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

func (s *Server) liveCandidateAuditBarrier(ctx context.Context, stop bool) error {
	if s == nil {
		return nil
	}
	s.liveCandidateAuditStateMu.Lock()
	if stop {
		s.liveCandidateAuditStopping.Store(true)
	}
	ch, done, cancel := s.liveCandidateAuditCh, s.liveCandidateAuditDone, s.liveCandidateAuditCancel
	s.liveCandidateAuditStateMu.Unlock()
	if ch == nil {
		return nil
	}
	ack := make(chan error, 1)
	item := liveCandidateAuditQueueItem{barrier: ack, stop: stop}
	select {
	case ch <- item:
	case <-done:
		return nil
	case <-ctx.Done():
		if stop && cancel != nil {
			cancel()
		}
		return ctx.Err()
	}
	select {
	case err := <-ack:
		if stop {
			select {
			case <-done:
			case <-ctx.Done():
				if cancel != nil {
					cancel()
				}
				return ctx.Err()
			}
		}
		return err
	case <-done:
		return nil
	case <-ctx.Done():
		if stop && cancel != nil {
			cancel()
		}
		return ctx.Err()
	}
}

func (s *Server) flushLiveCandidateAudit(ctx context.Context) error {
	return s.liveCandidateAuditBarrier(ctx, false)
}

func (s *Server) stopLiveCandidateAudit(ctx context.Context) error {
	return s.liveCandidateAuditBarrier(ctx, true)
}

// recordLiveCandidateDrop appends an exact economic receipt to the bounded operator ring and
// offers it to the restart-safe writer queue. The two known scheduler-duplicate reasons instead
// enter a counted in-memory summary that the same worker flushes periodically and at barriers. A
// true return means the receipt or its count entered the durable pipeline, not that this hot caller
// synchronously waited for SQLite. Raw signals are ignored while ARM or AUTO is OFF.
func (s *Server) recordLiveCandidateDrop(c liveMirrorCandidate, event, stage, reason string,
	now time.Time, extra map[string]any) bool {
	return s.recordLiveCandidateDropMode(c, event, stage, reason, now, extra,
		c.LivePreflightAdmitted, true)
}

// recordAdmittedLiveCandidateDrop is only for immutable work that entered an AUTO queue while the
// operator gate was on. A later OFF/kill transition must still durably explain why that admitted
// candidate was removed; treating it as a new raw off-state signal would silently erase the exact
// terminal reason. Callers must first remove the candidate from an admitted queue.
func (s *Server) recordAdmittedLiveCandidateDrop(c liveMirrorCandidate, event, stage, reason string,
	now time.Time, extra map[string]any) bool {
	return s.recordLiveCandidateDropMode(c, event, stage, reason, now, extra, true, true)
}

// recordLiveCandidateHousekeeping keeps an operator/audit receipt for a secondary queue copy
// without manufacturing another economic LIVE terminal for the immutable attempt.
func (s *Server) recordLiveCandidateHousekeeping(c liveMirrorCandidate,
	event, stage, reason string, now time.Time, extra map[string]any) bool {
	return s.recordLiveCandidateDropMode(c, event, stage, reason, now, extra,
		c.LivePreflightAdmitted, false)
}

// recordAdmittedLiveCandidateHousekeeping is the OFF/kill-safe form for a secondary queue copy
// that entered while AUTO was enabled.
func (s *Server) recordAdmittedLiveCandidateHousekeeping(c liveMirrorCandidate,
	event, stage, reason string, now time.Time, extra map[string]any) bool {
	return s.recordLiveCandidateDropMode(c, event, stage, reason, now, extra, true, false)
}

func (s *Server) recordLiveCandidateDropMode(c liveMirrorCandidate, event, stage, reason string,
	now time.Time, extra map[string]any, admittedBeforeDisable, economicTerminal bool) bool {
	if !admittedBeforeDisable && !s.liveOperatorAutoOn() {
		return false
	}
	if now.IsZero() {
		now = time.Now()
	}
	c.Platform = strings.ToLower(strings.TrimSpace(c.Platform))
	c.Ticker = strings.TrimSpace(c.Ticker)
	c.Side = strings.ToUpper(strings.TrimSpace(c.Side))
	c.Action = strings.ToUpper(strings.TrimSpace(c.Action))
	if c.Action == "" {
		c.Action = "BUY"
	}
	c.Source = strings.TrimSpace(c.Source)
	event, stage, reason = strings.TrimSpace(event), strings.TrimSpace(stage), strings.TrimSpace(reason)
	if event == "" {
		event = "AUTO-LIVE-CANDIDATE-DROP"
	}
	if stage == "" {
		stage = "unknown-stage"
	}
	if reason == "" {
		reason = "unspecified-rejection"
	}
	candidateAt := c.At
	if candidateAt.IsZero() {
		candidateAt = now
	}
	age := now.Sub(candidateAt)
	row := make(map[string]any, len(extra)+20)
	for key, value := range extra {
		row[key] = value
	}
	system := liveCandidateDropSystem(c)
	// Required identity/timing fields win over caller extras so a receipt cannot silently relabel
	// the rejected money route.
	row["event"] = event
	row["stage"] = stage
	row["venue"] = c.Platform
	row["system"] = system
	row["source"] = c.Source
	row["ticker"] = c.Ticker
	row["side"] = c.Side
	row["action"] = c.Action
	row["reason"] = reason
	row["candidate_at"] = candidateAt.UTC().Format(time.RFC3339Nano)
	row["logged_at"] = now.UTC().Format(time.RFC3339Nano)
	row["age_ms"] = age.Milliseconds()
	row["ttl_ms"] = liveMirrorTTL.Milliseconds()
	row["input_topology"] = strings.TrimSpace(c.InputTopology)
	row["signal_contract"] = strings.TrimSpace(c.SignalContractID)
	if !c.InputObservedAt.IsZero() {
		row["input_observed_at"] = c.InputObservedAt.UTC().Format(time.RFC3339Nano)
	}
	receipt := liveCandidateAuditReceipt{row: row, event: event, system: system, logged: now}
	if liveCandidateAuditAggregateReason(reason) {
		// These are scheduler housekeeping observations, not separate economic decisions. Keeping
		// one execution-shadow and operator-ring row per duplicate previously crowded the unique
		// final-check evidence out of all three bounded diagnostic paths.
		return s.aggregateLiveCandidateAudit(receipt)
	}
	// Economic decisions receive a LIVE terminal; secondary combo/cache lifecycle observations
	// remain visible without being able to supersede the handler's cash result.
	if economicTerminal {
		s.executionShadowRecordDrop(c, stage, reason, now, row)
	} else {
		s.executionShadowRecordHousekeeping(c, stage, reason, now, row)
	}
	s.liveLogAdd(row)

	if s.enqueueLiveCandidateAudit(receipt) {
		return true
	}
	// Lightweight unit-test/diagnostic Servers may intentionally have no durable store. The exact
	// ring receipt above is still valid; absence of a configured sink is not queue overflow.
	if s.store == nil && s.liveCandidateAuditWrite == nil {
		return false
	}
	if s.liveCandidateAuditStopping.Load() {
		s.liveLogAdd(map[string]any{
			"event": "AUTO-LIVE-CANDIDATE-AUDIT-STOPPING", "venue": c.Platform,
			"system": system, "ticker": c.Ticker, "side": c.Side, "stage": stage,
			"reason": reason,
		})
		return false
	}
	total := s.liveCandidateAuditDropped.Add(1)
	s.liveLogAdd(map[string]any{
		"event": "AUTO-LIVE-CANDIDATE-AUDIT-OVERFLOW", "venue": c.Platform,
		"system": system, "ticker": c.Ticker, "side": c.Side, "stage": stage,
		"reason": reason, "dropped_total": total,
		"error": fmt.Sprintf("bounded durable audit queue full (capacity %d)", cap(s.liveCandidateAuditCh)),
	})
	// The operator ring is bounded too. Emit at the first loss and powers of two so the normal
	// suite log remains a second explicit trail without turning a pressure episode into log spam.
	if s.log != nil && (total == 1 || total&(total-1) == 0) {
		s.log.Error("LIVE candidate audit queue overflow; exact identity retained in operator ring",
			"dropped_total", total, "queue_capacity", cap(s.liveCandidateAuditCh),
			"venue", c.Platform, "system", system, "ticker", c.Ticker, "side", c.Side)
	}
	select {
	case s.liveCandidateAuditOverflowWake <- struct{}{}:
	default:
	}
	return false
}
