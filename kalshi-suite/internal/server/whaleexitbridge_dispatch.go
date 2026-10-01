package server

// The whale-exit bridge can be expensive: it may resolve a Gamma market, search both
// tradeable venues, read an identity certificate, inspect books, and write evidence.
// None of that work belongs on the skilled-flow producer's single-flight. This dispatcher
// makes the producer path a bounded, in-memory enqueue only. One worker deliberately
// serializes bridge evaluation so a burst cannot become a REST/SQLite subscription flood.

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

const (
	whaleExitBridgeQueueCapacity = 64
	whaleExitBridgeJobTimeout    = 25 * time.Second
	whaleExitBridgeJobMaxAge     = 2 * time.Minute
	whaleExitBridgeDedupTTL      = 30 * time.Minute
	whaleExitRejectBatchSize     = 20
	whaleExitRejectBatchWindow   = 5 * time.Minute
)

type whaleExitBridgeRejectKey struct {
	server        *Server
	reason, venue string
}

type whaleExitBridgeRejectEntry struct {
	lastEmit, pendingSince time.Time
	pending                int
}

type whaleExitBridgeRejectBatcher struct {
	mu   sync.Mutex
	rows map[whaleExitBridgeRejectKey]whaleExitBridgeRejectEntry
}

func (b *whaleExitBridgeRejectBatcher) record(server *Server, reason, venue string,
	now time.Time) (emit bool, count int, since time.Time) {
	key := whaleExitBridgeRejectKey{server: server, reason: reason, venue: venue}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.rows == nil {
		b.rows = make(map[whaleExitBridgeRejectKey]whaleExitBridgeRejectEntry)
	}
	for k, row := range b.rows {
		if now.Sub(row.lastEmit) > 2*time.Hour && row.pending == 0 {
			delete(b.rows, k)
		}
	}
	row, exists := b.rows[key]
	if !exists {
		b.rows[key] = whaleExitBridgeRejectEntry{lastEmit: now}
		return true, 1, now
	}
	if row.pending == 0 {
		row.pendingSince = now
	}
	row.pending++
	if row.pending >= whaleExitRejectBatchSize || now.Sub(row.lastEmit) >= whaleExitRejectBatchWindow {
		emit, count, since = true, row.pending, row.pendingSince
		row.pending = 0
		row.pendingSince = time.Time{}
		row.lastEmit = now
	}
	b.rows[key] = row
	return emit, count, since
}

var whaleExitBridgeRejectBatches whaleExitBridgeRejectBatcher

type whaleExitBridgeJob struct {
	server                          *Server
	conditionID, title, soldOutcome string
	sourcePrice, notional           float64
	traders                         int
	enqueuedAt                      time.Time
}

func (j whaleExitBridgeJob) key() string {
	return strings.ToLower(strings.TrimSpace(j.conditionID)) + "\x00" +
		strings.ToLower(strings.TrimSpace(j.soldOutcome))
}

type whaleExitBridgeEnqueueResult uint8

const (
	whaleExitBridgeQueued whaleExitBridgeEnqueueResult = iota
	whaleExitBridgeDuplicate
	whaleExitBridgeQueueFull
)

type whaleExitBridgeDispatchNotice struct {
	duplicate int
	full      int
	sample    whaleExitBridgeJob
}

type whaleExitBridgeDispatcher struct {
	jobs chan whaleExitBridgeJob
	stop chan struct{}
	done chan struct{}

	now      func() time.Time
	run      func(context.Context, whaleExitBridgeJob)
	report   func(context.Context, *Server, whaleExitBridgeDispatchNotice)
	stopOnce sync.Once

	mu      sync.Mutex
	seen    map[*Server]map[string]time.Time
	notices map[*Server]whaleExitBridgeDispatchNotice
}

func newWhaleExitBridgeDispatcher(capacity int,
	run func(context.Context, whaleExitBridgeJob),
	report func(context.Context, *Server, whaleExitBridgeDispatchNotice),
) *whaleExitBridgeDispatcher {
	if capacity < 1 {
		capacity = 1
	}
	if run == nil {
		run = func(context.Context, whaleExitBridgeJob) {}
	}
	if report == nil {
		report = func(context.Context, *Server, whaleExitBridgeDispatchNotice) {}
	}
	d := &whaleExitBridgeDispatcher{
		jobs:    make(chan whaleExitBridgeJob, capacity),
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
		now:     time.Now,
		run:     run,
		report:  report,
		seen:    make(map[*Server]map[string]time.Time),
		notices: make(map[*Server]whaleExitBridgeDispatchNotice),
	}
	go d.loop()
	return d
}

func (d *whaleExitBridgeDispatcher) close() {
	d.stopOnce.Do(func() { close(d.stop) })
	<-d.done
}

// enqueue never performs I/O and never waits for the worker. A duplicate is coalesced for the
// same interval as the source event latch. A saturated queue drops new bridge work rather than
// delaying the primary whale/flow producer; the worker emits one aggregated durable diagnostic.
func (d *whaleExitBridgeDispatcher) enqueue(job whaleExitBridgeJob) whaleExitBridgeEnqueueResult {
	now := d.now()
	job.enqueuedAt = now
	key := job.key()

	d.mu.Lock()
	byKey := d.seen[job.server]
	if byKey == nil {
		byKey = make(map[string]time.Time)
		d.seen[job.server] = byKey
	}
	for k, at := range byKey {
		if now.Sub(at) >= whaleExitBridgeDedupTTL {
			delete(byKey, k)
		}
	}
	if at, ok := byKey[key]; ok && now.Sub(at) < whaleExitBridgeDedupTTL {
		n := d.notices[job.server]
		n.duplicate++
		n.sample = job
		d.notices[job.server] = n
		d.mu.Unlock()
		return whaleExitBridgeDuplicate
	}

	select {
	case d.jobs <- job:
		byKey[key] = now
		d.mu.Unlock()
		return whaleExitBridgeQueued
	default:
		n := d.notices[job.server]
		n.full++
		n.sample = job
		d.notices[job.server] = n
		d.mu.Unlock()
		return whaleExitBridgeQueueFull
	}
}

func (d *whaleExitBridgeDispatcher) takeNotice(server *Server) whaleExitBridgeDispatchNotice {
	d.mu.Lock()
	n := d.notices[server]
	delete(d.notices, server)
	d.mu.Unlock()
	return n
}

func (d *whaleExitBridgeDispatcher) takeAllNotices() map[*Server]whaleExitBridgeDispatchNotice {
	d.mu.Lock()
	out := d.notices
	d.notices = make(map[*Server]whaleExitBridgeDispatchNotice)
	d.mu.Unlock()
	return out
}

func (d *whaleExitBridgeDispatcher) reportNotice(server *Server, n whaleExitBridgeDispatchNotice) {
	if n.duplicate == 0 && n.full == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	d.report(ctx, server, n)
}

func (d *whaleExitBridgeDispatcher) loop() {
	defer close(d.done)
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-d.stop:
			return
		case <-ticker.C:
			for server, n := range d.takeAllNotices() {
				d.reportNotice(server, n)
			}
		case job := <-d.jobs:
			if age := d.now().Sub(job.enqueuedAt); age > whaleExitBridgeJobMaxAge {
				if job.server != nil {
					job.server.auditWhaleExitBridge(context.Background(), "REJECTED", "dispatcher-job-expired",
						job.conditionID, job.soldOutcome, "", job.sourcePrice, job.notional, job.traders,
						whaleExitBridgeCandidate{}, storageRulePairCertificateZero(), nil, "")
				}
			} else {
				ctx, cancel := context.WithTimeout(context.Background(), whaleExitBridgeJobTimeout)
				d.run(ctx, job)
				cancel()
			}
			d.reportNotice(job.server, d.takeNotice(job.server))
		}
	}
}

// Kept as a tiny helper so the dispatcher file does not need to make callers fabricate a
// certificate for queue-level diagnostics.
func storageRulePairCertificateZero() storage.RulePairCertificateSpec {
	return storage.RulePairCertificateSpec{}
}

var whaleExitBridgeDispatch = newWhaleExitBridgeDispatcher(
	whaleExitBridgeQueueCapacity,
	func(ctx context.Context, job whaleExitBridgeJob) {
		if job.server != nil {
			job.server.logWhaleExitHoldBridge(ctx, job.conditionID, job.title, job.soldOutcome,
				job.sourcePrice, job.notional, job.traders)
		}
	},
	func(ctx context.Context, server *Server, notice whaleExitBridgeDispatchNotice) {
		if server == nil || server.store == nil {
			return
		}
		detail := map[string]any{
			"system":              whaleExitHoldBridgeFamily,
			"state":               "COALESCED",
			"duplicate_events":    notice.duplicate,
			"queue_full_drops":    notice.full,
			"queue_capacity":      whaleExitBridgeQueueCapacity,
			"source_condition_id": notice.sample.conditionID,
			"source_sold_outcome": notice.sample.soldOutcome,
			"meaning":             "bridge work was coalesced or dropped to keep the primary whale feed live; no trade authority was created",
		}
		b, _ := json.Marshal(detail)
		_ = server.store.Audit(ctx, "warn", "whale-exit-bridge",
			fmt.Sprintf("COALESCED %s: duplicate=%d queue_full=%d",
				whaleExitHoldBridgeFamily, notice.duplicate, notice.full), string(b))
	},
)

func (s *Server) enqueueWhaleExitHoldBridge(conditionID, title, soldOutcome string,
	sourcePrice, notional float64, traders int) whaleExitBridgeEnqueueResult {
	return whaleExitBridgeDispatch.enqueue(whaleExitBridgeJob{
		server: s, conditionID: conditionID, title: title, soldOutcome: soldOutcome,
		sourcePrice: sourcePrice, notional: notional, traders: traders,
	})
}
