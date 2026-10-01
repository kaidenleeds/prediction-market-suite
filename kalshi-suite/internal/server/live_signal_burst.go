package server

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const (
	liveSignalArbitrationCandidateCap = liveSignalIntentCap
	liveSignalPreflightWorkers        = 16
	liveSignalPreflightQueueCap       = liveSignalIntentCap
)

type liveSignalPreflightBatch struct {
	mu        sync.Mutex
	remaining int
	accepted  int
	ready     chan struct{}
	readyOnce sync.Once
}

func newLiveSignalPreflightBatch(size int) *liveSignalPreflightBatch {
	batch := &liveSignalPreflightBatch{remaining: size, ready: make(chan struct{})}
	if size == 0 {
		close(batch.ready)
	}
	return batch
}

// finish releases the caller as soon as one executable candidate has reached the mirror queue.
// Rejected siblings continue independently; when none is accepted, the caller is released only
// after the complete batch has produced its durable terminal reasons.
func (b *liveSignalPreflightBatch) finish(accepted bool) {
	if b == nil {
		return
	}
	b.mu.Lock()
	if b.remaining > 0 {
		b.remaining--
		if accepted {
			b.accepted++
		}
	}
	ready := b.accepted > 0 || b.remaining == 0
	b.mu.Unlock()
	if ready {
		b.readyOnce.Do(func() { close(b.ready) })
	}
}

func (b *liveSignalPreflightBatch) wait(ctx context.Context) int {
	if b == nil {
		return 0
	}
	select {
	case <-ctx.Done():
	case <-b.ready:
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.accepted
}

type liveSignalPreflightJob struct {
	intent liveSignalIntent
	batch  *liveSignalPreflightBatch
	ctx    context.Context
	seq    uint64
}

type liveSignalPreflightRun func(context.Context, liveSignalIntent) bool
type liveSignalPreflightDrop func(liveSignalIntent, string, string, map[string]any)

// liveSignalPreflightScheduler is one persistent, bounded priority lane per Server. The old
// per-batch worker pool waited for the slowest sibling before the first accepted candidate could
// dispatch. This lane starts all workers once, always gives the strongest measured point the next
// available worker, and publishes each accepted candidate immediately.
type liveSignalPreflightScheduler struct {
	server *Server
	ctx    context.Context
	cancel context.CancelFunc
	run    liveSignalPreflightRun
	drop   liveSignalPreflightDrop

	mu          sync.Mutex
	cond        *sync.Cond
	jobs        []liveSignalPreflightJob
	closed      bool
	nextSeq     uint64
	queueCap    int
	workers     sync.WaitGroup
	workersDone chan struct{}
	active      atomic.Int64
}

var liveSignalPreflightSchedulers sync.Map // *Server -> *liveSignalPreflightScheduler

func newLiveSignalPreflightScheduler(server *Server, ctx context.Context, workers, queueCap int,
	run liveSignalPreflightRun, drop liveSignalPreflightDrop) *liveSignalPreflightScheduler {
	if ctx == nil {
		ctx = context.Background()
	}
	if workers <= 0 {
		workers = 1
	}
	if queueCap <= 0 {
		queueCap = liveSignalPreflightQueueCap
	}
	schedulerCtx, cancel := context.WithCancel(ctx)
	scheduler := &liveSignalPreflightScheduler{
		server: server, ctx: schedulerCtx, cancel: cancel, run: run, drop: drop, queueCap: queueCap,
		jobs: make([]liveSignalPreflightJob, 0, queueCap), workersDone: make(chan struct{}),
	}
	scheduler.cond = sync.NewCond(&scheduler.mu)
	for i := 0; i < workers; i++ {
		scheduler.workers.Add(1)
		go func() {
			defer scheduler.workers.Done()
			scheduler.worker()
		}()
	}
	go func() {
		scheduler.workers.Wait()
		close(scheduler.workersDone)
	}()
	go func() {
		<-schedulerCtx.Done()
		scheduler.stop()
		<-scheduler.workersDone
		if server != nil {
			liveSignalPreflightSchedulers.CompareAndDelete(server, scheduler)
		}
	}()
	return scheduler
}

func liveSignalPreflightSchedulerFor(s *Server, ctx context.Context) *liveSignalPreflightScheduler {
	for {
		if existing, ok := liveSignalPreflightSchedulers.Load(s); ok {
			scheduler := existing.(*liveSignalPreflightScheduler)
			scheduler.mu.Lock()
			open := !scheduler.closed
			scheduler.mu.Unlock()
			if open {
				return scheduler
			}
			liveSignalPreflightSchedulers.CompareAndDelete(s, scheduler)
			continue
		}
		created := newLiveSignalPreflightScheduler(s, ctx, liveSignalPreflightWorkers,
			liveSignalPreflightQueueCap,
			func(runCtx context.Context, intent liveSignalIntent) bool {
				return s.processLiveAllowlistedSignalIntentJob(runCtx, intent)
			},
			func(intent liveSignalIntent, stage, reason string, extra map[string]any) {
				s.recordLiveCandidateDrop(liveCandidateFromSignalIntent(intent),
					"AUTO-LIVE-SIGNAL-DROP", stage, reason, time.Now(), extra)
			})
		actual, loaded := liveSignalPreflightSchedulers.LoadOrStore(s, created)
		if !loaded {
			return created
		}
		// A concurrent caller won the registry seat. Stop this unused scheduler before retrying.
		created.stop()
		scheduler := actual.(*liveSignalPreflightScheduler)
		scheduler.mu.Lock()
		open := !scheduler.closed
		scheduler.mu.Unlock()
		if open {
			return scheduler
		}
		liveSignalPreflightSchedulers.CompareAndDelete(s, scheduler)
	}
}

func stopLiveSignalPreflightSchedulerFor(s *Server) *liveSignalPreflightScheduler {
	if s == nil {
		return nil
	}
	if existing, ok := liveSignalPreflightSchedulers.Load(s); ok {
		scheduler := existing.(*liveSignalPreflightScheduler)
		scheduler.stop()
		// Unregister synchronously at the generation boundary. Waiting for the scheduler's
		// cleanup goroutine left a tiny window where a fast OFF→ON saw the closed old pointer and
		// could not register the new epoch. CompareAndDelete prevents the old cleanup from ever
		// deleting a newer replacement.
		liveSignalPreflightSchedulers.CompareAndDelete(s, scheduler)
		return scheduler
	}
	return nil
}

func liveSignalPreflightWorkPending(s *Server) bool {
	if s == nil {
		return false
	}
	existing, ok := liveSignalPreflightSchedulers.Load(s)
	if !ok {
		return false
	}
	scheduler := existing.(*liveSignalPreflightScheduler)
	if scheduler.active.Load() > 0 {
		return true
	}
	scheduler.mu.Lock()
	pending := !scheduler.closed && len(scheduler.jobs) > 0
	scheduler.mu.Unlock()
	return pending || scheduler.active.Load() > 0
}

func liveSignalIntentTTLReason(intent liveSignalIntent, now time.Time) string {
	age := now.Sub(intent.At)
	if age < 0 {
		return "signal-intent-clock-skew"
	}
	if age > liveMirrorTTL {
		return "expired-before-live-first-preflight"
	}
	return ""
}

func strongerLiveSignalIntentJob(a, b liveSignalPreflightJob) bool {
	aNaN, bNaN := a.intent.Point != a.intent.Point, b.intent.Point != b.intent.Point
	if aNaN != bNaN {
		return !aNaN
	}
	if a.intent.Point != b.intent.Point {
		return a.intent.Point > b.intent.Point
	}
	if !a.intent.At.Equal(b.intent.At) {
		return a.intent.At.Before(b.intent.At)
	}
	return a.seq < b.seq
}

func (s *liveSignalPreflightScheduler) submit(ctx context.Context,
	intents []liveSignalIntent) *liveSignalPreflightBatch {
	batch := newLiveSignalPreflightBatch(len(intents))
	if len(intents) == 0 {
		return batch
	}
	if ctx == nil {
		ctx = context.Background()
	}
	now := time.Now()
	jobs := make([]liveSignalPreflightJob, 0, len(intents))
	for _, intent := range intents {
		// Crossing this bounded scheduler boundary makes the observation admitted work. Its final
		// reason must survive a later AUTO OFF/shutdown even though new raw signals are then silent.
		intent.PreflightAdmitted = true
		if s.server != nil && !intent.GenerationBound {
			intent.Generation = s.server.liveSignalIntentGeneration.Load()
			intent.GenerationBound = true
		}
		if reason := liveSignalIntentTTLReason(intent, now); reason != "" {
			if s.drop != nil {
				s.drop(intent, "signal-worker", reason, nil)
			}
			batch.finish(false)
			continue
		}
		jobs = append(jobs, liveSignalPreflightJob{intent: intent, batch: batch, ctx: ctx})
	}
	sort.SliceStable(jobs, func(i, j int) bool { return strongerLiveSignalIntentJob(jobs[i], jobs[j]) })

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		for _, job := range jobs {
			if s.drop != nil {
				s.drop(job.intent, "signal-preflight-queue", "signal-preflight-scheduler-stopped", nil)
			}
			job.batch.finish(false)
		}
		return batch
	}
	for i := range jobs {
		s.nextSeq++
		jobs[i].seq = s.nextSeq
		s.jobs = append(s.jobs, jobs[i])
	}
	// Merge all still-pending bursts before trimming. A newly arrived stronger point can replace a
	// weaker queued proxy, but work already running is never cancelled or duplicated.
	sort.SliceStable(s.jobs, func(i, j int) bool { return strongerLiveSignalIntentJob(s.jobs[i], s.jobs[j]) })
	var evicted []liveSignalPreflightJob
	if len(s.jobs) > s.queueCap {
		evicted = append(evicted, s.jobs[s.queueCap:]...)
		s.jobs = s.jobs[:s.queueCap]
	}
	s.cond.Broadcast()
	s.mu.Unlock()
	for _, job := range evicted {
		if s.drop != nil {
			s.drop(job.intent, "signal-preflight-queue",
				"signal-preflight-queue-cap-weaker-proxy",
				map[string]any{"queue_capacity": s.queueCap})
		}
		job.batch.finish(false)
	}
	return batch
}

func (s *liveSignalPreflightScheduler) worker() {
	for {
		s.mu.Lock()
		for len(s.jobs) == 0 && !s.closed {
			s.cond.Wait()
		}
		if s.closed {
			s.mu.Unlock()
			return
		}
		job := s.jobs[0]
		copy(s.jobs, s.jobs[1:])
		s.jobs = s.jobs[:len(s.jobs)-1]
		s.active.Add(1)
		s.mu.Unlock()
		func() {
			defer s.active.Add(-1)
			if s.server != nil {
				if reason := s.server.liveIntentGenerationReason(job.intent.Generation,
					job.intent.GenerationBound); reason != "" {
					if s.drop != nil {
						s.drop(job.intent, "signal-worker", reason, nil)
					}
					job.batch.finish(false)
					return
				}
			}
			if reason := liveSignalIntentTTLReason(job.intent, time.Now()); reason != "" {
				if s.drop != nil {
					s.drop(job.intent, "signal-worker", reason, nil)
				}
				job.batch.finish(false)
				return
			}
			if err := job.ctx.Err(); err != nil {
				if s.drop != nil {
					s.drop(job.intent, "signal-worker", "signal-preflight-context-cancelled-before-start", nil)
				}
				job.batch.finish(false)
				return
			}
			intentCtx, cancel := context.WithTimeout(job.ctx, 4*time.Second)
			stopSchedulerCancel := context.AfterFunc(s.ctx, cancel)
			accepted := false
			if s.run != nil {
				accepted = s.run(intentCtx, job.intent)
			}
			stopSchedulerCancel()
			cancel()
			job.batch.finish(accepted)
		}()
	}
}

func (s *liveSignalPreflightScheduler) stop() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	dropped := append([]liveSignalPreflightJob(nil), s.jobs...)
	s.jobs = nil
	s.cond.Broadcast()
	s.mu.Unlock()
	s.cancel()
	for _, job := range dropped {
		if s.drop != nil {
			s.drop(job.intent, "signal-preflight-shutdown", "signal-preflight-scheduler-stopped", nil)
		}
		job.batch.finish(false)
	}
}

func (s *liveSignalPreflightScheduler) wait(ctx context.Context) error {
	if s == nil {
		return nil
	}
	s.stop()
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-s.workersDone:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func liveCandidateFromSignalIntent(intent liveSignalIntent) liveMirrorCandidate {
	c := liveMirrorCandidate{Platform: intent.Signal.Platform, Ticker: intent.Signal.Ticker,
		Title: intent.Signal.Title, Side: intent.Signal.Side, Family: intent.Signal.SignalType,
		Source: "auto-cons-" + intent.Signal.SignalType, Price: intent.Signal.EntryPrice, At: intent.At,
		ShadowAttemptID:      intent.ShadowAttemptID,
		InputTopology:        r147SignalInputTopology(intent.Signal),
		InputObservedAt:      r147SignalInputObservedAt(intent.Signal),
		LiveIntentGeneration: intent.Generation, LiveIntentGenerationBound: intent.GenerationBound,
		LivePreflightAdmitted: intent.PreflightAdmitted}
	if bound, why, declared := bindStaticTakerSignalCandidate(c, intent.Signal); declared && why == "" {
		c = bound
	}
	return c
}

func liveSignalIntentExactKey(intent liveSignalIntent) string {
	return liveCandidateFromSignalIntent(intent).key()
}

// collectLiveSignalIntentBurst shares the arbitration window with the mirror queue. The first raw
// intent used to be preflighted alone and only then did mirror arbitration sleep, leaving its
// same-burst competitors upstream and allowing queue order to decide real money. A wake may have
// no first raw intent; in that case this same window still drains any raw signals arriving beside
// the already-queued mirror candidate.
func (s *Server) collectLiveSignalIntentBurst(ctx context.Context, first *liveSignalIntent) []liveSignalIntent {
	out := make([]liveSignalIntent, 0, liveSignalArbitrationCandidateCap)
	seen := make(map[string]int, liveSignalArbitrationCandidateCap)
	add := func(intent liveSignalIntent) {
		key := liveSignalIntentExactKey(intent)
		if at, ok := seen[key]; ok {
			// One executable identity gets one seat; preserve the strongest measured instance and
			// let current-book proof decide the final rank downstream.
			if intent.Point > out[at].Point || (intent.Point == out[at].Point && intent.At.After(out[at].At)) {
				out[at] = intent
			}
			return
		}
		if len(out) < liveSignalArbitrationCandidateCap {
			seen[key] = len(out)
			out = append(out, intent)
			return
		}
		// The upstream channel itself is bounded to this capacity. If a pathological scan still
		// emits more unique identities while we drain, retain the strongest measured set instead
		// of the first arrivals. The displaced identity receives the normal durable drop receipt.
		weak := 0
		for i := 1; i < len(out); i++ {
			if out[i].Point < out[weak].Point {
				weak = i
			}
		}
		if intent.Point <= out[weak].Point {
			s.recordLiveCandidateDrop(liveCandidateFromSignalIntent(intent),
				"AUTO-LIVE-SIGNAL-DROP", "signal-arbitration", "decision-window-cap-weaker-proxy", time.Now(),
				map[string]any{"candidate_capacity": liveSignalArbitrationCandidateCap})
			return
		}
		oldKey := liveSignalIntentExactKey(out[weak])
		delete(seen, oldKey)
		s.recordLiveCandidateDrop(liveCandidateFromSignalIntent(out[weak]),
			"AUTO-LIVE-SIGNAL-DROP", "signal-arbitration", "decision-window-cap-replaced-by-stronger-proxy", time.Now(),
			map[string]any{"candidate_capacity": liveSignalArbitrationCandidateCap})
		seen[key] = weak
		out[weak] = intent
	}
	if first != nil {
		add(*first)
	}
	timer := time.NewTimer(liveMirrorArbitrationWindow)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return out
		case <-timer.C:
			// Drain everything already queued at the decision boundary. A ready timer must not
			// randomly beat the ninth signal and recreate first-arrival bias.
			for {
				select {
				case intent := <-s.liveSignalIntentChannel():
					add(intent)
				default:
					return out
				}
			}
		case intent := <-s.liveSignalIntentChannel():
			add(intent)
		}
	}
}

// submitLiveSignalIntentBatch publishes one complete measured-point-ranked arrival window to the
// persistent bounded preflight lane without waiting for venue reads, proof storage, or sibling
// rejections. Every accepted worker already enqueues exactly one mirror candidate and wakes
// MonitorLiveAuto; every rejection/eviction keeps its durable terminal receipt in the scheduler.
// Returning the batch is useful to tests and shutdown bookkeeping, but the money loop must not wait
// on it: doing so leaves later raw signals stranded upstream while the first burst performs REST and
// SQLite work.
func (s *Server) submitLiveSignalIntentBatch(ctx context.Context,
	intents []liveSignalIntent) *liveSignalPreflightBatch {
	if len(intents) == 0 {
		return newLiveSignalPreflightBatch(0)
	}
	return liveSignalPreflightSchedulerFor(s, ctx).submit(ctx, intents)
}
