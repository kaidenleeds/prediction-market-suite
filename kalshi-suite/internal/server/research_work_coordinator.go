package server

import (
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

const (
	researchDurabilityGrace = 2 * time.Second
	// Leave checkpoint headroom before the writer reaches the old 512 MiB danger line. A live
	// production soak showed research readers being cancelled only after the WAL had already grown
	// into the 512-779 MiB range, at which point replay/lifecycle receipt writes repeatedly lost
	// their short durability window. Core market and execution writers are never gated here.
	researchWALPressureBytes      = int64(256 << 20)
	researchWALPressurePollPeriod = 250 * time.Millisecond
	// SQLite retains a recycled 256 MiB WAL shell because journal_size_limit is 256 MiB. Physical
	// file length therefore is not current checkpoint debt. A recent PASSIVE/TRUNCATE receipt is
	// authoritative inside this window. Above the ordinary rail, a missing or stale receipt fails
	// closed until the admission path refreshes it; the old genuine 512+ MiB danger rail is always
	// unconditional.
	researchWALCheckpointReceiptFresh = 3 * time.Minute
	// A retained WAL shell can be reused without changing its physical length. While a heavy read
	// is active, refresh the logical frame receipt on this cadence so hidden frame reuse cannot
	// remain invisible for the three-minute general receipt window.
	researchWALLogicalProbeFresh = 2 * time.Second
	researchWALLogicalProbeLimit = time.Second
	researchWALEmergencyBytes    = int64(512 << 20)
	researchWALDefaultPageBytes  = int64(4096)
	// Reserve the shared heavy lane long enough for one complete systems rotation and the active
	// twelve-part funnel to finish its first boot cycle. The bound prevents a broken collector
	// from starving maintenance indefinitely.
	researchSystemsStartupPriorityMax = 8 * time.Minute
)

func researchStartupDeferrable(name string) bool {
	switch name {
	case "market-tree-catalog", "ml-accuracy", "combo-lab-enumerate", "ptt-backfill",
		"inference", "research-digest", "portfolio", "replay", "settlement-research-ledgers",
		"settlement-subcent-golf", "catalog", "broad-signal-settlement", "retention",
		"fill-retro", "xvident":
		return true
	default:
		return false
	}
}

// researchSystemsStartupPriority keeps wide historical maintenance from taking the sole heavy
// token while current-boot collectors are still receiving their first bounded turns. Raw Step7
// evidence, identity foundation, rules, and the systems lane are intentionally not deferred.
func (s *Server) researchSystemsStartupPriority() bool {
	if s == nil || s.bootAt.IsZero() {
		return false
	}
	age := time.Since(s.bootAt)
	if age < 0 || age > researchSystemsStartupPriorityMax {
		return false
	}
	rotation := s.researchSystemsRotationRuntime()
	rotation.Lock()
	initialComplete := rotation.initialRotationComplete
	rotation.Unlock()
	collectors := s.r138CollectorState()
	collectors.mu.Lock()
	funnelActive := collectors.systemFunnelCycleActive
	collectors.mu.Unlock()
	return !initialComplete || funnelActive
}

// researchLiveActive is deliberately broader than LIVE AUTO dispatch. Once real-money authority
// is armed, analytic scans yield the database even if AUTO is temporarily off or paused. This
// keeps manual/live reconciliation, the DB canary, and pending-risk reads ahead of research work.
func (s *Server) researchLiveActive() bool {
	if s == nil {
		return false
	}
	s.liveMu.Lock()
	active := s.liveArmed || s.liveAuto
	s.liveMu.Unlock()
	return active
}

// researchDurabilityContext preserves the old "finish the receipt even when the sweep is
// cancelled" contract without turning that durability write into an unbounded extension of the
// shared research lane. Each cleanup write receives at most one small grace window starting now;
// when the caller is a bounded lane it is also capped by the common lane-deadline-plus-grace
// ceiling. A shutdown cancellation therefore cannot inherit most of an unused 55-second lane.
func researchDurabilityContext(parent context.Context) (context.Context, context.CancelFunc) {
	return researchDurabilityContextWithGrace(parent, researchDurabilityGrace)
}

func researchDurabilityContextWithGrace(parent context.Context, grace time.Duration) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	if grace <= 0 {
		grace = researchDurabilityGrace
	}
	deadline := time.Now().Add(grace)
	if parentDeadline, bounded := parent.Deadline(); bounded && parentDeadline.Add(grace).Before(deadline) {
		deadline = parentDeadline.Add(grace)
	}
	base := context.WithoutCancel(parent)
	return context.WithDeadline(base, deadline)
}

// runCooperativelyBoundedResearchLane keeps the collector synchronous: the shared gate is never
// released while work continues in a detached goroutine. Every operation in an admitted lane must
// honor the supplied context; final receipt/cursor writes use researchDurabilityContext above.
func runCooperativelyBoundedResearchLane(parent context.Context, limit time.Duration,
	fn func(context.Context)) bool {
	if limit <= 0 {
		limit = time.Second
	}
	laneCtx, cancel := context.WithTimeout(parent, limit)
	defer cancel()
	fn(laneCtx)
	return errors.Is(laneCtx.Err(), context.DeadlineExceeded)
}

// researchWorkCoordinator admits one expensive, research-only database job at a time. Core feed,
// signal, paper and execution writers deliberately do not use this gate: when research is busy,
// research defers instead of making the money path wait behind a queue of analytic readers/writers.
type researchWorkCoordinator struct {
	token             chan struct{}
	mu                sync.Mutex
	last              string
	deferred          map[string]bool
	walBusy           bool
	walPressureBytes  int64
	walPressurePeriod time.Duration
	// Tests may replace the production PASSIVE logical-frame probe. The closure must return
	// current pressure truth, not a cached physical-size guess.
	walProbe func(context.Context, int64, bool) (bool, int64)
}

var researchWorkCoordinators sync.Map // map[*Server]*researchWorkCoordinator

// researchWALCheckpointReceipt is the latest measured checkpoint debt for one database. SQLite
// may keep a fully checkpointed WAL file allocated at journal_size_limit, so PhysicalBytes is
// provenance and a post-receipt growth bound, never the current debt by itself.
type researchWALCheckpointReceipt struct {
	ObservedAt    time.Time
	PhysicalBytes int64
	PageBytes     int64
	Busy          bool
	Valid         bool
	LogFrames     int64
	Checkpointed  int64
	CheckpointErr string
}

var researchWALCheckpointReceipts sync.Map // map[clean data dir]researchWALCheckpointReceipt

func researchWALReceiptKey(dataDir string) string {
	return filepath.Clean(dataDir)
}

func researchWALPhysicalBytes(dataDir string) int64 {
	info, err := os.Stat(filepath.Join(dataDir, "kalshi.db-wal"))
	if err != nil {
		return 0
	}
	return info.Size()
}

// researchWALPageBytes reads the SQLite WAL header's big-endian page-size field. A truncated or
// synthetic WAL has no header; this suite's database uses SQLite's ordinary 4 KiB page size, so
// that is the conservative production fallback for a missing header.
func researchWALPageBytes(dataDir string) int64 {
	f, err := os.Open(filepath.Join(dataDir, "kalshi.db-wal"))
	if err != nil {
		return researchWALDefaultPageBytes
	}
	defer f.Close()
	var header [12]byte
	if _, err = f.ReadAt(header[:], 0); err != nil {
		return researchWALDefaultPageBytes
	}
	page := int64(binary.BigEndian.Uint32(header[8:12]))
	if page == 1 {
		page = 65536
	}
	if page < 512 || page > 65536 || page&(page-1) != 0 {
		return researchWALDefaultPageBytes
	}
	return page
}

func publishResearchWALCheckpointReceipt(dataDir string, observedAt time.Time,
	busy, logFrames, checkpointed int, checkpointErr error) {
	if observedAt.IsZero() {
		observedAt = time.Now()
	}
	receipt := researchWALCheckpointReceipt{
		ObservedAt: observedAt, PhysicalBytes: researchWALPhysicalBytes(dataDir),
		PageBytes: researchWALPageBytes(dataDir), Busy: busy != 0,
		Valid:     checkpointErr == nil && logFrames >= 0 && checkpointed >= 0,
		LogFrames: int64(logFrames), Checkpointed: int64(checkpointed),
	}
	if checkpointErr != nil {
		receipt.Busy = true
		receipt.CheckpointErr = checkpointErr.Error()
	}
	if !receipt.Valid {
		// An errored or malformed receipt has no trustworthy debt count. Fail closed until the
		// next valid checkpoint receipt instead of presenting unknown debt as a healthy zero.
		// A valid PASSIVE busy receipt still carries exact log/checkpointed frame counts.
		receipt.Busy = true
	}
	researchWALCheckpointReceipts.Store(researchWALReceiptKey(dataDir), receipt)
}

func clearResearchWALCheckpointReceipt(dataDir string) {
	researchWALCheckpointReceipts.Delete(researchWALReceiptKey(dataDir))
}

func researchWALDebtBytes(receipt researchWALCheckpointReceipt, currentPhysical int64) int64 {
	if !receipt.Valid {
		return 0
	}
	debtFrames := receipt.LogFrames - receipt.Checkpointed
	if debtFrames < 0 {
		debtFrames = 0
	}
	frameBytes := receipt.PageBytes + 24 // one WAL frame header plus one database page
	if frameBytes <= 24 {
		frameBytes = researchWALDefaultPageBytes + 24
	}
	maxInt64 := int64(^uint64(0) >> 1)
	debt := maxInt64
	if debtFrames <= maxInt64/frameBytes {
		debt = debtFrames * frameBytes
	}
	// The checkpoint receipt freezes debt at one instant. Any file extension after that instant is
	// newly written WAL, not retained capacity, so include it as a conservative upper bound.
	if growth := currentPhysical - receipt.PhysicalBytes; growth > 0 {
		if debt > maxInt64-growth {
			return maxInt64
		}
		debt += growth
	}
	return debt
}

func researchWorkCoordinatorFor(s *Server) *researchWorkCoordinator {
	if value, ok := researchWorkCoordinators.Load(s); ok {
		return value.(*researchWorkCoordinator)
	}
	created := &researchWorkCoordinator{
		token:             make(chan struct{}, 1),
		deferred:          map[string]bool{},
		walPressureBytes:  researchWALPressureBytes,
		walPressurePeriod: researchWALPressurePollPeriod,
	}
	created.token <- struct{}{}
	value, _ := researchWorkCoordinators.LoadOrStore(s, created)
	return value.(*researchWorkCoordinator)
}

// researchWALUnderPressure is a cheap cached-checkpoint guard, not a database query. Once actual
// uncheckpointed debt is high, admitting another research-only reader can pin the checkpoint
// horizon while live writers add hundreds of megabytes behind it. Core feeds, Paper execution,
// settlement, and the checkpoint lane do not use this gate and therefore continue normally.
func researchWALUnderPressure(dataDir string) (bool, int64) {
	return researchWALAbove(dataDir, researchWALPressureBytes)
}

func researchWALAbove(dataDir string, threshold int64) (bool, int64) {
	if threshold <= 0 {
		threshold = researchWALPressureBytes
	}
	physical := researchWALPhysicalBytes(dataDir)
	// Preserve the old proven emergency rail even when a receipt claims low debt. A physical WAL
	// beyond 512 MiB is never dismissed by cached telemetry.
	if physical > researchWALEmergencyBytes {
		return true, physical
	}
	value, ok := researchWALCheckpointReceipts.Load(researchWALReceiptKey(dataDir))
	if !ok {
		// Unknown logical debt cannot dismiss a WAL already above the ordinary rail. Production
		// admission immediately requests a fresh PASSIVE receipt and retries from that truth.
		return physical > threshold, physical
	}
	receipt := value.(researchWALCheckpointReceipt)
	age := time.Since(receipt.ObservedAt)
	if age < 0 || age > researchWALCheckpointReceiptFresh {
		// A stale receipt cannot tell retained capacity from newly reused frames. Fail closed above
		// the ordinary rail until production admission refreshes the logical checkpoint counters.
		return physical > threshold, physical
	}
	if !receipt.Valid {
		return true, physical
	}
	// PASSIVE can report busy while still returning exact log/checkpointed frame counts. That is
	// expected when the token-owning heavy reader itself pins a small checkpoint horizon. Treating
	// every valid busy receipt as pressure canceled score-state/proper-score at 61-229 MiB even
	// though the measured logical debt was below the 256 MiB rail. The frame delta below is the
	// current debt; the active guard probes it again at least every two seconds and still cancels
	// at the ordinary rail, while physical WAL above 512 MiB remains unconditional.
	debt := researchWALDebtBytes(receipt, physical)
	if debt > threshold {
		if debt > physical {
			return true, debt
		}
		return true, physical
	}
	return false, physical
}

// researchWALProbe refreshes SQLite's logical frame counters before every heavy admission and at
// least every two seconds while a heavy reader is active. PASSIVE does not block readers/writers;
// it also reveals debt reused inside a retained 256 MiB file where physical growth is zero.
func (s *Server) researchWALProbe(
	ctx context.Context,
	threshold int64,
	force bool,
) (bool, int64) {
	if s == nil {
		return true, 0
	}
	if s.store == nil {
		return true, researchWALPhysicalBytes(s.cfg().DataDir)
	}
	dataDir := s.cfg().DataDir
	if !force {
		if value, ok := researchWALCheckpointReceipts.Load(researchWALReceiptKey(dataDir)); ok {
			receipt := value.(researchWALCheckpointReceipt)
			age := time.Since(receipt.ObservedAt)
			if age >= 0 && age < researchWALLogicalProbeFresh {
				return researchWALAbove(dataDir, threshold)
			}
		}
	}
	if ctx == nil {
		ctx = context.Background()
	}
	probeCtx, cancel := context.WithTimeout(ctx, researchWALLogicalProbeLimit)
	defer cancel()
	busy, frames, checkpointed, err := s.store.WALCheckpointPassive(probeCtx)
	publishResearchWALCheckpointReceipt(dataDir, time.Now(), busy, frames, checkpointed, err)
	return researchWALAbove(dataDir, threshold)
}

// runHeavyResearchWithActiveWALGuard also watches work that was admitted while the WAL was safe.
// A long read can pin the checkpoint horizon after admission, so the guard cancels only the
// derived research context when the WAL crosses the safety boundary. It then waits for fn to
// actually return before its caller releases the shared token. Core feeds, execution, settlement,
// and checkpoints neither receive this context nor use this helper.
func runHeavyResearchWithActiveWALGuard(parent context.Context, dataDir string, threshold int64,
	poll time.Duration, onPressure func(int64), fn func(context.Context),
	probes ...func(context.Context, int64, bool) (bool, int64)) (pressured bool) {
	if parent == nil {
		parent = context.Background()
	}
	if threshold <= 0 {
		threshold = researchWALPressureBytes
	}
	if poll <= 0 {
		poll = researchWALPressurePollPeriod
	}

	workCtx, cancelWork := context.WithCancel(parent)
	monitorStop := make(chan struct{})
	monitorDone := make(chan struct{})
	var callbackReturned atomic.Bool
	var pressureSeen atomic.Bool
	go func() {
		defer close(monitorDone)
		ticker := time.NewTicker(poll)
		defer ticker.Stop()
		for {
			select {
			case <-monitorStop:
				return
			case <-parent.Done():
				return
			case <-ticker.C:
				underPressure, bytes := researchWALAbove(dataDir, threshold)
				if len(probes) > 0 && probes[0] != nil {
					underPressure, bytes = probes[0](workCtx, threshold, false)
				}
				if !underPressure || callbackReturned.Load() {
					continue
				}
				if pressureSeen.CompareAndSwap(false, true) {
					// Publish the retry/deferred state before cancellation becomes visible to the
					// callback. Otherwise the callback can observe ctx.Done, return control to a
					// caller, and expose a momentary false "not deferred" state.
					if onPressure != nil {
						onPressure(bytes)
					}
					cancelWork()
				}
				return
			}
		}
	}()
	defer func() {
		callbackReturned.Store(true)
		close(monitorStop)
		<-monitorDone
		cancelWork()
		pressured = pressureSeen.Load()
	}()

	fn(workCtx)
	return pressureSeen.Load()
}

// tryRunHeavyResearch is intentionally nonblocking for periodic jobs. A skipped periodic pass has
// not consumed its collector-specific due timestamp, so the next tick retries it. Inference uses a
// short bounded wait plus an explicit retry loop because its next ordinary cadence is 30 minutes.
func (s *Server) tryRunHeavyResearch(ctx context.Context, name string, wait time.Duration, fn func(context.Context)) bool {
	gate := researchWorkCoordinatorFor(s)
	deferJob := func() {
		gate.mu.Lock()
		gate.deferred[name] = true
		gate.mu.Unlock()
	}
	// Real-money safety reads always win. Do not even acquire the research token while LIVE is
	// armed; the existing retry loops will resume the deferred job after the operator disarms.
	if s.researchLiveActive() {
		deferJob()
		return false
	}
	if researchStartupDeferrable(name) && s.researchSystemsStartupPriority() {
		deferJob()
		return false
	}
	gate.mu.Lock()
	walThreshold, walPoll, walProbe := gate.walPressureBytes, gate.walPressurePeriod, gate.walProbe
	gate.mu.Unlock()
	if walThreshold <= 0 {
		walThreshold = researchWALPressureBytes
	}
	if walPoll <= 0 {
		walPoll = researchWALPressurePollPeriod
	}
	if walProbe == nil {
		walProbe = s.researchWALProbe
	}
	markWALPressure := func(bytes int64, active bool) {
		gate.mu.Lock()
		gate.deferred[name] = true
		first := !gate.walBusy
		gate.walBusy = true
		gate.mu.Unlock()
		if first && s.log != nil {
			message := "heavy research deferred until WAL checkpoint catches up"
			if active {
				message = "active heavy research canceled; retry waits for WAL checkpoint"
			}
			s.log.Warn(message, "job", name, "wal_mb", bytes>>20,
				"threshold_mb", walThreshold>>20)
		}
	}
	clearWALPressure := func() {
		gate.mu.Lock()
		recovered := gate.walBusy
		gate.walBusy = false
		gate.mu.Unlock()
		if recovered && s.log != nil {
			s.log.Info("heavy research resumed after WAL checkpoint")
		}
	}
	dataDir := s.cfg().DataDir
	admit := func() bool {
		gate.mu.Lock()
		defer gate.mu.Unlock()
		// If another named lane missed while this lane last owned the token, make the repeated
		// lane yield once. The next phased tick of the deferred lane can then acquire instead of
		// losing forever to deterministic timer ordering.
		if gate.last == name {
			for pending := range gate.deferred {
				if pending != name {
					gate.deferred[name] = true
					return false
				}
			}
		}
		delete(gate.deferred, name)
		return true
	}
	finish := func() {
		gate.mu.Lock()
		gate.last = name
		gate.mu.Unlock()
		gate.token <- struct{}{}
	}
	run := func() bool {
		// Only the current token owner may issue a PASSIVE checkpoint probe. A losing scheduler tick
		// must not publish a transient busy receipt that the legitimate owner's active guard could
		// mistake for new pressure and use to cancel healthy work.
		if err := ctx.Err(); err != nil {
			deferJob()
			gate.token <- struct{}{}
			return false
		}
		if s.researchLiveActive() {
			deferJob()
			gate.token <- struct{}{}
			return false
		}
		if pressured, bytes := walProbe(ctx, walThreshold, true); pressured {
			markWALPressure(bytes, false)
			gate.token <- struct{}{}
			return false
		}
		// ARM may have changed while the bounded logical probe was running. LIVE still wins before
		// the research callback receives any database time.
		if err := ctx.Err(); err != nil {
			deferJob()
			gate.token <- struct{}{}
			return false
		}
		if s.researchLiveActive() {
			deferJob()
			gate.token <- struct{}{}
			return false
		}
		clearWALPressure()
		defer finish()
		// Admission and ARM can race. Watch an admitted job too, cancel its cooperative context as
		// soon as LIVE becomes armed, and retain the token until the callback actually returns.
		liveCtx, cancelLive := context.WithCancel(ctx)
		liveStop := make(chan struct{})
		liveDone := make(chan struct{})
		var canceledForLive atomic.Bool
		go func() {
			defer close(liveDone)
			period := walPoll
			if period <= 0 || period > 100*time.Millisecond {
				period = 100 * time.Millisecond
			}
			ticker := time.NewTicker(period)
			defer ticker.Stop()
			for {
				if s.researchLiveActive() {
					if canceledForLive.CompareAndSwap(false, true) {
						deferJob()
						cancelLive()
						if s.log != nil {
							s.log.Info("active heavy research canceled while LIVE is armed", "job", name)
						}
					}
					return
				}
				select {
				case <-liveStop:
					return
				case <-liveCtx.Done():
					return
				case <-ticker.C:
				}
			}
		}()
		canceledForWAL := runHeavyResearchWithActiveWALGuard(liveCtx, dataDir, walThreshold, walPoll,
			func(bytes int64) { markWALPressure(bytes, true) }, fn, walProbe)
		close(liveStop)
		<-liveDone
		cancelLive()
		return !canceledForWAL && !canceledForLive.Load()
	}
	if wait <= 0 {
		select {
		case <-ctx.Done():
			return false
		case <-gate.token:
			if !admit() {
				gate.token <- struct{}{}
				return false
			}
			return run()
		default:
			deferJob()
			return false
		}
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		deferJob()
		return false
	case <-timer.C:
		deferJob()
		return false
	case <-gate.token:
		if !admit() {
			gate.token <- struct{}{}
			return false
		}
		return run()
	}
}
