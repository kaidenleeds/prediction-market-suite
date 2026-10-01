package server

import (
	"context"
	"encoding/json"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
)

// paperMonitorLane is one independently scheduled MonitorPaper responsibility.
// Keeping settlement responsibilities in separate lanes is important: venue resolution can spend
// minutes on REST/backfill work, while the book and experiment ledgers mostly fold results that are
// already durable. Those cheap folds must not sit behind the venue backlog.
type paperMonitorLane struct {
	name     string
	every    time.Duration
	deadline time.Duration
	work     func(context.Context)
}

// startBoundedMonitorLane runs one immediate pass, then one pass per cadence. Each caller owns its
// goroutine, deadline and pulse receipt; a blocked lane therefore cannot queue work in another lane.
// Work is intentionally serial within a lane so book files and the live ledger retain their prior
// ordering and never overlap with themselves.
func startBoundedMonitorLane(
	ctx context.Context,
	name string,
	every, deadline time.Duration,
	work func(context.Context),
	pulseStart func(string),
	guarded func(string, func()),
	pulseDone func(string),
) {
	pass := func() {
		pulseStart(name)
		tctx, cancel := context.WithTimeout(ctx, deadline)
		guarded(name, func() { work(tctx) })
		cancel()
		pulseDone(name)
	}
	go func() {
		pass()
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				pass()
			}
		}
	}()
}

// settlementMonitorLanes is the settlement topology used by MonitorPaper. A lane may be internally
// sequential, but no lane waits behind another:
//   - core: authoritative venue resolution and the broad signal/tape drain;
//   - books: funded-Paper strategy ledgers and their file-backed books;
//   - research: one-share/proper-score evidence ledgers;
//   - funded-combos: the actual Paper Combos portfolio, isolated from suggestion/backfill work;
//   - staged-bundle-paper: additive 2-6 leg Paper packages and their authoritative settlement;
//   - subcent-golf: settlement-only follow-up for the prospective golf cohort;
//   - crossvenue: combo-overlay and lock legs, whose two venues must remain ordered together;
//   - live: real-order journals, kept together because both share live-state bookkeeping.
func (s *Server) settlementMonitorLanes() []paperMonitorLane {
	return []paperMonitorLane{
		{name: "settlement-core", every: 7 * time.Second, deadline: 4 * time.Minute, work: s.sweepSettlements},
		{name: "settlement-books", every: 7 * time.Second, deadline: 90 * time.Second, work: s.settlePaperStrategyBooks},
		{name: "settlement-research", every: 10 * time.Second, deadline: 90 * time.Second, work: s.settleResearchLedgers},
		{name: "settlement-funded-combos", every: 10 * time.Second, deadline: 45 * time.Second, work: s.settleParlays},
		{name: "staged-bundle-paper", every: 2 * time.Second, deadline: 30 * time.Second, work: s.sweepR148StagedPaperBundles},
		{name: "settlement-subcent-golf", every: 90 * time.Second, deadline: 75 * time.Second, work: s.settleSubcentGolfLedger},
		{name: "settlement-crossvenue", every: 11 * time.Second, deadline: 2 * time.Minute, work: s.settleCrossVenueLedgers},
		{name: "settlement-live", every: 15 * time.Second, deadline: 2 * time.Minute, work: s.settleLiveLedgers},
	}
}

func (s *Server) settlePaperStrategyBooks(ctx context.Context) {
	// Resolve the tickers actually held by strategy books before folding their files. The helper is
	// bounded and venue-authoritative; broad research-signal history stays in settlement-core.
	s.sweepPriorityPaperSettlements(ctx)
	s.settleMLBook(ctx)
	s.settleKflowBooks(ctx)
	s.settleRawFlowBook(ctx)
	s.settleWeatherBook(ctx)
	s.settleFreshInvBook(ctx)
	s.settleXvgBook(ctx)
	s.settleFavLong80Book(ctx)
	s.settleCheapbandBook(ctx)
	s.settleGenfollowBook(ctx)
}

func (s *Server) settleResearchLedgers(ctx context.Context) {
	// These ledgers grade research observations, not funded Paper or LIVE positions. Keep their
	// broad SQLite joins behind the same cooperative gate as every other analytic report so an
	// ARM transition cancels the pass and the live account/order/settlement writers stay first.
	s.tryRunHeavyResearch(ctx, "settlement-research-ledgers", 0, func(workCtx context.Context) {
		s.settleUnitTrials(workCtx)
		s.settleProperScoreTrials(workCtx)
	})
}

// settleSubcentGolfLedger performs only terminal-outcome follow-up. Candidate discovery and book
// sampling stay on their ten-minute research cadence; this lane gives the existing capped resolver
// a fresh chance every 90 seconds without creating another depth subscription or collection scan.
func (s *Server) settleSubcentGolfLedger(ctx context.Context) {
	s.tryRunHeavyResearch(ctx, "settlement-subcent-golf", 0, s.settleSubcentGolfLedgerResearch)
}

func (s *Server) settleSubcentGolfLedgerResearch(ctx context.Context) {
	if s.kal == nil || s.store == nil {
		return
	}
	board, _ := s.kal.TopLiquidCached()
	s.metaMu.Lock()
	warm := make(map[string]kalshi.Market, len(s.kmkts))
	for ticker, market := range s.kmkts {
		warm[ticker] = market
	}
	s.metaMu.Unlock()
	s.settleSubcentGolf(ctx, completeKalshiBookMarkets(board, warm), time.Now().UTC())
}

func (s *Server) settleCrossVenueLedgers(ctx context.Context) {
	s.settleXvComboBook(ctx)
	s.settleXvLocks(ctx)
}

// beginLiveSettlementLanePass and finishLiveSettlementLanePass are the lifecycle receipt for the
// complete scheduled LIVE-settlement pass. The individual methods below have their own locks, but
// no collection of momentary TryLock checks can prove that a serial pass is not between methods.
// The irreversible final-snapshot boundary closes admission here and waits for the pass as a unit.
func (s *Server) beginLiveSettlementLanePass() bool {
	if s == nil {
		return false
	}
	s.liveSettlementLaneLifecycleMu.Lock()
	defer s.liveSettlementLaneLifecycleMu.Unlock()
	if s.liveSettlementLaneClosing {
		return false
	}
	if s.liveSettlementLaneDone == nil {
		s.liveSettlementLaneDone = make(chan struct{})
	}
	s.liveSettlementLaneInFlight++
	return true
}

func (s *Server) finishLiveSettlementLanePass() {
	if s == nil {
		return
	}
	s.liveSettlementLaneLifecycleMu.Lock()
	if s.liveSettlementLaneInFlight > 0 {
		s.liveSettlementLaneInFlight--
	}
	if s.liveSettlementLaneClosing && s.liveSettlementLaneInFlight == 0 &&
		!s.liveSettlementLaneDoneClosed {
		close(s.liveSettlementLaneDone)
		s.liveSettlementLaneDoneClosed = true
	}
	s.liveSettlementLaneLifecycleMu.Unlock()
}

func (s *Server) fenceAndJoinLiveSettlementLane(ctx context.Context) error {
	if s == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	s.liveSettlementLaneLifecycleMu.Lock()
	if s.liveSettlementLaneDone == nil {
		s.liveSettlementLaneDone = make(chan struct{})
	}
	s.liveSettlementLaneClosing = true
	if s.liveSettlementLaneInFlight == 0 && !s.liveSettlementLaneDoneClosed {
		close(s.liveSettlementLaneDone)
		s.liveSettlementLaneDoneClosed = true
	}
	done := s.liveSettlementLaneDone
	s.liveSettlementLaneLifecycleMu.Unlock()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Server) liveSettlementLaneCheckpoint(step string) {
	if s != nil && s.liveSettlementLaneStep != nil {
		s.liveSettlementLaneStep(step)
	}
}

func (s *Server) settleLiveLedgers(ctx context.Context) {
	if !s.beginLiveSettlementLanePass() {
		return
	}
	defer s.finishLiveSettlementLanePass()
	s.sweepLiveCombos(ctx)
	s.liveSettlementLaneCheckpoint("after-live-combos")
	s.sweepLiveSettlements(ctx)
	s.liveSettlementLaneCheckpoint("after-live-settlements")
	s.settleLiveDispatchShadows(ctx)
	s.liveSettlementLaneCheckpoint("after-live-dispatch-shadow")
	s.recoverLivePolicyMirrorExecutionGaps(ctx)
	s.liveSettlementLaneCheckpoint("after-live-policy-mirror-recovery")
	s.recoverFundedPaperGaps(ctx)
	s.liveSettlementLaneCheckpoint("after-funded-paper-recovery")
	s.settleLivePolicyMirror(ctx)
	s.liveSettlementLaneCheckpoint("after-live-policy-mirror")
	s.settleExecutionShadowLedger(ctx)
	s.liveSettlementLaneCheckpoint("after-execution-shadow")
}

const signalResolutionCursorKV = "r142:signal-resolution-cursors"

type signalResolutionCursorState struct {
	Kalshi     int `json:"kalshi"`
	Polymarket int `json:"polymarket"`
	PolyUS     int `json:"polyus"`
}

// loadSignalResolutionCursors restores the broad signal-drain bookmark once per process. The open
// ticker lists are stable, oldest-first views; integer bookmarks are deliberately clamped by
// rrWindow, so resolution/removal between boots cannot make an old bookmark unsafe.
// sweepSettlements is the sole owner of these fields, so no second lock is needed around rrWindow.
func (s *Server) loadSignalResolutionCursors(ctx context.Context) {
	if s.sigCurLoaded || s.store == nil {
		return
	}
	s.sigCurLoaded = true
	raw, ok := s.store.KVGet(ctx, signalResolutionCursorKV)
	if !ok || raw == "" {
		return
	}
	var state signalResolutionCursorState
	if json.Unmarshal([]byte(raw), &state) != nil {
		return
	}
	if state.Kalshi >= 0 {
		s.sigCurKal = state.Kalshi
	}
	if state.Polymarket >= 0 {
		s.sigCurPoly = state.Polymarket
	}
	if state.PolyUS >= 0 {
		s.sigCurPus = state.PolyUS
	}
}

// persistSignalResolutionCursors makes progress survive the short operator restarts that used to
// reset all three drains to index zero. The write gets a detached but strictly bounded grace period:
// a just-expired four-minute core pass may save its completed windows, but shutdown can wait at most
// two seconds and there is no unbounded context.WithoutCancel work.
func (s *Server) persistSignalResolutionCursors() {
	if !s.sigCurLoaded || s.store == nil {
		return
	}
	state := signalResolutionCursorState{
		Kalshi:     s.sigCurKal,
		Polymarket: s.sigCurPoly,
		PolyUS:     s.sigCurPus,
	}
	raw, err := json.Marshal(state)
	if err != nil {
		return
	}
	pctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := s.store.KVSet(pctx, signalResolutionCursorKV, string(raw)); err != nil && s.log != nil {
		s.log.Warn("signal-resolution cursor persist failed", "err", err)
	}
}
