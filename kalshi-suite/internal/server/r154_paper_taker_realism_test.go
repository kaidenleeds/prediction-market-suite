package server

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/config"
	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func r154PaperTakerFixture(t *testing.T, ticker string) (*Server, string,
	func(storage.Signal, float64, float64, float64) (storage.Signal, string, bool)) {
	t.Helper()
	s := testServer(t)
	s.mutateCfg(func(c *config.Config) { c.Auto.PaperTakerDelayMS = 1 })
	s.livePolicyMirrorWireDelay = time.Millisecond

	series := strings.ToUpper(strings.SplitN(ticker, "-", 2)[0])
	s.kalFeeMu.Lock()
	s.kalFees = map[string]kalFeeInfo{series: {taker: .07, typ: "quadratic", multiplier: 1}}
	s.kalEventFees = map[string]kalFeeInfo{}
	s.kalFeesAt = time.Now()
	s.kalFeeMu.Unlock()
	s.metaMu.Lock()
	if s.kmkts == nil {
		s.kmkts = map[string]kalshi.Market{}
	}
	if s.kmktsAt == nil {
		s.kmktsAt = map[string]time.Time{}
	}
	s.kmkts[ticker] = kalshi.Market{Ticker: ticker, EventTicker: series + "-EVENT"}
	s.kmktsAt[ticker] = time.Now()
	s.metaMu.Unlock()

	quoteFor := func(in storage.Signal, bid, ask, depth float64) (storage.Signal, string, bool) {
		out := in
		bidDepth, age, tick := 20.0, .01, .01
		fee, source, known := s.fillFeeReceipt("kalshi", ticker, false, 1, ask)
		if !known {
			t.Fatalf("exact taker fee unavailable: %q", source)
		}
		makerFee, _, makerKnown := s.fillFeeReceipt("kalshi", ticker, true, 1, bid)
		if !makerKnown {
			t.Fatal("exact maker fee unavailable")
		}
		out.EntryPrice, out.BookDepth, out.SpreadCents, out.FeePC = ask, depth, (ask-bid)*100, &fee
		out.BookFeatureVer, out.PricingVersion, out.BookSource =
			mlBookFeatureVersion, mlBookFeatureSchema,
			"kalshi_ws_full_orderbook:g1:s1:q1"
		out.BookBid, out.BookAsk = &bid, &ask
		out.BookBidDepth, out.BookAskDepth = &bidDepth, &depth
		out.BookQuoteAgeS, out.BookMakerTick, out.BookTakerTick = &age, &tick, &tick
		out.BookMakerFeePC, out.BookTakerFeePC = &makerFee, &fee
		return out, source, true
	}

	feeOne, _, ok := s.fillFeeReceipt("kalshi", ticker, false, 1, .40)
	if !ok {
		t.Fatal("one-share fee setup failed")
	}
	cell := verdictEnt{Family: "taker:r154-paper@kalshi", SourceFamily: "r154-paper",
		Platform: "kalshi", OriginLayer: "model", Route: "taker", Side: "YES", Group: "taker",
		N: 25, Mean: .08, MeanAsk: .40, FeePC: feeOne}
	injectVerdicts(s, []verdictEnt{cell})
	key := gfRosterKey("r154-paper", "kalshi", "YES", "taker")
	s.swMu.Lock()
	s.swTable = &swState{At: time.Now(), SubShare: map[string]float64{key: 100}}
	s.swMu.Unlock()
	return s, key, quoteFor
}

func TestR154DelayedPaperTakerZeroFillsAboveOriginalLimitAndReceipts(t *testing.T) {
	const ticker = "R154PAPER-LIMIT"
	s, key, quoteFor := r154PaperTakerFixture(t, ticker)
	calls := 0
	s.completeBookSignalFn = func(in storage.Signal) (storage.Signal, string, bool) {
		calls++
		if calls >= 4 {
			return quoteFor(in, .39, .41, 20) // still within the old 3¢ chase, but above the IOC limit
		}
		return quoteFor(in, .38, .40, 20)
	}
	started := time.Now()
	s.genfollowConsider(context.Background(), storage.Signal{Platform: "kalshi", Ticker: ticker,
		Title: "R154 delayed limit", Side: "YES", SignalType: "r154-paper",
		EntryPrice: .40, ResolveHours: 1})
	if calls != 4 {
		t.Fatalf("complete-book reads=%d want reference + sizing + delayed + final-wire", calls)
	}
	if elapsed := time.Since(started); elapsed < minPaperTakerDelay {
		t.Fatalf("Paper execution did not wait for the configured/clamped delay: %v", elapsed)
	}
	s.gfBookMu.Lock()
	open := 0
	if b := s.gfLoadLocked().Subs[key]; b != nil {
		open = len(b.Open)
	}
	s.gfBookMu.Unlock()
	if open != 0 {
		t.Fatalf("ask above the original limit created %d Paper lot(s)", open)
	}

	var attempts int
	if err := s.store.DBForTest().QueryRow(`SELECT COUNT(*) FROM audit_log
WHERE category='paper-taker-attempt' AND detail LIKE ?`, "%"+ticker+"%").Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if attempts != 1 {
		t.Fatalf("durable Paper attempt receipts=%d want 1", attempts)
	}
	var terminal string
	if err := s.store.DBForTest().QueryRow(`SELECT detail FROM audit_log
WHERE category='system-route' AND message LIKE ? ORDER BY id DESC LIMIT 1`,
		"PAPER-ZERO-FILL%"+ticker+"%").Scan(&terminal); err != nil {
		t.Fatal(err)
	}
	var receipt map[string]any
	if err := json.Unmarshal([]byte(terminal), &receipt); err != nil {
		t.Fatal(err)
	}
	if got := receipt["reason"]; got != "execution-ask-above-original-limit-at-final-wire" {
		t.Fatalf("zero-fill reason=%v", got)
	}
	if got, _ := receipt["attempted_contracts"].(float64); got < 1 {
		t.Fatalf("attempted quantity missing: %+v", receipt)
	}
	if got, _ := receipt["filled_contracts"].(float64); got != 0 {
		t.Fatalf("zero-fill receipt claims filled contracts: %+v", receipt)
	}
}

func TestR164PaperCancellationBeforeFinalBookIsNotObserved(t *testing.T) {
	tests := []struct {
		name           string
		cancelBookRead int
		wantReason     string
	}{
		{
			name:           "before delayed book",
			cancelBookRead: 2,
			wantReason:     "execution-context-ended-before-delayed-book-observation",
		},
		{
			name:           "during wire delay",
			cancelBookRead: 3,
			wantReason:     "execution-wire-delay-canceled-before-final-book-recheck",
		},
	}
	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ticker := fmt.Sprintf("R164PAPER-CANCEL-%d", i)
			s, key, quoteFor := r154PaperTakerFixture(t, ticker)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			calls := 0
			s.completeBookSignalFn = func(in storage.Signal) (storage.Signal, string, bool) {
				calls++
				out, source, ok := quoteFor(in, .38, .40, 20)
				if calls == tc.cancelBookRead {
					cancel()
				}
				return out, source, ok
			}
			s.genfollowConsider(ctx, storage.Signal{
				Platform: "kalshi", Ticker: ticker, Title: "R164 canceled observation",
				Side: "YES", SignalType: "r154-paper", EntryPrice: .40, ResolveHours: 1,
			})

			s.gfBookMu.Lock()
			open := 0
			if book := s.gfLoadLocked().Subs[key]; book != nil {
				open = len(book.Open)
			}
			s.gfBookMu.Unlock()
			if open != 0 {
				t.Fatalf("canceled observation created %d Paper lot(s)", open)
			}
			var terminal string
			if err := s.store.DBForTest().QueryRow(`SELECT detail FROM audit_log
WHERE category='system-route' AND message LIKE ? ORDER BY id DESC LIMIT 1`,
				"PAPER-NOT-OBSERVED%"+ticker+"%").Scan(&terminal); err != nil {
				t.Fatal(err)
			}
			var receipt map[string]any
			if err := json.Unmarshal([]byte(terminal), &receipt); err != nil {
				t.Fatal(err)
			}
			if got := receipt["reason"]; got != tc.wantReason {
				t.Fatalf("not-observed reason=%v want %q", got, tc.wantReason)
			}
			var falseZeroFills int
			if err := s.store.DBForTest().QueryRow(`SELECT COUNT(*) FROM audit_log
WHERE category='system-route' AND message LIKE ?`,
				"PAPER-ZERO-FILL%"+ticker+"%").Scan(&falseZeroFills); err != nil {
				t.Fatal(err)
			}
			if falseZeroFills != 0 {
				t.Fatalf("canceled observation emitted %d false zero-fill receipt(s)", falseZeroFills)
			}
		})
	}
}

func TestR165DelayedPaperTakerUsesOneShareExplorationWithinVisibleDepth(t *testing.T) {
	const ticker = "R154PAPER-DEPTH"
	s, key, quoteFor := r154PaperTakerFixture(t, ticker)
	calls := 0
	s.completeBookSignalFn = func(in storage.Signal) (storage.Signal, string, bool) {
		calls++
		if calls >= 4 {
			return quoteFor(in, .38, .40, 1)
		}
		return quoteFor(in, .38, .40, 20)
	}
	s.genfollowConsider(context.Background(), storage.Signal{Platform: "kalshi", Ticker: ticker,
		Title: "R154 delayed depth", Side: "YES", SignalType: "r154-paper",
		EntryPrice: .40, ResolveHours: 1})
	s.gfBookMu.Lock()
	book := s.gfLoadLocked().Subs[key]
	if book == nil || len(book.Open) != 1 {
		s.gfBookMu.Unlock()
		t.Fatalf("executable delayed quote did not create one Paper lot: %+v", book)
	}
	gotContracts, gotPrice := book.Open[0].Contracts, book.Open[0].Price
	s.gfBookMu.Unlock()
	if gotContracts != 1 || math.Abs(gotPrice-.40) > 1e-9 {
		t.Fatalf("delayed fill=%v @ %v want visible-depth cap 1 @ .40", gotContracts, gotPrice)
	}
	var terminal string
	if err := s.store.DBForTest().QueryRow(`SELECT detail FROM audit_log
WHERE category='system-route' AND message LIKE ? ORDER BY id DESC LIMIT 1`,
		"PAPER-FILLED%"+ticker+"%").Scan(&terminal); err != nil {
		t.Fatal(err)
	}
	var receipt map[string]any
	if err := json.Unmarshal([]byte(terminal), &receipt); err != nil {
		t.Fatal(err)
	}
	if attempted, _ := receipt["attempted_contracts"].(float64); attempted != 1 {
		t.Fatalf("corrected Paper exploration attempted=%v contracts; want exactly one", attempted)
	}
	if filled, _ := receipt["filled_contracts"].(float64); filled != 1 {
		t.Fatalf("terminal receipt lost partial fill: %+v", receipt)
	}
}

func TestR163FundedPaperRejectsFinalWireBookProvenanceRegression(t *testing.T) {
	const ticker = "R163PAPER-PROVENANCE"
	s, key, quoteFor := r154PaperTakerFixture(t, ticker)
	calls := 0
	s.completeBookSignalFn = func(in storage.Signal) (storage.Signal, string, bool) {
		calls++
		out, source, ok := quoteFor(in, .38, .40, 20)
		switch calls {
		case 3:
			out.BookSource = "kalshi_ws_full_orderbook:g1:s1:q2"
		case 4:
			out.BookSource = "kalshi_ws_full_orderbook:g1:s1:q1"
		}
		return out, source, ok
	}
	s.genfollowConsider(context.Background(), storage.Signal{
		Platform: "kalshi", Ticker: ticker, Title: "R163 provenance regression",
		Side: "YES", SignalType: "r154-paper", EntryPrice: .40, ResolveHours: 1,
	})
	s.gfBookMu.Lock()
	open := 0
	if book := s.gfLoadLocked().Subs[key]; book != nil {
		open = len(book.Open)
	}
	s.gfBookMu.Unlock()
	if open != 0 {
		t.Fatalf("backward final-wire sequence created %d Paper lot(s)", open)
	}
	var terminal string
	if err := s.store.DBForTest().QueryRow(`SELECT detail FROM audit_log
WHERE category='system-route' AND message LIKE ? ORDER BY id DESC LIMIT 1`,
		"PAPER-ZERO-FILL%"+ticker+"%").Scan(&terminal); err != nil {
		t.Fatal(err)
	}
	var receipt map[string]any
	if err := json.Unmarshal([]byte(terminal), &receipt); err != nil {
		t.Fatal(err)
	}
	if got := receipt["reason"]; got != "execution-final-wire-book-provenance-regressed" {
		t.Fatalf("provenance zero-fill reason=%v", got)
	}
}

func TestR154LiveIntentPublishesBeforePaperWorkerAndNeverWaitsForIt(t *testing.T) {
	const ticker = "R154PAPER-ORDERING"
	s, _, quoteFor := r154PaperTakerFixture(t, ticker)
	r163ArmKalshiFlowYES(t, s, ticker)
	key := gfRosterKey("kalshi-flow", "kalshi", "YES", "taker")

	paperEntered := make(chan struct{})
	unblockPaper := make(chan struct{})
	var calls atomic.Int32
	s.completeBookSignalFn = func(in storage.Signal) (storage.Signal, string, bool) {
		// Call one is LIVE's fresh preflight. Only the later Paper touch may block.
		if calls.Add(1) == 2 {
			close(paperEntered)
			<-unblockPaper
		}
		return quoteFor(in, .38, .40, 20)
	}
	returned := make(chan struct{})
	signalAt := time.Now()
	go func() {
		s.genfollowTriggeredSystems(context.Background(), storage.Signal{
			Platform: "kalshi", Ticker: ticker, Title: "R154 LIVE-first ordering",
			Side: "YES", SignalType: "kalshi-flow", EntryPrice: .40, ResolveHours: 1,
			ExecExpr: r147InputReceiptExpr("K", signalAt),
		})
		close(returned)
	}()
	select {
	case <-returned:
	case <-time.After(250 * time.Millisecond):
		close(unblockPaper)
		t.Fatal("signal producer waited behind Paper validation")
	}
	var liveIntent liveSignalIntent
	select {
	case liveIntent = <-s.liveSignalIntentChannel():
	case <-time.After(250 * time.Millisecond):
		close(unblockPaper)
		t.Fatal("LIVE intent was not published before funded Paper")
	}
	if !s.enqueueCanonicalSystemExecutionShadow(
		liveIntent.Signal, liveIntent.At, liveIntent.ShadowAttemptID) {
		close(unblockPaper)
		t.Fatal("selected fixture could not schedule canonical execution receipt")
	}
	// Run the actual cash preflight. It has no authenticated exchange cohort in this fixture, so it
	// must end as a durable no-send. Corrected Paper may start only from that exact terminal and must
	// not make the cash preflight wait for its later book touch.
	started := time.Now()
	if s.processLiveAllowlistedSignalIntentJob(context.Background(), liveIntent) {
		close(unblockPaper)
		t.Fatal("cash plan unexpectedly passed without authenticated prior exchange evidence")
	}
	if elapsed := time.Since(started); elapsed > 250*time.Millisecond {
		close(unblockPaper)
		t.Fatalf("cash preflight waited %v behind corrected Paper", elapsed)
	}
	select {
	case <-paperEntered:
	case <-time.After(3 * time.Second):
		close(unblockPaper)
		pending := 0
		s.genfollowPaperPassedSignals.Range(func(_, _ any) bool { pending++; return true })
		s.liveMu.Lock()
		logs := append([]map[string]any(nil), s.liveLog...)
		s.liveMu.Unlock()
		t.Fatalf("Paper worker never received the queued candidate: pending=%d logs=%+v", pending, logs)
	}
	if got := len(s.liveSignalIntentChannel()); got != 0 {
		close(unblockPaper)
		t.Fatalf("LIVE intent count=%d when Paper began; want completed LIVE lane first", got)
	}
	close(unblockPaper)
	drainCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := s.stopGenfollowPaperWorkers(drainCtx); err != nil {
		t.Fatalf("drain Paper worker: %v", err)
	}
	s.gfBookMu.Lock()
	book := s.gfLoadLocked().Subs[key]
	if book == nil || len(book.Open) != 1 {
		s.gfBookMu.Unlock()
		t.Fatalf("durable no-send terminal did not produce one labelled counterfactual: %+v", book)
	}
	lot := book.Open[0]
	s.gfBookMu.Unlock()
	if lot.ExecutionTruthContract != fundedPaperLiveTruthContractV1 ||
		lot.ExecutionLiveTerminalKind != fundedPaperLiveTerminalNoSend ||
		lot.ExecutionLiveTerminal != "not-sent" || lot.ExecutionLiveTerminalID == "" {
		t.Fatalf("Paper lot lost its durable LIVE terminal contract: %+v", lot)
	}
}

func TestR154FundedPaperBookCannotStartPolyUSOverflowREST(t *testing.T) {
	s := testServer(t)
	if _, _, ok := s.completeBookSignalCached(storage.Signal{
		Platform: "polyus", Ticker: "r154-paper-cache-only", Side: "YES",
	}); ok {
		t.Fatal("missing subscribed Paper book unexpectedly became executable")
	}
	s.polyUSOnDemandMu.Lock()
	state := s.polyUSOnDemand
	s.polyUSOnDemandMu.Unlock()
	if state != nil {
		t.Fatal("funded Paper initialized the PolyUS overflow REST lane")
	}
}

func TestR154FundedPaperUsesFixedLiveFirstGrace(t *testing.T) {
	enqueuedAt := time.Now().Add(-genfollowPaperLiveFirstGrace + 100*time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	started := time.Now()
	if !waitForGenfollowPaperDue(ctx, enqueuedAt.Add(genfollowPaperLiveFirstGrace)) {
		t.Fatal("Paper LIVE-first grace canceled unexpectedly")
	}
	if elapsed := time.Since(started); elapsed < 50*time.Millisecond || elapsed > 500*time.Millisecond {
		t.Fatalf("Paper fixed grace elapsed after %v; want about 100ms", elapsed)
	}
}

func TestR163FundedPaperUsesOneSerialMainLedgerExecutor(t *testing.T) {
	s := testServer(t)
	const jobs = 8
	var active, maxActive, finished atomic.Int32
	intents := make([]genfollowPaperIntent, 0, jobs)
	for i := 0; i < jobs; i++ {
		intents = append(intents, genfollowPaperIntent{
			EnqueuedAt: time.Now().Add(-genfollowPaperLiveFirstGrace),
			Signals: []genfollowPaperSignal{{
				Signal: storage.Signal{Platform: "kalshi", Ticker: fmt.Sprintf("R163-SAT-%03d", i),
					Side: "YES", SignalType: "kalshi-flow", EntryPrice: .40},
				testRun: func(ctx context.Context) {
					current := active.Add(1)
					for {
						prior := maxActive.Load()
						if current <= prior || maxActive.CompareAndSwap(prior, current) {
							break
						}
					}
					timer := time.NewTimer(10 * time.Millisecond)
					select {
					case <-timer.C:
					case <-ctx.Done():
						if !timer.Stop() {
							select {
							case <-timer.C:
							default:
							}
						}
					}
					active.Add(-1)
					finished.Add(1)
				},
			}},
		})
	}
	in := make(chan genfollowPaperIntent, jobs)
	done := make(chan struct{})
	for _, intent := range intents {
		in <- intent
	}
	close(in)
	go s.runGenfollowPaperWorkers(context.Background(), in, done)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("serial funded-Paper executor did not drain its bounded queue")
	}
	if got := finished.Load(); got != jobs {
		t.Fatalf("Paper executor completed %d/%d jobs", got, jobs)
	}
	if got := maxActive.Load(); got != genfollowPaperWorkerCount {
		t.Fatalf("Paper execution concurrency=%d want exactly %d", got, genfollowPaperWorkerCount)
	}
}

func TestR163FundedPaperWaitsForLiveCompletionAfterItsFixedDueTime(t *testing.T) {
	s := testServer(t)
	s.liveDispatchShadowQuietDelay = -1
	s.genfollowPaperQuietTimeout = time.Second
	in := make(chan genfollowPaperIntent, 1)
	done := make(chan struct{})
	paperStarted := make(chan time.Time, 1)
	enqueuedAt := time.Now().Add(-genfollowPaperLiveFirstGrace)
	endLive := s.beginLiveCashPriority()
	in <- genfollowPaperIntent{EnqueuedAt: enqueuedAt, Signals: []genfollowPaperSignal{{
		Signal: storage.Signal{Platform: "kalshi", Ticker: "R163-PAPER-DUE",
			Side: "YES", SignalType: "spotlag", EntryPrice: .40},
		testRun: func(context.Context) { paperStarted <- time.Now() },
	}}}
	close(in)
	go s.runGenfollowPaperWorkers(context.Background(), in, done)

	select {
	case <-paperStarted:
		endLive()
		t.Fatal("funded Paper began shared work while the LIVE lane was active")
	case <-time.After(100 * time.Millisecond):
	}
	releasedAt := time.Now()
	endLive()
	select {
	case began := <-paperStarted:
		if delay := began.Sub(releasedAt); delay > 250*time.Millisecond {
			t.Fatalf("Paper did not resume promptly after LIVE completed: %v", delay)
		}
	case <-time.After(time.Second):
		t.Fatal("Paper did not resume after the LIVE lane became quiet")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Paper worker did not finish after LIVE completion")
	}
}

func TestR163FundedPaperLivePriorityWaitIsBounded(t *testing.T) {
	s := testServer(t)
	s.liveDispatchShadowQuietDelay = -1
	s.genfollowPaperQuietTimeout = 75 * time.Millisecond
	endLive := s.beginLiveCashPriority()
	defer endLive()
	ran := make(chan struct{}, 1)
	in := make(chan genfollowPaperIntent, 1)
	in <- genfollowPaperIntent{EnqueuedAt: time.Now().Add(-genfollowPaperLiveFirstGrace),
		Signals: []genfollowPaperSignal{{
			Signal: storage.Signal{Platform: "kalshi", Ticker: "R163-PAPER-BOUNDED",
				Side: "YES", SignalType: "spotlag", EntryPrice: .40},
			testRun: func(context.Context) { ran <- struct{}{} },
		}}}
	close(in)
	done := make(chan struct{})
	started := time.Now()
	go s.runGenfollowPaperWorkers(context.Background(), in, done)
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Paper LIVE-priority wait exceeded its configured bound")
	}
	if elapsed := time.Since(started); elapsed < 50*time.Millisecond ||
		elapsed > 400*time.Millisecond {
		t.Fatalf("bounded Paper wait elapsed=%v", elapsed)
	}
	select {
	case <-ran:
		t.Fatal("timed-out Paper job touched shared work")
	default:
	}
}
