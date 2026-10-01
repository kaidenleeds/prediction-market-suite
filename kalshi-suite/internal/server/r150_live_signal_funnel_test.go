package server

import (
	"context"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestR150AllowedSignalHintClosesBookPriorityCircularity(t *testing.T) {
	s := testServer(t)
	cfg := *s.cfg()
	cfg.Risk.LiveProspectiveAllocation = true
	cfg.Risk.LiveSystemKalshi = true
	cfg.Risk.LiveSystemAllowlist = "kalshi|kalshi-flow|YES|taker,kalshi|spotlag|NO|taker"
	s.cfgP.Store(&cfg)

	s.noteLiveSignalDepthBookHint(storage.Signal{Platform: "kalshi", Ticker: "KX-ALLOW-YES",
		Side: "YES", SignalType: "kalshi-flow"})
	s.noteLiveSignalDepthBookHint(storage.Signal{Platform: "kalshi", Ticker: "KX-NOT-ALLOW-NO",
		Side: "NO", SignalType: "kalshi-flow"})
	s.noteLiveSignalDepthBookHint(storage.Signal{Platform: "kalshi", Ticker: "KX-ALLOW-SPOT",
		Side: "NO", SignalType: "spotlag"})

	groups := s.positiveDepthBookGroups("kalshi")
	got := map[string][]string{}
	for _, group := range groups {
		got[group.System+"|"+group.Side] = group.IDs
	}
	if ids := got["kalshi-flow|YES"]; len(ids) != 1 || ids[0] != "KX-ALLOW-YES" {
		t.Fatalf("allowed Kalshi-flow signal was not promoted into full-depth priority: %+v", groups)
	}
	if _, exists := got["kalshi-flow|NO"]; exists {
		t.Fatalf("unallowlisted side borrowed a full-depth LIVE hint: %+v", groups)
	}
	if ids := got["spotlag|NO"]; len(ids) != 1 || ids[0] != "KX-ALLOW-SPOT" {
		t.Fatalf("allowed spotlag signal was not promoted into full-depth priority: %+v", groups)
	}

	// Explicit positive routes are selected before the generic near-horizon tail. This is a WS
	// subscription hint only; no Paper or LIVE authority is asserted by the planner.
	plan := planDepthBooks(1, nil,
		[]depthBookCandidate{{ID: "KX-GENERIC-NEAR", Near: true, ResolveAt: time.Now().Add(time.Hour)}},
		groups[:1], 4, 2, true)
	if len(plan.IDs) != 1 || plan.IDs[0] != "KX-ALLOW-YES" {
		t.Fatalf("generic horizon retained the seat needed by the current allowed signal: %+v", plan)
	}
}

func TestR150LiveMirrorKalshiFullBookReturnsExecutableTick(t *testing.T) {
	const ticker = "KXR150-TICK"
	s := testServer(t)
	s.kalshiExecutableBookFn = func(candidate liveMirrorCandidate) (r159KalshiExecutableBook, string) {
		if candidate.Ticker != ticker || candidate.Side != "YES" {
			return r159KalshiExecutableBook{}, "unexpected-test-candidate"
		}
		now := time.Now()
		return r159KalshiExecutableBook{
			MakerPrice: .40, MakerDepth: 10, TakerPrice: .42, TakerDepth: 8,
			MakerTick: .01, TakerTick: .01, SourceAt: now, ReceivedAt: now,
			Source: "kalshi_ws_full_orderbook:test",
		}, ""
	}
	cfg := *s.cfg()
	cfg.Auto.RouteQueueEnabled = false
	cfg.Risk.LiveProspectiveAllocation = true
	cfg.Risk.LiveSystemKalshi = true
	s.cfgP.Store(&cfg)

	for _, tc := range []struct {
		name, family string
		wantMaker    bool
		wantPrice    float64
	}{
		{name: "maker route keeps market tick", wantMaker: true, wantPrice: .40},
		{name: "prospective taker route keeps market tick", family: "spotlag", wantMaker: false, wantPrice: .42},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, why := s.liveMirrorExecutable(t.Context(), liveMirrorCandidate{
				Platform: "kalshi", Ticker: ticker, Side: "YES", Family: tc.family,
				Source: "r150-tick-regression", Price: .42, At: time.Now(),
			})
			if why != "" || q.Tick != .01 || q.Price != tc.wantPrice || q.Maker != tc.wantMaker || q.Depth <= 0 {
				t.Fatalf("valid full Kalshi book lost its executable tick: quote=%+v reason=%q", q, why)
			}
		})
	}
}

func TestR150UnknownSignalHorizonRefreshesThenPaperFillsAndQueues(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	ticker := "KXR150-FLOW-YES"

	cfg := *s.cfg()
	cfg.Risk.LiveProspectiveAllocation = true
	cfg.Risk.LiveSystemKalshi = true
	cfg.Risk.LiveSystemAllowlist = "kalshi|kalshi-flow|YES|taker"
	cfg.Auto.PaperTakerDelayMS = 1
	s.cfgP.Store(&cfg)
	s.paperHorizonFn = func(context.Context, string, string, string) float64 { return 1 }
	s.liveMu.Lock()
	s.liveArmed, s.liveAuto = true, true
	s.liveMu.Unlock()

	s.kalFeeMu.Lock()
	s.kalFees = map[string]kalFeeInfo{"KXR150": {taker: .07, typ: "quadratic", multiplier: 1}}
	s.kalEventFees = map[string]kalFeeInfo{}
	s.kalFeesAt = time.Now()
	s.kalFeeMu.Unlock()
	s.metaMu.Lock()
	if s.kmktsAt == nil {
		s.kmktsAt = map[string]time.Time{}
	}
	s.kmktsAt[ticker] = time.Now()
	s.metaMu.Unlock()

	quoteFor := func(in storage.Signal) (storage.Signal, string, bool) {
		if in.Side != "YES" {
			return storage.Signal{}, "", false
		}
		out := in
		bid, ask, bidDepth, askDepth, age, tick := .38, .40, 20.0, 20.0, .01, .01
		fee, source, known := s.fillFeeReceipt("kalshi", in.Ticker, false, 1, ask)
		makerFee, _, makerKnown := s.fillFeeReceipt("kalshi", in.Ticker, true, 1, bid)
		if !known || !makerKnown {
			t.Fatalf("exact fee fixture unavailable: taker=%v maker=%v source=%q", known, makerKnown, source)
		}
		out.EntryPrice, out.BookDepth, out.SpreadCents, out.FeePC = ask, askDepth, 2, &fee
		out.BookFeatureVer, out.PricingVersion, out.BookSource = mlBookFeatureVersion, mlBookFeatureSchema,
			"kalshi_ws_full_orderbook:g1:s1:q1"
		out.BookBid, out.BookAsk, out.BookBidDepth, out.BookAskDepth = &bid, &ask, &bidDepth, &askDepth
		out.BookQuoteAgeS, out.BookMakerTick, out.BookTakerTick = &age, &tick, &tick
		out.BookMakerFeePC, out.BookTakerFeePC = &makerFee, &fee
		return out, source, true
	}
	s.completeBookSignalFn = quoteFor
	feeOne, _, ok := s.fillFeeReceipt("kalshi", ticker, false, 1, .40)
	if !ok {
		t.Fatal("one-share fee setup failed")
	}
	cell := verdictEnt{Family: "taker:kalshi-flow@kalshi", SourceFamily: "kalshi-flow",
		Platform: "kalshi", OriginLayer: "model", Route: "taker", Side: "YES", Group: "taker",
		N: 100, Mean: .08, MeanAsk: .40, FeePC: feeOne}
	injectVerdicts(s, []verdictEnt{cell})
	key := gfRosterKey("kalshi-flow", "kalshi", "YES", "taker")
	s.swMu.Lock()
	s.swTable = &swState{At: time.Now(), SubShare: map[string]float64{key: 100}}
	s.swMu.Unlock()

	// This is the exact overnight failure shape: the fast producer knew the signal but its cache
	// did not know the market clock, so ResolveHours arrived as zero.
	sig := storage.Signal{Platform: "kalshi", Ticker: ticker, Title: "R150 flow",
		Side: "YES", SignalType: "kalshi-flow", EntryPrice: .39, ResolveHours: 0}
	if err := s.insertSignal(ctx, sig); err != nil {
		t.Fatal(err)
	}
	if got := len(s.liveSignalIntentChannel()); got != 1 {
		t.Fatalf("selected signal did not enter LIVE-first queue before Paper: got=%d want=1", got)
	}
	intent := <-s.liveSignalIntentChannel()
	if intent.Signal.Ticker != ticker || intent.Signal.Side != "YES" ||
		intent.Signal.SignalType != "kalshi-flow" || intent.ShadowAttemptID == "" {
		t.Fatalf("LIVE-first intent lost exact selected signal lineage: %+v", intent)
	}
	// The retired-cash fixture does not schedule the ordinary post-terminal comparison itself.
	// Funded Paper now requires that shared canonical execution receipt and may not invent a later
	// private execution window.
	if !s.enqueueCanonicalSystemExecutionShadow(intent.Signal, intent.At, intent.ShadowAttemptID) {
		t.Fatal("selected fixture could not schedule canonical execution receipt")
	}
	// Run the real preflight job. R165 retires new cash, so this must return false, but only after it
	// has registered the exact corrected Paper comparison and emitted the durable local terminal.
	if s.processLiveAllowlistedSignalIntentJob(ctx, intent) {
		t.Fatal("retired R165 cash lane unexpectedly admitted an order")
	}
	r163AwaitDurableIntentNoSend(t, s, intent)

	deadline := time.Now().Add(3 * time.Second)
	var book *kfBook
	var queued []liveMirrorCandidate
	filled := false
	for {
		s.gfBookMu.Lock()
		if current := s.gfLoadLocked().Subs[key]; current != nil {
			copyBook := *current
			copyBook.Open = append([]kfPos(nil), current.Open...)
			book = &copyBook
		}
		filled = book != nil && len(book.Open) == 1 && book.Open[0].Ticker == ticker &&
			book.Open[0].Side == "YES" && book.Open[0].Price == .40
		s.gfBookMu.Unlock()
		s.liveMirrorMu.Lock()
		queued = append([]liveMirrorCandidate(nil), s.liveMirrorQ...)
		s.liveMirrorMu.Unlock()
		if filled {
			break
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !filled {
		var auditMessage, auditDetail string
		_ = s.store.DBForTest().QueryRowContext(ctx, `SELECT message,COALESCE(detail,'') FROM audit_log
WHERE category='system-route' AND message LIKE '%kalshi-flow%' ORDER BY id DESC LIMIT 1`).
			Scan(&auditMessage, &auditDetail)
		t.Fatalf("unknown producer clock still starved the Paper route: book=%+v audit=%q %s",
			book, auditMessage, auditDetail)
	}
	if len(queued) != 0 {
		t.Fatalf("Paper completion created a stale second LIVE handoff: %+v", queued)
	}
	groups := s.positiveDepthBookGroups("kalshi")
	if len(groups) != 1 || groups[0].System != "kalshi-flow" || len(groups[0].IDs) != 1 ||
		groups[0].IDs[0] != ticker {
		t.Fatalf("signal did not leave a bounded WS priority retry hint: %+v", groups)
	}
}
