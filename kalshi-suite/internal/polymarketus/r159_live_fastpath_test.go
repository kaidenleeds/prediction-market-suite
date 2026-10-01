package polymarketus

import (
	"fmt"
	"testing"
	"time"
)

func TestR159CurrentOpenFullBookRequiresSameGenerationExplicitOpen(t *testing.T) {
	ws := (&Client{}).NewMarketsWS()
	ws.SetOpenSlugs([]string{"strict"})
	gen := ws.sessionSeq.Add(1)
	ws.notePrimaryStart(gen)
	ws.mu.Lock()
	ws.activeFullGen = gen
	ws.activeFull = map[string]bool{"strict": true}
	ws.mu.Unlock()
	frame := []byte(`{"marketData":{"marketSlug":"strict","state":"MARKET_STATE_OPEN","transactTime":"2026-07-18T12:00:00Z","bids":[{"px":"0.40","qty":"5"}],"offers":[{"px":"0.42","qty":"6"}]}}`)
	if !ws.ingestGeneration(frame, gen) {
		t.Fatal("current-generation full frame was not accepted")
	}
	ws.notePrimaryData(gen)

	receipt, ok := ws.CurrentOpenFullBook("strict")
	if !ok || receipt.Generation != gen || len(receipt.Bids) != 1 || len(receipt.Asks) != 1 ||
		receipt.ExplicitOpenAt.IsZero() {
		t.Fatalf("strict full-book receipt=%+v ok=%v", receipt, ok)
	}
	receipt.Bids[0].Quantity = 999
	if again, ok := ws.CurrentOpenFullBook("strict"); !ok || again.Bids[0].Quantity != 5 {
		t.Fatal("strict accessor exposed its mutable internal ladder")
	}

	// The broad research accessor may borrow a fresh complete REST-open proof. The money accessor
	// must still reject when explicit OPEN provenance is removed from the active WS generation.
	ws.mu.Lock()
	ws.live["strict"].stateKnown = false
	ws.live["strict"].stateGen = 0
	ws.live["strict"].fullLifecycleAt = time.Time{}
	ws.mu.Unlock()
	if _, _, _, _, ok := ws.FullBookLevelsAt("strict"); !ok {
		t.Fatal("fixture did not preserve the broader REST-backed research accessor")
	}
	if _, ok := ws.CurrentOpenFullBook("strict"); ok {
		t.Fatal("REST-open proof incorrectly substituted for current-generation WS OPEN")
	}

	// A connection handoff invalidates every prior-generation full book immediately.
	gen2 := ws.sessionSeq.Add(1)
	ws.notePrimaryStart(gen2)
	ws.notePrimaryData(gen2)
	if _, ok := ws.CurrentOpenFullBook("strict"); ok {
		t.Fatal("retired generation retained execution authority")
	}
}

func TestR159HotRotationInvalidatesEvictedFullDepthButKeepsLiteBBO(t *testing.T) {
	ws := (&Client{}).NewMarketsWSWithFullBookCap(MinMarketsFullBookCap)
	priority := make([]string, 0, MinMarketsFullBookCap+10)
	for i := 0; i < MinMarketsFullBookCap+10; i++ {
		priority = append(priority, fmt.Sprintf("rotate-%03d", i))
	}
	ws.SetOpenSlugs(priority)
	gen := ws.sessionSeq.Add(1)
	ws.notePrimaryStart(gen)
	s := &pusSession{gen: gen, writeJSON: func(any) error { return nil }}
	s.primary.Store(true)
	if err := ws.subscribeMarkets(s, priority); err != nil {
		t.Fatalf("initial exact subscription failed: %v", err)
	}
	evicted := fmt.Sprintf("rotate-%03d", MinMarketsFullBookCap-1)
	hot := fmt.Sprintf("rotate-%03d", MinMarketsFullBookCap+9)
	fullFrame := func(slug string) []byte {
		return []byte(fmt.Sprintf(`{"marketData":{"marketSlug":%q,"state":"MARKET_STATE_OPEN","bids":[{"px":"0.40","qty":"5"}],"offers":[{"px":"0.42","qty":"6"}]}}`, slug))
	}
	if !ws.ingestGeneration(fullFrame(evicted), gen) {
		t.Fatal("initial evictable full frame was not accepted")
	}
	ws.notePrimaryData(gen)
	if _, ok := ws.CurrentOpenFullBook(evicted); !ok {
		t.Fatal("installed full-depth member lacked execution authority")
	}

	ws.mu.Lock()
	ws.hotFull = map[string]time.Time{hot: time.Now()}
	ws.mu.Unlock()
	if err := ws.subscribeMarkets(s, priority); err != nil {
		t.Fatalf("hot exact subscription rotation failed: %v", err)
	}
	if _, ok := ws.CurrentOpenFullBook(evicted); ok {
		t.Fatal("evicted same-generation slug retained stale execution depth")
	}
	// Full-depth invalidation must not tear down the broad LITE/TRADE view. The last BBO remains
	// usable under the fresh complete REST-open proof until the next LITE change arrives.
	if bid, ask, ok := ws.LiveBidAsk(evicted); !ok || bid != .40 || ask != .42 {
		t.Fatalf("full-board BBO was lost with hot eviction: %.2f/%.2f ok=%v", bid, ask, ok)
	}
	if _, ok := ws.CurrentOpenFullBook(hot); ok {
		t.Fatal("newly subscribed hot slug became executable before its own full snapshot")
	}
	if !ws.ingestGeneration(fullFrame(hot), gen) {
		t.Fatal("hot full frame was not accepted")
	}
	ws.notePrimaryData(gen)
	if receipt, ok := ws.CurrentOpenFullBook(hot); !ok || receipt.Generation != gen {
		t.Fatalf("hot slug did not gain authority from its own new snapshot: %+v ok=%v",
			receipt, ok)
	}
}

func TestR159HotFullPriorityReplacesOnlyBoundedTail(t *testing.T) {
	priority := make([]string, 0, 120)
	for i := 0; i < 120; i++ {
		priority = append(priority, fmt.Sprintf("m-%03d", i))
	}
	got := fullPriorityWithHot(priority, 100, []string{"m-110", "m-111", "m-050", "missing"})
	if len(got) != 100 {
		t.Fatalf("hot full priority len=%d, want 100", len(got))
	}
	for i := 0; i < 98; i++ {
		if got[i] != priority[i] {
			t.Fatalf("stable base prefix moved at %d: got=%q want=%q", i, got[i], priority[i])
		}
	}
	if got[98] != "m-110" || got[99] != "m-111" {
		t.Fatalf("hot tail=%v, want m-110/m-111", got[98:])
	}
}

func TestR159PromoteFullBookIsProvenOpenBoundedAndNonblocking(t *testing.T) {
	ws := (&Client{}).NewMarketsWS()
	open := make([]string, 0, marketsHotFullBookCap+2)
	for i := 0; i < marketsHotFullBookCap+2; i++ {
		open = append(open, fmt.Sprintf("hot-%03d", i))
	}
	ws.SetOpenSlugs(open)
	wakes := 0
	ws.promoteMu.Lock()
	ws.promoteFn = func() bool { wakes++; return true }
	ws.promoteMu.Unlock()

	if ws.PromoteFullBook("not-open") {
		t.Fatal("unknown market was promoted")
	}
	for _, slug := range open {
		if !ws.PromoteFullBook(slug) {
			t.Fatalf("proven-open %q did not wake promotion", slug)
		}
	}
	ws.mu.Lock()
	n := len(ws.hotFull)
	_, oldestStillPresent := ws.hotFull[open[0]]
	ws.mu.Unlock()
	if n != marketsHotFullBookCap || oldestStillPresent {
		t.Fatalf("hot set len=%d oldest_present=%v, want bounded cap with oldest evicted",
			n, oldestStillPresent)
	}
	if wakes != len(open) {
		t.Fatalf("promotion wakes=%d, want %d", wakes, len(open))
	}
}
