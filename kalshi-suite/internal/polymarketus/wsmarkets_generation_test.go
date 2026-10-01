package polymarketus

import (
	"errors"
	"math"
	"testing"
	"time"
)

func TestMarketsWSQuietBooksFollowHealthyGenerationNotWallClock(t *testing.T) {
	ws := (&Client{}).NewMarketsWS()
	ws.SetOpenSlugs([]string{"quiet"})
	gen := ws.sessionSeq.Add(1)
	ws.notePrimaryStart(gen)
	frame := []byte(`{"marketData":{"marketSlug":"quiet","state":"MARKET_STATE_OPEN","transactTime":"2020-01-01T00:00:00Z","bids":[{"px":"0.40","qty":"5"}],"offers":[{"px":"0.42","qty":"6"}]}}`)
	if !ws.ingestGeneration(frame, gen) {
		t.Fatal("valid current-generation full snapshot was not accepted")
	}
	ws.notePrimaryData(gen)

	ws.mu.Lock()
	e := ws.live["quiet"]
	e.at = time.Now().Add(-6 * time.Hour)
	e.quoteAt = time.Now().Add(-6 * time.Hour)
	e.bookAt = time.Now().Add(-6 * time.Hour)
	ws.mu.Unlock()

	if px, ok := ws.LiveYes("quiet"); !ok || math.Abs(px-0.41) > 1e-9 {
		t.Fatalf("quiet current-generation price = %.2f ok=%v", px, ok)
	}
	if bid, ask, ok := ws.LiveBidAsk("quiet"); !ok || bid != 0.40 || ask != 0.42 {
		t.Fatalf("quiet current-generation BBO = %.2f/%.2f ok=%v", bid, ask, ok)
	}
	if _, _, _, _, ok := ws.Book3("quiet"); !ok {
		t.Fatal("quiet current-generation book shape expired by wall clock")
	}
	if _, _, _, _, _, ok := ws.FullBookTouchAt("quiet"); !ok {
		t.Fatal("quiet current-generation touch depth expired by wall clock")
	}
	if _, _, _, _, ok := ws.FullBookLevelsAt("quiet"); !ok {
		t.Fatal("quiet current-generation full levels expired by wall clock")
	}
	if ws.Count() != 1 || ws.ExecutableCount() != 1 {
		t.Fatalf("quiet counts=%d/%d, want 1/1", ws.Count(), ws.ExecutableCount())
	}
	if got := ws.ConnectionReceipt(); got.ActiveGeneration != gen || got.QuietExecutable != 1 || !got.DataSeen {
		t.Fatalf("quiet connection receipt=%+v", got)
	}

	// A quiet individual book remains valid, but a globally data-dark primary does not. Heartbeats
	// alone cannot extend authority after the bounded full-universe reconciliation window.
	ws.primaryDataAt.Store(time.Now().Add(-MarketsWSPrimaryDataMaxAge - time.Second).UnixNano())
	ws.notePrimaryTransport(gen)
	if ws.Count() != 0 || ws.ExecutableCount() != 0 {
		t.Fatalf("globally dark primary retained authority: count=%d exec=%d", ws.Count(), ws.ExecutableCount())
	}
	if _, _, _, _, ok := ws.FullBookLevelsAt("quiet"); ok {
		t.Fatal("globally dark primary retained full-depth authority")
	}
	ws.notePrimaryData(gen)
	if _, _, ok := ws.LiveBidAsk("quiet"); !ok {
		t.Fatal("a new real payload did not restore same-generation authority")
	}
}

func TestMarketsWSReconnectFailsClosedUntilNewGenerationSnapshot(t *testing.T) {
	ws := (&Client{}).NewMarketsWS()
	ws.SetOpenSlugs([]string{"m"})
	gen1 := ws.sessionSeq.Add(1)
	ws.notePrimaryStart(gen1)
	full1 := []byte(`{"marketData":{"marketSlug":"m","state":"MARKET_STATE_OPEN","bids":[{"px":"0.30","qty":"5"}],"offers":[{"px":"0.34","qty":"6"}]}}`)
	if !ws.ingestGeneration(full1, gen1) {
		t.Fatal("generation 1 snapshot rejected")
	}
	ws.notePrimaryData(gen1)

	gen2 := ws.sessionSeq.Add(1)
	if gen2 <= gen1 {
		t.Fatalf("session generations not monotonic: %d then %d", gen1, gen2)
	}
	ws.notePrimaryStart(gen2)
	ws.notePrimaryTransport(gen2) // heartbeat/pong is vitality, never a replacement book
	if ws.Count() != 0 || ws.ExecutableCount() != 0 {
		t.Fatalf("old generation survived handoff: count=%d exec=%d", ws.Count(), ws.ExecutableCount())
	}
	if got := ws.ConnectionReceipt(); got.DataSeen {
		t.Fatalf("heartbeat fabricated first market data: %+v", got)
	}
	if ws.ingestGeneration(full1, gen1) {
		t.Fatal("late frame from retired generation was accepted")
	}
	if _, _, ok := ws.LiveBidAsk("m"); ok {
		t.Fatal("late retired frame restored executable authority")
	}

	lite2 := []byte(`{"marketDataLite":{"marketSlug":"m","bestBid":"0.31","bestAsk":"0.35"}}`)
	if !ws.ingestGeneration(lite2, gen2) {
		t.Fatal("generation 2 LITE snapshot rejected")
	}
	ws.notePrimaryData(gen2)
	if bid, ask, ok := ws.LiveBidAsk("m"); !ok || bid != 0.31 || ask != 0.35 {
		t.Fatalf("replacement generation did not recover BBO: %.2f/%.2f ok=%v", bid, ask, ok)
	}
	if _, _, _, _, ok := ws.Book3("m"); ok {
		t.Fatal("LITE replacement incorrectly revived old-generation full depth")
	}
}

func TestMarketsWSDisconnectAndMalformedFramesCannotRetainAuthority(t *testing.T) {
	ws := (&Client{}).NewMarketsWS()
	ws.SetOpenSlugs([]string{"m"})
	gen := ws.sessionSeq.Add(1)
	ws.notePrimaryStart(gen)
	if !ws.ingestGeneration([]byte(`{"marketDataLite":{"marketSlug":"m","bestBid":"0.51","bestAsk":"0.53"}}`), gen) {
		t.Fatal("initial LITE snapshot rejected")
	}
	ws.notePrimaryData(gen)
	s := &pusSession{gen: gen}
	s.primary.Store(true)
	ws.noteDisconnect(s, errors.New("test disconnect"))
	if _, _, ok := ws.LiveBidAsk("m"); ok || ws.Count() != 0 || ws.ExecutableCount() != 0 {
		t.Fatal("disconnect left cached generation authoritative")
	}
	receipt := ws.ConnectionReceipt()
	if receipt.ActiveGeneration != 0 || receipt.Disconnects != 1 || receipt.LastCloseError != "test disconnect" {
		t.Fatalf("disconnect receipt=%+v", receipt)
	}

	gen2 := ws.sessionSeq.Add(1)
	ws.notePrimaryStart(gen2)
	ws.notePrimaryTransport(gen2)
	for _, frame := range [][]byte{
		[]byte(`{"heartbeat":{}}`),
		[]byte(`{"marketData":{"bids":[]}}`),
		[]byte(`{"marketData":{not-json}}`),
	} {
		if ws.ingestGeneration(frame, gen2) {
			t.Fatalf("non-usable frame counted as accepted market data: %s", frame)
		}
	}
	if got := ws.ConnectionReceipt(); got.DataSeen || ws.Count() != 0 || ws.ExecutableCount() != 0 {
		t.Fatalf("heartbeat/malformed envelope fabricated readiness: %+v", got)
	}
}
