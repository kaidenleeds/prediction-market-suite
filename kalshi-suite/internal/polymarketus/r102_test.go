// r102_test.go — R102 pins: bug 242 (chunked WS subscribe — no silent 100-slug truncation) and
// bug 243 (trade-print denomination: evidence votes, latch, and post-latch normalization).
package polymarketus

import (
	"fmt"
	"strconv"
	"testing"
	"time"
)

func TestR102SubscribeChunking(t *testing.T) {
	mk := func(n int) []string {
		out := make([]string, n)
		for i := range out {
			out[i] = "slug-" + strconv.Itoa(i)
		}
		return out
	}
	cases := []struct {
		n          int
		wantFrames int
	}{
		{1, 3}, {99, 3}, {100, 3}, {101, 6}, {1200, 30}, {2500, 56}, {9426, 196},
	}
	for _, c := range cases {
		frames := marketSubFrames(mk(c.n))
		if len(frames) != c.wantFrames {
			t.Fatalf("n=%d: got %d frames, want %d (bug 242: every requested slug must subscribe)", c.n, len(frames), c.wantFrames)
		}
	}
	// full coverage + fixed per-chunk request_ids (a resub must re-address the SAME server-side subs)
	frames := marketSubFrames(mk(1200))
	seenMD, seenLite, seenTR := map[string]bool{}, map[string]bool{}, map[string]bool{}
	for _, f := range frames {
		sub := f["subscribe"].(map[string]any)
		id := sub["request_id"].(string)
		slugs := sub["market_slugs"].([]string)
		if len(slugs) == 0 || len(slugs) > marketsSubChunk {
			t.Fatalf("frame %s carries %d slugs (chunk cap %d)", id, len(slugs), marketsSubChunk)
		}
		switch sub["subscription_type"].(int) {
		case 1:
			for _, s := range slugs {
				seenMD[s] = true
			}
			if id != fmt.Sprintf("md-%d", (indexOf(slugs[0]))/marketsSubChunk) {
				t.Fatalf("md frame id %q not the fixed per-chunk id", id)
			}
		case 2:
			for _, s := range slugs {
				seenLite[s] = true
			}
			if id != fmt.Sprintf("mdl-%d", (indexOf(slugs[0]))/marketsSubChunk) {
				t.Fatalf("mdl frame id %q not the fixed per-chunk id", id)
			}
		case 3:
			for _, s := range slugs {
				seenTR[s] = true
			}
		}
	}
	if len(seenMD) != marketsFullBookCap || len(seenLite) != 1200 || len(seenTR) != 1200 {
		t.Fatalf("coverage full=%d lite=%d trade=%d, want %d/1200/1200", len(seenMD), len(seenLite), len(seenTR), marketsFullBookCap)
	}
	// R132: the old defensive total cap silently discarded every slug after #2399. Full-universe
	// LITE and TRADE coverage must remain lossless while full depth stays within its bandwidth cap.
	frames = marketSubFrames(mk(2501))
	seenMD, seenLite, seenTR = map[string]bool{}, map[string]bool{}, map[string]bool{}
	for _, f := range frames {
		sub := f["subscribe"].(map[string]any)
		for _, s := range sub["market_slugs"].([]string) {
			switch sub["subscription_type"].(int) {
			case 1:
				seenMD[s] = true
			case 2:
				seenLite[s] = true
			case 3:
				seenTR[s] = true
			}
		}
	}
	if len(seenMD) != marketsFullBookCap || len(seenLite) != 2501 || len(seenTR) != 2501 {
		t.Fatalf("tiered coverage full=%d lite=%d trade=%d, want %d/2501/2501", len(seenMD), len(seenLite), len(seenTR), marketsFullBookCap)
	}
}

func indexOf(slug string) int { // "slug-123" → 123 (test helper for the fixed-id assertion)
	n, _ := strconv.Atoi(slug[len("slug-"):])
	return n
}

// feedBook plants a fresh full-book frame so denomination votes have a live mid to test against.
func feedBook(ws *MarketsWS, slug string, bid, ask float64) {
	ws.ingest([]byte(fmt.Sprintf(
		`{"marketData":{"marketSlug":"%s","state":"MARKET_STATE_OPEN","bids":[{"px":{"value":"%.2f"},"qty":"10"}],"offers":[{"px":{"value":"%.2f"},"qty":"10"}]}}`,
		slug, bid, ask)))
}

func feedNoTrade(ws *MarketsWS, slug string, px float64) {
	ws.ingest([]byte(fmt.Sprintf(
		`{"trade":{"market_slug":"%s","price":%.4f,"quantity":100,"taker":{"intent":"ORDER_INTENT_BUY_SHORT"}}}`,
		slug, px)))
}

func TestR102DenomOutcomeLatch(t *testing.T) {
	ws := (&Client{}).NewMarketsWS()
	feedBook(ws, "s1", 0.96, 0.98) // live mid 0.97
	// 25 NO prints at 0.03 = outcome-denominated evidence (|0.03−0.97|=0.94 vs |0.97−0.97|=0)
	for i := 0; i < 25; i++ {
		feedNoTrade(ws, "s1", 0.03)
	}
	mode, ov, _ := ws.DenomStats()
	if mode != "outcome" || ov < 25 {
		t.Fatalf("mode=%q outVotes=%d, want outcome latch at ≥25 votes", mode, ov)
	}
	// post-latch: a NO print must feed the YES momentum window as 1−px (0.97), not 0.03
	feedBook(ws, "s1", 0.96, 0.98) // reset mid
	feedNoTrade(ws, "s1", 0.03)
	px, _, ok := ws.LiveYesAt("s1")
	if !ok || px < 0.9 {
		t.Fatalf("post-latch NO print left YES at %.3f — momentum window still poisoned (bug 243)", px)
	}
}

func TestR102DenomYesLatch(t *testing.T) {
	ws := (&Client{}).NewMarketsWS()
	feedBook(ws, "s2", 0.96, 0.98)
	for i := 0; i < 25; i++ {
		feedNoTrade(ws, "s2", 0.97) // YES-denominated evidence: NO print carries the YES px
	}
	mode, _, yv := ws.DenomStats()
	if mode != "yes" || yv < 25 {
		t.Fatalf("mode=%q yesVotes=%d, want yes latch", mode, yv)
	}
	// post-latch: NO-flow notional becomes (1−px)·qty — the dollars the NO buyer actually paid
	_, noBefore, _ := ws.TakerStats("s2", time.Minute)
	feedNoTrade(ws, "s2", 0.97)
	_, noAfter, _ := ws.TakerStats("s2", time.Minute)
	if d := noAfter - noBefore; d < 2.9 || d > 3.1 { // (1−0.97)·100 = $3
		t.Fatalf("post-latch NO notional delta $%.2f, want ~$3 (was px·qty=$97 pre-fix)", d)
	}
}

func TestR102DenomAbstainNearMid(t *testing.T) {
	ws := (&Client{}).NewMarketsWS()
	feedBook(ws, "s3", 0.48, 0.52) // mid 0.50 — px and 1−px are indistinguishable
	for i := 0; i < 30; i++ {
		feedNoTrade(ws, "s3", 0.50)
	}
	if mode, ov, yv := ws.DenomStats(); mode != "unknown" || ov != 0 || yv != 0 {
		t.Fatalf("near-mid prints must abstain: mode=%q ov=%d yv=%d", mode, ov, yv)
	}
}
