package polymarketus

import (
	"fmt"
	"strconv"
	"testing"
)

func TestR133MarketsFullBookCapClamp(t *testing.T) {
	for _, tc := range []struct{ in, want int }{
		{0, 600}, {-5, 600}, {1, 50}, {50, 50}, {775, 775}, {1000, 1000}, {5000, 1000},
	} {
		if got := ClampMarketsFullBookCap(tc.in); got != tc.want {
			t.Fatalf("ClampMarketsFullBookCap(%d)=%d, want %d", tc.in, got, tc.want)
		}
	}
	if got := (&Client{}).NewMarketsWSWithFullBookCap(5000).FullBookCap(); got != 1000 {
		t.Fatalf("socket cap=%d, want clamped 1000", got)
	}
	if got := (&Client{}).NewMarketsWS().FullBookCap(); got != 600 {
		t.Fatalf("default socket cap=%d, want 600", got)
	}
}

func TestR133MarketSubFramesRespectConfiguredCapWithoutTruncatingLiteTrade(t *testing.T) {
	slugs := make([]string, 1237)
	for i := range slugs {
		slugs[i] = "slug-" + strconv.Itoa(i)
	}
	frames := marketSubFramesForCap(slugs, 1000)
	// 10 full-depth chunks + 13 LITE chunks + 13 TRADE chunks.
	if len(frames) != 36 {
		t.Fatalf("frames=%d, want 36 for full=1000 and wide=1237", len(frames))
	}
	seen := map[int]map[string]bool{1: {}, 2: {}, 3: {}}
	for _, frame := range frames {
		sub := frame["subscribe"].(map[string]any)
		typ := sub["subscription_type"].(int)
		chunk := sub["market_slugs"].([]string)
		if len(chunk) == 0 || len(chunk) > marketsSubChunk {
			t.Fatalf("%s has invalid chunk length %d", sub["request_id"], len(chunk))
		}
		for _, slug := range chunk {
			if seen[typ][slug] {
				t.Fatalf("subscription type %d duplicated %q", typ, slug)
			}
			seen[typ][slug] = true
		}
	}
	if got := len(seen[1]); got != 1000 {
		t.Fatalf("full-depth slugs=%d, want configured 1000", got)
	}
	if got := len(seen[2]); got != len(slugs) {
		t.Fatalf("LITE slugs=%d, want complete %d", got, len(slugs))
	}
	if got := len(seen[3]); got != len(slugs) {
		t.Fatalf("TRADE slugs=%d, want complete %d", got, len(slugs))
	}
	if frames[9]["subscribe"].(map[string]any)["request_id"] != fmt.Sprintf("md-%d", 9) {
		t.Fatal("configured tenth full-depth frame lost stable request id md-9")
	}
}
