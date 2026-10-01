package polymarketus

import (
	"testing"
	"time"
)

func TestSetOpenSlugsPrunesTerminalLiveCache(t *testing.T) {
	ws := &MarketsWS{live: map[string]*pusLive{
		"keep": {yes: .4, at: time.Now()},
		"gone": {yes: .6, at: time.Now()},
	}}
	ws.SetOpenSlugs([]string{"KEEP"})
	if _, ok := ws.live["gone"]; ok {
		t.Fatal("terminal slug remained in the live cache")
	}
	if _, ok := ws.live["keep"]; !ok {
		t.Fatal("open slug was pruned")
	}
	if live, pruned := ws.CacheStats(); live != 1 || pruned != 1 {
		t.Fatalf("cache receipt live=%d pruned=%d", live, pruned)
	}
	ws.SetOpenSlugs(nil)
	if live, pruned := ws.CacheStats(); live != 0 || pruned != 2 {
		t.Fatalf("authoritative empty universe did not drain cache: live=%d pruned=%d", live, pruned)
	}
}
