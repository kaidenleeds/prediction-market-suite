package server

import (
	"context"
	"testing"
	"time"
)

func TestR133PolyUSFullUniverseCrawlBudget(t *testing.T) {
	if polyUSFullUniverseCrawlTimeout != 8*time.Minute {
		t.Fatalf("full-universe crawl budget=%v, want 8m for the roughly 135-page sequential board", polyUSFullUniverseCrawlTimeout)
	}
	started := time.Now()
	ctx, cancel := polyUSFullUniverseCrawlContext(context.Background())
	defer cancel()
	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("full-universe crawl context must remain bounded")
	}
	got := deadline.Sub(started)
	if got < polyUSFullUniverseCrawlTimeout-100*time.Millisecond || got > polyUSFullUniverseCrawlTimeout+100*time.Millisecond {
		t.Fatalf("crawl context budget=%v, want approximately %v", got, polyUSFullUniverseCrawlTimeout)
	}
}
