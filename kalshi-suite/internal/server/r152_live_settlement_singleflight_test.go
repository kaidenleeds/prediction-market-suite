package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
)

func TestR152PriorityAndScheduledLiveSettlementSweepsNeverOverlap(t *testing.T) {
	s := testServer(t)
	settled := time.Now().UTC().Add(-time.Minute).Format(time.RFC3339Nano)
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	var calls, active, maxActive atomic.Int32
	venue := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/portfolio/settlements" {
			http.NotFound(w, r)
			return
		}
		calls.Add(1)
		nowActive := active.Add(1)
		defer active.Add(-1)
		for {
			prior := maxActive.Load()
			if nowActive <= prior || maxActive.CompareAndSwap(prior, nowActive) {
				break
			}
		}
		entered <- struct{}{}
		<-release
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"settlements":[{"ticker":"KX-R152-FINAL","market_result":"yes",` +
			`"yes_count_fp":"1.00","no_count_fp":"0.00","yes_total_cost_dollars":"0.40",` +
			`"no_total_cost_dollars":"0.00","revenue":100,"fee_cost":"0.01","settled_time":"` +
			settled + `"}],"cursor":""}`))
	}))
	defer venue.Close()
	s.kal = kalshi.NewClient(venue.URL, queueTestSigner(t), 10_000, 2*time.Second)

	// Establish the prior authenticated open count, then start the normal settlement pass.
	s.observeKalshiLiveOpenCount(2)
	s.liveSettleAt = time.Time{}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		s.sweepLiveSettlements(context.Background())
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("scheduled settlement sweep did not reach the venue")
	}

	// While the first request is in flight, the UI observes 2 -> 1 and wakes the priority pass.
	// Without the single-flight boundary this issues a concurrent second GET and both workers read
	// the same old KV watermark before appending the same journal row.
	if !s.observeKalshiLiveOpenCount(1) {
		t.Fatal("open-count shrink did not request priority settlement work")
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		s.sweepLiveSettlements(context.Background())
	}()
	time.Sleep(75 * time.Millisecond)
	if got := maxActive.Load(); got != 1 {
		t.Fatalf("scheduled and priority settlement requests overlapped: max active=%d", got)
	}
	close(release)
	wg.Wait()
	if got := calls.Load(); got != 2 {
		t.Fatalf("priority retry calls=%d, want two serialized reads", got)
	}

	b, err := os.ReadFile(filepath.Join(s.cfg().DataDir, "settlement_journal.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	lines := 0
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		if strings.TrimSpace(line) != "" {
			lines++
		}
	}
	if lines != 1 {
		t.Fatalf("one settlement was journaled %d times", lines)
	}
}
