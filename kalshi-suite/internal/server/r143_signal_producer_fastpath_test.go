package server

import (
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
)

func TestR143WhaleProducerModeIsExplicit(t *testing.T) {
	for _, tc := range []struct {
		url  string
		want bool
	}{
		{"/api/whales", false},
		{"/api/whales?producer=0", false},
		{"/api/whales?producer=1", true},
		{"/api/whales?producer=TRUE", true},
	} {
		req := httptest.NewRequest("GET", tc.url, nil)
		if got := whaleProducerMode(req); got != tc.want {
			t.Fatalf("whaleProducerMode(%q)=%v want %v", tc.url, got, tc.want)
		}
	}
}

func TestR143ColdProducerCompletesWithoutAnyVenueRequest(t *testing.T) {
	requests := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests++
		http.Error(w, "producer must not call venue REST", http.StatusTeapot)
	}))
	defer upstream.Close()

	s := testServer(t)
	s.kal = kalshi.NewClient(upstream.URL, nil, 1, 50*time.Millisecond)
	req := httptest.NewRequest(http.MethodGet, "/api/whales?producer=1", nil)
	rec := httptest.NewRecorder()
	started := time.Now()
	s.handleWhales(rec, req)

	if requests != 0 {
		t.Fatalf("cached-only producer made %d venue REST requests", requests)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("cold cached-only producer took %s", elapsed)
	}
	receipt, ok := s.signalProducerSnapshot()["kalshi"]
	if !ok || receipt.Completed.IsZero() || receipt.Errors != 0 {
		t.Fatalf("producer did not publish a healthy completed receipt: %+v", receipt)
	}
}

func TestR143KalshiProducerMetadataUsesResidentCaches(t *testing.T) {
	s := testServer(t)
	ticker := "KX-R143-FAST"
	s.metaMu.Lock()
	if s.kmkts == nil {
		s.kmkts = map[string]kalshi.Market{}
	}
	s.kmkts[ticker] = kalshi.Market{
		Ticker:             ticker,
		YesBid:             0.41,
		YesAsk:             0.43,
		ExpectedExpiration: time.Now().Add(3 * time.Hour).UTC().Format(time.RFC3339Nano),
	}
	s.metaMu.Unlock()
	s.kobImbMu.Lock()
	if s.kobImb == nil {
		s.kobImb = map[string]kobImbEntry{}
	}
	s.kobImb[ticker] = kobImbEntry{imb: 0.25, depth: 37, at: time.Now()}
	s.kobImbMu.Unlock()

	spread, hours, depth := s.kalSigMetaCached(ticker)
	if math.Abs(spread-2) > 0.001 {
		t.Fatalf("cached spread=%v want 2 cents", spread)
	}
	if hours < 2.9 || hours > 3.1 {
		t.Fatalf("cached horizon=%v want about 3 hours", hours)
	}
	if depth != 37 {
		t.Fatalf("cached depth=%v want 37", depth)
	}
	if imb, ok := s.kalImbalanceCached(ticker); !ok || math.Abs(imb-0.25) > 0.001 {
		t.Fatalf("cached imbalance=(%v,%v) want (0.25,true)", imb, ok)
	}
}
