package server

// R75 tests — /api/markets server-side pagination + search over the slim in-memory row snapshot,
// and the cold-boot "building" response (the old path 502'd or hung until the browser aborted).
// No network on the warm paths: the snapshot cache is pre-seeded so handleMarkets never touches
// the Kalshi client; the cold test uses a dead-endpoint client that fails instantly.

import (
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
)

func seedMarketRows(n int) []marketRow {
	rows := make([]marketRow, 0, n)
	for i := 0; i < n; i++ {
		tk := fmt.Sprintf("KXTEST-26JUL05-T%03d", i)
		title := fmt.Sprintf("Test market %03d", i)
		rows = append(rows, marketRow{Ticker: tk, Title: title, YesBid: 0.40, YesAsk: 0.42,
			Volume24h: float64(n - i), searchKey: strings.ToLower(title + " " + tk)})
	}
	return rows
}

type mktPage struct {
	Building bool             `json:"building"`
	Count    int              `json:"count"`
	Total    int              `json:"total"`
	Universe int              `json:"universe"`
	Offset   int              `json:"offset"`
	Limit    int              `json:"limit"`
	Markets  []map[string]any `json:"markets"`
}

func getMarkets(t *testing.T, s *Server, url string) mktPage {
	t.Helper()
	rec := httptest.NewRecorder()
	s.handleMarkets(rec, httptest.NewRequest("GET", url, nil))
	var d mktPage
	if err := json.Unmarshal(rec.Body.Bytes(), &d); err != nil {
		t.Fatalf("bad JSON from %s: %v", url, err)
	}
	return d
}

func TestHandleMarketsPaginatesAndStaysSlim(t *testing.T) {
	s := &Server{mktRows: seedMarketRows(750), mktUniverse: 750, mktAt: time.Now()}

	d := getMarkets(t, s, "/api/markets")
	if d.Count != 300 || d.Total != 750 || d.Universe != 750 || d.Offset != 0 || d.Limit != 300 || len(d.Markets) != 300 {
		t.Fatalf("default page: count=%d total=%d universe=%d offset=%d limit=%d rows=%d",
			d.Count, d.Total, d.Universe, d.Offset, d.Limit, len(d.Markets))
	}
	if d.Markets[0]["ticker"] != "KXTEST-26JUL05-T000" {
		t.Fatalf("page 1 must start at row 0, got %v", d.Markets[0]["ticker"])
	}
	// SLIM shape: fields the tab never rendered must not serialize (they were ~15%% of the payload).
	for _, k := range []string{"no_ask", "spread_cents", "searchKey", "spread"} {
		if _, has := d.Markets[0][k]; has {
			t.Fatalf("slim row leaked field %q", k)
		}
	}

	// Last partial page: offset past the tail clamps, count carries the remainder.
	d = getMarkets(t, s, "/api/markets?offset=700&limit=300")
	if d.Count != 50 || len(d.Markets) != 50 || d.Markets[0]["ticker"] != "KXTEST-26JUL05-T700" {
		t.Fatalf("tail page: count=%d rows=%d first=%v", d.Count, len(d.Markets), d.Markets[0]["ticker"])
	}

	// Limit clamps: 0 -> default 300; 99999 -> 1000.
	if d = getMarkets(t, s, "/api/markets?limit=0"); d.Limit != 300 {
		t.Fatalf("limit=0 should default to 300, got %d", d.Limit)
	}
	if d = getMarkets(t, s, "/api/markets?limit=99999"); d.Limit != 1000 {
		t.Fatalf("limit=99999 should clamp to 1000, got %d", d.Limit)
	}
}

func TestHandleMarketsServerSideSearch(t *testing.T) {
	s := &Server{mktRows: seedMarketRows(750), mktUniverse: 750, mktAt: time.Now()}

	// Ticker fragment (case-insensitive) — exactly one row carries T042.
	d := getMarkets(t, s, "/api/markets?q=t042")
	if d.Total != 1 || d.Count != 1 || d.Markets[0]["ticker"] != "KXTEST-26JUL05-T042" {
		t.Fatalf("q=t042: total=%d count=%d first=%v", d.Total, d.Count, d.Markets)
	}
	if d.Universe != 750 {
		t.Fatalf("universe must report the FULL set under a search, got %d", d.Universe)
	}
	// Title words match too; a miss is empty but well-formed.
	if d = getMarkets(t, s, "/api/markets?q=test+market+7"); d.Total == 0 {
		t.Fatalf("title search returned nothing")
	}
	if d = getMarkets(t, s, "/api/markets?q=zzzznope"); d.Total != 0 || d.Count != 0 {
		t.Fatalf("miss should be empty, got total=%d", d.Total)
	}
}

func TestHandleMarketsBuildingWhenCold(t *testing.T) {
	// Dead endpoint + instant timeout: the background pull fails fast, no cache ever lands, and
	// the handler must answer 200 {"building":true} instead of hanging (the R73 abort) or 502ing.
	kc := kalshi.NewClient("http://127.0.0.1:1", nil, 1000, 50*time.Millisecond)
	s := &Server{kal: kc}
	done := make(chan mktPage, 1)
	go func() { done <- getMarkets(t, s, "/api/markets") }()
	select {
	case d := <-done:
		if !d.Building {
			t.Fatalf("cold handler must report building, got %+v", d)
		}
		if d.Markets == nil || len(d.Markets) != 0 {
			t.Fatalf("building response should carry an empty markets array, got %v", d.Markets)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("cold /api/markets hung — the exact failure mode R75 removes")
	}
}
