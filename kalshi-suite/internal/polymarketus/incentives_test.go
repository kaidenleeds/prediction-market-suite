package polymarketus

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestActiveIncentiveProgramsPaginatesAndDeduplicates(t *testing.T) {
	requests := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.URL.Path != "/v1/incentives" || r.URL.Query().Get("statuses") != "active" || r.URL.Query().Get("pageSize") != "100" {
			t.Fatalf("unexpected request %s", r.URL.String())
		}
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("pageToken") == "" {
			_, _ = w.Write([]byte(`{"programs":[{"marketSlug":"MKT-B","timePeriods":[{"programId":"p2","start":"2026-07-01T00:00:00Z","rewardPool":2}]},{"marketSlug":"MKT-A","timePeriods":[{"programId":"p1","start":"2026-07-01T00:00:00Z","rewardPool":1}]}],"nextPageToken":"next"}`))
			return
		}
		_, _ = w.Write([]byte(`{"programs":[{"marketSlug":"mkt-a","timePeriods":[{"programId":"p1","start":"2026-07-01T00:00:00Z","rewardPool":1},{"programId":"p3","start":"2026-07-02T00:00:00Z","rewardPool":3}]}]}`))
	}))
	defer ts.Close()
	p := NewPublicClient(time.Second)
	p.baseURL, p.http = ts.URL, ts.Client()
	rows, err := p.ActiveIncentivePrograms(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if requests != 2 || len(rows) != 2 || rows[0].MarketSlug != "mkt-a" || len(rows[0].TimePeriods) != 2 {
		t.Fatalf("requests=%d rows=%+v", requests, rows)
	}
}

func TestActiveIncentiveProgramsForSymbolsBatchesCompleteExecutableUniverse(t *testing.T) {
	requests := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		symbols := r.URL.Query()["symbols"]
		if r.URL.Query().Get("statuses") != "active" || r.URL.Query().Get("pageSize") != "100" ||
			len(symbols) == 0 || len(symbols) > incentiveSymbolBatchSize || r.URL.Query().Get("pageToken") != "" {
			t.Fatalf("unexpected scoped request %s", r.URL.String())
		}
		rows := make([]IncentiveMarket, 0, len(symbols))
		for _, symbol := range symbols {
			rows = append(rows, IncentiveMarket{MarketSlug: strings.ToUpper(symbol),
				TimePeriods: []IncentivePeriod{{ProgramID: "program-" + symbol, Status: "active"}}})
		}
		_ = json.NewEncoder(w).Encode(IncentiveProgramsPage{Programs: rows})
	}))
	defer ts.Close()
	p := NewPublicClient(2 * time.Second)
	p.baseURL, p.http = ts.URL, ts.Client()
	input := make([]string, 0, 120)
	for i := 0; i < 120; i++ {
		input = append(input, fmt.Sprintf("market-%03d", i))
	}
	input = append(input, "MARKET-000") // duplicate/case cannot inflate the requested universe.
	rows, stats, err := p.ActiveIncentiveProgramsForSymbols(context.Background(), input)
	if err != nil || requests != 3 || len(rows) != 120 || !stats.Complete ||
		stats.RequestedSymbols != 120 || stats.Batches != 3 || stats.Pages != 3 || stats.Programs != 120 {
		t.Fatalf("requests=%d rows=%d stats=%+v err=%v", requests, len(rows), stats, err)
	}
}

func TestActiveIncentiveProgramsForSymbolsRejectsScopeLeakWithoutPartialRows(t *testing.T) {
	requests := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		symbols := r.URL.Query()["symbols"]
		slug := symbols[0]
		if requests == 2 {
			slug = "not-requested"
		}
		_ = json.NewEncoder(w).Encode(IncentiveProgramsPage{Programs: []IncentiveMarket{{MarketSlug: slug,
			TimePeriods: []IncentivePeriod{{ProgramID: "program", Status: "active"}}}}})
	}))
	defer ts.Close()
	p := NewPublicClient(2 * time.Second)
	p.baseURL, p.http = ts.URL, ts.Client()
	input := make([]string, 0, 51)
	for i := 0; i < 51; i++ {
		input = append(input, fmt.Sprintf("market-%03d", i))
	}
	rows, stats, err := p.ActiveIncentiveProgramsForSymbols(context.Background(), input)
	if err == nil || !strings.Contains(err.Error(), "unrequested market") || rows != nil ||
		requests != 2 || stats.Complete || stats.Batches != 1 {
		t.Fatalf("requests=%d rows=%v stats=%+v err=%v", requests, rows, stats, err)
	}
}

func TestActiveIncentiveProgramsForSymbolsRetriesTransientPage(t *testing.T) {
	requests := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if requests == 1 {
			http.Error(w, "try again", http.StatusServiceUnavailable)
			return
		}
		_ = json.NewEncoder(w).Encode(IncentiveProgramsPage{Programs: []IncentiveMarket{{MarketSlug: "market",
			TimePeriods: []IncentivePeriod{{ProgramID: "program", Status: "active"}}}}})
	}))
	defer ts.Close()
	p := NewPublicClient(2 * time.Second)
	p.baseURL, p.http, p.pageRetryBase = ts.URL, ts.Client(), time.Millisecond
	rows, stats, err := p.ActiveIncentiveProgramsForSymbols(context.Background(), []string{"market"})
	if err != nil || requests != 2 || len(rows) != 1 || !stats.Complete {
		t.Fatalf("requests=%d rows=%v stats=%+v err=%v", requests, rows, stats, err)
	}
}

func TestActiveIncentiveProgramsSupportsCurrentFullBoardPagination(t *testing.T) {
	const pages = 25
	requests := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		page := 0
		if token := r.URL.Query().Get("pageToken"); token != "" {
			if _, err := fmt.Sscanf(token, "page-%d", &page); err != nil {
				t.Fatalf("bad page token %q", token)
			}
		}
		next := ""
		if page+1 < pages {
			next = fmt.Sprintf("page-%d", page+1)
		}
		_, _ = fmt.Fprintf(w, `{"programs":[{"marketSlug":"mkt-%d","timePeriods":[{"programId":"p-%d","rewardPool":1}]}],"nextPageToken":%q}`, page, page, next)
	}))
	defer ts.Close()
	p := NewPublicClient(2 * time.Second)
	p.baseURL, p.http = ts.URL, ts.Client()
	rows, err := p.ActiveIncentivePrograms(context.Background())
	if err != nil || requests != pages || len(rows) != pages {
		t.Fatalf("requests=%d rows=%d err=%v", requests, len(rows), err)
	}
}

func TestIncentivePeriodActiveAtUsesStatusAndBounds(t *testing.T) {
	now := time.Date(2026, 7, 12, 0, 0, 0, 0, time.UTC)
	if !(IncentivePeriod{Start: "2026-07-10T00:00:00Z", End: "2026-07-13T00:00:00Z"}).ActiveAt(now) {
		t.Fatal("current period rejected")
	}
	for _, p := range []IncentivePeriod{{Status: "closed"}, {Start: "2026-07-13T00:00:00Z"}, {End: "2026-07-12T00:00:00Z"}} {
		if p.ActiveAt(now) {
			t.Fatalf("inactive period accepted: %+v", p)
		}
	}
}

func TestIncentiveEarningsUsesAuthenticatedExactDateQuery(t *testing.T) {
	var c *Client
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r147VerifySignedRequest(t, c, r)
		if r.URL.Path != "/v1/incentives/earnings" || r.URL.Query().Get("startDate") != "2026-07-01" ||
			r.URL.Query().Get("endDate") != "2026-07-12" || r.Header.Get("X-PM-Signature") == "" {
			t.Fatalf("bad authenticated request: %s headers=%v", r.URL.String(), r.Header)
		}
		_, _ = w.Write([]byte(`{"rewards":[{"reward":12.5,"programType":"liquidityProgram","marketSlug":"MKT","date":"2026-07-11"}]}`))
	}))
	defer ts.Close()
	c = &Client{baseURL: ts.URL, keyID: "test", priv: ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize)), http: ts.Client()}
	rows, err := c.IncentiveEarnings(context.Background(), time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 7, 12, 0, 0, 0, 0, time.UTC))
	if err != nil || len(rows) != 1 || rows[0].Reward != 12.5 || rows[0].MarketSlug != "mkt" {
		t.Fatalf("rows=%+v err=%v", rows, err)
	}
}
