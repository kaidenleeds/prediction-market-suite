package kalshi

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestActiveIncentiveProgramsPaginatesActiveOnly(t *testing.T) {
	requests := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.URL.Path != "/incentive_programs" || r.URL.Query().Get("status") != "active" ||
			r.URL.Query().Get("type") != "liquidity" || r.URL.Query().Get("limit") != "10000" {
			t.Fatalf("request=%s?%s", r.URL.Path, r.URL.RawQuery)
		}
		if r.URL.Query().Get("cursor") == "" {
			fmt.Fprint(w, `{"incentive_programs":[{"id":"p1","market_ticker":"KX1","target_size_fp":"10.00"}],"next_cursor":"next 1"}`)
		} else {
			fmt.Fprint(w, `{"incentive_programs":[{"id":"p2","market_ticker":"KX2","target_size_fp":"2.50"}],"next_cursor":""}`)
		}
	}))
	defer ts.Close()
	c := NewClient(ts.URL, nil, 1000, time.Second)
	rows, err := c.ActiveIncentivePrograms(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if requests != 2 || len(rows) != 2 || rows[0].TargetSize() != 10 || rows[1].TargetSize() != 2.5 {
		t.Fatalf("requests=%d rows=%+v", requests, rows)
	}
}

func TestMarketLifecycleResearchHookIsTransitionOnly(t *testing.T) {
	c := NewClient("http://example.invalid", nil, 100, time.Second)
	var got []MarketLifecycleEvent
	c.SetMarketLifecycleHandler(func(e MarketLifecycleEvent) { got = append(got, e) })
	c.ingestLifecycle([]byte(`{"type":"market_lifecycle_v2","msg":{"event_type":"created","market_ticker":"KXNEW","open_ts":1700000000}}`))
	c.ingestLifecycle([]byte(`{"type":"market_lifecycle_v2","msg":{"event_type":"deactivated","market_ticker":"KXPAUSE","ts_ms":1700000000123}}`))
	c.ingestLifecycle([]byte(`{"type":"market_lifecycle_v2","msg":{"event_type":"close_date_updated","market_ticker":"KXREOPEN","close_ts":1700000100}}`))
	c.ingestLifecycle([]byte(`{"type":"market_lifecycle_v2","msg":{"event_type":"activated","market_ticker":"KXREOPEN"}}`))
	if len(got) != 3 {
		t.Fatalf("got %+v", got)
	}
	if got[0].EventType != "deactivated" || got[0].Observed.UnixMilli() != 1700000000123 {
		t.Fatalf("deactivated=%+v", got[0])
	}
	if got[1].CloseTime == "" || got[2].EventType != "activated" {
		t.Fatalf("close/activate=%+v", got)
	}
}

func TestGetEventSnapshotUsesNestedMarketsAndFixedPointDepth(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("with_nested_markets") != "true" {
			t.Fatal("missing nested market query")
		}
		fmt.Fprint(w, `{"event":{"event_ticker":"E1","mutually_exclusive":true,"settlement_sources":[{"name":"Official League","url":"https://league.example"}],"markets":[{"ticker":"M1","status":"active","rules_primary":"If A wins, Yes.","yes_bid_dollars":"0.59","yes_bid_size_fp":"12.50","no_ask_dollars":"0.41"}]}}`)
	}))
	defer ts.Close()
	c := NewClient(ts.URL, nil, 1000, time.Second)
	e, err := c.GetEventSnapshot(context.Background(), "E1")
	if err != nil {
		t.Fatal(err)
	}
	if !e.MutuallyExclusive || len(e.SettlementSources) != 1 || e.SettlementSources[0].Name != "Official League" ||
		len(e.Markets) != 1 || e.Markets[0].RulesPrimary == "" || e.Markets[0].NoAsk.Float() != .41 || e.Markets[0].YesBidSize.Float() != 12.5 {
		t.Fatalf("snapshot=%+v", e)
	}
}

func TestGetQueuePositionsEmptyNeverNeedsCredentials(t *testing.T) {
	c := NewClient("http://example.invalid", nil, 100, time.Second)
	rows, err := c.GetQueuePositions(context.Background(), nil)
	if err != nil || len(rows) != 0 {
		t.Fatalf("rows=%v err=%v", rows, err)
	}
}

func TestGetQueuePositionsReadsOfficialZeroIndexedFixedPoint(t *testing.T) {
	signer, err := NewSigner("queue-test", []byte(testPEM(t)))
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/portfolio/orders/queue_positions" || r.URL.Query().Get("market_tickers") != "KX1,KX2" {
			t.Fatalf("request=%s?%s", r.URL.Path, r.URL.RawQuery)
		}
		for _, h := range []string{"KALSHI-ACCESS-KEY", "KALSHI-ACCESS-SIGNATURE", "KALSHI-ACCESS-TIMESTAMP"} {
			if r.Header.Get(h) == "" {
				t.Fatalf("missing %s", h)
			}
		}
		fmt.Fprint(w, `{"queue_positions":[{"order_id":"first","market_ticker":"KX1","queue_position_fp":"0.00"},{"order_id":"later","market_ticker":"KX2","queue_position_fp":"12.50"}]}`)
	}))
	defer ts.Close()
	c := NewClient(ts.URL, signer, 1000, time.Second)
	rows, err := c.GetQueuePositions(context.Background(), []string{"KX2", "KX1", "KX1"})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0].Position() != 0 || rows[1].Position() != 12.5 {
		t.Fatalf("rows=%+v", rows)
	}
}

func TestGetQueuePositionsRejectsMissingRequiredSchema(t *testing.T) {
	for _, body := range []string{
		`{}`,
		`{"queue_positions":[{"order_id":"o1","market_ticker":"KX1"}]}`,
		`{"queue_positions":[{"order_id":"","market_ticker":"KX1","queue_position_fp":"0.00"}]}`,
		`{"queue_positions":[{"order_id":"o1","market_ticker":"KX1","queue_position_fp":"not-a-number"}]}`,
		`{"queue_positions":[{"order_id":"o1","market_ticker":"KX1","queue_position_fp":"0.00"},{"order_id":"o1","market_ticker":"KX1","queue_position_fp":"1.00"}]}`,
	} {
		t.Run(body, func(t *testing.T) {
			signer, err := NewSigner("queue-test", []byte(testPEM(t)))
			if err != nil {
				t.Fatal(err)
			}
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				fmt.Fprint(w, body)
			}))
			defer ts.Close()
			c := NewClient(ts.URL, signer, 1000, time.Second)
			rows, err := c.GetQueuePositions(context.Background(), []string{"KX1"})
			if err == nil || rows != nil {
				t.Fatalf("schema must fail closed: rows=%+v err=%v", rows, err)
			}
		})
	}
}
