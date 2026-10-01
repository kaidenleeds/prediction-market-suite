package deribit

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"
)

func TestOptionSummaryPreservesServerClockAndSortsOpenInterest(t *testing.T) {
	now := time.Now().UTC()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("currency") != "BTC" || r.URL.Query().Get("kind") != "option" {
			t.Fatalf("query=%s", r.URL.RawQuery)
		}
		_, _ = fmt.Fprintf(w, `{"jsonrpc":"2.0","usOut":%d,"result":[
{"instrument_name":"BTC-A","underlying_price":100,"mark_iv":50,"creation_timestamp":%d,"open_interest":1},
{"instrument_name":"BTC-B","underlying_price":100,"mark_iv":50,"creation_timestamp":%d,"open_interest":9}]}`,
			now.UnixMicro(), now.UnixMilli(), now.UnixMilli())
	}))
	defer ts.Close()
	c := NewClient(time.Second)
	c.BaseURL, c.HTTP = ts.URL, ts.Client()
	got, err := c.OptionSummary(context.Background(), "btc")
	if err != nil || len(got.Rows) != 2 || got.Rows[0].InstrumentName != "BTC-B" || got.ServerTime.IsZero() {
		t.Fatalf("snapshot=%+v err=%v", got, err)
	}
}

func TestOptionInstrumentsAndThresholdSurfaceUseExactMetadata(t *testing.T) {
	now := time.Now().UTC()
	expiry := now.Add(30 * 24 * time.Hour).Truncate(time.Millisecond)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/public/get_book_summary_by_currency":
			_, _ = fmt.Fprintf(w, `{"jsonrpc":"2.0","usOut":%d,"result":[
{"instrument_name":"BTC-EXACT-C","underlying_price":100,"mark_iv":50,"creation_timestamp":%d,"open_interest":9},
{"instrument_name":"BTC-EXACT-P","underlying_price":100,"mark_iv":60,"creation_timestamp":%d,"open_interest":1},
{"instrument_name":"BTC-NO-METADATA","underlying_price":100,"mark_iv":1,"creation_timestamp":%d,"open_interest":999}]}`,
				now.UnixMicro(), now.UnixMilli(), now.UnixMilli(), now.UnixMilli())
		case "/public/get_instruments":
			if r.URL.Query().Get("expired") != "false" {
				t.Fatalf("missing expired=false contract: %s", r.URL.RawQuery)
			}
			_, _ = fmt.Fprintf(w, `{"jsonrpc":"2.0","usOut":%d,"result":[
{"instrument_name":"BTC-EXACT-C","kind":"option","base_currency":"BTC","quote_currency":"USD","price_index":"btc_usd","option_type":"call","strike":110,"expiration_timestamp":%d,"creation_timestamp":%d,"is_active":true},
{"instrument_name":"BTC-EXACT-P","kind":"option","base_currency":"BTC","quote_currency":"USD","price_index":"btc_usd","option_type":"put","strike":110,"expiration_timestamp":%d,"creation_timestamp":%d,"is_active":true},
{"instrument_name":"BTC-INACTIVE","kind":"option","option_type":"call","strike":1,"expiration_timestamp":%d,"is_active":false}]}`,
				now.UnixMicro(), expiry.UnixMilli(), now.UnixMilli(), expiry.UnixMilli(), now.UnixMilli(), expiry.UnixMilli())
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()
	c := NewClient(time.Second)
	c.BaseURL, c.HTTP = ts.URL, ts.Client()
	summary, err := c.OptionSummary(context.Background(), "BTC")
	if err != nil {
		t.Fatal(err)
	}
	instruments, err := c.OptionInstruments(context.Background(), "BTC")
	if err != nil {
		t.Fatal(err)
	}
	if len(instruments.Rows) != 2 {
		t.Fatalf("instrument rows=%d, want two active exact contracts", len(instruments.Rows))
	}
	points, err := ThresholdSurface(summary, instruments)
	if err != nil {
		t.Fatal(err)
	}
	if len(points) != 1 {
		t.Fatalf("points=%+v, want one expiry/strike surface point", points)
	}
	p := points[0]
	if p.Strike != 110 || p.ProbabilityAbove <= 0 || p.ProbabilityAbove >= 1 ||
		p.ProbabilityLow > p.ProbabilityAbove || p.ProbabilityHigh < p.ProbabilityAbove ||
		len(p.Contributors) != 2 || p.OpenInterest != 10 || p.PriceIndex != "btc_usd" {
		t.Fatalf("threshold point=%+v", p)
	}
}

func TestThresholdSurfaceRejectsCrossClockJoin(t *testing.T) {
	_, err := ThresholdSurface(Snapshot{Currency: "BTC", ServerTime: time.Now()},
		InstrumentSnapshot{Currency: "BTC", ServerTime: time.Now().Add(time.Minute)})
	if err == nil {
		t.Fatal("cross-clock option metadata join was accepted")
	}
}

func TestLiveDeribitOptionSummary(t *testing.T) {
	if os.Getenv("KALSHI_SUITE_LIVE_DERIBIT_TEST") != "1" {
		t.Skip("set KALSHI_SUITE_LIVE_DERIBIT_TEST=1 for the bounded official-source contract probe")
	}
	client := NewClient(20 * time.Second)
	got, err := client.OptionSummary(context.Background(), "BTC")
	if err != nil || len(got.Rows) == 0 || got.ServerTime.IsZero() {
		t.Fatalf("live snapshot rows=%d time=%s err=%v", len(got.Rows), got.ServerTime, err)
	}
	meta, err := client.OptionInstruments(context.Background(), "BTC")
	if err != nil || len(meta.Rows) == 0 || meta.ServerTime.IsZero() {
		t.Fatalf("live metadata rows=%d time=%s err=%v", len(meta.Rows), meta.ServerTime, err)
	}
	points, err := ThresholdSurface(got, meta)
	if err != nil || len(points) == 0 {
		t.Fatalf("live threshold surface rows=%d err=%v", len(points), err)
	}
}
