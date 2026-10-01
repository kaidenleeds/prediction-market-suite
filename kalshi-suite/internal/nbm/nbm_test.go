package nbm

import (
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

const sample = ` KORD    NBM V5.0 NBP GUIDANCE    7/12/2026  0700 UTC
         MON 13| TUE 14
  UTC    00  12| 00  12
  FHR    17  29| 41  53
  TXNMN  86  68| 93  74
  TXNSD   2   3|  3   3
  TXNP1  84  63| 90  70
  TXNP2  85  65| 91  72
  TXNP5  86  68| 93  74
  TXNP7  87  70| 94  77
  TXNP9  88  72| 96  78
 KNYC    NBM V5.0 NBP GUIDANCE    7/12/2026  0700 UTC
  UTC    00  12| 00  12
  FHR    17  29| 41  53
  TXNP5  80  67| 90  71
`

func TestParseBulletinRetainsRequestedProbabilisticStation(t *testing.T) {
	rows, err := parseBulletin(strings.NewReader(sample), "fixture", map[string]bool{"KORD": true})
	if err != nil || len(rows) != 1 || rows[0].Station != "KORD" || rows[0].Run.Hour() != 7 {
		t.Fatalf("rows=%+v err=%v", rows, err)
	}
	if got := rows[0].Values["TXNP9"]; len(got) != 4 || got[3] != 78 {
		t.Fatalf("percentiles=%v", got)
	}
}

func TestFetchLatestFallsBackAndReportsMissing(t *testing.T) {
	requests := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if requests == 1 {
			http.NotFound(w, r)
			return
		}
		_, _ = fmt.Fprint(w, sample)
	}))
	defer ts.Close()
	c := &Client{BaseURL: ts.URL, HTTP: ts.Client()}
	receipt, err := c.FetchLatest(context.Background(), []string{"KORD", "KMIA"},
		time.Date(2026, 7, 12, 13, 0, 0, 0, time.UTC))
	if err != nil || requests != 2 || len(receipt.Forecasts) != 1 || len(receipt.Missing) != 1 || receipt.Missing[0] != "KMIA" {
		t.Fatalf("receipt=%+v requests=%d err=%v", receipt, requests, err)
	}
	if receipt.Attempts != 2 {
		t.Fatalf("attempts=%d want 2", receipt.Attempts)
	}
}

func TestFetchLatestStopsFallbackStormWhenContextExpires(t *testing.T) {
	requests := make(chan struct{}, 20)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- struct{}{}
		<-r.Context().Done()
	}))
	defer ts.Close()
	c := &Client{BaseURL: ts.URL, HTTP: ts.Client()}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err := c.FetchLatest(ctx, []string{"KORD"}, time.Date(2026, 7, 12, 13, 0, 0, 0, time.UTC))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error=%v, want context deadline", err)
	}
	var fetchErr *FetchError
	if !errors.As(err, &fetchErr) || fetchErr.Attempts != 1 {
		t.Fatalf("fetch error=%+v raw=%v", fetchErr, err)
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("cancelled fetch took %v", elapsed)
	}
	if got := len(requests); got != 1 {
		t.Fatalf("requests=%d want 1; cancelled context must not scan older cycles", got)
	}
}

func TestFetchLatestLetsTransportDecodeGzip(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			t.Errorf("default transport did not negotiate gzip: %q", r.Header.Get("Accept-Encoding"))
		}
		w.Header().Set("Content-Encoding", "gzip")
		zw := gzip.NewWriter(w)
		_, _ = zw.Write([]byte(sample))
		_ = zw.Close()
	}))
	defer ts.Close()
	c := &Client{BaseURL: ts.URL, HTTP: ts.Client()}
	receipt, err := c.FetchLatest(context.Background(), []string{"KORD"},
		time.Date(2026, 7, 12, 13, 0, 0, 0, time.UTC))
	if err != nil || len(receipt.Forecasts) != 1 || receipt.Forecasts[0].Station != "KORD" {
		t.Fatalf("gzip receipt=%+v err=%v", receipt, err)
	}
}

func TestLiveNBMProbabilisticBulletin(t *testing.T) {
	if os.Getenv("KALSHI_SUITE_LIVE_NBM_TEST") != "1" {
		t.Skip("set KALSHI_SUITE_LIVE_NBM_TEST=1 for the bounded official-source contract probe")
	}
	receipt, err := NewClient(45*time.Second).FetchLatest(context.Background(), []string{"KORD"}, time.Now())
	if err != nil || len(receipt.Forecasts) != 1 || receipt.Forecasts[0].Run.IsZero() || receipt.BytesRead <= 0 {
		t.Fatalf("live NBM receipt=%+v err=%v", receipt, err)
	}
}
