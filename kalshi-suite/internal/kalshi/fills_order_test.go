package kalshi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestR140GetFillsForOrderHasNoTimeCutoffAndKeepsOrderFilterAcrossPages(t *testing.T) {
	hits := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		if r.URL.Path != "/portfolio/fills" || r.URL.Query().Get("order_id") != "old-order" ||
			r.URL.Query().Get("min_ts") != "" {
			t.Fatalf("order-scoped fill query=%s", r.URL.String())
		}
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("cursor") == "" {
			_, _ = w.Write([]byte(`{"fills":[{"fill_id":"f1","order_id":"old-order","ticker":"MVE"}],"cursor":"next"}`))
			return
		}
		_, _ = w.Write([]byte(`{"fills":[{"fill_id":"f2","order_id":"old-order","ticker":"MVE"}],"cursor":""}`))
	}))
	defer ts.Close()
	signer, err := NewSigner("fills-order", []byte(testPEM(t)))
	if err != nil {
		t.Fatal(err)
	}
	c := NewClient(ts.URL, signer, 1000, time.Second)
	rows, err := c.GetFillsForOrder(context.Background(), "old-order")
	if err != nil || len(rows) != 2 || hits != 2 || rows[1].OrderID != "old-order" {
		t.Fatalf("order fills rows=%+v hits=%d err=%v", rows, hits, err)
	}
}
