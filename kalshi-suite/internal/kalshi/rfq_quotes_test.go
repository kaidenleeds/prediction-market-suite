package kalshi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func newRFQQuotesTestClient(t *testing.T, baseURL string) *Client {
	t.Helper()
	signer, err := NewSigner("rfq-quotes-test", []byte(testPEM(t)))
	if err != nil {
		t.Fatalf("build test signer: %v", err)
	}
	return NewClient(baseURL, signer, 10000, 2*time.Second)
}

func TestGetQuotesUsesRFQCreatorIdentityPaginatesAndDeduplicates(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hit := hits.Add(1)
		if r.Method != http.MethodGet || r.URL.Path != "/communications/quotes" {
			t.Fatalf("request=%s %s", r.Method, r.URL.Path)
		}
		q := r.URL.Query()
		if q.Get("rfq_id") != "rfq/one" || q.Get("limit") != "500" ||
			q.Get("rfq_user_filter") != "self" || q.Has("user_filter") {
			t.Fatalf("requester quote filters=%q", r.URL.RawQuery)
		}
		q2 := map[string]any{"id": "q-2", "rfq_id": "rfq/one", "market_ticker": "MVE"}
		switch hit {
		case 1:
			if q.Get("cursor") != "" {
				t.Fatalf("first cursor=%q", q.Get("cursor"))
			}
			writeTestJSON(t, w, map[string]any{
				"quotes": []map[string]any{
					{"id": "q-1", "rfq_id": "rfq/one", "market_ticker": "MVE"},
					q2,
				},
				"cursor": "next/page",
			})
		case 2:
			if q.Get("cursor") != "next/page" {
				t.Fatalf("second cursor=%q", q.Get("cursor"))
			}
			writeTestJSON(t, w, map[string]any{
				"quotes": []map[string]any{
					q2, // exact page-boundary duplicate must not inflate quote counts
					{"id": "q-3", "rfq_id": "rfq/one", "market_ticker": "MVE"},
				},
				"cursor": "",
			})
		default:
			t.Fatalf("unexpected page %d", hit)
		}
	}))
	defer srv.Close()

	rows, err := newRFQQuotesTestClient(t, srv.URL).GetQuotes(context.Background(), "rfq/one")
	if err != nil {
		t.Fatalf("GetQuotes: %v", err)
	}
	if hits.Load() != 2 || len(rows) != 3 || rows[0].ID != "q-1" || rows[1].ID != "q-2" || rows[2].ID != "q-3" {
		t.Fatalf("hits=%d quotes=%+v", hits.Load(), rows)
	}
	for _, row := range rows {
		if row.Status != "open" {
			t.Fatalf("official timestamp-only open quote %q normalized to %q", row.ID, row.Status)
		}
	}
}

func TestQuoteLifecycleIsDerivedFromCurrentTimestampSchema(t *testing.T) {
	tests := []struct {
		name string
		in   Quote
		want string
	}{
		{name: "open", in: Quote{}, want: "open"},
		{name: "accepted", in: Quote{AcceptedTS: "2026-07-16T10:00:00Z"}, want: "accepted"},
		{name: "confirmed", in: Quote{AcceptedTS: "2026-07-16T10:00:00Z", ConfirmedTS: "2026-07-16T10:00:01Z"}, want: "confirmed"},
		{name: "executed", in: Quote{ConfirmedTS: "2026-07-16T10:00:01Z", ExecutedTS: "2026-07-16T10:00:02Z"}, want: "executed"},
		{name: "cancelled wins", in: Quote{ExecutedTS: "2026-07-16T10:00:02Z", CancelledTS: "2026-07-16T10:00:03Z"}, want: "cancelled"},
		{name: "legacy status", in: Quote{Status: "OPEN"}, want: "open"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := normalizeQuoteStatus(tt.in).Status; got != tt.want {
				t.Fatalf("status=%q want=%q", got, tt.want)
			}
		})
	}
}

func TestGetOwnQuotesKeepsMakerIdentitySeparate(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if q.Get("rfq_id") != "rfq-2" || q.Get("user_filter") != "self" || q.Has("rfq_user_filter") {
			t.Fatalf("maker quote filters=%q", r.URL.RawQuery)
		}
		writeTestJSON(t, w, map[string]any{"quotes": []map[string]any{}, "cursor": ""})
	}))
	defer srv.Close()

	rows, err := newRFQQuotesTestClient(t, srv.URL).GetOwnQuotes(context.Background(), "rfq-2")
	if err != nil || len(rows) != 0 {
		t.Fatalf("GetOwnQuotes rows=%+v err=%v", rows, err)
	}
}

func TestGetQuotesRejectsRepeatedCursorAndConflictingDuplicate(t *testing.T) {
	t.Run("repeated cursor", func(t *testing.T) {
		var hits atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hit := hits.Add(1)
			writeTestJSON(t, w, map[string]any{
				"quotes": []map[string]any{{"id": "q-" + string(rune('0'+hit)), "rfq_id": "rfq-3"}},
				"cursor": "stuck",
			})
		}))
		defer srv.Close()
		rows, err := newRFQQuotesTestClient(t, srv.URL).GetQuotes(context.Background(), "rfq-3")
		if err == nil || rows != nil || hits.Load() != 2 {
			t.Fatalf("rows=%+v err=%v hits=%d", rows, err, hits.Load())
		}
	})

	t.Run("conflicting duplicate", func(t *testing.T) {
		var hits atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if hits.Add(1) == 1 {
				writeTestJSON(t, w, map[string]any{
					"quotes": []map[string]any{{"id": "same", "rfq_id": "rfq-4", "status": "open"}},
					"cursor": "next",
				})
				return
			}
			writeTestJSON(t, w, map[string]any{
				"quotes": []map[string]any{{"id": "same", "rfq_id": "rfq-4", "status": "cancelled"}},
				"cursor": "",
			})
		}))
		defer srv.Close()
		rows, err := newRFQQuotesTestClient(t, srv.URL).GetQuotes(context.Background(), "rfq-4")
		if err == nil || rows != nil || hits.Load() != 2 {
			t.Fatalf("rows=%+v err=%v hits=%d", rows, err, hits.Load())
		}
	})
}
