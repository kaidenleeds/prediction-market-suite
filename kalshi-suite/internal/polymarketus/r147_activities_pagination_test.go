package polymarketus

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func r147VerifySignedRequest(t *testing.T, c *Client, r *http.Request) {
	t.Helper()
	ts := r.Header.Get("X-PM-Timestamp")
	sig, err := base64.StdEncoding.DecodeString(r.Header.Get("X-PM-Signature"))
	if err != nil {
		t.Fatalf("decode signature: %v", err)
	}
	pub := c.priv.Public().(ed25519.PublicKey)
	if !ed25519.Verify(pub, []byte(ts+r.Method+r.URL.EscapedPath()), sig) {
		t.Fatalf("signature did not cover bare URL path %q", r.URL.EscapedPath())
	}
	if r.URL.RawQuery != "" && ed25519.Verify(pub, []byte(ts+r.Method+r.RequestURI), sig) {
		t.Fatalf("signature incorrectly included query string in request URI %q", r.RequestURI)
	}
}

func TestR147ActivitiesPaginatesNewestFirstAndSignsEscapedCursor(t *testing.T) {
	page := 0
	var client *Client
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		page++
		r147VerifySignedRequest(t, client, r)
		if got := r.URL.Query().Get("limit"); got != "100" {
			t.Errorf("limit=%q, want 100", got)
		}
		switch page {
		case 1:
			if r.RequestURI != "/v1/portfolio/activities?limit=100" {
				t.Errorf("first URI=%q", r.RequestURI)
			}
			_, _ = w.Write([]byte(`{"activities":[{"type":"ACTIVITY_TYPE_TRADE","trade":{"id":"new","marketSlug":"newest","price":{"value":"0.40"},"qty":"1","createTime":"2026-07-15T12:00:00Z"}}],"nextCursor":"next &/","eof":false}`))
		case 2:
			if r.URL.Query().Get("cursor") != "next &/" || !strings.Contains(r.RequestURI, "cursor=next+%26%2F") {
				t.Errorf("cursor was not URL-escaped exactly: URI=%q decoded=%q", r.RequestURI, r.URL.Query().Get("cursor"))
			}
			_, _ = w.Write([]byte(`{"activities":[{"type":"ACTIVITY_TYPE_POSITION_RESOLUTION","positionResolution":{"marketSlug":"older","tradeId":"resolution-1","side":"POSITION_RESOLUTION_SIDE_LONG","updateTime":"2026-07-15T11:00:00Z","beforePosition":{"netPosition":"2","avgPx":{"value":"0.40"}},"afterPosition":{"realized":{"value":"0.20"}}}}],"nextCursor":"","eof":true}`))
		default:
			t.Errorf("unexpected page %d", page)
		}
	}))
	defer ts.Close()
	client = r147PUSClient(ts)

	rows, err := client.Activities(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || page != 2 || rows[0].Slug != "newest" || rows[1].Slug != "older" {
		t.Fatalf("newest-first page order was not preserved: rows=%+v pages=%d", rows, page)
	}
}

func TestR147ActivitiesRejectsBrokenPagination(t *testing.T) {
	tests := []struct {
		name string
		body func(call int) string
	}{
		{"missing-activities", func(int) string { return `{"eof":true}` }},
		{"missing-eof", func(int) string { return `{"activities":[]}` }},
		{"missing-next-cursor", func(int) string { return `{"activities":[],"eof":false}` }},
		{"cursor-cycle", func(int) string { return `{"activities":[],"nextCursor":"same","eof":false}` }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls++
				_, _ = w.Write([]byte(tc.body(calls)))
			}))
			defer ts.Close()
			if _, err := r147PUSClient(ts).Activities(context.Background()); err == nil {
				t.Fatal("expected fail-closed pagination error")
			}
		})
	}
}

func TestR147ActivitiesRejectsStableDuplicateAcrossPagesButKeepsIDLessFills(t *testing.T) {
	t.Run("stable-trade-id", func(t *testing.T) {
		calls := 0
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			calls++
			if calls == 1 {
				_, _ = w.Write([]byte(`{"activities":[{"type":"ACTIVITY_TYPE_TRADE","trade":{"id":"dup","marketSlug":"a","price":{"value":"0.4"},"qty":"1"}}],"nextCursor":"two","eof":false}`))
				return
			}
			_, _ = w.Write([]byte(`{"activities":[{"type":"ACTIVITY_TYPE_TRADE","trade":{"id":"dup","marketSlug":"a","price":{"value":"0.4"},"qty":"1"}}],"eof":true}`))
		}))
		defer ts.Close()
		if _, err := r147PUSClient(ts).Activities(context.Background()); err == nil || !strings.Contains(err.Error(), "repeated stable activity") {
			t.Fatalf("stable duplicate did not fail closed: %v", err)
		}
	})

	t.Run("idless-same-time-fills", func(t *testing.T) {
		calls := 0
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			calls++
			activity := `{"type":"ACTIVITY_TYPE_TRADE","trade":{"marketSlug":"same","price":{"value":"0.4"},"qty":"1","createTime":"2026-07-15T12:00:00Z"}}`
			if calls == 1 {
				_, _ = w.Write([]byte(`{"activities":[` + activity + `],"nextCursor":"two","eof":false}`))
				return
			}
			_, _ = w.Write([]byte(`{"activities":[` + activity + `],"eof":true}`))
		}))
		defer ts.Close()
		rows, err := r147PUSClient(ts).Activities(context.Background())
		if err != nil || len(rows) != 2 {
			t.Fatalf("id-less same-time fills were incorrectly collapsed: rows=%+v err=%v", rows, err)
		}
	})
}

func TestR147ActivitiesRejectsMaxPageOverflow(t *testing.T) {
	calls := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		_, _ = fmt.Fprintf(w, `{"activities":[],"nextCursor":"cursor-%d","eof":false}`, calls)
	}))
	defer ts.Close()
	if _, err := r147PUSClient(ts).Activities(context.Background()); err == nil || !strings.Contains(err.Error(), "exceeded 100 pages") {
		t.Fatalf("max-page overflow did not fail closed: %v", err)
	}
	if calls != 100 {
		t.Fatalf("bounded paginator made %d calls, want exactly 100", calls)
	}
}
