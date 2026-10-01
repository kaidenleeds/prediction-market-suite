package polymarketus

import (
	"context"
	"crypto/ed25519"
	"net/http"
	"net/http/httptest"
	"testing"
)

func r147PUSClient(ts *httptest.Server) *Client {
	return &Client{
		baseURL: ts.URL,
		keyID:   "r147-test",
		priv:    ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize)),
		http:    ts.Client(),
	}
}

func TestR147LivePositionsPaginatesAllPages(t *testing.T) {
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
			if got := r.URL.Query().Get("cursor"); got != "" {
				t.Errorf("first cursor=%q", got)
			}
			_, _ = w.Write([]byte(`{"positions":{"a":{"netPositionDecimal":"1","cost":{"value":"0.40"},"marketMetadata":{"slug":"a"}}},"nextCursor":"next 1","eof":false}`))
		case 2:
			if got := r.URL.Query().Get("cursor"); got != "next 1" {
				t.Errorf("second cursor=%q, want decoded next 1", got)
			}
			_, _ = w.Write([]byte(`{"positions":{"b":{"netPositionDecimal":"2","cost":{"value":"0.80"},"marketMetadata":{"slug":"b"}}},"nextCursor":"","eof":true}`))
		default:
			t.Errorf("unexpected page %d", page)
		}
	}))
	defer ts.Close()
	client = r147PUSClient(ts)

	rows, err := client.LivePositions(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || page != 2 {
		t.Fatalf("rows=%+v pages=%d, want two rows/two pages", rows, page)
	}
	if rows[0].Slug != "a" || rows[1].Slug != "b" {
		t.Fatalf("unexpected flatten order/content: %+v", rows)
	}
}

func TestR147LivePositionsRejectsBrokenPagination(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{"missing-eof", `{"positions":{}}`},
		{"missing-next-cursor", `{"positions":{},"eof":false}`},
		{"cursor-cycle", `{"positions":{},"nextCursor":"same","eof":false}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				if tc.name == "cursor-cycle" && calls == 1 {
					_, _ = w.Write([]byte(tc.body))
					return
				}
				if tc.name == "cursor-cycle" {
					_, _ = w.Write([]byte(`{"positions":{},"nextCursor":"same","eof":false}`))
					return
				}
				_, _ = w.Write([]byte(tc.body))
			}))
			defer ts.Close()
			if _, err := r147PUSClient(ts).LivePositions(context.Background()); err == nil {
				t.Fatal("expected fail-closed pagination error")
			}
		})
	}
}

func TestR147LivePositionsRejectsDuplicateMarketAcrossPages(t *testing.T) {
	calls := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			_, _ = w.Write([]byte(`{"positions":{"dup":{"marketMetadata":{"slug":"dup"}}},"nextCursor":"two","eof":false}`))
			return
		}
		_, _ = w.Write([]byte(`{"positions":{"dup":{"marketMetadata":{"slug":"dup"}}},"eof":true}`))
	}))
	defer ts.Close()
	if _, err := r147PUSClient(ts).LivePositions(context.Background()); err == nil {
		t.Fatal("expected duplicate market to fail closed")
	}
}
