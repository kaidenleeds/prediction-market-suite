package polymarket

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestConditionByTokenValidatesAndCachesCanonicalID(t *testing.T) {
	want := "0x" + strings.Repeat("Ab", 32)
	var calls atomic.Int32
	c := NewClient(time.Second)
	c.http = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		calls.Add(1)
		if r.URL.Host != "clob.polymarket.com" || r.URL.Path != "/markets-by-token/123456" {
			t.Fatalf("unexpected lookup URL: %s", r.URL.String())
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(`{"condition_id":"` + want + `"}`)),
			Header:     make(http.Header),
		}, nil
	})}

	for i := 0; i < 2; i++ {
		got, ok, err := c.ConditionByToken(context.Background(), "000123456")
		if err != nil || !ok || got != strings.ToLower(want) {
			t.Fatalf("call %d: got=%q ok=%v err=%v", i+1, got, ok, err)
		}
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("HTTP calls=%d, want one cached lookup", got)
	}
	if got, found, known := c.CachedConditionByToken("123456"); !known || !found || got != strings.ToLower(want) {
		t.Fatalf("cache tri-state got=%q found=%v known=%v", got, found, known)
	}
}

func TestConditionByTokenRejectsTruncatedCondition(t *testing.T) {
	c := NewClient(time.Second)
	c.http = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(`{"condition_id":"0x` + strings.Repeat("a", 62) + `"}`)),
			Header:     make(http.Header),
		}, nil
	})}

	got, ok, err := c.ConditionByToken(context.Background(), "42")
	if got != "" || ok || !errors.Is(err, ErrInvalidConditionID) {
		t.Fatalf("got=%q ok=%v err=%v; want strict invalid-condition failure", got, ok, err)
	}
}

func TestConditionByTokenCachesDefinitiveMiss(t *testing.T) {
	var calls atomic.Int32
	c := NewClient(time.Second)
	c.http = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		calls.Add(1)
		return &http.Response{
			StatusCode: http.StatusNotFound,
			Body:       io.NopCloser(strings.NewReader(`{}`)),
			Header:     make(http.Header),
		}, nil
	})}

	for i := 0; i < 2; i++ {
		got, ok, err := c.ConditionByToken(context.Background(), "99")
		if err != nil || ok || got != "" {
			t.Fatalf("call %d: got=%q ok=%v err=%v", i+1, got, ok, err)
		}
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("HTTP calls=%d, want one cached definitive miss", got)
	}
	if got, found, known := c.CachedConditionByToken("99"); !known || found || got != "" {
		t.Fatalf("miss cache tri-state got=%q found=%v known=%v", got, found, known)
	}
}
