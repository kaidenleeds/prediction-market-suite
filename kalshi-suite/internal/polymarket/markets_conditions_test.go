package polymarket

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"
)

type marketsRoundTripFunc func(*http.Request) (*http.Response, error)

func (f marketsRoundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func marketsGammaResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Body:       io.NopCloser(strings.NewReader(body)),
		Header:     make(http.Header),
	}
}

func TestMarketsByConditionsStrictSurfacesAnyFailedChunk(t *testing.T) {
	ids := make([]string, 21)
	for i := range ids {
		ids[i] = "condition-" + strconv.Itoa(i)
	}
	transportErr := errors.New("gamma transport unavailable")
	c := NewClient(time.Second)
	c.http = &http.Client{Transport: marketsRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Host != "gamma-api.polymarket.com" || r.URL.Path != "/markets" {
			return nil, errors.New("unexpected gamma URL: " + r.URL.String())
		}
		requested := r.URL.Query()["condition_ids"]
		if r.URL.Query().Get("closed") == "" && len(requested) == 1 && requested[0] == ids[20] {
			return nil, transportErr
		}
		if r.URL.Query().Get("closed") == "" && len(requested) == 20 {
			return marketsGammaResponse(http.StatusOK, `[{"conditionId":"`+ids[0]+`"}]`), nil
		}
		return marketsGammaResponse(http.StatusOK, `[]`), nil
	})}

	got, err := c.MarketsByConditionsStrict(context.Background(), ids)
	if err == nil || !errors.Is(err, transportErr) {
		t.Fatalf("strict error=%v, want wrapped transport failure", err)
	}
	if _, ok := got[ids[0]]; !ok {
		t.Fatalf("successful chunk missing from partial result: %#v", got)
	}

	// The old signature remains a best-effort read API: it may use successful chunks, while
	// the strict mutation API above makes the exact same partial response non-authoritative.
	compat := c.MarketsByConditions(context.Background(), ids)
	if _, ok := compat[ids[0]]; !ok {
		t.Fatalf("compatibility wrapper discarded successful chunk: %#v", compat)
	}
}

func TestMarketsByConditionsStrictDistinguishesSuccessfulAbsence(t *testing.T) {
	var calls int
	c := NewClient(time.Second)
	c.http = &http.Client{Transport: marketsRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		calls++
		return marketsGammaResponse(http.StatusOK, `[]`), nil
	})}

	got, err := c.MarketsByConditionsStrict(context.Background(), []string{"condition-absent"})
	if err != nil || len(got) != 0 {
		t.Fatalf("successful venue absence got=%#v err=%v", got, err)
	}
	if calls != 2 {
		t.Fatalf("calls=%d, want open + closed lifecycle passes", calls)
	}
}
