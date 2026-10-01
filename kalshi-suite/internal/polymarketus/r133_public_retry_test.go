package polymarketus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type r133RoundTripFunc func(*http.Request) (*http.Response, error)

func (f r133RoundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type r133TemporaryNetError struct{}

func (r133TemporaryNetError) Error() string   { return "temporary connection reset" }
func (r133TemporaryNetError) Timeout() bool   { return false }
func (r133TemporaryNetError) Temporary() bool { return true }

func r133RetryMarket(i int) Market {
	active, closed, archived := true, false, false
	return Market{
		ID:       fmt.Sprintf("retry-%03d", i),
		Slug:     fmt.Sprintf("retry-%03d", i),
		Active:   &active,
		Closed:   &closed,
		Archived: &archived,
		State:    "MARKET_STATE_OPEN",
	}
}

func r133RetryClient(ts *httptest.Server) *PublicClient {
	p := NewPublicClient(time.Second)
	p.baseURL = ts.URL
	p.pageRetryBase = time.Nanosecond
	return p
}

func TestOpenMarketsAllRetriesOnlyLateTruncatedPage(t *testing.T) {
	all := make([]Market, 300)
	for i := range all {
		all[i] = r133RetryMarket(i)
	}

	var mu sync.Mutex
	var offsets []int
	failedOnce := false
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
		mu.Lock()
		offsets = append(offsets, offset)
		fail := offset == 200 && !failedOnce
		if fail {
			failedOnce = true
		}
		mu.Unlock()
		if fail {
			_, _ = w.Write([]byte(`{"markets":[{"id":"cut-off"}`))
			return
		}
		end := offset + 100 // Simulate the gateway's own cap below the requested 250.
		if end > len(all) {
			end = len(all)
		}
		page := []Market{}
		if offset < len(all) {
			page = all[offset:end]
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"markets": page})
	}))
	defer ts.Close()

	got, stats, err := r133RetryClient(ts).OpenMarketsAll(context.Background(), "orderBy=slug")
	if err != nil || len(got) != len(all) {
		t.Fatalf("complete crawl after one truncated page: rows=%d stats=%+v err=%v", len(got), stats, err)
	}
	if stats.Pages != 4 || stats.Seen != 300 || stats.Unique != 300 || stats.Open != 300 {
		t.Fatalf("retry must not double-count the failed page: %+v", stats)
	}
	mu.Lock()
	defer mu.Unlock()
	if want := "[0 100 200 200 300]"; fmt.Sprint(offsets) != want {
		t.Fatalf("offsets=%v, want %s; retry must resume the same offset without replaying prior pages", offsets, want)
	}
}

func TestOpenMarketsAllRetries429And5xxAtSameOffset(t *testing.T) {
	for _, status := range []int{http.StatusTooManyRequests, http.StatusServiceUnavailable} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			calls := 0
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				if calls == 1 {
					_ = json.NewEncoder(w).Encode(map[string]any{"markets": []Market{r133RetryMarket(0)}})
					return
				}
				if calls == 2 {
					w.Header().Set("Retry-After", "0")
					w.WriteHeader(status)
					_, _ = w.Write([]byte(`{"error":"transient"}`))
					return
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"markets": []Market{}})
			}))
			defer ts.Close()

			got, stats, err := r133RetryClient(ts).OpenMarketsAll(context.Background(), "")
			if err != nil || len(got) != 1 || calls != 3 || stats.Pages != 2 || stats.Seen != 1 {
				t.Fatalf("status=%d rows=%d calls=%d stats=%+v err=%v", status, len(got), calls, stats, err)
			}
		})
	}
}

func TestOpenMarketsAllRetriesTransientTransportAtSameOffset(t *testing.T) {
	var offsets []int
	failedOnce := false
	p := NewPublicClient(time.Second)
	p.baseURL = "http://polyus.test"
	p.pageRetryBase = time.Nanosecond
	p.http.Transport = r133RoundTripFunc(func(r *http.Request) (*http.Response, error) {
		offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
		offsets = append(offsets, offset)
		if offset == 1 && !failedOnce {
			failedOnce = true
			return nil, r133TemporaryNetError{}
		}
		page := []Market{}
		if offset == 0 {
			page = []Market{r133RetryMarket(0)}
		}
		raw, _ := json.Marshal(map[string]any{"markets": page})
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(string(raw))),
			Request:    r,
		}, nil
	})

	got, stats, err := p.OpenMarketsAll(context.Background(), "")
	if err != nil || len(got) != 1 || stats.Pages != 2 || fmt.Sprint(offsets) != "[0 1 1]" {
		t.Fatalf("transport retry must resume only the failed offset: rows=%d offsets=%v stats=%+v err=%v", len(got), offsets, stats, err)
	}
}

func TestOpenMarketsAllPersistentPageFailureReturnsNoPartialRows(t *testing.T) {
	var offsets []string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		offset := r.URL.Query().Get("offset")
		offsets = append(offsets, offset)
		if offset == "0" {
			_ = json.NewEncoder(w).Encode(map[string]any{"markets": []Market{r133RetryMarket(0)}})
			return
		}
		w.Header().Set("Retry-After", "0")
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer ts.Close()

	p := r133RetryClient(ts)
	got, stats, err := p.OpenMarketsAll(context.Background(), "")
	if err == nil || got != nil {
		t.Fatalf("exhausted late page must return no partial rows: got=%v stats=%+v err=%v", got, stats, err)
	}
	if stats.Pages != 1 || stats.Seen != 1 || strings.Join(offsets, ",") != "0,1,1,1" {
		t.Fatalf("bounded retry receipt is wrong: offsets=%v stats=%+v", offsets, stats)
	}
}

func TestOpenMarketsAllDoesNotRetryPermanentPageOrOutwaitContext(t *testing.T) {
	t.Run("non-transient 4xx", func(t *testing.T) {
		calls := 0
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls++
			if r.URL.Query().Get("offset") == "0" {
				_ = json.NewEncoder(w).Encode(map[string]any{"markets": []Market{r133RetryMarket(0)}})
				return
			}
			w.WriteHeader(http.StatusBadRequest)
		}))
		defer ts.Close()
		got, _, err := r133RetryClient(ts).OpenMarketsAll(context.Background(), "")
		if err == nil || got != nil || calls != 2 {
			t.Fatalf("400 must fail once with no partial rows: rows=%v calls=%d err=%v", got, calls, err)
		}
	})

	t.Run("schema error", func(t *testing.T) {
		calls := 0
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls++
			if r.URL.Query().Get("offset") == "0" {
				_ = json.NewEncoder(w).Encode(map[string]any{"markets": []Market{r133RetryMarket(0)}})
				return
			}
			_, _ = w.Write([]byte(`{"markets":"not-an-array"}`))
		}))
		defer ts.Close()
		got, _, err := r133RetryClient(ts).OpenMarketsAll(context.Background(), "")
		if err == nil || got != nil || calls != 2 {
			t.Fatalf("schema error must fail once with no partial rows: rows=%v calls=%d err=%v", got, calls, err)
		}
	})

	t.Run("Retry-After honors parent deadline", func(t *testing.T) {
		calls := 0
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			calls++
			w.Header().Set("Retry-After", "60")
			w.WriteHeader(http.StatusTooManyRequests)
		}))
		defer ts.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		started := time.Now()
		got, _, err := r133RetryClient(ts).OpenMarketsAll(ctx, "")
		if got != nil || !errors.Is(err, context.DeadlineExceeded) || calls != 1 || time.Since(started) > time.Second {
			t.Fatalf("deadline must interrupt Retry-After: rows=%v calls=%d elapsed=%v err=%v", got, calls, time.Since(started), err)
		}
	})
}
