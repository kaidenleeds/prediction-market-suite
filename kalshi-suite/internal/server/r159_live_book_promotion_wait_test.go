package server

import (
	"context"
	"testing"
	"time"
)

func TestR159PromotedLiveBookAlreadyResidentHasNoWait(t *testing.T) {
	calls := 0
	want := liveMirrorQuote{Price: .42, Depth: 7}
	started := time.Now()
	got, why := r159WaitForPromotedLiveBook(t.Context(), time.Second, time.Millisecond,
		func() (liveMirrorQuote, string) {
			calls++
			return want, ""
		})
	if why != "" || got.Price != want.Price || got.Depth != want.Depth {
		t.Fatalf("quote=%+v reason=%q, want %+v and no rejection", got, why, want)
	}
	if calls != 1 {
		t.Fatalf("resident book reads=%d, want exactly one", calls)
	}
	if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
		t.Fatalf("resident book paid an unexpected wait: %s", elapsed)
	}
}

func TestR159PromotedLiveBookRetriesOnlyAsyncSubscriptionMiss(t *testing.T) {
	calls := 0
	got, why := r159WaitForPromotedLiveBook(t.Context(), time.Second, time.Millisecond,
		func() (liveMirrorQuote, string) {
			calls++
			if calls < 3 {
				return liveMirrorQuote{}, "kalshi-ws-current-generation-full-book-unavailable"
			}
			return liveMirrorQuote{Price: .44, Depth: 5}, ""
		})
	if why != "" || got.Price != .44 || got.Depth != 5 {
		t.Fatalf("quote=%+v reason=%q", got, why)
	}
	if calls != 3 {
		t.Fatalf("book reads=%d, want initial plus two bounded cache retries", calls)
	}
}

func TestR159PromotedLiveBookDoesNotRetryPermanentRejection(t *testing.T) {
	calls := 0
	_, why := r159WaitForPromotedLiveBook(t.Context(), time.Second, time.Millisecond,
		func() (liveMirrorQuote, string) {
			calls++
			return liveMirrorQuote{}, "kalshi-cached-market-not-active"
		})
	if why != "kalshi-cached-market-not-active" {
		t.Fatalf("reason=%q", why)
	}
	if calls != 1 {
		t.Fatalf("permanent rejection reads=%d, want one", calls)
	}
}

func TestR159PromotedLiveBookWaitIsContextBounded(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	go func() {
		time.Sleep(5 * time.Millisecond)
		cancel()
	}()
	started := time.Now()
	_, why := r159WaitForPromotedLiveBook(ctx, time.Second, time.Millisecond,
		func() (liveMirrorQuote, string) {
			calls++
			return liveMirrorQuote{}, "no-current-generation-explicitly-open-full-book"
		})
	if why != "no-current-generation-explicitly-open-full-book" {
		t.Fatalf("reason=%q", why)
	}
	if elapsed := time.Since(started); elapsed > 250*time.Millisecond {
		t.Fatalf("context cancellation did not bound wait: %s", elapsed)
	}
	if calls < 1 {
		t.Fatal("reader was never called")
	}
}

func TestR159BookPromotionPendingReasonsAreNarrow(t *testing.T) {
	for _, reason := range []string{
		"kalshi-ws-full-book-unavailable",
		"kalshi-ws-current-generation-full-book-unavailable",
		"no-current-generation-explicitly-open-full-book",
	} {
		if !r159LiveBookPromotionPending(reason) {
			t.Fatalf("expected retryable reason %q", reason)
		}
	}
	for _, reason := range []string{
		"polyus-ws-unavailable",
		"invalid-full-book-or-auth-unavailable",
		"current-polyus-complete-crawl-rules-unavailable",
		"kalshi-ws-book-one-sided-or-empty",
		"kalshi-cached-market-not-active",
	} {
		if r159LiveBookPromotionPending(reason) {
			t.Fatalf("permanent/safety reason unexpectedly retryable: %q", reason)
		}
	}
}
