package server

import (
	"context"
	"time"
)

const (
	// A fresh allowlisted signal may promote an off-prefix market into the bounded full-depth
	// WebSocket set. Subscription reconciliation is asynchronous, so give only that cold-start
	// condition a short cache-only wait instead of permanently rejecting the signal before its
	// first snapshot arrives. Markets already resident pay no timer or retry.
	r159LiveBookPromotionMaxWait = 2 * time.Second
	r159LiveBookPromotionPoll    = 10 * time.Millisecond
)

func r159LiveBookPromotionPending(reason string) bool {
	switch reason {
	case "kalshi-ws-full-book-unavailable",
		"kalshi-ws-current-generation-full-book-unavailable",
		"no-current-generation-explicitly-open-full-book":
		return true
	default:
		return false
	}
}

func r159WaitForPromotedLiveBook(ctx context.Context, maxWait, pollEvery time.Duration,
	read func() (liveMirrorQuote, string)) (liveMirrorQuote, string) {
	if read == nil {
		return liveMirrorQuote{}, "live-book-reader-unavailable"
	}
	if ctx == nil {
		ctx = context.Background()
	}
	quote, why := read()
	if why == "" || !r159LiveBookPromotionPending(why) || maxWait <= 0 {
		return quote, why
	}
	if pollEvery <= 0 {
		pollEvery = r159LiveBookPromotionPoll
	}
	timer := time.NewTimer(maxWait)
	defer timer.Stop()
	ticker := time.NewTicker(pollEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return liveMirrorQuote{}, why
		case <-timer.C:
			return liveMirrorQuote{}, why
		case <-ticker.C:
			quote, why = read()
			if why == "" || !r159LiveBookPromotionPending(why) {
				return quote, why
			}
		}
	}
}

// liveMirrorExecutableAfterPromotion is still a zero-public-REST boundary. It immediately reads
// resident WebSocket truth and only waits when the exact miss can be repaired by the asynchronous
// subscription promotion already published ahead of this preflight.
func (s *Server) liveMirrorExecutableAfterPromotion(ctx context.Context,
	candidate liveMirrorCandidate) (liveMirrorQuote, string) {
	maxWait := r159LiveBookPromotionMaxWait
	if !candidate.At.IsZero() {
		remaining := liveMirrorTTL - time.Since(candidate.At)
		if remaining <= 0 {
			return s.liveMirrorExecutable(ctx, candidate)
		}
		if remaining < maxWait {
			maxWait = remaining
		}
	}
	return r159WaitForPromotedLiveBook(ctx, maxWait, r159LiveBookPromotionPoll,
		func() (liveMirrorQuote, string) {
			return s.liveMirrorExecutable(ctx, candidate)
		})
}
