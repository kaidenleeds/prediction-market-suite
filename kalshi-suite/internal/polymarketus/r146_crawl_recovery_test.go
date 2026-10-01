package polymarketus

import (
	"errors"
	"testing"
)

func TestR146HTTP2GoAwayIsRetryableTransport(t *testing.T) {
	err := errors.New(`polyus GET /v1/markets body: http2: server sent GOAWAY and closed the connection; ErrCode=NO_ERROR`)
	if !transientMarketsTransport(err) || !RetryableMarketsCrawlError(err) {
		t.Fatalf("graceful HTTP/2 connection rotation was not retryable: %v", err)
	}
	if RetryableMarketsCrawlError(errors.New("polyus market lifecycle fields absent")) {
		t.Fatal("a schema/lifecycle failure was incorrectly made a fast transport retry")
	}
}

func TestR146MovingTailBoundaryRequestsFreshCompleteCrawl(t *testing.T) {
	err := errors.New("polyus markets pagination made no progress at offset 7900")
	if !RetryableMarketsCrawlError(err) {
		t.Fatalf("moving offset boundary should request a fresh complete crawl: %v", err)
	}
}
