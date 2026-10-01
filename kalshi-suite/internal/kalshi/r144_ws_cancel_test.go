package kalshi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// Auditor r70: canceling the ticker supervisor must close both authenticated role sockets now,
// rather than leave their read loops alive until the 60-second deadline.
func TestR144TickerStreamCancellationClosesPrimaryAndStandby(t *testing.T) {
	accepted := make(chan struct{}, 2)
	closed := make(chan struct{}, 2)
	up := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		accepted <- struct{}{}
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				break
			}
		}
		_ = conn.Close()
		closed <- struct{}{}
	}))
	defer ts.Close()

	signer, err := NewSigner("ticker-cancel-test", []byte(testPEM(t)))
	if err != nil {
		t.Fatal(err)
	}
	c := NewClient(BaseDemo, signer, 10, time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		c.StartTickerStream(ctx, "ws"+strings.TrimPrefix(ts.URL, "http"))
		close(done)
	}()
	for i := 0; i < 2; i++ {
		select {
		case <-accepted:
		case <-time.After(3 * time.Second):
			t.Fatalf("timed out waiting for ticker role socket %d", i+1)
		}
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("ticker supervisor did not return promptly after cancellation")
	}
	for i := 0; i < 2; i++ {
		select {
		case <-closed:
		case <-time.After(2 * time.Second):
			t.Fatalf("ticker cancellation left role socket %d open", i+1)
		}
	}
	if c.ForceReconnectWS() {
		t.Fatal("canceled ticker supervisor left its reconnect callback armed")
	}
}
