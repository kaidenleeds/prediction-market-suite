package polymarketus

import (
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// Auditor r70: a quiet private-account socket used to survive context cancellation until its
// 75-second read deadline. Exercise the real subscription/read loop and require immediate close.
func TestR144PrivateWSCancellationClosesSession(t *testing.T) {
	accepted := make(chan struct{}, 1)
	closed := make(chan struct{}, 1)
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

	secret := base64.StdEncoding.EncodeToString(make([]byte, 32))
	c, err := NewClient("private-cancel-test", secret, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	w := c.NewPrivateWS()
	w.wsURL = "ws" + strings.TrimPrefix(ts.URL, "http")
	subscribed := make(chan struct{}, 1)
	w.SetOnState(func(state, _ string) {
		if state == "SUBSCRIBED" {
			select {
			case subscribed <- struct{}{}:
			default:
			}
		}
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		w.Stream(ctx)
		close(done)
	}()
	select {
	case <-accepted:
	case <-time.After(3 * time.Second):
		t.Fatal("private websocket did not connect")
	}
	select {
	case <-subscribed:
	case <-time.After(3 * time.Second):
		t.Fatal("private websocket did not finish subscribing")
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("private websocket did not return promptly after cancellation")
	}
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("private websocket transport remained open after cancellation")
	}
	if w.Healthy() {
		t.Fatal("canceled private websocket remained healthy")
	}
}
