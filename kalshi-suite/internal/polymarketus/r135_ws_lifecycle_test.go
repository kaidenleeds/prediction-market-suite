package polymarketus

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// Auditor r63 P2: cancellation used to return from Stream while both role sockets and their read
// goroutines remained alive. Exercise the real runSession read loops against a local websocket
// peer and prove cancellation closes primary + standby, disables the kick hook, and does not redial.
func TestMarketsWSStreamCancellationClosesBothSessions(t *testing.T) {
	closed := make(chan struct{}, 4)
	accepted := make(chan struct{}, 4)
	up := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		accepted <- struct{}{}
		_, _, _ = conn.ReadMessage() // client teardown closes the transport and releases this handler
		_ = conn.Close()
		closed <- struct{}{}
	}))
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http")
	var dials atomic.Int64
	ws := (&Client{}).NewMarketsWS()
	ws.dialFn = func(ctx context.Context, _ []string) (*pusSession, error) {
		conn, _, err := websocket.DefaultDialer.DialContext(ctx, wsURL, nil)
		if err != nil {
			return nil, err
		}
		dials.Add(1)
		return &pusSession{conn: conn}, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		ws.Stream(ctx, func() []string { return []string{"market"} })
		close(done)
	}()
	for i := 0; i < 2; i++ {
		select {
		case <-accepted:
		case <-time.After(3 * time.Second):
			t.Fatalf("timed out waiting for both role sockets; accepted=%d", i)
		}
	}

	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Stream did not return after cancellation")
	}
	for i := 0; i < 2; i++ {
		select {
		case <-closed:
		case <-time.After(3 * time.Second):
			t.Fatalf("cancellation left a role socket open; closed=%d", i)
		}
	}
	if ws.ForceReconnect() {
		t.Fatal("canceled Stream left its ForceReconnect closure armed")
	}
	if got := ws.ConnectionReceipt(); got.ActiveGeneration != 0 {
		t.Fatalf("canceled Stream left generation %d authoritative", got.ActiveGeneration)
	}
	time.Sleep(25 * time.Millisecond) // let any erroneous onDead refill attempt reach dialFn
	if got := dials.Load(); got != 2 {
		t.Fatalf("cancellation spawned replacement sessions: dials=%d want 2", got)
	}
}

// If the standby cannot be dialed, the five-minute reconciliation must still refresh the active
// socket in place and replace its old universe with the latest exact complete plan.
func TestMarketsWSReconcileExactPlanWhenStandbyUnavailable(t *testing.T) {
	accepted := make(chan struct{}, 1)
	up := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		accepted <- struct{}{}
		_, _, _ = conn.ReadMessage()
		_ = conn.Close()
	}))
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http")
	ws := (&Client{}).NewMarketsWS()
	ws.rotationEvery = 20 * time.Millisecond
	var dials atomic.Int64
	writes := make(chan any, 16)
	primary := make(chan *pusSession, 1)
	ws.dialFn = func(ctx context.Context, _ []string) (*pusSession, error) {
		if dials.Add(1) != 1 {
			return nil, errors.New("standby unavailable")
		}
		conn, _, err := websocket.DefaultDialer.DialContext(ctx, wsURL, nil)
		if err != nil {
			return nil, err
		}
		s := &pusSession{
			conn:    conn,
			wide:    []string{"legacy"},
			wideSet: map[string]bool{"legacy": true},
			writeJSON: func(v any) error {
				select {
				case writes <- v:
				default:
				}
				return nil
			},
		}
		primary <- s
		return s, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		ws.Stream(ctx, func() []string { return []string{"fresh"} })
		close(done)
	}()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("Stream did not stop")
		}
	}()

	var s *pusSession
	select {
	case s = <-primary:
	case <-time.After(3 * time.Second):
		t.Fatal("primary did not connect")
	}
	select {
	case <-accepted:
	case <-time.After(3 * time.Second):
		t.Fatal("server did not accept primary")
	}
	select {
	case <-writes:
	case <-time.After(3 * time.Second):
		t.Fatal("reconciliation did not resubscribe the active primary without a standby")
	}
	if got := s.wideCount(); got != 1 {
		t.Fatalf("same-session reconcile retained a removed slug: got %d want 1", got)
	}
	if full, lite, trade := ws.SubCoverage(); full != 1 || lite != 1 || trade != 1 {
		t.Fatalf("reconciled coverage full/lite/trade=%d/%d/%d want 1/1/1", full, lite, trade)
	}
	if got := ws.ConnectionReceipt().ActiveGeneration; got == 0 || got != s.gen {
		t.Fatalf("same-session reconciliation changed generation: active=%d session=%d", got, s.gen)
	}
}

// A healthy standby is disaster recovery, not a reason to invalidate good primary books every
// five minutes. Reconciliation updates both subscriptions but keeps the active generation stable.
func TestMarketsWSReconcileDoesNotRotateHealthyPrimary(t *testing.T) {
	up := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		for {
			if _, _, err = conn.ReadMessage(); err != nil {
				_ = conn.Close()
				return
			}
		}
	}))
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http")
	ws := (&Client{}).NewMarketsWS()
	ws.rotationEvery = 20 * time.Millisecond
	var dials atomic.Int64
	writes := make(chan uint64, 32)
	ws.dialFn = func(ctx context.Context, _ []string) (*pusSession, error) {
		conn, _, err := websocket.DefaultDialer.DialContext(ctx, wsURL, nil)
		if err != nil {
			return nil, err
		}
		gen := uint64(dials.Add(1))
		s := &pusSession{conn: conn, gen: gen, wide: []string{"legacy"},
			wideSet: map[string]bool{"legacy": true}}
		s.writeJSON = func(any) error {
			select {
			case writes <- gen:
			default:
			}
			return nil
		}
		return s, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		ws.Stream(ctx, func() []string { return []string{"fresh"} })
		close(done)
	}()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("Stream did not stop")
		}
	}()

	deadline := time.After(3 * time.Second)
	seen := map[uint64]bool{}
	for len(seen) < 2 {
		select {
		case gen := <-writes:
			seen[gen] = true
		case <-deadline:
			t.Fatalf("both sessions were not reconciled: %v", seen)
		}
	}
	before := ws.ConnectionReceipt().ActiveGeneration
	if before == 0 {
		t.Fatal("primary generation never became active")
	}
	ws.SetOpenSlugs([]string{"fresh"})
	if !ws.ingestGeneration([]byte(`{"marketDataLite":{"marketSlug":"fresh","bestBid":"0.40","bestAsk":"0.42"}}`), before) {
		t.Fatal("could not seed current-generation executable book")
	}
	ws.notePrimaryData(before)
	if ws.ExecutableCount() != 1 {
		t.Fatal("seeded primary book was not executable")
	}
	time.Sleep(75 * time.Millisecond)
	after := ws.ConnectionReceipt().ActiveGeneration
	if after != before {
		t.Fatalf("healthy reconciliation rotated generation: before=%d after=%d", before, after)
	}
	if got := dials.Load(); got != 2 {
		t.Fatalf("healthy reconciliation churned sockets: dials=%d want 2", got)
	}
	if ws.ExecutableCount() != 1 {
		t.Fatal("healthy reconciliation invalidated the primary's executable cached book")
	}
}

func testMarketSlugs(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("market-%03d", i)
	}
	return out
}

func waitMarketsWS(t *testing.T, what string, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestMarketsWSReconcilePrunesRemovedRequestIDsAndStandbyCannotPublish(t *testing.T) {
	ws := (&Client{}).NewMarketsWS()
	writes := &captureMarketWrites{}
	primary := &pusSession{gen: 1, writeJSON: writes.write}
	primary.primary.Store(true)
	ws.notePrimaryStart(primary.gen)
	ws.expectPrimarySubscriptionPlan(primary, testMarketSlugs(205))
	if err := ws.subscribeMarkets(primary, testMarketSlugs(205)); err != nil {
		t.Fatal(err)
	}
	if slugs, frames, _ := ws.SubPlan(); slugs != 205 || frames != 9 {
		t.Fatalf("initial active-primary plan slugs/frames=%d/%d want 205/9", slugs, frames)
	}

	// A standby finishing later must never overwrite the active primary's coverage receipt.
	standby := &pusSession{gen: 2, writeJSON: func(any) error { return nil }}
	if err := ws.subscribeMarkets(standby, testMarketSlugs(17)); err != nil {
		t.Fatal(err)
	}
	if slugs, frames, _ := ws.SubPlan(); slugs != 205 || frames != 9 {
		t.Fatalf("standby overwrote active coverage: slugs/frames=%d/%d", slugs, frames)
	}

	writes.reset()
	ws.expectPrimarySubscriptionPlan(primary, testMarketSlugs(95))
	if err := ws.subscribeMarkets(primary, testMarketSlugs(95)); err != nil {
		t.Fatal(err)
	}
	writes.mu.Lock()
	gotFrames := append([]map[string]any(nil), writes.frames...)
	writes.mu.Unlock()
	var unsubscribed []string
	for _, frame := range gotFrames {
		if u, ok := frame["unsubscribe"].(map[string]any); ok {
			id, _ := u["request_id"].(string)
			unsubscribed = append(unsubscribed, id)
		}
	}
	// Tail request IDs (the retained *-0 IDs) changed from 100 to 95 members, so they must be
	// explicitly removed too. All removals precede all exact replacement subscriptions.
	wantUnsubscribed := []string{"md-0", "md-1", "md-2", "mdl-0", "mdl-1", "mdl-2", "trade-0", "trade-1", "trade-2"}
	if fmt.Sprint(unsubscribed) != fmt.Sprint(wantUnsubscribed) {
		t.Fatalf("unsubscribe request IDs=%v want %v", unsubscribed, wantUnsubscribed)
	}
	if len(gotFrames) != 12 {
		t.Fatalf("exact prune wrote %d frames want 9 unsubs + 3 replacements", len(gotFrames))
	}
	for i := 0; i < len(wantUnsubscribed); i++ {
		if _, ok := gotFrames[i]["unsubscribe"]; !ok {
			t.Fatalf("frame %d installed replacement before all old memberships were removed: %#v", i, gotFrames[i])
		}
	}
	for i := len(wantUnsubscribed); i < len(gotFrames); i++ {
		if _, ok := gotFrames[i]["subscribe"]; !ok {
			t.Fatalf("frame %d is not an exact replacement subscription: %#v", i, gotFrames[i])
		}
	}
	if got := primary.wideCount(); got != 95 {
		t.Fatalf("exact session universe=%d want 95", got)
	}
	if len(primary.requests) != 3 {
		t.Fatalf("active request IDs=%d want 3: %v", len(primary.requests), primary.requests)
	}
	if slugs, frames, _ := ws.SubPlan(); slugs != 95 || frames != 3 {
		t.Fatalf("pruned active-primary plan slugs/frames=%d/%d want 95/3", slugs, frames)
	}
	if full, lite, trade := ws.SubCoverage(); full != 95 || lite != 95 || trade != 95 {
		t.Fatalf("pruned coverage full/lite/trade=%d/%d/%d want 95/95/95", full, lite, trade)
	}
	writes.reset()
	if err := ws.subscribeMarkets(primary, testMarketSlugs(95)); err != nil {
		t.Fatal(err)
	}
	if len(writes.frames) != 0 {
		t.Fatalf("unchanged exact plan emitted %d duplicate subscription frames", len(writes.frames))
	}
}

func TestMarketsWSBoundsWidePlanWhenCatalogIsHuge(t *testing.T) {
	ws := (&Client{}).NewMarketsWS()
	s := &pusSession{gen: 1, writeJSON: func(any) error { return nil }}
	s.primary.Store(true)
	ws.notePrimaryStart(s.gen)
	all := testMarketSlugs(DefaultMarketsWideBookCap + 250)
	ws.expectPrimarySubscriptionPlan(s, all)
	if err := ws.subscribeMarkets(s, all); err != nil {
		t.Fatal(err)
	}
	if slugs, frames, _ := ws.SubPlan(); slugs != DefaultMarketsWideBookCap || frames != 206 {
		t.Fatalf("bounded plan slugs/frames=%d/%d want %d/206", slugs, frames, DefaultMarketsWideBookCap)
	}
	if full, lite, trade := ws.SubCoverage(); full != DefaultMarketsFullBookCap ||
		lite != DefaultMarketsWideBookCap || trade != DefaultMarketsWideBookCap {
		t.Fatalf("bounded coverage full/lite/trade=%d/%d/%d", full, lite, trade)
	}
}

func TestMarketsWSPromotedStandbyPublishesOnlyAfterExactEqualCountPlanSucceeds(t *testing.T) {
	ws := (&Client{}).NewMarketsWS()
	writes := &captureMarketWrites{}
	s := &pusSession{gen: 7, writeJSON: writes.write}
	oldPlan := []string{"old-a", "old-b"}
	newPlan := []string{"new-a", "new-b"} // same count; membership is intentionally different
	if err := ws.subscribeMarkets(s, oldPlan); err != nil {
		t.Fatal(err)
	}
	oldHash := s.planHash

	// Promotion invalidates count-only authority. Even an explicit publication attempt must stay
	// blank because this standby owns two different markets than the current desired two.
	ws.notePrimaryStart(s.gen)
	s.primary.Store(true)
	ws.expectPrimarySubscriptionPlan(s, newPlan)
	ws.publishSessionCoverage(s)
	if slugs, frames, _ := ws.SubPlan(); slugs != 0 || frames != 0 {
		t.Fatalf("stale equal-count standby published before exact reconcile: slugs/frames=%d/%d", slugs, frames)
	}
	if oldHash == marketSubscriptionHashForSlugs(newPlan, ws.FullBookCap()) {
		t.Fatal("test setup did not produce distinct membership hashes")
	}

	// A failed exact-plan write must remain fail-closed rather than restoring the stale count.
	s.writeJSON = func(any) error { return errors.New("forced reconcile failure") }
	if err := ws.subscribeMarkets(s, newPlan); err == nil {
		t.Fatal("forced exact-plan reconciliation unexpectedly succeeded")
	}
	if slugs, frames, _ := ws.SubPlan(); slugs != 0 || frames != 0 {
		t.Fatalf("failed exact reconcile published coverage: slugs/frames=%d/%d", slugs, frames)
	}

	// Only the complete successful replacement may restore coverage.
	s.writeJSON = writes.write
	if err := ws.subscribeMarkets(s, newPlan); err != nil {
		t.Fatal(err)
	}
	if slugs, frames, _ := ws.SubPlan(); slugs != 2 || frames != 3 {
		t.Fatalf("successful exact reconcile coverage=%d/%d want 2/3", slugs, frames)
	}
	if s.planHash != marketSubscriptionHashForSlugs(newPlan, ws.FullBookCap()) {
		t.Fatal("promoted session did not retain the exact current membership hash")
	}
}

func TestMarketsWSPrimaryDeathPromotesStandbyAndRecoversData(t *testing.T) {
	type peer struct {
		id   int64
		conn *websocket.Conn
	}
	peers := make(chan peer, 4)
	var accepted atomic.Int64
	up := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		peers <- peer{id: accepted.Add(1), conn: conn}
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				_ = conn.Close()
				return
			}
		}
	}))
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http")
	ws := (&Client{}).NewMarketsWS()
	ws.rotationEvery = time.Hour
	ws.SetOpenSlugs([]string{"m"})
	var dials atomic.Int64
	ws.dialFn = func(ctx context.Context, _ []string) (*pusSession, error) {
		conn, _, err := websocket.DefaultDialer.DialContext(ctx, wsURL, nil)
		if err != nil {
			return nil, err
		}
		gen := uint64(dials.Add(1))
		return &pusSession{conn: conn, gen: gen, wide: []string{"m"}, wideSet: map[string]bool{"m": true},
			requests: map[string]marketSubscriptionPlan{
				"md-0":    {subscriptionType: 1, marketSlugs: "m"},
				"mdl-0":   {subscriptionType: 2, marketSlugs: "m"},
				"trade-0": {subscriptionType: 3, marketSlugs: "m"},
			},
			full: 1, frames: 3, subAt: time.Now()}, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		ws.Stream(ctx, func() []string { return []string{"m"} })
		close(done)
	}()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("Stream did not stop")
		}
	}()

	peerByID := map[int64]*websocket.Conn{}
	for len(peerByID) < 2 {
		select {
		case p := <-peers:
			peerByID[p.id] = p.conn
		case <-time.After(3 * time.Second):
			t.Fatal("primary and standby were not accepted")
		}
	}
	waitMarketsWS(t, "generation 1 primary", func() bool {
		return ws.ConnectionReceipt().ActiveGeneration == 1
	})
	book := []byte(`{"marketData":{"marketSlug":"m","state":"MARKET_STATE_OPEN","bids":[{"px":"0.40","qty":"5"}],"offers":[{"px":"0.42","qty":"6"}]}}`)
	if err := peerByID[1].WriteMessage(websocket.TextMessage, book); err != nil {
		t.Fatal(err)
	}
	waitMarketsWS(t, "generation 1 executable book", func() bool { return ws.ExecutableCount() == 1 })

	_ = peerByID[1].Close()
	waitMarketsWS(t, "standby promotion", func() bool {
		return ws.ConnectionReceipt().ActiveGeneration == 2 && ws.ExecutableCount() == 0
	})
	if err := peerByID[2].WriteMessage(websocket.TextMessage, book); err != nil {
		t.Fatal(err)
	}
	waitMarketsWS(t, "generation 2 executable recovery", func() bool { return ws.ExecutableCount() == 1 })
	select {
	case p := <-peers:
		peerByID[p.id] = p.conn
	case <-time.After(3 * time.Second):
		t.Fatal("replacement standby was not created")
	}
	if got := ws.ConnectionReceipt(); got.ActiveGeneration != 2 || got.PrimarySwitches != 1 {
		t.Fatalf("failover receipt=%+v", got)
	}
}

type slowReconcileProbe struct {
	active, maxActive, writes atomic.Int64
}

func (p *slowReconcileProbe) write(any) error {
	n := p.active.Add(1)
	for old := p.maxActive.Load(); n > old && !p.maxActive.CompareAndSwap(old, n); old = p.maxActive.Load() {
	}
	p.writes.Add(1)
	time.Sleep(12 * time.Millisecond)
	p.active.Add(-1)
	return nil
}

func TestMarketsWSReconcileCoalescesAndStopsBeforeStreamReturns(t *testing.T) {
	up := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				_ = conn.Close()
				return
			}
		}
	}))
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http")
	ws := (&Client{}).NewMarketsWS()
	ws.rotationEvery = 2 * time.Millisecond
	probes := make(chan *slowReconcileProbe, 2)
	var dials atomic.Int64
	ws.dialFn = func(ctx context.Context, _ []string) (*pusSession, error) {
		conn, _, err := websocket.DefaultDialer.DialContext(ctx, wsURL, nil)
		if err != nil {
			return nil, err
		}
		p := &slowReconcileProbe{}
		probes <- p
		return &pusSession{conn: conn, gen: uint64(dials.Add(1)), writeJSON: p.write}, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		ws.Stream(ctx, func() []string { return []string{"m"} })
		close(done)
	}()
	p1, p2 := <-probes, <-probes
	time.Sleep(100 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Stream did not wait for the bounded reconcile")
	}
	for i, p := range []*slowReconcileProbe{p1, p2} {
		if got := p.maxActive.Load(); got != 1 {
			t.Fatalf("session %d overlapping writes=%d", i+1, got)
		}
		before := p.writes.Load()
		time.Sleep(30 * time.Millisecond)
		if after := p.writes.Load(); after != before {
			t.Fatalf("session %d reconcile continued after Stream return: %d -> %d", i+1, before, after)
		}
		if before > 12 {
			t.Fatalf("session %d accumulated reconcile backlog: writes=%d", i+1, before)
		}
	}
}
