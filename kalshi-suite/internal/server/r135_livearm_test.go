package server

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/polymarketus"
)

func TestR135LiveArmRefusesFailedPolyUSSweepThenRetries(t *testing.T) {
	s := testServer(t)
	r149EnableLiveDestinations(s, true, true)
	venue := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"orders":[],"market_positions":[],"cursor":"","balance":10000}`))
	}))
	defer venue.Close()
	key, err := rsa.GenerateKey(rand.Reader, 1024)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := kalshi.NewSigner("test-key", pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}))
	if err != nil {
		t.Fatal(err)
	}
	s.kal = kalshi.NewClient(venue.URL, signer, 1000, time.Second)

	calls := 0
	sweepSawArmed := false
	s.pusArmSweep = func(context.Context, []string) ([]string, error) {
		calls++
		s.liveMu.Lock()
		serverArmed := s.liveArmed
		s.liveMu.Unlock()
		if serverArmed || s.kal.Armed() {
			sweepSawArmed = true
		}
		if calls == 1 {
			return nil, errors.New("forced PolyUS cancel outage")
		}
		return []string{"orphan-1"}, nil
	}
	s.pusArmOpenOrders = func(context.Context) ([]polymarketus.PUSOrder, error) { return nil, nil }
	s.pusArmPositions = func(context.Context) ([]polymarketus.PUSPosition, error) {
		return []polymarketus.PUSPosition{{Slug: "open-position", Net: 2, Cost: 0.8}}, nil
	}
	s.liveBankCashRead = func(_ context.Context, venue string) (float64, bool) {
		if venue == "kalshi" || venue == "polyus" {
			return 100, true
		}
		return 0, false
	}
	// ARM now requires fresh per-venue daily-loss truth before opening either write gate. Keep this
	// orphan-sweep test hermetic with authenticated-empty receipt seams; loss-ledger failures have
	// dedicated refusal tests.
	s.liveLossKalshiRead = func(context.Context) ([]kalshi.MarketPosition, error) { return nil, nil }
	s.liveLossKalshiSettlementRead = func(context.Context) ([]kalshi.Settlement, error) { return nil, nil }
	s.liveLossPolyUSRead = func(context.Context) ([]polymarketus.PUSActivity, error) { return nil, nil }

	arm := func() *httptest.ResponseRecorder {
		t.Helper()
		r := httptest.NewRequest(http.MethodPost, "/api/live/arm", strings.NewReader(`{"confirm":true}`))
		w := httptest.NewRecorder()
		s.handleLiveArm(w, r)
		return w
	}

	first := arm()
	if first.Code != http.StatusBadGateway || !strings.Contains(first.Body.String(), "arm refused") {
		t.Fatalf("failed sweep response=%d %s", first.Code, first.Body.String())
	}
	s.liveMu.Lock()
	armed := s.liveArmed
	s.liveMu.Unlock()
	if armed || s.kal.Armed() {
		t.Fatalf("failed PolyUS sweep left a write gate armed: server=%v kalshi=%v", armed, s.kal.Armed())
	}
	rows, err := s.store.ListAudit(context.Background(), 20)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, row := range rows {
		if row.Level == "error" && row.Category == "live" && strings.Contains(row.Message, "PolyUS pre-session orphan sweep failed") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("failed arm sweep did not persist an error audit: %+v", rows)
	}

	second := arm()
	if second.Code != http.StatusOK {
		t.Fatalf("later successful retry response=%d %s", second.Code, second.Body.String())
	}
	s.liveMu.Lock()
	armed = s.liveArmed
	s.liveMu.Unlock()
	if calls != 2 || !armed || !s.kal.Armed() {
		t.Fatalf("successful retry calls=%d server_armed=%v kalshi_armed=%v", calls, armed, s.kal.Armed())
	}
	if sweepSawArmed {
		t.Fatal("ARM opened a write gate before clean-slate and NAV verification completed")
	}
	s.pusLiveMu.Lock()
	seededPositions, seededAt := append([]polymarketus.PUSPosition(nil), s.pusPosC...), s.pusLiveAt
	s.pusLiveMu.Unlock()
	if len(seededPositions) != 1 || seededPositions[0].Slug != "open-position" || seededAt.IsZero() {
		t.Fatalf("ARM did not seed its fresh authenticated position receipt: rows=%+v at=%v", seededPositions, seededAt)
	}
	third := arm()
	if third.Code != http.StatusOK || !strings.Contains(third.Body.String(), "already_armed") {
		t.Fatalf("repeated ARM response=%d %s", third.Code, third.Body.String())
	}
	if calls != 2 {
		t.Fatalf("repeated ARM swept current-session orders: sweep calls=%d, want 2", calls)
	}
}

func TestR147LiveArmRefusesUnreadableAuthenticatedNAV(t *testing.T) {
	s := testServer(t)
	r149EnableLiveDestinations(s, true, false)
	venue := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"orders":[],"market_positions":[],"cursor":"","balance":10000}`))
	}))
	defer venue.Close()
	key, err := rsa.GenerateKey(rand.Reader, 1024)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := kalshi.NewSigner("test-key", pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}))
	if err != nil {
		t.Fatal(err)
	}
	s.kal = kalshi.NewClient(venue.URL, signer, 1000, time.Second)
	// The clean-slate succeeds, but the post-sweep authenticated balance read does not.
	s.liveBankCashRead = func(context.Context, string) (float64, bool) { return 0, false }

	r := httptest.NewRequest(http.MethodPost, "/api/live/arm", strings.NewReader(`{"confirm":true}`))
	w := httptest.NewRecorder()
	s.handleLiveArm(w, r)
	if w.Code != http.StatusServiceUnavailable || !strings.Contains(w.Body.String(), "bankroll/NAV is unreadable") {
		t.Fatalf("unreadable NAV response=%d %s", w.Code, w.Body.String())
	}
	s.liveMu.Lock()
	armed, autoOn := s.liveArmed, s.liveAuto
	s.liveMu.Unlock()
	if armed || autoOn || s.kal.Armed() {
		t.Fatalf("unreadable NAV left write authority: server=%v auto=%v client=%v", armed, autoOn, s.kal.Armed())
	}

	// A configured authenticated PolyUS account is equally mandatory. Kalshi truth is now valid,
	// but a missing PolyUS balance receipt must still keep both venue gates off once PolyUS is an
	// explicitly authorized destination.
	r149EnableLiveDestinations(s, true, true)
	s.pusPriv = &polymarketus.PrivateWS{}
	s.pusArmSweep = func(context.Context, []string) ([]string, error) { return nil, nil }
	s.pusArmOpenOrders = func(context.Context) ([]polymarketus.PUSOrder, error) { return nil, nil }
	s.pusArmPositions = func(context.Context) ([]polymarketus.PUSPosition, error) { return nil, nil }
	s.liveBankCashRead = func(_ context.Context, venue string) (float64, bool) {
		if venue == "kalshi" {
			return 100, true
		}
		return 0, false
	}
	r = httptest.NewRequest(http.MethodPost, "/api/live/arm", strings.NewReader(`{"confirm":true}`))
	w = httptest.NewRecorder()
	s.handleLiveArm(w, r)
	if w.Code != http.StatusServiceUnavailable || !strings.Contains(w.Body.String(), "PolyUS authenticated bankroll/NAV is unreadable") {
		t.Fatalf("unreadable PolyUS NAV response=%d %s", w.Code, w.Body.String())
	}
	s.liveMu.Lock()
	armed, autoOn = s.liveArmed, s.liveAuto
	s.liveMu.Unlock()
	if armed || autoOn || s.kal.Armed() {
		t.Fatalf("unreadable PolyUS NAV left write authority: server=%v auto=%v client=%v", armed, autoOn, s.kal.Armed())
	}
}

func TestR147LiveArmRefusesUnreadableFreshPolyUSPositions(t *testing.T) {
	s := testServer(t)
	r149EnableLiveDestinations(s, true, true)
	venue := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"orders":[],"market_positions":[],"cursor":"","balance":10000}`))
	}))
	defer venue.Close()
	key, err := rsa.GenerateKey(rand.Reader, 1024)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := kalshi.NewSigner("test-key", pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}))
	if err != nil {
		t.Fatal(err)
	}
	s.kal = kalshi.NewClient(venue.URL, signer, 1000, time.Second)
	s.pusArmSweep = func(context.Context, []string) ([]string, error) { return nil, nil }
	s.pusArmOpenOrders = func(context.Context) ([]polymarketus.PUSOrder, error) { return nil, nil }
	positionReads := 0
	s.pusArmPositions = func(context.Context) ([]polymarketus.PUSPosition, error) {
		positionReads++
		return nil, errors.New("forced positions outage")
	}

	r := httptest.NewRequest(http.MethodPost, "/api/live/arm", strings.NewReader(`{"confirm":true}`))
	w := httptest.NewRecorder()
	s.handleLiveArm(w, r)
	if w.Code != http.StatusBadGateway || !strings.Contains(w.Body.String(), "position verification unreadable") {
		t.Fatalf("unreadable positions response=%d %s", w.Code, w.Body.String())
	}
	if positionReads != 3 {
		t.Fatalf("position verification attempts=%d, want 3", positionReads)
	}
	s.liveMu.Lock()
	armed, autoOn := s.liveArmed, s.liveAuto
	s.liveMu.Unlock()
	if armed || autoOn || s.kal.Armed() {
		t.Fatalf("unreadable positions left write authority: server=%v auto=%v client=%v", armed, autoOn, s.kal.Armed())
	}
}
