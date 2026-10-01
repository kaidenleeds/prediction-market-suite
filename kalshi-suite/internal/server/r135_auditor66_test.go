package server

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
)

func TestR135Auditor66KalshiLiveLabelIsStartClockFirst(t *testing.T) {
	s := testServer(t)
	ticker := "KXLIVE-CLOCK"
	setActivity := func(at time.Time) {
		s.metaMu.Lock()
		if s.kmktsAt == nil {
			s.kmktsAt = map[string]time.Time{}
		}
		s.kmktsAt[ticker] = at
		s.metaMu.Unlock()
	}
	setActivity(time.Now())
	future := int64(3600)
	if got := s.sigIsLiveProspective("kalshi", ticker, &future); got != 0 {
		t.Fatalf("known future start + fresh tape = %d, want PRE(0)", got)
	}
	if got := s.sigLiveFutureOverrideN.Load(); got != 1 {
		t.Fatalf("future-start contradiction counter=%d want 1", got)
	}
	et := etLocation()
	futureTicker := "KXCS2GAME-" + strings.ToUpper(time.Now().In(et).Add(2*time.Hour).Format("06Jan021504")) + "ENTSAW-SAW"
	s.metaMu.Lock()
	s.kmktsAt[futureTicker] = time.Now()
	s.metaMu.Unlock()
	if got := s.sigIsLive("kalshi", futureTicker); got != 0 {
		t.Fatalf("ticker-derived future start + fresh activity = %d, want PRE(0)", got)
	}
	past := int64(-10)
	if got := s.sigIsLiveWithStart("kalshi", ticker, &past); got != 1 {
		t.Fatalf("started + fresh tape = %d, want LIVE(1)", got)
	}
	setActivity(time.Now().Add(-2 * time.Minute))
	if got := s.sigIsLiveWithStart("kalshi", ticker, &past); got != -1 {
		t.Fatalf("started + stale tape = %d, want UNKNOWN(-1)", got)
	}
	setActivity(time.Now())
	if got := s.sigIsLiveWithStart("kalshi", ticker, nil); got != -1 {
		t.Fatalf("unknown start + fresh tape = %d, want UNKNOWN(-1)", got)
	}

	// The public status receipt versions the prospective cohort and exposes contradictions.
	s.kal = kalshi.NewClient("http://127.0.0.1:1", nil, 1000, time.Millisecond)
	req := httptest.NewRequest(http.MethodGet, "/api/status", nil)
	rr := httptest.NewRecorder()
	s.handleStatus(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	var status map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	if status["signal_live_label_version"] != sigLiveLabelVersion || status["signal_live_future_start_overrides"] != float64(1) {
		t.Fatalf("status lost prospective label receipt: %+v", status)
	}
}

func TestR135Auditor66PaperResetCorruptMLBookReturnsFailure(t *testing.T) {
	s := testServer(t)
	path := filepath.Join(s.cfg().DataDir, "ml_paper.json")
	if err := os.WriteFile(path, []byte(`{"open":`), 0o644); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/paper/reset", nil)
	rr := httptest.NewRecorder()
	s.handlePaperReset(rr, req)
	if rr.Code < 400 {
		t.Fatalf("corrupt book returned success: status=%d body=%s", rr.Code, rr.Body.String())
	}
	var got map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got["ok"] != false || got["error"] != "ml_book_unreadable" {
		t.Fatalf("corrupt reset receipt=%+v", got)
	}
}

func TestR135Auditor66BootRetryNeverTouchesRetiredShadow(t *testing.T) {
	s := testServer(t)
	cfg := *s.cfg()
	cfg.Auto.ResetOnStart = true
	cfg.Auto.ShadowBookRetired = true
	cfg.Auto.KflowBooksFrozen = true
	s.cfgP.Store(&cfg)
	writeBookJSON(t, cfg.DataDir, "ml_shadow.json", map[string]any{
		"bank0":    800.0,
		"open":     []any{map[string]any{"ticker": "SHADOW-OPEN", "side": "YES", "price": .4, "contracts": 1.0}},
		"closed":   []any{},
		"lifetime": map[string]any{"net": 7.5, "closed": 2.0, "wins": 1.0, "net_base": 0.0, "bank_ver": 3.0},
	})

	oldRetries, oldResetSleep, oldBootSleep := resetLockRetries, resetLockRetrySleep, bootResetRetrySleep
	oldWait, oldStale := bookLockWait, bookLockStale
	resetLockRetries, resetLockRetrySleep, bootResetRetrySleep = 0, time.Millisecond, 100*time.Millisecond
	bookLockWait, bookLockStale = time.Millisecond, 10*time.Minute
	defer func() {
		resetLockRetries, resetLockRetrySleep, bootResetRetrySleep = oldRetries, oldResetSleep, oldBootSleep
		bookLockWait, bookLockStale = oldWait, oldStale
	}()
	release, ok := acquireBookLock(cfg.DataDir)
	if !ok {
		t.Fatal("could not take test book lock")
	}
	go func() {
		time.Sleep(70 * time.Millisecond) // initial lock attempt times out; free during Boot's outer sleep
		release()
	}()
	s.BootResetPnL(context.Background())

	raw, err := os.ReadFile(filepath.Join(cfg.DataDir, "ml_shadow.json"))
	if err != nil {
		t.Fatal(err)
	}
	var book map[string]any
	if err := json.Unmarshal(raw, &book); err != nil {
		t.Fatal(err)
	}
	if opens, _ := book["open"].([]any); len(opens) != 1 {
		t.Fatalf("boot retry closed retired shadow lots: %+v", book)
	}
	life, _ := book["lifetime"].(map[string]any)
	if life["net_base"] != float64(0) {
		t.Fatalf("boot retry rebased retired shadow: %+v", life)
	}
}

func TestR135Auditor66MalformedNewestTradeDoesNotBlankRawFlow(t *testing.T) {
	now := time.Now().UTC()
	raw := `[
		{"ticker":"FLOW","count_fp":"100","yes_price_dollars":"0.90","taker_outcome_side":"no","created_time":"malformed"},
		{"ticker":"FLOW","count_fp":"0.001","yes_price_dollars":"0.40","taker_outcome_side":"yes","created_time":"` + now.Add(-time.Minute).Format(time.RFC3339) + `"},
		{"ticker":"FLOW","count_fp":"100","yes_price_dollars":"0.10","taker_outcome_side":"no","created_time":"` + now.Add(-16*time.Minute).Format(time.RFC3339) + `"}
	]`
	var tape []kalshi.Trade
	if err := json.Unmarshal([]byte(raw), &tape); err != nil {
		t.Fatal(err)
	}
	yes, no, n := kalshiFlow15mFromTape(tape, "FLOW", now)
	if n != 1 || no != 0 || math.Abs(yes-.0004) > 1e-12 {
		t.Fatalf("malformed timestamp stopped valid tiny raw flow: yes=%v no=%v n=%d", yes, no, n)
	}
	if strings.TrimSpace(tape[0].CreatedTime) != "malformed" {
		t.Fatal("test fixture lost malformed-newest ordering")
	}
}
