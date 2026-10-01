package server

// R114 historical maker-autopsy settings remain pinned as unable to bypass R166's stronger global
// book-native ML new-entry retirement. The comboprobe rolling-window miss still stores verdict
// "unmatched_window" (NOT illegal_probed)
//     so legality telemetry only counts real venue answers.

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func r114post(t *testing.T, s *Server, row map[string]any, ref float64) map[string]any {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"row": row, "ref_price": ref})
	req := httptest.NewRequest("POST", "/api/mlpost", strings.NewReader(string(body)))
	w := httptest.NewRecorder()
	s.handleMLPost(w, req)
	var out map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &out)
	return out
}

func TestR114MLMakerBandSettingsCannotBypassR166Retirement(t *testing.T) {
	s := testServer(t)
	cfg := *s.cfg()
	cfg.Auto.MakerSimBooks = true
	cfg.Auto.MakerAdverseGuard = false
	s.cfgP.Store(&cfg)
	for _, tc := range []struct {
		ticker string
		floor  float64
	}{{"R114B1", 0}, {"R114B2", -1}, {"R114B3", 80}} {
		cfgNow := *s.cfg()
		cfgNow.Auto.MLMakerMinPxC = tc.floor
		s.cfgP.Store(&cfgNow)
		out := r114post(t, s, map[string]any{"ticker": tc.ticker,
			"side": "yes", "platform": "kalshi", "contracts": 2.0, "title": "t"}, 0.60)
		if out["status"] != "reject" || out["reason"] != "book-native-ml-paper-log-only" {
			t.Fatalf("maker floor %.0f bypassed R166 retirement: %v", tc.floor, out)
		}
	}
}

func TestR114MLDepthSettingCannotBypassR166Retirement(t *testing.T) {
	s := testServer(t)
	cfg := *s.cfg()
	cfg.Auto.MakerSimBooks = true
	cfg.Auto.MakerAdverseGuard = true
	s.cfgP.Store(&cfg)
	out := r114post(t, s, map[string]any{"ticker": "R114D1", "side": "yes", "platform": "kalshi", "contracts": 2.0, "title": "t"}, 0.60)
	if out["status"] != "reject" || out["reason"] != "book-native-ml-paper-log-only" {
		t.Fatalf("depth setting bypassed R166 retirement: %v", out)
	}
}

func TestR114ProbeWindowMissVerdict(t *testing.T) {
	// A stored unmatched_window verdict must not count as illegal and must honor TTLSec.
	v := cprobeVerdict{Verdict: "unmatched_window", Reason: cprobeOutOfWindow, At: time.Now(), TTLSec: 120}
	if got := cprobeVerdictTTL(v); got != 2*time.Minute {
		t.Fatalf("TTLSec override wrong: %v", got)
	}
	v.TTLSec = 0
	if got := cprobeVerdictTTL(v); got != cprobeSoftTTL {
		t.Fatalf("unmatched_window default must be soft TTL, got %v", got)
	}
	s := testServer(t)
	s.cprobeStore(t.Context(), "r114key", v)
	s.cprobeMu.Lock()
	defer s.cprobeMu.Unlock()
	if s.cprobeIllegal != 0 || s.cprobeWindowMiss != 1 {
		t.Fatalf("window miss counted wrong: illegal=%d miss=%d", s.cprobeIllegal, s.cprobeWindowMiss)
	}
}
