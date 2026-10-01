package server

import (
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/kalshi-suite/kalshi-suite/internal/config"
)

func TestLiveCryptoCapDefaultsSettingsAndRails(t *testing.T) {
	if got := config.Default().Risk.LiveCryptoCapPct; math.Abs(got-0.10) > 1e-12 {
		t.Fatalf("default live crypto exposure cap=%v want 0.10", got)
	}

	s := testServer(t)
	rec := httptest.NewRecorder()
	s.handleSettings(rec, httptest.NewRequest(http.MethodPost, "/api/settings",
		strings.NewReader(`{"live_crypto_cap_pct":0.75}`)))
	if rec.Code != http.StatusOK || math.Abs(s.cfg().Risk.LiveCryptoCapPct-0.75) > 1e-12 {
		t.Fatalf("crypto cap POST code=%d stored=%v body=%s", rec.Code, s.cfg().Risk.LiveCryptoCapPct, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	s.handleSettings(rec, httptest.NewRequest(http.MethodGet, "/api/settings", nil))
	var got map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil || math.Abs(got["live_crypto_cap_pct"].(float64)-0.75) > 1e-12 {
		t.Fatalf("crypto cap GET=%v err=%v", got, err)
	}

	s.liveMu.Lock()
	s.liveBankArm = map[string]float64{"kalshi": 100, "polyus": 100}
	s.liveMu.Unlock()
	s.mutateCfg(func(c *config.Config) {
		c.Risk.LiveProspectiveAllocation = true
		c.Risk.LiveSystemKalshi = true
		c.Risk.LiveSystemPolyUS = true
	})
	rails := s.liveRailsView()
	if pct, _ := rails["crypto_pct"].(float64); math.Abs(pct-0.75) > 1e-12 {
		t.Fatalf("crypto rail telemetry pct=%v want .75", rails["crypto_pct"])
	}
	// The requested 75% crypto sleeve is still tightened by the 20% total-exposure wall.
	if cap, _ := rails["crypto_cap"].(float64); math.Abs(cap-40) > 1e-12 {
		t.Fatalf("crypto rail telemetry cap=%v want 40", rails["crypto_cap"])
	}
}

func TestXMatchLiveIdentitiesStayExactAndBoundedWhenSelected(t *testing.T) {
	allow := "kalshi|kalshi-flow|YES|taker,kalshi|spotlag|YES|taker,kalshi|spotlag|NO|taker," +
		"kalshi|xmatch|YES|taker,kalshi|xmatch|NO|taker"
	rows, canonical, why := parseLiveSystemAllowlist(allow)
	if why != "" || len(rows) != 5 || len(canonical) != 5 {
		t.Fatalf("five exact LIVE identities rejected: rows=%d canonical=%v why=%q", len(rows), canonical, why)
	}
	for _, id := range []string{"kalshi|xmatch|YES|taker", "kalshi|xmatch|NO|taker"} {
		if _, ok := rows[id]; !ok {
			t.Fatalf("missing exact xmatch identity %q in %v", id, rows)
		}
	}
}
