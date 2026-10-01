package server

import (
	"encoding/json"
	"net/http/httptest"
	"testing"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/polymarket"
)

func auditor67KalshiMarket(t *testing.T, ticker, title, subtitle, ask, expiration string) kalshi.Market {
	t.Helper()
	var m kalshi.Market
	b, err := json.Marshal(map[string]any{
		"ticker":                   ticker,
		"title":                    title,
		"yes_sub_title":            subtitle,
		"yes_ask_dollars":          ask,
		"expected_expiration_time": expiration,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("decode Kalshi market: %v", err)
	}
	return m
}

func TestAuditor67ConsensusStructuralTelemetryExactlyOnce(t *testing.T) {
	resetMatchTelemetry()
	xvTestReset()

	const (
		family = "auditor67-consensus"
		ticker = "KXBTCD-26JUL1012-T55999.99"
		slug   = "bitcoin-above-56k-on-july-10-2026"
	)
	kKey, _, kOK := xvKalshiAnchor(ticker)
	pKey, _, pOK := xvPolyAnchor(slug, "2026-07-10T16:00:00Z")
	if !kOK || !pOK || kKey != pKey {
		t.Fatalf("test anchors disagree: kalshi=%q/%v poly=%q/%v", kKey, kOK, pKey, pOK)
	}
	xvReg.add("kalshi", ticker, kKey, "crypto")
	xvReg.add("polymarket", slug, pKey, "crypto")

	s := &Server{kal: &kalshi.Client{}}
	tradable := auditor67KalshiMarket(t, ticker, "Bitcoin above $56,000", "Yes", "0.42", "2026-07-10T16:00:00Z")
	pm := polymarket.Market{
		Slug: slug, Question: "Will Bitcoin be above $56,000?",
		EndDate: "2026-07-10T16:00:00Z", OutcomesRaw: `["Yes","No"]`,
	}

	if got, _, side, price, ok := s.consensusKalshiMatch(family, pm, "Yes", []kalshi.Market{tradable}); !ok || got != ticker || side != "YES" || price != 0.42 {
		t.Fatalf("structural hit = ticker %q side %q price %.2f ok=%v", got, side, price, ok)
	}
	// Every direct structural refusal must add one attempt and no hit: absent anchored
	// member, unprovable side mapping, and non-tradeable price.
	if _, _, _, _, ok := s.consensusKalshiMatch(family, pm, "Yes", nil); ok {
		t.Fatal("anchored member absent from the scan must refuse")
	}
	unmappable := pm
	unmappable.OutcomesRaw = `["Up","Down"]`
	if _, _, _, _, ok := s.consensusKalshiMatch(family, unmappable, "Maybe", []kalshi.Market{tradable}); ok {
		t.Fatal("unmappable structural side must refuse")
	}
	illiquid := auditor67KalshiMarket(t, ticker, "Bitcoin above $56,000", "Yes", "0.99", "2026-07-10T16:00:00Z")
	if _, _, _, _, ok := s.consensusKalshiMatch(family, pm, "Yes", []kalshi.Market{illiquid}); ok {
		t.Fatal("resolved/illiquid structural price must refuse")
	}

	matchLogMu.Lock()
	attempts, hits := matchAttempts[family], matchHits[family]
	structural, fuzzy := matchStructHits[family], matchFuzzyHits[family]
	rows := append([]map[string]string(nil), matchMissRing...)
	matchLogMu.Unlock()
	if attempts != 4 || hits != 1 || structural != 1 || fuzzy != 0 {
		t.Fatalf("counters attempts=%d hits=%d structural=%d fuzzy=%d, want 4/1/1/0", attempts, hits, structural, fuzzy)
	}
	if len(rows) != 3 || rows[0]["reason"] != "xv-twin-not-in-scan" || rows[1]["reason"] != "xv-struct-side-unmappable" || rows[2]["reason"] != "xv-struct-price-gate" {
		t.Fatalf("structural refusal receipts = %v", rows)
	}

	rr := httptest.NewRecorder()
	s.handleMatchLog(rr, httptest.NewRequest("GET", "/api/matchlog", nil))
	var payload struct {
		Families map[string]struct {
			Attempts       int64 `json:"attempts"`
			Hits           int64 `json:"hits"`
			Misses         int64 `json:"misses"`
			StructuralHits int64 `json:"structural_hits"`
			FuzzyHits      int64 `json:"fuzzy_hits"`
		} `json:"families"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode matchlog: %v", err)
	}
	f := payload.Families[family]
	if f.Attempts != 4 || f.Hits != 1 || f.Misses != 3 || f.StructuralHits != 1 || f.FuzzyHits != 0 {
		t.Fatalf("matchlog family receipt = %+v", f)
	}
}

func TestAuditor67ConsensusFuzzyTelemetryClassifiedOnce(t *testing.T) {
	resetMatchTelemetry()
	xvTestReset()

	const family = "auditor67-consensus-fuzzy"
	s := &Server{kal: &kalshi.Client{}}
	km := auditor67KalshiMarket(t, "KXMLBGAME-YANKS", "Yankees vs Red Sox Winner", "New York Yankees", "0.42", "2026-07-03T22:00:00Z")
	pm := polymarket.Market{
		Question: "Will the New York Yankees beat the Boston Red Sox?",
		EndDate:  "2026-07-03T23:00:00Z", OutcomesRaw: `["New York Yankees","Boston Red Sox"]`,
	}
	if _, _, side, _, ok := s.consensusKalshiMatch(family, pm, "New York Yankees", []kalshi.Market{km}); !ok || side != "YES" {
		t.Fatalf("fuzzy consensus match failed: side=%q ok=%v", side, ok)
	}

	matchLogMu.Lock()
	attempts, hits := matchAttempts[family], matchHits[family]
	structural, fuzzy := matchStructHits[family], matchFuzzyHits[family]
	matchLogMu.Unlock()
	if attempts != 1 || hits != 1 || structural != 0 || fuzzy != 1 {
		t.Fatalf("fuzzy counters attempts=%d hits=%d structural=%d fuzzy=%d, want 1/1/0/1", attempts, hits, structural, fuzzy)
	}
}

func TestAuditor67SlugDateStrictCalendar(t *testing.T) {
	valid := map[string]string{
		"game-2024-02-29": "2024-02-29",
		"game-2026-04-30": "2026-04-30",
		"game-2100-02-28": "2100-02-28",
	}
	for ref, want := range valid {
		got, ok := matchRefDate(ref)
		if !ok || got.Format("2006-01-02") != want {
			t.Fatalf("matchRefDate(%q) = %v/%v, want %s/true", ref, got, ok, want)
		}
	}
	for _, ref := range []string{
		"game-2026-02-29", // not a leap year
		"game-2026-02-30",
		"game-2026-04-31",
		"game-2100-02-29", // century year is not a leap year
	} {
		if got, ok := matchRefDate(ref); ok {
			t.Fatalf("impossible date %q normalized to %s", ref, got.Format("2006-01-02"))
		}
	}
}
