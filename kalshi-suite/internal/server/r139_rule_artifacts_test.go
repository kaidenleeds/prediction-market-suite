package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/polymarket"
	"github.com/kalshi-suite/kalshi-suite/internal/polymarketus"
	"github.com/kalshi-suite/kalshi-suite/internal/resolutionbasis"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestR139PolymarketGammaRuleArtifactUsesOfficialDescriptionAndResolutionSource(t *testing.T) {
	artifact, ok := researchPolymarketRuleArtifact(polymarket.Market{ConditionID: "0xrules",
		Description:      "Official edge-case and cancellation language.",
		ResolutionSource: "https://official.example/result", SportsMarketType: "moneyline"})
	if !ok || artifact.Source != "polymarket-gamma-complete-catalog" ||
		artifact.Description != "Official edge-case and cancellation language." ||
		len(artifact.SettlementSources) != 1 || artifact.SettlementSources[0].URL != "https://official.example/result" ||
		artifact.Hash == "" || artifact.VoidPolicy != "" || artifact.ScalarPolicy != "" {
		t.Fatalf("Gamma rule artifact was incomplete or prose-normalized: %+v ok=%v", artifact, ok)
	}
}

func seedR139RuleFoundation(t *testing.T, s *Server, kTicker, pSlug string) {
	t.Helper()
	event := storage.CanonicalEventSpec{EventID: "sports:rule-game", EventType: "sports-game",
		Domain: "mlb", OutcomeSetStatus: "incomplete", EvidenceJSON: `{}`}
	payoff := storage.CanonicalPayoffSpec{PayoffID: "sports:rule-game|full_game|winner|PHI",
		EventID: event.EventID, Label: "winner PHI", PredicateJSON: `{"kind":"winner","yes_team":"PHI"}`,
		PayoutCeiling: 1, IdentityStatus: "structural", EvidenceJSON: `{}`}
	instruments := []storage.CanonicalInstrumentSpec{
		{Venue: "kalshi", Ticker: kTicker, EventID: event.EventID, PayoffID: payoff.PayoffID,
			NativeSide: "YES", Orientation: "same", IdentityStatus: "structural", EvidenceJSON: `{}`},
		{Venue: "polyus", Ticker: pSlug, EventID: event.EventID, PayoffID: payoff.PayoffID,
			NativeSide: "YES", Orientation: "same", IdentityStatus: "structural", EvidenceJSON: `{}`},
	}
	if _, err := s.store.RegisterCanonicalBatch(context.Background(), []storage.CanonicalEventSpec{event},
		[]storage.CanonicalPayoffSpec{payoff}, instruments); err != nil {
		t.Fatal(err)
	}
}

func TestR139RuntimeKPUSResolutionRequiresCurrentNormalizedRuleCertificate(t *testing.T) {
	s := testServer(t)
	ctx, now := context.Background(), time.Now().UTC()
	kTicker, pSlug := "KX-RUNTIME-RULE", "pus-runtime-rule"
	seedR139RuleFoundation(t, s, kTicker, pSlug)
	candidate := xvlCandidate{pair: "K-PUS", aVenue: "kalshi", aID: kTicker,
		bVenue: "polyus", bID: pSlug, sameSide: true}
	orient := xvlOrient{aSide: "YES", bSide: "NO"}
	inputs := s.xvlResolutionInputs(ctx, candidate)
	blocked := s.xvlCertifiedResolutionCertificate(ctx, candidate, orient, inputs)
	if blocked.Compatible || resolutionbasis.Verify(blocked) == nil {
		t.Fatalf("structural identity bypassed normalized rule review: %+v", blocked)
	}
	k, _, err := s.store.InsertVenueRuleArtifact(ctx, storage.VenueRuleArtifact{Venue: "kalshi",
		InstrumentID: kTicker, Source: "kalshi exact rule REST", Primary: "Exact K rule",
		SettlementSources: []storage.RuleSettlementSource{{Name: "League", URL: "https://league.example/final"}}, Observed: now})
	if err != nil {
		t.Fatal(err)
	}
	p, _, err := s.store.InsertVenueRuleArtifact(ctx, storage.VenueRuleArtifact{Venue: "polyus",
		InstrumentID: pSlug, Source: "polyus exact description REST", Description: "Exact P rule", Observed: now})
	if err != nil {
		t.Fatal(err)
	}
	terms := storage.NormalizedRuleTerms{SettlementSourceID: "league-final",
		SettlementSourceURL: "https://league.example/final", PredicateHash: strings.Repeat("4", 64),
		RulesHash: strings.Repeat("5", 64), VoidPolicy: "refund", ScalarPolicy: "binary",
		UnknownPolicy: "refund", TimingPolicy: "final-including-overtime"}
	cert, _, err := s.store.RegisterRulePairCertificate(ctx, storage.RulePairCertificateSpec{
		PairID: "k-pus|" + kTicker + "|" + pSlug, Version: 1, KalshiTicker: kTicker, PolyUSSlug: pSlug,
		KalshiArtifactHash: k.ArtifactHash, PolyUSArtifactHash: p.ArtifactHash,
		CanonicalEventID: "sports:rule-game", CanonicalPayoffID: "sports:rule-game|full_game|winner|PHI",
		Left: terms, Right: terms, ReviewMethod: "human_review", ReviewProvenance: "independent R139 review",
		ReviewEvidenceHash: strings.Repeat("6", 64)})
	if err != nil {
		t.Fatal(err)
	}
	verified := s.xvlCertifiedResolutionCertificate(ctx, candidate, orient, inputs)
	if !verified.Compatible || resolutionbasis.Verify(verified) != nil ||
		verified.Left.BasisID != cert.BasisID || verified.Right.RulesHash != terms.RulesHash {
		t.Fatalf("current normalized rule certificate did not unlock identity gate: %+v", verified)
	}
	p.Description = "PolyUS rule changed after review"
	if _, inserted, err := s.store.InsertVenueRuleArtifact(ctx, p); err != nil || !inserted {
		t.Fatalf("raw revision inserted=%v err=%v", inserted, err)
	}
	stale := s.xvlCertifiedResolutionCertificate(ctx, candidate, orient, inputs)
	if stale.Compatible || resolutionbasis.Verify(stale) == nil {
		t.Fatalf("stale rule certificate continued to unlock identity: %+v", stale)
	}
}

func TestR139RuleCollectorCapturesExactTermsAndSurfacesMachineBlocker(t *testing.T) {
	s := testServer(t)
	kTicker, pSlug := "KX-COLLECT-RULE", "pus-collect-rule"
	if err := s.store.UpsertMarketGame(context.Background(), []storage.MarketGameRow{
		{Venue: "kalshi", Ticker: kTicker, GameID: "G-RULE", MktType: "winner", YesTeam: "PHI", NoTeam: "KC", Src: "struct"},
		{Venue: "polyus", Ticker: pSlug, GameID: "G-RULE", MktType: "winner", YesTeam: "PHI", NoTeam: "KC", Src: "struct"},
	}); err != nil {
		t.Fatal(err)
	}
	seedR139RuleFoundation(t, s, kTicker, pSlug)
	seedKalMeta(s, kalshi.Market{Ticker: kTicker, EventTicker: "EV-RULE", Status: "active",
		RulesPrimary: "Exact Kalshi winner rule.", RulesSecondary: "Exact postponement rule.",
		EarlyCloseCondition: "Winner declared."})
	s.polyUSMu.Lock()
	s.pusSweep = []polymarketus.Market{{Slug: pSlug, Description: "Exact PolyUS winner and cancellation rule.",
		SportsType: "baseball_team_full_game_winner"}}
	s.polyUSMu.Unlock()
	s.cacheResearchEventRuleSources(kalshi.EventSnapshot{EventTicker: "EV-RULE",
		SettlementSources: []struct {
			Name string `json:"name"`
			URL  string `json:"url"`
		}{{Name: "Official League", URL: "https://league.example/final"}},
		Markets: []kalshi.Market{{Ticker: kTicker}}})
	s.sweepResearchRuleArtifacts(context.Background())
	rec := httptest.NewRecorder()
	s.handleResearchRuleArtifacts(rec, httptest.NewRequest(http.MethodGet, "/api/research/rules", nil))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "MISSING_POLYUS_SETTLEMENT_SOURCE") ||
		!strings.Contains(rec.Body.String(), pSlug) || !strings.Contains(rec.Body.String(), `"live_authority":false`) {
		t.Fatalf("rule collector report=%d %s", rec.Code, rec.Body.String())
	}
	if strings.Contains(strings.ToLower(rec.Body.String()), "compatible_certified\":1") {
		t.Fatal("raw prose was auto-promoted to compatible")
	}
}

func TestR139ResearchRuleUIExplainsReviewBoundary(t *testing.T) {
	for _, want := range []string{"/api/research/rules", "Cross-venue rule review", "Point estimates and title similarity never certify rules",
		"K-PINT", "PUS-PINT", "complete three-way triangles", "needs all three current pair certificates"} {
		if !strings.Contains(dashboardHTML, want) {
			t.Fatalf("research UI missing %q", want)
		}
	}
}

func TestR139RuntimeKPINTHonorsReviewedInverseOrientation(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	event := storage.CanonicalEventSpec{EventID: "sports:runtime-kpint", EventType: "sports-game",
		Domain: "mlb", OutcomeSetStatus: "complete", EvidenceJSON: `{}`}
	payK := storage.CanonicalPayoffSpec{PayoffID: event.EventID + "|winner|PHI", EventID: event.EventID,
		Label: "PHI wins", PredicateJSON: `{"winner":"PHI"}`, PayoutCeiling: 1, IdentityStatus: "structural", EvidenceJSON: `{}`}
	payP := storage.CanonicalPayoffSpec{PayoffID: event.EventID + "|winner|KC", EventID: event.EventID,
		Label: "KC wins", PredicateJSON: `{"winner":"KC"}`, PayoutCeiling: 1, IdentityStatus: "structural", EvidenceJSON: `{}`}
	if _, err := s.store.RegisterCanonicalBatch(ctx, []storage.CanonicalEventSpec{event},
		[]storage.CanonicalPayoffSpec{payK, payP}, []storage.CanonicalInstrumentSpec{
			{Venue: "kalshi", Ticker: "K-RUNTIME-PINT", EventID: event.EventID, PayoffID: payK.PayoffID,
				NativeSide: "YES", Orientation: "same", IdentityStatus: "structural", EvidenceJSON: `{}`},
			{Venue: "polymarket", Ticker: "0xruntime-pint", EventID: event.EventID, PayoffID: payP.PayoffID,
				NativeSide: "YES", Orientation: "same", IdentityStatus: "structural", EvidenceJSON: `{}`},
		}); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	k, _, err := s.store.InsertVenueRuleArtifact(ctx, storage.VenueRuleArtifact{Venue: "kalshi",
		InstrumentID: "K-RUNTIME-PINT", Source: "Kalshi rules", Primary: "PHI rule", Observed: now})
	if err != nil {
		t.Fatal(err)
	}
	p, _, err := s.store.InsertVenueRuleArtifact(ctx, storage.VenueRuleArtifact{Venue: "polymarket",
		InstrumentID: "0xruntime-pint", Source: "Gamma", Description: "KC rule",
		SettlementSources: []storage.RuleSettlementSource{{Name: "Official", URL: "https://official.example"}}, Observed: now})
	if err != nil {
		t.Fatal(err)
	}
	terms := storage.NormalizedRuleTerms{SettlementSourceID: "official", SettlementSourceURL: "https://official.example",
		PredicateHash: strings.Repeat("7", 64), RulesHash: strings.Repeat("8", 64), VoidPolicy: "refund",
		ScalarPolicy: "binary", UnknownPolicy: "refund", TimingPolicy: "official-final"}
	cert, _, err := s.store.RegisterRulePairCertificate(ctx, storage.RulePairCertificateSpec{
		PairID: "k-pint|K-RUNTIME-PINT|0xruntime-pint", Version: 1, LeftVenue: "kalshi",
		LeftInstrumentID: "K-RUNTIME-PINT", RightVenue: "polymarket", RightInstrumentID: "0xruntime-pint",
		Orientation: "inverse", LeftArtifactHash: k.ArtifactHash, RightArtifactHash: p.ArtifactHash,
		CanonicalEventID: event.EventID, CanonicalPayoffID: payK.PayoffID, Left: terms, Right: terms,
		ReviewMethod: "human_review", ReviewProvenance: "signed inverse review",
		ReviewEvidenceHash: strings.Repeat("9", 64)})
	if err != nil {
		t.Fatal(err)
	}
	candidate := xvlCandidate{pair: "K-PINT", aVenue: "kalshi", aID: "K-RUNTIME-PINT",
		bVenue: "polymarket", bID: "0xruntime-pint", sameSide: false}
	inputs := s.xvlResolutionInputs(ctx, candidate)
	verified := s.xvlCertifiedResolutionCertificate(ctx, candidate, xvlOrient{aSide: "YES", bSide: "YES"}, inputs)
	if err := resolutionbasis.Verify(verified); err != nil || !verified.Compatible ||
		verified.Left.BasisID != cert.BasisID || verified.Right.InstrumentOrientation != "inverse" {
		t.Fatalf("reviewed inverse did not unlock exact complementary runtime pair: %+v err=%v", verified, err)
	}
	wrongOrientation := candidate
	wrongOrientation.sameSide = true
	blocked := s.xvlCertifiedResolutionCertificate(ctx, wrongOrientation, xvlOrient{aSide: "YES", bSide: "NO"}, inputs)
	if blocked.Compatible || resolutionbasis.Verify(blocked) == nil {
		t.Fatalf("candidate orientation overrode reviewed flip: %+v", blocked)
	}
}
