package server

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/polymarketus"
	"github.com/kalshi-suite/kalshi-suite/internal/resolutionbasis"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func r139ResolutionInput(venue, ticker, orientation string) storage.ResolutionBasisInput {
	return storage.ResolutionBasisInput{Venue: venue, Ticker: ticker, EventID: "event-1",
		PayoffID: "payoff-1", NativeSide: "YES", Orientation: orientation,
		SettlementSource: "official-box-score", RulesHash: "rules-1", CrossVenueBasis: "basis-1",
		VoidPolicy: "refund-0.5", ScalarPolicy: "binary-or-refund", UnknownPolicy: "refund-0.5",
		IdentityStatus: "verified", InstrumentVersion: 1, EventVersion: 1, PayoffVersion: 1}
}

func r139VerifiedLockCertificate(pair, aVenue, aID, bVenue, bID string, sameSide bool, aSide, bSide string) *resolutionbasis.Certificate {
	rightOrientation := "same"
	if !sameSide {
		rightOrientation = "inverse"
	}
	candidate := xvlCandidate{pair: pair, aVenue: aVenue, aID: aID, bVenue: bVenue, bID: bID, sameSide: sameSide}
	orient := xvlOrient{aSide: aSide, bSide: bSide}
	inputs := map[string]storage.ResolutionBasisInput{
		storage.ResolutionBasisKey(aVenue, aID): r139ResolutionInput(aVenue, aID, "same"),
		storage.ResolutionBasisKey(bVenue, bID): r139ResolutionInput(bVenue, bID, rightOrientation),
	}
	certificate := xvlResolutionCertificate(candidate, orient, inputs)
	return &certificate
}

func seedR139ResolutionBasis(t *testing.T, s *Server, kalshiTicker, polyUSSlug string) {
	t.Helper()
	event := storage.CanonicalEventSpec{EventID: "event-xvgap", EventType: "sports-game", Domain: "mlb",
		SettlementSource: "official-box-score", VoidPolicy: "refund-0.5", OutcomeSetStatus: "complete",
		EvidenceJSON: `{"scalar_policy":"binary-or-refund","unknown_policy":"refund-0.5"}`}
	payoff := storage.CanonicalPayoffSpec{PayoffID: "payoff-xvgap", EventID: event.EventID,
		Label: "Philadelphia wins", PredicateJSON: `{"winner":"PHI"}`, PayoutCeiling: 1,
		SettlementSource: "official-box-score", IdentityStatus: "verified",
		EvidenceJSON: `{"scalar_policy":"binary-or-refund","unknown_policy":"refund-0.5"}`}
	evidence := `{"void_policy":"refund-0.5","scalar_policy":"binary-or-refund","unknown_policy":"refund-0.5"}`
	instruments := []storage.CanonicalInstrumentSpec{
		{Venue: "kalshi", Ticker: kalshiTicker, EventID: event.EventID, PayoffID: payoff.PayoffID,
			NativeSide: "YES", Orientation: "same", SettlementSource: "official-box-score",
			RulesHash: "rules-xvgap", CrossVenueBasis: "basis-xvgap", IdentityStatus: "verified", EvidenceJSON: evidence},
		{Venue: "polyus", Ticker: polyUSSlug, EventID: event.EventID, PayoffID: payoff.PayoffID,
			NativeSide: "YES", Orientation: "same", SettlementSource: "official-box-score",
			RulesHash: "rules-xvgap", CrossVenueBasis: "basis-xvgap", IdentityStatus: "verified", EvidenceJSON: evidence},
	}
	if _, err := s.store.RegisterCanonicalBatch(context.Background(), []storage.CanonicalEventSpec{event},
		[]storage.CanonicalPayoffSpec{payoff}, instruments); err != nil {
		t.Fatal(err)
	}
	source := []storage.RuleSettlementSource{{Name: "Official League", URL: "https://league.example/results"}}
	kArtifact, _, err := s.store.InsertVenueRuleArtifact(context.Background(), storage.VenueRuleArtifact{
		Venue: "kalshi", InstrumentID: kalshiTicker, Source: "test-kalshi-rules",
		Primary: "If Philadelphia wins, Yes.", SettlementSources: source,
	})
	if err != nil {
		t.Fatal(err)
	}
	pArtifact, _, err := s.store.InsertVenueRuleArtifact(context.Background(), storage.VenueRuleArtifact{
		Venue: "polyus", InstrumentID: polyUSSlug, Source: "test-polyus-rules",
		Description: "Official winner including overtime.", SportsType: "baseball_team_full_game_winner",
		SettlementSources: source,
	})
	if err != nil {
		t.Fatal(err)
	}
	terms := storage.NormalizedRuleTerms{SettlementSourceID: "official-league",
		SettlementSourceURL: "https://league.example/results", PredicateHash: strings.Repeat("a", 64),
		RulesHash: strings.Repeat("b", 64), VoidPolicy: "refund-0.5", ScalarPolicy: "binary-or-refund",
		UnknownPolicy: "refund-0.5", TimingPolicy: "full-game-including-overtime"}
	if _, _, err := s.store.RegisterRulePairCertificate(context.Background(), storage.RulePairCertificateSpec{
		PairID: "k-pus|" + kalshiTicker + "|" + polyUSSlug, KalshiTicker: kalshiTicker, PolyUSSlug: polyUSSlug,
		KalshiArtifactHash: kArtifact.ArtifactHash, PolyUSArtifactHash: pArtifact.ArtifactHash,
		CanonicalEventID: event.EventID, CanonicalPayoffID: payoff.PayoffID, Left: terms, Right: terms,
		ReviewMethod: "human_review", ReviewProvenance: "hermetic test fixture",
		ReviewEvidenceHash: strings.Repeat("c", 64), Version: 1,
	}); err != nil {
		t.Fatal(err)
	}
	series := strings.ToUpper(kalshiTicker)
	if i := strings.Index(series, "-"); i > 0 {
		series = series[:i]
	}
	s.kalFeeMu.Lock()
	if s.kalFees == nil {
		s.kalFees = map[string]kalFeeInfo{}
	}
	s.kalFees[series] = kalFeeInfo{taker: .07, maker: .0175, typ: "quadratic_with_maker_fees", multiplier: 1}
	s.kalFeesAt = time.Now()
	s.kalFeeMu.Unlock()
	theta := .03
	s.polyUSMu.Lock()
	s.pusSweep = append(s.pusSweep, polymarketus.Market{Slug: polyUSSlug, FeeCoeff: &theta, TickSize: .01})
	s.pusSweepAt = time.Now()
	s.polyUSMu.Unlock()
}

func TestR139XVGapRefusesVisiblePriceWithoutExecutableBook(t *testing.T) {
	s := testServer(t)
	seedKalMeta(s, kalshi.Market{Ticker: "KXMLBGAME-26JUL06PHIKC-PHI", Status: "active", LastPrice: 0.60})
	seedXVWinnerTwin(s, "atc-mlb-phi-kc-2026-07-06-phi", "KXMLBGAME-26JUL06PHIKC-PHI", "PHI")
	rows := s.xvGapCandidates(context.Background(), []polyUSMarket{{
		Slug: "atc-mlb-phi-kc-2026-07-06-phi", Game: "Phillies at Royals", Yes: 0.50,
		Bid: 0.49, Ask: 0.51, BidSz: 10, AskSz: 10,
	}})
	if len(rows) != 0 {
		t.Fatalf("last trade / visible percentage manufactured cross-venue rows without a Kalshi BBO: %+v", rows)
	}
}

func TestR139XVGapFuzzyIdentityCannotEnterStrategyLedger(t *testing.T) {
	s := testServer(t)
	seedKalBook(s, kalshi.Market{Ticker: "KXMLBGAME-26JUL06PHIKC-PHI", Status: "active",
		YesBid: 0.58, YesAsk: 0.62, LastPrice: 0.60})
	rows := s.xvGapCandidates(context.Background(), []polyUSMarket{{
		Slug: "atc-mlb-phi-kc-2026-07-06-phi", Game: "Phillies at Royals", Team: "PHI", Yes: 0.50,
		Bid: 0.48, Ask: 0.52, BidSz: 10, AskSz: 10,
	}})
	if len(rows) != 0 {
		t.Fatalf("regex/title-only pair contaminated the strategy ledger: %+v", rows)
	}
}

func TestR139XVGapRefusesFrozenBookOnInactiveMarket(t *testing.T) {
	s := testServer(t)
	seedKalBook(s, kalshi.Market{Ticker: "KXMLBGAME-26JUL06PHIKC-PHI", Status: "closed",
		YesBid: 0.58, YesAsk: 0.62, LastPrice: 0.60})
	seedXVWinnerTwin(s, "atc-mlb-phi-kc-2026-07-06-phi", "KXMLBGAME-26JUL06PHIKC-PHI", "PHI")
	rows := s.xvGapCandidates(context.Background(), []polyUSMarket{{
		Slug: "atc-mlb-phi-kc-2026-07-06-phi", Game: "Phillies at Royals", Yes: 0.50,
		Bid: 0.48, Ask: 0.52, BidSz: 10, AskSz: 10,
	}})
	if len(rows) != 0 {
		t.Fatalf("inactive market's frozen book manufactured cross-venue rows: %+v", rows)
	}
}

func TestR139XVGapPricesBothExpressionsAtSideSpecificBookAsks(t *testing.T) {
	s := testServer(t)
	ticker, slug := "KXMLBGAME-26JUL06PHIKC-PHI", "atc-mlb-phi-kc-2026-07-06-phi"
	seedKalBook(s, kalshi.Market{Ticker: ticker, Status: "active",
		Title: "Philadelphia at Kansas City", YesBid: 0.58, YesAsk: 0.62, LastPrice: 0.60})
	seedXVWinnerTwin(s, slug, ticker, "PHI")
	seedR139ResolutionBasis(t, s, ticker, slug)
	s.polyUSMu.Lock()
	s.polyUSAt = time.Now()
	s.polyUSMu.Unlock()
	rows := s.xvGapCandidates(context.Background(), []polyUSMarket{{
		Slug: slug, Game: "Philadelphia at Kansas City", Yes: 0.50,
		Bid: 0.48, Ask: 0.52, BidSz: 12, AskSz: 15,
	}})
	if len(rows) != 2 {
		t.Fatalf("book-backed matched pair rows=%d, want 2: %+v", len(rows), rows)
	}
	if got := rows[0]; got.Platform != "polyus" || got.Side != "YES" || math.Abs(got.EntryPrice-0.52) > 1e-9 ||
		!strings.Contains(got.ExecExpr, ":book=polyus-snapshot-book") || !strings.Contains(got.ExecExpr, ":resolution=") {
		t.Fatalf("PolyUS row did not preserve executable YES ask/source: %+v", got)
	}
	if got := rows[1]; got.Platform != "kalshi" || got.Side != "NO" || math.Abs(got.EntryPrice-0.42) > 1e-9 ||
		!strings.Contains(got.ExecExpr, ":book=kalshi-ticker-ws") || !strings.Contains(got.ExecExpr, ":resolution=") {
		t.Fatalf("Kalshi row did not preserve executable NO ask/source: %+v", got)
	}
}

func TestR139XVGapRefusesStalePolyUSSnapshotFallback(t *testing.T) {
	s := testServer(t)
	ticker, slug := "KXMLBGAME-26JUL06PHIKC-PHI", "atc-mlb-phi-kc-2026-07-06-phi"
	seedKalBook(s, kalshi.Market{Ticker: ticker, Status: "active",
		Title: "Philadelphia at Kansas City", YesBid: 0.58, YesAsk: 0.62, LastPrice: 0.60})
	seedXVWinnerTwin(s, slug, ticker, "PHI")
	seedR139ResolutionBasis(t, s, ticker, slug)
	s.polyUSMu.Lock()
	s.polyUSAt = time.Now().Add(-3 * time.Minute)
	s.polyUSMu.Unlock()
	rows := s.xvGapCandidates(context.Background(), []polyUSMarket{{
		Slug: slug, Game: "Philadelphia at Kansas City", Yes: 0.50,
		Bid: 0.48, Ask: 0.52, BidSz: 12, AskSz: 15,
	}})
	if len(rows) != 0 {
		t.Fatalf("stale retained PolyUS snapshot manufactured cross-venue rows: %+v", rows)
	}
}

func TestR139XVLockUsesFreshExecutableBBOInsteadOfRESTSnapshotPrice(t *testing.T) {
	s := testServer(t)
	ticker := "KXMLBGAME-26JUL06PHIKC-PHI"
	seedKalBook(s, kalshi.Market{Ticker: ticker, Status: "active", Title: "Philadelphia at Kansas City",
		YesBid: 0.40, YesAsk: 0.44, LastPrice: 0.42})

	// Drift the slower market metadata quote after seeding the executable ticker BBO. The lock
	// scanner must still use 0.40/0.44, never the later 0.10/0.90 REST snapshot values.
	s.metaMu.Lock()
	km := s.kmkts[ticker]
	km.YesBid, km.YesAsk = 0.10, 0.90
	s.kmkts[ticker] = km
	s.metaMu.Unlock()
	bid, ask, ok := s.xvlKalshiQuote(ticker)
	if !ok || math.Abs(bid-0.40) > 1e-9 || math.Abs(ask-0.44) > 1e-9 {
		t.Fatalf("lock quote=%v/%v ok=%v, want executable BBO 0.40/0.44", bid, ask, ok)
	}

	s.metaMu.Lock()
	s.kmktsAt[ticker] = time.Now().Add(-3 * time.Minute)
	s.metaMu.Unlock()
	if bid, ask, ok = s.xvlKalshiQuote(ticker); ok || bid != 0 || ask != 0 {
		t.Fatalf("stale lifecycle metadata must fail closed: %v/%v ok=%v", bid, ask, ok)
	}
}

func TestR139XVLockRejectsStalledPolyUSSnapshot(t *testing.T) {
	s := testServer(t)
	m := polyUSMarket{Slug: "atc-mlb-phi-kc-2026-07-06-phi", Bid: 0.48, Ask: 0.52, BidSz: 7, AskSz: 9}
	if _, _, _, _, ok := s.xvlPolyUSQuote(m); ok {
		t.Fatal("unstamped PolyUS snapshot must not become lock evidence")
	}
	s.polyUSMu.Lock()
	s.polyUSAt = time.Now()
	s.polyUSMu.Unlock()
	bid, ask, bidDepth, askDepth, ok := s.xvlPolyUSQuote(m)
	if !ok || bid != 0.48 || ask != 0.52 || bidDepth != 7 || askDepth != 9 {
		t.Fatalf("fresh PolyUS snapshot quote=%v/%v depth=%v/%v ok=%v", bid, ask, bidDepth, askDepth, ok)
	}
	s.polyUSMu.Lock()
	s.polyUSAt = time.Now().Add(-3 * time.Minute)
	s.polyUSMu.Unlock()
	if _, _, _, _, ok = s.xvlPolyUSQuote(m); ok {
		t.Fatal("stale PolyUS snapshot must fail closed")
	}
}

func TestR139XVLockPreservesPartialSettlementPayouts(t *testing.T) {
	for _, tc := range []struct {
		yesValue float64
		side     string
		want     float64
	}{
		{1, "YES", 1}, {1, "NO", 0}, {0, "YES", 0}, {0, "NO", 1},
		{0.5, "YES", 0.5}, {0.5, "NO", 0.5}, {0.1, "YES", 0.1}, {0.1, "NO", 0.9},
	} {
		got, ok := xvlSidePayout(tc.yesValue, tc.side)
		if !ok || math.Abs(got-tc.want) > 1e-9 {
			t.Fatalf("yes=%v side=%s payout=%v ok=%v, want %v", tc.yesValue, tc.side, got, ok, tc.want)
		}
	}
	if _, ok := xvlSidePayout(0.5, "DRAW"); ok {
		t.Fatal("non-canonical side must not become settlement evidence")
	}
	if math.Abs((0.5+0.5)-1) > 1e-9 {
		t.Fatal("two void/refund legs must remain a complete $1 payoff")
	}
}

func TestR139XVLockFriendlyNamesRemainDisplayOnly(t *testing.T) {
	s := testServer(t)
	ticker := "KXMLBGAME-26JUL06PHIKC-PHI"
	seedKalBook(s, kalshi.Market{Ticker: ticker, Status: "active", Title: "Philadelphia at Kansas City",
		YesSubTitle: "Philadelphia", YesBid: 0.40, YesAsk: 0.44, LastPrice: 0.42})
	name := s.xvlFriendlyName("kalshi", ticker)
	if name == "" || name == ticker || !strings.Contains(strings.ToLower(name), "philadelphia") {
		t.Fatalf("friendly name=%q, want readable title distinct from raw identity", name)
	}
	label := xvlFriendlyLeg(name, ticker)
	if !strings.Contains(label, name) || !strings.Contains(label, ticker) {
		t.Fatalf("audit label must preserve friendly and raw identity: %q", label)
	}
}

func TestR139ResearchRuleArtifactsRetainRawTermsWithoutInventingEquivalence(t *testing.T) {
	s := testServer(t)
	ticker, slug := "KX-RULE", "pus-rule"
	seedKalMeta(s, kalshi.Market{Ticker: ticker, Status: "active", RulesPrimary: "If Team A wins, Yes.",
		EarlyCloseCondition: "Close after a winner is declared."})
	theta := .03
	s.polyUSMu.Lock()
	s.pusSweep = []polymarketus.Market{{Slug: slug, SportsType: "basketball_team_full_game_winner",
		Description: "Official winner; canceled games settle to the last fair market price.", FeeCoeff: &theta}}
	s.polyUSMu.Unlock()
	artifacts := s.researchRuleArtifacts([]storage.StructuralIdentityRow{
		{Venue: "kalshi", Ticker: ticker}, {Venue: "polyus", Ticker: slug},
	})
	kal := artifacts[storage.ResolutionBasisKey("kalshi", ticker)]
	pus := artifacts[storage.ResolutionBasisKey("polyus", slug)]
	if kal.Hash == "" || kal.Primary == "" || kal.VoidPolicy != "" {
		t.Fatalf("Kalshi raw rules were lost or an absent void term was invented: %+v", kal)
	}
	if pus.Hash == "" || pus.Description == "" || pus.VoidPolicy != "" || pus.ScalarPolicy != "" {
		t.Fatalf("PolyUS raw prose was lost or natural language was falsely promoted to normalized policy: %+v", pus)
	}
	if kal.Hash == pus.Hash {
		t.Fatal("different raw venue rule artifacts were falsely normalized as equivalent")
	}
}

func TestR139XVLockProofRequiresImmutableCompatibleResolutionBasis(t *testing.T) {
	op := xvLockOpp{Pair: "K-PUS", AVenue: "kalshi", AID: "KX-1", ASide: "YES",
		BVenue: "polyus", BID: "pus-1", BSide: "NO"}
	if xvlProofEligible(op) {
		t.Fatal("legacy/nil resolution certificate entered lock proof")
	}
	op.ResolutionBasis = r139VerifiedLockCertificate(op.Pair, op.AVenue, op.AID,
		op.BVenue, op.BID, true, op.ASide, op.BSide)
	if !xvlProofEligible(op) || resolutionbasis.Verify(*op.ResolutionBasis) != nil {
		t.Fatalf("complete same-payoff resolution certificate was rejected: %+v", op.ResolutionBasis)
	}

	mutated := *op.ResolutionBasis
	mutated.Right.UnknownPolicy = "cancel-as-loss"
	op.ResolutionBasis = &mutated
	if xvlProofEligible(op) {
		t.Fatal("post-issue resolution-term mutation bypassed immutable hash")
	}
}

func TestR139XVLockCertificateFailsClosedOnMissingOrMismatchedVenueRules(t *testing.T) {
	candidate := xvlCandidate{pair: "K-PUS", aVenue: "kalshi", aID: "KX-1",
		bVenue: "polyus", bID: "pus-1", sameSide: true}
	orient := xvlOrient{aSide: "YES", bSide: "NO"}
	missing := xvlResolutionCertificate(candidate, orient, nil)
	if missing.Compatible || resolutionbasis.Verify(missing) == nil || missing.ContentHash == "" {
		t.Fatalf("missing semantic registry receipt was not hashed and blocked: %+v", missing)
	}
	inputs := map[string]storage.ResolutionBasisInput{
		storage.ResolutionBasisKey(candidate.aVenue, candidate.aID): r139ResolutionInput(candidate.aVenue, candidate.aID, "same"),
		storage.ResolutionBasisKey(candidate.bVenue, candidate.bID): r139ResolutionInput(candidate.bVenue, candidate.bID, "same"),
	}
	right := inputs[storage.ResolutionBasisKey(candidate.bVenue, candidate.bID)]
	right.VoidPolicy = "cancel-as-loss"
	inputs[storage.ResolutionBasisKey(candidate.bVenue, candidate.bID)] = right
	mismatch := xvlResolutionCertificate(candidate, orient, inputs)
	if mismatch.Compatible || mismatch.Blocker != "void/refund policy mismatch" || resolutionbasis.Verify(mismatch) == nil {
		t.Fatalf("venue void mismatch entered lock proof: %+v", mismatch)
	}
}

func TestR139XVLockInverseIdentityRequiresSameSelectedSides(t *testing.T) {
	cert := r139VerifiedLockCertificate("K-PUS", "kalshi", "KX-1", "polyus", "pus-1",
		false, "YES", "YES")
	if resolutionbasis.Verify(*cert) != nil {
		t.Fatalf("valid inverted-payoff lock rejected: %+v", cert)
	}
	bad := r139VerifiedLockCertificate("K-PUS", "kalshi", "KX-1", "polyus", "pus-1",
		false, "YES", "NO")
	if bad.Compatible || bad.Blocker != "selected sides do not form a complementary payout" {
		t.Fatalf("non-complementary inverse sides passed: %+v", bad)
	}
}
