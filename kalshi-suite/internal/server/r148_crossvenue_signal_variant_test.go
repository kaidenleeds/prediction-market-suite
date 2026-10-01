package server

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestR148CrossVenueVariantsAreCertifiedPortableCellsNotBlanketParity(t *testing.T) {
	for _, tc := range []struct {
		system, destination string
		want                bool
	}{
		{"kalshi-flow", "polyus", true},
		{"spotlag", "polyus", true},
		{"polyus-flow", "kalshi", true},
		{"wxedge", "polyus", false},            // weather is review-only, not objective-result parity
		{"arb", "polyus", false},               // already owns a cross-venue strategy
		{"ml-book", "polyus", false},           // independently scored venue-native ML route
		{"event-basket-lock", "kalshi", false}, // multi-leg product, never a single mirror
	} {
		if got := r148CrossVenueProducerCapable(tc.system, tc.destination, "YES"); got != tc.want {
			t.Fatalf("%s -> %s producer=%v want %v", tc.system, tc.destination, got, tc.want)
		}
	}
	variants := r145KnownSystemVariants()
	has := func(system, venue, side string) bool {
		for _, row := range variants {
			if row.SystemID == system && row.Venue == venue && row.Side == side && row.Route == "taker" &&
				r145CatalogVariantHasCodePath(row) {
				return true
			}
		}
		return false
	}
	if !has("kalshi-flow", "polyus", "YES") || !has("kalshi-flow", "polyus", "NO") ||
		!has("invert:kalshi-flow", "polyus", "YES") || !has("polyus-flow", "kalshi", "NO") {
		t.Fatalf("certified cross-venue execution cells absent from catalog")
	}
	if has("wxedge", "polyus", "YES") || has("arb", "polyus", "NO") &&
		r148CrossVenueProducerCapable("arb", "polyus", "NO") {
		t.Fatal("an unsupported/dedicated strategy was represented as a generic portable variant")
	}
}

func TestR148LiveBoundaryRechecksCertificateAndInverseRetainsComplementaryLineage(t *testing.T) {
	s := testServer(t)
	if why := s.liveR148CrossVenueVariantReason(context.Background(), liveMirrorCandidate{
		Platform: "kalshi", Ticker: "K-NATIVE", Side: "YES"}); why != "" {
		t.Fatalf("native signal was forced through a cross-venue certificate: %s", why)
	}
	c := liveMirrorCandidate{Platform: "polyus", Ticker: "pus-missing", Side: "YES",
		CrossVenueSourceVenue: "kalshi", CrossVenueSourceTicker: "K-MISSING", CrossVenueSourceSide: "YES",
		CrossVenueCertificateHash: strings.Repeat("a", 64)}
	if why := s.liveR148CrossVenueVariantReason(context.Background(), c); why != "cross-venue-certificate-missing-stale-or-ambiguous" {
		t.Fatalf("missing current certificate reached LIVE boundary: %q", why)
	}
	base := storage.Signal{Platform: "polyus", Ticker: "pus-missing", Side: "YES",
		SignalType: "kalshi-flow", EntryPrice: .4,
		ExecExpr: r148CrossVenueSignalMarker + "/source=kalshi:K-MISSING/source-side=YES/certificate=" + strings.Repeat("a", 64) + "/input=K-PUS"}
	inverse, ok := independentInverseSignalIdentity(base)
	if !ok || inverse.Side != "NO" {
		t.Fatalf("cross-venue inverse identity=%+v ok=%v", inverse, ok)
	}
	lineage, ok := r148CrossVenueLineage(inverse)
	if !ok || lineage.SourceSide != "YES" || lineage.SourceTicker != "K-MISSING" ||
		lineage.CertificateHash != strings.Repeat("a", 64) {
		t.Fatalf("inverse lost complementary cross-venue certificate lineage: %+v ok=%v expr=%s", lineage, ok, inverse.ExecExpr)
	}
}

func TestR148CrossVenueSignalUsesCertificateSideAndClearsSourceBook(t *testing.T) {
	fee, bid, ask, depth := .02, .40, .44, 17.0
	source := storage.Signal{Platform: "kalshi", Ticker: "K-SOURCE", Side: "YES",
		SignalType: "kalshi-flow", Title: "source", EntryPrice: .61, SpreadCents: 9,
		BookDepth: 4, FeePC: &fee, BookBid: &bid, BookAsk: &ask, BookAskDepth: &depth,
		BookFeatureVer: mlBookFeatureVersion, PricingVersion: mlBookFeatureSchema,
		ExecExpr: "input=K"}
	cert := storage.RulePairCertificateSpec{SpecHash: strings.Repeat("a", 64), Orientation: "inverse",
		CanonicalEventID: "sports:event", CanonicalPayoffID: "sports:event|winner|A"}
	destinationSide, ok := whaleExitCertificateSide(source.Side, cert.Orientation)
	if !ok || destinationSide != "NO" {
		t.Fatalf("inverse orientation side=%q ok=%v", destinationSide, ok)
	}
	out := r148PrepareCrossVenueSignal(source, cert, "polyus", "pus-destination", destinationSide,
		"friendly destination", "K-PUS", time.Time{}, 2.5)
	if out.Platform != "polyus" || out.Ticker != "pus-destination" || out.Side != "NO" ||
		out.EntryPrice != 0 || out.FeePC != nil || out.BookBid != nil || out.BookAsk != nil ||
		out.BookFeatureVer != 0 || out.ResolveHours != 2.5 ||
		!strings.Contains(out.ExecExpr, r148CrossVenueSignalMarker) ||
		!strings.Contains(out.ExecExpr, "certificate="+cert.SpecHash) ||
		!strings.Contains(out.ExecExpr, "input=K-PUS") {
		t.Fatalf("prepared destination signal retained/invented source economics: %+v", out)
	}
	if !r148CrossVenueSignalAlreadyDerived(out) {
		t.Fatal("derived signal lost its recursion guard")
	}
}

func TestR148CrossVenueVariantPreservesExternalInputClockThroughInverse(t *testing.T) {
	observed := time.Now().UTC().Add(-2 * time.Minute).Truncate(time.Millisecond)
	source := storage.Signal{Platform: "kalshi", Ticker: "K-SPOT-SOURCE", Side: "YES",
		SignalType: "spotlag", ExecExpr: r147InputReceiptExpr("K-SPOT", observed)}
	cert := storage.RulePairCertificateSpec{SpecHash: strings.Repeat("b", 64), Orientation: "same"}
	derived := r148PrepareCrossVenueSignal(source, cert, "polyus", "pus-spot-destination", "YES",
		"destination", "K-PUS-SPOT", r147SignalInputObservedAt(source), 1)
	if got := r147SignalInputObservedAt(derived); !got.Equal(observed) {
		t.Fatalf("destination minted/replaced source clock: got=%s want=%s expr=%s", got, observed, derived.ExecExpr)
	}
	inverse, ok := independentInverseSignalIdentity(derived)
	if !ok || inverse.Side != "NO" || !r147SignalInputObservedAt(inverse).Equal(observed) {
		t.Fatalf("inverse lost stale source clock: ok=%v inverse=%+v", ok, inverse)
	}
	contract := r147SystemVariantSignalContract{ExecutionVenue: "polyus", InputTopology: "K-PUS-SPOT",
		RequiredInputs: []string{"K", "PUS", "SPOT"}}
	if reason := liveAllocationInputClockReason(contract, r147LiveInputClockSnapshot{
		KalshiClient: true, KalshiSubscribed: 1, KalshiFresh: 1,
		PUSClient: true, PUSFrameOK: true, PUSExecutable: 1,
		ExternalObservedAt: r147SignalInputObservedAt(inverse), Now: time.Now(),
	}); reason != "prospective-allocation-spot-input-receipt-not-fresh" {
		t.Fatalf("stale external clock gained LIVE freshness after adapter: %q", reason)
	}
	if !r148TopologyNeedsInputTimestamp("K-PUS-SPOT") || r148TopologyNeedsInputTimestamp("K-PUS") {
		t.Fatal("external timestamp requirement classified incorrectly")
	}
}

func TestR148DecisionFingerprintSeparatesChangedEconomicsButDedupsAgeOnly(t *testing.T) {
	age, latency := 1.0, 8.0
	source := storage.Signal{Platform: "kalshi", Ticker: "K-DECISION", Side: "YES",
		SignalType: "kalshi-flow", EntryPrice: .42, Strength: .81, Episode: 2,
		BookQuoteAgeS: &age, BookLatencyMS: &latency, ExecExpr: "input=K"}
	quoted := source
	quoted.Platform, quoted.Ticker, quoted.EntryPrice = "polyus", "pus-decision", .44
	cert := storage.RulePairCertificateSpec{SpecHash: strings.Repeat("c", 64)}
	slot := "2026-07-15T18:3"
	first, _ := r148CrossVenueDecisionFingerprint(source, "COLLECTED", "accepted", "polyus",
		quoted.Ticker, "YES", cert, &quoted, "minimumTradeQty", slot)
	age2, latency2 := 4.9, 31.0
	source.BookQuoteAgeS, source.BookLatencyMS = &age2, &latency2
	quoted.BookQuoteAgeS, quoted.BookLatencyMS = &age2, &latency2
	ageOnly, _ := r148CrossVenueDecisionFingerprint(source, "COLLECTED", "accepted", "polyus",
		quoted.Ticker, "YES", cert, &quoted, "minimumTradeQty", slot)
	if first != ageOnly {
		t.Fatal("identical economic decision stopped deduping only because age/latency advanced")
	}
	quoted.EntryPrice = .45
	changedQuote, _ := r148CrossVenueDecisionFingerprint(source, "COLLECTED", "accepted", "polyus",
		quoted.Ticker, "YES", cert, &quoted, "minimumTradeQty", slot)
	if changedQuote == first {
		t.Fatal("changed executable destination price was hidden by audit dedup")
	}
	source.Strength = .87
	changedSource, _ := r148CrossVenueDecisionFingerprint(source, "COLLECTED", "accepted", "polyus",
		quoted.Ticker, "YES", cert, &quoted, "minimumTradeQty", slot)
	if changedSource == changedQuote {
		t.Fatal("changed source observation was hidden by audit dedup")
	}
	nextSlot, _ := r148CrossVenueDecisionFingerprint(source, "COLLECTED", "accepted", "polyus",
		quoted.Ticker, "YES", cert, &quoted, "minimumTradeQty", "2026-07-15T18:4")
	if nextSlot == changedSource {
		t.Fatal("a distinct durable source-signal slot was hidden by audit dedup")
	}
}

func TestR148CrossVenueTopologiesRetainExternalInputAndDestination(t *testing.T) {
	for source, destinations := range map[string]map[string]string{
		"K-SPOT":       {"polyus": "K-PUS-SPOT"},
		"K-SPORTSBOOK": {"polyus": "K-PUS-SPORTSBOOK"},
		"PUS":          {"kalshi": "K-PUS"},
	} {
		for venue, expected := range destinations {
			if got := r148CrossVenueTopologyName(source, venue); got != expected {
				t.Fatalf("topology %s -> %s=%s want %s", source, venue, got, expected)
			}
		}
	}
	parsed := storage.Signal{ExecExpr: "xv-exec-variant=1/input=K-PUS-SPOT/input_at=2026-07-15T00:00:00Z"}
	if got := r147SignalInputTopology(parsed); got != "K-PUS-SPOT" {
		t.Fatalf("durable cross-venue topology parsed as %q", got)
	}
}

func TestR148CertifiedDestinationSignalRunsSharedPipelineOnceAndOwnsInverse(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	kTicker, pSlug := "K-R148-E2E", "pus-r148-e2e"
	event := storage.CanonicalEventSpec{EventID: "sports:r148-e2e", EventType: "sports-game",
		Domain: "soccer", OutcomeSetStatus: "complete", EvidenceJSON: `{}`}
	payoff := storage.CanonicalPayoffSpec{PayoffID: event.EventID + "|full_game|winner|HOME",
		EventID: event.EventID, Label: "home wins", PredicateJSON: `{"kind":"winner","yes_team":"HOME"}`,
		PayoutCeiling: 1, IdentityStatus: "structural", EvidenceJSON: `{}`}
	instruments := []storage.CanonicalInstrumentSpec{
		{Venue: "kalshi", Ticker: kTicker, EventID: event.EventID, PayoffID: payoff.PayoffID,
			NativeSide: "YES", Orientation: "same", IdentityStatus: "structural", EvidenceJSON: `{}`},
		{Venue: "polyus", Ticker: pSlug, EventID: event.EventID, PayoffID: payoff.PayoffID,
			NativeSide: "YES", Orientation: "same", IdentityStatus: "structural", EvidenceJSON: `{}`},
	}
	if _, err := s.store.RegisterCanonicalBatch(ctx, []storage.CanonicalEventSpec{event},
		[]storage.CanonicalPayoffSpec{payoff}, instruments); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	kArtifact, _, err := s.store.InsertVenueRuleArtifact(ctx, storage.VenueRuleArtifact{
		Venue: "kalshi", InstrumentID: kTicker, Source: "official Kalshi market",
		Primary: "HOME wins the official full game", Observed: now,
		SettlementSources: []storage.RuleSettlementSource{{Name: "official", URL: "https://league.example/final"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	pArtifact, _, err := s.store.InsertVenueRuleArtifact(ctx, storage.VenueRuleArtifact{
		Venue: "polyus", InstrumentID: pSlug, Source: "official PolyUS market",
		Description: "HOME wins the official full game", Observed: now,
		SettlementSources: []storage.RuleSettlementSource{{Name: "official", URL: "https://league.example/final"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	terms := storage.NormalizedRuleTerms{SettlementSourceID: "official-final",
		SettlementSourceURL: "https://league.example/final", PredicateHash: strings.Repeat("1", 64),
		RulesHash: strings.Repeat("2", 64), VoidPolicy: "refund", ScalarPolicy: "binary",
		UnknownPolicy: "refund", TimingPolicy: "official-final"}
	cert, inserted, err := s.store.RegisterRulePairCertificate(ctx, storage.RulePairCertificateSpec{
		PairID: "k-pus|" + kTicker + "|" + pSlug, Version: 1,
		KalshiTicker: kTicker, PolyUSSlug: pSlug,
		KalshiArtifactHash: kArtifact.ArtifactHash, PolyUSArtifactHash: pArtifact.ArtifactHash,
		CanonicalEventID: event.EventID, CanonicalPayoffID: payoff.PayoffID,
		Left: terms, Right: terms, ReviewMethod: "human_review",
		ReviewProvenance: "R148 end-to-end exact route fixture", ReviewEvidenceHash: strings.Repeat("3", 64),
	})
	if err != nil || !inserted {
		t.Fatalf("certificate inserted=%v err=%v", inserted, err)
	}
	s.polyUSMu.Lock()
	s.polyUSAt = now
	s.polyUSMkts = []polyUSMarket{{Slug: pSlug, Question: "HOME wins", MinimumQty: 1,
		Start: now.Add(30 * time.Minute).Format(time.RFC3339)}}
	s.polyUSMu.Unlock()
	s.completeBookSignalFn = func(sig storage.Signal) (storage.Signal, string, bool) {
		if sig.Platform != "polyus" || sig.Ticker != pSlug || (sig.Side != "YES" && sig.Side != "NO") {
			return storage.Signal{}, "", false
		}
		bid, ask := .40, .42
		if sig.Side == "NO" {
			bid, ask = .57, .59
		}
		bidDepth, askDepth, age, tick, makerFee, takerFee, latency := 25.0, 20.0, .1, .01, 0.0, .01, 7.0
		sig.EntryPrice, sig.SpreadCents, sig.BookDepth = ask, (ask-bid)*100, askDepth
		sig.BookFeatureVer, sig.PricingVersion, sig.BookSource = mlBookFeatureVersion, mlBookFeatureSchema, "r148-atomic-book"
		sig.BookBid, sig.BookAsk, sig.BookBidDepth, sig.BookAskDepth = &bid, &ask, &bidDepth, &askDepth
		sig.BookQuoteAgeS, sig.BookMakerTick, sig.BookTakerTick = &age, &tick, &tick
		sig.BookMakerFeePC, sig.BookTakerFeePC, sig.BookLatencyMS = &makerFee, &takerFee, &latency
		sig.FeePC = &takerFee
		return sig, "r148-exact-fee", true
	}

	s.collectR148CrossVenueSignalVariant(ctx, storage.Signal{Platform: "kalshi", Ticker: kTicker,
		Title: "HOME wins", Side: "YES", SignalType: "kalshi-flow", EntryPrice: .43,
		Strength: .8, ExecExpr: "input=K"})
	rows, err := s.store.DBForTest().QueryContext(ctx, `SELECT signal_type,side,entry_price,exec_expr
FROM signal_log WHERE platform='polyus' AND ticker=? ORDER BY signal_type`, pSlug)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	got := map[string]struct {
		side, expr string
		price      float64
	}{}
	for rows.Next() {
		var family, side, expr string
		var price float64
		if err := rows.Scan(&family, &side, &price, &expr); err != nil {
			t.Fatal(err)
		}
		got[family] = struct {
			side, expr string
			price      float64
		}{side: side, expr: expr, price: price}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	base, baseOK := got["kalshi-flow"]
	inverse, inverseOK := got["invert:kalshi-flow"]
	if !baseOK || base.side != "YES" || base.price != .42 ||
		!strings.Contains(base.expr, "certificate="+cert.SpecHash) ||
		!inverseOK || inverse.side != "NO" || inverse.price != .59 ||
		!strings.Contains(inverse.expr, "certificate="+cert.SpecHash) {
		t.Fatalf("shared destination/inverse pipeline rows=%+v", got)
	}
	for family := range got {
		if strings.HasPrefix(family, "invert:invert:") {
			t.Fatalf("cross-venue adapter recursively inverted itself: %s", family)
		}
	}
	if why := s.liveR148CrossVenueVariantReason(ctx, liveMirrorCandidate{Platform: "polyus",
		Ticker: pSlug, Side: "YES", CrossVenueSourceVenue: "kalshi", CrossVenueSourceTicker: kTicker,
		CrossVenueSourceSide: "YES", CrossVenueCertificateHash: cert.SpecHash}); why != "" {
		t.Fatalf("current exact certificate failed final LIVE identity check: %s", why)
	}
	if why := s.liveR148CrossVenueVariantReason(ctx, liveMirrorCandidate{Platform: "polyus",
		Ticker: pSlug, Side: "NO", Inverted: true, CrossVenueSourceVenue: "kalshi", CrossVenueSourceTicker: kTicker,
		CrossVenueSourceSide: "YES", CrossVenueCertificateHash: cert.SpecHash}); why != "" {
		t.Fatalf("current exact inverse certificate failed final LIVE identity check: %s", why)
	}
	derivedForPaper := storage.Signal{Platform: "polyus", Ticker: pSlug, Side: "YES",
		SignalType: "kalshi-flow", ExecExpr: base.expr}
	if why := s.paperR148CrossVenueVariantReason(ctx, derivedForPaper, false); why != "" {
		t.Fatalf("current exact certificate failed final Paper identity check: %s", why)
	}
	// A raw-rule revision after collection but before Paper placement must revoke the old immutable
	// certificate at the money boundary; the one-minute discovery cache cannot authorize the fill.
	pArtifact.Description += " revised settlement wording"
	if _, changed, err := s.store.InsertVenueRuleArtifact(ctx, pArtifact); err != nil || !changed {
		t.Fatalf("raw rule revision changed=%v err=%v", changed, err)
	}
	if why := s.paperR148CrossVenueVariantReason(ctx, derivedForPaper, false); why != "cross-venue-certificate-missing-stale-or-ambiguous" {
		t.Fatalf("stale certificate reached final Paper boundary: %q", why)
	}
}
