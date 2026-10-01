package storage

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestR148CompatibleCertificateLookupIsExactBidirectionalAndCurrentOnly(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	kTicker, pSlug := "K-R148-CERT", "pus-r148-cert"
	seedRuleCanonical(t, st, kTicker, pSlug)
	now := time.Now().UTC()
	k, _, err := st.InsertVenueRuleArtifact(ctx, VenueRuleArtifact{Venue: "kalshi", InstrumentID: kTicker,
		Source: "kalshi market REST", Primary: "official objective winner",
		SettlementSources: []RuleSettlementSource{{Name: "Official", URL: "https://official.example/final"}}, Observed: now})
	if err != nil {
		t.Fatal(err)
	}
	p, _, err := st.InsertVenueRuleArtifact(ctx, VenueRuleArtifact{Venue: "polyus", InstrumentID: pSlug,
		Source: "polyus market REST", Description: "official objective winner",
		SettlementSources: []RuleSettlementSource{{Name: "Official", URL: "https://official.example/final"}}, Observed: now})
	if err != nil {
		t.Fatal(err)
	}
	terms := NormalizedRuleTerms{SettlementSourceID: "official-final",
		SettlementSourceURL: "https://official.example/final", PredicateHash: strings.Repeat("1", 64),
		RulesHash: strings.Repeat("2", 64), VoidPolicy: "refund", ScalarPolicy: "binary",
		UnknownPolicy: "refund", TimingPolicy: "official-final"}
	cert, inserted, err := st.RegisterRulePairCertificate(ctx, RulePairCertificateSpec{
		PairID: "k-pus|" + kTicker + "|" + pSlug, Version: 1, KalshiTicker: kTicker, PolyUSSlug: pSlug,
		KalshiArtifactHash: k.ArtifactHash, PolyUSArtifactHash: p.ArtifactHash,
		CanonicalEventID: "sports:game-rule", CanonicalPayoffID: "sports:game-rule|full_game|winner|PHI",
		Left: terms, Right: terms, ReviewMethod: "human_review", ReviewProvenance: "R148 exact route fixture",
		ReviewEvidenceHash: strings.Repeat("3", 64)})
	if err != nil || !inserted {
		t.Fatalf("certificate inserted=%v err=%v cert=%+v", inserted, err, cert)
	}
	for _, tc := range []struct{ venue, id, other string }{
		{"kalshi", kTicker, "polyus"}, {"polyus", pSlug, "kalshi"},
	} {
		got, err := st.CompatibleRulePairCertificatesForInstrument(ctx, tc.venue, tc.id, tc.other, 3)
		if err != nil || len(got) != 1 || got[0].SpecHash != cert.SpecHash {
			t.Fatalf("lookup %s:%s -> %s got=%+v err=%v", tc.venue, tc.id, tc.other, got, err)
		}
	}
	if got, err := st.CompatibleRulePairCertificatesForInstrument(ctx, "kalshi", "K-NOT-THIS", "polyus", 3); err != nil || len(got) != 0 {
		t.Fatalf("unmatched instrument borrowed a certificate: %+v err=%v", got, err)
	}
	// A changed raw artifact makes the immutable old certificate non-current immediately.
	p.Description += " changed"
	if _, changed, err := st.InsertVenueRuleArtifact(ctx, p); err != nil || !changed {
		t.Fatalf("raw revision changed=%v err=%v", changed, err)
	}
	if got, err := st.CompatibleRulePairCertificatesForInstrument(ctx, "kalshi", kTicker, "polyus", 3); err != nil || len(got) != 0 {
		t.Fatalf("stale certificate remained route-authoritative: %+v err=%v", got, err)
	}
}
