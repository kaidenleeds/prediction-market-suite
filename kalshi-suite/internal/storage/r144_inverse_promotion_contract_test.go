package storage

import (
	"context"
	"testing"
)

func TestR144NativeInversePromotionContractIsFamilySelectorExact(t *testing.T) {
	selector := "native-inverse-v1:aaaaaaaa"
	cohort := "native-inverse-v1|selector=" + selector + "|candidate|policy=" + selector
	p := ResearchPromotionProof{SystemID: "paired-bridge-inversion", Cohort: cohort,
		Venue: "kalshi", Route: "taker", StrategyFamily: "invert:kalshi-flow",
		SelectorID: selector, FiredSide: "YES"}
	c := ResearchPromotionCandidate{SystemID: p.SystemID, Cohort: cohort, Venue: "kalshi",
		Route: "taker", Ticker: "KXR144", Side: "NO", StrategyFamily: "invert:kalshi-flow",
		SelectorID: selector, FiredSide: "YES"}
	if !nativeInversePromotionCandidateValid(p, c) {
		t.Fatal("exact immutable inverse candidate was rejected")
	}
	for name, mutate := range map[string]func(*ResearchPromotionCandidate){
		"family missing": func(x *ResearchPromotionCandidate) { x.StrategyFamily = "" },
		"nested":         func(x *ResearchPromotionCandidate) { x.StrategyFamily = "invert:invert:kalshi-flow" },
		"selector":       func(x *ResearchPromotionCandidate) { x.SelectorID = "native-inverse-v1:bbbbbbbb" },
		"matched control": func(x *ResearchPromotionCandidate) {
			x.Cohort = "native-inverse-v1|selector=" + selector + "|matched-control|policy=" + selector
		},
		"same side":   func(x *ResearchPromotionCandidate) { x.FiredSide = "NO" },
		"maker drift": func(x *ResearchPromotionCandidate) { x.Route = "maker" },
	} {
		drift := c
		mutate(&drift)
		if nativeInversePromotionCandidateValid(p, drift) {
			t.Fatalf("%s drift retained promotion authority: %+v", name, drift)
		}
	}
}

func TestR144ExistingPairedBridgeCohortKeepsItsPriorContract(t *testing.T) {
	p := ResearchPromotionProof{SystemID: "paired-bridge-inversion",
		Cohort: "same-clock-bridge-inverse|candidate|policy=bridge-inverse-frozen-v1",
		Venue:  "kalshi", Route: "taker"}
	c := ResearchPromotionCandidate{SystemID: p.SystemID, Cohort: p.Cohort, Venue: p.Venue,
		Route: p.Route, Ticker: "KXBRIDGE", Side: "NO", SelectorID: "bridge-inverse-frozen-v1"}
	if !nativeInversePromotionCandidateValid(p, c) {
		t.Fatal("pre-existing fixed bridge cohort was incorrectly subjected to native-inverse-v1 fields")
	}
}

func TestR144PromotionMigrationAddsNonAuthoritativeInverseIdentityDefaults(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	for _, column := range []string{"strategy_family", "selector_id", "fired_side"} {
		var n int
		if err := st.db.QueryRowContext(context.Background(),
			`SELECT COUNT(*) FROM pragma_table_info('research_promotion_intents') WHERE name=?`, column).Scan(&n); err != nil || n != 1 {
			t.Fatalf("migration column %s count=%d err=%v", column, n, err)
		}
	}
}
