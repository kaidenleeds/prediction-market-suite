package storage

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/paper"
)

func TestProperMomentumSellCandidateCannotLeakIntoGenericBuyBridge(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	eventID, ticker, cohort := "momentum-event", "KXMOMSELL", "proper-score-v2|mode=momentum|action=sell-reduce-only|promotion=kalshi-fok-v1"
	registerR138EvidenceInstrument(t, st, eventID, "kalshi", ticker)
	row := r138EvidenceFixture("candidate", true)
	row.Observed, row.SystemID, row.OpportunityID = time.Now().UTC(), "proper-score-executor", "momentum-sell"
	row.Cohort, row.CanonicalEventID, row.EventVersion = cohort, eventID, 0
	row.CanonicalPayoffID, row.PayoffVersion = "", 0
	row.Venue, row.Ticker, row.Route, row.Side = "kalshi", ticker, "taker", "NO"
	row.Cost, row.Fee, row.NetLower, row.NetUpper = .38, .01, -.39, .61
	row.CapacityCurve = []CapacityPoint{{Size: 1, Cost: .38, Fee: .01, PayoutFloor: 0, NetFloor: -.39}}
	row.Inputs = map[string]any{"promotion_action": "SELL", "owned_side": "YES",
		"required_prior_position": 1.0, "actual_sale_proceeds": .62,
		"promotion_expected_net_lower": .08}
	id, inserted, err := st.InsertResearchSystemObservation(ctx, row)
	if err != nil || !inserted {
		t.Fatalf("sell observation id=%d inserted=%v err=%v", id, inserted, err)
	}
	proof := ResearchPromotionProof{SystemID: row.SystemID, ExperimentVersion: 1, Cohort: cohort,
		Venue: "kalshi", Route: "taker"}
	if got, found, err := st.LatestResearchPromotionCandidate(ctx, proof, row.Observed.Add(-time.Second)); err != nil || found {
		t.Fatalf("SELL leaked into generic BUY bridge: found=%v got=%+v err=%v", found, got, err)
	}
	got, found, err := st.LatestProperMomentumSellCandidate(ctx, proof, row.Observed.Add(-time.Second))
	if err != nil || !found || got.ObservationID != id || got.OwnedSide != "YES" || got.SyntheticSide != "NO" {
		t.Fatalf("typed SELL missing: found=%v got=%+v err=%v", found, got, err)
	}
}

func TestProperMomentumPaperSellAtomicallyPreservesExactPosition(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	first, err := st.InsertPaperFill(ctx, paper.Fill{Platform: "kalshi", Ticker: "KXSELL",
		Title: "fixture", Side: "YES", Action: "BUY", Price: .4, Contracts: 1, Fee: .01,
		Source: "r139p:fixture"})
	if err != nil {
		t.Fatal(err)
	}
	_, inserted, err := st.InsertProperMomentumPaperSell(ctx, paper.Fill{Platform: "kalshi",
		Ticker: "KXSELL", Title: "fixture", Side: "YES", Action: "SELL", Price: .6,
		Contracts: 1, Fee: .01, Source: "r139ms:fixture"}, 1, first, "r139p:fixture")
	if err != nil || !inserted {
		t.Fatalf("exact Paper SELL inserted=%v err=%v", inserted, err)
	}
	fills, err := st.ListPaperFills(ctx)
	if err != nil {
		t.Fatal(err)
	}
	positions, _ := paper.Aggregate(fills)
	if len(positions) != 0 {
		t.Fatalf("post-sell position=%+v", positions)
	}
	// The stale snapshot boundary cannot sell again after the one promoted share is flat.
	if _, inserted, err := st.InsertProperMomentumPaperSell(ctx, paper.Fill{Platform: "kalshi",
		Ticker: "KXSELL", Title: "fixture", Side: "YES", Action: "SELL", Price: .6,
		Contracts: 1, Fee: .01, Source: "r139ms:stale"}, 1, first, "r139p:fixture"); err != nil || inserted {
		t.Fatalf("stale fill boundary inserted=%v err=%v", inserted, err)
	}
}

func TestProperMomentumPaperSellRejectsOppositeOrMismatchedPosition(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	last, _ := st.InsertPaperFill(ctx, paper.Fill{Platform: "kalshi", Ticker: "KXMIX",
		Title: "fixture", Side: "YES", Action: "BUY", Price: .4, Contracts: 1, Source: "open"})
	last, _ = st.InsertPaperFill(ctx, paper.Fill{Platform: "kalshi", Ticker: "KXMIX",
		Title: "fixture", Side: "NO", Action: "BUY", Price: .5, Contracts: 1, Source: "hedge"})
	if _, inserted, err := st.InsertProperMomentumPaperSell(ctx, paper.Fill{Platform: "kalshi",
		Ticker: "KXMIX", Title: "fixture", Side: "YES", Action: "SELL", Price: .6,
		Contracts: 1, Fee: .01, Source: "typed"}, 1, last, "open"); err != nil || inserted {
		t.Fatalf("opposite-side hedge did not block inserted=%v err=%v", inserted, err)
	}
	if _, inserted, err := st.InsertProperMomentumPaperSell(ctx, paper.Fill{Platform: "kalshi",
		Ticker: "KXMIX", Title: "fixture", Side: "YES", Action: "SELL", Price: .6,
		Contracts: 1, Fee: .01, Source: "typed"}, 2, last, "open"); err == nil || inserted {
		t.Fatalf("mismatched required prior did not block inserted=%v err=%v", inserted, err)
	}
}

func TestProperMomentumPaperSellRejectsMixedSourceSameSideLot(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	_, _ = st.InsertPaperFill(ctx, paper.Fill{Platform: "kalshi", Ticker: "KXMIXSRC",
		Title: "fixture", Side: "YES", Action: "BUY", Price: .4, Contracts: .5,
		Source: "r139p:accepted"})
	last, _ := st.InsertPaperFill(ctx, paper.Fill{Platform: "kalshi", Ticker: "KXMIXSRC",
		Title: "fixture", Side: "YES", Action: "BUY", Price: .41, Contracts: .5,
		Source: "manual-other"})
	fills, err := st.ListPaperFills(ctx)
	if err != nil {
		t.Fatal(err)
	}
	positions, _ := paper.Aggregate(fills)
	if len(positions) != 1 || math.Abs(positions[0].Contracts-1) > 1e-9 ||
		positions[0].Source != "r139p:accepted" {
		t.Fatalf("fixture must reproduce aggregate first-source ambiguity: %+v", positions)
	}
	if _, exact := ExactProperMomentumPaperOpen(fills, "KXMIXSRC", "YES"); exact {
		t.Fatal("mixed-source same-side lot passed exact promoted-lot proof")
	}
	if _, inserted, err := st.InsertProperMomentumPaperSell(ctx, paper.Fill{Platform: "kalshi",
		Ticker: "KXMIXSRC", Title: "fixture", Side: "YES", Action: "SELL", Price: .6,
		Contracts: 1, Fee: .01, Source: "r139ms:typed"}, 1, last, "r139p:accepted"); err != nil || inserted {
		t.Fatalf("mixed-source same-side lot inserted=%v err=%v", inserted, err)
	}
}
