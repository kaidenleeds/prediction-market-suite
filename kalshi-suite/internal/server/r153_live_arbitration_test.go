package server

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/objectiveidentity"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func r153Ranked(ticker, event, occurrence string, predicate objectiveidentity.PurchasedPredicate,
	lowerDay, meanDay, depth float64, at time.Time) liveMirrorRankedCandidate {
	return liveMirrorRankedCandidate{
		Candidate: liveMirrorCandidate{Platform: "kalshi", Ticker: ticker, Side: "YES", At: at},
		Quote:     liveMirrorQuote{Depth: depth}, LowerPerDay: lowerDay, MeanPerDay: meanDay,
		Descriptor: kalshiSemanticDescriptor{EventTicker: event,
			Milestone: kalshi.Milestone{ID: occurrence}, Predicate: predicate},
	}
}

func TestR153LiveArbitrationRanksProfitRateThenDepthThenFIFO(t *testing.T) {
	now := time.Now()
	weak := r153Ranked("WEAK", "E1", "M1", objectiveidentity.PurchasedPredicate{}, .02, .04, 1000, now)
	strong := r153Ranked("STRONG", "E2", "M2", objectiveidentity.PurchasedPredicate{}, .03, .04, 1, now.Add(time.Millisecond))
	if !strongerLiveMirrorRank(strong, weak) {
		t.Fatal("higher conservative fee-net capital-day rate did not outrank raw depth")
	}
	depth := strong
	depth.Quote.Depth = 2
	if !strongerLiveMirrorRank(depth, strong) {
		t.Fatal("depth did not break an equal proof/rate tie")
	}
	earlier := strong
	earlier.Candidate.At = now.Add(-time.Millisecond)
	if !strongerLiveMirrorRank(earlier, strong) {
		t.Fatal("first observed candidate did not break a true economic tie")
	}
}

func TestR153LiveArbitrationChoosesStrongestCompatibleSet(t *testing.T) {
	now := time.Now()
	winner := objectiveidentity.PurchasedPredicate{EventID: "GAME", Period: "full", Structured: true,
		ScoreAlternatives: [][]objectiveidentity.ScoreConstraint{{{
			HomeCoeff: 1, AwayCoeff: -1, Comparator: "gt", Threshold: 0,
		}}}}
	opposingSpread := objectiveidentity.PurchasedPredicate{EventID: "GAME", Period: "full", Structured: true,
		ScoreAlternatives: [][]objectiveidentity.ScoreConstraint{{{
			HomeCoeff: -1, AwayCoeff: 1, Comparator: "gt", Threshold: 3.5,
		}}}}
	total := objectiveidentity.PurchasedPredicate{EventID: "GAME", Period: "full", Structured: true,
		ScoreAlternatives: [][]objectiveidentity.ScoreConstraint{{{
			HomeCoeff: 1, AwayCoeff: 1, Comparator: "gt", Threshold: 182.5,
		}}}}
	w := r153Ranked("WINNER", "E-W", "GAME", winner, .05, .07, 20, now)
	s := r153Ranked("SPREAD", "E-S", "GAME", opposingSpread, .04, .06, 20, now)
	o := r153Ranked("TOTAL", "E-T", "GAME", total, .03, .05, 20, now)
	if ok, _ := liveMirrorRankSetCompatible([]liveMirrorRankedCandidate{w}, s); ok {
		t.Fatal("weaker opposing spread remained compatible after stronger winner was selected")
	}
	if ok, why := liveMirrorRankSetCompatible([]liveMirrorRankedCandidate{w}, o); !ok {
		t.Fatalf("winner plus total was blocked: %s", why)
	}
}

func TestR153LiveArbitrationUnknownRelatedOnlyAllowsOne(t *testing.T) {
	now := time.Now()
	unknown := objectiveidentity.PurchasedPredicate{EventID: "GAME", Period: "full"}
	a := r153Ranked("A", "EA", "GAME", unknown, .05, .06, 10, now)
	b := r153Ranked("B", "EB", "GAME", unknown, .04, .05, 10, now.Add(time.Millisecond))
	if ok, why := liveMirrorRankSetCompatible([]liveMirrorRankedCandidate{a}, b); ok ||
		why != "same-cycle-related-payoff-unclassified" {
		t.Fatalf("unknown related pair = ok %v why %q", ok, why)
	}
}

func TestR153RawIntentBurstIncludesLaterStrongerCandidateBeforeRanking(t *testing.T) {
	s := testServer(t)
	now := time.Now()
	weak := liveSignalIntent{Signal: storage.Signal{Platform: "kalshi", Ticker: "WEAK",
		Side: "YES", SignalType: "kalshi-flow"}, Point: .02, At: now}
	strong := liveSignalIntent{Signal: storage.Signal{Platform: "kalshi", Ticker: "STRONG",
		Side: "YES", SignalType: "kalshi-flow"}, Point: .08, At: now.Add(2 * time.Millisecond)}
	go func() {
		time.Sleep(2 * time.Millisecond)
		s.liveSignalIntentChannel() <- strong
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	batch := s.collectLiveSignalIntentBurst(ctx, &weak)
	if len(batch) != 2 || batch[0].Signal.Ticker != "WEAK" || batch[1].Signal.Ticker != "STRONG" {
		t.Fatalf("bounded raw-intent window failed to include later competitor: %+v", batch)
	}

	// Both represent mutually exclusive native winner contracts for one official occurrence. The
	// later signal has the stronger current proof, so it must be ranked before compatibility drops
	// the weaker first-arriving signal.
	predicate := objectiveidentity.PurchasedPredicate{EventID: "GAME", Period: "full", Structured: true,
		ScoreAlternatives: [][]objectiveidentity.ScoreConstraint{{{
			HomeCoeff: 1, AwayCoeff: -1, Comparator: "gt", Threshold: 0,
		}}}}
	rows := make([]liveMirrorRankedCandidate, 0, len(batch))
	for _, intent := range batch {
		rows = append(rows, r153Ranked(intent.Signal.Ticker, "SAME-NATIVE-EVENT",
			"SAME-GAME", predicate, intent.Point, intent.Point, 10, intent.At))
	}
	sort.SliceStable(rows, func(i, j int) bool { return strongerLiveMirrorRank(rows[i], rows[j]) })
	if rows[0].Candidate.Ticker != "STRONG" {
		t.Fatalf("first arrival still won over stronger later candidate: %+v", rows)
	}
	if ok, _ := liveMirrorRankSetCompatible(rows[:1], rows[1]); ok {
		t.Fatal("weaker contradictory same-game candidate survived after stronger selection")
	}
}

func TestR153DecisionWindowDoesNotTruncateAtEightArrivals(t *testing.T) {
	s := testServer(t)
	now := time.Now()
	first := liveSignalIntent{Signal: storage.Signal{Platform: "kalshi", Ticker: "KX-0",
		Side: "YES", SignalType: "kalshi-flow"}, Point: .01, At: now}
	for i := 1; i <= 8; i++ {
		point := .01 + float64(i)/100
		if i == 8 {
			point = .90 // the old first-eight cap silently left this strongest ninth arrival upstream
		}
		s.liveSignalIntentChannel() <- liveSignalIntent{Signal: storage.Signal{Platform: "kalshi",
			Ticker: fmt.Sprintf("KX-%d", i), Side: "YES", SignalType: "kalshi-flow"},
			Point: point, At: now.Add(time.Duration(i) * time.Millisecond)}
	}
	batch := s.collectLiveSignalIntentBurst(context.Background(), &first)
	if len(batch) != 9 {
		t.Fatalf("decision-window batch=%d, want all 9", len(batch))
	}
	foundStrong := false
	for _, intent := range batch {
		if intent.Signal.Ticker == "KX-8" && intent.Point == .90 {
			foundStrong = true
		}
	}
	if !foundStrong {
		t.Fatal("stronger ninth decision-window candidate was truncated")
	}
}

func TestR153BlankContractCandidatesKeepDistinctProofIdentity(t *testing.T) {
	base := liveMirrorCandidate{Platform: "kalshi", Ticker: "KX-SAME", Side: "YES", Action: "BUY"}
	weak := base
	weak.Family, weak.Source = "sealed-weak", "r139p:weak"
	strong := base
	strong.Family, strong.Source = "sealed-strong", "r139p:strong"
	if weak.key() == strong.key() {
		t.Fatalf("different blank-contract proof systems coalesced before arbitration: %q", weak.key())
	}
	exactDuplicate := weak
	if weak.key() != exactDuplicate.key() {
		t.Fatal("exact proof duplicate lost deterministic coalescing key")
	}
}
