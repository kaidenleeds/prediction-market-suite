package objectiveidentity

import "testing"

const intersectionTestEvent = "sports:nba:2026-07-16:BKN@HOU"

func scorePredicate(name, period string, alternatives ...[]ScoreConstraint) PurchasedPredicate {
	return PurchasedPredicate{
		EventID:           intersectionTestEvent,
		InstrumentID:      name,
		Period:            period,
		Structured:        true,
		ScoreAlternatives: alternatives,
	}
}

func scoreClause(home, away float64, comparator string, threshold float64) []ScoreConstraint {
	return []ScoreConstraint{{
		HomeCoeff:  home,
		AwayCoeff:  away,
		Comparator: comparator,
		Threshold:  threshold,
	}}
}

func requirePurchasedSet(t *testing.T, wantContradiction bool, predicates ...PurchasedPredicate) IntersectionVerdict {
	t.Helper()
	got := EvaluatePurchasedSet(predicates)
	if got.Contradictory != wantContradiction {
		t.Fatalf("EvaluatePurchasedSet() = %+v, want contradictory=%v", got, wantContradiction)
	}
	if !got.Related || !got.Comparable {
		t.Fatalf("structured same-event predicates lost relation/comparability: %+v", got)
	}
	wantReason := IntersectionFeasible
	if wantContradiction {
		wantReason = IntersectionEmpty
	}
	if got.ReasonCode != wantReason {
		t.Fatalf("reason=%q, want %q: %+v", got.ReasonCode, wantReason, got)
	}
	return got
}

func TestPurchasedSetWinnerAgainstOpposingSpread(t *testing.T) {
	// M = Brooklyn score - Houston score. Brooklyn wins requires M>0, while Houston -3.5
	// requires M<-3.5. No final score can pay both positions.
	brooklynWins := scorePredicate("brooklyn-moneyline", "full_game", scoreClause(1, -1, "gt", 0))
	houstonMinus35 := scorePredicate("houston-minus-3.5", "full_game", scoreClause(1, -1, "lt", -3.5))
	got := requirePurchasedSet(t, true, brooklynWins, houstonMinus35)
	if got.Metric != "score:full_game" {
		t.Fatalf("contradiction metric=%q, want score:full_game", got.Metric)
	}

	// The result must not depend on which signal arrived first.
	requirePurchasedSet(t, true, houstonMinus35, brooklynWins)
}

func TestPurchasedSetWinnerAndSameTeamSpreadRemainCompatible(t *testing.T) {
	houstonWins := scorePredicate("houston-moneyline", "full_game", scoreClause(1, -1, "lt", 0))
	houstonMinus35 := scorePredicate("houston-minus-3.5", "full_game", scoreClause(1, -1, "lt", -3.5))
	requirePurchasedSet(t, false, houstonWins, houstonMinus35)
}

func TestPurchasedSetWinnerAndGameTotalRemainCompatible(t *testing.T) {
	brooklynWins := scorePredicate("brooklyn-moneyline", "full_game", scoreClause(1, -1, "gt", 0))
	over1825 := scorePredicate("over-182.5", "full_game", scoreClause(1, 1, "gt", 182.5))
	requirePurchasedSet(t, false, brooklynWins, over1825)
}

func TestPurchasedSetOpposingSpreads(t *testing.T) {
	t.Run("both teams cannot cover minus 3.5", func(t *testing.T) {
		brooklynMinus35 := scorePredicate("brooklyn-minus-3.5", "full_game", scoreClause(1, -1, "gt", 3.5))
		houstonMinus35 := scorePredicate("houston-minus-3.5", "full_game", scoreClause(1, -1, "lt", -3.5))
		requirePurchasedSet(t, true, brooklynMinus35, houstonMinus35)
	})

	t.Run("both teams can cover plus 3.5", func(t *testing.T) {
		brooklynPlus35 := scorePredicate("brooklyn-plus-3.5", "full_game", scoreClause(1, -1, "gt", -3.5))
		houstonPlus35 := scorePredicate("houston-plus-3.5", "full_game", scoreClause(1, -1, "lt", 3.5))
		requirePurchasedSet(t, false, brooklynPlus35, houstonPlus35)
	})
}

func TestPurchasedSetCatchesJointTeamTotalContradiction(t *testing.T) {
	brooklynOver1005 := scorePredicate("brooklyn-team-total-over-100.5", "full_game",
		scoreClause(1, 0, "gt", 100.5))
	houstonOver905 := scorePredicate("houston-team-total-over-90.5", "full_game",
		scoreClause(0, 1, "gt", 90.5))
	gameUnder1905 := scorePredicate("game-total-under-190.5", "full_game",
		scoreClause(1, 1, "lt", 190.5))

	// Every pair has a possible score, but all three together do not. This pins the global
	// whole-position-set check rather than an insufficient pairwise-only guard.
	requirePurchasedSet(t, false, brooklynOver1005, gameUnder1905)
	requirePurchasedSet(t, false, houstonOver905, gameUnder1905)
	requirePurchasedSet(t, false, brooklynOver1005, houstonOver905)
	requirePurchasedSet(t, true, brooklynOver1005, houstonOver905, gameUnder1905)
}

func TestPurchasedSetSoccerDrawAndNoDisjunctions(t *testing.T) {
	homeWins := scorePredicate("home-win", "regulation", scoreClause(1, -1, "gt", 0))
	awayWins := scorePredicate("away-win", "regulation", scoreClause(1, -1, "lt", 0))
	drawYes := scorePredicate("draw-yes", "regulation", scoreClause(1, -1, "eq", 0))
	drawNo := scorePredicate("draw-no", "regulation",
		scoreClause(1, -1, "lt", 0), scoreClause(1, -1, "gt", 0))
	homeNo := scorePredicate("home-no", "regulation", scoreClause(1, -1, "lte", 0))
	awayNo := scorePredicate("away-no", "regulation", scoreClause(1, -1, "gte", 0))

	requirePurchasedSet(t, false, drawNo, homeWins)
	requirePurchasedSet(t, false, drawNo, awayWins)
	requirePurchasedSet(t, true, drawNo, drawYes)
	requirePurchasedSet(t, true, drawYes, homeWins)
	requirePurchasedSet(t, true, drawYes, awayWins)

	// In a three-outcome market, NO(Home) and NO(Away) intersect at Draw. Treating either NO as
	// the other team's YES would incorrectly reject this feasible pair.
	requirePurchasedSet(t, false, homeNo, awayNo)
}

func TestPurchasedSetLinksPeriodScoresWithoutTreatingWinnersAsOpposites(t *testing.T) {
	fullGameHome := scorePredicate("full-game-home", "full_game", scoreClause(1, -1, "gt", 0))
	firstHalfAway := scorePredicate("first-half-away", "first_half", scoreClause(1, -1, "lt", 0))
	requirePurchasedSet(t, false, fullGameHome, firstHalfAway)

	firstHalfOver35 := scorePredicate("first-half-over-3.5", "first_half", scoreClause(1, 1, "gt", 3.5))
	fullGameUnder25 := scorePredicate("full-game-under-2.5", "full_game", scoreClause(1, 1, "lt", 2.5))
	got := requirePurchasedSet(t, true, firstHalfOver35, fullGameUnder25)
	if got.Metric != "score:cross_period" {
		t.Fatalf("cross-period contradiction metric=%q", got.Metric)
	}

	// The production Kalshi adapter historically emitted the compact "full" alias. Keep it
	// recognized so stored/replayed predicates cannot bypass cross-period protection.
	fullAliasUnder25 := scorePredicate("full-alias-under-2.5", "full", scoreClause(1, 1, "lt", 2.5))
	requirePurchasedSet(t, true, firstHalfOver35, fullAliasUnder25)

	// Component periods are additive, not independent. Until explicit composition metadata exists,
	// Q1 and Q2 constraints plus a full-game constraint must fail closed.
	q1Over50 := scorePredicate("q1-over-50", "quarter:1", scoreClause(1, 1, "gt", 50))
	q2Over50 := scorePredicate("q2-over-50", "quarter:2", scoreClause(1, 1, "gt", 50))
	fullUnder90 := scorePredicate("full-under-90", "full_game", scoreClause(1, 1, "lt", 90))
	requirePurchasedSet(t, true, q1Over50, q2Over50, fullUnder90)
}
