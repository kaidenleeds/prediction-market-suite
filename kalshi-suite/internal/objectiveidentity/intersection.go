package objectiveidentity

import "strings"

// PurchasedPredicate is the side-adjusted, executable proposition used by the funded exposure
// guard. It is deliberately smaller than Contract: this API answers only whether two positions
// tied to one official occurrence can both pay $1. Constraints are ANDed; two predicates that do
// not constrain the same scalar are related but not logically contradictory.
type PurchasedPredicate struct {
	EventID      string
	InstrumentID string
	Period       string
	Structured   bool
	Constraints  []ScalarConstraint
	// ScoreAlternatives is DNF over one period's two participant scores. Each inner slice is AND;
	// the outer slice is OR. Winner/spread/total/team-total/exact-score/BTTS adapters use this so a
	// whole funded set is checked jointly rather than only pairwise.
	ScoreAlternatives [][]ScoreConstraint
}

type ScalarConstraint struct {
	Metric     string
	Comparator string // gt|gte|lt|lte|eq|neq
	Threshold  float64
}

type ScoreConstraint struct {
	HomeCoeff, AwayCoeff float64
	Comparator           string // gt|gte|lt|lte|eq|neq
	Threshold            float64
}

type IntersectionVerdict struct {
	Related       bool
	Comparable    bool
	Contradictory bool
	ReasonCode    string
	Metric        string
}

const (
	IntersectionDifferentEvent     = "DIFFERENT_EVENT"
	IntersectionUnclassified       = "RELATED_UNCLASSIFIED"
	IntersectionDifferentDimension = "RELATED_DIFFERENT_DIMENSION"
	IntersectionFeasible           = "RELATED_FEASIBLE_INTERSECTION"
	IntersectionEmpty              = "REJECT_EMPTY_PAYOFF_INTERSECTION"
)

func numericConstraintOK(value float64, comparator string, threshold float64) bool {
	const eps = 1e-9
	switch normToken(comparator) {
	case "gt":
		return value > threshold+eps
	case "gte":
		return value >= threshold-eps
	case "lt":
		return value < threshold-eps
	case "lte":
		return value <= threshold+eps
	case "eq":
		return value >= threshold-eps && value <= threshold+eps
	case "neq":
		return value < threshold-eps || value > threshold+eps
	}
	return false
}

type scalarInterval struct {
	lo, hi         float64
	loOpen, hiOpen bool
}

func scalarConstraintRanges(c ScalarConstraint) ([]scalarInterval, bool) {
	switch normToken(c.Comparator) {
	case "gt":
		return []scalarInterval{{lo: c.Threshold, hi: 1e300, loOpen: true}}, true
	case "gte":
		return []scalarInterval{{lo: c.Threshold, hi: 1e300}}, true
	case "lt":
		return []scalarInterval{{lo: -1e300, hi: c.Threshold, hiOpen: true}}, true
	case "lte":
		return []scalarInterval{{lo: -1e300, hi: c.Threshold}}, true
	case "eq":
		return []scalarInterval{{lo: c.Threshold, hi: c.Threshold}}, true
	case "neq":
		return []scalarInterval{{lo: -1e300, hi: c.Threshold, hiOpen: true},
			{lo: c.Threshold, hi: 1e300, loOpen: true}}, true
	}
	return nil, false
}

func scalarIntervalsOverlap(a, b scalarInterval) bool {
	lo, loOpen := a.lo, a.loOpen
	if b.lo > lo {
		lo, loOpen = b.lo, b.loOpen
	} else if b.lo == lo {
		loOpen = a.loOpen || b.loOpen
	}
	hi, hiOpen := a.hi, a.hiOpen
	if b.hi < hi {
		hi, hiOpen = b.hi, b.hiOpen
	} else if b.hi == hi {
		hiOpen = a.hiOpen || b.hiOpen
	}
	return lo < hi || (lo == hi && !loOpen && !hiOpen)
}

func purchasedConstraintsByMetric(p PurchasedPredicate) map[string][][]scalarInterval {
	out := map[string][][]scalarInterval{}
	for _, constraint := range p.Constraints {
		metric := normToken(constraint.Metric)
		ranges, ok := scalarConstraintRanges(constraint)
		if metric == "" || !ok {
			continue
		}
		out[metric] = append(out[metric], ranges)
	}
	return out
}

// metricFeasible intersects every AND-clause for one metric. Each clause may itself be a union
// (currently used for != / a purchased NO on a draw contract).
func metricFeasible(clauses [][]scalarInterval) bool {
	if len(clauses) == 0 {
		return true
	}
	current := append([]scalarInterval(nil), clauses[0]...)
	for _, clause := range clauses[1:] {
		next := make([]scalarInterval, 0, len(current)*len(clause))
		for _, left := range current {
			for _, right := range clause {
				if !scalarIntervalsOverlap(left, right) {
					continue
				}
				lo, loOpen := left.lo, left.loOpen
				if right.lo > lo {
					lo, loOpen = right.lo, right.loOpen
				} else if right.lo == lo {
					loOpen = left.loOpen || right.loOpen
				}
				hi, hiOpen := left.hi, left.hiOpen
				if right.hi < hi {
					hi, hiOpen = right.hi, right.hiOpen
				} else if right.hi == hi {
					hiOpen = left.hiOpen || right.hiOpen
				}
				next = append(next, scalarInterval{lo: lo, hi: hi, loOpen: loOpen, hiOpen: hiOpen})
			}
		}
		if len(next) == 0 {
			return false
		}
		current = next
	}
	return len(current) > 0
}

func EvaluatePurchasedIntersection(left, right PurchasedPredicate) IntersectionVerdict {
	left.EventID, right.EventID = strings.TrimSpace(left.EventID), strings.TrimSpace(right.EventID)
	if left.EventID == "" || right.EventID == "" || left.EventID != right.EventID {
		return IntersectionVerdict{ReasonCode: IntersectionDifferentEvent}
	}
	verdict := IntersectionVerdict{Related: true}
	if !left.Structured || !right.Structured {
		verdict.ReasonCode = IntersectionUnclassified
		return verdict
	}
	lm, rm := purchasedConstraintsByMetric(left), purchasedConstraintsByMetric(right)
	shared := false
	for metric, leftClauses := range lm {
		rightClauses, ok := rm[metric]
		if !ok {
			continue
		}
		shared = true
		verdict.Comparable = true
		if !metricFeasible(append(append([][]scalarInterval(nil), leftClauses...), rightClauses...)) {
			verdict.Contradictory = true
			verdict.ReasonCode = IntersectionEmpty
			verdict.Metric = metric
			return verdict
		}
	}
	if !shared {
		verdict.ReasonCode = IntersectionDifferentDimension
		return verdict
	}
	verdict.ReasonCode = IntersectionFeasible
	return verdict
}

func scorePredicateOK(p PurchasedPredicate, home, away float64) bool {
	if len(p.ScoreAlternatives) == 0 {
		return true
	}
	for _, alternative := range p.ScoreAlternatives {
		ok := len(alternative) > 0
		for _, constraint := range alternative {
			value := constraint.HomeCoeff*home + constraint.AwayCoeff*away
			if !numericConstraintOK(value, constraint.Comparator, constraint.Threshold) {
				ok = false
				break
			}
		}
		if ok {
			return true
		}
	}
	return false
}

func scoreSearchMax(predicates []PurchasedPredicate) int {
	max := 500.0
	for _, predicate := range predicates {
		for _, alternative := range predicate.ScoreAlternatives {
			for _, constraint := range alternative {
				v := constraint.Threshold
				if v < 0 {
					v = -v
				}
				if v+100 > max {
					max = v + 100
				}
			}
		}
	}
	if max > 2500 {
		max = 2500
	}
	return int(max + 1)
}

func scoreSetFeasible(predicates []PurchasedPredicate) bool {
	max := scoreSearchMax(predicates)
	return scoreSetFeasibleAtMax(predicates, max)
}

func scoreSetFeasibleAtMax(predicates []PurchasedPredicate, max int) bool {
	for home := 0; home <= max; home++ {
		for away := 0; away <= max; away++ {
			ok := true
			for _, predicate := range predicates {
				if !scorePredicateOK(predicate, float64(home), float64(away)) {
					ok = false
					break
				}
			}
			if ok {
				return true
			}
		}
	}
	return false
}

func fullScorePeriod(period string) bool {
	switch normToken(period) {
	case "full", "full_game", "regulation", "match", "game":
		return true
	}
	return false
}

// scorePeriodsFeasible links a period score to its enclosing full-game score. Period markets are
// not independent outcomes: a first-half total over 3.5 cannot coexist with a full-game total
// under 2.5. The suffix table answers whether any feasible full-game score dominates a feasible
// component/prefix score for both participants. Different non-full periods remain separate unless
// an enclosing full-game predicate is present; inventing a relationship for maps/sets would be less
// safe than leaving the funded semantic adapter to reject an unclassified product.
func scorePeriodsFeasible(byPeriod map[string][]PurchasedPredicate, all []PurchasedPredicate) bool {
	var full []PurchasedPredicate
	nonFullPeriods := 0
	for period, group := range byPeriod {
		if fullScorePeriod(period) {
			full = append(full, group...)
		} else {
			nonFullPeriods++
		}
	}
	if len(full) == 0 || len(byPeriod) < 2 {
		return true
	}
	// Two different component periods cannot safely be checked independently against one full-game
	// constraint: Q1 > 50 and Q2 > 50 each fit beneath full < 90 in isolation, but not together.
	// Until the official adapter models additive period composition, refuse that funded set rather
	// than certify a false intersection.
	if nonFullPeriods > 1 {
		return false
	}
	max := scoreSearchMax(all)
	width := max + 1
	suffix := make([]bool, width*width)
	for home := max; home >= 0; home-- {
		for away := max; away >= 0; away-- {
			ok := true
			for _, predicate := range full {
				if !scorePredicateOK(predicate, float64(home), float64(away)) {
					ok = false
					break
				}
			}
			if home < max && suffix[(home+1)*width+away] {
				ok = true
			}
			if away < max && suffix[home*width+away+1] {
				ok = true
			}
			suffix[home*width+away] = ok
		}
	}
	for period, group := range byPeriod {
		if fullScorePeriod(period) {
			continue
		}
		compatible := false
		for home := 0; home <= max && !compatible; home++ {
			for away := 0; away <= max; away++ {
				ok := true
				for _, predicate := range group {
					if !scorePredicateOK(predicate, float64(home), float64(away)) {
						ok = false
						break
					}
				}
				if ok && suffix[home*width+away] {
					compatible = true
					break
				}
			}
		}
		if !compatible {
			return false
		}
	}
	return true
}

// EvaluatePurchasedSet checks the candidate together with the complete already-funded predicate
// set. This catches joint contradictions that pairwise checks miss (for example both team totals
// over a line plus a game total under their sum), including period scores that cannot fit inside
// the purchased full-game score.
func EvaluatePurchasedSet(predicates []PurchasedPredicate) IntersectionVerdict {
	if len(predicates) < 2 {
		return IntersectionVerdict{ReasonCode: IntersectionFeasible}
	}
	eventID := strings.TrimSpace(predicates[0].EventID)
	if eventID == "" {
		return IntersectionVerdict{Related: true, ReasonCode: IntersectionUnclassified}
	}
	byPeriod := map[string][]PurchasedPredicate{}
	for _, predicate := range predicates {
		if strings.TrimSpace(predicate.EventID) != eventID {
			return IntersectionVerdict{ReasonCode: IntersectionDifferentEvent}
		}
		period := normToken(predicate.Period)
		if !predicate.Structured || period == "" ||
			(len(predicate.ScoreAlternatives) == 0 && len(predicate.Constraints) == 0) {
			return IntersectionVerdict{Related: true, ReasonCode: IntersectionUnclassified}
		}
		byPeriod[period] = append(byPeriod[period], predicate)
	}
	verdict := IntersectionVerdict{Related: true, Comparable: true, ReasonCode: IntersectionFeasible}
	for period, group := range byPeriod {
		if !scoreSetFeasible(group) {
			verdict.Contradictory, verdict.ReasonCode, verdict.Metric = true, IntersectionEmpty, "score:"+period
			return verdict
		}
		metrics := map[string][][]scalarInterval{}
		for _, predicate := range group {
			for metric, clauses := range purchasedConstraintsByMetric(predicate) {
				metrics[metric] = append(metrics[metric], clauses...)
			}
		}
		for metric, clauses := range metrics {
			if !metricFeasible(clauses) {
				verdict.Contradictory, verdict.ReasonCode, verdict.Metric = true, IntersectionEmpty, metric
				return verdict
			}
		}
	}
	if !scorePeriodsFeasible(byPeriod, predicates) {
		verdict.Contradictory, verdict.ReasonCode, verdict.Metric = true, IntersectionEmpty, "score:cross_period"
		return verdict
	}
	return verdict
}
