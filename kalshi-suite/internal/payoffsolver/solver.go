// Package payoffsolver evaluates certified payoff-state portfolios at executable depth. It is a
// research primitive only: a positive floor is not an order instruction, and non-atomic routes
// retain their partial-fill loss explicitly.
package payoffsolver

import (
	"errors"
	"math"
	"sort"
	"strings"
)

// MaxSupportedLegs is the suite-wide execution/research ceiling. Keeping the primitive itself
// bounded prevents a caller from bypassing the six-leg contract enforced by Combo Lab, Paper,
// and LIVE Kalshi RFQs.
const MaxSupportedLegs = 6

type Certificate struct {
	CanonicalEventID string
	EventVersion     int
	States           []string
	RulesHash        string
	RelationsHash    string
	Verified         bool
	Complete         bool
}

type Level struct {
	Price, Quantity, FeePerShare float64
	// FeeQuotes carries authoritative nonlinear/rounded fee totals for the exact quantities the
	// bounded solver may consume at this price level. When present, an unmatched quantity fails
	// closed instead of multiplying a one-share fee through a quadratic schedule.
	FeeQuotes []FeeQuote
}

type FeeQuote struct {
	Quantity, Total float64
}

type Leg struct {
	ID, Venue, Ticker, Side, PayoffID            string
	BookSource, SourceClockID, FeeSource         string
	UnwindBookSource, UnwindFeeSource            string
	Payoff                                       []float64
	Levels, UnwindLevels                         []Level
	FullBookLevels, FullUnwindLevels             []Level
	QuoteAgeSeconds, TickSize, DecisionLatencyMS float64
}

type Problem struct {
	Certificate        Certificate
	Legs               []Leg
	Sizes              []float64
	MaxLegs            int
	MaxQuoteAgeSeconds float64
	AtomicRoute        bool
}

type Solution struct {
	LegIDs, Tickers                        []string
	Size, Cost, Fees                       float64
	PayoutFloor, NetFloor                  float64
	BottleneckDepth                        float64
	PartialFillWorstLoss, UnwindWorstLoss  float64
	AtomicRoute, UnwindKnown, ResearchOnly bool
	StatePayouts                           []float64
}

func validateProblem(p Problem) error {
	if !p.Certificate.Verified || !p.Certificate.Complete || p.Certificate.CanonicalEventID == "" ||
		p.Certificate.EventVersion <= 0 || len(p.Certificate.States) == 0 || p.Certificate.RulesHash == "" ||
		p.Certificate.RelationsHash == "" {
		return errors.New("verified complete payoff certificate required")
	}
	if len(p.Legs) < 2 || len(p.Legs) > 20 || len(p.Sizes) == 0 || p.MaxQuoteAgeSeconds <= 0 {
		return errors.New("invalid bounded solver problem")
	}
	if p.MaxLegs < 2 || p.MaxLegs > MaxSupportedLegs || p.MaxLegs > len(p.Legs) {
		return errors.New("invalid max-leg bound")
	}
	seen := map[string]bool{}
	for _, leg := range p.Legs {
		if leg.ID == "" || leg.Ticker == "" || leg.PayoffID == "" || seen[leg.ID] ||
			(leg.Side != "YES" && leg.Side != "NO") || len(leg.Payoff) != len(p.Certificate.States) ||
			leg.QuoteAgeSeconds < 0 || leg.QuoteAgeSeconds > p.MaxQuoteAgeSeconds || leg.TickSize <= 0 || len(leg.Levels) == 0 {
			return errors.New("invalid executable payoff leg")
		}
		seen[leg.ID] = true
		for _, x := range leg.Payoff {
			if x < 0 || !finite(x) {
				return errors.New("invalid payoff vector")
			}
		}
		for _, level := range leg.Levels {
			if level.Price <= 0 || level.Price >= 1 || level.Quantity <= 0 || level.FeePerShare < 0 ||
				!finite(level.Price) || !finite(level.Quantity) || !finite(level.FeePerShare) {
				return errors.New("invalid executable level")
			}
			lastQ := 0.0
			for _, q := range level.FeeQuotes {
				if q.Quantity <= lastQ || q.Quantity > level.Quantity+1e-9 || q.Total < 0 ||
					!finite(q.Quantity) || !finite(q.Total) {
					return errors.New("invalid exact fee quote")
				}
				lastQ = q.Quantity
			}
		}
	}
	for _, q := range p.Sizes {
		if q <= 0 || !finite(q) {
			return errors.New("invalid capacity size")
		}
	}
	return nil
}

func finite(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }

func levelFeeAt(level Level, q float64) (float64, bool) {
	if len(level.FeeQuotes) == 0 {
		return q * level.FeePerShare, true
	}
	for _, fq := range level.FeeQuotes {
		if math.Abs(fq.Quantity-q) <= 1e-9 {
			return fq.Total, true
		}
	}
	return 0, false
}

func legCostAt(leg Leg, q float64) (cost, fees float64, ok bool) {
	remaining := q
	for _, level := range leg.Levels {
		take := math.Min(remaining, level.Quantity)
		cost += take * level.Price
		fee, known := levelFeeAt(level, take)
		if !known {
			return 0, 0, false
		}
		fees += fee
		remaining -= take
		if remaining <= 1e-12 {
			return cost, fees, true
		}
	}
	return 0, 0, false
}

func legDepth(leg Leg) float64 {
	total := 0.0
	for _, level := range leg.Levels {
		total += level.Quantity
	}
	return total
}

func legProceedsAt(leg Leg, q float64) (proceeds, fees float64, ok bool) {
	remaining := q
	for _, level := range leg.UnwindLevels {
		take := math.Min(remaining, level.Quantity)
		proceeds += take * level.Price
		fee, known := levelFeeAt(level, take)
		if !known {
			return 0, 0, false
		}
		fees += fee
		remaining -= take
		if remaining <= 1e-12 {
			return proceeds, fees, true
		}
	}
	return 0, 0, false
}

// unwindEnvelope returns the worst executable mark-to-unwind result for any non-empty proper
// subset of legs. The separate partial-fill field retains the hold-to-zero bound; unavailable
// unwind books copy that conservative bound and remain explicitly unknown.
func unwindEnvelope(legs []Leg, chosen []int, q float64, entryCosts, entryFees []float64,
	partialWorst float64) (float64, bool) {
	if len(chosen) < 2 || len(chosen) > MaxSupportedLegs {
		return partialWorst, false
	}
	legResults := make([]float64, len(chosen))
	for i, idx := range chosen {
		proceeds, exitFee, ok := legProceedsAt(legs[idx], q)
		if !ok {
			return partialWorst, false
		}
		legResults[i] = proceeds - exitFee - entryCosts[i] - entryFees[i]
	}
	worst := 0.0
	full := (1 << len(chosen)) - 1
	for mask := 1; mask < full; mask++ {
		v := 0.0
		for i := range legResults {
			if mask&(1<<i) != 0 {
				v += legResults[i]
			}
		}
		if v < worst {
			worst = v
		}
	}
	return worst, true
}

// Evaluate enumerates every executable subset/size, including negative controls. Callers retain
// these rows prospectively; no-opportunity must not disappear merely because its net floor is <=0.
func Evaluate(p Problem) ([]Solution, error) {
	if err := validateProblem(p); err != nil {
		return nil, err
	}
	legs := append([]Leg(nil), p.Legs...)
	sort.Slice(legs, func(i, j int) bool { return legs[i].ID < legs[j].ID })
	sizes := append([]float64(nil), p.Sizes...)
	sort.Float64s(sizes)
	var out []Solution
	chosen := make([]int, 0, p.MaxLegs)
	var walk func(int)
	walk = func(next int) {
		if len(chosen) >= 2 {
			for _, q := range sizes {
				statePayouts := make([]float64, len(p.Certificate.States))
				cost, fees, capacity, executable := 0.0, 0.0, math.Inf(1), true
				ids, tickers := make([]string, 0, len(chosen)), make([]string, 0, len(chosen))
				entryCosts, entryFees := make([]float64, 0, len(chosen)), make([]float64, 0, len(chosen))
				for _, idx := range chosen {
					leg := legs[idx]
					lc, lf, ok := legCostAt(leg, q)
					if !ok {
						executable = false
						break
					}
					cost, fees = cost+lc, fees+lf
					entryCosts, entryFees = append(entryCosts, lc), append(entryFees, lf)
					capacity = math.Min(capacity, legDepth(leg))
					ids, tickers = append(ids, leg.ID), append(tickers, leg.Ticker)
					for state, payout := range leg.Payoff {
						statePayouts[state] += q * payout
					}
				}
				if !executable {
					continue
				}
				floor := statePayouts[0]
				for _, payout := range statePayouts[1:] {
					floor = math.Min(floor, payout)
				}
				net := floor - cost - fees
				partialWorst := -(cost + fees)
				unwindWorst, unwindKnown := unwindEnvelope(legs, chosen, q, entryCosts, entryFees, partialWorst)
				candidate := Solution{LegIDs: ids, Tickers: tickers, Size: q, Cost: cost, Fees: fees,
					PayoutFloor: floor, NetFloor: net, BottleneckDepth: capacity,
					PartialFillWorstLoss: partialWorst, UnwindWorstLoss: unwindWorst,
					AtomicRoute: p.AtomicRoute, UnwindKnown: unwindKnown,
					ResearchOnly: true, StatePayouts: statePayouts}
				out = append(out, candidate)
			}
		}
		if len(chosen) == p.MaxLegs {
			return
		}
		for i := next; i < len(legs); i++ {
			chosen = append(chosen, i)
			walk(i + 1)
			chosen = chosen[:len(chosen)-1]
		}
	}
	walk(0)
	return out, nil
}

// Solve returns the strongest positive fee-net floor from Evaluate. The objective is the largest
// worst-state fee-net floor, never point EV or displayed-depth multiplication.
func Solve(p Problem) (Solution, bool, error) {
	rows, err := Evaluate(p)
	if err != nil {
		return Solution{}, false, err
	}
	best, found := Solution{}, false
	for _, candidate := range rows {
		if candidate.NetFloor > 0 && (!found || candidate.NetFloor > best.NetFloor+1e-12 ||
			(math.Abs(candidate.NetFloor-best.NetFloor) <= 1e-12 && candidate.Cost+candidate.Fees < best.Cost+best.Fees)) {
			best, found = candidate, true
		}
	}
	return best, found, nil
}

// ExclusiveOutcomeLegs builds the exact-state YES simplex for an authoritative mutually-exclusive,
// exhaustive outcome set. Titles are not accepted as identity; callers supply payoff IDs and books.
func ExclusiveOutcomeLegs(outcomeIDs []string, books map[string]Leg) ([]Leg, error) {
	if len(outcomeIDs) < 2 {
		return nil, errors.New("exclusive outcome set too small")
	}
	legs := make([]Leg, 0, len(outcomeIDs))
	for i, id := range outcomeIDs {
		if strings.TrimSpace(id) == "" {
			return nil, errors.New("blank payoff identity")
		}
		leg, ok := books[id]
		if !ok || leg.PayoffID != id {
			return nil, errors.New("missing exact payoff book")
		}
		leg.Payoff = make([]float64, len(outcomeIDs))
		leg.Payoff[i] = 1
		legs = append(legs, leg)
	}
	return legs, nil
}
