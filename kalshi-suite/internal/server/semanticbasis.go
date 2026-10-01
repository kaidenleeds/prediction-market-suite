package server

import (
	"context"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/payoffsolver"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

type semanticBasisPairView struct {
	LeftVenue, LeftTicker, RightVenue, RightTicker string
	EventID, LeftPayoffID, RightPayoffID           string
	State, Kind, LeftSide, RightSide, Blocker      string
	SolverEligible                                 bool
}

type semanticBasisReport struct {
	GeneratedAt                              string
	Instruments, Relations, Pairs, Eligible  int
	Truncated, InputTruncated, PairBudgetHit bool
	States                                   map[string]int
	Rows                                     []semanticBasisPairView
}

const semanticBasisPairBudget = 50000
const semanticBasisInstrumentBudget = 50000

func semanticInstrument(in storage.SemanticInstrumentView) payoffsolver.SemanticInstrument {
	return payoffsolver.SemanticInstrument{Venue: in.Venue, Ticker: in.Ticker, EventID: in.EventID,
		PayoffID: in.PayoffID, NativeSide: in.NativeSide, Orientation: in.Orientation,
		SettlementSource: in.SettlementSource, RulesHash: in.RulesHash, CrossVenueBasis: in.CrossVenueBasis,
		FeeAuthority: in.FeeAuthority, IdentityStatus: in.IdentityStatus,
		EventVersion: in.EventVersion, PayoffVersion: in.PayoffVersion}
}

func semanticRelation(in storage.SemanticRelationView) payoffsolver.SemanticRelation {
	return payoffsolver.SemanticRelation{EventID: in.EventID, LeftPayoffID: in.LeftPayoffID,
		RightPayoffID: in.RightPayoffID, RelationType: in.RelationType,
		IdentityStatus: in.IdentityStatus, EventVersion: in.EventVersion,
		LeftPayoffVersion: in.LeftPayoffVersion, RightPayoffVersion: in.RightPayoffVersion,
		PayoutFloor: in.PayoutFloor}
}

func (s *Server) buildSemanticBasisReport(ctx context.Context, rowLimit int) (semanticBasisReport, error) {
	instruments, relationRows, err := s.store.SemanticBasisInputs(ctx, semanticBasisInstrumentBudget)
	out := semanticBasisReport{GeneratedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Instruments: len(instruments), Relations: len(relationRows), States: map[string]int{},
		InputTruncated: len(instruments) >= semanticBasisInstrumentBudget}
	if err != nil {
		return out, err
	}
	if rowLimit <= 0 || rowLimit > 2000 {
		rowLimit = 500
	}
	relations := make([]payoffsolver.SemanticRelation, 0, len(relationRows))
	for _, relation := range relationRows {
		relations = append(relations, semanticRelation(relation))
	}
	groups := map[string][]storage.SemanticInstrumentView{}
	for _, instrument := range instruments {
		// Compare every cross-venue instrument under the immutable event. Basis mismatches must be
		// counted as explicit blockers rather than disappearing before the semantic router sees them.
		key := instrument.EventID
		groups[key] = append(groups[key], instrument)
	}
	keys := make([]string, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	pairBudgetReached := false
	for _, key := range keys {
		group := groups[key]
		for i := 0; i < len(group); i++ {
			for j := i + 1; j < len(group); j++ {
				if strings.EqualFold(group[i].Venue, group[j].Venue) {
					continue
				}
				if out.Pairs >= semanticBasisPairBudget {
					out.PairBudgetHit, out.Truncated, pairBudgetReached = true, true, true
					break
				}
				route := payoffsolver.RouteSemanticBasis(semanticInstrument(group[i]), semanticInstrument(group[j]), relations)
				out.Pairs++
				out.States[route.State]++
				if route.SolverEligible {
					out.Eligible++
				}
				if len(out.Rows) < rowLimit {
					out.Rows = append(out.Rows, semanticBasisPairView{LeftVenue: group[i].Venue,
						LeftTicker: group[i].Ticker, RightVenue: group[j].Venue, RightTicker: group[j].Ticker,
						EventID: group[i].EventID, LeftPayoffID: group[i].PayoffID, RightPayoffID: group[j].PayoffID,
						State: route.State, Kind: route.Kind, LeftSide: route.LeftSide, RightSide: route.RightSide,
						Blocker: route.Blocker, SolverEligible: route.SolverEligible})
				} else {
					out.Truncated = true
				}
			}
			if pairBudgetReached {
				break
			}
		}
		if pairBudgetReached {
			break
		}
	}
	return out, nil
}

func (s *Server) handleSemanticBasis(w http.ResponseWriter, r *http.Request) {
	report, err := s.buildSemanticBasisReport(r.Context(), 500)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "semantic basis unavailable",
			"research_only": true, "funded": false, "paper_authority": false, "live_authority": false})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"generated_at": report.GeneratedAt, "research_only": true, "funded": false,
		"paper_authority": false, "live_authority": false,
		"state":       "PARTIAL_BLOCKED_UNTIL_NORMALIZED_RULE_CERTIFICATES_AND_ROUTE_INTEGRATION",
		"policy":      "solver_eligible means semantic-certificate eligible only; current books, exact fees, clocks, partial-fill loss, OOS replication and risk remain mandatory",
		"instruments": report.Instruments, "relations": report.Relations, "pairs": report.Pairs,
		"solver_eligible": report.Eligible, "states": report.States, "truncated": report.Truncated,
		"input_truncated": report.InputTruncated, "pair_budget": semanticBasisPairBudget,
		"pair_budget_hit": report.PairBudgetHit,
		"rows":            report.Rows,
	})
}

func (s *Server) sweepSemanticBasis(ctx context.Context) {
	started := time.Now()
	report, err := s.buildSemanticBasisReport(ctx, 1)
	receipt := storage.CollectorReceipt{CollectorID: "semantic-basis-router",
		CycleID:      "semantic-basis-" + started.UTC().Truncate(5*time.Minute).Format("20060102T1504Z"),
		ExperimentID: "payoff-constraint-solver", ExperimentVersion: 1, Started: started,
		Completed: time.Now(), Status: "healthy", Source: "newest immutable event/payoff/instrument/relation certificates; never titles",
		SchemaVersion: "semantic-basis-r138-v1", ExpectedCadence: 5 * time.Minute,
		Systems:    []string{"payoff-constraint-solver", "identity-challenged-cross-venue-lock"},
		Exclusions: map[string]int{}, Metrics: map[string]any{}}
	if err != nil {
		receipt.Status, receipt.ErrorClass, receipt.ErrorText = "blocked", "storage", "semantic certificate read failed"
	} else {
		receipt.Eligible, receipt.Attempted = report.Eligible, report.Pairs
		receipt.Metrics = map[string]any{"instruments": report.Instruments, "relations": report.Relations,
			"pairs": report.Pairs, "solver_eligible": report.Eligible, "states": report.States,
			"input_truncated": report.InputTruncated, "pair_budget": semanticBasisPairBudget,
			"pair_budget_hit": report.PairBudgetHit}
		receipt.Exclusions["explicitly_blocked"] = report.Pairs - report.Eligible
		if report.InputTruncated || report.PairBudgetHit {
			receipt.Status, receipt.ErrorClass = "blocked", "bounded_input"
			receipt.ErrorText = "semantic router hit its bounded input/pair budget; no completeness claim is allowed"
		} else if report.Eligible == 0 {
			receipt.Status, receipt.ExpectedZero = "healthy_empty", true
			receipt.ZeroReason = "all pairs absent or explicitly blocked by immutable semantic evidence"
		}
	}
	dctx, cancel := researchDurabilityContext(ctx)
	defer cancel()
	_, _ = s.store.InsertCollectorReceipt(dctx, receipt)
}
