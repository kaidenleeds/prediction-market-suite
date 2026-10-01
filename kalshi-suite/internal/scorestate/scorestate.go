// Package scorestate implements the frozen v1 marginal score-state model used by the
// score-state-surface research collector. It deliberately has no storage, network, Paper, or LIVE
// dependency. Discovery quotes may fit the latent marginal, but the target market is always left
// out and execution economics are applied by the caller from a later current book.
package scorestate

import (
	"errors"
	"math"
	"sort"
	"strings"
)

const ModelVersion = "score-state-surface-loo-logistic-v1"

// Predicate is one monotone binary claim about a single latent score variable. Margin is the
// canonical home-minus-away score; total is the canonical combined score. Props and joint claims
// are intentionally outside v1 because their correlations are not identified by winner/spread/
// total marginals.
type Predicate struct {
	Axis      string  `json:"axis"`
	Threshold float64 `json:"threshold"`
	Greater   bool    `json:"greater"`
}

type Market struct {
	ID        string    `json:"id"`
	Kind      string    `json:"kind"`
	Mid       float64   `json:"discovery_mid"`
	Predicate Predicate `json:"predicate"`
}

type Estimate struct {
	ModelVersion string   `json:"model_version"`
	Axis         string   `json:"axis"`
	Fair         float64  `json:"fair"`
	Lower        float64  `json:"lower"`
	Upper        float64  `json:"upper"`
	Uncertainty  float64  `json:"uncertainty_reserve"`
	TrainingN    int      `json:"training_n"`
	TrainingIDs  []string `json:"training_ids"`
	JointClaim   bool     `json:"joint_claim"`
}

type logisticFit struct {
	mu, scale, loss float64
}

func cleanAxis(v string) string {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "margin":
		return "margin"
	case "total":
		return "total"
	default:
		return ""
	}
}

func validMarket(v Market) bool {
	kind := strings.ToLower(strings.TrimSpace(v.Kind))
	axis := cleanAxis(v.Predicate.Axis)
	if strings.TrimSpace(v.ID) == "" || axis == "" || v.Mid <= 0 || v.Mid >= 1 ||
		math.IsNaN(v.Mid) || math.IsInf(v.Mid, 0) || math.IsNaN(v.Predicate.Threshold) ||
		math.IsInf(v.Predicate.Threshold, 0) {
		return false
	}
	if axis == "margin" {
		return kind == "winner" || kind == "spread"
	}
	return kind == "total"
}

func logisticCDF(x, mu, scale float64) float64 {
	z := (x - mu) / scale
	if z >= 40 {
		return 1
	}
	if z <= -40 {
		return 0
	}
	return 1 / (1 + math.Exp(-z))
}

func predict(p Predicate, mu, scale float64) float64 {
	cdf := logisticCDF(p.Threshold, mu, scale)
	if p.Greater {
		return 1 - cdf
	}
	return cdf
}

func clamp01(v float64) float64 {
	if v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}

func uniqueThresholds(rows []Market) int {
	seen := map[float64]bool{}
	for _, row := range rows {
		// A micro-unit canonicalization is deterministic and avoids treating representation noise as
		// an independent score constraint.
		seen[math.Round(row.Predicate.Threshold*1e6)/1e6] = true
	}
	return len(seen)
}

func samePredicate(a, b Predicate) bool {
	return cleanAxis(a.Axis) == cleanAxis(b.Axis) && a.Greater == b.Greater &&
		math.Abs(a.Threshold-b.Threshold) <= 1e-6
}

// FitLeaveOneOut fits a deterministic coherent marginal to every valid same-axis discovery quote
// except targetID. The target's Mid is never read after identity validation. The returned interval
// combines a near-optimal parameter set with the largest in-sample residual and a fixed one-point
// reserve; this is intentionally conservative research evidence, not a calibrated confidence
// interval or a joint score distribution.
func FitLeaveOneOut(markets []Market, targetID string) (Estimate, error) {
	targetID = strings.TrimSpace(targetID)
	var target Market
	found := false
	for _, row := range markets {
		if strings.TrimSpace(row.ID) == targetID {
			target, found = row, true
			break
		}
	}
	if !found || !validMarket(target) {
		return Estimate{}, errors.New("target is missing or unsupported")
	}
	axis := cleanAxis(target.Predicate.Axis)
	training := make([]Market, 0, len(markets)-1)
	seenID := map[string]bool{}
	for _, row := range markets {
		// Equivalent cross-venue listings of the target claim are also excluded. Otherwise a cloned
		// target quote could leak through a different ticker even though targetID itself is absent.
		if row.ID == targetID || samePredicate(row.Predicate, target.Predicate) || !validMarket(row) ||
			cleanAxis(row.Predicate.Axis) != axis || seenID[row.ID] {
			continue
		}
		seenID[row.ID] = true
		training = append(training, row)
	}
	if len(training) < 2 || uniqueThresholds(training) < 2 {
		return Estimate{}, errors.New("at least two distinct same-axis leave-one-out constraints are required")
	}
	sort.Slice(training, func(i, j int) bool { return training[i].ID < training[j].ID })

	minT, maxT := training[0].Predicate.Threshold, training[0].Predicate.Threshold
	for _, row := range training[1:] {
		minT = math.Min(minT, row.Predicate.Threshold)
		maxT = math.Max(maxT, row.Predicate.Threshold)
	}
	span := math.Max(1, maxT-minT)
	// Scale ratios cover tight soccer margins through wide basketball/total surfaces while staying
	// deterministic and data-unit agnostic.
	scaleRatios := []float64{0.05, 0.075, 0.1, 0.15, 0.2, 0.3, 0.45, 0.65, 0.9, 1.25, 1.75, 2.5, 3.5, 5}
	all := make([]logisticFit, 0, len(scaleRatios)*161)
	best := logisticFit{loss: math.Inf(1)}
	for _, ratio := range scaleRatios {
		scale := math.Max(0.05, span*ratio)
		lo, hi := minT-6*scale, maxT+6*scale
		for i := 0; i <= 160; i++ {
			mu := lo + (hi-lo)*float64(i)/160
			loss := 0.0
			for _, row := range training {
				d := predict(row.Predicate, mu, scale) - row.Mid
				loss += d * d
			}
			loss /= float64(len(training))
			fit := logisticFit{mu: mu, scale: scale, loss: loss}
			all = append(all, fit)
			if fit.loss < best.loss-1e-15 || (math.Abs(fit.loss-best.loss) <= 1e-15 &&
				(fit.scale < best.scale || (fit.scale == best.scale && fit.mu < best.mu))) {
				best = fit
			}
		}
	}
	if math.IsInf(best.loss, 1) {
		return Estimate{}, errors.New("latent fit failed")
	}
	// The near-optimal set prevents a brittle single-grid parameter from masquerading as certainty.
	tolerance := math.Max(0.0004, best.loss*0.25)
	point := predict(target.Predicate, best.mu, best.scale)
	low, high := point, point
	for _, fit := range all {
		if fit.loss <= best.loss+tolerance+1e-15 {
			p := predict(target.Predicate, fit.mu, fit.scale)
			low, high = math.Min(low, p), math.Max(high, p)
		}
	}
	maxResidual := 0.0
	for _, row := range training {
		maxResidual = math.Max(maxResidual, math.Abs(predict(row.Predicate, best.mu, best.scale)-row.Mid))
	}
	reserve := math.Min(0.49, maxResidual+0.01)
	ids := make([]string, len(training))
	for i := range training {
		ids[i] = training[i].ID
	}
	return Estimate{ModelVersion: ModelVersion, Axis: axis, Fair: clamp01(point),
		Lower: clamp01(low - reserve), Upper: clamp01(high + reserve), Uncertainty: reserve,
		TrainingN: len(training), TrainingIDs: ids, JointClaim: false}, nil
}
