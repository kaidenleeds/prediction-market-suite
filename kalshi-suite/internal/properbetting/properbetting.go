// Package properbetting implements the position-vector construction in Gu, Kagan, Sun, Wu,
// and Xu (2026), "When do prophets profit in prediction markets?"  It is deliberately pure:
// venue adapters provide immutable books, fee functions, and actual fill updates.
package properbetting

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
)

const probabilityEpsilon = 1e-12

type Transform string

const (
	Brier     Transform = "brier"
	Log       Transform = "log"
	Spherical Transform = "spherical"
)

type Level struct {
	Price    float64 `json:"price"`
	Quantity float64 `json:"quantity"`
	Tick     float64 `json:"tick,omitempty"`
}

// Position is s_G(p,q)=gradient G(p)-gradient G(q). Canonical subtracts the smallest component.
// In a frictionless complete simplex that constant complete-set shift has equal cost and payout.
// It is NOT free on a CLOB with spread/fees: execution must retain Raw as the paper truth and may
// use Canonical only after pricing every outcome of the complete set. Scale is kept explicit.
type Position struct {
	Transform Transform `json:"transform"`
	Forecast  []float64 `json:"forecast"`
	Market    []float64 `json:"market"`
	Raw       []float64 `json:"raw"`
	Canonical []float64 `json:"canonical"`
	Shift     float64   `json:"constant_shift"`
	Scale     float64   `json:"scale"`
}

type CoordinateAction struct {
	Index        int       `json:"index"`
	Side         string    `json:"side"`
	ForecastSide float64   `json:"forecast_side"`
	Qtilde       float64   `json:"q_tilde"`
	RawBinary    []float64 `json:"raw_binary"`
	Target       float64   `json:"target_quantity"`
	NoTrade      bool      `json:"no_trade"`
}

type DepthEvaluation struct {
	Requested      float64 `json:"requested"`
	RoundedRequest float64 `json:"rounded_request"`
	Filled         float64 `json:"filled"`
	Cancelled      float64 `json:"cancelled"`
	SpotPrice      float64 `json:"spot_price"`
	AveragePrice   float64 `json:"average_price"`
	IntegratedCost float64 `json:"integrated_cost"`
	SpotCost       float64 `json:"spot_cost"`
	LiquidityLoss  float64 `json:"liquidity_loss"`
	Fee            float64 `json:"fee"`
	ExpectedNet    float64 `json:"expected_net"`
	LevelsUsed     int     `json:"levels_used"`
	FullFill       bool    `json:"full_fill"`
	Fills          []Fill  `json:"fills"`
	MinimumTick    float64 `json:"minimum_tick"`
}

type SaleEvaluation struct {
	Requested       float64 `json:"requested"`
	RoundedRequest  float64 `json:"rounded_request"`
	Filled          float64 `json:"filled"`
	Cancelled       float64 `json:"cancelled"`
	SpotPrice       float64 `json:"spot_price"`
	AveragePrice    float64 `json:"average_price"`
	IntegratedValue float64 `json:"integrated_proceeds"`
	SpotValue       float64 `json:"spot_proceeds"`
	LiquidityLoss   float64 `json:"liquidity_loss"`
	Fee             float64 `json:"fee"`
	ExpectedNet     float64 `json:"expected_net"`
	LevelsUsed      int     `json:"levels_used"`
	FullFill        bool    `json:"full_fill"`
	Fills           []Fill  `json:"fills"`
	MinimumTick     float64 `json:"minimum_tick"`
}

type Fill struct {
	Price    float64 `json:"price"`
	Quantity float64 `json:"quantity"`
}

// FeeFunc receives the route's entire execution set at once. That lets the authoritative adapter
// apply venue-correct aggregate-order, per-fill, per-contract, capped, or rebate rounding instead
// of assuming fees round independently at each depth level.
type FeeFunc func(fills []Fill) (fee float64, known bool)

func finite(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }

func validateDistribution(v []float64, interior bool) error {
	if len(v) < 2 {
		return errors.New("proper position needs at least two outcomes")
	}
	sum := 0.0
	for _, x := range v {
		if !finite(x) || x < 0 || x > 1 || interior && (x <= 0 || x >= 1) {
			return errors.New("invalid probability vector")
		}
		sum += x
	}
	if math.Abs(sum-1) > 1e-9 {
		return fmt.Errorf("probability vector sum %.12f is not one", sum)
	}
	return nil
}

func gradient(transform Transform, v []float64) ([]float64, error) {
	interior := transform == Log
	if err := validateDistribution(v, interior); err != nil {
		return nil, err
	}
	out := make([]float64, len(v))
	switch transform {
	case Brier:
		for i := range v {
			out[i] = 2 * v[i]
		}
	case Log:
		for i := range v {
			out[i] = math.Log(v[i]) + 1 // the +1 cancels in the gradient difference
		}
	case Spherical:
		norm := 0.0
		for _, x := range v {
			norm += x * x
		}
		norm = math.Sqrt(norm)
		if norm <= 0 {
			return nil, errors.New("zero spherical norm")
		}
		for i := range v {
			out[i] = v[i] / norm
		}
	default:
		return nil, fmt.Errorf("unknown proper transform %q", transform)
	}
	return out, nil
}

func NewPosition(transform Transform, p, q []float64) (Position, error) {
	if len(p) != len(q) {
		return Position{}, errors.New("forecast and market dimensions differ")
	}
	gp, err := gradient(transform, p)
	if err != nil {
		return Position{}, err
	}
	gq, err := gradient(transform, q)
	if err != nil {
		return Position{}, err
	}
	raw := make([]float64, len(p))
	minimum := math.Inf(1)
	for i := range raw {
		raw[i] = gp[i] - gq[i]
		minimum = math.Min(minimum, raw[i])
	}
	canonical := make([]float64, len(raw))
	for i := range raw {
		canonical[i] = raw[i] - minimum
	}
	return Position{Transform: transform, Forecast: append([]float64(nil), p...),
		Market: append([]float64(nil), q...), Raw: raw, Canonical: canonical,
		Shift: -minimum, Scale: 1}, nil
}

func (p Position) Rescaled(scale float64) (Position, error) {
	if !finite(scale) || scale <= 0 {
		return Position{}, errors.New("proper position scale must be positive")
	}
	out := p
	out.Raw, out.Canonical = append([]float64(nil), p.Raw...), append([]float64(nil), p.Canonical...)
	for i := range out.Raw {
		out.Raw[i] *= scale
		out.Canonical[i] *= scale
	}
	out.Shift, out.Scale = p.Shift*scale, p.Scale*scale
	return out, nil
}

// BidAskActions applies Appendix C.1 coordinate by coordinate. yesAsk[k] is q+_k; noAsk[k]
// is q-_k. A coordinate buys YES when p>q+, buys NO when p<1-q-, and otherwise lies in the
// spread-induced no-trade zone. Each selected coordinate retains its exact q-tilde.
func BidAskActions(transform Transform, forecast, yesAsk, noAsk []float64) ([]CoordinateAction, error) {
	if len(forecast) == 0 || len(forecast) != len(yesAsk) || len(forecast) != len(noAsk) {
		return nil, errors.New("bid-ask vector dimensions differ")
	}
	out := make([]CoordinateAction, len(forecast))
	for i, p := range forecast {
		yp, np := yesAsk[i], noAsk[i]
		if !finite(p) || !finite(yp) || !finite(np) || p < 0 || p > 1 || yp <= 0 || yp >= 1 || np <= 0 || np >= 1 || yp+np < 1-1e-9 {
			return nil, fmt.Errorf("invalid bid-ask coordinate %d", i)
		}
		a := CoordinateAction{Index: i, Side: "NONE", ForecastSide: p, Qtilde: p, NoTrade: true}
		q := 0.0
		switch {
		case p > yp:
			a.Side, a.ForecastSide, a.Qtilde, a.NoTrade = "YES", p, yp, false
			q = yp
		case p < 1-np:
			a.Side, a.ForecastSide, a.Qtilde, a.NoTrade = "NO", 1-p, 1-np, false
			q = 1 - np
		default:
			out[i] = a
			continue
		}
		pos, err := NewPosition(transform, []float64{p, 1 - p}, []float64{q, 1 - q})
		if err != nil {
			return nil, err
		}
		a.RawBinary = pos.Raw
		if a.Side == "YES" {
			a.Target = pos.Canonical[0]
		} else {
			a.Target = pos.Canonical[1]
		}
		if !finite(a.Target) || a.Target <= 0 {
			return nil, fmt.Errorf("non-positive proper target at coordinate %d", i)
		}
		out[i] = a
	}
	return out, nil
}

// NormalizeWeights preserves the paper's rescaling equivalence while making the empirical
// allocation contract explicit: sum(abs(weight)) equals exposure. It never selects a direction.
func NormalizeWeights(raw []float64, exposure float64) ([]float64, error) {
	if !finite(exposure) || exposure <= 0 || len(raw) == 0 {
		return nil, errors.New("invalid proper allocation exposure")
	}
	total := 0.0
	for _, x := range raw {
		if !finite(x) {
			return nil, errors.New("non-finite proper allocation")
		}
		total += math.Abs(x)
	}
	if total <= probabilityEpsilon {
		return nil, errors.New("zero proper allocation")
	}
	out := make([]float64, len(raw))
	for i := range raw {
		out[i] = raw[i] * exposure / total
	}
	return out, nil
}

func alignedToTick(price, tick float64) bool {
	if tick <= 0 || !finite(tick) {
		return false
	}
	r := price / tick
	return math.Abs(r-math.Round(r)) <= 1e-7
}

// WalkDepth integrates the piecewise-constant CLOB cost. The exact fill set is passed once to the
// venue adapter so aggregate-order, per-fill, capped, rebated, or other fee schedules are not
// approximated by an average-price formula. Any unfilled remainder is a cancellation with zero
// payoff and zero fee.
func WalkDepth(levels []Level, requested, forecastSide, tick, lot float64, fee FeeFunc) (DepthEvaluation, error) {
	var out DepthEvaluation
	if !finite(requested) || requested <= 0 || !finite(forecastSide) || forecastSide < 0 || forecastSide > 1 ||
		!finite(tick) || tick <= 0 || !finite(lot) || lot <= 0 || fee == nil {
		return out, errors.New("invalid depth-walk contract")
	}
	out.Requested = requested
	out.RoundedRequest = math.Floor((requested+1e-12)/lot) * lot
	if out.RoundedRequest <= 0 {
		return out, errors.New("proper target is below the venue lot")
	}
	book := append([]Level(nil), levels...)
	sort.SliceStable(book, func(i, j int) bool { return book[i].Price < book[j].Price })
	remaining := out.RoundedRequest
	for _, level := range book {
		if remaining <= 1e-12 {
			break
		}
		levelTick := tick
		if level.Tick > 0 {
			levelTick = level.Tick
		}
		if !finite(level.Price) || !finite(level.Quantity) || !finite(levelTick) || level.Price <= 0 || level.Price >= 1 ||
			level.Quantity <= 0 || levelTick <= 0 || !alignedToTick(level.Price, levelTick) {
			return DepthEvaluation{}, errors.New("invalid or off-tick depth level")
		}
		if out.MinimumTick == 0 || levelTick < out.MinimumTick {
			out.MinimumTick = levelTick
		}
		q := math.Min(remaining, level.Quantity)
		if out.LevelsUsed == 0 {
			out.SpotPrice = level.Price
		}
		out.LevelsUsed++
		out.Filled += q
		out.IntegratedCost += q * level.Price
		out.Fills = append(out.Fills, Fill{Price: level.Price, Quantity: q})
		remaining -= q
	}
	out.Cancelled = math.Max(0, out.RoundedRequest-out.Filled)
	out.FullFill = out.Cancelled <= 1e-9
	if out.Filled <= 0 {
		return out, errors.New("no executable depth")
	}
	var known bool
	out.Fee, known = fee(append([]Fill(nil), out.Fills...))
	if !known || !finite(out.Fee) || out.Fee <= -out.Filled {
		return DepthEvaluation{}, errors.New("exact route fee unavailable")
	}
	out.AveragePrice = out.IntegratedCost / out.Filled
	out.SpotCost = out.Filled * out.SpotPrice
	out.LiquidityLoss = out.IntegratedCost - out.SpotCost
	if out.LiquidityLoss < -1e-9 {
		return DepthEvaluation{}, errors.New("negative liquidity loss from malformed book")
	}
	out.ExpectedNet = out.Filled*forecastSide - out.IntegratedCost - out.Fee
	return out, nil
}

// WalkSaleDepth is the exact bid-side twin used by the zero-authority momentum simulator. It
// values only actual simulated fills, charges the venue's SELL fee contract, and treats every
// unfilled remainder as a cancellation. Liquidity loss is top-bid proceeds minus integrated
// proceeds, so a malformed ascending bid ladder cannot create fictitious improvement.
func WalkSaleDepth(levels []Level, requested, forecastSide, tick, lot float64, fee FeeFunc) (SaleEvaluation, error) {
	var out SaleEvaluation
	if !finite(requested) || requested <= 0 || !finite(forecastSide) || forecastSide < 0 || forecastSide > 1 ||
		!finite(tick) || tick <= 0 || !finite(lot) || lot <= 0 || fee == nil {
		return out, errors.New("invalid sale depth-walk contract")
	}
	out.Requested = requested
	out.RoundedRequest = math.Floor((requested+1e-12)/lot) * lot
	if out.RoundedRequest <= 0 {
		return out, errors.New("proper sale target is below the venue lot")
	}
	book := append([]Level(nil), levels...)
	sort.SliceStable(book, func(i, j int) bool { return book[i].Price > book[j].Price })
	remaining := out.RoundedRequest
	for _, level := range book {
		if remaining <= 1e-12 {
			break
		}
		levelTick := tick
		if level.Tick > 0 {
			levelTick = level.Tick
		}
		if !finite(level.Price) || !finite(level.Quantity) || !finite(levelTick) || level.Price <= 0 ||
			level.Price >= 1 || level.Quantity <= 0 || levelTick <= 0 || !alignedToTick(level.Price, levelTick) {
			return SaleEvaluation{}, errors.New("invalid or off-tick sale depth level")
		}
		if out.MinimumTick == 0 || levelTick < out.MinimumTick {
			out.MinimumTick = levelTick
		}
		q := math.Min(remaining, level.Quantity)
		if out.LevelsUsed == 0 {
			out.SpotPrice = level.Price
		}
		out.LevelsUsed++
		out.Filled += q
		out.IntegratedValue += q * level.Price
		out.Fills = append(out.Fills, Fill{Price: level.Price, Quantity: q})
		remaining -= q
	}
	out.Cancelled = math.Max(0, out.RoundedRequest-out.Filled)
	out.FullFill = out.Cancelled <= 1e-9
	if out.Filled <= 0 {
		return out, errors.New("no executable sale depth")
	}
	var known bool
	out.Fee, known = fee(append([]Fill(nil), out.Fills...))
	if !known || !finite(out.Fee) || out.Fee <= -out.Filled {
		return SaleEvaluation{}, errors.New("exact sale route fee unavailable")
	}
	out.AveragePrice = out.IntegratedValue / out.Filled
	out.SpotValue = out.Filled * out.SpotPrice
	out.LiquidityLoss = out.SpotValue - out.IntegratedValue
	if out.LiquidityLoss < -1e-9 {
		return SaleEvaluation{}, errors.New("negative sale liquidity loss from malformed book")
	}
	out.ExpectedNet = out.IntegratedValue - out.Fee - out.Filled*forecastSide
	return out, nil
}

type Rebalance struct {
	PriorActual float64 `json:"prior_actual"`
	Target      float64 `json:"target"`
	Requested   float64 `json:"requested_delta"`
	Filled      float64 `json:"filled_delta"`
	Cancelled   float64 `json:"cancelled_delta"`
	PostActual  float64 `json:"post_actual"`
}

type RebalanceLeg struct {
	Action   string  `json:"action"`
	Side     string  `json:"side"`
	Quantity float64 `json:"quantity"`
}

// PlanRebalance makes side crossings explicit. A +YES to -NO transition is two venue routes:
// close the filled YES position, then buy NO. Each leg must receive its own book, fee, and fill.
func PlanRebalance(priorActual, target float64) ([]RebalanceLeg, error) {
	if !finite(priorActual) || !finite(target) {
		return nil, errors.New("non-finite rebalance")
	}
	var out []RebalanceLeg
	appendLeg := func(action, side string, qty float64) {
		if qty > 1e-12 {
			out = append(out, RebalanceLeg{Action: action, Side: side, Quantity: qty})
		}
	}
	if priorActual > 0 && target < 0 {
		appendLeg("SELL", "YES", priorActual)
		appendLeg("BUY", "NO", -target)
		return out, nil
	}
	if priorActual < 0 && target > 0 {
		appendLeg("SELL", "NO", -priorActual)
		appendLeg("BUY", "YES", target)
		return out, nil
	}
	switch {
	case target >= 0 && target >= priorActual:
		appendLeg("BUY", "YES", target-priorActual)
	case target >= 0:
		appendLeg("SELL", "YES", priorActual-target)
	case target <= 0 && target <= priorActual:
		appendLeg("BUY", "NO", priorActual-target)
	default:
		appendLeg("SELL", "NO", target-priorActual)
	}
	return out, nil
}

// ApplyRebalance computes the next momentum target only from the actually filled position.
// Positive positions are YES, negative positions are NO. A canceled order has filledDelta=0 and
// therefore cannot move position. Overshoots and opposite-direction fills fail closed.
func ApplyRebalance(priorActual, target, filledDelta float64) (Rebalance, error) {
	if !finite(priorActual) || !finite(target) || !finite(filledDelta) {
		return Rebalance{}, errors.New("non-finite rebalance")
	}
	requested := target - priorActual
	if priorActual*target < 0 {
		return Rebalance{}, errors.New("side crossing requires separately priced close and open legs")
	}
	if math.Abs(filledDelta) > math.Abs(requested)+1e-9 || requested*filledDelta < -1e-12 {
		return Rebalance{}, errors.New("fill is inconsistent with requested rebalance")
	}
	return Rebalance{PriorActual: priorActual, Target: target, Requested: requested,
		Filled: filledDelta, Cancelled: requested - filledDelta, PostActual: priorActual + filledDelta}, nil
}

func Potential(transform Transform, p []float64) (float64, error) {
	if err := validateDistribution(p, transform == Log); err != nil {
		return 0, err
	}
	switch transform {
	case Brier:
		v := -1.0
		for _, x := range p {
			v += x * x
		}
		return v, nil
	case Log:
		v := 0.0
		for _, x := range p {
			v += x * math.Log(x)
		}
		return v, nil
	case Spherical:
		v := 0.0
		for _, x := range p {
			v += x * x
		}
		return math.Sqrt(v), nil
	default:
		return 0, fmt.Errorf("unknown proper transform %q", transform)
	}
}

func Score(transform Transform, p []float64, outcome int) (float64, error) {
	if outcome < 0 || outcome >= len(p) {
		return 0, errors.New("outcome outside score vector")
	}
	g, err := Potential(transform, p)
	if err != nil {
		return 0, err
	}
	grad, err := gradient(transform, p)
	if err != nil {
		return 0, err
	}
	dot := 0.0
	for i := range p {
		y := 0.0
		if i == outcome {
			y = 1
		}
		dot += grad[i] * (y - p[i])
	}
	return g + dot, nil
}

func Bregman(transform Transform, q, p []float64) (float64, error) {
	if len(p) != len(q) {
		return 0, errors.New("Bregman dimensions differ")
	}
	gq, err := Potential(transform, q)
	if err != nil {
		return 0, err
	}
	gp, err := Potential(transform, p)
	if err != nil {
		return 0, err
	}
	gradp, err := gradient(transform, p)
	if err != nil {
		return 0, err
	}
	dot := 0.0
	for i := range p {
		dot += gradp[i] * (q[i] - p[i])
	}
	return gq - gp - dot, nil
}

func ParseTransform(raw string) (Transform, error) {
	t := Transform(strings.ToLower(strings.TrimSpace(raw)))
	switch t {
	case Brier, Log, Spherical:
		return t, nil
	default:
		return "", fmt.Errorf("unknown proper transform %q", raw)
	}
}
