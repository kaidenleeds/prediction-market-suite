// Package ev computes Kalshi trading fees and post-fee expected value. The
// quadratic trade fee is rounded up to $0.0001, then a balance-alignment charge
// (with the venue's accumulator/rebates across partial fills) restores cents:
//
//	taker (general):      ceil( 0.07   * C * P * (1-P) )
//	maker (general):      ceil( 0.0175 * C * P * (1-P) )   // a quarter of taker
//	S&P 500 / Nasdaq-100: coefficient 0.035 (taker), 0.00875 (maker)
//
// where C = contracts and P = price in dollars (0..1). Verified against Kalshi's
// published fee table (effective Feb 2026). Note: float math must be de-dusted
// before the ceil, or values like 0.07*100*0.5*0.5 = 1.7500000000000002 round up
// to $1.76 instead of the correct $1.75.
package ev

import "math"

type MarketClass int

const (
	General MarketClass = iota // 0.07 taker coefficient
	IndexSP                    // S&P 500 / Nasdaq-100: 0.035 taker coefficient
)

func takerCoeff(cls MarketClass) float64 {
	if cls == IndexSP {
		return 0.035
	}
	return 0.07
}

// Fee returns the all-in balance fee for a BUY order. The venue reports the quadratic trade fee
// at $0.0001 precision, then applies a sub-cent rounding fee (and accumulator rebates across
// partial fills) so the account balance remains cent-aligned. FeeCoeff models one equivalent
// aggregate fill, which is the correct projection for the suite's whole-contract orders.
// maker=true uses the maker coefficient (a quarter of the taker coefficient).
func Fee(cls MarketClass, maker bool, contracts int, price float64) float64 {
	if contracts <= 0 || price <= 0 || price >= 1 {
		return 0
	}
	coeff := takerCoeff(cls)
	if maker {
		coeff /= 4
	}
	return FeeCoeff(coeff, contracts, price)
}

// FeeCoeff is the quadratic Kalshi fee with an EXPLICIT coefficient — the venue-truth path
// (R70-B SCHEMA_AUDIT #2): per-series coefficients now come from GET /series/fee_changes
// (fee_multiplier × the 0.07 base; 0 = fee-free program series), so the fee formula can no
// longer assume the two hardcoded classes. Same de-dust + aggregate-fill rounding as Fee.
func FeeCoeff(coeff float64, contracts int, price float64) float64 {
	return FeeBreakdownCoeff(coeff, float64(contracts), price, true).Net
}

// FeeParts separates the receipt's trade fee from the balance-alignment rounding component.
type FeeParts struct {
	Trade    float64
	Rounding float64
	Net      float64
}

// FeeBreakdownCoeff models one equivalent aggregate fill. buy=false is the SELL balance path;
// the distinction matters for subpenny/fractional orders even though it collapses at whole-cent
// prices. A venue-side per-order accumulator makes many partial fills converge to this aggregate.
func FeeBreakdownCoeff(coeff, contracts, price float64, buy bool) FeeParts {
	if coeff <= 0 || contracts <= 0 || price <= 0 || price >= 1 {
		return FeeParts{}
	}
	centicents := coeff * contracts * price * (1 - price) * 10000
	centicents = math.Round(centicents*1e6) / 1e6 // strip float dust before ceil-to-$0.0001
	trade := math.Ceil(centicents) / 10000
	revenue := contracts * price
	if buy {
		revenue = -revenue
	}
	change := revenue - trade
	floored := math.Floor((change+1e-12)*100) / 100
	rounding := change - floored
	if rounding < 1e-12 {
		rounding = 0
	}
	return FeeParts{Trade: trade, Rounding: rounding, Net: trade + rounding}
}

// Trade describes a candidate YES position for EV analysis.
type Trade struct {
	Class     MarketClass
	Maker     bool
	Contracts int
	Price     float64 // entry price in dollars (cost per YES contract, 0..1)
	FairValue float64 // your estimated probability it resolves YES (0..1)
}

// Result is the post-fee economics of a trade.
type Result struct {
	Contracts     int     `json:"contracts"`
	Price         float64 `json:"price"`
	FairValue     float64 `json:"fair_value"`
	Fee           float64 `json:"fee"`               // total entry fee, dollars
	EdgePerC      float64 `json:"edge_per_contract"` // fair - price (before fee)
	EVPerContract float64 `json:"ev_per_contract"`   // after fee
	EVTotal       float64 `json:"ev_total"`          // after fee, all contracts
	CostBasis     float64 `json:"cost_basis"`        // price*contracts + fee
	Breakeven     float64 `json:"breakeven"`         // fair value needed for EV=0
}

// Evaluate computes post-fee EV for buying YES at Price with belief FairValue,
// held to settlement. A YES contract pays $1 if it resolves YES. Expected gross
// payout per contract = FairValue; cost = Price; the entry fee is amortized per
// contract. Kalshi charges no settlement fee, so holding to resolution incurs no
// second fee (selling early would — not modeled here).
func Evaluate(t Trade) Result {
	fee := Fee(t.Class, t.Maker, t.Contracts, t.Price)
	feePerC := 0.0
	if t.Contracts > 0 {
		feePerC = fee / float64(t.Contracts)
	}
	edge := t.FairValue - t.Price
	evPerC := edge - feePerC
	return Result{
		Contracts:     t.Contracts,
		Price:         t.Price,
		FairValue:     t.FairValue,
		Fee:           fee,
		EdgePerC:      edge,
		EVPerContract: evPerC,
		EVTotal:       evPerC * float64(t.Contracts),
		CostBasis:     t.Price*float64(t.Contracts) + fee,
		Breakeven:     t.Price + feePerC,
	}
}
