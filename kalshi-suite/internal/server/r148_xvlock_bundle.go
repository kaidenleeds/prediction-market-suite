package server

import (
	"context"
	"errors"
	"math"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/payoffsolver"
	"github.com/kalshi-suite/kalshi-suite/internal/resolutionbasis"
)

// captureR148XVLockBundle turns an already certified K-PUS scanner opportunity into the same
// immutable typed-bundle contract used by the staged coordinator. Polymarket Global remains
// research-only and is refused here. A visible price pair without an exact compatible resolution
// certificate, full two-sided books, exact fees, current clocks and known unwind is not emitted.
func (s *Server) captureR148XVLockBundle(ctx context.Context, op xvLockOpp, observed time.Time) (int, int, error) {
	if op.Pair != "K-PUS" || op.AVenue != "kalshi" || op.BVenue != "polyus" ||
		op.ResolutionBasis == nil || resolutionbasis.Verify(*op.ResolutionBasis) != nil ||
		!op.FeeAKnown || !op.FeeBKnown || op.ADepth < 1 || op.BDepth < 1 || op.MarginC <= 0 {
		return 0, 0, errors.New("xvlock candidate lacks exact K-PUS identity/book/fee/depth proof")
	}
	cert := *op.ResolutionBasis
	if !strings.EqualFold(cert.Left.Venue, op.AVenue) || cert.Left.InstrumentID != op.AID ||
		!strings.EqualFold(cert.Right.Venue, op.BVenue) || cert.Right.InstrumentID != op.BID ||
		cert.LeftSide != op.ASide || cert.RightSide != op.BSide {
		return 0, 0, errors.New("xvlock certificate orientation does not match scanner legs")
	}
	payoff := func(nativeSide, orientation, selected string) []float64 {
		representsBasis := strings.EqualFold(selected, nativeSide)
		if strings.EqualFold(orientation, "inverse") {
			representsBasis = !representsBasis
		}
		if representsBasis {
			return []float64{1, 0}
		}
		return []float64{0, 1}
	}
	leftPayoff := payoff(cert.Left.NativeSide, cert.Left.InstrumentOrientation, op.ASide)
	rightPayoff := payoff(cert.Right.NativeSide, cert.Right.InstrumentOrientation, op.BSide)
	if leftPayoff[0]+rightPayoff[0] != 1 || leftPayoff[1]+rightPayoff[1] != 1 {
		return 0, 0, errors.New("xvlock certificate does not define a one-dollar complementary payoff")
	}
	capacity := math.Min(op.ADepth, op.BDepth)
	sizes := r138SolverSizes(capacity)
	if len(sizes) == 0 {
		return 0, 0, errors.New("xvlock current depth has no supported executable size")
	}
	left, _, leftOK := s.r138KalshiSolverLeg(op.AID, op.ASide, cert.Left.PayoffID+"|"+op.ASide, leftPayoff, sizes)
	if !leftOK || s.polyUSWS == nil {
		return 0, 0, errors.New("xvlock Kalshi full book/fee curve is unavailable")
	}
	_, _, _, receivedAt, bookOK := s.polyUSWS.FullBookLevelsAt(op.BID)
	if !bookOK || receivedAt.IsZero() {
		return 0, 0, errors.New("xvlock PolyUS full book clock is unavailable")
	}
	quoteAge := time.Since(receivedAt).Seconds()
	if quoteAge < 0 || quoteAge > 3 {
		return 0, 0, errors.New("xvlock PolyUS full book is stale")
	}
	_, tick, tickKnown := s.polyUSFreshFeeAuthority(op.BID)
	right, _, rightOK := s.r138PolySolverLeg(op.BID, op.BSide, cert.Right.PayoffID+"|"+op.BSide,
		rightPayoff, op.BAsk, op.BDepth, tick, quoteAge, sizes)
	if !tickKnown || !rightOK {
		return 0, 0, errors.New("xvlock PolyUS full book/tick/fee curve is unavailable")
	}
	problem := payoffsolver.Problem{Certificate: payoffsolver.Certificate{
		CanonicalEventID: cert.Left.EventID, EventVersion: cert.Left.EventVersion,
		States: []string{"basis-true", "basis-false"}, RulesHash: cert.ContentHash,
		RelationsHash: cert.ContentHash, Verified: true, Complete: true,
	}, Legs: []payoffsolver.Leg{left, right}, Sizes: sizes, MaxLegs: 2,
		MaxQuoteAgeSeconds: 3, AtomicRoute: false}
	rows, err := payoffsolver.Evaluate(problem)
	if err != nil {
		return 0, 0, err
	}
	rows = r138AllLegSolutions(rows, 2)
	return s.recordR148NamedRouteBundles(ctx, observed, "xvlock", "directional-gap-resolution-audit",
		op.Key, cert.ContentHash, "verified",
		"cross-venue route is non-atomic; staged execution and untouched replication are required",
		problem, rows, map[string]any{"pair": op.Pair, "orientation": op.Orient,
			"resolution_certificate": cert, "scanner_margin_c": op.MarginC})
}
