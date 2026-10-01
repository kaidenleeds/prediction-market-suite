package server

import "strings"

// clienteleExactArm is one executable representation of the same certified binary payoff. Native
// side is the side submitted to that venue. CanonicalSide keeps YES/NO performance comparable
// when the cross-venue certificate says the PolyUS instrument is inverted.
type clienteleExactArm struct {
	Venue, NativeSide, CanonicalSide, SelectorArm string
	Leg                                           nativeExactLeg
}

// concreteClienteleExactArms enumerates all four independently executable single-order choices
// from one current certified K-PUS pair. The old collector kept only the canonical-YES expression
// (Kalshi YES and its PolyUS equivalent), which left the complementary side disconnected. This
// helper never chooses an arm and never grants authority; it only supplies exact same-clock
// controls to the existing prior-day frozen selector.
func concreteClienteleExactArms(orientation string, kalshi, polyus []nativeExactLeg) []clienteleExactArm {
	bySide := func(legs []nativeExactLeg) map[string]nativeExactLeg {
		out := map[string]nativeExactLeg{}
		for _, leg := range legs {
			side := strings.ToUpper(strings.TrimSpace(leg.Side))
			if side == "YES" || side == "NO" {
				out[side] = leg
			}
		}
		return out
	}
	k, p := bySide(kalshi), bySide(polyus)
	if len(k) != 2 || len(p) != 2 {
		return nil
	}
	polyCanonicalYes := "YES"
	if strings.EqualFold(strings.TrimSpace(orientation), "inverted") {
		polyCanonicalYes = "NO"
	}
	polyCanonicalNo := concreteOppositeSide(polyCanonicalYes)
	return []clienteleExactArm{
		{Venue: "kalshi", NativeSide: "YES", CanonicalSide: "YES",
			SelectorArm: "kalshi", Leg: k["YES"]},
		{Venue: "kalshi", NativeSide: "NO", CanonicalSide: "NO",
			SelectorArm: "kalshi", Leg: k["NO"]},
		{Venue: "polyus", NativeSide: polyCanonicalYes, CanonicalSide: "YES",
			SelectorArm: "polyus", Leg: p[polyCanonicalYes]},
		{Venue: "polyus", NativeSide: polyCanonicalNo, CanonicalSide: "NO",
			SelectorArm: "polyus", Leg: p[polyCanonicalNo]},
	}
}
