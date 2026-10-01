package server

import "testing"

func r145ClienteleLegs(venue string) []nativeExactLeg {
	return []nativeExactLeg{
		{Venue: venue, Ticker: venue + "-ticker", Side: "NO", Ask: .42, Depth: 4},
		{Venue: venue, Ticker: venue + "-ticker", Side: "YES", Ask: .59, Depth: 5},
	}
}

func TestR145ClienteleExactArmsEnumerateBothSidesAndVenues(t *testing.T) {
	for _, tc := range []struct {
		orientation                       string
		polyYesCanonical, polyNoCanonical string
	}{{"same", "YES", "NO"}, {"inverted", "NO", "YES"}} {
		arms := concreteClienteleExactArms(tc.orientation,
			r145ClienteleLegs("kalshi"), r145ClienteleLegs("polyus"))
		if len(arms) != 4 {
			t.Fatalf("%s arms=%d want 4: %+v", tc.orientation, len(arms), arms)
		}
		seen := map[string]bool{}
		for _, arm := range arms {
			key := arm.Venue + "|" + arm.NativeSide
			if seen[key] || arm.Leg.Venue != arm.Venue || arm.Leg.Side != arm.NativeSide ||
				arm.SelectorArm == "" || (arm.CanonicalSide != "YES" && arm.CanonicalSide != "NO") {
				t.Fatalf("%s invalid/duplicate arm %+v", tc.orientation, arm)
			}
			seen[key] = true
		}
		if arms[2].NativeSide != tc.polyYesCanonical || arms[2].CanonicalSide != "YES" ||
			arms[3].NativeSide != tc.polyNoCanonical || arms[3].CanonicalSide != "NO" {
			t.Fatalf("%s PolyUS canonical mapping wrong: %+v", tc.orientation, arms[2:])
		}
	}
}

func TestR145ClienteleExactArmsRejectIncompleteBook(t *testing.T) {
	if got := concreteClienteleExactArms("same", r145ClienteleLegs("kalshi"),
		[]nativeExactLeg{{Venue: "polyus", Ticker: "p", Side: "YES"}}); got != nil {
		t.Fatalf("incomplete two-sided PolyUS book produced executable controls: %+v", got)
	}
}
