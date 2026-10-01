package server

import (
	"strings"
	"testing"
)

func TestR147ExactProducerCapabilityCannotBeCreatedByCatalogDeclaration(t *testing.T) {
	for _, tc := range []struct {
		system, venue, side string
		want                bool
	}{
		{"freshlist", "kalshi", "YES", true},
		{"freshlist", "kalshi", "NO", false},
		{"freshlist", "polyus", "YES", true},
		{"kalshi-flow", "polyus", "YES", false},
		{"polyus-flow", "kalshi", "NO", false},
		{"xvgap", "polyus", "YES", true},
		{"xvgap", "polyus", "NO", false},
		{"invert:freshlist", "kalshi", "NO", true},
		{"invert:freshlist", "kalshi", "YES", false},
	} {
		if got := r147NativeSignalProducerCapable(tc.system, tc.venue, tc.side); got != tc.want {
			t.Fatalf("producer %s %s %s=%v want %v", tc.system, tc.venue, tc.side, got, tc.want)
		}
	}
	fake := r145SystemCatalogVariant{SystemID: "kalshi-flow", Venue: "polyus", Side: "YES", Route: "taker"}
	fake.Handoff = r145NativeVariantHandoff(fake)
	if fake.Handoff != r145HandoffUnregistered || r145CatalogVariantHasCodePath(fake) {
		t.Fatalf("manual declaration fabricated a producer: %+v", fake)
	}
}

func TestR147VenueAndCrossVenueSignalContractsAreExplicit(t *testing.T) {
	contracts := r147SystemVariantSignalContracts()
	find := func(system, venue, side, route, topology string) bool {
		for _, row := range contracts {
			if row.SystemID == system && row.ExecutionVenue == venue && row.Side == side &&
				row.Route == route && row.InputTopology == topology && row.ProducerCapable {
				return true
			}
		}
		return false
	}
	for _, want := range [][5]string{
		{"favlong", "kalshi", "YES", "taker", "K"},
		{"favlong", "polyus", "YES", "taker", "PUS"},
		{"xmatch", "kalshi", "YES", "taker", "K-PINT"},
		{"xvgap", "polyus", "YES", "taker", "K-PUS"},
		{"confluence", "kalshi", "YES", "taker", "K-PUS-PINT"},
		{"confluence", "polyus", "NO", "taker", "PUS-PINT"},
	} {
		if !find(want[0], want[1], want[2], want[3], want[4]) {
			t.Fatalf("missing exact venue/topology contract %v", want)
		}
	}
	for _, row := range contracts {
		if row.ExecutionVenue == "polymarket" || row.ExecutionVenue == "cross" {
			t.Fatalf("research input masqueraded as execution venue: %+v", row)
		}
		if row.PINTResearchInput && !row.CrossVenue {
			t.Fatalf("PINT input not marked cross-venue: %+v", row)
		}
	}
	counts := r147SystemVariantSignalContractCounts()
	if counts.TypedExecutionVariants != len(r145KnownSystemVariants()) || counts.SignalContracts < counts.TypedExecutionVariants ||
		counts.ProducerCapable+counts.ProducerBlocked != counts.TypedExecutionVariants || counts.CrossVenueInputs == 0 {
		t.Fatalf("invalid signal contract counts: %+v", counts)
	}
	t.Logf("R147 signal contracts: %+v", counts)
}

func TestR167ProperMomentumSELLDerivativesAreExplicitAndSeparate(t *testing.T) {
	contracts := r147SystemVariantSignalContracts()
	found := 0
	for _, row := range contracts {
		if row.SystemID != "proper-score-momentum" {
			continue
		}
		found++
		if row.ParentSystemID != "proper-score-executor" || row.ExecutionVenue != "kalshi" ||
			(row.Side != "YES" && row.Side != "NO") || row.Action != "SELL" ||
			row.Route != "taker" || row.TimeInForce != "fill_or_kill" || !row.ReduceOnly ||
			!row.ExecutionDerivative || row.LiveAuthority || !row.ProducerCapable ||
			!strings.HasPrefix(row.InputTopology, "K-PROPER-") {
			t.Fatalf("untruthful proper momentum derivative: %+v", row)
		}
	}
	counts := r147SystemVariantSignalContractCounts()
	if found != 6 || counts.DerivativeContracts != 6 || counts.SignalContracts != 385 ||
		counts.TypedExecutionVariants != 302 {
		t.Fatalf("proper momentum census found=%d counts=%+v", found, counts)
	}
}
