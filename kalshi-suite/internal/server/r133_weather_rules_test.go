package server

import (
	"testing"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/polymarket"
)

func TestR133WeatherSettlementSourceMismatchFailsClosed(t *testing.T) {
	// These are the four exact Jul 9 rows that later settled with both lock legs losing. The visible
	// station/date/bin matched, but Kalshi used the NWS Climatological Report and Poly-int used
	// Wunderground. Keep both anchors for venue-specific history while refusing every twin path.
	cases := []struct {
		kalshi string
		pint   string
	}{
		{"KXHIGHAUS-26JUL09-B98.5", "highest-temperature-in-austin-on-july-9-2026-98-99f"},
		{"KXHIGHAUS-26JUL09-B96.5", "highest-temperature-in-austin-on-july-9-2026-96-97f"},
		{"KXHIGHTSFO-26JUL09-B68.5", "highest-temperature-in-san-francisco-on-july-9-2026-68-69f"},
		{"KXHIGHTHOU-26JUL09-B94.5", "highest-temperature-in-houston-on-july-9-2026-94-95f"},
	}

	for _, tc := range cases {
		t.Run(tc.kalshi, func(t *testing.T) {
			xvTestReset()
			kKey, kKind, kOK := xvKalshiAnchor(tc.kalshi)
			pKey, pKind, pOK := xvPolyAnchor(tc.pint, "2026-07-09T12:00:00Z")
			if !kOK || !pOK || kKind != "wx" || pKind != "wx" || kKey != pKey {
				t.Fatalf("expected retained same-shape wx anchors: K=(%q,%q,%v) P=(%q,%q,%v)",
					kKey, kKind, kOK, pKey, pKind, pOK)
			}
			xvReg.add("kalshi", tc.kalshi, kKey, kKind)
			xvReg.add("polymarket", tc.pint, pKey, pKind)

			if kind, anchored := xvAnchorKind("kalshi", tc.kalshi); !anchored || kind != "wx" {
				t.Fatalf("Kalshi venue-specific anchor lost: kind=%q anchored=%v", kind, anchored)
			}
			if kind, anchored := xvAnchorKind("polymarket", tc.pint); !anchored || kind != "wx" {
				t.Fatalf("Poly-int venue-specific anchor lost: kind=%q anchored=%v", kind, anchored)
			}
			if xvMemberRulesEquivalent("wx") {
				t.Fatal("weather must remain settlement-rules-unproven")
			}
			if _, _, _, ok := xvTwin("kalshi", tc.kalshi); ok {
				t.Fatal("Kalshi weather bucket entered the member-twin/lock path")
			}
			if _, _, _, ok := xvTwin("polymarket", tc.pint); ok {
				t.Fatal("Poly-int weather bucket entered the member-twin/lock path")
			}
			if k, pus := pintDynamicXVTwins(tc.pint); k != "" || pus != "" {
				t.Fatalf("Poly-int priority/lock matcher bypassed rule guard: kalshi=%q polyus=%q", k, pus)
			}
			legacy := xvLockOpp{Pair: "K-PINT", AVenue: "kalshi", AID: tc.kalshi,
				AWon: 0, BVenue: "polymarket", BID: tc.pint, BWon: 0, Mismatch: true}
			if xvlProofEligible(legacy) {
				t.Fatal("legacy weather mismatch contaminated xvlock proof")
			}
		})
	}
}

func TestR133MemberRulesEquivalenceIsAllowlist(t *testing.T) {
	if !xvMemberRulesEquivalent("crypto") {
		t.Fatal("verified crypto member twins should remain enabled")
	}
	for _, kind := range []string{"wx", "econ", "pol", "future-kind", ""} {
		if xvMemberRulesEquivalent(kind) {
			t.Fatalf("unverified kind %q must fail closed", kind)
		}
	}
}

func TestR133WeatherCannotBypassRulesGuardThroughColdFuzzyMatch(t *testing.T) {
	xvTestReset() // model the interval before the first registry rebuild
	pm := polymarket.Market{
		Slug:     "highest-temperature-in-austin-on-july-9-2026-98-99f",
		Question: "Will the highest temperature in Austin be between 98-99F on July 9?",
		EndDate:  "2026-07-09T12:00:00Z",
	}
	km := kalshi.Market{
		Ticker:             "KXHIGHAUS-26JUL09-B98.5",
		Title:              "Will the high temp in Austin be 98-99 on Jul 9, 2026?",
		YesSubTitle:        "98 to 99",
		ExpectedExpiration: "2026-07-10T06:00:00Z",
	}
	if _, ok := bestKalshiMatch(pm, []kalshi.Market{km}); ok {
		t.Fatal("cold-start title fuzz bypassed the weather settlement-source guard")
	}
}

func TestR133XvLockProofQuarantineDoesNotDeleteHistory(t *testing.T) {
	op := xvLockOpp{Key: "legacy-weather-row", Pair: "K-PINT", Orient: "NOa+YESb",
		AVenue: "kalshi", AID: "KXHIGHAUS-26JUL09-B98.5", ASide: "NO", AWon: 0,
		BVenue: "polymarket", BID: "highest-temperature-in-austin-on-july-9-2026-98-99f",
		BSide: "YES", BWon: 0, Mismatch: true, RealizedC: -70.24}
	book := xvLockBook{Closed: []xvLockOpp{op}, MismatchN: 1}
	if xvlProofEligible(book.Closed[0]) {
		t.Fatal("known NWS/Wunderground row must be excluded from proof")
	}
	if len(book.Closed) != 1 || book.Closed[0].AWon != 0 || book.Closed[0].BWon != 0 ||
		!book.Closed[0].Mismatch || book.Closed[0].RealizedC != -70.24 || book.MismatchN != 1 {
		t.Fatalf("quarantine altered venue-specific settlement history: %+v", book)
	}

	crypto := xvLockOpp{Pair: "K-PINT", AVenue: "kalshi", AID: "KXBTCD-26JUL1012-T55999.99",
		ASide: "YES", BVenue: "polymarket", BID: "bitcoin-above-56k-on-july-10-2026", BSide: "NO"}
	crypto.ResolutionBasis = r139VerifiedLockCertificate(crypto.Pair, crypto.AVenue, crypto.AID,
		crypto.BVenue, crypto.BID, true, crypto.ASide, crypto.BSide)
	if !xvlProofEligible(crypto) {
		t.Fatal("verified threshold-crypto lock class was quarantined")
	}
}
