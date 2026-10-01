package kalshi

import "testing"

func TestR132PerpetualsNeverEnterBinaryBetUniverse(t *testing.T) {
	for _, m := range []Market{
		{Ticker: "KXBTCPERP-X", EventTicker: "KXBTCPERP"},
		{Ticker: "ODD-X-PERP", EventTicker: "ODD"},
	} {
		if isBettable(m) {
			t.Fatalf("perpetual market entered binary universe: %+v", m)
		}
	}
	if !isBettable(Market{Ticker: "KXBTC15M-26JUL10-T1", EventTicker: "KXBTC15M-26JUL10"}) {
		t.Fatal("ordinary binary market was excluded")
	}
}
