package server

// r117_test.go — xinv-pcrypto (inverted crypto bridge twin): the pure side-inversion +
// price-derivation core. No network, no server: xinvDerive is deliberately pure so the
// YES/NO vocabulary guard and the [0.01,0.99] entry refusal are pinned here.

import "testing"

func TestXinvDerive(t *testing.T) {
	cases := []struct {
		name      string
		pcSide    string
		yesMark   float64
		wantSide  string
		wantEntry float64
		wantOK    bool
	}{
		// pcrypto favors Up → we take the Kalshi NO side, priced at 1−yesMark.
		{"up inverts to NO", "Up", 0.70, "NO", 0.30, true},
		{"down inverts to YES", "Down", 0.70, "YES", 0.70, true},
		// YES/NO vocabulary tolerated (canonical exec-venue labels), any case/whitespace.
		{"yes inverts to NO", " yes ", 0.40, "NO", 0.60, true},
		{"no inverts to YES", "NO", 0.40, "YES", 0.40, true},
		// entry outside [0.01,0.99] refuses — degenerate derived price, not a signal.
		{"NO entry below 1c refused", "Up", 0.995, "", 0, false},
		{"YES entry above 99c refused", "Down", 0.995, "", 0, false},
		{"boundary 0.99 accepted", "Down", 0.99, "YES", 0.99, true},
		{"boundary 0.01 accepted", "Up", 0.99, "NO", 0.01, true},
		// unusable marks refuse.
		{"zero mark refused", "Up", 0, "", 0, false},
		{"mark of 1 refused", "Down", 1, "", 0, false},
		{"negative mark refused", "Up", -0.2, "", 0, false},
		// unknown side vocabulary refuses — never guess a direction.
		{"outcome label refused", "Chiefs", 0.5, "", 0, false},
		{"empty side refused", "", 0.5, "", 0, false},
	}
	for _, c := range cases {
		side, entry, ok := xinvDerive(c.pcSide, c.yesMark)
		if ok != c.wantOK || side != c.wantSide || !almostEq(entry, c.wantEntry) {
			t.Errorf("%s: xinvDerive(%q, %v) = (%q, %v, %v), want (%q, %v, %v)",
				c.name, c.pcSide, c.yesMark, side, entry, ok, c.wantSide, c.wantEntry, c.wantOK)
		}
	}
}

func almostEq(a, b float64) bool {
	d := a - b
	return d < 1e-9 && d > -1e-9
}
