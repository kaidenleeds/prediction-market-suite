package storage

// R89 (auditor bug 56): timestamp stamps must sort lexicographically = chronologically.
// time.RFC3339Nano drops trailing zeros ("…05.5Z"), which sorts AFTER "…05.4999Z"-style longer
// fractions of an EARLIER instant is fine — but "…05.9Z" vs "…05.95Z" inverts ('Z' > '5').
// The fixed-width tsLayout keeps every stamp the same width so string order == time order.

import (
	"testing"
	"time"
)

func TestTsLayoutFixedWidthLexicographicOrder(t *testing.T) {
	base := time.Date(2026, 7, 6, 5, 0, 5, 0, time.UTC)
	cases := []struct{ a, b time.Time }{
		{base.Add(900 * time.Millisecond), base.Add(950 * time.Millisecond)}, // the RFC3339Nano inversion pair (.9Z vs .95Z)
		{base, base.Add(1 * time.Nanosecond)},
		{base.Add(500 * time.Millisecond), base.Add(500*time.Millisecond + 100*time.Nanosecond)},
		{base.Add(999999999 * time.Nanosecond), base.Add(time.Second)},
	}
	for _, c := range cases {
		sa, sb := c.a.UTC().Format(tsLayout), c.b.UTC().Format(tsLayout)
		if len(sa) != len(sb) {
			t.Fatalf("layout not fixed width: %q (%d) vs %q (%d)", sa, len(sa), sb, len(sb))
		}
		if !(sa < sb) {
			t.Fatalf("lexicographic order broken: %q !< %q", sa, sb)
		}
		// document the OLD bug: RFC3339Nano really does invert the .9/.95 pair
		if c.a.Equal(base.Add(900 * time.Millisecond)) {
			na, nb := c.a.UTC().Format(time.RFC3339Nano), c.b.UTC().Format(time.RFC3339Nano)
			if na < nb {
				t.Log("note: RFC3339Nano pair happened to order correctly — layout change still required for the general case")
			}
		}
	}
}

func TestNowRFCParsesAndIsFixedWidth(t *testing.T) {
	s1 := nowRFC()
	if _, err := time.Parse(time.RFC3339Nano, s1); err != nil {
		t.Fatalf("nowRFC output %q does not parse as RFC3339Nano: %v", s1, err)
	}
	s2 := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC).Format(tsLayout)
	if len(s1) != len(s2) {
		t.Fatalf("nowRFC width %d != layout width %d (%q vs %q)", len(s1), len(s2), s1, s2)
	}
}
