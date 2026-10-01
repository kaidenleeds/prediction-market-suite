package polyid

import (
	"strings"
	"testing"
)

func TestNormalizeConditionIDStrictWidthAndHex(t *testing.T) {
	validUpper := "0x" + strings.Repeat("Ab", 32)
	if len(validUpper) != 66 {
		t.Fatalf("bad test fixture length: %d", len(validUpper))
	}
	got, ok := NormalizeConditionID("  " + validUpper + "\n")
	if !ok || got != strings.ToLower(validUpper) {
		t.Fatalf("NormalizeConditionID() = %q, %v", got, ok)
	}

	for _, bad := range []string{
		"", "0x" + strings.Repeat("a", 62), "0x" + strings.Repeat("a", 63), "0x" + strings.Repeat("a", 65),
		"0X" + strings.Repeat("a", 64), "0x" + strings.Repeat("a", 63) + "g", strings.Repeat("a", 64),
		" " + strings.ToLower(validUpper),
	} {
		if ValidConditionID(bad) {
			t.Errorf("accepted malformed id %q (len=%d)", bad, len(bad))
		}
	}
}
