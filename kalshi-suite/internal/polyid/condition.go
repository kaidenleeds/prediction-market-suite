// Package polyid owns the canonical validation rules for Polymarket identifiers.
package polyid

import "strings"

// NormalizeConditionID returns the canonical lower-case representation of id.
// A Polymarket condition id is a 32-byte value encoded as exactly 0x + 64 hex
// characters. The boolean is false for truncated token-derived values and every
// other non-condition identifier.
func NormalizeConditionID(id string) (string, bool) {
	id = strings.TrimSpace(id)
	if len(id) != 66 || id[0] != '0' || id[1] != 'x' {
		return "", false
	}
	for i := 2; i < len(id); i++ {
		c := id[i]
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return "", false
		}
	}
	return strings.ToLower(id), true
}

// ValidConditionID reports whether id is exactly one canonical-width condition id.
func ValidConditionID(id string) bool {
	if id != strings.TrimSpace(id) {
		return false
	}
	_, ok := NormalizeConditionID(id)
	return ok
}
