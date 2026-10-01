package kalshi

import (
	"encoding/json"
	"testing"
)

// Aggressor must prefer the successor outcome_side field and fall back to the
// deprecated taker_side while Kalshi still sends it (deprecation of 2026-05-06).
func TestTradeAggressorPrefersOutcomeSide(t *testing.T) {
	if got := (Trade{TakerSide: "yes"}).Aggressor(); got != "yes" {
		t.Fatalf("fallback: got %q", got)
	}
	if got := (Trade{TakerSide: "yes", OutcomeSide: "no"}).Aggressor(); got != "no" {
		t.Fatalf("outcome_side must win: got %q", got)
	}
	if got := (Trade{}).Aggressor(); got != "" {
		t.Fatalf("empty trade: got %q", got)
	}
}

func TestTradeCanonicalPublicDirection(t *testing.T) {
	var tr Trade
	if err := json.Unmarshal([]byte(`{"ticker":"KXTEST","taker_side":"yes","taker_outcome_side":"no","taker_book_side":"ask"}`), &tr); err != nil {
		t.Fatal(err)
	}
	if got := tr.Aggressor(); got != "no" {
		t.Fatalf("canonical taker_outcome_side must win, got %q", got)
	}
	if tr.TakerSide != "no" {
		t.Fatalf("legacy compatibility field was not normalized: %q", tr.TakerSide)
	}

	tr = Trade{TakerBookSide: "bid"}
	if got := tr.Aggressor(); got != "yes" {
		t.Fatalf("bid must map to yes, got %q", got)
	}
}
