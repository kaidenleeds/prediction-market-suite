package kalshi

import (
	"encoding/json"
	"testing"
)

func TestHistoricalTradeCanonicalDirectionPrecedence(t *testing.T) {
	var tr HistoricalTrade
	if err := json.Unmarshal([]byte(`{"trade_id":"t1","taker_outcome_side":"no","taker_book_side":"bid","taker_side":"yes"}`), &tr); err != nil {
		t.Fatal(err)
	}
	if got := tr.Aggressor(); got != "no" {
		t.Fatalf("canonical outcome side must win, got %q", got)
	}
	if tr.TakerSide != "no" {
		t.Fatalf("legacy compatibility field was not normalized: %q", tr.TakerSide)
	}
}

func TestHistoricalTradeCanonicalBookSideAndLegacyFallback(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{"book bid", `{"taker_book_side":"bid","taker_side":"no"}`, "yes"},
		{"book ask", `{"taker_book_side":"ask","taker_side":"yes"}`, "no"},
		{"legacy", `{"taker_side":"YES"}`, "yes"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var tr HistoricalTrade
			if err := json.Unmarshal([]byte(tc.raw), &tr); err != nil {
				t.Fatal(err)
			}
			if got := tr.Aggressor(); got != tc.want || tr.TakerSide != tc.want {
				t.Fatalf("direction=%q compatibility=%q, want %q", got, tr.TakerSide, tc.want)
			}
		})
	}
}
