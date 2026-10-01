package server

import (
	"math"
	"testing"
)

func TestR163KalshiSelectedOutcomeFillPriceNormalizesV2YesBookPrice(t *testing.T) {
	tests := []struct {
		name, side    string
		raw, fallback float64
		want          float64
	}{
		{name: "YES keeps YES-book fill", side: "YES", raw: .68, fallback: .32, want: .68},
		{name: "NO complements YES-book fill", side: "NO", raw: .68, fallback: .32, want: .32},
		{name: "lowercase NO complements", side: "no", raw: .91, fallback: .09, want: .09},
		{name: "missing raw keeps selected-side fallback", side: "NO", raw: 0, fallback: .32, want: .32},
		{name: "NaN raw keeps selected-side fallback", side: "NO", raw: math.NaN(), fallback: .32, want: .32},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := kalshiSelectedOutcomeFillPrice(tc.side, tc.raw, tc.fallback)
			if math.Abs(got-tc.want) > 1e-12 {
				t.Fatalf("selected outcome fill price=%v want=%v", got, tc.want)
			}
		})
	}
}
