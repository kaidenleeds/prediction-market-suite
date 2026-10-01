package polymarketus

import (
	"encoding/json"
	"testing"
)

func TestR139MarketRetainsResolutionDescription(t *testing.T) {
	var m Market
	if err := json.Unmarshal([]byte(`{"slug":"rule-market","question":"Who wins?","description":"Official winner; canceled games settle at last fair market price.","feeCoefficient":0}`), &m); err != nil {
		t.Fatal(err)
	}
	if m.Description == "" || m.FeeCoeff == nil || *m.FeeCoeff != 0 {
		t.Fatalf("resolution description or fee presence was discarded: %+v", m)
	}
}
