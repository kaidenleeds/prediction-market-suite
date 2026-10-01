package polymarket

import (
	"encoding/json"
	"testing"
)

func TestR139GammaMarketDecodesOfficialRuleFields(t *testing.T) {
	var market Market
	if err := json.Unmarshal([]byte(`{"conditionId":"0xrules","description":"Exact resolution rules.","resolutionSource":"https://official.example/final"}`), &market); err != nil {
		t.Fatal(err)
	}
	if market.ConditionID != "0xrules" || market.Description != "Exact resolution rules." ||
		market.ResolutionSource != "https://official.example/final" {
		t.Fatalf("Gamma rule fields were dropped: %+v", market)
	}
}
