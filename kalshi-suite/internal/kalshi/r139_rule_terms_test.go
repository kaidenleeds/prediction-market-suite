package kalshi

import (
	"encoding/json"
	"testing"
)

func TestR139MarketRetainsAuthoritativeRuleTerms(t *testing.T) {
	var m Market
	if err := json.Unmarshal([]byte(`{"ticker":"KX-RULE","rules_primary":"If A wins, Yes.","rules_secondary":"Postponements follow the contract.","early_close_condition":"After a winner is declared."}`), &m); err != nil {
		t.Fatal(err)
	}
	if m.RulesPrimary != "If A wins, Yes." || m.RulesSecondary == "" || m.EarlyCloseCondition == "" {
		t.Fatalf("rule terms were discarded: %+v", m)
	}
}
