package kalshi

import "testing"

func TestRawFlowRetainsTinyValidKalshiPrintWithoutDollarFloor(t *testing.T) {
	c := &Client{}
	c.ingestTrade([]byte(`{"type":"trade","msg":{"trade_id":"tiny","market_ticker":"KXTINY-YES","count_fp":"0.1","yes_price_dollars":"0.02","no_price_dollars":"0.98","taker_outcome_side":"yes","ts":1770000000}}`))
	tape, ok := c.LiveTape()
	if !ok || len(tape) != 1 {
		t.Fatalf("tiny valid print was filtered from raw tape: ok=%v n=%d", ok, len(tape))
	}
	if got := tape[0].Count.Float() * tape[0].YesPrice.Float(); got <= 0 || got >= 0.01 {
		t.Fatalf("expected retained sub-cent notional, got $%.6f", got)
	}
}

func TestRawFlowRejectsInvalidKalshiPrintsOnly(t *testing.T) {
	c := &Client{}
	c.ingestTrade([]byte(`{"type":"trade","msg":{"trade_id":"zero","market_ticker":"KXTINY-YES","count_fp":"0","yes_price_dollars":"0.02","no_price_dollars":"0.98","taker_outcome_side":"yes"}}`))
	if tape, ok := c.LiveTape(); ok || len(tape) != 0 {
		t.Fatalf("invalid zero print entered raw tape: ok=%v n=%d", ok, len(tape))
	}
}
