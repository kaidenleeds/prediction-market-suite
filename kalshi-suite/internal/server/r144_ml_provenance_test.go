package server

import (
	"math"
	"testing"
	"time"
)

func TestR144GoMLSettlementProvenanceMatchesBookNativeContract(t *testing.T) {
	m := map[string]any{
		"p_win": 0.70, "price": 0.40, "ev_net": 0.25, "won": 1.0,
		"marks": []any{[]any{10.0, 0.40, "mark"}, []any{20.0, 0.45, "mark"}},
	}
	stampMLTerminalProvenance(m, 1, .38, 2, .40, time.Unix(30, 0))
	for key, want := range map[string]float64{
		"model_probability": .70, "market_price_at_decision": .40,
		"predicted_fee_net_edge": .25, "entry_price": .40,
		"settlement_payout": 1, "realized_cents_per_share": 19,
		"close_market_price": .45, "clv": .05,
	} {
		got, ok := m[key].(float64)
		if !ok || math.Abs(got-want) > 1e-9 {
			t.Fatalf("%s=%v want %.4f", key, m[key], want)
		}
	}
	if m["final_result"] != "won" || m["terminal_reason"] != "settlement" {
		t.Fatalf("terminal labels: %+v", m)
	}
}

func TestR144GoMLSettlementDoesNotTurnPayoutIntoCLV(t *testing.T) {
	m := map[string]any{"p_win": .55, "price": .50, "ev_net": .04, "won": 0.0}
	stampMLTerminalProvenance(m, 0, -.50, 1, .50, time.Unix(30, 0))
	if m["close_market_price"] != nil || m["clv"] != nil {
		t.Fatalf("missing market close must remain null, got close=%v clv=%v", m["close_market_price"], m["clv"])
	}
}

func TestR144MLAccuracyCountsIndependentMarketsNotRepeatedEntries(t *testing.T) {
	rows := []mlAccBet{
		{Venue: "kalshi", Ticker: "KX-ONE", Side: "YES", Opened: 20, P: .7, Y: 1},
		{Venue: "kalshi", Ticker: "KX-ONE", Side: "NO", Opened: 10, P: .4, Y: 0},
		{Venue: "polyus", Ticker: "GAME-TWO", Side: "YES", Opened: 15, P: .6, Y: 1},
	}
	got := mlAccIndependentMarkets(rows)
	if len(got) != 2 {
		t.Fatalf("unique market n=%d want 2", len(got))
	}
	for _, row := range got {
		if row.Ticker == "KX-ONE" && row.Opened != 10 {
			t.Fatalf("earliest independent KX-ONE decision not retained: %+v", row)
		}
	}
	if mlAccSampleBucket(10)[:4] != "🔴" || mlAccSampleBucket(11)[:4] != "🟠" ||
		mlAccSampleBucket(41)[:4] != "🟡" || mlAccSampleBucket(121)[:4] != "🟢" ||
		mlAccSampleBucket(501)[:4] != "🔵" || mlAccSampleBucket(1001)[:4] != "🟣" {
		t.Fatalf("sample bucket boundaries drifted")
	}
}

func TestR144MLComboIdentityAndResolutionBlocksStaySeparateFromSingles(t *testing.T) {
	bundle, keys := mlAccComboIdentity([]byte(`[
		{"ticker":"KX-B","side":"no"},
		{"ticker":"KX-A","side":"yes"}
	]`), "DISPLAY", 1)
	if bundle != "KX-A|YES,KX-B|NO" || len(keys) != 2 || keys[0] != "kalshi|KX-A" {
		t.Fatalf("canonical combo identity=(%q,%v)", bundle, keys)
	}
	stringBundle, stringKeys := mlAccComboIdentity([]byte(`["KX-B|no","KX-A|yes"]`), "", 2)
	if stringBundle != bundle || len(stringKeys) != 2 {
		t.Fatalf("string/object combo schemas disagree: (%q,%v) vs (%q,%v)", bundle, keys, stringBundle, stringKeys)
	}
	rows := []mlAccBet{
		{BundleKey: "A|YES,B|YES", ResolutionKeys: []string{"kalshi|A", "kalshi|B"}, Opened: 1},
		// Retry of the exact bundle must not create a second scored prediction unit.
		{BundleKey: "A|YES,B|YES", ResolutionKeys: []string{"kalshi|A", "kalshi|B"}, Opened: 2},
		// Different bundle sharing A remains separately scored but in the same dependence block.
		{BundleKey: "A|YES,C|YES", ResolutionKeys: []string{"kalshi|A", "kalshi|C"}, Opened: 3},
		{BundleKey: "D|YES,E|YES", ResolutionKeys: []string{"kalshi|D", "kalshi|E"}, Opened: 4},
	}
	unique := mlAccUniqueComboBundles(rows)
	if len(unique) != 3 || mlAccComboIndependenceBlocks(unique) != 2 {
		t.Fatalf("combo units=%d blocks=%d want 3/2", len(unique), mlAccComboIndependenceBlocks(unique))
	}
}
