package kalshi

import "testing"

// R110 — pins ComboLegal against the LIVE-PROBED collection shape (2026-07-07, public GET
// /trade-api/v2/multivariate_event_collections?status=open): KXMVECROSSCATEGORY-R, size_min=2,
// size_max=0 (no max), per-event fields e.g.
//
//	{"ticker":"KXWNBAGAME-26JUL07DALNY","is_yes_only":true,"size_max":1}
//	{"ticker":"KXWNBASPREAD-26JUL07DALNY","is_yes_only":false,"size_max":1}
//
// The legal and illegal cases below mirror the captured public response shape.
func r110Collection() MVCollection {
	return MVCollection{
		CollectionTicker: "KXMVECROSSCATEGORY-R",
		SizeMin:          2,
		SizeMax:          0,
		Events: map[string]bool{
			"KXWNBAGAME-26JUL07DALNY":   false,
			"KXWNBAGAME-26JUL07CHIPHX":  false,
			"KXWNBASPREAD-26JUL07DALNY": false,
			"KXMLBGAME-26JUL07COLLAD":   false,
		},
		EventCfg: map[string]MVEventCfg{
			"KXWNBAGAME-26JUL07DALNY":   {IsYesOnly: true, SizeMax: 1},
			"KXWNBAGAME-26JUL07CHIPHX":  {IsYesOnly: true, SizeMax: 1},
			"KXWNBASPREAD-26JUL07DALNY": {IsYesOnly: false, SizeMax: 1},
			"KXMLBGAME-26JUL07COLLAD":   {IsYesOnly: true, SizeMax: 1},
		},
	}
}

func TestR110ComboLegal(t *testing.T) {
	col := r110Collection()
	// LEGAL: two YES moneyline legs, different events (live probe example 1).
	if r := col.ComboLegal([]ComboLeg{
		{EventTicker: "KXWNBAGAME-26JUL07DALNY", Side: "yes"},
		{EventTicker: "KXWNBAGAME-26JUL07CHIPHX", Side: "yes"},
	}); r != "" {
		t.Fatalf("two cross-event YES legs must be legal, got %q", r)
	}
	// LEGAL: cross-series, NO allowed on the spread event (is_yes_only=false) (probe example 2).
	if r := col.ComboLegal([]ComboLeg{
		{EventTicker: "KXMLBGAME-26JUL07COLLAD", Side: "YES"},
		{EventTicker: "KXWNBASPREAD-26JUL07DALNY", Side: "no"},
	}); r != "" {
		t.Fatalf("cross-series YES+NO(spread) must be legal, got %q", r)
	}
	// ILLEGAL: leg from an event outside the collection (probe example 3: INXD-* is not listed).
	if r := col.ComboLegal([]ComboLeg{
		{EventTicker: "KXWNBAGAME-26JUL07DALNY", Side: "yes"},
		{EventTicker: "INXD-26JUL07", Side: "yes"},
	}); r == "" {
		t.Fatal("out-of-collection event must be illegal")
	}
	// ILLEGAL: two legs from one event with per-event size_max=1 (probe example 4).
	if r := col.ComboLegal([]ComboLeg{
		{EventTicker: "KXWNBAGAME-26JUL07DALNY", Side: "yes"},
		{EventTicker: "KXWNBAGAME-26JUL07DALNY", Side: "yes"},
	}); r == "" {
		t.Fatal("two legs from a size_max=1 event must be illegal")
	}
	// ILLEGAL: NO side on a yes-only event (probe example 5).
	if r := col.ComboLegal([]ComboLeg{
		{EventTicker: "KXWNBAGAME-26JUL07DALNY", Side: "no"},
		{EventTicker: "KXWNBAGAME-26JUL07CHIPHX", Side: "yes"},
	}); r == "" {
		t.Fatal("NO leg on a yes-only event must be illegal")
	}
	// ILLEGAL: one leg is not a combo (size_min=2).
	if r := col.ComboLegal([]ComboLeg{{EventTicker: "KXWNBAGAME-26JUL07DALNY", Side: "yes"}}); r == "" {
		t.Fatal("single leg must be below size_min")
	}
}

// TestR111ScreenshotCombo pins the operator's LIVE app combo (2026-07-09 France vs Morocco,
// quoted 7.59x, COMBO 4 markets) against the live-probed collection fields (2026-07-07 R111,
// GET /multivariate_event_collections/KXMVESPORTSMULTIGAMEEXTENDED-R):
//
//	KXWCADVANCE-26JUL09FRAMAR  size_max:1    yes_only:false   ← "France advances"
//	KXWCCORNERS-26JUL09FRAMAR  size_max:null yes_only:false   ← "7+ corners"
//	KXWCGOAL-26JUL09FRAMAR     size_max:null yes_only:true    ← "Desire Doue: 1+"
//	KXWCTCORNERS-26JUL09FRAMAR size_max:null yes_only:false   ← "France: 6+ (team corners)"
//
// FOUR legs, ONE game, FOUR distinct events → LEGAL. This is the case R110's prose ("max 1 leg
// per event" misread as per-game) wrongly called impossible.
func r111FramarCollection() MVCollection {
	return MVCollection{
		CollectionTicker: "KXMVESPORTSMULTIGAMEEXTENDED-R",
		SizeMin:          2,
		SizeMax:          0,
		Events: map[string]bool{
			"KXWCADVANCE-26JUL09FRAMAR":  false,
			"KXWCCORNERS-26JUL09FRAMAR":  false,
			"KXWCGOAL-26JUL09FRAMAR":     false,
			"KXWCTCORNERS-26JUL09FRAMAR": false,
		},
		EventCfg: map[string]MVEventCfg{
			"KXWCADVANCE-26JUL09FRAMAR":  {IsYesOnly: false, SizeMax: 1},
			"KXWCCORNERS-26JUL09FRAMAR":  {IsYesOnly: false, SizeMax: 0}, // null = uncapped
			"KXWCGOAL-26JUL09FRAMAR":     {IsYesOnly: true, SizeMax: 0},
			"KXWCTCORNERS-26JUL09FRAMAR": {IsYesOnly: false, SizeMax: 0},
		},
	}
}

func TestR111ScreenshotCombo(t *testing.T) {
	col := r111FramarCollection()
	screenshot := []ComboLeg{
		{EventTicker: "KXWCADVANCE-26JUL09FRAMAR", Side: "yes"},  // France advances
		{EventTicker: "KXWCCORNERS-26JUL09FRAMAR", Side: "yes"},  // 7+ corners
		{EventTicker: "KXWCGOAL-26JUL09FRAMAR", Side: "yes"},     // Desire Doue: 1+
		{EventTicker: "KXWCTCORNERS-26JUL09FRAMAR", Side: "yes"}, // France: 6+ team corners
	}
	if r := col.ComboLegal(screenshot); r != "" {
		t.Fatalf("the operator's live 4-leg same-game combo must be LEGAL, got %q", r)
	}
	if tick, r := ComboLegalAny([]MVCollection{col}, screenshot); tick == "" || r != "" {
		t.Fatalf("ComboLegalAny must accept the screenshot combo, got %q / %q", tick, r)
	}
	// TWO legs of ONE uncapped event (two corners bands) → also legal (size_max=null).
	if r := col.ComboLegal([]ComboLeg{
		{EventTicker: "KXWCTCORNERS-26JUL09FRAMAR", Side: "yes"},
		{EventTicker: "KXWCTCORNERS-26JUL09FRAMAR", Side: "yes"},
	}); r != "" {
		t.Fatalf("two legs of an uncapped event must be legal, got %q", r)
	}
	// NO leg on the yes-only player-goals event → refused; NO on the corners event → fine.
	if r := col.ComboLegal([]ComboLeg{
		{EventTicker: "KXWCGOAL-26JUL09FRAMAR", Side: "no"},
		{EventTicker: "KXWCCORNERS-26JUL09FRAMAR", Side: "yes"},
	}); r == "" {
		t.Fatal("NO on a yes-only event must be refused")
	}
	if r := col.ComboLegal([]ComboLeg{
		{EventTicker: "KXWCGOAL-26JUL09FRAMAR", Side: "yes"},
		{EventTicker: "KXWCCORNERS-26JUL09FRAMAR", Side: "no"},
	}); r != "" {
		t.Fatalf("NO on a non-yes-only event must be legal, got %q", r)
	}
	// Two ADVANCE legs (size_max=1) → refused: the per-EVENT cap is the real rule.
	if r := col.ComboLegal([]ComboLeg{
		{EventTicker: "KXWCADVANCE-26JUL09FRAMAR", Side: "yes"},
		{EventTicker: "KXWCADVANCE-26JUL09FRAMAR", Side: "yes"},
	}); r == "" {
		t.Fatal("two legs of a size_max=1 event must be refused")
	}
}

func TestR110ComboLegalAny(t *testing.T) {
	cols := []MVCollection{r110Collection()}
	if tick, r := ComboLegalAny(cols, []ComboLeg{
		{EventTicker: "KXWNBAGAME-26JUL07DALNY", Side: "yes"},
		{EventTicker: "KXMLBGAME-26JUL07COLLAD", Side: "yes"},
	}); tick != "KXMVECROSSCATEGORY-R" || r != "" {
		t.Fatalf("legal pair must resolve to the collection, got %q / %q", tick, r)
	}
	if tick, r := ComboLegalAny(cols, []ComboLeg{
		{EventTicker: "KXWNBAGAME-26JUL07DALNY", Side: "yes"},
		{EventTicker: "UNLISTED-EVENT", Side: "yes"},
	}); tick != "" || r == "" {
		t.Fatalf("unlisted event must fail every collection, got %q / %q", tick, r)
	}
}
