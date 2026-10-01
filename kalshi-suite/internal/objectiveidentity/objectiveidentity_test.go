package objectiveidentity

import "testing"

func sports(kind string) Contract {
	return Contract{Venue: "kalshi", InstrumentID: "K-1", Genre: "sports", EventID: "sports:G1",
		League: "mlb", Participants: []string{"PHI", "KC"}, OutcomeCardinality: 2, Period: "full_game", MarketType: kind,
		Selection: "PHI", Opposite: "KC", Comparator: "wins", DeadlineUTC: "2026-07-12T20:00:00Z", Timezone: "UTC",
		VoidPolicy: "invalid-refund", OvertimePolicy: "official-final-including-overtime",
		TiePolicy: "league-final", PolicySource: "official-schema:test-v1",
		SettlementClass: "objective_result", SourceKind: "official_structured", Structured: true}
}

func TestSoccerThreeWayWinnerNeverAliasesNoHomeToYesAway(t *testing.T) {
	home := sports("winner")
	home.League = "soccer"
	home.Participants = []string{"HOME", "AWAY"}
	home.OutcomeCardinality = 3
	home.Selection, home.Opposite = "HOME", ""
	home.TiePolicy = "draw-is-third-outcome"

	sameHome := home
	sameHome.Venue, sameHome.InstrumentID = "polyus", "home-win"
	if v := Evaluate(home, sameHome); !v.Compatible || v.Orientation != "same" {
		t.Fatalf("same Home proposition must match: %+v", v)
	}

	away := sameHome
	away.Selection, away.InstrumentID = "AWAY", "away-win"
	if v := Evaluate(home, away); v.Compatible || v.ReasonCode != RejectSelection {
		t.Fatalf("three-way YES(Home) and YES(Away) are mutually exclusive, not complements: %+v", v)
	}

	draw := sameHome
	draw.Selection, draw.InstrumentID = "DRAW", "draw"
	if v := Evaluate(home, draw); v.Compatible || v.ReasonCode != RejectSelection {
		t.Fatalf("three-way YES(Home) and YES(Draw) must remain distinct: %+v", v)
	}

	twoWayAway := away
	twoWayAway.OutcomeCardinality = 2
	if v := Evaluate(home, twoWayAway); v.Compatible || v.ReasonCode != RejectOutcomeCardinality {
		t.Fatalf("two-way/three-way outcome-universe mismatch must fail closed: %+v", v)
	}
}

func TestTwoOutcomeWinnerInverseStillRequiresExplicitOpposite(t *testing.T) {
	home := sports("winner")
	away := home
	away.Selection, away.Opposite = "KC", "PHI"
	away.Venue, away.InstrumentID = "polyus", "away-win"
	if v := Evaluate(home, away); !v.Compatible || v.Orientation != "inverse" {
		t.Fatalf("authoritative two-outcome opposite must remain invertible: %+v", v)
	}
	home.Opposite, away.Opposite = "", ""
	if v := Evaluate(home, away); v.Compatible || v.ReasonCode != RejectSelection {
		t.Fatalf("participant count alone must never manufacture an inverse: %+v", v)
	}
}

func TestObjectiveSportsTaxonomy(t *testing.T) {
	tests := []struct {
		name                string
		left, right         Contract
		ok                  bool
		orientation, reason string
	}{
		{"moneyline", sports("winner"), sports("winner"), true, "same", AcceptObjectiveExact},
		{"spread -1.5", func() Contract { c := sports("spread"); c.Comparator = "covers"; c.Threshold = "-1.5"; return c }(),
			func() Contract {
				c := sports("spread")
				c.Comparator = "covers"
				c.Threshold = "-1.5"
				c.Venue = "polyus"
				return c
			}(), true, "same", AcceptObjectiveExact},
		{"inverse spread", func() Contract { c := sports("spread"); c.Comparator = "covers"; c.Threshold = "-1.5"; return c }(),
			func() Contract {
				c := sports("spread")
				c.Comparator = "covers"
				c.Selection = "KC"
				c.Opposite = "PHI"
				c.Threshold = "1.5"
				return c
			}(), true, "inverse", AcceptObjectiveExact},
		{"total", func() Contract {
			c := sports("total")
			c.Selection = "TOTAL"
			c.Comparator = "over"
			c.Threshold = "8.5"
			return c
		}(),
			func() Contract {
				c := sports("total")
				c.Selection = "TOTAL"
				c.Comparator = "over"
				c.Threshold = "8.5"
				return c
			}(), true, "same", AcceptObjectiveExact},
		{"player scorer", func() Contract {
			c := sports("player_scorer")
			c.PlayerID = "PLAYER-7"
			c.Metric = "goals"
			c.Comparator = "gte"
			c.Threshold = "1"
			return c
		}(),
			func() Contract {
				c := sports("player_scorer")
				c.PlayerID = "PLAYER-7"
				c.Metric = "goals"
				c.Comparator = "gte"
				c.Threshold = "1"
				return c
			}(), true, "same", AcceptObjectiveExact},
		{"player prop", func() Contract {
			c := sports("player_prop")
			c.PlayerID = "PLAYER-9"
			c.Metric = "strikeouts"
			c.Comparator = "over"
			c.Threshold = "5.5"
			return c
		}(),
			func() Contract {
				c := sports("player_prop")
				c.PlayerID = "PLAYER-9"
				c.Metric = "strikeouts"
				c.Comparator = "over"
				c.Threshold = "5.5"
				return c
			}(), true, "same", AcceptObjectiveExact},
		{"overtime mismatch", sports("winner"), func() Contract { c := sports("winner"); c.OvertimePolicy = "regulation-only"; return c }(), false, "", RejectOvertime},
		{"void mismatch", sports("winner"), func() Contract { c := sports("winner"); c.VoidPolicy = "loss"; return c }(), false, "", RejectVoid},
		{"title only", func() Contract { c := sports("winner"); c.Structured = false; return c }(), sports("winner"), false, "", RejectTitleOnly},
		{"ambiguous side", func() Contract { c := sports("winner"); c.Selection = ""; return c }(), sports("winner"), false, "", RejectSelection},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			v := Evaluate(tc.left, tc.right)
			if v.Compatible != tc.ok || v.Orientation != tc.orientation || v.ReasonCode != tc.reason || len(v.EvidenceHash) != 64 {
				t.Fatalf("verdict=%+v", v)
			}
		})
	}
}

func TestObjectiveCryptoWeatherElectionAndDiscretionaryTaxonomy(t *testing.T) {
	crypto := Contract{Venue: "kalshi", InstrumentID: "K-BTC", Genre: "crypto", EventID: "btc:2026-07-12T20:00:00Z",
		Participants: []string{"BTCUSD"}, Period: "close", MarketType: "crypto_threshold", Selection: "BTCUSD",
		Metric: "price", Comparator: "gt", Threshold: "100000", DeadlineUTC: "2026-07-12T20:00:00Z", Timezone: "UTC",
		VoidPolicy: "invalid-refund", SettlementClass: "objective_result", SourceKind: "official_structured", Structured: true}
	crypto.PolicySource = "official-schema:crypto-v1"
	other := crypto
	other.Venue = "polymarket"
	if v := Evaluate(crypto, other); !v.Compatible || v.Orientation != "same" {
		t.Fatalf("crypto=%+v", v)
	}
	inv := other
	inv.Comparator = "lte"
	if v := Evaluate(crypto, inv); !v.Compatible || v.Orientation != "inverse" {
		t.Fatalf("inverse crypto=%+v", v)
	}
	for _, genre := range []string{"weather", "election", "politics", "judged"} {
		a, b := crypto, other
		a.Genre, b.Genre = genre, genre
		if v := Evaluate(a, b); v.Compatible || v.ReasonCode != RejectReviewGenre {
			t.Fatalf("%s=%+v", genre, v)
		}
	}
	a, b := crypto, other
	b.SettlementClass = "oracle_discretion"
	if v := Evaluate(a, b); v.Compatible || v.ReasonCode != RejectDiscretionary {
		t.Fatalf("discretionary=%+v", v)
	}
}

func TestObjectiveRejectsBoundaryAndIdentityDrift(t *testing.T) {
	base := sports("winner")
	checks := []struct {
		reason string
		mutate func(*Contract)
	}{
		{RejectEvent, func(c *Contract) { c.EventID = "sports:G2" }},
		{RejectParticipants, func(c *Contract) { c.Participants = []string{"PHI", "NYM"} }},
		{RejectPeriod, func(c *Contract) { c.Period = "first_half" }},
		{RejectTie, func(c *Contract) { c.TiePolicy = "draw-is-win" }},
		{RejectDeadline, func(c *Contract) { c.DeadlineUTC = "2026-07-12T21:00:00Z" }},
	}
	for _, tc := range checks {
		other := base
		tc.mutate(&other)
		if v := Evaluate(base, other); v.ReasonCode != tc.reason {
			t.Fatalf("want %s got %+v", tc.reason, v)
		}
	}
}

func TestObjectiveDirectionalTierNeverBecomesLockProof(t *testing.T) {
	left, right := sports("winner"), sports("winner")
	left.PolicySource, left.VoidPolicy, left.OvertimePolicy, left.TiePolicy = "", "", "", ""
	right.PolicySource, right.VoidPolicy, right.OvertimePolicy, right.TiePolicy = "", "", "", ""
	if strict := Evaluate(left, right); strict.Compatible || strict.ReasonCode != RejectPolicySourceMissing {
		t.Fatalf("strict material-policy gate=%+v", strict)
	}
	directional := EvaluateDirectional(left, right)
	if !directional.Compatible || directional.LockEligible || directional.Orientation != "same" ||
		directional.ReasonCode != AcceptObjectiveDirectional || directional.LockReasonCode != RejectPolicySourceMissing ||
		directional.RiskTier != "directional_only_material_policy_unverified" {
		t.Fatalf("directional tier=%+v", directional)
	}
	certified := Certified(directional)
	if !certified.LockEligible || certified.ReasonCode != AcceptObjectiveExact ||
		certified.RiskTier != "lock_safe_current_reviewed_certificate" || len(certified.EvidenceHash) != 64 {
		t.Fatalf("reviewed-certificate upgrade=%+v", certified)
	}
	bad := right
	bad.EventID = "sports:other"
	if v := EvaluateDirectional(left, bad); v.Compatible || v.ReasonCode != RejectEvent {
		t.Fatalf("directional tier bypassed core predicate=%+v", v)
	}
}
