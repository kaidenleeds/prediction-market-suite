package server

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/objectiveidentity"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func objectiveTestArtifact(source string) researchVenueRuleArtifact {
	return researchVenueRuleArtifact{Source: source, SportsType: "baseball_team_full_game_winner",
		VoidPolicy: "principal-refund", OvertimePolicy: "official-final-including-overtime",
		TiePolicy: "official-final-winner", MaterialPolicySource: source + ":official-structured-policy-v1"}
}

func objectiveTestPair(pairType, lv, lid, rv, rid string) storage.StructuralRulePair {
	return storage.StructuralRulePair{PairID: map[string]string{"K-PUS": "k-pus", "K-PINT": "k-pint", "PUS-PINT": "pus-pint"}[pairType] + "|" + lid + "|" + rid,
		PairType: pairType, GameID: "mlb:2026-07-12:PHI@KC", MarketType: "winner",
		League: "mlb", Away: "PHI", Home: "KC", LeftVenue: lv, LeftInstrumentID: lid,
		RightVenue: rv, RightInstrumentID: rid, LeftYesTeam: "PHI", RightYesTeam: "PHI",
		CanonicalEventID: "sports:mlb:2026-07-12:PHI@KC",
		LeftBoundaryUTC:  "2026-07-13T03:00:00Z", RightBoundaryUTC: "2026-07-13T03:00:00Z"}
}

func TestObjectiveAdaptersCoverEveryVenuePairAndIgnoreFriendlyNames(t *testing.T) {
	for _, tc := range []struct{ pair, lv, lid, rv, rid string }{
		{"K-PUS", "kalshi", "K-OBJ", "polyus", "pus-obj"},
		{"K-PINT", "kalshi", "K-OBJ", "polymarket", "0xobj"},
		{"PUS-PINT", "polyus", "pus-obj", "polymarket", "0xobj"},
	} {
		pair := objectiveTestPair(tc.pair, tc.lv, tc.lid, tc.rv, tc.rid)
		left := objectivePairContract(pair, true, objectiveTestArtifact(tc.lv))
		right := objectivePairContract(pair, false, objectiveTestArtifact(tc.rv))
		verdict := objectiveidentity.Evaluate(left, right)
		if !verdict.Compatible || verdict.Orientation != "same" || verdict.ReasonCode != objectiveidentity.AcceptObjectiveExact {
			t.Fatalf("%s verdict=%+v", tc.pair, verdict)
		}
		// Friendly labels are not Contract fields and therefore cannot alter the predicate hash.
		left2, right2 := left, right
		if got := objectiveidentity.Evaluate(left2, right2); got.PredicateHash != verdict.PredicateHash {
			t.Fatalf("%s objective hash drifted outside structured fields", tc.pair)
		}
	}
}

func TestObjectiveAdapterSeparatesIndependentPoliciesFromVenueOrderCloseTimes(t *testing.T) {
	pair := objectiveTestPair("K-PUS", "kalshi", "K-MISSING", "polyus", "pus-missing")
	left := objectivePairContract(pair, true, researchVenueRuleArtifact{Source: "kalshi", SportsType: "baseball_team_full_game_winner"})
	right := objectivePairContract(pair, false, objectiveTestArtifact("polyus"))
	if v := objectiveidentity.Evaluate(left, right); v.Compatible || v.ReasonCode != objectiveidentity.RejectPolicySourceMissing {
		t.Fatalf("missing independent policy source=%+v", v)
	}
	if v := objectiveidentity.EvaluateDirectional(left, right); !v.Compatible || v.LockEligible ||
		v.ReasonCode != objectiveidentity.AcceptObjectiveDirectional || v.LockReasonCode != objectiveidentity.RejectPolicySourceMissing {
		t.Fatalf("objective result was not admitted directionally and kept out of locks=%+v", v)
	}
	pair.RightBoundaryUTC = "2026-07-13T05:00:00Z"
	left = objectivePairContract(pair, true, objectiveTestArtifact("kalshi"))
	right = objectivePairContract(pair, false, objectiveTestArtifact("polyus"))
	if v := objectiveidentity.Evaluate(left, right); !v.Compatible || !v.LockEligible ||
		v.NormalizedLeft.VenueCloseUTC == v.NormalizedRight.VenueCloseUTC {
		t.Fatalf("venue order-close times contaminated the shared sports payoff identity=%+v", v)
	}
}

func objectiveScopedTestPair(gameID, league, away, home, kalshiTicker, polyUSSlug string,
	kalshiLine, polyUSLine float64) storage.StructuralRulePair {
	return storage.StructuralRulePair{
		PairID: "k-pus|" + kalshiTicker + "|" + polyUSSlug, PairType: "K-PUS",
		GameID: gameID, MarketType: "prop", LeftMarketType: "prop", RightMarketType: "prop",
		League: league, Away: away, Home: home, StartUTC: "2026-07-18T21:00:00Z",
		LeftVenue: "kalshi", LeftInstrumentID: kalshiTicker, RightVenue: "polyus",
		RightInstrumentID: polyUSSlug, LeftLine: kalshiLine, RightLine: polyUSLine,
		CanonicalEventID: "sports:" + gameID,
		LeftBoundaryUTC:  "2026-07-19T00:00:00Z", RightBoundaryUTC: "2026-07-18T21:00:00Z",
	}
}

func objectiveScopedArtifacts(polySportsType string) (researchVenueRuleArtifact, researchVenueRuleArtifact) {
	left := objectiveTestArtifact("kalshi")
	left.SportsType = ""
	right := objectiveTestArtifact("polyus")
	right.SportsType = polySportsType
	return left, right
}

func TestObjectiveExactScopedSportsNormalizesThe57MeasuredFalseRejects(t *testing.T) {
	type mlbFixture struct {
		gameID, away, home, kalEvent, polyEvent, kalAway string
		totalLines                                       []int
	}
	mlb := []mlbFixture{
		{"mlb:2026-07-12:ARI@LAD", "ARI", "LAD", "26JUL121610AZLAD", "az-lad-2026-07-12", "AZ", []int{2, 3, 4, 5, 6}},
		{"mlb:2026-07-16:NYM@PHI", "NYM", "PHI", "26JUL161910NYMPHI", "nym-phi-2026-07-16", "NYM", []int{2, 3, 4, 5, 6}},
		{"mlb:2026-07-17:CWS@TOR", "CWS", "TOR", "26JUL171915CWSTOR", "cws-tor-2026-07-17", "CWS", []int{2}},
		{"mlb:2026-07-17:PIT@CLE", "PIT", "CLE", "26JUL171910PITCLE", "pit-cle-2026-07-17", "PIT", []int{2, 3, 4, 5, 6}},
		{"mlb:2026-07-17:SD@KC", "SD", "KC", "26JUL172010SDKC", "sd-kc-2026-07-17", "SD", []int{2, 3, 4, 5, 6}},
	}
	kalArtifact, spreadArtifact := objectiveScopedArtifacts("baseball_team_first_five_spread")
	_, totalArtifact := objectiveScopedArtifacts("baseball_team_first_five_total")
	normalized := 0
	for _, game := range mlb {
		for lineWhole := 1; lineWhole <= 2; lineWhole++ {
			magnitude := float64(lineWhole) + 0.5
			kalSuffix := strconv.Itoa(lineWhole + 1)
			polySuffix := strconv.Itoa(lineWhole) + "pt5"
			for _, tc := range []struct {
				name, kalTeam, polySign, orientation string
				polyLine                             float64
			}{
				{"same", game.kalAway, "neg", "same", -magnitude},
				{"inverse", game.home, "pos", "inverse", magnitude},
			} {
				pair := objectiveScopedTestPair(game.gameID, "mlb", game.away, game.home,
					"KXMLBF5SPREAD-"+game.kalEvent+"-"+tc.kalTeam+kalSuffix,
					"asc-mlb-"+game.polyEvent+"-f5-"+tc.polySign+"-"+polySuffix, magnitude, tc.polyLine)
				left := objectivePairContract(pair, true, kalArtifact)
				right := objectivePairContract(pair, false, spreadArtifact)
				verdict := objectiveidentity.EvaluateDirectional(left, right)
				if !verdict.Compatible || verdict.Orientation != tc.orientation ||
					left.Period != "first_five" || right.Period != "first_five" ||
					left.MarketType != "spread" || right.MarketType != "spread" {
					t.Fatalf("%s %s scoped spread verdict=%+v left=%+v right=%+v", game.gameID, tc.name, verdict, left, right)
				}
				normalized++
			}
		}
		for _, lineWhole := range game.totalLines {
			line := float64(lineWhole) + 0.5
			pair := objectiveScopedTestPair(game.gameID, "mlb", game.away, game.home,
				"KXMLBF5TOTAL-"+game.kalEvent+"-"+strconv.Itoa(lineWhole+1),
				"tsc-mlb-"+game.polyEvent+"-f5-"+strconv.Itoa(lineWhole)+"pt5", line, line)
			left := objectivePairContract(pair, true, kalArtifact)
			right := objectivePairContract(pair, false, totalArtifact)
			verdict := objectiveidentity.EvaluateDirectional(left, right)
			if !verdict.Compatible || verdict.Orientation != "same" ||
				left.Period != "first_five" || right.Period != "first_five" ||
				left.MarketType != "total" || right.MarketType != "total" ||
				left.Threshold != right.Threshold {
				t.Fatalf("%s scoped total verdict=%+v left=%+v right=%+v", game.gameID, verdict, left, right)
			}
			normalized++
		}
	}

	fwc := []struct {
		gameID, away, home, kalEvent, polyEvent string
	}{
		{"fwc:2026-07-14:ESP@FRA", "ESP", "FRA", "26JUL14FRAESP", "fra-esp-2026-07-14"},
		{"fwc:2026-07-15:ARG@ENG", "ARG", "ENG", "26JUL15ENGARG", "eng-arg-2026-07-15"},
		{"fwc:2026-07-18:ENG@FRA", "ENG", "FRA", "26JUL18FRAENG", "fra-eng-2026-07-18"},
		{"fwc:2026-07-19:ARG@ESP", "ARG", "ESP", "26JUL19ESPARG", "esp-arg-2026-07-19"},
	}
	_, firstHalfArtifact := objectiveScopedArtifacts("soccer_team_first_half_total")
	for _, game := range fwc {
		for lineWhole := 0; lineWhole <= 3; lineWhole++ {
			line := float64(lineWhole) + 0.5
			pair := objectiveScopedTestPair(game.gameID, "fwc", game.away, game.home,
				"KXWC1HTOTAL-"+game.kalEvent+"-"+strconv.Itoa(lineWhole+1),
				"tsc-fwc-"+game.polyEvent+"-fh-"+strconv.Itoa(lineWhole)+"pt5", line, line)
			left := objectivePairContract(pair, true, kalArtifact)
			right := objectivePairContract(pair, false, firstHalfArtifact)
			verdict := objectiveidentity.EvaluateDirectional(left, right)
			if !verdict.Compatible || verdict.Orientation != "same" ||
				left.Period != "first_half" || right.Period != "first_half" ||
				left.MarketType != "total" || right.MarketType != "total" ||
				left.Threshold != right.Threshold {
				t.Fatalf("%s scoped first-half total verdict=%+v left=%+v right=%+v", game.gameID, verdict, left, right)
			}
			normalized++
		}
	}
	if normalized != 57 {
		t.Fatalf("normalized measured pairs=%d, want 57", normalized)
	}
}

func TestObjectiveExactScopedSportsStillRejectsWrongLinePeriodAndFamily(t *testing.T) {
	kalArtifact, f5TotalArtifact := objectiveScopedArtifacts("baseball_team_first_five_total")
	lineMismatch := objectiveScopedTestPair("mlb:2026-07-12:ARI@LAD", "mlb", "ARI", "LAD",
		"KXMLBF5TOTAL-26JUL121610AZLAD-3", "tsc-mlb-az-lad-2026-07-12-f5-3pt5", 2.5, 3.5)
	left := objectivePairContract(lineMismatch, true, kalArtifact)
	right := objectivePairContract(lineMismatch, false, f5TotalArtifact)
	if verdict := objectiveidentity.EvaluateDirectional(left, right); verdict.Compatible ||
		verdict.ReasonCode != objectiveidentity.RejectThreshold {
		t.Fatalf("different first-five lines matched: verdict=%+v left=%+v right=%+v", verdict, left, right)
	}

	_, secondHalfArtifact := objectiveScopedArtifacts("soccer_team_second_half_total")
	periodMismatch := objectiveScopedTestPair("fwc:2026-07-15:ARG@ENG", "fwc", "ARG", "ENG",
		"KXWC1HTOTAL-26JUL15ENGARG-1", "tsc-fwc-eng-arg-2026-07-15-sh-0pt5", 0.5, 0.5)
	left = objectivePairContract(periodMismatch, true, kalArtifact)
	right = objectivePairContract(periodMismatch, false, secondHalfArtifact)
	if verdict := objectiveidentity.EvaluateDirectional(left, right); verdict.Compatible ||
		verdict.ReasonCode != objectiveidentity.RejectPeriod {
		t.Fatalf("first-half/second-half mismatch matched: verdict=%+v left=%+v right=%+v", verdict, left, right)
	}

	if _, ok := objectiveExactScopedSportsTerms("polyus", "tsc-wnba-sea-atl-2026-07-09-fh-79pt5",
		"wnba", "SEA", "ATL", "basketball_team_first_half_total", 79.5); ok {
		t.Fatal("out-of-scope WNBA title/side shape was normalized")
	}
	if _, ok := objectiveExactScopedSportsTerms("polyus", "tsc-mlb-az-lad-2026-07-12-f5-2pt5",
		"mlb", "ARI", "LAD", "baseball_team_full_game_total", 2.5); ok {
		t.Fatal("wrong official scope metadata was normalized")
	}
	if _, ok := objectiveExactScopedSportsTerms("kalshi", "KXMLBF5TOTAL-26JUL121610AZLAD-3",
		"mlb", "ARI", "LAD", "", 3.5); ok {
		t.Fatal("ticker/structural-line disagreement was normalized")
	}
}

func objectivePlayerTestPair(pairType, lv, lid, rv, rid string) storage.StructuralRulePair {
	pair := objectiveTestPair(pairType, lv, lid, rv, rid)
	pair.MarketType, pair.LeftMarketType, pair.RightMarketType = "prop", "prop", "player_prop"
	pair.LeftYesTeam, pair.RightYesTeam = "", ""
	pair.LeftLine, pair.RightLine = 25.5, 25.5
	pair.LeftTitle = "Lakers at Celtics — LeBron James over 25.5 points (full game)"
	pair.RightTitle = "Will LeBron James record over 25.5 points in the full game?"
	pair.League, pair.Away, pair.Home = "nba", "LAL", "BOS"
	pair.GameID, pair.CanonicalEventID = "nba:2026-07-12:LAL@BOS", "sports:nba:2026-07-12:LAL@BOS"
	return pair
}

func objectivePlayerTestArtifact(source, period, side string) researchVenueRuleArtifact {
	if period == "" {
		period = "full_game"
	}
	if side == "" {
		side = "over"
	}
	artifact := objectiveTestArtifact(source)
	artifact.SportsType = "basketball_player_" + period + "_points_" + side
	return artifact
}

func TestObjectiveOrdinaryPlayerPropsAreVenuePairAgnostic(t *testing.T) {
	for _, tc := range []struct{ pairType, lv, lid, rv, rid string }{
		{"K-PUS", "kalshi", "K-PLAYER", "polyus", "P-PLAYER"},
		{"K-PINT", "kalshi", "K-PLAYER", "polymarket", "I-PLAYER"},
		{"PUS-PINT", "polyus", "P-PLAYER", "polymarket", "I-PLAYER"},
	} {
		pair := objectivePlayerTestPair(tc.pairType, tc.lv, tc.lid, tc.rv, tc.rid)
		left := objectivePairContract(pair, true, objectivePlayerTestArtifact(tc.lv, "full_game", "over"))
		right := objectivePairContract(pair, false, objectivePlayerTestArtifact(tc.rv, "full_game", "over"))
		if verdict := objectiveidentity.Evaluate(left, right); !verdict.Compatible || verdict.Orientation != "same" ||
			verdict.ReasonCode != objectiveidentity.AcceptObjectiveExact {
			t.Errorf("%s ordinary player prop=%+v left=%+v right=%+v", tc.pairType, verdict, left, right)
		}
	}
}

func TestObjectivePlayerPropsUseExactPlayerLinePeriodAndSideAndLogEveryDecision(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	started := time.Date(2026, 7, 12, 22, 0, 0, 0, time.UTC)
	base := objectivePlayerTestPair("K-PUS", "kalshi", "K-PLAYER", "polyus", "P-PLAYER")
	leftArtifact := objectivePlayerTestArtifact("kalshi", "full_game", "over")
	rightArtifact := objectivePlayerTestArtifact("polyus", "full_game", "over")

	type fixture struct {
		name, rightID, wantReason string
		mutate                    func(*storage.StructuralRulePair, *researchVenueRuleArtifact)
		wantCompatible            bool
		wantOrientation           string
	}
	fixtures := []fixture{
		{name: "ordinary player prop", rightID: "P-EXACT", wantReason: objectiveidentity.AcceptObjectiveExact,
			wantCompatible: true, wantOrientation: "same"},
		{name: "different player", rightID: "P-PLAYER-MISMATCH", wantReason: objectiveidentity.RejectPlayer,
			mutate: func(pair *storage.StructuralRulePair, _ *researchVenueRuleArtifact) {
				pair.RightTitle = "Will Jayson Tatum record over 25.5 points in the full game?"
			}},
		{name: "different line", rightID: "P-LINE-MISMATCH", wantReason: objectiveidentity.RejectThreshold,
			mutate: func(pair *storage.StructuralRulePair, _ *researchVenueRuleArtifact) { pair.RightLine = 26.5 }},
		{name: "different period", rightID: "P-PERIOD-MISMATCH", wantReason: objectiveidentity.RejectPeriod,
			mutate: func(_ *storage.StructuralRulePair, artifact *researchVenueRuleArtifact) {
				artifact.SportsType = "basketball_player_first_half_points_over"
			}},
		{name: "complementary side", rightID: "P-INVERSE", wantReason: objectiveidentity.AcceptObjectiveExact,
			wantCompatible: true, wantOrientation: "inverse",
			mutate: func(pair *storage.StructuralRulePair, artifact *researchVenueRuleArtifact) {
				pair.RightTitle = "Will LeBron James record under 25.5 points in the full game?"
				artifact.SportsType = "basketball_player_full_game_points_under"
			}},
		{name: "unrecognized side", rightID: "P-SIDE-MISMATCH", wantReason: objectiveidentity.RejectComparator,
			mutate: func(pair *storage.StructuralRulePair, artifact *researchVenueRuleArtifact) {
				pair.RightTitle = "Will LeBron James record exactly 25.5 points in the full game?"
				artifact.SportsType = "basketball_player_full_game_points_exact"
			}},
	}
	for index, tc := range fixtures {
		pair, artifact := base, rightArtifact
		pair.RightInstrumentID = tc.rightID
		pair.PairID = "k-pus|" + pair.LeftInstrumentID + "|" + tc.rightID
		if tc.mutate != nil {
			tc.mutate(&pair, &artifact)
		}
		left := objectivePairContract(pair, true, leftArtifact)
		right := objectivePairContract(pair, false, artifact)
		verdict := objectiveidentity.Evaluate(left, right)
		if verdict.Compatible != tc.wantCompatible || verdict.ReasonCode != tc.wantReason ||
			(tc.wantOrientation != "" && verdict.Orientation != tc.wantOrientation) {
			t.Errorf("%s verdict=%+v left=%+v right=%+v", tc.name, verdict, left, right)
		}
		if inserted, err := s.recordObjectiveMatchDecision(ctx, started.Add(time.Duration(index)*time.Second),
			pair, left, right, verdict, ""); err != nil || !inserted {
			t.Fatalf("%s decision log inserted=%v err=%v", tc.name, inserted, err)
		}
	}
	report, err := s.store.CrossVenueDecisionReport(ctx, 20)
	if err != nil {
		t.Fatal(err)
	}
	recent, ok := report["recent"].([]storage.CrossVenueDecisionView)
	if !ok || len(recent) != len(fixtures) {
		t.Fatalf("every considered player decision must be durable: %T %+v", report["recent"], report["recent"])
	}
	seen := map[string]string{}
	for _, row := range recent {
		seen[row.RightInstrumentID] = row.ReasonCode
	}
	for _, tc := range fixtures {
		if seen[tc.rightID] != tc.wantReason {
			t.Errorf("%s missing/reclassified in append-only ledger: got=%q all=%v", tc.name, seen[tc.rightID], seen)
		}
	}
}

func TestObjectiveAnchorContractsAdmitCryptoDirectionallyAndReviewWeatherPolitics(t *testing.T) {
	crypto := objectiveAnchorCandidate{pairType: "K-PINT", pairID: "k-pint|K-BTC|0xbtc",
		leftVenue: "kalshi", leftID: "K-BTC", rightVenue: "polymarket", rightID: "0xbtc",
		key: "crypto|BTC|2026-07-12T20:00:00Z|T100000|up", kind: "crypto"}
	if v := objectiveidentity.EvaluateDirectional(objectiveAnchorContract(crypto, true), objectiveAnchorContract(crypto, false)); !v.Compatible || v.LockEligible || v.ReasonCode != objectiveidentity.AcceptObjectiveDirectional {
		t.Fatalf("crypto anchor=%+v", v)
	}
	for _, tc := range []struct{ kind, key, reason string }{
		{"wx", "wx|NYC|high|2026-07-12|84-85F", objectiveidentity.RejectReviewGenre},
		{"pol", "pol|president|2028", objectiveidentity.RejectReviewGenre},
		{"econ", "econ|us-cpi-yoy|2026-06", objectiveidentity.RejectReviewGenre},
	} {
		candidate := crypto
		candidate.kind, candidate.key = tc.kind, tc.key
		if v := objectiveidentity.EvaluateDirectional(objectiveAnchorContract(candidate, true), objectiveAnchorContract(candidate, false)); v.Compatible || v.ReasonCode != tc.reason {
			t.Fatalf("%s anchor=%+v", tc.kind, v)
		}
	}
}

func TestObjectiveAnchorPagesUseKeyAndOffsetCursorWithoutStarvation(t *testing.T) {
	s := testServer(t)
	xvReg.reset()
	defer xvReg.reset()
	xvReg.mu.Lock()
	for i := 0; i < 5; i++ {
		key := "crypto|BTC|2026-07-12T2" + string(rune('0'+i)) + ":00:00Z|T100000|up"
		xvReg.orderedKeys = append(xvReg.orderedKeys, key)
		xvReg.byKey[key] = &xvEntry{kind: "crypto", ids: map[string][]string{
			"kalshi": {"K-" + string(rune('A'+i))}, "polymarket": {"p-" + string(rune('A'+i))}}}
	}
	xvReg.mu.Unlock()
	cursor := objectiveAnchorCursor{}
	seen := map[string]bool{}
	for n := 0; n < 3; n++ {
		page := s.objectiveAnchorCandidates(cursor, 2, 2)
		if len(page.Candidates) == 0 || len(page.Candidates) > 2 {
			t.Fatalf("page %d=%+v", n, page)
		}
		for _, candidate := range page.Candidates {
			seen[candidate.key] = true
		}
		cursor = page.Next
	}
	if len(seen) != 5 {
		t.Fatalf("anchor rotation saw %d/5 keys: %v", len(seen), seen)
	}
	page := s.objectiveAnchorCandidates(cursor, 2, 2)
	if !page.Wrapped || len(page.Candidates) == 0 {
		t.Fatalf("anchor cursor did not wrap: %+v", page)
	}
}

func TestObjectiveAcceptedAndRejectedDecisionsAreBothAppendOnlyAndKeepRawIDs(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	started := time.Date(2026, 7, 12, 20, 0, 0, 0, time.UTC)
	acceptedPair := objectiveTestPair("K-PUS", "kalshi", "K-ACCEPT", "polyus", "pus-accept")
	left := objectivePairContract(acceptedPair, true, objectiveTestArtifact("kalshi"))
	right := objectivePairContract(acceptedPair, false, objectiveTestArtifact("polyus"))
	accepted := objectiveidentity.Evaluate(left, right)
	if inserted, err := s.recordObjectiveMatchDecision(ctx, started, acceptedPair, left, right, accepted, ""); err != nil || !inserted {
		t.Fatalf("accepted log inserted=%v err=%v", inserted, err)
	}
	rejectedPair := objectiveTestPair("K-PINT", "kalshi", "K-REJECT", "polymarket", "0xreject")
	rleft := objectivePairContract(rejectedPair, true, objectiveTestArtifact("kalshi"))
	rright := objectivePairContract(rejectedPair, false, researchVenueRuleArtifact{Source: "Gamma", SportsType: "moneyline"})
	rejected := objectiveidentity.Evaluate(rleft, rright)
	if inserted, err := s.recordObjectiveMatchDecision(ctx, started, rejectedPair, rleft, rright, rejected, ""); err != nil || !inserted {
		t.Fatalf("rejected log inserted=%v err=%v verdict=%+v", inserted, err, rejected)
	}
	directionalPair := objectiveTestPair("PUS-PINT", "polyus", "pus-directional", "polymarket", "0xdirectional")
	dleft := objectivePairContract(directionalPair, true, researchVenueRuleArtifact{Source: "PolyUS", SportsType: "baseball_team_full_game_winner"})
	dright := objectivePairContract(directionalPair, false, researchVenueRuleArtifact{Source: "Gamma", SportsType: "moneyline"})
	directional := objectiveidentity.EvaluateDirectional(dleft, dright)
	if !directional.Compatible || directional.LockEligible {
		t.Fatalf("directional fixture=%+v", directional)
	}
	if inserted, err := s.recordObjectiveMatchDecision(ctx, started, directionalPair, dleft, dright, directional, ""); err != nil || !inserted {
		t.Fatalf("directional log inserted=%v err=%v", inserted, err)
	}
	report, err := s.store.CrossVenueDecisionReport(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	recent, ok := report["recent"].([]storage.CrossVenueDecisionView)
	if !ok || len(recent) != 3 {
		t.Fatalf("recent=%T %+v", report["recent"], report["recent"])
	}
	seen := map[string]bool{}
	for _, row := range recent {
		seen[row.LeftInstrumentID+"|"+row.RightInstrumentID] = true
	}
	if !seen["K-ACCEPT|pus-accept"] || !seen["K-REJECT|0xreject"] {
		t.Fatalf("raw IDs lost: %+v", recent)
	}
	var foundDirectional bool
	for _, row := range recent {
		if row.LeftInstrumentID == "pus-directional" {
			foundDirectional = row.Accepted && !row.LockEligible && row.ReasonCode == objectiveidentity.AcceptObjectiveDirectional
		}
	}
	if !foundDirectional {
		t.Fatalf("directional-only risk tier was not retained: %+v", recent)
	}
	if _, err := s.store.DBForTest().Exec(`UPDATE research_crossvenue_match_decisions SET reason_code='REJECT_MUTATED'`); err == nil {
		t.Fatal("decision ledger was mutable")
	}
}
