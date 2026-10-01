package server

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/objectiveidentity"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func objectiveSportsPeriod(sportsType, marketType string) string {
	t := strings.ToLower(strings.TrimSpace(sportsType))
	t = strings.NewReplacer("-", "_", " ", "_", "/", "_").Replace(t)
	for _, scoped := range []struct{ token, period string }{
		{"first_five", "first_five"}, {"first_half", "first_half"},
		{"second_half", "second_half"}, {"first_quarter", "first_quarter"},
		{"second_quarter", "second_quarter"}, {"third_quarter", "third_quarter"},
		{"fourth_quarter", "fourth_quarter"}, {"first_period", "first_period"},
		{"second_period", "second_period"}, {"third_period", "third_period"},
	} {
		if strings.Contains(t, scoped.token) {
			return scoped.period
		}
	}
	if strings.Contains(t, "to_advance") || strings.EqualFold(marketType, "advance") {
		return "advance"
	}
	if strings.Contains(t, "full_game") || strings.Contains(t, "full_time") || t == "moneyline" || t == "spreads" || t == "totals" ||
		strings.EqualFold(marketType, "winner") || strings.EqualFold(marketType, "spread") || strings.EqualFold(marketType, "total") {
		return "full_game"
	}
	return ""
}

// objectiveSportsOutcomeCardinality describes the exhaustive winner family, not the binary
// YES/NO sides of one venue contract. In particular, a soccer match has two participants but
// Home/Draw/Away are three mutually-exclusive outcomes. Unknown leagues stay zero and therefore
// cannot manufacture a different-selection inverse.
func objectiveSportsOutcomeCardinality(league, sportsType, marketType string) int {
	marketType = strings.ToLower(strings.TrimSpace(marketType))
	if marketType == "advance" || strings.Contains(strings.ToLower(sportsType), "to_advance") {
		return 2
	}
	if marketType != "winner" {
		return 0
	}
	t := strings.ToLower(strings.TrimSpace(sportsType))
	lg := strings.ToLower(strings.TrimSpace(league))
	if strings.Contains(t, "drawable_outcome") || strings.Contains(t, "soccer") {
		return 3
	}
	switch lg {
	case "soccer", "mls", "epl", "ucl", "uefa", "fwc", "fifa", "bun", "bundesliga",
		"laliga", "la_liga", "seriea", "serie_a", "ligue1", "ligue_1":
		return 3
	case "mlb", "nba", "wnba", "nhl", "ncaab", "ncaaf", "atp", "wta", "tennis",
		"ufc", "boxing":
		return 2
	default:
		return 0
	}
}

var (
	objectivePlayerNameCleanRE = regexp.MustCompile(`[^\pL]+`)
	objectivePlayerLeadRE      = regexp.MustCompile(`(?i)^(?:will\s+)?([\pL][\pL .'-]{0,70}?)\s+(?:over|under|to\s+(?:score|record|make|hit|have|get|throw)|scores?|records?|makes?|hits?|has|gets?|throws?)\b`)
	objectiveExplicitPlayerRE  = regexp.MustCompile(`(?i)(?:^|[-_])(?:player|athlete):([a-z][a-z'_-]{1,50})$`)
	objectiveComparedNumberRE  = regexp.MustCompile(`(?i)\b(?:over|under|at\s+least|more\s+than|less\s+than)\s+(-?[0-9]+(?:\.[0-9]+)?)`)
	objectivePlusNumberRE      = regexp.MustCompile(`(?i)\b([0-9]+(?:\.[0-9]+)?)\s*\+`)
	objectiveKalshiMLBF5Spread = regexp.MustCompile(`^KXMLBF5SPREAD-[A-Z0-9]+-([A-Z]{2,4})([0-9]+)$`)
	objectiveKalshiMLBF5Total  = regexp.MustCompile(`^KXMLBF5TOTAL-[A-Z0-9]+-([0-9]+)$`)
	objectiveKalshiFWC1HTotal  = regexp.MustCompile(`^KXWC1HTOTAL-[A-Z0-9]+-([0-9]+)$`)
	objectivePolyUSMLBF5Spread = regexp.MustCompile(`^asc-mlb-([a-z0-9]+)-[a-z0-9]+-[0-9]{4}-[0-9]{2}-[0-9]{2}-f5-(neg|pos)-([0-9]+)pt5$`)
	objectivePolyUSMLBF5Total  = regexp.MustCompile(`^tsc-mlb-[a-z0-9]+-[a-z0-9]+-[0-9]{4}-[0-9]{2}-[0-9]{2}-f5-([0-9]+)pt5$`)
	objectivePolyUSFWC1HTotal  = regexp.MustCompile(`^tsc-fwc-[a-z0-9]+-[a-z0-9]+-[0-9]{4}-[0-9]{2}-[0-9]{2}-fh-([0-9]+)pt5$`)
)

type objectiveScopedSportsTerms struct {
	period, marketType, selection, opposite string
	line                                    float64
}

func objectiveCanonicalTeamAbbr(league, raw string) string {
	raw = strings.ToUpper(strings.TrimSpace(raw))
	for _, row := range giSeed[strings.ToLower(strings.TrimSpace(league))] {
		fields := strings.Split(row, "|")
		if len(fields) < 2 {
			continue
		}
		canonical := strings.ToUpper(strings.TrimSpace(fields[0]))
		if raw == canonical {
			return canonical
		}
		if len(fields) >= 3 {
			for _, alias := range strings.Split(fields[2], ",") {
				if raw == strings.ToUpper(strings.TrimSpace(alias)) {
					return canonical
				}
			}
		}
	}
	return ""
}

func objectiveScopedOpponent(league, selection, away, home string) (string, bool) {
	selection = objectiveCanonicalTeamAbbr(league, selection)
	away = objectiveCanonicalTeamAbbr(league, away)
	home = objectiveCanonicalTeamAbbr(league, home)
	switch {
	case selection == "" || away == "" || home == "" || away == home:
		return "", false
	case selection == away:
		return home, true
	case selection == home:
		return away, true
	default:
		return "", false
	}
}

func objectiveHalfLine(raw string, kalshi bool) (float64, bool) {
	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 || (kalshi && n < 1) {
		return 0, false
	}
	if kalshi {
		return float64(n) - 0.5, true
	}
	return float64(n) + 0.5, true
}

// objectiveExactScopedSportsTerms closes only the three structured holes measured in the R158
// decision ledger. It deliberately does not use title similarity:
//   - Kalshi MLB first-five spreads/totals,
//   - PolyUS MLB first-five spreads/totals, and
//   - Kalshi/PolyUS World Cup first-half totals.
//
// Every grammar is venue-, league-, period-, type-, side-, and line-specific. The independently
// recorded structural line must agree with the identifier before a generic "prop" row can be
// normalized. Rule artifacts, material-policy review, Paper authority, and LIVE authority remain
// downstream gates; this helper establishes only the objective payoff predicate.
func objectiveExactScopedSportsTerms(venue, instrumentID, league, away, home, sportsType string,
	structuralLine float64) (objectiveScopedSportsTerms, bool) {
	venue = strings.ToLower(strings.TrimSpace(venue))
	instrumentID = strings.TrimSpace(instrumentID)
	league = strings.ToLower(strings.TrimSpace(league))
	sportsType = strings.ToLower(strings.TrimSpace(sportsType))
	lineMatches := func(want float64) bool { return math.Abs(structuralLine-want) < 1e-9 }

	if venue == "kalshi" && league == "mlb" {
		upperID := strings.ToUpper(instrumentID)
		if match := objectiveKalshiMLBF5Spread.FindStringSubmatch(upperID); len(match) == 3 {
			magnitude, ok := objectiveHalfLine(match[2], true)
			opposite, teamOK := objectiveScopedOpponent(league, match[1], away, home)
			if !ok || !teamOK || math.Abs(math.Abs(structuralLine)-magnitude) >= 1e-9 {
				return objectiveScopedSportsTerms{}, false
			}
			return objectiveScopedSportsTerms{period: "first_five", marketType: "spread",
				selection: objectiveCanonicalTeamAbbr(league, match[1]), opposite: opposite, line: -magnitude}, true
		}
		if match := objectiveKalshiMLBF5Total.FindStringSubmatch(upperID); len(match) == 2 {
			line, ok := objectiveHalfLine(match[1], true)
			if !ok || !lineMatches(line) {
				return objectiveScopedSportsTerms{}, false
			}
			return objectiveScopedSportsTerms{period: "first_five", marketType: "total",
				selection: "over", line: line}, true
		}
	}
	if venue == "kalshi" && league == "fwc" {
		if match := objectiveKalshiFWC1HTotal.FindStringSubmatch(strings.ToUpper(instrumentID)); len(match) == 2 {
			line, ok := objectiveHalfLine(match[1], true)
			if !ok || !lineMatches(line) {
				return objectiveScopedSportsTerms{}, false
			}
			return objectiveScopedSportsTerms{period: "first_half", marketType: "total",
				selection: "over", line: line}, true
		}
	}
	if venue == "polyus" && league == "mlb" {
		lowerID := strings.ToLower(instrumentID)
		if sportsType == "baseball_team_first_five_spread" {
			if match := objectivePolyUSMLBF5Spread.FindStringSubmatch(lowerID); len(match) == 4 {
				line, ok := objectiveHalfLine(match[3], false)
				if match[2] == "neg" {
					line = -line
				}
				selection := objectiveCanonicalTeamAbbr(league, match[1])
				opposite, teamOK := objectiveScopedOpponent(league, selection, away, home)
				if !ok || !teamOK || !lineMatches(line) {
					return objectiveScopedSportsTerms{}, false
				}
				return objectiveScopedSportsTerms{period: "first_five", marketType: "spread",
					selection: selection, opposite: opposite, line: line}, true
			}
		}
		if sportsType == "baseball_team_first_five_total" {
			if match := objectivePolyUSMLBF5Total.FindStringSubmatch(lowerID); len(match) == 2 {
				line, ok := objectiveHalfLine(match[1], false)
				if !ok || !lineMatches(line) {
					return objectiveScopedSportsTerms{}, false
				}
				return objectiveScopedSportsTerms{period: "first_five", marketType: "total",
					selection: "over", line: line}, true
			}
		}
	}
	if venue == "polyus" && league == "fwc" && sportsType == "soccer_team_first_half_total" {
		if match := objectivePolyUSFWC1HTotal.FindStringSubmatch(strings.ToLower(instrumentID)); len(match) == 2 {
			line, ok := objectiveHalfLine(match[1], false)
			if !ok || !lineMatches(line) {
				return objectiveScopedSportsTerms{}, false
			}
			return objectiveScopedSportsTerms{period: "first_half", marketType: "total",
				selection: "over", line: line}, true
		}
	}
	return objectiveScopedSportsTerms{}, false
}

func objectiveCanonicalPlayerName(raw string) string {
	raw = strings.TrimSpace(raw)
	upper := strings.ToUpper(raw)
	for _, prefix := range []string{"PLAYER:", "ATHLETE:"} {
		if strings.HasPrefix(upper, prefix) {
			raw = strings.TrimSpace(raw[len(prefix):])
			break
		}
	}
	name := strings.Join(strings.Fields(objectivePlayerNameCleanRE.ReplaceAllString(raw, " ")), " ")
	if len(name) < 2 || len(name) > 64 || len(strings.Fields(name)) > 6 {
		return ""
	}
	switch strings.ToLower(name) {
	case "yes", "no", "over", "under", "total", "player", "athlete", "field", "any player":
		return ""
	}
	return strings.ToUpper(name)
}

// objectivePlayerID reads only venue-native structural identity fields and tightly-shaped raw
// catalog labels. friendlyName output is never passed here. A loose title resemblance therefore
// cannot create a match: both legs must independently yield the same player, metric, line and
// period, otherwise objectiveidentity emits a durable reject.
func objectivePlayerID(selection, title, instrumentID string) string {
	selection = strings.TrimSpace(selection)
	selectionUpper := strings.ToUpper(selection)
	explicitSelection := strings.HasPrefix(selectionUpper, "PLAYER:") || strings.HasPrefix(selectionUpper, "ATHLETE:")
	selectionPlayer := ""
	if explicitSelection || len(strings.Fields(selection)) >= 2 {
		selectionPlayer = objectiveCanonicalPlayerName(selection)
	}
	resolved := ""
	merge := func(player string) bool {
		if player == "" {
			return true
		}
		if resolved == "" {
			resolved = player
			return true
		}
		return resolved == player
	}
	if explicitSelection && !merge(selectionPlayer) {
		return ""
	}
	if match := objectiveExplicitPlayerRE.FindStringSubmatch(instrumentID); len(match) == 2 {
		if explicit := objectiveCanonicalPlayerName(match[1]); explicit != "" {
			if !merge(explicit) {
				return ""
			}
		}
	}
	candidates := []string{strings.TrimSpace(title)}
	for _, separator := range []string{" — ", " | "} {
		if i := strings.LastIndex(title, separator); i >= 0 {
			candidates = []string{strings.TrimSpace(title[i+len(separator):])}
			break
		}
	}
	for _, candidate := range candidates {
		if match := objectivePlayerLeadRE.FindStringSubmatch(candidate); len(match) == 2 {
			if player := objectiveCanonicalPlayerName(match[1]); player != "" {
				if !merge(player) {
					return ""
				}
				continue
			}
		}
		// A venue subtitle that is just a person's name is authoritative structured metadata. Do
		// not apply this rule to the full question, which could contain teams and narrative prose.
		if candidate != strings.TrimSpace(title) {
			if !strings.ContainsAny(candidate, "0123456789?:") {
				if player := objectiveCanonicalPlayerName(candidate); player != "" {
					if !merge(player) {
						return ""
					}
				}
			}
		}
	}
	// An unprefixed two-word structural selection is used only when the official raw title also
	// contains that exact entity. This avoids turning a two-word team label into a player ID.
	if selectionPlayer != "" && !explicitSelection {
		cleanTitle := objectiveCanonicalPlayerName(title)
		if strings.Contains(cleanTitle, selectionPlayer) && !merge(selectionPlayer) {
			return ""
		}
	}
	return resolved
}

func objectivePlayerMetric(raw string) (marketType, metric, comparator, threshold string) {
	t := strings.ToLower(strings.TrimSpace(raw))
	comparator = ""
	switch {
	case strings.Contains(t, "under"):
		comparator = "under"
	case strings.Contains(t, "over"):
		comparator = "over"
	case strings.Contains(t, "at least") || strings.Contains(t, " or more") || objectivePlusNumberRE.MatchString(t):
		comparator = "gte"
	case strings.Contains(t, "less than"):
		comparator = "lt"
	}
	switch {
	case strings.Contains(t, "goal_scorer") || strings.Contains(t, "goalscorer"):
		return "player_scorer", "goals", "gte", "1"
	case strings.Contains(t, "to score") && strings.Contains(t, "goal"):
		return "player_scorer", "goals", "gte", "1"
	case strings.Contains(t, "strikeout"):
		return "player_prop", "strikeouts", comparator, ""
	case strings.Contains(t, "rebound"):
		return "player_prop", "rebounds", comparator, ""
	case strings.Contains(t, "assist"):
		return "player_prop", "assists", comparator, ""
	case strings.Contains(t, "three_pointer") || strings.Contains(t, "three pointer") || strings.Contains(t, "3-pointer"):
		return "player_prop", "three_pointers", comparator, ""
	case strings.Contains(t, "passing_yard") || strings.Contains(t, "passing yard"):
		return "player_prop", "passing_yards", comparator, ""
	case strings.Contains(t, "rushing_yard") || strings.Contains(t, "rushing yard"):
		return "player_prop", "rushing_yards", comparator, ""
	case strings.Contains(t, "receiving_yard") || strings.Contains(t, "receiving yard"):
		return "player_prop", "receiving_yards", comparator, ""
	case strings.Contains(t, "touchdown"):
		return "player_prop", "touchdowns", comparator, ""
	case strings.Contains(t, "shot"):
		return "player_prop", "shots", comparator, ""
	case strings.Contains(t, "point"):
		return "player_prop", "points", comparator, ""
	case strings.Contains(t, "hit"):
		return "player_prop", "hits", comparator, ""
	case strings.Contains(t, "total_base"):
		return "player_prop", "total_bases", comparator, ""
	case strings.Contains(t, "save"):
		return "player_prop", "saves", comparator, ""
	case strings.Contains(t, "block"):
		return "player_prop", "blocks", comparator, ""
	case strings.Contains(t, "steal"):
		return "player_prop", "steals", comparator, ""
	}
	return "", "", "", ""
}

func objectivePlayerThreshold(raw string, line float64) string {
	if line != 0 {
		return strconv.FormatFloat(line, 'f', -1, 64)
	}
	for _, re := range []*regexp.Regexp{objectiveComparedNumberRE, objectivePlusNumberRE} {
		if match := re.FindStringSubmatch(raw); len(match) == 2 {
			if _, err := strconv.ParseFloat(match[1], 64); err == nil {
				return match[1]
			}
		}
	}
	return ""
}

func objectivePairContract(pair storage.StructuralRulePair, left bool, artifact researchVenueRuleArtifact) objectiveidentity.Contract {
	venue, id, selection, opposite, line := pair.RightVenue, pair.RightInstrumentID,
		pair.RightYesTeam, pair.RightNoTeam, pair.RightLine
	boundary := pair.RightBoundaryUTC
	if left {
		venue, id, selection, opposite, line = pair.LeftVenue, pair.LeftInstrumentID,
			pair.LeftYesTeam, pair.LeftNoTeam, pair.LeftLine
		boundary = pair.LeftBoundaryUTC
	}
	marketType, title := pair.RightMarketType, pair.RightTitle
	if left {
		marketType, title = pair.LeftMarketType, pair.LeftTitle
	}
	if marketType == "" {
		marketType = pair.MarketType
	}
	marketType = strings.ToLower(strings.TrimSpace(marketType))
	identityRaw := strings.Join([]string{artifact.SportsType, title, selection, id}, " ")
	period := objectiveSportsPeriod(identityRaw, marketType)
	if scoped, ok := objectiveExactScopedSportsTerms(venue, id, pair.League, pair.Away, pair.Home,
		artifact.SportsType, line); ok {
		period, marketType, selection, opposite, line = scoped.period, scoped.marketType,
			scoped.selection, scoped.opposite, scoped.line
	}
	contract := objectiveidentity.Contract{Venue: venue, InstrumentID: id, Genre: "sports",
		EventID: pair.CanonicalEventID, League: pair.League, Participants: []string{pair.Away, pair.Home},
		OutcomeCardinality: objectiveSportsOutcomeCardinality(pair.League, artifact.SportsType, marketType),
		Period:             period, MarketType: marketType, Selection: selection, Opposite: opposite,
		EventStartUTC: pair.StartUTC, VenueCloseUTC: boundary, Timezone: "UTC", SettlementClass: "objective_result",
		SourceKind: "official_structured", Structured: strings.TrimSpace(pair.GameID) != ""}
	// A separate soccer winner contract's NO side is the union of the other two outcomes. Clear
	// any legacy two-team shortcut and never label it as the other team's YES proposition.
	if marketType == "winner" && contract.OutcomeCardinality != 2 {
		contract.Opposite = ""
	}
	if contract.Opposite == "" && (marketType != "winner" || contract.OutcomeCardinality == 2) {
		switch strings.ToUpper(strings.TrimSpace(contract.Selection)) {
		case strings.ToUpper(pair.Away):
			contract.Opposite = pair.Home
		case strings.ToUpper(pair.Home):
			contract.Opposite = pair.Away
		}
	}
	switch marketType {
	case "winner":
		contract.Comparator = "wins"
	case "advance":
		contract.Comparator = "advances"
	case "spread":
		contract.Comparator, contract.Threshold = "covers", strconv.FormatFloat(line, 'f', -1, 64)
	case "total":
		contract.Selection = "TOTAL"
		contract.Comparator = strings.ToLower(strings.TrimSpace(selection))
		contract.Threshold = strconv.FormatFloat(line, 'f', -1, 64)
	case "prop", "player_prop", "player_scorer":
		if mt, metric, comparator, threshold := objectivePlayerMetric(identityRaw); mt != "" {
			contract.MarketType, contract.Metric, contract.Comparator, contract.Threshold = mt, metric, comparator, threshold
			contract.PlayerID = objectivePlayerID(selection, title, id)
			contract.Selection = contract.PlayerID
			if contract.Threshold == "" {
				contract.Threshold = objectivePlayerThreshold(identityRaw, line)
			}
		}
	}
	// Material policies are per-leg evidence. Never synthesize one shared policy from the pair:
	// differing settlement-source prose is harmless only when each venue independently supplies
	// matching structured void/overtime/tie semantics (or a versioned official mapping supplies
	// them). The current adapters leave absent fields empty and the taxonomy logs a review reject.
	contract.VoidPolicy, contract.OvertimePolicy, contract.TiePolicy = artifact.VoidPolicy, artifact.OvertimePolicy, artifact.TiePolicy
	contract.PolicySource = artifact.MaterialPolicySource
	return contract
}

// objectiveRuleDecision evaluates the actual payoff and, when possible, appends a current
// zero-authority structured certificate. Existing current reviewed certificates remain valid and
// are reused; raw artifact or canonical-identity gaps convert the verdict into an explicit reject.
func (s *Server) objectiveRuleDecision(ctx context.Context, pair storage.StructuralRulePair,
	leftArtifact, rightArtifact storage.VenueRuleArtifact,
	leftRaw, rightRaw researchVenueRuleArtifact) (objectiveidentity.Contract, objectiveidentity.Contract,
	objectiveidentity.Verdict, string, error) {
	left := objectivePairContract(pair, true, leftRaw)
	right := objectivePairContract(pair, false, rightRaw)
	verdict := objectiveidentity.EvaluateDirectional(left, right)
	if !verdict.Compatible {
		return left, right, verdict, "", nil
	}
	if leftArtifact.ArtifactHash == "" || rightArtifact.ArtifactHash == "" {
		return left, right, objectiveidentity.Blocked(verdict, objectiveidentity.RejectMissingRawRules), "", nil
	}
	if cert, current, err := s.store.CompatibleRulePairCertificateFor(ctx, pair.LeftVenue, pair.LeftInstrumentID,
		pair.RightVenue, pair.RightInstrumentID); err != nil {
		return left, right, objectiveidentity.LockBlocked(verdict, objectiveidentity.RejectCertificateStorage), "", err
	} else if current && cert.Orientation == verdict.Orientation {
		return left, right, objectiveidentity.Certified(verdict), cert.SpecHash, nil
	}
	// Directional comparison is useful for convergence/fade research but is not a risk-free
	// lock. Only the strict material-policy verdict may enter the certificate path below.
	if !verdict.LockEligible {
		return left, right, verdict, "", nil
	}
	inputs, err := s.store.ResolutionBasisInputsForKeys(ctx, []storage.ResolutionBasisInstrumentKey{
		{Venue: pair.LeftVenue, Ticker: pair.LeftInstrumentID},
		{Venue: pair.RightVenue, Ticker: pair.RightInstrumentID},
	})
	if err != nil {
		return left, right, objectiveidentity.LockBlocked(verdict, objectiveidentity.RejectMissingCanonical), "", err
	}
	li, lok := inputs[storage.ResolutionBasisKey(pair.LeftVenue, pair.LeftInstrumentID)]
	ri, rok := inputs[storage.ResolutionBasisKey(pair.RightVenue, pair.RightInstrumentID)]
	if !lok || !rok || li.EventID == "" || ri.EventID == "" || li.EventID != ri.EventID || li.EventVersion != ri.EventVersion ||
		(verdict.Orientation == "same" && (li.PayoffID != ri.PayoffID || li.PayoffVersion != ri.PayoffVersion)) ||
		(verdict.Orientation == "inverse" && li.PayoffID == ri.PayoffID) {
		return left, right, objectiveidentity.LockBlocked(verdict, objectiveidentity.RejectMissingCanonical), "", nil
	}
	version, err := s.store.NextRulePairCertificateVersion(ctx, pair.PairID)
	if err != nil {
		return left, right, objectiveidentity.LockBlocked(verdict, objectiveidentity.RejectCertificateStorage), "", err
	}
	normLeft := verdict.NormalizedLeft
	terms := storage.NormalizedRuleTerms{
		SettlementSourceID:  "objective-result:" + normLeft.EventID,
		SettlementSourceURL: "urn:objective-result:" + verdict.PredicateHash,
		PredicateHash:       verdict.PredicateHash, RulesHash: verdict.RulesHash,
		VoidPolicy: normLeft.VoidPolicy, ScalarPolicy: "binary-0-or-1-or-principal-refund",
		UnknownPolicy: "principal-refund", TimingPolicy: strings.Join([]string{normLeft.Period,
			normLeft.DeadlineUTC, normLeft.Timezone, normLeft.OvertimePolicy, normLeft.TiePolicy}, "|"),
	}
	cert, _, err := s.store.RegisterRulePairCertificate(ctx, storage.RulePairCertificateSpec{
		PairID: pair.PairID, LeftVenue: pair.LeftVenue, LeftInstrumentID: pair.LeftInstrumentID,
		RightVenue: pair.RightVenue, RightInstrumentID: pair.RightInstrumentID,
		Orientation: verdict.Orientation, LeftArtifactHash: leftArtifact.ArtifactHash,
		RightArtifactHash: rightArtifact.ArtifactHash, LeftPayoffID: li.PayoffID, RightPayoffID: ri.PayoffID,
		CanonicalEventID: li.EventID, CanonicalPayoffID: li.PayoffID, Left: terms, Right: terms,
		ReviewMethod:       "structured_official_schema",
		ReviewProvenance:   "automatic exact objective predicate taxonomy " + objectiveidentity.TaxonomyVersion,
		ReviewEvidenceHash: verdict.EvidenceHash, Version: version,
	})
	if err != nil {
		return left, right, objectiveidentity.LockBlocked(verdict, objectiveidentity.RejectCertificateStorage), "", err
	}
	return left, right, verdict, cert.SpecHash, nil
}

func (s *Server) recordObjectiveMatchDecision(ctx context.Context, started time.Time,
	pair storage.StructuralRulePair, left, right objectiveidentity.Contract,
	verdict objectiveidentity.Verdict, certificateHash string) (bool, error) {
	return s.store.InsertCrossVenueMatchDecision(ctx, storage.CrossVenueMatchDecision{
		Observed: started, CycleID: collectorCycleID(started), PairID: pair.PairID, PairType: pair.PairType,
		LeftVenue: pair.LeftVenue, LeftInstrumentID: pair.LeftInstrumentID,
		RightVenue: pair.RightVenue, RightInstrumentID: pair.RightInstrumentID,
		FriendlyLeft:  s.xvlFriendlyName(pair.LeftVenue, pair.LeftInstrumentID),
		FriendlyRight: s.xvlFriendlyName(pair.RightVenue, pair.RightInstrumentID),
		Left:          left, Right: right, Verdict: verdict, CertificateHash: certificateHash,
	})
}

func objectiveDecisionErrorClass(err error) string {
	if err == nil {
		return ""
	}
	return fmt.Sprintf("objective_match: %v", err)
}

type objectiveAnchorCandidate struct {
	pairType, pairID, leftVenue, leftID, rightVenue, rightID string
	key, kind                                                string
	ambiguous                                                bool
}

func objectivePairIdentity(leftVenue, leftID, rightVenue, rightID string) (pairType, pairID string) {
	switch leftVenue + "|" + rightVenue {
	case "kalshi|polyus":
		return "K-PUS", "k-pus|" + leftID + "|" + rightID
	case "kalshi|polymarket":
		return "K-PINT", "k-pint|" + leftID + "|" + rightID
	case "polyus|polymarket":
		return "PUS-PINT", "pus-pint|" + leftID + "|" + rightID
	}
	return "", ""
}

type objectiveAnchorCursor struct {
	Key    string `json:"key,omitempty"`
	Offset int    `json:"offset,omitempty"`
}

type objectiveAnchorPage struct {
	Candidates             []objectiveAnchorCandidate
	Next                   objectiveAnchorCursor
	TotalKeys, ScannedKeys int
	DeferredKeys           int
	Wrapped                bool
}

func (s *Server) objectiveAnchorCandidates(after objectiveAnchorCursor, keyBudget, candidateLimit int) objectiveAnchorPage {
	if keyBudget <= 0 || keyBudget > 1000 {
		keyBudget = 500
	}
	if candidateLimit <= 0 || candidateLimit > 1000 {
		candidateLimit = 500
	}
	conditionBySlug := map[string]string{}
	if s.poly != nil {
		for _, market := range s.poly.CompleteCatalogSnapshot().Markets {
			if market.Slug != "" && market.ConditionID != "" {
				conditionBySlug[strings.ToLower(market.Slug)] = market.ConditionID
			}
		}
	}
	xvReg.mu.Lock()
	keys := xvReg.orderedKeys
	page := objectiveAnchorPage{TotalKeys: len(keys)}
	start := 0
	if after.Key != "" {
		start = sort.SearchStrings(keys, after.Key)
		if start >= len(keys) {
			start, after, page.Wrapped = 0, objectiveAnchorCursor{}, len(keys) > 0
		} else if keys[start] != after.Key || after.Offset == 0 {
			for start < len(keys) && keys[start] <= after.Key {
				start++
			}
			if start >= len(keys) {
				start, after, page.Wrapped = 0, objectiveAnchorCursor{}, len(keys) > 0
			}
		}
	}
	for index := start; index < len(keys) && page.ScannedKeys < keyBudget; index++ {
		key := keys[index]
		entry := xvReg.byKey[key]
		page.ScannedKeys++
		page.Next = objectiveAnchorCursor{Key: key}
		if entry == nil {
			continue
		}
		ids := map[string][]string{}
		for venue, rawIDs := range entry.ids {
			ids[venue] = append([]string(nil), rawIDs...)
			if venue == "polymarket" {
				for i, rawID := range ids[venue] {
					if condition := conditionBySlug[strings.ToLower(rawID)]; condition != "" {
						ids[venue][i] = condition
					}
				}
			}
			sort.Strings(ids[venue])
		}
		var keyCandidates []objectiveAnchorCandidate
		for _, pair := range [][2]string{{"kalshi", "polyus"}, {"kalshi", "polymarket"}, {"polyus", "polymarket"}} {
			leftIDs, rightIDs := ids[pair[0]], ids[pair[1]]
			if len(leftIDs) == 0 || len(rightIDs) == 0 {
				continue
			}
			ambiguous := len(leftIDs) != 1 || len(rightIDs) != 1
			for _, leftID := range leftIDs {
				for _, rightID := range rightIDs {
					pairType, pairID := objectivePairIdentity(pair[0], leftID, pair[1], rightID)
					if pairID != "" {
						keyCandidates = append(keyCandidates, objectiveAnchorCandidate{pairType: pairType, pairID: pairID,
							leftVenue: pair[0], leftID: leftID, rightVenue: pair[1], rightID: rightID,
							key: key, kind: entry.kind, ambiguous: ambiguous})
					}
				}
			}
		}
		offset := 0
		if key == after.Key && after.Offset > 0 {
			offset = after.Offset
			if offset > len(keyCandidates) {
				offset = len(keyCandidates)
			}
		}
		remaining := candidateLimit - len(page.Candidates)
		if remaining <= 0 {
			page.Next = objectiveAnchorCursor{Key: key, Offset: offset}
			break
		}
		end := len(keyCandidates)
		if end-offset > remaining {
			end = offset + remaining
		}
		page.Candidates = append(page.Candidates, keyCandidates[offset:end]...)
		if end < len(keyCandidates) {
			page.Next = objectiveAnchorCursor{Key: key, Offset: end}
			break
		}
	}
	if page.TotalKeys > page.ScannedKeys {
		page.DeferredKeys = page.TotalKeys - page.ScannedKeys
	}
	xvReg.mu.Unlock()
	return page
}

func objectiveAnchorContract(candidate objectiveAnchorCandidate, left bool) objectiveidentity.Contract {
	venue, id := candidate.rightVenue, candidate.rightID
	if left {
		venue, id = candidate.leftVenue, candidate.leftID
	}
	genre := map[string]string{"crypto": "crypto", "wx": "weather", "econ": "discretionary", "pol": "politics"}[candidate.kind]
	contract := objectiveidentity.Contract{Venue: venue, InstrumentID: id, Genre: genre,
		EventID: candidate.key, Period: "event", MarketType: candidate.kind,
		SettlementClass: "objective_result", SourceKind: "official_structured", Structured: true,
		Timezone: "UTC"}
	parts := strings.Split(candidate.key, "|")
	if candidate.kind != "crypto" || len(parts) < 5 {
		return contract
	}
	underlying, deadline, descriptor, direction := parts[1], parts[2], parts[3], parts[4]
	contract.Participants, contract.Selection = []string{underlying}, underlying
	contract.Period, contract.Metric, contract.MarketType = descriptor, "price", "crypto_threshold"
	contract.DeadlineUTC = deadline
	switch {
	case strings.HasPrefix(descriptor, "T"):
		contract.Comparator, contract.Threshold = "gte", strings.TrimPrefix(descriptor, "T")
	case strings.HasPrefix(descriptor, "G"):
		contract.Comparator, contract.Threshold = "gt", strings.TrimPrefix(descriptor, "G")
	case strings.HasPrefix(descriptor, "U"):
		contract.Comparator, contract.Threshold = "lt", strings.TrimPrefix(descriptor, "U")
	case strings.HasPrefix(descriptor, "updown"):
		contract.Metric, contract.Threshold = "return", "0"
		if direction == "down" {
			contract.Comparator = "lte"
		} else {
			contract.Comparator = "gt"
		}
	}
	return contract
}

type objectiveAnchorSweepResult struct {
	eligible, inserted, duplicates       int
	reasons                              map[string]int
	pairTypes                            map[string]int
	cursor                               objectiveAnchorCursor
	totalKeys, scannedKeys, deferredKeys int
	wrapped                              bool
	err                                  error
}

// sweepObjectiveAnchorDecisions extends the accepted/rejected ledger beyond sports. The strict
// identifier grammars in xvident provide exact crypto predicates; weather/econ/politics remain
// review-only. No anchor decision creates a rule certificate or any money authority.
func (s *Server) sweepObjectiveAnchorDecisions(ctx context.Context, started time.Time) objectiveAnchorSweepResult {
	result := objectiveAnchorSweepResult{reasons: map[string]int{}, pairTypes: map[string]int{}}
	var cursor objectiveAnchorCursor
	if raw, ok := s.store.KVGet(ctx, "r139:objective-anchor-cursor"); ok && raw != "" {
		_ = json.Unmarshal([]byte(raw), &cursor)
	}
	page := s.objectiveAnchorCandidates(cursor, 500, 500)
	candidates := page.Candidates
	result.cursor, result.totalKeys, result.scannedKeys = page.Next, page.TotalKeys, page.ScannedKeys
	result.deferredKeys, result.wrapped = page.DeferredKeys, page.Wrapped
	result.eligible = len(candidates)
	rows := make([]storage.StructuralIdentityRow, 0, len(candidates)*2)
	for _, candidate := range candidates {
		rows = append(rows, storage.StructuralIdentityRow{Venue: candidate.leftVenue, Ticker: candidate.leftID},
			storage.StructuralIdentityRow{Venue: candidate.rightVenue, Ticker: candidate.rightID})
	}
	rawArtifacts := s.researchRuleArtifacts(rows)
	storedArtifacts := map[string]storage.VenueRuleArtifact{}
	for key, raw := range rawArtifacts {
		parts := strings.SplitN(key, "|", 2)
		if len(parts) != 2 {
			continue
		}
		stored, inserted, err := s.store.InsertVenueRuleArtifact(ctx, storage.VenueRuleArtifact{
			Venue: parts[0], InstrumentID: parts[1], Source: raw.Source,
			Primary: raw.Primary, Secondary: raw.Secondary, EarlyClose: raw.EarlyClose,
			Description: raw.Description, SportsType: raw.SportsType, SettlementSources: raw.SettlementSources,
			Observed: started})
		if err != nil {
			result.err = err
			continue
		}
		storedArtifacts[key] = stored
		if inserted {
			result.inserted++
		} else {
			result.duplicates++
		}
	}
	for _, candidate := range candidates {
		left, right := objectiveAnchorContract(candidate, true), objectiveAnchorContract(candidate, false)
		verdict := objectiveidentity.EvaluateDirectional(left, right)
		if candidate.ambiguous {
			verdict = objectiveidentity.Blocked(verdict, objectiveidentity.RejectAmbiguous)
		}
		leftKey, rightKey := storage.ResolutionBasisKey(candidate.leftVenue, candidate.leftID), storage.ResolutionBasisKey(candidate.rightVenue, candidate.rightID)
		if storedArtifacts[leftKey].ArtifactHash == "" || storedArtifacts[rightKey].ArtifactHash == "" {
			verdict = objectiveidentity.Blocked(verdict, objectiveidentity.RejectMissingRawRules)
		}
		inserted, err := s.store.InsertCrossVenueMatchDecision(ctx, storage.CrossVenueMatchDecision{
			Observed: started, CycleID: collectorCycleID(started), PairID: candidate.pairID, PairType: candidate.pairType,
			LeftVenue: candidate.leftVenue, LeftInstrumentID: candidate.leftID,
			RightVenue: candidate.rightVenue, RightInstrumentID: candidate.rightID,
			FriendlyLeft:  s.xvlFriendlyName(candidate.leftVenue, candidate.leftID),
			FriendlyRight: s.xvlFriendlyName(candidate.rightVenue, candidate.rightID),
			Left:          left, Right: right, Verdict: verdict})
		if err != nil {
			result.err = err
		} else if inserted {
			result.inserted++
		} else {
			result.duplicates++
		}
		result.reasons[verdict.ReasonCode]++
		result.pairTypes[candidate.pairType]++
	}
	if page.ScannedKeys > 0 {
		if raw, err := json.Marshal(page.Next); err != nil {
			result.err = err
		} else if err := s.store.KVSet(ctx, "r139:objective-anchor-cursor", string(raw)); err != nil {
			result.err = err
		}
	}
	return result
}
