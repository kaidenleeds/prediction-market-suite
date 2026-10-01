package server

// gameident.go — R102 STRUCTURAL GAME MATCHING (operator directive: "the venues literally TELL us
// the structure — stop matching on string fuzz").
//
// The venues publish the game identity as METADATA, live-probed 2026-07-07:
//
//	Kalshi  event ticker  KXMLBSPREAD-26JUL062210COLLAD  = series (league+market type) + ET date +
//	        ET start HHMM + AWAY+HOME abbrev pair; market fields floor_strike (line), custom_strike
//	        (team entity), yes_sub_title/no_sub_title (side identity as city/nickname).
//	PolyUS  league event  {gameId:10078644, slug:"mlb-col-lad-2026-07-06", startTime:"2026-07-07T02:10:00Z",
//	        teams:[{name,abbreviation,league}...]} + current per-market sportsMarketType, line, and the
//	        long side's team object (with ordering:"away"/"home").
//
// Those two describe the SAME game with matching away-home order and TO-THE-MINUTE identical start
// times (Kalshi 2210 ET == PUS 02:10Z), so every market hard-joins to a canonical game_id
// ("<league>:<ET-date>:<AWAY>@<HOME>[#2]") at ingest — deterministically, with no title parsing.
// The fuzzy matcher (polyUSMatchForSide et al.) becomes FALLBACK-ONLY for markets with no
// structural anchor, and every fallback is telemetry-tagged so the auditor can measure how often
// fuzz still decides anything. Same-city (bug 205) and doubleheader (bug 206) confusions are
// structurally impossible for anchored games: abbrevs are league-scoped identities, and two games
// of one team-pair-day are distinct canonical rows joined only by start-time proximity (≤75 min),
// with ambiguity REFUSED (R90 doctrine: a missing observation beats a fabricated join).

import (
	"context"
	"math"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/polymarketus"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

// giTeam is one canonical team/participant in the league-aware team table. Seeded statically for
// the major US leagues (+ World Cup nations) and ENRICHED live from PolyUS team objects (the venue
// hands us name+abbrev+league on every event refresh — the table converges within one refresh).
type giTeam struct {
	league  string
	abbr    string   // canonical UPPER abbrev (PUS displayAbbreviation where known)
	name    string   // full name "Los Angeles Dodgers"
	alts    []string // alternate abbrevs seen for the same team (Kalshi variants: CWS/CHW, SF/SFG…)
	nameTok map[string]bool
}

// giGame is one canonical game.
type giGame struct {
	id                 string
	league             string
	away, home         string // canonical team abbrevs (UPPER); away/home per venue order (both venues list away first)
	awayName, homeName string
	start              time.Time // UTC; zero = date-only anchor (no venue start time yet)
	etDate             string    // ET calendar date "2026-07-06" (the venues' shared date convention)
	kalshiEvents       map[string]bool
	pusEventID         string
	pusGameID          int
	live               bool
	dh                 int // doubleheader ordinal (0 = only/first discovered, 1 = "#2", …)
}

// giMktRef is one market's structural anchor. YES-side identity:
//
//	winner:  yesTeam = the team the YES side backs ("draw" for 3-way soccer draw markets)
//	total:   yesTeam = "over"|"under", line = the total
//	spread:  yesTeam = the team the YES side backs TO COVER line (signed: Kalshi "T wins by >X" ⇒
//	         yesTeam=T line=−X; PUS long "U +X" ⇒ yesTeam=U line=+X — equivalent when the teams are
//	         opponents and the lines negate, with the PUS YES equal to the Kalshi NO)
//	advance: knockout to-advance (R106 bug 255). yesTeam = the advancing team the YES/long side
//	         backs; noTeam = the OTHER team when the venue lists both on ONE instrument (PUS
//	         single-slug shape: NO/short side IS the opponent — exact complement in a 2-team KO).
//	prop:    yesTeam = "" (grouped under the game; no cross-venue side equivalence claimed)
type giMktRef struct {
	gameID  string
	venue   string
	id      string // kalshi ticker / pus slug
	mktType string // winner|spread|total|advance|prop
	yesTeam string
	noTeam  string // R106: the short/NO side's team on two-sided single instruments ("" elsewhere)
	line    float64
}

// giState is the whole registry (one lock: reads are map hits; writes are ingest-path only).
type giState struct {
	games    map[string]*giGame             // game_id → game
	byKey    map[string][]*giGame           // league|etDate|AWAY|HOME → games (doubleheaders share a key)
	byMkt    map[string]*giMktRef           // venue|id → anchor
	gameMkts map[string]map[string][]string // game_id → venue → market ids
	kalEv    map[string]string              // kalshi EVENT ticker → game_id
	teams    map[string]map[string]*giTeam  // league → UPPER abbrev/alt → team
	byName   map[string]map[string]*giTeam  // league → normalized full name → team
	// counters (coverage stats — the auditor's fallback-rate surface)
	kalSeen, kalAnchored, pusSeen, pusAnchored int64
	structHits, fuzzHits                       map[string]int64 // per family
	dirtyGames, dirtyMkts                      map[string]bool
	persistAt                                  time.Time
}

func newGiState() *giState {
	st := &giState{
		games: map[string]*giGame{}, byKey: map[string][]*giGame{}, byMkt: map[string]*giMktRef{},
		gameMkts: map[string]map[string][]string{}, kalEv: map[string]string{},
		teams: map[string]map[string]*giTeam{}, byName: map[string]map[string]*giTeam{},
		structHits: map[string]int64{}, fuzzHits: map[string]int64{},
		dirtyGames: map[string]bool{}, dirtyMkts: map[string]bool{},
	}
	st.seedTeams()
	return st
}

// ── team table ────────────────────────────────────────────────────────────────────────────────

// giTwoWordNicks: nicknames that are two words — everything else splits city = name minus last word.
var giTwoWordNicks = []string{"Trail Blazers", "Red Sox", "White Sox", "Blue Jays", "Maple Leafs",
	"Golden Knights", "Red Wings", "Blue Jackets"}

// giSeed rows: "ABBR|Full Name|alt1,alt2" — the major US leagues, canonical abbrev first (PUS
// display abbrevs), with the Kalshi/most-common variants as alts. World Cup nations are FIFA
// 3-letter codes (the 2026 48-team field's common entrants; PUS enrichment fills any gap live).
var giSeed = map[string][]string{
	"mlb": {
		"ARI|Arizona Diamondbacks|AZ", "ATL|Atlanta Braves|", "BAL|Baltimore Orioles|", "BOS|Boston Red Sox|",
		"CHC|Chicago Cubs|", "CWS|Chicago White Sox|CHW", "CIN|Cincinnati Reds|", "CLE|Cleveland Guardians|",
		"COL|Colorado Rockies|", "DET|Detroit Tigers|", "HOU|Houston Astros|", "KC|Kansas City Royals|KCR",
		"LAA|Los Angeles Angels|ANA", "LAD|Los Angeles Dodgers|", "MIA|Miami Marlins|", "MIL|Milwaukee Brewers|",
		"MIN|Minnesota Twins|", "NYM|New York Mets|", "NYY|New York Yankees|", "ATH|Athletics|OAK",
		"PHI|Philadelphia Phillies|", "PIT|Pittsburgh Pirates|", "SD|San Diego Padres|SDP",
		"SF|San Francisco Giants|SFG", "SEA|Seattle Mariners|", "STL|St. Louis Cardinals|",
		"TB|Tampa Bay Rays|TBR", "TEX|Texas Rangers|", "TOR|Toronto Blue Jays|", "WSH|Washington Nationals|WAS",
	},
	"nba": {
		"ATL|Atlanta Hawks|", "BOS|Boston Celtics|", "BKN|Brooklyn Nets|BRK", "CHA|Charlotte Hornets|CHO",
		"CHI|Chicago Bulls|", "CLE|Cleveland Cavaliers|", "DAL|Dallas Mavericks|", "DEN|Denver Nuggets|",
		"DET|Detroit Pistons|", "GSW|Golden State Warriors|GS", "HOU|Houston Rockets|", "IND|Indiana Pacers|",
		"LAC|Los Angeles Clippers|", "LAL|Los Angeles Lakers|", "MEM|Memphis Grizzlies|", "MIA|Miami Heat|",
		"MIL|Milwaukee Bucks|", "MIN|Minnesota Timberwolves|", "NOP|New Orleans Pelicans|NO",
		"NYK|New York Knicks|NY", "OKC|Oklahoma City Thunder|", "ORL|Orlando Magic|", "PHI|Philadelphia 76ers|",
		"PHX|Phoenix Suns|PHO", "POR|Portland Trail Blazers|", "SAC|Sacramento Kings|", "SAS|San Antonio Spurs|SA",
		"TOR|Toronto Raptors|", "UTA|Utah Jazz|UTAH", "WAS|Washington Wizards|WSH",
	},
	"nfl": {
		"ARI|Arizona Cardinals|ARZ", "ATL|Atlanta Falcons|", "BAL|Baltimore Ravens|", "BUF|Buffalo Bills|",
		"CAR|Carolina Panthers|", "CHI|Chicago Bears|", "CIN|Cincinnati Bengals|", "CLE|Cleveland Browns|",
		"DAL|Dallas Cowboys|", "DEN|Denver Broncos|", "DET|Detroit Lions|", "GB|Green Bay Packers|GNB",
		"HOU|Houston Texans|", "IND|Indianapolis Colts|", "JAX|Jacksonville Jaguars|JAC", "KC|Kansas City Chiefs|KAN",
		"LV|Las Vegas Raiders|LVR", "LAC|Los Angeles Chargers|", "LAR|Los Angeles Rams|LA", "MIA|Miami Dolphins|",
		"MIN|Minnesota Vikings|", "NE|New England Patriots|NWE", "NO|New Orleans Saints|NOR",
		"NYG|New York Giants|", "NYJ|New York Jets|", "PHI|Philadelphia Eagles|", "PIT|Pittsburgh Steelers|",
		"SF|San Francisco 49ers|SFO", "SEA|Seattle Seahawks|", "TB|Tampa Bay Buccaneers|TAM",
		"TEN|Tennessee Titans|", "WSH|Washington Commanders|WAS",
	},
	"nhl": {
		"ANA|Anaheim Ducks|", "BOS|Boston Bruins|", "BUF|Buffalo Sabres|", "CGY|Calgary Flames|CAL",
		"CAR|Carolina Hurricanes|", "CHI|Chicago Blackhawks|", "COL|Colorado Avalanche|", "CBJ|Columbus Blue Jackets|CLB",
		"DAL|Dallas Stars|", "DET|Detroit Red Wings|", "EDM|Edmonton Oilers|", "FLA|Florida Panthers|",
		"LAK|Los Angeles Kings|LA", "MIN|Minnesota Wild|", "MTL|Montreal Canadiens|MON", "NSH|Nashville Predators|",
		"NJD|New Jersey Devils|NJ", "NYI|New York Islanders|", "NYR|New York Rangers|", "OTT|Ottawa Senators|",
		"PHI|Philadelphia Flyers|", "PIT|Pittsburgh Penguins|", "SJS|San Jose Sharks|SJ", "SEA|Seattle Kraken|",
		"STL|St. Louis Blues|", "TBL|Tampa Bay Lightning|TB", "TOR|Toronto Maple Leafs|", "UTA|Utah Mammoth|UTAH",
		"VAN|Vancouver Canucks|", "VGK|Vegas Golden Knights|VEG", "WSH|Washington Capitals|WAS", "WPG|Winnipeg Jets|WIN",
	},
	"wnba": {
		"ATL|Atlanta Dream|", "CHI|Chicago Sky|", "CON|Connecticut Sun|CONN,CT", "DAL|Dallas Wings|",
		"GSV|Golden State Valkyries|GV", "IND|Indiana Fever|", "LVA|Las Vegas Aces|LV", "LAS|Los Angeles Sparks|LA",
		"MIN|Minnesota Lynx|", "NYL|New York Liberty|NY", "PHX|Phoenix Mercury|PHO", "SEA|Seattle Storm|",
		"WAS|Washington Mystics|WSH",
	},
	"fwc": { // 2026 World Cup national teams (FIFA codes; PUS enrichment fills the rest of the 48)
		"USA|United States|", "MEX|Mexico|", "CAN|Canada|", "ARG|Argentina|", "BRA|Brazil|", "FRA|France|",
		"ENG|England|", "ESP|Spain|", "GER|Germany|", "POR|Portugal|", "NED|Netherlands|", "BEL|Belgium|",
		"CRO|Croatia|", "ITA|Italy|", "URU|Uruguay|", "COL|Colombia|", "ECU|Ecuador|", "CHI|Chile|",
		"PER|Peru|", "PAR|Paraguay|", "JPN|Japan|", "KOR|South Korea|", "AUS|Australia|", "IRN|Iran|",
		"KSA|Saudi Arabia|SAU", "QAT|Qatar|", "MAR|Morocco|", "SEN|Senegal|", "GHA|Ghana|", "NGA|Nigeria|",
		"CMR|Cameroon|", "EGY|Egypt|", "ALG|Algeria|", "TUN|Tunisia|", "CIV|Ivory Coast|", "DEN|Denmark|",
		"SWE|Sweden|", "NOR|Norway|", "POL|Poland|", "SUI|Switzerland|", "AUT|Austria|", "SRB|Serbia|",
		"TUR|Turkey|", "UKR|Ukraine|", "WAL|Wales|", "SCO|Scotland|", "CRC|Costa Rica|", "PAN|Panama|",
		"JAM|Jamaica|", "HON|Honduras|", "NZL|New Zealand|", "UZB|Uzbekistan|", "JOR|Jordan|",
	},
}

func (st *giState) seedTeams() {
	for lg, rows := range giSeed {
		for _, row := range rows {
			p := strings.Split(row, "|")
			if len(p) < 2 {
				continue
			}
			var alts []string
			if len(p) >= 3 && p[2] != "" {
				alts = strings.Split(p[2], ",")
			}
			st.learnTeam(lg, p[1], p[0], alts...)
		}
	}
}

func giNormName(name string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(strings.TrimSpace(name)) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == ' ' {
			b.WriteRune(r)
		}
	}
	return strings.Join(strings.Fields(b.String()), " ")
}

// learnTeam registers (or refreshes) a team. Existing rows only gain alt abbrevs — the canonical
// abbrev is stable once set (first writer wins; seeds run before any venue data).
func (st *giState) learnTeam(league, name, abbr string, alts ...string) *giTeam {
	league = strings.ToLower(strings.TrimSpace(league))
	abbr = strings.ToUpper(strings.TrimSpace(abbr))
	if league == "" || (abbr == "" && strings.TrimSpace(name) == "") {
		return nil
	}
	if st.teams[league] == nil {
		st.teams[league] = map[string]*giTeam{}
		st.byName[league] = map[string]*giTeam{}
	}
	nn := giNormName(name)
	t := st.byName[league][nn]
	if t == nil && abbr != "" {
		t = st.teams[league][abbr]
	}
	if t == nil {
		t = &giTeam{league: league, abbr: abbr, name: strings.TrimSpace(name), nameTok: map[string]bool{}}
		if t.abbr == "" {
			t.abbr = strings.ToUpper(nn) // degenerate: nameless abbrev or abbrevless name
		}
		for _, w := range strings.Fields(nn) {
			t.nameTok[w] = true
		}
	}
	if t.name == "" && name != "" {
		t.name = strings.TrimSpace(name)
		for _, w := range strings.Fields(nn) {
			t.nameTok[w] = true
		}
	}
	reg := func(a string) {
		a = strings.ToUpper(strings.TrimSpace(a))
		if a == "" {
			return
		}
		if ex, ok := st.teams[league][a]; ok && ex != t {
			return // abbrev collision with a DIFFERENT team — first owner keeps it (never silently re-point identity)
		}
		st.teams[league][a] = t
		if a != t.abbr {
			seen := false
			for _, x := range t.alts {
				if x == a {
					seen = true
					break
				}
			}
			if !seen {
				t.alts = append(t.alts, a)
			}
		}
	}
	reg(t.abbr)
	reg(abbr)
	for _, a := range alts {
		reg(a)
	}
	if nn != "" {
		st.byName[league][nn] = t
	}
	return t
}

// teamByAbbr resolves a league-scoped abbrev (canonical or alt).
func (st *giState) teamByAbbr(league, abbr string) *giTeam {
	m := st.teams[strings.ToLower(league)]
	if m == nil {
		return nil
	}
	return m[strings.ToUpper(strings.TrimSpace(abbr))]
}

// teamFromText resolves free text (side/outcome string) to ONE of the two game teams using
// DISTINCTIVE evidence only (R100 bug-205 doctrine): a token both teams share (the city of a
// same-city pair) is never evidence; abbrev evidence must not fit the opponent too. nil = refuse.
func (st *giState) teamFromText(g *giGame, text string) *giTeam {
	a, h := st.teamByAbbr(g.league, g.away), st.teamByAbbr(g.league, g.home)
	if a == nil || h == nil {
		return nil
	}
	toks := map[string]bool{}
	for _, w := range strings.Fields(giNormName(text)) {
		toks[w] = true
	}
	up := strings.ToUpper(strings.TrimSpace(text))
	score := func(t, opp *giTeam) int {
		n := 0
		for w := range toks {
			if t.nameTok[w] && !opp.nameTok[w] && len(w) >= 3 {
				n++
			}
		}
		abbrs := append([]string{t.abbr}, t.alts...)
		oppAbbrs := map[string]bool{opp.abbr: true}
		for _, x := range opp.alts {
			oppAbbrs[x] = true
		}
		for _, x := range abbrs {
			if x != "" && up == x && !oppAbbrs[x] {
				n += 2
			}
		}
		return n
	}
	sa, sh := score(a, h), score(h, a)
	switch {
	case sa > 0 && sh == 0:
		return a
	case sh > 0 && sa == 0:
		return h
	}
	if strings.Contains(giNormName(text), "draw") || strings.Contains(giNormName(text), "tie") {
		return &giTeam{league: g.league, abbr: "draw", name: "Draw"}
	}
	return nil // ambiguous / no distinctive evidence — refuse (never guess)
}

// ── Kalshi series → (league, market type) ──────────────────────────────────────────────────────

// giKalLeagueCodes: series code (after "KX") → our league key (PUS league slug where both list it).
// Ordered longest-first at match time so WNBA beats NBA, ITFW beats ITF, etc.
var giKalLeagueCodes = map[string]string{
	"VALORANT": "valorant", "DOTA2": "dota2", "WCBB": "wcbb", "WNBA": "wnba", "BOXING": "boxing",
	"FIBA": "fiba", "MLB": "mlb", "NBA": "nba", "NFL": "nfl", "NHL": "nhl", "MLS": "mls",
	"CBB": "cbb", "CWS": "cws", "IPL": "ipl", "EPL": "epl", "UCL": "ucl", "BUN": "bun",
	"WC": "fwc", "UFC": "ufc", "ATP": "atp", "WTA": "wta", "ITFW": "itfw", "ITF": "itfm",
	"LOL": "lol", "CS2": "cs2", "COD": "cod", "BSL": "bsl", "WT20": "wt20", "T20": "t20",
	"TEST": "test-cricket", "COUNTYCHAMP": "countychamp", "MLC": "mlc",
}

// giKalTypeCodes: series suffix → market type class (R100 type-class split: total ≠ spread ≠ prop;
// half/first-five/team totals are PROPS — they must never pair with a full-game total).
// R106 (bug 255): ADVANCE = knockout "to advance" (incl. ET/pens) — its OWN class, because pairing
// it with a regulation-time winner (KXWCGAME has a TIE outcome) would be a semantic wrong-twin.
var giKalTypeCodes = map[string]string{
	"GAME": "winner", "MATCH": "winner", "FIGHT": "winner", "CHALLENGERMATCH": "winner",
	"WINNER": "winner", "SPREAD": "spread", "TOTAL": "total", "ADVANCE": "advance",
	"TEAMTOTAL": "prop", "F5SPREAD": "prop", "F5TOTAL": "prop", "1HTOTAL": "prop", "BTTS": "prop",
	"CORNERS": "prop", "SCORE": "prop", "GOAL": "prop", "SOA": "prop", "AST": "prop", "RFI": "prop",
	"HR": "prop", "TB": "prop", "HIT": "prop", "KS": "prop", "PTS": "prop", "REB": "prop",
	// Scoped series without a terminal GAME/SPREAD/TOTAL token still belong to the fixture graph,
	// but they must never become full-game cross-venue twins.
	"MAP": "prop", "SET": "prop", "TOTALMAPS": "prop",
	"F3": "prop", "F5": "prop", "F7": "prop",
	"1H": "prop", "2H": "prop", "1Q": "prop", "2Q": "prop", "3Q": "prop", "4Q": "prop", "OT": "prop",
}

var (
	giKalCodesByLen []string // league codes longest-first (init below)
	giKalTypesByLen []string // market suffixes longest-first (TEAMTOTAL before TOTAL, etc.)
)

// Competition-only modifiers preserve the terminal payoff type. Every unlisted modifier is
// conservatively a prop: it may anchor to the fixture, but cannot silently become a full-game twin.
var giKalCompetitionQualifiers = map[string]map[string]bool{
	"nba": {"SUMMER": true},
}

func init() {
	for c := range giKalLeagueCodes {
		giKalCodesByLen = append(giKalCodesByLen, c)
	}
	sort.Slice(giKalCodesByLen, func(i, j int) bool {
		if len(giKalCodesByLen[i]) != len(giKalCodesByLen[j]) {
			return len(giKalCodesByLen[i]) > len(giKalCodesByLen[j])
		}
		return giKalCodesByLen[i] < giKalCodesByLen[j]
	})
	for c := range giKalTypeCodes {
		giKalTypesByLen = append(giKalTypesByLen, c)
	}
	sort.Slice(giKalTypesByLen, func(i, j int) bool {
		if len(giKalTypesByLen[i]) != len(giKalTypesByLen[j]) {
			return len(giKalTypesByLen[i]) > len(giKalTypesByLen[j])
		}
		return giKalTypesByLen[i] < giKalTypesByLen[j]
	})
}

// giKalSeries parses a Kalshi SERIES ticker (the part before the first '-') into league + type.
// KXMLBSPREAD → (mlb, spread); KXBOXING → (boxing, winner); anything unknown → not sports.
func giKalSeries(series string) (league, mktType string, ok bool) {
	s := strings.ToUpper(strings.TrimSpace(series))
	if !strings.HasPrefix(s, "KX") {
		return "", "", false
	}
	s = s[2:]
	for _, code := range giKalCodesByLen {
		if !strings.HasPrefix(s, code) {
			continue
		}
		rest := s[len(code):]
		if rest == "" {
			return giKalLeagueCodes[code], "winner", true // KXBOXING-style: the series IS the match market
		}
		if t, k := giKalTypeCodes[rest]; k {
			return giKalLeagueCodes[code], t, true
		}
		// Competition names may sit between the league and the payoff token. Match the longest
		// terminal token so TEAMTOTAL is not mistaken for TOTAL and CHALLENGERMATCH is not mistaken
		// for MATCH. Only a known competition-only modifier retains that terminal type; periods,
		// maps, sets, innings, and unknown modifiers anchor conservatively as props.
		for _, suffix := range giKalTypesByLen {
			if !strings.HasSuffix(rest, suffix) || len(rest) == len(suffix) {
				continue
			}
			qualifier := strings.TrimSuffix(rest, suffix)
			league := giKalLeagueCodes[code]
			if giKalCompetitionQualifiers[league][qualifier] {
				return league, giKalTypeCodes[suffix], true
			}
			return league, "prop", true
		}
	}
	return "", "", false
}

// giKalEvRe: EVENT segment = <yy><MON><dd>[HHMM]<PAIR>.
var giKalEvRe = regexp.MustCompile(`^(\d{2})(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)(\d{2})([A-Z0-9]*)$`)

// giSplitPair splits a concatenated abbrev pair ("COLSF" → COL+SF) against the league team table.
// Exactly ONE valid split is required — two valid splits (pathological vocab) REFUSE (R90 doctrine).
func (st *giState) giSplitPair(league, pair string) (away, home *giTeam, ok bool) {
	if len(pair) < 3 || len(pair) > 12 {
		return nil, nil, false
	}
	var hits [][2]*giTeam
	for i := 2; i <= len(pair)-2 && i <= 5; i++ {
		a := st.teamByAbbr(league, pair[:i])
		h := st.teamByAbbr(league, pair[i:])
		if a != nil && h != nil && a != h {
			hits = append(hits, [2]*giTeam{a, h})
		}
	}
	if len(hits) != 1 {
		return nil, nil, false
	}
	return hits[0][0], hits[0][1], true
}

// giParseKalEvent decomposes a Kalshi sports EVENT ticker into (league, type, away, home, start).
// Start is ET-embedded (America/New_York — probes: 2210 ET == the PUS 02:10Z start exactly);
// events without HHMM anchor date-only (start zero, etDate set).
func (st *giState) giParseKalEvent(eventTicker string) (league, mktType string, away, home *giTeam, start time.Time, etDate string, ok bool) {
	tk := strings.ToUpper(strings.TrimSpace(eventTicker))
	i := strings.IndexByte(tk, '-')
	if i <= 0 || i+1 >= len(tk) {
		return
	}
	league, mktType, ok = giKalSeries(tk[:i])
	if !ok {
		return
	}
	ok = false
	seg := tk[i+1:]
	if j := strings.IndexByte(seg, '-'); j >= 0 {
		seg = seg[:j]
	}
	m := giKalEvRe.FindStringSubmatch(seg)
	if m == nil {
		return
	}
	et := etLocation()
	if et == nil {
		return
	}
	yy, _ := strconv.Atoi(m[1])
	day, _ := strconv.Atoi(m[3])
	if day < 1 || day > 31 {
		return
	}
	tail := m[4]
	// tail = [HHMM]PAIR — a 4-digit head is a start time IF the remainder still splits into two
	// known teams; digit-leading abbrevs (esports "100T") make both parses possible, in which case
	// agreement is required (same pair) or we refuse.
	type cand struct {
		a, h   *giTeam
		hh, mm int
		timed  bool
	}
	var cands []cand
	if a, h, ok2 := st.giSplitPair(league, tail); ok2 {
		cands = append(cands, cand{a: a, h: h})
	}
	if len(tail) > 4 {
		allDig := true
		for k := 0; k < 4; k++ {
			if tail[k] < '0' || tail[k] > '9' {
				allDig = false
				break
			}
		}
		if allDig {
			hh, _ := strconv.Atoi(tail[:2])
			mm, _ := strconv.Atoi(tail[2:4])
			if hh <= 23 && mm <= 59 {
				if a, h, ok2 := st.giSplitPair(league, tail[4:]); ok2 {
					cands = append(cands, cand{a: a, h: h, hh: hh, mm: mm, timed: true})
				}
			}
		}
	}
	var pick cand
	switch len(cands) {
	case 1:
		pick = cands[0]
	case 2:
		if cands[0].a == cands[1].a && cands[0].h == cands[1].h {
			pick = cands[1] // same pair either way — keep the timed parse
		} else {
			return // genuinely ambiguous — refuse (never guess a game identity)
		}
	default:
		return
	}
	dt := time.Date(2000+yy, kalshiMonths[m[2]], day, pick.hh, pick.mm, 0, 0, et)
	etDate = dt.Format("2006-01-02")
	if pick.timed {
		start = dt.UTC()
	}
	return league, mktType, pick.a, pick.h, start, etDate, true
}

// ── canonical games ─────────────────────────────────────────────────────────────────────────────

const giDoubleheaderWindow = 75 * time.Minute

// getGame finds-or-creates the canonical game for (league, etDate, away, home, start). Two games
// of the same key whose starts differ by more than the doubleheader window are DISTINCT canonical
// rows ("#2" suffix by discovery order). A timeless caller (no venue start) matches only when the
// key holds exactly one game — two candidates without a time is ambiguous ⇒ nil (refused).
func (st *giState) getGame(league, etDate string, away, home *giTeam, start time.Time) *giGame {
	if away == nil || home == nil || league == "" || etDate == "" {
		return nil
	}
	key := league + "|" + etDate + "|" + away.abbr + "|" + home.abbr
	list := st.byKey[key]
	if len(list) == 0 {
		// R106 (bug 255): venue ORDER disagreement — soccer neutral-site pairs (World Cup knockouts)
		// have only a nominal home/away, and Kalshi's ticker pair order can disagree with the PUS
		// ordering tags (FRAMAR vs MAR@FRA). The SAME two teams on the same ET day ARE the same
		// game; MLB doubleheaders are unaffected (both venues list away-first there, so the ordered
		// key always hits before this fallback).
		if alt := st.byKey[league+"|"+etDate+"|"+home.abbr+"|"+away.abbr]; len(alt) > 0 {
			list = alt
		}
	}
	if !start.IsZero() {
		for _, g := range list {
			if g.start.IsZero() {
				// date-only row (Kalshi timeless series discovered first): adopt the first venue-真 start
				g.start = start.UTC()
				st.dirtyGames[g.id] = true
				return g
			}
			d := g.start.Sub(start)
			if d < 0 {
				d = -d
			}
			if d <= giDoubleheaderWindow {
				return g
			}
		}
	} else {
		if len(list) == 1 {
			return list[0]
		}
		if len(list) > 1 {
			return nil // doubleheader + no time evidence — refuse (bug-206 doctrine)
		}
	}
	id := league + ":" + etDate + ":" + away.abbr + "@" + home.abbr
	if n := len(list); n > 0 {
		id += "#" + strconv.Itoa(n+1)
	}
	g := &giGame{id: id, league: league, away: away.abbr, home: home.abbr,
		awayName: away.name, homeName: home.name, start: start.UTC(), etDate: etDate,
		kalshiEvents: map[string]bool{}, dh: len(list)}
	if start.IsZero() {
		g.start = time.Time{}
	}
	st.games[id] = g
	st.byKey[key] = append(st.byKey[key], g)
	st.dirtyGames[id] = true
	return g
}

func (st *giState) attachMkt(g *giGame, venue, id, mktType, yesTeam string, line float64) {
	st.attachMkt2(g, venue, id, mktType, yesTeam, "", line)
}

// attachMkt2 (R106 bug 255): attachMkt + the two-sided noTeam identity.
func (st *giState) attachMkt2(g *giGame, venue, id, mktType, yesTeam, noTeam string, line float64) {
	mk := venue + "|" + id
	if old := st.byMkt[mk]; old != nil && old.gameID == g.id && old.mktType == mktType && old.yesTeam == yesTeam && old.noTeam == noTeam && old.line == line {
		return // unchanged — skip the dirty churn (bug-229 doctrine)
	}
	st.byMkt[mk] = &giMktRef{gameID: g.id, venue: venue, id: id, mktType: mktType, yesTeam: yesTeam, noTeam: noTeam, line: line}
	if st.gameMkts[g.id] == nil {
		st.gameMkts[g.id] = map[string][]string{}
	}
	seen := false
	for _, x := range st.gameMkts[g.id][venue] {
		if x == id {
			seen = true
			break
		}
	}
	if !seen {
		st.gameMkts[g.id][venue] = append(st.gameMkts[g.id][venue], id)
	}
	st.dirtyMkts[mk] = true
}

// ── ingest anchors ──────────────────────────────────────────────────────────────────────────────

// giAnchorKalshi anchors one Kalshi market to its canonical game (called from the catalog sweep —
// the full board passes through every 5 min, so vocabulary learned from PUS converges quickly).
func (s *Server) giAnchorKalshi(m kalshi.Market) bool {
	if m.Ticker == "" || m.EventTicker == "" {
		return false
	}
	st := s.gi()
	s.giMu.Lock()
	defer s.giMu.Unlock()
	if _, sports := giSportsSeriesCheck(m.EventTicker); !sports {
		return false // not a sports-series market — no structural claim, no counter
	}
	st.kalSeen++
	if ref := st.byMkt["kalshi|"+m.Ticker]; ref != nil {
		st.kalAnchored++
		return true // already anchored (idempotent fast path)
	}
	league, mktType, away, home, start, etDate, ok := st.giParseKalEvent(m.EventTicker)
	if !ok {
		return false
	}
	g := st.getGame(league, etDate, away, home, start)
	if g == nil {
		return false
	}
	if !g.kalshiEvents[m.EventTicker] {
		g.kalshiEvents[m.EventTicker] = true
		st.kalEv[m.EventTicker] = g.id
		st.dirtyGames[g.id] = true
	}
	yesTeam, line := "", 0.0
	suffix := ""
	if i := strings.LastIndexByte(m.Ticker, '-'); i > 0 && i+1 < len(m.Ticker) {
		suffix = strings.ToUpper(m.Ticker[i+1:])
	}
	noTeam := ""
	switch mktType {
	case "winner":
		t := st.teamByAbbr(league, suffix)
		if t == nil { // subtitle carries the side identity when the suffix isn't a plain abbrev
			t = st.teamFromText(g, m.YesSubTitle)
		}
		if t == nil {
			yesTeam = "" // anchored to the game, side unresolved — grouped but never side-joined
		} else {
			yesTeam = t.abbr
		}
	case "advance":
		// R106 (bug 255): KXWCADVANCE-26JUL09FRAMAR-FRA — YES = the suffix team advances. In a
		// 2-team knockout, NO ≡ the opponent advances, so noTeam = the game's other side (exact
		// complement — this is what lets a Kalshi per-team advance market twin the PUS
		// single-slug shape on EITHER side).
		t := st.teamByAbbr(league, suffix)
		if t == nil {
			t = st.teamFromText(g, m.YesSubTitle) // "France advances"
		}
		if t == nil {
			yesTeam = ""
		} else {
			yesTeam = t.abbr
			switch yesTeam {
			case g.away:
				noTeam = g.home
			case g.home:
				noTeam = g.away
			}
		}
	case "total":
		yesTeam, line = "over", m.FloorStrike.Float() // probe: YES = "Over 8.5 runs scored", floor_strike 8.5
	case "spread":
		ab := strings.TrimRight(suffix, "0123456789.")
		t := st.teamByAbbr(league, ab)
		if t == nil {
			t = st.teamFromText(g, m.YesSubTitle) // "Dodgers wins by over 7.5 runs"
		}
		if t != nil {
			yesTeam, line = t.abbr, -m.FloorStrike.Float() // YES = team wins by >X ⇒ team covers −X
		}
	default:
		mktType = "prop"
		line = m.FloorStrike.Float()
	}
	st.attachMkt2(g, "kalshi", m.Ticker, mktType, yesTeam, noTeam, line)
	st.kalAnchored++
	return true
}

// giSportsSeriesCheck reports whether an event ticker belongs to a known sports series.
func giSportsSeriesCheck(eventTicker string) (string, bool) {
	tk := strings.ToUpper(strings.TrimSpace(eventTicker))
	i := strings.IndexByte(tk, '-')
	if i <= 0 {
		return "", false
	}
	lg, _, ok := giKalSeries(tk[:i])
	return lg, ok
}

// giAnchorPUSEvent anchors a whole PolyUS league event (games carry gameId/teams/start first-class).
// Called from refreshPolyUS; per-market map hits make re-anchoring free.
func (s *Server) giAnchorPUSEvent(lg string, e polymarketus.Event) {
	if e.ID == "" || len(e.Markets) == 0 {
		return
	}
	st := s.gi()
	s.giMu.Lock()
	defer s.giMu.Unlock()
	// learn every team the event exposes (event-level + market-side objects with ordering)
	var away, home *giTeam
	for _, t := range e.Teams {
		st.learnTeam(lg, t.Name, giPreferAbbr(t))
	}
	for _, m := range e.Markets {
		for _, sd := range m.Sides {
			if sd.Team.Name != "" || sd.Team.Abbreviation != "" {
				t := st.learnTeam(lg, sd.Team.Name, giPreferAbbr(sd.Team))
				switch strings.ToLower(sd.Team.Ordering) {
				case "away":
					away = t
				case "home":
					home = t
				}
			}
		}
	}
	if away == nil || home == nil {
		// venue slug order is away-home ("mlb-col-lad-2026-07-06" == Kalshi COLLAD) — fallback when
		// no market side carried an ordering tag
		if parts := strings.Split(strings.ToLower(e.Slug), "-"); len(parts) >= 3 {
			a := st.teamByAbbr(lg, parts[1])
			h := st.teamByAbbr(lg, parts[2])
			if a != nil && h != nil && a != h {
				away, home = a, h
			}
		}
	}
	if away == nil || home == nil || away == home {
		return
	}
	start, err := parsePolyTime(strings.TrimSpace(e.StartTime))
	if err != nil || start.IsZero() {
		return
	}
	et := etLocation()
	if et == nil {
		return
	}
	etDate := start.In(et).Format("2006-01-02")
	g := st.getGame(lg, etDate, away, home, start)
	if g == nil {
		return
	}
	if g.pusEventID == "" {
		g.pusEventID, g.pusGameID = e.ID, e.GameID
		st.dirtyGames[g.id] = true
	}
	g.live = e.Live
	for _, m := range e.Markets {
		if m.Slug == "" {
			continue
		}
		st.pusSeen++
		if ref := st.byMkt["polyus|"+m.Slug]; ref != nil {
			st.pusAnchored++
			continue
		}
		mktType := pusKindOf(m)
		// R125 PERIOD-SCOPE GUARD (audit: 11/50 sampled twins were partial-game↔full-game joins).
		// The granular V2 enum is SCOPE-BLIND: baseball_team_first_five_total and
		// baseball_team_full_game_total BOTH report SPORTS_MARKET_TYPE_TOTAL (live-probed
		// 2026-07-09 on tsc-mlb-ath-det-2026-07-09-f5-3pt5). Only the v1 sportsMarketType string
		// carries the scope. Anything not verifiably FULL-GAME anchors as "prop" — grouped for
		// research, refused by twinLocked, exactly like player props. Refuse-by-default: an
		// unknown NEW scope spelling loses recall, never correctness.
		if !pusFullScope(m.SportsType) {
			switch mktType {
			case "winner", "total", "spread":
				st.attachMkt2(g, "polyus", m.Slug, "prop", "", "", m.Line)
				st.pusAnchored++
				continue
			}
		}
		yesTeam, noTeam, line := "", "", 0.0
		switch {
		case m.IsToAdvance():
			// R106 (bug 255): single-slug two-sided "to advance" (V2 still says MONEYLINE — this
			// case MUST precede IsMoneyline). YES = the long side's team; the short side is the
			// opponent on the SAME instrument (its price = 1 − YES on the one shared book).
			mktType = "advance"
			if t := st.teamByAbbr(lg, giPreferAbbr(m.LongTeam())); t != nil {
				yesTeam = t.abbr
			}
			if sh := m.ShortTeam(); sh.Name != "" || sh.Abbreviation != "" {
				if t := st.teamByAbbr(lg, giPreferAbbr(sh)); t != nil {
					noTeam = t.abbr
				}
			}
		case m.IsMoneyline():
			mktType = "winner"
			lt := m.LongTeam()
			if t := st.teamByAbbr(lg, giPreferAbbr(lt)); t != nil {
				yesTeam = t.abbr
			} else if polyUSStructuredDraw(m.SportsType, m.Slug) {
				// Current 3-way soccer draw rows have no team object. The exact venue scope plus
				// exact outcome slug is still authoritative structure and maps to the synthetic
				// DRAW outcome in the same canonical game.
				yesTeam = "draw"
			}
			if m.TwoSidedSingle() { // future-proof: the venue may single-slug regular moneylines too
				if sh := m.ShortTeam(); sh.Name != "" || sh.Abbreviation != "" {
					if t := st.teamByAbbr(lg, giPreferAbbr(sh)); t != nil {
						noTeam = t.abbr
					}
				}
			}
		case m.IsTotal():
			mktType = "total"
			yesTeam, line = "over", m.Line // long side desc "Over" (probe) — YES backs the over
			for _, sd := range m.Sides {
				if sd.Long && strings.EqualFold(strings.TrimSpace(sd.Description), "Under") {
					yesTeam = "under"
				}
			}
		case m.IsSpread():
			mktType = "spread"
			lt := m.LongTeam()
			if t := st.teamByAbbr(lg, giPreferAbbr(lt)); t != nil {
				yesTeam = t.abbr
				line = m.Line // venue line rides the long team ("Spread: COL (+1.5)" line=1.5)
				// R125 (audit: every neg-slug spread was mis-sided): the venue NOW sends the line
				// ALREADY SIGNED (live-probed 2026-07-09: asc-…-neg-16pt5 → line=-16.5 longdesc
				// "-16.50"; asc-…-pos-3pt5 → line=+3.5 longdesc "+3.50"). The legacy description
				// sign-flip assumed an UNSIGNED line and double-flipped signed ones into the exact
				// opposite handicap ("SEA −16.5" became "SEA +16.5", which then flip-twinned the
				// WRONG Kalshi side). The flip now applies only to unsigned (legacy) payloads.
				if m.Line >= 0 {
					for _, sd := range m.Sides {
						if sd.Long && strings.HasPrefix(strings.TrimSpace(sd.Description), "-") {
							line = -m.Line // "-1.50" long side: the venue lists the favorite long
						}
					}
				}
			}
		default:
			if mktType == "" || mktType == "future" {
				mktType = "prop"
			}
			line = m.Line
		}
		st.attachMkt2(g, "polyus", m.Slug, mktType, yesTeam, noTeam, line)
		st.pusAnchored++
	}
}

func giPreferAbbr(t polymarketus.Team) string {
	if t.DisplayAbbr != "" {
		return t.DisplayAbbr
	}
	return t.Abbreviation
}

// pusFullScope — R125: does the venue's v1 sportsMarketType string say this market covers the
// FULL game? Live-probed spellings (2026-07-09): full scope = "*_full_game_*" (winner/total/
// spread across baseball/basketball/soccer); partial = "*_first_five_*", "*_first_half_*",
// "*_second_half_*". "_to_advance" is its own twin class (giAnchorPUSEvent's advance case) and
// passes through. Empty is fail-closed for current live data: removed legacy fields cannot restore
// fixtures) — keeps pre-R125 behavior. ALLOW-LIST by design: an unknown scope spelling demotes
// to prop (never twinned) rather than risking a partial↔full wrong-join.
// R138 supersedes the older fixture note above: empty current metadata is now refused.
func pusFullScope(v1 string) bool {
	t := strings.ToLower(strings.TrimSpace(v1))
	if t == "" {
		return false
	}
	return strings.Contains(t, "full_game") || isPolyUSSoccerFullTimeWinnerScope(t) || strings.HasSuffix(t, "_to_advance")
}

// isPolyUSSoccerFullTimeWinnerScope recognizes the current venue-native regulation-time 3-way
// winner class. It intentionally does not accept generic "full_time" text: exact soccer winner
// structure is required so a partial or to-advance market cannot be joined accidentally.
func isPolyUSSoccerFullTimeWinnerScope(scope string) bool {
	t := strings.ToLower(strings.TrimSpace(scope))
	return strings.HasPrefix(t, "soccer_") && strings.HasSuffix(t, "_full_time_winner")
}

// polyUSStructuredDraw recognizes the venue's structural draw outcome. Current 3-way soccer rows
// expose an exact `...-draw`/`...-tie` outcome slug while leaving the team object empty. The exact
// scope gate prevents an unrelated title or slug from becoming identity evidence.
func polyUSStructuredDraw(scope, slug string) bool {
	if !isPolyUSSoccerFullTimeWinnerScope(scope) &&
		!(strings.HasPrefix(strings.ToLower(strings.TrimSpace(scope)), "soccer_") &&
			strings.Contains(strings.ToLower(strings.TrimSpace(scope)), "full_game") &&
			strings.HasSuffix(strings.ToLower(strings.TrimSpace(scope)), "_winner")) {
		return false
	}
	slug = strings.ToLower(strings.Trim(strings.TrimSpace(slug), "/"))
	if i := strings.LastIndexByte(slug, '/'); i >= 0 {
		slug = slug[i+1:]
	}
	i := strings.LastIndexByte(slug, '-')
	if i < 0 || i+1 >= len(slug) {
		return false
	}
	last := slug[i+1:]
	return last == "draw" || last == "tie"
}

// ── twin lookups (the consumers' fast path) ─────────────────────────────────────────────────────

// gi returns the registry (lazy init under giMu's caller — safe: all callers lock giMu after).
func (s *Server) gi() *giState {
	s.giInitOnce.Do(func() { s.giReg = newGiState() })
	return s.giReg
}

// giPUSTwin maps an anchored Kalshi market to its PolyUS same-game twin. sameSide=true means the
// PUS YES equals the Kalshi YES (winner/total with matching identity); sameSide=false means the
// PUS market prices the OPPOSITE side (spread listed on the other team: kal YES == pus NO).
func (s *Server) giPUSTwin(kalTicker string) (slug string, sameSide bool, ok bool) {
	st := s.gi()
	s.giMu.Lock()
	defer s.giMu.Unlock()
	ref := st.byMkt["kalshi|"+kalTicker]
	if ref == nil {
		return "", false, false
	}
	return st.twinLocked(ref, "polyus")
}

// giKalshiTwin maps an anchored PolyUS market to its Kalshi same-game twin.
func (s *Server) giKalshiTwin(slug string) (ticker string, sameSide bool, ok bool) {
	st := s.gi()
	s.giMu.Lock()
	defer s.giMu.Unlock()
	ref := st.byMkt["polyus|"+slug]
	if ref == nil {
		return "", false, false
	}
	return st.twinLocked(ref, "kalshi")
}

func (st *giState) twinLocked(ref *giMktRef, wantVenue string) (string, bool, bool) {
	if ref.mktType == "prop" || ref.yesTeam == "" {
		return "", false, false // props/unresolved sides never claim cross-venue equivalence
	}
	ids := st.gameMkts[ref.gameID][wantVenue]
	g := st.games[ref.gameID]
	if g == nil {
		return "", false, false
	}
	opp := func(team string) string {
		switch team {
		case g.away:
			return g.home
		case g.home:
			return g.away
		}
		return ""
	}
	for _, id := range ids {
		c := st.byMkt[wantVenue+"|"+id]
		if c == nil || c.mktType != ref.mktType {
			continue
		}
		switch ref.mktType {
		case "winner":
			if c.yesTeam == ref.yesTeam && ref.yesTeam != "" {
				return c.id, true, true
			}
		case "advance":
			// R106 (bug 255): same advancing team = same side. Opposite team = the SAME contract's
			// complement (2-team knockout: NO("X advances") ≡ "opponent advances") — so a Kalshi
			// per-team advance market twins the PUS single-slug instrument on EITHER side.
			if c.yesTeam == ref.yesTeam && ref.yesTeam != "" {
				return c.id, true, true
			}
			if ref.yesTeam != "" && c.yesTeam != "" && c.yesTeam == opp(ref.yesTeam) {
				return c.id, false, true // kal YES == pus NO (or vice versa)
			}
		case "total":
			if c.line == ref.line && c.line > 0 && c.yesTeam != "" && ref.yesTeam != "" {
				return c.id, c.yesTeam == ref.yesTeam, true
			}
		case "spread":
			if c.yesTeam == ref.yesTeam && c.line == ref.line && ref.yesTeam != "" {
				return c.id, true, true
			}
			if c.yesTeam != "" && c.yesTeam == opp(ref.yesTeam) && math.Abs(c.line+ref.line) < 1e-9 {
				return c.id, false, true // same handicap listed from the other team: YES ↔ NO
			}
		}
	}
	return "", false, false
}

// giGameOfRef resolves any ref (kalshi ticker / kalshi event ticker / pus slug) to its game id.
func (s *Server) giGameOfRef(refs ...string) (string, bool) {
	st := s.gi()
	s.giMu.Lock()
	defer s.giMu.Unlock()
	for _, ref := range refs {
		ref = strings.TrimSpace(ref)
		if ref == "" {
			continue
		}
		if r := st.byMkt["kalshi|"+ref]; r != nil {
			return r.gameID, true
		}
		if r := st.byMkt["polyus|"+strings.ToLower(ref)]; r != nil {
			return r.gameID, true
		}
		up := strings.ToUpper(ref)
		if gid, ok := st.kalEv[up]; ok {
			return gid, true
		}
		if i := strings.LastIndexByte(up, '-'); i > 0 { // market ticker → its event ticker
			if gid, ok := st.kalEv[up[:i]]; ok {
				return gid, true
			}
		}
	}
	return "", false
}

// giPUSSideMarket finds the PolyUS WINNER/ADVANCE market of a game whose YES (or, R106, whose
// two-sided SHORT side) backs the team named by the free-text side — the structural replacement
// for the fuzzy polyUSMatchForSide scan. flip=true means the named team is the instrument's
// short/NO side: its price = 1 − the slug's YES. The candidate must still pass the fuzzy path's
// liquidity gates (caller checks the snapshot row).
func (s *Server) giPUSSideMarket(gameID, sideText string) (slug string, flip bool, ok bool) {
	st := s.gi()
	s.giMu.Lock()
	defer s.giMu.Unlock()
	g := st.games[gameID]
	if g == nil {
		return "", false, false
	}
	t := st.teamFromText(g, sideText)
	if t == nil {
		return "", false, false
	}
	for _, id := range st.gameMkts[gameID]["polyus"] {
		c := st.byMkt["polyus|"+id]
		if c == nil {
			continue
		}
		switch c.mktType {
		case "winner", "advance":
			if c.yesTeam == t.abbr {
				return c.id, false, true
			}
			// Draw is a distinct YES proposition on a 3-way winner instrument. It is never a
			// two-outcome short/NO alias. Only advance/two-sided venue rows populate noTeam.
			if t.abbr != "draw" && c.noTeam != "" && c.noTeam == t.abbr {
				return c.id, true, true // R106: the short team of a single-slug two-sided instrument
			}
		}
	}
	return "", false, false
}

// giNoteStruct / giNoteFuzz: the fallback-rate counters (per matcher family).
func (s *Server) giNoteStruct(family string) {
	s.giMu.Lock()
	s.gi().structHits[family]++
	s.giMu.Unlock()
}
func (s *Server) giNoteFuzz(family string) {
	s.giMu.Lock()
	s.gi().fuzzHits[family]++
	s.giMu.Unlock()
}

// ── persistence + stats ─────────────────────────────────────────────────────────────────────────

// giPersist flushes dirty games/joins to SQLite (piggybacks the catalog sweep cadence). Also logs
// the coverage line the auditor grades: anchored counts + struct-vs-fuzz serve rates.
func (s *Server) giPersist(ctx context.Context) {
	st := s.gi()
	s.giMu.Lock()
	if time.Since(st.persistAt) < 4*time.Minute && len(st.dirtyGames)+len(st.dirtyMkts) < 500 {
		s.giMu.Unlock()
		return
	}
	st.persistAt = time.Now()
	gRows := make([]storage.GameIdentityRow, 0, len(st.dirtyGames))
	for id := range st.dirtyGames {
		g := st.games[id]
		if g == nil {
			continue
		}
		kev := ""
		for ev := range g.kalshiEvents {
			if kev == "" || ev < kev {
				kev = ev // deterministic representative event (the GAME/winner event usually sorts first)
			}
		}
		startUTC := ""
		if !g.start.IsZero() {
			startUTC = g.start.UTC().Format(time.RFC3339)
		}
		gRows = append(gRows, storage.GameIdentityRow{GameID: g.id, League: g.league, Away: g.away, Home: g.home,
			AwayName: g.awayName, HomeName: g.homeName, StartUTC: startUTC, KalshiEvent: kev,
			PUSEventID: g.pusEventID, PUSGameID: g.pusGameID})
	}
	mRows := make([]storage.MarketGameRow, 0, len(st.dirtyMkts))
	for mk := range st.dirtyMkts {
		ref := st.byMkt[mk]
		if ref == nil {
			continue
		}
		mRows = append(mRows, storage.MarketGameRow{Venue: ref.venue, Ticker: ref.id, GameID: ref.gameID,
			MktType: ref.mktType, YesTeam: ref.yesTeam, NoTeam: ref.noTeam, Line: ref.line, Src: "struct"})
	}
	games, kalA, kalS, pusA, pusS := len(st.games), st.kalAnchored, st.kalSeen, st.pusAnchored, st.pusSeen
	st.dirtyGames, st.dirtyMkts = map[string]bool{}, map[string]bool{}
	s.giMu.Unlock()
	if len(gRows) > 0 {
		if err := s.store.UpsertGameIdentity(ctx, gRows); err != nil {
			s.log.Warn("gameident: game upsert failed (retries next sweep)", "err", err, "rows", len(gRows))
			s.giMu.Lock()
			for _, r := range gRows {
				st.dirtyGames[r.GameID] = true
			}
			s.giMu.Unlock()
		}
	}
	if len(mRows) > 0 {
		if err := s.store.UpsertMarketGame(ctx, mRows); err != nil {
			s.log.Warn("gameident: market-join upsert failed (retries next sweep)", "err", err, "rows", len(mRows))
			s.giMu.Lock()
			for _, r := range mRows {
				st.dirtyMkts[r.Venue+"|"+r.Ticker] = true
			}
			s.giMu.Unlock()
		}
	}
	kp, ppct := 0.0, 0.0
	if kalS > 0 {
		kp = 100 * float64(kalA) / float64(kalS)
	}
	if pusS > 0 {
		ppct = 100 * float64(pusA) / float64(pusS)
	}
	s.log.Info("gameident coverage", "games", games,
		"kalshi_anchored_pct", strconv.FormatFloat(kp, 'f', 1, 64),
		"polyus_anchored_pct", strconv.FormatFloat(ppct, 'f', 1, 64),
		"flushed_games", len(gRows), "flushed_joins", len(mRows))
}

// handleGameTree — GET /api/gametree (R102 Part 2): the hierarchical Markets-tab view. Games (live
// first, then by start) → submarkets grouped by type, each row carrying BOTH venues' YES/NO prices
// where both list it (the canonical join makes the pairing trivial).
func (s *Server) handleGameTree(w http.ResponseWriter, r *http.Request) {
	type mrow struct {
		Type    string  `json:"type"`
		Label   string  `json:"label"`
		Line    float64 `json:"line,omitempty"`
		KTicker string  `json:"k_ticker,omitempty"`
		KURL    string  `json:"k_url,omitempty"`
		KBid    float64 `json:"k_bid,omitempty"`
		KAsk    float64 `json:"k_ask,omitempty"`
		PSlug   string  `json:"p_slug,omitempty"`
		PURL    string  `json:"p_url,omitempty"`
		PBid    float64 `json:"p_bid,omitempty"`
		PAsk    float64 `json:"p_ask,omitempty"`
		PFlip   bool    `json:"p_flip,omitempty"` // PUS row prices the OPPOSITE side (spread twin)
	}
	type grow struct {
		GameID   string `json:"game_id"`
		League   string `json:"league"`
		Away     string `json:"away"`
		Home     string `json:"home"`
		AwayName string `json:"away_name"`
		HomeName string `json:"home_name"`
		Start    string `json:"start"`
		Live     bool   `json:"live"`
		Venues   string `json:"venues"` // "K+P" | "K" | "P"
		Markets  []mrow `json:"markets"`
	}
	// snapshot prices OUTSIDE giMu (kmkts needs metaMu; PUS needs polyUSMu)
	pusPx := map[string]polyUSMarket{}
	for _, m := range s.polyUSSnapshot() {
		pusPx[m.Slug] = m
	}
	st := s.gi()
	s.giMu.Lock()
	type pend struct {
		g    *giGame
		refs []*giMktRef
	}
	var games []pend
	cutOld := time.Now().Add(-8 * time.Hour)
	cutNew := time.Now().Add(72 * time.Hour)
	for id, g := range st.games {
		if !g.start.IsZero() && (g.start.Before(cutOld) || g.start.After(cutNew)) {
			continue // the tree shows the actionable window; the DB keeps history
		}
		var refs []*giMktRef
		for venue, ids := range st.gameMkts[id] {
			for _, mid := range ids {
				if ref := st.byMkt[venue+"|"+mid]; ref != nil {
					refs = append(refs, ref)
				}
			}
		}
		if len(refs) == 0 {
			continue
		}
		games = append(games, pend{g: g, refs: refs})
	}
	// pair + shape under the lock (map reads only), price fills after
	out := make([]grow, 0, len(games))
	for _, p := range games {
		g := p.g
		var rows []mrow
		paired := map[string]bool{}
		for _, ref := range p.refs {
			if ref.venue != "kalshi" {
				continue
			}
			row := mrow{Type: ref.mktType, Line: ref.line, KTicker: ref.id, Label: giRowLabel(g, ref)}
			if slug, sameSide, ok := st.twinLocked(ref, "polyus"); ok {
				row.PSlug, row.PFlip = slug, !sameSide
				paired["polyus|"+slug] = true
			}
			rows = append(rows, row)
		}
		for _, ref := range p.refs {
			if ref.venue != "polyus" || paired["polyus|"+ref.id] {
				continue
			}
			rows = append(rows, mrow{Type: ref.mktType, Line: ref.line, PSlug: ref.id, Label: giRowLabel(g, ref)})
		}
		ven := ""
		hasK, hasP := len(st.gameMkts[g.id]["kalshi"]) > 0, len(st.gameMkts[g.id]["polyus"]) > 0
		switch {
		case hasK && hasP:
			ven = "K+P"
		case hasK:
			ven = "K"
		default:
			ven = "P"
		}
		startS := ""
		if !g.start.IsZero() {
			startS = g.start.UTC().Format(time.RFC3339)
		}
		out = append(out, grow{GameID: g.id, League: g.league, Away: g.away, Home: g.home,
			AwayName: g.awayName, HomeName: g.homeName, Start: startS, Live: g.live, Venues: ven, Markets: rows})
	}
	s.giMu.Unlock()
	// price fill (outside giMu): Kalshi from kmkts/live WS; PUS from the snapshot
	for gi := range out {
		typeOrder := map[string]int{"winner": 0, "advance": 1, "spread": 2, "total": 3, "prop": 4}
		sort.SliceStable(out[gi].Markets, func(a, b int) bool {
			ra, rb := out[gi].Markets[a], out[gi].Markets[b]
			if typeOrder[ra.Type] != typeOrder[rb.Type] {
				return typeOrder[ra.Type] < typeOrder[rb.Type]
			}
			if ra.Line != rb.Line {
				return ra.Line < rb.Line
			}
			return ra.Label < rb.Label
		})
		if len(out[gi].Markets) > 40 {
			out[gi].Markets = out[gi].Markets[:40] // prop ladders can be deep — cap the payload
		}
		for mi := range out[gi].Markets {
			row := &out[gi].Markets[mi]
			if row.KTicker != "" {
				row.KURL = kalshiMarketTickerURL(row.KTicker)
				s.metaMu.Lock()
				if km, ok := s.kmkts[row.KTicker]; ok {
					row.KBid, row.KAsk = km.YesBid.Float(), km.YesAsk.Float()
				}
				s.metaMu.Unlock()
				if row.KBid == 0 && row.KAsk == 0 && s.kal != nil {
					if lp, ok := s.kal.LivePrice(row.KTicker); ok {
						row.KBid, row.KAsk = lp, lp
					}
				}
			}
			if row.PSlug != "" {
				row.PURL = "https://polymarket.us/market/" + row.PSlug
				if pm, ok := pusPx[row.PSlug]; ok {
					row.PBid, row.PAsk = pm.Bid, pm.Ask
				} else if s.polyUSWS != nil {
					if ly, ok := s.polyUSWS.LiveYes(row.PSlug); ok {
						row.PBid, row.PAsk = ly, ly
					}
				}
			}
		}
	}
	sort.SliceStable(out, func(a, b int) bool {
		if out[a].Live != out[b].Live {
			return out[a].Live
		}
		return out[a].Start < out[b].Start
	})
	if len(out) > 250 {
		out = out[:250]
	}
	s.giMu.Lock()
	stats := map[string]any{
		"games": len(st.games), "kal_seen": st.kalSeen, "kal_anchored": st.kalAnchored,
		"pus_seen": st.pusSeen, "pus_anchored": st.pusAnchored,
		"struct_hits": copyInt64Map(st.structHits), "fuzz_hits": copyInt64Map(st.fuzzHits),
	}
	s.giMu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{"games": out, "stats": stats})
}

func copyInt64Map(m map[string]int64) map[string]int64 {
	c := make(map[string]int64, len(m))
	for k, v := range m {
		c[k] = v
	}
	return c
}

func giRowLabel(g *giGame, ref *giMktRef) string {
	switch ref.mktType {
	case "winner":
		switch ref.yesTeam {
		case g.away:
			return g.awayName + " wins"
		case g.home:
			return g.homeName + " wins"
		case "draw":
			return "Draw"
		}
		return "Winner"
	case "advance": // R106 (bug 255)
		nm := ref.yesTeam
		switch ref.yesTeam {
		case g.away:
			nm = g.awayName
		case g.home:
			nm = g.homeName
		}
		if nm == "" {
			return "To Advance"
		}
		return nm + " advances"
	case "total":
		return "Total " + strconv.FormatFloat(ref.line, 'f', -1, 64)
	case "spread":
		nm := ref.yesTeam
		switch ref.yesTeam {
		case g.away:
			nm = g.awayName
		case g.home:
			nm = g.homeName
		}
		return "Spread " + nm + " " + fmtSigned(ref.line)
	}
	return "Prop"
}

func fmtSigned(v float64) string {
	if v > 0 {
		return "+" + strconv.FormatFloat(v, 'f', -1, 64)
	}
	return strconv.FormatFloat(v, 'f', -1, 64)
}
