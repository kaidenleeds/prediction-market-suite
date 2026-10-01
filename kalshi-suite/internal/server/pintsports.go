package server

// pintsports.go — R127 POLY-INT SPORTS ANCHORING (the parked R125 recall gap: ~14.9k poly-int
// sports catalog rows were match-eligible but the structural matcher never attempted them).
//
// A budgeted background sweep walks OPEN poly-int (venue 'polymarket') sports catalog rows,
// pulls gamma EVENT metadata (one request serves every nested market of a game), derives the
// canonical game identity and joins pint markets to EXISTING gameident anchors — it never
// creates games, so a join can only land on a game Kalshi/PolyUS already described. Joins
// persist to market_game under venue 'polyint' with src 'pint:unverified' (flip to
// 'pint:verified' by hand after the 30-sample verification — one UPDATE).
//
// STRUCTURAL METADATA ONLY (R100/R102/R125 doctrine — hasty slug parsing manufactured 26% wrong
// joins once already). Live-probed 2026-07-09 (mlb-sea-mia / wnba-sea-atl / fifwc-esp-bel /
// fifwc-arg-che-more-markets):
//
//	event   slug "<league>-<away>-<home>-<yyyy-mm-dd>[-more-markets]" (STRICT grammar; anything
//	        else — exact-score, futures, timestamped props — is not a game event and is skipped);
//	        teams:[{name,abbreviation,league,ordering:"away"/"home"}] (ordering tags are REQUIRED:
//	        fifwc slugs list HOME first while wnba lists AWAY first — slug order is not identity);
//	        startTime RFC3339.
//	market  sportsMarketType — the venue's own type string, and unlike the PUS V2 enum it is
//	        SCOPE-AWARE: full-game = "moneyline"/"spreads"/"totals" (+ "soccer_team_to_advance");
//	        period/prop variants carry their own spellings ("baseball_team_first_five_spread",
//	        "first_half_totals", "soccer_team_totals", "points", "nrfi", …). ALLOW-LIST by design
//	        (pusFullScope's refuse-by-default): an unknown spelling refuses, never twins.
//	        line — SIGNED, riding outcome-0 ("spread-home-11pt5" ⇒ line=-11.5, outcomes[0]=home
//	        team), which is ALREADY the Kalshi-YES orientation giMktRef uses (yesTeam covers
//	        `line`) — no sign flip anywhere (the R125 double-flip lesson).
//	        outcomes — the two team names (moneyline/spreads/advance), ["Over","Under"] (totals),
//	        or ["Yes","No"] (soccer per-side 3-way markets, side = the slug's team-abbrev suffix
//	        matched against the event's OWN team vocabulary; "-draw" = the draw side).
//
// GUARDS: same-city twins resolve via teamFromText's distinctive-token rule on FULL team names
// (bug-205); doubleheaders join only inside the ±75min start window and a timeless lookup with
// two candidates REFUSES (bug-206); venue order disagreement falls back to the reversed key the
// way getGame does (R106 neutral-site lesson); market-type semantics twin only type==type via
// twinLocked (spread lines must negate exactly on the flip side).
//
// GATE: anchoring/logging runs behind auto.pint_sports_anchor (default ON — zero-risk, log/DB
// only). CONSUMPTION is behind auto.pint_sports_consume (default OFF): the ONLY read path out of
// this registry is pintLockCandidates(), which returns nil until armed, so the xvlock scanner —
// and everything else — sees nothing until the operator flips the knob after hand-verification.
// Poly-int stays venue-locked for betting either way; matching is for gap/lock SCANNING only.

import (
	"context"
	"math/rand"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/polymarket"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

const (
	pintEventBudget  = 20                     // gamma event fetches per pass (1 request ≈ a whole game's markets)
	pintMarketBudget = 200                    // markets classified per pass (whichever budget exhausts first)
	pintFetchSpacing = 150 * time.Millisecond // politeness gap between gamma event fetches
	pintSweepMinGap  = 4 * time.Minute        // self-throttle under the 5-min run cadence
	pintRetryTTL     = 6 * time.Hour          // don't refetch the same event slug more often than this
	pintCursorKey    = "pint_sports_cursor"   // kv resume cursor (last catalog event_key processed)
	pintSrcTag       = "pint:unverified"      // market_game src until the 30-sample hand-verification
)

// pintLeagues: pint event-slug league token → gameident league key. Only leagues gameident can
// actually anchor (seeded team tables or PUS-enriched vocab) are eligible; anything else is
// skipped, never guessed. fifwc = the venue's World Cup token (gameident key "fwc").
var pintLeagues = map[string]string{
	"mlb": "mlb", "nba": "nba", "nfl": "nfl", "nhl": "nhl", "wnba": "wnba",
	"fifwc": "fwc", "ucl": "ucl", "epl": "epl", "mls": "mls",
	"cbb": "cbb", "wcbb": "wcbb",
	"cs2": "cs2", "dota2": "dota2", "lol": "lol", "val": "valorant",
	"atp": "atp", "wta": "wta",
}

// pintEventRe: the venue's game-event slug grammar (STRICT — see header). Doubleheader/second
// listings that break the grammar are refused rather than guessed.
var pintEventRe = regexp.MustCompile(`^([a-z0-9]+)-([a-z0-9]{2,5})-([a-z0-9]{2,5})-(\d{4}-\d{2}-\d{2})(-more-markets)?$`)

// pintScopeType — R125 refuse-by-default ALLOW-LIST: the venue sportsMarketType spellings that
// verifiably cover the FULL game, mapped to giMktRef's type classes. Everything else (period
// markets, team totals, props, BTTS, exact scores, empty/legacy metadata) refuses.
var pintScopeType = map[string]string{
	"moneyline":              "winner",
	"spreads":                "spread",
	"totals":                 "total",
	"soccer_team_to_advance": "advance",
}

// pintClassify maps the venue's sportsMarketType to our type class; ok=false = refused scope.
func pintClassify(smt string) (string, bool) {
	t, ok := pintScopeType[strings.ToLower(strings.TrimSpace(smt))]
	return t, ok
}

// pintRef is one anchored poly-int market (giMktRef semantics, venue "polyint").
type pintRef struct {
	condID, slug, question string
	gameID                 string
	league                 string
	mktType                string // winner|spread|total|advance
	yesTeam, noTeam        string
	line                   float64
	start                  time.Time
}

// pintState — the whole registry + counters. Package-level like xvReg (tests reset it); the
// Server struct is deliberately untouched (heavy concurrent edits land there this round).
type pintState struct {
	mu                                         sync.Mutex
	refs                                       map[string]*pintRef  // conditionId → anchored ref
	bySlug                                     map[string]string    // lower(market slug) → conditionId
	tried                                      map[string]time.Time // event slug → last fetch (retry TTL)
	refused                                    map[string]int64     // reason → market rows refused
	candRows, candEvents, fetched, anchoredNew int64
	cursor                                     string
	loadedDB                                   bool
	sweepAt                                    time.Time
}

func newPintState() *pintState {
	return &pintState{refs: map[string]*pintRef{}, bySlug: map[string]string{},
		tried: map[string]time.Time{}, refused: map[string]int64{}}
}

var pintReg = newPintState()

func (st *pintState) reset() { // tests (never reassign the struct — the mutex must survive)
	st.mu.Lock()
	st.refs = map[string]*pintRef{}
	st.bySlug = map[string]string{}
	st.tried = map[string]time.Time{}
	st.refused = map[string]int64{}
	st.candRows, st.candEvents, st.fetched, st.anchoredNew = 0, 0, 0, 0
	st.cursor = ""
	st.loadedDB = false
	st.sweepAt = time.Time{}
	st.mu.Unlock()
}

func (st *pintState) refuse(reason string, n int) {
	if n <= 0 {
		return
	}
	st.mu.Lock()
	st.refused[reason] += int64(n)
	st.mu.Unlock()
}

// pintParseEventKey applies the strict game grammar. gameSlug = the base game slug (the
// "-more-markets" satellite event shares its game's slug prefix and its market slugs).
func pintParseEventKey(eventKey string) (league, gameSlug, awayTok, homeTok, date string, ok bool) {
	m := pintEventRe.FindStringSubmatch(strings.ToLower(strings.TrimSpace(eventKey)))
	if m == nil {
		return "", "", "", "", "", false
	}
	lg, hit := pintLeagues[m[1]]
	if !hit {
		return "", "", "", "", "", false
	}
	return lg, m[1] + "-" + m[2] + "-" + m[3] + "-" + m[4], m[2], m[3], m[4], true
}

// pintResolveTeams resolves the event's away/home from the venue's OWN team objects (name +
// abbreviation + ordering tag). Ordering tags are required: fifwc slugs list home first while
// wnba lists away first (live-probed), so slug order is never identity. nil = refuse.
func pintResolveTeams(st *giState, lg string, ev polymarket.SportsEvent) (away, home *giTeam) {
	resolve := func(t polymarket.SportsTeam) *giTeam {
		if nn := giNormName(t.Name); nn != "" {
			if byName := st.byName[lg]; byName != nil {
				if rt := byName[nn]; rt != nil {
					return rt
				}
			}
		}
		return st.teamByAbbr(lg, t.Abbreviation)
	}
	for _, t := range ev.Teams {
		rt := resolve(t)
		if rt == nil {
			continue
		}
		switch strings.ToLower(strings.TrimSpace(t.Ordering)) {
		case "away":
			away = rt
		case "home":
			home = rt
		}
	}
	if away == home { // both orderings resolved onto one team — corrupt metadata, refuse
		return nil, nil
	}
	return away, home
}

// pintFindGame is getGame's LOOKUP-ONLY sibling: it never creates rows, so pint markets join
// only games the tradeable venues already anchored. Second return = refuse reason ("" on hit).
func pintFindGame(st *giState, league, etDate string, away, home *giTeam, start time.Time) (*giGame, string) {
	if away == nil || home == nil || league == "" || etDate == "" {
		return nil, "event_teams_unresolved"
	}
	list := st.byKey[league+"|"+etDate+"|"+away.abbr+"|"+home.abbr]
	if len(list) == 0 {
		// R106 neutral-site lesson: venue ORDER disagreement — same two teams same ET day.
		list = st.byKey[league+"|"+etDate+"|"+home.abbr+"|"+away.abbr]
	}
	if len(list) == 0 {
		return nil, "no_anchor"
	}
	if !start.IsZero() {
		for _, g := range list {
			if g.start.IsZero() {
				continue
			}
			d := g.start.Sub(start)
			if d < 0 {
				d = -d
			}
			if d <= giDoubleheaderWindow {
				return g, ""
			}
		}
		if len(list) == 1 && list[0].start.IsZero() {
			return list[0], "" // lone date-only anchor: date evidence is all either side has
		}
		return nil, "no_anchor" // timed start matches no candidate (wrong game of a doubleheader ≠ a join)
	}
	if len(list) == 1 {
		return list[0], ""
	}
	return nil, "ambiguous_game" // doubleheader + no time evidence — refuse (bug-206 doctrine)
}

// pintAnchorEvent classifies + anchors every market of one fetched gamma event against an
// EXISTING canonical game. Returns the market_game rows under the suite-wide public venue id
// `polymarket` (the pre-R139 `polyint` alias is migrated at storage open).
func (s *Server) pintAnchorEvent(lg, gameSlug string, ev polymarket.SportsEvent) []storage.MarketGameRow {
	nMkts := len(ev.Markets)
	if nMkts == 0 {
		pintReg.refuse("gamma_empty", 1)
		return nil
	}
	start, _ := parsePolyTime(strings.TrimSpace(ev.StartTime)) // zero on parse failure → date-only lookup
	etDate := ""
	if et := etLocation(); et != nil && !start.IsZero() {
		etDate = start.In(et).Format("2006-01-02")
	}
	if etDate == "" { // the slug date IS the venues' shared ET-date convention (probe: 01:00Z game under the 07-11 slug)
		if i := strings.LastIndexByte(gameSlug, '-'); i >= 10 {
			etDate = gameSlug[len(gameSlug)-10:]
		}
	}

	type hit struct {
		ref pintRef
		row storage.MarketGameRow
	}
	var hits []hit
	refuseLocal := map[string]int{}

	st := s.gi()
	s.giMu.Lock()
	away, home := pintResolveTeams(st, lg, ev)
	g, why := pintFindGame(st, lg, etDate, away, home, start)
	if g == nil {
		s.giMu.Unlock()
		pintReg.refuse(why, nMkts)
		return nil
	}
	for _, m := range ev.Markets {
		if m.ConditionID == "" || m.Slug == "" {
			refuseLocal["bad_row"]++
			continue
		}
		smt := strings.TrimSpace(m.SportsMarketType)
		mktType, fullGame := pintClassify(smt)
		if !fullGame {
			if smt == "" {
				refuseLocal["no_meta"]++ // absent metadata: never guess (pre-R125 scope-blindness class)
			} else {
				refuseLocal["scope"]++ // recognized venue string outside the full-game allow-list
			}
			continue
		}
		outs := m.Outcomes()
		if len(outs) != 2 {
			refuseLocal["bad_outcomes"]++
			continue
		}
		yesTeam, noTeam, line := "", "", 0.0
		mslug := strings.ToLower(strings.TrimSpace(m.Slug))
		switch mktType {
		case "winner":
			if strings.EqualFold(outs[0], "yes") && strings.EqualFold(outs[1], "no") {
				// soccer per-side 3-way shape: side identity = the slug suffix token matched
				// against the event's OWN team abbreviations (venue vocabulary), or the draw.
				suffix := strings.TrimPrefix(mslug, gameSlug+"-")
				if suffix == mslug { // no suffix and Yes/No outcomes — unidentifiable side
					refuseLocal["side_unresolved"]++
					continue
				}
				switch suffix {
				case "draw", "tie":
					yesTeam = "draw"
				default:
					var vt *polymarket.SportsTeam
					for i := range ev.Teams {
						if strings.ToLower(strings.TrimSpace(ev.Teams[i].Abbreviation)) == suffix {
							vt = &ev.Teams[i]
							break
						}
					}
					if vt == nil {
						refuseLocal["side_unresolved"]++
						continue
					}
					t := st.teamFromText(g, vt.Name)
					if t == nil {
						refuseLocal["side_unresolved"]++
						continue
					}
					yesTeam = t.abbr
				}
			} else {
				// outcomes are the two team names; outcome-0 is the YES side (venue convention,
				// live-probed). Distinctive-evidence resolution only (bug-205 same-city rule).
				yes := st.teamFromText(g, outs[0])
				no := st.teamFromText(g, outs[1])
				if yes == nil || no == nil || yes == no || yes.abbr == "draw" || no.abbr == "draw" {
					refuseLocal["side_unresolved"]++
					continue
				}
				yesTeam, noTeam = yes.abbr, no.abbr
			}
		case "spread":
			v, okL := m.SportsLine()
			if !okL {
				refuseLocal["line_missing"]++
				continue
			}
			t := st.teamFromText(g, outs[0])
			if t == nil || t.abbr == "draw" {
				refuseLocal["side_unresolved"]++
				continue
			}
			// slug side marker cross-check where present ("spread-home-*"/"spread-away-*"):
			// a disagreement between the venue's own two identity carriers is corrupt → refuse.
			if strings.Contains(mslug, "-spread-home-") && t.abbr != g.home {
				refuseLocal["side_conflict"]++
				continue
			}
			if strings.Contains(mslug, "-spread-away-") && t.abbr != g.away {
				refuseLocal["side_conflict"]++
				continue
			}
			yesTeam, line = t.abbr, v // venue line is SIGNED and rides outcome-0 = Kalshi-YES orientation; NO flip (R125)
		case "total":
			v, okL := m.SportsLine()
			if !okL || v <= 0 {
				refuseLocal["line_missing"]++
				continue
			}
			side := strings.ToLower(strings.TrimSpace(outs[0]))
			if side != "over" && side != "under" {
				refuseLocal["side_unresolved"]++
				continue
			}
			yesTeam, line = side, v
		case "advance":
			// two-sided single instrument (probe: outcomes = the two team names): YES = outcome-0
			// advances; in a 2-team KO the NO side IS the opponent (R106 complement semantics).
			ty := st.teamFromText(g, outs[0])
			tn := st.teamFromText(g, outs[1])
			if ty == nil || tn == nil || ty == tn || ty.abbr == "draw" || tn.abbr == "draw" {
				refuseLocal["side_unresolved"]++
				continue
			}
			yesTeam, noTeam = ty.abbr, tn.abbr
		}
		ref := pintRef{condID: m.ConditionID, slug: mslug, question: strings.TrimSpace(m.Question),
			gameID: g.id, league: lg, mktType: mktType, yesTeam: yesTeam, noTeam: noTeam,
			line: line, start: g.start}
		hits = append(hits, hit{ref: ref, row: storage.MarketGameRow{Venue: "polymarket",
			Ticker: m.ConditionID, GameID: g.id, MktType: mktType, YesTeam: yesTeam,
			NoTeam: noTeam, Line: line, Src: pintSrcTag}})
	}
	s.giMu.Unlock()

	pintReg.mu.Lock()
	for _, h := range hits {
		if _, seen := pintReg.refs[h.ref.condID]; !seen {
			pintReg.anchoredNew++
		}
		r := h.ref
		pintReg.refs[r.condID] = &r
		if r.slug != "" {
			pintReg.bySlug[r.slug] = r.condID
		}
	}
	pintReg.mu.Unlock()
	for reason, n := range refuseLocal {
		pintReg.refuse(reason, n)
	}
	rows := make([]storage.MarketGameRow, 0, len(hits))
	for _, h := range hits {
		rows = append(rows, h.row)
	}
	return rows
}

// pintSportsSweep — the 5-min budgeted anchor pass (registered in MonitorPaper's run table).
// Walks the catalog's distinct poly-int event keys behind a kv cursor, spends the gamma budget
// on grammar-eligible game events, anchors their markets, persists joins, advances the cursor
// (wrapping to "" at board end so no_anchor refusals get retried as new games appear).
func (s *Server) pintSportsSweep(ctx context.Context) {
	if s.store == nil || s.poly == nil {
		return
	}
	if !s.cfg().Auto.PintSportsAnchorOn() {
		return
	}
	pintReg.mu.Lock()
	if time.Since(pintReg.sweepAt) < pintSweepMinGap {
		pintReg.mu.Unlock()
		return
	}
	pintReg.sweepAt = time.Now()
	needLoad := !pintReg.loadedDB
	pintReg.loadedDB = true
	cursor := pintReg.cursor
	pintReg.mu.Unlock()

	if needLoad { // boot reload: persisted joins + the resume cursor survive restarts
		if v, ok := s.store.KVGet(ctx, pintCursorKey); ok {
			cursor = v
		}
		if rows, err := s.store.MarketGameRows(ctx, "polymarket"); err == nil {
			pintReg.mu.Lock()
			for _, r := range rows {
				if _, seen := pintReg.refs[r.Ticker]; !seen {
					pintReg.refs[r.Ticker] = &pintRef{condID: r.Ticker, gameID: r.GameID,
						mktType: r.MktType, yesTeam: r.YesTeam, noTeam: r.NoTeam, line: r.Line}
				}
			}
			pintReg.cursor = cursor
			pintReg.mu.Unlock()
		}
	}

	nowTS := time.Now().UTC().Format(time.RFC3339)
	events, markets := 0, 0
	var pending []storage.MarketGameRow
	wrapped := false
	for events < pintEventBudget && markets < pintMarketBudget && ctx.Err() == nil {
		page, err := s.store.PolyIntEventPage(ctx, cursor, nowTS, 400)
		if err != nil || len(page) == 0 {
			wrapped = true
			break
		}
		for _, pg := range page {
			cursor = pg.EventKey
			lg, gameSlug, _, _, _, ok := pintParseEventKey(pg.EventKey)
			if !ok {
				continue // not a game event (futures/props/exact-score/other venues' grammar)
			}
			pintReg.mu.Lock()
			pintReg.candRows += int64(pg.Rows)
			pintReg.candEvents++
			last, triedRecently := pintReg.tried[pg.EventKey]
			pintReg.mu.Unlock()
			if triedRecently && time.Since(last) < pintRetryTTL {
				continue
			}
			st := s.gi()
			s.giMu.Lock()
			vocab := len(st.teams[lg]) > 0
			s.giMu.Unlock()
			if !vocab {
				pintReg.refuse("no_vocab", pg.Rows) // league recognized, team table empty — no budget spent
				continue
			}
			ev, okE := s.poly.EventBySlug(ctx, pg.EventKey)
			events++
			pintReg.mu.Lock()
			pintReg.fetched++
			pintReg.tried[pg.EventKey] = time.Now()
			if len(pintReg.tried) > 4000 { // bound the retry map
				for k, t := range pintReg.tried {
					if time.Since(t) > 2*pintRetryTTL {
						delete(pintReg.tried, k)
					}
				}
			}
			pintReg.mu.Unlock()
			if !okE {
				pintReg.refuse("gamma_miss", 1)
			} else {
				rows := s.pintAnchorEvent(lg, gameSlug, ev)
				markets += len(ev.Markets)
				pending = append(pending, rows...)
			}
			if events >= pintEventBudget || markets >= pintMarketBudget {
				break
			}
			select {
			case <-ctx.Done():
			case <-time.After(pintFetchSpacing):
			}
		}
		if len(page) < 400 {
			wrapped = true
			break
		}
	}
	if wrapped {
		cursor = "" // board end: next pass rescans from the top (new games + retried refusals)
	}
	pintReg.mu.Lock()
	pintReg.cursor = cursor
	anchored := len(pintReg.refs)
	pintReg.mu.Unlock()
	if len(pending) > 0 {
		if err := s.store.UpsertMarketGame(ctx, pending); err != nil {
			s.log.Warn("pintsports: market_game upsert failed (rows retry on the next wrap)", "err", err, "rows", len(pending))
		}
	}
	_ = s.store.KVSet(ctx, pintCursorKey, cursor)
	if events > 0 || len(pending) > 0 {
		s.log.Info("pintsports sweep", "events_fetched", events, "markets_seen", markets,
			"joins_written", len(pending), "anchored_total", anchored, "cursor", cursor)
	}
}

// ── twins + consumers (GATED) ──────────────────────────────────────────────────────────────────

// pintTwinPair is one pint ref with its structural cross-venue twins (computed via the SAME
// twinLocked semantics gameident's consumers use — type equality, side identity, exact/negated
// lines, R106 flip semantics).
type pintTwinPair struct {
	ref        pintRef
	kalshi     string
	kalshiSame bool
	polyus     string
	polyusSame bool
}

// pintTwinPairs enumerates pint refs holding at least one twin. UNGATED internal — the only
// consumer-facing surface is pintLockCandidates below (gate) and the /api/xvpairs stats payload
// (read-only numbers).
func (s *Server) pintTwinPairs() []pintTwinPair {
	pintReg.mu.Lock()
	refs := make([]pintRef, 0, len(pintReg.refs))
	for _, r := range pintReg.refs {
		refs = append(refs, *r)
	}
	pintReg.mu.Unlock()
	sort.Slice(refs, func(i, j int) bool { return refs[i].condID < refs[j].condID })
	st := s.gi()
	var out []pintTwinPair
	s.giMu.Lock()
	for _, r := range refs {
		gref := &giMktRef{gameID: r.gameID, venue: "polyint", id: r.condID,
			mktType: r.mktType, yesTeam: r.yesTeam, noTeam: r.noTeam, line: r.line}
		kt, ks, okK := st.twinLocked(gref, "kalshi")
		ps, pss, okP := st.twinLocked(gref, "polyus")
		if !okK && !okP {
			continue
		}
		p := pintTwinPair{ref: r}
		if okK {
			p.kalshi, p.kalshiSame = kt, ks
		}
		if okP {
			p.polyus, p.polyusSame = ps, pss
		}
		out = append(out, p)
	}
	s.giMu.Unlock()
	return out
}

// pintLockCandidates — THE consume gate (auto.pint_sports_consume, default FALSE). Until armed,
// every consumer sees nil; armed, it feeds the log-only xvlock scanner K↔Pint sports pairs and
// PUS↔Pint pairs priced off existing caches (zero new API). Poly-int YES and opposite-token
// asks/depth come from the bounded CLOB working set; an unquoted pair fails closed.
func (s *Server) pintLockCandidates() []xvlCandidate {
	if !s.cfg().Auto.PintSportsConsume || s.poly == nil {
		return nil
	}
	pairs := s.pintTwinPairs()
	if len(pairs) == 0 {
		return nil
	}
	pusPx := map[string]polyUSMarket{}
	for _, m := range s.polyUSSnapshot() {
		pusPx[m.Slug] = m
	}
	var out []xvlCandidate
	for _, p := range pairs {
		if len(out) >= 150 {
			break
		}
		gm, okC := s.xvlPintCached(p.ref.condID)
		if !okC || gm.BestBid <= 0 || gm.BestAsk <= 0 || gm.BestAsk >= 1 {
			continue
		}
		if p.kalshi != "" {
			if kb, ka, fresh := s.xvlKalshiQuote(p.kalshi); fresh {
				out = append(out, xvlCandidate{pair: "K-PINT", aVenue: "kalshi", aID: p.kalshi,
					bVenue: "polymarket", bID: p.ref.condID, sameSide: p.kalshiSame,
					aBid: kb, aAsk: ka, bBid: gm.BestBid, bAsk: gm.BestAsk,
					aAskSz: -1, aBidSz: -1, bAskSz: gm.BestAskDepth, bBidSz: gm.BestBidDepth,
					bNoAsk: gm.NoBestAsk, bNoAskSz: gm.NoBestAskDepth})
			}
		}
		if p.polyus != "" {
			if pm, okP := pusPx[p.polyus]; okP && pm.Bid > 0 && pm.Ask > 0 && pm.Ask < 1 {
				out = append(out, xvlCandidate{pair: "PUS-PINT", aVenue: "polyus", aID: p.polyus,
					bVenue: "polymarket", bID: p.ref.condID, sameSide: p.polyusSame,
					aBid: pm.Bid, aAsk: pm.Ask, bBid: gm.BestBid, bAsk: gm.BestAsk,
					aAskSz: pm.AskSz, aBidSz: pm.BidSz,
					bAskSz: gm.BestAskDepth, bBidSz: gm.BestBidDepth,
					bNoAsk: gm.NoBestAsk, bNoAskSz: gm.NoBestAskDepth})
			}
		}
	}
	return out
}

// ── coverage payload (/api/xvpairs "pint_sports" section) ──────────────────────────────────────

// pintPairsPayload returns the coverage stats + N random join samples (?samples=N, default 5,
// cap 50) — the surface the 30-sample hand-verification reads after merge.
func (s *Server) pintPairsPayload(ctx context.Context, samplesN int) map[string]any {
	pairs := s.pintTwinPairs()
	kPint, pusPint := 0, 0
	for _, p := range pairs {
		if p.kalshi != "" {
			kPint++
		}
		if p.polyus != "" {
			pusPint++
		}
	}
	if samplesN <= 0 {
		samplesN = 5
	}
	if samplesN > 50 {
		samplesN = 50
	}
	rand.Shuffle(len(pairs), func(i, j int) { pairs[i], pairs[j] = pairs[j], pairs[i] })
	if len(pairs) > samplesN {
		pairs = pairs[:samplesN]
	}
	title := func(venue, id string) string {
		if s.store == nil || id == "" {
			return ""
		}
		t, _ := s.store.CatalogTitle(ctx, venue, id)
		return t
	}
	samples := make([]map[string]any, 0, len(pairs))
	for _, p := range pairs {
		q := p.ref.question
		if q == "" {
			q = title("polymarket", p.ref.condID)
		}
		row := map[string]any{
			"pint_cond_id": p.ref.condID, "pint_slug": p.ref.slug, "pint_title": q,
			"game_id": p.ref.gameID, "league": p.ref.league, "type": p.ref.mktType,
			"yes_team": p.ref.yesTeam, "line": p.ref.line,
		}
		if p.ref.noTeam != "" {
			row["no_team"] = p.ref.noTeam
		}
		if !p.ref.start.IsZero() {
			row["start"] = p.ref.start.UTC().Format(time.RFC3339)
		}
		if p.kalshi != "" {
			row["kalshi_twin"], row["kalshi_same_side"] = p.kalshi, p.kalshiSame
			row["kalshi_title"] = title("kalshi", p.kalshi)
		}
		if p.polyus != "" {
			row["polyus_twin"], row["polyus_same_side"] = p.polyus, p.polyusSame
			row["polyus_title"] = title("polyus", p.polyus)
		}
		samples = append(samples, row)
	}
	pintReg.mu.Lock()
	refused := make(map[string]int64, len(pintReg.refused))
	for k, v := range pintReg.refused {
		refused[k] = v
	}
	payload := map[string]any{
		"anchor_on":        s.cfg().Auto.PintSportsAnchorOn(),
		"consume_armed":    s.cfg().Auto.PintSportsConsume,
		"candidate_rows":   pintReg.candRows,
		"candidate_events": pintReg.candEvents,
		"events_fetched":   pintReg.fetched,
		"anchored":         len(pintReg.refs),
		"refused":          refused,
		"joined_k_pint":    kPint,
		"joined_pus_pint":  pusPint,
		"cursor":           pintReg.cursor,
		"samples":          samples,
		"note":             "R127: poly-int sports joins (market_game venue='polyint', src '" + pintSrcTag + "'); counters are session-cumulative; consumers OFF until auto.pint_sports_consume — verify 30 samples by hand first",
	}
	pintReg.mu.Unlock()
	return payload
}
