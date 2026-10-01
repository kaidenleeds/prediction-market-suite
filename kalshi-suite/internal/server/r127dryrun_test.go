package server

// r127dryrun_test.go — the R127 offline coverage estimator (guarded by PINT_DRYRUN=1; see
// TestR127OfflineCoverage). READ-ONLY against a scratch copy of the live DB; the optional live
// half (PINT_DRYRUN_LIVE=n) spends ≤n gamma event fetches at 150ms spacing through the REAL
// anchor path. Never touches the running suite.

import (
	"context"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/polymarket"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

type pintDryTally struct {
	events, rows, tokenResolved, gameFound, noGame, ambiguous, noVocab int
}

func runPintDryRun(t *testing.T) {
	ctx := context.Background()
	store, err := storage.Open("../../data")
	if err != nil {
		t.Fatalf("open DB copy: %v", err)
	}

	// hydrate the registry from the copy's game_identity + market_game (kalshi/polyus)
	s := &Server{}
	st := s.gi()
	games, err := store.GameIdentityRows(ctx)
	if err != nil {
		t.Fatalf("game_identity: %v", err)
	}
	sort.Slice(games, func(i, j int) bool { return games[i].GameID < games[j].GameID }) // "#2" appends after base
	s.giMu.Lock()
	for _, r := range games {
		parts := strings.SplitN(r.GameID, ":", 3)
		if len(parts) != 3 || r.League == "" || r.Away == "" || r.Home == "" {
			continue
		}
		etDate := parts[1]
		st.learnTeam(r.League, r.AwayName, r.Away)
		st.learnTeam(r.League, r.HomeName, r.Home)
		start := time.Time{}
		if ts, e := time.Parse(time.RFC3339, r.StartUTC); e == nil {
			start = ts.UTC()
		}
		key := r.League + "|" + etDate + "|" + r.Away + "|" + r.Home
		g := &giGame{id: r.GameID, league: r.League, away: r.Away, home: r.Home,
			awayName: r.AwayName, homeName: r.HomeName, start: start, etDate: etDate,
			kalshiEvents: map[string]bool{}, dh: len(st.byKey[key])}
		st.games[g.id] = g
		st.byKey[key] = append(st.byKey[key], g)
	}
	joins := 0
	for _, venue := range []string{"kalshi", "polyus"} {
		rows, e := store.MarketGameRows(ctx, venue)
		if e != nil {
			t.Fatalf("market_game %s: %v", venue, e)
		}
		for _, r := range rows {
			if st.games[r.GameID] == nil {
				continue
			}
			st.byMkt[venue+"|"+r.Ticker] = &giMktRef{gameID: r.GameID, venue: venue, id: r.Ticker,
				mktType: r.MktType, yesTeam: r.YesTeam, noTeam: r.NoTeam, line: r.Line}
			if st.gameMkts[r.GameID] == nil {
				st.gameMkts[r.GameID] = map[string][]string{}
			}
			st.gameMkts[r.GameID][venue] = append(st.gameMkts[r.GameID][venue], r.Ticker)
			joins++
		}
	}
	s.giMu.Unlock()
	t.Logf("hydrated: %d games, %d kalshi+polyus joins", len(games), joins)

	// walk EVERY open poly-int event key through the real grammar + a timeless anchor lookup
	nowTS := time.Now().UTC().Format(time.RFC3339)
	perLeague := map[string]*pintDryTally{}
	tally := func(lg string) *pintDryTally {
		if perLeague[lg] == nil {
			perLeague[lg] = &pintDryTally{}
		}
		return perLeague[lg]
	}
	var liveCandidates []string // event keys whose game exists (the live half samples these)
	totalKeys, grammarMiss := 0, 0
	cursor := ""
	for {
		page, e := store.PolyIntEventPage(ctx, cursor, nowTS, 1000)
		if e != nil {
			t.Fatalf("event page: %v", e)
		}
		if len(page) == 0 {
			break
		}
		for _, pg := range page {
			cursor = pg.EventKey
			totalKeys++
			lg, _, awayTok, homeTok, date, ok := pintParseEventKey(pg.EventKey)
			if !ok {
				grammarMiss++
				continue
			}
			ta := tally(lg)
			ta.events++
			ta.rows += pg.Rows
			s.giMu.Lock()
			if len(st.teams[lg]) == 0 {
				ta.noVocab++
				s.giMu.Unlock()
				continue
			}
			away := st.teamByAbbr(lg, awayTok)
			home := st.teamByAbbr(lg, homeTok)
			if away == nil || home == nil || away == home {
				s.giMu.Unlock()
				continue
			}
			ta.tokenResolved++
			g, why := pintFindGame(st, lg, date, away, home, time.Time{})
			s.giMu.Unlock()
			switch {
			case g != nil:
				ta.gameFound++
				liveCandidates = append(liveCandidates, pg.EventKey)
			case why == "ambiguous_game":
				ta.ambiguous++
			default:
				ta.noGame++
			}
		}
		if len(page) < 1000 {
			break
		}
	}
	t.Logf("open poly-int event keys: %d (grammar-miss %d = futures/props/other shapes)", totalKeys, grammarMiss)
	lgs := make([]string, 0, len(perLeague))
	for lg := range perLeague {
		lgs = append(lgs, lg)
	}
	sort.Strings(lgs)
	for _, lg := range lgs {
		ta := perLeague[lg]
		t.Logf("league %-9s events=%-4d rows=%-5d token_resolved=%-4d game_found=%-4d no_game=%-4d ambiguous=%-2d no_vocab=%d",
			lg, ta.events, ta.rows, ta.tokenResolved, ta.gameFound, ta.noGame, ta.ambiguous, ta.noVocab)
	}

	// live half: run the REAL anchor path on n sampled game-found events (metadata quality +
	// full-game vs period split measured on real payloads, not estimated)
	nLive, _ := strconv.Atoi(os.Getenv("PINT_DRYRUN_LIVE"))
	if nLive <= 0 {
		return
	}
	if nLive > len(liveCandidates) {
		nLive = len(liveCandidates)
	}
	pintReg.reset()
	client := polymarket.NewClient(12 * time.Second)
	fetched := 0
	for _, key := range liveCandidates {
		if fetched >= nLive {
			break
		}
		lg, gameSlug, _, _, _, _ := pintParseEventKey(key)
		ev, ok := client.EventBySlug(ctx, key)
		fetched++
		if !ok {
			pintReg.refuse("gamma_miss", 1)
			continue
		}
		s.pintAnchorEvent(lg, gameSlug, ev)
		time.Sleep(150 * time.Millisecond)
	}
	pintReg.mu.Lock()
	anchored := len(pintReg.refs)
	refused := map[string]int64{}
	for k, v := range pintReg.refused {
		refused[k] = v
	}
	pintReg.mu.Unlock()
	kTwin, pTwin := 0, 0
	for _, p := range s.pintTwinPairs() {
		if p.kalshi != "" {
			kTwin++
		}
		if p.polyus != "" {
			pTwin++
		}
	}
	t.Logf("LIVE sample: %d events fetched → %d markets anchored, refused=%v, k_twins=%d, pus_twins=%d",
		fetched, anchored, refused, kTwin, pTwin)
}
