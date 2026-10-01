package server

// R106 UNIVERSAL MARKET TREE (operator ask: EVERY genre incl. unknown).
//
// The R102 tree covered SPORTS only (gameident's anchored games); everything else lived in flat
// per-venue lists. This extends the venue-provided structure to every market: every Kalshi market
// belongs to an EVENT (event_ticker) and every event to a SERIES (ticker prefix — venue
// convention), and series carry a venue CATEGORY (GET /series?category=...). PolyUS rows carry
// the venue's own category/league; Poly-int markets carry their parent event slug + tag category.
//
//	genre (venue category metadata, NEVER title-guessing)
//	└─ event group (Kalshi event_ticker · PUS event id · poly-int event slug)
//	   └─ member markets with YES/NO prices + venue links
//
// Canonical genres: Sports · Crypto · Weather · Politics · Economics · Entertainment. Any OTHER
// venue category auto-creates its own node verbatim ("Science and Technology", "Health", …);
// series/markets with NO category metadata land under "Other/Unknown" — still event-grouped,
// never a flat dump. Sports keeps the R102 game tree as its anchored content; sports markets the
// game registry could NOT anchor appear here as ordinary event groups (full coverage, no holes).
//
// Lazy: /api/markettree (genres) → ?genre=X (event groups) → ?genre=X&event=K|<key> (members).
// All reads are in-memory caches (TopLiquidCached / polyUSSnapshot / poly.CachedMarkets — the
// exact catalogSweep sources) behind a 60s tree cache; the ONLY REST is the series→category map
// (≤ ~14 public calls per 12h, background tier).
//
// R107 (this pass):
//
//	WARM BOOT   the series→category map persists to data/series_categories.json on every
//	            successful refresh (wx_registry pattern: atomic tmp+rename) and seeds an EMPTY
//	            map synchronously at boot — the Kalshi genre split no longer depends on boot age
//	            (measured live: 36.5% Other/Unknown on a cold boot vs 25.15% warm the day
//	            before). A stale file still serves while the 12h background refresh replaces it.
//	UNIVERSE    each rebuild folds market_catalog rows (storage.CatalogUniverse — the ~89k-row
//	            tracked board) that the live caches don't currently carry: kalshi genre resolves
//	            via series prefix against the persisted map (+ gameident sports series); rows
//	            with unknowable genre are COUNTED under Other/Unknown, never dropped. Catalog-
//	            only members are title+url shells (no prices — the catalog has no book). The
//	            per-genre payload gains n_universe next to the live count; the top level gains
//	            universe_totals per venue and unknown_pct_universe. One indexed catalog pass per
//	            rebuild; the 60s tree cache and members-level laziness are unchanged.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

// kalshiCategoryEnum — the venue's category vocabulary to enumerate via GET /series?category=.
// A category the venue adds later simply isn't enumerated yet: its series land under
// Other/Unknown (grouped, visible) until this list learns it — documented degradation, no holes.
var kalshiCategoryEnum = []string{
	"Sports", "Crypto", "Climate and Weather", "Politics", "Elections", "World",
	"Economics", "Financials", "Companies", "Culture", "Entertainment",
	"Science and Technology", "Health", "Transportation",
}

// canonGenre maps a venue category/league string onto the canonical genre set; anything unmapped
// auto-creates its own node (Title-cased venue term — venue metadata verbatim, per the operator's
// "unknown genres auto-create from the venue's own series metadata").
func canonGenre(raw string) string {
	c := strings.ToLower(strings.TrimSpace(raw))
	switch c {
	case "":
		return "Other/Unknown"
	case "sports", "sport":
		return "Sports"
	case "crypto", "cryptocurrency":
		return "Crypto"
	case "climate and weather", "weather", "climate":
		return "Weather"
	case "politics", "elections", "world", "geopolitics":
		return "Politics"
	case "economics", "economy", "financials", "finance", "companies", "business":
		return "Economics"
	case "culture", "entertainment", "pop culture", "pop-culture", "music", "movies":
		return "Entertainment"
	}
	// PUS sports league slugs (fwc/mlb/nba/…) — the league IS the venue metadata for game rows.
	if _, ok := pusLeagueGenre[c]; ok {
		return "Sports"
	}
	// auto-create: venue term verbatim, Title-cased — small connectives stay lower ("Science and
	// Technology") so a PUS "science and technology" and Kalshi's exact category MERGE into one
	// node instead of casing-twins (observed live: "Science And Technology" vs "Science and
	// Technology" during the R106 soak).
	words := strings.Fields(c)
	for i, w := range words {
		if i > 0 && (w == "and" || w == "of" || w == "the" || w == "for" || w == "in") {
			continue
		}
		if len(w) > 0 {
			words[i] = strings.ToUpper(w[:1]) + w[1:]
		}
	}
	return strings.Join(words, " ")
}

// polyIntGenre — poly-int's first tag label is a fine-grained TOPIC ("Bitcoin", "California
// Midterm", "Ai"), NOT a venue category: raw auto-creation off it minted dozens of one-market
// pseudo-genres on the first live build. The tag (venue metadata, never the title) normalizes
// onto the canonical genres via the tag vocabulary below; unmapped topics sit under
// Other/Unknown (still event-grouped). Auto-creation stays reserved for REAL venue categories
// (Kalshi series categories / PUS category strings).
func polyIntGenre(tag string) string {
	c := strings.ToLower(strings.TrimSpace(tag))
	if c == "" {
		return "Other/Unknown"
	}
	switch g := canonGenre(c); g {
	case "Sports", "Crypto", "Weather", "Politics", "Economics", "Entertainment":
		return g
	}
	for kw, genre := range polyTagKeywords {
		if strings.Contains(c, kw) {
			return genre
		}
	}
	return "Other/Unknown"
}

// polyTagKeywords: poly-int tag-vocabulary → canonical genre (tag metadata normalization —
// titles are never read). "Science and Technology" targets Kalshi's own category name so the
// venues MERGE under one node.
var polyTagKeywords = map[string]string{
	"bitcoin": "Crypto", "ethereum": "Crypto", "solana": "Crypto", "xrp": "Crypto", "doge": "Crypto",
	"crypto": "Crypto", "coin": "Crypto", "btc": "Crypto", "eth": "Crypto",
	"nba": "Sports", "nfl": "Sports", "mlb": "Sports", "nhl": "Sports", "ufc": "Sports", "mma": "Sports",
	"boxing": "Sports", "tennis": "Sports", "golf": "Sports", "soccer": "Sports", "fifa": "Sports",
	"world cup": "Sports", "esports": "Sports", "olympic": "Sports", "f1": "Sports", "nascar": "Sports",
	"football": "Sports", "basketball": "Sports", "baseball": "Sports", "hockey": "Sports", "cricket": "Sports",
	"election": "Politics", "midterm": "Politics", "senate": "Politics", "congress": "Politics",
	"president": "Politics", "trump": "Politics", "geopolit": "Politics", "ukraine": "Politics",
	"israel": "Politics", "iran": "Politics", "nato": "Politics", "governor": "Politics", "mayor": "Politics",
	"fed": "Economics", "inflation": "Economics", "gdp": "Economics", "interest rate": "Economics",
	"stock": "Economics", "s&p": "Economics", "nasdaq": "Economics", "tariff": "Economics",
	"acquisition": "Economics", "ipo": "Economics", "earnings": "Economics", "recession": "Economics",
	"movie": "Entertainment", "music": "Entertainment", "oscar": "Entertainment", "grammy": "Entertainment",
	"celebrity": "Entertainment", "box office": "Entertainment", "album": "Entertainment",
	"temperature": "Weather", "hurricane": "Weather", "tornado": "Weather",
	"ai": "Science and Technology", "openai": "Science and Technology", "spacex": "Science and Technology",
	"tech": "Science and Technology", "science": "Science and Technology",
}

// pusLeagueGenre: the league slugs refreshPolyUS iterates (all sports by construction).
var pusLeagueGenre = map[string]bool{
	"mlb": true, "nba": true, "nfl": true, "nhl": true, "mls": true, "wnba": true, "cbb": true,
	"cws": true, "wcbb": true, "epl": true, "ucl": true, "bun": true, "fwc": true, "ufc": true,
	"boxing": true, "atp": true, "wta": true, "itfw": true, "itfm": true, "fiba": true,
	"valorant": true, "dota2": true, "lol": true, "cs2": true, "cod": true, "ipl": true,
	"bsl": true, "wt20": true, "t20": true, "test-cricket": true, "countychamp": true, "mlc": true,
}

// mtMember is one leaf market row.
type mtMember struct {
	Venue  string  `json:"venue"` // kalshi | polyus | polymarket (research)
	ID     string  `json:"id"`    // ticker / slug / conditionId
	Title  string  `json:"title"`
	Kind   string  `json:"kind,omitempty"`
	YesBid float64 `json:"yes_bid,omitempty"`
	YesAsk float64 `json:"yes_ask,omitempty"`
	URL    string  `json:"url,omitempty"`
}

// mtEvent is one event group node. N counts LIVE members (priced, in the venue caches right
// now); NUniverse additionally counts catalog-folded members (R107 — title+url shells, no book).
type mtEvent struct {
	Key       string `json:"key"` // venue-prefixed event key ("K|KXWCADVANCE-26JUL09FRAMAR", "P|ev-123", "I|slug")
	Title     string `json:"title"`
	Venue     string `json:"venue"` // K | P | I
	N         int    `json:"n"`
	NUniverse int    `json:"n_universe"`       // R107: live + catalog members
	Series    string `json:"series,omitempty"` // kalshi series ticker (the venue's own hierarchy tier)
}

type mtGenreNode struct {
	events  map[string]*mtEvent
	members map[string][]mtMember // event key → members
	n       int                   // live members
	nu      int                   // R107: live + catalog members (n_universe)
}

type mtCache struct {
	mu      sync.Mutex
	at      time.Time
	genres  map[string]*mtGenreNode
	warming bool
	// R107: per-venue market_catalog universe totals (guarded by mu, stamped with the tree)
	uniTotals map[string]storage.CatalogVenueCount
	// series → category map (Kalshi), refreshed ≤ every 12h via SeriesByCategory enumeration
	catMu     sync.Mutex
	seriesCat map[string]string
	catAt     time.Time
	catBusy   bool
	// R107 WARM BOOT: series_categories.json load state (catMu-guarded)
	catFileTried bool      // one synchronous load attempt per boot
	catFileAt    time.Time // when the map currently serving was enumerated (file SavedAt or live refresh)
}

// kalSeriesOf: SERIES = the ticker segment before the first '-' (venue convention, the same parse
// giKalSeries/eventTickerOf ride).
func kalSeriesOf(ticker string) string {
	if i := strings.IndexByte(ticker, '-'); i > 0 {
		return ticker[:i]
	}
	return ticker
}

// ── R107 WARM BOOT: series→category persistence (wx_registry.json pattern) ─────────────────────

const mtSeriesCatFile = "series_categories.json"

// mtSeriesFile is the persisted snapshot: the enumeration result + when it was taken (age-stamp —
// a loaded file older than the 12h window serves immediately AND still triggers the background
// refresh; the boot split just no longer collapses to Other/Unknown while that runs).
type mtSeriesFile struct {
	SavedAt string            `json:"saved_at"`
	Cats    map[string]string `json:"cats"`
}

// mtWriteSeriesFile / mtReadSeriesFile are the pure IO halves (atomic tmp+rename write; loose
// NUL-tolerant read) — pure so the R107 round-trip test runs without a Server or feeds.
func mtWriteSeriesFile(path string, cats map[string]string, at time.Time) error {
	if len(cats) == 0 {
		return fmt.Errorf("refusing to persist an empty series map") // never clobber a good file (wx pattern)
	}
	blob, err := json.MarshalIndent(mtSeriesFile{SavedAt: at.UTC().Format(time.RFC3339), Cats: cats}, "", " ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, blob, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func mtReadSeriesFile(path string) (map[string]string, time.Time, bool) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, time.Time{}, false
	}
	b = bytes.TrimRight(b, "\x00")
	var f mtSeriesFile
	if json.Unmarshal(b, &f) != nil || len(f.Cats) == 0 {
		return nil, time.Time{}, false
	}
	at, _ := time.Parse(time.RFC3339, f.SavedAt)
	return f.Cats, at, true
}

// mtSeriesCategory returns the Kalshi series→category map, kicking a bounded background refresh
// when stale (≤len(kalshiCategoryEnum) public REST calls per 12h — the tree's only REST).
// R107 WARM BOOT: an EMPTY map first seeds synchronously from data/series_categories.json.
func (s *Server) mtSeriesCategory() map[string]string {
	c := &s.mktTree
	c.catMu.Lock()
	if len(c.seriesCat) == 0 && !c.catFileTried {
		c.catFileTried = true
		if cats, at, ok := mtReadSeriesFile(filepath.Join(s.cfg().DataDir, mtSeriesCatFile)); ok {
			c.seriesCat, c.catFileAt = cats, at
			if time.Since(at) < 12*time.Hour {
				c.catAt = at // fresh-enough file: the immediate re-enumeration is skipped entirely
			}
			// stale file: catAt stays zero → the background refresh kicks below while the file serves
		}
	}
	fresh := time.Since(c.catAt) < 12*time.Hour && len(c.seriesCat) > 0
	busy := c.catBusy
	snap := c.seriesCat
	if !fresh && !busy {
		c.catBusy = true
		go s.runGuarded("markettree-cats", func() {
			defer func() {
				c.catMu.Lock()
				c.catBusy = false
				c.catMu.Unlock()
			}()
			if s.kal == nil {
				return
			}
			out := map[string]string{}
			for _, cat := range kalshiCategoryEnum {
				cctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				series, err := s.kal.SeriesByCategory(cctx, cat)
				cancel()
				if err != nil {
					continue // partial map is fine — missing series read as Other/Unknown until next refresh
				}
				for _, sr := range series {
					if sr.Ticker != "" {
						out[strings.ToUpper(sr.Ticker)] = cat
					}
				}
			}
			if len(out) > 0 {
				now := time.Now()
				c.catMu.Lock()
				c.seriesCat, c.catAt = out, now
				c.catFileAt = now
				c.catMu.Unlock()
				// R107 WARM BOOT: persist every successful refresh (atomic tmp+rename; tiny file)
				if err := mtWriteSeriesFile(filepath.Join(s.cfg().DataDir, mtSeriesCatFile), out, now); err != nil {
					s.log.Warn("markettree: series-category persist failed — next boot starts cold again", "err", err)
				}
			}
		})
	}
	c.catMu.Unlock()
	return snap
}

// mtGenreOfSeries resolves a Kalshi series ticker to its genre: the venue category map first
// (persisted/enumerated), gameident's sports series table as fallback (covers sports even before
// the category map warms), else Other/Unknown. (R107: extracted so the boot-split test pins it.)
func (s *Server) mtGenreOfSeries(series string, cats map[string]string) string {
	if cat, ok := cats[strings.ToUpper(series)]; ok {
		return canonGenre(cat)
	}
	if _, _, sports := giKalSeries(series); sports {
		return "Sports"
	}
	return "Other/Unknown"
}

// ── R107: one insertion path for live adds + catalog folds ─────────────────────────────────────

// mtBuilder wraps the genre-node maps so the live caches and the catalog fold share ONE
// insertion path with venue|id dedupe — live rows always win (they insert first).
type mtBuilder struct {
	genres  map[string]*mtGenreNode
	seen    map[string]bool   // "venue|id" → already a member
	evGenre map[string]string // event key ("I|slug") → the genre a LIVE sibling resolved (fold adoption)
}

func newMtBuilder() *mtBuilder {
	return &mtBuilder{genres: map[string]*mtGenreNode{}, seen: map[string]bool{}, evGenre: map[string]string{}}
}

func (b *mtBuilder) node(g string) *mtGenreNode {
	n := b.genres[g]
	if n == nil {
		n = &mtGenreNode{events: map[string]*mtEvent{}, members: map[string][]mtMember{}}
		b.genres[g] = n
	}
	return n
}

// add inserts one member. live=false rows are catalog-only breadth (R107): counted in the
// universe totals (nu/NUniverse), never in the live counts, and never duplicating a live row.
// A catalog fold whose event group a live sibling already placed adopts that sibling's genre
// (the sibling's tag/league IS venue metadata for the whole event).
func (b *mtBuilder) add(genre, evKey, evTitle, venueTag, series string, m mtMember, live bool) bool {
	id := m.Venue + "|" + m.ID
	if b.seen[id] {
		return false
	}
	b.seen[id] = true
	key := venueTag + "|" + evKey
	if live {
		if _, dup := b.evGenre[key]; !dup {
			b.evGenre[key] = genre
		}
	} else if g, hit := b.evGenre[key]; hit {
		genre = g
	}
	n := b.node(genre)
	ev := n.events[key]
	if ev == nil {
		ev = &mtEvent{Key: key, Title: evTitle, Venue: venueTag, Series: series}
		n.events[key] = ev
	}
	ev.NUniverse++
	n.nu++
	if live {
		ev.N++
		n.n++
	}
	n.members[key] = append(n.members[key], m)
	return true
}

// mtFoldCatalogRow folds one market_catalog row the live caches don't carry (R107 FULL
// UNIVERSE). Genre comes from venue series metadata where derivable (kalshi series prefix →
// category map + gameident sports fallback; poly/PUS rows adopt a live sibling's event genre);
// rows whose genre is unknowable are COUNTED under Other/Unknown — never dropped. Catalog-only
// members are title+url shells: no prices (the catalog has no book — laziness kept).
func (s *Server) mtFoldCatalogRow(b *mtBuilder, venue string, r storage.CatalogRow, cats map[string]string) bool {
	if r.Ticker == "" {
		return false
	}
	genre, evKey, venueTag, series, url := "Other/Unknown", r.EventKey, "", "", ""
	switch venue {
	case "kalshi":
		if evKey == "" {
			evKey = eventTickerOf(r.Ticker)
		}
		series = kalSeriesOf(evKey)
		genre = s.mtGenreOfSeries(series, cats)
		venueTag = "K"
		url = kalshiMarketTickerURL(r.Ticker)
	case "polymarket":
		venueTag = "I"
		if evKey == "" {
			evKey = r.Ticker // conditionId-only row: degenerate one-market group, still counted
		} else {
			url = "https://polymarket.com/event/" + evKey // catalog event_key IS the parent event slug
		}
		// genre: the catalog carries no tag for poly-int → Other/Unknown unless a live sibling
		// already placed this event (b.add adopts the sibling's genre)
	case "polyus":
		venueTag = "P"
		if evKey == "" {
			evKey = r.Ticker
		}
		url = "https://polymarket.us/market/" + r.Ticker
		// league lives only in the live snapshot → Other/Unknown (counted), sibling-adopted where live
	default:
		return false
	}
	return b.add(genre, evKey, evKey, venueTag, series,
		mtMember{Venue: venue, ID: r.Ticker, Title: strings.TrimSpace(r.Title), Kind: r.Kind, URL: url}, false)
}

// mtBuild assembles the full tree from the in-memory venue caches + the R107 catalog fold
// (60s cache unchanged).
func (s *Server) mtBuild(ctx context.Context) map[string]*mtGenreNode {
	if ctx == nil {
		ctx = context.Background()
	}
	c := &s.mktTree
	c.mu.Lock()
	if time.Since(c.at) < 60*time.Second && c.genres != nil {
		g := c.genres
		c.mu.Unlock()
		return g
	}
	c.mu.Unlock()
	cats := s.mtSeriesCategory()
	b := newMtBuilder()
	// ── Kalshi: the windowed board cache (the /api/markets source) ───────────────────────────
	if s.kal != nil {
		kmkts, _ := s.kal.TopLiquidCached()
		for _, m := range kmkts {
			if m.Ticker == "" {
				continue
			}
			ev := m.EventTicker
			if ev == "" {
				ev = eventTickerOf(m.Ticker)
			}
			series := kalSeriesOf(ev)
			genre := s.mtGenreOfSeries(series, cats)
			title := strings.TrimSpace(m.Title)
			sub := strings.TrimSpace(m.YesSubTitle)
			mtitle := title
			if sub != "" {
				mtitle += " — " + sub
			}
			b.add(genre, ev, title, "K", series, mtMember{Venue: "kalshi", ID: m.Ticker, Title: mtitle,
				Kind: signalKindOf("kalshi", m.Ticker, title), YesBid: m.YesBid.Float(), YesAsk: m.YesAsk.Float(),
				URL: kalshiMarketTickerURL(m.Ticker)}, true)
		}
	}
	// ── PolyUS: the live snapshot (league = venue metadata; generic rows carry venue category) ─
	for _, m := range s.polyUSSnapshot() {
		if m.Slug == "" {
			continue
		}
		genre := canonGenre(m.League)
		evKey := m.EventID
		if evKey == "" {
			evKey = m.Slug
		}
		title := strings.TrimSpace(m.Game)
		lbl := title
		if q := strings.TrimSpace(m.Question); q != "" && q != title {
			lbl = title + " — " + q
		} else if m.TeamName != "" {
			lbl = title + " — " + m.TeamName
		}
		b.add(genre, evKey, title, "P", "", mtMember{Venue: "polyus", ID: m.Slug, Title: lbl, Kind: m.Kind,
			YesBid: m.Bid, YesAsk: m.Ask, URL: "https://polymarket.us/market/" + m.Slug}, true)
	}
	// ── Poly-int (research venue): event grouping via the parent event slug; tag category ─────
	if s.poly != nil {
		for _, m := range s.poly.CachedMarkets() {
			if m.ConditionID == "" || m.Closed {
				continue
			}
			evKey := m.Slug
			if len(m.Events) > 0 && m.Events[0].Slug != "" {
				evKey = m.Events[0].Slug
			}
			b.add(polyIntGenre(m.Category()), evKey, evKey, "I", "", mtMember{Venue: "polymarket",
				ID: m.ConditionID, Title: strings.TrimSpace(m.Question),
				YesBid: m.BestBid, YesAsk: m.BestAsk, URL: m.EventURL()}, true)
		}
	}
	// ── R107 FULL-UNIVERSE FOLD: catalog rows the live caches don't carry. One indexed pass per
	// venue per rebuild. close_ts semantics are per-venue (see storage/catalogread.go): polyus
	// close_ts is the game START, so its cutoff lags 12h instead of dropping every running game.
	var uni map[string]storage.CatalogVenueCount
	if s.store != nil {
		// Persistent catalog breadth is display-only. During an armed session (or while another
		// analytic reader owns the gate) the live venue caches above are sufficient and return now.
		s.tryRunHeavyResearch(ctx, "market-tree-catalog", 0, func(workCtx context.Context) {
			now := time.Now()
			for _, vf := range []struct {
				venue       string
				closedAfter time.Time
			}{{"kalshi", now}, {"polymarket", now}, {"polyus", now.Add(-12 * time.Hour)}} {
				rows, err := s.store.CatalogUniverse(workCtx, vf.venue, vf.closedAfter)
				if err != nil {
					continue // catalog unavailable → the tree stays live-only (documented degradation)
				}
				for i := range rows {
					s.mtFoldCatalogRow(b, vf.venue, rows[i], cats)
				}
			}
			if u, err := s.store.CatalogCounts(workCtx); err == nil {
				uni = u
			}
		})
	}
	genres := b.genres
	c.mu.Lock()
	c.genres, c.at = genres, time.Now()
	if uni != nil {
		c.uniTotals = uni
	}
	c.mu.Unlock()
	// R107: the xvident registry rides the same snapshots on the same cadence (self-throttled to
	// 5 min inside; the 5-min catalog sweep kicks it too, so tree traffic isn't load-bearing).
	go s.runGuarded("xvident-rebuild", func() { s.xvRebuildCoordinated(context.Background()) })
	return genres
}

// handleMarketTree — GET /api/markettree[?genre=X[&event=K|key]]: the lazy 3-level universal tree.
func (s *Server) handleMarketTree(w http.ResponseWriter, r *http.Request) {
	genres := s.mtBuild(r.Context())
	q := r.URL.Query()
	gname := strings.TrimSpace(q.Get("genre"))
	if gname == "" { // level 1: genre list (canonical first, auto-created after, Other/Unknown last)
		type grow struct {
			Genre     string `json:"genre"`
			Events    int    `json:"events"`
			Markets   int    `json:"markets"`
			NUniverse int    `json:"n_universe"` // R107: live + catalog-folded members
		}
		out := make([]grow, 0, len(genres))
		total, unknown := 0, 0
		totalUni, unknownUni := 0, 0
		for g, n := range genres {
			out = append(out, grow{Genre: g, Events: len(n.events), Markets: n.n, NUniverse: n.nu})
			total += n.n
			totalUni += n.nu
			if g == "Other/Unknown" {
				unknown = n.n
				unknownUni = n.nu
			}
		}
		rank := map[string]int{"Sports": 0, "Crypto": 1, "Weather": 2, "Politics": 3, "Economics": 4, "Entertainment": 5}
		sort.Slice(out, func(i, j int) bool {
			ri, iok := rank[out[i].Genre]
			rj, jok := rank[out[j].Genre]
			switch {
			case iok && jok:
				return ri < rj
			case iok:
				return true
			case jok:
				return false
			case out[i].Genre == "Other/Unknown":
				return false
			case out[j].Genre == "Other/Unknown":
				return true
			}
			return out[i].Genre < out[j].Genre
		})
		unkPct := 0.0
		if total > 0 {
			unkPct = math.Round(10000*float64(unknown)/float64(total)) / 100
		}
		unkPctUni := 0.0
		if totalUni > 0 {
			unkPctUni = math.Round(10000*float64(unknownUni)/float64(totalUni)) / 100
		}
		c := &s.mktTree
		c.mu.Lock()
		uni := make(map[string]storage.CatalogVenueCount, len(c.uniTotals))
		for k, v := range c.uniTotals {
			uni[k] = v
		}
		c.mu.Unlock()
		c.catMu.Lock()
		catAge := ""
		if !c.catFileAt.IsZero() {
			catAge = time.Since(c.catFileAt).Round(time.Minute).String()
		}
		c.catMu.Unlock()
		writeJSON(w, http.StatusOK, map[string]any{
			"genres": out, "total_markets": total, "unknown_pct": unkPct,
			"total_universe": totalUni, "unknown_pct_universe": unkPctUni, // R107
			"universe_totals": uni,    // R107: per-venue market_catalog {total, open}
			"series_map_age":  catAge, // R107: age of the series→category map (warm-boot visibility)
			"note":            "genres from venue series/category metadata only (no title guessing); Sports additionally renders the anchored R102 game tree; n_universe folds the market_catalog board (R107)",
		})
		return
	}
	n := genres[gname]
	if n == nil {
		writeJSON(w, http.StatusOK, map[string]any{"events": []any{}, "note": fmt.Sprintf("no live markets under %q right now", gname)})
		return
	}
	evKey := strings.TrimSpace(q.Get("event"))
	if evKey == "" { // level 2: event groups
		evs := make([]mtEvent, 0, len(n.events))
		for _, e := range n.events {
			evs = append(evs, *e)
		}
		sort.Slice(evs, func(i, j int) bool {
			if evs[i].N != evs[j].N {
				return evs[i].N > evs[j].N // live-rich groups first; catalog-only (N=0) tail
			}
			if evs[i].NUniverse != evs[j].NUniverse {
				return evs[i].NUniverse > evs[j].NUniverse
			}
			return evs[i].Title < evs[j].Title
		})
		if len(evs) > 400 {
			evs = evs[:400]
		}
		writeJSON(w, http.StatusOK, map[string]any{"genre": gname, "events": evs})
		return
	}
	mem := n.members[evKey] // level 3: member markets with YES/NO
	sort.Slice(mem, func(i, j int) bool { return mem[i].Title < mem[j].Title })
	if len(mem) > 120 {
		mem = mem[:120]
	}
	writeJSON(w, http.StatusOK, map[string]any{"genre": gname, "event": evKey, "markets": mem})
}
