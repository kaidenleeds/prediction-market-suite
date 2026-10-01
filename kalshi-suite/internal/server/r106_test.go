package server

// R106 tests — auditor bug 255 (PUS two-sided single instruments, live Jul-9 with the WC quarter-
// finals) + the R106 paper-maker live-parity cancel rule.
//
// The aadc fixture mirrors the LIVE venue response for
// GET /v1/market/slug/aadc-fwc-fra-mar-2026-07-09-to-advance (probed 2026-07-07): one slug, two
// marketSides (France long/home, Morocco short/away), both sides' identifier == the market slug,
// sportsMarketTypeV2 still MONEYLINE, sportsMarketType "soccer_game_to_advance".

import (
	"context"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/config"
	"github.com/kalshi-suite/kalshi-suite/internal/polymarketus"
)

const r106AdvSlug = "aadc-fwc-fra-mar-2026-07-09-to-advance"

// r106AdvJSON — trimmed live venue shape (fields our decoder reads).
const r106AdvJSON = `{
  "id":"188395",
  "question":"Which team will advance from France vs Morocco on 2026-07-09 4:00PM ET?",
  "slug":"` + r106AdvSlug + `",
  "category":"sports",
  "marketType":"moneyline",
  "sportsMarketType":"soccer_game_to_advance",
  "sportsMarketTypeV2":"SPORTS_MARKET_TYPE_MONEYLINE",
  "gameStartTime":"2026-07-09T20:00:00Z",
  "marketSides":[
    {"identifier":"` + r106AdvSlug + `","description":"France","price":"0.7800","long":true,
     "team":{"name":"France","abbreviation":"fra","league":"fwc","ordering":"home","displayAbbreviation":"FRA"}},
    {"identifier":"` + r106AdvSlug + `","description":"Morocco","price":"0.23","long":false,
     "team":{"name":"Morocco","abbreviation":"mar","league":"fwc","ordering":"away","displayAbbreviation":"MAR"}}
  ]
}`

func r106AdvMarket(t *testing.T) polymarketus.Market {
	t.Helper()
	var m polymarketus.Market
	if err := json.Unmarshal([]byte(r106AdvJSON), &m); err != nil {
		t.Fatalf("fixture: %v", err)
	}
	return m
}

// TestR106ToAdvanceVenueShape pins the venue-metadata detectors against the live response shape.
func TestR106ToAdvanceVenueShape(t *testing.T) {
	m := r106AdvMarket(t)
	if !m.IsToAdvance() {
		t.Fatal("soccer_game_to_advance must classify IsToAdvance")
	}
	if !m.TwoSidedSingle() {
		t.Fatal("both sides carry the market's own slug as identifier — must be TwoSidedSingle")
	}
	if lt := m.LongTeam(); lt.Name != "France" {
		t.Fatalf("LongTeam = %q, want France", lt.Name)
	}
	if sh := m.ShortTeam(); sh.Name != "Morocco" || sh.Abbreviation != "mar" {
		t.Fatalf("ShortTeam = %+v, want Morocco/mar", sh)
	}
	if pusKindOf(m) != "advance" {
		t.Fatalf("pusKindOf = %q, want advance (V2 says MONEYLINE — the v1 string must win)", pusKindOf(m))
	}
	// LEGACY regression: a per-team moneyline (old shape: side identifiers are their own slugs)
	// must NOT classify two-sided.
	legacy := polymarketus.Market{Slug: "atc-mlb-chc-cws-2026-07-09-chc", SportsTypeV2: "SPORTS_MARKET_TYPE_MONEYLINE",
		Sides: []polymarketus.MarketSide{
			{Identifier: "atc-mlb-chc-cws-2026-07-09-chc", Long: true, Team: polymarketus.Team{Name: "Chicago Cubs", Abbreviation: "chc"}},
			{Identifier: "atc-mlb-chc-cws-2026-07-09-cws", Long: false, Team: polymarketus.Team{Name: "Chicago White Sox", Abbreviation: "cws"}},
		}}
	if legacy.TwoSidedSingle() {
		t.Fatal("legacy per-team slugs must not classify TwoSidedSingle")
	}
	if legacy.IsToAdvance() {
		t.Fatal("legacy moneyline must not classify IsToAdvance")
	}
}

// TestR106AdvanceTypeClasses pins the type-guard vocabulary: advance is its OWN class on both
// venues (never twins a regulation-time winner) and the Kalshi ADVANCE series parses.
func TestR106AdvanceTypeClasses(t *testing.T) {
	if lg, mt, ok := giKalSeries("KXWCADVANCE"); !ok || lg != "fwc" || mt != "advance" {
		t.Fatalf("giKalSeries(KXWCADVANCE) = (%q,%q,%v), want (fwc,advance,true)", lg, mt, ok)
	}
	if c := matchTypeOf("Which team will advance from France vs Morocco on 2026-07-09 4:00PM ET?", ""); c != "advance" {
		t.Fatalf("matchTypeOf(venue question) = %q, want advance", c)
	}
	if c := matchTypeOf("France vs Morocco: To Advance", ""); c != "advance" {
		t.Fatalf("matchTypeOf(kalshi title) = %q, want advance", c)
	}
	if c := matchTypeOfMkt(polyUSMarket{Kind: "advance"}); c != "advance" {
		t.Fatalf("matchTypeOfMkt(kind=advance) = %q, want advance", c)
	}
	// the guard split: an advance question must not classify as winner (and vice versa)
	if c := matchTypeOf("Will France beat Morocco?", ""); c != "winner" {
		t.Fatalf("plain winner question = %q, want winner", c)
	}
}

// r106AnchorAdvance anchors the FRA-MAR QF on both venues: PUS single-slug two-sided instrument +
// Kalshi per-team KXWCADVANCE markets (venue pair order FRAMAR = nominal home-first — exercises the
// R106 swapped-key game join).
func r106AnchorAdvance(t *testing.T, s *Server) {
	t.Helper()
	adv := r106AdvMarket(t)
	ev := polymarketus.Event{ID: "ev-framar", Slug: "fwc-fra-mar-2026-07-09", GameID: 4242,
		StartTime: "2026-07-09T20:00:00Z", Markets: []polymarketus.Market{adv}}
	s.giAnchorPUSEvent("fwc", ev)
	for _, tk := range []string{"FRA", "MAR"} {
		m := kalMkt(t, `{"ticker":"KXWCADVANCE-26JUL09FRAMAR-`+tk+`","event_ticker":"KXWCADVANCE-26JUL09FRAMAR","title":"France vs Morocco: To Advance","yes_sub_title":"`+map[string]string{"FRA": "France advances", "MAR": "Morocco advances"}[tk]+`"}`)
		if !s.giAnchorKalshi(m) {
			t.Fatalf("kalshi advance anchor failed for %s", tk)
		}
	}
}

// TestR106AdvanceAnchorTwinsAndFlip — the structural heart of bug 255: one game node, the PUS
// instrument typed "advance" with yes/no teams, and BOTH Kalshi per-team advance markets twinning
// the ONE PUS slug (short side = same slug flipped).
func TestR106AdvanceAnchorTwinsAndFlip(t *testing.T) {
	s := &Server{}
	r106AnchorAdvance(t, s)
	st := s.gi()
	s.giMu.Lock()
	games := len(st.games)
	ref := st.byMkt["polyus|"+r106AdvSlug]
	s.giMu.Unlock()
	if games != 1 {
		t.Fatalf("games = %d, want 1 (venue pair-order disagreement must not split the game)", games)
	}
	if ref == nil || ref.mktType != "advance" || ref.yesTeam != "FRA" || ref.noTeam != "MAR" {
		t.Fatalf("PUS ref = %+v, want advance yes=FRA no=MAR", ref)
	}
	// Kalshi FRA advance ↔ PUS long side (same side)
	if slug, sameSide, ok := s.giPUSTwin("KXWCADVANCE-26JUL09FRAMAR-FRA"); !ok || slug != r106AdvSlug || !sameSide {
		t.Fatalf("FRA twin = (%q,%v,%v), want %q same-side", slug, sameSide, ok, r106AdvSlug)
	}
	// Kalshi MAR advance ↔ the SAME PUS slug, flipped (kal YES == pus NO)
	if slug, sameSide, ok := s.giPUSTwin("KXWCADVANCE-26JUL09FRAMAR-MAR"); !ok || slug != r106AdvSlug || sameSide {
		t.Fatalf("MAR twin = (%q,%v,%v), want %q FLIPPED", slug, sameSide, ok, r106AdvSlug)
	}
	// Reverse direction: the PUS instrument's Kalshi twin is the same-side FRA market
	if tk, sameSide, ok := s.giKalshiTwin(r106AdvSlug); !ok || tk != "KXWCADVANCE-26JUL09FRAMAR-FRA" || !sameSide {
		t.Fatalf("kalshi twin = (%q,%v,%v), want FRA same-side", tk, sameSide, ok)
	}
	// Side lookup: France = the slug's YES; Morocco = the SAME slug flipped
	gid, ok := s.giGameOfRef(r106AdvSlug)
	if !ok {
		t.Fatal("game ref lookup failed")
	}
	if slug, flip, ok := s.giPUSSideMarket(gid, "France"); !ok || slug != r106AdvSlug || flip {
		t.Fatalf("side France = (%q,flip=%v,%v), want %q flip=false", slug, flip, ok, r106AdvSlug)
	}
	if slug, flip, ok := s.giPUSSideMarket(gid, "Morocco"); !ok || slug != r106AdvSlug || !flip {
		t.Fatalf("side Morocco = (%q,flip=%v,%v), want %q flip=TRUE", slug, flip, ok, r106AdvSlug)
	}
	// A KXWCGAME (regulation-time 3-way, has TIE) must NOT twin the advance instrument.
	km := kalMkt(t, `{"ticker":"KXWCGAME-26JUL09FRAMAR-FRA","event_ticker":"KXWCGAME-26JUL09FRAMAR","title":"France vs Morocco Winner?","yes_sub_title":"Reg Time: France"}`)
	if !s.giAnchorKalshi(km) {
		t.Fatal("KXWCGAME anchor failed")
	}
	if slug, _, ok := s.giPUSTwin("KXWCGAME-26JUL09FRAMAR-FRA"); ok {
		t.Fatalf("regulation-time winner must NOT twin the advance instrument, got %q", slug)
	}
}

// TestR106MatchForSideTwoSidedFlip — the shared matcher serves BOTH teams off the one instrument:
// long side plain, short side with flip=true and the flipped (1−YES) price. Callers that express
// bets (pmatch/rbridge) write side NO on flipped matches — pinned here via the flip flag.
func TestR106MatchForSideTwoSidedFlip(t *testing.T) {
	s := &Server{}
	r106AnchorAdvance(t, s)
	s.polyUSMkts = []polyUSMarket{{
		League: "fwc", EventID: "ev-framar", Game: "France vs Morocco",
		Team: "FRA", TeamName: "France", ShortTeam: "mar", ShortTeamName: "Morocco", TwoSided: true,
		Slug: r106AdvSlug, Kind: "advance", Question: "To Advance",
		Yes: 0.775, Bid: 0.77, Ask: 0.78, Start: "2026-07-09T20:00:00Z",
	}}
	q := "Which team will advance from France vs Morocco on 2026-07-09 4:00PM ET?"
	slug, px, flip, ok := s.polyUSMatchForSide("arb", q, "France", r106AdvSlug)
	if !ok || slug != r106AdvSlug || flip || px != 0.775 {
		t.Fatalf("France = (%q,%v,flip=%v,%v), want %q@0.775 flip=false", slug, px, flip, ok, r106AdvSlug)
	}
	slug, px, flip, ok = s.polyUSMatchForSide("arb", q, "Morocco", r106AdvSlug)
	if !ok || slug != r106AdvSlug || !flip {
		t.Fatalf("Morocco = (%q,%v,flip=%v,%v), want %q flipped", slug, px, flip, ok, r106AdvSlug)
	}
	if diff := px - 0.225; diff > 1e-9 || diff < -1e-9 {
		t.Fatalf("Morocco price = %v, want 1−0.775 = 0.225", px)
	}
}

// TestR106WeatherBookRules pins the Weather book (operator ask): tunable 20–40¢ default band,
// one lot per market, the cross-book hedge guard BOTH ways (weather-hedge), the $300 seed at the
// operator's $1,300 split, stop/settle/reset semantics (rawflow pattern), and the book-realized
// edge hook (no ML, no other family's stats — cold start sizes at base).
func TestR106WeatherBookRules(t *testing.T) {
	s := testServer(t)
	r107kal(s)
	ctx := context.Background()
	// SELF-HEALING SEED (found live during the R106 deploy): the book gets touched (NAV sweep)
	// BEFORE the allocation exists ⇒ a Bank-0 book is minted and cached...
	s.wxBookMu.Lock()
	if b := s.wxBookLoadLocked(); b.Bank != 0 {
		t.Fatalf("pre-alloc touch should mint bank 0 (alloc_weather unset), got %v", b.Bank)
	}
	s.wxBookMu.Unlock()
	s.mutateCfg(func(c *config.Config) {
		c.Auto.PaperTotalStart = 1300
		c.Auto.AllocKalshi, c.Auto.AllocPolyus, c.Auto.AllocML = 350.0/1300, 100.0/1300, 350.0/1300
		c.Auto.AllocRawFlow, c.Auto.AllocWeather = 200.0/1300, 300.0/1300
	})
	if !s.weatherBookEnabled() {
		t.Fatal("alloc_weather>0 must enable the book")
	}
	if seed := s.weatherSeedBank(); math.Abs(seed-300) > 1e-6 {
		t.Fatalf("seed bank = %v, want 300 (alloc_weather × 1300)", seed)
	}
	// …and the NEXT access after the allocation lands must re-seed it — no restart, no reset.
	s.wxBookMu.Lock()
	if b := s.wxBookLoadLocked(); math.Abs(b.Bank-300) > 1e-6 {
		s.wxBookMu.Unlock()
		t.Fatalf("bank after enable = %v, want self-healed 300", b.Bank)
	}
	s.wxBookMu.Unlock()
	if lo, hi := s.weatherBand(); lo != 0.20 || hi != 0.40 {
		t.Fatalf("default band = %v–%v, want 0.20–0.40", lo, hi)
	}
	openN := func() int {
		s.wxBookMu.Lock()
		defer s.wxBookMu.Unlock()
		return len(s.wxBookLoadLocked().Open)
	}
	// R132 safety freeze: the production choke point never opens a new lot. The old mechanics
	// remain below as an explicit unfrozen regression helper so settlement/state coverage survives.
	if !weatherNewPlacementsFrozen || weatherPlacementFreezeReason == "" {
		t.Fatal("weather placement freeze and its operator-visible reason must stay pinned")
	}
	s.weatherBookPlace(ctx, "KXHIGHFREEZE-X", "freeze probe", "YES", 0.30, 1)
	if openN() != 0 {
		t.Fatalf("R132 weather freeze must refuse every new lot (open=%d)", openN())
	}
	// Band: 45¢ and 15¢ refuse; 30¢ places.
	s.weatherBookPlaceUnfrozen(ctx, "KXHIGHNY-X", "NY high (85-86)", "YES", 0.45, 1)
	s.weatherBookPlaceUnfrozen(ctx, "KXHIGHNY-X", "NY high (85-86)", "YES", 0.15, 1)
	if openN() != 0 {
		t.Fatalf("out-of-band prices must not place (open=%d)", openN())
	}
	s.weatherBookPlaceUnfrozen(ctx, "KXHIGHNY-X", "NY high (85-86)", "YES", 0.30, 1)
	if openN() != 1 {
		t.Fatalf("30¢ in-band must place (open=%d)", openN())
	}
	// Config-tunable band: widen to 10–60 and a 45¢ entry places.
	s.mutateCfg(func(c *config.Config) { c.Auto.WeatherBandLoC, c.Auto.WeatherBandHiC = 10, 60 })
	s.weatherBookPlaceUnfrozen(ctx, "KXHIGHCHI-X", "CHI high (90-91)", "YES", 0.45, 1)
	if openN() != 2 {
		t.Fatalf("tuned band must accept 45¢ (open=%d)", openN())
	}
	s.mutateCfg(func(c *config.Config) { c.Auto.WeatherBandLoC, c.Auto.WeatherBandHiC = 0, 0 })
	// One lot per MARKET — same ticker, either side, refuses.
	s.weatherBookPlaceUnfrozen(ctx, "KXHIGHNY-X", "NY high (85-86)", "NO", 0.30, 1)
	s.weatherBookPlaceUnfrozen(ctx, "KXHIGHNY-X", "NY high (85-86)", "YES", 0.25, 1)
	if openN() != 2 {
		t.Fatalf("one-lot-per-market violated (open=%d)", openN())
	}
	// Hedge guard INBOUND: ML book holds KXHIGHLA-X YES → Weather NO refuses, YES allowed.
	mlBook := map[string]any{"open": []map[string]any{{"platform": "kalshi", "ticker": "KXHIGHLA-X", "side": "YES"}}}
	bts, _ := json.Marshal(mlBook)
	if err := os.WriteFile(filepath.Join(s.cfg().DataDir, "ml_paper.json"), bts, 0o644); err != nil {
		t.Fatal(err)
	}
	s.mlOpenMu.Lock()
	s.mlOpenSides = nil // bust the 30s cache
	s.mlOpenMu.Unlock()
	s.weatherBookPlaceUnfrozen(ctx, "KXHIGHLA-X", "LA high", "NO", 0.30, 1)
	if openN() != 2 {
		t.Fatalf("ML-hedge block violated (open=%d)", openN())
	}
	s.weatherBookPlaceUnfrozen(ctx, "KXHIGHLA-X", "LA high", "YES", 0.30, 1)
	if openN() != 3 {
		t.Fatalf("same-side-as-ML must be allowed (open=%d)", openN())
	}
	// Hedge guard OUTBOUND (symmetric): an auto-* source must refuse the OPPOSITE side of an
	// open Weather lot, and betConflictReason names the rule.
	if r := s.betConflictReason("kalshi", "KXHIGHNY-X", "NO", "auto-cons-kalshi", nil); r != "weather-hedge" {
		t.Fatalf("outbound weather-hedge = %q, want weather-hedge", r)
	}
	if r := s.betConflictReason("kalshi", "KXHIGHNY-X", "YES", "auto-cons-kalshi", nil); r != "" {
		t.Fatalf("same side must pass the weather guard, got %q", r)
	}
	// Stop-loss: freeze a stop, mark through it, settle pass closes it (reason stop-loss).
	s.wxBookMu.Lock()
	wb := s.wxBookLoadLocked()
	for i := range wb.Open {
		if wb.Open[i].Ticker == "KXHIGHNY-X" {
			wb.Open[i].SL = 0.24
		}
	}
	s.wxBookMu.Unlock()
	s.topMarksMu.Lock()
	if s.topMarks == nil {
		s.topMarks = map[string]float64{}
	}
	s.topMarks["KXHIGHNY-X"] = 0.20
	s.topMarksMu.Unlock()
	s.settleWeatherBook(ctx)
	if openN() != 2 {
		t.Fatalf("stop-loss must close the marked-through lot (open=%d)", openN())
	}
	s.wxBookMu.Lock()
	closedN := len(s.wxBookLoadLocked().Closed)
	reason := ""
	if closedN > 0 {
		reason = s.wxBookLoadLocked().Closed[closedN-1].Reason
	}
	s.wxBookMu.Unlock()
	if closedN != 1 || reason != "stop-loss" {
		t.Fatalf("stop close: n=%d reason=%q — want 1 lot, stop-loss", closedN, reason)
	}
	// Book-realized edge: below n=5 closes it must report no evidence (engine sizes at base).
	if _, ok := s.wxBookRealizedEdge(); ok {
		t.Fatal("book-realized edge must need n≥5 settled lots")
	}
	// Reset starts ONLY this book flat: bases stamp, bank re-seeds to $300, sparkline restarts,
	// and old opens move to the reset archive (nothing else is touched by this call).
	s.resetWeatherBook(ctx)
	s.wxBookMu.Lock()
	wb = s.wxBookLoadLocked()
	if wb.NetBase != wb.Net || math.Abs(wb.Bank-300) > 1e-6 || len(wb.Equity) != 0 || len(wb.Open) != 0 {
		t.Fatalf("reset: netBase=%v net=%v bank=%v equity=%d open=%d — want stamped/300/0/0", wb.NetBase, wb.Net, wb.Bank, len(wb.Equity), len(wb.Open))
	}
	s.wxBookMu.Unlock()
	if got := s.weatherNetSinceEpoch(); got != 0 {
		t.Fatalf("post-reset epoch net = %v, want 0", got)
	}
}

// TestR106WouldQuoteCompute pins the RFQ would-quote pricing (edge 36, log-only): side-adjusted
// leg mids → product → correlation haircut → two-sided bids at margin, confidence tags, the
// blind-leg refusal, and the USD size cap.
func TestR106WouldQuoteCompute(t *testing.T) {
	legs := []rfqLegSnap{
		{Mkt: "A", Side: "yes", Bid: 0.50, Ask: 0.52, Src: "bookws"}, // mid .51
		{Mkt: "B", Side: "no", Bid: 0.30, Ask: 0.32, Src: "bookws"},  // yes-mid .31 → side-adjusted .69
	}
	q, ok := computeWouldQuote(legs, 100, 0, 0, 0, false) // all defaults: 3¢ floor / 1.5¢·2 / $5
	if !ok {
		t.Fatal("two clean legs must price")
	}
	wantProd := 0.51 * 0.69
	if math.Abs(q.Prod-math.Round(wantProd*10000)/10000) > 1e-9 {
		t.Fatalf("prod = %v, want %v", q.Prod, wantProd)
	}
	wantFair := wantProd * 0.97
	if math.Abs(q.Fair-math.Round(wantFair*10000)/10000) > 1e-9 {
		t.Fatalf("fair = %v, want %v (one 0.97 haircut for 2 legs)", q.Fair, wantFair)
	}
	if q.MarginC != 3.0 { // max(3¢ floor, 1.5¢×2)
		t.Fatalf("margin = %v¢, want 3", q.MarginC)
	}
	if q.Conf != "high" {
		t.Fatalf("conf = %q, want high (all legs bookws)", q.Conf)
	}
	if math.Abs(q.YesBid-(math.Round((wantFair-0.03)*1000)/1000)) > 1e-9 {
		t.Fatalf("yes bid = %v", q.YesBid)
	}
	if math.Abs(q.NoBid-(math.Round((1-wantFair-0.03)*1000)/1000)) > 1e-9 {
		t.Fatalf("no bid = %v", q.NoBid)
	}
	// USD size cap: 100 contracts at the pricier side would cost ≫ $5 — capped down.
	if px := math.Max(q.YesBid, q.NoBid); q.Size != math.Floor(5/px) {
		t.Fatalf("size = %v, want floor(5/%v)", q.Size, px)
	}
	// meta-sourced leg degrades confidence; flush-time degrades further.
	legs[1].Src = "meta"
	if q2, ok2 := computeWouldQuote(legs, 10, 0, 0, 0, false); !ok2 || q2.Conf != "med" {
		t.Fatalf("meta leg conf = %q, want med", q2.Conf)
	}
	if q3, ok3 := computeWouldQuote(legs, 10, 0, 0, 0, true); !ok3 || q3.Conf != "low" {
		t.Fatalf("flush conf = %q, want low", q3.Conf)
	}
	// a blind leg refuses — never invent a quote.
	legs[1].Bid, legs[1].Ask = 0, 0
	if _, ok4 := computeWouldQuote(legs, 10, 0, 0, 0, false); ok4 {
		t.Fatal("blind leg must refuse to quote")
	}
}

// TestR106WQNoteInitAndCount — regression pin for the R106 soak crash: wqNote's ByConf map was
// nil on the very first quote (flush-path panic recovered; the arrival-path twin killed the WS
// read goroutine and the process). The FIRST wqNote on a fresh Server must not panic and must
// count correctly; a re-quote of the same combo replaces the ledger entry (newest stance wins).
func TestR106WQNoteInitAndCount(t *testing.T) {
	s := &Server{}
	row := &rfqWouldRow{RFQID: "r1", Ticker: "KXMVE-TEST", At: "2026-07-07T15:00:00Z"}
	q := wqQuote{YesBid: 0.30, NoBid: 0.65, Size: 5, Conf: "high"}
	s.wqNote(row, q) // must not panic (nil ByConf + nil wqOpen)
	s.wqNote(row, wqQuote{YesBid: 0.31, NoBid: 0.64, Size: 5, Conf: "low"})
	s.wqMu.Lock()
	defer s.wqMu.Unlock()
	if s.wqStats.Quoted != 2 || s.wqStats.ByConf["high"] != 1 || s.wqStats.ByConf["low"] != 1 {
		t.Fatalf("counts = %+v, want quoted 2, high 1, low 1", s.wqStats)
	}
	if len(s.wqOpen) != 1 || s.wqOpen["KXMVE-TEST"].YesBid != 0.31 {
		t.Fatalf("ledger = %+v, want ONE entry at the newest quote", s.wqOpen)
	}
}

// TestR106MakerSimLiveParity pins the paper maker sim's cancellation doctrine to the LIVE layers.
// R132: scalar mark/midpoint crossings can never fill a maker order; executable aggressor tape and
// queue consumption are tested separately. This helper owns only moved-1tick, close-3min, and timeout.
func TestR106MakerSimLiveParity(t *testing.T) {
	min10 := 10*time.Minute + time.Second
	cases := []struct {
		name              string
		cur               float64
		curOK             bool
		px                float64
		tick              float64
		age               time.Duration
		closeIn           time.Duration
		closeKnown        bool
		wantAct, wantRule string
	}{
		{"lower one-tick mark is a cancel, never a fill receipt", 0.44, true, 0.45, 0.01, time.Minute, 0, false, "cancel", "moved-1tick"},
		{"cancel on upper one-tick move", 0.46, true, 0.45, 0.01, time.Minute, 0, false, "cancel", "moved-1tick"},
		{"REGRESSION: sub-cent venue tick cancels", 0.451, true, 0.45, 0.001, time.Minute, 0, false, "cancel", "moved-1tick"},
		{"rest inside the venue-tick band", 0.45049, true, 0.45, 0.001, time.Minute, 0, false, "", ""},
		{"touch AT our price = queue-position fantasy, keep resting", 0.45, true, 0.45, 0.001, time.Minute, 0, false, "", ""},
		{"invalid tick fails safe to 1c", 0.455, true, 0.45, 0, time.Minute, 0, false, "", ""},
		{"close-3min pull (live R24b, was live-only)", 0.45049, true, 0.45, 0.001, time.Minute, 2 * time.Minute, true, "cancel", "close-3min"},
		{"10-min bound", 0.45049, true, 0.45, 0.001, min10, time.Hour, true, "cancel", "expire-10m"},
		{"10-min bound with no observable price", 0, false, 0.45, 0.001, min10, 0, false, "cancel", "expire-10m"},
		{"close pull still applies inside the tick band", 0.44951, true, 0.45, 0.001, time.Minute, time.Minute, true, "cancel", "close-3min"},
		{"no observable price keeps resting", 0, false, 0.45, 0.001, time.Minute, 0, false, "", ""},
	}
	for _, c := range cases {
		act, rule := makerSimDecision(c.cur, c.curOK, c.px, c.tick, c.age, c.closeIn, c.closeKnown)
		if act != c.wantAct || rule != c.wantRule {
			t.Fatalf("%s: got (%q,%q), want (%q,%q)", c.name, act, rule, c.wantAct, c.wantRule)
		}
	}
}
