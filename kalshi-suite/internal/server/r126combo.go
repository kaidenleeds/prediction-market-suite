package server

// r126combo.go — R126 Part 3: LOG-ONLY combined-market (MVE) mispricing sampler.
//
// The correlation-arb thesis: Kalshi combined markets on correlated SAME-GAME legs (moneyline +
// spread, moneyline + total, spread + total) should trade AWAY from the naive leg product — the
// legs co-move — while cross-game pairs should sit near the product. This endpoint MEASURES that:
// it samples live same-game pairs (and cross-game controls), computes naive product vs the parlay
// lab's correlation-adjusted joint (plabJointPEx, the R109 pair-rho estimator), then asks the
// venue for the actual combined market (lookup-only; optional create probe — a LISTING, not an
// order) and records its live quote. If quotes track the adjusted joint rather than the product,
// the thesis is real; if they track the product, the correlation edge is harvestable.
//
// LOG-ONLY, hard-budgeted: no orders, no RFQs, never CreateMVEMarket (the write-gated path).
// Venue calls per invocation: `lookups` (PUT lookup + re-lookup + GET market reads, default 40,
// max 60) and `create` (POST create-market-in-collection probes, default 0, max 20 — ALSO gated
// by the suite's persisted weekly cprobe create budget, comboprobe.go). GET /api/r126combo.

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
)

const (
	r126DefCorr, r126MaxCorr       = 20, 40
	r126DefIndep, r126MaxIndep     = 10, 20
	r126DefLookups, r126MaxLookups = 40, 60
	r126MaxCreates                 = 20
	r126MaxLegSpreadC              = 0.15 // legs need a live two-sided BBO with spread ≤ 15¢
	r126ColsPerPair                = 2    // collections asked per pair (each ask spends budget)
)

// r126Params — the clamped request parameters (echoed in the response as params_used).
type r126Params struct {
	NCorr   int `json:"n_corr"`
	NIndep  int `json:"n_indep"`
	Lookups int `json:"lookups"`
	Creates int `json:"create"`
}

// r126ClampParams applies defaults and hard caps (pure — pinned in r126combo_test.go).
// n_corr/n_indep/lookups: ≤0 ⇒ default, capped at max. create: default 0 (no creation probes),
// negative ⇒ 0, capped at 20.
func r126ClampParams(nCorr, nIndep, lookups, creates int) r126Params {
	def := func(v, d, max int) int {
		if v <= 0 {
			v = d
		}
		if v > max {
			v = max
		}
		return v
	}
	if creates < 0 {
		creates = 0
	}
	if creates > r126MaxCreates {
		creates = r126MaxCreates
	}
	return r126Params{
		NCorr:   def(nCorr, r126DefCorr, r126MaxCorr),
		NIndep:  def(nIndep, r126DefIndep, r126MaxIndep),
		Lookups: def(lookups, r126DefLookups, r126MaxLookups),
		Creates: creates,
	}
}

// r126Classify buckets a Kalshi sports ticker by its series suffix (live naming, verified on the
// tape cache: KXMLBGAME/KXWNBAGAME = moneyline, KX<SPORT>SPREAD = spread, KX<SPORT>TOTAL =
// total). sport is the series with the class suffix stripped, so KXMLBGAME / KXMLBSPREAD /
// KXMLBTOTAL all report sport "KXMLB" and same-game class pairs group per sport. Anything else
// (crypto, weather, props, indices) is not a class market → ok=false.
func r126Classify(ticker string) (class, sport string, ok bool) {
	i := strings.Index(ticker, "-")
	if i <= 0 {
		return "", "", false
	}
	ser := strings.ToUpper(ticker[:i])
	switch {
	case strings.HasSuffix(ser, "SPREAD"):
		return "spread", strings.TrimSuffix(ser, "SPREAD"), true
	case strings.HasSuffix(ser, "TOTAL"):
		return "total", strings.TrimSuffix(ser, "TOTAL"), true
	case strings.HasSuffix(ser, "GAME"):
		return "ml", strings.TrimSuffix(ser, "GAME"), true
	}
	return "", "", false
}

// r126PairClass canonicalizes a class pair: ("ml","total") → "ML+TOTAL" (sorted, so leg order
// never changes the label).
func r126PairClass(c1, c2 string) string {
	a, b := strings.ToUpper(c1), strings.ToUpper(c2)
	if a > b {
		a, b = b, a
	}
	return a + "+" + b
}

// r126Mkt is one live leg candidate.
type r126Mkt struct {
	Ticker  string
	Event   string // venue event ticker (collection membership key)
	GameKey string // legEventKey "kgame:<middle>" — same game ⇔ same key
	Class   string // ml | spread | total
	Sport   string // series base, e.g. KXMLB
	Bid     float64
	Ask     float64
	Src     string // bookws | meta (rfqLegBBO source)
}

func (m r126Mkt) mid() float64 { return (m.Bid + m.Ask) / 2 }

// r126Pair is one sampled measurement pair.
type r126Pair struct {
	Kind    string // corr | indep
	Class   string // corr: "ML+SPREAD" | "ML+TOTAL" | "SPREAD+TOTAL"; indep: "INDEP" (control)
	GameKey string // corr: the shared game key; indep: "keyA|keyB"
	A, B    r126Mkt
}

// r126SamplePairs (pure — pinned in tests): correlated SAME-GAME pairs spread evenly across the
// three class pairs (round-robin) and across sports (sport-interleaved within each class pair);
// independent CROSS-GAME control pairs, sport-interleaved so consecutive picks prefer DIFFERENT
// sports. Deterministic for a given input order (the handler sorts the universe by ticker).
func r126SamplePairs(mkts []r126Mkt, nCorr, nIndep int) []r126Pair {
	byGame := map[string][]r126Mkt{}
	for _, m := range mkts {
		if m.GameKey != "" {
			byGame[m.GameKey] = append(byGame[m.GameKey], m)
		}
	}
	gameOrder := make([]string, 0, len(byGame))
	for gk := range byGame {
		gameOrder = append(gameOrder, gk)
	}
	sort.Strings(gameOrder)

	classPairs := [][2]string{{"ml", "spread"}, {"ml", "total"}, {"spread", "total"}}

	// one candidate pair per (game, class pair), grouped by sport
	cands := map[string]map[string][]r126Pair{} // pairClass → sport → pairs (game order)
	for _, gk := range gameOrder {
		byClass := map[string]r126Mkt{}
		for _, m := range byGame[gk] {
			if cur, ok := byClass[m.Class]; !ok || m.Ticker < cur.Ticker {
				byClass[m.Class] = m // deterministic representative per class
			}
		}
		for _, cp := range classPairs {
			a, okA := byClass[cp[0]]
			b, okB := byClass[cp[1]]
			if !okA || !okB {
				continue
			}
			pc := r126PairClass(cp[0], cp[1])
			if cands[pc] == nil {
				cands[pc] = map[string][]r126Pair{}
			}
			cands[pc][a.Sport] = append(cands[pc][a.Sport], r126Pair{Kind: "corr", Class: pc, GameKey: gk, A: a, B: b})
		}
	}
	// flatten each class pair sport-interleaved: sport1[0], sport2[0], …, sport1[1], …
	flat := map[string][]r126Pair{}
	for pc, bySport := range cands {
		sports := make([]string, 0, len(bySport))
		for sp := range bySport {
			sports = append(sports, sp)
		}
		sort.Strings(sports)
		for i := 0; ; i++ {
			added := false
			for _, sp := range sports {
				if i < len(bySport[sp]) {
					flat[pc] = append(flat[pc], bySport[sp][i])
					added = true
				}
			}
			if !added {
				break
			}
		}
	}
	var out []r126Pair
	idx := map[string]int{}
	for len(out) < nCorr {
		progress := false
		for _, cp := range classPairs {
			pc := r126PairClass(cp[0], cp[1])
			if idx[pc] < len(flat[pc]) && len(out) < nCorr {
				out = append(out, flat[pc][idx[pc]])
				idx[pc]++
				progress = true
			}
		}
		if !progress {
			break
		}
	}

	// independent controls: one representative per game (prefer the moneyline), games interleaved
	// by sport, then consecutive disjoint pairs — mostly cross-sport when several sports are live.
	reps := make([]r126Mkt, 0, len(gameOrder))
	for _, gk := range gameOrder {
		group := byGame[gk]
		best := group[0]
		for _, m := range group[1:] {
			if m.Class == "ml" && best.Class != "ml" {
				best = m
				continue
			}
			if m.Class == best.Class && m.Ticker < best.Ticker {
				best = m
			}
		}
		reps = append(reps, best)
	}
	bySport := map[string][]r126Mkt{}
	for _, m := range reps {
		bySport[m.Sport] = append(bySport[m.Sport], m)
	}
	spOrder := make([]string, 0, len(bySport))
	for sp := range bySport {
		spOrder = append(spOrder, sp)
	}
	sort.Strings(spOrder)
	inter := make([]r126Mkt, 0, len(reps))
	for i := 0; ; i++ {
		added := false
		for _, sp := range spOrder {
			if i < len(bySport[sp]) {
				inter = append(inter, bySport[sp][i])
				added = true
			}
		}
		if !added {
			break
		}
	}
	nInd := 0
	for i := 0; i+1 < len(inter) && nInd < nIndep; i += 2 {
		a, b := inter[i], inter[i+1]
		if a.GameKey == b.GameKey {
			continue
		}
		out = append(out, r126Pair{Kind: "indep", Class: "INDEP", GameKey: a.GameKey + "|" + b.GameKey, A: a, B: b})
		nInd++
	}
	return out
}

// r126JointForPair — naive product vs correlation-adjusted joint for one pair. Corr pairs share
// the game EventKey and get the plabJointPEx pair adjustment (ovGroups {{0,1}}; rho comes from
// the pair class's live counters — e.g. cell "ov:kgame:KXMLBGAME+KXMLBTOTAL"). Indep pairs carry
// distinct EventKeys and ovGroups nil, so adj == product exactly (the control). Caller holds
// plabMu (plabJointPEx reads s.plabCorr).
func (s *Server) r126JointForPair(p r126Pair) (prod, adj, rho float64, rhoN int) {
	p1, p2 := p.A.mid(), p.B.mid()
	prod = p1 * p2
	legs := []plabLeg{
		{Ticker: p.A.Ticker, Side: "yes", Platform: "kalshi", Price: p1, PWin: p1, EventKey: p.A.GameKey},
		{Ticker: p.B.Ticker, Side: "yes", Platform: "kalshi", Price: p2, PWin: p2, EventKey: p.B.GameKey},
	}
	var ov [][]int
	if p.Kind == "corr" {
		ov = [][]int{{0, 1}}
	}
	adj, rho, rhoN = s.plabJointPEx(legs, ov)
	return prod, adj, rho, rhoN
}

// r126MatchCols — collections that list BOTH legs' events and allow yes-side legs. Membership is
// checked here, never probed (the R112 lesson: probing a non-member collection only produces a
// membership refusal, which poisons legality verdicts). Two same-game legs live in DIFFERENT
// events (KXMLBGAME-… vs KXMLBTOTAL-…), so per-event size caps of 1 don't bind.
func r126MatchCols(cols []kalshi.MVCollection, evA, evB string) []kalshi.MVCollection {
	if evA == "" || evB == "" {
		return nil
	}
	var out []kalshi.MVCollection
	for _, c := range cols {
		if _, ok := c.Events[evA]; !ok {
			continue
		}
		if _, ok := c.Events[evB]; !ok {
			continue
		}
		if c.LegOK(evA, "yes") != "" || c.LegOK(evB, "yes") != "" {
			continue
		}
		if c.SizeMin > 2 || (c.SizeMax > 0 && c.SizeMax < 2) {
			continue
		}
		out = append(out, c)
	}
	return out
}

// ── response shapes ──────────────────────────────────────────────────────────────────────────

type r126LegJSON struct {
	Ticker string  `json:"ticker"`
	Bid    float64 `json:"bid"`
	Ask    float64 `json:"ask"`
	Mid    float64 `json:"mid"`
}

type r126ComboJSON struct {
	// State: skipped (budget out / no venue client) | no_collection | legal | created_legal |
	// unknown (never instantiated, no create budget) | create_refused | illegal | transport_error
	State      string   `json:"state"`
	Collection string   `json:"collection,omitempty"`
	Ticker     string   `json:"ticker,omitempty"`
	YesBid     *float64 `json:"yes_bid,omitempty"`
	YesAsk     *float64 `json:"yes_ask,omitempty"`
	Last       *float64 `json:"last,omitempty"`
	OI         *float64 `json:"oi,omitempty"`
	Volume     *float64 `json:"volume,omitempty"`
	Reason     string   `json:"venue_reason,omitempty"`
}

type r126WQJSON struct {
	YesBid float64 `json:"yes_bid"`
	NoBid  float64 `json:"no_bid"`
}

type r126Row struct {
	Kind             string        `json:"kind"` // corr | indep
	Class            string        `json:"class"`
	GameKey          string        `json:"game_key"`
	Legs             []r126LegJSON `json:"legs"`
	Product          float64       `json:"product"`
	AdjJoint         float64       `json:"adj_joint"`
	Rho              float64       `json:"rho"`
	RhoN             int           `json:"rho_n"`
	Combo            r126ComboJSON `json:"combo"`
	WQ               *r126WQJSON   `json:"wq,omitempty"`
	MispQuoteVsProdC *float64      `json:"mispricing_quote_vs_product_c"` // null when no live quote
	MispAdjVsProdC   float64       `json:"mispricing_adj_vs_product_c"`
}

type r126Sum struct {
	N                int      `json:"n"`
	MeanAdjVsProdC   float64  `json:"mean_adj_vs_product_c"`
	MeanQuoteVsProdC *float64 `json:"mean_quote_vs_product_c"` // null when no rows have quotes
	CountLiveQuotes  int      `json:"count_with_live_quotes"`
}

// handleR126Combo (GET /api/r126combo) — the measurement pass. LOG-ONLY: lookup/create probes
// and market reads under the call budgets; no RFQs, no orders, never CreateMVEMarket.
func (s *Server) handleR126Combo(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	qi := func(name string, def int) int {
		v := strings.TrimSpace(r.URL.Query().Get(name))
		if v == "" {
			return def
		}
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
		return def
	}
	p := r126ClampParams(qi("n_corr", 0), qi("n_indep", 0), qi("lookups", 0), qi("create", 0))

	// Universe: classified sports markets from the tape cache (metaMu — the one safe read rule)
	// with a game key and a live two-sided BBO (book WS preferred, REST meta fallback).
	type tkm struct {
		tk string
		m  kalshi.Market
	}
	s.metaMu.Lock()
	scan := make([]tkm, 0, 1024)
	for tk, m := range s.kmkts {
		if _, _, ok := r126Classify(tk); !ok {
			continue
		}
		if m.Result != "" {
			continue
		}
		if m.Status != "" && !strings.EqualFold(m.Status, "active") {
			continue
		}
		scan = append(scan, tkm{tk, m})
	}
	s.metaMu.Unlock()
	sort.Slice(scan, func(i, j int) bool { return scan[i].tk < scan[j].tk })
	mkts := make([]r126Mkt, 0, len(scan))
	for _, e := range scan {
		class, sport, _ := r126Classify(e.tk)
		gk := s.legEventKey("kalshi", e.tk, e.m.Title)
		if !strings.HasPrefix(gk, "kgame:") {
			continue
		}
		bbo := s.rfqLegBBO(e.tk)
		if bbo.Bid <= 0 || bbo.Ask <= 0 || bbo.Ask < bbo.Bid || bbo.Ask-bbo.Bid > r126MaxLegSpreadC+1e-9 {
			continue
		}
		mkts = append(mkts, r126Mkt{Ticker: e.tk, Event: e.m.EventTicker, GameKey: gk,
			Class: class, Sport: sport, Bid: bbo.Bid, Ask: bbo.Ask, Src: bbo.Src})
	}

	pairs := r126SamplePairs(mkts, p.NCorr, p.NIndep)

	round4 := func(v float64) float64 { return math.Round(v*10000) / 10000 }
	roundC := func(v float64) float64 { return math.Round(v*100) / 100 }
	fp := func(v float64) *float64 { return &v }

	// Pure math first: product vs correlation-adjusted joint, under plabMu (rho cells).
	rows := make([]r126Row, 0, len(pairs))
	s.plabMu.Lock()
	if s.store != nil {
		s.plabState(ctx) // lazy-load the persisted correlation counters (once per boot)
	}
	for _, pr := range pairs {
		prod, adj, rho, rhoN := s.r126JointForPair(pr)
		rows = append(rows, r126Row{
			Kind: pr.Kind, Class: pr.Class, GameKey: pr.GameKey,
			Legs: []r126LegJSON{
				{Ticker: pr.A.Ticker, Bid: pr.A.Bid, Ask: pr.A.Ask, Mid: round4(pr.A.mid())},
				{Ticker: pr.B.Ticker, Bid: pr.B.Bid, Ask: pr.B.Ask, Mid: round4(pr.B.mid())},
			},
			Product: round4(prod), AdjJoint: round4(adj), Rho: round4(rho), RhoN: rhoN,
			Combo:          r126ComboJSON{State: "skipped"},
			MispAdjVsProdC: roundC((adj - prod) * 100),
		})
	}
	s.plabMu.Unlock()

	// RFQ-sim would-quote per pair (pure arithmetic off the leg BBOs — the R106 pricing).
	a := s.cfg().Auto
	for i := range rows {
		pr := pairs[i]
		snaps := []rfqLegSnap{
			{Mkt: pr.A.Ticker, Side: "yes", Bid: pr.A.Bid, Ask: pr.A.Ask, Src: pr.A.Src},
			{Mkt: pr.B.Ticker, Side: "yes", Bid: pr.B.Bid, Ask: pr.B.Ask, Src: pr.B.Src},
		}
		if q, ok := computeWouldQuote(snaps, 1, a.RFQSimMarginC, a.RFQSimMarginPerLegC, a.RFQSimMaxUSD, false); ok {
			rows[i].WQ = &r126WQJSON{YesBid: q.YesBid, NoBid: q.NoBid}
		}
	}

	// Venue phase: combined-market lookup (+ optional create probe) + market read, hard-budgeted.
	// Rows are corr-first (the sampler's order), so the thesis rows get the budget first.
	lookLeft, createLeft := p.Lookups, p.Creates
	lookSpent, createSpent := 0, 0
	comboFound, liveQuotes := 0, 0
	var cols []kalshi.MVCollection
	if s.kal != nil {
		cols = s.comboCollections(ctx)
	}
	for i := range rows {
		if s.kal == nil || lookLeft <= 0 {
			break // remaining rows keep state "skipped"
		}
		pr := pairs[i]
		match := r126MatchCols(cols, pr.A.Event, pr.B.Event)
		if len(match) == 0 {
			rows[i].Combo.State = "no_collection"
			continue
		}
		if len(match) > r126ColsPerPair {
			match = match[:r126ColsPerPair]
		}
		legs := []kalshi.MVELeg{
			{MarketTicker: strings.ToUpper(pr.A.Ticker), EventTicker: pr.A.Event, Side: "yes"},
			{MarketTicker: strings.ToUpper(pr.B.Ticker), EventTicker: pr.B.Event, Side: "yes"},
		}
		state, mtk, reason := "skipped", "", ""
		for _, col := range match {
			if lookLeft <= 0 {
				break
			}
			cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			st, mt, _, rs, err := s.kal.LookupComboFull(cctx, col.CollectionTicker, legs)
			cancel()
			lookLeft--
			lookSpent++
			if err != nil {
				state = "transport_error"
				break
			}
			if st == kalshi.ComboIllegal {
				state, reason = "illegal", rs
				continue // the next collection may serve it
			}
			if st == kalshi.ComboLegal {
				state, mtk = "legal", mt
				rows[i].Combo.Collection = col.CollectionTicker
				break
			}
			// ComboUnknown — never instantiated. Escalate to a create probe (a public LISTING,
			// not an order; no money moves) only inside BOTH this call's create budget and the
			// suite's persisted weekly cprobe budget, then re-lookup once for the ticker.
			state = "unknown"
			if createLeft > 0 && s.cprobeCreateAllow(ctx) {
				cctx2, cancel2 := context.WithTimeout(ctx, 10*time.Second)
				ok, crs, cerr := s.kal.CreateComboProbe(cctx2, col.CollectionTicker, legs)
				cancel2()
				createLeft--
				createSpent++
				switch {
				case cerr == nil && ok && lookLeft > 0:
					cctx3, cancel3 := context.WithTimeout(ctx, 10*time.Second)
					st2, mt2, _, _, err2 := s.kal.LookupComboFull(cctx3, col.CollectionTicker, legs)
					cancel3()
					lookLeft--
					lookSpent++
					if err2 == nil && st2 == kalshi.ComboLegal {
						state, mtk = "created_legal", mt2
						rows[i].Combo.Collection = col.CollectionTicker
					}
				case cerr == nil && !ok:
					state, reason = "create_refused", crs
				}
			}
			break
		}
		rows[i].Combo.State = state
		rows[i].Combo.Ticker = mtk
		if reason != "" {
			rows[i].Combo.Reason = cprobeReasonShort(reason)
		}
		if mtk == "" || lookLeft <= 0 {
			continue
		}
		comboFound++
		mctx, mcancel := context.WithTimeout(ctx, 10*time.Second)
		cm, err := s.kal.GetMarket(mctx, mtk)
		mcancel()
		lookLeft--
		lookSpent++
		if err != nil {
			continue
		}
		yb, ya := cm.YesBid.Float(), cm.YesAsk.Float()
		if yb > 0 {
			rows[i].Combo.YesBid = fp(yb)
		}
		if ya > 0 {
			rows[i].Combo.YesAsk = fp(ya)
		}
		if v := cm.LastPrice.Float(); v > 0 {
			rows[i].Combo.Last = fp(v)
		}
		if v := cm.OpenInterest.Float(); v > 0 {
			rows[i].Combo.OI = fp(v)
		}
		if v := cm.Volume.Float(); v > 0 {
			rows[i].Combo.Volume = fp(v)
		}
		if yb > 0 && ya > 0 && ya >= yb {
			liveQuotes++
			rows[i].MispQuoteVsProdC = fp(roundC(((yb+ya)/2 - rows[i].Product) * 100))
		}
	}

	// Per-class summary: adjusted-vs-product always; quote-vs-product over rows with live quotes.
	type acc struct {
		n, nq    int
		adj, quo float64
	}
	accs := map[string]*acc{}
	for _, row := range rows {
		a := accs[row.Class]
		if a == nil {
			a = &acc{}
			accs[row.Class] = a
		}
		a.n++
		a.adj += row.MispAdjVsProdC
		if row.MispQuoteVsProdC != nil {
			a.nq++
			a.quo += *row.MispQuoteVsProdC
		}
	}
	sums := map[string]r126Sum{}
	for class, a := range accs {
		sm := r126Sum{N: a.n, MeanAdjVsProdC: roundC(a.adj / float64(a.n)), CountLiveQuotes: a.nq}
		if a.nq > 0 {
			sm.MeanQuoteVsProdC = fp(roundC(a.quo / float64(a.nq)))
		}
		sums[class] = sm
	}

	nCorrRows, nIndepRows := 0, 0
	for _, row := range rows {
		if row.Kind == "corr" {
			nCorrRows++
		} else {
			nIndepRows++
		}
	}
	if s.store != nil {
		actx, acancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		_ = s.store.Audit(actx, "info", "r126combo", fmt.Sprintf(
			"R126 combo sample: %d corr + %d indep pairs (universe %d legs) · budget lookups %d/%d creates %d/%d · %d combined markets found · %d with live two-sided quotes",
			nCorrRows, nIndepRows, len(mkts), lookSpent, p.Lookups, createSpent, p.Creates, comboFound, liveQuotes), "")
		acancel()
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"note":         "R126 Part 3 measurement (log-only): combined-market quotes vs naive leg product vs correlation-adjusted joint. Lookup/create probes only, hard-budgeted — no RFQs, no orders.",
		"params_used":  p,
		"budget_spent": map[string]int{"lookups": lookSpent, "creates": createSpent},
		"universe":     map[string]int{"classified_live_legs": len(mkts), "pairs_sampled": len(pairs)},
		"rows":         rows,
		"summary":      sums,
	})
}
