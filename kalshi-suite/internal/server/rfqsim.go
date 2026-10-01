package server

// R106 RFQ WOULD-QUOTE SIMULATOR (edge 36, log-only — the profit groundwork).
//
// The R101 logger proved the venue's RFQ flow is priceable: 98% of combo legs carry real BBO+sizes,
// median leg spread 1¢, and 0/2,302 combos ever had a competing quote. This layer computes, on
// every incoming RFQ, the two-sided quote we WOULD post — legs priced from live BBOs, a
// configurable conservative margin, size-capped — LOGS it (the rfq_wouldquote.jsonl rows gain wq_*
// fields), and GRADES it at combo settlement (venue-truth SettledYes, the sweepLiveCombos
// pattern): what the quote would have earned/lost on each side, with the ADVERSE-SELECTION-honest
// headline = we are hit on whichever side loses us more. NO actual quoting, NO orders — pure
// logging; /api/rfqsim serves the running case.
//
// Fill-assumption honesty: quote events are PRIVATE on the venue WS (we can never see whether a
// real RFQ traded or at what price), so "would we have been hit" is unknowable per-RFQ. The
// simulator therefore grades BOTH hypothetical hits and headlines the WORST (requester picks the
// side that hurts us — the upper bound on adverse selection), with the YES/NO splits as color.
// grade-confidence: high = every leg priced from the live orderbook WS at RFQ arrival · med =
// some legs from the REST meta cache · low = legs attached at the 10-min flush (stale by design).

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// wqQuote is the computed would-quote for one RFQ.
type wqQuote struct {
	YesBid  float64 // we'd bid this to BUY the combo's YES side (fair − margin)
	NoBid   float64 // and this to buy its NO side (1−fair − margin) — venue quotes are two-sided
	Fair    float64 // ∏ side-adjusted leg mids × 0.97^(n−1) correlation haircut
	Prod    float64 // ∏ side-adjusted leg mids (no haircut)
	MarginC float64 // margin applied (cents)
	Size    float64 // contracts we'd quote
	Conf    string  // high | med | low
}

// computeWouldQuote prices a combo off its leg snapshot. Returns ok=false when any leg lacks a
// two-sided price (never invent a quote from a blind leg — the live combo path's rule).
func computeWouldQuote(legs []rfqLegSnap, rfqContracts, marginFloorC, marginPerLegC, maxUSD float64, atFlush bool) (wqQuote, bool) {
	if len(legs) == 0 {
		return wqQuote{}, false
	}
	prod, conf := 1.0, "high"
	for _, lg := range legs {
		if lg.Bid <= 0 || lg.Ask <= 0 || lg.Ask < lg.Bid {
			return wqQuote{}, false // blind/degenerate leg — no quote
		}
		mid := (lg.Bid + lg.Ask) / 2
		if strings.EqualFold(strings.TrimSpace(lg.Side), "no") {
			mid = 1 - mid
		}
		if mid <= 0 || mid >= 1 {
			return wqQuote{}, false
		}
		prod *= mid
		if lg.Src != "bookws" {
			conf = "med" // REST meta cache can be minutes old
		}
	}
	if atFlush {
		conf = "low" // flush-time legs (first-sighting ticker) — stale by construction
	}
	fair := prod * math.Pow(parlayCorrHaircut, float64(len(legs)-1))
	if marginFloorC <= 0 {
		marginFloorC = 3.0 // conservative default: the live combo path's 3¢ floor
	}
	if marginPerLegC <= 0 {
		marginPerLegC = 1.5 // + the live path's 1.5¢/leg
	}
	marginC := marginPerLegC * float64(len(legs))
	if marginC < marginFloorC {
		marginC = marginFloorC
	}
	m := marginC / 100
	clamp := func(v float64) float64 {
		if v < 0.01 {
			return 0.01
		}
		if v > 0.99 {
			return 0.99
		}
		return math.Round(v*1000) / 1000 // combos tick deci-cent
	}
	yesBid := clamp(fair - m)
	noBid := clamp((1 - fair) - m)
	if maxUSD <= 0 {
		maxUSD = 5 // conservative default (the live combo session budget's unit)
	}
	size := rfqContracts
	if size <= 0 {
		size = 1
	}
	// cap by dollars at the pricier side (both sides quoted — the worst-case outlay bounds size)
	px := math.Max(yesBid, noBid)
	if px > 0 && size*px > maxUSD {
		size = math.Floor(maxUSD / px)
	}
	if size < 1 {
		size = 1
	}
	return wqQuote{YesBid: yesBid, NoBid: noBid, Fair: math.Round(fair*10000) / 10000,
		Prod: math.Round(prod*10000) / 10000, MarginC: marginC, Size: size, Conf: conf}, true
}

// wqOpenEnt is one ungraded would-quote awaiting its combo's settlement.
type wqOpenEnt struct {
	RFQID  string  `json:"rfq_id"`
	Ticker string  `json:"ticker"`
	YesBid float64 `json:"yes_bid"`
	NoBid  float64 `json:"no_bid"`
	Size   float64 `json:"size"`
	Conf   string  `json:"conf"`
	At     string  `json:"at"`
	atT    time.Time
}

// wqNote registers a computed quote for later grading (bounded; newest wins per combo ticker —
// re-RFQd combos re-quote at fresher prices, and one settlement grades the latest stance).
func (s *Server) wqNote(row *rfqWouldRow, q wqQuote) {
	s.wqMu.Lock()
	defer s.wqMu.Unlock()
	if s.wqOpen == nil {
		s.wqOpen = map[string]wqOpenEnt{}
	}
	if len(s.wqOpen) >= 2000 { // runaway bound: drop the oldest entry
		oldK, oldT := "", time.Now()
		for k, e := range s.wqOpen {
			if e.atT.Before(oldT) {
				oldK, oldT = k, e.atT
			}
		}
		if oldK != "" {
			delete(s.wqOpen, oldK)
			s.wqStats.Expired++
		}
	}
	s.wqOpen[row.Ticker] = wqOpenEnt{RFQID: row.RFQID, Ticker: row.Ticker,
		YesBid: q.YesBid, NoBid: q.NoBid, Size: q.Size, Conf: q.Conf, At: row.At, atT: time.Now()}
	s.wqStats.Quoted++
	if s.wqStats.ByConf == nil {
		// R106d: this nil map CRASHED the suite 4 min into the first soak — the flush-path panic
		// was runGuarded-recovered (loop=rfq-quoter-sampler, 15:22:48Z), the arrival-path twin
		// then hit the UNGUARDED WS read goroutine and took the process down. Lazily init like
		// wqOpen above; pinned by TestR106WQNoteInitAndCount.
		s.wqStats.ByConf = map[string]int{}
	}
	s.wqStats.ByConf[q.Conf]++
}

// wqStatsT is the running would-quote scoreboard (guarded by wqMu). PnLAdverse is THE number:
// the requester hits whichever of our two bids loses us more.
type wqStatsT struct {
	Quoted     int            `json:"quoted"`
	Graded     int            `json:"graded"`
	Expired    int            `json:"expired"`
	PnLAdverse float64        `json:"pnl_adverse"`
	PnLYes     float64        `json:"pnl_yes_hit"`
	PnLNo      float64        `json:"pnl_no_hit"`
	ByConf     map[string]int `json:"quoted_by_conf"`
}

// wqGradePass grades open would-quotes whose combo settled — venue truth via GetMarket.SettledYes
// (the sweepLiveCombos pattern). Runs on the 10-min sampler goroutine with its OWN counted budget
// (≤25 GetMarket/flush, on top of the leg resolver's ≤100 — both under the AIMD limiter's
// background tier). Grades append to the same jsonl (kind "wq_grade") + the in-memory scoreboard.
func (s *Server) wqGradePass(ctx context.Context) {
	if s.kal == nil {
		return
	}
	s.wqMu.Lock()
	cands := make([]wqOpenEnt, 0, len(s.wqOpen))
	for _, e := range s.wqOpen {
		cands = append(cands, e)
	}
	s.wqMu.Unlock()
	if len(cands) == 0 {
		return
	}
	sort.Slice(cands, func(i, j int) bool { return cands[i].atT.Before(cands[j].atT) }) // oldest first (closest to settlement)
	type grade struct {
		ent  wqOpenEnt
		sv   float64
		drop bool
	}
	var grades []grade
	budget := 25
	for _, e := range cands {
		if budget <= 0 || ctx.Err() != nil {
			break
		}
		if time.Since(e.atT) > 7*24*time.Hour { // never settled inside a week — expire from the ledger
			grades = append(grades, grade{ent: e, drop: true})
			continue
		}
		if time.Since(e.atT) < 10*time.Minute {
			continue // too fresh to have settled — don't spend budget
		}
		budget--
		mctx, mcancel := context.WithTimeout(ctx, 6*time.Second)
		m, err := s.kal.GetMarket(mctx, e.Ticker)
		mcancel()
		if err != nil {
			continue
		}
		sv := m.SettledYes()
		if sv < 0 {
			continue // not final yet
		}
		grades = append(grades, grade{ent: e, sv: sv})
	}
	if len(grades) == 0 {
		return
	}
	var f *os.File
	if path := filepath.Join(s.cfg().DataDir, "rfq_wouldquote.jsonl"); true {
		f, _ = os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	}
	enc := (*json.Encoder)(nil)
	if f != nil {
		enc = json.NewEncoder(f)
	}
	now := time.Now().UTC().Format(time.RFC3339)
	s.wqMu.Lock()
	for _, g := range grades {
		delete(s.wqOpen, g.ent.Ticker)
		if g.drop {
			s.wqStats.Expired++
			continue
		}
		fee := 0.0 // maker side of a combo fill — modeled with the venue maker coefficient
		if s.kal != nil {
			fee = s.kalFee(g.ent.Ticker, true, int(g.ent.Size), g.ent.YesBid)
		}
		pnlYes := g.ent.Size*(g.sv-g.ent.YesBid) - fee
		pnlNo := g.ent.Size*((1-g.sv)-g.ent.NoBid) - fee
		adverse := math.Min(pnlYes, pnlNo)
		s.wqStats.Graded++
		s.wqStats.PnLYes += pnlYes
		s.wqStats.PnLNo += pnlNo
		s.wqStats.PnLAdverse += adverse
		row := map[string]any{
			"kind": "wq_grade", "at": now, "rfq_id": g.ent.RFQID, "ticker": g.ent.Ticker,
			"quoted_at": g.ent.At, "conf": g.ent.Conf, "yes_bid": g.ent.YesBid, "no_bid": g.ent.NoBid,
			"size": g.ent.Size, "settle_val": g.sv,
			"pnl_yes_hit": math.Round(pnlYes*100) / 100, "pnl_no_hit": math.Round(pnlNo*100) / 100,
			"pnl_adverse": math.Round(adverse*100) / 100,
		}
		if enc != nil {
			_ = enc.Encode(row)
		}
		s.wqGrades = append(s.wqGrades, row)
		if len(s.wqGrades) > 50 {
			s.wqGrades = s.wqGrades[len(s.wqGrades)-50:]
		}
	}
	stats := s.wqStats
	s.wqMu.Unlock()
	if f != nil {
		_ = f.Close()
	}
	actx, acancel := context.WithTimeout(ctx, 5*time.Second)
	_ = s.store.Audit(actx, "info", "rfqpulse",
		fmt.Sprintf("R106 would-quote grades: +%d (total %d graded / %d quoted) · adverse P&L %+.2f (yes-hit %+.2f · no-hit %+.2f)",
			len(grades), stats.Graded, stats.Quoted, stats.PnLAdverse, stats.PnLYes, stats.PnLNo), "")
	acancel()
}

// handleRFQSim (GET /api/rfqsim) — the running would-quote case: scoreboard + open ledger size +
// the newest grades. Log-only; nothing here can place an order.
func (s *Server) handleRFQSim(w http.ResponseWriter, _ *http.Request) {
	s.wqMu.Lock()
	stats := s.wqStats
	openN := len(s.wqOpen)
	grades := append([]map[string]any(nil), s.wqGrades...)
	s.wqMu.Unlock()
	a := s.cfg().Auto
	writeJSON(w, http.StatusOK, map[string]any{
		"note":   "log-only simulator: the quote the strategy would post per RFQ (legs at live BBO minus margin, two-sided), graded at combo settlement; headline P&L assumes adverse selection (hit on the worse side)",
		"margin": map[string]any{"floor_c": a.RFQSimMarginC, "per_leg_c": a.RFQSimMarginPerLegC, "max_usd": a.RFQSimMaxUSD},
		"stats":  stats,
		"open":   openN,
		"grades": grades,
	})
}
