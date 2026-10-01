package server

// r115.go — R115 operator pass: (1) fill-time re-check (re-score p_win at the FILL moment, not just
// the decision moment, and pull resting posts whose edge died), (2) queue-position realism for the
// maker sim (price-time priority: we join the BACK of the visible queue at our level; prints AT the
// level chew the queue ahead before any of our contracts fill; partial fills allowed), (3) budgeted
// REST orderbook fallback so decision-relevant tickers stop flying blind (depth_src=none was ~half
// of post rows pre-R115 — Part 3 of the pass).

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// ---- fill-time re-check --------------------------------------------------------------------

// mlPredPWin returns the sidecar's CURRENT p_win for a market/side from ml_predictions.json
// (cached ≤30s; rejects a stale predictions file >3min old — a dead sidecar must not drive
// edge-died cancels or drift stamps with hour-old numbers).
func (s *Server) mlPredPWin(platform, ticker, side string) (float64, bool) {
	return s.mlCachedPWin(platform, ticker, side, true)
}

// mlResearchPWin exposes the same calibrated estimate only to telemetry and risk-veto paths.
// It cannot authorize or size an entry while the model's money authority is false.
func (s *Server) mlResearchPWin(platform, ticker, side string) (float64, bool) {
	return s.mlCachedPWin(platform, ticker, side, false)
}

func (s *Server) mlExecutionAuthorized() bool {
	_, _ = s.mlResearchPWin("kalshi", "\x00authority-refresh", "YES")
	s.mlPredMu.Lock()
	defer s.mlPredMu.Unlock()
	return s.mlPredExecAuthority
}

func mlFundedRouteAllowed(origin string, authority func() bool) bool {
	if !strings.EqualFold(strings.TrimSpace(origin), "ml") {
		return true
	}
	return authority != nil && authority()
}

func mlPaperMoneyAuthority(executionEnabled, paperAuthority bool) bool {
	return executionEnabled && paperAuthority
}

func mlLiveMoneyAuthority(executionEnabled, paperAuthority, liveAuthority bool) bool {
	return executionEnabled && paperAuthority && liveAuthority
}

// mlMoneyAuthority remains the strict LIVE alias for older call sites. Paper-only paths must call
// mlPaperMoneyAuthority explicitly so a provisional model can collect paper evidence without
// widening any real-money seam.
func mlMoneyAuthority(executionEnabled, paperAuthority, liveAuthority bool) bool {
	return mlLiveMoneyAuthority(executionEnabled, paperAuthority, liveAuthority)
}

func (s *Server) mlCachedPWin(platform, ticker, side string, requireAuthority bool) (float64, bool) {
	s.mlPredMu.Lock()
	defer s.mlPredMu.Unlock()
	if time.Since(s.mlPredAt) > 30*time.Second {
		s.mlPredAt = time.Now()
		path := filepath.Join(s.cfg().DataDir, "ml_predictions.json")
		if fi, err := os.Stat(path); err != nil || time.Since(fi.ModTime()) > 3*time.Minute {
			s.mlPredMap = nil // stale/missing file: serve nothing until the sidecar writes again
			s.mlPredExecAuthority = false
			s.mlEceGatedOK = false
			return 0, false
		}
		var pr struct {
			ExecutionEnabled bool `json:"execution_enabled"`
			LiveAuthority    bool `json:"live_authority"`
			PaperAuthority   bool `json:"paper_authority"`
			// R122 (407): the sidecar's live-gates calibration sensor rides the same file.
			EceGated  *float64 `json:"ece_gated"`
			EceGatedN int      `json:"ece_gated_n"`
			Preds     []struct {
				Ticker   string  `json:"ticker"`
				Side     string  `json:"side"`
				Platform string  `json:"platform"`
				PWin     float64 `json:"p_win"`
			} `json:"predictions"`
		}
		if !s.readJSONLoose(path, &pr) {
			s.mlPredMap = nil
			s.mlPredExecAuthority = false
			s.mlEceGatedOK = false
			return 0, false
		}
		m := make(map[string]float64, len(pr.Preds))
		for _, p := range pr.Preds {
			if p.Ticker != "" && p.PWin > 0 {
				m[strings.ToLower(p.Platform)+"|"+p.Ticker+"|"+strings.ToUpper(p.Side)] = p.PWin
			}
		}
		s.mlPredMap = m
		s.mlPredExecAuthority = mlMoneyAuthority(pr.ExecutionEnabled, pr.PaperAuthority, pr.LiveAuthority)
		if pr.EceGated != nil {
			s.mlEceGatedV, s.mlEceGatedN, s.mlEceGatedOK = *pr.EceGated, pr.EceGatedN, true
		} else {
			s.mlEceGatedN, s.mlEceGatedOK = 0, false
		}
	}
	if requireAuthority && !s.mlPredExecAuthority {
		return 0, false
	}
	if s.mlPredMap == nil {
		// R123: even with a dead sidecar file, a FRESH Go-eval tick score is a valid answer —
		// it was anchored to the sidecar's last publish and repriced at the live tick.
		if pw, at, ok := s.goEvalScore(platform, ticker, side); ok && time.Since(at) <= mlEvalFreshFor {
			return pw, true
		}
		return 0, false
	}
	// R123 OVERLAY: prefer the tick-fresh Go-eval score (updates within ms of a price move)
	// over the file value (sidecar cycle cadence). Every consumer of this seam — the fill-time
	// recheck, the edge-died post pull, mlMakerFill's drift stamps — turns tick-fresh with it.
	if pw, at, ok := s.goEvalScore(platform, ticker, side); ok && time.Since(at) <= mlEvalFreshFor {
		return pw, true
	}
	v, ok := s.mlPredMap[strings.ToLower(platform)+"|"+ticker+"|"+strings.ToUpper(side)]
	return v, ok
}

// mlEceGatedNow — R122 (407 sensor read): current ece_gated from the sidecar's predictions file
// (refreshes the same 30s cache mlPredPWin owns). ok=false ⇒ sensor absent/stale.
func (s *Server) mlEceGatedNow() (float64, int, bool) {
	_, _ = s.mlResearchPWin("kalshi", "\x00router-sensor-refresh", "YES") // trigger the cache refresh
	s.mlPredMu.Lock()
	defer s.mlPredMu.Unlock()
	return s.mlEceGatedV, s.mlEceGatedN, s.mlEceGatedOK
}

// stampFillRecheck writes the fill-moment facts onto the mfs row: the market price at fill, the
// decision-time p_win and the FILL-TIME p_win (re-scored from the sidecar's current predictions).
// The drift (fill_pwin − dec_pwin) is the operator's Part-2 number: how much edge decayed between
// deciding and actually getting filled.
func (s *Server) stampFillRecheck(ctx context.Context, p *pendingMaker, cur float64, rule string) {
	fillPW, okPW := 0.0, false
	if pw, ok := s.mlResearchPWin(p.platform, p.ticker, p.side); ok {
		fillPW, okPW = pw, true
	}
	_ = s.store.SetMakerFillCheck(ctx, p.id, cur, p.decPWin, fillPW, okPW, rule)
	if p.queueKnown {
		_ = s.store.SetMakerQueueState(ctx, p.id, p.queueAhead, p.queueLeft, p.filledCt)
	}
}

// ---- queue-position model ------------------------------------------------------------------

const (
	queueTradeAway = iota
	queueTradeAt
	queueTradeThrough
)

func queueTradeClass(tradePx, postPx, tick float64) int {
	if tick <= 0 {
		tick = 0.01
	}
	eps := math.Max(1e-9, tick/1000)
	if tradePx < postPx-eps {
		return queueTradeThrough
	}
	if math.Abs(tradePx-postPx) <= eps {
		return queueTradeAt
	}
	return queueTradeAway
}

// queueAheadAt reads the visible resting size at our price level on our side (the queue we join
// the back of). Kalshi book-WS only; ok=false requires an opposite-aggressor trade through the
// price but cannot model at-level queue consumption.
func (s *Server) queueAheadAt(platform, ticker, side string, px float64) (float64, bool) {
	if platform != "kalshi" || s.kal == nil {
		return 0, false
	}
	return s.kal.BookLevelSize(ticker, strings.EqualFold(side, "NO"), px, 5*time.Second)
}

// queueChewTape consumes NEW tape prints since the last sweep against this resting post:
//   - a print THROUGH our level (better for the aggressor than our price) proves the whole level
//     traded → queue gone, we fill in full;
//   - a print AT our level chews the queue AHEAD of us first; overflow fills our contracts
//     (partials allowed). Only prints whose aggressor was hitting OUR side of the book count.
func (s *Server) queueChewTape(p *pendingMaker) {
	if s.kal == nil {
		return
	}
	tape, ok := s.kal.LiveTape()
	if !ok {
		return
	}
	isNo := strings.EqualFold(p.side, "NO")
	tick := 0.01
	if m, ok := s.kmkt(p.ticker); ok {
		tick = m.TickBelow(p.px)
	}
	if tick <= 0 {
		tick = 0.01
	}
	newest := p.tapeCutoff
	for _, t := range tape { // newest-first
		if t.Ticker != p.ticker {
			continue
		}
		ts, err := time.Parse(time.RFC3339, t.CreatedTime)
		if err != nil {
			continue
		}
		if !ts.After(p.tapeCutoff) {
			break // newest-first: everything older is already processed
		}
		if ts.After(newest) {
			newest = ts
		}
		// our resting BUY on side X is hit when the taker's outcome side is the OPPOSITE
		agg := strings.ToLower(t.Aggressor())
		if (isNo && agg != "yes") || (!isNo && agg != "no") {
			continue
		}
		tp := float64(t.YesPrice)
		if isNo {
			tp = 1 - tp
		}
		ct := float64(t.Count)
		switch queueTradeClass(tp, p.px, tick) {
		case queueTradeThrough: // traded THROUGH by at least one venue tick — queue and our order consumed
			p.queueLeft = 0
			p.filledCt = p.contracts
		case queueTradeAt: // traded AT the level — price-time priority
			if p.queueLeft > 0 {
				use := math.Min(ct, p.queueLeft)
				p.queueLeft -= use
				ct -= use
			}
			if ct > 0 && p.filledCt < p.contracts {
				p.filledCt = math.Min(p.contracts, p.filledCt+ct)
			}
		}
	}
	p.tapeCutoff = newest
}

// ---- budgeted REST orderbook fallback (Part 3: fix the invisible books) ----------------------

const (
	restBookTTL    = 60 * time.Second // one REST look per ticker per minute is plenty at decision cadence
	restBookBudget = 30               // max REST orderbook fetches per minute (protects the token bucket)
)

// restBookDepth returns total visible depth for a ticker via a budgeted REST orderbook fetch,
// used ONLY when both the book-WS working set and the kob cache miss. Results (including observed
// EMPTY books — that is a real observation, not a miss) cache for restBookTTL.
func (s *Server) restBookDepth(ticker string) (float64, bool) {
	if s.kal == nil {
		return 0, false
	}
	s.restBookMu.Lock()
	if s.restBookAt == nil {
		s.restBookAt = map[string]time.Time{}
		s.restBookD = map[string]float64{}
	}
	if at, ok := s.restBookAt[ticker]; ok && time.Since(at) < restBookTTL {
		d := s.restBookD[ticker]
		s.restBookMu.Unlock()
		return d, true
	}
	if time.Since(s.restBookW) > time.Minute {
		s.restBookW, s.restBookN = time.Now(), 0
	}
	if s.restBookN >= restBookBudget {
		s.restBookMu.Unlock()
		return 0, false
	}
	s.restBookN++
	s.restBookMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ob, err := s.kal.GetOrderbook(ctx, ticker)
	if err != nil || ob == nil {
		return 0, false
	}
	d := 0.0
	for _, l := range ob.YesBids {
		d += l.Size
	}
	for _, l := range ob.YesAsks {
		d += l.Size
	}
	s.restBookMu.Lock()
	s.restBookAt[ticker] = time.Now()
	s.restBookD[ticker] = d
	if len(s.restBookAt) > 4000 { // bound the caches
		for k, at := range s.restBookAt {
			if time.Since(at) > restBookTTL {
				delete(s.restBookAt, k)
				delete(s.restBookD, k)
			}
		}
	}
	s.restBookMu.Unlock()
	return d, true
}
