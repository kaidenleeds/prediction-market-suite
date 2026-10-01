// sharpline.go — sharp-odds fair-value anchor (audit Q7-C1, LOG-ONLY).
//
// De-vigged Pinnacle h2h lines (via The Odds API) are the best public estimate of a game's true
// probability — an oracle for the sports-heavy Kalshi book and a free CLV grader for every sports
// signal. This scaffold fetches h2h odds for the major leagues, removes the vig, matches each team
// to its Kalshi moneyline market, and LOGS a "sharpline" signal whenever Kalshi deviates from the
// sharp fair value by ≥2¢. Nothing trades on it: log-only → Edge finder → DECIDES, per the
// promotion discipline (3–4 weeks of accrual, then per-league net-EV t-LB > 0 at n≥600 before any
// probation Kelly).
//
// Requires an Odds API key (config: odds_api_key; ~$30–99/mo for the volume this cadence needs).
// Without a key the scaffold is inert.
package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

var sharplineSports = []string{
	"baseball_mlb", "basketball_nba", "icehockey_nhl", "americanfootball_nfl", "soccer_epl",
}

type oddsEvent struct {
	HomeTeam     string `json:"home_team"`
	AwayTeam     string `json:"away_team"`
	CommenceTime string `json:"commence_time"` // R69: game start (ISO8601) — date evidence for the crossMatchKalshi date guard
	Bookmakers   []struct {
		Key     string `json:"key"`
		Markets []struct {
			Key      string `json:"key"`
			Outcomes []struct {
				Name  string  `json:"name"`
				Price float64 `json:"price"` // decimal odds
			} `json:"outcomes"`
		} `json:"markets"`
	} `json:"bookmakers"`
}

// sweepSharpline fetches sharp h2h odds and logs Kalshi deviations. Self-throttled (default 30 min;
// config sharpline_minutes) and budget-aware — each sweep is len(sharplineSports) requests.
func (s *Server) sweepSharpline(ctx context.Context) {
	key := strings.TrimSpace(s.cfg().Auto.OddsAPIKey)
	if key == "" {
		key = strings.TrimSpace(os.Getenv("ODDS_API_KEY")) // same env-fallback pattern as the venue keys
	}
	// R122 (Part 3 logging-continuity): sharpline_enabled NO LONGER gates this log-only family —
	// the standing rule is that config/book state gates PLACEMENT only, and sharpline has no
	// placement path. The key still parses (deprecated no-op, promotion_min_rows precedent);
	// keyless remains the one honest off-state (venue data unavailable).
	if key == "" {
		// R90 DO-THIS 3 (OPERATOR OVERRIDE of the auditor's "re-enter the key" ask): keyless is a
		// clean, SUPPORTED state — a DAILY INFO breadcrumb while keyless (R117/auditor 366).
		// No warnings, no red readiness, and the famine sentinel never grades sharpline while
		// keyless (sse.go famineCadence skips it). Revival is HOT: this sweep re-reads the config
		// every pass, so pasting odds_api_key into Settings starts logging within one
		// sharpline_minutes cycle — no restart.
		if key == "" && time.Since(s.sharpKeylessAt) >= 24*time.Hour {
			s.sharpKeylessAt = time.Now()
			s.log.Info("sharpline idle (no key) — daily breadcrumb (auditor 366); set odds_api_key in Settings to enable; revives hot, no restart")
		}
		return
	}
	every := time.Duration(s.cfg().Auto.SharplineMinutes) * time.Minute
	if every <= 0 {
		every = 30 * time.Minute
	}
	if time.Since(s.sharpAt) < every {
		return
	}
	s.sharpAt = time.Now()
	// Kalshi match pool: the liquid window + FRESH tape-discovered markets (same union as the arb matcher).
	kmarkets, _ := s.kal.GetTopLiquidMarkets(ctx)
	s.metaMu.Lock()
	for tk, m := range s.kmkts {
		if at, ok := s.kmktsAt[tk]; ok && time.Since(at) <= 15*time.Minute {
			// R122 (auditor 462): closed/settled markets never enter the twin-match pool (the mark
			// paths keep re-stamping settled tickers fresh — see the pmatch/arbBlob sites).
			if m.Result != "" || (m.Status != "" && !strings.EqualFold(m.Status, "active")) {
				continue
			}
			kmarkets = append(kmarkets, m)
		}
	}
	s.metaMu.Unlock()
	if len(kmarkets) == 0 {
		return
	}
	client := &http.Client{Timeout: 10 * time.Second}
	logged := 0
	for _, sport := range sharplineSports {
		if ctx.Err() != nil || logged >= 40 {
			return
		}
		u := "https://api.the-odds-api.com/v4/sports/" + url.PathEscape(sport) +
			"/odds?regions=eu&markets=h2h&bookmakers=pinnacle&apiKey=" + url.QueryEscape(key)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			continue
		}
		resp, err := client.Do(req)
		if err != nil {
			// R122 (auditor 466): transport errors were swallowed bare while non-200s warned —
			// the dark-family class (120/198 precedent). Same Warn shape as the status branch.
			s.log.Warn("sharpline fetch failed", "sport", sport, "err", err)
			continue
		}
		var events []oddsEvent
		derr := json.NewDecoder(resp.Body).Decode(&events)
		_ = resp.Body.Close()
		if resp.StatusCode != 200 || derr != nil {
			s.log.Warn("sharpline fetch failed", "sport", sport, "status", resp.StatusCode)
			continue
		}
		sourceReceivedAt := time.Now()
		for _, ev := range events {
			// R69: the event's commence_time is the date evidence crossMatchKalshi's new date guard
			// runs on (its sibling bestKalshiMatch always had a ±14h window; this matcher had none —
			// same two teams' NEXT series game could token-match). Unparseable/absent → guard silent.
			evDate, evDateOK := parseTime(strings.TrimSpace(ev.CommenceTime))
			// de-vig Pinnacle's h2h: p_i = (1/odds_i) / Σ(1/odds_j) across ALL listed outcomes
			// (2-way or 3-way with draw — normalization handles both).
			type fair struct {
				team string
				p    float64
			}
			var fairs []fair
			for _, bk := range ev.Bookmakers {
				if !strings.EqualFold(bk.Key, "pinnacle") {
					continue
				}
				for _, mk := range bk.Markets {
					if mk.Key != "h2h" {
						continue
					}
					sum := 0.0
					for _, o := range mk.Outcomes {
						if o.Price > 1 {
							sum += 1 / o.Price
						}
					}
					if sum <= 0 {
						continue
					}
					for _, o := range mk.Outcomes {
						if o.Price > 1 {
							fairs = append(fairs, fair{team: o.Name, p: (1 / o.Price) / sum})
						}
					}
				}
			}
			for _, fv := range fairs {
				if strings.EqualFold(fv.team, "Draw") || fv.p <= 0.05 || fv.p >= 0.95 {
					continue // draws have no clean Kalshi moneyline twin; extremes carry no usable edge
				}
				question := ev.HomeTeam + " vs " + ev.AwayTeam + " " + fv.team + " wins"
				// R70: crossMatchKalshi now returns (ticker, title, 0–1 fraction) — previously it
				// returned (title, url, PERCENT), so the ticker column carried a title and the price
				// band below rejected every row (kPx was 2–98, never inside 0.02–0.98). Fixed at the
				// source; this caller's fraction math below has been correct all along.
				ticker, _, kPx, ok := crossMatchKalshi(question, kmarkets, evDate, evDateOK)
				if !ok || kPx <= 0.02 || kPx >= 0.98 {
					continue
				}
				dev := fv.p - kPx // >0 ⇒ Kalshi UNDERPRICES the team vs the sharp fair value
				if dev < 0.02 && dev > -0.02 {
					continue // inside the noise band — not a signal
				}
				side, sidePx := "YES", kPx
				if dev < 0 {
					side, sidePx = "NO", 1-kPx
				}
				if sidePx <= 0.02 || sidePx >= 0.98 {
					continue
				}
				_ = s.insertSignal(ctx, storage.Signal{
					Platform: "kalshi", Ticker: ticker, Title: question, Side: side,
					SignalType: "sharpline", EntryPrice: sidePx,
					Strength:   dev,  // signed sharp-vs-Kalshi deviation (the edge estimate)
					Underlying: fv.p, // the de-vigged sharp fair probability (the anchor itself)
					Category:   "Sports",
					ExecExpr:   r147InputReceiptExpr("K-SPORTSBOOK", sourceReceivedAt),
					// R99 bug 200 (auditor r25): stamp the family's OWN key — borrowing kalshi-flow's
					// realized prior leaked a foreign family's track record into this column (an ML
					// feature). Unmapped families read betConfidence's neutral 0.55 baseline — honest.
					Confidence: s.signalConfidence("sharpline", sidePx),
				})
				logged++
			}
		}
	}
	if logged > 0 {
		_ = s.store.Audit(ctx, "info", "signal", fmt.Sprintf("sharpline logged %d Kalshi-vs-Pinnacle deviations (log-only)", logged), "")
	}
}
