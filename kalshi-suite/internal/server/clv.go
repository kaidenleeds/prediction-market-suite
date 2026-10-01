package server

// clv.go — R127-D (B4): the CLV SCORECARD — closing-line value per strategy family.
//
// CLV = (final pre-settlement price − entry price), per contract, measured in the DIRECTION OF
// THE POSITION — the classic sportsbook sharpness measure: did our entries beat the close?
// A family can lose money after fees and still show positive CLV (it reads the market earlier
// than the crowd), and vice versa; that's exactly why it earns its own card next to the P&L
// verdicts instead of being folded into them.
//
// DATA SOURCE + HONEST CAVEATS (do not oversell this number):
//   - signal_log.post_path holds the side-directional price marks appended while a filled row
//     was held (AppendSignalPostPath, throttled sampling, capped ~1200 chars) or reconstructed
//     from 1-min candles by the backfill worker (SetSignalPostPathByID, ≤6h window). The LAST
//     mark of that path is therefore an APPROXIMATION of the close — a sampled breadcrumb, not
//     the true last tick. Live-appended paths that hit the length cap stopped sampling early;
//     candle backfills stop 6h after signal time. Both writers store the marks ALREADY
//     side-adjusted (a NO hold tracks 1−yes), and entry_price is the side's own cost, so
//     CLV = tail − entry needs no side flip here.
//   - Marks are formatted '%.3f' by both writers (exactly 5 chars in [0,1]); the SQL tail
//     extraction (storage.FamilyCLVStats) only accepts that exact shape and NULLs anything else,
//     so odd legacy tokens drop out instead of corrupting the mean.
//   - No fee adjustment: CLV is a price-reading measure, not P&L (the league table above it is
//     the fee-net money view).
//   - The CI is a rough normal ±1.96·sd/√n — context only, NOT the verdict engine's always-valid
//     confidence sequence. Nothing arms or retires off this card.
//
// SURFACES: GET /api/clv (the card) + ONE plain line in the briefing scoreboard (briefscore.go)
// for families with at least 20 distinct markets after within-market averaging. Compute is a
// single grouped SQL over resolved rows (90d),
// cached 10 min; a MonitorPaper warm pass keeps briefing renders on the cache. On query
// error/timeout the last card is served and the next window retries — a render never blocks.

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"sort"
	"time"
)

// clvBriefMinMarkets — a family needs this many distinct markets after within-market averaging
// to make the briefing line. Repeated snapshots/episodes from one ticker never manufacture n.
const clvBriefMinMarkets = 20

type clvEnt struct {
	Family  string  `json:"family"`
	Plain   string  `json:"plain_name"`
	N       int     `json:"n"`           // settled CLV observations
	Markets int     `json:"markets"`     // distinct market tickers; the independent sample bucket shown to humans
	MeanC   float64 `json:"clv_cents"`   // mean CLV in ¢/contract; positive = our entries beat the close
	LoC     float64 `json:"ci_lo_cents"` // rough 95% normal CI (context only — see file header)
	HiC     float64 `json:"ci_hi_cents"`
}

// computeCLV returns the per-family CLV card, best first. 10-min cached.
func (s *Server) computeCLV(ctx context.Context) []clvEnt {
	s.clvMu.Lock()
	// The 10-min window gates on clvAt alone (not a non-nil cache): after an error/empty pass the
	// backoff must still hold, or a stalled DB would re-pay the scan on every briefing render.
	if !s.clvAt.IsZero() && time.Since(s.clvAt) < 10*time.Minute {
		out := append([]clvEnt(nil), s.clvCache...)
		s.clvMu.Unlock()
		return out
	}
	s.clvMu.Unlock()
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second) // briefCollectingCount doctrine: bounded, never blocks a render
	fams, err := s.store.FamilyCLVStats(cctx, time.Now().AddDate(0, 0, -90))
	cancel()
	s.clvMu.Lock()
	defer s.clvMu.Unlock()
	if err != nil {
		s.clvAt = time.Now() // back off on the same 10-min window; keep serving the last card
		return append([]clvEnt(nil), s.clvCache...)
	}
	out := make([]clvEnt, 0, len(fams))
	for _, f := range fams {
		e := clvEnt{Family: f.Family, Plain: famPlainName(f.Family), N: f.N,
			Markets: f.Markets, MeanC: vRnd4(f.Mean * 100)}
		e.LoC, e.HiC = e.MeanC, e.MeanC
		if f.Markets > 1 && f.SD > 0 {
			r := 1.96 * f.SD / math.Sqrt(float64(f.Markets)) * 100
			e.LoC, e.HiC = vRnd4(e.MeanC-r), vRnd4(e.MeanC+r)
		}
		out = append(out, e)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].MeanC != out[j].MeanC {
			return out[i].MeanC > out[j].MeanC
		}
		return out[i].Family < out[j].Family // deterministic under ties
	})
	s.clvCache, s.clvAt = out, time.Now()
	return append([]clvEnt(nil), out...)
}

// clvLine formats the ONE briefing line from a best-first card (pure — pinned in r127b_test.go).
// Only families with n ≥ clvBriefMinMarkets distinct tickers count; "" = nothing measurable yet.
func clvLine(ents []clvEnt) string {
	measured := make([]clvEnt, 0, len(ents))
	for _, e := range ents {
		if e.Markets >= clvBriefMinMarkets {
			measured = append(measured, e)
		}
	}
	if len(measured) == 0 {
		return ""
	}
	best, worst := measured[0], measured[len(measured)-1]
	if len(measured) == 1 {
		return fmt.Sprintf("📐 signal CLV (sampled): %s %+.1f¢ · n%d %s · 1 family",
			best.Plain, best.MeanC, best.Markets, briefSampleBucket(best.Markets))
	}
	return fmt.Sprintf("📐 signal CLV (sampled): best %s %+.1f¢ · n%d %s | worst %s %+.1f¢ · n%d %s · %d families",
		best.Plain, best.MeanC, best.Markets, briefSampleBucket(best.Markets),
		worst.Plain, worst.MeanC, worst.Markets, briefSampleBucket(worst.Markets), len(measured))
}

// clvBriefLine — the briefing hook (briefscore.go writes it right after the league table).
func (s *Server) clvBriefLine(ctx context.Context) string { return clvLine(s.computeCLV(ctx)) }

// handleCLV — GET /api/clv: the scorecard endpoint.
func (s *Server) handleCLV(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"method": "Signal CLV = last post_path mark − discovery price, side-directional, per contract (no fees). " +
			"post_path tail ≈ the close: sampled/backfilled marks, NOT the true last tick — see clv.go header. " +
			"This is timing evidence, not executable maker/taker proof; route economics use captured books. " +
			"Each ticker is averaged first; n is the distinct venue+ticker sample. Repeated rows are diagnostics only. " +
			"CI is a rough normal interval across market means " +
			"(context only; the verdict engine's CS is the money verdict).",
		"window_days": 90,
		"families":    s.computeCLV(r.Context()),
	})
}
