package server

// paperhorizon.go owns the entry-horizon contracts for executable Paper/live routes. Singles and
// final LIVE handoffs remain regular <=4h / crypto <=2h. The two funded Paper Combo portfolios may
// collect farther ahead (regular <=24h / crypto <=6h), but still require an authoritative positive
// clock and recheck the shorter 4h/2h contract before any future LIVE handoff.

import (
	"context"
	"math"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/config"
	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
)

const (
	paperRegularHardMaxHours      = 4.0
	paperCryptoHardMaxHours       = 2.0
	paperComboRegularHardMaxHours = 24.0
	paperComboCryptoHardMaxHours  = 6.0
	paperMLRegularHardMaxHours    = 24.0
	paperMLCryptoHardMaxHours     = 6.0
)

func paperEntryHorizonLimits(a config.AutoConfig) (regular, crypto float64) {
	regular = paperRegularHardMaxHours
	if a.ConsensusMaxHoursOut > 0 && a.ConsensusMaxHoursOut < regular {
		regular = a.ConsensusMaxHoursOut
	}
	crypto = paperCryptoHardMaxHours
	if a.ConsensusCryptoMaxHoursOut > 0 && a.ConsensusCryptoMaxHoursOut < crypto {
		crypto = a.ConsensusCryptoMaxHoursOut
	}
	return regular, crypto
}

// liveComboEntryHorizonLimits is the final handoff contract. It deliberately retains the same
// 4h/2h ceiling as LIVE singles even though the Paper Combo collectors can study a wider window.
func liveComboEntryHorizonLimits(a config.AutoConfig) (regular, crypto float64) {
	regular = paperRegularHardMaxHours
	if a.ParlayMaxHoursOut > 0 && a.ParlayMaxHoursOut < regular {
		regular = a.ParlayMaxHoursOut
	}
	crypto = paperCryptoHardMaxHours
	if a.ParlayMaxHoursOut > 0 && a.ParlayMaxHoursOut < crypto {
		crypto = a.ParlayMaxHoursOut
	}
	if a.ConsensusCryptoMaxHoursOut > 0 && a.ConsensusCryptoMaxHoursOut < crypto {
		crypto = a.ConsensusCryptoMaxHoursOut
	}
	return regular, crypto
}

// paperComboEntryHorizonLimits is collection/admission for the funded System Combo and ML Combo
// Paper portfolios. Dedicated knobs can tighten, but never widen, the 24h/6h Paper ceilings.
func paperComboEntryHorizonLimits(a config.AutoConfig) (regular, crypto float64) {
	regular = paperComboRegularHardMaxHours
	if a.PaperComboMaxHoursOut > 0 && a.PaperComboMaxHoursOut < regular {
		regular = a.PaperComboMaxHoursOut
	}
	crypto = paperComboCryptoHardMaxHours
	if a.PaperComboCryptoMaxHoursOut > 0 && a.PaperComboCryptoMaxHoursOut < crypto {
		crypto = a.PaperComboCryptoMaxHoursOut
	}
	if a.PaperComboMaxHoursOut > 0 && a.PaperComboMaxHoursOut < crypto {
		crypto = a.PaperComboMaxHoursOut
	}
	return regular, crypto
}

// paperMLEntryHorizonLimits is the wider collection contract only for book-native-v2 Paper
// singles. It never authorizes LIVE; dispatchLiveMirror and each venue handler still use 4h/2h.
func paperMLEntryHorizonLimits(a config.AutoConfig) (regular, crypto float64) {
	regular = paperMLRegularHardMaxHours
	if a.PaperMLMaxHoursOut > 0 && a.PaperMLMaxHoursOut < regular {
		regular = a.PaperMLMaxHoursOut
	}
	crypto = paperMLCryptoHardMaxHours
	if a.PaperMLCryptoMaxHoursOut > 0 && a.PaperMLCryptoMaxHoursOut < crypto {
		crypto = a.PaperMLCryptoMaxHoursOut
	}
	if a.PaperMLMaxHoursOut > 0 && a.PaperMLMaxHoursOut < crypto {
		crypto = a.PaperMLMaxHoursOut
	}
	return regular, crypto
}

func paperEntryHorizonEligible(resolveHours, limitHours float64) bool {
	return resolveHours > 0 && limitHours > 0 && resolveHours <= limitHours &&
		!math.IsNaN(resolveHours) && !math.IsInf(resolveHours, 0) &&
		!math.IsNaN(limitHours) && !math.IsInf(limitHours, 0)
}

// paperEntryHorizonValueKnown distinguishes an authoritative/cached positive clock from the zero
// sentinel used by cache-only signal producers. Unknown must be refreshed before a funded route
// rejects it; NaN/Inf are equally non-authoritative and receive the same bounded refresh attempt.
func paperEntryHorizonValueKnown(resolveHours float64) bool {
	return resolveHours > 0 && !math.IsNaN(resolveHours) && !math.IsInf(resolveHours, 0)
}

// paperEntryHorizonOK applies the canonical contract to a horizon captured in the same decision
// pass as the executable quote. Include title because PolyUS crypto slugs/titles are not always
// encoded like Kalshi tickers.
func (s *Server) paperEntryHorizonOK(resolveHours float64, ticker, title string) bool {
	regular, crypto := paperEntryHorizonLimits(s.cfg().Auto)
	limit := regular
	if isCryptoTicker(ticker + " " + title) {
		limit = crypto
	}
	return paperEntryHorizonEligible(resolveHours, limit)
}

func (s *Server) paperComboEntryHorizonOK(resolveHours float64, ticker, title string) bool {
	limit := s.paperComboEntryHorizonLimit(ticker, title)
	return paperEntryHorizonEligible(resolveHours, limit)
}

func (s *Server) paperComboEntryHorizonLimit(ticker, title string) float64 {
	regular, crypto := paperComboEntryHorizonLimits(s.cfg().Auto)
	if isCryptoTicker(ticker + " " + title) {
		return crypto
	}
	return regular
}

func (s *Server) liveComboEntryHorizonLimit(ticker, title string) float64 {
	regular, crypto := liveComboEntryHorizonLimits(s.cfg().Auto)
	if isCryptoTicker(ticker + " " + title) {
		return crypto
	}
	return regular
}

func (s *Server) paperMLEntryHorizonLimit(ticker, title string) float64 {
	regular, crypto := paperMLEntryHorizonLimits(s.cfg().Auto)
	if isCryptoTicker(ticker + " " + title) {
		return crypto
	}
	return regular
}

// fundedKalshiResultAt returns the clock a funded order may trust for when money can actually be
// resolved. Golf's expected_expiration_time is routinely a tee/start-style estimate while the
// round result is not final until later. For golf, use the later venue close clock; if that clock
// is absent, fail closed instead of manufacturing a four-hour result promise.
func fundedKalshiResultAt(m kalshi.Market) (time.Time, bool) {
	if subcentGolfMarket(m) {
		closeAt, closeOK := parseTime(strings.TrimSpace(m.CloseTime))
		if !closeOK {
			return time.Time{}, false
		}
		if expectedAt, expectedOK := parseTime(strings.TrimSpace(m.ExpectedExpiration)); expectedOK &&
			expectedAt.After(closeAt) {
			return expectedAt, true
		}
		return closeAt, true
	}
	return parseTime(firstNonEmpty(m.ExpectedExpiration, m.CloseTime))
}

// paperEntryResolveHours reads a strict money clock. sigResolveHours intentionally keeps a
// just-ended PolyUS game at +0.1h for research/settlement discovery; funded entry must not inherit
// that research convenience. Here the actual estimated end is allowed to become non-positive, so
// closed/past PolyUS markets fail exactly like Kalshi markets. Tests must opt into the seam instead
// of relying on testServer's default.
func (s *Server) paperEntryResolveHours(ctx context.Context, platform, ticker, title string) float64 {
	if s.paperHorizonFn != nil {
		return s.paperHorizonFn(ctx, platform, ticker, title)
	}
	if strings.EqualFold(platform, "kalshi") {
		// The urgent AUTO lane must never turn a clock check into a public market request. Its
		// current complete-board receipt already carries the funded close/expiration fields and
		// lifecycle state used by the executable-book boundary. A missing resident row or clock is
		// therefore an exact fail-closed result, not permission to fall through to the catalog or
		// sigResolveHours -> GetMarket recovery path used by background Paper/research work.
		if r159KalshiResidentAdmissionOnly(ctx) {
			m, ok := s.r159KalshiResidentMarket(ticker)
			if !ok {
				return 0
			}
			ts, parsed := fundedKalshiResultAt(m)
			if !parsed {
				return 0
			}
			return time.Until(ts).Hours()
		}
		// Prefer already-ingested venue timestamps. This avoids turning a transient REST failure into
		// "unknown" when the hot market cache or complete cursor catalog already owns the same clock.
		if m, ok := s.kmkt(ticker); ok {
			if ts, parsed := fundedKalshiResultAt(m); parsed {
				return time.Until(ts).Hours()
			}
			if subcentGolfMarket(m) {
				return 0
			}
		}
		// The durable catalog stores one legacy Kalshi clock and cannot prove whether a golf value
		// came from expected_expiration_time or the later close_time. Without current full market
		// metadata, golf must fail closed rather than reintroduce the tee-time bug through fallback.
		if subcentGolfMarket(kalshi.Market{Ticker: ticker, Title: title}) {
			return 0
		}
		if closeTS, _, ok := s.store.CatalogCloseTS(ctx, "kalshi", ticker); ok {
			if ts, parsed := parseTime(closeTS); parsed {
				return time.Until(ts).Hours()
			}
		}
	}
	if strings.EqualFold(platform, "polyus") {
		const gameLen = 3.5
		for _, m := range s.polyUSSnapshot() {
			if m.Slug != ticker {
				continue
			}
			// PolyUS' market/event `endDate` is the scheduled event boundary used by the catalog,
			// not a reliable settlement timestamp. The research clock and catalog therefore treat it
			// as game start and add the same bounded game-length estimate below. Using m.Resolve raw
			// here made one signal report (for example) 1.7h to resolution while the final funded gate
			// declared the identical live match already elapsed. That silently starved otherwise valid
			// PolyUS Paper candidates and made Paper/LIVE use a different clock from collection.
			if start, err := parsePolyTime(m.Start); err == nil {
				hours := time.Until(start.Add(time.Duration(gameLen * float64(time.Hour)))).Hours()
				// `Live` proves only that the venue currently labels the market in-play; it is not an
				// authoritative close/settlement clock. Once the bounded start+game estimate elapses,
				// returning a fresh +2h forever would let an arbitrarily old game re-enter every money
				// lane. Preserve the real negative clock and fail closed until a real later end is known.
				return hours
			}
			// A LIVE flag without a parseable start/end proves tradeability, not time remaining.
			// Horizon-gated Paper/LIVE routes require both and therefore refuse this row.
			break
		}
		if startHours, ok := s.pusCatalogHours(ctx, ticker); ok {
			return startHours + gameLen
		}
		return 0
	}
	return s.sigResolveHours(ctx, platform, ticker)
}

// paperEntryHorizonNow revalidates a resting Paper quote at post/fill time. Unknown and past
// clocks fail closed; a stale decision-time horizon can never authorize a later fill.
func (s *Server) paperEntryHorizonNow(ctx context.Context, platform, ticker, title string) bool {
	hours := s.paperEntryResolveHours(ctx, platform, ticker, title)
	return s.paperEntryHorizonOK(hours, ticker, title)
}

func (s *Server) paperComboEntryHorizonNow(ctx context.Context, platform, ticker, title string) bool {
	hours := s.paperEntryResolveHours(ctx, platform, ticker, title)
	return s.paperComboEntryHorizonOK(hours, ticker, title)
}

func (s *Server) paperMLEntryHorizonNow(ctx context.Context, platform, ticker, title string) bool {
	hours := s.paperEntryResolveHours(ctx, platform, ticker, title)
	return paperEntryHorizonEligible(hours, s.paperMLEntryHorizonLimit(ticker, title))
}

// paperRouteEntryHorizonNow selects the wider Paper window only for the current book-native ML
// executor. All other systems keep the 4h/2h funded-single contract.
func (s *Server) paperRouteEntryHorizonNow(ctx context.Context, platform, ticker, title, source string) bool {
	if strings.EqualFold(strings.TrimSpace(source), "auto-ml") {
		return s.paperMLEntryHorizonNow(ctx, platform, ticker, title)
	}
	return s.paperEntryHorizonNow(ctx, platform, ticker, title)
}

func (s *Server) paperComboEntryHorizonDecision(ctx context.Context, platform, ticker, title string) (hours, limit float64, ok bool) {
	hours = s.paperEntryResolveHours(ctx, platform, ticker, title)
	limit = s.paperComboEntryHorizonLimit(ticker, title)
	return hours, limit, paperEntryHorizonEligible(hours, limit)
}

func (s *Server) liveComboEntryHorizonDecision(ctx context.Context, platform, ticker, title string) (hours, limit float64, ok bool) {
	hours = s.paperEntryResolveHours(ctx, platform, ticker, title)
	limit = s.liveComboEntryHorizonLimit(ticker, title)
	return hours, limit, paperEntryHorizonEligible(hours, limit)
}
