package server

// R133 book-native ML observations.
//
// The legacy signal price remains useful context, but it is not an executable quote.  This file
// captures a separate, versioned, side-specific snapshot only from a complete live book.  A row
// stays version 0 when any required component is absent: no midpoint reconstruction, no 1-price
// reconstruction from a single side, no default fee, and no old-depth/new-BBO pairing.

import (
	"fmt"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

const (
	// The numeric feature layout is unchanged from R133 v1; the v2 cohort is identified by the
	// new pricing/label provenance strings. Keeping layout version 1 preserves non-ML research
	// consumers while old rows remain excluded from ML by their blank provenance.
	mlBookFeatureVersion = 1
	mlBookFeatureSchema  = "book-native-v2"
)

type mlBookTouch struct {
	MakerPrice float64
	TakerPrice float64
	MakerDepth float64
	TakerDepth float64
}

// mlNormalizeBookSide converts one internally consistent YES book into the outcome-space book
// owned by the signal.  Kalshi and PolyUS both expose a YES/long-denominated BBO; a NO buyer rests
// against the YES ask and crosses the YES bid.  Both source sides must exist, so complements here
// are exact binary-book identities rather than a fabricated opposite quote from one visible price.
func mlNormalizeBookSide(side string, yesBid, yesAsk, yesBidDepth, yesAskDepth float64) (mlBookTouch, bool) {
	if yesBid <= 0 || yesAsk <= yesBid || yesAsk >= 1 || yesBidDepth <= 0 || yesAskDepth <= 0 {
		return mlBookTouch{}, false
	}
	var out mlBookTouch
	switch strings.ToUpper(strings.TrimSpace(side)) {
	case "YES", "UP":
		out = mlBookTouch{MakerPrice: yesBid, TakerPrice: yesAsk, MakerDepth: yesBidDepth, TakerDepth: yesAskDepth}
	case "NO", "DOWN":
		out = mlBookTouch{MakerPrice: 1 - yesAsk, TakerPrice: 1 - yesBid, MakerDepth: yesAskDepth, TakerDepth: yesBidDepth}
	default:
		return mlBookTouch{}, false
	}
	if out.MakerPrice <= 0 || out.TakerPrice <= out.MakerPrice || out.TakerPrice >= 1 {
		return mlBookTouch{}, false
	}
	return out, true
}

func finiteMLBook(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }

// finalizeMLSignalProvenance opts a row into the prospective v2 label cohort only after the
// executable-pricing snapshot has completed. A visible price or liveness classification alone
// leaves both version gates closed.
func finalizeMLSignalProvenance(sig *storage.Signal) {
	if sig == nil || sig.BookFeatureVer != mlBookFeatureVersion || sig.PricingVersion != mlBookFeatureSchema {
		return
	}
	switch strings.ToLower(strings.TrimSpace(sig.Platform)) {
	case "kalshi":
		if sig.SecsToStart == nil || (*sig.SecsToStart > 0 && sig.IsLive != 0) ||
			(*sig.SecsToStart <= 0 && sig.IsLive != -1 && sig.IsLive != 1) {
			return // no historical is_live contamination may wear the prospective label version
		}
	case "polyus":
		if sig.IsLive != 0 && sig.IsLive != 1 {
			return
		}
		if sig.SecsToStart != nil && *sig.SecsToStart > 0 && sig.IsLive != 0 {
			return
		}
	default:
		return
	}
	sig.LabelVersion = sigLiveLabelVersion
}

func (s *Server) mlPUSBookMeta(slug string) (polyUSMarket, bool) {
	theta, tick, ok := s.polyUSFreshFeeAuthority(slug)
	if !ok || tick <= 0 || !finiteMLBook(tick) {
		return polyUSMarket{}, false
	}
	// Copy theta before taking its address: the returned metadata is an immutable receipt derived
	// from the fresh complete sweep, not an alias into a mutable discovery cache.
	return polyUSMarket{Slug: strings.ToLower(strings.TrimSpace(slug)), TickSize: tick, FeeCoeff: &theta}, true
}

// stampMLBookSnapshot is cache/WS-only and never blocks on venue I/O. BookFeatureVer is assigned
// last, after every invariant and exact fee receipt passes, so partial failures remain honest
// version-0 rows with NULL fields.
func (s *Server) stampMLBookSnapshot(sig *storage.Signal) {
	s.stampMLBookSnapshotMode(sig, false)
}

// stampMLBookSnapshotSequenced is the funded-Paper cache-only variant. It reads the ladder and its
// WebSocket generation/subscription/sequence under the same lock, so the delayed IOC can compare
// two real receipts without a REST lookup or a second provenance read.
func (s *Server) stampMLBookSnapshotSequenced(sig *storage.Signal) {
	s.stampMLBookSnapshotMode(sig, true)
}

func (s *Server) stampMLBookSnapshotMode(sig *storage.Signal, sequenceStamped bool) {
	if s == nil || sig == nil || sig.BookFeatureVer != 0 || sig.Ticker == "" {
		return
	}
	var touch mlBookTouch
	var quoteAge, makerTick, takerTick, makerFee, takerFee float64
	var source string
	var ok bool

	switch strings.ToLower(strings.TrimSpace(sig.Platform)) {
	case "kalshi":
		if s.kal == nil {
			return
		}
		var yesBid, yesAsk, bidDepth, askDepth float64
		if sequenceStamped {
			book, age, provenance, live := s.kal.LiveBookWithProvenance(sig.Ticker, 5*time.Second)
			if !live || book == nil || len(book.YesBids) == 0 || len(book.YesAsks) == 0 ||
				provenance.Generation == 0 || provenance.SubscriptionID <= 0 ||
				provenance.Sequence <= 0 || provenance.ReceivedAt.IsZero() {
				return
			}
			yesBid, yesAsk = book.YesBids[0].Price, book.YesAsks[0].Price
			bidDepth, askDepth = book.YesBids[0].Size, book.YesAsks[0].Size
			quoteAge = age.Seconds()
			source = fmt.Sprintf("kalshi_ws_full_orderbook:g%d:s%d:q%d",
				provenance.Generation, provenance.SubscriptionID, provenance.Sequence)
		} else {
			book, age, live := s.kal.LiveBook(sig.Ticker, 5*time.Second)
			if !live || book == nil || len(book.YesBids) == 0 || len(book.YesAsks) == 0 {
				return
			}
			yesBid, yesAsk = book.YesBids[0].Price, book.YesAsks[0].Price
			bidDepth, askDepth = book.YesBids[0].Size, book.YesAsks[0].Size
			quoteAge, source = age.Seconds(), "kalshi-ws-depth"
		}
		touch, ok = mlNormalizeBookSide(sig.Side, yesBid, yesAsk, bidDepth, askDepth)
		if !ok {
			return
		}
		market, found := s.kmkt(sig.Ticker)
		if !found {
			// kmkts is only the tape/warm subset. The depth planner deliberately selects from the
			// complete cached board, so a perfectly fresh subscribed WS book can exist for a ticker
			// absent from kmkts. Recover only its already-resident full-board metadata here; this
			// never calls REST or weakens the fresh atomic-book requirement above.
			market, found = s.kal.MarketCached(sig.Ticker)
		}
		if !found {
			return // exact tick/taper schedule is part of book-v1, not an optional default
		}
		if market.Result != "" || (market.Status != "" && !strings.EqualFold(market.Status, "active") &&
			!strings.EqualFold(market.Status, "open")) {
			return
		}
		closeRaw := market.ExpectedExpiration
		if closeRaw == "" {
			closeRaw = market.CloseTime
		}
		if closeRaw != "" {
			if closeAt, err := time.Parse(time.RFC3339, closeRaw); err != nil || !closeAt.After(time.Now()) {
				return // malformed/ended lifecycle is not a current executable observation
			}
		}
		var makerTickKnown, takerTickKnown bool
		makerTick, makerTickKnown = market.TickForKnown(touch.MakerPrice)
		takerTick, takerTickKnown = market.TickForKnown(touch.TakerPrice)
		if !makerTickKnown || !takerTickKnown {
			return
		}
		var makerKnown, takerKnown bool
		makerFee, makerKnown, _ = s.kalFeeExact(sig.Ticker, true, 1, touch.MakerPrice)
		takerFee, takerKnown, _ = s.kalFeeExact(sig.Ticker, false, 1, touch.TakerPrice)
		if !makerKnown || !takerKnown {
			return
		}

	case "polyus":
		if s.polyUSWS == nil {
			return
		}
		yesBid, yesAsk, bidDepth, askDepth, at, live := s.polyUSWS.FullBookTouchAt(sig.Ticker)
		if !live {
			return
		}
		touch, ok = mlNormalizeBookSide(sig.Side, yesBid, yesAsk, bidDepth, askDepth)
		if !ok {
			return
		}
		market, marketFound := s.mlPUSBookMeta(sig.Ticker)
		makerTick, takerTick = market.TickSize, market.TickSize
		feeKnown := marketFound && market.FeeCoeff != nil && validPolyUSFeeTheta(*market.FeeCoeff)
		if feeKnown {
			makerFee = polyUSFeeWithTheta(true, 1, touch.MakerPrice, *market.FeeCoeff)
			takerFee = polyUSFeeWithTheta(false, 1, touch.TakerPrice, *market.FeeCoeff)
		}
		marketFound = feeKnown && makerTick > 0
		if !marketFound {
			return
		}
		quoteAge, source = time.Since(at).Seconds(), "polyus-ws-full"

	default:
		return // Poly-int stays research-only and is excluded from this tradeable ML cohort.
	}

	if !finiteMLBook(quoteAge) || quoteAge < 0 || !finiteMLBook(makerTick) || makerTick <= 0 ||
		!finiteMLBook(takerTick) || takerTick <= 0 || !finiteMLBook(makerFee) || !finiteMLBook(takerFee) {
		return
	}
	bid, ask, bidDepth, askDepth := touch.MakerPrice, touch.TakerPrice, touch.MakerDepth, touch.TakerDepth
	sig.BookBid, sig.BookAsk = &bid, &ask
	sig.BookBidDepth, sig.BookAskDepth = &bidDepth, &askDepth
	sig.BookQuoteAgeS = &quoteAge
	sig.BookMakerTick, sig.BookTakerTick = &makerTick, &takerTick
	sig.BookMakerFeePC, sig.BookTakerFeePC = &makerFee, &takerFee
	sig.BookSource = source
	sig.PricingVersion = mlBookFeatureSchema
	sig.BookFeatureVer = mlBookFeatureVersion
}

// handleMLBookPx is the current-decision twin of the stored observation. It exposes only complete
// book-v1 snapshots and never falls back to a midpoint/last trade/REST estimate. The sidecar uses
// this for final ranking and its paper fill gate; an absent key means wait for a real book.
func (s *Server) handleMLBookPx(w http.ResponseWriter, r *http.Request) {
	type row struct {
		FeatureSchema  string   `json:"feature_schema"`
		Version        int      `json:"book_feature_version"`
		LiveAuthority  bool     `json:"live_authority"`
		MakerPrice     float64  `json:"maker_price"`
		TakerPrice     float64  `json:"taker_price"`
		MakerDepth     float64  `json:"maker_depth"`
		TakerDepth     float64  `json:"taker_depth"`
		QuoteAgeS      float64  `json:"quote_age_s"`
		MakerTick      float64  `json:"maker_tick"`
		TakerTick      float64  `json:"taker_tick"`
		MakerFeePC     float64  `json:"maker_fee_pc"`
		TakerFeePC     float64  `json:"taker_fee_pc"`
		ExitPrice      float64  `json:"exit_price"`
		ExitTakerFeePC float64  `json:"exit_taker_fee_pc"`
		ExitFeeKnown   bool     `json:"exit_fee_known"`
		ExitFeeSource  string   `json:"exit_fee_source"`
		FeeKnown       bool     `json:"fee_known"`
		MakerFeeSource string   `json:"maker_fee_source"`
		TakerFeeSource string   `json:"taker_fee_source"`
		LatencyMS      *float64 `json:"latency_ms,omitempty"`
		Source         string   `json:"source"`
	}
	out := make(map[string]row)
	for _, item := range strings.Split(r.URL.Query().Get("q"), ",") {
		parts := strings.SplitN(item, "|", 3)
		if len(parts) != 3 {
			continue
		}
		sig := storage.Signal{Platform: strings.ToLower(strings.TrimSpace(parts[0])),
			Ticker: strings.TrimSpace(parts[1]), Side: strings.ToUpper(strings.TrimSpace(parts[2]))}
		s.stampMLBookSnapshot(&sig)
		if sig.BookFeatureVer != mlBookFeatureVersion || sig.BookBid == nil || sig.BookAsk == nil ||
			sig.BookBidDepth == nil || sig.BookAskDepth == nil || sig.BookQuoteAgeS == nil ||
			sig.BookMakerTick == nil || sig.BookTakerTick == nil ||
			sig.BookMakerFeePC == nil || sig.BookTakerFeePC == nil {
			continue
		}
		feeSource := "kalshi:authoritative-fee-registry"
		exitFee := 0.0
		exitKnown := false
		if sig.Platform == "polyus" {
			feeSource = polyUSFeeAuthoritySource
			if market, ok := s.mlPUSBookMeta(sig.Ticker); ok && market.FeeCoeff != nil {
				exitFee = polyUSFeeWithTheta(false, 1, *sig.BookBid, *market.FeeCoeff)
				exitKnown = finiteMLBook(exitFee)
			}
		} else {
			exitFee, exitKnown, _ = s.kalFeeExact(sig.Ticker, false, 1, *sig.BookBid)
		}
		if !exitKnown {
			continue // an unpriced exit is not an honest horizon-close receipt
		}
		out[item] = row{FeatureSchema: mlBookFeatureSchema, Version: sig.BookFeatureVer,
			MakerPrice: *sig.BookBid, TakerPrice: *sig.BookAsk,
			MakerDepth: *sig.BookBidDepth, TakerDepth: *sig.BookAskDepth,
			QuoteAgeS: *sig.BookQuoteAgeS, MakerTick: *sig.BookMakerTick, TakerTick: *sig.BookTakerTick,
			MakerFeePC: *sig.BookMakerFeePC, TakerFeePC: *sig.BookTakerFeePC,
			ExitPrice: *sig.BookBid, ExitTakerFeePC: exitFee, ExitFeeKnown: true, ExitFeeSource: feeSource,
			FeeKnown: true, MakerFeeSource: feeSource, TakerFeeSource: feeSource,
			LatencyMS: sig.BookLatencyMS, Source: sig.BookSource}
	}
	writeJSON(w, http.StatusOK, out)
}
