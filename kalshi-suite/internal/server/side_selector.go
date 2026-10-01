package server

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

const (
	sideClassTwo    = "true_two_side_selector"
	sideClassOne    = "one_semantically_valid_side"
	sideClassMulti  = "multi_leg"
	sideClassRouter = "router_or_data_control"
)

type systemSideSelectionRow struct {
	Family         string `json:"family"`
	SideClass      string `json:"side_class"`
	DiscoveryClass string `json:"discovery_class"`
	Rule           string `json:"rule"`
}

type nativeSideDecision struct {
	Family, Venue, EmittedSide   string
	SelectedFamily, SelectedSide string
	Ask, Depth, Fee, Mean, Lo    float64
	FeeSource, BookSource        string
	State, Reason                string
}

var trueTwoSideFamilies = map[string]bool{
	"auto-ml": true, "independent-probabilistic-weather": true, "ml-book": true,
	"outcome-set-expansion-shock": true, "paired-bridge-inversion": true,
	"proper-score-brier": true, "proper-score-executor": true, "proper-score-log": true,
	"proper-score-spherical": true, "score-state-surface": true, "sharpline": true,
	"subcent-golf": true, "weather-curve-residual": true,
}

var oneSideFamilies = map[string]bool{
	"kalshi-whale": true, "kalshi-flow": true, "kflow": true, "polyus-whale": true,
	"polyus-flow": true, "polyus-consensus": true, "poly-whale": true, "poly-consensus": true,
	"pflow": true, "pcrypto": true, "kcrypto": true, "kthresh": true, "xmatch": true,
	"pmatch": true, "pbridge": true, "confluence": true, "cross": true, "favlong": true,
	"basket": true, "fade": true, "divergence": true, "insider": true, "skillbuy": true,
	"whale-exit": true, "whale-exit-hold-bridge": true, "meanrev": true, "xvlag": true, "xvgap": true, "spotlag": true,
	"arb": true, "pfbridge": true, "fbridge": true, "freshlist": true, "freshfade": true,
	"xinv-pcrypto": true, "bookskew": true, "fundtilt": true, "wxedge": true,
	"poly-pred-kalshi": true, "xvgap2": true, "xvgapk": true, "notail": true,
	"weather-curve-revision": true, "kflow-pre": true, "kflow-live": true, "weather": true,
	"book:freshinv-k": true, "book:favlong80": true, "book:cheapband-k": true,
	"book:freshlist-p": true, "book:xvgap": true, "book:cheapband-p": true, "rawflow": true,
	"settlement-latency-carry": true, "series-roll-anchor": true,
	"side-normalized-crowding-fade": true, "maker-salvage-matched-cohort": true,
}

var multiLegFamilies = map[string]bool{
	"xvlock": true, "lockstack": true, "combo-overlay": true,
	"combo-synth": true, "combo-rfq": true, "payoff-constraint-solver": true,
	"identity-challenged-cross-venue-lock": true, "incentive-subsidized-structural-lock": true,
	"event-basket-lock@kalshi": true, "event-basket-lock@polyus": true,
	"nested-ladder-lock": true, "time-nested-lock": true, "joint-marginal-lock": true,
	"rfq-sim": true, "parlay-2leg": true, "parlay-3leg": true, "parlay-4leg": true,
	"parlay-5leg": true, "parlay-6leg": true, "deadline-hazard-surface": true,
}

var routerFamilies = map[string]bool{
	"manual": true, "maker-fills": true, "queue-priority": true, "incentive-maker": true,
	"lifecycle-reopen": true, "systems-regimes": true, "behavioral-bias-regime": true,
	"fee-rounding-batch": true, "forecast-persona-router": true,
	"replenishment-fingerprint": true, "clientele-clock-basis": true,
	"collateral-release-rotation": true, "semantic-complexity-premium": true,
	"attention-spillover-graph": true, "flow-direction-integrity": true,
	"raw-flow-observer@kalshi": true, "raw-flow-observer@polyus": true,
	"raw-flow-observer@polymarket": true, "adaptive-whale-scorer@kalshi": true,
	"adaptive-whale-scorer@polyus": true, "adaptive-whale-scorer@polymarket": true,
}

var singleTriggerFamilies = map[string]bool{
	"subcent-golf": true, "kalshi-whale": true, "kalshi-flow": true, "kflow": true,
	"polyus-whale": true, "polyus-flow": true, "polyus-consensus": true, "poly-whale": true,
	"poly-consensus": true, "favlong": true, "meanrev": true, "freshlist": true,
	"freshfade": true, "whale-exit-hold-bridge": true, "bookskew": true, "kflow-pre": true, "kflow-live": true,
	"book:freshinv-k": true, "book:favlong80": true, "book:cheapband-k": true,
	"book:freshlist-p": true, "book:cheapband-p": true, "rawflow": true,
}

var notDiscoveryFamilies = map[string]bool{
	"manual": true, "maker-fills": true, "queue-priority": true, "lifecycle-reopen": true,
	"systems-regimes": true, "fee-rounding-batch": true, "flow-direction-integrity": true,
	"raw-flow-observer@kalshi": true, "raw-flow-observer@polyus": true,
	"raw-flow-observer@polymarket": true,
}

func sideSelectionBaseFamily(family string) string {
	v := strings.TrimSpace(family)
	for _, prefix := range []string{"taker:", "maker:", "unit:", "book:"} {
		if strings.HasPrefix(strings.ToLower(v), prefix) {
			v = v[len(prefix):]
			break
		}
	}
	if strings.HasPrefix(strings.ToLower(v), "gf:") {
		v = v[3:]
		v = strings.TrimSuffix(strings.TrimSuffix(v, "-k"), "-p")
	}
	if strings.HasPrefix(strings.ToLower(v), "side-control:") {
		return strings.ToLower(strings.TrimSpace(v))
	}
	if i := strings.Index(v, "@"); i > 0 {
		v = v[:i]
	}
	if i := strings.Index(v, " ["); i > 0 {
		v = v[:i]
	}
	return strings.ToLower(strings.TrimSpace(v))
}

func systemSideClassification(family string) (sideClass, discovery, rule string) {
	exact := strings.ToLower(strings.TrimSpace(family))
	base := sideSelectionBaseFamily(family)
	key := exact
	if !trueTwoSideFamilies[key] && !oneSideFamilies[key] && !multiLegFamilies[key] && !routerFamilies[key] {
		key = base
	}
	discovery = "multi_indicator"
	if notDiscoveryFamilies[exact] || notDiscoveryFamilies[key] {
		discovery = "not_discovery"
	} else if singleTriggerFamilies[exact] || singleTriggerFamilies[key] {
		discovery = "single_trigger"
	}
	// The executable System registry is authoritative for registered systems. Historical display
	// maps below still classify legacy/native families, but cannot downgrade a registered exact
	// single-order system into a non-originating router (or turn a typed bundle into a single).
	if spec, ok := storage.SystemExecutionCapability(base); ok {
		switch spec.HandoffClass {
		case storage.SystemHandoffBundle, storage.SystemHandoffUnsupported:
			if spec.ActionClass == storage.SystemActionBundle {
				return sideClassMulti, "multi_indicator", "ordered legs require their own books, fees, payoff certificate and partial-fill envelope"
			}
		case storage.SystemHandoffChild:
			return sideClassRouter, discovery, "selects, vetoes, or overlays a separately named executable child and cannot originate a naked opposite-side order"
		case storage.SystemHandoffSingle:
			if spec.SupportsSide("YES") && spec.SupportsSide("NO") {
				return sideClassTwo, discovery, "YES and NO collect separate executable asks, depth, fees and side-specific proof; choose the stronger positive lower bound or abstain"
			}
			return sideClassOne, discovery, "only the registered executable side is eligible; algebraic inversion is diagnostic only"
		}
	}
	if multiLegFamilies[key] {
		return sideClassMulti, "multi_indicator", "ordered legs require their own books, fees, payoff certificate and partial-fill envelope"
	}
	if routerFamilies[key] {
		return sideClassRouter, discovery, "may route, veto, rank or validate data but cannot originate an opposite-side order"
	}
	if trueTwoSideFamilies[key] {
		return sideClassTwo, discovery, "YES and NO collect separate executable asks, depth, fees and side-specific proof; choose the stronger positive lower bound or abstain"
	}
	if strings.HasPrefix(base, "side-control:") {
		return sideClassOne, "single_trigger", "opposite observation control only; no order authority without its own sealed system proof"
	}
	if strings.HasPrefix(base, "invert:") || strings.HasPrefix(base, "xinv-") {
		return sideClassOne, discovery, "explicit independently named opposite expression; never derived inside an order path"
	}
	return sideClassOne, discovery, "only the emitted semantic side is eligible; algebraic inversion is diagnostic only"
}

func sideSelectionReceipt(coverage []leaderboardCoverageRow) map[string]any {
	seen := map[string]bool{}
	rows := make([]systemSideSelectionRow, 0, len(coverage))
	counts := map[string]int{sideClassTwo: 0, sideClassOne: 0, sideClassMulti: 0, sideClassRouter: 0,
		"multi_indicator": 0, "single_trigger": 0, "not_discovery": 0}
	for _, c := range coverage {
		if strings.TrimSpace(c.Family) == "" || seen[c.Family] {
			continue
		}
		seen[c.Family] = true
		sc, dc, rule := systemSideClassification(c.Family)
		rows = append(rows, systemSideSelectionRow{Family: c.Family, SideClass: sc, DiscoveryClass: dc, Rule: rule})
		counts[sc]++
		counts[dc]++
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Family < rows[j].Family })
	registeredRows, registeredCounts := registeredSideSelectionRows()
	return map[string]any{
		"rule":   "for a true two-side system, YES and NO are independent executable routes; select the larger current positive route lower bound or abstain. Never infer order authority as 1 minus a result.",
		"counts": counts, "rows": rows, "registered_counts": registeredCounts,
		"registered_rows":   registeredRows,
		"opposite_exposure": "all Paper and LIVE dispatches still pass the shared same-market opposite-side conflict guard",
	}
}

func registeredSideSelectionRows() ([]systemSideSelectionRow, map[string]int) {
	seen := map[string]bool{}
	rows := make([]systemSideSelectionRow, 0, 110)
	for _, registry := range []map[string]bool{trueTwoSideFamilies, oneSideFamilies, multiLegFamilies, routerFamilies} {
		for family := range registry {
			if seen[family] {
				continue
			}
			seen[family] = true
			sc, dc, rule := systemSideClassification(family)
			rows = append(rows, systemSideSelectionRow{Family: family, SideClass: sc, DiscoveryClass: dc, Rule: rule})
		}
	}
	counts := map[string]int{sideClassTwo: 0, sideClassOne: 0, sideClassMulti: 0, sideClassRouter: 0,
		"multi_indicator": 0, "single_trigger": 0, "not_discovery": 0}
	for _, row := range rows {
		counts[row.SideClass]++
		counts[row.DiscoveryClass]++
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Family < rows[j].Family })
	return rows, counts
}

func (s *Server) selectNativeModelSide(ctx context.Context, family, venue, ticker, emittedSide string) nativeSideDecision {
	out := nativeSideDecision{Family: family, Venue: strings.ToLower(strings.TrimSpace(venue)),
		EmittedSide: strings.ToUpper(strings.TrimSpace(emittedSide)), State: "ABSTAIN"}
	if out.EmittedSide != "YES" && out.EmittedSide != "NO" {
		out.Reason = "invalid-emitted-side"
		return out
	}
	// The generic signal row carries one current score only. A true two-side model must therefore
	// emit each scored side itself. A one-side model may additionally own an explicitly named
	// invert:<family> strategy: that route is considered only when its own prospective executable
	// one-share ledger exists. The direct row is never algebraically reused as inverse proof.
	type candidate struct{ family, side string }
	sides := []candidate{{family: family, side: out.EmittedSide}}
	if inverse, ok := independentInverseSignalIdentity(storage.Signal{
		SignalType: family, Platform: out.Venue, Ticker: ticker, Side: out.EmittedSide,
	}); ok {
		sides = append(sides, candidate{family: inverse.SignalType, side: inverse.Side})
	}
	bestLo := math.Inf(-1)
	for _, candidate := range sides {
		ask, depth, bookSource, ok := s.executableAsk(ctx, out.Venue, ticker, candidate.side, true)
		if !ok || ask <= 0 || ask >= 1 || depth < 1 {
			continue
		}
		fee, feeSource, feeKnown := s.unitTrialFeeExact(out.Venue, ticker, ask)
		if !feeKnown || strings.TrimSpace(feeSource) == "" {
			continue
		}
		u, found, err := s.store.UnitTrialRouteLeaderboard(ctx, time.Now().UTC(), candidate.family, out.Venue, "model", candidate.side)
		if err != nil || !found {
			continue
		}
		state, mean, lo := liveMirrorUnitAdjusted(u, ask, fee)
		if state != "PROVEN+" || lo < s.liveMirrorEdgeFloor() || mean < s.liveMirrorEdgeFloor() {
			continue
		}
		if lo > bestLo {
			bestLo = lo
			out.SelectedFamily, out.SelectedSide, out.Ask, out.Depth, out.Fee = candidate.family, candidate.side, ask, depth, fee
			out.Mean, out.Lo, out.FeeSource, out.BookSource = mean, lo, feeSource, bookSource
		}
	}
	if out.SelectedSide == "" {
		out.Reason = "no-side-has-positive-mature-executable-lower-bound"
		return out
	}
	out.State, out.Reason = "SELECT", "positive-independent-side-specific-lower-bound; inverse route, when selected, owns separate quote and outcome history"
	return out
}

// independentInverseSignalIdentity defines the mirrored strategy at the signal boundary, before
// any order path runs. One-side detectors and true two-side models may each own a separately
// measured contrarian rule for an emitted side; routers/multi-leg systems cannot manufacture a
// directional position. Price/book/model fields are cleared because
// the opposite side must be quoted independently; copying the emitted-side values would recreate
// the exact counterfactual-price bug this contract replaces.
func independentInverseSignalIdentity(sig storage.Signal) (storage.Signal, bool) {
	family := strings.TrimSpace(sig.SignalType)
	platform := strings.ToLower(strings.TrimSpace(sig.Platform))
	side := strings.ToUpper(strings.TrimSpace(sig.Side))
	if family == "" || (platform != "kalshi" && platform != "polyus") ||
		(side != "YES" && side != "NO") {
		return storage.Signal{}, false
	}
	lowFamily := strings.ToLower(family)
	if strings.HasPrefix(lowFamily, "invert:") || strings.HasPrefix(lowFamily, "side-control:") ||
		strings.HasPrefix(lowFamily, "counterfactual:") {
		return storage.Signal{}, false // no nested inverse and no upgrade of legacy controls
	}
	// A two-side forecaster may also have an independently measured contrarian rule for each
	// emitted side. It is still a separate named system with its own opposite book and settlement;
	// only routers/multi-leg systems and non-trading controls lack a coherent one-leg inverse.
	if sideClass, discovery, _ := systemSideClassification(family); (sideClass != sideClassOne && sideClass != sideClassTwo) || discovery == "not_discovery" {
		return storage.Signal{}, false
	}
	out := sig
	out.Platform = platform
	out.SignalType = "invert:" + family
	if side == "YES" {
		out.Side = "NO"
	} else {
		out.Side = "YES"
	}
	out.EntryPrice, out.BookDepth, out.SpreadCents = 0, 0, 0
	out.FeePC, out.ModelProb = nil, nil
	out.PxAgeS = nil
	out.BookFeatureVer = 0
	out.BookBid, out.BookAsk, out.BookBidDepth, out.BookAskDepth = nil, nil, nil, nil
	out.BookQuoteAgeS, out.BookMakerTick, out.BookTakerTick = nil, nil, nil
	out.BookMakerFeePC, out.BookTakerFeePC, out.BookLatencyMS = nil, nil, nil
	out.BookSource, out.LabelVersion, out.PricingVersion = "", "", ""
	out.ExecExpr = fmt.Sprintf("independent-inverse-system/base=%s/emitted=%s/discovery-price=%.6f", family, side, sig.EntryPrice)
	if lineage, crossVenue := r148CrossVenueLineage(sig); crossVenue {
		// Preserve the side that actually fired at the source. The candidate's Inverted flag owns
		// the opposite economic exposure at the destination; rewriting source-side here made the
		// audit trail falsely claim a NO source signal had fired when the real source emitted YES.
		out.ExecExpr += fmt.Sprintf("/%s/source=%s:%s/source-side=%s/certificate=%s",
			r148CrossVenueSignalMarker, lineage.SourceVenue, lineage.SourceTicker,
			lineage.SourceSide, lineage.CertificateHash)
	}
	if topology := r147SignalInputTopology(sig); topology != "" {
		out.ExecExpr += "/" + r147InputReceiptExpr(topology, r147SignalInputObservedAt(sig))
	}
	return out, true
}

// resetExecutableBookSnapshot makes every decision-time refresh all-or-nothing. In particular,
// stampMLBookSnapshot must never see an older complete receipt and return early while the caller
// believes it repriced the route.
func resetExecutableBookSnapshot(sig *storage.Signal) {
	if sig == nil {
		return
	}
	sig.BookFeatureVer = 0
	sig.BookBid, sig.BookAsk, sig.BookBidDepth, sig.BookAskDepth = nil, nil, nil, nil
	sig.BookQuoteAgeS, sig.BookMakerTick, sig.BookTakerTick = nil, nil, nil
	sig.BookMakerFeePC, sig.BookTakerFeePC, sig.BookLatencyMS = nil, nil, nil
	sig.BookSource, sig.LabelVersion, sig.PricingVersion = "", "", ""
}

// currentCompleteBookSignal returns one internally consistent, side-specific, executable book
// receipt. It requires both sides of the book, both touch depths, the venue's actual tick schedule,
// current lifecycle/freshness authority, and exact maker+taker fee schedules. It never falls back
// to a midpoint, last trade, or one-sided quote. A PolyUS signal outside the bounded 1,000-depth WS
// prefix may use the rate/concurrency-limited authoritative OPEN full-book REST overflow receipt.
func (s *Server) currentCompleteBookSignal(sig storage.Signal) (storage.Signal, string, bool) {
	return s.currentCompleteBookSignalMode(sig, true, false)
}

// currentCompleteBookSignalCached is the funded Paper follower's deliberately lower-priority
// boundary. It may consume a complete already-subscribed venue book, but it cannot start PolyUS
// overflow REST work that competes with a real-money candidate. Collection and LIVE keep the
// overflow-capable wrapper above.
func (s *Server) currentCompleteBookSignalCached(sig storage.Signal) (storage.Signal, string, bool) {
	return s.currentCompleteBookSignalMode(sig, false, true)
}

func (s *Server) currentCompleteBookSignalMode(sig storage.Signal, allowPolyUSOverflow,
	sequenceStampedKalshi bool) (storage.Signal, string, bool) {
	out := sig
	resetExecutableBookSnapshot(&out)
	if sequenceStampedKalshi && strings.EqualFold(out.Platform, "kalshi") {
		s.stampMLBookSnapshotSequenced(&out)
	} else {
		s.stampMLBookSnapshot(&out)
	}
	if allowPolyUSOverflow && out.BookFeatureVer != mlBookFeatureVersion && strings.EqualFold(out.Platform, "polyus") {
		s.stampPolyUSOnDemandBookSnapshot(&out)
	}
	if out.BookFeatureVer != mlBookFeatureVersion || out.PricingVersion != mlBookFeatureSchema ||
		out.BookBid == nil || out.BookAsk == nil || out.BookBidDepth == nil || out.BookAskDepth == nil ||
		out.BookQuoteAgeS == nil || out.BookMakerTick == nil || out.BookTakerTick == nil ||
		out.BookMakerFeePC == nil || out.BookTakerFeePC == nil || strings.TrimSpace(out.BookSource) == "" {
		return storage.Signal{}, "", false
	}
	bid, ask := *out.BookBid, *out.BookAsk
	if !finiteMLBook(bid) || !finiteMLBook(ask) || bid <= 0 || ask <= bid || ask >= 1 ||
		!finiteMLBook(*out.BookBidDepth) || !finiteMLBook(*out.BookAskDepth) ||
		*out.BookBidDepth < 1 || *out.BookAskDepth < 1 ||
		!finiteMLBook(*out.BookQuoteAgeS) || *out.BookQuoteAgeS < 0 ||
		!finiteMLBook(*out.BookMakerTick) || *out.BookMakerTick <= 0 ||
		!finiteMLBook(*out.BookTakerTick) || *out.BookTakerTick <= 0 ||
		!finiteMLBook(*out.BookMakerFeePC) || !finiteMLBook(*out.BookTakerFeePC) {
		return storage.Signal{}, "", false
	}
	fee, feeSource, feeKnown := s.fillFeeReceipt(out.Platform, out.Ticker, false, 1, ask)
	if !feeKnown || strings.TrimSpace(feeSource) == "" || math.Abs(fee-*out.BookTakerFeePC) > 1e-9 {
		return storage.Signal{}, "", false
	}
	out.EntryPrice = ask
	out.BookDepth = *out.BookAskDepth
	out.SpreadCents = (ask - bid) * 100
	out.FeePC = &fee
	return out, feeSource, true
}

// currentIndependentInverseSignal binds the named inverse to the actual opposite-side taker book.
// It never uses 1-entry_price. Both this preflight and the eventual Paper/LIVE-ready shared route
// repeat a complete side-specific book, depth, tick, lifecycle and exact-fee check.
func (s *Server) currentIndependentInverseSignal(ctx context.Context, sig storage.Signal, allowREST bool) (storage.Signal, bool) {
	_ = ctx       // retained for the stable call contract; the bounded overflow owns its own deadline
	_ = allowREST // currentCompleteBookSignal alone decides whether authoritative full depth is valid
	out, ok := independentInverseSignalIdentity(sig)
	if !ok {
		return storage.Signal{}, false
	}
	out, feeSource, ok := s.currentCompleteBookSignal(out)
	if !ok {
		return storage.Signal{}, false
	}
	out.PricingVersion = "independent-inverse-executable-v1"
	out.ExecExpr += "/book=" + out.BookSource + "/fee=" + feeSource
	return out, true
}
