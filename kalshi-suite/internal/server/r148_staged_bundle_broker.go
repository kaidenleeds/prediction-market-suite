package server

// Production adapter for the append-only staged bundle coordinator. The adapter is deliberately
// Kalshi-only: Kalshi supplies a client_order_id and durable order-id/fill lookup, while PolyUS
// currently supplies neither an idempotency key nor an intent-only recovery lookup. A PolyUS leg
// is therefore durably refused before an execution intent exists; it is never "best effort" live.

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/polymarketus"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

type r148ServerStagedBroker struct {
	s *Server

	// Narrow seams keep adapter/recovery tests hermetic. Production constructors leave these nil.
	quoteFn        func(context.Context, storage.ResearchRouteBundleLeg, string, float64) (r148StagedQuote, error)
	identityFn     func(context.Context, storage.ResearchRouteBundle) error
	boundaryFn     func(context.Context, storage.ResearchRouteBundle, *storage.ResearchRouteBundleLeg, string, bool) string
	wireQuoteFn    func(storage.ResearchRouteBundleLeg, string, float64) (r148StagedQuote, error)
	beforeCreateFn func()
	createFn       func(context.Context, kalshi.OrderRequest) (*kalshi.CreateOrderResult, error)
	orderFn        func(context.Context, string) (kalshi.Order, error)
	fillsFn        func(context.Context, string) ([]kalshi.Fill, error)
	clientOrdersFn func(context.Context, string, string, time.Time) ([]kalshi.Order, error)
	feeFn          func(string, bool, float64, float64, bool) (float64, bool, string)
	navFn          func(context.Context, string) (float64, string)
	headroomFn     func(context.Context, string) (float64, error)
	clusterFn      func(context.Context, storage.ResearchRouteBundle, float64) (float64, error)
	pusOrderFn     func(context.Context, string) (polymarketus.OrderState, error)
	pusPosFn       func(context.Context) ([]polymarketus.PUSPosition, error)
	pusActFn       func(context.Context) ([]polymarketus.PUSActivity, error)
	pusOpenFn      func(context.Context) ([]polymarketus.PUSOrder, error)
	pusMetaFn      func(context.Context, string) (polymarketus.MarketMeta, error)
	riskBaselineFn func(context.Context, storage.ResearchRouteBundle) (map[string]r148LiveRiskBaseline, error)
	riskReceiptFn  func(context.Context, string, storage.ResearchRouteBundleLeg, string, r148StagedReceipt) error
	now            func() time.Time
}

const r148PUSStagedBlockReason = "PolyUS staged execution is fail-closed: Retail has no client-order-id and Activities has no orderId/side/action join for intent-only recovery"

func newR148ServerStagedBroker(s *Server) *r148ServerStagedBroker {
	return &r148ServerStagedBroker{s: s}
}

func (b *r148ServerStagedBroker) stagedLiveMoneyPolicyGeneration() uint64 {
	if b == nil || b.s == nil {
		return 0
	}
	return b.s.liveMoneyPolicyGeneration.Load()
}

func (b *r148ServerStagedBroker) clock() time.Time {
	if b.now != nil {
		return b.now().UTC()
	}
	return time.Now().UTC()
}

func (b *r148ServerStagedBroker) durableBlock(ctx context.Context, bundle storage.ResearchRouteBundle, reason string) error {
	err := errors.New(reason)
	if b.s == nil || b.s.store == nil {
		return err
	}
	key := "r148_staged_block_" + r148Hash(map[string]string{"bundle": bundle.BundleID, "reason": reason})[:20]
	if _, exists := b.s.store.KVGet(ctx, key); !exists {
		if persistErr := b.s.store.KVSet(ctx, key, b.clock().Format(time.RFC3339Nano)); persistErr != nil {
			return fmt.Errorf("%s; durable external-block receipt failed: %w", reason, persistErr)
		}
		_ = b.s.store.Audit(ctx, "warn", "live", "staged bundle refused before venue intent", reason)
	}
	return err
}

func (b *r148ServerStagedBroker) allowlistReason(bundle storage.ResearchRouteBundle) string {
	allowed, why := parseR148StagedBundleAllowlist(b.s.cfg().Risk.LiveStagedBundleAllowlist)
	if why != "" {
		return why
	}
	if len(allowed) == 0 {
		return "live-staged-bundle-allowlist-empty"
	}
	for _, leg := range bundle.Legs {
		key := strings.Join([]string{strings.ToLower(leg.Venue), strings.ToLower(bundle.SystemID),
			strings.ToUpper(leg.Side), "taker"}, "|")
		if _, ok := allowed[key]; !ok {
			return "live-staged-bundle-identity-not-allowlisted:" + key
		}
	}
	return ""
}

const r148StagedAllowlistMax = 12

func parseR148StagedBundleAllowlist(raw string) (map[string]struct{}, string) {
	parts := strings.FieldsFunc(raw, func(r rune) bool { return r == ',' || r == ';' || r == '\n' || r == '\r' })
	out := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		fields := strings.Split(strings.TrimSpace(part), "|")
		if len(fields) != 4 {
			return nil, "live-staged-bundle-allowlist-entry-must-be-venue|system|side|taker"
		}
		venue := strings.ToLower(strings.TrimSpace(fields[0]))
		system := strings.ToLower(strings.TrimSpace(fields[1]))
		side := strings.ToUpper(strings.TrimSpace(fields[2]))
		route := strings.ToLower(strings.TrimSpace(fields[3]))
		if venue != "kalshi" || (system != "event-basket-lock" && system != "nested-ladder-lock" &&
			system != "time-nested-lock" && system != "xvlock") || (side != "YES" && side != "NO") || route != "taker" {
			return nil, "live-staged-bundle-allowlist-entry-invalid"
		}
		out[strings.Join([]string{venue, system, side, route}, "|")] = struct{}{}
		if len(out) > r148StagedAllowlistMax {
			return nil, "live-staged-bundle-allowlist-exceeds-twelve-identities"
		}
	}
	return out, ""
}

func (b *r148ServerStagedBroker) boundaryReason(ctx context.Context, bundle storage.ResearchRouteBundle,
	leg *storage.ResearchRouteBundleLeg, action string, includeRisk bool) string {
	if b.boundaryFn != nil {
		return b.boundaryFn(ctx, bundle, leg, action, includeRisk)
	}
	if b.s == nil || b.s.store == nil {
		return "staged production broker is unavailable"
	}
	if !b.s.liveAutoCombosEnabled() {
		return "AUTO combo belt is disabled"
	}
	if !b.s.cfg().Risk.LiveStagedBundles {
		return "live staged-bundle belt is disabled"
	}
	for _, row := range bundle.Legs {
		if row.Venue == "polyus" {
			return r148PUSStagedBlockReason
		}
	}
	if why := b.s.liveWriteBoundaryReason(true); why != "" {
		return why
	}
	if why := b.allowlistReason(bundle); why != "" {
		return why
	}
	if err := b.currentIdentity(ctx, bundle); err != nil {
		return "current payoff/rule/lifecycle certificate failed: " + err.Error()
	}
	legs := bundle.Legs
	if leg != nil {
		legs = []storage.ResearchRouteBundleLeg{*leg}
	}
	for _, row := range legs {
		switch row.Venue {
		case "kalshi":
			if b.s.kal == nil || !b.s.kal.HasCredentials() || !b.s.kal.Armed() {
				return "authenticated and armed Kalshi order client is unavailable"
			}
			if blocked, end := kalshiMaintenanceWindow(b.clock()); blocked {
				return "Kalshi maintenance blocks staged orders until " + end.UTC().Format(time.RFC3339)
			}
		case "polyus":
			return r148PUSStagedBlockReason
		default:
			return "unsupported staged execution venue: " + row.Venue
		}
		if why := b.s.liveContractLimitReason(row.Quantity); why != "" {
			return why
		}
		if !b.s.paperEntryHorizonNow(ctx, row.Venue, row.Ticker, "") {
			return "market is outside the current funded horizon or has an unknown/past lifecycle: " + row.Ticker
		}
	}
	if includeRisk && leg != nil && strings.EqualFold(action, "BUY") {
		q, err := b.Quote(ctx, *leg, "BUY", leg.Quantity)
		if err != nil {
			return err.Error()
		}
		cost := q.Price*leg.Quantity + q.Fee
		candidate := liveMirrorCandidate{Platform: leg.Venue, Ticker: leg.Ticker,
			Side: leg.Side, Family: bundle.SystemID}
		if why := b.s.liveVenueBudgetCheckFor(ctx, leg.Venue, cost, &candidate); why != "" {
			return why
		}
		if why := b.s.liveRiskCheck(cost, leg.Venue); why != "" {
			return why
		}
	}
	return ""
}

func (b *r148ServerStagedBroker) currentStagedNAV(ctx context.Context, venue string) (float64, string) {
	if b.navFn != nil {
		return b.navFn(ctx, venue)
	}
	return b.s.liveVenueBankroll(ctx, venue)
}

func (b *r148ServerStagedBroker) currentStagedVenueHeadroom(ctx context.Context, venue string) (float64, error) {
	if b.headroomFn != nil {
		return b.headroomFn(ctx, venue)
	}
	k, p := b.s.liveVenueRemaining(ctx)
	if venue == "kalshi" && k > 0 {
		return k, nil
	}
	if venue == "polyus" && p > 0 {
		return p, nil
	}
	return 0, errors.New("current venue/combined 50% exposure headroom is unavailable")
}

// currentKalshiRelatedHeadroom computes the smallest remaining 10%-of-current-NAV rail across
// every canonical event touched by the bundle.  The full bundle cost is charged to each cluster:
// an additive route cannot split one related position into smaller-looking per-leg tickets.
func (b *r148ServerStagedBroker) currentKalshiRelatedHeadroom(ctx context.Context,
	bundle storage.ResearchRouteBundle, nav float64) (float64, error) {
	if b.clusterFn != nil {
		return b.clusterFn(ctx, bundle, nav)
	}
	cap, ok := liveRailLimit(nav, b.s.cfg().Risk.LiveClusterCapPct, -1)
	if !ok || b.s.kal == nil {
		return 0, errors.New("current related-event 10% rail is unavailable")
	}
	targets := map[string]bool{}
	for _, leg := range bundle.Legs {
		if leg.Venue != "kalshi" {
			continue
		}
		targets[b.s.liveMirrorClusterKey("kalshi", leg.Ticker, "")] = true
	}
	if len(targets) == 0 {
		return 0, errors.New("staged bundle has no canonical related-event identity")
	}
	var (
		positions []kalshi.MarketPosition
		orders    []kalshi.Order
		posErr    error
		ordErr    error
		wg        sync.WaitGroup
	)
	wg.Add(2)
	go func() { defer wg.Done(); positions, posErr = b.s.kal.GetPositions(ctx) }()
	go func() { defer wg.Done(); orders, ordErr = b.s.kal.GetOrders(ctx) }()
	wg.Wait()
	if posErr != nil || ordErr != nil {
		return 0, errors.New("current Kalshi related-event exposure is unreadable")
	}
	exposure := make(map[string]float64, len(targets))
	for _, p := range positions {
		key := b.s.liveMirrorClusterKey("kalshi", p.Ticker, "")
		if targets[key] {
			exposure[key] += math.Abs(p.ExposureUSD())
		}
	}
	for _, order := range orders {
		key := b.s.liveMirrorClusterKey("kalshi", order.Ticker, "")
		if targets[key] {
			risk, known := kalshiRestingRisk(order)
			if !known {
				return 0, errors.New("current Kalshi related-event resting risk is unreadable")
			}
			exposure[key] += risk
		}
	}
	headroom := math.Inf(1)
	for key := range targets {
		headroom = math.Min(headroom, cap-exposure[key])
	}
	if !finitePositive(headroom) {
		return 0, errors.New("current related-event 10% rail has no remaining capacity")
	}
	return headroom, nil
}

func (b *r148ServerStagedBroker) currentPolyUSRelatedHeadroom(ctx context.Context,
	bundle storage.ResearchRouteBundle, nav float64) (float64, error) {
	if b.clusterFn != nil {
		return b.clusterFn(ctx, bundle, nav)
	}
	cap, ok := liveRailLimit(nav, b.s.cfg().Risk.LiveClusterCapPct, -1)
	if !ok {
		return 0, errors.New("current related-event 10% rail is unavailable")
	}
	targets := map[string]bool{}
	for _, leg := range bundle.Legs {
		if leg.Venue == "polyus" {
			targets[b.s.liveMirrorClusterKey("polyus", leg.Ticker, "")] = true
		}
	}
	if len(targets) == 0 {
		return 0, errors.New("staged bundle has no PolyUS related-event identity")
	}
	positions, err := b.pusPositions(ctx)
	if err != nil {
		return 0, errors.New("current PolyUS related-event positions are unreadable")
	}
	orders, err := b.pusOpenOrders(ctx)
	if err != nil {
		return 0, errors.New("current PolyUS related-event orders are unreadable")
	}
	exposure := make(map[string]float64, len(targets))
	for _, p := range positions {
		key := b.s.liveMirrorClusterKey("polyus", p.Slug, "")
		if targets[key] {
			// Retail short-position cost conventions differ across schema generations. One dollar
			// per absolute contract is the conservative loss rail when exact cost is unavailable.
			exposure[key] += math.Max(math.Abs(p.Cost), math.Abs(p.Net))
		}
	}
	for _, order := range orders {
		key := b.s.liveMirrorClusterKey("polyus", order.Slug, "")
		if targets[key] {
			exposure[key] += math.Abs(order.Qty)
		}
	}
	headroom := math.Inf(1)
	for key := range targets {
		headroom = math.Min(headroom, cap-exposure[key])
	}
	if !finitePositive(headroom) {
		return 0, errors.New("current PolyUS related-event 10% rail has no remaining capacity")
	}
	return headroom, nil
}

func (b *r148ServerStagedBroker) currentStagedRelatedHeadroom(ctx context.Context,
	bundle storage.ResearchRouteBundle, venue string, nav float64) (float64, error) {
	if venue == "kalshi" {
		return b.currentKalshiRelatedHeadroom(ctx, bundle, nav)
	}
	if venue == "polyus" {
		return b.currentPolyUSRelatedHeadroom(ctx, bundle, nav)
	}
	return 0, errors.New("related-event rail does not support staged venue " + venue)
}

func (b *r148ServerStagedBroker) quoteStagedMultiplier(ctx context.Context,
	bundle storage.ResearchRouteBundle, multiplier int) (map[int]r148StagedQuote, float64, float64, error) {
	if multiplier < 1 {
		return nil, 0, 0, errors.New("staged package multiplier is below one")
	}
	quotes := make(map[int]r148StagedQuote, len(bundle.Legs))
	cost, fee := 0.0, 0.0
	for i, leg := range bundle.Legs {
		qty := leg.Quantity * float64(multiplier)
		if leg.Venue == "kalshi" && math.Abs(qty-math.Round(qty)) > 1e-8 {
			return nil, 0, 0, errors.New("Kalshi staged quantity is not on the one-contract step")
		}
		q, err := b.Quote(ctx, leg, "BUY", qty)
		if err != nil {
			return nil, 0, 0, err
		}
		if q.Available+1e-9 < qty {
			return nil, 0, 0, errors.New("current all-leg depth cannot fill the staged package")
		}
		quotes[i], cost, fee = q, cost+q.Price*qty, fee+q.Fee
	}
	return quotes, cost, fee, nil
}

// currentStagedUnwindPreflight proves that every leg can be flattened *now* at an exact
// executable taker book, quantity, tick and fee. When entryQuotes is supplied it also rebuilds
// the proper-subset unwind envelope from the same current books used for admission. A frozen
// research unwind receipt is evidence, not permission to cross the first LIVE venue boundary.
func (b *r148ServerStagedBroker) currentStagedUnwindPreflight(ctx context.Context,
	bundle storage.ResearchRouteBundle, entryQuotes map[int]r148StagedQuote) (map[int]r148StagedQuote, error) {
	if !bundle.UnwindKnown || len(bundle.Legs) < 2 || len(bundle.Legs) > 6 {
		return nil, errors.New("current staged unwind preflight lacks a certified 2-6 leg unwind envelope")
	}
	exits := make(map[int]r148StagedQuote, len(bundle.Legs))
	for i, leg := range bundle.Legs {
		q, err := b.Quote(ctx, leg, "SELL", leg.Quantity)
		if err != nil {
			return nil, fmt.Errorf("current unwind leg %d (%s): %w", i, leg.Ticker, err)
		}
		if q.Price <= 0 || q.Price >= 1 || q.Available+1e-9 < leg.Quantity || q.Fee < 0 ||
			q.Tick <= 0 || strings.TrimSpace(q.Source) == "" || math.IsNaN(q.Price) ||
			math.IsInf(q.Price, 0) || math.IsNaN(q.Available) || math.IsInf(q.Available, 0) ||
			math.IsNaN(q.Fee) || math.IsInf(q.Fee, 0) || math.IsNaN(q.Tick) || math.IsInf(q.Tick, 0) {
			return nil, fmt.Errorf("current unwind leg %d (%s) lacks exact executable depth/tick/fee authority", i, leg.Ticker)
		}
		exits[i] = q
	}
	if entryQuotes == nil {
		return exits, nil
	}
	if len(entryQuotes) != len(bundle.Legs) {
		return nil, errors.New("current staged unwind envelope lacks every matching entry quote")
	}
	legResults := make([]float64, len(bundle.Legs))
	for i, leg := range bundle.Legs {
		entry, ok := entryQuotes[i]
		if !ok || entry.Price <= 0 || entry.Price >= 1 || entry.Available+1e-9 < leg.Quantity ||
			entry.Fee < 0 || entry.Tick <= 0 || strings.TrimSpace(entry.Source) == "" {
			return nil, fmt.Errorf("current staged entry leg %d lacks matching depth/tick/fee authority", i)
		}
		exit := exits[i]
		legResults[i] = leg.Quantity*exit.Price - exit.Fee - leg.Quantity*entry.Price - entry.Fee
	}
	worst := 0.0
	full := (1 << len(bundle.Legs)) - 1
	for mask := 1; mask < full; mask++ { // a complete bundle settles by its payoff floor, not an unwind
		v := 0.0
		for i := range legResults {
			if mask&(1<<i) != 0 {
				v += legResults[i]
			}
		}
		worst = math.Min(worst, v)
	}
	if worst < bundle.UnwindWorst-1e-9 {
		return nil, fmt.Errorf("current executable unwind envelope %.6f is worse than certified bound %.6f", worst, bundle.UnwindWorst)
	}
	return exits, nil
}

// SizeCurrent turns a one-package research candidate into one immutable live execution plan.
// It preserves every leg ratio and only scales by whole Kalshi package multiples.  Current
// authenticated NAV compounds deposits/profit and de-risks withdrawals/losses; the arm receipt is
// retained as the drawdown reference.  Proof-Kelly, 5% order, 50% exposure, 10% related-event,
// exact all-leg depth, global contract and fee-net edge limits can only reduce the result.
func (b *r148ServerStagedBroker) SizeCurrent(ctx context.Context, bundle storage.ResearchRouteBundle,
	proof storage.StagedBundleExecutionProof) (storage.ResearchRouteBundle, r148StagedExecutionPlan, error) {
	venues := r148StagedBundleVenues(bundle)
	if len(venues) == 0 {
		return storage.ResearchRouteBundle{}, r148StagedExecutionPlan{}, errors.New("staged bundle has no execution venue")
	}
	for _, leg := range bundle.Legs {
		if leg.Venue != "kalshi" && leg.Venue != "polyus" {
			return storage.ResearchRouteBundle{}, r148StagedExecutionPlan{}, errors.New("unsupported staged execution venue")
		}
	}
	if why := b.boundaryReason(ctx, bundle, nil, "BUY", false); why != "" {
		return storage.ResearchRouteBundle{}, r148StagedExecutionPlan{}, errors.New(why)
	}
	venueNAV, venueArm := map[string]float64{}, map[string]float64{}
	nav, arm := 0.0, 0.0
	for _, venue := range venues {
		currentNAV, navSource := b.currentStagedNAV(ctx, venue)
		currentArm := b.s.liveArmBankroll(venue)
		if !finitePositive(currentNAV) || strings.TrimSpace(navSource) == "" || navSource == "unknown" || !finitePositive(currentArm) {
			display := "Kalshi"
			if venue == "polyus" {
				display = "PolyUS"
			}
			return storage.ResearchRouteBundle{}, r148StagedExecutionPlan{}, errors.New("fresh authenticated " + display + " NAV or ARM baseline is unavailable")
		}
		venueNAV[venue], venueArm[venue] = currentNAV, currentArm
		nav, arm = nav+currentNAV, arm+currentArm
	}
	baseQuotes, baseCost, baseFee, err := b.quoteStagedMultiplier(ctx, bundle, 1)
	if err != nil {
		return storage.ResearchRouteBundle{}, r148StagedExecutionPlan{}, err
	}
	baseAllIn := baseCost + baseFee
	baseVenueAllIn := make(map[string]float64, len(venues))
	for i, leg := range bundle.Legs {
		baseVenueAllIn[leg.Venue] += baseQuotes[i].Price*leg.Quantity + baseQuotes[i].Fee
	}
	requiredEdge := b.s.liveMirrorEdgeFloor()
	currentNetUnit := (bundle.PayoutFloor - baseAllIn) / bundle.Size
	if !finitePositive(bundle.PayoutFloor) || baseAllIn <= 0 || baseAllIn >= bundle.PayoutFloor ||
		currentNetUnit+1e-12 < requiredEdge || !finitePositive(proof.Mean) ||
		!finitePositive(proof.LowerBound) || proof.LowerBound > proof.Mean+1e-12 {
		return storage.ResearchRouteBundle{}, r148StagedExecutionPlan{}, errors.New("current fee-inclusive bundle edge or sealed proof lower bound does not clear the live floor")
	}
	effectiveLower := math.Min(proof.LowerBound, currentNetUnit)
	effectiveMean := math.Max(effectiveLower, math.Min(proof.Mean, currentNetUnit))
	payoutPerUnit := bundle.PayoutFloor / bundle.Size
	priceNormalized := baseAllIn / bundle.PayoutFloor
	if priceNormalized <= 0 || priceNormalized >= 1 || payoutPerUnit <= 0 {
		return storage.ResearchRouteBundle{}, r148StagedExecutionPlan{}, errors.New("staged payoff-normalized price is invalid")
	}
	venueTarget, venueHeadrooms, relatedHeadrooms := map[string]float64{}, map[string]float64{}, map[string]float64{}
	targetUSD, venueHeadroom, relatedHeadroom := 0.0, math.Inf(1), math.Inf(1)
	kellyFrac := math.Inf(1)
	maxMultiplier := int(^uint(0) >> 1)
	for _, venue := range venues {
		venueBudget, venueKelly := b.s.liveProofOrderUSDForFloor(venue, venueNAV[venue], priceNormalized,
			effectiveMean/payoutPerUnit, effectiveLower/payoutPerUnit, requiredEdge/payoutPerUnit)
		if !finitePositive(venueBudget) || !finitePositive(venueKelly) {
			return storage.ResearchRouteBundle{}, r148StagedExecutionPlan{}, errors.New("proof-lower-bound Adaptive Allocation cannot fund one " + venue + " staged package")
		}
		headroom, err := b.currentStagedVenueHeadroom(ctx, venue)
		if err != nil {
			return storage.ResearchRouteBundle{}, r148StagedExecutionPlan{}, err
		}
		related, err := b.currentStagedRelatedHeadroom(ctx, bundle, venue, venueNAV[venue])
		if err != nil {
			return storage.ResearchRouteBundle{}, r148StagedExecutionPlan{}, err
		}
		venueBudget = math.Min(venueBudget, math.Min(headroom, related))
		if venueBudget+1e-9 < baseVenueAllIn[venue] {
			return storage.ResearchRouteBundle{}, r148StagedExecutionPlan{}, errors.New("current " + venue + " rails cannot fund one valid staged package")
		}
		venueTarget[venue], venueHeadrooms[venue], relatedHeadrooms[venue] = venueBudget, headroom, related
		targetUSD += venueBudget
		venueHeadroom, relatedHeadroom = math.Min(venueHeadroom, headroom), math.Min(relatedHeadroom, related)
		kellyFrac = math.Min(kellyFrac, venueKelly)
		maxMultiplier = min(maxMultiplier, int(math.Floor(venueBudget/baseVenueAllIn[venue])))
	}
	b.s.riskMu.Lock()
	maxContracts := b.s.maxContracts
	b.s.riskMu.Unlock()
	for i, leg := range bundle.Legs {
		if !finitePositive(leg.Quantity) || (leg.Venue == "kalshi" &&
			(math.Abs(leg.Quantity-math.Round(leg.Quantity)) > 1e-8 || leg.Quantity < 1)) {
			return storage.ResearchRouteBundle{}, r148StagedExecutionPlan{}, errors.New("source bundle ratio is not on the venue quantity step")
		}
		maxMultiplier = min(maxMultiplier, int(math.Floor(baseQuotes[i].Available/leg.Quantity+1e-12)))
		if maxContracts > 0 {
			maxMultiplier = min(maxMultiplier, int(math.Floor(float64(maxContracts)/leg.Quantity+1e-12)))
		}
	}
	if maxMultiplier < 1 {
		return storage.ResearchRouteBundle{}, r148StagedExecutionPlan{}, errors.New("exact current all-leg depth/quantity steps cannot fill one package")
	}
	// Exact nonlinear fees are recomputed at candidate size.  A monotone binary search finds the
	// largest whole package that still clears dollars and the per-unit live edge floor.
	best, bestCost, bestFee := 0, 0.0, 0.0
	bestQuotes := map[int]r148StagedQuote(nil)
	for lo, hi := 1, maxMultiplier; lo <= hi; {
		mid := lo + (hi-lo)/2
		quotes, cost, fee, quoteErr := b.quoteStagedMultiplier(ctx, bundle, mid)
		net := bundle.PayoutFloor*float64(mid) - cost - fee
		passes := quoteErr == nil && net/(bundle.Size*float64(mid))+1e-12 >= requiredEdge
		if passes {
			candidateVenueCost := map[string]float64{}
			for i, leg := range bundle.Legs {
				candidateVenueCost[leg.Venue] += quotes[i].Price*leg.Quantity*float64(mid) + quotes[i].Fee
			}
			for venue, venueCost := range candidateVenueCost {
				if venueCost > venueTarget[venue]+1e-9 {
					passes = false
					break
				}
			}
		}
		if passes {
			best, bestCost, bestFee, bestQuotes = mid, cost, fee, quotes
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	if best < 1 {
		return storage.ResearchRouteBundle{}, r148StagedExecutionPlan{}, errors.New("post-scale exact fees leave no valid staged package")
	}
	plan := r148StagedExecutionPlan{Version: 1, PackageMultiplier: best,
		Size: bundle.Size * float64(best), Cost: bestCost, Fee: bestFee,
		PayoutFloor:      bundle.PayoutFloor * float64(best),
		NetFloor:         bundle.PayoutFloor*float64(best) - bestCost - bestFee,
		PartialFillWorst: bundle.PartialFillWorst * float64(best),
		UnwindWorst:      bundle.UnwindWorst * float64(best),
		LegQuantities:    make([]float64, len(bundle.Legs)), NAV: nav, ArmBaseline: arm,
		KellyFraction: kellyFrac, TargetUSD: targetUSD, VenueHeadroom: venueHeadroom,
		RelatedHeadroom: relatedHeadroom, RequiredEdge: requiredEdge, VenueNAV: venueNAV,
		VenueArmBaseline: venueArm, VenueTargetUSD: venueTarget, VenueHeadroomUSD: venueHeadrooms,
		RelatedHeadroomUSD: relatedHeadrooms}
	for i, leg := range bundle.Legs {
		plan.LegQuantities[i] = leg.Quantity * float64(best)
	}
	sized, err := r148ApplyExecutionPlan(bundle, plan)
	if err != nil {
		return storage.ResearchRouteBundle{}, r148StagedExecutionPlan{}, err
	}
	for i := range sized.Legs {
		q := bestQuotes[i]
		sized.Legs[i].IntegratedCost = q.Price * sized.Legs[i].Quantity
		sized.Legs[i].ExactFee, sized.Legs[i].VisibleDepth = q.Fee, q.Available
		sized.Legs[i].Tick, sized.Legs[i].BookSource, sized.Legs[i].FeeSource = q.Tick, q.Source, q.Source
	}
	if _, err := b.currentStagedUnwindPreflight(ctx, sized, bestQuotes); err != nil {
		return storage.ResearchRouteBundle{}, r148StagedExecutionPlan{}, err
	}
	return sized, plan, nil
}

// Admission returns the exact authenticated current quotes used to choose and persist the
// least-liquid-first route. It is the only way a new staged journal may be created.
func (b *r148ServerStagedBroker) Admission(ctx context.Context, bundle storage.ResearchRouteBundle,
	_ storage.StagedBundleExecutionProof) (map[int]r148StagedQuote, error) {
	if b.s == nil || !b.s.cfg().Risk.LiveStagedBundles {
		return nil, errors.New("live staged-bundle belt is disabled")
	}
	for _, leg := range bundle.Legs {
		if leg.Venue == "polyus" {
			return nil, b.durableBlock(ctx, bundle, r148PUSStagedBlockReason)
		}
	}
	// Production package admission owns one API-native event_ticker per leg. This runs before the
	// whole-package reservation exists, so any current account/risk conflict is external to this
	// bundle. Test brokers with an explicit boundary seam retain their hermetic state-machine path.
	if b.boundaryFn == nil && b.identityFn == nil {
		legs := make([]kalshiPackageLeg, 0, len(bundle.Legs))
		for _, leg := range bundle.Legs {
			if leg.Venue == "kalshi" {
				legs = append(legs, kalshiPackageLeg{Ticker: leg.Ticker, Side: leg.Side})
			}
		}
		if why := b.s.liveKalshiPackageEventHeaderConflict(ctx, legs, ""); why != "" {
			return nil, errors.New("staged event-header exposure guard: " + why)
		}
	}
	if why := b.boundaryReason(ctx, bundle, nil, "BUY", false); why != "" {
		return nil, errors.New(why)
	}
	quotes := make(map[int]r148StagedQuote, len(bundle.Legs))
	total := 0.0
	allPrincipal := 0.0
	venueCost := map[string]float64{}
	venueCryptoPrincipal := map[string]float64{}
	cryptoKnown := true
	clusterCost := map[string]float64{}
	clusterCandidate := map[string]liveMirrorCandidate{}
	for i, leg := range bundle.Legs {
		q, err := b.Quote(ctx, leg, "BUY", leg.Quantity)
		if err != nil {
			return nil, err
		}
		quotes[i] = q
		principal := q.Price * leg.Quantity
		cost := principal + q.Fee
		total += cost
		allPrincipal += principal
		venueCost[leg.Venue] += cost
		candidate := liveMirrorCandidate{Platform: leg.Venue, Ticker: leg.Ticker, Side: leg.Side,
			Family: bundle.SystemID}
		switch liveCryptoCandidateClass(candidate) {
		case liveCryptoYes:
			venueCryptoPrincipal[leg.Venue] += principal
		case liveCryptoNo:
		default:
			cryptoKnown = false
		}
		cluster := b.s.liveMirrorClusterKey(leg.Venue, leg.Ticker, "")
		clusterCost[cluster] += cost
		clusterCandidate[cluster] = candidate
	}
	if _, err := b.currentStagedUnwindPreflight(ctx, bundle, quotes); err != nil {
		return nil, err
	}
	for venue, cost := range venueCost {
		cryptoCost, proportionalKnown := liveCryptoProportionalCost(total, allPrincipal,
			venueCryptoPrincipal[venue])
		if why := b.s.liveVenueBudgetCheckForStagedBundle(ctx, venue, cost,
			cryptoCost, cryptoKnown && proportionalKnown); why != "" {
			return nil, errors.New(why)
		}
		if why := b.s.liveRiskCheck(cost, venue); why != "" {
			return nil, errors.New(why)
		}
	}
	for cluster := range clusterCost {
		// The bundle's payoff certificate itself establishes dependence. Charge the full bundle
		// cost to every involved canonical cluster rather than letting cross-event legs split the
		// 10% correlated-risk rail into individually passing fragments.
		if why := b.s.liveMirrorClusterGuard(ctx, clusterCandidate[cluster], total); why != "" {
			return nil, errors.New(why)
		}
	}
	return quotes, nil
}

func (b *r148ServerStagedBroker) Quote(ctx context.Context, leg storage.ResearchRouteBundleLeg,
	action string, quantity float64) (r148StagedQuote, error) {
	if b.quoteFn != nil {
		return b.quoteFn(ctx, leg, action, quantity)
	}
	if b.s == nil || (leg.Venue != "kalshi" && leg.Venue != "polyus") || quantity <= 0 ||
		(action != "BUY" && action != "SELL") {
		return r148StagedQuote{}, errors.New("unsupported staged quote request")
	}
	quoteSide := strings.ToUpper(leg.Side)
	if action == "SELL" {
		if quoteSide == "YES" {
			quoteSide = "NO"
		} else if quoteSide == "NO" {
			quoteSide = "YES"
		}
	}
	ask, depth, source, ok := b.s.executableAsk(ctx, leg.Venue, leg.Ticker, quoteSide, true)
	if !ok || depth+1e-9 < quantity {
		return r148StagedQuote{}, errors.New("current " + leg.Venue + " full book lacks requested FOK depth")
	}
	price := ask
	if action == "SELL" {
		price = 1 - ask
	}
	if leg.Venue == "polyus" {
		meta, err := b.pusMeta(ctx, leg.Ticker)
		if err != nil || !meta.Open() || meta.TickUSD <= 0 || meta.MinQty <= 0 {
			return r148StagedQuote{}, errors.New("current PolyUS lifecycle/tick/minimum authority is unavailable")
		}
		if quantity+1e-9 < meta.MinQty || math.Abs(quantity/meta.MinQty-math.Round(quantity/meta.MinQty)) > 1e-8 ||
			math.Abs(math.Round(price/meta.TickUSD)*meta.TickUSD-price) > 1e-8 {
			return r148StagedQuote{}, errors.New("current PolyUS quantity minimum or price tick is not satisfied")
		}
		fee, feeSource, feeKnown := b.s.polyUSFeeExactAuthority(leg.Ticker, false, quantity, price)
		if !feeKnown || fee < 0 || strings.TrimSpace(feeSource) == "" {
			return r148StagedQuote{}, errors.New("current exact PolyUS taker fee is unavailable")
		}
		return r148StagedQuote{Price: price, Available: depth, Fee: fee, Tick: meta.TickUSD,
			Source: source + ";" + feeSource + ";current-retail-meta"}, nil
	}
	if b.s.kal == nil {
		return r148StagedQuote{}, errors.New("authenticated Kalshi client is unavailable")
	}
	market, known := b.s.kmkt(leg.Ticker)
	if !known {
		var err error
		market, err = b.s.kal.GetMarket(ctx, leg.Ticker)
		if err != nil {
			return r148StagedQuote{}, fmt.Errorf("current market/tick lookup: %w", err)
		}
	}
	tick, tickKnown := market.TickForKnown(price)
	if !tickKnown || tick <= 0 || math.Abs(kalshi.SnapPx(price, tick)-price) > 1e-8 {
		return r148StagedQuote{}, errors.New("current Kalshi tick authority is absent or book price is off-grid")
	}
	fee, feeKnown, feeSource := b.s.kalFeeExactAction(leg.Ticker, false, quantity, price, action == "BUY")
	if !feeKnown || fee < 0 {
		return r148StagedQuote{}, errors.New("current exact Kalshi taker fee is unavailable")
	}
	return r148StagedQuote{Price: price, Available: depth, Fee: fee, Tick: tick,
		Source: source + ";" + feeSource + ";current-tick"}, nil
}

func (b *r148ServerStagedBroker) Gate(ctx context.Context, bundle storage.ResearchRouteBundle,
	leg storage.ResearchRouteBundleLeg, action string, prior r148StagedQuote) error {
	if why := b.boundaryReason(ctx, bundle, &leg, action, true); why != "" {
		return errors.New(why)
	}
	current, err := b.Quote(ctx, leg, action, leg.Quantity)
	if err != nil {
		return err
	}
	if current.Available+1e-9 < leg.Quantity || current.Tick <= 0 {
		return errors.New("current FOK depth/tick disappeared at the pre-submit gate")
	}
	if action == "BUY" {
		if current.Price*leg.Quantity+current.Fee > prior.Price*leg.Quantity+prior.Fee+1e-9 {
			return errors.New("current all-in buy cost worsened after bundle reprice")
		}
	} else if current.Price*leg.Quantity-current.Fee < prior.Price*leg.Quantity-prior.Fee-1e-9 {
		return errors.New("current fee-net unwind value worsened after bundle reprice")
	}
	if action == "BUY" {
		// Recheck every exit immediately before each sequential BUY. The prior admission receipt may
		// be milliseconds old, and a newly un-exitable first leg is not made safe by a durable journal.
		if _, err := b.currentStagedUnwindPreflight(ctx, bundle, nil); err != nil {
			return fmt.Errorf("pre-submit current unwind preflight: %w", err)
		}
		cost := current.Price*leg.Quantity + current.Fee
		candidate := liveMirrorCandidate{Platform: leg.Venue, Ticker: leg.Ticker, Side: leg.Side,
			Family: bundle.SystemID}
		if why := b.s.liveMirrorClusterGuard(ctx, candidate, cost); why != "" {
			return errors.New(why)
		}
	}
	return nil
}

func r148KalshiFOKRequest(leg storage.ResearchRouteBundleLeg, action string, quantity, outcomeLimit float64,
	idempotencyKey string) (kalshi.OrderRequest, error) {
	if leg.Venue != "kalshi" || (leg.Side != "YES" && leg.Side != "NO") ||
		(action != "BUY" && action != "SELL") || quantity <= 0 || outcomeLimit <= 0 || outcomeLimit >= 1 {
		return kalshi.OrderRequest{}, errors.New("invalid staged Kalshi FOK request")
	}
	req := kalshi.OrderRequest{Ticker: leg.Ticker, ClientOrderID: idemKey("stg-", idempotencyKey),
		Count: strconv.FormatFloat(quantity, 'f', -1, 64), TimeInForce: "fill_or_kill",
		SelfTradePreventionType: "taker_at_cross", CancelOrderOnPause: true,
		ReduceOnly: action == "SELL"}
	// The V2 book is expressed in YES inventory: bid is YES exposure and ask is NO exposure.
	if (action == "BUY" && leg.Side == "YES") || (action == "SELL" && leg.Side == "NO") {
		req.Side = "bid"
	} else {
		req.Side = "ask"
	}
	if leg.Side == "YES" {
		req.Price = kalshi.FmtPx(outcomeLimit)
	} else {
		req.Price = kalshi.FmtPx(math.Round((1-outcomeLimit)*1e6) / 1e6)
	}
	return req, nil
}

func (b *r148ServerStagedBroker) exactFee(ticker string, maker bool, quantity, price float64,
	buy bool) (float64, bool, string) {
	if b.feeFn != nil {
		return b.feeFn(ticker, maker, quantity, price, buy)
	}
	return b.s.kalFeeExactAction(ticker, maker, quantity, price, buy)
}

func (b *r148ServerStagedBroker) residentExactFee(ticker string, maker bool,
	quantity, price float64, buy bool) (float64, bool, string) {
	if b.feeFn != nil {
		return b.feeFn(ticker, maker, quantity, price, buy)
	}
	return b.s.kalFeeExactActionResident(ticker, maker, quantity, price, buy)
}

// wireBoundaryReason is deliberately resident-only. The potentially slow identity, lifecycle,
// account and database checks run before the final venue fence in SubmitFOK. Once the reader fence
// is held, this method may inspect only current in-process authority and immutable resident facts.
func (b *r148ServerStagedBroker) wireBoundaryReason(ctx context.Context,
	bundle storage.ResearchRouteBundle, leg storage.ResearchRouteBundleLeg, action string) string {
	// Hermetic broker tests use boundaryFn as their already-proven production-boundary seam. Never
	// call an arbitrary test seam while holding the real-money fence. Kill/disarm/AUTO authority is
	// still real and is always re-read, including in those tests.
	if b.boundaryFn != nil {
		return b.s.liveWriteBoundaryReason(true)
	}
	if b == nil || b.s == nil || b.s.store == nil {
		return "staged production broker is unavailable"
	}
	if !b.s.liveAutoCombosEnabled() {
		return "AUTO combo belt is disabled"
	}
	if !b.s.cfg().Risk.LiveStagedBundles {
		return "live staged-bundle belt is disabled"
	}
	if why := b.s.liveWriteBoundaryReason(true); why != "" {
		return why
	}
	if why := b.allowlistReason(bundle); why != "" {
		return why
	}
	if leg.Venue != "kalshi" {
		if leg.Venue == "polyus" {
			return r148PUSStagedBlockReason
		}
		return "unsupported staged execution venue: " + leg.Venue
	}
	if b.s.kal == nil || !b.s.kal.HasCredentials() || !b.s.kal.Armed() {
		return "authenticated and armed Kalshi order client is unavailable"
	}
	if blocked, end := kalshiMaintenanceWindow(b.clock()); blocked {
		return "Kalshi maintenance blocks staged orders until " + end.UTC().Format(time.RFC3339)
	}
	if why := b.s.liveContractLimitReason(leg.Quantity); why != "" {
		return why
	}
	residentCtx := r159KalshiResidentAdmissionContext(ctx)
	if !b.s.paperEntryHorizonNow(residentCtx, leg.Venue, leg.Ticker, "") {
		return "market is outside the current funded horizon or has an unknown/past resident lifecycle: " + leg.Ticker
	}
	return ""
}

// wireQuote proves the last executable price, full requested depth, tick and exact fee from
// resident state only. It never invokes the REST-capable Quote path.
func (b *r148ServerStagedBroker) wireQuote(leg storage.ResearchRouteBundleLeg,
	action string, quantity float64) (r148StagedQuote, error) {
	if b.wireQuoteFn != nil {
		return b.wireQuoteFn(leg, action, quantity)
	}
	if b == nil || b.s == nil || leg.Venue != "kalshi" || quantity <= 0 ||
		(action != "BUY" && action != "SELL") {
		return r148StagedQuote{}, errors.New("unsupported staged resident quote request")
	}
	readBook := b.s.r159KalshiWSExecutableBook
	if b.s.kalshiExecutableBookFn != nil {
		readBook = b.s.kalshiExecutableBookFn
	}
	book, why := readBook(liveMirrorCandidate{
		Platform: "kalshi", Ticker: leg.Ticker, Side: leg.Side,
	})
	if why != "" {
		return r148StagedQuote{}, errors.New(why)
	}
	price, available, tick := book.TakerPrice, book.TakerDepth, book.TakerTick
	if action == "SELL" {
		price, available, tick = book.MakerPrice, book.MakerDepth, book.MakerTick
	}
	if price <= 0 || price >= 1 || available+1e-9 < quantity || tick <= 0 ||
		math.Abs(kalshi.SnapPx(price, tick)-price) > 1e-8 {
		return r148StagedQuote{}, errors.New("resident Kalshi book lacks requested FOK depth or exact on-grid price")
	}
	fee, known, feeSource := b.residentExactFee(
		leg.Ticker, false, quantity, price, action == "BUY")
	if !known || fee < 0 || strings.TrimSpace(feeSource) == "" {
		return r148StagedQuote{}, errors.New("resident exact Kalshi taker fee is unavailable")
	}
	return r148StagedQuote{
		Price: price, Available: available, Fee: fee, Tick: tick,
		Source: firstNonEmpty(book.Source, "kalshi-resident-full-book") + ";" + feeSource,
	}, nil
}

func (b *r148ServerStagedBroker) create(ctx context.Context, req kalshi.OrderRequest) (*kalshi.CreateOrderResult, error) {
	if b.createFn != nil {
		return b.createFn(ctx, req)
	}
	b.s.invalidateR154KalshiAdmissionSnapshot()
	result, err := b.s.kal.CreateOrder(ctx, req)
	b.s.invalidateR154KalshiAdmissionSnapshot()
	return result, err
}

func (b *r148ServerStagedBroker) order(ctx context.Context, orderID string) (kalshi.Order, error) {
	if b.orderFn != nil {
		return b.orderFn(ctx, orderID)
	}
	return b.s.kal.GetOrder(ctx, orderID)
}

func (b *r148ServerStagedBroker) fills(ctx context.Context, orderID string) ([]kalshi.Fill, error) {
	if b.fillsFn != nil {
		return b.fillsFn(ctx, orderID)
	}
	return b.s.kal.GetFillsForOrder(ctx, orderID)
}

func (b *r148ServerStagedBroker) clientOrders(ctx context.Context, ticker, clientOrderID string,
	minTS time.Time) ([]kalshi.Order, error) {
	if b.clientOrdersFn != nil {
		return b.clientOrdersFn(ctx, ticker, clientOrderID, minTS)
	}
	if b.s == nil || b.s.kal == nil {
		return nil, errors.New("authenticated Kalshi order-history client is unavailable")
	}
	return b.s.kal.GetOrdersByClientOrderID(ctx, ticker, clientOrderID, minTS)
}

func r148ExpectedKalshiDirection(leg storage.ResearchRouteBundleLeg, action string) (string, string) {
	expectedOutcome := leg.Side
	if action == "SELL" {
		expectedOutcome = concreteOppositeSide(leg.Side)
	}
	expectedBook := "bid"
	if expectedOutcome == "NO" {
		expectedBook = "ask"
	}
	return expectedOutcome, expectedBook
}

func r148ValidateKalshiOrder(order kalshi.Order, leg storage.ResearchRouteBundleLeg,
	action, clientOrderID string) error {
	expectedOutcome, expectedBook := r148ExpectedKalshiDirection(leg, action)
	outcome := strings.ToUpper(strings.TrimSpace(order.OutcomeSide))
	book := strings.ToLower(strings.TrimSpace(order.BookSide))
	clientMatches := clientOrderID == "" || strings.TrimSpace(order.ClientOrderID) == clientOrderID
	if !order.CurrentExecutionSchemaKnown() || strings.TrimSpace(order.OrderID) == "" || !clientMatches ||
		strings.TrimSpace(order.Ticker) != leg.Ticker || outcome != expectedOutcome || book != expectedBook ||
		math.Abs(order.Initial()-leg.Quantity) > 1e-9 {
		return errors.New("Kalshi recovered order does not match the immutable staged intent")
	}
	return nil
}

func (b *r148ServerStagedBroker) SubmitFOK(ctx context.Context, bundle storage.ResearchRouteBundle,
	leg storage.ResearchRouteBundleLeg, action string, quantity, limit float64,
	idempotencyKey string) (r148StagedReceipt, error) {
	if b == nil || b.s == nil {
		return r148StagedReceipt{}, errors.New("staged production broker is unavailable")
	}
	if leg.Venue == "polyus" {
		return r148StagedReceipt{}, errors.New(r148PUSStagedBlockReason)
	}
	req, err := r148KalshiFOKRequest(leg, action, quantity, limit, idempotencyKey)
	if err != nil {
		return r148StagedReceipt{}, err
	}
	projectedFee, feeKnown, feeSource := b.exactFee(leg.Ticker, false, quantity, limit, action == "BUY")
	if !feeKnown || projectedFee < 0 || strings.TrimSpace(feeSource) == "" {
		return r148StagedReceipt{}, errors.New("final exact Kalshi FOK fee is unavailable")
	}
	// Slow REST identity/lifecycle recovery, account views and package-risk database lookups are
	// preflight work. They must never occupy the writer-preferred cash fence and delay a kill,
	// disarm, AUTO-off or Settings publication.
	in, _, coordinatedRisk, riskErr := b.stagedRiskForBundle(ctx, bundle)
	if riskErr != nil {
		return r148StagedReceipt{}, fmt.Errorf("final staged package risk identity: %w", riskErr)
	}
	if why := b.boundaryReason(ctx, bundle, &leg, action, false); why != "" {
		return r148StagedReceipt{}, errors.New("final staged venue boundary: " + why)
	}
	// Each BUY rechecks the full package for duplicate headers, then this leg against current
	// positions/orders/other reservations. The package's own whole-vector reservation is ignored by
	// exact id; it cannot exempt any venue-visible exposure. SELL is a reduce-only unwind and must
	// remain possible after the guard itself detects exposure.
	if action == "BUY" && b.boundaryFn == nil && b.identityFn == nil {
		allLegs := make([]kalshiPackageLeg, 0, len(bundle.Legs))
		for _, packageLeg := range bundle.Legs {
			if packageLeg.Venue == "kalshi" {
				allLegs = append(allLegs, kalshiPackageLeg{Ticker: packageLeg.Ticker, Side: packageLeg.Side})
			}
		}
		if _, why := b.s.kalshiPackageEventTickersStrict(ctx, allLegs); why != "" {
			return r148StagedReceipt{}, errors.New("final staged package event-header guard: " + why)
		}
		ignoreRisk := ""
		if coordinatedRisk {
			ignoreRisk = r148StagedRiskID(in.ExecutionID)
		}
		if why := b.s.liveKalshiPackageEventHeaderConflict(ctx,
			[]kalshiPackageLeg{{Ticker: leg.Ticker, Side: leg.Side}}, ignoreRisk); why != "" {
			return r148StagedReceipt{}, errors.New("final staged event-header exposure guard: " + why)
		}
	}

	executionID := ""
	if coordinatedRisk {
		executionID = in.ExecutionID
	}

	persistKnownNoSend := func(source, reason string) (r148StagedReceipt, error) {
		receipt := r148StagedReceipt{
			State: r148ReceiptUnfilled, Authoritative: true, Source: source, Reason: reason,
		}
		if coordinatedRisk {
			recordRiskReceipt := b.RecordStagedRiskReceipt
			if b.riskReceiptFn != nil {
				recordRiskReceipt = b.riskReceiptFn
			}
			if recordErr := recordRiskReceipt(ctx, executionID, leg, action, receipt); recordErr != nil {
				return r148StagedReceipt{}, fmt.Errorf("durable staged known-no-send receipt: %w", recordErr)
			}
		}
		return receipt, nil
	}
	startAttempt := func() error {
		var started bool
		var startErr error
		executionID, started, startErr = b.startKnownStagedRiskAttempt(
			ctx, executionID, coordinatedRisk, leg, action, req.ClientOrderID)
		if startErr != nil {
			return fmt.Errorf("durable staged pre-send risk event: %w", startErr)
		}
		if coordinatedRisk && !started {
			return errors.New("coordinated staged attempt was not durably started")
		}
		return nil
	}

	// This is the short final money boundary. Only current in-process authority, the mandatory
	// submit-started journal append, resident book/fee proof and the venue POST occur under it.
	b.s.liveWriteFence.RLock()
	if why := b.wireBoundaryReason(ctx, bundle, leg, action); why != "" {
		if startErr := startAttempt(); startErr != nil {
			b.s.liveWriteFence.RUnlock()
			return r148StagedReceipt{}, startErr
		}
		b.s.liveWriteFence.RUnlock()
		return persistKnownNoSend(r163StagedWireAuthorityNoSendSource,
			"final staged authority changed; venue was not called: "+why)
	}
	admittedGeneration, generationBound := r163StagedMoneyPolicyFromContext(ctx)
	currentGeneration := b.stagedLiveMoneyPolicyGeneration()
	if action == "BUY" && coordinatedRisk &&
		(!generationBound || admittedGeneration != currentGeneration) {
		if startErr := startAttempt(); startErr != nil {
			b.s.liveWriteFence.RUnlock()
			return r148StagedReceipt{}, startErr
		}
		reason := "LIVE authority or risk policy changed after staged admission; venue was not called"
		if !generationBound {
			reason = "staged BUY lacks an immutable LIVE money-policy generation; venue was not called"
		}
		b.s.liveWriteFence.RUnlock()
		return persistKnownNoSend(r163StagedMoneyPolicyNoSendSource, reason)
	}
	if startErr := startAttempt(); startErr != nil {
		b.s.liveWriteFence.RUnlock()
		return r148StagedReceipt{}, startErr
	}
	currentQuote, quoteErr := b.wireQuote(leg, action, quantity)
	if quoteErr != nil {
		b.s.liveWriteFence.RUnlock()
		return persistKnownNoSend(r163StagedWireBookNoSendSource,
			"final resident staged book/fee proof failed; venue was not called: "+quoteErr.Error())
	}
	if action == "BUY" {
		if currentQuote.Price*quantity+currentQuote.Fee >
			limit*quantity+projectedFee+1e-9 {
			b.s.liveWriteFence.RUnlock()
			return persistKnownNoSend(r163StagedWireBookNoSendSource,
				"final resident staged all-in BUY cost worsened; venue was not called")
		}
	} else if currentQuote.Price*quantity-currentQuote.Fee <
		limit*quantity-projectedFee-1e-9 {
		b.s.liveWriteFence.RUnlock()
		return persistKnownNoSend(r163StagedWireBookNoSendSource,
			"final resident staged fee-net SELL value worsened; venue was not called")
	}
	req, err = r148KalshiFOKRequest(leg, action, quantity, currentQuote.Price, idempotencyKey)
	if err != nil {
		b.s.liveWriteFence.RUnlock()
		return persistKnownNoSend(r163StagedWireBookNoSendSource,
			"final resident staged request was invalid; venue was not called: "+err.Error())
	}
	if b.beforeCreateFn != nil {
		b.beforeCreateFn()
	}
	// Test seams can mutate kill or policy in the final instruction before POST. Production also
	// benefits from this second zero-I/O read if an asynchronous safety source published directly.
	if why := b.wireBoundaryReason(ctx, bundle, leg, action); why != "" {
		b.s.liveWriteFence.RUnlock()
		return persistKnownNoSend(r163StagedWireAuthorityNoSendSource,
			"final staged authority changed immediately before POST; venue was not called: "+why)
	}
	currentGeneration = b.stagedLiveMoneyPolicyGeneration()
	if action == "BUY" && coordinatedRisk &&
		(!generationBound || admittedGeneration != currentGeneration) {
		b.s.liveWriteFence.RUnlock()
		return persistKnownNoSend(r163StagedMoneyPolicyNoSendSource,
			"LIVE authority or risk policy changed immediately before POST; venue was not called")
	}
	result, err := b.create(ctx, req)
	b.s.liveWriteFence.RUnlock()
	if err != nil {
		if coordinatedRisk {
			_ = b.RecordStagedRiskReceipt(ctx, executionID, leg, action, r148StagedReceipt{
				State: r148ReceiptAmbiguous, Source: "kalshi-create-error", Reason: err.Error()})
		}
		return r148StagedReceipt{}, err
	}
	if result == nil || strings.TrimSpace(result.OrderID) == "" {
		if coordinatedRisk {
			_ = b.RecordStagedRiskReceipt(ctx, executionID, leg, action, r148StagedReceipt{
				State: r148ReceiptAmbiguous, Source: "kalshi-create-response",
				Reason: "accepted response lacked a durable order id"})
		}
		return r148StagedReceipt{}, errors.New("Kalshi accepted response lacked a durable order id")
	}
	if coordinatedRisk {
		if riskErr := b.RecordStagedRiskReceipt(ctx, executionID, leg, action, r148StagedReceipt{
			OrderID: result.OrderID, State: r148ReceiptPending, Source: "kalshi-create-ack",
			Reason: "venue acknowledgement; fill not implied"}); riskErr != nil {
			return r148StagedReceipt{}, fmt.Errorf("accepted staged order risk receipt: %w", riskErr)
		}
	}
	b.s.refreshR154KalshiAdmissionSnapshotAsync()
	b.s.liveMu.Lock()
	if b.s.kalPlaced == nil {
		b.s.kalPlaced = map[string]time.Time{}
	}
	b.s.kalPlaced[leg.Ticker] = b.clock()
	b.s.liveMu.Unlock()
	b.s.liveLogAdd(map[string]any{"event": "STAGED-FOK-SUBMITTED", "system": bundle.SystemID,
		"bundle_id": bundle.BundleID, "ticker": leg.Ticker, "side": leg.Side, "action": action,
		"order_id": result.OrderID, "quantity": quantity, "outcome_limit": limit})
	// The create acknowledgement never implies a fill. Reconcile immediately; a transient read
	// failure returns pending with the durable order id and is retried from the journal.
	receipt, reconcileErr := b.Reconcile(ctx, leg, action, result.OrderID)
	if reconcileErr != nil {
		receipt = r148StagedReceipt{OrderID: result.OrderID, State: r148ReceiptPending,
			Source: "kalshi-create-ack", Reason: reconcileErr.Error()}
	}
	if coordinatedRisk {
		if riskErr := b.RecordStagedRiskReceipt(ctx, executionID, leg, action, receipt); riskErr != nil {
			return r148StagedReceipt{}, fmt.Errorf("staged order/fill risk receipt: %w", riskErr)
		}
	}
	if action == "BUY" {
		cost := 0.0
		switch receipt.State {
		case r148ReceiptFilled:
			cost = receipt.FilledQty*receipt.AveragePrice + receipt.FeeTotal
		case r148ReceiptPending, r148ReceiptAmbiguous:
			cost = quantity*limit + projectedFee
		}
		if cost > 0 {
			b.s.liveRiskBook(cost, "kalshi")
		}
	}
	return receipt, nil
}

func r148KalshiTerminalStatus(status string) bool {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "executed", "filled", "canceled", "cancelled", "expired", "rejected":
		return true
	default:
		return false
	}
}

func r148KalshiReceipt(order kalshi.Order, fills []kalshi.Fill, leg storage.ResearchRouteBundleLeg,
	action string) r148StagedReceipt {
	r := r148StagedReceipt{OrderID: order.OrderID, State: r148ReceiptPending,
		Authoritative: true, Source: "kalshi-get-order+order-scoped-fills"}
	qty, gross, fee := 0.0, 0.0, 0.0
	expectedOutcome, _ := r148ExpectedKalshiDirection(leg, action)
	seenFills := map[string]bool{}
	for _, fill := range fills {
		q := fill.Qty()
		price := fill.YesPriceUSD()
		if leg.Side == "NO" {
			price = fill.NoPriceUSD()
		}
		fillID := strings.TrimSpace(fill.FillID)
		if fillID == "" || seenFills[fillID] || strings.TrimSpace(fill.OrderID) != order.OrderID || strings.TrimSpace(fill.Ticker) != leg.Ticker ||
			!strings.EqualFold(fill.SideYesNo(), expectedOutcome) || q <= 0 || price <= 0 || price >= 1 || !fill.FeeKnown() {
			r.State, r.Authoritative, r.Reason = r148ReceiptAmbiguous, false,
				"fill lacks exact order/ticker/direction, quantity, outcome price, or fee"
			return r
		}
		seenFills[fillID] = true
		qty, gross, fee = qty+q, gross+q*price, fee+fill.FeeUSD()
	}
	r.FilledQty, r.FeeTotal = qty, fee
	if qty > 0 {
		r.AveragePrice = gross / qty
	}
	if math.Abs(order.Filled()-qty) > 1e-9 {
		r.State, r.Authoritative, r.Reason = r148ReceiptAmbiguous, false, "order and fill ledgers disagree"
		return r
	}
	if strings.EqualFold(strings.TrimSpace(order.Status), "executed") && qty <= 1e-9 {
		r.State, r.Authoritative, r.Reason = r148ReceiptAmbiguous, false, "executed FOK has no authoritative scoped fills"
		return r
	}
	if qty+1e-9 >= leg.Quantity {
		r.State, r.Reason = r148ReceiptFilled, "authoritative FOK fill"
		return r
	}
	if r148KalshiTerminalStatus(order.Status) {
		if qty > 1e-9 {
			r.State, r.Reason = r148ReceiptFilled, "authoritative partial FOK fill"
		} else {
			r.State, r.Reason = r148ReceiptUnfilled, "authoritative terminal FOK without fills"
		}
		return r
	}
	r.Reason = "order remains nonterminal"
	return r
}

func (b *r148ServerStagedBroker) Reconcile(ctx context.Context, leg storage.ResearchRouteBundleLeg,
	action, orderID string) (r148StagedReceipt, error) {
	if leg.Venue == "polyus" {
		if strings.HasPrefix(orderID, r148PUSSyntheticPrefix) {
			return b.reconcilePUSIntent(ctx, leg, action, strings.TrimPrefix(orderID, r148PUSSyntheticPrefix))
		}
		state, err := b.pusOrder(ctx, orderID)
		if err != nil {
			return r148StagedReceipt{}, err
		}
		return r148PUSOrderReceipt(state, leg, action), nil
	}
	order, err := b.order(ctx, orderID)
	if err != nil {
		return r148StagedReceipt{}, err
	}
	if strings.TrimSpace(order.OrderID) == "" {
		order.OrderID = orderID
	}
	if order.OrderID != orderID || (order.Ticker != "" && order.Ticker != leg.Ticker) {
		return r148StagedReceipt{}, errors.New("Kalshi order lookup returned a different immutable identity")
	}
	if err := r148ValidateKalshiOrder(order, leg, action, ""); err != nil {
		return r148StagedReceipt{}, err
	}
	fills, err := b.fills(ctx, orderID)
	if err != nil {
		return r148StagedReceipt{}, err
	}
	return r148KalshiReceipt(order, fills, leg, action), nil
}

func (b *r148ServerStagedBroker) Freeze(ctx context.Context, reason string) error {
	if b.s == nil || b.s.store == nil {
		return errors.New("durable staged freeze is unavailable")
	}
	// The immutable staged journal and pending-risk ledger retain every filled/unknown leg across a
	// restart. Freeze means pause new money and reconcile/unwind that journal; the global emergency
	// stop is operator-only.
	b.s.setLiveAutoSafetyPause("staged-risk", "staged bundle execution requires exact recovery: "+reason)
	return nil
}

func (b *r148ServerStagedBroker) currentIdentity(ctx context.Context, bundle storage.ResearchRouteBundle) error {
	if b.identityFn != nil {
		return b.identityFn(ctx, bundle)
	}
	if b.s == nil || b.s.kal == nil || bundle.CertificateStatus != "verified" ||
		bundle.CertificateHash == "" || bundle.EventVersion <= 0 {
		return errors.New("verified immutable bundle identity is absent")
	}
	markets := make(map[string]kalshi.Market, len(bundle.Legs))
	for _, leg := range bundle.Legs {
		if leg.Venue != "kalshi" {
			return errors.New("non-Kalshi leg has no safe staged recovery adapter")
		}
		m, err := b.s.kal.GetMarket(ctx, leg.Ticker)
		if err != nil {
			return fmt.Errorf("market %s: %w", leg.Ticker, err)
		}
		if m.Ticker != leg.Ticker || m.Result != "" ||
			(!strings.EqualFold(m.Status, "active") && !strings.EqualFold(m.Status, "open")) {
			return fmt.Errorf("market %s is not current/open", leg.Ticker)
		}
		markets[leg.Ticker] = m
	}
	switch bundle.SystemID {
	case "event-basket-lock":
		return b.currentEventBasketIdentity(ctx, bundle, markets)
	case "nested-ladder-lock":
		return b.currentNestedIdentity(ctx, bundle, markets)
	case "time-nested-lock":
		return b.currentDeadlineIdentity(ctx, bundle)
	default:
		return errors.New("unsupported staged payoff identity " + bundle.SystemID)
	}
}

func (b *r148ServerStagedBroker) currentEventBasketIdentity(ctx context.Context,
	bundle storage.ResearchRouteBundle, markets map[string]kalshi.Market) error {
	event := ""
	for _, m := range markets {
		if event == "" {
			event = m.EventTicker
		}
		if m.EventTicker == "" || m.EventTicker != event {
			return errors.New("event basket membership no longer has one canonical event")
		}
	}
	if bundle.CanonicalEventID != "venue:kalshi:"+event {
		return errors.New("canonical event id changed")
	}
	snap, err := b.s.kal.GetEventSnapshot(ctx, event)
	if err != nil {
		return err
	}
	if !snap.MutuallyExclusive || r138ConditionalCertificateHash(snap, "at-most-one-normal-settlement") != bundle.CertificateHash {
		return errors.New("current event membership/rule certificate differs from the admitted certificate")
	}
	member := make(map[string]bool, len(snap.Markets))
	for _, m := range snap.Markets {
		member[m.Ticker] = true
	}
	for _, leg := range bundle.Legs {
		if leg.Side != "NO" || !member[leg.Ticker] {
			return errors.New("event basket leg is absent or no longer the certified NO payoff")
		}
	}
	return nil
}

func (b *r148ServerStagedBroker) currentNestedIdentity(ctx context.Context,
	bundle storage.ResearchRouteBundle, markets map[string]kalshi.Market) error {
	if len(bundle.Legs) != 2 {
		return errors.New("nested ladder must have exactly two legs")
	}
	event := markets[bundle.Legs[0].Ticker].EventTicker
	if event == "" || markets[bundle.Legs[1].Ticker].EventTicker != event ||
		bundle.CanonicalEventID != "venue:kalshi:"+event {
		return errors.New("nested ladder canonical event changed")
	}
	snap, err := b.s.kal.GetEventSnapshot(ctx, event)
	if err != nil {
		return err
	}
	rungs := nestedLadderRungs(snap)
	strike := map[string]float64{}
	for _, rung := range rungs {
		strike[rung.ticker] = rung.strike
	}
	low, high := bundle.Legs[0], bundle.Legs[1]
	if low.Side != "YES" {
		low, high = high, low
	}
	lo, lok := strike[low.Ticker]
	hi, hok := strike[high.Ticker]
	if low.Side != "YES" || high.Side != "NO" || !lok || !hok || lo >= hi {
		return errors.New("nested ladder side/strike relation changed")
	}
	hash := storage.R138HashJSON(map[string]any{"event": snap.EventTicker, "low": lo,
		"high": hi, "strike_type": "greater", "conditional": "normal-settlement"})
	if hash != bundle.CertificateHash {
		return errors.New("nested ladder rule certificate changed")
	}
	return nil
}

func (b *r148ServerStagedBroker) currentDeadlineIdentity(ctx context.Context,
	bundle storage.ResearchRouteBundle) error {
	if len(bundle.Legs) != 2 {
		return errors.New("time nested lock must have exactly two legs")
	}
	rows, err := b.s.store.VerifiedDeadlineRelations(ctx, "kalshi", 500)
	if err != nil {
		return err
	}
	for _, row := range rows {
		if row.EventID != bundle.CanonicalEventID || row.EventVersion != bundle.EventVersion || !r138DeadlineVoidVerified(row) {
			continue
		}
		match := map[string]string{row.LeftTicker: "NO", row.RightTicker: "YES"}
		ok := true
		for _, leg := range bundle.Legs {
			ok = ok && match[leg.Ticker] == leg.Side
		}
		if !ok {
			continue
		}
		hash := storage.R138HashJSON(map[string]any{"relation": row.RelationID,
			"relation_version": row.RelationVersion, "event_version": row.EventVersion,
			"evidence": row.EvidenceJSON, "void_policy": row.VoidPolicy})
		if hash == bundle.CertificateHash {
			return nil
		}
	}
	return errors.New("current verified deadline/void certificate is missing or changed")
}

// Sorted only for deterministic audit output; execution order itself is persisted by Admission.
func r148StagedBundleVenues(bundle storage.ResearchRouteBundle) []string {
	set := map[string]bool{}
	for _, leg := range bundle.Legs {
		set[leg.Venue] = true
	}
	venues := make([]string, 0, len(set))
	for venue := range set {
		venues = append(venues, venue)
	}
	sort.Strings(venues)
	return venues
}
