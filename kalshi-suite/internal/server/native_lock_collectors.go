package server

// Dedicated bounded collectors for the final three native Systems-tab research specifications.
// They share already collected identity/candidate rows and current in-memory books; none creates an
// order, RFQ, combined market, paper position, or LIVE authority.

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

var nativeLockSystemIDs = []string{"time-nested-lock", "joint-marginal-lock", "fee-rounding-batch"}

type nativeLockScheduler struct {
	sync.Mutex
	lastTimeNested, lastJointMarginal, lastFeeRounding time.Time
}

var nativeLockSchedulers sync.Map // *Server -> *nativeLockScheduler

// Terminal settlement reconciliation is deliberately scheduled separately from native candidate
// discovery. Discovery may wait behind the heavy-research gate; authoritative settlements must
// not, or a long inference/replay pass would postpone grading already frozen observations.
type nativeTerminalScheduler struct {
	sync.Mutex
	last time.Time
}

var nativeTerminalSchedulers sync.Map // *Server -> *nativeTerminalScheduler

func (s *Server) nativeLockScheduler() *nativeLockScheduler {
	v, _ := nativeLockSchedulers.LoadOrStore(s, &nativeLockScheduler{})
	return v.(*nativeLockScheduler)
}

func (s *Server) nativeTerminalScheduler() *nativeTerminalScheduler {
	v, _ := nativeTerminalSchedulers.LoadOrStore(s, &nativeTerminalScheduler{})
	return v.(*nativeTerminalScheduler)
}

func (s *Server) nativeLockLaneDue(lane string, now time.Time) bool {
	st := s.nativeLockScheduler()
	st.Lock()
	defer st.Unlock()
	var last time.Time
	switch lane {
	case "time-nested":
		last = st.lastTimeNested
	case "joint-marginal":
		last = st.lastJointMarginal
	case "fee-rounding":
		last = st.lastFeeRounding
	default:
		return false
	}
	return last.IsZero() || now.Sub(last) >= 5*time.Minute
}

func (s *Server) sweepNativeLockLaneIfDue(ctx context.Context, lane string, now time.Time) {
	st := s.nativeLockScheduler()
	st.Lock()
	due := false
	switch lane {
	case "time-nested":
		due = st.lastTimeNested.IsZero() || now.Sub(st.lastTimeNested) >= 5*time.Minute
		if due {
			st.lastTimeNested = now
		}
	case "joint-marginal":
		due = st.lastJointMarginal.IsZero() || now.Sub(st.lastJointMarginal) >= 5*time.Minute
		if due {
			st.lastJointMarginal = now
		}
	case "fee-rounding":
		due = st.lastFeeRounding.IsZero() || now.Sub(st.lastFeeRounding) >= 5*time.Minute
		if due {
			st.lastFeeRounding = now
		}
	}
	st.Unlock()
	if !due {
		return
	}
	switch lane {
	case "time-nested":
		s.sweepNativeTimeNestedLock(ctx, now)
	case "joint-marginal":
		s.sweepNativeJointMarginalLock(ctx, now)
	case "fee-rounding":
		s.sweepNativeFeeRoundingBatch(ctx, now)
	}
}

type nativeExactLeg struct {
	Venue, Ticker, Side, BookSource, FeeSource, SourceClockID string
	Ask, Depth, Tick, Fee, QuoteAge                           float64
	QuoteObserved                                             time.Time
}

func (s *Server) nativeKalshiExactLeg(ticker, side string) (nativeExactLeg, bool) {
	if s == nil || s.kal == nil {
		return nativeExactLeg{}, false
	}
	book, age, provenance, ok := s.kal.LiveBookWithProvenance(ticker, 3*time.Second)
	if !ok || book == nil || len(book.YesBids) == 0 || len(book.YesAsks) == 0 ||
		provenance.Generation == 0 || provenance.SubscriptionID <= 0 || provenance.Sequence <= 0 || provenance.ReceivedAt.IsZero() {
		return nativeExactLeg{}, false
	}
	market, ok := s.kmkt(ticker)
	if !ok {
		return nativeExactLeg{}, false
	}
	side = strings.ToUpper(strings.TrimSpace(side))
	ask, depth := book.YesAsks[0].Price, book.YesAsks[0].Size
	if side == "NO" {
		ask, depth = 1-book.YesBids[0].Price, book.YesBids[0].Size
	}
	tick, tickKnown := market.TickForKnown(ask)
	fee, feeKnown, feeSource := s.kalFeeExact(ticker, false, 1, ask)
	if (side != "YES" && side != "NO") || ask <= 0 || ask >= 1 || depth < 1 ||
		!tickKnown || tick <= 0 || !feeKnown || fee < 0 || strings.TrimSpace(feeSource) == "" {
		return nativeExactLeg{}, false
	}
	clockID := fmt.Sprintf("kalshi-book:g%d:sid%d:seq%d|received=%s", provenance.Generation,
		provenance.SubscriptionID, provenance.Sequence, provenance.ReceivedAt.UTC().Format(time.RFC3339Nano))
	if !provenance.SourceAt.IsZero() {
		clockID += "|source=" + provenance.SourceAt.UTC().Format(time.RFC3339Nano)
	}
	return nativeExactLeg{Venue: "kalshi", Ticker: ticker, Side: side, BookSource: "kalshi_book_ws_full",
		FeeSource: feeSource, SourceClockID: clockID, Ask: ask, Depth: depth, Tick: tick, Fee: fee,
		QuoteAge: age.Seconds(), QuoteObserved: provenance.ReceivedAt.UTC()}, true
}

func nativeLockEnvelope(legs []nativeExactLeg) (cost, fees, depth, tick, age float64, feeSource, bookSource string) {
	depth, tick = math.Inf(1), math.Inf(1)
	var feeSources, bookSources []string
	for _, leg := range legs {
		cost += leg.Ask
		fees += leg.Fee
		depth = math.Min(depth, leg.Depth)
		tick = math.Min(tick, leg.Tick)
		age = math.Max(age, leg.QuoteAge)
		feeSources = append(feeSources, leg.FeeSource)
		bookSources = append(bookSources, leg.BookSource)
	}
	if math.IsInf(depth, 0) {
		depth = 0
	}
	if math.IsInf(tick, 0) {
		tick = 0
	}
	return cost, fees, depth, tick, age, r138JoinAuthorities(feeSources...), r138JoinAuthorities(bookSources...)
}

func nativeEvidenceJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return "{}"
	}
	return string(b)
}

type nativeAggregateRoute struct {
	System, Opportunity, EventID, Venue, Identity, SourceArtifact string
	QuoteSource, FeeSource, Decision, Reason, AlternativeGroup    string
	Observed, DecisionAt                                          time.Time
	QuoteAge, Tick, Qty, Depth, Fees                              float64
	PayoutLow, PayoutHigh, NetLow, NetHigh, PartialWorst          float64
	CapitalSeconds                                                float64
	Evidence                                                      any
}

func (s *Server) insertNativeAggregateRoute(ctx context.Context, v nativeAggregateRoute) (bool, error) {
	if v.Observed.IsZero() {
		v.Observed = time.Now().UTC()
	}
	if v.DecisionAt.IsZero() {
		v.DecisionAt = time.Now().UTC()
	}
	oppID := storage.ResearchRouteStableID(v.System, v.Opportunity, v.Observed.UTC().Truncate(5*time.Minute).Format(time.RFC3339))
	routeID := "control-" + storage.ResearchRouteStableID(v.QuoteSource, v.FeeSource,
		fmt.Sprintf("%.9f|%.9f|%.9f", v.NetLow, v.NetHigh, v.Qty))
	return s.store.InsertResearchRouteOpportunity(ctx, storage.ResearchRouteOpportunity{
		OpportunityID: oppID, RouteID: routeID, Observed: v.Observed, DecisionAt: v.DecisionAt,
		SystemName: v.System, CanonicalEventID: v.EventID, IdentityStatus: v.Identity,
		Venue: v.Venue, Side: "MULTI", Route: "lock", Action: "control",
		QuoteSource: v.QuoteSource, QuoteSequence: v.Observed.UTC().Format(time.RFC3339Nano),
		QuoteAgeSeconds: v.QuoteAge, QuoteAgeKnown: true,
		DecisionLatencyMS: math.Max(0, time.Since(v.DecisionAt).Seconds()*1000), LatencyKnown: true,
		TickSize: v.Tick, TickKnown: v.Tick > 0, ExecutableDepth: v.Depth, DepthKnown: true,
		RequestedQty: v.Qty, FeeAmount: v.Fees, FeeAuthority: v.FeeSource, FeeKnown: v.FeeSource != "",
		ExpectedPayoutLow: v.PayoutLow, ExpectedPayoutHigh: v.PayoutHigh,
		ExpectedNetLow: v.NetLow, ExpectedNetHigh: v.NetHigh, PartialFillWorst: v.PartialWorst,
		CapitalSeconds: math.Max(0, v.CapitalSeconds), Decision: v.Decision,
		DecisionReason: v.Reason, AlternativeGroup: v.AlternativeGroup,
		EvidenceJSON: nativeEvidenceJSON(map[string]any{"source_artifact": v.SourceArtifact,
			"aggregate_route": v.Evidence, "funded": false, "paper_authority": false, "live_authority": false}),
	})
}

func (s *Server) writeNativeCollectorReceipts(ctx context.Context, receipt storage.CollectorReceipt,
	family, platform, producerReason string, inputRows, quoteAttempts, moneyTruth int) {
	durableCtx, cancelDurability := researchDurabilityContext(ctx)
	defer cancelDurability()
	receipt.Attempted = quoteAttempts
	receipt.Completed = time.Now()
	if receipt.Status == "healthy" && receipt.Eligible == 0 {
		receipt.Status, receipt.ExpectedZero = "healthy_empty", true
		if receipt.ZeroReason == "" {
			receipt.ZeroReason = "no eligible source-native rows"
		}
	}
	_, _ = s.store.InsertCollectorReceipt(durableCtx, receipt)
	exclusions := map[string]int{}
	for k, n := range receipt.Exclusions {
		exclusions[k] = n
	}
	first, last := time.Time{}, time.Time{}
	if inputRows > 0 {
		first, last = receipt.Started, receipt.Completed
	}
	_ = s.store.InsertNativeSystemFunnelReceipts(durableCtx, []storage.NativeSystemFunnelReceipt{{
		Observed: receipt.Completed, CycleID: receipt.CollectorID + "|" + receipt.CycleID,
		Family: family, Platform: platform, OriginLayer: "strategy",
		ProducerState: "RESEARCH_ONLY_CURRENT", ProducerReason: producerReason,
		Eligible: receipt.Eligible, InputRows: inputRows, QuoteAttempts: quoteAttempts,
		MoneyTruthAttempts: moneyTruth, Exclusions: exclusions, FirstInput: first, LastInput: last,
	}})
}

func (s *Server) sweepNativeTimeNestedLock(ctx context.Context, now time.Time) {
	receipt := storage.CollectorReceipt{CollectorID: "native-time-nested-lock", CycleID: collectorCycleID(now),
		Status: "healthy", Started: now,
		Source:        "latest immutable verified implication/deadline relations + current side-specific Kalshi books + exact route fees",
		SchemaVersion: "native-time-nested-lock-r139-v1", ExpectedCadence: 5 * time.Minute,
		Systems: []string{"time-nested-lock"}, Exclusions: map[string]int{}, Metrics: map[string]any{}}
	inputRows, quoteAttempts, moneyTruth := 0, 0, 0
	defer func() {
		receipt.Metrics["verified_ordered_deadlines"] = inputRows
		receipt.Metrics["current_exact_book_fee_routes"] = moneyTruth
		receipt.Metrics["candidate_authority"] = false
		s.writeNativeCollectorReceipts(ctx, receipt, "time-nested-lock", "kalshi",
			"verified time implications are evaluated at current books; non-atomic or uncertified void paths remain controls", inputRows, quoteAttempts, moneyTruth)
	}()
	rows, err := s.store.VerifiedDeadlineRelations(ctx, "kalshi", 100)
	if err != nil {
		receipt.Status, receipt.ErrorClass, receipt.ErrorText = "error", "storage", err.Error()
		return
	}
	receipt.Eligible = len(rows)
	if len(rows) == 0 {
		receipt.ZeroReason = "immutable graph currently contains no fully verified Kalshi implication relation"
		return
	}
	for _, row := range rows {
		leftDeadline, leftOK := storage.R138DeadlineFromPredicate(row.LeftPredicate)
		rightDeadline, rightOK := storage.R138DeadlineFromPredicate(row.RightPredicate)
		if !leftOK || !rightOK || !leftDeadline.Before(rightDeadline) {
			receipt.Exclusions["deadline_axis_missing_or_inconsistent"]++
			continue
		}
		inputRows++
		left, leftOK := s.nativeKalshiExactLeg(row.LeftTicker, "NO")
		right, rightOK := s.nativeKalshiExactLeg(row.RightTicker, "YES")
		if !leftOK || !rightOK {
			receipt.Exclusions["current_side_book_tick_depth_or_fee_unavailable"]++
			continue
		}
		quoteAttempts++
		cost, fees, depth, tick, age, feeSource, bookSource := nativeLockEnvelope([]nativeExactLeg{left, right})
		moneyTruth++
		voidVerified := r138DeadlineVoidVerified(row)
		payoutLow := 0.0
		if voidVerified {
			payoutLow = 1
		}
		netLow, netHigh := payoutLow-cost-fees, 2-cost-fees
		conditionalNet := 1 - cost - fees
		decision, reason := "control", "fee-net lower bound is non-positive"
		if conditionalNet > 0 {
			decision, reason = "blocked", "conditional normal-settlement lock is positive, but two legs are non-atomic"
		}
		if !voidVerified {
			decision, reason = "blocked", "void payoff is not certified and the two-leg route is non-atomic"
		}
		inserted, insertErr := s.insertNativeAggregateRoute(ctx, nativeAggregateRoute{
			System: "time-nested-lock", Opportunity: row.RelationID, EventID: row.EventID,
			Venue: "kalshi", Identity: "verified", SourceArtifact: "immutable verified implication relation",
			QuoteSource: bookSource, FeeSource: feeSource, Decision: decision, Reason: reason,
			AlternativeGroup: row.RelationID, Observed: now, DecisionAt: time.Now(), QuoteAge: age,
			Tick: tick, Qty: 1, Depth: depth, Fees: fees, PayoutLow: payoutLow, PayoutHigh: 2,
			NetLow: netLow, NetHigh: netHigh, PartialWorst: -math.Max(left.Ask+left.Fee, right.Ask+right.Fee),
			CapitalSeconds: math.Max(0, time.Until(rightDeadline).Seconds()),
			Evidence: map[string]any{"relation_id": row.RelationID, "left_ticker": row.LeftTicker,
				"right_ticker": row.RightTicker, "left_deadline": leftDeadline, "right_deadline": rightDeadline,
				"all_leg_cost": cost, "exact_fee": fees, "conditional_normal_net_lower": conditionalNet,
				"void_verified": voidVerified, "atomic": false},
		})
		if insertErr != nil {
			receipt.Exclusions["route_storage_error"]++
			receipt.Status, receipt.ErrorClass, receipt.ErrorText = "error", "storage", insertErr.Error()
		} else if inserted {
			receipt.Inserted++
		} else {
			receipt.Duplicates++
		}
	}
}

func (s *Server) nativeCurrentBasketLeg(venue string, raw storage.BasketLeg, poly map[string]r138LinkedBook) (nativeExactLeg, bool) {
	if strings.EqualFold(venue, "kalshi") {
		return s.nativeKalshiExactLeg(raw.Ticker, raw.Side)
	}
	if !strings.EqualFold(venue, "polyus") {
		return nativeExactLeg{}, false
	}
	book, ok := poly[strings.ToLower(strings.TrimSpace(raw.Ticker))]
	if !ok || s.polyUSWS == nil {
		return nativeExactLeg{}, false
	}
	bids, asks, sourceAt, receivedAt, fullOK := s.polyUSWS.FullBookLevelsAt(raw.Ticker)
	if !fullOK || len(bids) == 0 || len(asks) == 0 || sourceAt.IsZero() || receivedAt.IsZero() {
		return nativeExactLeg{}, false
	}
	side := strings.ToUpper(strings.TrimSpace(raw.Side))
	ask, depth := asks[0].Price, asks[0].Quantity
	if side == "NO" {
		ask, depth = 1-bids[0].Price, bids[0].Quantity
	}
	fee, feeSource, feeOK := s.polyUSFeeExactAuthority(raw.Ticker, false, 1, ask)
	// An unchanged full book is change-driven, so its own receive timestamp can be old while the
	// active socket generation is healthy. FullBookLevelsAt already proves generation + lifecycle;
	// use the live transport receipt as decision age and retain both book clocks in SourceClockID.
	transportAge, transportOK := s.polyUSWS.PrimaryFrameAge()
	quoteAge := transportAge.Seconds()
	if (side != "YES" && side != "NO") || ask <= 0 || ask >= 1 || depth < 1 || book.Tick <= 0 ||
		!feeOK || feeSource == "" || !transportOK || quoteAge < 0 {
		return nativeExactLeg{}, false
	}
	clockID := "polyus-market-ws:source=" + sourceAt.UTC().Format(time.RFC3339Nano) +
		"|received=" + receivedAt.UTC().Format(time.RFC3339Nano)
	return nativeExactLeg{Venue: "polyus", Ticker: raw.Ticker, Side: side, Ask: ask, Depth: depth,
		Tick: book.Tick, Fee: fee, QuoteAge: quoteAge, BookSource: "polyus_market_ws_full", FeeSource: feeSource,
		SourceClockID: clockID, QuoteObserved: receivedAt.UTC()}, true
}

func (s *Server) sweepNativeJointMarginalLock(ctx context.Context, now time.Time) {
	receipt := storage.CollectorReceipt{CollectorID: "native-joint-marginal-lock", CycleID: collectorCycleID(now),
		Status: "healthy", Started: now,
		Source:        "prospective certified event baskets + revalidated current side-specific venue books and exact marginal fees",
		SchemaVersion: "native-joint-marginal-lock-r139-v1", ExpectedCadence: 5 * time.Minute,
		Systems: []string{"joint-marginal-lock"}, Exclusions: map[string]int{}, Metrics: map[string]any{}}
	inputRows, quoteAttempts, moneyTruth := 0, 0, 0
	defer func() {
		receipt.Metrics["certified_basket_inputs"] = inputRows
		receipt.Metrics["current_marginal_money_truth"] = moneyTruth
		receipt.Metrics["joint_quote_requests_created"] = 0
		receipt.Metrics["candidate_authority"] = false
		s.writeNativeCollectorReceipts(ctx, receipt, "joint-marginal-lock", "multi",
			"certified baskets and current marginal costs collect; no RFQ is created just to manufacture a joint quote", inputRows, quoteAttempts, moneyTruth)
	}()
	rows, err := s.store.RecentNativeBasketInputs(ctx, now.Add(-15*time.Minute), 25)
	if err != nil {
		receipt.Status, receipt.ErrorClass, receipt.ErrorText = "error", "storage", err.Error()
		return
	}
	// One newest source row per exact event/route; repeated five-minute basket frames are controls,
	// not independent inputs inside the same collector cycle.
	seen := map[string]bool{}
	var unique []storage.NativeBasketInput
	for _, row := range rows {
		key := row.Venue + "|" + row.EventID + "|" + row.Route
		if !seen[key] {
			seen[key] = true
			unique = append(unique, row)
		}
	}
	receipt.Eligible = len(unique)
	if len(unique) == 0 {
		receipt.ZeroReason = "no prospective certified event-basket row exists yet"
		return
	}
	polyBooks, _ := s.r138PolyUSLinkedBooks()
	for _, row := range unique {
		routeSafe := (row.Route == "buy-yes-all" && row.MutuallyExclusive && row.Exhaustive) ||
			(row.Route == "buy-no-all" && row.MutuallyExclusive)
		if !row.IdentityComplete || !routeSafe || len(row.Legs) < 2 || len(row.Legs) > parlayLegLimit {
			receipt.Exclusions["identity_or_leg_count_not_certified"]++
			continue
		}
		inputRows++
		legs := make([]nativeExactLeg, 0, len(row.Legs))
		complete := true
		for _, raw := range row.Legs {
			leg, ok := s.nativeCurrentBasketLeg(row.Venue, raw, polyBooks)
			if !ok || !strings.EqualFold(leg.Venue, row.Venue) {
				complete = false
				break
			}
			legs = append(legs, leg)
		}
		if !complete {
			receipt.Exclusions["current_side_book_tick_depth_or_fee_unavailable"]++
			continue
		}
		quoteAttempts++
		cost, fees, depth, tick, age, feeSource, bookSource := nativeLockEnvelope(legs)
		moneyTruth++
		receipt.Exclusions["read_only_joint_quote_unavailable"]++
		conditionalNet := row.PayoutLower - cost - fees
		inserted, insertErr := s.insertNativeAggregateRoute(ctx, nativeAggregateRoute{
			System: "joint-marginal-lock", Opportunity: row.Venue + "|" + row.EventID + "|" + row.Route,
			EventID: "venue:" + row.Venue + ":" + row.EventID, Venue: row.Venue, Identity: "structural",
			SourceArtifact: row.IdentitySource, QuoteSource: bookSource, FeeSource: feeSource,
			Decision: "blocked", Reason: "current marginal route is measured, but no read-only joint quote exists; creating an RFQ/combined market is forbidden research mutation",
			AlternativeGroup: row.EventID, Observed: now, DecisionAt: time.Now(), QuoteAge: age,
			Tick: tick, Qty: 1, Depth: depth, Fees: fees, PayoutLow: 0, PayoutHigh: float64(len(legs)),
			NetLow: -cost - fees, NetHigh: float64(len(legs)) - cost - fees,
			PartialWorst: -cost - fees,
			Evidence: map[string]any{"route": row.Route, "legs": row.Legs, "all_leg_cost": cost,
				"exact_marginal_fee": fees, "conditional_normal_payout_lower": row.PayoutLower,
				"conditional_normal_net_lower": conditionalNet, "joint_quote_available": false,
				"joint_quote_request_created": false, "atomic": false},
		})
		if insertErr != nil {
			receipt.Exclusions["route_storage_error"]++
			receipt.Status, receipt.ErrorClass, receipt.ErrorText = "error", "storage", insertErr.Error()
		} else if inserted {
			receipt.Inserted++
		} else {
			receipt.Duplicates++
		}
	}
}

type nativeFeeBatchPoint struct {
	Quantity, AggregateFee, SeparateFee, Savings float64
	FeeSource                                    string
}

func nativeFeeBatchPoints(maxQty int, feeOne float64, quote func(float64) (float64, string, bool)) ([]nativeFeeBatchPoint, bool) {
	if maxQty < 2 || feeOne < 0 || quote == nil {
		return nil, false
	}
	if maxQty > 100 { // computation bound only; never an order-size cap or recommendation.
		maxQty = 100
	}
	out := make([]nativeFeeBatchPoint, 0, maxQty-1)
	for q := 2; q <= maxQty; q++ {
		aggregate, source, ok := quote(float64(q))
		if !ok || aggregate < 0 || strings.TrimSpace(source) == "" {
			return nil, false
		}
		separate := float64(q) * feeOne
		out = append(out, nativeFeeBatchPoint{Quantity: float64(q), AggregateFee: aggregate,
			SeparateFee: separate, Savings: separate - aggregate, FeeSource: source})
	}
	return out, len(out) > 0
}

func bestNativeFeeBatchPoint(rows []nativeFeeBatchPoint) (nativeFeeBatchPoint, bool) {
	if len(rows) == 0 {
		return nativeFeeBatchPoint{}, false
	}
	rows = append([]nativeFeeBatchPoint(nil), rows...)
	sort.Slice(rows, func(i, j int) bool {
		if math.Abs(rows[i].Savings-rows[j].Savings) > 1e-12 {
			return rows[i].Savings > rows[j].Savings
		}
		return rows[i].Quantity < rows[j].Quantity
	})
	return rows[0], true
}

func (s *Server) sweepNativeFeeRoundingBatch(ctx context.Context, now time.Time) {
	receipt := storage.CollectorReceipt{CollectorID: "native-fee-rounding-batch", CycleID: collectorCycleID(now),
		Status: "healthy", Started: now,
		Source:        "recent real book-native Kalshi unit candidates + current top-level depth + actual aggregate and separate venue fee authority",
		SchemaVersion: "native-fee-rounding-batch-r139-v1", ExpectedCadence: 5 * time.Minute,
		Systems: []string{"fee-rounding-batch"}, Exclusions: map[string]int{}, Metrics: map[string]any{}}
	inputRows, quoteAttempts, moneyTruth, optimizationCandidates := 0, 0, 0, 0
	defer func() {
		receipt.Metrics["real_book_native_inputs"] = inputRows
		receipt.Metrics["fee_curves_compared"] = moneyTruth
		receipt.Metrics["positive_rounding_savings_controls"] = optimizationCandidates
		receipt.Metrics["size_search_bound"] = "all whole quantities from 2 through current top-level depth; compute-only ceiling 100"
		receipt.Metrics["candidate_authority"] = false
		s.writeNativeCollectorReceipts(ctx, receipt, "fee-rounding-batch", "kalshi",
			"actual aggregate-versus-separate fee math is joined only to a recent executable unit candidate; savings never creates alpha or money authority", inputRows, quoteAttempts, moneyTruth)
	}()
	rows, err := s.store.RecentNativeUnitCandidates(ctx, now.Add(-2*time.Hour), 50)
	if err != nil {
		receipt.Status, receipt.ErrorClass, receipt.ErrorText = "error", "storage", err.Error()
		return
	}
	receipt.Eligible = len(rows)
	if len(rows) == 0 {
		receipt.ZeroReason = "no recent open exact-fee Kalshi unit candidate has at least two units of observed depth"
		return
	}
	for _, row := range rows {
		decisionStart := time.Now()
		inputRows++
		leg, ok := s.nativeKalshiExactLeg(row.Ticker, row.Side)
		if !ok || leg.Depth < 2 {
			receipt.Exclusions["current_top_level_book_tick_depth_or_fee_unavailable"]++
			continue
		}
		quoteAttempts++
		maxQty := int(math.Floor(leg.Depth + 1e-9))
		points, ok := nativeFeeBatchPoints(maxQty, leg.Fee, func(q float64) (float64, string, bool) {
			fee, known, source := s.kalFeeExact(row.Ticker, false, q, leg.Ask)
			return fee, source, known
		})
		best, bestOK := bestNativeFeeBatchPoint(points)
		if !ok || !bestOK {
			receipt.Exclusions["aggregate_fee_authority_incomplete"]++
			continue
		}
		moneyTruth++
		optimizationCandidate := best.Savings > 1e-12
		if optimizationCandidate {
			optimizationCandidates++
		}
		principal := best.Quantity * leg.Ask
		reason := "measured control: aggregate fee has no positive saving versus separate one-unit fees"
		if optimizationCandidate {
			reason = "measured fee saving on an existing candidate; underlying executable-edge proof and sizing authority remain separate"
		}
		opportunity := row.Family + "|" + row.Ticker + "|" + row.Side + fmt.Sprintf("|episode=%d", row.Episode)
		oppID := storage.ResearchRouteStableID("fee-rounding-batch", opportunity, now.UTC().Truncate(5*time.Minute).Format(time.RFC3339))
		routeID := "batch-" + storage.ResearchRouteStableID(fmt.Sprintf("%.9f|%.0f", leg.Ask, best.Quantity), best.FeeSource)
		inserted, insertErr := s.store.InsertResearchRouteOpportunity(ctx, storage.ResearchRouteOpportunity{
			OpportunityID: oppID, RouteID: routeID, Observed: now, DecisionAt: time.Now(),
			SystemName: "fee-rounding-batch", CanonicalEventID: "venue:kalshi:" + row.Ticker,
			CanonicalPayoffID: row.Ticker + "|" + strings.ToUpper(row.Side), IdentityStatus: "verified",
			Venue: "kalshi", Ticker: row.Ticker, Side: strings.ToUpper(row.Side), Route: "taker", Action: "control",
			QuoteSource: leg.BookSource, QuoteSequence: now.UTC().Format(time.RFC3339Nano),
			QuoteAgeSeconds: leg.QuoteAge, QuoteAgeKnown: true,
			DecisionLatencyMS: time.Since(decisionStart).Seconds() * 1000, LatencyKnown: true,
			TickSize: leg.Tick, TickKnown: true,
			ExecutablePrice: leg.Ask, ExecutableDepth: leg.Depth, DepthKnown: true, RequestedQty: best.Quantity,
			FeeAmount: best.AggregateFee, FeeAuthority: best.FeeSource, FeeKnown: true,
			ExpectedPayoutLow: 0, ExpectedPayoutHigh: best.Quantity,
			ExpectedNetLow:   -principal - best.AggregateFee,
			ExpectedNetHigh:  best.Quantity - principal - best.AggregateFee,
			PartialFillWorst: -principal - best.AggregateFee,
			CapitalSeconds:   math.Max(0, row.ResolveHours*3600), Decision: "control", DecisionReason: reason,
			AlternativeGroup: opportunity, EvidenceJSON: nativeEvidenceJSON(map[string]any{
				"source_family": row.Family, "source_origin": row.Origin, "source_unit_opened": row.Opened,
				"source_unit_ask": row.Ask, "current_ask": leg.Ask, "current_top_depth": leg.Depth,
				"quantity": best.Quantity, "aggregate_fee": best.AggregateFee,
				"separate_fee": best.SeparateFee, "fee_savings": best.Savings,
				"optimization_candidate": optimizationCandidate, "all_quantity_points": points,
				"funded": false, "paper_authority": false, "live_authority": false}),
		})
		if insertErr != nil {
			receipt.Exclusions["route_storage_error"]++
			receipt.Status, receipt.ErrorClass, receipt.ErrorText = "error", "storage", insertErr.Error()
		} else if inserted {
			receipt.Inserted++
		} else {
			receipt.Duplicates++
		}
	}
}

func (s *Server) sweepNativePlaceholderSystems(ctx context.Context, now time.Time) {
	s.sweepNativeLockLaneIfDue(ctx, "time-nested", now)
	s.sweepNativeLockLaneIfDue(ctx, "joint-marginal", now)
	s.sweepNativeLockLaneIfDue(ctx, "fee-rounding", now)
	// Terminal grading owns an independent, ungated cadence below. Keep this function candidate-
	// discovery-only so the heavy research coordinator can never delay settlement continuity.
}

func (s *Server) sweepNativeTerminalSettlementsIfDue(ctx context.Context, now time.Time) {
	st := s.nativeTerminalScheduler()
	st.Lock()
	due := st.last.IsZero() || now.Sub(st.last) >= 5*time.Minute
	if due {
		st.last = now
	}
	st.Unlock()
	if !due {
		return
	}
	s.sweepNativeRouteTerminalGrades(ctx, now)
	s.sweepResearchSystemTerminalGrades(ctx, now)
}

func (s *Server) researchTerminalSettlementTick(ctx context.Context) {
	s.sweepNativeTerminalSettlementsIfDue(ctx, time.Now())
}

type nativeLockCollectionView struct {
	SystemID, CollectorID, State, Reason string
	Rows, Candidates, Controls, Blocked  int
	Grades                               int
	Cycles, Alerts                       int
	// CurrentCycle marks the compact, latest-receipt projection. These counters describe
	// collector activity in one bounded cycle; they are deliberately separate from the
	// cumulative economic/open counters above.
	CurrentCycle                                   bool
	CurrentCycleMatched, CurrentCycleInserted      int
	CurrentCycleDuplicates, CurrentCycleCandidates int
}

func nativeLockCollectorID(system string) string { return "native-" + system }

func (s *Server) queryNativeLockCollectionViews(ctx context.Context) ([]nativeLockCollectionView, error) {
	stats, statsErr := s.store.NativeRouteSystemStats(ctx, nativeLockSystemIDs)
	if statsErr != nil {
		return nil, fmt.Errorf("native route stats: %w", statsErr)
	}
	collectorIDs := make([]string, 0, len(nativeLockSystemIDs))
	for _, system := range nativeLockSystemIDs {
		collectorIDs = append(collectorIDs, nativeLockCollectorID(system))
	}
	byCollector, liveErr := s.store.NativeCollectorCycleStats(ctx, collectorIDs, time.Now().UTC())
	if liveErr != nil {
		return nil, fmt.Errorf("native collector cycles: %w", liveErr)
	}
	out := make([]nativeLockCollectionView, 0, len(nativeLockSystemIDs))
	for _, system := range nativeLockSystemIDs {
		collectorID := nativeLockCollectorID(system)
		v := nativeLockCollectionView{SystemID: system, CollectorID: collectorID,
			State: "STARTING", Reason: "waiting for first dedicated collector receipt"}
		st := stats[system]
		v.Rows, v.Candidates, v.Controls, v.Blocked, v.Grades = st.Rows, st.Candidates, st.Controls, st.Blocked, st.Grades
		if row, ok := byCollector[collectorID]; ok {
			v.Cycles = row.Cycles
			if row.Alert {
				v.Alerts = 1
			}
			switch {
			case row.NeverRan:
				v.State, v.Reason = "STARTING", "collector registered; waiting for first bounded cycle"
			case row.Status == "healthy_empty":
				v.State, v.Reason = "RESEARCH_ONLY_HEALTHY_EMPTY", row.ZeroReason
			case row.Status == "healthy" && !row.Alert:
				v.State, v.Reason = "RESEARCH_ONLY_COLLECTING", "source-native controls and exact money truth are current; zero Paper/LIVE authority"
			default:
				v.State = "COLLECTOR_" + strings.ToUpper(firstNonEmpty(row.Status, "UNKNOWN"))
				v.Reason = firstNonEmpty(firstNonEmpty(row.ErrorText, row.ZeroReason), "collector stale or unavailable")
			}
		}
		out = append(out, v)
	}
	return out, nil
}

func nativeCompactMetricInt(metrics map[string]any, key string) int {
	switch value := metrics[key].(type) {
	case int:
		return max(0, value)
	case int64:
		return max(0, int(value))
	case float64:
		if value > 0 && !math.IsNaN(value) && !math.IsInf(value, 0) {
			return int(math.Round(value))
		}
	}
	return 0
}

// queryNativeLockCollectionViewsCompact is the default UI reader. Unlike the explicit diagnostic
// above, it never counts the append-only route or collector histories. Each system is projected
// from its one indexed latest collector receipt: new + already-durable exact route matches in that
// bounded cycle, current liveness, and current optimization candidates.
func (s *Server) queryNativeLockCollectionViewsCompact(ctx context.Context) ([]nativeLockCollectionView, error) {
	collectorIDs := make([]string, 0, len(nativeLockSystemIDs))
	for _, system := range nativeLockSystemIDs {
		collectorIDs = append(collectorIDs, nativeLockCollectorID(system))
	}
	current, err := s.store.NativeCollectorCurrentStats(ctx, collectorIDs, time.Now().UTC())
	if err != nil {
		return nil, fmt.Errorf("native current collector receipts: %w", err)
	}
	out := make([]nativeLockCollectionView, 0, len(nativeLockSystemIDs))
	for _, system := range nativeLockSystemIDs {
		collectorID := nativeLockCollectorID(system)
		row := current[collectorID]
		v := nativeLockCollectionView{SystemID: system, CollectorID: collectorID,
			State: "STARTING", Reason: "collector registered; waiting for first bounded cycle"}
		if row.NeverRan {
			if row.ErrorText != "" {
				v.Reason = row.ErrorText
			}
			v.Alerts = 1
			out = append(out, v)
			continue
		}
		v.CurrentCycle = true
		v.CurrentCycleInserted = row.Inserted
		v.CurrentCycleDuplicates = row.Duplicates
		v.CurrentCycleMatched = row.Inserted + row.Duplicates
		if system == "fee-rounding-batch" {
			v.CurrentCycleCandidates = nativeCompactMetricInt(row.Metrics, "positive_rounding_savings_controls")
			if v.CurrentCycleCandidates > v.CurrentCycleMatched {
				v.CurrentCycleCandidates = v.CurrentCycleMatched
			}
		}
		if row.Alert {
			v.Alerts = 1
		}
		switch {
		case row.Status == "healthy_empty" && !row.Alert:
			v.State, v.Reason = "RESEARCH_ONLY_HEALTHY_EMPTY", firstNonEmpty(row.ZeroReason,
				"latest bounded cycle completed with no eligible exact route")
		case row.Status == "healthy" && !row.Alert:
			v.State = "RESEARCH_ONLY_COLLECTING"
			v.Reason = fmt.Sprintf("latest bounded cycle matched %d exact routes (%d new, %d already durable); lifetime totals stay in explicit diagnostics",
				v.CurrentCycleMatched, row.Inserted, row.Duplicates)
		case row.Status == "healthy" && row.Alert:
			v.State, v.Reason = "COLLECTOR_STALE", firstNonEmpty(row.ErrorText,
				"latest bounded collector receipt exceeded three expected cadences")
		default:
			v.State = "COLLECTOR_" + strings.ToUpper(firstNonEmpty(row.Status, "UNKNOWN"))
			v.Reason = firstNonEmpty(firstNonEmpty(row.ErrorText, row.ZeroReason), "collector unavailable")
		}
		out = append(out, v)
	}
	return out, nil
}

func (s *Server) nativeLockCollectionViews(ctx context.Context) []nativeLockCollectionView {
	rows, err := s.queryNativeLockCollectionViews(ctx)
	if err == nil {
		return rows
	}
	// Compatibility fallback for diagnostics that explicitly request a live report. Briefings and
	// the default research UI use the R141 last-good digest and therefore never multiply this one
	// report failure into three visible system errors.
	out := make([]nativeLockCollectionView, 0, len(nativeLockSystemIDs))
	for _, system := range nativeLockSystemIDs {
		out = append(out, nativeLockCollectionView{SystemID: system, CollectorID: nativeLockCollectorID(system),
			State: "COLLECTOR_REPORT_ERROR", Reason: err.Error(), Alerts: 1})
	}
	return out
}

func (s *Server) overlayNativeLockCoverage(ctx context.Context, rows []leaderboardCoverageRow) []leaderboardCoverageRow {
	return overlayNativeLockCoverageRows(rows, s.nativeLockCollectionViews(ctx))
}

func overlayNativeLockCoverageRows(rows []leaderboardCoverageRow, views []nativeLockCollectionView) []leaderboardCoverageRow {
	byID := make(map[string]nativeLockCollectionView, len(views))
	for _, view := range views {
		byID[view.SystemID] = view
	}
	for i := range rows {
		view, ok := byID[rows[i].Family]
		if !ok {
			continue
		}
		rows[i].State, rows[i].Note = view.State, view.Reason
		rows[i].CollectionAlerts = max(rows[i].CollectionAlerts, view.Alerts)
		if view.CurrentCycle {
			// The compact digest has one latest receipt, not a cumulative ledger scan. Never map
			// its matched/duplicate counts onto economic n, open rows, or lifetime cycles.
			rows[i].CollectionCurrentCycle = view.CurrentCycleMatched
			rows[i].CollectionCurrentCandidates = view.CurrentCycleCandidates
			rows[i].CollectionSource = view.CollectorID + ",latest_current_cycle_receipt"
			continue
		}
		rows[i].N = view.Rows
		rows[i].CollectionN, rows[i].CollectionCandidates = view.Rows, view.Candidates
		rows[i].CollectionCycles = view.Cycles
		rows[i].CollectionOpen = max(0, view.Rows-view.Grades)
		rows[i].CollectionSource = view.CollectorID + ",research_route_opportunities"
	}
	return rows
}
