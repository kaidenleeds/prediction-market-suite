package server

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

const concreteCarrySignalConsumer = "settlement-latency-carry"

func attentionHorizonTolerance(horizon int) time.Duration {
	if horizon == 60 {
		return 20 * time.Second
	}
	return time.Minute
}

func (s *Server) concreteAttentionExitFee(venue, ticker string, price float64) (float64, string, bool) {
	switch strings.ToLower(strings.TrimSpace(venue)) {
	case "kalshi":
		fee, known, source := s.kalFeeExactAction(ticker, false, 1, price, false)
		return fee, source, known
	case "polyus":
		// The current official PolyUS theta schedule is action-symmetric in price and quantity;
		// unlike Kalshi it has no distinct balance-alignment SELL rounding path.
		return s.polyUSFeeExactAuthority(ticker, false, 1, price)
	default:
		return 0, "", false
	}
}

func (s *Server) concreteAttentionExit(venue, ticker, side string, now time.Time,
	poly map[string]r138LinkedBook) (storage.AttentionMarkout, string, bool) {
	side = strings.ToUpper(strings.TrimSpace(side))
	venue = strings.ToLower(strings.TrimSpace(venue))
	if side != "YES" && side != "NO" {
		return storage.AttentionMarkout{}, "invalid_side", false
	}
	if venue == "kalshi" {
		if s.kal == nil {
			return storage.AttentionMarkout{}, "kalshi_client_unavailable", false
		}
		book, age, prov, ok := s.kal.LiveBookWithProvenance(ticker, 3*time.Second)
		if !ok || book == nil || len(book.YesBids) == 0 || len(book.YesAsks) == 0 {
			return storage.AttentionMarkout{}, "fresh_complete_kalshi_book_unavailable", false
		}
		bid, depth := book.YesBids[0].Price, book.YesBids[0].Size
		if side == "NO" {
			bid, depth = 1-book.YesAsks[0].Price, book.YesAsks[0].Size
		}
		m, mok := s.kmkt(ticker)
		if !mok {
			return storage.AttentionMarkout{}, "current_market_metadata_unavailable", false
		}
		tick, tickOK := m.TickForKnown(bid)
		fee, feeSource, feeOK := s.concreteAttentionExitFee("kalshi", ticker, bid)
		if bid <= 0 || bid >= 1 || depth < 1 || !tickOK || tick <= 0 || !feeOK || feeSource == "" {
			return storage.AttentionMarkout{}, "kalshi_exit_tick_depth_or_fee_unavailable", false
		}
		if prov.Generation == 0 || prov.SubscriptionID <= 0 || prov.Sequence <= 0 || prov.ReceivedAt.IsZero() {
			return storage.AttentionMarkout{}, "kalshi_source_clock_unavailable", false
		}
		clockID := fmt.Sprintf("kalshi-book:g%d:sid%d:seq%d|received=%s", prov.Generation,
			prov.SubscriptionID, prov.Sequence, prov.ReceivedAt.UTC().Format(time.RFC3339Nano))
		if !prov.SourceAt.IsZero() {
			clockID += "|source=" + prov.SourceAt.UTC().Format(time.RFC3339Nano)
		}
		return storage.AttentionMarkout{Venue: venue, Ticker: ticker, Side: side, ExitBid: bid, ExitDepth: depth,
			Tick: tick, QuoteAge: age.Seconds(), ExitFee: fee, BookSource: "kalshi_book_ws_full", FeeSource: feeSource,
			SourceClockID: clockID, Captured: now}, "", true
	}
	if venue != "polyus" {
		return storage.AttentionMarkout{}, "unsupported_venue", false
	}
	book, ok := poly[strings.ToLower(strings.TrimSpace(ticker))]
	if !ok || s.polyUSWS == nil {
		return storage.AttentionMarkout{}, "fresh_complete_polyus_book_unavailable", false
	}
	bids, asks, sourceAt, receivedAt, fullOK := s.polyUSWS.FullBookLevelsAt(ticker)
	if !fullOK || len(bids) == 0 || len(asks) == 0 || sourceAt.IsZero() || receivedAt.IsZero() {
		return storage.AttentionMarkout{}, "fresh_complete_polyus_book_or_source_clock_unavailable", false
	}
	quoteAge := now.Sub(receivedAt).Seconds()
	if quoteAge < 0 {
		return storage.AttentionMarkout{}, "fresh_complete_polyus_book_unavailable", false
	}
	bid, depth := bids[0].Price, bids[0].Quantity
	if side == "NO" {
		bid, depth = 1-asks[0].Price, asks[0].Quantity
	}
	fee, feeSource, feeOK := s.concreteAttentionExitFee("polyus", ticker, bid)
	if bid <= 0 || bid >= 1 || depth < 1 || book.Tick <= 0 || !feeOK || feeSource == "" {
		return storage.AttentionMarkout{}, "polyus_exit_tick_depth_or_fee_unavailable", false
	}
	return storage.AttentionMarkout{Venue: venue, Ticker: ticker, Side: side, ExitBid: bid, ExitDepth: depth,
		Tick: book.Tick, QuoteAge: quoteAge, ExitFee: fee, BookSource: "polyus_market_ws_full", FeeSource: feeSource,
		SourceClockID: "polyus-market-ws:source=" + sourceAt.UTC().Format(time.RFC3339Nano) +
			"|received=" + receivedAt.UTC().Format(time.RFC3339Nano), Captured: now}, "", true
}

func (s *Server) sweepConcreteTimedEvidence(ctx context.Context, now time.Time) {
	started := time.Now()
	receipt := storage.CollectorReceipt{CollectorID: "concrete-attention-horizons",
		CycleID: collectorCycleID(started), ExperimentID: "attention-spillover-graph", ExperimentVersion: 1,
		Status: "healthy", Started: started, Source: storage.R139ConcreteAttentionSource,
		SchemaVersion: "attention-executable-markout-v1", ExpectedCadence: 5 * time.Second, Exclusions: map[string]int{},
		Systems: []string{"attention-spillover-graph"}, Metrics: map[string]any{}}
	defer func() {
		receipt.Completed = time.Now()
		dctx, cancel := researchDurabilityContext(ctx)
		defer cancel()
		_, _ = s.store.InsertCollectorReceipt(dctx, receipt)
	}()
	due, err := s.store.PendingAttentionMarkouts(ctx, now, 100)
	if err != nil {
		receipt.Status, receipt.ErrorClass, receipt.ErrorText = "error", "storage", err.Error()
		return
	}
	receipt.Eligible, receipt.Attempted = len(due), len(due)
	if len(due) == 0 {
		receipt.Status, receipt.ExpectedZero = "healthy_empty", true
		receipt.ZeroReason = "no linked attention observation has a 60s or 300s mark due"
		return
	}
	poly, _ := s.r138PolyUSLinkedBooks()
	for _, row := range due {
		late := now.After(row.Target.Add(attentionHorizonTolerance(row.Horizon)))
		mark, reason, exactExitOK := s.concreteAttentionExit(row.Venue, row.Ticker, row.Side, now, poly)
		if late {
			missReason := "collector_poll_arrived_after_fixed_horizon_tolerance"
			if !exactExitOK {
				missReason = "fixed_horizon_expired_without_exact_exit:" + reason
			}
			inserted, missErr := s.store.InsertAttentionMarkoutMiss(ctx, row, now, missReason)
			if missErr != nil {
				receipt.Exclusions["miss_storage_error"]++
			} else if inserted {
				receipt.Inserted++
				receipt.Exclusions["fixed_horizon_missed"]++
			} else {
				receipt.Duplicates++
			}
			continue
		}
		if !exactExitOK {
			receipt.Exclusions["waiting_"+reason]++
			continue
		}
		mark.ObservationID, mark.Horizon, mark.Target = row.ObservationID, row.Horizon, row.Target
		mark.EntryAllIn = row.EntryCost + row.EntryFee
		mark.Markout = mark.ExitBid - mark.ExitFee - mark.EntryAllIn
		mark.Evidence = map[string]any{"observation_id": row.ObservationID, "horizon_s": row.Horizon,
			"target_ts": row.Target, "capture_offset_ms": now.Sub(row.Target).Seconds() * 1000, "entry_cost": row.EntryCost,
			"entry_fee": row.EntryFee, "exit_is_taker_sell_at_executable_bid": true}
		inserted, insertErr := s.store.InsertAttentionMarkout(ctx, mark)
		if insertErr != nil {
			receipt.Exclusions["markout_storage_error"]++
		} else if inserted {
			receipt.Inserted++
		} else {
			receipt.Duplicates++
		}
	}
}

type collateralComparable struct {
	Market                   storage.MarketGameRow
	League, Phase, Liquidity string
	Rank                     string
}

func chooseCollateralNoReleaseControl(target storage.MarketGameRow, targetLeague, targetPhase, targetLiquidity, creditHash string,
	candidates []collateralComparable, creditedGames map[string]bool) (storage.MarketGameRow, bool) {
	best, bestRank := storage.MarketGameRow{}, ""
	for _, c := range candidates {
		m := c.Market
		if m.Venue != target.Venue || m.GameID == target.GameID || m.MktType != target.MktType ||
			c.League != targetLeague || c.Phase != targetPhase || c.Liquidity != targetLiquidity || creditedGames[m.GameID] || m.Src != "struct" {
			continue
		}
		if (m.MktType == "spread" || m.MktType == "total") && math.Abs(m.Line-target.Line) > 1e-9 {
			continue
		}
		rank := storage.R138HashJSON(map[string]any{"credit": creditHash, "game": m.GameID, "ticker": m.Ticker})
		if bestRank == "" || rank < bestRank || (rank == bestRank && m.Ticker < best.Ticker) {
			best, bestRank = m, rank
		}
	}
	return best, bestRank != ""
}

func (s *Server) insertConcreteObserverControl(ctx context.Context, system, opportunity, cohort, source,
	certificate, blocker string, observed time.Time, inputs map[string]any) (bool, error) {
	exists, existsErr := s.store.ConcreteResearchObservationExists(ctx, system, opportunity, "observer", "control", 0)
	if existsErr != nil {
		return false, existsErr
	}
	if exists {
		return false, nil
	}
	_, inserted, err := s.store.InsertResearchSystemObservation(ctx, storage.ResearchSystemObservation{
		Observed: observed, SystemID: system, OpportunityID: opportunity, Kind: "control", Cohort: cohort,
		Venue: "multi", Route: "observer", CertificateStatus: "not_applicable", CertificateHash: certificate,
		SourceClockID: "frozen-control:" + observed.UTC().Format(time.RFC3339Nano), SourceArtifact: source,
		PayoutLower: 0, PayoutUpper: 0, NetLower: 0, NetUpper: 0, OutcomeStatus: "settled", Blocker: blocker, Inputs: inputs})
	return inserted, err
}

func (s *Server) collectConcreteCarryProspective(ctx context.Context, now time.Time, poly map[string]r138LinkedBook, metrics map[string]int) {
	cursor, err := s.store.ConcreteSignalCursor(ctx, concreteCarrySignalConsumer)
	if err != nil {
		metrics["carry_cursor_read_error"]++
		return
	}
	metrics["carry_cursor_before"] = int(cursor)
	triggers, err := s.store.ConcreteSignalTriggersAfter(ctx, now.Add(-10*time.Minute), cursor, concreteSignalPageLimit)
	if err != nil {
		metrics["carry_prospective_source_error"]++
		return
	}
	metrics["carry_page_rows"] = len(triggers)
	pageFailed := false
	for _, trigger := range triggers {
		identity, ok, identityErr := s.store.CurrentCanonicalInstrument(ctx, trigger.Platform, trigger.Ticker)
		if identityErr != nil {
			metrics["carry_identity_read_error"]++
			pageFailed = true
			continue
		}
		legs, bookOK := s.concreteExactPair(trigger.Platform, trigger.Ticker, poly)
		if !ok || identity.IdentityStatus != "verified" || !bookOK {
			metrics["carry_current_money_truth_unavailable"]++
			continue
		}
		var leg nativeExactLeg
		for _, candidate := range legs {
			if candidate.Side == strings.ToUpper(trigger.Side) {
				leg = candidate
			}
		}
		if leg.Ticker == "" || leg.Ask < .90 {
			continue
		}
		freezeID := "carry-freeze|" + trigger.Platform + "|" + trigger.Ticker + "|" + strconv.FormatInt(trigger.ID, 10)
		prior, _, freezeErr := s.store.FreezeCarryInput(ctx, storage.CarryFreezeRequest{FreezeID: freezeID, Frozen: now,
			SignalID: trigger.ID, Venue: trigger.Platform, Ticker: trigger.Ticker, Side: trigger.Side,
			EntryAllIn: leg.Ask + leg.Fee, ExpectedHoldDays: math.Max(0, trigger.ResolveHours/24), Evidence: map[string]any{
				"discovery_signal_id": trigger.ID, "discovery_price_used": false, "current_book_repriced": true}})
		if freezeErr != nil {
			metrics["carry_freeze_error"]++
			pageFailed = true
			continue
		}
		inputs := map[string]any{"freeze_id": prior.FreezeID, "source_cutoff_ts": prior.SourceCutoff,
			"void_prior_n": prior.VoidPriorN, "void_prior_events": prior.VoidPriorEvents,
			"void_probability_upper": prior.VoidProbabilityUpper, "void_reserve": prior.VoidReserve,
			"void_policy": prior.VoidPolicy, "benchmark_kind": prior.BenchmarkKind,
			"benchmark_receipt_id": prior.BenchmarkReceiptID, "benchmark_rate_per_dollar_day": prior.BenchmarkRate,
			"benchmark_reserve": prior.BenchmarkReserve, "benchmark_justification": prior.BenchmarkJustification,
			"expected_hold_days": prior.ExpectedHoldDays, "inputs_frozen_before_economic_observation": true}
		controlInputs := map[string]any{}
		for k, v := range inputs {
			controlInputs[k] = v
		}
		controlInputs["control_realized_net"] = 0
		if inserted, controlErr := s.insertConcreteObserverControl(ctx, "settlement-latency-carry", freezeID+"|no-trade",
			"frozen-carry-matched-no-trade", "prior-only carry input freeze", prior.SpecHash, prior.Blocker, now, controlInputs); controlErr != nil {
			metrics["carry_control_insert_error"]++
			pageFailed = true
		} else if inserted {
			metrics["carry_control_inserted"]++
		}
		if prior.InputState != "ready" {
			metrics["carry_blocked_"+prior.Blocker]++
			continue
		}
		cost, fee := leg.Ask, leg.Fee
		// The executable observation's enforced payoff envelope remains the true one-share worst
		// case. The separate success-carry estimate applies the frozen void/censor and opportunity
		// reserves without pretending the contract is certain; later terminal inference, not this
		// estimate, decides whether the route has a positive lower bound.
		allIn := cost + fee
		reserveAdjustedSuccessCarry := 1 - allIn - prior.VoidReserve - prior.BenchmarkReserve
		netLow := math.Min(-allIn-prior.BenchmarkReserve, reserveAdjustedSuccessCarry)
		inputs["reserve_adjusted_success_carry_lower"] = reserveAdjustedSuccessCarry
		candidateOpportunity := freezeID + "|action"
		exists, existsErr := s.store.ConcreteResearchObservationExists(ctx, "settlement-latency-carry",
			candidateOpportunity, "taker", "candidate", 1)
		if existsErr != nil {
			metrics["carry_candidate_dedupe_error"]++
			pageFailed = true
			continue
		}
		if exists {
			metrics["carry_candidate_duplicates"]++
			continue
		}
		_, inserted, insertErr := s.store.InsertResearchSystemObservation(ctx, storage.ResearchSystemObservation{
			Observed: now, SystemID: "settlement-latency-carry", OpportunityID: candidateOpportunity, Kind: "candidate",
			Cohort: "near-certain-prior-reserved-carry", CanonicalEventID: identity.EventID, EventVersion: identity.EventVersion,
			CanonicalPayoffID: identity.PayoffID, PayoffVersion: identity.PayoffVersion, Venue: leg.Venue, Ticker: leg.Ticker,
			Route: "taker", Side: leg.Side, CertificateStatus: "verified", CertificateHash: prior.SpecHash,
			SourceClockID: leg.SourceClockID, SourceArtifact: "current exact book + immutable prior-only carry reserve/benchmark",
			BookSource: leg.BookSource, FeeSource: leg.FeeSource, QuoteAgeMax: leg.QuoteAge, TickMin: leg.Tick, Size: 1,
			Cost: cost, Fee: fee, PayoutLower: 0, PayoutUpper: 1, NetLower: netLow, NetUpper: 1 - cost - fee,
			VisibleCapacity: leg.Depth, CapitalSeconds: prior.ExpectedHoldDays * 86400,
			DecisionLatencyMS: math.Max(0, time.Since(now).Seconds()*1000), LatencyKnown: true, QuoteAgeKnown: true,
			TickKnown: true, DepthKnown: true, FeeKnown: true, CapacityCurve: []storage.CapacityPoint{{Size: 1, Cost: cost,
				Fee: fee, PayoutFloor: 0, NetFloor: netLow}}, OutcomeStatus: "open", Candidate: true, Inputs: inputs})
		if insertErr != nil {
			metrics["carry_candidate_insert_error"]++
			pageFailed = true
		} else if inserted {
			metrics["carry_candidate_inserted"]++
		}
	}
	if len(triggers) == 0 || pageFailed || ctx.Err() != nil {
		return
	}
	durableCtx, cancel := researchDurabilityContext(ctx)
	defer cancel()
	if err := s.store.AdvanceConcreteSignalCursor(durableCtx, concreteCarrySignalConsumer,
		triggers[len(triggers)-1].ID); err != nil {
		metrics["carry_cursor_advance_error"]++
		return
	}
	metrics["carry_cursor_advanced"] += len(triggers)
	metrics["carry_cursor_after"] = int(triggers[len(triggers)-1].ID)
}
