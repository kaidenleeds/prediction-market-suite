package server

import (
	"context"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/paper"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

const researchBundleQuoteFreshFor = 3 * time.Second

func researchBundleLiveAcceptAllowed(paperOnly, armed, liveAuto bool) bool {
	return !paperOnly && armed && liveAuto
}

func autoComboRequiresDistinctEvents(exactCertifiedConjunction bool) bool {
	return !exactCertifiedConjunction
}

func (s *Server) researchBundlePaperAutoReady() bool {
	s.autoMu.Lock()
	paperAuto, invert := s.autoOn, s.autoInvert
	s.autoMu.Unlock()
	s.sugMu.Lock()
	collecting := s.collecting
	s.sugMu.Unlock()
	return paperAuto && !invert && !collecting && !s.ksBlocked()
}

func researchBundleRequestMatches(h storage.ResearchRouteBundleHandoff, legs []liveComboValueLeg) bool {
	if !h.ComboEquivalent || !h.SealedUntouched || len(legs) != len(h.Bundle.Legs) ||
		len(legs) < 2 || len(legs) > 6 {
		return false
	}
	for i := range legs {
		if legs[i].Ticker != h.Bundle.Legs[i].Ticker ||
			!strings.EqualFold(legs[i].Side, h.Bundle.Legs[i].Side) {
			return false
		}
	}
	return true
}

func researchBundlePaperLegs(h storage.ResearchRouteBundleHandoff) []paper.Leg {
	out := make([]paper.Leg, 0, len(h.Bundle.Legs))
	for _, leg := range h.Bundle.Legs {
		entry := leg.IntegratedCost / math.Max(leg.Quantity, 1e-12)
		out = append(out, paper.Leg{Platform: "kalshi", Ticker: leg.Ticker,
			Side: leg.Side, Title: leg.Ticker, Outcome: leg.Side, Entry: entry})
	}
	return out
}

func (s *Server) researchBundlePaperAvailable(ctx context.Context) float64 {
	return math.Max(0, s.parlayAvailableUSD(ctx))
}

// researchBundleComboProposals is the only automatic source of typed solver bundles. It is empty
// until a sealed untouched route=rfq PASS exists. Collection matching is current and exact; no RFQ
// is created here because proposal discovery remains read-only.
func (s *Server) researchBundleComboProposals(ctx context.Context) []map[string]any {
	if !s.researchBundlePaperAutoReady() || s.store == nil {
		return nil
	}
	handoffs, err := s.store.CurrentResearchRouteBundleHandoffs(ctx,
		time.Now().UTC().Add(-20*time.Second), s.liveMirrorEdgeFloor(), 20)
	if err != nil || len(handoffs) == 0 {
		return nil
	}
	collections, err := s.kal.ComboCollections(ctx)
	if err != nil {
		return nil
	}
	out := []map[string]any{}
	for _, h := range handoffs {
		legs := make([]map[string]any, 0, len(h.Bundle.Legs))
		eventCounts := map[string]int{}
		valid := true
		for _, leg := range h.Bundle.Legs {
			m, ok := s.kmkt(leg.Ticker)
			if !ok || m.EventTicker == "" {
				valid = false
				break
			}
			eventCounts[m.EventTicker]++
			legs = append(legs, map[string]any{"ticker": leg.Ticker, "event": m.EventTicker,
				"side": strings.ToLower(leg.Side)})
		}
		if !valid {
			continue
		}
		for _, col := range collections {
			if (col.SizeMin > 0 && len(legs) < col.SizeMin) || (col.SizeMax > 0 && len(legs) > col.SizeMax) {
				continue
			}
			allowed, quoters := true, true
			for _, raw := range legs {
				event, _ := raw["event"].(string)
				side, _ := raw["side"].(string)
				if !col.Events[event] || col.LegOK(event, side) != "" {
					allowed = false
					break
				}
				if cfg, ok := col.EventCfg[event]; ok {
					if cfg.SizeMax > 0 && eventCounts[event] > cfg.SizeMax {
						allowed = false
						break
					}
					quoters = quoters && cfg.Quoters > 0
				}
			}
			if !allowed {
				continue
			}
			// The created RFQ security is the YES side of the conjunction. Its constituent legs may
			// themselves be YES or NO, but those leg directions are not separate NO-side RFQ orders.
			// Stamp liveness only after a current collection accepts the complete ordered bundle.
			s.noteR147SignalRuntime("payoff-constraint-solver", "kalshi", "YES", "rfq",
				h.Bundle.BundleID, "research_route_bundle:sealed_handoff", time.Now(), true, "")
			out = append(out, map[string]any{"collection_ticker": col.CollectionTicker,
				"legs": legs, "n_legs": len(legs), "quoters": quoters,
				"live_auto_eligible": true, "research_bundle_id": h.Bundle.BundleID,
				"sealed_proof_run_id": h.ProofRunID, "max_all_in_unit": h.MaxAllInUnit})
			break
		}
	}
	return out
}

func (s *Server) researchBundlePendingLiveProposals(ctx context.Context) []map[string]any {
	if !s.researchBundlePaperAutoReady() || s.store == nil {
		return nil
	}
	intents, err := s.store.PendingResearchRouteBundleLiveIntents(ctx, 20)
	if err != nil || len(intents) == 0 {
		return nil
	}
	collections, err := s.kal.ComboCollections(ctx)
	if err != nil {
		return nil
	}
	out := []map[string]any{}
	for _, intent := range intents {
		h, found, err := s.store.ResearchRouteBundleHandoffStatus(ctx, intent.BundleID,
			s.liveMirrorEdgeFloor())
		if err != nil || !found || !h.ComboEquivalent || !h.SealedUntouched {
			continue
		}
		legs := make([]map[string]any, 0, len(h.Bundle.Legs))
		eventCounts := map[string]int{}
		valid := true
		for _, leg := range h.Bundle.Legs {
			m, ok := s.kmkt(leg.Ticker)
			if !ok || m.EventTicker == "" {
				valid = false
				break
			}
			eventCounts[m.EventTicker]++
			legs = append(legs, map[string]any{"ticker": leg.Ticker, "event": m.EventTicker,
				"side": strings.ToLower(leg.Side)})
		}
		if !valid {
			continue
		}
		for _, col := range collections {
			if (col.SizeMin > 0 && len(legs) < col.SizeMin) || (col.SizeMax > 0 && len(legs) > col.SizeMax) {
				continue
			}
			allowed, quoters := true, true
			for _, raw := range legs {
				event, _ := raw["event"].(string)
				side, _ := raw["side"].(string)
				cfg, cfgOK := col.EventCfg[event]
				if !col.Events[event] || col.LegOK(event, side) != "" ||
					(cfgOK && cfg.SizeMax > 0 && eventCounts[event] > cfg.SizeMax) {
					allowed = false
					break
				}
				if cfgOK {
					quoters = quoters && cfg.Quoters > 0
				}
			}
			if allowed {
				out = append(out, map[string]any{"collection_ticker": col.CollectionTicker,
					"legs": legs, "n_legs": len(legs), "quoters": quoters,
					"live_auto_eligible": true, "research_intent_id": intent.IntentID,
					"sealed_proof_run_id": h.ProofRunID, "max_all_in_unit": intent.MaxAllInUnit})
				break
			}
		}
	}
	return out
}

type researchBundlePaperSchedule struct {
	sync.Mutex
	last time.Time
}

var researchBundlePaperSchedules sync.Map

func (s *Server) sweepResearchBundlePaperDispatch(ctx context.Context, now time.Time) {
	if !s.researchBundlePaperAutoReady() {
		return
	}
	v, _ := researchBundlePaperSchedules.LoadOrStore(s, &researchBundlePaperSchedule{})
	state := v.(*researchBundlePaperSchedule)
	state.Lock()
	if !state.last.IsZero() && now.Sub(state.last) < 7*time.Second {
		state.Unlock()
		return
	}
	state.last = now
	state.Unlock()
	for _, proposal := range s.researchBundleComboProposals(ctx) {
		bundleID, _ := proposal["research_bundle_id"].(string)
		if bundleID == "" {
			continue
		}
		if !s.r166CanBookVerifiedMultiLegPaper() {
			// One proposal is one venue-created conjunction security. Its constituent leg sides
			// are payoff inputs, not separately executable RFQ sides. Preserve one exact YES/RFQ
			// terminal keyed by the sealed bundle identity.
			s.noteR147SignalContractRuntime("payoff-constraint-solver", "kalshi", "YES", "rfq", "K",
				bundleID, "research_route_bundle:paper_execution", "excluded", "PAPER-NOT-OBSERVED",
				r166UnverifiedMultiLegPaperReason, now, true, "")
			_ = s.store.Audit(ctx, "info", "paper",
				"typed research RFQ candidate observed without creating an RFQ or booking Paper P&L",
				bundleID+" · "+r166UnverifiedMultiLegPaperReason)
			break
		}
		code, _ := s.selfPOST(s.handleLiveComboPlace, "/api/live/combo/place", map[string]any{
			"collection_ticker": proposal["collection_ticker"], "legs": proposal["legs"],
			"auto": true, "paper_only": true, "research_bundle_id": bundleID})
		_ = code
		break // bounded: at most one authenticated no-money RFQ per sweep
	}
}

func sameOpenResearchBundleQuote(quotes []kalshi.Quote, rfqID, quoteID, marketTicker string,
	price float64) (kalshi.Quote, bool) {
	return sameOpenRFQQuote(quotes, rfqID, quoteID, marketTicker, price, 1)
}

func sameOpenRFQQuote(quotes []kalshi.Quote, rfqID, quoteID, marketTicker string,
	price, quantity float64) (kalshi.Quote, bool) {
	for _, q := range quotes {
		if q.ID != quoteID || q.RFQID != rfqID || q.MarketTicker != marketTicker ||
			!strings.EqualFold(q.Status, "open") {
			continue
		}
		px, pxOK := parseFiniteFloat(q.YesBidDollars)
		qty, qtyOK := parseFiniteFloat(q.YesContractsFp)
		if !qtyOK || qty <= 0 {
			qty, qtyOK = parseFiniteFloat(q.ContractsFp)
		}
		if pxOK && qtyOK && math.Abs(px-price) <= 1e-9 && math.Abs(qty-quantity) <= 1e-9 {
			return q, true
		}
	}
	return kalshi.Quote{}, false
}

func parseFiniteFloat(raw string) (float64, bool) {
	v, err := strconv.ParseFloat(raw, 64)
	return v, err == nil && !math.IsNaN(v) && !math.IsInf(v, 0)
}
