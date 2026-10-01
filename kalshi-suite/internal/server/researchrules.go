package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/objectiveidentity"
	"github.com/kalshi-suite/kalshi-suite/internal/polymarket"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

// researchVenueRuleArtifact is the exact venue payload attached to a canonical instrument. Raw
// artifacts are useful even when semantic normalization is incomplete; they must never be treated
// as cross-venue equality merely because the event/title matcher agrees.
type researchVenueRuleArtifact struct {
	Source, Hash, SettlementSource                          string
	VoidPolicy, ScalarPolicy, UnknownPolicy                 string
	OvertimePolicy, TiePolicy, MaterialPolicySource         string
	Primary, Secondary, EarlyClose, Description, SportsType string
	SettlementSources                                       []storage.RuleSettlementSource
}

func researchPolymarketRuleArtifact(m polymarket.Market) (researchVenueRuleArtifact, bool) {
	description, resolutionSource := strings.TrimSpace(m.Description), strings.TrimSpace(m.ResolutionSource)
	if description == "" && resolutionSource == "" {
		return researchVenueRuleArtifact{}, false
	}
	artifact := researchVenueRuleArtifact{Source: "polymarket-gamma-complete-catalog",
		Description: description, SportsType: strings.TrimSpace(m.SportsMarketType)}
	if resolutionSource != "" {
		artifact.SettlementSource = resolutionSource
		artifact.SettlementSources = []storage.RuleSettlementSource{{Name: "Gamma resolutionSource", URL: resolutionSource}}
	}
	artifact.Hash = storage.R138HashJSON(map[string]any{"description": artifact.Description,
		"resolution_source": resolutionSource, "sports_market_type": artifact.SportsType})
	return artifact, true
}

func explicitRulePolicies(raw string) (voidPolicy, scalarPolicy, unknownPolicy string) {
	// Natural-language rule prose is retained byte-for-byte as evidence but is not a normalized
	// policy. Even an apparently clear phrase can hide timing, cancellation, exception, or scalar
	// differences. Only an independently reviewed certificate can populate these fields.
	_ = raw
	return "", "", ""
}

func (s *Server) cacheResearchEventRuleSources(snap kalshi.EventSnapshot) {
	sources := make([]storage.RuleSettlementSource, 0, len(snap.SettlementSources))
	for _, source := range snap.SettlementSources {
		sources = append(sources, storage.RuleSettlementSource{Name: source.Name, URL: source.URL})
	}
	if len(sources) == 0 {
		return
	}
	s.researchRuleMu.Lock()
	if s.researchRuleSources == nil {
		s.researchRuleSources = map[string][]storage.RuleSettlementSource{}
	}
	for _, market := range snap.Markets {
		if market.Ticker != "" {
			s.researchRuleSources[storage.ResolutionBasisKey("kalshi", market.Ticker)] = append([]storage.RuleSettlementSource(nil), sources...)
		}
	}
	s.researchRuleMu.Unlock()
}

func (s *Server) researchRuleArtifacts(rows []storage.StructuralIdentityRow) map[string]researchVenueRuleArtifact {
	wanted := make(map[string]bool, len(rows))
	for _, row := range rows {
		wanted[storage.ResolutionBasisKey(row.Venue, row.Ticker)] = true
	}
	out := make(map[string]researchVenueRuleArtifact, len(wanted))
	s.metaMu.Lock()
	for ticker, market := range s.kmkts {
		key := storage.ResolutionBasisKey("kalshi", ticker)
		if !wanted[key] {
			continue
		}
		raw := strings.Join([]string{market.RulesPrimary, market.RulesSecondary, market.EarlyCloseCondition}, "\n")
		if strings.TrimSpace(raw) == "" {
			continue
		}
		voidPolicy, scalarPolicy, unknownPolicy := explicitRulePolicies(raw)
		artifact := researchVenueRuleArtifact{Source: "kalshi-market-rest-rules",
			Primary: market.RulesPrimary, Secondary: market.RulesSecondary, EarlyClose: market.EarlyCloseCondition,
			VoidPolicy: voidPolicy, ScalarPolicy: scalarPolicy, UnknownPolicy: unknownPolicy}
		artifact.Hash = storage.R138HashJSON(map[string]any{"primary": artifact.Primary,
			"secondary": artifact.Secondary, "early_close": artifact.EarlyClose})
		out[key] = artifact
	}
	s.metaMu.Unlock()
	s.researchRuleMu.Lock()
	for key, artifact := range out {
		if sources := s.researchRuleSources[key]; len(sources) > 0 {
			artifact.SettlementSources = append([]storage.RuleSettlementSource(nil), sources...)
			out[key] = artifact
		}
	}
	s.researchRuleMu.Unlock()

	s.polyUSMu.Lock()
	for _, market := range s.pusSweep {
		key := storage.ResolutionBasisKey("polyus", market.Slug)
		if !wanted[key] || strings.TrimSpace(market.Description) == "" {
			continue
		}
		voidPolicy, scalarPolicy, unknownPolicy := explicitRulePolicies(market.Description)
		artifact := researchVenueRuleArtifact{Source: "polyus-complete-market-rest-description",
			Description: market.Description, SportsType: market.SportsType,
			VoidPolicy: voidPolicy, ScalarPolicy: scalarPolicy, UnknownPolicy: unknownPolicy}
		artifact.Hash = storage.R138HashJSON(map[string]any{"description": artifact.Description,
			"sports_market_type": artifact.SportsType})
		out[key] = artifact
	}
	s.polyUSMu.Unlock()
	// Gamma's complete-catalog snapshot is the official cached discovery schema and contains both
	// description and resolutionSource. This read is intentionally cache-only: the five-minute rule
	// collector must never create a second network crawl or subscription flood.
	if s.poly != nil {
		for _, market := range s.poly.CompleteCatalogSnapshot().Markets {
			key := storage.ResolutionBasisKey("polymarket", market.ConditionID)
			if !wanted[key] {
				continue
			}
			if artifact, ok := researchPolymarketRuleArtifact(market); ok {
				out[key] = artifact
			}
		}
	}
	return out
}

func (s *Server) sweepResearchRuleArtifacts(ctx context.Context) {
	started := time.Now().UTC()
	source := storage.R139CrossVenueRuleArtifactSource
	schema := "cross-venue-rules-r139-v3"
	var pairCursor storage.StructuralRuleCursor
	if raw, ok := s.store.KVGet(ctx, "r139:objective-rule-pair-cursor"); ok && raw != "" {
		_ = json.Unmarshal([]byte(raw), &pairCursor)
	}
	pairPage, err := s.store.StructuralRuleCandidatePageAfter(ctx, started.Add(-20*time.Minute), pairCursor, 200)
	pairs := pairPage.Rows
	if err != nil {
		dctx, cancel := researchDurabilityContext(ctx)
		defer cancel()
		_, _ = s.store.InsertCollectorReceipt(dctx, storage.CollectorReceipt{
			CollectorID: "cross-venue-rule-artifacts", CycleID: collectorCycleID(started),
			ExperimentID: "identity-challenged-cross-venue-lock", ExperimentVersion: 1,
			Status: "error", Started: started, Completed: time.Now(), ErrorClass: "storage_read",
			ErrorText: err.Error(), Source: source, SchemaVersion: schema, ExpectedCadence: 5 * time.Minute,
			Systems: []string{"identity-challenged-cross-venue-lock", "payoff-constraint-solver"},
		})
		return
	}
	receipt := storage.CollectorReceipt{CollectorID: "cross-venue-rule-artifacts",
		CycleID: collectorCycleID(started), ExperimentID: "identity-challenged-cross-venue-lock",
		ExperimentVersion: 1, Status: "healthy", Started: started, Source: source,
		SchemaVersion: schema, ExpectedCadence: 5 * time.Minute,
		Systems:  []string{"identity-challenged-cross-venue-lock", "payoff-constraint-solver"},
		Eligible: len(pairs), Attempted: len(pairs), Exclusions: map[string]int{}, Metrics: map[string]any{}}
	anchorResult := s.sweepObjectiveAnchorDecisions(ctx, started)
	if len(pairs) == 0 && anchorResult.eligible == 0 {
		receipt.Status, receipt.ExpectedZero = "healthy_empty", true
		receipt.ZeroReason = "no current exact structural pair across Kalshi, PolyUS, and Polymarket exists in the bounded universe"
		receipt.Completed = time.Now()
		dctx, cancel := researchDurabilityContext(ctx)
		defer cancel()
		_, _ = s.store.InsertCollectorReceipt(dctx, receipt)
		return
	}
	rows := make([]storage.StructuralIdentityRow, 0, len(pairs)*2)
	for _, pair := range pairs {
		rows = append(rows,
			storage.StructuralIdentityRow{Venue: pair.LeftVenue, Ticker: pair.LeftInstrumentID},
			storage.StructuralIdentityRow{Venue: pair.RightVenue, Ticker: pair.RightInstrumentID})
	}
	artifacts := s.researchRuleArtifacts(rows)
	storedArtifacts := make(map[string]storage.VenueRuleArtifact, len(artifacts))
	for _, pair := range pairs {
		for _, leg := range []struct{ venue, id string }{{pair.LeftVenue, pair.LeftInstrumentID}, {pair.RightVenue, pair.RightInstrumentID}} {
			artifact, ok := artifacts[storage.ResolutionBasisKey(leg.venue, leg.id)]
			if !ok {
				receipt.Exclusions["missing_"+leg.venue+"_raw_rules"]++
				continue
			}
			stored, inserted, insertErr := s.store.InsertVenueRuleArtifact(ctx, storage.VenueRuleArtifact{
				Venue: leg.venue, InstrumentID: leg.id, Source: artifact.Source,
				Primary: artifact.Primary, Secondary: artifact.Secondary, EarlyClose: artifact.EarlyClose,
				Description: artifact.Description, SportsType: artifact.SportsType,
				SettlementSources: artifact.SettlementSources, Observed: started,
			})
			if insertErr != nil {
				receipt.Exclusions["artifact_storage_error"]++
				receipt.ErrorClass, receipt.ErrorText = "storage_write", insertErr.Error()
			} else if inserted {
				storedArtifacts[storage.ResolutionBasisKey(leg.venue, leg.id)] = stored
				receipt.Inserted++
			} else {
				storedArtifacts[storage.ResolutionBasisKey(leg.venue, leg.id)] = stored
				receipt.Duplicates++
			}
		}
		leftKey := storage.ResolutionBasisKey(pair.LeftVenue, pair.LeftInstrumentID)
		rightKey := storage.ResolutionBasisKey(pair.RightVenue, pair.RightInstrumentID)
		left, right, verdict, certificateHash, decisionErr := s.objectiveRuleDecision(ctx, pair,
			storedArtifacts[leftKey], storedArtifacts[rightKey], artifacts[leftKey], artifacts[rightKey])
		if decisionErr != nil {
			receipt.Exclusions["objective_match_prerequisite_error"]++
			// The rejected immutable decision still records the exact prerequisite failure. A
			// transient storage error must not make the candidate vanish from audit truth.
			receipt.ErrorClass, receipt.ErrorText = "objective_match", objectiveDecisionErrorClass(decisionErr)
		}
		decisionInserted, logErr := s.recordObjectiveMatchDecision(ctx, started, pair, left, right, verdict, certificateHash)
		if logErr != nil {
			receipt.Exclusions["decision_log_storage_error"]++
			receipt.ErrorClass, receipt.ErrorText = "storage_write", logErr.Error()
		} else if decisionInserted {
			receipt.Inserted++
		} else {
			receipt.Duplicates++
		}
		receipt.Exclusions[strings.ToLower(verdict.ReasonCode)]++
		if verdict.Compatible {
			pair.Orientation = verdict.Orientation
		}
		// The legacy rule-review surface still receives every structurally oriented pair, even
		// when the stricter objective matcher rejects it. This preserves exact raw-artifact
		// blockers while the new decision ledger explains the objective-predicate rejection.
		if pair.Orientation != "" {
			review, inserted, reviewErr := s.store.RecordRulePairReview(ctx, pair, started)
			if reviewErr != nil {
				receipt.Exclusions["pair_review_storage_error"]++
				receipt.ErrorClass, receipt.ErrorText = "storage_write", reviewErr.Error()
				continue
			}
			receipt.Exclusions[strings.ToLower(review.NormalizationState)]++
			if inserted {
				receipt.Inserted++
			} else {
				receipt.Duplicates++
			}
		}
	}
	receipt.Eligible += anchorResult.eligible
	receipt.Attempted += anchorResult.eligible
	receipt.Inserted += anchorResult.inserted
	receipt.Duplicates += anchorResult.duplicates
	for reason, n := range anchorResult.reasons {
		receipt.Exclusions[strings.ToLower(reason)] += n
	}
	if anchorResult.err != nil {
		receipt.Exclusions["anchor_decision_storage_error"]++
		receipt.ErrorClass, receipt.ErrorText = "storage_write", anchorResult.err.Error()
	}
	if receipt.ErrorClass != "" {
		receipt.Status = "error"
	}
	receipt.Metrics["bounded_pairs"] = len(pairs)
	pairTypes := map[string]int{}
	for _, pair := range pairs {
		pairTypes[pair.PairType]++
	}
	for pairType, n := range anchorResult.pairTypes {
		pairTypes[pairType] += n
	}
	receipt.Metrics["pair_types"] = pairTypes
	receipt.Metrics["structural_total"] = pairPage.Total
	receipt.Metrics["structural_deferred"] = pairPage.Deferred
	receipt.Metrics["structural_cursor"] = pairPage.Next
	receipt.Metrics["structural_wrapped"] = pairPage.Wrapped
	receipt.Metrics["anchor_total_keys"] = anchorResult.totalKeys
	receipt.Metrics["anchor_scanned_keys"] = anchorResult.scannedKeys
	receipt.Metrics["anchor_deferred_keys"] = anchorResult.deferredKeys
	receipt.Metrics["anchor_cursor"] = anchorResult.cursor
	receipt.Metrics["anchor_wrapped"] = anchorResult.wrapped
	receipt.Metrics["objective_taxonomy"] = objectiveidentity.TaxonomyVersion
	receipt.Metrics["accepted"] = receipt.Exclusions["accept_objective_exact"] + receipt.Exclusions["accept_objective_directional"]
	receipt.Metrics["lock_safe_accepted"] = receipt.Exclusions["accept_objective_exact"]
	rejected := 0
	for reason, n := range receipt.Exclusions {
		if strings.HasPrefix(reason, "reject_") {
			rejected += n
		}
	}
	receipt.Metrics["rejected"] = rejected
	receipt.Metrics["network_requests"] = 0
	receipt.Metrics["title_equivalence_inference"] = false
	if len(pairs) > 0 {
		if raw, marshalErr := json.Marshal(pairPage.Next); marshalErr != nil {
			receipt.ErrorClass, receipt.ErrorText = "cursor_write", marshalErr.Error()
		} else if cursorErr := s.store.KVSet(ctx, "r139:objective-rule-pair-cursor", string(raw)); cursorErr != nil {
			receipt.ErrorClass, receipt.ErrorText = "cursor_write", cursorErr.Error()
		}
	}
	if receipt.ErrorClass != "" {
		receipt.Status = "error"
	}
	receipt.Completed = time.Now()
	dctx, cancel := researchDurabilityContext(ctx)
	defer cancel()
	_, _ = s.store.InsertCollectorReceipt(dctx, receipt)
}

func (s *Server) researchRuleTick(ctx context.Context) {
	s.tryRunHeavyResearch(ctx, "rules", 0, func(ctx context.Context) {
		now := time.Now()
		s.researchRuleMu.Lock()
		due := now.Sub(s.lastResearchRuleAt) >= 5*time.Minute
		if due {
			s.lastResearchRuleAt = now
		}
		s.researchRuleMu.Unlock()
		if due {
			s.sweepResearchRuleArtifacts(ctx)
		}
	})
}

func (s *Server) handleResearchRuleArtifacts(w http.ResponseWriter, r *http.Request) {
	report, err := s.store.RuleArtifactReport(r.Context(), 200)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, report)
}

type normalizedRuleTermsRequest struct {
	SettlementSourceID  string `json:"settlement_source_id"`
	SettlementSourceURL string `json:"settlement_source_url"`
	PredicateHash       string `json:"predicate_hash"`
	RulesHash           string `json:"rules_hash"`
	VoidPolicy          string `json:"void_policy"`
	ScalarPolicy        string `json:"scalar_policy"`
	UnknownPolicy       string `json:"unknown_policy"`
	TimingPolicy        string `json:"timing_policy"`
}

type ruleCertificateRequest struct {
	PairID            string `json:"pair_id"`
	LeftVenue         string `json:"left_venue"`
	LeftInstrumentID  string `json:"left_instrument_id"`
	RightVenue        string `json:"right_venue"`
	RightInstrumentID string `json:"right_instrument_id"`
	Orientation       string `json:"orientation"`
	LeftArtifactHash  string `json:"left_artifact_hash"`
	RightArtifactHash string `json:"right_artifact_hash"`
	LeftPayoffID      string `json:"left_payoff_id"`
	RightPayoffID     string `json:"right_payoff_id"`
	// Legacy K-PUS fields remain accepted for existing operator tooling.
	KalshiTicker       string                     `json:"kalshi_ticker"`
	PolyUSSlug         string                     `json:"polyus_slug"`
	KalshiArtifactHash string                     `json:"kalshi_artifact_hash"`
	PolyUSArtifactHash string                     `json:"polyus_artifact_hash"`
	CanonicalEventID   string                     `json:"canonical_event_id"`
	CanonicalPayoffID  string                     `json:"canonical_payoff_id"`
	Left               normalizedRuleTermsRequest `json:"left"`
	Right              normalizedRuleTermsRequest `json:"right"`
	ReviewMethod       string                     `json:"review_method"`
	ReviewProvenance   string                     `json:"review_provenance"`
	ReviewEvidenceHash string                     `json:"review_evidence_hash"`
	Version            int                        `json:"version"`
	Funded             bool                       `json:"funded"`
	PaperAuthority     bool                       `json:"paper_authority"`
	LiveAuthority      bool                       `json:"live_authority"`
}

func toNormalizedRuleTerms(v normalizedRuleTermsRequest) storage.NormalizedRuleTerms {
	return storage.NormalizedRuleTerms{SettlementSourceID: v.SettlementSourceID,
		SettlementSourceURL: v.SettlementSourceURL, PredicateHash: v.PredicateHash,
		RulesHash: v.RulesHash, VoidPolicy: v.VoidPolicy, ScalarPolicy: v.ScalarPolicy,
		UnknownPolicy: v.UnknownPolicy, TimingPolicy: v.TimingPolicy}
}

func (s *Server) handleResearchRuleCertificate(w http.ResponseWriter, r *http.Request) {
	var req ruleCertificateRequest
	if err := decodeBoundedJSON(w, r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid bounded normalized rule certificate"})
		return
	}
	cert, inserted, err := s.store.RegisterRulePairCertificate(r.Context(), storage.RulePairCertificateSpec{
		PairID: req.PairID, LeftVenue: req.LeftVenue, LeftInstrumentID: req.LeftInstrumentID,
		RightVenue: req.RightVenue, RightInstrumentID: req.RightInstrumentID, Orientation: req.Orientation,
		LeftArtifactHash: req.LeftArtifactHash, RightArtifactHash: req.RightArtifactHash,
		LeftPayoffID: req.LeftPayoffID, RightPayoffID: req.RightPayoffID,
		KalshiTicker: req.KalshiTicker, PolyUSSlug: req.PolyUSSlug,
		KalshiArtifactHash: req.KalshiArtifactHash, PolyUSArtifactHash: req.PolyUSArtifactHash,
		CanonicalEventID: req.CanonicalEventID, CanonicalPayoffID: req.CanonicalPayoffID,
		Left: toNormalizedRuleTerms(req.Left), Right: toNormalizedRuleTerms(req.Right),
		ReviewMethod: req.ReviewMethod, ReviewProvenance: req.ReviewProvenance,
		ReviewEvidenceHash: req.ReviewEvidenceHash, Version: req.Version,
		Funded: req.Funded, PaperAuthority: req.PaperAuthority, LiveAuthority: req.LiveAuthority})
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"certificate": cert, "inserted": inserted,
		"funded": false, "paper_authority": false, "live_authority": false,
		"trading_authority": false,
		"note":              fmt.Sprintf("certificate %s is research identity evidence; every price/fee/freshness gate still applies", cert.SpecHash)})
}

func attachResearchRuleArtifact(base map[string]any, artifact researchVenueRuleArtifact) string {
	base["rule_artifact_source"] = artifact.Source
	base["rule_artifact_hash"] = artifact.Hash
	base["rules_primary"] = artifact.Primary
	base["rules_secondary"] = artifact.Secondary
	base["early_close_condition"] = artifact.EarlyClose
	base["description"] = artifact.Description
	base["sports_market_type"] = artifact.SportsType
	base["settlement_sources"] = artifact.SettlementSources
	base["void_policy"] = artifact.VoidPolicy
	base["scalar_policy"] = artifact.ScalarPolicy
	base["unknown_policy"] = artifact.UnknownPolicy
	base["semantic_normalization_status"] = "raw_authoritative_terms_captured; cross-venue equivalence not inferred"
	b, _ := json.Marshal(base)
	return string(b)
}
