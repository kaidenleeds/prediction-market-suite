package server

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

// r168ForwardGenerationID freezes the first catalog-complete forward-evidence generation.
// It is evidence metadata only: it grants no LIVE or cash authority.
const r168ForwardGenerationID = "r168-forward-all-contracts-v3"

const (
	r168ForwardDetectorModelVersion = "r168-forward-detector-receipt-v3"
	r168ForwardEventIdentityVersion = "resident-structural-event-identity-v3"
)

func r168ForwardResearchGeneration() storage.ForwardResearchGeneration {
	declared := r147SystemVariantSignalContracts()
	contracts := make([]storage.ForwardResearchContract, 0, len(declared))
	for _, row := range declared {
		executionClass, realistic := r147ContractPaperExecution(row)
		contracts = append(contracts, storage.ForwardResearchContract{
			ContractID:     r147SignalContractIdentity(row),
			SystemID:       row.SystemID,
			Venue:          row.ExecutionVenue,
			Side:           row.Side,
			Action:         row.Action,
			Route:          row.Route,
			InputTopology:  row.InputTopology,
			ExecutionClass: executionClass,
			Realistic:      realistic,
			CashAuthority:  false,
		})
	}
	return storage.ForwardResearchGeneration{
		GenerationID:           r168ForwardGenerationID,
		DetectorReceiptVersion: r168ForwardDetectorModelVersion,
		EventIdentityVersion:   r168ForwardEventIdentityVersion,
		OpenedAt:               time.Now().UTC(),
		Contracts:              contracts,
		CashAuthority:          false,
	}
}

// PrepareForwardResearchGeneration freezes the exact current r147 catalog before any feed can
// emit a detector event. A changed catalog must use a new generation id; it cannot rewrite the
// evidence cohort in place.
func (s *Server) PrepareForwardResearchGeneration(ctx context.Context) error {
	if s == nil || s.store == nil {
		return errors.New("forward-research storage unavailable")
	}
	_, err := s.store.PrepareForwardResearchGeneration(ctx, r168ForwardResearchGeneration())
	return err
}

func (s *Server) handleForwardResearch(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	report, err := s.store.ForwardResearchReport(ctx, r168ForwardGenerationID)
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, report)
}

func r168ForwardModelVersion(sig storage.Signal) string {
	if version := strings.TrimSpace(sig.PricingVersion); version != "" {
		return version
	}
	return r168ForwardDetectorModelVersion
}

// r168ForwardVerifiedEventCluster uses only the resident structural game graph that was already
// built from venue metadata. It never parses a title, reads storage, or calls a venue. A missing
// structural join stays unknown: the looser display/proxy key is useful for diagnostics but cannot
// become independence proof.
func (s *Server) r168ForwardVerifiedEventCluster(attempt storage.ExecutionShadowAttempt) (string, bool) {
	if s == nil {
		return "", false
	}
	refs := []string{attempt.Ticker}
	if strings.EqualFold(strings.TrimSpace(attempt.Venue), "kalshi") {
		// EventTicker is a resident venue-native parent reference. giGameOfRef still requires that
		// the structural game registry independently anchored it; the parent alone is not proof.
		if market, ok := s.kmkt(attempt.Ticker); ok {
			refs = append(refs, market.EventTicker)
		}
	}
	gameID, ok := s.giGameOfRef(refs...)
	gameID = strings.TrimSpace(gameID)
	if !ok || gameID == "" {
		return "", false
	}
	if strings.HasPrefix(strings.ToLower(gameID), "sports:") {
		return gameID, true
	}
	return "sports:" + gameID, true
}

// r168StampForwardDetectorEvent adds only already-computed identity and cached-book facts. It
// performs no database, network, account, or venue work and therefore cannot delay cash routing.
func (s *Server) r168StampForwardDetectorEvent(event *storage.ExecutionShadowEvent,
	attempt storage.ExecutionShadowAttempt, executionModelVersion string) {
	if event == nil {
		return
	}
	if event.Evidence == nil {
		event.Evidence = map[string]any{}
	}
	executionModelVersion = strings.TrimSpace(executionModelVersion)
	if executionModelVersion == "" {
		executionModelVersion = r168ForwardDetectorModelVersion
	}
	if event.BookGeneration == nil && event.BookSubscriptionID == nil && event.BookSequence == nil {
		event.BookGeneration, event.BookSubscriptionID, event.BookSequence =
			executionShadowQuoteReceipt(liveMirrorQuote{BookSource: event.BookSource})
	}
	event.Evidence["forward_generation_id"] = r168ForwardGenerationID
	event.Evidence["signal_contract_id"] = strings.TrimSpace(attempt.SignalContract)
	event.Evidence["execution_model_version"] = executionModelVersion
	event.Evidence["forward_detector_receipt_version"] = r168ForwardDetectorModelVersion
	event.Evidence["event_cluster_identity_version"] = r168ForwardEventIdentityVersion
	event.Evidence["trigger_unix_ms"] = attempt.TriggerUnixMS
	clusterID, clusterVerified := s.r168ForwardVerifiedEventCluster(attempt)
	event.Evidence["event_cluster_id"] = clusterID
	event.Evidence["event_cluster_proxy"] = s.liveMirrorClusterKey(attempt.Venue,
		attempt.Ticker, attempt.Title)
	event.Evidence["event_cluster_verified"] = clusterVerified
	if clusterVerified {
		event.Evidence["event_cluster_source"] = "resident-structural-game-identity"
	} else {
		event.Evidence["event_cluster_source"] = ""
	}
	event.Evidence["book_transport"] = strings.TrimSpace(event.BookSource)
	event.Evidence["book_generation_proved"] = event.BookGeneration != nil
	event.Evidence["book_subscription_proved"] = event.BookSubscriptionID != nil
	event.Evidence["book_sequence_proved"] = event.BookSequence != nil
	event.Evidence["cash_authority"] = false
	event.Evidence["real_money_authority"] = false
}
