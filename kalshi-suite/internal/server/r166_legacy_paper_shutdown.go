package server

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

const r166LegacyPaperNewEntriesRetired = true

func r166SpecialistInputTopology(sig storage.Signal, systemID, supplied string) (string, string) {
	observed := r147SignalInputTopology(sig)
	if observed == "" {
		observed = strings.ToUpper(strings.TrimSpace(supplied))
	}
	matches := r147ExactSignalContracts(strings.ToLower(strings.TrimSpace(systemID)),
		strings.ToLower(strings.TrimSpace(sig.Platform)), strings.ToUpper(strings.TrimSpace(sig.Side)), "taker")
	if observed != "" {
		for _, contract := range matches {
			if strings.EqualFold(contract.InputTopology, observed) {
				return strings.ToUpper(contract.InputTopology), ""
			}
		}
		return "", "specialist-input-topology-not-declared"
	}
	unique := ""
	for _, contract := range matches {
		candidate := strings.ToUpper(strings.TrimSpace(contract.InputTopology))
		if unique == "" {
			unique = candidate
		} else if unique != candidate {
			return "", "specialist-input-topology-ambiguous"
		}
	}
	if unique == "" {
		return "", "specialist-exact-taker-contract-absent"
	}
	return unique, ""
}

func (s *Server) auditR166SpecialistTopologyExclusion(ctx context.Context, sig storage.Signal,
	systemID, source, reason string, at time.Time) {
	if s == nil || s.store == nil {
		return
	}
	detail, _ := json.Marshal(map[string]any{
		"system": strings.ToLower(strings.TrimSpace(systemID)), "venue": strings.ToLower(strings.TrimSpace(sig.Platform)),
		"ticker": sig.Ticker, "side": strings.ToUpper(strings.TrimSpace(sig.Side)), "route": "taker",
		"source": source, "reason": reason, "input_topology": r147SignalInputTopology(sig),
		"observed_at": at.UTC().Format(time.RFC3339Nano), "order_result": "PAPER-NOT-OBSERVED",
	})
	auditCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 500*time.Millisecond)
	defer cancel()
	_ = s.store.Audit(auditCtx, "warn", "system-route",
		"PAPER-NOT-OBSERVED "+strings.ToLower(strings.TrimSpace(systemID))+" "+
			strings.ToLower(strings.TrimSpace(sig.Platform))+" "+sig.Ticker+": "+reason, string(detail))
}

// enqueueR166SpecialistTaker preserves a specialist's actual signal definition while moving its
// execution observation to the one delayed two-complete-book Paper IOC boundary. The old JSON
// book remains settlement/history-only and never receives this new lot.
func (s *Server) enqueueR166SpecialistTaker(ctx context.Context, sig storage.Signal,
	systemID, inputTopology, source string) {
	if s == nil || !r166LegacyPaperNewEntriesRetired {
		return
	}
	sig.SignalType = strings.ToLower(strings.TrimSpace(systemID))
	at := time.Now().UTC()
	topology, why := r166SpecialistInputTopology(sig, sig.SignalType, inputTopology)
	if why != "" {
		s.auditR166SpecialistTopologyExclusion(ctx, sig, sig.SignalType, source, why, at)
		return
	}
	if r147SignalInputTopology(sig) == "" {
		sig.ExecExpr = r147InputReceiptExpr(topology, at)
	}
	s.noteR147SignalRuntimeForSignal(sig.SignalType, sig, "taker", source,
		at, true, "")
	s.genfollowTriggeredSystems(ctx, sig)
}

func r166LegacySyntheticPaperRoute(platform string, liveDemo, liveProd bool) bool {
	return r166LegacyPaperNewEntriesRetired && !liveProd &&
		!(strings.EqualFold(platform, "kalshi") && liveDemo)
}
