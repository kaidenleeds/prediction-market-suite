package server

import (
	"context"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

const researchPaperCandidateMaxAge = 45 * time.Second

// currentResearchCandidateIdentityReason binds a stored action to the current immutable event and
// payoff versions. Paper and LIVE both call this same fail-closed contract; matching a ticker is
// never enough after venue rules or the canonical payoff mapping change.
func (s *Server) currentResearchCandidateIdentityReason(ctx context.Context,
	c storage.ResearchPromotionCandidate) string {
	if s.store == nil {
		return "canonical-identity-store-unavailable"
	}
	current, found, err := s.store.CurrentCanonicalInstrument(ctx, c.Venue, c.Ticker)
	if err != nil {
		return "canonical-identity-read-error"
	}
	if !found {
		return "canonical-identity-missing"
	}
	if !strings.EqualFold(strings.TrimSpace(current.IdentityStatus), "verified") {
		return "canonical-identity-not-verified"
	}
	if current.EventID != c.CanonicalEventID || current.EventVersion != c.EventVersion {
		return "canonical-event-version-changed"
	}
	if current.PayoffID != c.CanonicalPayoffID || current.PayoffVersion != c.PayoffVersion {
		return "canonical-payoff-version-changed"
	}
	return ""
}

func researchPaperCandidateFreshReason(c storage.ResearchPromotionCandidate, now time.Time) string {
	if c.Observed.IsZero() {
		return "candidate-observed-time-missing"
	}
	age := now.Sub(c.Observed)
	if age < -5*time.Second {
		return "candidate-observed-time-in-future"
	}
	if age > researchPaperCandidateMaxAge {
		return "candidate-stale"
	}
	return ""
}
