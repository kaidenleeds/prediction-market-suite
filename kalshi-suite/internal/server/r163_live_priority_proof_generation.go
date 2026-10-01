package server

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

// livePriorityProofGeneration names the execution architecture whose settled strategy receipts
// may authorize the current LIVE-priority route. Bump this semantic generation when detector,
// admission, or execution behavior changes enough that old results are no longer exchangeable.
//
// The reserved Episode also prevents a valid older strategy row on the same ticker from winning
// unit_trials' existing no-migration uniqueness key before this generation can persist its row.
const (
	livePriorityProofGeneration        = "cash-live-priority-v1"
	livePriorityProofGenerationEpisode = 1_630_001
	// Pre-IOC UnitTrial rows describe opportunities, not executable fills. Until a separate
	// current-generation cohort is built from authoritative LIVE fills, their actual fill
	// prices/fees, later settlements, zero-fill rate, and independent time/event blocks, they
	// cannot authorize normal cash sizing. The explicit q=1 UNPROVEN canary contract is separate.
	liveFillConditionedProofUnavailableReason = "prospective-allocation-authoritative-live-fill-cohort-unavailable"
)

func livePriorityProofQuoteSourcePrefix() string {
	return "strategy-decision/live-priority/" + livePriorityProofGeneration + "/"
}

func livePriorityProofQuoteSource(bookSource, signalContract string) string {
	return livePriorityProofQuoteSourcePrefix() +
		strings.Trim(strings.TrimSpace(bookSource), "/") + "/" +
		strings.Trim(strings.TrimSpace(signalContract), "/")
}

// livePriorityUnitTrialRouteLeaderboard keeps the cache and storage query scoped to the same
// immutable source generation. Historical rows remain visible through the general leaderboard;
// this opportunity-only statistic is diagnostic and still cannot authorize normal cash.
func (s *Server) livePriorityUnitTrialRouteLeaderboard(ctx context.Context, now time.Time,
	family, platform, origin, side string) (storage.UnitTrialLeaderboardStat, bool, error) {
	if s == nil || s.store == nil {
		return storage.UnitTrialLeaderboardStat{}, false,
			errors.New("route proof store unavailable")
	}
	prefix := livePriorityProofQuoteSourcePrefix()
	key := r154LiveRouteProofKey(family, platform, origin, side) + "\x1f" + prefix
	return s.liveRouteProofCache.get(ctx, now, key,
		func() (storage.UnitTrialLeaderboardStat, bool, error) {
			return s.store.UnitTrialRouteLeaderboardSourcePrefix(
				ctx, now, family, platform, origin, side, prefix)
		})
}
