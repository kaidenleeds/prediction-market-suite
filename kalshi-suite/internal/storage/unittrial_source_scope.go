package storage

import (
	"context"
	"errors"
	"strings"
	"time"
)

// UnitTrialRouteLeaderboardSourcePrefix computes one exact route from only the immutable
// quote-source namespace supplied by the caller. General leaderboards intentionally keep reading
// every historical generation. This narrower opportunity reader keeps generations honest, but it
// is not by itself a money-authorizing fill proof.
func (s *Store) UnitTrialRouteLeaderboardSourcePrefix(ctx context.Context, now time.Time,
	family, platform, originLayer, side, quoteSourcePrefix string) (
	UnitTrialLeaderboardStat, bool, error) {
	prefix := strings.TrimSpace(quoteSourcePrefix)
	if prefix == "" {
		return UnitTrialLeaderboardStat{}, false,
			errors.New("unit-trial quote-source prefix unavailable")
	}
	rows, err := s.unitTrialLeaderboard(ctx, now, `
 AND family=?
 AND LOWER(TRIM(platform))=LOWER(TRIM(?))
 AND origin_layer=?
 AND UPPER(TRIM(side))=UPPER(TRIM(?))
 AND LOWER(SUBSTR(TRIM(quote_source),1,LENGTH(?)))=LOWER(?)`,
		family, platform, originLayer, side, prefix, prefix)
	if err != nil || len(rows) == 0 {
		return UnitTrialLeaderboardStat{}, false, err
	}
	return rows[0], true, nil
}
