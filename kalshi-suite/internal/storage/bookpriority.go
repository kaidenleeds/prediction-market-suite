package storage

import (
	"context"
	"strings"
	"time"
)

// ListRecentOpenSignalTickersByPlatform returns the newest distinct unresolved markets for one
// venue inside a bounded activity window.  It is the book-seat planner's cache-friendly view:
// unlike ListOpenSignalTickersByPlatform it cannot walk years of unresolved research history or
// let old rows crowd current strategy inputs out of a bounded CLOB working set.
func (s *Store) ListRecentOpenSignalTickersByPlatform(ctx context.Context, platform string, since time.Time, limit int) ([]string, error) {
	platform = strings.ToLower(strings.TrimSpace(platform))
	if platform == "" || limit <= 0 {
		return nil, nil
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT ticker
FROM signal_log
WHERE resolved=0 AND platform=? AND ts>=? AND TRIM(ticker)<>''
GROUP BY ticker
ORDER BY MAX(ts) DESC
LIMIT ?`, platform, since.UTC().Format(time.RFC3339), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]string, 0, limit)
	for rows.Next() {
		var ticker string
		if err := rows.Scan(&ticker); err != nil {
			return nil, err
		}
		if ticker = strings.TrimSpace(ticker); ticker != "" {
			out = append(out, ticker)
		}
	}
	return out, rows.Err()
}
