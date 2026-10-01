// catalogread.go — R107: read-only market_catalog access (the table had writers + point probes
// but NO breadth reader; the market tree could only show the in-memory live caches, so the
// operator's "~72k tracked markets" were invisible everywhere except a COUNT(*)).
//
// close_ts semantics differ PER VENUE (DB-copy probe 2026-07-07, 88,901 rows):
//
//	kalshi      = expected_expiration/close_time (RFC3339 Z, never empty; 12,470 future / 51,085 past)
//	polymarket  = gamma endDate (RFC3339 Z; 333 rows empty — unknown horizon, NOT provably closed)
//	polyus      = game START time (the venue feed has no close; a live game runs hours past it —
//	              callers must pass a venue-appropriate cutoff, e.g. now-12h, not now)
//
// All non-empty values observed are RFC3339 UTC ("2026-07-07T17:05:00Z"), so a lexicographic
// string compare against an RFC3339-UTC cutoff is a correct time compare (same shape, same zone).
package storage

import (
	"context"
	"time"
)

// CatalogUniverse returns one venue's not-clearly-closed market_catalog rows: close_ts unknown
// (empty — never provably closed, kept) or after closedAfter. One PK-prefix index walk
// (PRIMARY KEY(venue,ticker)); ~63k kalshi rows scan in milliseconds. Callers pick closedAfter
// per the venue's close_ts semantics above (for polyus pass a lagged cutoff, e.g. now-12h).
func (s *Store) CatalogUniverse(ctx context.Context, venue string, closedAfter time.Time) ([]CatalogRow, error) {
	cut := closedAfter.UTC().Format(time.RFC3339)
	rows, err := s.db.QueryContext(ctx, `
SELECT ticker, event_key, kind, title, close_ts FROM market_catalog
WHERE venue = ? AND (close_ts = '' OR close_ts > ?)`, venue, cut)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []CatalogRow
	for rows.Next() {
		r := CatalogRow{Venue: venue}
		if err := rows.Scan(&r.Ticker, &r.EventKey, &r.Kind, &r.Title, &r.CloseTS); err != nil {
			return out, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// CatalogVenueCount is one venue's tracked-universe footprint: Total = every row inside the 90d
// retention window; Open = rows not clearly closed as of the call (close_ts empty or future —
// for polyus "open" reads as "game not yet started" per the close_ts semantics above).
type CatalogVenueCount struct {
	Total int64 `json:"total"`
	Open  int64 `json:"open"`
}

// CatalogCounts returns the per-venue universe totals in one indexed GROUP BY pass — the market
// tree's top-level universe_totals payload (R107). Never called on a hot path more than once per
// tree rebuild (60s cache above it).
func (s *Store) CatalogCounts(ctx context.Context) (map[string]CatalogVenueCount, error) {
	now := time.Now().UTC().Format(time.RFC3339)
	rows, err := s.db.QueryContext(ctx, `
SELECT venue,
       COUNT(*),
       SUM(CASE WHEN close_ts = '' OR close_ts > ? THEN 1 ELSE 0 END)
FROM market_catalog GROUP BY venue`, now)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]CatalogVenueCount{}
	for rows.Next() {
		var v string
		var c CatalogVenueCount
		if err := rows.Scan(&v, &c.Total, &c.Open); err != nil {
			return out, err
		}
		out[v] = c
	}
	return out, rows.Err()
}
