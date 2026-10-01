package storage

import (
	"context"
	"database/sql"
	"strings"
	"time"
)

// MakerRouteAttempt is one terminal, one-share maker-routing observation. A canceled/unfilled
// attempt is already a complete economic outcome (zero PnL); a filled attempt is returned only
// after settlement is known. Filled rows carry the exact fill-time fee/rebate receipt; today's
// mutable venue schedule is never used to rewrite historical economics.
type MakerRouteAttempt struct {
	ID        int64
	OpenedTS  time.Time
	Platform  string
	Ticker    string
	Side      string
	Source    string
	Family    string
	Inverted  bool
	PostPx    float64
	Filled    bool
	Open      bool
	Terminal  bool
	Settle    float64
	FeePC     float64
	FeeKnown  bool
	FeeSource string
}

// SetMakerStrategy stamps the actual routed family on a newly-created maker attempt. This is a
// separate update so old InsertMakerAttempt call sites and stored databases remain compatible.
// A blank family is intentionally ignored: blank means legacy/unknown and is barred from LIVE
// proof rather than guessed from a source whose inversion policy may have changed over time.
func (s *Store) SetMakerStrategy(ctx context.Context, id int64, family string, inverted bool) error {
	family = strings.TrimSpace(family)
	if id <= 0 || family == "" {
		return nil
	}
	inv := 0
	if inverted {
		inv = 1
	}
	_, err := s.db.ExecContext(ctx, `UPDATE maker_fill_stats
SET strategy_family=?, strategy_inverted=?
WHERE id=? AND strategy_family=''`, family, inv, id)
	return err
}

// MakerRouteAttempts returns the full tagged maker cohort for one exact route:
// venue + raw executor source + canonical family + inversion bit + outcome side. The optional
// side argument exists only for old read-only callers; every money-proof caller supplies it. It deliberately excludes:
//   - guard-refused counterfactuals (filled=-2),
//   - all untagged legacy rows (their direct/inverted route cannot be reconstructed safely).
//
// Pending and filled-but-unsettled attempts are retained as Open so quick terminal winners cannot
// prove while slow losses remain censored. Canceled attempts are terminal exact-zero outcomes.
func (s *Store) MakerRouteAttempts(ctx context.Context, platform, source, family string, inverted bool, side ...string) ([]MakerRouteAttempt, error) {
	platform, source, family = strings.TrimSpace(platform), strings.TrimSpace(source), strings.TrimSpace(family)
	wantedSide := ""
	if len(side) > 0 {
		wantedSide = strings.ToUpper(strings.TrimSpace(side[0]))
		if wantedSide != "YES" && wantedSide != "NO" {
			return nil, nil
		}
	}
	inv := 0
	if inverted {
		inv = 1
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT id,ts,platform,ticker,side,source,strategy_family,strategy_inverted,post_px,filled,settle_val,
       maker_fee_pc,maker_rebate_pc,maker_fee_source
FROM maker_fill_stats
WHERE LOWER(TRIM(platform))=LOWER(TRIM(?))
  AND LOWER(TRIM(source))=LOWER(TRIM(?))
  AND strategy_family=? AND strategy_inverted=?
  AND (?='' OR UPPER(TRIM(side))=?)
  AND filled IN (-1,0,1)
ORDER BY id`, platform, source, family, inv, wantedSide, wantedSide)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []MakerRouteAttempt
	for rows.Next() {
		var (
			a                    MakerRouteAttempt
			invInt               int
			filled               int
			opened               string
			settled, fee, rebate sql.NullFloat64
		)
		if err := rows.Scan(&a.ID, &opened, &a.Platform, &a.Ticker, &a.Side, &a.Source, &a.Family,
			&invInt, &a.PostPx, &filled, &settled, &fee, &rebate, &a.FeeSource); err != nil {
			return nil, err
		}
		a.Inverted = invInt != 0
		a.Filled = filled == 1
		a.Open = filled == -1 || (filled == 1 && !settled.Valid)
		a.Terminal = !a.Open
		for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
			if ts, err := time.Parse(layout, opened); err == nil {
				a.OpenedTS = ts.UTC()
				break
			}
		}
		if settled.Valid {
			a.Settle = settled.Float64
		}
		a.FeeKnown = fee.Valid && rebate.Valid && strings.TrimSpace(a.FeeSource) != ""
		if a.FeeKnown {
			a.FeePC = fee.Float64 - rebate.Float64
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// ListMakerRouteKeys enumerates exact tagged routes for scoreboard/API aggregation. It returns
// keys only; callers then use MakerRouteAttempts so the same terminal-row doctrine powers both
// display and LIVE authorization.
type MakerRouteKey struct {
	Platform string
	Source   string
	Family   string
	Inverted bool
	Side     string
}

func (s *Store) ListMakerRouteKeys(ctx context.Context) ([]MakerRouteKey, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT platform,source,strategy_family,strategy_inverted,UPPER(TRIM(side))
FROM maker_fill_stats
WHERE strategy_family!='' AND filled IN (-1,0,1)
  AND UPPER(TRIM(side)) IN ('YES','NO')
GROUP BY platform,source,strategy_family,strategy_inverted,UPPER(TRIM(side))
ORDER BY platform,strategy_family,source,strategy_inverted,UPPER(TRIM(side))`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []MakerRouteKey
	for rows.Next() {
		var k MakerRouteKey
		var inv int
		if err := rows.Scan(&k.Platform, &k.Source, &k.Family, &inv, &k.Side); err != nil {
			return nil, err
		}
		k.Inverted = inv != 0
		out = append(out, k)
	}
	return out, rows.Err()
}
