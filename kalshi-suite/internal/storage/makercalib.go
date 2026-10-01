package storage

import (
	"context"
	"math"
	"time"
)

type MakerCalibBin struct {
	Lo       float64 `json:"lo"`
	Hi       float64 `json:"hi"`
	N        int     `json:"n"`
	MeanP    float64 `json:"mean_p"`
	Realized float64 `json:"realized"`
}

// MakerDeployedCalib measures calibration on fills the maker router actually deployed, not on a
// small model-selected proxy cell. Side-adjusted settlements and fill-time probabilities share
// the same outcome space. The 14-day window lets the gate recover from old model regimes.
func (s *Store) MakerDeployedCalib(ctx context.Context, since time.Time) (ece float64, n int, bins []MakerCalibBin, err error) {
	rows, err := s.db.QueryContext(ctx, `SELECT fill_pwin,
CASE WHEN LOWER(TRIM(side)) IN ('no','down') THEN 1.0-settle_val ELSE settle_val END
FROM maker_fill_stats
WHERE filled=1 AND settle_val IS NOT NULL AND fill_pwin IS NOT NULL
  AND fill_pwin>=0 AND fill_pwin<=1 AND ts>=?`, since.UTC().Format(time.RFC3339))
	if err != nil {
		return 0, 0, nil, err
	}
	defer rows.Close()
	type acc struct {
		n      int
		sp, sy float64
	}
	var a [10]acc
	for rows.Next() {
		var p, y float64
		if err := rows.Scan(&p, &y); err != nil {
			return 0, 0, nil, err
		}
		if math.IsNaN(p) || math.IsNaN(y) || y < 0 || y > 1 {
			continue
		}
		bi := int(p * 10)
		if bi > 9 {
			bi = 9
		}
		a[bi].n++
		a[bi].sp += p
		a[bi].sy += y
		n++
	}
	if err := rows.Err(); err != nil {
		return 0, 0, nil, err
	}
	for i, b := range a {
		if b.n == 0 {
			continue
		}
		mp, ry := b.sp/float64(b.n), b.sy/float64(b.n)
		ece += float64(b.n) * math.Abs(mp-ry)
		bins = append(bins, MakerCalibBin{Lo: float64(i) / 10, Hi: float64(i+1) / 10,
			N: b.n, MeanP: math.Round(mp*1e4) / 1e4, Realized: math.Round(ry*1e4) / 1e4})
	}
	if n > 0 {
		ece /= float64(n)
	}
	return ece, n, bins, nil
}
