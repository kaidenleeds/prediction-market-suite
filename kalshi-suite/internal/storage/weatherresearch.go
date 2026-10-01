package storage

import (
	"context"
	"database/sql"
)

// WeatherResearchSignalStat is the settlement receipt for one unfunded weather research model.
// These rows live in signal_log so the suite's ordinary venue settlement worker resolves them;
// no paper_fill, unit_trial, maker attempt, or live-order row is created by the producer.
type WeatherResearchSignalStat struct {
	SignalType string  `json:"signal_type"`
	Logged     int     `json:"logged"`
	Open       int     `json:"open"`
	Resolved   int     `json:"resolved"`
	Won        int     `json:"won"`
	NetPC      float64 `json:"net_pc"`
}

// WeatherResearchAudit returns only the structured prospective cohort records.  Keeping this
// read path beside Store avoids loading/filtering the whole audit trail for the read-only report.
func (s *Store) WeatherResearchAudit(ctx context.Context, limit int) ([]AuditRow, error) {
	if limit <= 0 || limit > 5000 {
		limit = 500

	}
	rows, err := s.db.QueryContext(ctx, `
SELECT ts,level,category,message,COALESCE(detail,'')
FROM audit_log
WHERE category IN ('weather-curve-residual','weather-curve-revision','weather-nbm-independent')
ORDER BY id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]AuditRow, 0, limit)
	for rows.Next() {
		var row AuditRow
		if err := rows.Scan(&row.TS, &row.Level, &row.Category, &row.Message, &row.Detail); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// WeatherResearchSignalStats reports the fee-net one-share settlement outcome.  fee_pc is exact
// on every admitted research signal; a NULL fee therefore stays outside NetPC rather than being
// replaced with a guessed schedule.
func (s *Store) WeatherResearchSignalStats(ctx context.Context) ([]WeatherResearchSignalStat, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT signal_type,
       COUNT(*) AS logged,
       SUM(CASE WHEN resolved=0 THEN 1 ELSE 0 END) AS open_n,
       SUM(CASE WHEN resolved=1 THEN 1 ELSE 0 END) AS resolved_n,
       SUM(CASE WHEN resolved=1 AND won=1 THEN 1 ELSE 0 END) AS won_n,
       COALESCE(SUM(CASE WHEN resolved=1 AND won IN (0,1) AND fee_pc IS NOT NULL
                    THEN CASE WHEN won=1 THEN 1.0-entry_price-fee_pc
                              ELSE -entry_price-fee_pc END
                    ELSE 0 END),0) AS net_pc
FROM signal_log
WHERE signal_type IN ('weather-curve-residual','weather-curve-revision','independent-probabilistic-weather')
GROUP BY signal_type ORDER BY signal_type`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []WeatherResearchSignalStat
	for rows.Next() {
		var row WeatherResearchSignalStat
		var logged, openN, resolvedN, wonN sql.NullInt64
		if err := rows.Scan(&row.SignalType, &logged, &openN, &resolvedN, &wonN, &row.NetPC); err != nil {
			return nil, err
		}
		row.Logged, row.Open, row.Resolved, row.Won = int(logged.Int64), int(openN.Int64), int(resolvedN.Int64), int(wonN.Int64)
		out = append(out, row)
	}
	return out, rows.Err()
}
