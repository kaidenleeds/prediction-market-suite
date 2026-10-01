package storage

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

// NativeSystemFunnelReceipt is a minute-batched operational receipt for the shared
// signal -> executable quote -> one-share trial path. Counts are collection telemetry, never n,
// P&L evidence, or trading authority.
type NativeSystemFunnelReceipt struct {
	Observed                                     time.Time
	CycleID, Family, Platform, OriginLayer       string
	ProducerState, ProducerReason                string
	Eligible, InputRows, QuoteAttempts, UnitRows int
	MoneyTruthAttempts                           int
	Exclusions                                   map[string]int
	FirstInput, LastInput, LastEconomic          time.Time
}

type NativeSystemFunnelView struct {
	Family                     string         `json:"family"`
	Platform                   string         `json:"platform"`
	OriginLayer                string         `json:"origin_layer"`
	ProducerState              string         `json:"producer_state"`
	ProducerReason             string         `json:"producer_reason"`
	ObservedTS                 string         `json:"observed_ts"`
	FirstInputTS               string         `json:"first_input_ts"`
	LastInputTS                string         `json:"last_input_ts"`
	LastEconomicTS             string         `json:"last_economic_ts"`
	CycleEligible              int            `json:"cycle_eligible"`
	CycleInputRows             int            `json:"cycle_input_rows"`
	CycleQuoteAttempts         int            `json:"cycle_quote_attempts"`
	CycleUnitRows              int            `json:"cycle_unit_rows"`
	CycleMoneyTruthAttempts    int            `json:"cycle_money_truth_attempts"`
	LifetimeEligible           int64          `json:"lifetime_eligible"`
	LifetimeInputRows          int64          `json:"lifetime_input_rows"`
	LifetimeQuoteAttempts      int64          `json:"lifetime_quote_attempts"`
	LifetimeUnitRows           int64          `json:"lifetime_unit_rows"`
	LifetimeMoneyTruthAttempts int64          `json:"lifetime_money_truth_attempts"`
	EconomicRows24H            int            `json:"economic_rows_24h"`
	Exclusions                 map[string]int `json:"exclusions"`
}

func nativeFunnelTime(v time.Time) string {
	if v.IsZero() {
		return ""
	}
	return v.UTC().Format(time.RFC3339Nano)
}

func validateNativeFunnelReceipt(v *NativeSystemFunnelReceipt) error {
	if v.Observed.IsZero() {
		v.Observed = time.Now().UTC()
	}
	v.CycleID, v.Family = strings.TrimSpace(v.CycleID), strings.TrimSpace(v.Family)
	v.Platform = strings.ToLower(strings.TrimSpace(v.Platform))
	v.OriginLayer = strings.ToLower(strings.TrimSpace(v.OriginLayer))
	v.ProducerState = strings.ToUpper(strings.TrimSpace(v.ProducerState))
	v.ProducerReason = strings.TrimSpace(v.ProducerReason)
	if v.CycleID == "" || v.Family == "" || v.Platform == "" || v.ProducerState == "" || v.ProducerReason == "" {
		return errors.New("incomplete native system funnel receipt")
	}
	if v.OriginLayer == "" {
		v.OriginLayer = "model"
	}
	if v.OriginLayer != "model" && v.OriginLayer != "strategy" && v.OriginLayer != "control" {
		return errors.New("unsupported native system funnel origin")
	}
	if v.Eligible < 0 || v.InputRows < 0 || v.QuoteAttempts < 0 || v.UnitRows < 0 || v.MoneyTruthAttempts < 0 ||
		v.InputRows > v.Eligible || v.QuoteAttempts > v.InputRows || v.UnitRows > v.QuoteAttempts ||
		v.MoneyTruthAttempts > v.QuoteAttempts {
		return errors.New("invalid native system funnel counts")
	}
	if v.Exclusions == nil {
		v.Exclusions = map[string]int{}
	}
	for reason, n := range v.Exclusions {
		if strings.TrimSpace(reason) == "" || n < 0 {
			return errors.New("invalid native system funnel exclusion")
		}
	}
	return nil
}

// InsertNativeSystemFunnelReceipts writes one bounded batch transaction. The server aggregates
// hot-path signals in memory first, so this never turns the signal rate into a second write rate.
func (s *Store) InsertNativeSystemFunnelReceipts(ctx context.Context, rows []NativeSystemFunnelReceipt) error {
	if len(rows) == 0 {
		return nil
	}
	if len(rows) > 500 {
		return errors.New("native system funnel batch exceeds bound")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for i := range rows {
		v := &rows[i]
		if err := validateNativeFunnelReceipt(v); err != nil {
			return err
		}
		exclusions, err := json.Marshal(v.Exclusions)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO native_system_funnel_receipts(
observed_ts,cycle_id,family,platform,origin_layer,producer_state,producer_reason,
eligible_count,input_rows,quote_attempts,unit_rows,money_truth_attempts,exclusions_json,
first_input_ts,last_input_ts,last_economic_ts,funded,paper_authority,live_authority)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0,0,0)`, nativeFunnelTime(v.Observed), v.CycleID,
			v.Family, v.Platform, v.OriginLayer, v.ProducerState, v.ProducerReason,
			v.Eligible, v.InputRows, v.QuoteAttempts, v.UnitRows, v.MoneyTruthAttempts,
			string(exclusions), nativeFunnelTime(v.FirstInput), nativeFunnelTime(v.LastInput),
			nativeFunnelTime(v.LastEconomic)); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// NativeSystemFunnelViews combines the latest operational receipt with all-time funnel totals and
// the actual recent one-share ledger. A missing view is deliberately not interpreted as active.
func (s *Store) NativeSystemFunnelViews(ctx context.Context, now time.Time) ([]NativeSystemFunnelView, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	rows, err := s.db.QueryContext(ctx, `WITH latest AS (
 SELECT family,platform,origin_layer,MAX(id) AS id
 FROM native_system_funnel_receipts GROUP BY family,platform,origin_layer
), totals AS (
 SELECT family,platform,origin_layer,SUM(eligible_count) AS eligible_total,
        SUM(input_rows) AS input_total,SUM(quote_attempts) AS quote_total,
        SUM(unit_rows) AS unit_total,SUM(money_truth_attempts) AS money_truth_total
 FROM native_system_funnel_receipts GROUP BY family,platform,origin_layer
)
SELECT r.family,r.platform,r.origin_layer,r.producer_state,r.producer_reason,r.observed_ts,
 r.first_input_ts,r.last_input_ts,r.last_economic_ts,r.eligible_count,r.input_rows,
 r.quote_attempts,r.unit_rows,r.money_truth_attempts,r.exclusions_json,
 t.eligible_total,t.input_total,t.quote_total,t.unit_total,t.money_truth_total
FROM latest l JOIN native_system_funnel_receipts r ON r.id=l.id
JOIN totals t ON t.family=l.family AND t.platform=l.platform AND t.origin_layer=l.origin_layer
ORDER BY r.family,r.platform,r.origin_layer`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []NativeSystemFunnelView
	for rows.Next() {
		var v NativeSystemFunnelView
		var exclusions string
		if err := rows.Scan(&v.Family, &v.Platform, &v.OriginLayer, &v.ProducerState,
			&v.ProducerReason, &v.ObservedTS, &v.FirstInputTS, &v.LastInputTS, &v.LastEconomicTS,
			&v.CycleEligible, &v.CycleInputRows, &v.CycleQuoteAttempts, &v.CycleUnitRows,
			&v.CycleMoneyTruthAttempts, &exclusions, &v.LifetimeEligible, &v.LifetimeInputRows,
			&v.LifetimeQuoteAttempts, &v.LifetimeUnitRows, &v.LifetimeMoneyTruthAttempts); err != nil {
			return nil, err
		}
		_ = json.Unmarshal([]byte(exclusions), &v.Exclusions)
		out = append(out, v)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
