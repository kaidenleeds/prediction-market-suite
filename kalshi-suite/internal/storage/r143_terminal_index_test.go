package storage

import (
	"context"
	"strings"
	"testing"
)

func TestR143TerminalReadersUseBoundedIndexes(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	ctx := context.Background()
	checks := []struct {
		query string
		want  string
	}{
		{`SELECT id FROM signal_log WHERE platform='kalshi' AND ticker='KX' AND resolved=1
 AND settle_val>=0 AND settle_val<=1 ORDER BY resolved_at DESC,id DESC LIMIT 1`, "idx_signal_exact_settlement"},
		{`SELECT r2.id FROM signal_log r2 WHERE r2.platform='kalshi' AND r2.ticker='KX'
 AND r2.resolved=1 AND r2.settle_val>=0 AND r2.settle_val<=1
 AND r2.resolved_at IS NOT NULL AND r2.resolved_at!=''
 ORDER BY r2.resolved_at DESC,r2.id DESC LIMIT 1`, "idx_signal_exact_settlement"},
		{`SELECT id FROM signal_log WHERE platform='kalshi' AND ticker='KX' AND resolved=1
 AND settle_val>=0 AND book_feature_ver=1 AND pricing_version='book-native-v2'
 AND book_ask>0 AND book_taker_fee_pc IS NOT NULL ORDER BY id DESC LIMIT 1`, "idx_signal_series_roll"},
		{`SELECT opportunity_id FROM research_route_opportunities
 WHERE CAST(json_extract(evidence_json,'$.system_observation_id') AS INTEGER)=7
 AND route='taker' AND decision='candidate'`, "idx_rroute_system_observation"},
		{`SELECT id FROM research_route_events WHERE opportunity_id='o' AND route_id='r' AND event_type='grade'`, "idx_rroute_event_route_type"},
	}
	for _, tc := range checks {
		rows, err := st.db.QueryContext(ctx, "EXPLAIN QUERY PLAN "+tc.query)
		if err != nil {
			t.Fatal(err)
		}
		plan := ""
		for rows.Next() {
			var id, parent, unused int
			var detail string
			if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			plan += detail + "\n"
		}
		rows.Close()
		if !strings.Contains(plan, tc.want) {
			t.Fatalf("query plan did not use %s:\n%s", tc.want, plan)
		}
	}
}
