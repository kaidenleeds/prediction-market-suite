package storage

import (
	"context"
	"strings"
	"testing"
)

func TestR146IdentityLockFunnelUsesSystemIndex(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	rows, err := st.db.Query(`EXPLAIN QUERY PLAN SELECT observed_ts,system_name,canonical_event_id,venue,ticker,side,
identity_status,quote_source,fee_authority,decision,decision_reason,evidence_json
FROM research_route_opportunities
WHERE system_name='identity-challenged-cross-venue-lock'
   OR system_name GLOB 'xvlock*' OR system_name GLOB 'xvgap*'
ORDER BY observed_ts DESC LIMIT 20`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var plan strings.Builder
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		plan.WriteString(detail)
		plan.WriteByte('\n')
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if got := plan.String(); !strings.Contains(got, "idx_rroute_system") || strings.Contains(got, "idx_rroute_observed") {
		t.Fatalf("identity-lock funnel regressed to the 2.8M-row observed-time scan:\n%s", got)
	}
	if got, err := st.ResearchSystemFunnelInputs(context.Background(), "identity-challenged-cross-venue-lock", 20); err != nil || len(got) != 0 {
		t.Fatalf("fresh identity-lock funnel rows=%d err=%v", len(got), err)
	}
}
