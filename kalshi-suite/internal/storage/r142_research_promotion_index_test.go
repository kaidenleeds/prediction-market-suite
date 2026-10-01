package storage

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestR142RecentResearchPaperCandidatesUsesUTCObservedIndex(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	// Pin the access-path contract: wrapping observed_ts in julianday() used to convert this
	// 45-second lookup into a full immutable-observation scan on every five-second dispatch.
	const planSQL = `EXPLAIN QUERY PLAN SELECT o.id
FROM research_system_observations o INDEXED BY idx_rsystem_obs_observed JOIN research_event_specs e
 ON e.event_id=o.canonical_event_id AND e.version=o.event_version
WHERE o.observation_kind='candidate' AND o.candidate=1 AND o.blocker=''
 AND o.certificate_status='verified' AND o.route IN ('maker','taker')
 AND o.venue IN ('kalshi','polyus') AND o.observed_ts>=?
ORDER BY o.observed_ts DESC,o.id DESC LIMIT ?`
	rows, err := st.db.QueryContext(ctx, planSQL,
		time.Now().UTC().Add(-45*time.Second).Format(time.RFC3339Nano), 100)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	plan := ""
	for rows.Next() {
		var id, parent, notused int
		var detail string
		if err := rows.Scan(&id, &parent, &notused, &detail); err != nil {
			t.Fatal(err)
		}
		plan += detail + "\n"
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(plan, "idx_rsystem_obs_observed") ||
		strings.Contains(strings.ToLower(plan), "temp b-tree for order by") {
		t.Fatalf("recent candidate query lost its observed-time index/order path:\n%s", plan)
	}
}
