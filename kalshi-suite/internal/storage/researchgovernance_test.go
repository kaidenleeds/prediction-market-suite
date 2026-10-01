package storage

import (
	"context"
	"testing"
)

func TestResearchGovernanceAuthorityAndAppendOnlyChecks(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	if err := st.EnsureR138ResearchBlueprints(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := st.AppendResearchSecurityEvent(ctx, ResearchSecurityEvent{
		Category: "authority_check", Severity: "info", Message: "research authority remains zero",
		EvidenceJSON: `{"checked":true}`,
	}); err != nil {
		t.Fatal(err)
	}
	report, err := st.ResearchGovernanceReport(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if report["research_authority_violations"].(int) != 0 || !report["compliant_for_research"].(bool) ||
		report["live_expansion_authorized"].(bool) {
		t.Fatalf("report=%+v", report)
	}
	if report["authority_tables_checked"].(int) < 40 || len(report["partial_authority_columns"].([]string)) != 0 {
		t.Fatalf("governance schema coverage is incomplete: %+v", report)
	}
	if _, err := st.db.ExecContext(ctx, `DELETE FROM research_security_events`); err == nil {
		t.Fatal("security receipt delete succeeded")
	}
	if _, err := st.db.ExecContext(ctx, `INSERT INTO research_nested_ladders(
observed_ts,slot,event_id,low_ticker,high_ticker,low_strike,high_strike,low_yes_bid,low_yes_ask,
high_yes_bid,high_yes_ask,low_ask_depth,high_no_ask_depth,all_leg_cost,exact_entry_fee,
net_lock_if_both_fill,capacity_contracts,partial_fill_worst_loss,unwind_buffer,funded,
paper_authority,live_authority,identity_source)
VALUES('2026-07-12T00:00:00Z','slot','event','low','high',1,2,.4,.41,.2,.21,1,1,.62,0,
.38,1,-.41,.02,0,1,0,'test')`); err == nil {
		t.Fatal("legacy nested-ladder table admitted Paper authority")
	}
}

func TestForbiddenResearchSystemsAreRejectedWithoutFalsePositive(t *testing.T) {
	for _, name := range []string{"spoofing-alpha", "wash-trading-maker", "mnpi-news", "market-manipulation"} {
		if bad, _ := ForbiddenResearchSystem(name, name); !bad {
			t.Fatalf("%q was not rejected", name)
		}
	}
	for _, name := range []string{"flow-direction-integrity", "attention-spillover-graph", "anti-manipulation-detector"} {
		if bad, why := ForbiddenResearchSystem(name, name); bad {
			t.Fatalf("%q false positive: %s", name, why)
		}
	}
}
