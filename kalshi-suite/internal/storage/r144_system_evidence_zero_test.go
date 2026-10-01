package storage

import (
	"context"
	"encoding/json"
	"testing"
)

func TestR144SystemEvidenceIncludesEveryRegisteredZeroRowSystem(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	inserted, err := st.InsertResearchSystemRuntimeReceipt(context.Background(), ResearchSystemRuntimeReceipt{
		SystemID: "payoff-constraint-solver", State: "BLOCKED", Reason: "no certified payoff portfolio yet",
		Prerequisites: []string{"verified current payoff constraints and simultaneous exact books"},
	})
	if err != nil || !inserted {
		t.Fatalf("insert zero-row runtime receipt: inserted=%v err=%v", inserted, err)
	}

	report, err := st.ResearchSystemEvidenceReport(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(report["systems"])
	if err != nil {
		t.Fatal(err)
	}
	var systems map[string]struct {
		Rows       int64  `json:"rows"`
		Candidates int64  `json:"candidates"`
		Negative   int64  `json:"negative"`
		Controls   int64  `json:"controls"`
		State      string `json:"state"`
		Reason     string `json:"reason"`
		Blocker    string `json:"blocker"`
	}
	if err := json.Unmarshal(raw, &systems); err != nil {
		t.Fatal(err)
	}
	var wire map[string]map[string]any
	if err := json.Unmarshal(raw, &wire); err != nil {
		t.Fatal(err)
	}
	if len(systems) != len(ResearchExperimentIDs()) || len(systems) != 19 {
		t.Fatalf("evidence systems=%d, registry=%d", len(systems), len(ResearchExperimentIDs()))
	}
	payoff, ok := systems["payoff-constraint-solver"]
	if !ok {
		t.Fatal("registered zero-row payoff-constraint-solver disappeared from evidence report")
	}
	if payoff.Rows != 0 || payoff.Candidates != 0 || payoff.Negative != 0 || payoff.Controls != 0 {
		t.Fatalf("zero-row evidence was fabricated: %+v", payoff)
	}
	if payoff.State == "" || payoff.Reason == "" {
		t.Fatalf("zero-row system lacks honest runtime state/reason: %+v", payoff)
	}
	if payoff.State != "BLOCKED" || payoff.Reason != "no certified payoff portfolio yet" || payoff.Blocker != payoff.Reason {
		t.Fatalf("zero-row system lost its precise runtime blocker: %+v", payoff)
	}
	for _, key := range []string{"rows", "candidates", "negative", "controls", "state", "reason", "blocker"} {
		if _, exists := wire["payoff-constraint-solver"][key]; !exists {
			t.Fatalf("zero-row system wire evidence omitted %q: %#v", key, wire["payoff-constraint-solver"])
		}
	}
}
