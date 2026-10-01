package server

import (
	"strings"
	"testing"
)

func TestR144VerdictStateReceiptReadsPlainStateAndWritesMethodVersion(t *testing.T) {
	old, legacy := decodeVerdictStateReceipt("PROVEN+")
	if !legacy || old.State != "PROVEN+" || old.ProofMethod != "" {
		t.Fatalf("plain receipt compatibility failed: legacy=%v receipt=%+v", legacy, old)
	}
	v := verdictEnt{Family: "taker:edge@kalshi", Group: "taker", Route: "taker", State: "COLLECTING"}
	raw := encodeVerdictStateReceipt(v)
	got, legacy := decodeVerdictStateReceipt(raw)
	if legacy || got.State != "COLLECTING" || got.ProofMethod != verdictProofMethodContract {
		t.Fatalf("versioned receipt round trip failed: raw=%q legacy=%v receipt=%+v", raw, legacy, got)
	}
}

func TestR144ProofMethodResetIsGroupedAndNeverLeaksSentinel(t *testing.T) {
	previous := verdictStateReceipt{State: "PROVEN+"} // old plain-state receipt had no method tag
	resetA := verdictTelegramChangeFor(previous, verdictEnt{
		Family: "taker:kalshi-flow@kalshi", Group: "taker", Route: "taker", Side: "YES",
		State: "COLLECTING", N: 0, SettledRows: 949, UnclusteredMarkets: 949,
		Lo: -9999, Hi: 9999, Unit: "$/contract",
	})
	resetB := verdictTelegramChangeFor(previous, verdictEnt{
		Family: "taker:favlong@polyus", Group: "taker", Route: "taker", Side: "YES",
		State: "COLLECTING", N: 0, SettledRows: 714, UnclusteredMarkets: 714,
		Lo: -9999, Hi: 9999, Unit: "$/contract",
	})
	proved := verdictTelegramChangeFor(verdictStateReceipt{State: "COLLECTING", ProofMethod: verdictProofMethodContract}, verdictEnt{
		Family: "taker:new-edge@kalshi", Group: "taker", Route: "taker", Side: "NO",
		State: "PROVEN+", N: 30, Mean: .05, Lo: .01, Hi: .09, Unit: "$/contract",
		ProfitEvidence: true, FillConditioned: true, EvidenceTier: "authenticated_live_fill",
	})
	entries := map[string]verdictTelegramChange{resetA.Key: resetA, resetB.Key: resetB, proved.Key: proved}
	got := verdictTelegramBatchMessage(entries)
	for _, want := range []string{
		"3 changes in the last 15 minutes",
		"2 exact routes reset to prospective evidence",
		"1663 old settled route-market cells remain visible but excluded",
		"taker:new-edge@kalshi [NO] is now PROVEN+",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("grouped message missing %q: %s", want, got)
		}
	}
	for _, forbidden := range []string{"9999", "taker:kalshi-flow", "taker:favlong"} {
		if strings.Contains(got, forbidden) {
			t.Fatalf("grouped message leaked %q: %s", forbidden, got)
		}
	}
}

func TestR144VerdictTransitionLineUsesUnavailableForUnboundedRange(t *testing.T) {
	got := verdictTransitionLine(verdictEnt{Family: "taker:test@kalshi", State: "COLLECTING",
		N: 0, Lo: -9999, Hi: 9999, Unit: "$/contract"})
	if !strings.Contains(got, "95% range unavailable") || strings.Contains(got, "9999") {
		t.Fatalf("unbounded phone range was not sanitized: %s", got)
	}
}

func TestR144CurrentMethodNZeroIsARealRegressionNotAMethodReset(t *testing.T) {
	previous := verdictStateReceipt{State: "PROVEN+", ProofMethod: verdictProofMethodContract}
	v := verdictEnt{Family: "taker:test@kalshi", Group: "taker", Route: "taker",
		State: "COLLECTING", N: 0, SettledRows: 20, UnclusteredMarkets: 20}
	if change := verdictTelegramChangeFor(previous, v); change.Kind != "verdict" {
		t.Fatalf("same-method evidence loss must remain a visible verdict regression: %+v", change)
	}
}

func TestR144VerdictBatchCoalescesStableIdentityAndMigratesLegacyQueue(t *testing.T) {
	batch := verdictTelegramBatch{Lines: []string{
		"Experiment verdict: taker:old@kalshi [YES] is now COLLECTING (n=0, edge +0.000 $/contract, 95% range -9999.000..+9999.000)",
		"Experiment verdict: taker:tiny@kalshi [NO] is now COLLECTING (n=1, edge +0.010 $/contract, 95% range -9999.000..+9999.000)",
	}}
	batch.merge([]verdictTelegramChange{
		{Key: "edge|side=YES", Kind: "verdict", Line: "Experiment verdict: edge is now PROVEN+"},
		{Key: "edge|side=YES", Kind: "verdict", Line: "Experiment verdict: edge is now COLLECTING"},
	})
	if len(batch.Entries) != 3 || len(batch.Lines) != 0 {
		t.Fatalf("legacy migration/coalescing failed: %+v", batch)
	}
	if got := batch.Entries["edge|side=YES"].Line; !strings.Contains(got, "COLLECTING") {
		t.Fatalf("latest transition did not win: %q", got)
	}
	for _, entry := range batch.Entries {
		if strings.Contains(entry.Line, "9999") {
			t.Fatalf("legacy sentinel survived normalization: %+v", entry)
		}
	}
}
