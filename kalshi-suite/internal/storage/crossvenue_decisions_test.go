package storage

import (
	"context"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/objectiveidentity"
)

func storageObjectiveContract(venue, id string) objectiveidentity.Contract {
	return objectiveidentity.Contract{Venue: venue, InstrumentID: id, Genre: "sports", EventID: "sports:G1",
		League: "mlb", Participants: []string{"PHI", "KC"}, OutcomeCardinality: 2, Period: "full_game", MarketType: "winner",
		Selection: "PHI", Opposite: "KC", Comparator: "wins", DeadlineUTC: "2026-07-12T20:00:00Z",
		Timezone: "UTC", VoidPolicy: "refund", OvertimePolicy: "official-final", TiePolicy: "official-winner",
		PolicySource: venue + ":official-v1", SettlementClass: "objective_result",
		SourceKind: "official_structured", Structured: true}
}

func TestCrossVenueDecisionLedgerStoresDirectionalAndLockTiersImmutably(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	left, right := storageObjectiveContract("kalshi", "K1"), storageObjectiveContract("polyus", "p1")
	strict := objectiveidentity.Evaluate(left, right)
	if inserted, err := st.InsertCrossVenueMatchDecision(ctx, CrossVenueMatchDecision{Observed: time.Now(), CycleID: "cycle-1",
		PairID: "k-pus|K1|p1", PairType: "K-PUS", LeftVenue: "kalshi", LeftInstrumentID: "K1",
		RightVenue: "polyus", RightInstrumentID: "p1", Left: left, Right: right, Verdict: strict}); err != nil || !inserted {
		t.Fatalf("strict insert=%v err=%v verdict=%+v", inserted, err, strict)
	}
	left.PolicySource, left.VoidPolicy, left.OvertimePolicy, left.TiePolicy = "", "", "", ""
	right.PolicySource, right.VoidPolicy, right.OvertimePolicy, right.TiePolicy = "", "", "", ""
	directional := objectiveidentity.EvaluateDirectional(left, right)
	if inserted, err := st.InsertCrossVenueMatchDecision(ctx, CrossVenueMatchDecision{Observed: time.Now(), CycleID: "cycle-2",
		PairID: "k-pus|K1|p1", PairType: "K-PUS", LeftVenue: "kalshi", LeftInstrumentID: "K1",
		RightVenue: "polyus", RightInstrumentID: "p1", Left: left, Right: right, Verdict: directional}); err != nil || !inserted {
		t.Fatalf("directional insert=%v err=%v verdict=%+v", inserted, err, directional)
	}
	report, err := st.CrossVenueDecisionReport(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if report["total"] != 2 || report["accepted_directional"] != 2 || report["lock_eligible"] != 1 {
		t.Fatalf("report=%+v", report)
	}
	if _, err := st.db.Exec(`UPDATE research_crossvenue_match_decisions SET reason_code='REJECT_MUTATED'`); err == nil {
		t.Fatal("append-only decision was mutable")
	}
}
