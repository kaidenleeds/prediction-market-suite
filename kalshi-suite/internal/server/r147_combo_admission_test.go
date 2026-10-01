package server

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/paper"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestR147LegacyComboBacklogCannotConsumeCurrentHorizonSample(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	epoch, err := s.plabCurrentAdmissionEpoch(ctx, now, true)
	if err != nil {
		t.Fatal(err)
	}
	legacy := make([]storage.PlabCand, 0, plabSampleOpenCap)
	for i := 0; i < plabSampleOpenCap; i++ {
		legacy = append(legacy, storage.PlabCand{ID: fmt.Sprintf("pre-horizon-%03d", i), At: epoch - 1,
			Bucket: "indep", Class: "ind:2leg", Legality: "synthetic_legs_only", Prod: .16,
			JointP: .49, EVSyn: .1, LegFees: "[0,0]", Legs: "[]", NLegs: 2,
			Cohort: storage.ComboLabCohortAllEligible, RouteState: "synthetic-settlement-only"})
	}
	if n, err := s.store.PlabInsertBatch(ctx, legacy); err != nil || n != plabSampleOpenCap {
		t.Fatalf("legacy insert n=%d err=%v", n, err)
	}
	pool := r133SamplePool(18)
	s.plabSampleFeeFn = func(plabLeg, float64) (float64, bool) { return 0, true }
	st := s.plabWriteProspectiveSample(ctx, pool, 6, 0, now)
	if st.Inserted == 0 || st.CurrentOpenBefore != 0 || st.LegacyOpen != plabSampleOpenCap {
		t.Fatalf("legacy backlog consumed current horizon sample: %+v", st)
	}
	current, err := s.store.PlabOpenCurrentCountSince(ctx, 6, epoch)
	if err != nil || current != int64(st.Inserted) {
		t.Fatalf("current epoch rows=%d err=%v, want inserted=%d", current, err, st.Inserted)
	}
	total, err := s.store.PlabOpenCount(ctx)
	if err != nil || total != int64(plabSampleOpenCap+st.Inserted) {
		t.Fatalf("legacy settlement truth was lost: total=%d err=%v", total, err)
	}
}

func TestR147CurrentSingleVenueComboSampleCoversTwoThroughSixAndSeparatesCohorts(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	epoch, err := s.plabCurrentAdmissionEpoch(ctx, now, true)
	if err != nil {
		t.Fatal(err)
	}
	pool := make([]plabLeg, 0, 12)
	for i := 0; i < 12; i++ {
		pool = append(pool, plabLeg{Platform: "kalshi", Ticker: fmt.Sprintf("R147-COMBO-%02d", i),
			Side: "YES", Price: .40, PWin: .70, EVNet: .30, Depth: 25,
			EventKey: fmt.Sprintf("event:r147:%02d", i), TwinKey: fmt.Sprintf("twin:r147:%02d", i),
			RollingPositive: true, PositiveSystems: []string{"taker:r147-fixture@kalshi [YES]"},
			SystemRoute: "taker"})
	}
	s.plabSampleFeeFn = func(plabLeg, float64) (float64, bool) { return 0, true }
	st := s.plabWriteProspectiveSample(ctx, pool, 6, 0, now)
	if st.Inserted == 0 {
		t.Fatalf("no current combo rows inserted: %+v", st)
	}
	counts, err := s.store.PlabOpenCurrentCohortLegCountsSince(ctx, 6, epoch)
	if err != nil {
		t.Fatal(err)
	}
	for legs := 2; legs <= 6; legs++ {
		if counts[storage.ComboLabCohortAllEligible][legs] == 0 {
			t.Fatalf("general %d-leg current sample missing: %+v", legs, counts)
		}
		if counts[storage.ComboLabCohortRollingPositive][legs] == 0 {
			t.Fatalf("system-backed %d-leg current sample missing: %+v", legs, counts)
		}
	}
	gradable, err := s.store.PlabGradable(ctx, now.Add(time.Hour).Unix(), 100)
	if err != nil || len(gradable) != 0 {
		t.Fatalf("unsettled current candidates became fabricated grades: rows=%d err=%v", len(gradable), err)
	}
}

func TestR147ComboAdmissionEpochRejectsCorruptAuthority(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	if err := s.store.KVSet(ctx, plabKVAdmission, `{"version":"wrong","epoch_unix":1}`); err != nil {
		t.Fatal(err)
	}
	st := s.plabWriteProspectiveSample(ctx, r133SamplePool(8), 6, 0, time.Now().UTC())
	if st.Inserted != 0 || st.Rejected["admission_epoch_error"] != 1 {
		t.Fatalf("corrupt admission authority did not fail closed: %+v", st)
	}
}

func TestR147PositiveSystemKalshiComboFreezesAndRevalidatesCurrentCollection(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	s.kmkts = map[string]kalshi.Market{
		"K-A": {Ticker: "K-A", EventTicker: "EV-A"},
		"K-B": {Ticker: "K-B", EventTicker: "EV-B"},
	}
	legal := kalshi.MVCollection{CollectionTicker: "COLL", SizeMin: 2,
		Events: map[string]bool{"EV-A": false, "EV-B": false},
		EventCfg: map[string]kalshi.MVEventCfg{
			"EV-A": {IsYesOnly: true, SizeMax: 1},
			"EV-B": {IsYesOnly: true, SizeMax: 1},
		}}
	s.comboCols, s.comboColsAt = []kalshi.MVCollection{legal}, time.Now()
	s.plabSampleFeeFn = func(plabLeg, float64) (float64, bool) { return 0, true }
	pool := []plabLeg{
		{Platform: "kalshi", Ticker: "K-A", Side: "YES", Price: .40, PWin: .70, EVNet: .30,
			Depth: 20, EventKey: "event-a", RollingPositive: true,
			PositiveSystems: []string{"taker:system-a@kalshi [YES]"}, SystemRoute: "taker"},
		{Platform: "kalshi", Ticker: "K-B", Side: "YES", Price: .40, PWin: .70, EVNet: .30,
			Depth: 20, EventKey: "event-b", RollingPositive: true,
			PositiveSystems: []string{"taker:system-b@kalshi [YES]"}, SystemRoute: "taker"},
	}
	candidates, rejected := s.positiveSystemComboCandidates(ctx, pool, 2)
	if len(candidates) != 1 || candidates[0].Collection != "COLL" || rejected["kalshi_not_rfq_compatible"] != 0 {
		t.Fatalf("positive-system candidate lacked frozen Kalshi collection truth: candidates=%+v rejected=%+v", candidates, rejected)
	}
	legs := []paper.Leg{{Platform: "kalshi", Ticker: "K-A", Side: "YES"},
		{Platform: "kalshi", Ticker: "K-B", Side: "YES"}}
	if err := s.validateFreshFundedComboCollection(ctx, legs, "COLL"); err != nil {
		t.Fatalf("unchanged current collection rejected: %v", err)
	}
	if err := s.validateFreshFundedComboCollection(ctx, legs, ""); err == nil ||
		!strings.Contains(strings.ToLower(err.Error()), "frozen collection") {
		t.Fatalf("missing frozen collection identity accepted: %v", err)
	}
	s.comboCols = []kalshi.MVCollection{{CollectionTicker: "OTHER", SizeMin: 2,
		Events: legal.Events, EventCfg: legal.EventCfg}}
	s.comboColsAt = time.Now()
	if err := s.validateFreshFundedComboCollection(ctx, legs, "COLL"); err == nil ||
		!strings.Contains(strings.ToLower(err.Error()), "changed") {
		t.Fatalf("changed collection identity accepted: %v", err)
	}
}
