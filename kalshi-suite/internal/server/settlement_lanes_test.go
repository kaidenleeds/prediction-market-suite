package server

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestSettlementTopologyKeepsIndependentLanes(t *testing.T) {
	s := &Server{}
	lanes := s.settlementMonitorLanes()
	want := []string{
		"settlement-core",
		"settlement-books",
		"settlement-research",
		"settlement-funded-combos",
		"staged-bundle-paper",
		"settlement-subcent-golf",
		"settlement-crossvenue",
		"settlement-live",
	}
	if len(lanes) != len(want) {
		t.Fatalf("settlement lanes=%d want %d", len(lanes), len(want))
	}
	for i, name := range want {
		if lanes[i].name != name {
			t.Fatalf("lane[%d]=%q want %q", i, lanes[i].name, name)
		}
		if lanes[i].work == nil || lanes[i].every <= 0 || lanes[i].deadline <= 0 {
			t.Fatalf("lane %q has incomplete bounded schedule: %+v", name, lanes[i])
		}
	}
}

func TestBlockedCoreLaneCannotStarveBookOrResearchLanes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	releaseCore := make(chan struct{})
	coreStarted := make(chan struct{}, 1)
	bookRan := make(chan struct{}, 1)
	researchRan := make(chan struct{}, 1)
	guard := func(_ string, f func()) { f() }
	noop := func(string) {}

	startBoundedMonitorLane(ctx, "settlement-core", time.Hour, time.Second, func(context.Context) {
		coreStarted <- struct{}{}
		<-releaseCore // model a venue client that has not returned yet
	}, noop, guard, noop)
	select {
	case <-coreStarted:
	case <-time.After(time.Second):
		t.Fatal("core lane did not start")
	}

	startBoundedMonitorLane(ctx, "settlement-books", time.Hour, time.Second, func(context.Context) {
		bookRan <- struct{}{}
	}, noop, guard, noop)
	startBoundedMonitorLane(ctx, "settlement-research", time.Hour, time.Second, func(context.Context) {
		researchRan <- struct{}{}
	}, noop, guard, noop)

	for name, ran := range map[string]<-chan struct{}{
		"books":    bookRan,
		"research": researchRan,
	} {
		select {
		case <-ran:
		case <-time.After(time.Second):
			close(releaseCore)
			t.Fatalf("%s settlement lane waited behind blocked core", name)
		}
	}
	close(releaseCore)
}

func TestSignalResolutionCursorPersistsAcrossServerRestart(t *testing.T) {
	s1 := testServer(t)
	s1.sigCurLoaded = true
	s1.sigCurKal = 41
	s1.sigCurPoly = 17
	s1.sigCurPus = 29
	s1.persistSignalResolutionCursors()

	raw, ok := s1.store.KVGet(context.Background(), signalResolutionCursorKV)
	if !ok {
		t.Fatal("signal-resolution cursor was not persisted")
	}
	var stored signalResolutionCursorState
	if err := json.Unmarshal([]byte(raw), &stored); err != nil {
		t.Fatalf("decode stored cursor: %v", err)
	}
	if stored.Kalshi != 41 || stored.Polymarket != 17 || stored.PolyUS != 29 {
		t.Fatalf("stored cursor=%+v", stored)
	}

	// A fresh Server around the same Store models process-state loss without destroying the DB.
	s2 := &Server{store: s1.store, log: s1.log}
	s2.loadSignalResolutionCursors(context.Background())
	if s2.sigCurKal != 41 || s2.sigCurPoly != 17 || s2.sigCurPus != 29 {
		t.Fatalf("restored cursors=(%d,%d,%d)", s2.sigCurKal, s2.sigCurPoly, s2.sigCurPus)
	}
}
