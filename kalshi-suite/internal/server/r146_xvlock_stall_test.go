package server

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestR146ResolutionCertificateInputIsOnlyTheRequestedPair(t *testing.T) {
	inputs := make(map[string]storage.ResolutionBasisInput, 10_002)
	for i := 0; i < 10_000; i++ {
		key := fmt.Sprintf("kalshi|unrelated-%05d", i)
		inputs[key] = storage.ResolutionBasisInput{Venue: "kalshi", Ticker: key}
	}
	leftKey, rightKey := "kalshi|left", "polyus|right"
	inputs[leftKey] = storage.ResolutionBasisInput{Venue: "kalshi", Ticker: "left", EventID: "event"}
	inputs[rightKey] = storage.ResolutionBasisInput{Venue: "polyus", Ticker: "right", EventID: "event"}

	pair := xvlPairResolutionInputs(inputs, leftKey, rightKey)
	if len(pair) != 2 {
		t.Fatalf("certificate input retained %d rows; want only the two requested legs", len(pair))
	}
	if pair[leftKey].Ticker != "left" || pair[rightKey].Ticker != "right" {
		t.Fatalf("pair inputs lost a requested leg: %+v", pair)
	}
	if _, leaked := pair["kalshi|unrelated-09999"]; leaked {
		t.Fatal("certificate input retained an unrelated catalog row")
	}
	// Values are copied structs: certificate normalization cannot mutate the shared scan batch.
	left := pair[leftKey]
	left.EventID = "changed"
	pair[leftKey] = left
	if inputs[leftKey].EventID != "event" {
		t.Fatal("pair normalization mutated the shared resolution-input batch")
	}
}

func TestR147XvLockSettlementDoesNotHoldLedgerAcrossResolutionIO(t *testing.T) {
	s := testServer(t)
	s.xvlBook = &xvLockBook{Open: []xvLockOpp{{
		Key: "K-PUS|A|B|same", AVenue: "kalshi", AID: "A", ASide: "YES",
		BVenue: "polyus", BID: "B", BSide: "NO", AWon: -1, BWon: -1,
	}}}
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	s.xvlLegWonFn = func(ctx context.Context, _, _, _ string, _ *int) float64 {
		once.Do(func() { close(entered) })
		select {
		case <-ctx.Done():
			return -1
		case <-release:
			return -1
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		s.settleXvLocks(ctx)
		close(done)
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("settlement resolver did not start")
	}
	if !s.xvlMu.TryLock() {
		close(release)
		<-done
		t.Fatal("settlement held xvlMu while resolution I/O was blocked")
	}
	s.xvlMu.Unlock()
	close(release)
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("settlement did not finish after resolver release")
	}
}

func TestR146XvLockWaitHonorsCanceledContext(t *testing.T) {
	s := testServer(t)
	s.xvlMu.Lock()
	defer s.xvlMu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	started := time.Now()
	if s.xvlLockContext(ctx, time.Minute) {
		t.Fatal("canceled lock wait unexpectedly acquired the busy mutex")
	}
	if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
		t.Fatalf("canceled lock wait took %s; want immediate bounded cancellation", elapsed)
	}
}

func TestR146BriefingUsesLastGoodComboMetricsInsteadOfWaitingForXvLock(t *testing.T) {
	s := testServer(t)
	want := bookSessionMetrics{
		NetUSD: 7.25, Bets: 4, Units: 8, UnitsComplete: true, Open: 3, OpenComplete: true,
	}
	s.cacheBriefPortfolioMetrics(vbCombos, want, true)
	s.xvlMu.Lock()
	defer s.xvlMu.Unlock()

	started := time.Now()
	got, ok := s.briefPortfolioSessionMetrics(context.Background(), vbCombos)
	if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
		t.Fatalf("briefing waited %s behind xvlock; want immediate last-good fallback", elapsed)
	}
	if !ok || got != want {
		t.Fatalf("briefing fallback = (%+v,%v), want (%+v,true)", got, ok, want)
	}
}
