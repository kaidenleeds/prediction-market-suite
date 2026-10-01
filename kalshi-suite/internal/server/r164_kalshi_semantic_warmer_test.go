package server

import (
	"context"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
)

func waitR164ResidentSemantic(t *testing.T, s *Server, tickers ...string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		ready := true
		for _, ticker := range tickers {
			if _, ok := s.r159KalshiResidentSemanticDescriptor(ticker, "YES"); !ok {
				ready = false
				break
			}
		}
		if ready {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("semantic cache did not warm for %v", tickers)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestR164ResidentSemanticMissFailsClosedThenBackgroundWarmRecovers(t *testing.T) {
	s, venue := newR153SemanticServer(t)
	r159WarmCompleteBoard(t, s)
	ctx := r159KalshiResidentAdmissionContext(context.Background())
	held := r153Held(r153BKNWinner, r153GameEvent, "YES", "position")

	if got := s.kalshiSemanticExposureConflict(ctx, r153HOUOver35, "YES", held); got != "kalshi-payoff-identity-unavailable" {
		t.Fatalf("cold resident guard=%q, want fail-closed candidate identity miss", got)
	}
	waitR164ResidentSemantic(t, s, r153HOUOver35, r153BKNWinner)
	before := r159SemanticPublicCallCount(venue)
	if got := s.kalshiSemanticExposureConflict(ctx, r153HOUOver35, "YES", held); got != "kalshi-logical-payoff-conflict" {
		t.Fatalf("warmed resident guard=%q, want real payoff conflict", got)
	}
	if after := r159SemanticPublicCallCount(venue); after != before {
		t.Fatalf("resident money guard made %d public calls after warm", after-before)
	}
}

func TestR164SemanticWarmAdmissionIsNonblockingDedupedAndCoversSnapshot(t *testing.T) {
	s, venue := newR153SemanticServer(t)
	r159WarmCompleteBoard(t, s)

	venue.mu.Lock()
	started := time.Now()
	for range 100 {
		if !s.scheduleKalshiSemanticWarm(r153HOUOver35) {
			venue.mu.Unlock()
			t.Fatal("bounded semantic warm was not admitted")
		}
	}
	if elapsed := time.Since(started); elapsed > 25*time.Millisecond {
		venue.mu.Unlock()
		t.Fatalf("semantic warm admission blocked for %v", elapsed)
	}
	venue.mu.Unlock()

	s.scheduleKalshiSemanticSnapshotWarm(r154KalshiAdmissionSnapshot{
		positions: []kalshi.MarketPosition{{Ticker: r153BKNWinner, Position: 1}},
		orders: []kalshi.Order{{
			Ticker: r153Over1825, Action: "buy", OutcomeSide: "yes",
		}},
	})
	waitR164ResidentSemantic(t, s, r153HOUOver35, r153BKNWinner, r153Over1825)
	venue.mu.RLock()
	hourEventCalls := venue.eventCalls[r153SpreadEvent]
	venue.mu.RUnlock()
	if hourEventCalls != 1 {
		t.Fatalf("100 same-ticker warm requests made %d Event calls; want one", hourEventCalls)
	}
}
