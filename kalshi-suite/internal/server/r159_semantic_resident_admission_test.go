package server

import (
	"context"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
)

func r159SemanticPublicCallCount(venue *r153SemanticVenue) int {
	venue.mu.RLock()
	defer venue.mu.RUnlock()
	total := venue.boardCalls + venue.milestoneCalls
	for _, calls := range venue.marketCalls {
		total += calls
	}
	for _, calls := range venue.eventCalls {
		total += calls
	}
	return total
}

func r159WarmCompleteBoard(t *testing.T, s *Server) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := s.kal.GetTopLiquidMarkets(ctx); err != nil {
		t.Fatalf("warm complete board: %v", err)
	}
	if markets, observedAt := s.kal.CompleteBoardSnapshot(); len(markets) == 0 || observedAt.IsZero() {
		t.Fatalf("complete board receipt missing after warm: markets=%d at=%v", len(markets), observedAt)
	}
}

func r159WarmSemanticDescriptor(t *testing.T, s *Server, ticker, side string) {
	t.Helper()
	if _, err := s.kalshiSemanticDescriptor(context.Background(), ticker, side); err != nil {
		t.Fatalf("warm semantic descriptor %s: %v", ticker, err)
	}
}

func TestR159UrgentKalshiEventAdmissionUsesOnlyResidentPublicFacts(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*testing.T, *Server) ([]kalshi.MarketPosition, []kalshi.Order)
	}{
		{
			name: "held position",
			setup: func(_ *testing.T, _ *Server) ([]kalshi.MarketPosition, []kalshi.Order) {
				return []kalshi.MarketPosition{{Ticker: r153BKNWinner, Position: 1}}, nil
			},
		},
		{
			name: "held order",
			setup: func(_ *testing.T, _ *Server) ([]kalshi.MarketPosition, []kalshi.Order) {
				return nil, []kalshi.Order{{
					Ticker: r153BKNWinner, Action: "buy", OutcomeSide: "yes",
				}}
			},
		},
		{
			name: "durable reservation",
			setup: func(t *testing.T, s *Server) ([]kalshi.MarketPosition, []kalshi.Order) {
				insertR153SemanticReservation(t, s, r153BKNWinner)
				return nil, nil
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s, venue := newR153SemanticServer(t)
			r159WarmCompleteBoard(t, s)
			r159WarmSemanticDescriptor(t, s, r153HOUOver35, "YES")
			r159WarmSemanticDescriptor(t, s, r153BKNWinner, "YES")
			positions, orders := test.setup(t, s)
			before := r159SemanticPublicCallCount(venue)

			got := s.liveKalshiEventHeaderConflict(
				r159KalshiResidentAdmissionContext(context.Background()),
				r153HOUOver35, "YES", positions, orders)
			if got != "kalshi-logical-payoff-conflict" {
				t.Fatalf("resident urgent guard = %q, want semantic payoff conflict", got)
			}
			if after := r159SemanticPublicCallCount(venue); after != before {
				t.Fatalf("urgent guard made %d public semantic/market calls, want zero", after-before)
			}
		})
	}
}

func TestR159UrgentKalshiEventAdmissionFailsClosedWithoutPublicRecovery(t *testing.T) {
	t.Run("candidate market absent", func(t *testing.T) {
		s, venue := newR153SemanticServer(t)
		r159WarmCompleteBoard(t, s)
		before := r159SemanticPublicCallCount(venue)
		got := s.liveKalshiEventHeaderConflict(
			r159KalshiResidentAdmissionContext(context.Background()),
			"KXR159-UNKNOWN-CANDIDATE", "YES", nil, nil)
		if got != "kalshi-event-identity-unavailable" {
			t.Fatalf("cold urgent guard = %q, want event identity refusal", got)
		}
		if after := r159SemanticPublicCallCount(venue); after != before {
			t.Fatalf("cold urgent guard made %d public calls, want zero", after-before)
		}
	})

	t.Run("standalone pick does not require cold broader semantics", func(t *testing.T) {
		s, venue := newR153SemanticServer(t)
		r159WarmCompleteBoard(t, s)
		before := r159SemanticPublicCallCount(venue)
		got := s.liveKalshiEventHeaderConflict(
			r159KalshiResidentAdmissionContext(context.Background()),
			r153HOUOver35, "YES", nil, nil)
		if got != "" {
			t.Fatalf("standalone resident urgent guard refused cold semantics: %q", got)
		}
		if after := r159SemanticPublicCallCount(venue); after != before {
			t.Fatalf("standalone urgent guard made %d public calls, want zero", after-before)
		}
	})

	t.Run("semantic event expired", func(t *testing.T) {
		s, venue := newR153SemanticServer(t)
		r159WarmCompleteBoard(t, s)
		r159WarmSemanticDescriptor(t, s, r153HOUOver35, "YES")
		r159WarmSemanticDescriptor(t, s, r153BKNWinner, "YES")
		s.kalSemMu.Lock()
		entry := s.kalSemEvents[r153SpreadEvent]
		entry.At = time.Now().Add(-kalshiSemanticSuccessTTL - time.Second)
		s.kalSemEvents[r153SpreadEvent] = entry
		s.kalSemMu.Unlock()
		before := r159SemanticPublicCallCount(venue)

		got := s.liveKalshiEventHeaderConflict(
			r159KalshiResidentAdmissionContext(context.Background()),
			r153HOUOver35, "YES",
			[]kalshi.MarketPosition{{Ticker: r153BKNWinner, Position: 1}}, nil)
		if got != "kalshi-payoff-identity-unavailable" {
			t.Fatalf("expired urgent guard = %q, want payoff identity refusal", got)
		}
		if after := r159SemanticPublicCallCount(venue); after != before {
			t.Fatalf("expired urgent guard made %d public calls, want zero", after-before)
		}
	})

	t.Run("held combined-market metadata absent", func(t *testing.T) {
		s, venue := newR153SemanticServer(t)
		r159WarmCompleteBoard(t, s)
		r159WarmSemanticDescriptor(t, s, r153HOUOver35, "YES")
		before := r159SemanticPublicCallCount(venue)

		got := s.liveKalshiEventHeaderConflict(
			r159KalshiResidentAdmissionContext(context.Background()),
			r153HOUOver35, "YES",
			[]kalshi.MarketPosition{{Ticker: "KXMVE-R159-COLD", Position: 1}}, nil)
		if got != "kalshi-held-event-identity-unavailable" {
			t.Fatalf("cold held-combo urgent guard = %q, want held identity refusal", got)
		}
		if after := r159SemanticPublicCallCount(venue); after != before {
			t.Fatalf("cold held-combo urgent guard made %d public calls, want zero", after-before)
		}
	})
}

func TestR159UrgentKalshiDirectSemanticCacheMissNeverFallsBack(t *testing.T) {
	s, venue := newR153SemanticServer(t)
	ctx := r159KalshiResidentAdmissionContext(context.Background())
	if _, err := s.kalshiSemanticEvent(ctx, r153GameEvent); err == nil {
		t.Fatal("cold resident-only Event lookup unexpectedly succeeded")
	}
	if _, err := s.kalshiSemanticMilestone(ctx, r153GameEvent); err == nil {
		t.Fatal("cold resident-only Milestone lookup unexpectedly succeeded")
	}
	if calls := r159SemanticPublicCallCount(venue); calls != 0 {
		t.Fatalf("resident-only direct cache misses made %d public calls, want zero", calls)
	}
}

func TestR159UrgentKalshiHorizonUsesResidentBoardAndNeverPublicREST(t *testing.T) {
	s, venue := newR153SemanticServer(t)
	s.paperHorizonFn = nil
	expiresAt := time.Now().UTC().Add(90 * time.Minute).Format(time.RFC3339)
	venue.mu.Lock()
	venue.markets[r153HOUOver35]["expected_expiration_time"] = expiresAt
	venue.mu.Unlock()
	r159WarmCompleteBoard(t, s)
	before := r159SemanticPublicCallCount(venue)
	ctx := r159KalshiResidentAdmissionContext(context.Background())

	hours := s.paperEntryResolveHours(ctx, "kalshi", r153HOUOver35, "")
	if hours < 1 || hours > 2 {
		t.Fatalf("resident horizon=%.4f hours, want roughly 1.5", hours)
	}
	if cold := s.paperEntryResolveHours(ctx, "kalshi", "KXR159-COLD-HORIZON", ""); cold != 0 {
		t.Fatalf("cold resident horizon=%.4f, want fail-closed zero", cold)
	}
	if after := r159SemanticPublicCallCount(venue); after != before {
		t.Fatalf("resident horizon made %d public calls, want zero", after-before)
	}
}
