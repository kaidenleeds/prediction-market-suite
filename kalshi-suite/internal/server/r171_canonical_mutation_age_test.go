package server

import (
	"context"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func r171CanonicalTerminal(t *testing.T, ticker string,
	mutate func(call int, out *storage.Signal)) (storage.ExecutionShadowEvent, int) {
	t.Helper()
	s, _, quoteFor := r154PaperTakerFixture(t, ticker)
	calls := 0
	s.completeBookSignalFn = func(in storage.Signal) (storage.Signal, string, bool) {
		calls++
		out, source, ok := quoteFor(in, .38, .40, 20)
		if mutate != nil {
			mutate(calls, &out)
		}
		return out, source, ok
	}
	releaseShadow := holdExecutionShadowWriterStart(t, s)

	at := time.Now().UTC()
	sig := storage.Signal{
		Platform: "kalshi", Ticker: ticker, Title: "R171 canonical mutation age",
		Side: "YES", SignalType: "r171-canonical-age", EntryPrice: .40,
		ResolveHours: 1, ExecExpr: r147InputReceiptExpr("K", at),
	}
	c := liveCandidateFromSignalIntent(liveSignalIntent{Signal: sig, At: at})
	c = s.executionShadowEnsureCandidate(c, "r171-canonical-mutation-age-test")
	if c.ShadowAttemptID == "" {
		t.Fatal("canonical fixture did not create an execution-shadow attempt")
	}
	c.ProspectiveQty = 1
	c.ProspectiveTimeInForce = liveProspectiveIOC
	s.simulateCanonicalSystemExecutionShadow(context.Background(), livePolicyMirrorWork{
		Signal: sig, SignalAt: at, DueAt: at, Candidate: c,
		ExperimentKind: canonicalSystemShadowExperimentKind, ExecutionOnly: true,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	releaseShadow()
	if err := s.stopExecutionShadowWriter(ctx); err != nil {
		t.Fatalf("stop execution-shadow writer: %v", err)
	}
	rows, err := s.store.ListExecutionShadowAttempts(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if row.Attempt.AttemptID != c.ShadowAttemptID {
			continue
		}
		for _, event := range row.Events {
			if event.Stage == "counterfactual-execution-terminal" {
				return event, calls
			}
		}
	}
	t.Fatalf("canonical terminal missing for %s: %+v", c.ShadowAttemptID, rows)
	return storage.ExecutionShadowEvent{}, calls
}

func TestR171CanonicalKalshiMutationAgeSafetyRail(t *testing.T) {
	tests := []struct {
		name       string
		ages       []float64
		wantReason string
		wantCalls  int
	}{
		{
			name:       "initial quote over five seconds",
			ages:       []float64{5.001},
			wantReason: "canonical-initial-kalshi-book-mutation-age-exceeded",
			wantCalls:  1,
		},
		{
			name:       "final quote over five seconds",
			ages:       []float64{.01, 5.001},
			wantReason: "canonical-final-kalshi-book-mutation-age-exceeded",
			wantCalls:  2,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			const ticker = "R171-AGE-RAIL"
			terminal, calls := r171CanonicalTerminal(t, ticker,
				func(call int, out *storage.Signal) {
					index := call - 1
					if index >= len(tc.ages) {
						index = len(tc.ages) - 1
					}
					age := tc.ages[index]
					out.BookQuoteAgeS = &age
				})
			if calls != tc.wantCalls {
				t.Fatalf("book calls=%d want %d", calls, tc.wantCalls)
			}
			if terminal.ShadowState != livePolicyMirrorNotObserved ||
				terminal.ShadowReason != tc.wantReason {
				t.Fatalf("terminal state=%q reason=%q", terminal.ShadowState, terminal.ShadowReason)
			}
			if got := terminal.Evidence["kalshi_mutation_age_policy"]; got !=
				canonicalKalshiMutationAgePolicy {
				t.Fatalf("mutation-age policy=%v", got)
			}
			if got := terminal.Evidence["profit_authority"]; got != false {
				t.Fatalf("stale refusal profit authority=%v", got)
			}
		})
	}
}

func TestR171CanonicalModeledVisibleFillRecordsSequenceRelation(t *testing.T) {
	tests := []struct {
		name         string
		finalSource  string
		wantRelation string
		wantSame     bool
		wantAdvanced bool
	}{
		{
			name: "identical sequence", finalSource: "kalshi_ws_full_orderbook:g1:s1:q1",
			wantRelation: "identical", wantSame: true,
		},
		{
			name: "advanced sequence", finalSource: "kalshi_ws_full_orderbook:g1:s1:q2",
			wantRelation: "advanced", wantAdvanced: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			const ticker = "R171-SEQUENCE"
			terminal, calls := r171CanonicalTerminal(t, ticker,
				func(call int, out *storage.Signal) {
					if call == 2 {
						out.BookSource = tc.finalSource
					}
				})
			if calls != 2 {
				t.Fatalf("book calls=%d want 2", calls)
			}
			if terminal.ShadowState != livePolicyMirrorModeledFill || terminal.ShadowReason != "" {
				t.Fatalf("terminal state=%q reason=%q", terminal.ShadowState, terminal.ShadowReason)
			}
			if got := terminal.Evidence["fill_evidence_class"]; got != "modeled_visible_fill" {
				t.Fatalf("fill evidence class=%v", got)
			}
			if got := terminal.Evidence["exchange_fill_observed"]; got != false {
				t.Fatalf("exchange-fill authority=%v", got)
			}
			if got := terminal.Evidence["profit_authority"]; got != false {
				t.Fatalf("profit authority=%v", got)
			}
			if got := terminal.Evidence["kalshi_initial_final_sequence_relation"]; got !=
				tc.wantRelation {
				t.Fatalf("sequence relation=%v want %q", got, tc.wantRelation)
			}
			if got := terminal.Evidence["kalshi_initial_final_sequence_identical"]; got !=
				tc.wantSame {
				t.Fatalf("sequence identical=%v want %t", got, tc.wantSame)
			}
			if got := terminal.Evidence["kalshi_initial_final_sequence_advanced"]; got !=
				tc.wantAdvanced {
				t.Fatalf("sequence advanced=%v want %t", got, tc.wantAdvanced)
			}
		})
	}
}
