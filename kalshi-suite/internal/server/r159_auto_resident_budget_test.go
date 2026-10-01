package server

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
)

func TestR159AutoEarlyBudgetFailsClosedOnColdHeldComboWithoutPublicREST(t *testing.T) {
	s, venue := newR153SemanticServer(t)
	portfolioValue := int64(100)
	snapshot := r154KalshiAdmissionSnapshot{
		observedAt: time.Now(),
		balance: kalshi.Balance{
			Balance:        10_000,
			PortfolioValue: &portfolioValue,
		},
		positions: []kalshi.MarketPosition{{
			Ticker:         "KXMVE-R159-COLD-BUDGET",
			Position:       1,
			MarketExposure: 40,
		}},
	}
	ctx := r154ContextWithKalshiAdmissionSnapshot(context.Background(), snapshot)
	ctx = r159KalshiResidentAdmissionContext(ctx)
	candidate := liveMirrorCandidate{
		Platform: "kalshi",
		Ticker:   r153HOUOver35,
		Title:    "R159 budget candidate",
		Side:     "YES",
		Source:   "auto-cons-kflow",
		At:       time.Now(),
	}
	before := r159SemanticPublicCallCount(venue)

	reason := s.liveVenueBudgetCheckFor(ctx, "kalshi", .41, &candidate)
	if !strings.Contains(reason, "resident held combined-market leg metadata unavailable") {
		t.Fatalf("cold held combo budget reason = %q, want resident-only fail-closed reason", reason)
	}
	if after := r159SemanticPublicCallCount(venue); after != before {
		t.Fatalf("early AUTO budget made %d public calls, want zero", after-before)
	}
}

func TestR159AutoAdmissionMarksResidentBeforeEveryEarlyBudget(t *testing.T) {
	raw, err := os.ReadFile("server.go")
	if err != nil {
		t.Fatal(err)
	}
	handler := string(raw)
	start := strings.Index(handler, "func (s *Server) handleLivePlace")
	end := strings.Index(handler[start+1:], "\nfunc ")
	if start < 0 || end < 0 {
		t.Fatal("handleLivePlace source block unavailable")
	}
	handler = handler[start : start+1+end]
	decodeAt := strings.Index(handler, "json.NewDecoder(r.Body).Decode(&body)")
	firstResidentAt := strings.Index(handler, "ctx = r159KalshiResidentAdmissionContext(ctx)")
	firstAdmissionAt := strings.Index(handler, "s.liveContractLimitReason")
	if decodeAt < 0 || firstResidentAt <= decodeAt || firstAdmissionAt <= firstResidentAt {
		t.Fatalf("AUTO resident marker is not installed at the start of admission: decode=%d resident=%d admission=%d",
			decodeAt, firstResidentAt, firstAdmissionAt)
	}
	snapshotAt := strings.Index(handler, "ctx, _, accountErr = s.r154KalshiAdmissionSnapshotContext(ctx)")
	residentAfterSnapshot := -1
	if snapshotAt >= 0 {
		if relative := strings.Index(handler[snapshotAt:], "ctx = r159KalshiResidentAdmissionContext(ctx)"); relative >= 0 {
			residentAfterSnapshot = snapshotAt + relative
		}
	}
	budgetAt := strings.Index(handler, `s.liveVenueBudgetCheckFor(ctx, "kalshi"`)
	if snapshotAt < 0 || residentAfterSnapshot <= snapshotAt || budgetAt <= residentAfterSnapshot {
		t.Fatalf("AUTO resident admission ordering changed: snapshot=%d resident=%d budget=%d",
			snapshotAt, residentAfterSnapshot, budgetAt)
	}
	rebindAt := strings.Index(handler, "ctx = recheck.Context")
	residentAfterRebind := -1
	if rebindAt >= 0 {
		if relative := strings.Index(handler[rebindAt:], "ctx = r159KalshiResidentAdmissionContext(ctx)"); relative >= 0 {
			residentAfterRebind = rebindAt + relative
		}
	}
	if rebindAt < 0 || residentAfterRebind <= rebindAt {
		t.Fatalf("AUTO refreshed context is not re-marked resident-only: rebind=%d resident=%d",
			rebindAt, residentAfterRebind)
	}

	refreshRaw, err := os.ReadFile("r156_live_admission_retry.go")
	if err != nil {
		t.Fatal(err)
	}
	refresh := string(refreshRaw)
	refreshSnapshotAt := strings.Index(refresh, "nextCtx, _, err := s.r154KalshiAdmissionSnapshotContext(ctx)")
	refreshResidentAt := strings.Index(refresh, "nextCtx = r159KalshiResidentAdmissionContext(nextCtx)")
	refreshBudgetAt := strings.Index(refresh, `s.liveVenueBudgetCheckFor(nextCtx, "kalshi"`)
	if refreshSnapshotAt < 0 || refreshResidentAt <= refreshSnapshotAt ||
		refreshBudgetAt <= refreshResidentAt {
		t.Fatalf("refreshed AUTO resident admission ordering changed: snapshot=%d resident=%d budget=%d",
			refreshSnapshotAt, refreshResidentAt, refreshBudgetAt)
	}
}
