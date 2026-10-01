package server

// R156 bounded Kalshi admission refresh.
//
// A private fill/order update can invalidate the immutable account snapshot carried by the next
// serialized LIVE handler. That is a normal account-epoch race, not evidence that the candidate is
// unsafe. The handler may refresh exactly once before any durable reservation exists, but the new
// account view must pass every account/risk decision again. This file owns that complete recheck;
// the reservation loop and the final consume/write boundary remain in handleLivePlace.

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

type r156KalshiAdmissionRecheckRequest struct {
	Auto            bool
	TriggerUnixMS   int64
	Candidate       liveMirrorCandidate
	BudgetCandidate liveMirrorCandidate
	CostUSD         float64
	Count           int
	WireUnit        float64
	FeeUSD          float64

	ProspectiveAllocation bool
	Canary                bool
	ProofMean             float64
	ProofLower            float64
	ProofFeePerContract   float64
	Quote                 liveMirrorQuote

	Sealed bool
	Intent storage.ResearchPromotionIntent
}

type r156KalshiAdmissionRecheckResult struct {
	Context    context.Context
	HTTPStatus int
	Reason     string
	SnapshotMS int64
}

func r156KalshiAdmissionFreshnessReason(ctx context.Context, auto bool, triggerUnixMS int64,
	now time.Time) string {
	if ctx == nil {
		return "LIVE request context is unavailable"
	}
	if err := ctx.Err(); err != nil {
		return "LIVE request deadline elapsed before account admission could be renewed"
	}
	if !auto {
		return ""
	}
	if _, why := r159LiveAutoSignalAt(true, triggerUnixMS, now); why != "" {
		return why
	}
	return ""
}

func r156KalshiAdmissionFailure(ctx context.Context, status int, reason string,
	snapshotMS int64) r156KalshiAdmissionRecheckResult {
	return r156KalshiAdmissionRecheckResult{
		Context: ctx, HTTPStatus: status, Reason: reason, SnapshotMS: snapshotMS,
	}
}

// r156RefreshAndRecheckKalshiAdmission preserves the caller's context/deadline and shadows only
// its immutable admission-snapshot value. The normal handler path never calls this method. A
// caller invoking it must still rerun the final event-header/pending-ticker checks and reservation.
func (s *Server) r156RefreshAndRecheckKalshiAdmission(ctx context.Context,
	in r156KalshiAdmissionRecheckRequest) r156KalshiAdmissionRecheckResult {
	if why := r156KalshiAdmissionFreshnessReason(ctx, in.Auto, in.TriggerUnixMS, time.Now()); why != "" {
		return r156KalshiAdmissionFailure(ctx, http.StatusConflict, why, 0)
	}

	snapshotStarted := time.Now()
	nextCtx, _, err := s.r154KalshiAdmissionSnapshotContext(ctx)
	snapshotMS := time.Since(snapshotStarted).Milliseconds()
	if err != nil {
		return r156KalshiAdmissionFailure(nextCtx, http.StatusServiceUnavailable,
			"LIVE authenticated account refresh is unavailable: "+err.Error(), snapshotMS)
	}
	if in.Auto {
		// SnapshotContext may return a newly wrapped context. Restore the urgent AUTO marker before
		// any refreshed account, budget, cluster, horizon, or identity check can recover via REST.
		nextCtx = r159KalshiResidentAdmissionContext(nextCtx)
	}
	if why := r156KalshiAdmissionFreshnessReason(nextCtx, in.Auto, in.TriggerUnixMS, time.Now()); why != "" {
		return r156KalshiAdmissionFailure(nextCtx, http.StatusConflict, why, snapshotMS)
	}

	if in.Auto {
		if why := s.liveMirrorAccountGuard(nextCtx, in.Candidate); why != "" {
			return r156KalshiAdmissionFailure(nextCtx, http.StatusConflict,
				"AUTO live account guard after account refresh: "+why, snapshotMS)
		}
		if why := r156KalshiAdmissionFreshnessReason(nextCtx, true, in.TriggerUnixMS, time.Now()); why != "" {
			return r156KalshiAdmissionFailure(nextCtx, http.StatusConflict, why, snapshotMS)
		}
	}

	if msg := s.liveVenueBudgetCheckFor(nextCtx, "kalshi", in.CostUSD, &in.BudgetCandidate); msg != "" {
		return r156KalshiAdmissionFailure(nextCtx, http.StatusForbidden, msg+" (after account refresh)", snapshotMS)
	}
	if why := r156KalshiAdmissionFreshnessReason(nextCtx, in.Auto, in.TriggerUnixMS, time.Now()); why != "" {
		return r156KalshiAdmissionFailure(nextCtx, http.StatusConflict, why, snapshotMS)
	}
	if msg := s.liveRiskCheck(in.CostUSD, "kalshi"); msg != "" {
		return r156KalshiAdmissionFailure(nextCtx, http.StatusForbidden, msg+" (after account refresh)", snapshotMS)
	}
	if why := r156KalshiAdmissionFreshnessReason(nextCtx, in.Auto, in.TriggerUnixMS, time.Now()); why != "" {
		return r156KalshiAdmissionFailure(nextCtx, http.StatusConflict, why, snapshotMS)
	}

	if in.Canary {
		if in.Count != 1 || in.WireUnit <= 0 || in.WireUnit >= 1 ||
			in.FeeUSD < 0 || math.IsNaN(in.FeeUSD) || math.IsInf(in.FeeUSD, 0) {
			return r156KalshiAdmissionFailure(nextCtx, http.StatusServiceUnavailable,
				"one-contract canary final wire receipt became invalid after account refresh", snapshotMS)
		}
		wireQuote := in.Quote
		wireQuote.Price = in.WireUnit
		plan, planWhy := s.liveOneContractCanaryReproof(nextCtx, in.Candidate, wireQuote)
		if planWhy != "" || !plan.valid() || !plan.Canary {
			return r156KalshiAdmissionFailure(nextCtx, http.StatusConflict,
				"one-contract canary proof refused after account refresh: "+planWhy, snapshotMS)
		}
		if why := r156KalshiAdmissionFreshnessReason(nextCtx, in.Auto,
			in.TriggerUnixMS, time.Now()); why != "" {
			return r156KalshiAdmissionFailure(nextCtx, http.StatusConflict, why, snapshotMS)
		}
	} else if in.ProspectiveAllocation {
		if in.Count < 1 || in.WireUnit <= 0 || in.WireUnit >= 1 ||
			in.FeeUSD < 0 || math.IsNaN(in.FeeUSD) || math.IsInf(in.FeeUSD, 0) {
			return r156KalshiAdmissionFailure(nextCtx, http.StatusServiceUnavailable,
				"prospective allocation final wire receipt became invalid after account refresh", snapshotMS)
		}
		feePerContract := in.FeeUSD / float64(in.Count)
		wireMean, wireLower := liveAllocationWireAdjusted(in.ProofMean, in.ProofLower,
			in.Quote.Price, in.ProofFeePerContract, in.WireUnit, feePerContract)
		if wireLower+1e-12 < s.liveAllocationEdgeFloor() {
			return r156KalshiAdmissionFailure(nextCtx, http.StatusConflict,
				"prospective allocation final wire cost erased its fee-net lower bound after account refresh", snapshotMS)
		}
		bank, bankSrc := s.liveSizingBankroll(nextCtx, "kalshi")
		maxCount, _, _, _, sizeWhy := s.liveProspectiveAllocationSize("kalshi", bank, in.WireUnit,
			wireMean, wireLower, feePerContract, in.Quote.Depth, in.Quote.MinQty, in.Quote.MinQtyKnown)
		if sizeWhy != "" {
			return r156KalshiAdmissionFailure(nextCtx, http.StatusForbidden,
				"prospective allocation cannot size after account refresh: "+sizeWhy+":"+bankSrc, snapshotMS)
		}
		if float64(in.Count) > maxCount+1e-9 {
			return r156KalshiAdmissionFailure(nextCtx, http.StatusForbidden,
				fmt.Sprintf("prospective allocation quantity %d exceeds refreshed Adaptive Allocation Model limit %.0f",
					in.Count, maxCount), snapshotMS)
		}
		if why := r156KalshiAdmissionFreshnessReason(nextCtx, in.Auto, in.TriggerUnixMS, time.Now()); why != "" {
			return r156KalshiAdmissionFailure(nextCtx, http.StatusConflict, why, snapshotMS)
		}
	}

	if in.Auto {
		if why := s.liveMirrorClusterGuard(nextCtx, in.Candidate, in.CostUSD); why != "" {
			return r156KalshiAdmissionFailure(nextCtx, http.StatusForbidden,
				"AUTO live cluster guard after account refresh: "+why, snapshotMS)
		}
		if !s.paperEntryHorizonNow(nextCtx, "kalshi", in.Candidate.Ticker, "") {
			return r156KalshiAdmissionFailure(nextCtx, http.StatusForbidden,
				"AUTO live market left the funded entry horizon during account refresh", snapshotMS)
		}
		if in.Sealed {
			if why := s.liveAutoPromotionCurrentIdentityReason(nextCtx, in.Intent); why != "" {
				return r156KalshiAdmissionFailure(nextCtx, http.StatusConflict,
					"AUTO live sealed identity changed during account refresh: "+why, snapshotMS)
			}
		}
		if why := r156KalshiAdmissionFreshnessReason(nextCtx, true, in.TriggerUnixMS, time.Now()); why != "" {
			return r156KalshiAdmissionFailure(nextCtx, http.StatusConflict, why, snapshotMS)
		}
	}

	return r156KalshiAdmissionRecheckResult{
		Context: nextCtx, HTTPStatus: http.StatusOK, SnapshotMS: snapshotMS,
	}
}

// r156KalshiAdmissionRetryable proves the retry is both typed and pre-reservation. The riskID
// argument makes future call-site changes fail closed if an error is ever returned with a durable
// row already in hand.
func r156KalshiAdmissionRetryable(err error, refreshUsed bool, riskID string) bool {
	return !refreshUsed && strings.TrimSpace(riskID) == "" &&
		errors.Is(err, errR156KalshiAdmissionSnapshotNotCurrent)
}
