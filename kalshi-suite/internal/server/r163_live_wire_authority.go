package server

import "context"

const r163StagedMoneyPolicyNoSendSource = "staged-wire-money-policy-generation"

const (
	r163StagedWireAuthorityNoSendSource = "staged-wire-authority-no-send"
	r163StagedWireBookNoSendSource      = "staged-wire-book-no-send"
	r163ComboWireNoSendSource           = "combo-wire-authority-no-send"
)

type r163StagedMoneyPolicyContextKey struct{}

type r163StagedMoneyPolicyContext struct {
	Bound      bool
	Generation uint64
}

func r163WithStagedMoneyPolicy(ctx context.Context, bound bool, generation uint64) context.Context {
	return context.WithValue(ctx, r163StagedMoneyPolicyContextKey{},
		r163StagedMoneyPolicyContext{Bound: bound, Generation: generation})
}

func r163StagedMoneyPolicyFromContext(ctx context.Context) (uint64, bool) {
	v, ok := ctx.Value(r163StagedMoneyPolicyContextKey{}).(r163StagedMoneyPolicyContext)
	return v.Generation, ok && v.Bound
}

// r163LiveWireSystemAuthorityReason is the last current-config authority read for a normal System
// order. New-ML has its own destination switch and proof contract; every other AUTO System order
// must still be selected by the current venue switch and exact allowlist identity.
func (s *Server) r163LiveWireSystemAuthorityReason(auto, newML bool,
	c liveMirrorCandidate, route string) string {
	if !auto {
		return ""
	}
	if newML {
		if !s.liveNewMLVenueEnabled(c.Platform) {
			return "live-new-ml-disabled-for-destination-venue"
		}
		return ""
	}
	return s.liveSystemPostProofReason(c, route)
}

// r163MoneyInertSettings is deliberately the smaller list. Settings defaults to money-affecting:
// a future field cannot silently bypass the generation/fence merely because somebody forgot to add
// it to a cash-policy allowlist. Every exception below is operational/notification-only, boot-only,
// or the legacy Paper flat stake that funded LIVE no longer consumes.
var r163MoneyInertSettings = map[string]struct{}{
	"telegram_bot_token":           {},
	"telegram_chat_id":             {},
	"telegram_heartbeat":           {},
	"messenger_mode":               {},
	"briefing_dir":                 {},
	"briefing_enabled":             {},
	"briefing_every_minutes":       {},
	"kalshi_key_file":              {},
	"kalshi_key_id":                {},
	"polyus_key_file":              {},
	"polyus_key_id":                {},
	"latency_warn_rest_ms":         {},
	"latency_warn_ws_age_s":        {},
	"latency_warn_db_p95_ms":       {},
	"reset_on_start":               {},
	"stake_usd":                    {},
	"live_system_shadow_allowlist": {},
}

// r163SettingsPublishesLiveMoneyPolicy conservatively treats every non-inert Settings field as
// capable of changing real-money eligibility, sizing, route, price, horizon, or destination.
// Publication is linearized with final venue writes. New settings fail safe by default.
func r163SettingsPublishesLiveMoneyPolicy(raw map[string]any) bool {
	for key := range raw {
		if _, inert := r163MoneyInertSettings[key]; !inert {
			return true
		}
	}
	return false
}

func r163LiveComboWireReason(auto, comboEnabled bool,
	admittedIntent, currentIntent, admittedPolicy, currentPolicy uint64) string {
	if auto {
		if !comboEnabled {
			return "AUTO combo belt was disabled before RFQ acceptance"
		}
		if admittedIntent != currentIntent {
			return liveIntentGenerationChangedReason
		}
	}
	return r163LiveMoneyPolicyGenerationReason(admittedPolicy, currentPolicy)
}

func r163LiveMoneyPolicyGenerationReason(admitted, current uint64) string {
	if admitted == current {
		return ""
	}
	return "live-money-policy-changed-after-handler-admission"
}

func (s *Server) r163RejectKalshiWireMoneyPolicy(ctx context.Context, riskID string,
	admitted, current uint64) (map[string]any, error) {
	evidence := map[string]any{
		"reason":              r163LiveMoneyPolicyGenerationReason(admitted, current),
		"admitted_generation": admitted,
		"current_generation":  current,
	}
	if err := s.r154RejectKalshiNoSendRisk(ctx, riskID,
		"wire-money-policy-generation",
		"LIVE authority or risk policy changed; venue was not called",
		evidence); err != nil {
		return map[string]any{
			"error": "changed LIVE money policy refused the venue call, but its durable no-send receipt failed: " +
				err.Error(),
			"venue_attempted":     false,
			"risk_reservation_id": riskID,
			"execution_source":    "wire-money-policy-generation",
		}, err
	}
	return map[string]any{
		"error":               "LIVE authority or risk policy changed after handler admission; no order sent",
		"venue_attempted":     false,
		"risk_reservation_id": riskID,
		"execution_source":    "wire-money-policy-generation",
	}, nil
}

func (s *Server) r163RejectPolyUSWireMoneyPolicy(ctx context.Context, riskID string,
	admitted, current uint64, remaining, price float64) (map[string]any, error) {
	evidence := map[string]any{
		"reason":              r163LiveMoneyPolicyGenerationReason(admitted, current),
		"admitted_generation": admitted,
		"current_generation":  current,
	}
	if err := s.r148RiskCleanRejected(context.WithoutCancel(ctx), riskID, "entry",
		"polyus-wire-money-policy-generation",
		"LIVE authority or risk policy changed; venue was not called",
		evidence); err != nil {
		return map[string]any{
			"error": "changed LIVE money policy refused the PolyUS venue call, but its durable no-send receipt failed: " +
				err.Error(),
			"venue_attempted":     false,
			"risk_reservation_id": riskID,
			"execution_source":    "polyus-wire-money-policy-generation",
			"filled_qty":          0.0,
			"remaining_qty":       remaining,
			"average_fill_price":  price,
			"fee_total":           0.0,
			"fee_known":           true,
		}, err
	}
	return map[string]any{
		"error":               "LIVE authority or risk policy changed after handler admission; no PolyUS order sent",
		"venue_attempted":     false,
		"risk_reservation_id": riskID,
		"execution_source":    "polyus-wire-money-policy-generation",
		"filled_qty":          0.0,
		"remaining_qty":       remaining,
		"average_fill_price":  price,
		"fee_total":           0.0,
		"fee_known":           true,
	}, nil
}

// r163RejectPolyUSWireSystemAuthority is the PolyUS twin. It uses the same exact current authority
// reason and closes SubmitStarted before returning a known no-send response.
func (s *Server) r163RejectPolyUSWireSystemAuthority(ctx context.Context, riskID string,
	c liveMirrorCandidate, route, why string, remaining, price float64) (map[string]any, error) {
	evidence := map[string]any{
		"reason":          why,
		"system_identity": liveSystemIdentityKey(c, route),
		"requested_route": route,
		"platform":        c.Platform,
	}
	if err := s.r148RiskCleanRejected(context.WithoutCancel(ctx), riskID, "entry",
		"polyus-wire-system-authority",
		"current LIVE System authority changed; venue was not called",
		evidence); err != nil {
		return map[string]any{
			"error": "current LIVE System authority refused the PolyUS venue call, but its durable no-send receipt failed: " +
				err.Error(),
			"venue_attempted":     false,
			"risk_reservation_id": riskID,
			"execution_source":    "polyus-wire-system-authority",
			"filled_qty":          0.0,
			"remaining_qty":       remaining,
			"average_fill_price":  price,
			"fee_total":           0.0,
			"fee_known":           true,
		}, err
	}
	return map[string]any{
		"error":               "AUTO live current System authority changed before PolyUS submit: " + why,
		"venue_attempted":     false,
		"risk_reservation_id": riskID,
		"execution_source":    "polyus-wire-system-authority",
		"filled_qty":          0.0,
		"remaining_qty":       remaining,
		"average_fill_price":  price,
		"fee_total":           0.0,
		"fee_known":           true,
	}, nil
}

// r163RejectKalshiWireSystemAuthority closes a durable reservation after the final current-config
// read proves that the operator removed its normal System authority. The response deliberately
// carries venue_attempted=false so diagnostics cannot confuse this known no-send with an IOC
// zero-fill or an ambiguous venue mutation.
func (s *Server) r163RejectKalshiWireSystemAuthority(ctx context.Context, riskID string,
	c liveMirrorCandidate, route, why string) (map[string]any, error) {
	evidence := map[string]any{
		"reason":          why,
		"system_identity": liveSystemIdentityKey(c, route),
		"requested_route": route,
		"platform":        c.Platform,
	}
	if err := s.r154RejectKalshiNoSendRisk(ctx, riskID,
		"wire-system-authority",
		"current LIVE System authority changed; venue was not called",
		evidence); err != nil {
		return map[string]any{
			"error": "current LIVE System authority refused the venue call, but its durable no-send receipt failed: " +
				err.Error(),
			"venue_attempted":     false,
			"risk_reservation_id": riskID,
			"execution_source":    "wire-system-authority",
		}, err
	}
	return map[string]any{
		"error":               "AUTO live current System authority changed before submit: " + why,
		"venue_attempted":     false,
		"risk_reservation_id": riskID,
		"execution_source":    "wire-system-authority",
	}, nil
}
