package server

import (
	"strings"
	"testing"
	"time"
)

func greenLiveAutoGoInput() liveAutoGoInput {
	return liveAutoGoInput{
		Now: time.Now(), KillClear: true, KillHaltReady: true, FeedsOK: true,
		KalshiAuthOK: true, KalshiBooksOK: true, LatchesOK: true, LedgerOK: true,
		ProducersOK: true, DBKnown: true, DBLastMS: 1, DBP95MS: 4837,
		DBAge: time.Second, DBThresholdMS: 500, KalshiAccountOK: true,
		RiskReadable: true, SafetyPauseClear: true, DailyLossRailOK: true,
		LiveLossOK: true, AuthorityReady: true, Authority: []string{"kalshi|spotlag|YES|taker"},
	}
}

func TestR160DisarmedDailyLossSafetyPauseBlocksArmWithCanonicalParity(t *testing.T) {
	s := &Server{}
	s.setLiveAutoSafetyPause("z-account", "account reconciliation pending")
	s.setLiveAutoSafetyPause("daily-loss", "durable fee-net result -53.83 USD today breaches the 49.01 USD daily-loss rail")

	pauseReason, dailyLossReason := s.snapshotLiveAutoSafetyPauses()
	wantPause := "daily-loss: durable fee-net result -53.83 USD today breaches the 49.01 USD daily-loss rail; z-account: account reconciliation pending"
	if pauseReason != wantPause {
		t.Fatalf("canonical pause reason=%q, want %q", pauseReason, wantPause)
	}
	if !strings.Contains(dailyLossReason, "-53.83") || !strings.Contains(dailyLossReason, "49.01") {
		t.Fatalf("daily-loss breach detail was not surfaced exactly: %q", dailyLossReason)
	}

	in := greenLiveAutoGoInput()
	in.Armed, in.Auto = false, false
	in.SafetyPauseClear, in.SafetyPauseDetail = pauseReason == "", pauseReason
	in.DailyLossRailOK, in.DailyLossRailDetail = dailyLossReason == "", dailyLossReason
	got := evaluateLiveAutoGo(in)
	if got.SafeToArm || got.ReadyToEnableAuto || got.State != "PAUSED" {
		t.Fatalf("disarmed ARM refusal looked safe instead of PAUSED: %+v", got)
	}
	if check := got.Checks["automatic_safety_pause"]; check.OK || check.Detail != wantPause {
		t.Fatalf("canonical automatic pause check lost parity: %+v", check)
	}
	if check := got.Checks["daily_loss_rail"]; check.OK || check.Detail != dailyLossReason {
		t.Fatalf("daily-loss rail check lost breach status: %+v", check)
	}
	blockers := strings.Join(got.Blockers, " | ")
	if !strings.Contains(blockers, wantPause) || !strings.Contains(blockers, dailyLossReason) {
		t.Fatalf("operator blockers omitted active ARM refusal: %q", blockers)
	}
	if check := got.Checks["daily_loss_ledger"]; !check.OK {
		t.Fatalf("a breached rail was confused with ledger continuity: %+v", check)
	}
}

func TestR160FreshProcessNeedsPreArmDailyLossProof(t *testing.T) {
	s := &Server{}
	ok, detail := s.liveAutoGoLossReadiness([]string{"kalshi"})
	if ok || !strings.Contains(detail, "not current in this process") ||
		!strings.Contains(detail, "ARM performs the authenticated fresh check") ||
		!strings.Contains(detail, "kalshi live daily-loss ledger is not initialized") {
		t.Fatalf("fresh process advertised pre-ARM loss readiness: ok=%t detail=%q", ok, detail)
	}

	in := greenLiveAutoGoInput()
	in.LiveLossOK, in.LiveLossDetail = ok, detail
	got := evaluateLiveAutoGo(in)
	if got.SafeToArm || got.State != "NOT_READY" {
		t.Fatalf("fresh process daily-loss gap looked safe to arm: %+v", got)
	}
	if check := got.Checks["daily_loss_ledger"]; check.OK || check.Detail != detail {
		t.Fatalf("fresh pre-ARM blocker was not surfaced: %+v", check)
	}
}

func TestR160DisarmedFreshInitializedLossTruthHasNoContinuityBlocker(t *testing.T) {
	s := &Server{
		liveLossHasBase: true,
		liveLossTruthV:  map[string]bool{"kalshi": true, "polyus": true},
		liveLossTruthAtV: map[string]time.Time{
			"kalshi": time.Now(),
			"polyus": time.Now(),
		},
		liveLossErrV: map[string]string{},
	}
	ok, detail := s.liveAutoGoLossReadiness([]string{"kalshi", "polyus"})
	if !ok || detail != "authenticated daily-loss ledger ready" {
		t.Fatalf("fresh initialized venue truth remained blocked: ok=%t detail=%q", ok, detail)
	}

	in := greenLiveAutoGoInput()
	in.LiveLossOK, in.LiveLossDetail = ok, detail
	got := evaluateLiveAutoGo(in)
	if !got.SafeToArm || got.State != "SAFE_TO_ARM" {
		t.Fatalf("fresh initialized loss truth created a false blocker: %+v", got)
	}
}

func TestLiveAutoGoSurfacesDailyLossLedgerDiscontinuity(t *testing.T) {
	in := greenLiveAutoGoInput()
	in.Armed, in.Auto = true, true
	in.LiveLossOK = false
	in.LiveLossDetail = "settled market disappeared without authenticated receipt"
	got := evaluateLiveAutoGo(in)
	if got.State != "PAUSED" || got.SafeToArm || !strings.Contains(strings.Join(got.Blockers, " "), "daily-loss") {
		t.Fatalf("daily-loss discontinuity looked RUNNING: %+v", got)
	}
}

func TestLiveAutoGoUsesCurrentDBNotStaleHourP95(t *testing.T) {
	in := greenLiveAutoGoInput()
	got := evaluateLiveAutoGo(in)
	if !got.SafeToArm || got.State != "SAFE_TO_ARM" {
		t.Fatalf("current 1ms DB should be safe despite historical p95: %+v", got)
	}
	if got.QueuedCandidates != 0 || got.OpportunityState != "waiting_for_signal" {
		t.Fatalf("a quiet market must not be a readiness blocker: %+v", got)
	}
	if len(got.Warnings) == 0 || !strings.Contains(got.Warnings[0], "Earlier DB stalls") {
		t.Fatalf("historical p95 should remain visible as a warning: %+v", got.Warnings)
	}
}

func TestR163LiveAutoGoLabelsExecutableObservationsAsActivityNotProofOrQueuedBets(t *testing.T) {
	in := greenLiveAutoGoInput()
	in.FreshSignalStreams = 28
	got := evaluateLiveAutoGo(in)
	if got.RecentExecutableObservations != 28 || got.FreshSignalStreams != 28 ||
		got.ExecutableObservationWindowMS != liveMirrorTTL.Milliseconds() {
		t.Fatalf("executable-observation compatibility fields disagree: %+v", got)
	}
	if got.QueuedCandidates != 0 || got.OpportunityState != "waiting_for_signal" {
		t.Fatalf("activity receipts became queued bets: %+v", got)
	}
	for _, want := range []string{"distinct executable strategy observation", "detector activity only",
		"not statistical proof", "does not mean 28 bets"} {
		if !strings.Contains(got.OpportunityExplanation, want) {
			t.Fatalf("operator explanation omitted %q: %q", want, got.OpportunityExplanation)
		}
	}
}

func TestR163CanaryAuthorityNeedsNoSpecialDailyAllowance(t *testing.T) {
	s := testServer(t)
	cfg := *s.cfg()
	cfg.Risk.LiveProspectiveAllocation = true
	cfg.Risk.LiveSystemKalshi = true
	cfg.Risk.LiveSystemAllowlist = "kalshi|spotlag|YES|taker,kalshi|kalshi-flow|NO|taker"
	cfg.Risk.LiveSystemCanaryAllowlist = cfg.Risk.LiveSystemAllowlist
	s.cfgP.Store(&cfg)

	ready, routes, venues, detail := s.liveAutoGoAuthority(time.Now())
	if !ready || len(routes) != 2 || len(venues) != 1 || venues[0] != "kalshi" {
		t.Fatalf("canary authority still required a special daily allowance: ready=%t routes=%v venues=%v detail=%q",
			ready, routes, venues, detail)
	}
}

func TestLiveAutoGoBlocksCurrentSlowDBAndKillSwitch(t *testing.T) {
	in := greenLiveAutoGoInput()
	in.DBLastMS = 900
	got := evaluateLiveAutoGo(in)
	if got.SafeToArm || !strings.Contains(strings.Join(got.Blockers, " "), "database") {
		t.Fatalf("current slow DB did not block: %+v", got)
	}
	in = greenLiveAutoGoInput()
	in.KillClear = false
	in.KillDetail = "TRIPPED: fixture"
	got = evaluateLiveAutoGo(in)
	if got.SafeToArm || !strings.Contains(strings.Join(got.Blockers, " "), "Kill switch") {
		t.Fatalf("kill switch did not block: %+v", got)
	}
}

func TestLiveAutoGoDashboardIsMandatoryAndLiveUpdating(t *testing.T) {
	for _, want := range []string{"'live-go'", "id=\"liveGoW\"", "renderLiveAutoGo(d&&d.live_auto_go)", "LIVE AUTO SAFETY"} {
		if !strings.Contains(dashboardHTML, want) {
			t.Fatalf("dashboard missing %q", want)
		}
	}
}

func TestLiveAutoGoPendingRiskBlocksWhileProofIsInFlight(t *testing.T) {
	s := &Server{liveGoRiskBusy: true}
	count, readable, detail := s.liveAutoGoPendingRisk(time.Now())
	if readable || count != 0 || !strings.Contains(detail, "still running") {
		t.Fatalf("in-flight risk proof briefly looked safe: count=%d readable=%t detail=%q", count, readable, detail)
	}
}

func TestLiveAutoGoStagedRecoveryPauseCannotLookSafe(t *testing.T) {
	readable, detail := liveAutoGoMergeStagedRecovery(true, "readable; 0 active reservation(s)",
		"staged recovery paused for a transient durable-ledger read; retrying automatically")
	if readable || !strings.Contains(detail, "retrying automatically") {
		t.Fatalf("staged recovery pause looked safe: readable=%t detail=%q", readable, detail)
	}
	in := greenLiveAutoGoInput()
	in.RiskReadable, in.RiskDetail = readable, detail
	got := evaluateLiveAutoGo(in)
	if got.SafeToArm || !strings.Contains(strings.Join(got.Blockers, " "), "pending-risk") {
		t.Fatalf("staged recovery pause did not block safety card: %+v", got)
	}
}
