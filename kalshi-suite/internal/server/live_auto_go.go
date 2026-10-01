package server

// live_auto_go.go owns the operator-facing answer to one narrow question: "is the suite safe to
// ARM for LIVE AUTO right now?"  It is deliberately separate from opportunity selection. A quiet
// market is safe and says "waiting for a signal"; it is not painted as a broken runtime. Every
// eventual order still goes through the final signal, book, fee, depth, account, sizing and risk
// checks in liveautomirror.go and the venue handlers.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	liveGoDBSampleMaxAge = 90 * time.Second
	liveGoRiskCacheFor   = 5 * time.Second
)

type liveAutoGoCheck struct {
	OK     bool   `json:"ok"`
	Detail string `json:"detail"`
}

type liveAutoGoView struct {
	State             string                     `json:"state"`
	Headline          string                     `json:"headline"`
	SafeToArm         bool                       `json:"safe_to_arm"`
	ReadyToEnableAuto bool                       `json:"ready_to_enable_auto"`
	Armed             bool                       `json:"armed"`
	Auto              bool                       `json:"auto"`
	Blockers          []string                   `json:"blockers"`
	Warnings          []string                   `json:"warnings,omitempty"`
	Checks            map[string]liveAutoGoCheck `json:"checks"`
	Authority         []string                   `json:"authority,omitempty"`
	// RecentExecutableObservations is activity evidence only: distinct exact
	// route+ticker+side+signal-contract executable receipts refreshed inside liveMirrorTTL. It is
	// not statistical proof and is not a count of queued candidates, venue orders, or fills. Keep
	// FreshSignalStreams as a deprecated compatibility alias until existing API readers migrate.
	RecentExecutableObservations  int    `json:"recent_executable_observations"`
	ExecutableObservationWindowMS int64  `json:"executable_observation_window_ms"`
	FreshSignalStreams            int    `json:"fresh_signal_streams"`
	QueuedCandidates              int    `json:"queued_candidates"`
	OpportunityState              string `json:"opportunity_state"`
	OpportunityExplanation        string `json:"opportunity_explanation"`
	GeneratedAt                   string `json:"generated_at"`
}

type liveAutoGoInput struct {
	Now                 time.Time
	Armed               bool
	Auto                bool
	KillClear           bool
	KillDetail          string
	KillHaltReady       bool
	FeedsOK             bool
	FeedsDetail         string
	KalshiAuthOK        bool
	KalshiAuthDetail    string
	PolyUSRequired      bool
	PolyUSAuthOK        bool
	PolyUSAuthDetail    string
	KalshiBooksOK       bool
	KalshiBooksDetail   string
	KalshiBooksWarning  string
	PolyUSBooksOK       bool
	PolyUSBooksDetail   string
	LatchesOK           bool
	LedgerOK            bool
	LedgerDetail        string
	ProducersOK         bool
	ProducersDetail     string
	DBKnown             bool
	DBLastMS            float64
	DBP95MS             float64
	DBAge               time.Duration
	DBThresholdMS       float64
	KalshiAccountOK     bool
	KalshiAccountDetail string
	PolyUSAccountOK     bool
	PolyUSAccountDetail string
	RiskReadable        bool
	PendingRisk         int
	RiskDetail          string
	SafetyPauseClear    bool
	SafetyPauseDetail   string
	DailyLossRailOK     bool
	DailyLossRailDetail string
	LiveLossOK          bool
	LiveLossDetail      string
	AuthorityReady      bool
	Authority           []string
	AuthorityDetail     string
	FreshSignalStreams  int
	QueuedCandidates    int
}

func evaluateLiveAutoGo(in liveAutoGoInput) liveAutoGoView {
	if in.Now.IsZero() {
		in.Now = time.Now()
	}
	checks := map[string]liveAutoGoCheck{}
	blockers := []string{}
	warnings := []string{}
	add := func(key string, ok bool, detail, blocker string) {
		checks[key] = liveAutoGoCheck{OK: ok, Detail: detail}
		if !ok && blocker != "" {
			blockers = append(blockers, blocker)
		}
	}

	add("kill_switch", in.KillClear, in.KillDetail, "Kill switch is tripped.")
	add("kill_halt", in.KillHaltReady, map[bool]string{true: "halt/cancel sweep complete", false: "halt/cancel sweep still running"}[in.KillHaltReady], "Kill-switch cancel/disarm sweep is not complete.")
	add("feeds", in.FeedsOK, in.FeedsDetail, "Live signal feeds are not ready.")
	add("kalshi_auth", in.KalshiAuthOK, in.KalshiAuthDetail, "Kalshi authentication is not ready.")
	if in.PolyUSRequired {
		add("polyus_auth", in.PolyUSAuthOK, in.PolyUSAuthDetail, "PolyUS authentication is not ready.")
	}
	add("kalshi_books", in.KalshiBooksOK, in.KalshiBooksDetail, "Kalshi executable books are not ready.")
	if in.KalshiBooksWarning != "" {
		warnings = append(warnings, in.KalshiBooksWarning)
	}
	if in.PolyUSRequired {
		add("polyus_books", in.PolyUSBooksOK, in.PolyUSBooksDetail, "PolyUS executable books are not ready.")
	}
	add("restart_latches", in.LatchesOK, map[bool]string{true: "restart/dedup latches loaded", false: "restart/dedup latches unavailable"}[in.LatchesOK], "Restart/dedup safety latches are not loaded.")
	add("paper_ledgers", in.LedgerOK, in.LedgerDetail, "A Paper evidence ledger is unreadable.")
	add("signal_producers", in.ProducersOK, in.ProducersDetail, "An enabled LIVE system signal producer is stale or failing.")

	dbOK := in.DBKnown && in.DBAge >= 0 && in.DBAge <= liveGoDBSampleMaxAge && in.DBLastMS < in.DBThresholdMS
	dbDetail := "no current DB canary sample"
	if in.DBKnown {
		dbDetail = fmt.Sprintf("current %.0fms (%s old); one-hour p95 %.0fms; current limit %.0fms", in.DBLastMS, in.DBAge.Truncate(time.Second), in.DBP95MS, in.DBThresholdMS)
	}
	add("database_now", dbOK, dbDetail, "The current database response is slow or stale.")
	if dbOK && in.DBP95MS >= in.DBThresholdMS {
		warnings = append(warnings, fmt.Sprintf("Earlier DB stalls remain in the one-hour p95 (%.0fms); current DB response is %.0fms.", in.DBP95MS, in.DBLastMS))
	}

	add("kalshi_account", in.KalshiAccountOK, in.KalshiAccountDetail, "Kalshi balance, positions, or orders are not currently readable.")
	if in.PolyUSRequired {
		add("polyus_account", in.PolyUSAccountOK, in.PolyUSAccountDetail, "PolyUS balance, positions, or orders are not currently readable.")
	}
	add("pending_risk_ledger", in.RiskReadable, in.RiskDetail, "Durable pending-risk state cannot be read.")
	safetyPauseBlocker := ""
	if !in.SafetyPauseClear {
		safetyPauseBlocker = "Automatic LIVE safety pause: " + in.SafetyPauseDetail
	}
	add("automatic_safety_pause", in.SafetyPauseClear, in.SafetyPauseDetail, safetyPauseBlocker)
	dailyLossBlocker := ""
	if !in.DailyLossRailOK {
		dailyLossBlocker = "The daily-loss rail is breached: " + in.DailyLossRailDetail
	}
	add("daily_loss_rail", in.DailyLossRailOK, in.DailyLossRailDetail, dailyLossBlocker)
	add("daily_loss_ledger", in.LiveLossOK, in.LiveLossDetail, "The authenticated daily-loss ledger is not continuous.")
	if in.RiskReadable && !in.Armed && in.PendingRisk > 0 {
		blockers = append(blockers, fmt.Sprintf("%d unresolved durable risk reservation(s) must reconcile before ARM.", in.PendingRisk))
	} else if in.RiskReadable && in.Armed && in.PendingRisk > 0 {
		warnings = append(warnings, fmt.Sprintf("%d active order risk reservation(s) are being tracked.", in.PendingRisk))
	}
	add("live_authority", in.AuthorityReady, in.AuthorityDetail, "No LIVE-authorized System or New-ML route is enabled.")

	state := "NOT_READY"
	headline := "LIVE AUTO NOT READY"
	safe := len(blockers) == 0
	readyAuto := safe && in.Armed
	if !safe && ((!in.SafetyPauseClear && in.SafetyPauseDetail != "") || (in.Armed && in.Auto)) {
		// A running operator session keeps both controls ON through transient runtime trouble.
		// A canonical automatic safety pause remains PAUSED even while disarmed so readiness cannot
		// disagree with the ARM endpoint that would refuse the same active fault.
		state, headline = "PAUSED", "LIVE AUTO PAUSED"
	}
	if safe {
		state, headline = "SAFE_TO_ARM", "LIVE AUTO IS OK TO GO"
		if in.Armed {
			state, headline = "READY_TO_ENABLE_AUTO", "LIVE AUTO IS OK TO TURN ON"
		}
		if in.Armed && in.Auto {
			state, headline = "RUNNING", "LIVE AUTO RUNNING"
		}
	}
	opportunityState := "blocked"
	opportunityExplanation := "Runtime blockers must clear before an order can be evaluated."
	if safe {
		opportunityState = "waiting_for_signal"
		opportunityExplanation = "Safe to arm; no current opportunity is required. Waiting for a qualifying signal."
		if in.QueuedCandidates > 0 {
			opportunityState = "candidate_pending_final_checks"
			opportunityExplanation = fmt.Sprintf("%d fresh candidate(s) are waiting; every order still rechecks the live book, fees, depth, account and risk immediately before submission.", in.QueuedCandidates)
		} else if in.FreshSignalStreams > 0 {
			opportunityExplanation = fmt.Sprintf(
				"Safe to arm. %d distinct executable strategy observation(s) were refreshed in the last %d ms. This shows detector activity only; it is not statistical proof and does not mean %d bets reached the order queue.",
				in.FreshSignalStreams, liveMirrorTTL.Milliseconds(), in.FreshSignalStreams)
		}
	}
	return liveAutoGoView{
		State: state, Headline: headline, SafeToArm: safe, ReadyToEnableAuto: readyAuto,
		Armed: in.Armed, Auto: in.Auto, Blockers: blockers, Warnings: warnings, Checks: checks,
		Authority:                     in.Authority,
		RecentExecutableObservations:  in.FreshSignalStreams,
		ExecutableObservationWindowMS: liveMirrorTTL.Milliseconds(),
		FreshSignalStreams:            in.FreshSignalStreams,
		QueuedCandidates:              in.QueuedCandidates,
		OpportunityState:              opportunityState, OpportunityExplanation: opportunityExplanation,
		GeneratedAt: in.Now.UTC().Format(time.RFC3339Nano),
	}
}

func (s *Server) liveAutoGoDB(now time.Time) (last, p95 float64, age time.Duration, known bool) {
	if s.latmon == nil {
		return 0, 0, 0, false
	}
	s.latmon.mu.Lock()
	defer s.latmon.mu.Unlock()
	r := s.latmon.rings["db_ms"]
	if r == nil || r.n == 0 {
		return 0, 0, 0, false
	}
	pts := r.snap()
	pt := pts[len(pts)-1]
	return pt.MS, latPct(pts, .95), now.Sub(time.Unix(pt.T, 0)), true
}

// liveAutoGoPendingRisk is read-only. It never releases, reconciles, or mutates a reservation;
// the actual ARM transition owns those actions. A short cache prevents the 1s Live poll from
// turning one indexed safety read into a database load source during contention.
func (s *Server) liveAutoGoPendingRisk(now time.Time) (count int, readable bool, detail string) {
	s.liveGoMu.Lock()
	if !s.liveGoRiskAt.IsZero() && now.Sub(s.liveGoRiskAt) < liveGoRiskCacheFor {
		count, detail = s.liveGoRiskCount, s.liveGoRiskErr
		s.liveGoMu.Unlock()
		return count, detail == "", detail
	}
	if s.liveGoRiskBusy {
		s.liveGoMu.Unlock()
		return 0, false, "durable pending-risk proof is still running"
	}
	s.liveGoRiskBusy = true
	s.liveGoMu.Unlock()

	if s.store == nil {
		detail = "pending-risk store unavailable"
	} else {
		ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
		rows, err := s.store.ActiveLivePendingRisk(ctx)
		cancel()
		if err != nil {
			detail = err.Error()
		} else {
			count = len(rows)
		}
	}
	s.liveGoMu.Lock()
	s.liveGoRiskBusy = false
	s.liveGoRiskAt = time.Now() // cache only a completed proof; never expose a zero while it is in flight
	s.liveGoRiskCount, s.liveGoRiskErr = count, detail
	s.liveGoMu.Unlock()
	if detail != "" {
		return count, false, "read failed: " + detail
	}
	return count, true, fmt.Sprintf("readable; %d active reservation(s)", count)
}

func liveAutoGoMergeStagedRecovery(readable bool, detail, recoveryPause string) (bool, string) {
	recoveryPause = strings.TrimSpace(recoveryPause)
	if recoveryPause == "" {
		return readable, detail
	}
	return false, recoveryPause
}

// liveAutoGoLossReadiness uses only already-retained process memory. While disarmed, a clean
// restart has neither a persisted automatic-pause map nor a fresh authenticated loss proof, so
// the card must not promise that ARM will pass. The ARM endpoint itself owns that fresh venue
// refresh. Once armed, preserve liveLossVenueGate's established production and bare-test semantics.
func (s *Server) liveAutoGoLossReadiness(authorityVenues []string) (bool, string) {
	s.liveMu.Lock()
	armed := s.liveArmed
	s.liveMu.Unlock()
	for _, venue := range authorityVenues {
		var why string
		if armed {
			why = s.liveLossVenueGate(venue)
		} else {
			why = s.liveLossTruthReason(venue)
		}
		if why == "" {
			continue
		}
		if !armed {
			return false, "fresh pre-ARM daily-loss proof is not current in this process; " +
				"ARM performs the authenticated fresh check before opening either write gate (" + why + ")"
		}
		return false, why
	}
	return true, "authenticated daily-loss ledger ready"
}

func (s *Server) liveAutoGoAuthority(now time.Time) (ready bool, routes, venues []string, detail string) {
	venueSet := map[string]bool{}
	if s.cfg().Risk.LiveProspectiveAllocation {
		_, canonical, why := parseLiveSystemAllowlist(s.cfg().Risk.LiveSystemAllowlist)
		if why != "" {
			return false, nil, nil, strings.ReplaceAll(why, "-", " ")
		}
		for _, id := range canonical {
			venue := strings.SplitN(id, "|", 2)[0]
			if s.liveSystemVenueEnabled(venue) {
				routes = append(routes, id)
				venueSet[venue] = true
			}
		}
		if strings.TrimSpace(s.cfg().Risk.LiveSystemCanaryAllowlist) != "" {
			_, canary, canaryWhy := parseLiveSystemCanaryAllowlist(
				s.cfg().Risk.LiveSystemCanaryAllowlist,
				s.cfg().Risk.LiveSystemAllowlist)
			if canaryWhy != "" {
				return false, nil, nil, strings.ReplaceAll(canaryWhy, "-", " ")
			}
			if len(canary) == 0 {
				return false, nil, nil, "LIVE one contract canary requires a nonempty exact subset"
			}
		}
	}

	newMLEnabled := s.liveNewMLVenueEnabled("kalshi") || s.liveNewMLVenueEnabled("polyus")
	if newMLEnabled {
		var receipt newMLV2PredictionReceipt
		path := filepath.Join(s.cfg().DataDir, "ml_predictions.json")
		fi, err := os.Stat(path)
		fresh := err == nil && now.Sub(fi.ModTime()) >= 0 && now.Sub(fi.ModTime()) < 30*time.Minute
		if fresh && s.readJSONLoose(path, &receipt) && newMLV2ReceiptHasSealedLiveAuthority(receipt) {
			for _, venue := range []string{"kalshi", "polyus"} {
				if s.liveNewMLVenueEnabled(venue) {
					routes = append(routes, venue+"|new-ml-v2|BOTH|taker")
					venueSet[venue] = true
				}
			}
		}
	}
	for venue := range venueSet {
		venues = append(venues, venue)
	}
	sort.Strings(routes)
	sort.Strings(venues)
	if len(routes) == 0 {
		if newMLEnabled {
			return false, nil, nil, "New ML is enabled but has no fresh sealed untouched LIVE authority; no selected System route is enabled"
		}
		return false, nil, nil, "no selected System or New-ML destination route is enabled"
	}
	return true, routes, venues, fmt.Sprintf("%d exact route(s): %s", len(routes), strings.Join(routes, ", "))
}

type liveAutoGoAccountInput struct {
	KalshiBalanceOK bool
	KalshiPosOK     bool
	KalshiOrdersOK  bool
	KalshiDetail    string
	PolyUSOK        bool
	PolyUSDetail    string
}

func (s *Server) buildLiveAutoGo(now time.Time, account liveAutoGoAccountInput) liveAutoGoView {
	authorityReady, authority, authorityVenues, authorityDetail := s.liveAutoGoAuthority(now)
	venueRequired := map[string]bool{}
	for _, v := range authorityVenues {
		venueRequired[v] = true
	}
	// Destination readiness follows exact LIVE authority, not credential presence. A configured
	// but disabled PolyUS account cannot block a Kalshi-only canary.
	polyUSRequired := venueRequired["polyus"]

	ks := s.ks.State()
	s.liveMu.Lock()
	armed, autoOn, haltReady := s.liveArmed, s.liveAuto, s.killHaltReady
	s.liveMu.Unlock()

	kSubs, kFresh, kGaps, kErr := s.kal.BookStats()
	kPlan := s.depthBookPlanView("kalshi")
	kBooksOK, kBooksDetail, kBooksWarning := kalshiLiveBooksClassify(kSubs, kFresh, kGaps, kPlan.RequiredShortfall, kErr)

	pBooksOK, pBooksDetail := true, "not configured"
	if polyUSRequired {
		pBooksOK, pBooksDetail = s.liveAutoPolyUSBooks(now)
	}

	ledgers := s.paperLedgerIntegrityView()
	ledgerDetail := fmt.Sprintf("%d healthy; %d fresh/missing", ledgers.Healthy, ledgers.Missing)
	if len(ledgers.Broken) > 0 {
		ledgerDetail += "; " + strings.Join(ledgers.Broken, "; ")
	}
	requiredProducers := []string{}
	for _, venue := range []string{"kalshi", "polyus"} {
		if venueRequired[venue] {
			requiredProducers = append(requiredProducers, venue)
		}
	}
	requiredProducers = s.requiredLiveSignalProducers(requiredProducers)
	producerReceipts, producerNow := s.signalProducerSnapshotAt()
	producerOK, producerDetail := signalProducersClassify(producerNow, 3*time.Minute,
		requiredProducers, producerReceipts)

	dbLast, dbP95, dbAge, dbKnown := s.liveAutoGoDB(now)
	dbThreshold := s.cfg().Auto.LatWarnDBP95Ms
	if dbThreshold <= 0 {
		dbThreshold = 500
	}
	riskCount, riskReadable, riskDetail := s.liveAutoGoPendingRisk(now)
	riskReadable, riskDetail = liveAutoGoMergeStagedRecovery(
		riskReadable, riskDetail, s.r148StagedRecoveryPauseReason())
	safetyPauseReason, dailyLossPauseReason := s.snapshotLiveAutoSafetyPauses()
	safetyPauseDetail := "no automatic LIVE safety pause"
	if safetyPauseReason != "" {
		safetyPauseDetail = safetyPauseReason
	}
	dailyLossRailDetail := "no active daily-loss breach"
	if dailyLossPauseReason != "" {
		dailyLossRailDetail = dailyLossPauseReason
	}
	lossOK, lossDetail := s.liveAutoGoLossReadiness(authorityVenues)

	s.liveMirrorMu.Lock()
	queued, freshStreams := 0, 0
	for _, c := range s.liveMirrorQ {
		if age := now.Sub(c.At); age >= 0 && age <= liveMirrorTTL {
			queued++
		}
	}
	for _, at := range s.liveAllocationSignal {
		if age := now.Sub(at); age >= 0 && age <= liveMirrorTTL {
			freshStreams++
		}
	}
	s.liveMirrorMu.Unlock()

	kAccountOK := account.KalshiBalanceOK && account.KalshiPosOK && account.KalshiOrdersOK
	return evaluateLiveAutoGo(liveAutoGoInput{
		Now: now, Armed: armed, Auto: autoOn,
		KillClear: !ks.Tripped, KillDetail: func() string {
			if ks.Tripped {
				return "TRIPPED: " + ks.Reason
			}
			return "clear"
		}(), KillHaltReady: haltReady,
		FeedsOK: s.feedsReady(), FeedsDetail: "signal feeds warmed",
		KalshiAuthOK: s.kal != nil && s.kal.HasCredentials() && !strings.EqualFold(s.authState, "locked"), KalshiAuthDetail: s.authDetail,
		PolyUSRequired: polyUSRequired, PolyUSAuthOK: !polyUSRequired || (s.polyUSAuth != nil && strings.EqualFold(s.pusAuthState, "ok")), PolyUSAuthDetail: s.pusAuthDetail,
		KalshiBooksOK: kBooksOK, KalshiBooksDetail: kBooksDetail, KalshiBooksWarning: kBooksWarning,
		PolyUSBooksOK: pBooksOK, PolyUSBooksDetail: pBooksDetail,
		LatchesOK: s.latchesOK.Load(), LedgerOK: ledgers.OK, LedgerDetail: ledgerDetail,
		ProducersOK: producerOK, ProducersDetail: producerDetail,
		DBKnown: dbKnown, DBLastMS: dbLast, DBP95MS: dbP95, DBAge: dbAge, DBThresholdMS: dbThreshold,
		KalshiAccountOK: kAccountOK, KalshiAccountDetail: account.KalshiDetail,
		PolyUSAccountOK: !polyUSRequired || account.PolyUSOK, PolyUSAccountDetail: account.PolyUSDetail,
		RiskReadable: riskReadable, PendingRisk: riskCount, RiskDetail: riskDetail,
		SafetyPauseClear: safetyPauseReason == "", SafetyPauseDetail: safetyPauseDetail,
		DailyLossRailOK: dailyLossPauseReason == "", DailyLossRailDetail: dailyLossRailDetail,
		LiveLossOK: lossOK, LiveLossDetail: lossDetail,
		AuthorityReady: authorityReady, Authority: authority, AuthorityDetail: authorityDetail,
		FreshSignalStreams: freshStreams, QueuedCandidates: queued,
	})
}
