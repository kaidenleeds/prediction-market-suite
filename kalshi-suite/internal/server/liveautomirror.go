package server

// R132 PAPER -> LIVE AUTO mirror.
//
// A selected strategy signal may publish a short-lived LIVE candidate before funded Paper
// evaluation, while sealed promotions and New ML retain their route-identical Paper capability.
// The candidate is not an order: the live consumer independently proves that ARM + LIVE AUTO are
// still enabled, refreshes venue lifecycle and executable prices, replaces historical fee drag
// with the fee payable now, and applies the lane's exact proof contract. Balance-aware size may
// never exceed fresh executable depth or evidence capacity; legacy/raw ML and unselected rolling
// leaderboard rows remain blocked.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/polymarketus"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

const (
	liveMirrorQueueCap  = 256
	liveMirrorTTL       = 25 * time.Second
	liveMirrorDedup     = 30 * time.Second
	liveMirrorComboTTL  = 2 * time.Minute
	liveMirrorComboCap  = 32
	liveMirrorClaimTTL  = 6 * time.Hour
	liveMirrorClaimPref = "live_mirror_claim:"
	// Match the authenticated account lease: a handler quote older than this gets one final
	// conditional refresh, while the normal sub-250ms path makes no duplicate book request.
	liveMirrorHandlerQuoteMaxAge = 250 * time.Millisecond

	// PolyUS positions are REST-reconciled every 60s while its private order stream is healthy,
	// and every 10s without it.  The mirror may consume those shared caches (to avoid a second
	// rate-limit-heavy account poll), but never beyond the producer's cadence plus a small grace.
	liveMirrorPUSPushCacheTTL = 75 * time.Second
	liveMirrorPUSRESTCacheTTL = 15 * time.Second

	// These are safety floors, not tunable defaults. Settings may raise either bar but may not
	// lower prospective real-money allocation below a meaningful distinct-contract sample.
	// The exact operator allowlist is the final selection belt. Sixty distinct settled
	// contracts is the hard evidence floor for an explicitly selected identity; the
	// configured value may be raised but never lowered past this floor. This admits the
	// operator-selected spotlag YES/NO lanes (84/72 contracts) without weakening the
	// current-book, fee, depth, confidence-bound, ARM, or LIVE AUTO checks.
	liveAllocationMinMarketsFloor = 60
)

// liveMirrorCandidate is a short-lived money-lane intent. For sealed/New-ML lanes Price and At are
// the accepted Paper boundary. For an exact prospective allowlist lane they are the LIVE-first
// executable-book boundary captured directly from the fresh strategy signal. In either case the
// final handler independently reprices and rechecks proof, account state, sizing and risk.
type liveMirrorCandidate struct {
	Platform string
	Ticker   string
	Title    string
	Side     string
	// ShadowAttemptID is diagnostic lineage only. It joins the shared detector decision to LIVE,
	// funded Paper, and counterfactual receipts; it is never accepted as authority or sent to a
	// venue as an idempotency key.
	ShadowAttemptID string
	// InputTopology and SignalContractID bind prospective LIVE evidence to the exact canonical
	// R147 upstream-input contract. They are intentionally independent from execution venue:
	// PINT may be an input but can never become a money destination.
	InputTopology    string
	SignalContractID string
	InputObservedAt  time.Time // authoritative non-venue source receipt (spot/OKX/sportsbook/NOAA)
	// A portable cross-venue signal carries the exact source instrument and current normalized
	// certificate into the final money boundary. Native signals leave these blank. The certificate
	// is re-read immediately before dispatch; a stale rule artifact cannot ride a Paper receipt.
	CrossVenueSourceVenue     string
	CrossVenueSourceTicker    string
	CrossVenueSourceSide      string
	CrossVenueCertificateHash string
	// Family remains the economic strategy identity after Source is replaced by the opaque r139p
	// proof capability. SelectorID/FiredSide preserve the exact immutable inverse subroute.
	Family     string
	SelectorID string
	FiredSide  string
	Action     string // BUY default; typed proper-score momentum reductions use SELL
	Source     string
	Price      float64
	At         time.Time
	Inverted   bool
	// A LIVE-first signal already paid for an authoritative full-book/fee proof immediately before
	// enqueue. Arbitration may reuse that sub-second receipt for ranking; dispatch and the final
	// handler still reprice independently before any venue mutation.
	ArbitrationQuote liveMirrorQuote
	ArbitrationBasis string
	ArbitrationMean  float64
	ArbitrationLower float64
	ArbitrationFee   float64
	ArbitrationAt    time.Time
	// Prospective* freezes the quantity-aware execution plan that cleared the LIVE-first book.
	// IOC plans remain proved at the one-contract fee so any partial fill stays safe. A plan that
	// needs aggregate-order fee rounding is FOK: the venue must fill the whole proved quantity or
	// none of it.
	ProspectiveQty            float64
	ProspectiveRequestedFee   float64
	ProspectiveRequestedFeePC float64
	ProspectiveFeeSource      string
	ProspectiveTimeInForce    string
	ProspectivePlanReason     string
	// Canary is a separate one-contract experiment, never statistical proof. CanaryBasis records
	// the exact operator-selected identity and the observed point/lower estimates that justified
	// collecting one real IOC receipt.
	Canary      bool
	CanaryBasis string
	// LiveIntentGeneration binds an asynchronous LIVE-first preflight to the exact operator AUTO
	// session that admitted its raw signal. The boolean is separate because generation zero is a
	// valid initial session value on a freshly constructed Server.
	LiveIntentGeneration      uint64
	LiveIntentGenerationBound bool
	// LivePreflightAdmitted marks work that already entered the bounded AUTO preflight scheduler.
	// Its terminal receipt remains required after AUTO is turned off; raw off-state observations
	// still stay silent. This is process-only authority and is never accepted from an HTTP body.
	LivePreflightAdmitted bool
	// LiveFirstUnitPersisted is set only by the LIVE-first signal worker after its exact
	// full-book/fee UnitTrial insert succeeds. It is an internal receipt, never accepted from an
	// HTTP request. Together with the bound signal contract and proof fields it lets enqueue skip
	// the older duplicate observeLiveMirrorUnit network+database pass.
	LiveFirstUnitPersisted bool
}

type liveMirrorPUSDispatch struct {
	Source            string
	RequestedQty      float64
	RiskReservationID string
}

// PolyUS submit acknowledgement and order-state truth arrive on different goroutines. Keying the
// sealed source by venue order id lets the existing verifier attach only this AUTO order to its
// accepted Paper intent; manual orders can never be mistaken for a promotion fill.
var liveMirrorPUSDispatches sync.Map

func liveAutoPromotionContractReason(in storage.ResearchPromotionIntent, venue, ticker, side, route string) string {
	if !strings.EqualFold(in.Candidate.Venue, venue) {
		return "sealed Paper venue differs from the LIVE venue"
	}
	if strings.TrimSpace(in.Candidate.Ticker) != strings.TrimSpace(ticker) {
		return "sealed Paper instrument differs from the LIVE instrument"
	}
	if !strings.EqualFold(in.Candidate.Side, side) {
		return "sealed Paper side differs from the LIVE side"
	}
	if !strings.EqualFold(in.Proof.Route, route) {
		return "sealed Paper maker/taker route differs from the LIVE route"
	}
	return ""
}

func liveAutoPromotionCandidateReason(in storage.ResearchPromotionIntent, c liveMirrorCandidate, route string) string {
	if why := liveAutoPromotionContractReason(in, c.Platform, c.Ticker, c.Side, route); why != "" {
		return why
	}
	wantFamily := strings.ToLower(strings.TrimSpace(in.Candidate.StrategyFamily))
	if wantFamily == "" {
		if strings.TrimSpace(c.Family) != "" || strings.TrimSpace(c.SelectorID) != "" || strings.TrimSpace(c.FiredSide) != "" {
			return "sealed Paper strategy identity differs from the LIVE strategy identity"
		}
		return ""
	}
	if !strings.HasPrefix(wantFamily, "invert:") || strings.HasPrefix(wantFamily, "invert:invert:") ||
		strings.ToLower(strings.TrimSpace(c.Family)) != wantFamily ||
		strings.TrimSpace(c.SelectorID) != strings.TrimSpace(in.Candidate.SelectorID) ||
		!strings.EqualFold(strings.TrimSpace(c.FiredSide), strings.TrimSpace(in.Candidate.FiredSide)) {
		return "sealed Paper inverse family/selector differs from the LIVE strategy identity"
	}
	return ""
}

// liveAutoPromotionCurrentIdentityReason binds an accepted Paper proof to the venue instrument's
// current canonical event and payoff versions. A rules/payoff remap after Paper acceptance must
// never inherit the old sealed authority merely because venue+ticker+side still match.
func (s *Server) liveAutoPromotionCurrentIdentityReason(ctx context.Context,
	in storage.ResearchPromotionIntent) string {
	return s.currentResearchCandidateIdentityReason(ctx, in.Candidate)
}

func (c liveMirrorCandidate) key() string {
	action := strings.ToUpper(strings.TrimSpace(c.Action))
	if action == "" {
		action = "BUY"
	}
	key := action + "|" + strings.ToLower(strings.TrimSpace(c.Platform)) + "|" +
		strings.TrimSpace(c.Ticker) + "|" + strings.ToUpper(strings.TrimSpace(c.Side))
	if contract := strings.TrimSpace(c.SignalContractID); contract != "" {
		key += "|" + contract
	} else {
		// Sealed/New-ML/Paper-derived candidates may not carry an R147 signal contract. Preserve
		// their exact proof identity through parallel preflight so a faster weak system cannot
		// coalesce a slower stronger system before arbitration ranks both.
		//
		// A New-ML capability source is a fresh random one-use bearer token. It is handler
		// authority, not economic identity: including it here let every scan of the same open
		// Paper lot bypass the execution-queue dedup and reserve the sleeve again. New-ML owns one
		// family/market/side candidate per window, while duplicate attempts still produce their
		// own no-dedup audit receipt in enqueueLiveMirrorCandidate.
		sourceIdentity := strings.ToLower(strings.TrimSpace(c.Source))
		if isNewMLV2LiveSource(sourceIdentity) {
			sourceIdentity = "new-ml-v2"
		}
		proofIdentity := strings.Join([]string{
			strings.ToLower(strings.TrimSpace(c.Family)),
			strings.TrimSpace(c.SelectorID),
			strings.ToUpper(strings.TrimSpace(c.FiredSide)),
			sourceIdentity,
		}, "|")
		if proofIdentity != "|||" {
			key += "|proof=" + proofIdentity
		}
	}
	return key
}

func liveMirrorMLSource(source string) bool {
	src := strings.ToLower(strings.TrimSpace(source))
	return src == "ml" || src == "auto-ml" || src == "ml-book" || src == "book:ml" ||
		strings.HasPrefix(src, "ml:") || strings.HasPrefix(src, "ml-") || isNewMLV2LiveSource(src)
}

func (s *Server) liveMirrorEnabled() bool {
	s.liveMu.Lock()
	on := s.liveArmed && s.liveAuto
	s.liveMu.Unlock()
	return on && !s.ksBlocked()
}

func (s *Server) liveMirrorWakeChannel() chan struct{} {
	s.liveMirrorWakeOnce.Do(func() { s.liveMirrorWakeCh = make(chan struct{}, 1) })
	return s.liveMirrorWakeCh
}

func (s *Server) wakeLiveMirrorDispatch() {
	select {
	case s.liveMirrorWakeChannel() <- struct{}{}:
	default:
	}
}

// rearmLiveMirrorDispatchIfPending closes the edge-triggered wake gap after a bounded consume
// pass. A burst can contain more than the eight candidates one pass is allowed to inspect. The
// original wake token is consumed before that pass begins, so leaving the ninth candidate queued
// without publishing a new token strands it until the seven-second maintenance tick (and, under
// repeated slow reads, can age a genuine 25-second signal out). Keep the queue and wake bounded:
// this emits at most the channel's single coalesced token and never starts another goroutine.
func (s *Server) rearmLiveMirrorDispatchIfPending() bool {
	s.liveMirrorMu.Lock()
	pending := len(s.liveMirrorQ) > 0
	s.liveMirrorMu.Unlock()
	if pending {
		s.wakeLiveMirrorDispatch()
	}
	return pending
}

func (s *Server) liveFirstMirrorReceiptFresh(c liveMirrorCandidate, now time.Time) bool {
	return c.LiveFirstUnitPersisted && c.SignalContractID != "" &&
		s.liveMirrorArbitrationReceiptReusable(c, now, 2*time.Second) &&
		s.liveAllocationSignalFresh(c, c.ArbitrationAt)
}

// enqueueLiveMirrorCandidate is intentionally a no-op while either switch is off.  Enabling AUTO
// therefore never replays paper decisions made before the operator completed the ARM + AUTO
// handshake.  The queue is bounded and short-lived; venue claims have a separate restart-safe KV.
func (s *Server) enqueueLiveMirrorCandidate(c liveMirrorCandidate) bool {
	now := time.Now()
	// Queue admission is the immutable AUTO-session boundary for every in-process candidate,
	// including older sealed Paper capabilities. Unbound JSON remains parse-compatible at the
	// handlers, but an in-process money candidate can never enter asynchronous arbitration without
	// a generation receipt.
	if !c.LiveIntentGenerationBound {
		c.LiveIntentGeneration = s.liveSignalIntentGeneration.Load()
		c.LiveIntentGenerationBound = true
	}
	c = s.executionShadowEnsureCandidate(c, "qualified-live-candidate")
	// Candidate creation only copies its immutable receipt into memory. Flush it after this
	// function has published or rejected the cash candidate, never before the LIVE queue decision.
	defer s.wakeExecutionShadowWriter()
	c.Platform = strings.ToLower(strings.TrimSpace(c.Platform))
	c.Side = strings.ToUpper(strings.TrimSpace(c.Side))
	c.Action = strings.ToUpper(strings.TrimSpace(c.Action))
	if c.Action == "" {
		c.Action = "BUY"
	}
	c.Ticker = strings.TrimSpace(c.Ticker)
	c.Source = strings.TrimSpace(c.Source)
	if c.At.IsZero() {
		c.At = now
	}
	_, newMLV2 := s.newMLV2Capability(c, "")
	if (c.Platform != "kalshi" && c.Platform != "polyus") || (c.Action != "BUY" && c.Action != "SELL") || c.Ticker == "" ||
		(c.Side != "YES" && c.Side != "NO") || c.Price <= 0 || c.Price >= 1 ||
		math.IsNaN(c.Price) || math.IsInf(c.Price, 0) || (liveMirrorMLSource(c.Source) && !newMLV2) {
		s.recordLiveCandidateDrop(c, "AUTO-LIVE-MIRROR-DROP", "mirror-validation",
			"invalid-or-unauthorized-live-candidate", now, nil)
		return false
	}
	if why := s.liveIntentGenerationReason(c.LiveIntentGeneration, c.LiveIntentGenerationBound); why != "" {
		s.recordLiveCandidateDrop(c, "AUTO-LIVE-MIRROR-DROP", "mirror-generation",
			why, now, nil)
		return false
	}
	// A configured q=1 canary may only arrive from the canonical LIVE-first signal worker. In
	// particular, do this before observeLiveMirrorUnit: the legacy funded-Paper autoPlace path may
	// keep logging/filling Paper, but it cannot mint the exact receipt that would turn an old alias
	// or periodic clockless copy into real-money authority.
	if why := s.liveConfiguredCanaryAdmissionReason(c, "taker", now); why != "" {
		s.recordLiveCandidateDrop(c, "AUTO-LIVE-MIRROR-DROP", "canary-admission",
			why, now, nil)
		return false
	}
	// This prospective lane is independent of bankroll and LIVE state.  It records the actual
	// strategy side at a fresh executable ask, so future live authorization is based on prices a
	// real order could have paid rather than the earlier signal/logger mark.  It deliberately runs
	// before the ARM+AUTO queue gate; research must keep learning while real money is off.
	liveFirstReceipt := s.liveFirstMirrorReceiptFresh(c, now)
	if c.Action == "BUY" && !newMLV2 && !liveFirstReceipt { // SELL/New ML/LIVE-first have their own terminal-grade ledgers.
		bound, observed := s.observeLiveMirrorUnit(c)
		if bound.SignalContractID != "" {
			c = bound
		}
		if observed {
			key := liveAllocationSignalKey(c)
			if key == "" {
				s.recordLiveCandidateDrop(c, "AUTO-LIVE-MIRROR-DROP", "mirror-proof",
					"signal-contract-freshness-key-unavailable", time.Now(), nil)
				return false
			}
			s.liveMirrorMu.Lock()
			if s.liveAllocationSignal == nil {
				s.liveAllocationSignal = map[string]time.Time{}
			}
			s.liveAllocationSignal[key] = time.Now()
			s.liveMirrorMu.Unlock()
		}
	}
	now = time.Now()
	if now.Sub(c.At) > liveMirrorTTL {
		s.logLiveMirrorExpiry(c, now, "before-enqueue")
		return false
	}
	if !s.liveMirrorEnabled() {
		s.recordLiveCandidateDrop(c, "AUTO-LIVE-MIRROR-DROP", "mirror-gate",
			"live-runtime-gate-disabled", now, nil)
		return false
	}
	if c.Platform == "kalshi" {
		// Cache warming is background-only. LIVE never waits here and the final payoff guard still
		// requires a fully resident authoritative Event/Milestone receipt.
		s.scheduleKalshiSemanticWarm(c.Ticker)
	}
	key := c.key()
	s.liveMirrorMu.Lock()
	// Pair the final generation read with the queue lock. clearLiveMirrorCandidates advances the
	// generation before taking this same lock: either this append wins first and clear removes it,
	// or the old worker observes the new generation and cannot publish into the new AUTO session.
	if why := s.liveIntentGenerationReason(c.LiveIntentGeneration, c.LiveIntentGenerationBound); why != "" {
		s.liveMirrorMu.Unlock()
		s.recordLiveCandidateDrop(c, "AUTO-LIVE-MIRROR-DROP", "mirror-generation",
			why, time.Now(), nil)
		return false
	}
	if s.liveMirrorSeen == nil {
		s.liveMirrorSeen = map[string]time.Time{}
	}
	for k, at := range s.liveMirrorSeen {
		if now.Sub(at) > liveMirrorDedup {
			delete(s.liveMirrorSeen, k)
		}
	}
	if at, exists := s.liveMirrorSeen[key]; exists && now.Sub(at) <= liveMirrorDedup {
		s.liveMirrorMu.Unlock()
		s.recordLiveCandidateDrop(c, "AUTO-LIVE-MIRROR-DROP", "mirror-queue",
			"duplicate-coalesced-live-candidate", now,
			map[string]any{"coalesced_with_age_ms": now.Sub(at).Milliseconds()})
		return false
	}
	// Drop expired queue entries before applying the hard cap.
	var expired []liveMirrorCandidate
	kept := s.liveMirrorQ[:0]
	for _, q := range s.liveMirrorQ {
		if now.Sub(q.At) <= liveMirrorTTL {
			kept = append(kept, q)
		} else {
			expired = append(expired, q)
		}
	}
	s.liveMirrorQ = kept
	var evicted []liveMirrorCandidate
	if len(s.liveMirrorQ) >= liveMirrorQueueCap {
		evicted = append(evicted, s.liveMirrorQ[:len(s.liveMirrorQ)-liveMirrorQueueCap+1]...)
		copy(s.liveMirrorQ, s.liveMirrorQ[len(s.liveMirrorQ)-liveMirrorQueueCap+1:])
		s.liveMirrorQ = s.liveMirrorQ[:liveMirrorQueueCap-1]
	}
	s.liveMirrorQ = append(s.liveMirrorQ, c)
	var comboExpired, comboCoalesced, comboEvicted []liveMirrorCandidate
	if c.Platform == "kalshi" {
		comboKept := s.liveMirrorComboQ[:0]
		for _, q := range s.liveMirrorComboQ {
			switch {
			case now.Sub(q.At) > liveMirrorComboTTL:
				comboExpired = append(comboExpired, q)
			case q.key() == key:
				comboCoalesced = append(comboCoalesced, q)
			default:
				comboKept = append(comboKept, q)
			}
		}
		s.liveMirrorComboQ = append(comboKept, c)
		if len(s.liveMirrorComboQ) > liveMirrorComboCap {
			comboEvicted = append(comboEvicted,
				s.liveMirrorComboQ[:len(s.liveMirrorComboQ)-liveMirrorComboCap]...)
			s.liveMirrorComboQ = append([]liveMirrorCandidate(nil), s.liveMirrorComboQ[len(s.liveMirrorComboQ)-liveMirrorComboCap:]...)
		}
	}
	s.liveMirrorSeen[key] = now
	s.liveMirrorMu.Unlock()
	s.wakeLiveMirrorDispatch()
	for _, q := range expired {
		s.logLiveMirrorExpiry(q, now, "enqueue-prune")
	}
	for _, q := range evicted {
		s.recordLiveCandidateDrop(q, "AUTO-LIVE-MIRROR-DROP", "mirror-queue",
			"queue-cap-evicted", now, map[string]any{"queue_capacity": liveMirrorQueueCap})
	}
	for _, q := range comboExpired {
		s.recordLiveCandidateHousekeeping(q, "AUTO-LIVE-COMBO-CANDIDATE-DROP", "combo-candidate-queue",
			"combo-candidate-expired", now, map[string]any{"combo_ttl_ms": liveMirrorComboTTL.Milliseconds()})
	}
	for _, q := range comboCoalesced {
		s.recordLiveCandidateHousekeeping(q, "AUTO-LIVE-COMBO-CANDIDATE-DROP", "combo-candidate-queue",
			"combo-candidate-replaced-by-fresher-same-route", now, nil)
	}
	for _, q := range comboEvicted {
		s.recordLiveCandidateHousekeeping(q, "AUTO-LIVE-COMBO-CANDIDATE-DROP", "combo-candidate-queue",
			"combo-candidate-cap-evicted", now, map[string]any{"queue_capacity": liveMirrorComboCap})
	}
	return true
}

func (s *Server) liveAllocationSignalFresh(c liveMirrorCandidate, since time.Time) bool {
	key := liveAllocationSignalKey(c)
	if key == "" {
		return false
	}
	s.liveMirrorMu.Lock()
	at, ok := s.liveAllocationSignal[key]
	s.liveMirrorMu.Unlock()
	return ok && !at.Before(since) && time.Since(at) >= 0 && time.Since(at) <= liveMirrorTTL
}

func (s *Server) observeLiveMirrorUnit(c liveMirrorCandidate) (liveMirrorCandidate, bool) {
	if s.store == nil {
		return c, false
	}
	bound, _, why := s.liveAllocationBindSignalContract(c, "taker", false)
	if why != "" {
		return c, false
	}
	c = bound
	// Keep one economic history per family+venue+origin+side. The bound signal-contract receipt
	// stays in the quote provenance and freshness key, but must not fork/reset valid UnitTrial data.
	fam := liveMirrorFamily(c)
	if fam == "" {
		return c, false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	ask, depth, source, ok := s.executableAsk(ctx, c.Platform, c.Ticker, c.Side, false)
	if !ok {
		return c, false
	}
	// Use the taker fee for the proof lane. A live maker fill may cost less, but a quote that is
	// only profitable under an assumed rebate has not proved a generally executable edge.
	fee, feeSource, feeKnown := s.unitTrialFeeExact(c.Platform, c.Ticker, ask)
	_, err := s.insertCanonicalUnitTrial(ctx, storage.UnitTrial{
		OpenedTS: time.Now().UTC(), Family: fam, Platform: c.Platform, Ticker: c.Ticker,
		Side: c.Side, Ask: ask, FeePC: fee, FeeKnown: feeKnown, FeeSource: feeSource, Depth: depth,
		QuoteSource: "strategy-decision/" + source + "/" + c.SignalContractID,
	})
	// A repeated signal for the same open contract may deduplicate in the durable one-share table;
	// the just-refreshed ask/depth/exact-fee observation is still a valid session receipt. A real
	// storage error or unknown fee fails closed and never mints allocation-lane freshness.
	return c, err == nil && feeKnown
}

type liveMirrorQueueClear struct {
	singles            []liveMirrorCandidate
	combos             []liveMirrorCandidate
	intents            []liveSignalIntent
	preflightScheduler *liveSignalPreflightScheduler
}

// advanceAndClearLiveMirrorCandidatesWhileWriteLocked is the queue half of an operator/kill
// transition. The caller must own liveWriteFence for writing. Keeping this helper fence-free lets
// OFF/ON change authority, advance the generation, and remove all old work as one atomic boundary
// without recursively acquiring the non-reentrant RWMutex.
func (s *Server) advanceAndClearLiveMirrorCandidatesWhileWriteLocked() liveMirrorQueueClear {
	s.liveSignalIntentGeneration.Add(1)
	preflightScheduler := stopLiveSignalPreflightSchedulerFor(s)
	s.liveMirrorMu.Lock()
	cleared := liveMirrorQueueClear{
		singles:            append([]liveMirrorCandidate(nil), s.liveMirrorQ...),
		combos:             append([]liveMirrorCandidate(nil), s.liveMirrorComboQ...),
		preflightScheduler: preflightScheduler,
	}
	s.liveMirrorQ = nil
	s.liveMirrorComboQ = nil
	s.liveMirrorSeen = nil
	s.liveAllocationSignal = nil
	s.liveSignalIntentSeen = nil
	s.liveMirrorMu.Unlock()
	for {
		select {
		case intent := <-s.liveSignalIntentChannel():
			cleared.intents = append(cleared.intents, intent)
			continue
		default:
		}
		break
	}
	for _, wake := range []chan struct{}{s.liveMirrorWakeChannel(), s.liveBookWakeChannel()} {
		select {
		case <-wake:
		default:
		}
	}
	return cleared
}

func (s *Server) recordLiveMirrorQueueClear(cleared liveMirrorQueueClear, reason string) {
	for _, candidate := range cleared.singles {
		s.executionShadowRecordQueueClear(candidate, "live-mirror-single", reason)
	}
	for _, candidate := range cleared.combos {
		s.executionShadowRecordQueueCleanup(candidate, "live-mirror-combo", reason)
	}
	for _, intent := range cleared.intents {
		s.executionShadowRecordIntentQueueClear(intent, reason)
	}
}

// transitionLiveAuto publishes one complete operator AUTO session boundary. OFF does not become
// visible until every venue call that already owns the final read fence has returned; after the
// flip, no old generation or queued candidate can cross that fence. ON similarly clears the old
// session and publishes the new generation plus AUTO authority without an observable half-state.
func (s *Server) transitionLiveAuto(on bool) (liveMirrorQueueClear, string) {
	s.liveWriteFence.Lock()
	defer s.liveWriteFence.Unlock()

	s.liveMu.Lock()
	if on && !s.liveArmed {
		s.liveMu.Unlock()
		return liveMirrorQueueClear{}, "arm live orders first — AUTO requires an armed session"
	}
	// OFF is the first state change inside the writer fence for both directions. In the ON case it
	// keeps candidate admission closed until the new generation has been cleared and published.
	s.liveAuto = false
	s.liveMu.Unlock()

	cleared := s.advanceAndClearLiveMirrorCandidatesWhileWriteLocked()
	if !on {
		return cleared, ""
	}
	if s.ksBlocked() {
		return cleared, "kill switch tripped — reset it before enabling AUTO"
	}
	s.liveMu.Lock()
	defer s.liveMu.Unlock()
	if !s.liveArmed {
		return cleared, "arm live orders first — AUTO requires an armed session"
	}
	s.liveAuto = true
	return cleared, ""
}

func (s *Server) clearLiveMirrorCandidates() {
	// Pair the generation advance with the venue-write fence: a handler already inside its final
	// read fence finishes first; every later handler observes the new generation before it may
	// mutate a venue. Running preflight workers are cancelled but never joined.
	s.liveWriteFence.Lock()
	cleared := s.advanceAndClearLiveMirrorCandidatesWhileWriteLocked()
	s.liveWriteFence.Unlock()
	const clearReason = "operator-auto-boundary-cleared-before-dispatch"
	s.recordLiveMirrorQueueClear(cleared, clearReason)
}

func (s *Server) recentLiveMirrorComboCandidates() []liveMirrorCandidate {
	now := time.Now()
	s.liveMirrorMu.Lock()
	var dropped []liveMirrorCandidate
	kept := s.liveMirrorComboQ[:0]
	for _, c := range s.liveMirrorComboQ {
		if now.Sub(c.At) <= liveMirrorComboTTL && c.Platform == "kalshi" && !liveMirrorMLSource(c.Source) {
			kept = append(kept, c)
		} else {
			dropped = append(dropped, c)
		}
	}
	s.liveMirrorComboQ = kept
	out := append([]liveMirrorCandidate(nil), kept...)
	s.liveMirrorMu.Unlock()
	for _, c := range dropped {
		reason := "combo-candidate-not-route-eligible"
		if now.Sub(c.At) > liveMirrorComboTTL {
			reason = "combo-candidate-expired"
		}
		s.recordLiveCandidateHousekeeping(c, "AUTO-LIVE-COMBO-CANDIDATE-DROP", "combo-candidate-read",
			reason, now, map[string]any{"combo_ttl_ms": liveMirrorComboTTL.Milliseconds()})
	}
	return out
}

// popLiveMirrorCandidate refuses even to consume while the two-switch live gate is off.  This is
// separately enforced again inside each live HTTP handler immediately before its write path.
func (s *Server) popLiveMirrorCandidate() (liveMirrorCandidate, bool) {
	if !s.liveMirrorEnabled() {
		return liveMirrorCandidate{}, false
	}
	now := time.Now()
	s.liveMirrorMu.Lock()
	var expired []liveMirrorCandidate
	for len(s.liveMirrorQ) > 0 {
		c := s.liveMirrorQ[0]
		s.liveMirrorQ = s.liveMirrorQ[1:]
		if now.Sub(c.At) <= liveMirrorTTL {
			s.liveMirrorMu.Unlock()
			for _, stale := range expired {
				s.logLiveMirrorExpiry(stale, now, "before-dispatch")
			}
			return c, true
		}
		expired = append(expired, c)
	}
	s.liveMirrorMu.Unlock()
	for _, stale := range expired {
		s.logLiveMirrorExpiry(stale, now, "before-dispatch")
	}
	return liveMirrorCandidate{}, false
}

func (s *Server) logLiveMirrorExpiry(c liveMirrorCandidate, now time.Time, stage string) {
	age := now.Sub(c.At)
	if age < 0 {
		age = 0
	}
	s.recordLiveCandidateDrop(c, "AUTO-LIVE-MIRROR-DROP", stage, "expired-before-dispatch", now,
		map[string]any{"age_ms": age.Milliseconds(), "ttl_ms": liveMirrorTTL.Milliseconds()})
}

func liveMirrorFamily(c liveMirrorCandidate) string {
	if isNewMLV2LiveSource(c.Source) {
		return "new-ml-v2"
	}
	if systemID, ok := researchPaperExplorationSystem(c.Source); ok {
		return systemID
	}
	if family := strings.ToLower(strings.TrimSpace(c.Family)); family != "" {
		if strings.HasPrefix(family, "invert:invert:") || strings.HasPrefix(family, "counterfactual:") ||
			strings.HasPrefix(family, "side-control:") {
			return ""
		}
		return family
	}
	var fam string
	switch c.Source {
	case "freshinv":
		// The shared paper book is venue-split: Kalshi fades the listing, PolyUS follows it.
		// A source-only mapping used to judge the PolyUS direct follow against Kalshi's inverse
		// evidence. Venue truth wins even for legacy candidates whose Inverted flag was wrong.
		if c.Platform == "polyus" {
			return "freshlist"
		}
		return "invert:freshlist"
	case "xvgap":
		fam = "xvgap"
	case "favlong80", "auto-cons-favlong80":
		fam = "favlong80"
	case "weather":
		fam = "wxedge"
	case "rawflow":
		fam = "rawflow"
	default:
		fam = policySourceFamily(c.Source)
	}
	if c.Inverted && fam != "" && !strings.HasPrefix(fam, "invert:") {
		fam = "invert:" + fam
	}
	return fam
}

func (s *Server) liveMirrorFeePC(c liveMirrorCandidate, maker bool, price float64) float64 {
	return s.liveMirrorFeePCAtQuantity(c, maker, 1, price)
}

func (s *Server) liveMirrorFeePCAtQuantity(c liveMirrorCandidate, maker bool,
	quantity, price float64) float64 {
	if quantity <= 0 || math.IsNaN(quantity) || math.IsInf(quantity, 0) {
		return math.Inf(1)
	}
	switch c.Platform {
	case "kalshi":
		fee, known, _ := s.kalFeeExact(c.Ticker, maker, quantity, price)
		if !known {
			return math.Inf(1)
		}
		return math.Max(fee, 0) / quantity
	case "polyus":
		fee, _, known := s.polyUSFeeExactAuthority(c.Ticker, maker, quantity, price)
		if !known || quantity <= 0 {
			return math.Inf(1)
		}
		// Preserve the signed rebate in research ledgers, but do not grant uncollected maker rebates
		// as buying power or proof-cap headroom. The final PolyUS handler applies the same clamp to
		// its fresh request-time detail receipt.
		return math.Max(fee, 0) / quantity
	default:
		return math.Inf(1)
	}
}

func (s *Server) liveMirrorEdgeFloor() float64 {
	floor := s.cfg().Auto.MinEVPerContract
	if floor < 0.005 {
		floor = 0.005
	}
	return floor
}

// liveSystemVenueEnabled and liveNewMLVenueEnabled are destination-authority belts, not money
// authority by themselves. ARM + LIVE AUTO, the relevant proof contract, and every final venue
// check remain mandatory. Keeping the switches separate prevents a decision to canary one venue
// or one model from silently enabling the other three lanes.
func (s *Server) liveSystemVenueEnabled(platform string) bool {
	if !s.cfg().Risk.LiveProspectiveAllocation {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(platform)) {
	case "kalshi":
		return s.cfg().Risk.LiveSystemKalshi
	case "polyus":
		return s.cfg().Risk.LiveSystemPolyUS
	default:
		return false
	}
}

func (s *Server) liveNewMLVenueEnabled(platform string) bool {
	switch strings.ToLower(strings.TrimSpace(platform)) {
	case "kalshi":
		return s.cfg().Risk.LiveNewMLKalshi
	case "polyus":
		return s.cfg().Risk.LiveNewMLPolyUS
	default:
		return false
	}
}

const liveSystemAllowlistMax = 6

// parseLiveSystemAllowlist deliberately has no wildcard or aggregate syntax. A real-money System
// identity is one exact destination, economic family, fired side, and order route. Invalid config
// invalidates the entire belt instead of quietly ignoring the bad token and widening authority.
func parseLiveSystemAllowlist(raw string) (map[string]struct{}, []string, string) {
	parts := strings.FieldsFunc(raw, func(r rune) bool { return r == ',' || r == ';' || r == '\n' || r == '\r' })
	allowed := make(map[string]struct{}, len(parts))
	canonical := make([]string, 0, len(parts))
	for _, part := range parts {
		fields := strings.Split(strings.TrimSpace(part), "|")
		if len(fields) != 4 {
			return nil, nil, "live-system-allowlist-entry-must-be-venue|family|side|route"
		}
		venue := strings.ToLower(strings.TrimSpace(fields[0]))
		family := strings.ToLower(strings.TrimSpace(fields[1]))
		side := strings.ToUpper(strings.TrimSpace(fields[2]))
		route := strings.ToLower(strings.TrimSpace(fields[3]))
		if (venue != "kalshi" && venue != "polyus") || family == "" ||
			strings.HasPrefix(family, "counterfactual:") || strings.HasPrefix(family, "side-control:") ||
			strings.HasPrefix(family, "invert:invert:") || (side != "YES" && side != "NO") ||
			(route != "maker" && route != "taker") {
			return nil, nil, "live-system-allowlist-entry-invalid"
		}
		key := strings.Join([]string{venue, family, side, route}, "|")
		if _, duplicate := allowed[key]; duplicate {
			continue
		}
		allowed[key] = struct{}{}
		canonical = append(canonical, key)
		if len(canonical) > liveSystemAllowlistMax {
			return nil, nil, "live-system-allowlist-exceeds-six-identities"
		}
	}
	return allowed, canonical, ""
}

func liveSystemIdentityKey(c liveMirrorCandidate, route string) string {
	return strings.Join([]string{strings.ToLower(strings.TrimSpace(c.Platform)),
		strings.ToLower(strings.TrimSpace(liveMirrorFamily(c))), strings.ToUpper(strings.TrimSpace(c.Side)),
		strings.ToLower(strings.TrimSpace(route))}, "|")
}

func (s *Server) liveSystemIdentityAllowed(c liveMirrorCandidate, route string) (bool, string) {
	allowed, _, why := parseLiveSystemAllowlist(s.cfg().Risk.LiveSystemAllowlist)
	if why != "" {
		return false, why
	}
	if len(allowed) == 0 {
		return false, "live-system-allowlist-empty"
	}
	key := liveSystemIdentityKey(c, route)
	if _, ok := allowed[key]; !ok {
		return false, "live-system-identity-not-allowlisted:" + key
	}
	return true, ""
}

func liveSystemCanaryIdentitySupported(identity string) bool {
	switch identity {
	case "kalshi|spotlag|YES|taker",
		"kalshi|spotlag|NO|taker",
		"kalshi|kalshi-flow|YES|taker",
		"kalshi|kalshi-flow|NO|taker":
		return true
	default:
		return false
	}
}

// parseLiveSystemCanaryAllowlist is intentionally narrower than the normal System belt. Canary
// authority cannot add a route: every entry must already be in LiveSystemAllowlist, must be one
// of the four operator-selected Kalshi taker lanes, and has no wildcard/venue expansion syntax.
func parseLiveSystemCanaryAllowlist(raw, parentRaw string) (map[string]struct{}, []string, string) {
	canary, canonical, why := parseLiveSystemAllowlist(raw)
	if why != "" {
		return nil, nil, "live-system-canary-" + strings.TrimPrefix(why, "live-system-")
	}
	parent, _, parentWhy := parseLiveSystemAllowlist(parentRaw)
	if parentWhy != "" {
		return nil, nil, "live-system-canary-parent-allowlist-invalid"
	}
	for _, identity := range canonical {
		if !liveSystemCanaryIdentitySupported(identity) {
			return nil, nil, "live-system-canary-identity-not-supported"
		}
		if _, ok := parent[identity]; !ok {
			return nil, nil, "live-system-canary-identity-not-in-live-system-allowlist"
		}
	}
	return canary, canonical, ""
}

func (s *Server) liveSystemCanaryReason(c liveMirrorCandidate, route string) string {
	if why := s.liveSystemSelectionReason(c, route); why != "" {
		return why
	}
	allowed, _, why := parseLiveSystemCanaryAllowlist(
		s.cfg().Risk.LiveSystemCanaryAllowlist, s.cfg().Risk.LiveSystemAllowlist)
	if why != "" {
		return why
	}
	if len(allowed) == 0 {
		return "live-system-canary-allowlist-empty"
	}
	key := liveSystemIdentityKey(c, route)
	if _, ok := allowed[key]; !ok {
		return "live-system-identity-not-canary-allowlisted:" + key
	}
	return ""
}

// liveSystemSelectionReason is the exact operator-selected destination/family/side/route belt.
// Book subscription planning may consult it without granting proof or money authority; every
// write path consults it again after statistical/economic proof.
func (s *Server) liveSystemSelectionReason(c liveMirrorCandidate, route string) string {
	if !s.liveSystemVenueEnabled(c.Platform) {
		return "live-system-disabled-for-destination-venue"
	}
	if allowed, why := s.liveSystemIdentityAllowed(c, route); !allowed {
		return why
	}
	return ""
}

// liveSystemPostProofReason is the final operator-selection belt before a System reaches either
// venue write path. Keep the named boundary so callers cannot confuse selection with proof.
func (s *Server) liveSystemPostProofReason(c liveMirrorCandidate, route string) string {
	return s.liveSystemSelectionReason(c, route)
}

func liveMirrorUnitAdjusted(u storage.UnitTrialLeaderboardStat, currentPrice, currentFee float64) (state string, meanNow, loNow float64) {
	state = "COLLECTING"
	meanNow = u.MeanPC
	contractRadius := csRadius(u.SettledMarkets, u.SDPC)
	if !math.IsInf(contractRadius, 1) {
		loNow = u.MeanPC - contractRadius
	}
	// PAPER's edge and n are the distinct venue+ticker contract cohort, so its historical cost
	// baseline must be that exact cohort too. ProofMean* is a sparse canonical-event/30d research
	// subset; preferring it here could make an unrelated subset's cheap prices authorize a Paper
	// order. Sealed LIVE authorization is handled separately by liveMirrorProof.
	proofAsk, proofFee := u.MeanAsk, u.MeanFeePC
	costProofReady := u.SettledMarkets > 0 && proofAsk > 0
	penalty := 0.0
	if costProofReady {
		penalty = math.Max(0, currentPrice+currentFee-(proofAsk+proofFee))
	}
	meanNow -= penalty
	loNow -= penalty
	// This state is used for PAPER side selection only. It intentionally ignores the incomplete
	// canonical-event mapper; n is distinct venue+ticker contracts. LIVE still cannot consume this
	// rolling cohort directly (liveMirrorProof requires a sealed untouched Paper promotion).
	mature := costProofReady && u.SettledMarkets >= 20 && !math.IsInf(contractRadius, 1)
	if mature && loNow > 0 {
		state = "PROVEN+"
	} else if mature && u.MeanPC+contractRadius-penalty < 0 {
		state = "PROVEN-"
	}
	return state, meanNow, loNow
}

func (s *Server) liveAllocationMinMarkets() int {
	markets := s.cfg().Risk.LiveAllocationMinMarkets
	if markets < liveAllocationMinMarketsFloor {
		markets = liveAllocationMinMarketsFloor
	}
	return markets
}

func liveAllocationEconomicHistoryTopologyReason(family, platform, side, route string) string {
	// UnitTrial v1 is exact by family+destination+origin+side+route but predates the signal-contract
	// topology column. It is safe only when that identity has exactly one possible upstream path.
	// A current explicit receipt identifies what fired now; it cannot retroactively separate pooled
	// historical economics from another topology belonging to the same family cell.
	if len(r147ExactSignalContracts(family, platform, side, route)) != 1 {
		return "prospective-allocation-economic-history-mixes-multiple-input-topologies"
	}
	return ""
}

// liveAllocationEdgeFloor is deliberately separate from the general AUTO MinEV threshold. The
// prospective allocation lane's operator contract is a positive fee-net confidence lower
// bound, while normal AUTO may require a larger expected edge. Keeping the value explicit in
// config/API avoids silently weakening either policy. A posted zero/negative value still fails
// closed to the hard half-cent floor.
func (s *Server) liveAllocationEdgeFloor() float64 {
	floor := s.cfg().Risk.LiveAllocationMinEdge
	if floor < 0.005 {
		floor = 0.005
	}
	return floor
}

// liveMirrorDispatchProofFloor keeps the wire-bound all-in cap on the same authorization
// contract that admitted the candidate. Prospective allocation has its own explicit positive-LB
// threshold; accidentally reapplying normal AUTO's larger MinEV here would make every otherwise
// valid route fail one step after proof (or, in the opposite direction, could weaken a sealed
// route if the thresholds later diverge).
func (s *Server) liveMirrorDispatchProofFloor(prospectiveAllocation bool) float64 {
	if prospectiveAllocation {
		return s.liveAllocationEdgeFloor()
	}
	return s.liveMirrorEdgeFloor()
}

// liveAllocationWireAdjusted applies the same one-way conservatism at the final handler boundary:
// a worse wire all-in cost reduces both the mean and lower bound; a cheaper quote earns no new
// proof credit. Fees are per contract in both receipts.
func liveAllocationWireAdjusted(mean, lower, provedPrice, provedFee, wirePrice, wireFee float64) (float64, float64) {
	penalty := math.Max(0, wirePrice+wireFee-(provedPrice+provedFee))
	return mean - penalty, lower - penalty
}

// liveProspectiveAllocationProof is the normal-money boundary for the current exact strategy
// receipt. The underlying UnitTrial calculation remains observable, but those pre-IOC
// opportunities cannot authorize cash until an independent current-generation cohort proves
// authoritative LIVE fills, actual fill costs/fees, zero-fill rate, settlement, and
// dependence-aware time/event support.
func (s *Server) liveProspectiveAllocationProof(ctx context.Context, c liveMirrorCandidate, price float64, maker bool) (ok bool, basis string, meanNow, loNow, feePC float64) {
	return s.liveProspectiveAllocationProofAtQuantity(ctx, c, price, maker, 1)
}

func (s *Server) liveProspectiveAllocationProofAtQuantity(ctx context.Context,
	c liveMirrorCandidate, price float64, maker bool, quantity float64) (
	ok bool, basis string, meanNow, loNow, feePC float64) {
	ok, basis, meanNow, loNow, feePC =
		s.liveProspectiveDiagnosticProofAtQuantity(ctx, c, price, maker, quantity)
	if !ok {
		return ok, basis, meanNow, loNow, feePC
	}
	// A selected, executable hypothetical observation may keep its research statistics, but it is
	// not evidence that an IOC actually filled. Fail closed until an independent cohort joins
	// authoritative LIVE fills, actual fill cost/fee, terminal zero-fills, settlement, and
	// dependence-aware time/event blocks.
	return false, liveFillConditionedProofUnavailableReason, meanNow, loNow, feePC
}

func (s *Server) liveProspectiveDiagnosticProof(ctx context.Context,
	c liveMirrorCandidate, price float64, maker bool) (
	ok bool, basis string, meanNow, loNow, feePC float64) {
	return s.liveProspectiveDiagnosticProofAtQuantity(ctx, c, price, maker, 1)
}

// liveProspectiveDiagnosticProofAtQuantity grades the current-generation opportunity cohort for
// research and falsification only. Callers that can authorize money must use
// liveProspectiveAllocationProofAtQuantity, which adds the fill-conditioned promotion fence.
func (s *Server) liveProspectiveDiagnosticProofAtQuantity(ctx context.Context,
	c liveMirrorCandidate, price float64, maker bool, quantity float64) (
	ok bool, basis string, meanNow, loNow, feePC float64) {
	if !s.liveSystemVenueEnabled(c.Platform) {
		return false, "prospective-live-system-disabled-for-venue", 0, 0, 0
	}
	if quantity < 1 || quantity != math.Floor(quantity) {
		return false, "prospective-allocation-exact-whole-quantity-required", 0, 0, 0
	}
	if (c.Platform != "kalshi" && c.Platform != "polyus") || maker {
		return false, "prospective-allocation-kalshi-or-polyus-taker-singles-only", 0, 0, 0
	}
	action := strings.ToUpper(strings.TrimSpace(c.Action))
	if action == "" {
		action = "BUY"
	}
	if action != "BUY" {
		return false, "prospective-allocation-buy-singles-only", 0, 0, 0
	}
	if c.At.IsZero() || time.Since(c.At) < -2*time.Second || time.Since(c.At) > liveMirrorTTL {
		return false, "prospective-allocation-signal-receipt-stale", 0, 0, 0
	}
	family := liveMirrorFamily(c)
	if family == "" || strings.HasPrefix(family, "counterfactual:") || strings.HasPrefix(family, "side-control:") {
		return false, "prospective-allocation-exact-system-family-unavailable", 0, 0, 0
	}
	bound, contract, why := s.liveAllocationBindSignalContract(c, "taker", true)
	if why != "" {
		return false, why, 0, 0, 0
	}
	c = bound
	if why := liveAllocationEconomicHistoryTopologyReason(family, c.Platform, c.Side, "taker"); why != "" {
		return false, why, 0, 0, 0
	}
	if why := s.liveAllocationInputFreshReason(contract, c); why != "" {
		return false, why, 0, 0, 0
	}
	// Economic evidence is scoped to the current LIVE-priority execution generation as well as
	// family+destination+origin+side. Older Paper/general strategy rows remain reportable history,
	// but cannot authorize a route whose detector/admission/execution architecture differs.
	// SignalContractID remains the current-input freshness/identity belt inside that generation.
	economicFamily := family
	if s.store == nil {
		return false, "prospective-allocation-ledger-unavailable", 0, 0, 0
	}
	since := time.Now().UTC().Add(-liveMirrorTTL)
	if !s.liveAllocationSignalFresh(c, since) {
		return false, "prospective-allocation-no-fresh-exact-signal-receipt", 0, 0, 0
	}
	feePC = s.liveMirrorFeePCAtQuantity(c, false, quantity, price)
	if math.IsNaN(feePC) || math.IsInf(feePC, 0) || feePC < 0 {
		return false, "prospective-allocation-current-fee-unavailable", 0, 0, feePC
	}
	minMarkets := s.liveAllocationMinMarkets()
	floor := s.liveAllocationEdgeFloor()
	// Cash authority must come from the exact strategy stream firing now. Model-origin rows remain
	// useful discovery evidence, but they may combine signals and filters the exact strategy never
	// selected, so they cannot authorize or size this order.
	best, found, readErr := s.livePriorityUnitTrialRouteLeaderboard(
		ctx, time.Now().UTC(), economicFamily, c.Platform, "strategy", c.Side)
	if readErr != nil {
		return false, "prospective-allocation-route-history-read-failed", 0, 0, feePC
	}
	if !found || best.SettledMarkets < minMarkets {
		return false, fmt.Sprintf(
			"prospective-allocation-needs-%d-distinct-settled-exact-strategy-contracts",
			minMarkets), 0, 0, feePC
	}
	_, meanNow, loNow = liveMirrorUnitAdjusted(best, price, feePC)
	if math.IsNaN(meanNow) || math.IsInf(meanNow, 0) ||
		math.IsNaN(loNow) || math.IsInf(loNow, 0) || loNow < floor {
		return false, "prospective-allocation-current-cost-erases-confidence-lower-bound", meanNow, loNow, feePC
	}
	quantityBasis := ""
	if quantity > 1 {
		quantityBasis = fmt.Sprintf("q%.0f:", quantity)
	}
	basis = fmt.Sprintf("prospective-allocation:%s@%s[%s]/taker:%sinput=%s:contract=%s:%s:n%d:m%d-diagnostic:generation=%s",
		family, c.Platform, strings.ToUpper(c.Side), quantityBasis, c.InputTopology, c.SignalContractID, "strategy",
		best.SettledMarkets, best.SettledEventClusters, livePriorityProofGeneration)
	return true, basis, meanNow, loNow, feePC
}

// liveMirrorConfidence returns the fee-net edge used to authorize this one candidate. Live money
// is authorized only by the bankroll-neutral one-share lane: the same strategy side, on the same
// venue, observed prospectively at executable asks with exact fees. Signal/logger-price verdicts
// remain useful discovery evidence but cannot authorize cash. If today's all-in cost is worse than
// the historical unit lane's mean all-in cost, that entire difference is subtracted from both the
// mean and confidence lower bound; cheaper quotes receive no optimistic credit.
func (s *Server) liveMirrorProof(ctx context.Context, c liveMirrorCandidate, price float64, maker bool) (ok bool, basis string, meanNow, loNow, feePC float64) {
	if isNewMLV2LiveSource(c.Source) {
		return s.newMLV2LiveProof(c, price, maker)
	}
	if promoted, accepted := s.researchPromotionAccepted(ctx, c.Source); accepted {
		expectedRoute := "taker"
		if maker {
			expectedRoute = "maker"
		}
		if liveAutoPromotionCandidateReason(promoted, c, expectedRoute) != "" {
			return false, "sealed-promotion-contract-mismatch", 0, 0, 0
		}
		if why := s.researchPromotionCashAuthorityReason(ctx, promoted); why != "" {
			return false, why, 0, 0, 0
		}
		feePC = s.liveMirrorFeePC(c, maker, price)
		if math.IsNaN(feePC) || math.IsInf(feePC, 0) || price+feePC > promoted.MaxAllInUnit+1e-9 {
			return false, "current-all-in-cost-erases-sealed-edge", 0, promoted.Proof.LowerPC, feePC
		}
		// Size from the always-valid lower bound. The generic bridge remains one contract at the
		// dispatch layer until an independently sealed capacity curve exists.
		return true, "sealed-untouched-paper-accepted:" + promoted.Proof.SystemID + "@" + promoted.Proof.Venue,
			promoted.Proof.LowerPC, promoted.Proof.LowerPC, feePC
	}
	if liveMirrorMLSource(c.Source) {
		return false, "ml-live-disabled", 0, 0, 0
	}
	if strings.Contains(strings.ToLower(strings.TrimSpace(c.Source)), "proper-score") {
		return false, "proper-score-research-only", 0, 0, 0
	}
	if s.liveSystemVenueEnabled(c.Platform) {
		return s.liveProspectiveAllocationProof(ctx, c, price, maker)
	}
	// Native unit trials and maker-attempt scoreboards remain useful evidence generators, but they
	// are rolling/reused cohorts rather than an untouched preregistered decision. They may nominate
	// the next sealed experiment; they can never authorize cash directly.
	return false, "sealed-accepted-paper-intent-required", 0, 0, s.liveMirrorFeePC(c, maker, price)
}

func (s *Server) liveMirrorConfidence(ctx context.Context, c liveMirrorCandidate, price float64, maker bool) (bool, string, float64) {
	ok, basis, mean, _, _ := s.liveMirrorProof(ctx, c, price, maker)
	return ok, basis, mean
}

// liveMirrorProofDispatchFields binds the proof to a maximum wire-true all-in unit cost. If the
// venue handler refreshes/reprices the order, principal+exact fee per contract must remain at or
// below this ceiling. The spare room is exactly the always-valid lower bound above the configured
// live edge floor:
//
//	max all-in = proved price + proved fee + proved lower bound - required edge floor.
//
// The HTTP handlers consume max_all_in_unit immediately before their venue write. The remaining
// fields are an auditable receipt and are deliberately not alternative authorization inputs.
func liveMirrorProofDispatchFields(q liveMirrorQuote, basis string, meanNow, loNow, feePC, floor float64, checkedAt time.Time) (map[string]any, bool) {
	base := q.Price + feePC
	maxAllIn := base + loNow - floor
	if q.Price <= 0 || q.Price >= 1 || feePC < 0 || loNow < floor || floor <= 0 ||
		math.IsNaN(base) || math.IsInf(base, 0) || math.IsNaN(maxAllIn) || math.IsInf(maxAllIn, 0) ||
		maxAllIn+1e-12 < base {
		return nil, false
	}
	if checkedAt.IsZero() {
		checkedAt = time.Now()
	}
	return map[string]any{
		"max_all_in_unit":      maxAllIn,
		"mirror_proof_basis":   basis,
		"mirror_proof_mean":    meanNow,
		"mirror_proof_lower":   loNow,
		"mirror_proof_fee_pc":  feePC,
		"mirror_proof_floor":   floor,
		"mirror_proof_price":   q.Price,
		"mirror_proof_maker":   q.Maker,
		"mirror_proof_route":   q.Route,
		"mirror_proof_checked": checkedAt.UTC().Format(time.RFC3339Nano),
	}, true
}

// liveCanaryDispatchFields keeps the experimental order visibly separate from proof. Its wire cap
// preserves the positive point estimate at the normal edge floor, while the actual confidence
// lower bound is recorded unchanged even when it is negative.
func liveCanaryDispatchFields(q liveMirrorQuote, basis string, pointNow, observedLower,
	feePC, floor float64, checkedAt time.Time) (map[string]any, bool) {
	base := q.Price + feePC
	maxAllIn := base + pointNow - floor
	if q.Maker || q.Price <= 0 || q.Price >= 1 || feePC < 0 ||
		pointNow < floor || floor <= 0 ||
		math.IsNaN(base) || math.IsInf(base, 0) ||
		math.IsNaN(observedLower) || math.IsInf(observedLower, 0) ||
		math.IsNaN(maxAllIn) || math.IsInf(maxAllIn, 0) ||
		maxAllIn+1e-12 < base ||
		!strings.HasPrefix(basis, "one-contract-canary:UNPROVEN:") ||
		!strings.Contains(basis, ":point-source=exact-paper-model-cell:") {
		return nil, false
	}
	if checkedAt.IsZero() {
		checkedAt = time.Now()
	}
	return map[string]any{
		"max_all_in_unit":       maxAllIn,
		"canary_authority":      true,
		"canary_state":          "UNPROVEN_CANARY",
		"canary_basis":          basis,
		"canary_point_edge":     pointNow,
		"canary_observed_lower": observedLower,
		"canary_fee_pc":         feePC,
		"canary_edge_floor":     floor,
		"canary_checked":        checkedAt.UTC().Format(time.RFC3339Nano),
	}, true
}

func liveMirrorMergeFields(dst, src map[string]any) map[string]any {
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

// liveAutoTightenAllInCap lets an internal caller request a stricter ceiling, but never a looser
// one than the money handler derived from its own fresh proof. Invalid derived truth fails closed.
func liveAutoTightenAllInCap(requested, derived float64) (float64, bool) {
	if derived <= 0 || derived >= 1 || math.IsNaN(derived) || math.IsInf(derived, 0) {
		return 0, false
	}
	if requested > 0 && requested < derived && !math.IsNaN(requested) && !math.IsInf(requested, 0) {
		return requested, true
	}
	return derived, true
}

// liveMirrorSessionConflict is the no-feed-lag belt. These maps are process-session state, so any
// matching placement record blocks another mirror order even if the venue has already filled or
// canceled it and no longer reports a resting order. A restart clears the maps; the persisted
// mirror intent claim remains the restart-safe belt.
func (s *Server) liveMirrorSessionConflict(c liveMirrorCandidate) string {
	switch c.Platform {
	case "kalshi":
		s.pusPlMu.Lock()
		_, placed := s.kalPlaced[c.Ticker]
		if !placed { // tolerate case-only schema drift without relying on normalized map keys
			for ticker := range s.kalPlaced {
				if strings.EqualFold(strings.TrimSpace(ticker), c.Ticker) {
					placed = true
					break
				}
			}
		}
		s.pusPlMu.Unlock()
		if placed {
			return "existing-kalshi-session-placement"
		}
	case "polyus":
		placed := false
		s.pusPlMu.Lock()
		for _, rec := range s.pusPlaced {
			if strings.EqualFold(strings.TrimSpace(rec.slug), c.Ticker) {
				placed = true
				break
			}
		}
		s.pusPlMu.Unlock()
		if placed {
			return "existing-polyus-session-placement"
		}
		s.pusAmbigMu.Lock()
		_, ambiguous := s.pusAmbig[c.Ticker]
		if !ambiguous {
			for slug := range s.pusAmbig {
				if strings.EqualFold(strings.TrimSpace(slug), c.Ticker) {
					ambiguous = true
					break
				}
			}
		}
		s.pusAmbigMu.Unlock()
		if ambiguous {
			return "existing-polyus-ambiguous-session-placement"
		}
	}
	return ""
}

func liveMirrorKalshiStateConflict(c liveMirrorCandidate, positions []kalshi.MarketPosition, orders []kalshi.Order) string {
	for _, p := range positions {
		if strings.EqualFold(strings.TrimSpace(p.Ticker), c.Ticker) && p.PositionQty() != 0 {
			return "existing-kalshi-position"
		}
	}
	// GetOrders is requested with status=resting, so every returned row is venue-certified working
	// state even if a schema version omits/relabels the redundant Status field.
	for _, o := range orders {
		if strings.EqualFold(strings.TrimSpace(o.Ticker), c.Ticker) {
			return "existing-kalshi-resting-order"
		}
	}
	return ""
}

func liveMirrorPUSStateConflict(c liveMirrorCandidate, positions []polymarketus.PUSPosition, orders []polymarketus.PUSOrder) string {
	for _, p := range positions {
		if strings.EqualFold(strings.TrimSpace(p.Slug), c.Ticker) && !p.Expired && math.Abs(p.Net) > 1e-12 {
			return "existing-polyus-position"
		}
	}
	for _, o := range orders {
		if strings.EqualFold(strings.TrimSpace(o.Slug), c.Ticker) && o.Leaves > 1e-12 {
			return "existing-polyus-resting-order"
		}
	}
	return ""
}

// liveMirrorAccountGuard fails closed unless it can prove the target market is absent from real
// positions, resting orders, and this process's placement latches. Kalshi account truth is fetched
// fresh and concurrently. PolyUS reuses its deliberately throttled account cache plus the private
// order snapshot when healthy; stale/error cache state is unknown, therefore a refusal.
func (s *Server) liveMirrorAccountGuard(ctx context.Context, c liveMirrorCandidate) string {
	if why := s.liveMirrorSessionConflict(c); why != "" {
		return why
	}
	if s.store != nil {
		if why := s.r148PendingRiskTickerConflict(ctx, c.Platform, c.Ticker); why != "" {
			return why
		}
	}
	switch c.Platform {
	case "kalshi":
		if s.kal == nil {
			return "kalshi-account-client-unavailable"
		}
		var (
			positions []kalshi.MarketPosition
			orders    []kalshi.Order
			posErr    error
			ordErr    error
			wg        sync.WaitGroup
		)
		if snapshot, ok := r154KalshiAdmissionSnapshotFromContext(ctx); ok {
			positions, orders = snapshot.Positions(), snapshot.Orders()
		} else {
			wg.Add(2)
			go func() {
				defer wg.Done()
				positions, posErr = s.kal.GetPositions(ctx)
			}()
			go func() {
				defer wg.Done()
				orders, ordErr = s.kal.GetOrders(ctx)
			}()
			wg.Wait()
		}
		if posErr != nil {
			return "kalshi-position-read-unavailable"
		}
		if ordErr != nil {
			return "kalshi-order-read-unavailable"
		}
		if why := liveMirrorKalshiStateConflict(c, positions, orders); why != "" {
			return why
		}
		// Exact sibling-event/semantic exposure is checked once at the final event-header boundary
		// in handleLivePlace using this same carried account snapshot. Repeating it here performed
		// the same metadata and durable-ledger work twice without creating a newer account receipt.
		// Re-read the durable claim after the potentially slow account snapshot. This closes the
		// restart-safe same-ticker window if another dispatcher reserved while those calls ran.
		if s.store != nil {
			return s.r148PendingRiskTickerConflict(ctx, c.Platform, c.Ticker)
		}
		return ""

	case "polyus":
		if s.polyUSAuth == nil {
			return "polyus-account-client-unavailable"
		}
		privateOrders, privateOK := []polymarketus.PUSOrder(nil), false
		if s.pusPriv != nil {
			privateOrders, privateOK = s.pusPriv.Orders()
		}
		s.pusLiveMu.Lock()
		positions := append([]polymarketus.PUSPosition(nil), s.pusPosC...)
		cachedOrders := append([]polymarketus.PUSOrder(nil), s.pusOrdsC...)
		cacheAt, cacheErr := s.pusLiveAt, s.pusErrC
		s.pusLiveMu.Unlock()
		ttl := liveMirrorPUSRESTCacheTTL
		orders := cachedOrders
		if privateOK {
			ttl = liveMirrorPUSPushCacheTTL
			orders = privateOrders
		}
		age := time.Since(cacheAt)
		if cacheAt.IsZero() || age < 0 || age > ttl {
			return "polyus-account-cache-stale"
		}
		if strings.TrimSpace(cacheErr) != "" {
			return "polyus-account-cache-error"
		}
		if why := liveMirrorPUSStateConflict(c, positions, orders); why != "" {
			return why
		}
		if s.store != nil {
			return s.r148PendingRiskTickerConflict(ctx, c.Platform, c.Ticker)
		}
		return ""
	default:
		return "unsupported-live-mirror-platform"
	}
}

func (s *Server) liveMirrorClusterKey(platform, ticker, title string) string {
	key := strings.TrimSpace(s.legEventKey(platform, ticker, title))
	// Empty-title non-AEC PolyUS rows otherwise collapse to "polyus:" and falsely make the entire
	// venue one cluster. Unknown structure safely falls back to one market, never a venue-wide key.
	if key == "" || key == platform+":" {
		return platform + ":market:" + strings.ToLower(strings.TrimSpace(ticker))
	}
	return key
}

// kalshiRestingRisk prices one current-schema resting order. The bool is schema truth: callers
// handling real-money exposure must fail closed when the active order omits or contradicts its
// successor outcome_side/book_side pair or the corresponding side price.
func kalshiRestingRisk(o kalshi.Order) (float64, bool) {
	rem := o.RemainFP.Float()
	if rem <= 0 || math.IsNaN(rem) || math.IsInf(rem, 0) {
		return 0, false
	}
	side, known := o.CurrentRestingOutcomeSide()
	if !known {
		return 0, false
	}
	unit := o.YesPriceD.Float()
	if side == "no" {
		unit = o.NoPriceD.Float()
	}
	if unit <= 0 || unit >= 1 || math.IsNaN(unit) || math.IsInf(unit, 0) {
		return 0, false
	}
	risk := rem * unit
	if risk <= 0 || math.IsNaN(risk) || math.IsInf(risk, 0) {
		return 0, false
	}
	return risk, true
}

// liveMirrorClusterGuard caps correlated real-money exposure before the intent claim. The cap is a
// percentage of this venue's ARM bankroll, so a $1k book is not treated like the old $10 book.
// Kalshi games/events and crypto windows use legEventKey; PolyUS AEC games use their matchup slug.
func (s *Server) liveMirrorClusterGuard(ctx context.Context, c liveMirrorCandidate, cost float64) string {
	pct := s.cfg().Risk.LiveClusterCapPct
	cap, ok := liveRailLimit(s.liveRiskBankroll(c.Platform), pct, -1)
	if !ok {
		return "live-cluster-rail-unavailable"
	}
	want := s.liveMirrorClusterKey(c.Platform, c.Ticker, c.Title)
	exposure := 0.0
	switch c.Platform {
	case "kalshi":
		if s.kal == nil {
			return "kalshi-account-client-unavailable"
		}
		var (
			positions []kalshi.MarketPosition
			orders    []kalshi.Order
			posErr    error
			ordErr    error
			wg        sync.WaitGroup
		)
		if snapshot, ok := r154KalshiAdmissionSnapshotFromContext(ctx); ok {
			positions, orders = snapshot.Positions(), snapshot.Orders()
		} else {
			wg.Add(2)
			go func() { defer wg.Done(); positions, posErr = s.kal.GetPositions(ctx) }()
			go func() { defer wg.Done(); orders, ordErr = s.kal.GetOrders(ctx) }()
			wg.Wait()
		}
		if posErr != nil || ordErr != nil {
			return "kalshi-cluster-exposure-unavailable"
		}
		for _, p := range positions {
			if s.liveMirrorClusterKey("kalshi", p.Ticker, "") == want {
				exposure += math.Abs(p.ExposureUSD())
			}
		}
		for _, o := range orders {
			if s.liveMirrorClusterKey("kalshi", o.Ticker, "") == want {
				risk, known := kalshiRestingRisk(o)
				if !known {
					return "kalshi-cluster-exposure-unavailable"
				}
				exposure += risk
			}
		}
		restingByID, restingErr := kalshiRestingRiskByID(orders)
		if restingErr != nil {
			return "kalshi-cluster-exposure-unavailable"
		}
		_, pendingClusters, pendingErr := s.r148PendingRiskOverlay(ctx, "kalshi", restingByID)
		if pendingErr != nil {
			return "kalshi-pending-risk-unavailable"
		}
		exposure += pendingClusters[want]
	case "polyus":
		privateOrders, privateOK := []polymarketus.PUSOrder(nil), false
		if s.pusPriv != nil {
			privateOrders, privateOK = s.pusPriv.Orders()
		}
		s.pusLiveMu.Lock()
		positions := append([]polymarketus.PUSPosition(nil), s.pusPosC...)
		orders := append([]polymarketus.PUSOrder(nil), s.pusOrdsC...)
		cacheAt, cacheErr := s.pusLiveAt, s.pusErrC
		s.pusLiveMu.Unlock()
		ttl := liveMirrorPUSRESTCacheTTL
		if privateOK {
			orders, ttl = privateOrders, liveMirrorPUSPushCacheTTL
		}
		age := time.Since(cacheAt)
		if cacheAt.IsZero() || age < 0 || age > ttl || strings.TrimSpace(cacheErr) != "" {
			return "polyus-cluster-exposure-unavailable"
		}
		for _, p := range positions {
			if !p.Expired && s.liveMirrorClusterKey("polyus", p.Slug, p.Title) == want {
				exposure += math.Abs(p.Cost)
			}
		}
		restingByID := map[string]float64{}
		for _, o := range orders {
			if strings.EqualFold(o.Action, "BUY") && o.Leaves > 0 && s.liveMirrorClusterKey("polyus", o.Slug, o.Title) == want {
				exposure += o.Leaves * o.Price
			}
			if strings.EqualFold(o.Action, "BUY") && o.Leaves > 0 && strings.TrimSpace(o.ID) != "" {
				restingByID[o.ID] = o.Leaves * o.Price
			}
		}
		_, pendingClusters, pendingErr := s.r148PendingRiskOverlay(ctx, "polyus", restingByID)
		if pendingErr != nil {
			return "polyus-pending-risk-unavailable"
		}
		exposure += pendingClusters[want]
	default:
		return "unsupported-live-mirror-platform"
	}
	if exposure+cost > cap+0.001 {
		return fmt.Sprintf("event-cluster risk $%.2f + $%.2f exceeds %.0f%% venue rail $%.2f", exposure, cost, pct*100, cap)
	}
	return ""
}

// snapshotStrategyComboLegs values only recent Kalshi-book decisions whose own per-venue
// always-valid lower bound remains positive after current taker fees. The lower bound—not the
// ML model mean—becomes each leg's conservative fair probability.
func (s *Server) snapshotStrategyComboLegs(ctx context.Context, legs []liveComboValueLeg) ([]liveComboLegQuote, string) {
	if !validParlayLegCount(len(legs)) {
		return nil, fmt.Sprintf("combo needs 2-%d legs", parlayLegLimit)
	}
	recent := s.recentLiveMirrorComboCandidates()
	byKey := make(map[string]liveMirrorCandidate, len(recent))
	for _, c := range recent {
		byKey[c.Ticker+"|"+strings.ToUpper(c.Side)] = c
	}
	out := make([]liveComboLegQuote, 0, len(legs))
	for _, l := range legs {
		side := strings.ToUpper(strings.TrimSpace(l.Side))
		c, found := byKey[l.Ticker+"|"+side]
		if !found {
			return nil, "no recent proven Kalshi-book decision for leg " + l.Ticker
		}
		ask, depth, source, quoteOK := s.executableAsk(ctx, "kalshi", l.Ticker, side, true)
		if !quoteOK || ask <= 0 || ask >= 1 || depth <= 0 {
			return nil, "no fresh executable ask with visible size for strategy leg " + l.Ticker
		}
		proved, basis, _, lo, feePC := s.liveMirrorProof(ctx, c, ask, false)
		if !proved || lo <= 0 || math.IsInf(feePC, 0) || math.IsNaN(feePC) {
			if basis == "" {
				basis = "strategy proof unavailable"
			}
			return nil, basis + " for strategy leg " + l.Ticker
		}
		pwin := ask + feePC + lo
		if pwin >= 0.995 {
			pwin = 0.995
		}
		if pwin <= ask || pwin >= 1 {
			return nil, "invalid lower-bound fair probability for strategy leg " + l.Ticker
		}
		out = append(out, liveComboLegQuote{Ticker: l.Ticker, Side: side, Ask: ask, Depth: depth, PWin: pwin, Source: source + "/" + basis})
	}
	if _, _, ok := liveStrategyComboProducts(out); !ok {
		return nil, "strategy marginal lower bounds do not produce a positive Frechet joint lower bound"
	}
	return out, ""
}

type liveMirrorQuote struct {
	Price, Depth, Tick, MinQty float64
	SpreadCents                float64
	Maker                      bool
	Route, BookSource          string
	MinQtyKnown                bool
	SourceAt, ObservedAt       time.Time
	// CheckedAt is the local instant when the current socket generation and complete ladder were
	// read. ObservedAt remains the last market-specific venue frame receipt for audit provenance;
	// a quiet unchanged book can be older while its multiplexed socket is demonstrably current.
	CheckedAt time.Time
}

func (q liveMirrorQuote) fresh(now time.Time, maxAge time.Duration) bool {
	checkedAt := q.CheckedAt
	if checkedAt.IsZero() {
		checkedAt = q.ObservedAt // compatibility for durable/older test receipts
	}
	if checkedAt.IsZero() || now.IsZero() || maxAge <= 0 {
		return false
	}
	age := now.Sub(checkedAt)
	return age >= 0 && age <= maxAge
}

// liveMirrorStableRouteIdentity removes the queue/rate/ETA telemetry that routeDecision.Tag adds
// after the stable routing reason. Those observations can change between two identical taker
// decisions; they describe the route but are not themselves execution identity.
func liveMirrorStableRouteIdentity(route string) string {
	route = strings.TrimSpace(route)
	telemetry := strings.LastIndex(route, "(q=")
	if telemetry <= 0 || !strings.HasSuffix(route, ")") {
		return route
	}
	suffix := route[telemetry:]
	if !strings.Contains(suffix, ",r=") || !strings.Contains(suffix, ",eta=") ||
		!strings.Contains(suffix, ",hz=") {
		return route
	}
	return strings.TrimSpace(route[:telemetry])
}

func liveMirrorQuoteEvidence(q liveMirrorQuote, checkedAt time.Time) map[string]any {
	observedAt := ""
	ageMS := -1.0
	if !q.ObservedAt.IsZero() {
		observedAt = q.ObservedAt.UTC().Format(time.RFC3339Nano)
		if !checkedAt.IsZero() {
			ageMS = float64(checkedAt.Sub(q.ObservedAt)) / float64(time.Millisecond)
		}
	}
	out := map[string]any{
		"price": q.Price, "depth": q.Depth, "tick": q.Tick,
		"spread_cents": q.SpreadCents, "maker": q.Maker,
		"route_identity": liveMirrorStableRouteIdentity(q.Route),
		"route_tag":      q.Route, "book_source": q.BookSource,
		"minimum_quantity": q.MinQty, "minimum_quantity_known": q.MinQtyKnown,
		"source_at":   optionalExecutionShadowTimeText(q.SourceAt),
		"observed_at": observedAt, "checked_at": optionalExecutionShadowTimeText(q.CheckedAt),
		"age_ms": ageMS,
	}
	// BookSource is human-readable provenance. Persist typed resident-book identity too so a later
	// fillability audit never has to recover generation, subscription, or sequence by parsing it.
	generation, subscriptionID, sequence := executionShadowQuoteReceipt(q)
	if generation != nil {
		out["book_generation"] = *generation
	}
	if subscriptionID != nil {
		out["book_subscription_id"] = *subscriptionID
	}
	if sequence != nil {
		out["book_sequence"] = *sequence
	}
	return out
}

func optionalExecutionShadowTimeText(at time.Time) string {
	if at.IsZero() {
		return ""
	}
	return at.UTC().Format(time.RFC3339Nano)
}

func liveMirrorHandlerQuoteChangeReason(prior, current liveMirrorQuote, quantity float64) string {
	return liveMirrorHandlerQuoteChangeReasonForTIF(prior, current, quantity, true)
}

func liveMirrorHandlerQuoteChangeReasonForTIF(prior, current liveMirrorQuote, quantity float64,
	requireFullDepth bool) string {
	if !current.fresh(time.Now(), liveMirrorHandlerQuoteMaxAge) {
		return "refreshed-executable-quote-is-not-current"
	}
	if current.Maker != prior.Maker ||
		liveMirrorStableRouteIdentity(current.Route) != liveMirrorStableRouteIdentity(prior.Route) {
		return "refreshed-executable-route-changed"
	}
	if math.Abs(current.Price-prior.Price) > 1e-9 {
		return "refreshed-executable-price-changed"
	}
	if quantity <= 0 || (requireFullDepth && current.Depth+1e-9 < quantity) {
		return "refreshed-executable-depth-fell-below-request"
	}
	return ""
}

func liveMirrorPaperChaseRequired(sealedPromotion, newML, prospectiveSelected bool) bool {
	return sealedPromotion || newML || !prospectiveSelected
}

func kalshiFullBookSides(book *kalshi.Orderbook, side string) (makerPx, makerDepth, takerPx, takerDepth float64, ok bool) {
	if book == nil || len(book.YesBids) == 0 || len(book.YesAsks) == 0 {
		return 0, 0, 0, 0, false
	}
	if strings.EqualFold(side, "YES") {
		return book.YesBids[0].Price, book.YesBids[0].Size, book.YesAsks[0].Price, book.YesAsks[0].Size,
			book.YesBids[0].Size > 0 && book.YesAsks[0].Size > 0
	}
	if strings.EqualFold(side, "NO") {
		return 1 - book.YesAsks[0].Price, book.YesAsks[0].Size, 1 - book.YesBids[0].Price,
			book.YesBids[0].Size, book.YesBids[0].Size > 0 && book.YesAsks[0].Size > 0
	}
	return 0, 0, 0, 0, false
}

func polyUSFullBookSides(bids, asks []polymarketus.BookLevel, side string) (makerPx, makerDepth, takerPx, takerDepth float64, ok bool) {
	if len(bids) == 0 || len(asks) == 0 {
		return 0, 0, 0, 0, false
	}
	if strings.EqualFold(side, "YES") {
		return bids[0].Price, bids[0].Quantity, asks[0].Price, asks[0].Quantity,
			bids[0].Quantity > 0 && asks[0].Quantity > 0
	}
	if strings.EqualFold(side, "NO") {
		return 1 - asks[0].Price, asks[0].Quantity, 1 - bids[0].Price, bids[0].Quantity,
			bids[0].Quantity > 0 && asks[0].Quantity > 0
	}
	return 0, 0, 0, 0, false
}

// liveMirrorExecutable independently refreshes lifecycle and both executable touches from resident
// WebSocket truth. Kalshi pairs one current-generation sequence-stamped full ladder with the
// lifecycle-patched complete-board cache. PolyUS pairs one current-generation explicitly-OPEN full
// MARKET_DATA frame with immutable rules from the latest complete crawl. Neither branch performs
// inline metadata or order-book REST.
func (s *Server) liveMirrorExecutable(ctx context.Context, c liveMirrorCandidate) (liveMirrorQuote, string) {
	checkedAt := time.Now()
	makerPx, makerDepth, takerPx, takerDepth, makerTick, takerTick, bookSource := 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, ""
	bookSourceAt, bookObservedAt := time.Time{}, time.Time{}
	minQty, minQtyKnown := 1.0, c.Platform == "kalshi"
	switch c.Platform {
	case "kalshi":
		readBook := s.r159KalshiWSExecutableBook
		if s.kalshiExecutableBookFn != nil {
			readBook = s.kalshiExecutableBookFn
		}
		receipt, why := readBook(c)
		if why != "" {
			return liveMirrorQuote{}, why
		}
		makerPx, makerDepth = receipt.MakerPrice, receipt.MakerDepth
		takerPx, takerDepth = receipt.TakerPrice, receipt.TakerDepth
		makerTick, takerTick = receipt.MakerTick, receipt.TakerTick
		bookSourceAt, bookObservedAt = receipt.SourceAt, receipt.ReceivedAt
		bookSource = receipt.Source
	case "polyus":
		if s.polyUSWS == nil {
			return liveMirrorQuote{}, "polyus-ws-unavailable"
		}
		receipt, fullOK := s.polyUSWS.CurrentOpenFullBook(c.Ticker)
		if !fullOK {
			return liveMirrorQuote{}, "no-current-generation-explicitly-open-full-book"
		}
		bookSourceAt, bookObservedAt = receipt.SourceAt, receipt.ReceivedAt
		var sidesOK bool
		makerPx, makerDepth, takerPx, takerDepth, sidesOK =
			polyUSFullBookSides(receipt.Bids, receipt.Asks, c.Side)
		if !sidesOK || s.polyUSAuth == nil {
			return liveMirrorQuote{}, "invalid-full-book-or-auth-unavailable"
		}
		rules, _, rulesOK := s.polyUSLiveMarketRules(c.Ticker)
		if !rulesOK {
			return liveMirrorQuote{}, "current-polyus-complete-crawl-rules-unavailable"
		}
		makerTick, takerTick, bookSource = rules.TickSize, rules.TickSize,
			"polyus_ws_current_generation_open_full_market_data"
		minQty, minQtyKnown = rules.MinimumQty, true
	default:
		return liveMirrorQuote{}, "unsupported-live-mirror-platform"
	}
	if makerPx <= 0 || makerPx >= 1 || takerPx <= 0 || takerPx >= 1 || takerPx+1e-9 < makerPx {
		return liveMirrorQuote{}, "invalid-executable-book"
	}
	var rd routeDecision
	prospectiveSelected, sealedSource, newMLSource := false, false, false
	if promoted, accepted := s.researchPromotionAccepted(ctx, c.Source); accepted {
		sealedSource = true
		rd = routeDecision{Maker: promoted.Proof.Route == "maker",
			Reason: "sealed-identical-" + promoted.Proof.Route, Queue: -1}
	} else if cap, accepted := s.newMLV2Capability(c, ""); accepted {
		newMLSource = true
		rd = routeDecision{Maker: cap.route == "maker",
			Reason: "current-new-ml-identical-" + cap.route, Queue: -1}
	} else if s.liveSystemVenueEnabled(c.Platform) && liveMirrorFamily(c) != "" {
		// Prospective allocation is deliberately taker-only. Maker proof and queue probability are
		// separate experiments and may never leak into this route. Do not call the generic router
		// first: its fallback horizon resolver may perform a public market GET whose result is then
		// discarded by this fixed taker contract.
		rd = routeDecision{Maker: false, Reason: "prospective-allocation-taker", Queue: -1}
		prospectiveSelected = s.liveSystemSelectionReason(c, "taker") == ""
	} else {
		rd = s.routeMakerTaker(ctx, c.Platform, c.Ticker, c.Side, makerPx,
			"live-mirror:"+c.Source)
	}
	px := makerPx
	depth := makerDepth
	tick := makerTick
	if !rd.Maker {
		px = takerPx
		depth = takerDepth
		tick = takerTick
	}
	slip := s.cfg().Auto.ConsensusMaxEntrySlipCents
	if slip <= 0 {
		slip = 3
	}
	// Dormant sealed-Paper and New-ML diagnostics retain their route-identical price comparison,
	// although R165 blocks both from cash. Prospective diagnostics are different:
	// its proof is recomputed from this exact current ask and fee, and any deterioration is charged
	// to the confidence lower bound. Rejecting an allowlisted prospective route here solely because
	// it moved more than the Paper chase duplicated (and could pre-empt) the stricter economic test.
	if liveMirrorPaperChaseRequired(sealedSource, newMLSource, prospectiveSelected) &&
		px > c.Price+slip/100+1e-9 {
		return liveMirrorQuote{}, "moved-off-paper-decision"
	}
	if depth <= 0 || tick <= 0 {
		return liveMirrorQuote{}, "full-book-depth-or-tick-unavailable"
	}
	spreadCents := math.Max(0, (takerPx-makerPx)*100)
	if prospectiveSelected && spreadCents > gfSpreadCapC {
		return liveMirrorQuote{}, "current-spread-too-wide"
	}
	if bookObservedAt.IsZero() {
		return liveMirrorQuote{}, "executable-book-observation-time-unavailable"
	}
	return liveMirrorQuote{Price: px, Depth: depth, Tick: tick, Maker: rd.Maker,
		Route: rd.Tag(), BookSource: bookSource, MinQty: minQty, MinQtyKnown: minQtyKnown,
		SpreadCents: spreadCents, SourceAt: bookSourceAt, ObservedAt: bookObservedAt,
		CheckedAt: checkedAt}, ""
}

func liveMirrorClaimKey(c liveMirrorCandidate) string {
	sum := sha256.Sum256([]byte(c.key()))
	return liveMirrorClaimPref + hex.EncodeToString(sum[:16])
}

// claimLiveMirrorIntent is written before invoking a venue handler.  A crash in the tiny
// claim->submit window can skip one order but can never duplicate it after restart (safe direction).
func (s *Server) claimLiveMirrorIntent(ctx context.Context, c liveMirrorCandidate) bool {
	now := time.Now()
	key := liveMirrorClaimKey(c)
	s.liveMirrorMu.Lock()
	defer s.liveMirrorMu.Unlock()
	if s.liveMirrorClaim == nil {
		s.liveMirrorClaim = map[string]time.Time{}
	}
	if at, ok := s.liveMirrorClaim[key]; ok && now.Sub(at) < liveMirrorClaimTTL {
		return false
	}
	if s.store != nil {
		if raw, ok := s.store.KVGet(ctx, key); ok {
			at, err := time.Parse(time.RFC3339Nano, raw)
			if err != nil || now.Sub(at) < liveMirrorClaimTTL {
				return false
			}
		}
		if err := s.store.KVSet(ctx, key, now.UTC().Format(time.RFC3339Nano)); err != nil {
			return false // restart safety cannot be proven, so no live submit
		}
	}
	s.liveMirrorClaim[key] = now
	return true
}

// liveMirrorHandlerProvesNoVenueCall is intentionally Kalshi-only. handleLivePlace returns every
// post-CreateOrder result (success, clean reject, or transport ambiguity) as HTTP 200 with an
// execution state. Its non-200 responses are therefore proven pre-submit refusals. PolyUS has a
// different response contract (including post-submit 502s), so it may never use this shortcut.
func liveMirrorHandlerProvesNoVenueCall(platform string, code int, res map[string]any) bool {
	if attempted, explicit := res["venue_attempted"].(bool); explicit {
		return !attempted
	}
	if !strings.EqualFold(strings.TrimSpace(platform), "kalshi") || code == http.StatusOK {
		return false
	}
	state, _ := res["execution_state"].(string)
	return strings.TrimSpace(state) == ""
}

func liveKalshiTerminalZeroFill(state string, authoritative bool, filled, remaining float64) bool {
	return state == "unfilled" && authoritative &&
		filled >= -1e-9 && filled <= 1e-9 && !math.IsNaN(filled) && !math.IsInf(filled, 0) &&
		remaining >= -1e-9 && !math.IsNaN(remaining) && !math.IsInf(remaining, 0)
}

// releaseLiveMirrorPreSubmitClaim makes a later fresh signal retryable only after the Kalshi
// handler proved that it never called the venue. Persistence is expired before memory is cleared;
// a storage failure leaves the claim latched in the safer skip-one direction.
func (s *Server) releaseLiveMirrorPreSubmitClaim(ctx context.Context, c liveMirrorCandidate) bool {
	key := liveMirrorClaimKey(c)
	if s.store != nil {
		releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		err := s.store.KVSet(releaseCtx, key, time.Unix(0, 0).UTC().Format(time.RFC3339Nano))
		cancel()
		if err != nil {
			return false
		}
	}
	s.liveMirrorMu.Lock()
	delete(s.liveMirrorClaim, key)
	s.liveMirrorMu.Unlock()
	return true
}

func liveMirrorApplyPolyUSMinimum(count, minQty, unitRisk, sizedUSD float64,
	metaKnown, sealedOneUnit bool) (float64, string) {
	if sealedOneUnit && !metaKnown {
		return 0, "sealed-one-unit-proof-requires-current-polyus-minimum"
	}
	if !metaKnown || minQty <= count {
		return count, ""
	}
	if sealedOneUnit {
		return 0, "sealed-one-unit-proof-below-venue-minimum"
	}
	if minQty*unitRisk > sizedUSD+1e-9 {
		return 0, "minimum-order-exceeds-sized-stake"
	}
	return minQty, ""
}

func liveMirrorFullDepthAllows(maker bool, depth, count float64) bool {
	if depth <= 0 || count <= 0 {
		return false
	}
	return maker || depth+1e-9 >= count
}

// liveMirrorSizedCount converts the destination-venue balance-aware Adaptive Allocation Model dollar result into a
// capacity-safe whole-contract order. Both taker and maker routes are capped by the fresh selected
// touch depth and by the evidence contract: sealed systems use their governed capacity; New ML
// uses the contracts in the exact originating Paper order/fill. A venue minimum may never round an
// order above any of those three limits.
func liveMirrorSizedCount(usd, unitRisk, freshDepth, proofUnits, minQty float64, minKnown bool) (float64, string) {
	vals := []float64{usd, unitRisk, freshDepth, proofUnits}
	for _, v := range vals {
		if v <= 0 || math.IsNaN(v) || math.IsInf(v, 0) {
			return 0, "invalid-balance-depth-or-proof-capacity"
		}
	}
	balanceUnits := math.Floor(usd / unitRisk)
	capacityUnits := math.Floor(math.Min(freshDepth, proofUnits))
	count := math.Min(balanceUnits, capacityUnits)
	if count < 1 {
		return 0, "balance-depth-or-proof-capacity-below-one-contract"
	}
	if minKnown {
		if minQty <= 0 || math.IsNaN(minQty) || math.IsInf(minQty, 0) {
			return 0, "current-venue-minimum-invalid"
		}
		if count+1e-9 < minQty {
			return 0, "current-venue-minimum-exceeds-balance-depth-or-proof-capacity"
		}
	}
	return count, ""
}

// liveProspectiveAllocationSize applies the shared Adaptive Allocation Model, then limits its
// dollars to quantity actually executable at the current exact touch. The historical evidence is
// per-contract, so current same-price depth is the honest capacity boundary; it makes no claim
// about walking the book. Existing global contract, 5% per-order, 10% cluster, and 50% exposure
// rails can only tighten this result (the latter two are enforced immediately after sizing).
func (s *Server) liveProspectiveAllocationSize(venue string, bank, price, proofMean, proofLower, feePC,
	freshDepth, minQty float64, minKnown bool) (count, usd, effectiveFrac, capacityUnits float64, why string) {
	usd, effectiveFrac = s.liveProofOrderUSDForFloor(venue, bank, price, proofMean, proofLower, s.liveAllocationEdgeFloor())
	if usd <= 0 {
		return 0, usd, effectiveFrac, 0, "adaptive-allocation-cannot-size"
	}
	capacityUnits = freshDepth
	s.riskMu.Lock()
	maxContracts := s.maxContracts
	s.riskMu.Unlock()
	if maxContracts > 0 {
		capacityUnits = math.Min(capacityUnits, float64(maxContracts))
	}
	unitRisk := price + math.Max(feePC, 0)
	count, why = liveMirrorSizedCount(usd, unitRisk, freshDepth, capacityUnits, minQty, minKnown)
	return
}

func (s *Server) dispatchLiveMirror(ctx context.Context, c liveMirrorCandidate) (bool, string) {
	if strings.EqualFold(strings.TrimSpace(c.Platform), "kalshi") {
		ctx = r159KalshiResidentAdmissionContext(ctx)
	}
	if !s.liveMirrorEnabled() {
		return false, "auto-live-off"
	}
	if why := s.liveIntentGenerationReason(c.LiveIntentGeneration,
		c.LiveIntentGenerationBound); why != "" {
		return false, why
	}
	if time.Since(c.At) > liveMirrorTTL {
		return false, "stale-candidate"
	}
	// A typed proper-score momentum source owns exactly one accepted reduce-only SELL. It cannot
	// fall through into the four-System entry allowlist, and the special branch cannot run before
	// the current AUTO generation and signal lease have passed.
	if storage.ProperMomentumSellIntentFromSource(c.Source) != "" {
		if !strings.EqualFold(strings.TrimSpace(c.Action), "SELL") {
			return false, "proper-score-momentum-source-has-no-entry-authority"
		}
		// Paper evidence remains active, but this LIVE handoff is parked until its original trigger
		// clock and AUTO generation can be carried immutably through every retry.
		if !properMomentumLiveSellEnabled() {
			return false, properMomentumLiveDisabledReason
		}

		if why := s.liveIntentGenerationGateReason(true, c.LiveIntentGeneration,
			c.LiveIntentGenerationBound, true); why != "" {
			return false, why
		}
		if s.store == nil {
			return false, "proper-score-momentum-intent-store-unavailable"
		}
		in, accepted, err := s.store.AcceptedProperMomentumSellIntent(ctx, c.Source)
		if err != nil {
			return false, "proper-score-momentum-intent-read-unavailable"
		}
		if !accepted {
			return false, "proper-score-momentum-paper-acceptance-unavailable"
		}
		return s.dispatchProperMomentumLiveSell(ctx, c, in)
	}
	promoted, sealedPromotion := s.researchPromotionAccepted(ctx, c.Source)
	_, newMLV2 := s.newMLV2Capability(c, "")
	prospectiveAllocation := s.liveSystemVenueEnabled(c.Platform) &&
		!liveMirrorMLSource(c.Source) && !strings.Contains(strings.ToLower(strings.TrimSpace(c.Source)), "proper-score")
	if !sealedPromotion && !newMLV2 && !prospectiveAllocation {
		return false, "sealed-accepted-paper-intent-required"
	}
	// Every in-process AUTO candidate is bound at queue admission. Sealed proof remains economic
	// authority, not permission to cross an operator OFF/ON boundary.
	if why := s.liveIntentGenerationGateReason(true, c.LiveIntentGeneration,
		c.LiveIntentGenerationBound, true); why != "" {
		return false, why
	}
	if sealedPromotion {
		if why := liveAutoPromotionCandidateReason(promoted, c, promoted.Proof.Route); why != "" {
			return false, "sealed-promotion-contract-mismatch:" + why
		}
		if why := s.liveAutoPromotionCurrentIdentityReason(ctx, promoted); why != "" {
			return false, "sealed-promotion-current-identity:" + why
		}
		if why := s.researchPromotionCashAuthorityReason(ctx, promoted); why != "" {
			return false, why
		}
	}
	if prospectiveAllocation && !sealedPromotion && !newMLV2 {
		bound, _, why := s.liveAllocationBindSignalContract(c, "taker", true)
		if why != "" {
			return false, why
		}
		c = bound
	}
	if why := s.liveR148CrossVenueVariantReason(ctx, c); why != "" {
		return false, why
	}
	// Arbitration has already paid for an executable quote and statistical proof. Reuse that
	// sub-second read-only receipt for sizing on Kalshi; handleLivePlace independently owns the
	// final fresh book/proof/account pass immediately before the wire. This removes the middle of
	// three formerly serial quote/proof cycles without weakening the money boundary.
	q, basis, edge, proofLo, proofFee := c.ArbitrationQuote, c.ArbitrationBasis,
		c.ArbitrationMean, c.ArbitrationLower, c.ArbitrationFee
	reusePreflight := c.Platform == "kalshi" &&
		s.liveMirrorArbitrationReceiptReusable(c, time.Now(), 750*time.Millisecond)
	if !reusePreflight {
		if !s.paperEntryHorizonNow(ctx, c.Platform, c.Ticker, c.Title) {
			return false, "current-funded-entry-horizon-unavailable-or-exceeded"
		}
		var why string
		q, why = s.liveMirrorExecutable(ctx, c)
		if why != "" {
			return false, why
		}
	}
	var bank float64
	var bankSrc string
	if newMLV2 {
		bank, bankSrc = s.newMLV2LiveSizingBankrollFor(ctx, c.Platform, c.Source)
	} else if c.Platform == "kalshi" {
		bank, bankSrc = s.liveCachedSizingBankroll(c.Platform)
	} else {
		bank, bankSrc = s.liveSizingBankroll(ctx, c.Platform)
	}
	var usd, effectiveKelly, count, proofUnits float64
	routeName := "taker"
	if q.Maker {
		routeName = "maker"
	}
	if prospectiveAllocation {
		cell, cellOK := s.gfModeCached(liveMirrorFamily(c), c.Platform, c.Side)
		if !cellOK {
			return false, "current-exact-system-verdict-unavailable-or-stale"
		}
		plan, planWhy := s.liveProspectivePlan(ctx, c, q, bank, cell, false)
		if planWhy != "" || !plan.valid() {
			if planWhy == "" {
				planWhy = "prospective-allocation-no-self-consistent-exact-quantity"
			}
			return false, planWhy + ":" + bankSrc
		}
		applyLiveProspectivePlan(&c, plan)
		basis, edge, proofLo, proofFee = plan.Basis, plan.Mean, plan.Lower, plan.ProofFeePC
		count, usd, effectiveKelly, proofUnits =
			plan.Qty, plan.TargetUSD, plan.EffectiveFrac, plan.Capacity
	} else {
		if !reusePreflight {
			ok, proofBasis, proofMean, proofLower, currentFee :=
				s.liveMirrorProof(ctx, c, q.Price, q.Maker)
			if !ok {
				return false, proofBasis
			}
			basis, edge, proofLo, proofFee =
				proofBasis, proofMean, proofLower, currentFee
		}
		usd, effectiveKelly = s.liveProofOrderUSD(c.Platform, bank, q.Price, edge, proofLo)
		if usd <= 0 {
			return false, "proof-kelly-cannot-size:" + bankSrc
		}
	}
	canaryAllocation := c.Canary
	prospectiveAllocation = strings.HasPrefix(basis, "prospective-allocation:") || canaryAllocation
	if !newMLV2 {
		// The exact identity belt applies even to an older sealed System promotion. A sealed
		// statistical proof is not operator permission to trade an unselected family or side.
		route := "taker"
		if q.Maker {
			route = "maker"
		}
		if why := s.liveSystemPostProofReason(c, route); why != "" {
			return false, why
		}
	}
	var proofFields map[string]any
	var proofOK bool
	if canaryAllocation {
		proofFields, proofOK = liveCanaryDispatchFields(q, basis, edge, proofLo, proofFee,
			s.liveAllocationEdgeFloor(), time.Now())
	} else {
		proofFields, proofOK = liveMirrorProofDispatchFields(q, basis, edge, proofLo, proofFee,
			s.liveMirrorDispatchProofFloor(prospectiveAllocation), time.Now())
	}
	if !proofOK {
		return false, "invalid-proof-dispatch-cap"
	}
	if capNow, ok := proofFields["max_all_in_unit"].(float64); sealedPromotion && (!ok || capNow > promoted.MaxAllInUnit) {
		proofFields["max_all_in_unit"] = promoted.MaxAllInUnit
	}
	unitRisk := q.Price + math.Max(proofFee, 0)
	if sealedPromotion {
		proofUnits = promoted.Governance.Capacity
	} else if cap, ok := s.newMLV2Capability(c, routeName); ok {
		proofUnits = cap.paperUnits
	}
	if !prospectiveAllocation {
		var countWhy string
		count, countWhy = liveMirrorSizedCount(usd, unitRisk, q.Depth, proofUnits, q.MinQty, q.MinQtyKnown)
		if countWhy != "" {
			return false, countWhy
		}
	}
	proofFields["full_book_depth"] = q.Depth
	proofFields["proof_capacity_units"] = proofUnits
	if prospectiveAllocation {
		proofFields["proof_capacity_basis"] = "current-executable-touch-depth"
		proofFields["mirror_input_topology"] = c.InputTopology
		proofFields["mirror_signal_contract"] = c.SignalContractID
		proofFields["planned_quantity"] = c.ProspectiveQty
		proofFields["planned_fee_total"] = c.ProspectiveRequestedFee
		proofFields["planned_fee_pc"] = c.ProspectiveRequestedFeePC
		proofFields["planned_fee_source"] = c.ProspectiveFeeSource
		proofFields["planned_time_in_force"] = c.ProspectiveTimeInForce
		proofFields["planned_time_in_force_reason"] = c.ProspectivePlanReason
		proofFields["one_contract_canary"] = canaryAllocation
		if !c.InputObservedAt.IsZero() {
			proofFields["mirror_input_observed_at"] = c.InputObservedAt.UTC().Format(time.RFC3339Nano)
		}
	}
	proofFields["market_tick"] = q.Tick
	proofFields["full_book_source"] = q.BookSource
	if c.Platform != "kalshi" {
		if why := s.liveMirrorClusterGuard(ctx, c, count*unitRisk); why != "" {
			return false, why
		}
	}
	// PolyUS retains the dispatcher account belts because its handler response contract differs.
	// Kalshi performs the same authenticated account check inside handleLivePlace, followed by the
	// event-header, pending-risk, budget and final write-boundary checks. Repeating the account REST
	// scan here delayed the IOC while adding no independent authority.
	if c.Platform != "kalshi" {
		if why := s.liveMirrorAccountGuard(ctx, c); why != "" {
			return false, why
		}
	}
	if !s.claimLiveMirrorIntent(ctx, c) {
		return false, "duplicate-or-unpersisted-claim"
	}
	// Belt two closes the account-feed race across the durable claim write. If it now finds a
	// position/order/session placement, no venue call has occurred in this dispatch. Release the
	// pre-submit claim so a later genuinely fresh signal can retry; the durable pending-risk and
	// venue handler create their own stronger idempotency boundary immediately before submission.
	if c.Platform != "kalshi" {
		if why := s.liveMirrorAccountGuard(ctx, c); why != "" {
			_ = s.releaseLiveMirrorPreSubmitClaim(ctx, c)
			return false, why
		}
	}
	// Re-read the current semantic identity after the durable claim and final account guard, at the
	// last boundary before the venue handler. A concurrent rule/payoff version change releases this
	// pre-submit claim and skips the candidate; it can never dispatch under stale sealed semantics.
	if sealedPromotion {
		if why := s.liveAutoPromotionCurrentIdentityReason(ctx, promoted); why != "" {
			_ = s.releaseLiveMirrorPreSubmitClaim(ctx, c)
			return false, "sealed-promotion-current-identity:" + why
		}
	}
	if why := s.liveR148CrossVenueVariantReason(ctx, c); why != "" {
		_ = s.releaseLiveMirrorPreSubmitClaim(ctx, c)
		return false, why
	}
	if why := s.liveIntentGenerationGateReason(true, c.LiveIntentGeneration,
		c.LiveIntentGenerationBound, true); why != "" {
		_ = s.releaseLiveMirrorPreSubmitClaim(ctx, c)
		return false, why
	}
	if sealedPromotion {
		fresh, why, err := s.store.ResearchPromotionIntentFreshForNewDispatch(ctx, promoted)
		if err != nil {
			_ = s.releaseLiveMirrorPreSubmitClaim(ctx, c)
			return false, "sealed-promotion-freshness-check-error"
		}
		if !fresh {
			_ = s.releaseLiveMirrorPreSubmitClaim(ctx, c)
			return false, "sealed-promotion-proof-no-longer-current:" + why
		}
	}
	priceText := fmt.Sprintf("%.6f", q.Price)
	var code int
	var res map[string]any
	switch c.Platform {
	case "kalshi":
		arbitrationAt := ""
		if !c.ArbitrationAt.IsZero() {
			arbitrationAt = c.ArbitrationAt.UTC().Format(time.RFC3339Nano)
		}
		body := liveMirrorMergeFields(map[string]any{
			"ticker": c.Ticker, "side": c.Side, "price_dollars": priceText,
			"count": int(count), "taker": !q.Maker, "auto": true, "ml_managed": newMLV2,
			"fill_or_kill":     c.ProspectiveTimeInForce == liveProspectiveFOK,
			"canary_authority": c.Canary,
			"canary_basis":     c.CanaryBasis,
			"mirror_source":    c.Source, "mirror_inverted": c.Inverted, "mirror_paper_price": c.Price,
			"execution_shadow_attempt_id":  c.ShadowAttemptID,
			"live_intent_generation":       c.LiveIntentGeneration,
			"live_intent_generation_bound": c.LiveIntentGenerationBound,
			"mirror_signal_at":             c.At.UTC().Format(time.RFC3339Nano),
			"trigger_unix_ms":              c.At.UnixMilli(),
			"mirror_arbitration_at":        arbitrationAt,
			"cross_venue_source_venue":     c.CrossVenueSourceVenue,
			"cross_venue_source_ticker":    c.CrossVenueSourceTicker,
			"cross_venue_source_side":      c.CrossVenueSourceSide,
			"cross_venue_certificate_hash": c.CrossVenueCertificateHash,
		}, proofFields)
		code, res = s.selfPOSTContext(ctx, s.handleLivePlace, "/api/live/place", body)
	case "polyus":
		body := liveMirrorMergeFields(map[string]any{
			"slug": c.Ticker, "outcome": c.Side, "price_dollars": priceText,
			"count": count, "post_only": q.Maker, "auto": true,
			"mirror_source": c.Source, "mirror_inverted": c.Inverted, "mirror_paper_price": c.Price,
			"execution_shadow_attempt_id":  c.ShadowAttemptID,
			"live_intent_generation":       c.LiveIntentGeneration,
			"live_intent_generation_bound": c.LiveIntentGenerationBound,
			"trigger_unix_ms":              c.At.UnixMilli(),
			"cross_venue_source_venue":     c.CrossVenueSourceVenue,
			"cross_venue_source_ticker":    c.CrossVenueSourceTicker,
			"cross_venue_source_side":      c.CrossVenueSourceSide,
			"cross_venue_certificate_hash": c.CrossVenueCertificateHash,
		}, proofFields)
		code, res = s.selfPOSTContext(ctx, s.handlePolyUSLiveOrder, "/api/live/polyus/place", body)
	}
	if liveMirrorHandlerProvesNoVenueCall(c.Platform, code, res) {
		// This claim used to remain latched for six hours after a book/proof/budget/account refusal,
		// silently suppressing the next genuinely fresh signal even though no order could exist.
		_ = s.releaseLiveMirrorPreSubmitClaim(ctx, c)
	}
	state, _ := res["execution_state"].(string)
	orderID, _ := res["order_id"].(string)
	filled, _ := res["filled_qty"].(float64)
	remaining, _ := res["remaining_qty"].(float64)
	authoritative, _ := res["execution_authoritative"].(bool)
	avgPrice, _ := res["average_fill_price"].(float64)
	feeTotal, _ := res["fee_total"].(float64)
	feeKnownReceipt, _ := res["fee_known"].(bool)
	riskReservationID, _ := res["risk_reservation_id"].(string)
	executionSource, _ := res["execution_source"].(string)
	venueAttempted := state != ""
	if explicitVenueAttempted, ok := res["venue_attempted"].(bool); ok {
		venueAttempted = explicitVenueAttempted
	}
	if avgPrice <= 0 {
		avgPrice = q.Price
	}
	liveReason, _ := res["error"].(string)
	s.executionShadowRecordLiveResult(c, q, riskReservationID, orderID, state,
		executionSource, count, filled, avgPrice, feeTotal, feeKnownReceipt,
		authoritative, venueAttempted, liveReason, map[string]any{
			"handler_http_status": code, "handler_ok": res["ok"], "remaining_qty": remaining,
			"route": q.Route, "proof_basis": basis, "proof_mean": edge,
			"proof_lower": proofLo, "proof_fee_pc": proofFee,
			"requested_fee_total":  c.ProspectiveRequestedFee,
			"requested_fee_pc":     c.ProspectiveRequestedFeePC,
			"requested_fee_source": c.ProspectiveFeeSource,
			"time_in_force":        c.ProspectiveTimeInForce,
			"time_in_force_reason": c.ProspectivePlanReason,
			"one_contract_canary":  c.Canary,
			"canary_basis":         c.CanaryBasis,
		})
	if state != "" && sealedPromotion {
		_ = s.store.AppendResearchLiveExecutionReceipt(context.WithoutCancel(ctx), promoted,
			storage.ResearchLiveExecutionReceipt{State: state, OrderID: orderID, RequestedQty: count,
				FilledQty: filled, RemainingQty: remaining, AveragePrice: avgPrice, FeeTotal: math.Max(feeTotal, 0),
				FeeKnown: feeKnownReceipt, Authoritative: authoritative, ReceiptSource: fmt.Sprint(res["execution_source"]),
				Detail: "generic armed LIVE AUTO identical Paper route"})
	}
	if code == http.StatusOK && res["ok"] == true {
		if state == "" {
			state = "ambiguous"
			authoritative = false
		}
		if sealedPromotion {
			_ = s.store.AppendResearchPromotionEvent(context.WithoutCancel(ctx), promoted, "live_dispatched",
				promoted.Proof.Route, q.Price, proofFee, "armed LIVE AUTO dispatched identical Paper route; execution_state="+state)
		}
		displaySource := c.Source
		if newMLV2 {
			displaySource = "new-ml-v2"
		}
		sizingLabel := "self-tuned-proof-lower-kelly"
		if prospectiveAllocation {
			sizingLabel = "adaptive-allocation-model"
			if canaryAllocation {
				sizingLabel = "one-contract-unproven-canary"
			}
		}
		s.liveLogAdd(map[string]any{
			"event": "AUTO-LIVE-MIRROR", "venue": c.Platform, "ticker": c.Ticker,
			"side": c.Side, "source": displaySource, "paper_price": c.Price,
			"price_dollars": q.Price, "count": count, "route": q.Route,
			"confidence": basis, "fee_net_edge": edge, "proof_lower": proofLo,
			"proof_fee_pc": proofFee, "max_all_in_unit": proofFields["max_all_in_unit"],
			"sizing": sizingLabel, "sizing_bankroll": bank, "sizing_usd": usd,
			"prospective_allocation": prospectiveAllocation,
			"one_contract_canary":    canaryAllocation,
			"canary_state":           proofFields["canary_state"],
			"input_topology":         c.InputTopology, "signal_contract": c.SignalContractID,
			"kelly_ceiling": math.Min(s.cfg().Risk.LiveKellyMaxFrac, 0.50), "kelly_effective": effectiveKelly,
			"execution_state": state, "execution_authoritative": authoritative,
			"signal_to_dispatch_ms": time.Since(c.At).Milliseconds(),
		})
		if c.Platform == "kalshi" && liveKalshiTerminalZeroFill(state, authoritative, filled, remaining) {
			// The venue has proved this IOC terminal with zero execution. Clear only the signal
			// claim; the handler has already terminalized the durable risk row. Arbitration may
			// make one fresh-book retry while the original signal remains inside its short TTL.
			_ = s.releaseLiveMirrorPreSubmitClaim(context.WithoutCancel(ctx), c)
			return false, "authoritative-zero-fill"
		}
		// Accepted/resting/partial is a successfully tracked dispatch, never mislabeled as a fill.
		return true, state
	}
	if msg, _ := res["error"].(string); msg != "" {
		if promoted, accepted := s.researchPromotionAccepted(ctx, c.Source); accepted {
			_ = s.store.AppendResearchPromotionEvent(context.WithoutCancel(ctx), promoted, "live_rejected",
				promoted.Proof.Route, q.Price, proofFee, "live-handler:"+msg)
		}
		return false, "live-handler:" + msg
	}
	return false, fmt.Sprintf("live-handler-status-%d", code)
}

// consumeLiveMirrorCandidates first discovers actual recent New-ML Paper orders/fills, then tries
// a bounded number so a run of unknown/stale candidates cannot monopolize one 7-second pass.
func (s *Server) consumeLiveMirrorCandidates(ctx context.Context, maxPlaced int) int {
	_ = s.enqueueNewMLV2LiveCandidates(ctx)
	// Every exit path must republish the edge-triggered wake when the bounded attempt budget leaves
	// work behind. The next loop then continues immediately instead of waiting for the 7s ticker.
	defer s.rearmLiveMirrorDispatchIfPending()
	return s.consumeRankedLiveMirrorCandidates(ctx, maxPlaced)
}
