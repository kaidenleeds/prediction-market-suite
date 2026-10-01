package server

// R109 PARLAY RESEARCH COLLECTOR ("parlay lab") — LOG-ONLY, no paper betting, no orders.
//
// The R101 parlay book lost −$1,677 and was archived. The operator wants parlays back via DATA
// FIRST: the potential edge is CORRELATION — venues price legs independently, so positively-
// correlated combos (same-game favorite ML + over, same-storm weather stations, same-window
// crypto ladders) can be underpriced jointly. This layer, every lab tick:
//
//  1. CANDIDATES: from current executable +EV legs, persist one lossless manifest representing
//     every logical 2..N-leg subset. A separately-labelled, stable-hash sample (stratified by leg
//     count/linkage class) becomes at most 48 settlement rows per episode, with 384 open max.
//     The sample keeps learning alive; it never claims the raw logical space was evaluated.
//  2. PRICING: venue-implied parlay price = ∏(leg price). Model joint p = ∏(p_win) for indep;
//     for overlap legs the linked pair gets a correlation adjustment from the lab's OWN historical
//     joint-outcome estimator per linkage class, shrunk toward independence at low n
//     (rho_hat = rho_raw · n/(n+50); n=0 ⇒ independence — honest, not optimistic). Net EV after
//     fees is modeled BOTH ways: synthetic leg-by-leg taker execution (fees compound as the stake
//     rolls) vs a Kalshi MVE-style single combo fill (one fee on the combo price) — the cheaper
//     route is tagged per candidate.
//  3. GRADING: when every leg settles (store fast path + budgeted venue fetches), the joint
//     outcome is recorded, realized EV/$1 accrues per bucket × leg-count × linkage class, and the
//     linkage-class correlation counters update. Aggregates + correlation state persist in kv so
//     restarts don't lose the science.
//
// Surfaces: parlay_lab.jsonl (kind cand/grade), GET /api/parlaylab, a briefing line, dash panel.

import (
	"context"
	"crypto/sha1"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/polymarketus"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

const (
	// R144 starts a clean, rebuildable economics epoch. The legacy keys are deliberately left
	// untouched as an archive: their "per $1" values subtracted rolled-leg dollar fees from a
	// bare $1 denominator and therefore could report losses far below -100%.
	plabKVStats   = "parlaylab_stats_all_in_v2"
	plabKVCorr    = "parlaylab_corr_all_in_v2"
	plabKVSnap    = "parlaylab_stats_snap_all_in_v2" // 7d class snapshot for current economics only
	plabKVHorizon = "combo_lab_horizon_contract"
	plabKVCursor  = "combo_lab_settlement_cursor"
	// This durable epoch separates rows admitted under the present 4h-normal/2h-crypto contract
	// from older prospective rows. Legacy rows still settle and grade; they just cannot consume the
	// bounded current-policy sample or masquerade as current-horizon open tickets.
	plabKVAdmission = "combo_lab_admission_epoch:r147-4h2h-v1"
	plabKVOpenPfx   = "plabopen:"    // legacy pre-R128 per-entry kv ledger — read once and migrated to plab_open
	plabShrinkN     = 50.0           // rho shrinkage: rho_hat = rho_raw · n/(n+50)
	plabDedupTTL    = 12 * time.Hour // one candidate per combo per 12h
	// R128 (operator: "unlimited cap... every possible acceptable combo"): the open ledger moved
	// to SQLite (plab_open/plab_leg_open, storage/plab.go) — NO cap, NO eviction; logged combos
	// WAIT until their legs settle. Age, a changed horizon, or a later leg-limit policy never
	// authorizes deletion of an in-flight prospective result.
	plabSeenCap   = 250000           // in-memory 12h dedup snapshot bound (INSERT OR IGNORE is the guarantee)
	plabFetchBudg = 30               // venue GetMarket budget per grade pass (per TICKER now, store hits free)
	plabChkTTL    = 10 * time.Minute // per-ticker settlement recheck cooldown (sweep is ticker-centric)
	plabFailTTL   = 1 * time.Minute  // transport failures retry sooner than healthy non-terminal markets
	plabScanPage  = 8000             // broad store-fast page; keyset rotation covers universes beyond this bound
	plabGradeMax  = 4000             // gradable combos folded per pass (bounds tick time; backlog drains next pass)
	// R133: the full logical space stays in the manifest; only this deterministic prospective
	// sample becomes settlement rows. The open cap bounds DB growth even if every market is slow.
	plabSamplePerManifest = 48
	plabSampleProposalN   = 192
	plabSampleOpenCap     = 384
)

const (
	plabRegularHardMax = 4 * time.Hour
	plabCryptoHardMax  = 2 * time.Hour
	// Research-system collectors run on mixed 2m/5m/15m cadences and Combo Lab itself can be
	// deferred by the shared heavy-work gate. Keep their immutable discovery rows long enough for
	// at least one retry; admission still requires a fresh executable book/depth/fee reprice below.
	plabPositiveResearchLookback = 30 * time.Minute
)

type plabHorizonLedger struct {
	Version             string           `json:"version"`
	UpdatedAt           string           `json:"updated_at"`
	GeneratedRejected   map[string]int64 `json:"generated_rejected"`
	OpenRowsPruned      map[string]int64 `json:"open_rows_pruned"`
	LastGeneratedReject map[string]int   `json:"last_generated_reject"`
	LastOpenPrune       map[string]int   `json:"last_open_prune"`
}

type plabAdmissionContract struct {
	Version string `json:"version"`
	Epoch   int64  `json:"epoch_unix"`
}

func (s *Server) plabCurrentAdmissionEpoch(ctx context.Context, now time.Time, create bool) (int64, error) {
	if raw, ok := s.store.KVGet(ctx, plabKVAdmission); ok {
		var contract plabAdmissionContract
		if err := json.Unmarshal([]byte(raw), &contract); err != nil || contract.Version != "r147-4h2h-v1" || contract.Epoch <= 0 {
			return 0, fmt.Errorf("invalid Combo Lab admission epoch")
		}
		return contract.Epoch, nil
	}
	if !create {
		return 0, nil
	}
	contract := plabAdmissionContract{Version: "r147-4h2h-v1", Epoch: now.UTC().Unix()}
	body, err := json.Marshal(contract)
	if err != nil {
		return 0, err
	}
	if err := s.store.KVSet(ctx, plabKVAdmission, string(body)); err != nil {
		return 0, err
	}
	return contract.Epoch, nil
}

func plabCryptoLeg(l plabLeg) bool {
	return plabGenre(l) == "crypto" || isCryptoTicker(l.Ticker)
}

func (s *Server) plabHorizonLimit(l plabLeg) time.Duration {
	hours := s.cfg().Auto.ParlayMaxHoursOut
	hard := plabRegularHardMax
	if plabCryptoLeg(l) {
		hours = s.cfg().Auto.ConsensusCryptoMaxHoursOut
		hard = plabCryptoHardMax
	}
	if hours <= 0 || time.Duration(hours*float64(time.Hour)) > hard {
		return hard
	}
	return time.Duration(hours * float64(time.Hour))
}

// paperComboLegHorizonLimit is wider only for the two funded Paper Combo portfolios. The generic
// Combo Lab keeps its 4h/2h prospective contract, and every future LIVE RFQ handoff independently
// rechecks liveComboEntryHorizonDecision before money can move.
func (s *Server) paperComboLegHorizonLimit(l plabLeg) time.Duration {
	regular, crypto := paperComboEntryHorizonLimits(s.cfg().Auto)
	hours := regular
	if plabCryptoLeg(l) {
		hours = crypto
	}
	return time.Duration(hours * float64(time.Hour))
}

// plabLegResolveAt is cache/catalog-only: Combo Lab enumeration must not expand venue traffic.
// Every supported venue needs a current, parseable resolution estimate; absence fails closed.
func (s *Server) plabLegResolveAt(ctx context.Context, l plabLeg, now time.Time) (time.Time, bool) {
	if s.plabHorizonFn != nil {
		return s.plabHorizonFn(ctx, l, now)
	}
	switch strings.ToLower(strings.TrimSpace(l.Platform)) {
	case "kalshi":
		// The hot tape cache is deliberately bounded and can still be warming when the first
		// Combo pass runs. market_catalog carries the same venue-authored expiration from the
		// complete crawl, so use it as the no-network fallback instead of declaring hundreds of
		// otherwise current Kalshi candidates "unknown horizon" during every short boot.
		if m, ok := s.kmkt(l.Ticker); ok {
			if resolveAt, parsed := parseTime(firstNonEmpty(m.ExpectedExpiration, m.CloseTime)); parsed {
				return resolveAt, true
			}
		}
		if closeTS, _, ok := s.store.CatalogCloseTS(ctx, "kalshi", l.Ticker); ok {
			return parseTime(closeTS)
		}
		return time.Time{}, false
	case "polyus":
		s.polyUSMu.Lock()
		rows := append([]polyUSMarket(nil), s.polyUSMkts...)
		catalog := append([]polymarketus.Market(nil), s.pusSweep...)
		s.polyUSMu.Unlock()
		for _, m := range rows {
			if m.Slug != l.Ticker {
				continue
			}
			// Sports endDate/Resolve is an event boundary, not a dependable settlement clock.
			// Match the funded single and depth-planner contract: scheduled start + 3.5h. The old
			// Resolve-first order classified every one of the latest 79 positive ML rows as outside
			// four hours even when the game was already in progress.
			if start, err := parsePolyTime(m.Start); err == nil {
				return start.Add(3*time.Hour + 30*time.Minute), true
			}
			if m.Live {
				return now.Add(2 * time.Hour), true
			}
			// Crawl-only futures and non-sports instruments have no game start; for those rows the
			// venue endDate is the authoritative boundary and needs no sports padding.
			if ts, ok := parseTime(m.Resolve); ok {
				return ts, true
			}
			return time.Time{}, false
		}
		for _, m := range catalog {
			if m.Slug != l.Ticker {
				continue
			}
			if start, err := parsePolyTime(m.GameStart); err == nil {
				return start.Add(3*time.Hour + 30*time.Minute), true
			}
			if ts, ok := parseTime(m.EndDate); ok {
				return ts, true
			}
			return time.Time{}, false
		}
		if h, ok := s.pusCatalogHours(ctx, l.Ticker); ok {
			return now.Add(time.Duration((h + 3.5) * float64(time.Hour))), true
		}
	case "polymarket", "polyint", "poly-int":
		for _, m := range s.pintCatalogMarkets() {
			if m.ConditionID == l.Ticker || m.Slug == l.Ticker {
				return parseTime(m.EndDate)
			}
		}
	}
	return time.Time{}, false
}

// plabLegCurrentHorizon is the money-transfer contract: ordinary legs resolve within at most four
// hours, crypto within at most two, and unknown/already elapsed horizons never enter a new cohort.
func (s *Server) plabLegCurrentHorizon(ctx context.Context, l plabLeg, now time.Time) (float64, string, bool) {
	resolveAt, known := s.plabLegResolveAt(ctx, l, now)
	if !known || resolveAt.IsZero() {
		return 0, "unknown_horizon", false
	}
	left := resolveAt.Sub(now)
	if left <= 0 {
		return left.Hours(), "nonpositive_horizon", false
	}
	if limit := s.plabHorizonLimit(l); left > limit {
		if plabCryptoLeg(l) {
			return left.Hours(), "crypto_over_horizon", false
		}
		return left.Hours(), "regular_over_horizon", false
	}
	return left.Hours(), "", true
}

func (s *Server) paperComboLegCurrentHorizon(ctx context.Context, l plabLeg, now time.Time) (float64, string, bool) {
	resolveAt, known := s.plabLegResolveAt(ctx, l, now)
	if !known || resolveAt.IsZero() {
		return 0, "unknown_horizon", false
	}
	left := resolveAt.Sub(now)
	if left <= 0 {
		return left.Hours(), "nonpositive_horizon", false
	}
	if limit := s.paperComboLegHorizonLimit(l); left > limit {
		if plabCryptoLeg(l) {
			return left.Hours(), "crypto_over_horizon", false
		}
		return left.Hours(), "regular_over_horizon", false
	}
	return left.Hours(), "", true
}

// Kept as a small compatibility wrapper for existing callers/tests; unlike the retired 45-day
// check it is now the strict current Combo Lab horizon contract.
func (s *Server) plabLegGradable(platform, ticker string) bool {
	_, _, ok := s.plabLegCurrentHorizon(context.Background(), plabLeg{Platform: platform, Ticker: ticker}, time.Now())
	return ok
}

func (s *Server) persistPlabHorizonCounts(ctx context.Context, generated, pruned map[string]int) {
	if len(generated) == 0 && len(pruned) == 0 {
		return
	}
	ledger := plabHorizonLedger{Version: "r142-current-horizon-v1", GeneratedRejected: map[string]int64{}, OpenRowsPruned: map[string]int64{}}
	if raw, ok := s.store.KVGet(ctx, plabKVHorizon); ok {
		_ = json.Unmarshal([]byte(raw), &ledger)
	}
	if ledger.GeneratedRejected == nil {
		ledger.GeneratedRejected = map[string]int64{}
	}
	if ledger.OpenRowsPruned == nil {
		ledger.OpenRowsPruned = map[string]int64{}
	}
	ledger.Version, ledger.UpdatedAt = "r142-current-horizon-v1", time.Now().UTC().Format(time.RFC3339Nano)
	ledger.LastGeneratedReject, ledger.LastOpenPrune = generated, pruned
	for reason, n := range generated {
		ledger.GeneratedRejected[reason] += int64(n)
	}
	for reason, n := range pruned {
		ledger.OpenRowsPruned[reason] += int64(n)
	}
	if body, err := json.Marshal(ledger); err == nil {
		_ = s.store.KVSet(ctx, plabKVHorizon, string(body))
	}
}

// plabLeg is one leg of a would-be parlay candidate.
type plabLeg struct {
	Ticker   string  `json:"ticker"`
	Side     string  `json:"side"`
	Platform string  `json:"platform"`
	Price    float64 `json:"price"`
	PWin     float64 `json:"p_win"`
	EVNet    float64 `json:"ev_net"`
	EventKey string  `json:"event_key"`
	Src      string  `json:"src,omitempty"` // R128: "" = ml sidecar | "edge" = PROVEN+ family | "inv" = inverted PROVEN− family
	Depth    float64 `json:"depth,omitempty"`
	QuoteTS  string  `json:"quote_ts,omitempty"`
	QuoteSrc string  `json:"quote_src,omitempty"`
	TwinKey  string  `json:"twin_key,omitempty"`
	// RollingPositive marks a current positive one-share system route which may enter the
	// PAPER-only rolling-positive-system cohort regardless of settled n. The manifest refreshes
	// its exact taker ask/depth/fee/horizon before use; it never grants sealed or LIVE authority.
	RollingPositive bool     `json:"rolling_positive_system_leg,omitempty"`
	PositiveSystems []string `json:"positive_systems,omitempty"`
	SystemRoute     string   `json:"system_route,omitempty"`
	// Promotion fields are present only when this exact leg came from a current sealed,
	// preregistered profitable candidate. They do not grant combo execution authority: a
	// separately quoted identical RFQ combo must earn its own prospective Paper/holdout proof.
	Promoted        bool    `json:"promoted_system_leg,omitempty"`
	SystemID        string  `json:"system_id,omitempty"`
	ProofRunID      int64   `json:"sealed_run_id,omitempty"`
	ProofResultHash string  `json:"sealed_result_hash,omitempty"`
	ProofMeanAllIn  float64 `json:"sealed_mean_all_in,omitempty"`
	ProofLowerPC    float64 `json:"sealed_lower_pc,omitempty"`
	ProofCapacity   float64 `json:"sealed_capacity,omitempty"`
	ProofRoute      string  `json:"sealed_route,omitempty"`
	ProofObserved   string  `json:"sealed_candidate_observed_ts,omitempty"`
}

// plabOpenEnt is one ungraded candidate awaiting settlement of all legs.
type plabOpenEnt struct {
	ID       string    `json:"id"`
	At       time.Time `json:"at"`
	Legs     []plabLeg `json:"legs"`
	Bucket   string    `json:"bucket"` // overlap | indep
	Class    string    `json:"class"`  // linkage class label
	Prod     float64   `json:"venue_prod"`
	JointP   float64   `json:"joint_p"`
	Rho      float64   `json:"rho"`
	RhoN     int       `json:"rho_n"`
	FeeSyn   float64   `json:"fee_synth"` // expected fees per $1 stake, synthetic taker route
	FeeMVE   float64   `json:"fee_mve"`   // fee per $1 stake, Kalshi MVE single-fill route (-1 = n/a)
	EVSyn    float64   `json:"ev_synth"`  // model net EV per $1, synthetic route
	EVMVE    float64   `json:"ev_mve"`    // model net EV per $1, MVE route (-1000 = n/a)
	LegFees  []float64 `json:"leg_fees"`  // per-leg taker fee per $1-chain (for realized grading)
	Legality string    `json:"legality"`  // R112 API-oracle: legal_probed | illegal_probed | unprobed | synthetic_legs_only | illegal
	Cohort   string    `json:"cohort"`
	// No Combo Lab row is a placed bet. Promoted-system-combo rows remain awaiting-rfq until an
	// identical real route independently earns quote, Paper, settlement, and untouched proof.
	RouteState        string `json:"route_state"`
	CanonicalSystemID string `json:"canonical_system_id,omitempty"`
	ComboVenue        string `json:"combo_venue,omitempty"`
	RelationClass     string `json:"relation_class,omitempty"`
	ProducerFamily    string `json:"producer_family,omitempty"`
	ComboRoute        string `json:"combo_route,omitempty"`
	ExperimentEpoch   string `json:"experiment_epoch,omitempty"`
	ComboKey          string `json:"combo_key,omitempty"`
	// R111: two+ legs share a correlated-exposure cluster (same game+team across series, same
	// ladder). NOT illegal — same-game multi-leg combos are venue-legal (operator's 4-leg
	// France–Morocco screenshot, verified against the live collections) and are precisely the
	// correlated combos the lab exists to study. Tagged so the scoreboard can split on it.
	CorrCluster bool `json:"corr_cluster,omitempty"`
}

func plabComboAttribution(legs []plabLeg, bucket, cohort, routeState, epoch string) (system, venue, relation, producer, route, comboKey string) {
	system = fmt.Sprintf("parlay-%dleg", len(legs))
	venue = "mixed"
	if len(legs) > 0 {
		venue = strings.ToLower(strings.TrimSpace(legs[0].Platform))
		for _, leg := range legs[1:] {
			if !strings.EqualFold(leg.Platform, venue) {
				venue = "mixed"
				break
			}
		}
	}
	relation = "unknown"
	switch strings.ToLower(strings.TrimSpace(bucket)) {
	case "indep", "independent":
		relation = "independent"
	case "overlap", "related":
		relation = "related"
	}
	producer, route = "generic", "synthetic-taker-chain"
	switch cohort {
	case storage.ComboLabCohortRollingPositive:
		producer = "positive-system"
	case storage.ComboLabCohortPromotedSystem:
		producer, route = "rfq", "rfq-research"
	}
	if routeState == "awaiting-rfq-identical-proof" {
		route = "rfq-research"
	}
	if epoch == "" {
		epoch = "r147-4h2h-v1"
	}
	comboKey = plabID(legs)
	return
}

var errPlabInvalidGrade = errors.New("invalid Combo Lab grade")

// plabValidateLegSet is the structural money-truth gate shared by sampling, funded PAPER
// selection and terminal grading. A side is a separate payoff, but the same venue instrument may
// not appear twice in one conjunction (including YES+NO): that is a repeated leg, not independent
// evidence or a valid parlay.
func plabValidateLegSet(legs []plabLeg) error {
	if len(legs) < 2 || len(legs) > 64 {
		return fmt.Errorf("%w: implausible legacy leg count %d", errPlabInvalidGrade, len(legs))
	}
	seen := make(map[string]struct{}, len(legs))
	for i, leg := range legs {
		platform := strings.ToLower(strings.TrimSpace(leg.Platform))
		ticker := strings.ToUpper(strings.TrimSpace(leg.Ticker))
		side := strings.ToUpper(strings.TrimSpace(leg.Side))
		if platform == "" || ticker == "" || (side != "YES" && side != "NO") {
			return fmt.Errorf("%w: incomplete leg %d", errPlabInvalidGrade, i)
		}
		key := platform + "|" + ticker
		if _, duplicate := seen[key]; duplicate {
			return fmt.Errorf("%w: repeated instrument %s", errPlabInvalidGrade, key)
		}
		seen[key] = struct{}{}
	}
	return nil
}

func plabValidatePricedLegSet(legs []plabLeg) error {
	if err := plabValidateLegSet(legs); err != nil {
		return err
	}
	if len(legs) > parlayLegLimit {
		return fmt.Errorf("%w: new candidate exceeds %d-leg limit", errPlabInvalidGrade, parlayLegLimit)
	}
	for i, leg := range legs {
		if math.IsNaN(leg.Price) || math.IsInf(leg.Price, 0) || leg.Price <= 0 || leg.Price >= 1 ||
			math.IsNaN(leg.PWin) || math.IsInf(leg.PWin, 0) || leg.PWin <= 0 || leg.PWin >= 1 {
			return fmt.Errorf("%w: invalid price/probability on leg %d", errPlabInvalidGrade, i)
		}
	}
	return nil
}

// plabAllInReturn reports net return per total entry dollar. payout is either the realized joint
// payoff (0..1) or the frozen joint probability; prod is the product-price capital before fees.
// The old formula divided proceeds by $1 and then subtracted fees which were themselves hundreds
// of dollars on cheap 5/6-leg chains. Dividing both profit and cost by (1+fees) makes the unit
// explicit and guarantees a fully lost entry is exactly -1.00/$1, never -1,068 or -234,399.
func plabAllInReturn(payout, prod, fees float64) (float64, bool) {
	finite := func(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }
	if !finite(payout) || !finite(prod) || !finite(fees) || payout < 0 || payout > 1 ||
		prod <= 0 || prod >= 1 || fees < 0 {
		return 0, false
	}
	entry := 1 + fees
	proceeds := payout / prod
	if !finite(entry) || !finite(proceeds) || entry < 1 {
		return 0, false
	}
	ret := (proceeds - entry) / entry
	if !finite(ret) || ret < -1-1e-9 {
		return 0, false
	}
	return math.Max(-1, ret), true
}

func plabIndependenceKeys(legs []plabLeg) []string {
	set := map[string]struct{}{}
	for _, leg := range legs {
		key := strings.ToLower(strings.TrimSpace(leg.EventKey))
		if key == "" {
			key = strings.ToLower(strings.TrimSpace(leg.Platform)) + "|" + strings.ToUpper(strings.TrimSpace(leg.Ticker))
		}
		if key != "|" && key != "" {
			set[key] = struct{}{}
		}
	}
	out := make([]string, 0, len(set))
	for key := range set {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

// plabMarketKeys counts executable instruments, not semantic outcomes. Two venues listing the
// same event remain two markets; repeated samples of one venue+ticker remain one market.
func plabMarketKeys(legs []plabLeg) []string {
	set := map[string]struct{}{}
	for _, leg := range legs {
		key := strings.ToLower(strings.TrimSpace(leg.Platform)) + "|" + strings.ToUpper(strings.TrimSpace(leg.Ticker))
		if key != "|" {
			set[key] = struct{}{}
		}
	}
	out := make([]string, 0, len(set))
	for key := range set {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

// plabLegality classifies a candidate's venue-executability. R112 REWRITE (operator doctrine:
// stop inferring venue rules — the exchange's answer is the determiner). The R110/R111 inferred
// rule engine (kalshi.ComboLegalAny) is retired from this funnel; legality now comes from the
// probe cache (comboprobe.go: authed lookup-only probes, cached 24h, negatives included).
// Verdicts:
//   - "illegal":             two legs are VERIFIED-identical twins (twins.go) — degenerate;
//   - "synthetic_legs_only": structural — a non-Kalshi leg (PolyUS combos institutional-beta
//     only; poly-int has no combos — R110 research). The API cannot combine across venues;
//   - "legal_probed":        the exchange served the combined market for this leg set;
//   - "illegal_probed":      the exchange refused (venue reason cached);
//   - "unprobed":            no cached answer yet — queued, highest-EV first.
//
// Second return: corr_cluster — two+ legs share a correlated-exposure cluster.
func (s *Server) plabLegality(ctx context.Context, legs []plabLeg, ev float64) (string, bool) {
	corr := false
	seenTrue, seenCl := map[string]bool{}, map[string]bool{}
	for _, l := range legs {
		if ck := corrClusterKey(l.Platform, l.Ticker); ck != "" {
			if seenCl[ck] {
				corr = true
			}
			seenCl[ck] = true
		}
		if tk := s.twinKeyGate(l.Platform, l.Ticker); tk != "" {
			if seenTrue[tk] {
				return "illegal", corr
			}
			seenTrue[tk] = true
		}
	}
	for _, l := range legs {
		if l.Platform != "kalshi" {
			return "synthetic_legs_only", corr
		}
	}
	if v, ok := s.comboVerdict(legs); ok {
		return v.Verdict, corr
	}
	s.comboEnqueue(legs, ev)
	return "unprobed", corr
}

// plabAgg is one cell of the bucket × legs × class scoreboard.
type plabAgg struct {
	N             int     `json:"n"`
	IndependentN  int     `json:"independent_n,omitempty"`  // connected resolution blocks, not overlapping rows
	UniqueMarkets int     `json:"unique_markets,omitempty"` // unique venue+ticker instruments
	Wins          int     `json:"wins"`
	SumReal       float64 `json:"sum_real"` // realized net per all-in entry dollar, synthetic route
	SumMVE        float64 `json:"sum_mve"`  // realized net per all-in entry dollar, MVE route
	NMVE          int     `json:"n_mve"`
	SumPred       float64 `json:"sum_pred"` // predicted net per all-in entry dollar at log time
	SumJP         float64 `json:"sum_jp"`   // model joint p (calibration of the joint estimate)
	// R123 Part 3: post-R123 accumulators WITH sum-of-squares — the class scoreboard's CIs
	// (pre-R123 kv cells never stored squares, so CIs run on the post-R123 sample only; the
	// lifetime N/means above are untouched and keep aggregating).
	N2   int     `json:"n2,omitempty"`
	Sum2 float64 `json:"sum2,omitempty"`
	Sq2  float64 `json:"sq2,omitempty"`
	// Groups are rebuilt from immutable receipts. They are intentionally not cached in KV; the
	// receipt ledger is the source of truth and permits exact connected-component recounts.
	IndependenceGroups [][]string `json:"-"`
	MarketGroups       [][]string `json:"-"`
	Outcomes           []float64  `json:"-"` // aligned with IndependenceGroups
}

// plabIndependentBlocks counts connected components in the combo-overlap graph: two samples are
// dependent when they share any canonical event/instrument key. This is deliberately stricter
// than row count or unique-ticker count; a chain of overlapping combos remains one evidence block.
func plabIndependentBlocks(groups [][]string) int {
	if len(groups) == 0 {
		return 0
	}
	parent := make([]int, len(groups))
	for i := range parent {
		parent[i] = i
	}
	var find func(int) int
	find = func(x int) int {
		if parent[x] != x {
			parent[x] = find(parent[x])
		}
		return parent[x]
	}
	union := func(a, b int) {
		ra, rb := find(a), find(b)
		if ra != rb {
			parent[rb] = ra
		}
	}
	owner := map[string]int{}
	for i, keys := range groups {
		for _, key := range keys {
			if j, ok := owner[key]; ok {
				union(i, j)
			} else {
				owner[key] = i
			}
		}
	}
	roots := map[int]struct{}{}
	for i := range groups {
		roots[find(i)] = struct{}{}
	}
	return len(roots)
}

func plabUniqueMarkets(groups [][]string) int {
	set := map[string]struct{}{}
	for _, group := range groups {
		for _, key := range group {
			set[key] = struct{}{}
		}
	}
	return len(set)
}

func plabSampleBucket(uniqueMarkets int) (string, string) {
	bucket := evidenceSampleBucket(uniqueMarkets)
	return bucket.Key, bucket.Emoji
}

// plabCellCI — mean + always-valid CS radius (verdicts.go csRadius) on the post-R123 sample.
// ok=false until the cell has any post-R123 rows.
func plabCellCI(a *plabAgg) (mean, lo, hi float64, n int, ok bool) {
	if a == nil || len(a.Outcomes) == 0 || len(a.Outcomes) != len(a.IndependenceGroups) {
		return 0, 0, 0, 0, false
	}
	blocks := connectedOutcomeMeans(a.Outcomes, a.IndependenceGroups)
	if len(blocks) == 0 {
		return 0, 0, 0, 0, false
	}
	var sd float64
	n, mean, sd = meanSD(blocks)
	r := csRadius(n, sd)
	if math.IsNaN(r) || math.IsInf(r, 0) {
		// n<2 or zero variance has an unbounded always-valid interval. Omit the CI until it is
		// finite; emitting ±Inf poisons the entire /api/parlaylab JSON response.
		return mean, 0, 0, n, false
	}
	return mean, mean - r, mean + r, n, true
}

// plabProvenClassesLocked — classes whose post-R123 realized EV/$1 is CI-POSITIVE at n≥20
// (the verdict engine's PROVEN+ shape applied to scoreboard cells). Log-only ordering input;
// placement policy is untouched (parlays never place). Caller holds plabMu.
func (s *Server) plabProvenClassesLocked() map[string]bool {
	out := map[string]bool{}
	for k, a := range s.plabStats {
		if a == nil {
			continue
		}
		if _, lo, _, n, ok := plabCellCI(a); ok && n >= 20 && lo > 0 {
			parts := strings.SplitN(k, "|", 4) // bucket|Nleg|class|legality
			if len(parts) >= 3 {
				out[parts[2]] = true
			}
		}
	}
	return out
}

// plabCorrC is the per-linkage-class pairwise joint-outcome counter behind the rho estimator.
type plabCorrC struct {
	N  int `json:"n"`  // graded pair observations
	A  int `json:"a"`  // leg-1 wins
	B  int `json:"b"`  // leg-2 wins
	AB int `json:"ab"` // both won
}

// plabRho turns the class counters into a SHRUNK correlation estimate (honest at low n):
// rho_raw = (pAB − pA·pB)/√(pA(1−pA)·pB(1−pB)), rho_hat = rho_raw · n/(n+50). Degenerate
// marginals (all-win / all-lose) return 0 — no information about co-movement.
func plabRho(c plabCorrC) (float64, int) {
	if c.N < 5 {
		return 0, c.N
	}
	n := float64(c.N)
	pa, pb, pab := float64(c.A)/n, float64(c.B)/n, float64(c.AB)/n
	den := math.Sqrt(pa * (1 - pa) * pb * (1 - pb))
	if den < 1e-9 {
		return 0, c.N
	}
	raw := (pab - pa*pb) / den
	return clampF(raw*(n/(n+plabShrinkN)), -0.95, 0.95), c.N
}

// plabJointP composes the model joint probability: independent product, with the linked pair (if
// any) adjusted by rho: p12 = p1·p2 + rho·√(p1(1−p1)·p2(1−p2)). Legs beyond the linked pair
// multiply in independently.
func plabJointP(legs []plabLeg, linked bool, rho float64) float64 {
	if len(legs) == 0 {
		return 0
	}
	jp := 1.0
	if linked && len(legs) >= 2 {
		p1, p2 := legs[0].PWin, legs[1].PWin
		p12 := p1*p2 + rho*math.Sqrt(p1*(1-p1)*p2*(1-p2))
		jp = clampF(p12, 0.0005, 0.9995)
		for _, l := range legs[2:] {
			jp *= l.PWin
		}
	} else {
		for _, l := range legs {
			jp *= l.PWin
		}
	}
	return clampF(jp, 0.0001, 0.9999)
}

// plabLegacyModeledFees is retained only for historical test/reference math. No current candidate
// or money-truth path calls it: synthetic admission uses plabExactSyntheticFees and MVE remains
// unavailable until an authenticated RFQ quote supplies the identical combined route.
//   - synthetic: buy leg1 with $1 → 1/p1 contracts; on a win the payout rolls into leg2, etc.
//     The funded Paper representation reserves and charges every rolled-leg taker fee at entry, so
//     prospective economics charge that same complete fee chain (no probability discount).
//   - MVE (all-Kalshi combos only): one fill of C=1/∏p contracts at price ∏p, Kalshi taker
//     formula ceil(0.07·C·P·(1−P)) — the venue's combo-quote fee shape. feeMVE=-1 when n/a.
//
// Returns (expected synthetic fees, per-leg fee chain, mve fee).
func (s *Server) plabLegacyModeledFees(legs []plabLeg) (feeSyn float64, legFees []float64, feeMVE float64) {
	contracts := 1.0
	allKal := true
	legFees = make([]float64, len(legs))
	for i, l := range legs {
		if l.Price <= 0 {
			return 0, legFees, -1
		}
		contracts = contracts / l.Price // contracts held after buying leg i with the rolled stake
		f := s.blendedFee(l.Platform, l.Ticker, "", false, contracts, l.Price)
		legFees[i] = f
		feeSyn += f
		if l.Platform != "kalshi" {
			allKal = false
		}
	}
	feeMVE = -1
	if allKal {
		prod := 1.0
		for _, l := range legs {
			prod *= l.Price
		}
		if prod > 0 && prod < 1 {
			c := 1 / prod
			feeMVE = math.Ceil(7*c*prod*(1-prod)) / 100
		}
	}
	return feeSyn, legFees, feeMVE
}

// plabGenre — coarse leg genre for the cross-genre linkage classes (R122 Part 4): crypto
// ("coin:" event keys), sports ("kgame:"/"pusgame:"), weather (KXHIGH*/KXLOW* series), other.
// Deliberately coarse: these label MEASUREMENT cells, never pricing inputs.
func plabGenre(l plabLeg) string {
	switch {
	case strings.HasPrefix(l.EventKey, "coin:"):
		return "crypto"
	case strings.HasPrefix(l.EventKey, "kgame:"), strings.HasPrefix(l.EventKey, "pusgame:"):
		return "sports"
	}
	t := strings.ToUpper(l.Ticker)
	if strings.HasPrefix(t, "KXHIGH") || strings.HasPrefix(t, "KXLOW") {
		return "weather"
	}
	return "other"
}

// plabClass labels the linkage class: overlap combos carry the shared-key family + the sorted
// series prefixes (e.g. "ov:kgame:KXMLB+KXMLBTOTAL"); independents the leg count — and, for
// 2-leg independents (R122 Part 4), a sorted GENRE PAIR ("ind:2leg:crypto+weather") so
// cross-genre co-movement accrues its own correlation counters. Pre-R122 "ind:2leg" kv cells
// keep aggregating separately — the R110 legality-suffix doctrine.
func plabClass(legs []plabLeg, sharedKey string) string {
	if sharedKey == "" {
		if len(legs) == 2 {
			gs := []string{plabGenre(legs[0]), plabGenre(legs[1])}
			sort.Strings(gs)
			return "ind:2leg:" + gs[0] + "+" + gs[1]
		}
		return fmt.Sprintf("ind:%dleg", len(legs))
	}
	fam := sharedKey
	if i := strings.Index(sharedKey, ":"); i > 0 {
		fam = sharedKey[:i]
	}
	ser := make([]string, 0, len(legs))
	for _, l := range legs {
		p := l.Ticker
		if i := strings.Index(p, "-"); i > 0 {
			p = p[:i]
		}
		ser = append(ser, strings.ToUpper(p))
	}
	sort.Strings(ser)
	return "ov:" + fam + ":" + strings.Join(ser, "+")
}

// plabHourRing — R124: per-minute ring so the briefing reports last-hour lab volume without a
// timestamp list (candidate volume runs hundreds-thousands/hour post-R122). plabMu-guarded.
type plabHourRing struct {
	mins   [60]int64 // unix minute stamped into each slot (slot = minute % 60)
	counts [60]int
}

func (r *plabHourRing) add(now time.Time, n int) {
	if n <= 0 {
		return
	}
	m := now.Unix() / 60
	i := int(m % 60)
	if r.mins[i] != m {
		r.mins[i], r.counts[i] = m, 0
	}
	r.counts[i] += n
}

func (r *plabHourRing) lastHour(now time.Time) int {
	m := now.Unix() / 60
	tot := 0
	for i := range r.mins {
		if d := m - r.mins[i]; d >= 0 && d < 60 {
			tot += r.counts[i]
		}
	}
	return tot
}

// plabClassPlain — R124: phone-readable name for a linkage-class label (operator briefing rule:
// plain words, no raw token globs). Raw labels stay everywhere else (kv cells, /api, logs).
func plabClassPlain(class string) string {
	famPlain := func(fam string) string {
		switch fam {
		case "kgame", "pusgame":
			return "same game"
		case "coin":
			return "same coin"
		case "ladder":
			return "same ladder"
		}
		return "linked " + fam
	}
	switch {
	case strings.HasPrefix(class, "ind:"):
		rest := strings.TrimPrefix(class, "ind:") // "2leg" | "2leg:crypto+weather" | "3leg"
		parts := strings.SplitN(rest, ":", 2)
		legs := strings.TrimSuffix(parts[0], "leg")
		if len(parts) == 2 {
			return legs + "-leg, no known link (" + strings.ReplaceAll(parts[1], "+", " + ") + "; independence not proven)"
		}
		return legs + "-leg, no known link (independence not proven)"
	case strings.HasPrefix(class, "ov:multi:"):
		return strings.TrimSuffix(strings.TrimPrefix(class, "ov:multi:"), "leg") + "-leg linked mix"
	case strings.HasPrefix(class, "ov:"):
		rest := strings.TrimPrefix(class, "ov:")
		parts := strings.SplitN(rest, ":", 2)
		if len(parts) == 2 {
			return famPlain(parts[0]) + " (" + strings.ReplaceAll(parts[1], "KX", "") + ")"
		}
		return famPlain(parts[0])
	}
	return class
}

func plabID(legs []plabLeg) string {
	ks := make([]string, 0, len(legs))
	for _, l := range legs {
		ks = append(ks, l.Platform+"|"+l.Ticker+"|"+strings.ToUpper(l.Side))
	}
	sort.Strings(ks)
	h := sha1.Sum([]byte(strings.Join(ks, "&")))
	return hex.EncodeToString(h[:])[:16]
}

func plabCohortID(legs []plabLeg, cohort string) string {
	base := plabID(legs)
	if cohort == "" || cohort == storage.ComboLabCohortAllEligible {
		return base // compatibility: historical all-eligible IDs stay stable
	}
	proofs := make([]string, 0, len(legs))
	for _, leg := range legs {
		if cohort == storage.ComboLabCohortRollingPositive {
			systems := append([]string(nil), leg.PositiveSystems...)
			sort.Strings(systems)
			proofs = append(proofs, plabLegStableKey(leg)+"="+strings.Join(systems, ","))
		} else {
			proofs = append(proofs, fmt.Sprintf("%s=%s:%d:%s", plabLegStableKey(leg), leg.SystemID,
				leg.ProofRunID, leg.ProofResultHash))
		}
	}
	sort.Strings(proofs)
	h := sha1.Sum([]byte(cohort + "|" + base + "|" + strings.Join(proofs, "&")))
	return hex.EncodeToString(h[:])[:16]
}

// plabObservationID keeps retry/restart dedup stable within a 12-hour evidence episode while
// allowing the same instruments to produce a genuinely new prospective observation later. A
// permanent combo-only receipt key would silently suppress all future regimes after the first
// settlement.
func plabObservationID(comboID string, at time.Time) string {
	episode := at.UTC().Unix() / int64((12*time.Hour)/time.Second)
	h := sha1.Sum([]byte(fmt.Sprintf("all-in-v2|%s|%d", comboID, episode)))
	return hex.EncodeToString(h[:])[:16]
}

// plabRebuildV2StatsLocked reconstructs every economic aggregate from immutable all-in-v2 grade
// receipts. Caller holds plabMu. KV blobs are only warm caches; a crash after receipt insertion or
// a failed open-row deletion can never change n or replay a grade.
func (s *Server) plabRebuildV2StatsLocked(ctx context.Context) error {
	receipts, err := s.store.PlabGradeReceipts(ctx)
	if err != nil {
		return err
	}
	stats := map[string]*plabAgg{}
	corr := map[string]*plabCorrC{}
	for _, r := range receipts {
		var legs []plabLeg
		var payouts []float64
		if json.Unmarshal([]byte(r.LegsJSON), &legs) != nil || json.Unmarshal([]byte(r.PayoutsJSON), &payouts) != nil ||
			len(legs) != int(r.NLegs) || len(payouts) != len(legs) || plabValidateLegSet(legs) != nil {
			return fmt.Errorf("invalid durable Combo Lab receipt %s", r.ComboID)
		}
		a := stats[r.Cell]
		if a == nil {
			a = &plabAgg{}
			stats[r.Cell] = a
		}
		a.N++
		a.N2++
		if r.Won {
			a.Wins++
		}
		a.SumReal += r.RealizedReturn
		a.SumPred += r.PredictedReturn
		a.SumJP += r.JointP
		a.Sum2 += r.RealizedReturn
		a.Sq2 += r.RealizedReturn * r.RealizedReturn
		if r.MVEValid {
			a.NMVE++
			a.SumMVE += r.RealizedMVEReturn
		}
		a.IndependenceGroups = append(a.IndependenceGroups, append([]string(nil), r.IndependenceKeys...))
		a.MarketGroups = append(a.MarketGroups, append([]string(nil), r.MarketKeys...))
		a.Outcomes = append(a.Outcomes, r.RealizedReturn)

		binaryWins := make([]bool, len(payouts))
		for i, payout := range payouts {
			binaryWins[i] = payout >= 0.5
		}
		_, _, _, overlap := plabComboShape(legs)
		for _, pair := range overlap {
			pairClass := plabClass([]plabLeg{legs[pair[0]], legs[pair[1]]}, legs[pair[0]].EventKey)
			cc := corr[pairClass]
			if cc == nil {
				cc = &plabCorrC{}
				corr[pairClass] = cc
			}
			cc.N++
			if binaryWins[pair[0]] {
				cc.A++
			}
			if binaryWins[pair[1]] {
				cc.B++
			}
			if binaryWins[pair[0]] && binaryWins[pair[1]] {
				cc.AB++
			}
		}
		if r.Bucket == "indep" && len(legs) == 2 {
			cc := corr[r.Class]
			if cc == nil {
				cc = &plabCorrC{}
				corr[r.Class] = cc
			}
			cc.N++
			if binaryWins[0] {
				cc.A++
			}
			if binaryWins[1] {
				cc.B++
			}
			if binaryWins[0] && binaryWins[1] {
				cc.AB++
			}
		}
	}
	for _, a := range stats {
		a.IndependentN = plabIndependentBlocks(a.IndependenceGroups)
		a.UniqueMarkets = plabUniqueMarkets(a.MarketGroups)
	}
	s.plabStats, s.plabCorr, s.plabGraded = stats, corr, len(receipts)
	return nil
}

// plabState lazy-loads persisted aggregates + correlation counters + the OPEN LEDGER (kv) on
// first use, and seeds the display counters from kv truth (R122, auditor 447/448/408).
func (s *Server) plabState(ctx context.Context) {
	if s.plabLoaded {
		return
	}
	s.plabLoaded = true
	if s.plabStats == nil {
		s.plabStats = map[string]*plabAgg{}
	}
	if s.plabCorr == nil {
		s.plabCorr = map[string]*plabCorrC{}
	}
	if v, ok := s.store.KVGet(ctx, plabKVStats); ok {
		_ = json.Unmarshal([]byte(v), &s.plabStats)
	}
	if v, ok := s.store.KVGet(ctx, plabKVCorr); ok {
		_ = json.Unmarshal([]byte(v), &s.plabCorr)
	}
	if v, ok := s.store.KVGet(ctx, plabKVSnap); ok { // R123: 7d movers snapshot
		var snap struct {
			TS    int64                 `json:"ts"`
			Cells map[string][2]float64 `json:"cells"`
		}
		if json.Unmarshal([]byte(v), &snap) == nil && snap.TS > 0 {
			s.plabSnap = snap.Cells
			s.plabSnapTS = time.Unix(snap.TS, 0)
		}
	}
	if s.plabSeen == nil {
		s.plabSeen = map[string]time.Time{}
	}
	// R128: the open ledger lives in SQLite now (plab_open — unlimited, storage/plab.go). One-time
	// migration of the legacy R122 kv rows into the table; the dedup latch re-seeds from them too.
	if m, err := s.store.KVPrefix(ctx, plabKVOpenPfx); err == nil && len(m) > 0 {
		batch := make([]storage.PlabCand, 0, len(m))
		for id, v := range m {
			var e plabOpenEnt
			if json.Unmarshal([]byte(v), &e) == nil && e.ID != "" {
				s.plabSeen[e.ID] = e.At
				batch = append(batch, plabToRow(e))
			}
			_ = s.store.KVDel(ctx, plabKVOpenPfx+id)
		}
		if n, err := s.store.PlabInsertBatch(ctx, batch); err == nil && n > 0 {
			actx, acancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			_ = s.store.Audit(actx, "info", "parlaylab",
				fmt.Sprintf("R128: migrated %d legacy kv open-ledger rows into plab_open (unlimited ledger)", n), "")
			acancel()
		}
	}
	// R122 (408): seed graded/candidates from kv truth — the counters were memory-only, so every
	// restart re-zeroed the display (API/dash 0 vs briefing's kv-sum 49).
	// Receipts override the warm caches, including the intentional empty state on the first
	// all-in-v2 boot. Legacy poisoned aggregates remain archived under their old key names.
	if err := s.plabRebuildV2StatsLocked(ctx); err != nil {
		// Never fall back to an aggregate blob when its immutable receipts cannot be verified.
		s.plabStats, s.plabCorr, s.plabGraded = map[string]*plabAgg{}, map[string]*plabCorrC{}, 0
	}
	if s.plabGraded == 0 {
		for _, a := range s.plabStats {
			s.plabGraded += a.N
		}
	}
	if openN, err := s.store.PlabOpenCount(ctx); err == nil {
		s.plabOpenN, s.plabOpenNAt = int(openN), time.Now()
		if s.plabCand < s.plabGraded+int(openN) {
			s.plabCand = s.plabGraded + int(openN)
		}
	}
}

// plabToRow converts an in-memory open entry to its compact SQLite row.
func plabToRow(e plabOpenEnt) storage.PlabCand {
	if e.CanonicalSystemID == "" {
		e.CanonicalSystemID, e.ComboVenue, e.RelationClass, e.ProducerFamily,
			e.ComboRoute, e.ComboKey = plabComboAttribution(e.Legs, e.Bucket, e.Cohort, e.RouteState, e.ExperimentEpoch)
	}
	if e.ExperimentEpoch == "" {
		e.ExperimentEpoch = "r147-4h2h-v1"
	}
	lb, _ := json.Marshal(e.Legs)
	fb, _ := json.Marshal(e.LegFees)
	cc := storage.PlabCand{ID: e.ID, At: e.At.UTC().Unix(), Bucket: e.Bucket, Class: e.Class,
		Legality: e.Legality, CorrCluster: e.CorrCluster, Prod: e.Prod, JointP: e.JointP,
		Rho: e.Rho, RhoN: e.RhoN, FeeSyn: e.FeeSyn, FeeMVE: e.FeeMVE, EVSyn: e.EVSyn,
		EVMVE: e.EVMVE, LegFees: string(fb), Legs: string(lb), NLegs: len(e.Legs),
		Cohort: e.Cohort, RouteState: e.RouteState, CanonicalSystemID: e.CanonicalSystemID,
		ComboVenue: e.ComboVenue, RelationClass: e.RelationClass, ProducerFamily: e.ProducerFamily,
		ComboRoute: e.ComboRoute, ExperimentEpoch: e.ExperimentEpoch, ComboKey: e.ComboKey}
	for _, l := range e.Legs {
		cc.LegKeys = append(cc.LegKeys, storage.PlabLegKey{Platform: l.Platform, Ticker: l.Ticker})
	}
	return cc
}

// plabOpenCount — cached open-ledger size (30s; the ledger is unbounded so COUNT(*) is not free).
func (s *Server) plabOpenCount(ctx context.Context) int {
	s.plabMu.Lock()
	n, at := s.plabOpenN, s.plabOpenNAt
	s.plabMu.Unlock()
	if time.Since(at) < 30*time.Second {
		return n
	}
	if c, err := s.store.PlabOpenCount(ctx); err == nil {
		n = int(c)
		s.plabMu.Lock()
		s.plabOpenN, s.plabOpenNAt = n, time.Now()
		s.plabMu.Unlock()
	}
	return n
}

// plabAppend writes one row to data/parlay_lab.jsonl (rotate at 20MB keeping the newest half —
// the settlement-journal pattern).
func (s *Server) plabAppend(row map[string]any) {
	path := filepath.Join(s.cfg().DataDir, "parlay_lab.jsonl")
	if fi, e := os.Stat(path); e == nil && fi.Size() > 20<<20 {
		if old, e2 := os.ReadFile(path); e2 == nil {
			half := old[len(old)/2:]
			if i := strings.IndexByte(string(half), '\n'); i >= 0 && i+1 < len(half) {
				half = half[i+1:]
			}
			_ = os.WriteFile(path, half, 0o644)
		}
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return
	}
	_ = json.NewEncoder(f).Encode(row)
	_ = f.Close()
}

// plabEnumStats — the honest combinatorics of one enumeration cycle (R123 Part 2). Surfaced on
// /api/parlaylab + an audit breadcrumb: raw space vs floor-cleared vs enumerated vs logged.
type plabEnumStats struct {
	PoolN                 int                         `json:"pool_n"`
	MaxLegs               int                         `json:"max_legs"`
	RawSpace              float64                     `json:"raw_space"` // Σ C(pool,k), k=2..max — the "literally every combination" denominator
	UniqueInstruments     int                         `json:"unique_instruments"`
	StructuralSpaceDec    string                      `json:"structural_space_dec"` // route-row subsets with at most one leg per venue+ticker
	SameVenueSpaceDec     string                      `json:"same_venue_space_dec"` // structurally possible atomic venue combos
	FloorLogEdge          float64                     `json:"floor_log_edge"`       // the +EV superset screen: Σ log(p/price) ≥ this (corr slack folded in)
	TEffective            float64                     `json:"t_effective"`          // the threshold actually walked (== floor ⇒ FULL exhaustive pass)
	Complete              bool                        `json:"complete"`             // true ⇒ EVERY unseen combo ≥ floor was enumerated this cycle
	CountAtFloor          int                         `json:"count_at_floor"`       // unseen combos ≥ floor (-1 = more than one cycle's budget)
	Enumerated            int                         `json:"enumerated"`           // combos emitted from the walk this cycle
	FeeScreened           int                         `json:"fee_screened"`         // emitted but failed the EXACT fee-adjusted floor — seen-marked, never logged
	TwinSkipped           int                         `json:"twin_skipped"`         // identical-underlying exclusions (restatement guard)
	Logged                int                         `json:"logged"`
	Nodes                 int                         `json:"nodes"`
	CorrSlack             float64                     `json:"corr_slack"`
	MS                    float64                     `json:"ms"`
	At                    string                      `json:"at,omitempty"`
	RawSpaceDec           string                      `json:"raw_space_dec,omitempty"`
	Representation        string                      `json:"representation,omitempty"`
	SampleEpisode         string                      `json:"sample_episode,omitempty"`
	SampleAdmissionEpoch  string                      `json:"sample_admission_epoch,omitempty"`
	SampleProposed        int                         `json:"sample_proposed"`
	SampleEligible        int                         `json:"sample_eligible"`
	SampleInserted        int                         `json:"sample_inserted"`
	SampleRejected        map[string]int              `json:"sample_rejected"`
	SampleOpenCap         int                         `json:"sample_open_cap,omitempty"`
	SampleCurrentOpen     int                         `json:"sample_current_open,omitempty"`
	SampleLegacyOpen      int                         `json:"sample_legacy_open,omitempty"`
	PositiveRouteCoverage []plabPositiveRouteCoverage `json:"positive_system_route_coverage,omitempty"`
	TypedPositiveBundles  int                         `json:"typed_positive_bundles,omitempty"`
	TypedPositiveBySystem map[string]int              `json:"typed_positive_by_system,omitempty"`
	TypedPositiveByLegs   map[int]int                 `json:"typed_positive_by_legs,omitempty"`
}

type plabManifest struct {
	ID                   string                    `json:"id"`
	CreatedAt            string                    `json:"created_at"`
	AlgoVersion          string                    `json:"algo_version"`
	SingleFloor          float64                   `json:"single_floor"`
	ComboFloor           float64                   `json:"combo_floor"`
	MaxLegs              int                       `json:"max_legs"`
	RawSpaceDec          string                    `json:"raw_space_dec"`
	UniqueInstruments    int                       `json:"unique_instruments"`
	StructuralSpaceDec   string                    `json:"structural_space_dec"`
	SameVenueSpaceDec    string                    `json:"same_venue_space_dec"`
	SampleEpisode        string                    `json:"sample_episode"`
	SampleAdmissionEpoch string                    `json:"sample_admission_epoch"`
	Legs                 []plabLeg                 `json:"legs"`
	TypedPositiveBundles []plabTypedPositiveBundle `json:"typed_positive_route_bundles,omitempty"`
	Corr                 map[string]plabCorrC      `json:"correlation_snapshot"`
	Rules                map[string]any            `json:"rules"`
}

type plabTypedPositiveLeg struct {
	Index         int     `json:"index"`
	Venue         string  `json:"venue"`
	Ticker        string  `json:"ticker"`
	Side          string  `json:"side"`
	PayoffID      string  `json:"payoff_id"`
	Quantity      float64 `json:"quantity"`
	Cost          float64 `json:"integrated_cost"`
	Fee           float64 `json:"exact_fee"`
	Depth         float64 `json:"visible_depth"`
	Tick          float64 `json:"tick_size"`
	QuoteAge      float64 `json:"quote_age_s"`
	BookSource    string  `json:"book_source"`
	SourceClockID string  `json:"source_clock_id"`
	FeeSource     string  `json:"fee_source"`
}

type plabTypedPositiveBundle struct {
	BundleID          string                 `json:"bundle_id"`
	SystemID          string                 `json:"system_id"`
	Cohort            string                 `json:"cohort"`
	CanonicalEventID  string                 `json:"canonical_event_id"`
	Observed          string                 `json:"observed_ts"`
	RouteKind         string                 `json:"route_kind"`
	CertificateHash   string                 `json:"certificate_hash"`
	CertificateStatus string                 `json:"certificate_status"`
	Blocker           string                 `json:"blocker,omitempty"`
	NetFloor          float64                `json:"net_floor"`
	Cost              float64                `json:"total_cost"`
	Fee               float64                `json:"total_fee"`
	PayoutFloor       float64                `json:"payout_floor"`
	Legs              []plabTypedPositiveLeg `json:"legs"`
	Authority         string                 `json:"authority"`
	LiveAuthority     bool                   `json:"live_authority"`
	Grading           string                 `json:"grading"`
}

func (s *Server) plabTypedPositiveBundleRefs(ctx context.Context, now time.Time) []plabTypedPositiveBundle {
	bundles, err := s.store.RecentPositiveResearchRouteBundles(ctx, now.Add(-20*time.Minute), 100)
	if err != nil {
		return nil
	}
	out := make([]plabTypedPositiveBundle, 0, len(bundles))
	for _, bundle := range bundles {
		ref := plabTypedPositiveBundle{BundleID: bundle.BundleID, SystemID: bundle.SystemID,
			Cohort: bundle.Cohort, CanonicalEventID: bundle.CanonicalEventID,
			Observed: bundle.Observed.UTC().Format(time.RFC3339Nano), RouteKind: bundle.RouteKind,
			CertificateHash: bundle.CertificateHash, CertificateStatus: bundle.CertificateStatus,
			Blocker: bundle.Blocker, NetFloor: bundle.NetFloor, Cost: bundle.Cost,
			Fee: bundle.Fee, PayoutFloor: bundle.PayoutFloor, Authority: "PAPER_RESEARCH_ONLY",
			LiveAuthority: false,
			Grading:       "native immutable payoff-state ledger; never reinterpreted as an all-win parlay"}
		for _, leg := range bundle.Legs {
			ref.Legs = append(ref.Legs, plabTypedPositiveLeg{Index: leg.Index, Venue: leg.Venue,
				Ticker: leg.Ticker, Side: leg.Side, PayoffID: leg.PayoffID, Quantity: leg.Quantity,
				Cost: leg.IntegratedCost, Fee: leg.ExactFee, Depth: leg.VisibleDepth, Tick: leg.Tick,
				QuoteAge: leg.Age, BookSource: leg.BookSource, SourceClockID: leg.SourceClockID,
				FeeSource: leg.FeeSource})
		}
		out = append(out, ref)
	}
	return out
}

func plabRawSpace(n, maxLegs int) *big.Int {
	maxLegs = boundedParlayMaxLegs(maxLegs)
	if maxLegs > n {
		maxLegs = n
	}
	tot := new(big.Int)
	for k := 2; k <= maxLegs; k++ {
		tot.Add(tot, new(big.Int).Binomial(int64(n), int64(k)))
	}
	return tot
}

// plabStructuralSpaces separates three denominators which used to be presented as though they
// meant the same thing. raw space is C(route rows, k), even when two rows are alternate routes or
// sides of one instrument and therefore cannot coexist. structural space chooses at most one row
// per venue+ticker. same-venue space additionally requires every selected instrument to belong to
// one venue, which is the only shape an atomic venue combo could execute.
//
// A venue+ticker may still have several route/side rows. The generating-function coefficient for
// that instrument is therefore (1 + c*x), where c is the number of mutually exclusive choices.
// This is an upper bound before twin, probability, fee, edge, and venue-legality gates.
func plabStructuralSpaces(legs []plabLeg, maxLegs int) (uniqueInstruments int, structural, sameVenue *big.Int) {
	maxLegs = boundedParlayMaxLegs(maxLegs)
	byVenue := map[string]map[string]int{}
	all := map[string]int{}
	for _, leg := range legs {
		venue := strings.ToLower(strings.TrimSpace(leg.Platform))
		ticker := strings.ToUpper(strings.TrimSpace(leg.Ticker))
		if venue == "" || ticker == "" {
			continue
		}
		instrument := venue + "|" + ticker
		all[instrument]++
		if byVenue[venue] == nil {
			byVenue[venue] = map[string]int{}
		}
		byVenue[venue][instrument]++
	}

	choiceSpace := func(groups map[string]int) *big.Int {
		dp := make([]*big.Int, maxLegs+1)
		for i := range dp {
			dp[i] = new(big.Int)
		}
		dp[0].SetInt64(1)
		for _, choices := range groups {
			c := big.NewInt(int64(choices))
			for k := maxLegs; k >= 1; k-- {
				dp[k].Add(dp[k], new(big.Int).Mul(dp[k-1], c))
			}
		}
		total := new(big.Int)
		for k := 2; k <= maxLegs; k++ {
			total.Add(total, dp[k])
		}
		return total
	}

	structural = choiceSpace(all)
	sameVenue = new(big.Int)
	for _, groups := range byVenue {
		sameVenue.Add(sameVenue, choiceSpace(groups))
	}
	return len(all), structural, sameVenue
}

// plabSampleCand is one deterministically selected prospective grade row. It is a tiny,
// explicitly sampled measurement layer beside the full manifest; it never claims to enumerate
// or evaluate the manifest's raw combination space.
type plabSampleCand struct {
	Legs          []plabLeg
	ID            string
	Bucket, Class string
	Cohort        string
	// Routes is populated only for rolling-positive rows. A route is exact to
	// venue + named system + taker execution, so one prolific family cannot hide
	// a sparse route in the bounded prospective sample.
	Routes []string
	Score  uint64
}

func plabPositiveSystemRoutes(legs []plabLeg) []string {
	set := map[string]bool{}
	for _, leg := range legs {
		venue := strings.ToLower(strings.TrimSpace(leg.Platform))
		for _, system := range leg.PositiveSystems {
			system = strings.TrimSpace(system)
			if venue != "" && system != "" {
				set[venue+"|"+system+"|taker"] = true
			}
		}
	}
	out := make([]string, 0, len(set))
	for route := range set {
		out = append(out, route)
	}
	sort.Strings(out)
	return out
}

type plabPositiveRouteCoverage struct {
	RouteID          string       `json:"route_id"`
	Venue            string       `json:"venue"`
	System           string       `json:"system"`
	AvailableLegs    int          `json:"available_legs"`
	VenueInstruments int          `json:"venue_instruments"`
	ProposedByLegs   map[int]int  `json:"proposed_by_legs"`
	FeasibleByLegs   map[int]bool `json:"feasible_by_legs"`
	MissingFeasible  []int        `json:"missing_feasible,omitempty"`
	Infeasible       []int        `json:"infeasible,omitempty"`
	Complete         bool         `json:"complete"`
	Truth            string       `json:"truth"`
}

// plabBuildPositiveRouteCoverage is deliberately explicit about the distinction between
// "not selected inside a bounded sample" and "not constructible from the current venue
// universe." Feasibility means at least one leg for the route plus k distinct instruments on
// that venue; the normal duplicate-twin and economic gates may still reject a proposal later.
func plabBuildPositiveRouteCoverage(pool []plabLeg, proposed []plabSampleCand, maxLegs int) []plabPositiveRouteCoverage {
	maxLegs = boundedParlayMaxLegs(maxLegs)
	venueInstruments := map[string]map[string]bool{}
	routeLegs := map[string]map[string]bool{}
	for _, leg := range pool {
		if !leg.RollingPositive || leg.SystemRoute != "taker" {
			continue
		}
		venue := strings.ToLower(strings.TrimSpace(leg.Platform))
		instrument := venue + "|" + strings.ToUpper(strings.TrimSpace(leg.Ticker))
		if venueInstruments[venue] == nil {
			venueInstruments[venue] = map[string]bool{}
		}
		venueInstruments[venue][instrument] = true
		for _, route := range plabPositiveSystemRoutes([]plabLeg{leg}) {
			if routeLegs[route] == nil {
				routeLegs[route] = map[string]bool{}
			}
			routeLegs[route][instrument] = true
		}
	}
	proposedCounts := map[string]map[int]int{}
	for _, cand := range proposed {
		if cand.Cohort != storage.ComboLabCohortRollingPositive {
			continue
		}
		for _, route := range cand.Routes {
			if proposedCounts[route] == nil {
				proposedCounts[route] = map[int]int{}
			}
			proposedCounts[route][len(cand.Legs)]++
		}
	}
	routes := make([]string, 0, len(routeLegs))
	for route := range routeLegs {
		routes = append(routes, route)
	}
	sort.Strings(routes)
	out := make([]plabPositiveRouteCoverage, 0, len(routes))
	for _, route := range routes {
		parts := strings.SplitN(route, "|", 3)
		venue, system := parts[0], ""
		if len(parts) > 1 {
			system = parts[1]
		}
		row := plabPositiveRouteCoverage{RouteID: route, Venue: venue, System: system,
			AvailableLegs: len(routeLegs[route]), VenueInstruments: len(venueInstruments[venue]),
			ProposedByLegs: map[int]int{}, FeasibleByLegs: map[int]bool{}, Complete: true,
			Truth: "proposed is bounded sample coverage; feasible means the current venue has k distinct instruments including at least one exact-route leg; later identity/economic gates can still reject"}
		for k := 2; k <= maxLegs; k++ {
			row.ProposedByLegs[k] = proposedCounts[route][k]
			feasible := row.AvailableLegs > 0 && row.VenueInstruments >= k
			row.FeasibleByLegs[k] = feasible
			if !feasible {
				row.Infeasible = append(row.Infeasible, k)
			} else if row.ProposedByLegs[k] == 0 {
				row.MissingFeasible = append(row.MissingFeasible, k)
				row.Complete = false
			}
		}
		out = append(out, row)
	}
	return out
}

func plabLegStableKey(l plabLeg) string {
	return strings.ToLower(l.Platform) + "|" + strings.ToUpper(l.Ticker) + "|" + strings.ToUpper(l.Side)
}

func plabStableHash(s string) uint64 {
	h := sha1.Sum([]byte(s))
	return binary.BigEndian.Uint64(h[:8])
}

// plabSampleEpisode identifies the currently executable leg universe, excluding prices/times.
// Requotes of the same still-open universe therefore choose the same rows; plab_open's stable
// combo-id primary key makes the repeated manifest an INSERT-OR-IGNORE episode dedup.
func plabSampleEpisode(pool []plabLeg, maxLegs int) string {
	keys := make([]string, len(pool))
	for i, l := range pool {
		keys[i] = plabLegStableKey(l)
	}
	sort.Strings(keys)
	h := sha1.Sum([]byte(fmt.Sprintf("r133|%d|%s", maxLegs, strings.Join(keys, "&"))))
	return hex.EncodeToString(h[:])[:16]
}

// plabSamplePick chooses k unique legs by stable hash, optionally forcing linked pair indices.
// The input pool is canonical-sorted by the caller.
func plabSamplePick(pool []plabLeg, k int, seed string, forced []int) []plabLeg {
	if k < 2 || k > len(pool) || len(forced) > k {
		return nil
	}
	chosen := map[int]bool{}
	chosenInstrument := map[string]bool{}
	chosenTwin := map[string]bool{}
	idx := make([]int, 0, k)
	for _, i := range forced {
		if i < 0 || i >= len(pool) || chosen[i] {
			return nil
		}
		instrument := strings.ToLower(strings.TrimSpace(pool[i].Platform)) + "|" + strings.ToUpper(strings.TrimSpace(pool[i].Ticker))
		if instrument == "|" || chosenInstrument[instrument] {
			return nil
		}
		if twin := strings.TrimSpace(pool[i].TwinKey); twin != "" && chosenTwin[twin] {
			return nil
		} else if twin != "" {
			chosenTwin[twin] = true
		}
		chosen[i] = true
		chosenInstrument[instrument] = true
		idx = append(idx, i)
	}
	type scored struct {
		i int
		h uint64
	}
	left := make([]scored, 0, len(pool)-len(chosen))
	for i, l := range pool {
		if !chosen[i] {
			left = append(left, scored{i: i, h: plabStableHash(seed + "|" + plabLegStableKey(l))})
		}
	}
	sort.Slice(left, func(i, j int) bool {
		if left[i].h != left[j].h {
			return left[i].h < left[j].h
		}
		return left[i].i < left[j].i
	})
	for _, x := range left {
		if len(idx) >= k {
			break
		}
		instrument := strings.ToLower(strings.TrimSpace(pool[x.i].Platform)) + "|" + strings.ToUpper(strings.TrimSpace(pool[x.i].Ticker))
		if instrument == "|" || chosenInstrument[instrument] {
			continue
		}
		if twin := strings.TrimSpace(pool[x.i].TwinKey); twin != "" && chosenTwin[twin] {
			continue
		} else if twin != "" {
			chosenTwin[twin] = true
		}
		idx = append(idx, x.i)
		chosenInstrument[instrument] = true
	}
	if len(idx) != k {
		return nil
	}
	legs := make([]plabLeg, 0, k)
	for _, i := range idx {
		legs = append(legs, pool[i])
	}
	sort.Slice(legs, func(i, j int) bool { return plabLegStableKey(legs[i]) < plabLegStableKey(legs[j]) })
	return legs
}

// plabStableProspectiveSample makes a stable-hash, linkage-class/leg-count-stratified candidate
// queue without walking the combinatorial space. Generic hash picks cover independent shapes;
// forced within-event pairs ensure linked classes are represented even when random co-selection
// would be rare. The returned order round-robins leg counts, then linkage classes.
func plabStableProspectiveSample(in []plabLeg, maxLegs, limit int) ([]plabSampleCand, string) {
	maxLegs = boundedParlayMaxLegs(maxLegs)
	if len(in) < 2 || maxLegs < 2 || limit <= 0 {
		return nil, plabSampleEpisode(in, maxLegs)
	}
	pool := append([]plabLeg(nil), in...)
	sort.Slice(pool, func(i, j int) bool { return plabLegStableKey(pool[i]) < plabLegStableKey(pool[j]) })
	if maxLegs > len(pool) {
		maxLegs = len(pool)
	}
	episode := plabSampleEpisode(pool, maxLegs)
	all := map[string]plabSampleCand{}
	addCohort := func(cohort string, legs []plabLeg) {
		if len(legs) < 2 {
			return
		}
		if cohort == storage.ComboLabCohortPromotedSystem {
			for _, l := range legs {
				if !l.Promoted || l.SystemID == "" || l.ProofRunID <= 0 || l.ProofResultHash == "" || l.ProofMeanAllIn <= 0 ||
					!strings.EqualFold(l.Platform, "kalshi") || l.ProofCapacity < 1 || l.ProofRoute != "taker" {
					return
				}
			}
		} else if cohort == storage.ComboLabCohortRollingPositive {
			platform := ""
			for _, l := range legs {
				p := strings.ToLower(strings.TrimSpace(l.Platform))
				if !l.RollingPositive || len(l.PositiveSystems) == 0 || l.SystemRoute != "taker" ||
					(p != "kalshi" && p != "polyus") {
					return
				}
				if platform == "" {
					platform = p
				} else if p != platform {
					return // an atomic combo can never mix venues
				}
			}
		} else {
			cohort = storage.ComboLabCohortAllEligible
		}
		seenTwin := map[string]bool{}
		seenInstrument := map[string]bool{}
		for _, l := range legs {
			instrument := strings.ToLower(l.Platform) + "|" + strings.ToUpper(l.Ticker)
			if seenInstrument[instrument] {
				return // YES and NO are distinct decisions, but cannot be legs of one binary combo
			}
			seenInstrument[instrument] = true
			if l.TwinKey != "" {
				if seenTwin[l.TwinKey] {
					return
				}
				seenTwin[l.TwinKey] = true
			}
		}
		id := plabCohortID(legs, cohort)
		if _, exists := all[id]; exists {
			return
		}
		bucket, class, _, _ := plabComboShape(legs)
		routes := []string(nil)
		if cohort == storage.ComboLabCohortRollingPositive {
			routes = plabPositiveSystemRoutes(legs)
			if len(routes) == 0 {
				return
			}
		}
		all[id] = plabSampleCand{Legs: legs, ID: id, Bucket: bucket, Class: class, Cohort: cohort,
			Routes: routes, Score: plabStableHash("r143-grade-sample|" + cohort + "|" + id)}
	}
	add := func(legs []plabLeg) { addCohort(storage.ComboLabCohortAllEligible, legs) }
	// Broad deterministic proposals for every requested leg count.
	for k := 2; k <= maxLegs; k++ {
		for attempt := 0; attempt < 48; attempt++ {
			add(plabSamplePick(pool, k, fmt.Sprintf("%s|broad|%d|%d", episode, k, attempt), nil))
		}
	}
	// Target each real linkage group with a bounded set of stable-hash pairs.
	groups := map[string][]int{}
	for i, l := range pool {
		if l.EventKey != "" {
			groups[l.EventKey] = append(groups[l.EventKey], i)
		}
	}
	gkeys := make([]string, 0, len(groups))
	for key, g := range groups {
		if len(g) >= 2 {
			gkeys = append(gkeys, key)
		}
	}
	sort.Strings(gkeys)
	for _, key := range gkeys {
		type pair struct {
			a, b int
			h    uint64
		}
		var pairs []pair
		g := groups[key]
		for i := 0; i < len(g); i++ {
			for j := i + 1; j < len(g); j++ {
				pid := plabLegStableKey(pool[g[i]]) + "&" + plabLegStableKey(pool[g[j]])
				pairs = append(pairs, pair{a: g[i], b: g[j], h: plabStableHash("r133-linked|" + pid)})
			}
		}
		sort.Slice(pairs, func(i, j int) bool { return pairs[i].h < pairs[j].h })
		if len(pairs) > 6 {
			pairs = pairs[:6]
		}
		for _, p := range pairs {
			for k := 2; k <= maxLegs; k++ {
				add(plabSamplePick(pool, k, fmt.Sprintf("%s|linked|%s|%d", episode, key, k), []int{p.a, p.b}))
			}
		}
	}
	// A second explicit stratum contains only legs with a current sealed profitable system proof.
	// It is intentionally Kalshi+taker-only because the current book refresh below is an ask/taker
	// measurement. It is still synthetic settlement research, never an RFQ quote or LIVE proof.
	promoted := make([]plabLeg, 0, len(pool))
	for _, l := range pool {
		if l.Promoted && l.SystemID != "" && l.ProofRunID > 0 && l.ProofResultHash != "" && l.ProofMeanAllIn > 0 &&
			strings.EqualFold(l.Platform, "kalshi") && l.ProofCapacity >= 1 && l.ProofRoute == "taker" {
			promoted = append(promoted, l)
		}
	}
	if len(promoted) >= 2 {
		promotedMax := maxLegs
		if promotedMax > len(promoted) {
			promotedMax = len(promoted)
		}
		for k := 2; k <= promotedMax; k++ {
			for attempt := 0; attempt < 64; attempt++ {
				addCohort(storage.ComboLabCohortPromotedSystem,
					plabSamplePick(promoted, k, fmt.Sprintf("%s|promoted|%d|%d", episode, k, attempt), nil))
			}
		}
		promotedGroups := map[string][]int{}
		for i, l := range promoted {
			if l.EventKey != "" {
				promotedGroups[l.EventKey] = append(promotedGroups[l.EventKey], i)
			}
		}
		for key, g := range promotedGroups {
			if len(g) < 2 {
				continue
			}
			for k := 2; k <= promotedMax; k++ {
				addCohort(storage.ComboLabCohortPromotedSystem,
					plabSamplePick(promoted, k, fmt.Sprintf("%s|promoted-linked|%s|%d", episode, key, k), g[:2]))
			}
		}
	}
	// The rolling-positive stratum is deliberately separate from sealed promotion. It lets every
	// current positive system gather 2..6-leg PAPER evidence without implying LIVE proof.
	rollingByPlatform := map[string][]plabLeg{}
	for _, l := range pool {
		if l.RollingPositive && len(l.PositiveSystems) > 0 && l.SystemRoute == "taker" &&
			(strings.EqualFold(l.Platform, "kalshi") || strings.EqualFold(l.Platform, "polyus")) {
			platform := strings.ToLower(strings.TrimSpace(l.Platform))
			rollingByPlatform[platform] = append(rollingByPlatform[platform], l)
		}
	}
	platforms := make([]string, 0, len(rollingByPlatform))
	for platform := range rollingByPlatform {
		platforms = append(platforms, platform)
	}
	sort.Strings(platforms)
	for _, platform := range platforms {
		rolling := rollingByPlatform[platform]
		if len(rolling) < 2 {
			continue
		}
		rollingMax := maxLegs
		if rollingMax > len(rolling) {
			rollingMax = len(rolling)
		}
		for k := 2; k <= rollingMax; k++ {
			for attempt := 0; attempt < 64; attempt++ {
				addCohort(storage.ComboLabCohortRollingPositive,
					plabSamplePick(rolling, k, fmt.Sprintf("%s|rolling-positive|%s|%d|%d", episode, platform, k, attempt), nil))
			}
		}
		// Guarantee that a sparse exact system route gets deterministic proposals beside prolific
		// routes. The forced leg only guarantees route inclusion; every ordinary same-venue,
		// duplicate-instrument and twin gate still applies inside addCohort.
		routeIndices := map[string][]int{}
		for i, leg := range rolling {
			for _, route := range plabPositiveSystemRoutes([]plabLeg{leg}) {
				routeIndices[route] = append(routeIndices[route], i)
			}
		}
		routes := make([]string, 0, len(routeIndices))
		for route := range routeIndices {
			routes = append(routes, route)
		}
		sort.Strings(routes)
		for _, route := range routes {
			indices := routeIndices[route]
			for k := 2; k <= rollingMax; k++ {
				for attempt := 0; attempt < len(indices) && attempt < 4; attempt++ {
					forced := indices[attempt%len(indices)]
					addCohort(storage.ComboLabCohortRollingPositive,
						plabSamplePick(rolling, k, fmt.Sprintf("%s|rolling-route|%s|%d|%d", episode, route, k, attempt), []int{forced}))
				}
			}
		}
	}
	// Build cohort x leg-count queues, then round-robin linkage classes inside each cell.  The
	// earlier single queue sorted "all-eligible" ahead of "rolling-positive-system"; with a
	// bounded sample that let generic rows consume almost every seat before a positive-system row
	// was even visited.  Cohort-first fairness changes only which prospective rows are measured,
	// never their economic gates or LIVE authority.
	byCohortK := map[string]map[int]map[string][]plabSampleCand{}
	for _, c := range all {
		k := len(c.Legs)
		if byCohortK[c.Cohort] == nil {
			byCohortK[c.Cohort] = map[int]map[string][]plabSampleCand{}
		}
		if byCohortK[c.Cohort][k] == nil {
			byCohortK[c.Cohort][k] = map[string][]plabSampleCand{}
		}
		stratum := c.Bucket + "|" + c.Class
		byCohortK[c.Cohort][k][stratum] = append(byCohortK[c.Cohort][k][stratum], c)
	}
	cohortOrder := []string{storage.ComboLabCohortRollingPositive,
		storage.ComboLabCohortPromotedSystem, storage.ComboLabCohortAllEligible}
	queues := map[string]map[int][]plabSampleCand{}
	for _, cohort := range cohortOrder {
		queues[cohort] = map[int][]plabSampleCand{}
		for k := 2; k <= maxLegs; k++ {
			strata := byCohortK[cohort][k]
			keys := make([]string, 0, len(strata))
			for key := range strata {
				keys = append(keys, key)
				sort.Slice(strata[key], func(i, j int) bool {
					if strata[key][i].Score != strata[key][j].Score {
						return strata[key][i].Score < strata[key][j].Score
					}
					return strata[key][i].ID < strata[key][j].ID
				})
			}
			sort.Strings(keys)
			for round := 0; ; round++ {
				progress := false
				for _, key := range keys {
					if round < len(strata[key]) {
						queues[cohort][k] = append(queues[cohort][k], strata[key][round])
						progress = true
					}
				}
				if !progress {
					break
				}
			}
		}
	}
	// Reserve one candidate per exact route x feasible leg-count before the broad queues. This is
	// the bounded-sample equivalent of stratification: prolific routes can contribute more later,
	// but cannot consume the sparse route's first receipt.
	selected := map[string]bool{}
	covered := map[string]map[int]bool{}
	out := make([]plabSampleCand, 0, limit)
	appendSelected := func(c plabSampleCand) bool {
		if selected[c.ID] || len(out) >= limit {
			return false
		}
		selected[c.ID] = true
		out = append(out, c)
		for _, route := range c.Routes {
			if covered[route] == nil {
				covered[route] = map[int]bool{}
			}
			covered[route][len(c.Legs)] = true
		}
		return true
	}
	routeCells := map[string]map[int][]plabSampleCand{}
	for _, cand := range all {
		if cand.Cohort != storage.ComboLabCohortRollingPositive {
			continue
		}
		for _, route := range cand.Routes {
			if routeCells[route] == nil {
				routeCells[route] = map[int][]plabSampleCand{}
			}
			routeCells[route][len(cand.Legs)] = append(routeCells[route][len(cand.Legs)], cand)
		}
	}
	routeOrder := make([]string, 0, len(routeCells))
	for route := range routeCells {
		routeOrder = append(routeOrder, route)
		for k := 2; k <= maxLegs; k++ {
			sort.Slice(routeCells[route][k], func(i, j int) bool {
				if routeCells[route][k][i].Score != routeCells[route][k][j].Score {
					return routeCells[route][k][i].Score < routeCells[route][k][j].Score
				}
				return routeCells[route][k][i].ID < routeCells[route][k][j].ID
			})
		}
	}
	sort.Slice(routeOrder, func(i, j int) bool {
		hi := plabStableHash(episode + "|route-fair|" + routeOrder[i])
		hj := plabStableHash(episode + "|route-fair|" + routeOrder[j])
		if hi != hj {
			return hi < hj
		}
		return routeOrder[i] < routeOrder[j]
	})
	// First preserve at least one rolling-positive receipt at every constructible leg count. This
	// keeps the operator's 2..6 view honest even when the route count itself exceeds the row cap.
	for k := 2; k <= maxLegs && len(out) < limit; k++ {
		for _, route := range routeOrder {
			if len(routeCells[route][k]) == 0 {
				continue
			}
			if appendSelected(routeCells[route][k][0]) {
				break
			}
		}
	}
	for k := 2; k <= maxLegs && len(out) < limit; k++ {
		for _, route := range routeOrder {
			if covered[route][k] {
				continue
			}
			for _, cand := range routeCells[route][k] {
				if appendSelected(cand) {
					break
				}
			}
			if len(out) >= limit {
				break
			}
		}
	}
	// Global round-robin then fills remaining seats without duplicating reserved rows. It prevents
	// either a cohort or numerous 2-leg classes from starving the other research strata.
	pos := map[string]map[int]int{}
	for _, cohort := range cohortOrder {
		pos[cohort] = map[int]int{}
	}
	for len(out) < limit {
		progress := false
		for _, cohort := range cohortOrder {
			for k := 2; k <= maxLegs && len(out) < limit; k++ {
				for pos[cohort][k] < len(queues[cohort][k]) {
					cand := queues[cohort][k][pos[cohort][k]]
					pos[cohort][k]++
					if appendSelected(cand) {
						progress = true
						break
					}
				}
			}
		}
		if !progress {
			break
		}
	}
	return out, episode
}

func (s *Server) plabExecutableQuote(ctx context.Context, l plabLeg) (ask, depth float64, src string, ok bool) {
	if s.plabQuoteFn != nil {
		return s.plabQuoteFn(ctx, l)
	}
	// A current ladder on a closed/halted market is a stale artifact, not an executable quote.
	// Lifecycle comes from the same strict current caches used by each venue's pre-arm checks.
	switch strings.ToLower(l.Platform) {
	case "kalshi":
		m, known := s.kmkt(l.Ticker)
		if !known || m.Result != "" || (m.Status != "" && !strings.EqualFold(m.Status, "active") && !strings.EqualFold(m.Status, "open")) {
			return 0, 0, "kalshi-lifecycle", false
		}
		if ts, parsed := parseTime(firstNonEmpty(m.ExpectedExpiration, m.CloseTime)); parsed && !ts.After(time.Now()) {
			return 0, 0, "kalshi-lifecycle", false
		}
	case "polyus":
		open := false
		s.polyUSMu.Lock()
		if !s.polyUSAt.IsZero() && time.Since(s.polyUSAt) <= 5*time.Minute {
			for i := range s.polyUSMkts {
				m := s.polyUSMkts[i]
				// polyUSMkts is built only from strict-open source markets; a fresh exact-slug
				// presence is its lifecycle proof. Resolve is the venue endDate when supplied.
				if m.Slug == l.Ticker {
					open = true
					if ts, parsed := parseTime(m.Resolve); parsed && !ts.After(time.Now()) {
						open = false
					}
					break
				}
			}
		}
		if !open && !s.pusSweepAt.IsZero() && time.Since(s.pusSweepAt) <= 5*time.Minute {
			for i := range s.pusSweep {
				m := s.pusSweep[i]
				if m.Slug == l.Ticker && m.Open() {
					open = true
					if ts, parsed := parseTime(m.EndDate); parsed && !ts.After(time.Now()) {
						open = false
					}
					break
				}
			}
		}
		s.polyUSMu.Unlock()
		if !open {
			return 0, 0, "polyus-lifecycle", false
		}
	}
	side := strings.ToUpper(l.Side)
	switch strings.ToLower(l.Platform) {
	case "kalshi":
		if s.kal == nil {
			return 0, 0, "", false
		}
		ob, _, live := s.kal.LiveBook(l.Ticker, 5*time.Second)
		if live {
			src = "kalshi-ws"
		} else {
			ob, _ = s.kal.GetOrderbook(ctx, l.Ticker)
			src = "kalshi-rest"
		}
		if ob == nil {
			return 0, 0, src, false
		}
		if side == "NO" {
			if len(ob.YesBids) == 0 {
				return 0, 0, src, false
			}
			return 1 - ob.YesBids[0].Price, ob.YesBids[0].Size, src, true
		}
		if len(ob.YesAsks) == 0 {
			return 0, 0, src, false
		}
		return ob.YesAsks[0].Price, ob.YesAsks[0].Size, src, true
	case "polyus":
		if s.polyUSWS == nil {
			return 0, 0, "", false
		}
		bid, a, live := s.polyUSWS.LiveBidAsk(l.Ticker)
		if !live {
			return 0, 0, "polyus-ws", false
		}
		_, _, bids, asks, _ := s.polyUSWS.Book3(l.Ticker)
		if side == "NO" {
			return 1 - bid, bids, "polyus-ws", bid > 0 && bid < 1
		}
		return a, asks, "polyus-ws", a > 0 && a < 1
	default:
		// Poly-int has a separate outcome token book for every side. A Gamma probability or
		// 1-YES-bid complement is discovery data, not an executable NO quote. Parlay Lab uses
		// the same exact-book rule as xvlock and therefore fails closed unless the requested
		// outcome's current CLOB ask and depth are present.
		q, fresh := s.xvlPintCached(l.Ticker)
		return plabPintOutcomeAsk(q, side, fresh)
	}
}

func plabPintOutcomeAsk(q xvlPintQuote, side string, fresh bool) (ask, depth float64, src string, ok bool) {
	if !fresh {
		return 0, 0, "polyint-ws", false
	}
	if strings.EqualFold(side, "NO") {
		if q.NoBestAsk <= 0 || q.NoBestAsk >= 1 || q.NoBestAskDepth <= 0 {
			return 0, 0, "polyint-ws", false
		}
		return q.NoBestAsk, q.NoBestAskDepth, "polyint-ws", true
	}
	if q.BestAsk <= 0 || q.BestAsk >= 1 || q.BestAskDepth <= 0 {
		return 0, 0, "polyint-ws", false
	}
	return q.BestAsk, q.BestAskDepth, "polyint-ws", true
}

type plabQuoteResult struct {
	ask, depth float64
	src        string
	ok         bool
}

// plabExecutableQuotes prices the entire candidate universe with a small worker pool. The old
// serial loop made one slow REST fallback block every later leg (an 80s manifest tick in R133).
// Every input is still attempted exactly once and results retain input order, so this changes wall
// time only: the lossless manifest still represents every subset of the same quote-validated pool.
// The venue clients' shared limiter remains the request-rate authority.
func (s *Server) plabExecutableQuotes(ctx context.Context, in []plabLeg) []plabQuoteResult {
	out := make([]plabQuoteResult, len(in))
	if len(in) == 0 {
		return out
	}
	workers := 8
	if workers > len(in) {
		workers = len(in)
	}
	jobs := make(chan int)
	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func() {
			defer wg.Done()
			for i := range jobs {
				ask, depth, src, ok := s.plabExecutableQuote(ctx, in[i])
				out[i] = plabQuoteResult{ask: ask, depth: depth, src: src, ok: ok}
			}
		}()
	}
	for i := range in {
		select {
		case jobs <- i:
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			return out
		}
	}
	close(jobs)
	wg.Wait()
	return out
}

// plabExactTakerFee is the prospective sample's fail-closed fee gate. Unlike broader
// research/display helpers, it never substitutes a standard schedule when authoritative
// per-market metadata is missing or stale.
func (s *Server) plabExactTakerFee(l plabLeg, contracts float64) (float64, bool) {
	if s.plabSampleFeeFn != nil {
		return s.plabSampleFeeFn(l, contracts)
	}
	if contracts <= 0 || l.Price <= 0 || l.Price >= 1 {
		return 0, false
	}
	switch strings.ToLower(l.Platform) {
	case "kalshi":
		fee, known, _ := s.kalFeeExact(l.Ticker, false, contracts, l.Price)
		return fee, known
	case "polyus":
		var theta float64
		found, fresh := false, false
		s.polyUSMu.Lock()
		for i := range s.polyUSMkts {
			m := s.polyUSMkts[i]
			if m.Slug == l.Ticker && m.FeeCoeff != nil {
				theta, found = *m.FeeCoeff, true
				fresh = !s.polyUSAt.IsZero() && time.Since(s.polyUSAt) <= 5*time.Minute
				break
			}
		}
		if !found {
			for i := range s.pusSweep {
				m := s.pusSweep[i]
				if m.Slug == l.Ticker && m.FeeCoeff != nil {
					theta, found = *m.FeeCoeff, true
					fresh = !s.pusSweepAt.IsZero() && time.Since(s.pusSweepAt) <= 5*time.Minute
					break
				}
			}
		}
		s.polyUSMu.Unlock()
		if !found || !fresh || !validPolyUSFeeTheta(theta) {
			return 0, false
		}
		return polyUSFeeWithTheta(false, contracts, l.Price, theta), true
	default: // research-only poly-int: exact market curve or refuse
		if s.poly == nil {
			return 0, false
		}
		feeID := l.Ticker
		if q, fresh := s.xvlPintCached(l.Ticker); fresh && q.ConditionID != "" {
			feeID = q.ConditionID // CLOB fee schedules are condition-id keyed; lab legs may carry slugs
		}
		return s.poly.ClobFeeUSD(feeID, contracts, l.Price, true)
	}
}

func (s *Server) plabExactSyntheticFees(legs []plabLeg) (feeSyn float64, legFees []float64, ok bool) {
	contracts := 1.0
	legFees = make([]float64, len(legs))
	for i, l := range legs {
		if l.Price <= 0 || l.Price >= 1 || l.PWin <= 0 || l.PWin >= 1 {
			return 0, nil, false
		}
		contracts /= l.Price
		fee, known := s.plabExactTakerFee(l, contracts)
		if !known || math.IsNaN(fee) || math.IsInf(fee, 0) {
			return 0, nil, false
		}
		legFees[i] = fee
		feeSyn += fee // funded placement charges/reserves every rolled-leg fee immediately
	}
	return feeSyn, legFees, true
}

type plabSampleWriteStats struct {
	Episode                             string
	AdmissionEpoch                      int64
	Proposed, Eligible                  int
	Inserted, OpenAfter                 int
	CurrentOpenBefore, CurrentOpenAfter int
	LegacyOpen                          int
	Rejected                            map[string]int
	PositiveRouteCoverage               []plabPositiveRouteCoverage
}

// plabWriteProspectiveSample inserts a small settlement sample beside the lossless manifest.
// Stable combo IDs provide restart-safe open-episode dedup; the hard open-row ceiling bounds DB
// growth. Every row repeats joint-probability, duplicate-twin, legality, exact-fee and combo-floor
// gates. This never reports the raw manifest space as evaluated.
func (s *Server) plabWriteProspectiveSample(ctx context.Context, pool []plabLeg, maxLegs int, comboFloor float64, now time.Time) plabSampleWriteStats {
	proposals, episode := plabStableProspectiveSample(pool, maxLegs, plabSampleProposalN)
	st := plabSampleWriteStats{Episode: episode, Proposed: len(proposals),
		Rejected:              map[string]int{},
		PositiveRouteCoverage: plabBuildPositiveRouteCoverage(pool, proposals, maxLegs)}
	reject := func(reason string) { st.Rejected[reason]++ }
	admissionEpoch, admissionErr := s.plabCurrentAdmissionEpoch(ctx, now, true)
	if admissionErr != nil {
		reject("admission_epoch_error")
		return st
	}
	st.AdmissionEpoch = admissionEpoch
	if len(proposals) == 0 {
		reject("no_structurally_valid_proposal")
	}
	openN, err := s.store.PlabOpenCount(ctx)
	currentOpen, currentErr := s.store.PlabOpenCurrentCountSince(ctx, boundedParlayMaxLegs(maxLegs), admissionEpoch)
	st.OpenAfter = int(openN)
	st.CurrentOpenBefore, st.CurrentOpenAfter = int(currentOpen), int(currentOpen)
	if openN > currentOpen {
		st.LegacyOpen = int(openN - currentOpen)
	}
	if err != nil || currentErr != nil {
		reject("open_count_error")
		return st
	}
	if currentOpen >= plabSampleOpenCap {
		reject("current_open_cap")
		return st
	}
	room := plabSampleOpenCap - int(currentOpen)
	if room > plabSamplePerManifest {
		room = plabSamplePerManifest
	}
	batch := make([]storage.PlabCand, 0, room)
	processed := 0
	for _, c := range proposals {
		if len(batch) >= room {
			st.Rejected["not_evaluated_room_limit"] += len(proposals) - processed
			break
		}
		if ctx.Err() != nil {
			st.Rejected["not_evaluated_context_cancelled"] += len(proposals) - processed
			break
		}
		processed++
		if plabValidatePricedLegSet(c.Legs) != nil {
			reject("invalid_or_repeated_leg")
			continue
		}
		_, _, _, ovg := plabComboShape(c.Legs)
		s.plabMu.Lock()
		jp, rho, rhoN := s.plabJointPEx(c.Legs, ovg)
		s.plabMu.Unlock()
		prod := 1.0
		for _, l := range c.Legs {
			prod *= l.Price
		}
		if prod <= 0 || prod >= 1 || jp <= 0 || math.IsNaN(prod) || math.IsNaN(jp) {
			reject("invalid_joint_or_price_product")
			continue
		}
		feeSyn, legFees, feeOK := s.plabExactSyntheticFees(c.Legs)
		if !feeOK {
			reject("exact_fee_unavailable")
			continue
		}
		evSyn, economicsOK := plabAllInReturn(jp, prod, feeSyn)
		if !economicsOK {
			reject("invalid_all_in_economics")
			continue
		}
		if evSyn < comboFloor {
			reject("below_fee_adjusted_edge_floor")
			continue
		}
		legality, corrCl := s.plabLegality(ctx, c.Legs, evSyn)
		if legality == "illegal" || legality == "illegal_probed" {
			reject("venue_or_identity_illegal")
			continue
		}
		routeState := "synthetic-settlement-only"
		if c.Cohort == storage.ComboLabCohortRollingPositive {
			routeState = "paper-only-rolling-positive"
		} else if c.Cohort == storage.ComboLabCohortPromotedSystem {
			// A lookup-only legality probe is not an executable combined-market quote. Creating an
			// RFQ merely to collect research rows would mutate venue state, so record the blocker and
			// refuse any claim that this synthetic row can transfer 1:1 into LIVE.
			routeState = "awaiting-rfq-identical-proof"
		}
		st.Eligible++
		ent := plabOpenEnt{ID: plabObservationID(c.ID, now), At: now, Legs: c.Legs, Bucket: c.Bucket, Class: c.Class,
			Prod: prod, JointP: jp, Rho: rho, RhoN: rhoN, FeeSyn: feeSyn, FeeMVE: -1,
			EVSyn: evSyn, EVMVE: -1000, LegFees: legFees, Legality: legality, CorrCluster: corrCl,
			Cohort: c.Cohort, RouteState: routeState}
		batch = append(batch, plabToRow(ent))
	}
	if len(batch) == 0 {
		return st
	}
	nIns, err := s.store.PlabInsertBatch(ctx, batch)
	if err != nil {
		reject("insert_error")
		return st
	}
	if ignored := int64(len(batch)) - nIns; ignored > 0 {
		st.Rejected["duplicate_observation_ignored"] += int(ignored)
	}
	st.Inserted, st.OpenAfter = int(nIns), int(openN+nIns)
	st.CurrentOpenAfter = int(currentOpen + nIns)
	if st.Inserted > 0 {
		s.plabMu.Lock()
		if s.plabSeen == nil {
			s.plabSeen = map[string]time.Time{}
		}
		for _, row := range batch {
			s.plabSeen[row.ID] = now
		}
		pruneTimeMap(s.plabSeen, func(t time.Time) time.Time { return t }, plabSeenCap, plabDedupTTL)
		s.plabOpenN, s.plabOpenNAt = st.OpenAfter, time.Now()
		s.plabCand += st.Inserted
		s.plabHourLogged.add(time.Now(), st.Inserted)
		s.plabMu.Unlock()
	}
	return st
}

// plabWriteManifest replaces combo×leg materialization. One immutable, executable-quote pool plus
// the exact predicate/correlation snapshot denotes every acceptable subset. Logical coverage is
// never sampled or evicted; only a separately-labelled bounded prospective grade sample becomes
// settlement rows so the estimator can keep learning without recreating unbounded DB growth.
func (s *Server) plabWriteManifest(ctx context.Context, in []plabLeg, singleFloor float64) {
	a := s.cfg().Auto
	maxLegs := boundedParlayMaxLegs(a.ParlayLabMaxLegs)
	comboFloor := a.ParlayLabComboFloor
	now := time.Now().UTC()
	admissionEpoch, admissionErr := s.plabCurrentAdmissionEpoch(ctx, now, true)
	admissionEpochText := ""
	if admissionErr == nil && admissionEpoch > 0 {
		admissionEpochText = time.Unix(admissionEpoch, 0).UTC().Format(time.RFC3339)
	}
	typedPositiveBundles := s.plabTypedPositiveBundleRefs(ctx, now)
	typedPositiveBySystem := map[string]int{}
	typedPositiveByLegs := map[int]int{}
	for _, bundle := range typedPositiveBundles {
		typedPositiveBySystem[bundle.SystemID]++
		typedPositiveByLegs[len(bundle.Legs)]++
	}
	horizonRejected := map[string]int{}
	current := make([]plabLeg, 0, len(in))
	for _, l := range in {
		_, reason, ok := s.plabLegCurrentHorizon(ctx, l, now)
		if l.RollingPositive {
			_, reason, ok = s.paperComboLegCurrentHorizon(ctx, l, now)
		}
		if !ok {
			horizonRejected[reason]++
			continue
		}
		current = append(current, l)
	}
	if len(horizonRejected) > 0 {
		dctx, cancel := researchDurabilityContext(ctx)
		s.persistPlabHorizonCounts(dctx, horizonRejected, nil)
		cancel()
	}
	// Publish pre-quote exact positive candidates to the bounded depth planner. In particular,
	// PolyUS needs this bootstrap hint before Book3 can become fresh; requiring depth here made the
	// candidate depend circularly on a subscription it could never request.
	s.cachePositiveDepthBookGroups(current)
	if len(current) < 2 && len(typedPositiveBundles) == 0 {
		s.autoPlacePositiveSystemCombos(ctx, nil)
		return
	}
	pool := make([]plabLeg, 0, len(current))
	quotes := s.plabExecutableQuotes(ctx, current)
	for i, l := range current {
		q := quotes[i]
		if !q.ok || q.ask <= 0 || q.ask >= 1 || q.depth <= 0 {
			continue
		}
		if l.Promoted && (!strings.EqualFold(l.Platform, "kalshi") || l.ProofRoute != "taker" || l.ProofMeanAllIn <= 0 ||
			l.ProofRunID <= 0 || l.ProofResultHash == "" || l.ProofCapacity < 1 || q.depth < 1) {
			continue
		}
		l.Price, l.Depth, l.QuoteSrc, l.QuoteTS = q.ask, q.depth, q.src, now.Format(time.RFC3339Nano)
		fee, feeOK := s.plabExactTakerFee(l, 1)
		if !feeOK || fee < 0 || math.IsNaN(fee) || math.IsInf(fee, 0) {
			continue
		}
		if l.Promoted {
			// Reprice the sealed lower bound at this exact fresh ask+fee. Any adverse move relative
			// to the immutable candidate consumes the bound before this leg may enter the cohort.
			proofObserved, _ := time.Parse(time.RFC3339Nano, l.ProofObserved)
			if proofObserved.IsZero() || now.Sub(proofObserved) > 20*time.Second {
				continue
			}
			observedAllIn := l.Price + fee
			lowerNow := l.ProofLowerPC - math.Max(0, observedAllIn-l.ProofMeanAllIn)
			pwin, probabilityOK := plabProbabilityFromEdge(observedAllIn, lowerNow)
			if !probabilityOK {
				continue
			}
			l.PWin = pwin
		}
		l.EVNet = l.PWin - q.ask - fee
		l.TwinKey = s.twinKeyGate(l.Platform, l.Ticker)
		if l.EVNet >= singleFloor || (l.RollingPositive && l.EVNet > 0) {
			pool = append(pool, l)
		}
	}
	sort.Slice(pool, func(i, j int) bool {
		if pool[i].EVNet != pool[j].EVNet {
			return pool[i].EVNet > pool[j].EVNet
		}
		if pool[i].Platform != pool[j].Platform {
			return pool[i].Platform < pool[j].Platform
		}
		return pool[i].Ticker < pool[j].Ticker
	})
	// The old generic ML parlay bettor stays archived. AUTO Paper may fund only this freshly
	// repriced same-venue positive-system cohort; the placement function repeats all money gates.
	s.autoPlacePositiveSystemCombos(ctx, pool)
	raw := plabRawSpace(len(pool), maxLegs)
	uniqueInstruments, structuralSpace, sameVenueSpace := plabStructuralSpaces(pool, maxLegs)
	sampleEpisode := plabSampleEpisode(pool, maxLegs)
	corr := map[string]plabCorrC{}
	s.plabMu.Lock()
	for k, v := range s.plabCorr {
		if v != nil {
			corr[k] = *v
		}
	}
	s.plabMu.Unlock()
	m := plabManifest{CreatedAt: now.Format(time.RFC3339Nano), AlgoVersion: "r147-combo-funnel-v7",
		SingleFloor: singleFloor, ComboFloor: comboFloor, MaxLegs: maxLegs, RawSpaceDec: raw.String(),
		UniqueInstruments: uniqueInstruments, StructuralSpaceDec: structuralSpace.String(),
		SameVenueSpaceDec:    sameVenueSpace.String(),
		SampleEpisode:        sampleEpisode,
		SampleAdmissionEpoch: admissionEpochText,
		Legs:                 pool,
		TypedPositiveBundles: typedPositiveBundles,
		Corr:                 corr, Rules: map[string]any{
			"representation": "implicit-all-subsets", "min_legs": 2,
			"max_legs": maxLegs, "operator_name": "Combo Lab",
			"combination_denominators": map[string]any{
				"raw_space_dec":           "route-row upper bound before duplicate-instrument rejection",
				"structural_space_dec":    "at most one route/side row per venue+ticker; mixed venues still included",
				"same_venue_space_dec":    "at most one route/side row per venue+ticker and every leg on one venue; upper bound before twin/economic/legal gates",
				"unique_instruments":      "distinct venue+ticker instruments",
				"actual_prospective_rows": "sample_proposed -> sample_eligible -> sample_inserted, with explicit rejection reasons",
			},
			"current_horizon": map[string]any{"regular_hard_max_hours": plabRegularHardMax.Hours(), "crypto_hard_max_hours": plabCryptoHardMax.Hours(),
				"configured_regular_hours": a.ParlayMaxHoursOut, "configured_crypto_hours": a.ConsensusCryptoMaxHoursOut,
				"unknown_or_nonpositive": "rejected", "generation_rejections": horizonRejected},
			"no_duplicate_twin_key": true, "joint_probability_required": true,
			"actual_combo_execution_requires_live_mve_quote": true,
			"synthetic_sequential_roll_is_research_only":     true,
			"synthetic_fee_convention":                       "all rolled-leg taker fees reserved at entry; no probability discount",
			"cohorts": []string{storage.ComboLabCohortAllEligible, storage.ComboLabCohortRollingPositive,
				storage.ComboLabCohortPromotedSystem},
			"rolling_positive_system_rule":     "PAPER-only and n-independent: every leg comes from a current positive named system, then independently clears current executable taker ask, depth, exact fee, lifecycle, horizon and duplicate/twin identity gates; never sealed or LIVE-authorizing",
			"promoted_system_combo_rule":       "every leg must have current sealed profitable single-system proof, Kalshi taker book truth, exact fee/depth/freshness/capacity and legal lookup; remains awaiting-RFQ and not LIVE-transferable until identical combo route/Paper/holdout proof",
			"typed_positive_route_bundle_rule": "PAPER-research and n-independent: verified or structural immutable 2-6-leg research bundles with positive frozen net floor retain their exact ordered books, depth, fees, payoff states, certificate status, blocker and native route-bundle grading. Structural/blocked rows collect the evidence their uncertainty needs; they are never rewritten as all-win parlays and grant no promotion or LIVE authority",
			"fee_model":                        "exact-venue-schedule-at-executable-leg-quote", "exact_fee_required": true,
			"prospective_grade_sample": map[string]any{
				"method":                "stable-hash stratified by cohort, leg count, linkage class, and exact positive system+venue+taker route",
				"max_rows_per_manifest": plabSamplePerManifest, "max_current_2_to_6_open_rows": plabSampleOpenCap,
				"admission_epoch": admissionEpochText,
				"legacy_before_current_horizon_or_above_leg_limit_excluded_from_cap": true,
				"route_coverage_receipt": "API enumeration.positive_system_route_coverage reports proposed-by-leg, constructible leg counts, honest cap misses, and current-universe infeasibility",
				"episode_dedup":          "stable leg-set id + INSERT OR IGNORE",
				"exact_fee_required":     true, "not_full_space_evaluation": true,
			},
		}}
	base, _ := json.Marshal(m)
	h := sha1.Sum(base)
	m.ID = hex.EncodeToString(h[:])
	body, err := json.MarshalIndent(m, "", "  ")
	dir := filepath.Join(s.cfg().DataDir, "parlay_manifests")
	if err == nil {
		err = os.MkdirAll(dir, 0o755)
	}
	path := filepath.Join(dir, fmt.Sprintf("%d_%s.json", now.UnixNano(), m.ID[:12]))
	if err == nil {
		tmp := path + ".tmp"
		if err = os.WriteFile(tmp, body, 0o644); err == nil {
			err = os.Rename(tmp, path)
		}
	}
	sample := plabSampleWriteStats{Episode: sampleEpisode, Rejected: map[string]int{}}
	if err == nil {
		sample = s.plabWriteProspectiveSample(ctx, pool, maxLegs, comboFloor, now)
	}
	f64, _ := new(big.Float).SetInt(raw).Float64()
	est := plabEnumStats{PoolN: len(pool), MaxLegs: maxLegs, RawSpace: f64, RawSpaceDec: raw.String(),
		UniqueInstruments: uniqueInstruments, StructuralSpaceDec: structuralSpace.String(), SameVenueSpaceDec: sameVenueSpace.String(),
		Complete: true, Representation: "manifest", At: now.Format(time.RFC3339), MS: float64(time.Since(now).Milliseconds()),
		SampleEpisode: sample.Episode, SampleAdmissionEpoch: admissionEpochText,
		SampleProposed: sample.Proposed, SampleEligible: sample.Eligible,
		SampleInserted: sample.Inserted, SampleRejected: sample.Rejected, SampleOpenCap: plabSampleOpenCap,
		SampleCurrentOpen: sample.CurrentOpenAfter, SampleLegacyOpen: sample.LegacyOpen,
		PositiveRouteCoverage: sample.PositiveRouteCoverage,
		TypedPositiveBundles:  len(typedPositiveBundles), TypedPositiveBySystem: typedPositiveBySystem,
		TypedPositiveByLegs: typedPositiveByLegs}
	s.plabMu.Lock()
	s.plabEnum = est
	auditDue := time.Since(s.plabEnumAuditAt) > 30*time.Minute
	if auditDue {
		s.plabEnumAuditAt = time.Now()
	}
	s.plabMu.Unlock()
	if err != nil || auditDue {
		dctx, cancel := researchDurabilityContext(ctx)
		defer cancel()
		if err != nil {
			_ = s.store.Audit(dctx, "error", "parlaylab", "R132 manifest write failed; legacy materialization stayed disabled: "+err.Error(), "")
		} else {
			_ = s.store.Audit(dctx, "info", "parlaylab", fmt.Sprintf(
				"R146 combo manifest: %d route rows / %d instruments; raw route-row upper bound %s, duplicate-safe structural upper bound %s, same-venue upper bound %s (2..%d legs, not evaluated); prospective funnel proposed=%d eligible=%d inserted=%d, open cap %d",
				len(pool), uniqueInstruments, raw.String(), structuralSpace.String(), sameVenueSpace.String(), maxLegs,
				sample.Proposed, sample.Eligible, sample.Inserted, plabSampleOpenCap), "")
		}
	}
}

// plabEnumerateEx — R123 Part 2: PROVABLY-COMPLETE branch-and-bound enumeration (operator:
// every qualifying combination through the current project-wide six-leg ceiling, replacing
// R122's bucketed sampling).
//
// MATH: under independence a parlay clears an EV floor F iff jp/∏price > 1+fees+F, i.e.
// Σ log(p_i/price_i) > log(1+fees+F). Fees are ≥ 0 and same-event correlation can lift joint p
// by at most a bounded slack (computed by the caller from the class-rho table), so walking ALL
// combos with Σ log-edge ≥ log(1+F) − slack is a SUPERSET of every possibly-+EV combo; the
// EXACT fee-and-correlation-adjusted floor is re-tested per emitted combo before logging.
//
// Branch-and-bound on the DESC-sorted log-edges prunes provably: bestAdd[i][r] = the best
// achievable Σ from suffix i with ≤ r more legs, so a prefix dies the moment even the best
// remaining legs cannot reach the threshold (and, suffix bounds being monotone, the whole
// sibling loop dies with it). When the space above the floor exceeds the per-cycle budget, the
// walked threshold T is RAISED via bisection until the unseen count fits — the FLOOR is the
// only cut, NEVER random sampling (operator order) — and T is reported. The 12h dedup snapshot
// (`seen`) streams successive 2-min cycles down the edge ladder.
//
// twinKey[i] carries each leg's verified-identical-twin key ("" = none): two legs sharing one
// are the same real bet restated — excluded at the walk (restatement guard).
func plabEnumerateEx(pool []plabLeg, twinKey []string, maxLegs, budget int, floorLE float64,
	seen func(id string) bool, deadline time.Time) ([][]plabLeg, plabEnumStats) {
	maxLegs = boundedParlayMaxLegs(maxLegs)
	st := plabEnumStats{PoolN: len(pool), MaxLegs: maxLegs, FloorLogEdge: floorLE}
	if len(pool) < 2 || maxLegs < 2 || budget <= 0 {
		st.Complete = true
		return nil, st
	}
	if seen == nil {
		seen = func(string) bool { return false }
	}
	n := len(pool)
	if maxLegs > n {
		maxLegs = n
		st.MaxLegs = n
	}
	// sort DESC by log-edge INTERNALLY (the pruning bound's monotonicity depends on it —
	// completeness must never hinge on caller discipline). Pool legs guarantee 0<price<1, 0<p<1.
	{
		ord := make([]int, n)
		for i := range ord {
			ord[i] = i
		}
		sort.Slice(ord, func(a, b int) bool {
			return pool[ord[a]].PWin/pool[ord[a]].Price > pool[ord[b]].PWin/pool[ord[b]].Price
		})
		p2 := make([]plabLeg, n)
		t2 := make([]string, n)
		for x, i := range ord {
			p2[x] = pool[i]
			t2[x] = twinKey[i]
		}
		pool, twinKey = p2, t2
	}
	le := make([]float64, n)
	for i, l := range pool {
		le[i] = math.Log(l.PWin / l.Price)
	}
	// raw combination space Σ C(n,k), k=2..maxLegs (float64 — honest denominator, huge on purpose)
	{
		c := 1.0
		for k := 1; k <= maxLegs; k++ {
			c = c * float64(n-k+1) / float64(k)
			if k >= 2 {
				st.RawSpace += c
			}
		}
	}
	// bestAdd[i][r]: best achievable Σ log-edge from suffix i using ≤ r legs
	bestAdd := make([][]float64, n+1)
	for i := range bestAdd {
		bestAdd[i] = make([]float64, maxLegs+1)
	}
	for i := n - 1; i >= 0; i-- {
		for r := 0; r <= maxLegs; r++ {
			v := bestAdd[i+1][r]
			if r > 0 && le[i] > 0 {
				if w := le[i] + bestAdd[i+1][r-1]; w > v {
					v = w
				}
			}
			bestAdd[i][r] = v
		}
	}
	var out [][]plabLeg
	// walk enumerates unseen combos with Σ log-edge ≥ T, counting up to cap; collect=true also
	// gathers them. Returns (count, completed-without-abort, nodes).
	walk := func(T float64, cap int, collect bool) (int, bool, int) {
		cnt, nodes := 0, 0
		idxs := make([]int, 0, maxLegs)
		tw := make([]string, 0, maxLegs)
		var dfs func(start int, sum float64) bool
		dfs = func(start int, sum float64) bool {
			k := len(idxs)
			if k >= maxLegs {
				return true
			}
			for j := start; j < n; j++ {
				nodes++
				if nodes&4095 == 0 && time.Now().After(deadline) {
					return false
				}
				// bound is non-increasing in j (le desc, suffix best shrinks) ⇒ break, not continue
				if sum+le[j]+bestAdd[j+1][maxLegs-k-1] < T {
					break
				}
				if twinKey[j] != "" {
					dup := false
					for _, t := range tw {
						if t == twinKey[j] {
							dup = true
							break
						}
					}
					if dup {
						st.TwinSkipped++
						continue
					}
				}
				idxs = append(idxs, j)
				tw = append(tw, twinKey[j])
				s2 := sum + le[j]
				if len(idxs) >= 2 && s2 >= T {
					legs := make([]plabLeg, len(idxs))
					for x, ii := range idxs {
						legs[x] = pool[ii]
					}
					if !seen(plabID(legs)) {
						cnt++
						if collect {
							out = append(out, legs)
						}
						if cnt >= cap {
							idxs = idxs[:len(idxs)-1]
							tw = tw[:len(tw)-1]
							return false
						}
					}
				}
				ok := dfs(j+1, s2)
				idxs = idxs[:len(idxs)-1]
				tw = tw[:len(tw)-1]
				if !ok {
					return false
				}
			}
			return true
		}
		completed := dfs(0, 0)
		return cnt, completed, nodes
	}
	lo, hi := floorLE, bestAdd[0][maxLegs]+1e-9
	if bestAdd[0][maxLegs] < floorLE {
		st.TEffective = floorLE
		st.Complete = true // even the best combo can't clear the floor — trivially complete
		return nil, st
	}
	cnt, complete, nodes := walk(lo, budget+1, false)
	st.Nodes += nodes
	if complete && cnt <= budget {
		st.TEffective = lo
		st.Complete = true
		st.CountAtFloor = cnt
	} else {
		st.CountAtFloor = -1 // over one cycle's budget — bisect the threshold (floor stays the only cut)
		for it := 0; it < 14; it++ {
			mid := (lo + hi) / 2
			c2, comp2, nd := walk(mid, budget+1, false)
			st.Nodes += nd
			if comp2 && c2 <= budget {
				hi = mid
			} else {
				lo = mid
			}
		}
		st.TEffective = hi
	}
	_, _, nd := walk(st.TEffective, budget, true)
	st.Nodes += nd
	st.Enumerated = len(out)
	return out, st
}

// plabComboShape classifies an N-leg combo: its event groups (≥2 legs sharing a non-empty
// EventKey), bucket, and linkage class. One overlap group keeps the R109/R122 class naming
// exactly (kv cells keep aggregating); ≥2 disjoint overlap groups are a new R123 shape.
func plabComboShape(legs []plabLeg) (bucket, class, sharedKey string, ovGroups [][]int) {
	byEv := map[string][]int{}
	order := []string{}
	for i, l := range legs {
		if l.EventKey == "" {
			continue
		}
		if _, ok := byEv[l.EventKey]; !ok {
			order = append(order, l.EventKey)
		}
		byEv[l.EventKey] = append(byEv[l.EventKey], i)
	}
	for _, k := range order {
		if len(byEv[k]) >= 2 {
			ovGroups = append(ovGroups, byEv[k])
		}
	}
	switch len(ovGroups) {
	case 0:
		return "indep", plabClass(legs, ""), "", nil
	case 1:
		sk := legs[ovGroups[0][0]].EventKey
		return "overlap", plabClass(legs, sk), sk, ovGroups
	default:
		return "overlap", fmt.Sprintf("ov:multi:%dleg", len(legs)), legs[ovGroups[0][0]].EventKey, ovGroups
	}
}

// plabJointPEx generalizes plabJointP to N legs with MULTIPLE overlap groups: each group's
// FIRST pair gets the class-rho adjustment (the R109 pair estimator, looked up by the PAIR's
// own class so the well-populated 2-leg cells price 3+-leg combos too); everything else
// multiplies independently. Caller holds plabMu (reads s.plabCorr).
func (s *Server) plabJointPEx(legs []plabLeg, ovGroups [][]int) (jp, rhoUsed float64, rhoN int) {
	jp = 1.0
	adj := map[int]bool{}
	for _, g := range ovGroups {
		i, j := g[0], g[1]
		pairClass := plabClass([]plabLeg{legs[i], legs[j]}, legs[i].EventKey)
		var rho float64
		var n int
		if cc, ok := s.plabCorr[pairClass]; ok && cc != nil {
			rho, n = plabRho(*cc)
		}
		p1, p2 := legs[i].PWin, legs[j].PWin
		p12 := p1*p2 + rho*math.Sqrt(p1*(1-p1)*p2*(1-p2))
		jp *= clampF(p12, 0.0005, 0.9995)
		adj[i], adj[j] = true, true
		if math.Abs(rho) > math.Abs(rhoUsed) {
			rhoUsed, rhoN = rho, n
		}
	}
	for i, l := range legs {
		if !adj[i] {
			jp *= l.PWin
		}
	}
	return clampF(jp, 0.0001, 0.9999), rhoUsed, rhoN
}

// plabCorrSlack bounds how much same-event correlation could LIFT a combo's log joint-p above
// the independence sum — the completeness safety margin folded into the walk floor. Per event
// group: the max positive-rho pair boost (rho from the pair's class cell); a ≤maxLegs combo
// can contain at most maxLegs/2 disjoint adjusted pairs, so the top ⌊maxLegs/2⌋ boosts sum.
// Caller holds plabMu.
func (s *Server) plabCorrSlack(pool []plabLeg, maxLegs int) float64 {
	byEv := map[string][]int{}
	for i, l := range pool {
		if l.EventKey != "" {
			byEv[l.EventKey] = append(byEv[l.EventKey], i)
		}
	}
	var boosts []float64
	for _, g := range byEv {
		if len(g) < 2 {
			continue
		}
		best := 0.0
		for a := 0; a < len(g); a++ {
			for b := a + 1; b < len(g); b++ {
				i, j := g[a], g[b]
				pairClass := plabClass([]plabLeg{pool[i], pool[j]}, pool[i].EventKey)
				cc, ok := s.plabCorr[pairClass]
				if !ok || cc == nil {
					continue
				}
				rho, _ := plabRho(*cc)
				if rho <= 0 {
					continue
				}
				p1, p2 := pool[i].PWin, pool[j].PWin
				ind := p1 * p2
				if ind <= 0 {
					continue
				}
				p12 := clampF(ind+rho*math.Sqrt(p1*(1-p1)*p2*(1-p2)), 0.0005, 0.9995)
				if b2 := math.Log(p12 / ind); b2 > best {
					best = b2
				}
			}
		}
		if best > 0 {
			boosts = append(boosts, best)
		}
	}
	sort.Sort(sort.Reverse(sort.Float64Slice(boosts)))
	slack := 0.0
	for i := 0; i < len(boosts) && i < maxLegs/2; i++ {
		slack += boosts[i]
	}
	return slack
}

// parlayLabTick is the per-cycle hook (MonitorAuto loop "parlay-lab"): enumerate + log new
// candidates, then grade settled ones. Log-only — nothing here can place an order.
func (s *Server) parlayLabTick(ctx context.Context) {
	a := s.cfg().Auto
	s.plabMu.Lock()
	s.plabState(ctx)
	s.plabMu.Unlock()
	t0 := time.Now() // R122: measured tick cost (EWMA on /api/parlaylab — the widening's honesty meter)
	// R128 ORDER: GRADE FIRST — settlement always gets fresh budget/context before the (now
	// heavyweight, unlimited) enumeration can spend the tick. Grading continuity IS logging
	// continuity (R122); the first unlimited boot measured a 143s enumerate that would have
	// starved the sweep exactly the way the old 8k ledger died.
	s.parlayLabGradePass(ctx)
	s.comboProbeTick(ctx) // R112: ask the exchange about the highest-EV unprobed leg sets
	if a.ParlayLabEnabled {
		// Grading and exchange probes above must never be skipped. Only the unbounded combinatorial
		// enumeration joins the heavy research lane; if it defers, the next ordinary tick retries
		// from the durable candidate sources while settlement continuity remains intact.
		s.tryRunHeavyResearch(ctx, "combo-lab-enumerate", 0, func(ctx context.Context) {
			s.parlayLabEnumerateAndLog(ctx)
		})
	}
	ms := float64(time.Since(t0).Milliseconds())
	s.plabMu.Lock()
	if s.plabTickMs == 0 {
		s.plabTickMs = ms
	} else {
		s.plabTickMs = 0.8*s.plabTickMs + 0.2*ms
	}
	s.plabMu.Unlock()
}

// parlayLabEnumerateAndLog — the candidate-producing half of the tick (R122 split so grading can
// run unconditionally above).
// comboWarmPrerequisitesReady is the cheap, cache-only readiness wall for the one-shot Combo
// warm pass. It does not require both venues: one healthy executable venue is enough to begin
// collecting, while every leg still faces its own exact book/fee/horizon checks below.
func (s *Server) comboWarmPrerequisitesReady() bool {
	if !s.feedsReady() {
		return false
	}
	s.metaMu.Lock()
	kalshiMarkets := len(s.kmkts)
	s.metaMu.Unlock()
	s.polyUSMu.Lock()
	polyUSMarkets := len(s.polyUSMkts) + len(s.pusSweep)
	s.polyUSMu.Unlock()
	return kalshiMarkets > 0 || polyUSMarkets > 0
}

// runComboWarmTrigger retries only the shared-gate admission, then exits after one successful
// warm enumeration. This closes the old fixed +137s blind spot without creating another periodic
// collector or overlapping the regular two-minute Combo/settlement loop.
func runComboWarmTrigger(ctx context.Context, poll time.Duration, ready func() bool, attempt func() bool) {
	if poll <= 0 {
		poll = 2 * time.Second
	}
	ticker := time.NewTicker(poll)
	defer ticker.Stop()
	for {
		if ready() && attempt() {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Server) startComboWarmTrigger(ctx context.Context) {
	go runComboWarmTrigger(ctx, 2*time.Second, s.comboWarmPrerequisitesReady, func() bool {
		return s.tryRunHeavyResearch(ctx, "combo-lab-enumerate", 0, func(workCtx context.Context) {
			s.parlayLabEnumerateAndLog(workCtx)
		})
	})
}

func (s *Server) parlayLabEnumerateAndLog(ctx context.Context) {
	floor := s.cfg().Auto.ParlayLabMinEVNet
	if floor <= 0 {
		floor = 0.005 // ½¢/contract net — a minimal single-leg quality bar, EV-based per the prime directive
	}
	// 1) leg pool from the sidecar's live predictions (the parlaySuggest read, log-only twin)
	var pr struct {
		Preds []struct {
			Ticker, Side, Platform, Title string
			Price                         float64  `json:"price"`
			PWin                          float64  `json:"p_win"`
			EV                            float64  `json:"ev_per_contract"`
			EVNet                         *float64 `json:"ev_net"`
		} `json:"predictions"`
	}
	best := map[string]plabLeg{}
	merge := func(l plabLeg) {
		key := plabPoolKey(l)
		if old, ok := best[key]; ok {
			best[key] = mergePlabLeg(old, l)
		} else {
			best[key] = l
		}
	}
	if s.readJSONLoose(filepath.Join(s.cfg().DataDir, "ml_predictions.json"), &pr) {
		for _, p := range pr.Preds {
			evn := p.EV
			if p.EVNet != nil {
				evn = *p.EVNet
			}
			if evn < floor || p.Price <= 0.02 || p.Price >= 0.98 || p.PWin <= 0 || p.PWin >= 1 {
				continue
			}
			merge(plabLeg{Ticker: p.Ticker, Side: strings.ToUpper(p.Side), Platform: p.Platform,
				Price: p.Price, PWin: p.PWin, EVNet: evn,
				EventKey: s.legEventKey(p.Platform, p.Ticker, p.Title)})
		}
	}
	// Edge-based legs join only from directly PROVEN+ emitted-side systems. Opposite controls do
	// not become legs without a separately named current score and executable-route proof.
	strictLegs, rollingSignalLegs := s.plabSignalLegSets(ctx, floor)
	for _, l := range strictLegs {
		merge(l)
	}
	for _, l := range rollingSignalLegs {
		merge(l)
	}
	for _, l := range s.plabRollingPositiveResearchLegs(ctx) {
		merge(l)
	}
	// Exact UnitTrial route cells cover model and accepted-strategy origins separately. Their
	// economics come from the exact observed ask+fee row, never a generic ML estimate sharing the
	// same instrument.
	for _, l := range s.plabRollingPositiveUnitTrialLegs(ctx) {
		merge(l)
	}
	// This explicit source is narrower than the legacy PROVEN+ edge pool: every leg owns a
	// current sealed untouched PASS plus the exact immutable candidate/governance chain. Preserve
	// its proof metadata even when a generic model emitted the same side at a rosier estimate.
	for _, l := range s.plabPromotedLegs(ctx) {
		merge(l)
	}
	if len(best) == 0 {
		return
	}
	pool := make([]plabLeg, 0, len(best))
	for _, l := range best {
		pool = append(pool, l)
	}
	// R132: one immutable manifest represents every logical subset. Never fall back to the
	// live-database combo×leg materializer, even if the manifest write itself fails.
	s.plabWriteManifest(ctx, pool, floor)
	return // fail closed: never fall back to the retired 8GB live-DB materializer
	/* R128 RETIRED REFERENCE IMPLEMENTATION.
	The row-per-combination materializer below stays commented for historical audit context only.
	It cannot compile into a fallback path, even if manifest persistence fails.
	// R123 Part 2: pool ranked by LOG-edge (log(p_win/price)) DESC — the parlay-native
	// multiplicative ranking the branch-and-bound walks; 56 concurrent picks cap (R128: 40 + room
	// for the edge/inverted legs; the honest pool size rides the enum stats). The R122
	// collection-gated 4/5-leg special cases are SUPERSEDED: the exhaustive walk covers every
	// subset ≤ maxLegs; venue-legality stays the probe cache's job per combo (plabLegality below).
	sort.Slice(pool, func(i, j int) bool { return pool[i].PWin/pool[i].Price > pool[j].PWin/pool[j].Price })
	if len(pool) > 56 {
		pool = pool[:56]
	}
	maxLegs := boundedParlayMaxLegs(a.ParlayLabMaxLegs)
	comboFloor := a.ParlayLabComboFloor // fee-net EV/$1 the combo must clear (default 0 = any +EV)
	twinKeys := make([]string, len(pool))
	for i, l := range pool {
		twinKeys[i] = s.twinKeyGate(l.Platform, l.Ticker) // restatement guard — dup keys excluded in-walk
	}
	now := time.Now().UTC()
	seenSnap := map[string]bool{}
	s.plabMu.Lock()
	for id, t := range s.plabSeen {
		if now.Sub(t) < plabDedupTTL {
			seenSnap[id] = true
		}
	}
	slack := s.plabCorrSlack(pool, maxLegs)
	provenCls := s.plabProvenClassesLocked()
	s.plabMu.Unlock()
	floorLE := 0.0
	if comboFloor > -1 {
		floorLE = math.Log1p(comboFloor)
	}
	floorLE -= slack // completeness margin: same-event correlation can lift log joint-p at most this
	combos, est := plabEnumerateEx(pool, twinKeys, maxLegs, maxPer, floorLE,
		func(id string) bool { return seenSnap[id] }, time.Now().Add(15*time.Second))
	est.CorrSlack = slack
	// shape + joint-p under ONE lock pass (plabCorr reads); fees + the exact floor run unlocked
	// (blendedFee takes its own locks — no nesting).
	type candEnt struct {
		legs                  []plabLeg
		bucket, class, shared string
		jp, rho               float64
		rhoN                  int
		proven                bool
	}
	cands := make([]candEnt, 0, len(combos))
	s.plabMu.Lock()
	for _, legs := range combos {
		bucket, class, shared, ovg := plabComboShape(legs)
		jp, rho, rhoN := s.plabJointPEx(legs, ovg)
		cands = append(cands, candEnt{legs: legs, bucket: bucket, class: class, shared: shared,
			jp: jp, rho: rho, rhoN: rhoN, proven: provenCls[class]})
	}
	s.plabMu.Unlock()
	// R123 Part 3: class-aware candidate ordering — PROVEN-POSITIVE classes first (log-only
	// influence: parlays never place; this orders logging + probe enqueueing). Stable partition
	// keeps the walk's edge ordering within each half.
	sort.SliceStable(cands, func(i, j int) bool { return cands[i].proven && !cands[j].proven })
	logged := 0
	batch := make([]storage.PlabCand, 0, len(cands)) // R128: one tx per cycle (unlimited ledger)
	procDeadline := time.Now().Add(60 * time.Second) // R128: bound the per-candidate processing
	for _, ce := range cands {
		if time.Now().After(procDeadline) {
			// Un-processed candidates were NOT seen-marked — the next cycle's walk re-finds
			// them (dedup streaming). Coverage is preserved; the tick cost is bounded.
			break
		}
		legs := ce.legs
		if plabValidatePricedLegSet(legs) != nil {
			continue
		}
		id := plabObservationID(plabID(legs), now)
		s.plabMu.Lock()
		if s.plabSeen == nil {
			s.plabSeen = map[string]time.Time{}
		}
		if t, ok := s.plabSeen[id]; ok && now.Sub(t) < plabDedupTTL {
			s.plabMu.Unlock()
			continue
		}
		// seen-mark BEFORE the fee screen: a fee-screened combo is enumerated-and-judged — the
		// next cycle's walk must move past it, not re-judge it forever.
		s.plabSeen[id] = now
		pruneTimeMap(s.plabSeen, func(t time.Time) time.Time { return t }, plabSeenCap, plabDedupTTL)
		s.plabMu.Unlock()
		prod := 1.0
		for _, l := range legs {
			prod *= l.Price
		}
		feeSyn, legFees, feeOK := s.plabExactSyntheticFees(legs)
		if !feeOK {
			continue
		}
		evSyn, economicsOK := plabAllInReturn(ce.jp, prod, feeSyn)
		if !economicsOK {
			continue
		}
		feeMVE := -1.0 // no authenticated RFQ quote: modeled MVE cannot admit or claim money truth
		evMVE := -1000.0
		evBest := evSyn
		if feeMVE >= 0 && evMVE > evBest {
			evBest = evMVE
		}
		// R123: the EXACT fee-adjusted combo floor. The walk's log-edge screen was the provable
		// superset; this is the decision — below-floor combos are counted, never logged.
		// R128: this floor is the ONLY admission screen — the 8k cap + eviction are GONE
		// (operator: "every possible ACCEPTABLE combo"); accepted candidates batch-insert into
		// the unlimited plab_open table below.
		if evBest < comboFloor {
			est.FeeScreened++
			continue
		}
		legality, corrCl := s.plabLegality(ctx, legs, evSyn)
		ent := plabOpenEnt{ID: id, At: now, Legs: legs, Bucket: ce.bucket, Class: ce.class,
			Prod: prod, JointP: ce.jp, Rho: ce.rho, RhoN: ce.rhoN,
			FeeSyn: feeSyn, FeeMVE: feeMVE, EVSyn: evSyn, EVMVE: evMVE, LegFees: legFees,
			Legality: legality, CorrCluster: corrCl, Cohort: storage.ComboLabCohortAllEligible,
			RouteState: "synthetic-settlement-only"}
		batch = append(batch, plabToRow(ent))
		row := map[string]any{"kind": "cand", "at": now.Format(time.RFC3339), "id": id,
			"bucket": ce.bucket, "class": ce.class, "legality": legality, "corr_cluster": corrCl, "legs": legs, "venue_prod": math.Round(prod*1e4) / 1e4,
			"joint_p": math.Round(ce.jp*1e4) / 1e4, "rho": math.Round(ce.rho*1e3) / 1e3, "rho_n": ce.rhoN,
			"fee_synth": math.Round(feeSyn*1e4) / 1e4, "fee_mve": feeMVE,
			"economics_version": "all-in-v2", "ev_synth_per_all_in_$1": math.Round(evSyn*1e4) / 1e4,
			"n_legs": len(legs)}
		if ce.proven {
			row["class_proven"] = true // R123 Part 3: proven-positive class tag (log-only ranking input)
		}
		if feeMVE >= 0 {
			route := "mve"
			if feeSyn < feeMVE {
				route = "synth"
			}
			row["cheaper_route"] = route
		}
		s.plabAppend(row)
		logged++
	}
	// R128: one batched transaction admits the whole cycle (INSERT OR IGNORE — beyond-TTL dedup
	// re-walks are idempotent; `logged` counts rows actually NEW in the ledger).
	if len(batch) > 0 {
		ictx, icancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		nIns, ierr := s.store.PlabInsertBatch(ictx, batch)
		icancel()
		if ierr != nil {
			actx, acancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			_ = s.store.Audit(actx, "error", "parlaylab", "R128 open-ledger batch insert failed: "+ierr.Error(), "")
			acancel()
		}
		logged = int(nIns)
		s.plabMu.Lock()
		s.plabOpenN += logged // keep the cached count honest between refreshes
		s.plabMu.Unlock()
	} else {
		logged = 0
	}
	est.Logged = logged
	est.At = now.Format(time.RFC3339)
	est.MS = float64(time.Since(now).Milliseconds())
	s.plabMu.Lock()
	s.plabEnum = est
	auditDue := time.Since(s.plabEnumAuditAt) > 30*time.Minute && est.PoolN >= 2
	if auditDue {
		s.plabEnumAuditAt = time.Now()
	}
	if logged > 0 {
		s.plabCand += logged
		s.plabHourLogged.add(time.Now(), logged) // R124: briefing scoreboard's last-hour volume
	}
	s.plabMu.Unlock()
	if auditDue { // the honest combinatorics, on the record every ~30 min
		actx, acancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		_ = s.store.Audit(actx, "info", "parlaylab", fmt.Sprintf(
			"R123 exhaustive enumeration: pool %d → raw space %.3g combos (≤%d legs) · walked Σlog-edge ≥ %.4f (floor %.4f, corr slack %.4f, complete=%v) · %d enumerated · %d fee-screened · %d twin-excluded · %d logged · %d nodes · %.0fms",
			est.PoolN, est.RawSpace, est.MaxLegs, est.TEffective, est.FloorLogEdge, est.CorrSlack, est.Complete,
			est.Enumerated, est.FeeScreened, est.TwinSkipped, logged, est.Nodes, est.MS), "")
		acancel()
	}
	*/
}

func plabPositiveSystemKey(l plabLeg) string {
	systems := append([]string(nil), l.PositiveSystems...)
	for i := range systems {
		systems[i] = strings.TrimSpace(systems[i])
	}
	sort.Strings(systems)
	return strings.Join(systems, ",")
}

// plabPoolKey keeps one instrument's generic model, each exact positive route, and each sealed
// proof as separate candidate records. Combo construction still rejects duplicate instruments;
// separation only prevents one source from donating rosier economics to another source's tags.
func plabPoolKey(l plabLeg) string {
	base := strings.ToLower(strings.TrimSpace(l.Platform)) + "|" + strings.ToUpper(strings.TrimSpace(l.Ticker)) + "|" + strings.ToUpper(strings.TrimSpace(l.Side))
	if l.RollingPositive {
		return base + "|rolling|" + strings.ToLower(strings.TrimSpace(l.SystemRoute)) + "|" +
			strings.ToLower(strings.TrimSpace(l.Src)) + "|" + plabPositiveSystemKey(l)
	}
	if l.Promoted {
		return base + "|sealed|" + strings.TrimSpace(l.SystemID) + "|" + strconv.FormatInt(l.ProofRunID, 10)
	}
	return base + "|generic"
}

func mergePlabLeg(a, b plabLeg) plabLeg {
	// A generic model row can never donate PWin/EV to an exact positive-system route merely because
	// ticker+side match. The normal caller uses plabPoolKey, and this guard makes the invariant hold
	// even for direct/future callers.
	if a.RollingPositive != b.RollingPositive {
		if b.RollingPositive {
			return b
		}
		return a
	}
	if a.RollingPositive && (plabPositiveSystemKey(a) != plabPositiveSystemKey(b) ||
		!strings.EqualFold(a.SystemRoute, b.SystemRoute) || !strings.EqualFold(a.Src, b.Src)) {
		return a // distinct exact routes belong in distinct map keys, never in one blended row
	}
	out := a
	if b.EVNet > a.EVNet {
		out.Price, out.PWin, out.EVNet, out.EventKey, out.Src = b.Price, b.PWin, b.EVNet, b.EventKey, b.Src
	}
	out.RollingPositive = a.RollingPositive
	systems := map[string]bool{}
	for _, list := range [][]string{a.PositiveSystems, b.PositiveSystems} {
		for _, system := range list {
			if system = strings.TrimSpace(system); system != "" {
				systems[system] = true
			}
		}
	}
	out.PositiveSystems = out.PositiveSystems[:0]
	for system := range systems {
		out.PositiveSystems = append(out.PositiveSystems, system)
	}
	sort.Strings(out.PositiveSystems)
	if out.RollingPositive && b.SystemRoute != "" {
		out.SystemRoute = b.SystemRoute
	} else if out.RollingPositive && out.SystemRoute == "" {
		out.SystemRoute = a.SystemRoute
	}
	// Sealed metadata is orthogonal to the rolling Paper flag and always survives economic merging.
	sealed := a
	if b.Promoted {
		sealed = b
	}
	if sealed.Promoted {
		out.Promoted, out.SystemID = true, sealed.SystemID
		out.ProofRunID, out.ProofResultHash = sealed.ProofRunID, sealed.ProofResultHash
		out.ProofMeanAllIn, out.ProofLowerPC = sealed.ProofMeanAllIn, sealed.ProofLowerPC
		out.ProofCapacity, out.ProofRoute, out.ProofObserved = sealed.ProofCapacity, sealed.ProofRoute, sealed.ProofObserved
	}
	return out
}

// plabProbabilityFromEdge refuses a unit mismatch instead of hiding it behind a probability
// clamp. A fee-net per-contract edge added to an all-in price must remain a real probability.
// This defense stopped corrupted cents-vs-dollars rows from becoming .9999 PWin combo legs.
func plabProbabilityFromEdge(allIn, edge float64) (float64, bool) {
	if allIn <= 0 || allIn >= 1 || edge <= 0 || math.IsNaN(allIn) || math.IsNaN(edge) ||
		math.IsInf(allIn, 0) || math.IsInf(edge, 0) {
		return 0, false
	}
	p := allIn + edge
	return p, p > allIn && p < 1
}

// plabSignalLegSets reads verdicts once and returns (a) the historical strict PROVEN+ pool and
// (b) every currently positive named signal-system regardless of n for the PAPER-only rolling
// cohort. Neither path mechanically complements a losing system.
func (s *Server) plabSignalLegSets(ctx context.Context, floor float64) (strict, rolling []plabLeg) {
	vs := s.computeExperimentVerdicts(ctx)
	type pick struct {
		fam    string
		edge   float64
		strict bool
	}
	var picks []pick
	for _, v := range vs {
		if v.Locked || v.Group != "signal" || v.Unit != "$/contract" {
			continue
		}
		if v.Mean > 0 {
			picks = append(picks, pick{fam: v.Family, edge: v.Mean,
				strict: v.State == "PROVEN+" && v.Mean >= floor})
		}
	}
	sort.Slice(picks, func(i, j int) bool { return picks[i].edge > picks[j].edge })
	since := time.Now().UTC().Add(-20 * time.Minute).Format(time.RFC3339)
	for _, p := range picks {
		rows, err := s.store.RecentOpenSignals(ctx, p.fam, since, 0)
		if err != nil {
			continue
		}
		for _, r := range rows {
			side := strings.ToUpper(strings.TrimSpace(r.Side))
			if side != "YES" && side != "NO" { // outcome-label sides can't be flipped or settled safely
				continue
			}
			px := r.EntryPrice
			if px <= 0.03 || px >= 0.97 {
				continue
			}
			pwin, probabilityOK := plabProbabilityFromEdge(px, p.edge)
			if !probabilityOK {
				continue
			}
			leg := plabLeg{Ticker: r.Ticker, Side: side, Platform: r.Platform,
				Price: px, PWin: pwin, EVNet: p.edge,
				EventKey: s.legEventKey(r.Platform, r.Ticker, ""), Src: "rolling-positive-system",
				RollingPositive: true, PositiveSystems: []string{p.fam}, SystemRoute: "taker"}
			rolling = append(rolling, leg)
			if p.strict {
				strictLeg := leg
				strictLeg.Src = "edge"
				strict = append(strict, strictLeg)
			}
		}
	}
	return strict, rolling
}

// plabEdgeLegs is the compatibility wrapper for the strict historical cohort.
func (s *Server) plabEdgeLegs(ctx context.Context, floor float64) []plabLeg {
	strict, _ := s.plabSignalLegSets(ctx, floor)
	return strict
}

func (s *Server) plabRollingPositiveResearchLegs(ctx context.Context) []plabLeg {
	rows, err := s.store.RecentPositiveResearchComboCandidates(ctx,
		time.Now().UTC().Add(-plabPositiveResearchLookback), 500)
	if err != nil {
		return nil
	}
	out := make([]plabLeg, 0, len(rows))
	for _, c := range rows {
		side := strings.ToUpper(strings.TrimSpace(c.Side))
		if (side != "YES" && side != "NO") || c.ExpectedNetLower <= 0 || c.ObservedPrice <= 0.03 ||
			c.ObservedPrice >= 0.97 || c.ObservedFeePC < 0 || c.VisibleCapacity < 1 || c.SystemID == "" {
			continue
		}
		allIn := c.ObservedPrice + c.ObservedFeePC
		pwin, probabilityOK := plabProbabilityFromEdge(allIn, c.ExpectedNetLower)
		if !probabilityOK {
			continue
		}
		eventKey := c.CanonicalEventID
		if eventKey == "" {
			eventKey = s.legEventKey(c.Venue, c.Ticker, c.Title)
		}
		out = append(out, plabLeg{Ticker: c.Ticker, Side: side, Platform: c.Venue,
			Price: c.ObservedPrice, PWin: pwin,
			EVNet: c.ExpectedNetLower, EventKey: eventKey, Src: "rolling-positive-research-system",
			RollingPositive: true, PositiveSystems: []string{c.SystemID}, SystemRoute: "taker"})
	}
	return out
}

// plabRollingPositiveUnitTrialLegs closes the breadth gap between the economic Systems
// leaderboard and Combo Lab. Every exact positive family+venue+origin+side route with a current
// exact-money observation may supply a Paper research leg; n is not an admission threshold.
func (s *Server) plabRollingPositiveUnitTrialLegs(ctx context.Context) []plabLeg {
	rows, err := s.store.RecentPositiveUnitTrialLegs(ctx, time.Now().UTC().Add(-20*time.Minute), 1000)
	if err != nil {
		return nil
	}
	out := make([]plabLeg, 0, len(rows))
	for _, row := range rows {
		platform := strings.ToLower(strings.TrimSpace(row.Platform))
		side := strings.ToUpper(strings.TrimSpace(row.Side))
		allIn := row.Ask + row.FeePC
		if (platform != "kalshi" && platform != "polyus") || (side != "YES" && side != "NO") ||
			row.MeanPC <= 0 || row.Depth < 1 || allIn <= 0.02 || allIn >= 0.98 {
			continue
		}
		pwin, probabilityOK := plabProbabilityFromEdge(allIn, row.MeanPC)
		if !probabilityOK {
			continue
		}
		systemID := takerSystemID(row.Family, platform, side)
		if strings.EqualFold(row.OriginLayer, "strategy") {
			systemID = "strategy:" + strings.TrimSpace(row.Family) + "@" + platform + " [" + side + "]"
		}
		out = append(out, plabLeg{Ticker: row.Ticker, Side: side, Platform: platform,
			Price: row.Ask, PWin: pwin, EVNet: row.MeanPC,
			EventKey:        s.legEventKey(platform, row.Ticker, row.Category),
			Src:             "exact-unit-trial/" + strings.ToLower(strings.TrimSpace(row.OriginLayer)),
			RollingPositive: true, PositiveSystems: []string{systemID}, SystemRoute: "taker",
			Depth: row.Depth, QuoteTS: row.OpenedTS.UTC().Format(time.RFC3339Nano), QuoteSrc: row.QuoteSource})
	}
	return out
}

// plabPromotedLegs returns only exact current Kalshi taker candidates backed by the complete
// sealed research-promotion contract. Maker proofs are deliberately excluded: an ask-priced
// Combo Lab leg cannot inherit maker-route economics. The combined RFQ remains unproved and is
// recorded as awaiting-RFQ later; this function does not create venue state.
func (s *Server) plabPromotedLegs(ctx context.Context) []plabLeg {
	proofs, err := s.store.CurrentResearchPromotionProofs(ctx)
	if err != nil {
		return nil
	}
	now := time.Now().UTC()
	best := map[string]plabLeg{}
	for _, proof := range proofs {
		if !strings.EqualFold(proof.Venue, "kalshi") || proof.Route != "taker" ||
			proof.RunID <= 0 || proof.ResultHash == "" || proof.LowerPC <= 0 || proof.Governance.Capacity < 1 {
			continue
		}
		cand, found, readErr := s.store.LatestResearchPromotionCandidate(ctx, proof, now.Add(-20*time.Second))
		if readErr != nil || !found || !strings.EqualFold(cand.Venue, "kalshi") || cand.VisibleCapacity < 1 ||
			cand.ObservedPrice <= 0 || cand.ObservedPrice >= 1 || cand.ObservedFeePC < 0 ||
			(now.Sub(cand.Observed) > 20*time.Second) {
			continue
		}
		side := strings.ToUpper(strings.TrimSpace(cand.Side))
		if side != "YES" && side != "NO" {
			continue
		}
		allIn := cand.ObservedPrice + cand.ObservedFeePC
		lowerNow := proof.LowerPC - math.Max(0, allIn-proof.MeanAllIn)
		pwin, probabilityOK := plabProbabilityFromEdge(allIn, lowerNow)
		if !probabilityOK {
			continue
		}
		key := strings.ToUpper(cand.Ticker) + "|" + side
		leg := plabLeg{Ticker: cand.Ticker, Side: side, Platform: "kalshi",
			Price: cand.ObservedPrice, PWin: pwin, EVNet: lowerNow,
			EventKey: s.legEventKey("kalshi", cand.Ticker, cand.Title), Src: "sealed-promoted-system",
			Promoted: true, SystemID: proof.SystemID, ProofRunID: proof.RunID,
			ProofResultHash: strings.TrimPrefix(proof.ResultHash, "sha256:"), ProofMeanAllIn: proof.MeanAllIn,
			ProofLowerPC:  proof.LowerPC,
			ProofCapacity: math.Min(cand.VisibleCapacity, proof.Governance.Capacity), ProofRoute: proof.Route,
			ProofObserved: cand.Observed.Format(time.RFC3339Nano)}
		if old, ok := best[key]; !ok || leg.EVNet > old.EVNet {
			best[key] = leg
		}
	}
	out := make([]plabLeg, 0, len(best))
	for _, leg := range best {
		out = append(out, leg)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].EVNet != out[j].EVNet {
			return out[i].EVNet > out[j].EVNet
		}
		return plabLegStableKey(out[i]) < plabLegStableKey(out[j])
	})
	return out
}

func (s *Server) plabPruneCurrentHorizon(ctx context.Context, now time.Time, limit int) (int, map[string]int, error) {
	rows, err := s.store.PlabPendingHorizonRows(ctx, limit)
	if err != nil {
		return 0, nil, err
	}
	byReason := map[string][]string{}
	for _, row := range rows {
		var legs []plabLeg
		if json.Unmarshal([]byte(row.Legs), &legs) != nil || len(legs) < 2 {
			byReason["invalid_legacy_row"] = append(byReason["invalid_legacy_row"], row.ID)
		}
		// A valid prospective row is immutable once admitted. Re-evaluating its horizon against
		// current wall time (or a later postponed close) cannot authorize deletion: that destroys
		// exactly the late outcome the experiment was created to measure. New rows fail closed at
		// admission; legacy, ended, unknown and postponed rows remain here until venue settlement.
	}
	counts := map[string]int{}
	total := 0
	for reason, ids := range byReason {
		n, deleteErr := s.store.PlabDeletePendingBatch(ctx, ids)
		if deleteErr != nil {
			return total, counts, deleteErr
		}
		if n > 0 {
			counts[reason] = int(n)
			total += int(n)
		}
	}
	return total, counts, nil
}

type plabSettlementState uint8

const (
	plabSettlementResolved plabSettlementState = iota + 1
	plabSettlementNotTerminal
	plabSettlementTransportFailure
	plabSettlementUnsupported
)

type plabSettlementResult struct {
	State plabSettlementState
	Yes   float64
}

type plabSettlementFetcher func(context.Context, storage.PlabLegKey) plabSettlementResult

// plabFetchSettlement makes one real venue attempt and preserves the distinction between a
// healthy market that simply is not terminal yet and a failed transport. This matters for both
// observability and retry cadence; neither condition is a settlement value.
func (s *Server) plabFetchSettlement(ctx context.Context, tk storage.PlabLegKey) plabSettlementResult {
	platform := strings.ToLower(strings.TrimSpace(tk.Platform))
	switch platform {
	case "kalshi", "":
		if s.kal == nil {
			return plabSettlementResult{State: plabSettlementTransportFailure}
		}
		m, err := s.kal.GetMarket(ctx, tk.Ticker)
		if err != nil {
			return plabSettlementResult{State: plabSettlementTransportFailure}
		}
		if yv := m.SettledYes(); yv >= 0 && yv <= 1 {
			_ = s.store.ResolveSignalsForVenue(ctx, "kalshi", tk.Ticker, yv,
				"kalshi combo-paper terminal poll")
			return plabSettlementResult{State: plabSettlementResolved, Yes: yv}
		}
		return plabSettlementResult{State: plabSettlementNotTerminal}

	case "polyus":
		if s.polyUS == nil {
			return plabSettlementResult{State: plabSettlementTransportFailure}
		}
		b, _, code, err := s.polyUS.BookFull(ctx, tk.Ticker)
		if err != nil || code != http.StatusOK || b == nil {
			return plabSettlementResult{State: plabSettlementTransportFailure}
		}
		if yesValue, terminal := b.SettledYes(); terminal {
			// Do not coerce partial/scalar settlements to binary. The side adjustment and economic
			// payout are applied later from this exact authoritative YES value.
			_ = s.store.ResolveSignalsForVenue(ctx, "polyus", tk.Ticker, yesValue,
				polyUSFinalSettlementSource(b))
			return plabSettlementResult{State: plabSettlementResolved, Yes: yesValue}
		}
		return plabSettlementResult{State: plabSettlementNotTerminal}

	case "polymarket", "polyint", "poly-int":
		if s.poly == nil {
			return plabSettlementResult{State: plabSettlementTransportFailure}
		}
		conditionID := tk.Ticker
		for _, m := range s.pintCatalogMarkets() {
			if m.Slug == tk.Ticker && m.ConditionID != "" {
				conditionID = m.ConditionID
				break
			}
		}
		m, found, err := s.poly.MarketByCLOBStrict(ctx, conditionID)
		if err != nil {
			return plabSettlementResult{State: plabSettlementTransportFailure}
		}
		if !found {
			return plabSettlementResult{State: plabSettlementUnsupported}
		}
		winner, terminal := m.Resolved()
		if !terminal {
			return plabSettlementResult{State: plabSettlementNotTerminal}
		}
		outs := m.Outcomes()
		if len(outs) < 2 {
			return plabSettlementResult{State: plabSettlementUnsupported}
		}
		yv := 0.0
		if strings.EqualFold(winner, outs[0]) {
			yv = 1
		}
		_ = s.store.ResolveSignalsForVenue(ctx, "polymarket", tk.Ticker, yv,
			"polymarket CLOB combo-paper terminal poll")
		return plabSettlementResult{State: plabSettlementResolved, Yes: yv}
	}
	return plabSettlementResult{State: plabSettlementUnsupported}
}

type plabSettlementCursor struct {
	Platform string `json:"platform"`
	Ticker   string `json:"ticker"`
}

func (s *Server) plabLoadSettlementCursor(ctx context.Context) storage.PlabLegKey {
	raw, ok := s.store.KVGet(ctx, plabKVCursor)
	if !ok {
		return storage.PlabLegKey{}
	}
	var c plabSettlementCursor
	if json.Unmarshal([]byte(raw), &c) != nil {
		return storage.PlabLegKey{}
	}
	return storage.PlabLegKey{Platform: c.Platform, Ticker: c.Ticker}
}

func (s *Server) plabStoreSettlementCursor(ctx context.Context, tk storage.PlabLegKey) {
	b, err := json.Marshal(plabSettlementCursor{Platform: tk.Platform, Ticker: tk.Ticker})
	if err != nil {
		return
	}
	dctx, cancel := researchDurabilityContext(ctx)
	_ = s.store.KVSet(dctx, plabKVCursor, string(b))
	cancel()
}

func plabSettlementPassedHorizon(tk storage.PlabLegKey, now time.Time) bool {
	if tk.FirstAt <= 0 {
		return false
	}
	limit := plabRegularHardMax
	if isCryptoTicker(tk.Ticker) {
		limit = plabCryptoHardMax
	}
	return !time.Unix(tk.FirstAt, 0).Add(limit).After(now)
}

// parlayLabGradePass settles candidates whose legs all resolved and folds the outcomes into the
// scoreboard + correlation counters (persisted to kv).
//
// R128 REWRITE (the "graded 50 of 12k/hr" root cause): the old pass scanned EVERY open combo and
// resolved legs per-COMBO under the fetch budget — oldest unresolvable combos starved the rest.
// Now the sweep is TICKER-centric over the unlimited SQLite ledger:
//  0. preserve all valid unresolved truth (only structurally malformed rows can be removed);
//  1. resolve each DISTINCT unresolved ticker ONCE (store fast path free; venue fetches budgeted
//     per ticker with attempt-only cooldowns) and fan the exact value to every combo holding it;
//  2. grade every combo whose unresolved count hit 0 — O(new settlements), bounded per pass.
func (s *Server) parlayLabGradePass(ctx context.Context) {
	s.parlayLabGradePassWithFetcher(ctx, s.plabFetchSettlement)
}

func (s *Server) parlayLabGradePassWithFetcher(ctx context.Context, fetch plabSettlementFetcher) {
	graded, malformedPruned := 0, 0
	horizonReasons := map[string]int{}
	if s.plabFetchBad == nil { // grade pass runs only on the MonitorAuto goroutine — lock-free by design
		s.plabFetchBad = map[string]time.Time{}
	}
	if s.plabChkAt == nil {
		s.plabChkAt = map[string]time.Time{}
	}
	now := time.Now()
	cursor := s.plabLoadSettlementCursor(ctx)
	tks, scanErr := s.store.PlabUnresolvedTickersAfter(ctx, cursor, plabScanPage)
	if scanErr == nil && len(tks) > 0 {
		defer s.plabStoreSettlementCursor(ctx, tks[len(tks)-1])
	}
	work := make([]storage.PlabLegKey, 0, len(tks))
	for _, tk := range tks {
		if ctx.Err() != nil {
			break
		}
		yv, ok := s.store.ResolvedYesForVenue(ctx, tk.Platform, tk.Ticker)
		if ok {
			_, _ = s.store.PlabResolveTicker(ctx, tk.Platform, tk.Ticker, yv)
			continue
		}
		work = append(work, tk)
	}
	sort.SliceStable(work, func(i, j int) bool {
		pi, pj := plabSettlementPassedHorizon(work[i], now), plabSettlementPassedHorizon(work[j], now)
		if pi != pj {
			return pi
		}
		if work[i].FirstAt != work[j].FirstAt {
			return work[i].FirstAt < work[j].FirstAt
		}
		return work[i].Platform+"|"+work[i].Ticker < work[j].Platform+"|"+work[j].Ticker
	})
	budget := plabFetchBudg
	for _, tk := range work {
		if budget <= 0 || ctx.Err() != nil {
			break
		}
		ckey := tk.Platform + "|" + tk.Ticker
		if t, checked := s.plabChkAt[ckey]; checked && now.Sub(t) < plabChkTTL {
			continue
		}
		if t, failed := s.plabFetchBad[ckey]; failed && now.Sub(t) < plabFailTTL {
			continue
		}
		budget--
		result := fetch(ctx, tk)
		if ctx.Err() != nil {
			break
		}
		attemptedAt := time.Now()
		switch result.State {
		case plabSettlementResolved:
			if result.Yes >= 0 && result.Yes <= 1 {
				_, _ = s.store.PlabResolveTicker(ctx, tk.Platform, tk.Ticker, result.Yes)
			}
			delete(s.plabChkAt, ckey)
			delete(s.plabFetchBad, ckey)
		case plabSettlementNotTerminal:
			s.plabChkAt[ckey] = attemptedAt
			delete(s.plabFetchBad, ckey)
		case plabSettlementTransportFailure:
			s.plabFetchBad[ckey] = attemptedAt
			delete(s.plabChkAt, ckey)
		case plabSettlementUnsupported:
			s.plabChkAt[ckey] = attemptedAt
			delete(s.plabFetchBad, ckey)
		}
	}
	pruneTimeMap(s.plabChkAt, func(t time.Time) time.Time { return t }, 40000, time.Hour)
	pruneTimeMap(s.plabFetchBad, func(t time.Time) time.Time { return t }, 40000, 2*time.Hour)
	// 2) grade fully-settled combos ≥10 min old (bounded per pass; the backlog drains next tick).
	rows, gerr := s.store.PlabGradable(ctx, time.Now().Add(-10*time.Minute).UTC().Unix(), plabGradeMax)
	var doneIDs []string
	invalidDone := 0
	deletedGraded := 0
	if gerr == nil {
		ids := make([]string, len(rows))
		for i, r := range rows {
			ids[i] = r.ID
		}
		legVals, _ := s.store.PlabLegVals(ctx, ids)
		for _, r := range rows {
			var legs []plabLeg
			if json.Unmarshal([]byte(r.Legs), &legs) != nil || len(legs) == 0 {
				invalidDone++
				doneIDs = append(doneIDs, r.ID) // unparseable row — drop it, never wedge the sweep
				continue
			}
			var legFees []float64
			if json.Unmarshal([]byte(r.LegFees), &legFees) != nil || len(legFees) != len(legs) {
				// Do not rewrite a historical experiment with today's fee schedule.
				continue
			}
			vals := legVals[r.ID]
			payouts := make([]float64, len(legs))
			all := true
			for i, l := range legs {
				sv, ok := vals[l.Platform+"|"+l.Ticker]
				if !ok || math.IsNaN(sv) || math.IsInf(sv, 0) || sv < 0 || sv > 1 {
					all = false
					break
				}
				payout := sv
				if strings.EqualFold(l.Side, "NO") {
					payout = 1 - sv
				}
				payouts[i] = clampF(payout, 0, 1)
			}
			if !all { // defensive: unresolved=0 disagreed with the leg rows — retry next pass
				continue
			}
			e := plabOpenEnt{ID: r.ID, At: time.Unix(r.At, 0).UTC(), Legs: legs, Bucket: r.Bucket,
				Class: r.Class, Prod: r.Prod, JointP: r.JointP, FeeMVE: r.FeeMVE, EVSyn: r.EVSyn,
				LegFees: legFees, Legality: r.Legality, CorrCluster: r.CorrCluster,
				Cohort: r.Cohort, RouteState: r.RouteState, CanonicalSystemID: r.CanonicalSystemID,
				ComboVenue: r.ComboVenue, RelationClass: r.RelationClass, ProducerFamily: r.ProducerFamily,
				ComboRoute: r.ComboRoute, ExperimentEpoch: r.ExperimentEpoch, ComboKey: r.ComboKey}
			inserted, gradeErr := s.plabGradeOne(ctx, e, payouts)
			switch {
			case gradeErr == nil:
				doneIDs = append(doneIDs, r.ID) // new receipt or idempotent retry
				if inserted {
					graded++
				}
			case errors.Is(gradeErr, errPlabInvalidGrade):
				doneIDs = append(doneIDs, r.ID)
				invalidDone++
			default:
				// Receipt/storage failures retry; deletion here would lose gradeable truth.
			}
		}
		if len(doneIDs) > 0 {
			if err := s.store.PlabDeleteBatch(ctx, doneIDs); err == nil {
				deletedGraded = len(doneIDs)
			} else {
				// Durable receipt primary keys prevent economic double counts on retry.
				invalidDone = 0
			}
		}
	}
	if graded > 0 {
		s.plabMu.Lock()
		if err := s.plabRebuildV2StatsLocked(ctx); err != nil {
			s.plabStats, s.plabCorr, s.plabGraded = map[string]*plabAgg{}, map[string]*plabCorrC{}, 0
		}
		s.plabMu.Unlock()
	}
	// Only structurally malformed rows may be removed, and only AFTER resolution/economic grading.
	// Valid legacy, postponed, ended and unknown rows remain eligible for authoritative settlement.
	if n, reasons, err := s.plabPruneCurrentHorizon(ctx, time.Now().UTC(), 5000); err == nil && n > 0 {
		malformedPruned, horizonReasons = n, reasons
	}
	reportedMalformed := malformedPruned
	if invalidDone > 0 && deletedGraded > 0 {
		reportedMalformed += invalidDone
		horizonReasons["terminal_invalid_economics"] += invalidDone
	}
	if malformedPruned > 0 || deletedGraded > 0 {
		s.plabMu.Lock()
		s.plabOpenN -= malformedPruned + deletedGraded
		if s.plabOpenN < 0 {
			s.plabOpenN = 0
		}
		s.plabMu.Unlock()
	}
	s.plabPersist(ctx, graded, reportedMalformed, horizonReasons)
}

// plabGradeOne inserts one immutable economic receipt before touching any aggregate. false,nil
// means this combo was already graded (normally a prior delete failed) and must not increment n.
func (s *Server) plabGradeOne(ctx context.Context, e plabOpenEnt, payouts []float64) (bool, error) {
	if e.Cohort == "" {
		e.Cohort = storage.ComboLabCohortAllEligible
	}
	if e.RouteState == "" {
		e.RouteState = "synthetic-settlement-only"
	}
	if e.CanonicalSystemID == "" {
		e.CanonicalSystemID, e.ComboVenue, e.RelationClass, e.ProducerFamily,
			e.ComboRoute, e.ComboKey = plabComboAttribution(e.Legs, e.Bucket, e.Cohort, e.RouteState, e.ExperimentEpoch)
	}
	if e.ExperimentEpoch == "" {
		e.ExperimentEpoch = "r147-4h2h-v1"
	}
	if err := plabValidateLegSet(e.Legs); err != nil {
		return false, err
	}
	if len(payouts) != len(e.Legs) || len(e.LegFees) != len(e.Legs) {
		return false, fmt.Errorf("%w: payout/fee count mismatch", errPlabInvalidGrade)
	}
	// Economic grading uses the same funded-entry contract as admission/placement: every rolled-leg
	// fee was reserved at entry, so every fee is charged even when an early leg later loses.
	jointPayout, feesPaid := 1.0, 0.0
	binaryWins := make([]bool, len(payouts))
	won := true // reporting/correlation indicator only; never used to calculate economic payout
	for i, payout := range payouts {
		if math.IsNaN(payout) || math.IsInf(payout, 0) || payout < 0 || payout > 1 ||
			math.IsNaN(e.LegFees[i]) || math.IsInf(e.LegFees[i], 0) || e.LegFees[i] < 0 {
			return false, fmt.Errorf("%w: non-finite/out-of-range payout or fee", errPlabInvalidGrade)
		}
		feesPaid += e.LegFees[i]
		jointPayout *= payout
		binaryWins[i] = payout >= 0.5
		won = won && binaryWins[i]
	}
	realSyn, ok := plabAllInReturn(jointPayout, e.Prod, feesPaid)
	if !ok {
		return false, fmt.Errorf("%w: all-in realized economics", errPlabInvalidGrade)
	}
	predSyn, ok := plabAllInReturn(e.JointP, e.Prod, feesPaid)
	if !ok {
		return false, fmt.Errorf("%w: all-in predicted economics", errPlabInvalidGrade)
	}
	realMVE := 0.0
	hasMVE := e.FeeMVE >= 0
	if hasMVE {
		if realMVE, ok = plabAllInReturn(jointPayout, e.Prod, e.FeeMVE); !ok {
			return false, fmt.Errorf("%w: all-in MVE economics", errPlabInvalidGrade)
		}
	}
	// R112: a candidate logged "unprobed" may have a venue answer by settlement time —
	// refresh from the probe cache so the scoreboard folds it under the real verdict.
	if e.Legality == "unprobed" {
		if v, ok := s.comboVerdict(e.Legs); ok {
			e.Legality = v.Verdict
		}
	}
	// R110: legality joins the scoreboard key so venue-legal vs synthetic-only vs illegal
	// stats split cleanly (plabSummary's SplitN(…,3) rollup folds it into the class tail;
	// pre-R110 kv cells simply lack the suffix and keep aggregating separately).
	key := fmt.Sprintf("%s|%dleg|%s|%s|%s", e.Bucket, len(e.Legs), e.Class, e.Legality, e.Cohort)
	keys := plabIndependenceKeys(e.Legs)
	marketKeys := plabMarketKeys(e.Legs)
	legsJSON, _ := json.Marshal(e.Legs)
	payoutsJSON, _ := json.Marshal(payouts)
	inserted, err := s.store.InsertPlabGradeReceipt(ctx, storage.PlabGradeReceipt{
		ComboID: e.ID, CandidateAt: e.At.UTC().Unix(), GradedTS: time.Now().UTC().Format(time.RFC3339Nano),
		Cell: key, Bucket: e.Bucket, Class: e.Class, Legality: e.Legality, Cohort: e.Cohort,
		RouteState: e.RouteState, NLegs: int64(len(e.Legs)), Prod: e.Prod, JointP: e.JointP,
		Fees: feesPaid, EntryCapital: 1 + feesPaid, JointPayout: jointPayout,
		RealizedReturn: realSyn, PredictedReturn: predSyn, MVEValid: hasMVE,
		RealizedMVEReturn: realMVE, Won: won, IndependenceKeys: keys, MarketKeys: marketKeys,
		LegsJSON: string(legsJSON), PayoutsJSON: string(payoutsJSON),
		CanonicalSystemID: e.CanonicalSystemID, ComboVenue: e.ComboVenue,
		RelationClass: e.RelationClass, ProducerFamily: e.ProducerFamily,
		ComboRoute: e.ComboRoute, ExperimentEpoch: e.ExperimentEpoch, ComboKey: e.ComboKey,
	})
	if err != nil || !inserted {
		return inserted, err
	}
	s.plabMu.Lock()
	ag := s.plabStats[key]
	if ag == nil {
		ag = &plabAgg{}
		s.plabStats[key] = ag
	}
	ag.N++
	if won {
		ag.Wins++
	}
	ag.SumReal += realSyn
	ag.SumPred += predSyn
	ag.SumJP += e.JointP
	// R123 Part 3: post-R123 accumulators with squares — the scoreboard CI inputs.
	ag.N2++
	ag.Sum2 += realSyn
	ag.Sq2 += realSyn * realSyn
	ag.IndependenceGroups = append(ag.IndependenceGroups, keys)
	ag.MarketGroups = append(ag.MarketGroups, marketKeys)
	ag.Outcomes = append(ag.Outcomes, realSyn)
	ag.IndependentN = plabIndependentBlocks(ag.IndependenceGroups)
	ag.UniqueMarkets = plabUniqueMarkets(ag.MarketGroups)
	if hasMVE {
		ag.NMVE++
		ag.SumMVE += realMVE
	}
	// R123: correlation counters accrue per PAIR CLASS per overlap group — the estimator
	// that actually prices 3+-leg combos (plabJointPEx reads pair-class cells). For 2-leg
	// overlaps the pair class == the combo class, so the pre-R123 cells keep growing
	// unchanged; 3+-leg combos stop feeding their own combo-class corr cells (documented —
	// their scoreboard stats cells continue). The R122 genre-pair cells for 2-leg
	// INDEPENDENTS keep accruing as measurement (rho still never APPLIES to independents).
	_, _, _, ovg := plabComboShape(e.Legs)
	for _, g := range ovg {
		pairClass := plabClass([]plabLeg{e.Legs[g[0]], e.Legs[g[1]]}, e.Legs[g[0]].EventKey)
		cc := s.plabCorr[pairClass]
		if cc == nil {
			cc = &plabCorrC{}
			s.plabCorr[pairClass] = cc
		}
		cc.N++
		if binaryWins[g[0]] {
			cc.A++
		}
		if binaryWins[g[1]] {
			cc.B++
		}
		if binaryWins[g[0]] && binaryWins[g[1]] {
			cc.AB++
		}
	}
	if e.Bucket == "indep" && len(e.Legs) == 2 {
		cc := s.plabCorr[e.Class]
		if cc == nil {
			cc = &plabCorrC{}
			s.plabCorr[e.Class] = cc
		}
		cc.N++
		if binaryWins[0] {
			cc.A++
		}
		if binaryWins[1] {
			cc.B++
		}
		if binaryWins[0] && binaryWins[1] {
			cc.AB++
		}
	}
	s.plabGraded++
	s.plabHourGraded.add(time.Now(), 1) // R124: briefing scoreboard's last-hour volume
	settledRows, uniqueMarkets, independentBlocks := ag.N, ag.UniqueMarkets, ag.IndependentN
	sampleBucket, sampleEmoji := plabSampleBucket(uniqueMarkets)
	s.plabMu.Unlock()
	s.plabAppend(map[string]any{"kind": "grade", "at": time.Now().UTC().Format(time.RFC3339),
		"id": e.ID, "bucket": e.Bucket, "class": e.Class, "legality": e.Legality,
		"cohort": e.Cohort, "route_state": e.RouteState, "live_transferable": false,
		"corr_cluster": e.CorrCluster, "legs": len(e.Legs), "won": won,
		"leg_payouts": payouts, "joint_payout": math.Round(jointPayout*1e6) / 1e6,
		"joint_p": e.JointP, "venue_prod": e.Prod,
		"economics_version": "all-in-v2", "entry_capital_per_base_$1": math.Round((1+feesPaid)*1e6) / 1e6,
		"real_synth_per_all_in_$1":    math.Round(realSyn*1e4) / 1e4,
		"real_mve_per_all_in_$1":      math.Round(realMVE*1e4) / 1e4,
		"pred_ev_synth_per_all_in_$1": math.Round(predSyn*1e4) / 1e4,
		"real_synth_per_$1":           math.Round(realSyn*1e4) / 1e4, // compatibility alias; unit declared above
		"real_mve_per_$1":             math.Round(realMVE*1e4) / 1e4,
		"independence_keys":           keys, "market_keys": marketKeys, "settled_rows": settledRows,
		"unique_markets": uniqueMarkets, "unique_independent_markets": uniqueMarkets, // old field is a read-only alias
		"independent_resolution_blocks": independentBlocks,
		"sample_bucket":                 sampleBucket, "sample_bucket_emoji": sampleEmoji})
	return true, nil
}

// plabPersist writes aggregate blobs after economic grades or malformed-row cleanup.
func (s *Server) plabPersist(ctx context.Context, graded, malformedPruned int, horizonReasons map[string]int) {
	if graded > 0 || malformedPruned > 0 {
		s.plabMu.Lock()
		bs, _ := json.Marshal(s.plabStats)
		bc, _ := json.Marshal(s.plabCorr)
		// R123 Part 3: roll the 7d movers snapshot — when absent or aged out, freeze today's
		// per-cell (n, EV/$1) so the scoreboard can show Δ7d movers next week.
		var snapB []byte
		if s.plabSnapTS.IsZero() || time.Since(s.plabSnapTS) > 7*24*time.Hour {
			snap := map[string][2]float64{}
			for k, a := range s.plabStats {
				if a != nil && a.N > 0 {
					snap[k] = [2]float64{float64(a.N), a.SumReal / float64(a.N)}
				}
			}
			s.plabSnap = snap
			s.plabSnapTS = time.Now()
			snapB, _ = json.Marshal(map[string]any{"ts": s.plabSnapTS.Unix(), "cells": snap})
		}
		s.plabMu.Unlock()
		pctx, pcancel := context.WithTimeout(context.WithoutCancel(ctx), 8*time.Second)
		_ = s.store.KVSet(pctx, plabKVStats, string(bs))
		_ = s.store.KVSet(pctx, plabKVCorr, string(bc))
		if snapB != nil {
			_ = s.store.KVSet(pctx, plabKVSnap, string(snapB))
		}
		if malformedPruned > 0 {
			s.persistPlabHorizonCounts(pctx, nil, horizonReasons)
		}
		pcancel()
		if graded > 0 || malformedPruned > 0 {
			actx, acancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			_ = s.store.Audit(actx, "info", "parlaylab",
				fmt.Sprintf("R144 Combo Lab all-in-v2: +%d immutable grades · %d structurally malformed rows removed %v · valid unresolved and legacy rows preserved — synthetic research, no placed bets",
					graded, malformedPruned, horizonReasons), "")
			acancel()
		}
	}
}

// plabSummary renders the scoreboard rolled up by bucket × leg-count (EV per $1 leads — the
// prime directive: EV is the verdict, win rate is context).
func (s *Server) plabSummary() []map[string]any {
	s.plabMu.Lock()
	defer s.plabMu.Unlock()
	roll := map[string]*plabAgg{}
	for k, a := range s.plabStats {
		parts := strings.Split(k, "|")
		if len(parts) < 2 {
			continue
		}
		cohort := storage.ComboLabCohortAllEligible
		if len(parts) >= 5 && (parts[len(parts)-1] == storage.ComboLabCohortAllEligible ||
			parts[len(parts)-1] == storage.ComboLabCohortRollingPositive ||
			parts[len(parts)-1] == storage.ComboLabCohortPromotedSystem) {
			cohort = parts[len(parts)-1]
		}
		rk := cohort + "|" + parts[0] + "|" + parts[1]
		r := roll[rk]
		if r == nil {
			r = &plabAgg{}
			roll[rk] = r
		}
		r.N += a.N
		r.Wins += a.Wins
		r.SumReal += a.SumReal
		r.SumMVE += a.SumMVE
		r.NMVE += a.NMVE
		r.SumPred += a.SumPred
		r.SumJP += a.SumJP
		r.N2 += a.N2
		r.Sum2 += a.Sum2
		r.Sq2 += a.Sq2
		for _, group := range a.IndependenceGroups {
			r.IndependenceGroups = append(r.IndependenceGroups, append([]string(nil), group...))
		}
		for _, group := range a.MarketGroups {
			r.MarketGroups = append(r.MarketGroups, append([]string(nil), group...))
		}
		r.Outcomes = append(r.Outcomes, a.Outcomes...)
	}
	out := []map[string]any{}
	for k, r := range roll {
		if r.N == 0 {
			continue
		}
		parts := strings.SplitN(k, "|", 3)
		r.IndependentN = plabIndependentBlocks(r.IndependenceGroups)
		r.UniqueMarkets = plabUniqueMarkets(r.MarketGroups)
		bucket, emoji := plabSampleBucket(r.UniqueMarkets)
		out = append(out, map[string]any{"cell": k, "cohort": parts[0], "linkage": parts[1],
			"leg_count": strings.TrimSuffix(parts[2], "leg"), "n": r.N, "settled_rows": r.N,
			"unique_markets": r.UniqueMarkets, "unique_independent_markets": r.UniqueMarkets,
			"independent_resolution_blocks": r.IndependentN,
			"sample_bucket":                 bucket, "sample_bucket_emoji": emoji, "wins": r.Wins,
			"ev_real_per_$1":        math.Round(r.SumReal/float64(r.N)*1e4) / 1e4, // compatibility alias
			"ev_pred_per_$1":        math.Round(r.SumPred/float64(r.N)*1e4) / 1e4, // compatibility alias
			"ev_real_per_all_in_$1": math.Round(r.SumReal/float64(r.N)*1e4) / 1e4,
			"ev_pred_per_all_in_$1": math.Round(r.SumPred/float64(r.N)*1e4) / 1e4,
			"joint_p_mean":          math.Round(r.SumJP/float64(r.N)*1e4) / 1e4,
			"win_rate":              math.Round(float64(r.Wins)/float64(r.N)*1e3) / 1e3})
	}
	sort.Slice(out, func(i, j int) bool { // EV-first ordering (prime directive)
		return out[i]["ev_real_per_$1"].(float64) > out[j]["ev_real_per_$1"].(float64)
	})
	return out
}

// handleParlayLab (GET /api/parlaylab) — the running research case. Log-only.
func (s *Server) handleParlayLab(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	s.plabMu.Lock()
	s.plabState(ctx)
	cand, graded := s.plabCand, s.plabGraded
	classes := []map[string]any{}
	movers := []map[string]any{}
	rollingSettledN := map[int]int{}
	rollingSettledSum := map[int]float64{}
	rollingGroups := map[int][][]string{}
	rollingMarketGroups := map[int][][]string{}
	for k, a := range s.plabStats {
		if a.N == 0 {
			continue
		}
		parts := strings.Split(k, "|")
		cohort := storage.ComboLabCohortAllEligible
		if len(parts) >= 5 && (parts[len(parts)-1] == storage.ComboLabCohortAllEligible ||
			parts[len(parts)-1] == storage.ComboLabCohortRollingPositive ||
			parts[len(parts)-1] == storage.ComboLabCohortPromotedSystem) {
			cohort = parts[len(parts)-1]
		}
		if cohort == storage.ComboLabCohortRollingPositive && len(parts) >= 2 {
			if legs, err := strconv.Atoi(strings.TrimSuffix(parts[1], "leg")); err == nil && legs >= 2 && legs <= parlayLegLimit {
				rollingSettledN[legs] += a.N
				rollingSettledSum[legs] += a.SumReal
				rollingGroups[legs] = append(rollingGroups[legs], a.IndependenceGroups...)
				rollingMarketGroups[legs] = append(rollingMarketGroups[legs], a.MarketGroups...)
			}
		}
		bucket, emoji := plabSampleBucket(a.UniqueMarkets)
		row := map[string]any{"class": k, "cohort": cohort, "n": a.N, "settled_rows": a.N,
			"unique_markets": a.UniqueMarkets, "unique_independent_markets": a.UniqueMarkets,
			"independent_resolution_blocks": a.IndependentN,
			"sample_bucket":                 bucket, "sample_bucket_emoji": emoji,
			"ev_real_per_$1":        math.Round(a.SumReal/float64(a.N)*1e4) / 1e4, // compatibility alias
			"ev_real_per_all_in_$1": math.Round(a.SumReal/float64(a.N)*1e4) / 1e4,
			"win_rate":              math.Round(float64(a.Wins)/float64(a.N)*1e3) / 1e3}
		// R123 Part 3: always-valid CI on the post-R123 sample + the PROVEN+ shape flag.
		if mean, lo, hi, n2, ok := plabCellCI(a); ok {
			row["n_ci"] = n2
			row["ev_ci"] = []float64{math.Round(lo*1e4) / 1e4, math.Round(hi*1e4) / 1e4}
			row["proven_pos"] = n2 >= 20 && lo > 0
			_ = mean
			_ = hi
		}
		// R123 Part 3: Δ vs the 7d snapshot (movers) — only cells the snapshot knew.
		if s.plabSnap != nil {
			if prev, ok := s.plabSnap[k]; ok && prev[0] >= 5 && a.N >= 5 {
				d := a.SumReal/float64(a.N) - prev[1]
				row["delta_7d"] = math.Round(d*1e4) / 1e4
				if math.Abs(d) > 0.005 && a.N >= 20 {
					movers = append(movers, map[string]any{"class": k, "n": a.N,
						"ev_now":   math.Round(a.SumReal/float64(a.N)*1e4) / 1e4,
						"delta_7d": math.Round(d*1e4) / 1e4})
				}
			}
		}
		classes = append(classes, row)
	}
	corr := map[string]any{}
	for k, c := range s.plabCorr {
		rho, n := plabRho(*c)
		corr[k] = map[string]any{"n": n, "rho_shrunk": math.Round(rho*1e3) / 1e3}
	}
	enum := s.plabEnum
	snapAge := 0.0
	if !s.plabSnapTS.IsZero() {
		snapAge = time.Since(s.plabSnapTS).Hours() / 24
	}
	s.plabMu.Unlock()
	sort.Slice(classes, func(i, j int) bool {
		return classes[i]["ev_real_per_$1"].(float64) > classes[j]["ev_real_per_$1"].(float64)
	})
	sort.Slice(movers, func(i, j int) bool {
		return math.Abs(movers[i]["delta_7d"].(float64)) > math.Abs(movers[j]["delta_7d"].(float64))
	})
	if len(movers) > 8 {
		movers = movers[:8]
	}
	openN := s.plabOpenCount(ctx) // R128: SQLite-backed unlimited ledger (cached count)
	openCohorts, _ := s.store.PlabOpenCohortCounts(ctx)
	openLegs, _ := s.store.PlabOpenLegCounts(ctx)
	admissionEpoch, admissionEpochErr := s.plabCurrentAdmissionEpoch(ctx, time.Now().UTC(), false)
	openCohortLegs, _ := s.store.PlabOpenCurrentCohortLegCountsSince(ctx, parlayLegLimit, admissionEpoch)
	positiveSystemProgress := make([]map[string]any, 0, parlayLegLimit-1)
	for legs := 2; legs <= parlayLegLimit; legs++ {
		n := rollingSettledN[legs]
		unique := plabUniqueMarkets(rollingMarketGroups[legs])
		blocks := plabIndependentBlocks(rollingGroups[legs])
		bucket, emoji := plabSampleBucket(unique)
		row := map[string]any{"legs": legs, "settled_n": n, "settled_rows": n,
			"unique_markets": unique, "unique_independent_markets": unique,
			"independent_resolution_blocks": blocks,
			"sample_bucket":                 bucket, "sample_bucket_emoji": emoji,
			"open_n": openCohortLegs[storage.ComboLabCohortRollingPositive][legs]}
		if n > 0 {
			row["settled_net_per_$1"] = math.Round(rollingSettledSum[legs]/float64(n)*1e4) / 1e4
		}
		positiveSystemProgress = append(positiveSystemProgress, row)
	}
	allWithinLimit, openAboveLimit := int64(0), int64(0)
	for legs, n := range openLegs {
		if legs >= 2 && legs <= parlayLegLimit {
			allWithinLimit += n
		} else if legs > parlayLegLimit {
			openAboveLimit += n
		}
	}
	currentWithinLimit := int64(0)
	currentOpenCohorts := map[string]int64{}
	currentOpenLegs := map[int]int64{}
	for cohort, byLegs := range openCohortLegs {
		for legs, n := range byLegs {
			currentWithinLimit += n
			currentOpenCohorts[cohort] += n
			currentOpenLegs[legs] += n
		}
	}
	legacyBeforeCurrentHorizon := allWithinLimit - currentWithinLimit
	if legacyBeforeCurrentHorizon < 0 {
		legacyBeforeCurrentHorizon = 0
	}
	admissionEpochText := "not-started"
	if admissionEpochErr != nil {
		admissionEpochText = "invalid"
	} else if admissionEpoch > 0 {
		admissionEpochText = time.Unix(admissionEpoch, 0).UTC().Format(time.RFC3339)
	}
	routeBridge, routeBridgeErr := s.store.ResearchBundlePromotionStatus(ctx)
	s.plabMu.Lock()
	tickMs := s.plabTickMs
	loggedHr, gradedHr := s.plabHourLogged.lastHour(time.Now()), s.plabHourGraded.lastHour(time.Now())
	s.plabMu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{
		"name":                    "Combo Lab",
		"canonical_endpoint":      "/api/combolab",
		"compatibility_endpoint":  "/api/parlaylab remains a read-only compatibility alias",
		"economics_version":       "all-in-v2",
		"return_unit":             "net return per total entry dollar including every frozen rolled-leg fee",
		"legacy_aggregate_status": "archived and excluded: pre-R144 bare-$1 denominator could report impossible losses below -100%",
		"sample_bucket_legend": map[string]any{"basis": "unique venue+ticker markets, never duplicate settled rows",
			"red": "0-10", "orange": "11-40", "yellow": "41-120", "green": "121-500",
			"blue": "501-1000", "purple": "1001+"},
		"note":                               "Combo Lab is log-only research and separate from the funded Combos portfolio; synthetic grades are not placed profit. Rolling-positive-system rows test every current positive named system in a PAPER-only cohort after exact executable repricing. Current returns use all-in-v2 economics and each result exposes settled rows, unique venue+ticker markets and conservative connected resolution blocks. Promoted-system-combo remains the stricter sealed lane. No row is LIVE-transferable until the identical RFQ combo earns its own quote, Paper route, settlement, and untouched proof.",
		"counter_semantics":                  "candidates is the accepted sample-row counter; graded/settled_rows includes only immutable all-in-v2 receipts; unique_markets deduplicates venue+ticker identities (unique_independent_markets is a compatibility alias); independent_resolution_blocks collapses rows connected by a shared event/resolution key for confidence bounds; open is current-policy prospective rows only; settlement_backlog_open preserves older unresolved research; last-hour counters count sample rows only, never the logical manifest",
		"open":                               currentWithinLimit,
		"open_candidate_rows":                currentWithinLimit,
		"open_by_cohort":                     currentOpenCohorts,
		"open_by_cohort_and_legs":            openCohortLegs,
		"open_by_legs":                       currentOpenLegs,
		"settlement_backlog_open":            openN,
		"settlement_backlog_by_cohort":       openCohorts,
		"settlement_backlog_by_legs":         openLegs,
		"open_within_leg_limit":              currentWithinLimit,
		"open_above_leg_limit":               openAboveLimit,
		"legacy_open_before_current_horizon": legacyBeforeCurrentHorizon,
		"current_admission_epoch":            admissionEpochText,
		"current_max_legs":                   parlayLegLimit,
		"prospective_sample_cap": map[string]any{
			"current_2_to_6_open_rows":               currentWithinLimit,
			"current_2_to_6_limit":                   plabSampleOpenCap,
			"room":                                   math.Max(0, float64(plabSampleOpenCap)-float64(currentWithinLimit)),
			"legacy_before_current_horizon_excluded": legacyBeforeCurrentHorizon,
			"legacy_above_6_excluded":                openAboveLimit,
			"truth":                                  "the logical manifest is uncapped; only rows admitted after the durable 4h-normal/2h-crypto epoch use this cap; older and >6-leg rows keep settling without consuming it",
		},
		"open_cap":                       "logical manifest unlimited; current 2-6 prospective settlement sample capped separately",
		"logged_last_hour":               loggedHr, // R128: throughput receipts
		"graded_last_hour":               gradedHr,
		"tick_ms_ewma":                   math.Round(tickMs), // R122: measured cycle cost (the widening's honesty meter)
		"candidates":                     cand,
		"graded":                         graded,
		"graded_sample":                  graded,
		"retained_graded":                graded,
		"current_representation":         enum.Representation,
		"summary":                        s.plabSummary(),
		"classes":                        classes,
		"movers":                         movers, // R123 Part 3: biggest Δ7d classes (n≥20)
		"snapshot_age_days":              math.Round(snapAge*10) / 10,
		"enumeration":                    enum, // R123 Part 2: the honest combinatorics of the last cycle
		"positive_system_route_coverage": enum.PositiveRouteCoverage,
		"correlations":                   corr,
		"probe":                          s.cprobeStats(), // R112 API-oracle legality telemetry
		"twins":                          s.twinStats(),   // R112 verified-twin registry telemetry
		"live_transferable":              false,
		"rolling_positive_cohort_state":  "paper-only-unsealed-never-live-authorizing",
		"positive_system_leg_progress":   positiveSystemProgress,
		"funded_positive_system_combos":  s.positiveSystemComboStatus(ctx),
		"typed_positive_route_bundle_cohort": map[string]any{
			"state":          "paper-research-only-unsealed-never-live-authorizing",
			"candidates":     enum.TypedPositiveBundles,
			"by_system":      enum.TypedPositiveBySystem,
			"by_legs":        enum.TypedPositiveByLegs,
			"grading":        "native immutable payoff-state route ledger",
			"live_authority": false,
			"truth":          "Positive verified/structural 2-6-leg route bundles use their frozen exact books, depth, fee schedules and full payoff states. Certificate status and blockers stay visible and bar promotion. They are not converted into an all-legs-win parlay because that would misgrade solver portfolios.",
		},
		"promoted_cohort_state": "awaiting-rfq-identical-proof",
		"live_route_proved_bridge": map[string]any{"status": routeBridge,
			"error": func() string {
				if routeBridgeErr != nil {
					return routeBridgeErr.Error()
				}
				return ""
			}(),
			"truth": "This is the distinct typed Kalshi conjunction lane. Only its exact authenticated RFQ quote plus identical Paper acceptance and sealed untouched route proof can later enter armed LIVE AUTO; the all-eligible and positive-single-leg Combo Lab rows remain synthetic research."},
	})
}
