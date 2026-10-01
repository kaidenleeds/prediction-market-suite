package server

// positive_system_combos.go observes Combo Lab's current positive-system legs and retains the
// historical funded ledger. R166 stops new P&L entries until a delayed newer complete executable
// quote can produce an honest terminal fill/zero-fill/not-observed receipt. Old positions remain
// settleable, and this route never grants LIVE authority.

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/paper"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

const (
	positiveSystemComboStatusKV      = "positive_system_combo_paper_status"
	positiveSystemComboRouteSource   = storage.FundedPositiveSystemComboRoute
	positiveSystemComboMaxNewPerTurn = 10 // throughput bound, not a portfolio/open-position cap
	positiveSystemComboCooldown      = 10 * time.Minute
)

// fundedComboContract is frozen at candidate selection, then checked against a second live-book
// reprice immediately before insertion. It is a Paper-only storage contract, not LIVE authority.
type fundedComboContract struct {
	RouteSource string
	Cohort      string
	SystemIDs   []string
	JointP      float64
	ForceTaker  bool
	// Collection freezes the Kalshi combined-market collection selected by candidate discovery.
	// The funded placement wall re-fetches the open collection rules and requires the exact same
	// collection immediately before insertion. It stays blank for venues without an RFQ route.
	Collection string
	// Portfolio selects the independently compounded money wall and funded-relation identity.
	// Blank preserves the positive-system Combo Paper route for compatibility.
	Portfolio string
	// Attribution owns the product, not every contributing leg system. Contributor SystemIDs stay
	// informational; the registered parlay-Nleg row receives the economic result exactly once.
	ProducerFamily  string
	ExperimentEpoch string
}

type positiveSystemComboCandidate struct {
	Legs         []plabLeg
	Systems      []string
	Collection   string
	Prod         float64
	JointP       float64
	EVNet        float64
	EntryCapital float64
	Score        float64
	Sig          string
}

type positiveSystemComboPaperStatus struct {
	UpdatedAt     string         `json:"updated_at"`
	Epoch         string         `json:"epoch,omitempty"`
	State         string         `json:"state"`
	AutoOn        bool           `json:"auto_on"`
	ExactLegs     int            `json:"exact_positive_legs"`
	Systems       []string       `json:"systems"`
	ByVenue       map[string]int `json:"legs_by_venue"`
	Proposed      int            `json:"proposed"`
	Eligible      int            `json:"eligible"`
	Placed        int            `json:"placed_this_turn"`
	PlacedIDs     []int64        `json:"placed_ids,omitempty"`
	PlacedSystems []string       `json:"placed_systems,omitempty"`
	Open          int            `json:"funded_open"`
	OpenByLegs    map[int]int    `json:"funded_open_by_legs"`
	Closed        int            `json:"funded_closed"`
	ClosedByLegs  map[int]int    `json:"funded_closed_by_legs"`
	RealizedNet   float64        `json:"funded_realized_net"`
	NetPerDay     float64        `json:"funded_net_per_day"`
	Reasons       map[string]int `json:"reasons,omitempty"`
	MaxLegs       int            `json:"max_legs"`
	LiveAuthority bool           `json:"live_authority"`
	Truth         string         `json:"truth"`
}

func positiveSystemComboSignature(legs []plabLeg) string {
	parts := make([]string, 0, len(legs))
	for _, leg := range legs {
		parts = append(parts, strings.ToLower(leg.Platform)+"|"+strings.ToUpper(leg.Ticker)+"|"+strings.ToUpper(leg.Side))
	}
	sort.Strings(parts)
	return strings.Join(parts, "+")
}

func paperComboSignature(legs []paper.Leg) string {
	parts := make([]string, 0, len(legs))
	for _, leg := range legs {
		parts = append(parts, strings.ToLower(leg.Platform)+"|"+strings.ToUpper(leg.Ticker)+"|"+strings.ToUpper(leg.Side))
	}
	sort.Strings(parts)
	return strings.Join(parts, "+")
}

func isFundedPositiveSystemCombo(row paper.Parlay) bool {
	return row.RouteSource == positiveSystemComboRouteSource && row.Cohort == storage.ComboLabCohortRollingPositive
}

func positiveComboSystems(legs []plabLeg) []string {
	set := map[string]bool{}
	for _, leg := range legs {
		for _, system := range leg.PositiveSystems {
			if system = strings.TrimSpace(system); system != "" {
				set[system] = true
			}
		}
	}
	out := make([]string, 0, len(set))
	for system := range set {
		out = append(out, system)
	}
	sort.Strings(out)
	return out
}

// positiveSystemComboCandidates preserves the same prospective math as Combo Lab.  The 2-cent
// product floor is the funded route's existing 50x payout fuse expressed before placement.
// fundedComboRelationGate is deliberately stricter than Combo Lab. The lab may measure related
// synthetic conjunctions, but a funded Paper portfolio that is meant to transfer to LIVE may not
// guess their joint payoff from a sparse/shrunk correlation estimate. Until an exact venue quote
// or a certified payoff solver owns that related shape, funded rows require complete, distinct
// canonical event identities. This blocks mutually exclusive same-game outcomes from looking like
// independent positive legs while preserving them in the research-only Combo Lab.
func fundedComboRelationGate(legs []plabLeg) (bool, string) {
	seen := make(map[string]struct{}, len(legs))
	for _, leg := range legs {
		eventKey := strings.ToLower(strings.TrimSpace(leg.EventKey))
		if eventKey == "" {
			return false, "missing_relation_identity"
		}
		if _, exists := seen[eventKey]; exists {
			return false, "related_without_exact_joint_quote"
		}
		seen[eventKey] = struct{}{}
	}
	return true, ""
}

func (s *Server) positiveSystemComboCandidates(ctx context.Context, pool []plabLeg, maxLegs int) ([]positiveSystemComboCandidate, map[string]int) {
	rows, _ := plabStableProspectiveSample(pool, maxLegs, 512)
	out := make([]positiveSystemComboCandidate, 0, len(rows))
	rejected := map[string]int{}
	for _, row := range rows {
		if row.Cohort != storage.ComboLabCohortRollingPositive || len(row.Legs) < 2 || len(row.Legs) > parlayLegLimit {
			continue
		}
		if plabValidatePricedLegSet(row.Legs) != nil {
			rejected["invalid_leg_set"]++
			continue
		}
		if ok, reason := fundedComboRelationGate(row.Legs); !ok {
			rejected[reason]++
			continue
		}
		collection := ""
		if strings.EqualFold(row.Legs[0].Platform, "kalshi") {
			var ok bool
			collection, ok = s.fundedComboKalshiCollection(ctx, row.Legs)
			if !ok {
				rejected["kalshi_not_rfq_compatible"]++
				continue
			}
		}
		prod := 1.0
		for _, leg := range row.Legs {
			prod *= leg.Price
		}
		if prod < 0.02 || prod >= 1 { // identical to the funded 50x fuse
			continue
		}
		_, _, _, overlap := plabComboShape(row.Legs)
		s.plabMu.Lock()
		joint, _, _ := s.plabJointPEx(row.Legs, overlap)
		s.plabMu.Unlock()
		fee, _, ok := s.plabExactSyntheticFees(row.Legs)
		if !ok || joint <= 0 || joint >= 1 {
			continue
		}
		ev, economicsOK := plabAllInReturn(joint, prod, fee)
		if !economicsOK || ev <= 0 {
			continue
		}
		systems := positiveComboSystems(row.Legs)
		if len(systems) == 0 {
			continue
		}
		// Favor conservative profit rate: fee-net return, then nearer/higher joint hit chance.  This
		// only orders PAPER collection; it cannot authorize a LIVE route.
		score := ev * math.Sqrt(joint)
		out = append(out, positiveSystemComboCandidate{Legs: row.Legs, Systems: systems, Collection: collection,
			Prod: prod, JointP: joint, EVNet: ev, EntryCapital: 1 + fee,
			Score: score, Sig: positiveSystemComboSignature(row.Legs)})
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Score != out[j].Score {
			return out[i].Score > out[j].Score
		}
		return out[i].Sig < out[j].Sig
	})
	return out, rejected
}

func positiveComboFailureClass(err error) string {
	if err == nil {
		return ""
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "expected value"):
		return "final_fee_net_ev"
	case strings.Contains(msg, "paper-not-observed") || strings.Contains(msg, "delayed newer complete"):
		return "paper_not_observed_unverified_execution"
	case strings.Contains(msg, "portfolio") || strings.Contains(msg, "available"):
		return "portfolio_full"
	case strings.Contains(msg, "horizon"):
		return "horizon_changed"
	case strings.Contains(msg, "fresh complete") || strings.Contains(msg, "common touch") ||
		strings.Contains(msg, "live price") || strings.Contains(msg, "stale") || strings.Contains(msg, "moved"):
		return "book_changed"
	case strings.Contains(msg, "exact taker fee"):
		return "fee_authority_missing"
	case strings.Contains(msg, "quantity"):
		return "quantity_authority_missing"
	case strings.Contains(msg, "collection") || strings.Contains(msg, "rfq"):
		return "kalshi_not_rfq_compatible"
	case strings.Contains(msg, "artifact") || strings.Contains(msg, "payout multiple") || strings.Contains(msg, "bad combined"):
		return "price_sanity"
	default:
		return "placement_error"
	}
}

func (s *Server) savePositiveSystemComboStatus(ctx context.Context, st positiveSystemComboPaperStatus) {
	st.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	st.MaxLegs = parlayLegLimit
	st.LiveAuthority = false
	st.Truth = "signals and candidates are tracked, but new funded Combo Paper entries are not booked until a delayed newer complete executable quote can produce an honest fill, zero-fill, or not-observed result"
	if body, err := json.Marshal(st); err == nil {
		_ = s.store.KVSet(ctx, positiveSystemComboStatusKV, string(body))
	}
}

func (s *Server) positiveSystemComboStatus(ctx context.Context) positiveSystemComboPaperStatus {
	s.positiveComboPaperMu.Lock()
	defer s.positiveComboPaperMu.Unlock()
	return s.positiveSystemComboStatusLocked(ctx)
}

// positiveSystemComboStatusLocked reports only rows created in the current Reset-P&L epoch.
// Older funded rows remain in paper_parlays as research history, but cannot reappear in the
// current portfolio, P&L, Net/day, open counts, or duplicate/cooldown state after a reset.
func (s *Server) positiveSystemComboStatusLocked(ctx context.Context) positiveSystemComboPaperStatus {
	st := positiveSystemComboPaperStatus{State: "warming", ByVenue: map[string]int{}, OpenByLegs: map[int]int{},
		ClosedByLegs: map[int]int{}, Reasons: map[string]int{}, MaxLegs: parlayLegLimit, LiveAuthority: false}
	if raw, ok := s.store.KVGet(ctx, positiveSystemComboStatusKV); ok {
		_ = json.Unmarshal([]byte(raw), &st)
	}
	epoch, afterID := s.positiveSystemComboEpochBoundary()
	if !epoch.IsZero() && st.Epoch != epoch.UTC().Format(time.RFC3339Nano) {
		// Crash-safe half-boundary: pnl_reset is the durable authority. If the process stopped
		// after persisting that epoch but before rewriting this optional collector receipt, never
		// resurrect its old placed/count/reason fields on restart.
		s.autoMu.Lock()
		autoOn := s.autoOn
		s.autoMu.Unlock()
		st = freshPositiveSystemComboEpochStatus(epoch, autoOn)
	}
	if st.ByVenue == nil {
		st.ByVenue = map[string]int{}
	}
	if st.OpenByLegs == nil {
		st.OpenByLegs = map[int]int{}
	}
	if st.ClosedByLegs == nil {
		st.ClosedByLegs = map[int]int{}
	}
	if st.Reasons == nil {
		st.Reasons = map[string]int{}
	}
	// KV is only the last collector receipt. Recompute money/performance from durable provenance so
	// an old pre-migration receipt can never keep mislabeling legacy/generic/RFQ rows.
	st.Open, st.Closed, st.RealizedNet, st.NetPerDay = 0, 0, 0, 0
	st.OpenByLegs, st.ClosedByLegs = map[int]int{}, map[int]int{}
	if !epoch.IsZero() {
		st.Epoch = epoch.UTC().Format(time.RFC3339Nano)
	}
	var firstRouteTS time.Time
	if rows, err := s.store.ListParlays(ctx, ""); err == nil {
		for _, row := range rows {
			if !isFundedPositiveSystemCombo(row) || !positiveSystemComboRowInEpoch(row, epoch, afterID) {
				continue
			}
			if ts, err := time.Parse(time.RFC3339Nano, row.TS); err == nil && (firstRouteTS.IsZero() || ts.Before(firstRouteTS)) {
				firstRouteTS = ts
			}
			if row.Status == "open" {
				st.Open++
				st.OpenByLegs[len(row.Legs)]++
				continue
			}
			st.Closed++
			st.ClosedByLegs[len(row.Legs)]++
			if !row.Artifact {
				st.RealizedNet += row.Realized
			}
		}
	}
	if !firstRouteTS.IsZero() {
		st.NetPerDay = st.RealizedNet / math.Max(1.0/24, time.Since(firstRouteTS).Hours()/24)
	}
	return st
}

func (s *Server) positiveSystemComboEpoch() time.Time {
	epoch, _ := s.positiveSystemComboEpochBoundary()
	return epoch
}

func (s *Server) positiveSystemComboEpochBoundary() (time.Time, int64) {
	s.pnlMu.Lock()
	defer s.pnlMu.Unlock()
	return s.pnlEpoch, s.pnlComboAfterID
}

func positiveSystemComboRowInEpoch(row paper.Parlay, epoch time.Time, afterID int64) bool {
	if afterID > 0 {
		return row.ID > afterID
	}
	if epoch.IsZero() {
		return true
	}
	ts, err := time.Parse(time.RFC3339Nano, row.TS)
	return err == nil && !ts.Before(epoch)
}

// resetPositiveSystemComboStatusLocked clears only the current collector receipt. Durable rows
// are intentionally retained; positiveSystemComboRowInEpoch makes the new epoch the current
// portfolio and also clears pre-reset duplicate/cooldown state.
func (s *Server) resetPositiveSystemComboStatusLocked(ctx context.Context, epoch time.Time) {
	s.autoMu.Lock()
	autoOn := s.autoOn
	s.autoMu.Unlock()
	s.savePositiveSystemComboStatus(ctx, freshPositiveSystemComboEpochStatus(epoch, autoOn))
}

func freshPositiveSystemComboEpochStatus(epoch time.Time, autoOn bool) positiveSystemComboPaperStatus {
	return positiveSystemComboPaperStatus{
		State:        "warming",
		AutoOn:       autoOn,
		Epoch:        epoch.UTC().Format(time.RFC3339Nano),
		ByVenue:      map[string]int{},
		OpenByLegs:   map[int]int{},
		ClosedByLegs: map[int]int{},
		Reasons:      map[string]int{"post_reset_warmup": 1},
		MaxLegs:      parlayLegLimit,
	}
}

func (s *Server) positiveSystemComboBriefLine(ctx context.Context) string {
	st := s.positiveSystemComboStatus(ctx)
	if st.UpdatedAt == "" {
		return "💵 Positive-system Combos · warming · funded Paper only"
	}
	legs := make([]string, 0, parlayLegLimit-1)
	for n := 2; n <= parlayLegLimit; n++ {
		legs = append(legs, fmt.Sprintf("%dL o%d", n, st.OpenByLegs[n]))
	}
	reason := ""
	if st.Placed == 0 && len(st.Reasons) > 0 {
		keys := make([]string, 0, len(st.Reasons))
		for key := range st.Reasons {
			keys = append(keys, key)
		}
		sort.Slice(keys, func(i, j int) bool {
			if st.Reasons[keys[i]] != st.Reasons[keys[j]] {
				return st.Reasons[keys[i]] > st.Reasons[keys[j]]
			}
			return keys[i] < keys[j]
		})
		reason = fmt.Sprintf(" · zero reason %s=%d", strings.ReplaceAll(keys[0], "_", " "), st.Reasons[keys[0]])
	}
	return fmt.Sprintf("💵 Positive-system Combos · %s · %d systems/%d exact legs · eligible %d · placed +%d · closed %d/$%+.2f · %s%s",
		strings.ReplaceAll(st.State, "_", " "), len(st.Systems), st.ExactLegs, st.Eligible, st.Placed,
		st.Closed, st.RealizedNet, strings.Join(legs, " · "), reason)
}

// autoPlacePositiveSystemCombos is called with the freshly repriced executable manifest pool.
// It now records candidate counts and an explicit not-observed state without booking new P&L.
// The historical placement branch remains behind the R166 fail-closed gate for settlement tests.
func (s *Server) autoPlacePositiveSystemCombos(ctx context.Context, pool []plabLeg) {
	s.positiveComboPaperMu.Lock()
	defer s.positiveComboPaperMu.Unlock()
	st := positiveSystemComboPaperStatus{State: "ready", ByVenue: map[string]int{}, OpenByLegs: map[int]int{},
		ClosedByLegs: map[int]int{}, Reasons: map[string]int{}, MaxLegs: parlayLegLimit}
	epoch, afterID := s.positiveSystemComboEpochBoundary()
	if !epoch.IsZero() {
		st.Epoch = epoch.UTC().Format(time.RFC3339Nano)
	}
	systems := map[string]bool{}
	rolling := make([]plabLeg, 0, len(pool))
	for _, leg := range pool {
		if !leg.RollingPositive || leg.SystemRoute != "taker" || leg.EVNet <= 0 || leg.Depth < 1 {
			continue
		}
		venue := strings.ToLower(strings.TrimSpace(leg.Platform))
		if venue != "kalshi" && venue != "polyus" {
			continue
		}
		rolling = append(rolling, leg)
		st.ByVenue[venue]++
		for _, system := range leg.PositiveSystems {
			if system = strings.TrimSpace(system); system != "" {
				systems[system] = true
			}
		}
	}
	st.ExactLegs = len(rolling)
	for system := range systems {
		st.Systems = append(st.Systems, system)
	}
	sort.Strings(st.Systems)

	all, err := s.store.ListParlays(ctx, "")
	if err != nil {
		st.State, st.Reasons["ledger_error"] = "blocked", 1
		s.savePositiveSystemComboStatus(ctx, st)
		return
	}
	openSig := map[string]bool{}
	recentSig := map[string]bool{}
	var firstRouteTS time.Time
	for _, row := range all {
		if !isFundedPositiveSystemCombo(row) || !positiveSystemComboRowInEpoch(row, epoch, afterID) {
			continue // legacy/generic/manual/RFQ rows never label or throttle this route
		}
		sig := paperComboSignature(row.Legs)
		if ts, parseErr := time.Parse(time.RFC3339Nano, row.TS); parseErr == nil && (firstRouteTS.IsZero() || ts.Before(firstRouteTS)) {
			firstRouteTS = ts
		}
		if row.Status == "open" {
			openSig[sig] = true
			st.Open++
			st.OpenByLegs[len(row.Legs)]++
			continue
		}
		st.Closed++
		st.ClosedByLegs[len(row.Legs)]++
		if !row.Artifact {
			st.RealizedNet += row.Realized
		}
		stamp := firstNonEmpty(row.SettledTS, row.TS)
		if t, parseErr := time.Parse(time.RFC3339Nano, stamp); parseErr == nil && time.Since(t) < positiveSystemComboCooldown {
			recentSig[sig] = true
		}
	}
	if !firstRouteTS.IsZero() {
		days := math.Max(1.0/24, time.Since(firstRouteTS).Hours()/24)
		st.NetPerDay = st.RealizedNet / days
	}
	s.autoMu.Lock()
	st.AutoOn = s.autoOn
	s.autoMu.Unlock()
	if s.ksBlocked() {
		st.State, st.Reasons["kill_switch"] = "paused", 1
		s.savePositiveSystemComboStatus(ctx, st)
		return
	}
	if !st.AutoOn {
		st.State, st.Reasons["auto_off"] = "paused", 1
		s.savePositiveSystemComboStatus(ctx, st)
		return
	}
	if len(rolling) < 2 {
		st.State, st.Reasons["fewer_than_two_exact_legs"] = "waiting", 1
		s.savePositiveSystemComboStatus(ctx, st)
		return
	}

	candidates, rejected := s.positiveSystemComboCandidates(ctx, rolling, parlayLegLimit)
	for reason, n := range rejected {
		st.Reasons[reason] += n
	}
	st.Proposed = len(candidates)
	if len(candidates) == 0 {
		st.State = "waiting"
		if len(st.Reasons) == 0 {
			st.Reasons["no_fee_positive_sane_independent_combo"] = 1
		}
		s.savePositiveSystemComboStatus(ctx, st)
		return
	}
	st.Eligible = len(candidates)
	if !s.r166CanBookVerifiedMultiLegPaper() {
		st.State = "observing"
		st.Reasons["paper_not_observed_unverified_execution"] += len(candidates)
		s.savePositiveSystemComboStatus(ctx, st)
		if len(candidates) > 0 {
			_ = s.store.Audit(ctx, "info", "paper", fmt.Sprintf(
				"positive-system combo observed %d candidate(s) without booking Paper P&L", len(candidates)),
				r166UnverifiedMultiLegPaperReason)
		}
		return
	}
	coveredSystems := map[string]bool{}
	coveredLegs := map[int]bool{}
	for st.Placed < positiveSystemComboMaxNewPerTurn && len(candidates) > 0 {
		best := -1
		bestRank := math.Inf(-1)
		for i, cand := range candidates {
			if openSig[cand.Sig] || recentSig[cand.Sig] {
				continue
			}
			rank := cand.Score
			if !coveredLegs[len(cand.Legs)] && st.OpenByLegs[len(cand.Legs)] == 0 {
				rank += 1e6 // first collect one current row at every feasible 2..6 length
			}
			for _, system := range cand.Systems {
				if !coveredSystems[system] {
					rank += 1e5
				}
			}
			if rank > bestRank {
				best, bestRank = i, rank
			}
		}
		if best < 0 {
			st.Reasons["already_open_or_cooldown"]++
			break
		}
		cand := candidates[best]
		candidates = append(candidates[:best], candidates[best+1:]...)
		legs := make([]paper.Leg, 0, len(cand.Legs))
		for _, leg := range cand.Legs {
			legs = append(legs, paper.Leg{Platform: strings.ToLower(leg.Platform), Ticker: leg.Ticker,
				Side: strings.ToUpper(leg.Side), Entry: leg.Price})
		}
		// Conservative fractional Kelly on fee-net probability margin, capped at 2.5% of the
		// dedicated Paper portfolio.  The cap is per opportunity, never a total/open-count cap.
		// Convert all-in-dollar return back to its probability-space margin. The former
		// product*EVNet omitted fee capital and understated/warped the Kelly input.
		margin := math.Max(0, cand.JointP-cand.Prod*cand.EntryCapital)
		fstar := margin / math.Max(1e-9, 1-cand.Prod)
		kf := s.cfg().Auto.KellyFrac
		if kf <= 0 || kf > .25 {
			kf = .25
		}
		bank := s.parlayBankroll()
		stake := clampF(bank*fstar*kf, 1, bank*.025)
		contract := &fundedComboContract{RouteSource: positiveSystemComboRouteSource,
			Cohort: storage.ComboLabCohortRollingPositive, SystemIDs: append([]string(nil), cand.Systems...),
			JointP: cand.JointP, ForceTaker: true, Collection: cand.Collection,
			ProducerFamily: "positive-system", ExperimentEpoch: firstNonEmpty(st.Epoch,
				positiveSystemComboRouteSource+":"+storage.ComboLabCohortRollingPositive)}
		id, placeErr := s.placeParlayLegsWithContract(ctx, legs, stake,
			"positive-systems:"+strings.Join(cand.Systems, ","), contract)
		if placeErr != nil {
			st.Reasons[positiveComboFailureClass(placeErr)]++
			continue
		}
		st.Placed++
		st.PlacedIDs = append(st.PlacedIDs, id)
		st.Open++
		st.OpenByLegs[len(legs)]++
		openSig[cand.Sig] = true
		coveredLegs[len(legs)] = true
		for _, system := range cand.Systems {
			coveredSystems[system] = true
			st.PlacedSystems = append(st.PlacedSystems, system)
		}
	}
	sort.Strings(st.PlacedSystems)
	st.PlacedSystems = compactStrings(st.PlacedSystems)
	if st.Placed == 0 {
		st.State = "waiting"
		if len(st.Reasons) == 0 {
			st.Reasons["no_new_unique_combo"] = 1
		}
	} else {
		st.State = "collecting"
	}
	s.savePositiveSystemComboStatus(ctx, st)
}

func compactStrings(in []string) []string {
	if len(in) < 2 {
		return in
	}
	out := in[:1]
	for _, value := range in[1:] {
		if value != out[len(out)-1] {
			out = append(out, value)
		}
	}
	return out
}
