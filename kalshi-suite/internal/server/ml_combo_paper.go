package server

// New-ML Combo keeps its isolated historical $600 ledger and continues candidate collection.
// R166 stops new P&L entries until a delayed newer complete executable quote can produce an honest
// terminal fill/zero-fill/not-observed receipt. Old positions remain settleable.

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/paper"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

const (
	mlComboPaperPortfolio  = "ml-combos"
	mlComboPaperRoute      = "new-ml-combo-paper-v1"
	mlComboPaperCohort     = "book-native-v2-ml-combo"
	mlComboPaperStatusKV   = "new_ml_combo_paper_status"
	mlComboPaperBoundaryKV = "new_ml_combo_paper_boundary"
	mlComboPaperMaxPerTurn = 10
	mlComboPaperCooldown   = 10 * time.Minute
	mlComboPaperCycle      = 30 * time.Second
)

type mlComboPaperBoundary struct {
	Epoch   string `json:"epoch"`
	AfterID int64  `json:"after_id"`
}

type mlComboVenueStatus struct {
	Open      int     `json:"open"`
	Closed    int     `json:"closed"`
	Realized  float64 `json:"realized_net"`
	LiveRoute string  `json:"live_route"`
}

type mlComboPaperStatus struct {
	UpdatedAt       string                         `json:"updated_at"`
	Epoch           string                         `json:"epoch,omitempty"`
	State           string                         `json:"state"`
	Model           string                         `json:"model"`
	Grant           float64                        `json:"starting_grant"`
	Equity          float64                        `json:"equity"`
	Available       float64                        `json:"available"`
	OpenCost        float64                        `json:"open_cost"`
	RealizedNet     float64                        `json:"realized_net"`
	Predictions     int                            `json:"positive_predictions"`
	ExactLegs       int                            `json:"exact_executable_legs"`
	Proposed        int                            `json:"proposed"`
	Eligible        int                            `json:"eligible"`
	Placed          int                            `json:"placed_this_turn"`
	PlacedIDs       []int64                        `json:"placed_ids,omitempty"`
	Open            int                            `json:"open"`
	Closed          int                            `json:"closed"`
	OpenByLegs      map[int]int                    `json:"open_by_legs"`
	ClosedByLegs    map[int]int                    `json:"closed_by_legs"`
	ByVenue         map[string]*mlComboVenueStatus `json:"by_venue"`
	Reasons         map[string]int                 `json:"reasons,omitempty"`
	MaxLegs         int                            `json:"max_legs"`
	KalshiLiveShape string                         `json:"kalshi_live_shape"`
	PolyUSLiveShape string                         `json:"polyus_live_shape"`
	LiveAuthority   bool                           `json:"live_authority"`
}

type mlComboCandidate struct {
	Legs       []plabLeg
	Product    float64
	JointP     float64
	EVNet      float64
	Score      float64
	Signature  string
	Collection string
}

func isMLComboPaper(row paper.Parlay) bool {
	return row.RouteSource == mlComboPaperRoute && row.Cohort == mlComboPaperCohort
}

func newMLComboStatus() mlComboPaperStatus {
	return mlComboPaperStatus{
		State: "warming", Model: currentMLCohort, Grant: 600, MaxLegs: parlayLegLimit,
		OpenByLegs: map[int]int{}, ClosedByLegs: map[int]int{}, Reasons: map[string]int{},
		ByVenue: map[string]*mlComboVenueStatus{
			"kalshi": {LiveRoute: "RFQ-compatible shape only; LIVE disarmed and unproved"},
			"polyus": {LiveRoute: "Paper-only; no supported LIVE combo route"},
		},
		KalshiLiveShape: "same-venue collection-compatible 2-6 legs; an authenticated RFQ quote/fill is still required before LIVE",
		PolyUSLiveShape: "Paper collection only; not LIVE-transferable while the venue combo route is unsupported",
		LiveAuthority:   false,
	}
}

func (s *Server) mlComboPaperGrant() float64 {
	return fixedPaperPortfolioUSD(s.cfg().Auto.BookMLCombosUSD)
}

func (s *Server) mlComboBoundary(ctx context.Context) (time.Time, int64) {
	if raw, ok := s.store.KVGet(ctx, mlComboPaperBoundaryKV); ok {
		var b mlComboPaperBoundary
		if json.Unmarshal([]byte(raw), &b) == nil {
			t, _ := time.Parse(time.RFC3339Nano, b.Epoch)
			return t, b.AfterID
		}
	}
	// Before an independent reset exists, the suite-wide durable reset boundary owns this new
	// portfolio too. That makes cold boot and power-loss behavior identical to the other books.
	s.pnlMu.Lock()
	defer s.pnlMu.Unlock()
	return s.pnlEpoch, s.pnlComboAfterID
}

func (s *Server) setMLComboBoundary(ctx context.Context, epoch time.Time, afterID int64) error {
	b, err := json.Marshal(mlComboPaperBoundary{Epoch: epoch.UTC().Format(time.RFC3339Nano), AfterID: afterID})
	if err != nil {
		return err
	}
	return s.store.KVSet(ctx, mlComboPaperBoundaryKV, string(b))
}

func mlComboRowInEpoch(row paper.Parlay, epoch time.Time, afterID int64) bool {
	if afterID > 0 {
		return row.ID > afterID
	}
	if epoch.IsZero() {
		return true
	}
	t, err := time.Parse(time.RFC3339Nano, row.TS)
	return err == nil && !t.Before(epoch)
}

func (s *Server) mlComboPaperRows(ctx context.Context) ([]paper.Parlay, error) {
	rows, err := s.store.ListParlaysByRouteCohort(ctx, mlComboPaperRoute, mlComboPaperCohort, "")
	if err != nil {
		return nil, err
	}
	epoch, afterID := s.mlComboBoundary(ctx)
	out := make([]paper.Parlay, 0)
	for _, row := range rows {
		if mlComboRowInEpoch(row, epoch, afterID) {
			out = append(out, row)
		}
	}
	return out, nil
}

func mlComboVenue(row paper.Parlay) string {
	if len(row.Legs) == 0 {
		return "unknown"
	}
	return strings.ToLower(strings.TrimSpace(row.Legs[0].Platform))
}

func (s *Server) recomputeMLComboMoney(ctx context.Context, st *mlComboPaperStatus) bool {
	st.Grant = s.mlComboPaperGrant()
	rows, err := s.mlComboPaperRows(ctx)
	if err != nil {
		st.State, st.Reasons["ledger_error"] = "blocked", 1
		// Preserve the configured bankroll truth even if a canceled/contended read cannot prove
		// current availability. Money paths still fail closed through mlComboPaperAvailable.
		if st.Equity <= 0 && st.RealizedNet == 0 && st.OpenCost == 0 {
			st.Equity = st.Grant
		}
		st.Available = 0
		return false
	}
	st.Open, st.Closed, st.OpenCost, st.RealizedNet = 0, 0, 0, 0
	st.OpenByLegs, st.ClosedByLegs = map[int]int{}, map[int]int{}
	for _, venue := range []string{"kalshi", "polyus"} {
		v := st.ByVenue[venue]
		if v == nil {
			v = &mlComboVenueStatus{}
			st.ByVenue[venue] = v
		}
		v.Open, v.Closed, v.Realized = 0, 0, 0
		if venue == "kalshi" {
			v.LiveRoute = "RFQ-compatible shape only; LIVE disarmed and unproved"
		} else {
			v.LiveRoute = "Paper-only; no supported LIVE combo route"
		}
	}
	for _, row := range rows {
		venue := mlComboVenue(row)
		v := st.ByVenue[venue]
		if v == nil {
			v = &mlComboVenueStatus{LiveRoute: "unsupported"}
			st.ByVenue[venue] = v
		}
		if row.Status == "open" {
			st.Open++
			st.OpenByLegs[len(row.Legs)]++
			st.OpenCost += row.Stake + math.Max(0, row.Fees)
			v.Open++
			continue
		}
		st.Closed++
		st.ClosedByLegs[len(row.Legs)]++
		v.Closed++
		if !parlayArtifact(row) {
			st.RealizedNet += row.Realized
			v.Realized += row.Realized
		}
	}
	st.Equity = math.Max(0, st.Grant+st.RealizedNet)
	st.Available = math.Max(0, st.Equity-st.OpenCost)
	return true
}

func (s *Server) saveMLComboStatus(ctx context.Context, st mlComboPaperStatus) {
	st.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	st.Model, st.MaxLegs, st.LiveAuthority = currentMLCohort, parlayLegLimit, false
	if st.Reasons == nil {
		st.Reasons = map[string]int{}
	}
	if st.ByVenue == nil {
		st.ByVenue = newMLComboStatus().ByVenue
	}
	if epoch, _ := s.mlComboBoundary(ctx); !epoch.IsZero() {
		st.Epoch = epoch.UTC().Format(time.RFC3339Nano)
	}
	s.recomputeMLComboMoney(ctx, &st)
	if body, err := json.Marshal(st); err == nil {
		_ = s.store.KVSet(ctx, mlComboPaperStatusKV, string(body))
	}
}

func (s *Server) mlComboPaperStatus(ctx context.Context) mlComboPaperStatus {
	st := newMLComboStatus()
	if raw, ok := s.store.KVGet(ctx, mlComboPaperStatusKV); ok {
		_ = json.Unmarshal([]byte(raw), &st)
	}
	if st.Reasons == nil {
		st.Reasons = map[string]int{}
	}
	if st.ByVenue == nil {
		st.ByVenue = newMLComboStatus().ByVenue
	}
	s.recomputeMLComboMoney(ctx, &st)
	return st
}

func (s *Server) mlComboPaperAvailable(ctx context.Context) float64 {
	epoch, afterID := s.mlComboBoundary(ctx)
	funds, err := s.store.RouteCohortParlayFunds(ctx, mlComboPaperRoute, mlComboPaperCohort, afterID, epoch)
	if err != nil {
		return 0
	}
	equity := math.Max(0, s.mlComboPaperGrant()+funds.Realized)
	return math.Max(0, equity-funds.OpenCommitted)
}

func (s *Server) fundedComboAvailableUSD(ctx context.Context, portfolio string) float64 {
	if strings.EqualFold(strings.TrimSpace(portfolio), mlComboPaperPortfolio) {
		return s.mlComboPaperAvailable(ctx)
	}
	return s.parlayAvailableUSD(ctx)
}

func mlComboFailureClass(err error) string {
	if err == nil {
		return ""
	}
	return positiveComboFailureClass(err)
}

func (s *Server) mlComboPredictionPool(ctx context.Context, st *mlComboPaperStatus) []plabLeg {
	var file struct {
		ExecutionEnabled bool   `json:"execution_enabled"`
		PaperAuthority   bool   `json:"paper_authority"`
		FeatureSchema    string `json:"feature_schema"`
		ModelVersion     string `json:"model_version"`
		Predictions      []struct {
			Ticker, Side, Platform, Title string
			Price                         float64  `json:"price"`
			PWin                          float64  `json:"p_win"`
			EVNet                         *float64 `json:"ev_net"`
		} `json:"predictions"`
	}
	path := filepath.Join(s.cfg().DataDir, "ml_predictions.json")
	info, err := os.Stat(path)
	if err != nil || time.Since(info.ModTime()) > 3*time.Minute || info.ModTime().After(time.Now().Add(5*time.Second)) ||
		!s.readJSONLoose(path, &file) {
		st.Reasons["prediction_file_stale_or_unreadable"]++
		return nil
	}
	if file.FeatureSchema != currentMLCohort || !mlPaperMoneyAuthority(file.ExecutionEnabled, file.PaperAuthority) {
		st.Reasons["new_ml_not_paper_authorized"]++
		return nil
	}
	st.Model = firstNonEmpty(file.ModelVersion, currentMLCohort)
	pre := make([]plabLeg, 0, len(file.Predictions))
	now := time.Now().UTC()
	for _, p := range file.Predictions {
		venue := strings.ToLower(strings.TrimSpace(p.Platform))
		side := strings.ToUpper(strings.TrimSpace(p.Side))
		if (venue != "kalshi" && venue != "polyus") || p.Ticker == "" || (side != "YES" && side != "NO") ||
			p.PWin <= 0 || p.PWin >= 1 || p.EVNet == nil || *p.EVNet <= 0 {
			continue
		}
		st.Predictions++
		leg := plabLeg{Ticker: p.Ticker, Side: side, Platform: venue, Price: p.Price, PWin: p.PWin,
			EVNet: *p.EVNet, EventKey: s.legEventKey(venue, p.Ticker, p.Title), Src: "new-ml-book-native-v2"}
		if _, reason, ok := s.paperComboLegCurrentHorizon(ctx, leg, now); !ok {
			st.Reasons["horizon_"+reason]++
			continue
		}
		pre = append(pre, leg)
	}
	quotes := s.plabExecutableQuotes(ctx, pre)
	best := map[string]plabLeg{}
	for i, leg := range pre {
		q := quotes[i]
		if !q.ok || q.ask <= .02 || q.ask >= .98 || q.depth < 1 {
			st.Reasons["no_fresh_executable_book"]++
			continue
		}
		leg.Price, leg.Depth, leg.QuoteSrc, leg.QuoteTS = q.ask, q.depth, q.src, now.Format(time.RFC3339Nano)
		fee, ok := s.plabExactTakerFee(leg, 1)
		if !ok || fee < 0 || math.IsNaN(fee) || math.IsInf(fee, 0) {
			st.Reasons["exact_fee_unavailable"]++
			continue
		}
		leg.EVNet = leg.PWin - leg.Price - fee
		if leg.EVNet <= 0 {
			st.Reasons["not_positive_at_current_ask"]++
			continue
		}
		leg.TwinKey = s.twinKeyGate(leg.Platform, leg.Ticker)
		key := strings.ToLower(leg.Platform) + "|" + strings.ToUpper(leg.Ticker) + "|" + strings.ToUpper(leg.Side)
		if old, exists := best[key]; !exists || leg.EVNet > old.EVNet {
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
	st.ExactLegs = len(out)
	return out
}

func (s *Server) fundedComboKalshiCollection(ctx context.Context, legs []plabLeg) (string, bool) {
	collections, err := s.freshComboCollections(ctx, 15*time.Second)
	if err != nil {
		return "", false
	}
	comboLegs := make([]kalshi.ComboLeg, 0, len(legs))
	for _, leg := range legs {
		market, ok := s.kmkt(leg.Ticker)
		if !ok || strings.TrimSpace(market.EventTicker) == "" {
			return "", false
		}
		comboLegs = append(comboLegs, kalshi.ComboLeg{EventTicker: market.EventTicker, Side: strings.ToLower(leg.Side)})
	}
	collection, reason := kalshi.ComboLegalAny(collections, comboLegs)
	return collection, collection != "" && reason == ""
}

// mlComboKalshiCollection preserves the original test/API seam. Both funded Combo Paper routes
// now use the same current Kalshi collection-shape proof.
func (s *Server) mlComboKalshiCollection(ctx context.Context, legs []plabLeg) (string, bool) {
	return s.fundedComboKalshiCollection(ctx, legs)
}

// mlComboPrincipalKelly returns the optimal fraction of bankroll to risk as package principal.
// product is the executable package price and feeRatio is exact leg fees per $1 of principal.
// A losing ticket loses principal+fees; a winner receives 1/product per principal dollar.
func mlComboPrincipalKelly(jointP, product, feeRatio float64) (float64, bool) {
	if jointP <= 0 || jointP >= 1 || product <= 0 || product >= 1 || feeRatio < 0 ||
		math.IsNaN(jointP) || math.IsNaN(product) || math.IsNaN(feeRatio) ||
		math.IsInf(jointP, 0) || math.IsInf(product, 0) || math.IsInf(feeRatio, 0) {
		return 0, false
	}
	allIn := 1 + feeRatio
	denom := allIn * (1 - product*allIn)
	if denom <= 0 {
		return 0, false
	}
	fraction := (jointP - product*allIn) / denom
	return fraction, fraction > 0 && fraction < 1 && !math.IsNaN(fraction) && !math.IsInf(fraction, 0)
}

func mlComboKellyStake(bank, jointP, product, feeRatio, kellyFraction float64) (float64, string, bool) {
	fstar, ok := mlComboPrincipalKelly(jointP, product, feeRatio)
	if !ok || bank <= 0 {
		return 0, "non_positive_fee_adjusted_kelly", false
	}
	if kellyFraction <= 0 || kellyFraction > .25 {
		kellyFraction = .25
	}
	capUSD := bank * .025
	rawStake := bank * fstar * kellyFraction
	if capUSD < 1 || rawStake < 1 {
		return 0, "kelly_below_minimum_order", false
	}
	return math.Min(rawStake, capUSD), "", true
}

func (s *Server) mlComboCandidates(ctx context.Context, pool []plabLeg, st *mlComboPaperStatus) []mlComboCandidate {
	rows, _ := plabStableProspectiveSample(pool, parlayLegLimit, 512)
	out := make([]mlComboCandidate, 0, len(rows))
	for _, row := range rows {
		if row.Cohort != storage.ComboLabCohortAllEligible || len(row.Legs) < 2 || len(row.Legs) > parlayLegLimit || plabValidatePricedLegSet(row.Legs) != nil {
			continue
		}
		if ok, reason := fundedComboRelationGate(row.Legs); !ok {
			st.Reasons[reason]++
			continue
		}
		venue := strings.ToLower(row.Legs[0].Platform)
		sameVenue := venue == "kalshi" || venue == "polyus"
		product := 1.0
		for _, leg := range row.Legs {
			if !strings.EqualFold(leg.Platform, venue) {
				sameVenue = false
				break
			}
			product *= leg.Price
		}
		if !sameVenue || product < .02 || product >= 1 || 1/product > 50 {
			continue
		}
		collection := ""
		if venue == "kalshi" {
			var ok bool
			collection, ok = s.fundedComboKalshiCollection(ctx, row.Legs)
			if !ok {
				st.Reasons["kalshi_not_rfq_compatible"]++
				continue
			}
		}
		_, _, _, overlap := plabComboShape(row.Legs)
		s.plabMu.Lock()
		joint, _, _ := s.plabJointPEx(row.Legs, overlap)
		s.plabMu.Unlock()
		fee, _, ok := s.plabExactSyntheticFees(row.Legs)
		if !ok || joint <= 0 || joint >= 1 {
			continue
		}
		ev, ok := plabAllInReturn(joint, product, fee)
		if !ok || ev <= 0 {
			continue
		}
		out = append(out, mlComboCandidate{Legs: row.Legs, Product: product, JointP: joint,
			EVNet: ev, Score: ev * math.Sqrt(joint), Signature: positiveSystemComboSignature(row.Legs), Collection: collection})
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Score != out[j].Score {
			return out[i].Score > out[j].Score
		}
		return out[i].Signature < out[j].Signature
	})
	return out
}

func (s *Server) autoPlaceMLComboPaper(ctx context.Context) {
	st := newMLComboStatus()
	if raw, ok := s.store.KVGet(ctx, mlComboPaperStatusKV); ok {
		var prior mlComboPaperStatus
		if json.Unmarshal([]byte(raw), &prior) == nil {
			if t, err := time.Parse(time.RFC3339Nano, prior.UpdatedAt); err == nil && time.Since(t) < mlComboPaperCycle {
				return
			}
		}
	}
	pool := s.mlComboPredictionPool(ctx, &st)
	s.positiveComboPaperMu.Lock()
	defer s.positiveComboPaperMu.Unlock()
	if s.ksBlocked() {
		st.State, st.Reasons["kill_switch"] = "paused", 1
		s.saveMLComboStatus(ctx, st)
		return
	}
	s.autoMu.Lock()
	autoOn := s.autoOn
	s.autoMu.Unlock()
	if !autoOn {
		st.State, st.Reasons["auto_off"] = "paused", 1
		s.saveMLComboStatus(ctx, st)
		return
	}
	if len(pool) < 2 {
		st.State, st.Reasons["fewer_than_two_exact_ml_legs"] = "waiting", 1
		s.saveMLComboStatus(ctx, st)
		return
	}
	rows, err := s.mlComboPaperRows(ctx)
	if err != nil {
		st.State, st.Reasons["ledger_error"] = "blocked", 1
		s.saveMLComboStatus(ctx, st)
		return
	}
	// Compound from this portfolio's own current-epoch realized P&L before calculating Kelly.
	// The placement wall recomputes it again immediately before insertion.
	s.recomputeMLComboMoney(ctx, &st)
	openSig, recentSig := map[string]bool{}, map[string]bool{}
	hadOpen := false
	for _, row := range rows {
		sig := paperComboSignature(row.Legs)
		if row.Status == "open" {
			openSig[sig] = true
			hadOpen = true
			continue
		}
		stamp := firstNonEmpty(row.SettledTS, row.TS)
		if t, e := time.Parse(time.RFC3339Nano, stamp); e == nil && time.Since(t) < mlComboPaperCooldown {
			recentSig[sig] = true
		}
	}
	candidates := s.mlComboCandidates(ctx, pool, &st)
	st.Proposed, st.Eligible = len(candidates), len(candidates)
	if !s.r166CanBookVerifiedMultiLegPaper() {
		st.State = "observing"
		st.Reasons["paper_not_observed_unverified_execution"] += len(candidates)
		s.saveMLComboStatus(ctx, st)
		if len(candidates) > 0 {
			_ = s.store.Audit(ctx, "info", "paper", fmt.Sprintf(
				"New-ML combo observed %d candidate(s) without booking Paper P&L", len(candidates)),
				r166UnverifiedMultiLegPaperReason)
		}
		return
	}
	for _, cand := range candidates {
		if st.Placed >= mlComboPaperMaxPerTurn {
			break
		}
		if openSig[cand.Signature] || recentSig[cand.Signature] {
			st.Reasons["already_open_or_cooldown"]++
			continue
		}
		legs := make([]paper.Leg, 0, len(cand.Legs))
		for _, leg := range cand.Legs {
			legs = append(legs, paper.Leg{Platform: strings.ToLower(leg.Platform), Ticker: leg.Ticker,
				Side: strings.ToUpper(leg.Side), Entry: leg.Price})
		}
		fee, _, ok := s.plabExactSyntheticFees(cand.Legs)
		if !ok {
			st.Reasons["fee_authority_missing"]++
			continue
		}
		kf := s.cfg().Auto.KellyFrac
		bank := math.Max(0, s.mlComboPaperGrant()+st.RealizedNet)
		stake, sizingReason, ok := mlComboKellyStake(bank, cand.JointP, cand.Product, fee, kf)
		if !ok {
			st.Reasons[sizingReason]++
			continue
		}
		systemIDs := []string{"new-ml"}
		if cand.Collection != "" {
			systemIDs = append(systemIDs, "kalshi-collection:"+cand.Collection)
		}
		contract := &fundedComboContract{RouteSource: mlComboPaperRoute, Cohort: mlComboPaperCohort,
			SystemIDs: systemIDs, JointP: cand.JointP, ForceTaker: true, Portfolio: mlComboPaperPortfolio,
			Collection: cand.Collection, ProducerFamily: "ml",
			ExperimentEpoch: firstNonEmpty(st.Epoch, mlComboPaperRoute+":"+mlComboPaperCohort)}
		id, placeErr := s.placeParlayLegsWithContract(ctx, legs, stake, "new-ml-combo", contract)
		if placeErr != nil {
			st.Reasons[mlComboFailureClass(placeErr)]++
			continue
		}
		st.Placed++
		st.PlacedIDs = append(st.PlacedIDs, id)
		openSig[cand.Signature] = true
	}
	if st.Placed > 0 || (len(candidates) > 0 && hadOpen) {
		st.State = "collecting"
	} else {
		// A healthy collector with executable candidates but no new unique ticket is still
		// collecting; "waiting" is reserved for a genuinely missing/blocked input funnel.
		if len(candidates) > 0 {
			st.State = "collecting"
		} else {
			st.State = "waiting"
		}
		if len(st.Reasons) == 0 {
			st.Reasons["no_new_unique_combo"] = 1
		}
	}
	s.saveMLComboStatus(ctx, st)
}

func (s *Server) resetMLComboPaperLocked(ctx context.Context) (int, error) {
	rows, err := s.store.ListParlaysByRouteCohort(ctx, mlComboPaperRoute, mlComboPaperCohort, "open")
	if err != nil {
		return 0, err
	}
	closed := 0
	for _, row := range rows {
		changed, err := s.store.VoidParlayForReset(ctx, row.ID)
		if err != nil {
			return closed, err
		}
		if changed {
			closed++
		}
	}
	maxID := int64(0)
	rows, err = s.store.ListParlaysByRouteCohort(ctx, mlComboPaperRoute, mlComboPaperCohort, "")
	if err != nil {
		return closed, err
	}
	for _, row := range rows {
		if row.ID > maxID {
			maxID = row.ID
		}
	}
	if err := s.setMLComboBoundary(ctx, time.Now().UTC(), maxID); err != nil {
		return closed, err
	}
	st := newMLComboStatus()
	st.State, st.Reasons["post_reset_warmup"] = "warming", 1
	s.saveMLComboStatus(ctx, st)
	s.invalidatePortfolioEquityCache()
	return closed, nil
}

func (s *Server) handleMLComboPaper(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	st := s.mlComboPaperStatus(ctx)
	rows, err := s.mlComboPaperRows(ctx)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	open, settled := []paper.Parlay{}, []paper.Parlay{}
	for _, row := range rows {
		if row.Status == "open" {
			s.markParlay(&row)
			open = append(open, row)
		} else {
			settled = append(settled, row)
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": st, "open": open, "settled": settled})
}

func (s *Server) handleMLComboPaperReset(w http.ResponseWriter, r *http.Request) {
	s.positiveComboPaperMu.Lock()
	closed, err := s.resetMLComboPaperLocked(r.Context())
	s.positiveComboPaperMu.Unlock()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "closed": closed, "grant": s.mlComboPaperGrant()})
}

func (s *Server) mlComboPaperBriefLine(ctx context.Context) string {
	// Briefing construction has a global deadline and this line appears late in the document.
	// Give its tiny indexed read an independent bounded receipt so an exhausted upstream context
	// cannot turn a real $600 portfolio into a false $0 display.
	bctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	defer cancel()
	st := s.mlComboPaperStatus(bctx)
	pnl := fmt.Sprintf("+$%.2f", st.RealizedNet)
	if st.RealizedNet < 0 {
		pnl = fmt.Sprintf("-$%.2f", -st.RealizedNet)
	}
	return fmt.Sprintf("🤖🎲 ML Combo · $%.2f eq · $%.2f avail · P&L %s · 🅾️%d©️%d · K %d/%d · PUS %d/%d",
		st.Equity, st.Available, pnl, st.Open, st.Closed,
		st.ByVenue["kalshi"].Open, st.ByVenue["kalshi"].Closed,
		st.ByVenue["polyus"].Open, st.ByVenue["polyus"].Closed)
}
