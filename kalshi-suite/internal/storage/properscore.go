package storage

// Durable research-only proper-score observations. These rows never enter paper_fills,
// unit_trials, promotion, allocation, arming, or a venue order path.

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/properbetting"
)

type ProperScoreTrial struct {
	Observed                                              time.Time
	Slot, System, Transform, Platform, Ticker, Title      string
	Category, ForecastOriginSide, ForecastSignal          string
	ForecastSource, ForecastVersion, ModelBackend         string
	Calibration, Route, BookSource, SourceClockID         string
	StrategyMode, Cohort, FillStatus                      string
	ResolveHours, ForecastYes                             float64
	GeneratedAt                                           int64
	YesBid, YesAsk, YesBidDepth, YesAskDepth              float64
	NoBid, NoAsk, NoBidDepth, NoAskDepth                  float64
	QuoteAge, QYes, RawYes, RawNo                         float64
	CanonicalQty, ExecutableQty, EntryPrice, EntryDepth   float64
	ExactFeeTotal, ExpectedNet                            float64
	DecisionLatencyMS, ConstantShift, Rescale             float64
	NormalizedWeight, RequestedQty, TickSize, LotSize     float64
	IntegratedCost, SpotCost, LiquidityLoss               float64
	PriorActualPosition, TargetPosition                   float64
	ActualFilledDelta, CancelledDelta, PostActualPosition float64
	VectorDim, ResearchObservationID                      int64
	RawVector, DepthCurve                                 any
	ActualFill                                            bool
	SelectedSide, FeeSource, AbstainReason                string
}

// ProperScoreCoordinate is the immutable identity of one coordinate in a Proper Betting vector.
// Transform/mode/slot belong to the parent manifest; these fields distinguish the exact expected
// rows within that vector and intentionally include forecast provenance and route.
type ProperScoreCoordinate struct {
	Platform, Ticker, ForecastOriginSide, ForecastSignal string
	ForecastSource, ForecastVersion, Route               string
}

type ProperScoreManifest struct {
	Created                         time.Time
	Slot, Transform, StrategyMode   string
	ForecastSource, ForecastVersion string
	Coordinates                     []ProperScoreCoordinate
}

func normalizeProperScoreCoordinate(c ProperScoreCoordinate) ProperScoreCoordinate {
	c.Platform = strings.ToLower(strings.TrimSpace(c.Platform))
	c.Ticker = strings.TrimSpace(c.Ticker)
	c.ForecastOriginSide = strings.ToUpper(strings.TrimSpace(c.ForecastOriginSide))
	c.ForecastSignal = strings.TrimSpace(c.ForecastSignal)
	c.ForecastSource = strings.TrimSpace(c.ForecastSource)
	c.ForecastVersion = strings.TrimSpace(c.ForecastVersion)
	c.Route = strings.ToLower(strings.TrimSpace(c.Route))
	return c
}

func properScoreCoordinateValid(c ProperScoreCoordinate) bool {
	return (c.Platform == "kalshi" || c.Platform == "polyus") && c.Ticker != "" &&
		(c.ForecastOriginSide == "YES" || c.ForecastOriginSide == "NO") &&
		c.ForecastSource != "" && c.ForecastVersion != "" &&
		(c.Route == "taker" || c.Route == "maker")
}

// ProperScoreCoordinateKey is a versioned SHA-256 identity, not a delimiter-concatenated label.
// JSON's canonical struct-field order prevents venue strings from creating ambiguous identities.
func ProperScoreCoordinateKey(c ProperScoreCoordinate) string {
	c = normalizeProperScoreCoordinate(c)
	if !properScoreCoordinateValid(c) {
		return ""
	}
	b, _ := json.Marshal(struct {
		Version            int    `json:"version"`
		Platform           string `json:"platform"`
		Ticker             string `json:"ticker"`
		ForecastOriginSide string `json:"forecast_origin_side"`
		ForecastSignal     string `json:"forecast_signal"`
		ForecastSource     string `json:"forecast_source"`
		ForecastVersion    string `json:"forecast_version"`
		Route              string `json:"route"`
	}{1, c.Platform, c.Ticker, c.ForecastOriginSide, c.ForecastSignal,
		c.ForecastSource, c.ForecastVersion, c.Route})
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func ProperScoreCoordinateFromTrial(t ProperScoreTrial) ProperScoreCoordinate {
	return ProperScoreCoordinate{Platform: t.Platform, Ticker: t.Ticker,
		ForecastOriginSide: t.ForecastOriginSide, ForecastSignal: t.ForecastSignal,
		ForecastSource: t.ForecastSource, ForecastVersion: t.ForecastVersion, Route: t.Route}
}

func properScoreManifestHash(keys []string) string {
	b, _ := json.Marshal(struct {
		Version int      `json:"version"`
		Keys    []string `json:"coordinate_keys"`
	}{1, keys})
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

// RegisterProperScoreManifest freezes the expected coordinates in one transaction before any
// trials are attempted. An identical retry is idempotent; any later attempt to redefine a slot is
// rejected. This separation is deliberate: if a subsequent trial insert fails, the durable
// manifest remains and ProperScoreReport must classify the vector as incomplete.
func (s *Store) RegisterProperScoreManifest(ctx context.Context, m ProperScoreManifest) (string, bool, error) {
	if m.Created.IsZero() {
		m.Created = time.Now().UTC()
	}
	m.Slot = strings.TrimSpace(m.Slot)
	m.Transform = strings.ToLower(strings.TrimSpace(m.Transform))
	m.StrategyMode = strings.ToLower(strings.TrimSpace(m.StrategyMode))
	m.ForecastSource = strings.TrimSpace(m.ForecastSource)
	m.ForecastVersion = strings.TrimSpace(m.ForecastVersion)
	if m.Slot == "" || (m.Transform != "brier" && m.Transform != "log" && m.Transform != "spherical") ||
		(m.StrategyMode != "fundamental" && m.StrategyMode != "momentum") ||
		m.ForecastSource == "" || m.ForecastVersion == "" || len(m.Coordinates) == 0 {
		return "", false, fmt.Errorf("invalid proper-score manifest")
	}
	type frozenCoordinate struct {
		key string
		row ProperScoreCoordinate
	}
	coords := make([]frozenCoordinate, 0, len(m.Coordinates))
	seen := make(map[string]struct{}, len(m.Coordinates))
	for _, raw := range m.Coordinates {
		c := normalizeProperScoreCoordinate(raw)
		key := ProperScoreCoordinateKey(c)
		if key == "" || c.ForecastSource != m.ForecastSource || c.ForecastVersion != m.ForecastVersion {
			return "", false, fmt.Errorf("invalid proper-score manifest coordinate")
		}
		if _, duplicate := seen[key]; duplicate {
			return "", false, fmt.Errorf("duplicate proper-score manifest coordinate")
		}
		seen[key] = struct{}{}
		coords = append(coords, frozenCoordinate{key: key, row: c})
	}
	sort.Slice(coords, func(i, j int) bool { return coords[i].key < coords[j].key })
	keys := make([]string, len(coords))
	for i := range coords {
		keys[i] = coords[i].key
	}
	expectedHash := properScoreManifestHash(keys)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", false, err
	}
	defer tx.Rollback()
	var existingCount int
	var existingHash, existingSource, existingVersion string
	err = tx.QueryRowContext(ctx, `SELECT expected_coordinate_count,expected_coordinate_hash,
forecast_source,forecast_version FROM research_proper_score_manifests
WHERE slot=? AND transform=? AND strategy_mode=?`, m.Slot, m.Transform, m.StrategyMode).
		Scan(&existingCount, &existingHash, &existingSource, &existingVersion)
	if err == nil {
		if existingCount != len(coords) || existingHash != expectedHash ||
			existingSource != m.ForecastSource || existingVersion != m.ForecastVersion {
			return "", false, fmt.Errorf("proper-score manifest already frozen with different coordinates")
		}
		rows, qerr := tx.QueryContext(ctx, `SELECT coordinate_key
FROM research_proper_score_manifest_coordinates
WHERE slot=? AND transform=? AND strategy_mode=? ORDER BY coordinate_ordinal`,
			m.Slot, m.Transform, m.StrategyMode)
		if qerr != nil {
			return "", false, qerr
		}
		var durableKeys []string
		for rows.Next() {
			var key string
			if scanErr := rows.Scan(&key); scanErr != nil {
				_ = rows.Close()
				return "", false, scanErr
			}
			durableKeys = append(durableKeys, key)
		}
		if closeErr := rows.Close(); closeErr != nil {
			return "", false, closeErr
		}
		if len(durableKeys) != existingCount || properScoreManifestHash(durableKeys) != existingHash {
			return "", false, fmt.Errorf("proper-score manifest coordinate integrity failure")
		}
		if err := tx.Commit(); err != nil {
			return "", false, err
		}
		return expectedHash, false, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return "", false, err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO research_proper_score_manifests(
slot,transform,strategy_mode,created_ts,forecast_source,forecast_version,
expected_coordinate_count,expected_coordinate_hash,contract_version) VALUES(?,?,?,?,?,?,?,?,1)`,
		m.Slot, m.Transform, m.StrategyMode, m.Created.UTC().Format(time.RFC3339Nano),
		m.ForecastSource, m.ForecastVersion, len(coords), expectedHash); err != nil {
		return "", false, err
	}
	for ordinal, frozen := range coords {
		c := frozen.row
		if _, err = tx.ExecContext(ctx, `INSERT INTO research_proper_score_manifest_coordinates(
slot,transform,strategy_mode,coordinate_ordinal,coordinate_key,platform,ticker,
forecast_origin_side,forecast_signal,forecast_source,forecast_version,route)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`, m.Slot, m.Transform, m.StrategyMode, ordinal, frozen.key,
			c.Platform, c.Ticker, c.ForecastOriginSide, c.ForecastSignal,
			c.ForecastSource, c.ForecastVersion, c.Route); err != nil {
			return "", false, err
		}
	}
	if err := tx.Commit(); err != nil {
		return "", false, err
	}
	return expectedHash, true, nil
}

func validFinite(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }

func (s *Store) InsertProperScoreTrial(ctx context.Context, t ProperScoreTrial) (bool, error) {
	if t.Observed.IsZero() {
		t.Observed = time.Now().UTC()
	}
	if t.Slot == "" {
		t.Slot = researchSlot(t.Observed, time.Hour)
	}
	t.Transform = strings.ToLower(strings.TrimSpace(t.Transform))
	t.Platform = strings.ToLower(strings.TrimSpace(t.Platform))
	t.Route = strings.ToLower(strings.TrimSpace(t.Route))
	t.StrategyMode = strings.ToLower(strings.TrimSpace(t.StrategyMode))
	t.FillStatus = strings.ToLower(strings.TrimSpace(t.FillStatus))
	t.ForecastOriginSide = strings.ToUpper(strings.TrimSpace(t.ForecastOriginSide))
	t.SelectedSide = strings.ToUpper(strings.TrimSpace(t.SelectedSide))
	if t.StrategyMode == "" {
		t.StrategyMode = "fundamental"
	}
	if t.FillStatus == "" {
		t.FillStatus = "counterfactual"
	}
	if t.Rescale == 0 {
		t.Rescale = 1
	}
	if t.VectorDim == 0 {
		t.VectorDim = 2
	}
	if t.System != "proper-score-"+t.Transform || (t.Transform != "brier" && t.Transform != "log" && t.Transform != "spherical") ||
		(t.Platform != "kalshi" && t.Platform != "polyus") || strings.TrimSpace(t.Ticker) == "" ||
		(t.Route != "taker" && t.Route != "maker") ||
		(t.StrategyMode != "fundamental" && t.StrategyMode != "momentum") || strings.TrimSpace(t.Cohort) == "" ||
		(t.FillStatus != "counterfactual" && t.FillStatus != "filled" && t.FillStatus != "partial" &&
			t.FillStatus != "cancelled" && t.FillStatus != "legacy_unknown") ||
		(t.ForecastOriginSide != "YES" && t.ForecastOriginSide != "NO") ||
		strings.TrimSpace(t.ForecastSource) == "" || strings.TrimSpace(t.ForecastVersion) == "" ||
		strings.TrimSpace(t.BookSource) == "" ||
		!validFinite(t.ForecastYes) || t.ForecastYes < 0 || t.ForecastYes > 1 ||
		!validFinite(t.YesBid) || !validFinite(t.YesAsk) || t.YesBid <= 0 || t.YesAsk <= t.YesBid || t.YesAsk >= 1 ||
		t.YesBidDepth <= 0 || t.YesAskDepth <= 0 || t.NoBidDepth <= 0 || t.NoAskDepth <= 0 ||
		math.Abs(t.NoBid-(1-t.YesAsk)) > 1e-9 || math.Abs(t.NoAsk-(1-t.YesBid)) > 1e-9 ||
		t.QuoteAge < 0 || !validFinite(t.QuoteAge) || !validFinite(t.QYes) || t.QYes <= 0 || t.QYes >= 1 ||
		!validFinite(t.RawYes) || !validFinite(t.RawNo) || !validFinite(t.CanonicalQty) ||
		!validFinite(t.ExecutableQty) || !validFinite(t.ExactFeeTotal) || !validFinite(t.ExpectedNet) ||
		!validFinite(t.DecisionLatencyMS) || t.DecisionLatencyMS < 0 || !validFinite(t.ConstantShift) ||
		!validFinite(t.Rescale) || t.Rescale <= 0 || !validFinite(t.NormalizedWeight) ||
		!validFinite(t.RequestedQty) || t.RequestedQty < 0 || !validFinite(t.TickSize) || !validFinite(t.LotSize) ||
		!validFinite(t.IntegratedCost) || t.IntegratedCost < 0 || !validFinite(t.SpotCost) || t.SpotCost < 0 ||
		!validFinite(t.LiquidityLoss) || t.LiquidityLoss < -1e-9 || !validFinite(t.PriorActualPosition) ||
		!validFinite(t.TargetPosition) || !validFinite(t.ActualFilledDelta) || !validFinite(t.CancelledDelta) ||
		!validFinite(t.PostActualPosition) || t.VectorDim < 2 || t.ResearchObservationID < 0 {
		return false, fmt.Errorf("invalid proper-score observation")
	}
	if t.SelectedSide == "" {
		if t.ExecutableQty != 0 || t.EntryPrice != 0 || t.ExactFeeTotal != 0 || strings.TrimSpace(t.AbstainReason) == "" {
			return false, fmt.Errorf("invalid proper-score abstention")
		}
	} else if (t.SelectedSide != "YES" && t.SelectedSide != "NO") || t.ExecutableQty <= 0 ||
		t.EntryPrice <= 0 || t.EntryPrice >= 1 || t.EntryDepth < t.ExecutableQty ||
		t.ExactFeeTotal < 0 || strings.TrimSpace(t.FeeSource) == "" || t.ExpectedNet <= 0 ||
		t.TickSize <= 0 || t.LotSize <= 0 || strings.TrimSpace(t.SourceClockID) == "" ||
		t.RequestedQty < t.ExecutableQty || math.Abs(t.IntegratedCost-t.ExecutableQty*t.EntryPrice) > 1e-7 ||
		t.LiquidityLoss < -1e-9 {
		return false, fmt.Errorf("invalid proper-score route")
	}
	if t.ActualFill && t.FillStatus != "filled" && t.FillStatus != "partial" {
		return false, fmt.Errorf("actual proper-score fill has non-fill status")
	}
	if !t.ActualFill && math.Abs(t.ActualFilledDelta) > 1e-12 {
		return false, fmt.Errorf("counterfactual proper-score row moved actual position")
	}
	rawVector, err := json.Marshal(t.RawVector)
	if err != nil || len(rawVector) == 0 || string(rawVector) == "null" {
		rawVector = []byte("[]")
	}
	depthCurve, err := json.Marshal(t.DepthCurve)
	if err != nil || len(depthCurve) == 0 || string(depthCurve) == "null" {
		depthCurve = []byte("[]")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	coordinate := normalizeProperScoreCoordinate(ProperScoreCoordinateFromTrial(t))
	coordinateKey := ProperScoreCoordinateKey(coordinate)
	var manifestHash string
	err = tx.QueryRowContext(ctx, `SELECT m.expected_coordinate_hash
FROM research_proper_score_manifests m
JOIN research_proper_score_manifest_coordinates c
 ON c.slot=m.slot AND c.transform=m.transform AND c.strategy_mode=m.strategy_mode
WHERE m.slot=? AND m.transform=? AND m.strategy_mode=? AND c.coordinate_key=?
 AND c.platform=? AND c.ticker=? AND c.forecast_origin_side=? AND c.forecast_signal=?
 AND c.forecast_source=? AND c.forecast_version=? AND c.route=?`,
		t.Slot, t.Transform, t.StrategyMode, coordinateKey, coordinate.Platform, coordinate.Ticker,
		coordinate.ForecastOriginSide, coordinate.ForecastSignal, coordinate.ForecastSource,
		coordinate.ForecastVersion, coordinate.Route).Scan(&manifestHash)
	if errors.Is(err, sql.ErrNoRows) {
		return false, fmt.Errorf("proper-score trial is not in a frozen expected-coordinate manifest")
	}
	if err != nil {
		return false, err
	}
	if len(manifestHash) != 64 {
		return false, fmt.Errorf("proper-score manifest hash integrity failure")
	}
	r, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO research_proper_score_trials(
observed_ts,slot,system_name,transform,strategy_mode,cohort,platform,ticker,title,category,resolve_hours,forecast_yes,
forecast_origin_side,forecast_signal,forecast_source,forecast_version,model_backend,calibration,
generated_at,route,yes_bid,yes_ask,yes_bid_depth,yes_ask_depth,no_bid,no_ask,no_bid_depth,no_ask_depth,
book_source,source_clock_id,quote_age_s,decision_latency_ms,q_yes,raw_yes,raw_no,raw_vector_json,
vector_dim,constant_shift,rescale,normalized_weight,canonical_qty,requested_qty,executable_qty,
selected_side,tick_size,lot_size,depth_curve_json,entry_price,entry_depth,integrated_cost,spot_cost,
liquidity_loss,exact_fee_total,fee_source,expected_net,abstain_reason,prior_actual_position,target_position,
actual_filled_delta,cancelled_delta,post_actual_position,fill_status,actual_fill,research_observation_id)
VALUES(
?,?,?,?,?,?,?,?,
?,?,?,?,?,?,?,?,
?,?,?,?,?,?,?,?,
?,?,?,?,?,?,?,?,
?,?,?,?,?,?,?,?,
?,?,?,?,?,?,?,?,
?,?,?,?,?,?,?,?,
?,?,?,?,?,?,?,?)`,
		t.Observed.UTC().Format(time.RFC3339Nano), t.Slot, t.System, t.Transform, t.StrategyMode, t.Cohort,
		t.Platform, t.Ticker, t.Title, t.Category, t.ResolveHours, t.ForecastYes, t.ForecastOriginSide, t.ForecastSignal,
		t.ForecastSource, t.ForecastVersion, t.ModelBackend, t.Calibration, t.GeneratedAt,
		t.Route, t.YesBid, t.YesAsk, t.YesBidDepth, t.YesAskDepth, t.NoBid, t.NoAsk,
		t.NoBidDepth, t.NoAskDepth, t.BookSource, t.SourceClockID, t.QuoteAge, t.DecisionLatencyMS,
		t.QYes, t.RawYes, t.RawNo, string(rawVector), t.VectorDim, t.ConstantShift, t.Rescale,
		t.NormalizedWeight, t.CanonicalQty, t.RequestedQty, t.ExecutableQty, t.SelectedSide,
		t.TickSize, t.LotSize, string(depthCurve), t.EntryPrice, t.EntryDepth, t.IntegratedCost,
		t.SpotCost, t.LiquidityLoss, t.ExactFeeTotal, t.FeeSource, t.ExpectedNet, t.AbstainReason,
		t.PriorActualPosition, t.TargetPosition, t.ActualFilledDelta, t.CancelledDelta,
		t.PostActualPosition, t.FillStatus, boolInt(t.ActualFill), t.ResearchObservationID)
	if err != nil {
		return false, err
	}
	n, err := r.RowsAffected()
	if err != nil {
		return false, err
	}
	// Every proper-score row also enters the common route ledger. A positive forecast point
	// estimate remains BLOCKED here: only the separate sealed-inference promotion bridge can turn
	// the corresponding immutable system observation into Paper authority.
	oppID := ResearchRouteStableID(t.Slot, t.Platform, t.Ticker, t.ForecastOriginSide,
		t.ForecastSignal, t.ForecastSource, t.ForecastVersion)
	routeID := t.Transform + "-" + t.StrategyMode + "-taker"
	route := ResearchRouteOpportunity{
		OpportunityID: oppID, RouteID: routeID, Observed: t.Observed, DecisionAt: t.Observed,
		ExperimentID: "proper-score-executor", ExperimentVersion: 1, SystemName: t.System,
		IdentityStatus: "unverified", Venue: t.Platform, Ticker: t.Ticker, Side: "NONE",
		Route: "taker", Action: "abstain", QuoteSource: t.BookSource,
		QuoteSequence: t.SourceClockID, QuoteAgeSeconds: t.QuoteAge, QuoteAgeKnown: true,
		DecisionLatencyMS: t.DecisionLatencyMS, LatencyKnown: true, TickSize: t.TickSize,
		TickKnown: t.TickSize > 0, FeeAuthority: "not_applicable_no_order",
		Decision: "abstain", DecisionReason: t.AbstainReason,
		AlternativeGroup: oppID, EvidenceJSON: "{}",
	}
	evidence, _ := json.Marshal(map[string]any{
		"forecast_source": t.ForecastSource, "forecast_version": t.ForecastVersion,
		"forecast_yes": t.ForecastYes, "point_expected_net": t.ExpectedNet,
		"yes_bid": t.YesBid, "yes_ask": t.YesAsk, "yes_bid_depth": t.YesBidDepth,
		"yes_ask_depth": t.YesAskDepth, "no_bid": t.NoBid, "no_ask": t.NoAsk,
		"no_bid_depth": t.NoBidDepth, "no_ask_depth": t.NoAskDepth,
		"strategy_mode": t.StrategyMode, "cohort": t.Cohort, "raw_vector": t.RawVector,
		"constant_shift": t.ConstantShift, "rescale": t.Rescale,
		"normalized_weight": t.NormalizedWeight, "tick_size": t.TickSize,
		"lot_size": t.LotSize, "source_clock_id": t.SourceClockID,
		"integrated_cost": t.IntegratedCost, "spot_cost": t.SpotCost,
		"liquidity_loss": t.LiquidityLoss, "research_observation_id": t.ResearchObservationID,
	})
	route.EvidenceJSON = string(evidence)
	if t.SelectedSide != "" {
		route.Side, route.Action = t.SelectedSide, "buy"
		route.ExecutablePrice, route.ExecutableDepth, route.RequestedQty = t.EntryPrice, t.EntryDepth, t.RequestedQty
		route.DepthKnown = true
		route.FeeAmount, route.FeeAuthority = t.ExactFeeTotal, t.FeeSource
		route.FeeKnown = true
		route.ExpectedPayoutLow, route.ExpectedPayoutHigh = 0, t.ExecutableQty
		route.ExpectedNetLow = -(t.ExecutableQty*t.EntryPrice + t.ExactFeeTotal)
		route.ExpectedNetHigh = t.ExecutableQty*(1-t.EntryPrice) - t.ExactFeeTotal
		route.PartialFillWorst = route.ExpectedNetLow
		route.CapitalSeconds = math.Max(0, t.ResolveHours*3600)
		route.Decision = "blocked"
		route.DecisionReason = "positive point estimate only; untouched event/day lower bound and tick receipt pending"
	}
	routeInserted, err := insertResearchRouteOpportunityWith(ctx, tx, route)
	if err != nil {
		return false, fmt.Errorf("proper-score route ledger: %w", err)
	}
	if routeInserted {
		_, err = tx.ExecContext(ctx, `INSERT INTO research_route_events(
opportunity_id,route_id,observed_ts,event_type,outcome_status,reason,evidence_json)
VALUES(?,?,?,?,?,?,?)`, oppID, routeID, t.Observed.UTC().Format(time.RFC3339Nano), "refused",
			"research_only", route.DecisionReason, `{"order_sent":false}`)
		if err != nil {
			return false, err
		}
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return n > 0, nil
}

// ProperScoreActualPosition returns only a position produced by a durable actual fill receipt.
// Counterfactual rows and canceled quantities can never move the momentum rebalancer.
func (s *Store) ProperScoreActualPosition(ctx context.Context, mode, transform, platform, ticker,
	forecastSource, forecastVersion string) (float64, error) {
	var position float64
	err := s.db.QueryRowContext(ctx, `SELECT post_actual_position
FROM research_proper_score_trials WHERE actual_fill=1 AND strategy_mode=? AND transform=?
 AND platform=? AND ticker=? AND forecast_source=? AND forecast_version=?
ORDER BY id DESC LIMIT 1`, strings.ToLower(strings.TrimSpace(mode)),
		strings.ToLower(strings.TrimSpace(transform)), strings.ToLower(strings.TrimSpace(platform)),
		strings.TrimSpace(ticker), strings.TrimSpace(forecastSource), strings.TrimSpace(forecastVersion)).Scan(&position)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return position, err
}

func properOutcomeScore(transform string, p, y float64) (float64, bool) {
	t, err := properbetting.ParseTransform(transform)
	if err != nil || !validFinite(p) || p < 0 || p > 1 || (y != 0 && y != 1) {
		return 0, false
	}
	outcome := 1
	if y == 1 {
		outcome = 0
	}
	score, err := properbetting.Score(t, []float64{p, 1 - p}, outcome)
	return score, err == nil && validFinite(score)
}

func (s *Store) mirrorProperScorePayoffs(ctx context.Context, limit int) error {
	if limit <= 0 {
		limit = 4000
	}
	rows, err := s.db.QueryContext(ctx, `SELECT p.research_observation_id,p.platform,p.ticker,
p.selected_side,p.executable_qty,p.settle_yes,p.realized_net,p.closed_ts
FROM research_proper_score_trials p
WHERE p.settled=1 AND p.research_observation_id>0 AND p.realized_net IS NOT NULL
 AND NOT EXISTS(SELECT 1 FROM research_system_payoff_updates u
  WHERE u.observation_id=p.research_observation_id)
ORDER BY p.id LIMIT ?`, limit)
	if err != nil {
		return err
	}
	type pending struct {
		observationID              int64
		platform, ticker, side, at string
		qty, settle, realized      float64
	}
	var pendingRows []pending
	for rows.Next() {
		var p pending
		if err := rows.Scan(&p.observationID, &p.platform, &p.ticker, &p.side, &p.qty,
			&p.settle, &p.realized, &p.at); err != nil {
			_ = rows.Close()
			return err
		}
		pendingRows = append(pendingRows, p)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, p := range pendingRows {
		payout := p.qty * p.settle
		if p.side == "NO" {
			payout = p.qty * (1 - p.settle)
		}
		h := sha256.Sum256([]byte(fmt.Sprintf("proper-score|%s|%s|%.12f|%s", p.platform, p.ticker, p.settle, p.at)))
		realized := p.realized
		if _, err := s.AppendResearchPayoffUpdate(ctx, ResearchPayoffUpdate{ObservationID: p.observationID,
			Observed: sourceObservedTime(p.at, time.Now()), Status: "settled",
			PayoutLower: payout, PayoutUpper: payout, RealizedNet: &realized,
			SourceArtifact: "signal_log:canonical-binary-settlement:" + p.platform + ":" + p.ticker,
			SourceHash:     hex.EncodeToString(h[:]), Reason: "proper-score exact venue binary settlement"}); err != nil {
			return err
		}
	}
	return nil
}

// ResolveProperScoreTrials fans canonical venue settlements into each transform/persona row.
// The bounded open-key driver is critical: signal_log has millions of rows, so a whole-ledger
// GROUP BY here would recreate the historical WAL/CPU stall. Each distinct open ticker performs
// one idx_signal_ticker_resolved lookup. Proper-score theory is categorical; fractional scalar
// settlements are retained as explicitly unsupported rather than mis-scored as soft Bernoulli labels.
func (s *Store) ResolveProperScoreTrials(ctx context.Context, limit int) (int, error) {
	if limit <= 0 {
		limit = 4000
	}
	keyRows, err := s.db.QueryContext(ctx, `SELECT p.platform,p.ticker,MIN(p.id) first_id
FROM research_proper_score_trials p WHERE p.settled=0 AND (
 EXISTS(SELECT 1 FROM venue_settlements v WHERE v.platform=p.platform AND v.ticker=p.ticker)
 OR EXISTS(SELECT 1 FROM signal_log s INDEXED BY idx_signal_exact_settlement
   WHERE s.platform=p.platform AND s.ticker=p.ticker AND s.resolved=1
     AND s.settle_val>=0 AND s.settle_val<=1)
) GROUP BY p.platform,p.ticker ORDER BY first_id LIMIT 250`)
	if err != nil {
		return 0, err
	}
	type openKey struct{ platform, ticker string }
	var keys []openKey
	for keyRows.Next() {
		var k openKey
		var first int64
		if err := keyRows.Scan(&k.platform, &k.ticker, &first); err != nil {
			_ = keyRows.Close()
			return 0, err
		}
		keys = append(keys, k)
	}
	if err := keyRows.Close(); err != nil {
		return 0, err
	}
	type grade struct {
		id, observationID              int64
		y, forecast, book, delta, qty  float64
		capitalSeconds                 float64
		realized                       sql.NullFloat64
		closed, opportunityID, routeID string
		platform, ticker, side, reason string
		unsupported                    bool
	}
	var grades []grade
	for _, key := range keys {
		var settle float64
		var closed string
		err := s.db.QueryRowContext(ctx, `SELECT yes_value,resolved_at FROM venue_settlements
WHERE platform=? AND ticker=?`, key.platform, key.ticker).Scan(&settle, &closed)
		if err == sql.ErrNoRows {
			err = s.db.QueryRowContext(ctx, `SELECT settle_val,resolved_at FROM signal_log INDEXED BY idx_signal_ticker_resolved
WHERE ticker=? AND resolved=1 AND platform=? AND settle_val IS NOT NULL AND resolved_at IS NOT NULL AND resolved_at!=''
ORDER BY ts DESC LIMIT 1`, key.ticker, key.platform).Scan(&settle, &closed)
		}
		if err == sql.ErrNoRows {
			continue
		}
		if err != nil {
			return 0, err
		}
		remaining := limit - len(grades)
		if remaining <= 0 {
			break
		}
		rows, err := s.db.QueryContext(ctx, `SELECT id,transform,strategy_mode,slot,forecast_origin_side,forecast_signal,
forecast_source,forecast_version,observed_ts,forecast_yes,q_yes,selected_side,
executable_qty,entry_price,exact_fee_total,research_observation_id FROM research_proper_score_trials
WHERE settled=0 AND platform=? AND ticker=? ORDER BY id LIMIT ?`, key.platform, key.ticker, remaining)
		if err != nil {
			return 0, err
		}
		for rows.Next() {
			var g grade
			var transform, mode, slot, originSide, signal, source, version, observed, side string
			var p, q, qty, entry, fee float64
			if err := rows.Scan(&g.id, &transform, &mode, &slot, &originSide, &signal, &source, &version,
				&observed, &p, &q, &side, &qty, &entry, &fee, &g.observationID); err != nil {
				_ = rows.Close()
				return 0, err
			}
			g.y, g.closed, g.qty, g.platform, g.ticker, g.side = settle, closed, qty, key.platform, key.ticker, side
			g.opportunityID = ResearchRouteStableID(slot, key.platform, key.ticker, originSide, signal, source, version)
			g.routeID = transform + "-" + mode + "-taker"
			if ot, oerr := time.Parse(time.RFC3339Nano, observed); oerr == nil {
				if ct, cerr := time.Parse(time.RFC3339Nano, closed); cerr == nil && !ct.Before(ot) {
					g.capitalSeconds = ct.Sub(ot).Seconds()
				}
			}
			if !validFinite(settle) || settle < 0 || settle > 1 || (math.Abs(settle) > 1e-9 && math.Abs(settle-1) > 1e-9) {
				g.unsupported, g.reason = true, "non_binary_scalar_settlement"
				grades = append(grades, g)
				continue
			}
			if settle < .5 {
				g.y = 0
			} else {
				g.y = 1
			}
			var forecastOK, bookOK bool
			g.forecast, forecastOK = properOutcomeScore(transform, p, g.y)
			g.book, bookOK = properOutcomeScore(transform, q, g.y)
			if !forecastOK || !bookOK {
				g.unsupported, g.reason = true, "proper_score_undefined_for_probability_boundary"
				grades = append(grades, g)
				continue
			}
			g.delta = g.forecast - g.book
			if side == "YES" {
				g.realized = sql.NullFloat64{Float64: qty*(g.y-entry) - fee, Valid: true}
			} else if side == "NO" {
				g.realized = sql.NullFloat64{Float64: qty*((1-g.y)-entry) - fee, Valid: true}
			}
			grades = append(grades, g)
		}
		if err := rows.Close(); err != nil {
			return 0, err
		}
	}
	if len(grades) == 0 {
		return 0, s.mirrorProperScorePayoffs(ctx, limit)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	n := 0
	for _, g := range grades {
		if g.unsupported {
			r, err := tx.ExecContext(ctx, `UPDATE research_proper_score_trials SET settled=-1,
grade_status='unsupported',grade_reason=?,settle_yes=?,closed_ts=?
WHERE id=? AND settled=0`, g.reason, g.y, g.closed, g.id)
			if err != nil {
				return 0, err
			}
			if k, _ := r.RowsAffected(); k > 0 {
				n++
				_, err = tx.ExecContext(ctx, `INSERT INTO research_route_events(
opportunity_id,route_id,observed_ts,event_type,quantity,payout,capital_seconds,outcome_status,reason,evidence_json)
SELECT ?,?,?, 'grade',?,?,?,'unsupported',?,'{"source":"proper_score"}'
WHERE EXISTS(SELECT 1 FROM research_route_opportunities WHERE opportunity_id=? AND route_id=?)`,
					g.opportunityID, g.routeID, g.closed, g.qty, g.y, g.capitalSeconds, g.reason, g.opportunityID, g.routeID)
				if err != nil {
					return 0, err
				}
			}
			continue
		}
		var realized any
		if g.realized.Valid {
			realized = g.realized.Float64
		}
		r, err := tx.ExecContext(ctx, `UPDATE research_proper_score_trials SET settled=1,
grade_status='graded',grade_reason='',settle_yes=?,forecast_score=?,book_score=?,score_delta=?,realized_net=?,closed_ts=? WHERE id=? AND settled=0`,
			g.y, g.forecast, g.book, g.delta, realized, g.closed, g.id)
		if err != nil {
			return 0, err
		}
		if k, _ := r.RowsAffected(); k > 0 {
			n++
			_, err = tx.ExecContext(ctx, `INSERT INTO research_route_events(
opportunity_id,route_id,observed_ts,event_type,quantity,payout,realized_net,capital_seconds,
outcome_status,reason,evidence_json)
SELECT ?,?,?, 'grade',?,?,?,?, 'graded','binary settlement','{"source":"proper_score"}'
WHERE EXISTS(SELECT 1 FROM research_route_opportunities WHERE opportunity_id=? AND route_id=?)`,
				g.opportunityID, g.routeID, g.closed, g.qty, g.y, realized, g.capitalSeconds,
				g.opportunityID, g.routeID)
			if err != nil {
				return 0, err
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return n, s.mirrorProperScorePayoffs(ctx, limit)
}

// ProperScoreBriefReport computes only the three economic headline cells. The full diagnostic
// report below also scans simulated positions, comparator observations, modes, manifest defects,
// and legacy cohorts; using it for a phone briefing made an otherwise compact refresh exceed its
// 25-second budget on the production database. This query preserves the same immutable complete-
// vector contract while reading the manifest/trial ledger once and never touching comparators.
func (s *Store) ProperScoreBriefReport(ctx context.Context) (map[string]any, error) {
	transforms := map[string]any{}
	for _, name := range []string{"brier", "log", "spherical"} {
		transforms[name] = map[string]any{
			"normalized_executable_completed_slots":        0,
			"normalized_executable_unique_settled_markets": 0,
			"normalized_executable_settled_l1":             float64(0),
			"l1_weighted_realized_net":                     float64(0),
		}
	}
	rows, err := s.db.QueryContext(ctx, `WITH exact_slots AS (
 SELECT m.slot,m.transform,m.strategy_mode,m.created_ts,m.expected_coordinate_count
 FROM research_proper_score_manifests m
 WHERE LENGTH(m.expected_coordinate_hash)=64
  AND (SELECT COUNT(*) FROM research_proper_score_manifest_coordinates c
       WHERE c.slot=m.slot AND c.transform=m.transform AND c.strategy_mode=m.strategy_mode)
      =m.expected_coordinate_count
  AND (SELECT COUNT(*) FROM research_proper_score_trials t
       WHERE t.slot=m.slot AND t.transform=m.transform AND t.strategy_mode=m.strategy_mode
        AND t.observed_ts>=m.created_ts)=m.expected_coordinate_count
  AND NOT EXISTS (
   SELECT 1 FROM research_proper_score_manifest_coordinates c
   LEFT JOIN research_proper_score_trials t
    ON t.slot=c.slot AND t.transform=c.transform AND t.strategy_mode=c.strategy_mode
   AND t.platform=c.platform AND t.ticker=c.ticker
   AND t.forecast_origin_side=c.forecast_origin_side AND t.forecast_signal=c.forecast_signal
   AND t.forecast_source=c.forecast_source AND t.forecast_version=c.forecast_version
   AND t.route=c.route AND t.observed_ts>=m.created_ts
   WHERE c.slot=m.slot AND c.transform=m.transform AND c.strategy_mode=m.strategy_mode
    AND t.id IS NULL)
  AND NOT EXISTS (
   SELECT 1 FROM research_proper_score_trials t
   LEFT JOIN research_proper_score_manifest_coordinates c
    ON c.slot=t.slot AND c.transform=t.transform AND c.strategy_mode=t.strategy_mode
   AND c.platform=t.platform AND c.ticker=t.ticker
   AND c.forecast_origin_side=t.forecast_origin_side AND c.forecast_signal=t.forecast_signal
   AND c.forecast_source=t.forecast_source AND c.forecast_version=t.forecast_version
   AND c.route=t.route
   WHERE t.slot=m.slot AND t.transform=m.transform AND t.strategy_mode=m.strategy_mode
    AND t.observed_ts>=m.created_ts AND c.coordinate_key IS NULL)
  AND NOT EXISTS (
   SELECT 1 FROM research_proper_score_trials t
   WHERE t.slot=m.slot AND t.transform=m.transform AND t.strategy_mode=m.strategy_mode
    AND t.observed_ts>=m.created_ts AND t.settled!=1)
  AND EXISTS (
   SELECT 1 FROM research_proper_score_trials t
   WHERE t.slot=m.slot AND t.transform=m.transform AND t.strategy_mode=m.strategy_mode
    AND t.observed_ts>=m.created_ts AND t.cohort LIKE 'proper-score-v2|%'
    AND t.selected_side!='' AND ABS(t.normalized_weight)>1e-12
    AND t.executable_qty>0 AND t.tick_size>0 AND t.lot_size>0 AND t.source_clock_id!='')
  AND NOT EXISTS (
   SELECT 1 FROM research_proper_score_trials t
   WHERE t.slot=m.slot AND t.transform=m.transform AND t.strategy_mode=m.strategy_mode
    AND t.observed_ts>=m.created_ts AND t.cohort LIKE 'proper-score-v2|%'
    AND t.selected_side!='' AND ABS(t.normalized_weight)>1e-12
    AND t.executable_qty>0 AND t.tick_size>0 AND t.lot_size>0 AND t.source_clock_id!=''
    AND t.realized_net IS NULL)
), slot_metrics AS (
 SELECT e.slot,e.transform,e.strategy_mode,
  SUM(t.realized_net*t.normalized_weight) weighted_realized,
  SUM(ABS(t.normalized_weight)) settled_l1
 FROM exact_slots e JOIN research_proper_score_trials t
  ON t.slot=e.slot AND t.transform=e.transform AND t.strategy_mode=e.strategy_mode
  AND t.observed_ts>=e.created_ts
 WHERE t.settled=1 AND t.realized_net IS NOT NULL AND t.cohort LIKE 'proper-score-v2|%'
  AND t.selected_side!='' AND ABS(t.normalized_weight)>1e-12
  AND t.executable_qty>0 AND t.tick_size>0 AND t.lot_size>0 AND t.source_clock_id!=''
 GROUP BY e.slot,e.transform,e.strategy_mode
), market_counts AS (
 SELECT transform,COUNT(*) markets FROM (
  SELECT DISTINCT e.transform,t.platform,t.ticker
  FROM exact_slots e JOIN research_proper_score_trials t
   ON t.slot=e.slot AND t.transform=e.transform AND t.strategy_mode=e.strategy_mode
   AND t.observed_ts>=e.created_ts
  WHERE t.settled=1 AND t.realized_net IS NOT NULL AND t.cohort LIKE 'proper-score-v2|%'
   AND t.selected_side!='' AND ABS(t.normalized_weight)>1e-12
   AND t.executable_qty>0 AND t.tick_size>0 AND t.lot_size>0 AND t.source_clock_id!=''
 ) GROUP BY transform
)
SELECT s.transform,COUNT(*),COALESCE(MAX(m.markets),0),
 COALESCE(SUM(s.settled_l1),0),COALESCE(SUM(s.weighted_realized),0)
FROM slot_metrics s LEFT JOIN market_counts m ON m.transform=s.transform
GROUP BY s.transform`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		var batches, markets int
		var l1, realized float64
		if err := rows.Scan(&name, &batches, &markets, &l1, &realized); err != nil {
			return nil, err
		}
		if _, ok := transforms[name]; ok {
			transforms[name] = map[string]any{
				"normalized_executable_completed_slots":        batches,
				"normalized_executable_unique_settled_markets": markets,
				"normalized_executable_settled_l1":             l1,
				"l1_weighted_realized_net":                     realized,
			}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	paperPortfolios, err := s.ProperScorePaperPortfolioReport(ctx)
	if err != nil {
		return nil, err
	}
	return map[string]any{"transforms": transforms, "paper_portfolios": paperPortfolios}, nil
}

func (s *Store) ProperScoreReport(ctx context.Context) (map[string]any, error) {
	type summary struct {
		Rows, Actions, Abstains, Settled, SettledActions, Unsupported    int
		NormalizedExecutableRows, NormalizedExecutableSettled            int
		NormalizedExecutableCompletedSlots                               int
		NormalizedExecutableUniqueSettledMarkets                         int
		LegacyUnnormalizedRows, LegacyUnnormalizedSettled                int
		ManifestSlots, ManifestHashProvenSlots, ManifestCompletedSlots   int
		ManifestIncompleteSlots, ManifestOpenSlots, ManifestPartialSlots int
		ManifestUnsupportedSlots, UnmanifestedSlots                      int
		ManifestMissingCoordinates, ManifestExtraCoordinates             int
		Expected, Realized, WeightedExpected, WeightedRealized           sql.NullFloat64
		NormalizedSettledL1, LegacyUnnormalizedRealized, ScoreDelta      sql.NullFloat64
	}
	read := func(filter string, args ...any) (summary, error) {
		var v summary
		queryArgs := append(append([]any{}, args...), args...)
		err := s.db.QueryRowContext(ctx, `WITH scoped AS (
 SELECT * FROM research_proper_score_trials t WHERE 1=1`+filter+`
), scoped_manifests AS (
 SELECT * FROM research_proper_score_manifests m WHERE 1=1`+filter+`
), expected_state AS (
	SELECT m.slot,m.transform,m.strategy_mode,m.created_ts,m.expected_coordinate_count,m.expected_coordinate_hash,
  COUNT(c.coordinate_key) coordinate_rows,COUNT(t.id) matched_rows,
  COALESCE(SUM(t.settled=0),0) open_rows,COALESCE(SUM(t.settled=1),0) settled_rows,
  COALESCE(SUM(t.settled=-1),0) unsupported_rows,
  COALESCE(SUM(CASE WHEN c.coordinate_key IS NOT NULL AND t.id IS NULL THEN 1 ELSE 0 END),0) missing_rows
 FROM scoped_manifests m
 LEFT JOIN research_proper_score_manifest_coordinates c
  ON c.slot=m.slot AND c.transform=m.transform AND c.strategy_mode=m.strategy_mode
	LEFT JOIN scoped t ON t.slot=c.slot AND t.transform=c.transform AND t.strategy_mode=c.strategy_mode
  AND t.platform=c.platform AND t.ticker=c.ticker
  AND t.forecast_origin_side=c.forecast_origin_side AND t.forecast_signal=c.forecast_signal
  AND t.forecast_source=c.forecast_source AND t.forecast_version=c.forecast_version AND t.route=c.route
	 AND t.observed_ts>=m.created_ts
	GROUP BY m.slot,m.transform,m.strategy_mode,m.created_ts,m.expected_coordinate_count,m.expected_coordinate_hash
), actual_state AS (
 SELECT m.slot,m.transform,m.strategy_mode,COUNT(t.id) actual_rows,
  COALESCE(SUM(CASE WHEN t.id IS NOT NULL AND c.coordinate_key IS NULL THEN 1 ELSE 0 END),0) extra_rows
 FROM scoped_manifests m
	LEFT JOIN scoped t ON t.slot=m.slot AND t.transform=m.transform AND t.strategy_mode=m.strategy_mode
	 AND t.observed_ts>=m.created_ts
 LEFT JOIN research_proper_score_manifest_coordinates c
  ON c.slot=t.slot AND c.transform=t.transform AND c.strategy_mode=t.strategy_mode
  AND c.platform=t.platform AND c.ticker=t.ticker
  AND c.forecast_origin_side=t.forecast_origin_side AND c.forecast_signal=t.forecast_signal
  AND c.forecast_source=t.forecast_source AND c.forecast_version=t.forecast_version AND c.route=t.route
 GROUP BY m.slot,m.transform,m.strategy_mode
), manifest_state AS (
 SELECT e.*,a.actual_rows,a.extra_rows,
  (e.coordinate_rows=e.expected_coordinate_count AND a.actual_rows=e.expected_coordinate_count
   AND e.matched_rows=e.expected_coordinate_count AND e.missing_rows=0 AND a.extra_rows=0
   AND length(e.expected_coordinate_hash)=64) exact_coordinates
 FROM expected_state e JOIN actual_state a USING(slot,transform,strategy_mode)
), terminal_slots AS (
	SELECT slot,transform,strategy_mode,created_ts FROM manifest_state
 WHERE exact_coordinates AND settled_rows=expected_coordinate_count AND open_rows=0 AND unsupported_rows=0
), complete_slots AS (
	SELECT x.slot,x.transform,x.strategy_mode,x.created_ts FROM terminal_slots x
 WHERE EXISTS(SELECT 1 FROM scoped t WHERE t.slot=x.slot AND t.transform=x.transform
  AND t.strategy_mode=x.strategy_mode AND t.cohort LIKE 'proper-score-v2|%' AND t.selected_side!=''
  AND ABS(t.normalized_weight)>1e-12 AND t.executable_qty>0 AND t.tick_size>0 AND t.lot_size>0
  AND t.source_clock_id!='')
 AND NOT EXISTS(SELECT 1 FROM scoped t WHERE t.slot=x.slot AND t.transform=x.transform
  AND t.strategy_mode=x.strategy_mode AND t.cohort LIKE 'proper-score-v2|%' AND t.selected_side!=''
  AND ABS(t.normalized_weight)>1e-12 AND t.executable_qty>0 AND t.tick_size>0 AND t.lot_size>0
  AND t.source_clock_id!='' AND (t.settled!=1 OR t.realized_net IS NULL))
)
SELECT COUNT(*),COALESCE(SUM(selected_side!=''),0),
COALESCE(SUM(selected_side=''),0),COALESCE(SUM(settled=1),0),
COALESCE(SUM(settled=1 AND selected_side!=''),0),COALESCE(SUM(settled=-1),0),
COALESCE(SUM(cohort LIKE 'proper-score-v2|%' AND selected_side!='' AND
 ABS(normalized_weight)>1e-12 AND executable_qty>0 AND tick_size>0 AND lot_size>0 AND source_clock_id!=''),0),
COALESCE(SUM(settled=1 AND realized_net IS NOT NULL AND cohort LIKE 'proper-score-v2|%' AND
 selected_side!='' AND ABS(normalized_weight)>1e-12 AND executable_qty>0 AND tick_size>0 AND
 lot_size>0 AND source_clock_id!=''),0),
COALESCE(SUM(cohort NOT LIKE 'proper-score-v2|%'),0),
COALESCE(SUM(settled=1 AND realized_net IS NOT NULL AND cohort NOT LIKE 'proper-score-v2|%'),0),
SUM(CASE WHEN cohort LIKE 'proper-score-v2|%' AND selected_side!='' AND
 ABS(normalized_weight)>1e-12 AND executable_qty>0 AND tick_size>0 AND lot_size>0 AND
 source_clock_id!='' AND c.slot IS NOT NULL THEN expected_net ELSE 0 END),
SUM(CASE WHEN settled=1 AND realized_net IS NOT NULL AND cohort LIKE 'proper-score-v2|%' AND
 selected_side!='' AND ABS(normalized_weight)>1e-12 AND executable_qty>0 AND tick_size>0 AND
 lot_size>0 AND source_clock_id!='' AND c.slot IS NOT NULL THEN realized_net ELSE 0 END),
SUM(CASE WHEN cohort LIKE 'proper-score-v2|%' AND selected_side!='' AND ABS(normalized_weight)>1e-12
 AND executable_qty>0 AND tick_size>0 AND lot_size>0 AND source_clock_id!='' AND c.slot IS NOT NULL
 THEN expected_net*normalized_weight ELSE 0 END),
SUM(CASE WHEN settled=1 AND realized_net IS NOT NULL AND cohort LIKE 'proper-score-v2|%' AND
 selected_side!='' AND ABS(normalized_weight)>1e-12 AND executable_qty>0 AND tick_size>0 AND
 lot_size>0 AND source_clock_id!='' AND c.slot IS NOT NULL THEN realized_net*normalized_weight ELSE 0 END),
SUM(CASE WHEN settled=1 AND realized_net IS NOT NULL AND cohort LIKE 'proper-score-v2|%' AND
 selected_side!='' AND ABS(normalized_weight)>1e-12 AND executable_qty>0 AND tick_size>0 AND
 lot_size>0 AND source_clock_id!='' AND c.slot IS NOT NULL THEN ABS(normalized_weight) ELSE 0 END),
SUM(CASE WHEN settled=1 AND realized_net IS NOT NULL AND cohort NOT LIKE 'proper-score-v2|%'
 THEN realized_net ELSE 0 END),
AVG(score_delta),(SELECT COUNT(*) FROM complete_slots),
(SELECT COUNT(*) FROM (SELECT t.platform,t.ticker FROM scoped t JOIN complete_slots c
 ON c.slot=t.slot AND c.transform=t.transform AND c.strategy_mode=t.strategy_mode
 WHERE t.observed_ts>=c.created_ts AND t.settled=1 AND t.realized_net IS NOT NULL
  AND t.cohort LIKE 'proper-score-v2|%' AND t.selected_side!=''
  AND ABS(t.normalized_weight)>1e-12 AND t.executable_qty>0 AND t.tick_size>0
  AND t.lot_size>0 AND t.source_clock_id!='' GROUP BY t.platform,t.ticker)),
(SELECT COUNT(*) FROM manifest_state),
(SELECT COALESCE(SUM(exact_coordinates),0) FROM manifest_state),
(SELECT COUNT(*) FROM terminal_slots),
(SELECT COALESCE(SUM(NOT exact_coordinates),0) FROM manifest_state),
(SELECT COALESCE(SUM(exact_coordinates AND open_rows=expected_coordinate_count),0) FROM manifest_state),
(SELECT COALESCE(SUM(exact_coordinates AND open_rows>0 AND open_rows<expected_coordinate_count),0) FROM manifest_state),
(SELECT COALESCE(SUM(exact_coordinates AND open_rows=0 AND unsupported_rows>0),0) FROM manifest_state),
(SELECT COUNT(*) FROM (SELECT slot,transform,strategy_mode FROM scoped t
 WHERE NOT EXISTS(SELECT 1 FROM scoped_manifests m WHERE m.slot=t.slot AND m.transform=t.transform
  AND m.strategy_mode=t.strategy_mode) GROUP BY slot,transform,strategy_mode)),
(SELECT COALESCE(SUM(missing_rows),0) FROM manifest_state),
(SELECT COALESCE(SUM(extra_rows),0) FROM manifest_state)
FROM scoped t LEFT JOIN complete_slots c ON c.slot=t.slot AND c.transform=t.transform
	AND c.strategy_mode=t.strategy_mode AND t.observed_ts>=c.created_ts`, queryArgs...).Scan(&v.Rows, &v.Actions, &v.Abstains,
			&v.Settled, &v.SettledActions, &v.Unsupported,
			&v.NormalizedExecutableRows, &v.NormalizedExecutableSettled,
			&v.LegacyUnnormalizedRows, &v.LegacyUnnormalizedSettled,
			&v.Expected, &v.Realized,
			&v.WeightedExpected, &v.WeightedRealized, &v.NormalizedSettledL1,
			&v.LegacyUnnormalizedRealized, &v.ScoreDelta, &v.NormalizedExecutableCompletedSlots,
			&v.NormalizedExecutableUniqueSettledMarkets,
			&v.ManifestSlots, &v.ManifestHashProvenSlots, &v.ManifestCompletedSlots,
			&v.ManifestIncompleteSlots, &v.ManifestOpenSlots, &v.ManifestPartialSlots,
			&v.ManifestUnsupportedSlots, &v.UnmanifestedSlots,
			&v.ManifestMissingCoordinates, &v.ManifestExtraCoordinates)
		if err != nil {
			return v, err
		}
		return v, nil
	}
	pack := func(v summary) map[string]any {
		return map[string]any{"rows": v.Rows, "actions": v.Actions, "abstains": v.Abstains,
			"settled": v.Settled, "settled_actions": v.SettledActions, "unsupported_scalar": v.Unsupported,
			"normalized_executable_rows":                   v.NormalizedExecutableRows,
			"normalized_executable_settled":                v.NormalizedExecutableSettled,
			"normalized_executable_completed_slots":        v.NormalizedExecutableCompletedSlots,
			"normalized_executable_unique_settled_markets": v.NormalizedExecutableUniqueSettledMarkets,
			"normalized_executable_settled_l1":             v.NormalizedSettledL1.Float64,
			"manifest_slots":                               v.ManifestSlots,
			"manifest_hash_proven_slots":                   v.ManifestHashProvenSlots,
			"manifest_completed_slots":                     v.ManifestCompletedSlots,
			"manifest_incomplete_slots":                    v.ManifestIncompleteSlots,
			"manifest_open_slots":                          v.ManifestOpenSlots,
			"manifest_partial_slots":                       v.ManifestPartialSlots,
			"manifest_unsupported_slots":                   v.ManifestUnsupportedSlots,
			"unmanifested_slots":                           v.UnmanifestedSlots,
			"manifest_missing_coordinates":                 v.ManifestMissingCoordinates,
			"manifest_extra_coordinates":                   v.ManifestExtraCoordinates,
			"legacy_unnormalized_rows":                     v.LegacyUnnormalizedRows,
			"legacy_unnormalized_settled":                  v.LegacyUnnormalizedSettled,
			"expected_net":                                 v.Expected.Float64, "realized_net": v.Realized.Float64,
			"l1_weighted_expected_net":         v.WeightedExpected.Float64,
			"l1_weighted_realized_net":         v.WeightedRealized.Float64,
			"legacy_unnormalized_realized_net": v.LegacyUnnormalizedRealized.Float64,
			"mean_score_delta":                 v.ScoreDelta.Float64}
	}
	all, err := read("")
	if err != nil {
		return nil, err
	}
	transforms := map[string]any{}
	for _, name := range []string{"brier", "log", "spherical"} {
		v, err := read(" AND transform=?", name)
		if err != nil {
			return nil, err
		}
		transforms[name] = pack(v)
	}
	modes := map[string]any{}
	for _, mode := range []string{"fundamental", "momentum"} {
		v, err := read(" AND strategy_mode=?", mode)
		if err != nil {
			return nil, err
		}
		modes[mode] = pack(v)
	}
	simulated := map[string]any{}
	for _, mode := range []string{"fundamental", "momentum"} {
		var rows, filled, partial, noop, blocked int
		var gross, fees, expected, turnover sql.NullFloat64
		if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*),
COALESCE(SUM(fill_status='filled'),0),COALESCE(SUM(fill_status='partial'),0),
COALESCE(SUM(fill_status='noop'),0),COALESCE(SUM(fill_status='blocked'),0),
SUM(gross_cash_flow),SUM(fee_total),SUM(expected_value_delta),SUM(ABS(filled_delta))
FROM research_proper_score_simulated_positions WHERE strategy_mode=?`, mode).Scan(&rows, &filled,
			&partial, &noop, &blocked, &gross, &fees, &expected, &turnover); err != nil {
			return nil, err
		}
		simulated[mode] = map[string]any{"rows": rows, "filled": filled, "partial": partial,
			"noop": noop, "blocked": blocked, "gross_cash_flow": gross.Float64,
			"fees": fees.Float64, "expected_value_delta": expected.Float64,
			"filled_share_turnover": turnover.Float64, "authority": "zero"}
	}
	comparatorMetrics := map[string]any{}
	// Comparator row count is diagnostic only: equal-share/Adaptive-Allocation/no-trade deliberately emit one
	// control per coordinate, including zero-target controls. Economic denominators are the
	// normalized target L1 that reached terminal truth and complete hourly vector slots.
	rows, err := s.db.QueryContext(ctx, `WITH base AS (
	SELECT o.id,o.observed_ts,json_extract(o.inputs_json,'$.comparator_arm') arm,
  COALESCE(NULLIF(json_extract(o.inputs_json,'$.collection_slot'),''),substr(o.observed_ts,1,13)) slot,
  json_extract(o.inputs_json,'$.matched_strategy_mode') mode,
  json_extract(o.inputs_json,'$.matched_transform') transform,
  COALESCE(json_extract(o.inputs_json,'$.proper_coordinate_key'),'') coordinate_key,
  ABS(CAST(json_extract(o.inputs_json,'$.normalized_one_unit_target') AS REAL)) target,
  CAST(json_extract(o.inputs_json,'$.one_share_expected_net') AS REAL) expected,
  u.id update_id,u.realized_net realized
 FROM research_system_observations o LEFT JOIN research_system_payoff_updates u ON u.id=(
  SELECT u2.id FROM research_system_payoff_updates u2 WHERE u2.observation_id=o.id
  ORDER BY u2.id DESC LIMIT 1)
 WHERE o.system_id='proper-score-executor' AND o.observation_kind='control'
  AND o.cohort LIKE 'proper-score-comparator-v1|%'
), manifests AS (
 SELECT m.*,(SELECT COUNT(*) FROM research_proper_score_manifest_coordinates c
  WHERE c.slot=m.slot AND c.transform=m.transform AND c.strategy_mode=m.strategy_mode) coordinate_rows
 FROM research_proper_score_manifests m
), slots AS (
 SELECT arm,slot,mode,transform,
  SUM(target) target_l1,
  SUM(CASE WHEN update_id IS NOT NULL THEN target ELSE 0 END) settled_l1,
  SUM(CASE WHEN target>1e-12 AND update_id IS NULL THEN 1 ELSE 0 END) pending_targets,
	SUM(update_id IS NOT NULL) settled_rows,COUNT(*) raw_rows,COUNT(DISTINCT coordinate_key) coordinate_keys,
	MIN(observed_ts) first_observed,
  SUM(EXISTS(SELECT 1 FROM research_proper_score_manifest_coordinates c
   WHERE c.slot=base.slot AND c.transform=base.transform AND c.strategy_mode=base.mode
    AND c.coordinate_key=base.coordinate_key)) matched_coordinates
 FROM base GROUP BY arm,slot,mode,transform
), complete_slots AS (
 SELECT s.arm,s.slot,s.mode,s.transform FROM slots s JOIN manifests m
  ON m.slot=s.slot AND m.transform=s.transform AND m.strategy_mode=s.mode
 WHERE m.coordinate_rows=m.expected_coordinate_count AND s.raw_rows=m.expected_coordinate_count
  AND s.coordinate_keys=m.expected_coordinate_count AND s.matched_coordinates=m.expected_coordinate_count
	AND s.first_observed>=m.created_ts
  AND ((s.target_l1>1e-12 AND s.pending_targets=0)
    OR (s.arm='no-trade' AND s.settled_rows=s.raw_rows))
), complete AS (
 SELECT arm,COUNT(*) completed_slots FROM complete_slots
 GROUP BY arm
)
SELECT b.arm,COUNT(*),COALESCE(SUM(b.update_id IS NOT NULL),0),
 COALESCE(SUM(CASE WHEN cs.arm IS NOT NULL THEN b.expected*b.target ELSE 0 END),0),
 COALESCE(SUM(CASE WHEN cs.arm IS NOT NULL AND b.realized IS NOT NULL THEN b.realized*b.target ELSE 0 END),0),
 COALESCE(SUM(CASE WHEN cs.arm IS NOT NULL THEN b.target ELSE 0 END),0),
 COALESCE(SUM(CASE WHEN cs.arm IS NOT NULL AND b.realized IS NOT NULL THEN b.target ELSE 0 END),0),
 COALESCE(c.completed_slots,0)
FROM base b LEFT JOIN complete_slots cs ON cs.arm=b.arm AND cs.slot=b.slot
 AND cs.mode=b.mode AND cs.transform=b.transform
LEFT JOIN complete c ON c.arm=b.arm
GROUP BY b.arm,c.completed_slots`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var arm string
		var n, settled, completedSlots int
		var expected, realized, targetL1, settledL1 float64
		if err := rows.Scan(&arm, &n, &settled, &expected, &realized, &targetL1,
			&settledL1, &completedSlots); err != nil {
			_ = rows.Close()
			return nil, err
		}
		comparatorMetrics[arm] = map[string]any{"rows": n, "settled_rows": settled,
			"target_l1": targetL1, "settled_l1": settledL1, "completed_slots": completedSlots,
			"l1_weighted_expected_net": expected, "l1_weighted_realized_net": realized}
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	return map[string]any{
		"system": "proper-score", "state": "PREREGISTERED_EXECUTABLE_ACTIONS_COLLECTING", "research_only": true,
		"live_authorizes": false, "paper_authorizes": false,
		"vector_completion_contract": map[string]any{
			"version": 1, "durable": true, "immutable": true,
			"identity":                "collection slot + strategy mode + transform + frozen expected coordinate count/SHA-256 set hash",
			"complete":                "every expected coordinate is present exactly once, no extra coordinate exists, and every coordinate has supported terminal truth",
			"missing_insert_behavior": "slot remains incomplete; stored rows cannot shrink the expected vector",
		},
		"promotion_contract": "only a sealed preregistered untouched PASS may enter Paper; only its accepted identical one-share Paper route may enter armed LIVE AUTO",
		"route_parity": map[string]any{"fundamental_buy": "generic sealed BUY bridge",
			"kalshi_momentum_single_sell": "separate exact-position reduce-only FOK bridge after sealed SELL proof",
			"momentum_cross":              "blocked_non_atomic_close_plus_open",
			"polyus_momentum_sell":        "blocked_no_verified_fill_or_kill_receipt"},
		"portfolio_estimand":      "within each transform and collection slot, sum(abs(normalized_weight))=1 share unit; expected and realized portfolio metrics multiply one-share route returns by those frozen weights; capacity remains a separate one-share full-depth probe",
		"representation_contract": "raw gradient-difference vector is the paper truth; canonical complete-set shift is never treated as free on a CLOB, and Brier's common one-sided factor is removed by L1 normalization",
		"rule":                    "fresh calibrated forecast -> Brier/log/spherical proper position vector -> fundamental accumulation and momentum target cohorts -> current complete side book -> exact taker fee/full depth/tick/lot -> one-share route sample or spread abstention",
		"summary":                 pack(all), "transforms": transforms, "modes": modes,
		"simulated_immediate_taker": simulated,
		"comparators": map[string]any{"arms": []string{"equal-share", "max-margin",
			"mode4-normalized", "no-trade"}, "metrics": comparatorMetrics,
			"same_forecast_book_clock": true, "authority": "zero",
			"display_labels": map[string]string{"mode4-normalized": "Adaptive Allocation Model"},
			"mode4_target":   "adaptive proof-Kelly shape normalized to one unit; no bankroll"},
	}, nil
}
