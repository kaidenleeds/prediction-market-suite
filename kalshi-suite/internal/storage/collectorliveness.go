package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

type CollectorSpec struct {
	CollectorID, ExperimentID, Source, SchemaVersion, ZeroPolicy string
	ExperimentVersion, Version                                   int
	ExpectedCadence                                              time.Duration
	Systems                                                      []string
}

type CollectorReceipt struct {
	CollectorID, CycleID, ExperimentID, Status               string
	ZeroReason, ErrorClass, ErrorText, Source, SchemaVersion string
	ExperimentVersion                                        int
	Started, Completed                                       time.Time
	Eligible, Attempted, Inserted, Duplicates, ReplayGaps    int
	Exclusions                                               map[string]int
	Metrics                                                  map[string]any
	ExpectedZero                                             bool
	ExpectedCadence                                          time.Duration
	Systems                                                  []string
}

type collectorSpecHash struct {
	CollectorID, ExperimentID, Source, SchemaVersion, ZeroPolicy string
	ExperimentVersion, Version                                   int
	ExpectedCadenceS                                             float64
	Systems                                                      []string
}

const (
	R139CrossVenueRuleArtifactSource = "all structured K-PUS/K-PINT/PUS-PINT objective candidates + exact cached venue rule fields + reused Kalshi event settlement sources"
	R139SealedPaperPromotionSource   = "sealed preregistered untouched PASS plus identical fresh single-instrument candidate handed to normal Paper executor"
)

func cleanStringSet(in []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, s := range in {
		s = strings.TrimSpace(s)
		if s != "" && !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	sort.Strings(out)
	return out
}

func (s *Store) RegisterCollectorSpec(ctx context.Context, c CollectorSpec) (bool, error) {
	c.CollectorID = strings.TrimSpace(c.CollectorID)
	if c.CollectorID == "" || c.Source == "" || c.SchemaVersion == "" || c.ZeroPolicy == "" || c.ExpectedCadence <= 0 {
		return false, errors.New("incomplete collector specification")
	}
	if c.Version <= 0 {
		c.Version = 1
	}
	if c.ExperimentID != "" && c.ExperimentVersion <= 0 {
		c.ExperimentVersion = 1
	}
	c.Systems = cleanStringSet(c.Systems)
	systemsJSON, _ := json.Marshal(c.Systems)
	h, err := researchSpecHash(collectorSpecHash{CollectorID: c.CollectorID, ExperimentID: c.ExperimentID,
		Source: c.Source, SchemaVersion: c.SchemaVersion, ZeroPolicy: c.ZeroPolicy,
		ExperimentVersion: c.ExperimentVersion, Version: c.Version,
		ExpectedCadenceS: c.ExpectedCadence.Seconds(), Systems: c.Systems})
	if err != nil {
		return false, err
	}
	var existing string
	err = s.db.QueryRowContext(ctx, `SELECT spec_hash FROM research_collector_specs WHERE collector_id=? AND version=?`, c.CollectorID, c.Version).Scan(&existing)
	if err == nil {
		if existing != h {
			return false, fmt.Errorf("immutable collector spec drift: %s v%d (bump version)", c.CollectorID, c.Version)
		}
		return false, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return false, err
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO research_collector_specs(
collector_id,version,spec_hash,experiment_id,experiment_version,expected_cadence_s,source,
schema_version,systems_json,zero_policy,active,created_ts) VALUES(?,?,?,?,?,?,?,?,?,?,1,?)`,
		c.CollectorID, c.Version, h, c.ExperimentID, c.ExperimentVersion, c.ExpectedCadence.Seconds(),
		c.Source, c.SchemaVersion, string(systemsJSON), c.ZeroPolicy, nowRFC())
	return err == nil, err
}

func r138CollectorBlueprints() []CollectorSpec {
	return []CollectorSpec{
		{CollectorID: "canonical-identity", Source: "game_identity+market_game structural joins", SchemaVersion: "r138-v1", ZeroPolicy: "healthy_empty only when no structural rows remain in the current page", ExpectedCadence: 5 * time.Minute, Systems: ResearchExperimentIDs()},
		{CollectorID: "canonical-catalog-identity", Source: "market_catalog excluding structural game joins", SchemaVersion: "r138-v1", ZeroPolicy: "healthy_empty only when no source rows are newer than the durable watermark", ExpectedCadence: 5 * time.Minute, Systems: ResearchExperimentIDs()},
		{CollectorID: "semantic-basis-router", ExperimentID: "payoff-constraint-solver", ExperimentVersion: 1, Source: "newest immutable event/payoff/instrument/relation certificates; never titles", SchemaVersion: "semantic-basis-r138-v1", ZeroPolicy: "zero solver-eligible pairs is healthy only when every cross-venue pair has an explicit identity/source/rules/basis/fee blocker", ExpectedCadence: 5 * time.Minute, Systems: []string{"payoff-constraint-solver", "identity-challenged-cross-venue-lock"}},
		{CollectorID: "proper-score", Version: 2, ExperimentID: "proper-score-executor", ExperimentVersion: 1, Source: "ml_predictions book-native-v2 all_scored + current complete venue full-depth books", SchemaVersion: "proper-score-paper-v2", ZeroPolicy: "zero is healthy only when source forecasts are absent or every row has an explicit exclusion", ExpectedCadence: 5 * time.Minute, Systems: []string{"proper-score-executor", "forecast-persona-router"}},
		{CollectorID: "queue-priority", ExperimentID: "replenishment-fingerprint", ExperimentVersion: 1, Source: "Kalshi authenticated resting orders + official queue position", SchemaVersion: "kalshi-queue-v1", ZeroPolicy: "zero is healthy only when authenticated read succeeds and the account has no natural resting orders", ExpectedCadence: time.Minute, Systems: []string{"replenishment-fingerprint", "maker-salvage-matched-cohort"}},
		{CollectorID: "lifecycle-reopen", ExperimentID: "series-roll-anchor", ExperimentVersion: 1, Source: "Kalshi sequenced lifecycle events + current complete book", SchemaVersion: "kalshi-lifecycle-v1", ZeroPolicy: "raw frames are not reopen episodes; every exclusion requires a reason", ExpectedCadence: time.Minute, Systems: []string{"outcome-set-expansion-shock", "series-roll-anchor", "attention-spillover-graph"}},
		{CollectorID: "subcent-golf", ExperimentID: "outcome-set-expansion-shock", ExperimentVersion: 1, Source: "Kalshi market metadata + dynamic price ranges + current depth", SchemaVersion: "subcent-golf-v1", ZeroPolicy: "the full universe-to-insert exclusion funnel must explain zero rows", ExpectedCadence: 10 * time.Minute, Systems: []string{"outcome-set-expansion-shock", "replenishment-fingerprint"}},
		{CollectorID: "weather-research", Version: 2, ExperimentID: "deadline-hazard-surface", ExperimentVersion: 1,
			Source:          "Kalshi trade-derived forecast diagnostics plus official NOAA NBM station-quantile probability envelopes; no money authority",
			SchemaVersion:   "weather-research-r139-v2",
			ZeroPolicy:      "source/schema/book errors are blocked; zero candidate signals is healthy only after exact station/date/ladder joins were attempted and every fee-net probability lower bound failed to clear zero",
			ExpectedCadence: 30 * time.Minute, Systems: []string{"deadline-hazard-surface", "independent-probabilistic-weather"}},
		{CollectorID: "venue-notices", Version: 2, Source: "official venue notices plus direct fee, incentive, rule, documentation-index and API-schema artifacts", SchemaVersion: "venue-notices-r138-v2", ZeroPolicy: "zero new versions is healthy only after every pinned official source returns unchanged or all parsed notices deduplicate; any source/schema error is blocked", ExpectedCadence: 15 * time.Minute, Systems: ResearchExperimentIDs()},
		{CollectorID: "incentive-maker", ExperimentID: "incentive-subsidized-structural-lock", ExperimentVersion: 1, Source: "Kalshi official incentive programs + current books", SchemaVersion: "kalshi-incentives-v1", ZeroPolicy: "zero is healthy only after a successful official program read with no active programs", ExpectedCadence: 15 * time.Minute, Systems: []string{"incentive-subsidized-structural-lock"}},
		{CollectorID: "event-basket-lock", ExperimentID: "payoff-constraint-solver", ExperimentVersion: 1, Source: "complete structural event sets + exact books/fees", SchemaVersion: "basket-v1", ZeroPolicy: "zero candidates is healthy; eligible/attempted observations must remain visible", ExpectedCadence: 2 * time.Minute, Systems: []string{"payoff-constraint-solver", "identity-challenged-cross-venue-lock"}},
		{CollectorID: "nested-ladder-lock", ExperimentID: "payoff-constraint-solver", ExperimentVersion: 1, Source: "structural threshold ladders + exact books/fees", SchemaVersion: "nested-v1", ZeroPolicy: "zero candidates is healthy; eligible/attempted observations must remain visible", ExpectedCadence: 2 * time.Minute, Systems: []string{"payoff-constraint-solver", "deadline-hazard-surface"}},
		{CollectorID: "cross-venue-rule-artifacts", ExperimentID: "identity-challenged-cross-venue-lock", ExperimentVersion: 1, Source: R139CrossVenueRuleArtifactSource, SchemaVersion: "cross-venue-rules-r139-v3", ZeroPolicy: "zero is healthy only when no current structured pair exists; every candidate otherwise gets an immutable accepted/rejected decision, directional-only risk tier, and exact lock blocker", ExpectedCadence: 5 * time.Minute, Systems: []string{"identity-challenged-cross-venue-lock", "payoff-constraint-solver"}},
		{CollectorID: "native-time-nested-lock", Source: "latest immutable verified implication/deadline relations + current side-specific Kalshi books + exact route fees", SchemaVersion: "native-time-nested-lock-r139-v1", ZeroPolicy: "zero is healthy only when the immutable graph has no verified ordered-deadline relation; every relation excluded after that must have a durable reason", ExpectedCadence: 5 * time.Minute, Systems: []string{"time-nested-lock"}},
		{CollectorID: "native-joint-marginal-lock", Source: "prospective certified event baskets + revalidated current side-specific venue books and exact marginal fees", SchemaVersion: "native-joint-marginal-lock-r139-v1", ZeroPolicy: "zero is healthy only when no prospective certified basket exists; missing joint venue quote, current book, fee, or identity is an explicit exclusion", ExpectedCadence: 5 * time.Minute, Systems: []string{"joint-marginal-lock"}},
		{CollectorID: "native-fee-rounding-batch", Source: "recent real book-native Kalshi unit candidates + current top-level depth + actual aggregate and separate venue fee authority", SchemaVersion: "native-fee-rounding-batch-r139-v1", ZeroPolicy: "zero is healthy only when there is no recent exact-fee candidate with at least two current top-level units; zero fee savings is a measured control, not missing data", ExpectedCadence: 5 * time.Minute, Systems: []string{"fee-rounding-batch"}},
		{CollectorID: "native-route-terminal-grades", Source: "venue-scoped canonical signal settlements with exact settle_val joined to frozen native route controls", SchemaVersion: "native-route-terminal-grades-r139-v1", ZeroPolicy: "zero is healthy only when no ungraded native route has complete exact venue-scoped settlement receipts; unresolved routes remain open and never receive a fabricated zero", ExpectedCadence: 5 * time.Minute, Systems: []string{"time-nested-lock", "joint-marginal-lock", "fee-rounding-batch"}},
		{CollectorID: "research-system-terminal-grades", Source: "complete frozen taker observations + venue-scoped canonical exact settle_val", SchemaVersion: "research-system-terminal-grades-r139-v1", ZeroPolicy: "zero is healthy only when no complete ungraded taker observation has an exact post-quote venue settlement; observer and maker/no-fill controls are never converted into economic results", ExpectedCadence: 5 * time.Minute, Systems: ResearchExperimentIDs()},
		{CollectorID: "concrete-paired-systems", Source: "source-native triggers repriced from one current two-sided executable book; official credit receipts read separately", SchemaVersion: "concrete-paired-systems-r139-v1", ZeroPolicy: "zero is healthy only when no source trigger has verified canonical identity plus a current complete two-sided exact-fee book; input/control counts never become economic n", ExpectedCadence: 5 * time.Minute, Systems: []string{"paired-bridge-inversion", "side-normalized-crowding-fade", "clientele-clock-basis", "attention-spillover-graph", "series-roll-anchor", "semantic-complexity-premium", "forecast-persona-router", "collateral-release-rotation", "settlement-latency-carry"}},
		{CollectorID: "sealed-paper-promotion", Source: R139SealedPaperPromotionSource, SchemaVersion: "sealed-paper-live-bridge-r139-v1", ZeroPolicy: "zero is healthy until an all-candidate one-share sealed untouched PASS and a fresh exact matching contract both exist; input/control counts can never promote", ExpectedCadence: 5 * time.Second, Systems: ResearchExperimentIDs()},
	}
}

func (s *Store) EnsureR138CollectorBlueprints(ctx context.Context) error {
	// Preserve the shipped R138 proper-score contract. R139 adds the complete full-depth Paper
	// simulation schema as v2; mutating v1 would correctly prevent every existing database boot.
	legacyProperScore := CollectorSpec{CollectorID: "proper-score", Version: 1,
		ExperimentID: "proper-score-executor", ExperimentVersion: 1,
		Source:          "ml_predictions book-native-v2 all_scored + current complete venue books",
		SchemaVersion:   "r138-book-native-v2",
		ZeroPolicy:      "zero is healthy only when source forecasts are absent or every row has an explicit exclusion",
		ExpectedCadence: 5 * time.Minute, Systems: []string{"proper-score-executor", "forecast-persona-router"}}
	if _, err := s.RegisterCollectorSpec(ctx, legacyProperScore); err != nil {
		return err
	}
	// Preserve the shipped R138 weather contract before registering the operational independent
	// NBM extension as v2. RegisterCollectorSpec rejects mutation of either historical version.
	legacyWeather := CollectorSpec{CollectorID: "weather-research", Version: 1,
		ExperimentID: "deadline-hazard-surface", ExperimentVersion: 1,
		Source:        "Kalshi forecast history diagnostics; endogenous and permanently non-promotable",
		SchemaVersion: "kalshi-forecast-history-v1", ZeroPolicy: "HTTP/schema errors are blocked, never healthy empty",
		ExpectedCadence: 30 * time.Minute, Systems: []string{"deadline-hazard-surface"}}
	if _, err := s.RegisterCollectorSpec(ctx, legacyWeather); err != nil {
		return err
	}
	for _, c := range r138CollectorBlueprints() {
		if _, err := s.RegisterCollectorSpec(ctx, c); err != nil {
			return err
		}
	}
	return nil
}

func normalizeCollectorStatus(v string) string {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "healthy", "healthy_empty", "starved", "blocked", "error":
		return strings.ToLower(strings.TrimSpace(v))
	default:
		return "error"
	}
}

func sameCleanStringSet(a, b []string) bool {
	a, b = cleanStringSet(a), cleanStringSet(b)
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func (s *Store) currentCollectorSpec(ctx context.Context, collectorID string) (CollectorSpec, error) {
	var out CollectorSpec
	var cadence float64
	var systemsJSON string
	err := s.db.QueryRowContext(ctx, `SELECT collector_id,version,experiment_id,experiment_version,
expected_cadence_s,source,schema_version,systems_json,zero_policy
FROM research_collector_specs WHERE collector_id=? AND active=1
ORDER BY version DESC LIMIT 1`, collectorID).Scan(&out.CollectorID, &out.Version, &out.ExperimentID,
		&out.ExperimentVersion, &cadence, &out.Source, &out.SchemaVersion, &systemsJSON, &out.ZeroPolicy)
	if err != nil {
		return CollectorSpec{}, err
	}
	out.ExpectedCadence = time.Duration(cadence * float64(time.Second))
	if err := json.Unmarshal([]byte(systemsJSON), &out.Systems); err != nil {
		return CollectorSpec{}, fmt.Errorf("collector spec systems: %w", err)
	}
	out.Systems = cleanStringSet(out.Systems)
	return out, nil
}

// InsertCollectorReceipt appends one immutable cycle outcome. Receipts never influence trading;
// they explain whether a zero means no opportunity, exclusion, duplication, schema block, or error.
func (s *Store) InsertCollectorReceipt(ctx context.Context, r CollectorReceipt) (bool, error) {
	r.CollectorID, r.CycleID = strings.TrimSpace(r.CollectorID), strings.TrimSpace(r.CycleID)
	if r.CollectorID == "" || r.CycleID == "" || r.Source == "" || r.SchemaVersion == "" {
		return false, errors.New("incomplete collector receipt")
	}
	spec, err := s.currentCollectorSpec(ctx, r.CollectorID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, fmt.Errorf("collector receipt has no active immutable spec: %s", r.CollectorID)
		}
		return false, err
	}
	if r.Started.IsZero() {
		r.Started = time.Now()
	}
	if r.Completed.IsZero() {
		r.Completed = time.Now()
	}
	if r.Completed.Before(r.Started) || r.Eligible < 0 || r.Attempted < 0 || r.Inserted < 0 || r.Duplicates < 0 || r.ReplayGaps < 0 {
		return false, errors.New("invalid collector receipt values")
	}
	rawStatus := strings.ToLower(strings.TrimSpace(r.Status))
	r.Status = normalizeCollectorStatus(rawStatus)
	contractProblems := []string{}
	if rawStatus != r.Status {
		contractProblems = append(contractProblems, "invalid status")
	}
	if r.ExperimentID != spec.ExperimentID || r.ExperimentVersion != spec.ExperimentVersion {
		contractProblems = append(contractProblems, "experiment/version mismatch")
	}
	if r.Source != spec.Source {
		contractProblems = append(contractProblems, "source mismatch")
	}
	if r.SchemaVersion != spec.SchemaVersion {
		contractProblems = append(contractProblems, "schema mismatch")
	}
	if r.ExpectedCadence != spec.ExpectedCadence {
		contractProblems = append(contractProblems, "cadence mismatch")
	}
	if !sameCleanStringSet(r.Systems, spec.Systems) {
		contractProblems = append(contractProblems, "system coverage mismatch")
	}
	if len(contractProblems) > 0 {
		// Persist the failure under the canonical contract, never the caller's forged metadata. This
		// makes the latest liveness row red and gives the operator an exact repair target.
		r.Status, r.ExpectedZero, r.ZeroReason = "error", false, ""
		r.ErrorClass = "collector_contract_mismatch"
		r.ErrorText = strings.Join(contractProblems, "; ")
		r.ExperimentID, r.ExperimentVersion = spec.ExperimentID, spec.ExperimentVersion
		r.Source, r.SchemaVersion = spec.Source, spec.SchemaVersion
		r.ExpectedCadence, r.Systems = spec.ExpectedCadence, append([]string(nil), spec.Systems...)
	}
	if r.ErrorText != "" && r.Status != "blocked" {
		r.Status = "error"
	}
	if r.ExpectedZero && r.Status == "healthy" {
		r.Status = "healthy_empty"
	}
	if r.Status == "healthy_empty" {
		r.ExpectedZero = true
		if strings.TrimSpace(r.ZeroReason) == "" || r.Inserted != 0 {
			return false, errors.New("healthy_empty receipt requires a zero reason and zero inserts")
		}
	}
	if r.Status == "healthy" && r.ExpectedZero {
		return false, errors.New("healthy receipt cannot claim expected zero")
	}
	if r.Status == "error" && (strings.TrimSpace(r.ErrorClass) == "" || strings.TrimSpace(r.ErrorText) == "") {
		return false, errors.New("error receipt requires error class and text")
	}
	if r.Attempted == 0 && (r.Inserted > 0 || r.Duplicates > 0) {
		return false, errors.New("collector inserted or deduplicated rows without an attempt")
	}
	if r.ExpectedZero && r.Inserted > 0 {
		return false, errors.New("expected-zero receipt contains inserted rows")
	}
	exclusions := map[string]int{}
	excludedTotal := 0
	for k, n := range r.Exclusions {
		k = strings.TrimSpace(k)
		if k != "" && n > 0 {
			exclusions[k] = n
			excludedTotal += n
		}
	}
	exclusionsJSON, _ := json.Marshal(exclusions)
	if r.Metrics == nil {
		r.Metrics = map[string]any{}
	}
	r.Metrics["count_accounting"] = map[string]any{
		"eligible": r.Eligible, "attempted": r.Attempted, "inserted": r.Inserted,
		"duplicates": r.Duplicates, "excluded": excludedTotal,
		"note": "stage counters may fan out; every nonzero stage is preserved rather than forced into a false conservation equation",
	}
	metricsJSON, err := json.Marshal(r.Metrics)
	if err != nil {
		return false, fmt.Errorf("collector metrics: %w", err)
	}
	r.Systems = cleanStringSet(r.Systems)
	systemsJSON, _ := json.Marshal(r.Systems)
	res, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO research_collector_receipts(
collector_id,cycle_id,experiment_id,experiment_version,started_ts,completed_ts,status,eligible,
attempted,inserted,duplicates,excluded_total,exclusions_json,metrics_json,expected_zero,zero_reason,error_class,
error_text,source,schema_version,expected_cadence_s,duration_ms,replay_sequence_gaps,systems_json,
funded,paper_authority,live_authority) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0,0,0)`,
		r.CollectorID, r.CycleID, r.ExperimentID, r.ExperimentVersion,
		r.Started.UTC().Format(time.RFC3339Nano), r.Completed.UTC().Format(time.RFC3339Nano), r.Status,
		r.Eligible, r.Attempted, r.Inserted, r.Duplicates, excludedTotal, string(exclusionsJSON), string(metricsJSON),
		boolInt(r.ExpectedZero), r.ZeroReason, r.ErrorClass, r.ErrorText, r.Source, r.SchemaVersion,
		r.ExpectedCadence.Seconds(), r.Completed.Sub(r.Started).Seconds()*1000, r.ReplayGaps, string(systemsJSON))
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

type CollectorLivenessView struct {
	CollectorID, Status, Source, SchemaVersion, ZeroPolicy               string
	ReceiptStatus, RuntimeStartedTS                                      string
	ExperimentID, CompletedTS, LastSuccessTS, LastInsertTS               string
	LastErrorTS, ErrorClass, ErrorText, ZeroReason                       string
	Version, ExperimentVersion                                           int
	Eligible, Attempted, Inserted, Duplicates, ExcludedTotal             int
	LifetimeCycles, LifetimeInserted, ReplayGaps                         int
	ExpectedCadenceS, EffectiveCadenceS, BootGraceS                      float64
	LagSeconds, DurationMS                                               float64
	ExpectedZero, Alert, NeverRan, Funded, PaperAuthority, LiveAuthority bool
	CompletedThisBoot, NeverRanThisBoot, BootWarming                     bool
	Exclusions                                                           map[string]int
	Metrics                                                              map[string]any
	Systems                                                              []string
}

// SetCollectorRuntimeStart pins the process boundary used by liveness reports. Durable receipts
// are historical evidence and correctly survive restarts; this boundary prevents the latest
// pre-boot receipt from being presented as a completion by the current process. The grace is
// bounded below and never converts a genuine current-boot error into healthy data.
func (s *Store) SetCollectorRuntimeStart(start time.Time) {
	if s == nil {
		return
	}
	s.collectorRuntimeMu.Lock()
	s.collectorRuntimeStart = start.UTC()
	s.collectorRuntimeMu.Unlock()
}

func (s *Store) collectorRuntimeStartedAt() time.Time {
	if s == nil {
		return time.Time{}
	}
	s.collectorRuntimeMu.RLock()
	defer s.collectorRuntimeMu.RUnlock()
	return s.collectorRuntimeStart
}

// These contracts advertise a five-second opportunity clock but their owning loops are
// synchronously phase-rotated with bounded research lanes. A healthy rotation can therefore take
// about one minute even though an admitted opportunity is sampled at five-second precision.
// Alerting at 3*5 seconds makes quiet healthy collectors flash red between ordinary admissions.
// Keep the immutable contract visible and expose the effective liveness cadence separately;
// three missed real rotations still alert.
func effectiveCollectorLivenessCadence(collectorID string, declaredSeconds float64) float64 {
	if declaredSeconds <= 0 {
		return declaredSeconds
	}
	switch strings.TrimSpace(collectorID) {
	case "sealed-paper-promotion", "concrete-attention-horizons", "replenishment-toxicity", "step7-event-microstructure":
		if declaredSeconds < 60 {
			return 60
		}
	}
	return declaredSeconds
}

func collectorBootGraceSeconds(effectiveCadenceS float64) float64 {
	grace := 3 * effectiveCadenceS
	if grace < 90 {
		grace = 90
	}
	return grace
}

// applyCollectorLivenessClock derives current-process health from one collector's latest durable
// receipt. Both the full diagnostic report and compact operator digest use this exact clock so a
// historical green receipt cannot keep either surface green after three missed real cadences.
func applyCollectorLivenessClock(v *CollectorLivenessView, now, runtimeStarted time.Time) {
	v.ReceiptStatus = v.Status
	v.EffectiveCadenceS = effectiveCollectorLivenessCadence(v.CollectorID, v.ExpectedCadenceS)
	v.BootGraceS = collectorBootGraceSeconds(v.EffectiveCadenceS)
	v.NeverRan = v.CompletedTS == ""
	var completed time.Time
	if t, err := time.Parse(time.RFC3339Nano, v.CompletedTS); err == nil {
		completed = t
		v.LagSeconds = now.Sub(t).Seconds()
	}
	if !runtimeStarted.IsZero() {
		v.RuntimeStartedTS = runtimeStarted.Format(time.RFC3339Nano)
		v.CompletedThisBoot = !completed.IsZero() && !completed.Before(runtimeStarted)
		v.NeverRanThisBoot = !v.CompletedThisBoot
		if v.NeverRanThisBoot && now.Sub(runtimeStarted).Seconds() < v.BootGraceS {
			v.BootWarming = true
			v.Status = "warming"
		} else if v.NeverRanThisBoot {
			v.Status = "stale"
		}
	}
	currentBootReceipt := runtimeStarted.IsZero() || v.CompletedThisBoot
	if !v.BootWarming && (v.NeverRan || (!currentBootReceipt && v.NeverRanThisBoot) ||
		v.Status == "error" || v.Status == "blocked" || v.Status == "starved" ||
		(v.EffectiveCadenceS > 0 && v.LagSeconds > 3*v.EffectiveCadenceS)) {
		v.Alert = true
	}
}

func (s *Store) CollectorLivenessReport(ctx context.Context) (map[string]any, error) {
	// Keep the report in one statement. The former implementation held the outer spec/latest-row
	// cursor open, then issued one aggregate query per collector. Under normal eight-connection
	// pressure that cursor could wait minutes for another connection, pinning its old WAL snapshot
	// while writers grew the WAL by hundreds of megabytes. The two grouped CTEs return the same
	// lifetime facts without an N+1 query or a long-lived nested reader.
	rows, err := s.db.QueryContext(ctx, `WITH active_specs AS (
	SELECT c.* FROM research_collector_specs c
	JOIN (
		SELECT collector_id,MAX(version) AS version
		FROM research_collector_specs WHERE active=1 GROUP BY collector_id
	) latest ON latest.collector_id=c.collector_id AND latest.version=c.version
	WHERE c.active=1
), latest_receipt_ids AS (
	SELECT collector_id,MAX(id) AS id FROM research_collector_receipts GROUP BY collector_id
), receipt_stats AS (
	SELECT collector_id,COUNT(*) AS lifetime_cycles,COALESCE(SUM(inserted),0) AS lifetime_inserted,
		COALESCE(MAX(CASE WHEN status IN ('healthy','healthy_empty') THEN completed_ts ELSE '' END),'') AS last_success_ts,
		COALESCE(MAX(CASE WHEN inserted>0 THEN completed_ts ELSE '' END),'') AS last_insert_ts,
		COALESCE(MAX(CASE WHEN error_text<>'' THEN completed_ts ELSE '' END),'') AS last_error_ts
	FROM research_collector_receipts GROUP BY collector_id
)
SELECT c.collector_id,c.version,c.experiment_id,c.experiment_version,
c.expected_cadence_s,c.source,c.schema_version,c.systems_json,c.zero_policy,
COALESCE(r.status,''),COALESCE(r.completed_ts,''),COALESCE(r.eligible,0),COALESCE(r.attempted,0),
COALESCE(r.inserted,0),COALESCE(r.duplicates,0),COALESCE(r.excluded_total,0),
COALESCE(r.exclusions_json,'{}'),COALESCE(r.metrics_json,'{}'),COALESCE(r.expected_zero,0),COALESCE(r.zero_reason,''),
COALESCE(r.error_class,''),COALESCE(r.error_text,''),COALESCE(r.duration_ms,0),
COALESCE(r.replay_sequence_gaps,0),COALESCE(r.funded,0),COALESCE(r.paper_authority,0),COALESCE(r.live_authority,0),
COALESCE(rs.lifetime_cycles,0),COALESCE(rs.lifetime_inserted,0),COALESCE(rs.last_success_ts,''),
COALESCE(rs.last_insert_ts,''),COALESCE(rs.last_error_ts,'')
FROM active_specs c
LEFT JOIN latest_receipt_ids lr ON lr.collector_id=c.collector_id
LEFT JOIN research_collector_receipts r ON r.id=lr.id
LEFT JOIN receipt_stats rs ON rs.collector_id=c.collector_id
ORDER BY c.collector_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	now := time.Now()
	runtimeStarted := s.collectorRuntimeStartedAt()
	var out []CollectorLivenessView
	alerts := 0
	for rows.Next() {
		var v CollectorLivenessView
		var systemsJSON, exclusionsJSON, metricsJSON string
		var expectedZero, funded, paper, live int
		if err := rows.Scan(&v.CollectorID, &v.Version, &v.ExperimentID, &v.ExperimentVersion,
			&v.ExpectedCadenceS, &v.Source, &v.SchemaVersion, &systemsJSON, &v.ZeroPolicy,
			&v.Status, &v.CompletedTS, &v.Eligible, &v.Attempted, &v.Inserted, &v.Duplicates,
			&v.ExcludedTotal, &exclusionsJSON, &metricsJSON, &expectedZero, &v.ZeroReason, &v.ErrorClass,
			&v.ErrorText, &v.DurationMS, &v.ReplayGaps, &funded, &paper, &live,
			&v.LifetimeCycles, &v.LifetimeInserted, &v.LastSuccessTS, &v.LastInsertTS, &v.LastErrorTS); err != nil {
			return nil, err
		}
		v.ExpectedZero, v.Funded, v.PaperAuthority, v.LiveAuthority = expectedZero != 0, funded != 0, paper != 0, live != 0
		_ = json.Unmarshal([]byte(systemsJSON), &v.Systems)
		_ = json.Unmarshal([]byte(exclusionsJSON), &v.Exclusions)
		_ = json.Unmarshal([]byte(metricsJSON), &v.Metrics)
		applyCollectorLivenessClock(&v, now, runtimeStarted)
		if v.Alert {
			alerts++
		}
		out = append(out, v)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return map[string]any{
		"generated_at":  time.Now().UTC().Format(time.RFC3339Nano),
		"research_only": true, "funded": false, "paper_authority": false, "live_authority": false,
		"collectors": out, "active": len(out), "alerts": alerts,
	}, nil
}
