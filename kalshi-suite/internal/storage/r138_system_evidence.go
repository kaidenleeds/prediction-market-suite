package storage

// R138 Steps 6-9: one append-only, zero-authority evidence contract shared by the outcome,
// hazard, score-state, microstructure, incentive and official-release research collectors.

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
)

const r138EvidenceSchemaVersion = "r138-route-envelope-v1"

const (
	R139Step7EventMicrostructureSource = "every existing subscribed Kalshi touch delta + public trade; bounded RAM-clocked 5s/30s/300s samples with atomic raw plus byte-frozen projection outbox"
	R139Step7MakerSalvageSource        = "natural maker_fill_stats lifecycle mirror; no research-created orders"
	R139ConcreteAttentionSource        = "linked attention observations repriced at exact executable exits at preregistered 60s/300s targets"
)

func R138EvidenceSchemaVersion() string { return r138EvidenceSchemaVersion }

func R138Step6To9CollectorSpecs() []CollectorSpec {
	specs := []CollectorSpec{
		{CollectorID: "outcome-set-surface", ExperimentID: "outcome-set-expansion-shock", ExperimentVersion: 1,
			Source: "official venue event membership + current side-specific books", SchemaVersion: r138EvidenceSchemaVersion,
			ZeroPolicy:      "every fetched set is stored, including unchanged controls; zero requires no official event snapshots",
			ExpectedCadence: 2 * time.Minute, Systems: []string{"outcome-set-expansion-shock", "payoff-constraint-solver"}},
		{CollectorID: "deadline-hazard-certified", ExperimentID: "deadline-hazard-surface", ExperimentVersion: 1,
			Source: "verified canonical implication relations + simultaneous current books", SchemaVersion: r138EvidenceSchemaVersion,
			ZeroPolicy:      "healthy empty only when the immutable graph has no verified deadline implication pair",
			ExpectedCadence: 5 * time.Minute, Systems: []string{"deadline-hazard-surface", "payoff-constraint-solver"}},
		{CollectorID: "score-state-surface", ExperimentID: "score-state-surface", ExperimentVersion: 1,
			Source: "structural market-to-game joins + current executable books", SchemaVersion: r138EvidenceSchemaVersion,
			ZeroPolicy:      "every selected linked game is a stored control; missing books remain an explicit exclusion",
			ExpectedCadence: 5 * time.Minute, Systems: []string{"score-state-surface"}},
		{CollectorID: "replenishment-toxicity", ExperimentID: "replenishment-fingerprint", ExperimentVersion: 1,
			Source: "existing Kalshi sequenced WS books + authoritative aggressor tape; no added subscriptions", SchemaVersion: r138EvidenceSchemaVersion,
			ZeroPolicy:      "zero is healthy only when no fresh complete two-sided subscribed book exists",
			ExpectedCadence: 5 * time.Second, Systems: []string{"replenishment-fingerprint", "flow-direction-integrity", "maker-salvage-matched-cohort"}},
		{CollectorID: "step7-event-microstructure", Version: 3,
			ExperimentID: "replenishment-fingerprint", ExperimentVersion: 3,
			Source: R139Step7EventMicrostructureSource, SchemaVersion: "step7-event-microstructure-v3",
			ZeroPolicy:      "zero is healthy only when no depletion/flow summary is due and no frozen projection job is pending; every raw row has one immutable terminal or candidate/control payload",
			ExpectedCadence: 5 * time.Second, Systems: []string{"replenishment-fingerprint", "flow-direction-integrity"}},
		{CollectorID: "step7-maker-salvage", ExperimentID: "maker-salvage-matched-cohort", ExperimentVersion: 1,
			Source: R139Step7MakerSalvageSource, SchemaVersion: "step7-maker-salvage-v1",
			ZeroPolicy:      "zero is healthy only when no natural maker attempts exist; cancel and no-order controls are exact zero",
			ExpectedCadence: time.Minute, Systems: []string{"maker-salvage-matched-cohort"}},
		{CollectorID: "incentive-economics", ExperimentID: "incentive-subsidized-structural-lock", ExperimentVersion: 1,
			Source: "official Kalshi incentive programs + current book + exact maker fee; uncredited reward lower bound is zero", SchemaVersion: r138EvidenceSchemaVersion,
			ZeroPolicy:      "zero is healthy only after a successful official active-program crawl returns empty",
			ExpectedCadence: 15 * time.Minute, Systems: []string{"incentive-subsidized-structural-lock", "maker-salvage-matched-cohort"}},
		{CollectorID: "polyus-incentive-economics", Version: 2, ExperimentID: "incentive-subsidized-structural-lock", ExperimentVersion: 1,
			Source: "official public PolyUS GET /v1/incentives + current WS books + exact maker fees; credited earnings read separately", SchemaVersion: "polyus-incentives-v0.0.69",
			ZeroPolicy:      "zero is healthy only after the bounded active-program crawl completes and every absence/exclusion is receipted; advertised rewards remain zero until credited",
			ExpectedCadence: 15 * time.Minute, Systems: []string{"incentive-subsidized-structural-lock", "maker-salvage-matched-cohort"}},
		{CollectorID: "official-release-adapters", Version: 3, ExperimentID: "deadline-hazard-surface", ExperimentVersion: 1,
			Source:          "NOAA NBM station bulletin + Deribit BTC/ETH summaries joined to exact option metadata and frozen risk-neutral threshold surfaces + cached NWS gridpoint forecast, each with explicit source-clock provenance",
			SchemaVersion:   "r139-official-release-v3",
			ZeroPolicy:      "never silently empty: operational source failures are errors; valid source rows and explicit station/instrument exclusions are retained",
			ExpectedCadence: 30 * time.Minute, Systems: []string{"deadline-hazard-surface", "score-state-surface"}},
		{CollectorID: "payoff-envelope-capacity", ExperimentID: "payoff-constraint-solver", ExperimentVersion: 1,
			Source: "verified payoff certificate + actual depth levels + exact per-level route fees", SchemaVersion: r138EvidenceSchemaVersion,
			ZeroPolicy:      "zero candidates is healthy; every executable negative/control solution is retained",
			ExpectedCadence: 2 * time.Minute, Systems: []string{"payoff-constraint-solver", "deadline-hazard-surface", "incentive-subsidized-structural-lock"}},
		{CollectorID: "concrete-attention-horizons", ExperimentID: "attention-spillover-graph", ExperimentVersion: 1,
			Source: R139ConcreteAttentionSource, SchemaVersion: "attention-executable-markout-v1",
			ZeroPolicy:      "zero is healthy only when no linked attention observation has a 60s or 300s executable mark due; every miss keeps its reason",
			ExpectedCadence: 5 * time.Second, Systems: []string{"attention-spillover-graph"}},
		{CollectorID: "system-contract-coverage", Source: "runtime collector/evidence contract audit for every immutable system ID",
			SchemaVersion:   r138EvidenceSchemaVersion,
			ZeroPolicy:      "never empty: every one of the 19 systems must be COLLECTING, COLLECTING_PARTIAL, or BLOCKED with prerequisites",
			ExpectedCadence: 5 * time.Minute, Systems: ResearchExperimentIDs()},
	}
	return append(specs, R139SystemFunnelSpecs()...)
}

func R139SystemFunnelSpecs() []CollectorSpec {
	row := func(id, system, source, zero string) CollectorSpec {
		return CollectorSpec{CollectorID: id, ExperimentID: system, ExperimentVersion: 1,
			Source: source, SchemaVersion: "r139-system-funnel-v1", ZeroPolicy: zero,
			ExpectedCadence: 5 * time.Minute, Systems: []string{system}}
	}
	return []CollectorSpec{
		row("attention-spillover-funnel", "attention-spillover-graph", "source-native lifecycle transitions and pre/post book controls; no parent-child graph join", "zero is healthy only when no lifecycle transition/control row exists; graph eligibility remains a separate blocker"),
		row("clientele-clock-funnel", "clientele-clock-basis", "structural cross-venue game identities with UTC catalog last_seen; no venue-local hour or audience join", "zero is healthy only when no structurally shared cross-venue game exists"),
		row("collateral-release-funnel", "collateral-release-rotation", "settled one-share grade-close controls; authoritative collateral credit timestamp and amount are absent", "zero is healthy only when no settled route grade exists; grade close never substitutes for credit time"),
		row("deadline-source-funnel", "deadline-hazard-surface", "immutable official release frames with explicit source-clock status; no verified payoff relation or executable market-book join", "zero is healthy only when no official release frame exists; no market candidate is inferred"),
		row("proper-score-system-funnel", "proper-score-executor", "book-native proper-score trial mirror with current book and fee fields; no added order authority", "zero is healthy only when the native proper-score ledger has no trial rows"),
		row("forecast-persona-funnel", "forecast-persona-router", "book-native proper-score persona inputs; no frozen persona-selection policy is inferred", "zero is healthy only when the native proper-score ledger has no persona input rows"),
		row("semantic-complexity-funnel", "semantic-complexity-premium", "immutable official notice/rule/schema artifacts with predeclared complexity inputs", "zero is healthy only when no official notice artifact exists"),
		row("paired-bridge-funnel", "paired-bridge-inversion", "book-native bridge/gap unit routes retained for direct/inverse pairing", "zero is healthy only when no bridge route has a book-native input"),
		row("side-crowding-funnel", "side-normalized-crowding-fade", "book-native signals with bought-side flow, whale/concentration, and depth coordinates", "zero is healthy only when no book-native crowding input exists"),
		row("series-roll-funnel", "series-roll-anchor", "source-native lifecycle transitions and pre/post books; no prior-issue fair-anchor join", "zero is healthy only when no lifecycle transition/control row exists"),
		row("settlement-carry-funnel", "settlement-latency-carry", "settled one-share grade-close controls; determination and collateral-credit clocks are absent", "zero is healthy only when no settled route grade exists; grade close never substitutes for credit time"),
		row("identity-lock-funnel", "identity-challenged-cross-venue-lock", "unified cross-venue route ledger including identity rejections and exact fee/quote provenance", "zero is healthy only when no cross-venue route or rejection exists"),
	}
}

// These are the exact immutable contracts shipped in R138. Fresh databases must register them
// before the R139 v2 adapters so audit/replay can distinguish the blocked original contract from
// the now-validated implementation. An upgraded database already has these rows; registration is
// idempotent and detects any byte-level drift.
func r138Step6To9LegacyCollectorSpecs() []CollectorSpec {
	return []CollectorSpec{
		{CollectorID: "step7-event-microstructure", Version: 1,
			ExperimentID: "replenishment-fingerprint", ExperimentVersion: 1,
			Source:          "every existing subscribed Kalshi touch delta + public trade; bounded memory callback and episode/label summaries only",
			SchemaVersion:   "step7-event-microstructure-v1",
			ZeroPolicy:      "zero is healthy only when no depletion/flow summary is due; every callback and budget drop is receipted",
			ExpectedCadence: 5 * time.Second, Systems: []string{"replenishment-fingerprint", "flow-direction-integrity"}},
		{CollectorID: "polyus-incentive-economics", Version: 1,
			ExperimentID: "incentive-subsidized-structural-lock", ExperimentVersion: 1,
			Source: "official PolyUS GET /v1/incentives; no reward table fallback", SchemaVersion: "polyus-incentives-v0.0.69",
			ZeroPolicy:      "blocked until programs[].marketSlug/timePeriods[].{programId,programType,start,end,rewardPool,discountFactor,targetSize,period,createdAt}, nextPageToken, statuses query filtering, and earned-allocation receipts are implemented",
			ExpectedCadence: 15 * time.Minute, Systems: []string{"incentive-subsidized-structural-lock"}},
		{CollectorID: "official-release-adapters", Version: 1,
			ExperimentID: "deadline-hazard-surface", ExperimentVersion: 1,
			Source:          "cached authoritative public releases with exact clock provenance; blocked adapters are persisted",
			SchemaVersion:   r138EvidenceSchemaVersion,
			ZeroPolicy:      "never silently empty: NBM/options remain blocked until an exact-clock validated adapter exists",
			ExpectedCadence: 30 * time.Minute, Systems: []string{"deadline-hazard-surface", "score-state-surface"}},
		{CollectorID: "official-release-adapters", Version: 2,
			ExperimentID: "deadline-hazard-surface", ExperimentVersion: 1,
			Source:          "NOAA NBM station bulletin + Deribit BTC/ETH option summaries + cached NWS gridpoint forecast, each with explicit source-clock provenance",
			SchemaVersion:   "r139-official-release-v2",
			ZeroPolicy:      "never silently empty: operational source failures are errors; valid source rows and explicit station/instrument exclusions are retained",
			ExpectedCadence: 30 * time.Minute, Systems: []string{"deadline-hazard-surface", "score-state-surface"}},
	}
}

func r138Step8LegacySourceClockSpecs() []SourceClockSpec {
	return []SourceClockSpec{{SourceID: "deribit-option-summary", DisplayName: "Deribit public option summary adapter",
		AuthorityURL: "https://docs.deribit.com/api-reference/market-data/public-get_book_summary_by_currency",
		Version:      1, SchemaVersion: "deribit-option-summary-blocked-v1",
		SchemaHash: sourceSchemaFingerprint("deribit-option-summary-blocked-v1", "instrument_name", "mark_iv", "underlying_index", "open_interest"),
		ClockKind:  "arrival_only", Timezone: "UTC", ExpectedCadence: 30 * time.Minute, GapTolerance: 2 * time.Hour,
		RevisionPolicy:          "adapter remains blocked until source-time semantics and a frozen prediction-market mapping are validated",
		SettlementCompatibility: "not a settlement source; independent volatility research input only",
		CachePolicy:             "no runtime requests while blocked"},
		{SourceID: "deribit-option-summary", DisplayName: "Deribit public option summary adapter",
			AuthorityURL: "https://docs.deribit.com/api-reference/market-data/public-get_book_summary_by_currency",
			Version:      2, SchemaVersion: "deribit-option-summary-v2",
			SchemaHash: sourceSchemaFingerprint("deribit-option-summary-v2", "usOut", "instrument_name", "creation_timestamp", "mark_iv", "underlying_index", "open_interest"),
			ClockKind:  "source_timestamp", TimestampField: "usOut", Timezone: "UTC", ExpectedCadence: 30 * time.Minute, GapTolerance: 2 * time.Hour,
			RevisionPolicy:          "retain source-timestamped BTC/ETH summaries; prediction-market payoff mapping remains a separate frozen experiment input",
			SettlementCompatibility: "not a settlement source; independent volatility research input only",
			CachePolicy:             "bounded public response; retain top open-interest rows and immutable artifact hash"}}
}

func r138Step8SourceClockSpecs() []SourceClockSpec {
	return []SourceClockSpec{
		{SourceID: "nws-hourly-gridpoint", DisplayName: "NWS API hourly gridpoint forecast",
			AuthorityURL: "https://www.weather.gov/documentation/services-web-api", SchemaVersion: "nws-gridpoint-hourly-v1",
			SchemaHash: sourceSchemaFingerprint("nws-gridpoint-hourly-v1", "properties.periods.startTime", "properties.periods.temperature", "properties.periods.temperatureUnit"),
			ClockKind:  "arrival_only", Timezone: "UTC", ExpectedCadence: 30 * time.Minute, GapTolerance: 2 * time.Hour,
			RevisionPolicy:          "retain every changed cached forecast artifact; the API payload currently exposes valid periods but no trusted model issuance timestamp",
			SettlementCompatibility: "forecast input only; the venue-designated NWS CLI observation remains settlement authority",
			CachePolicy:             "reuse the existing bounded weather cache; never add a second request loop"},
		{SourceID: "deribit-option-summary", DisplayName: "Deribit public option summary adapter",
			AuthorityURL: "https://docs.deribit.com/api-reference/market-data/public-get_book_summary_by_currency", Version: 3, SchemaVersion: "deribit-option-threshold-surface-v3",
			SchemaHash: sourceSchemaFingerprint("deribit-option-threshold-surface-v3", "usOut", "instrument_name", "mark_iv", "underlying_price", "open_interest", "expiration_timestamp", "strike", "option_type", "price_index", "risk_neutral_probability_above"),
			ClockKind:  "source_timestamp", TimestampField: "usOut", Timezone: "UTC", ExpectedCadence: 30 * time.Minute, GapTolerance: 2 * time.Hour,
			RevisionPolicy:          "join each source-timestamped BTC/ETH summary to exact get_instruments metadata; retain the frozen zero-rate Black-Scholes risk-neutral threshold surface and contributor envelope; prediction-market identity/deadline/source equality remains a separate fail-closed join",
			SettlementCompatibility: "not a settlement source; independent volatility research input only",
			CachePolicy:             "two bounded public responses per currency every 30 minutes; retain top open-interest summaries plus immutable threshold-surface artifacts"},
	}
}

func R138Step8SourceClockSpec(sourceID string) (SourceClockSpec, bool) {
	for _, spec := range r138Step8SourceClockSpecs() {
		if spec.SourceID == sourceID {
			if spec.Version <= 0 {
				spec.Version = 1
			}
			spec.Active = true
			return spec, true
		}
	}
	return SourceClockSpec{}, false
}

func (s *Store) EnsureR138Step6To9CollectorBlueprints(ctx context.Context) error {
	if err := s.ensureR138SystemObservationTruthColumns(ctx); err != nil {
		return err
	}
	if err := s.ensureR138MicrostructureProvenanceColumns(ctx); err != nil {
		return err
	}
	for _, spec := range r138Step6To9LegacyCollectorSpecs() {
		if _, err := s.RegisterCollectorSpec(ctx, spec); err != nil {
			return err
		}
	}
	for _, spec := range R138Step6To9CollectorSpecs() {
		if _, err := s.RegisterCollectorSpec(ctx, spec); err != nil {
			return err
		}
	}
	for _, spec := range r138Step8LegacySourceClockSpecs() {
		if _, _, err := s.RegisterSourceClockSpec(ctx, spec); err != nil {
			return err
		}
	}
	for _, spec := range r138Step8SourceClockSpecs() {
		if spec.Version <= 0 {
			spec.Version = 1
		}
		if _, _, err := s.RegisterSourceClockSpec(ctx, spec); err != nil {
			return err
		}
	}
	// This upgrade used to run first as one CASE-heavy full-ledger GROUP BY. On the production
	// ledger it consumed almost the entire 15-second blueprint budget, so an interrupt prevented
	// every collector contract after it from registering. Register the immutable contracts first,
	// then use the selective bootstrap below. A transient deadline at this final maintenance step
	// schedules a restart-safe retry; it must not make otherwise-valid collectors fail closed.
	if err := s.ensureR144ResearchSystemCollectionTotals(ctx); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			s.startR144ResearchSystemCollectionTotalsRetry()
			return nil
		}
		return err
	}
	return nil
}

// ensureR144ResearchSystemCollectionTotals bootstraps the insert-maintained counter table exactly
// once for upgraded databases. The migration is one short IMMEDIATE transaction: writers either
// commit before its DELETE+recount and are included in the recount, or commit afterwards and are
// included by the INSERT trigger. A cancellation rolls the whole transaction back (including the
// latch), so a retry is exact and can never double-count.
//
// The first implementation put control and economic predicates inside four CASE expressions over
// every ledger row. That forced SQLite to evaluate the correlated payoff lookup across ~2M rows.
// These two selective passes preserve the identical truth contract while letting SQLite discard
// observer rows before the payoff lookup. On the production copy this reduced the expensive pass
// from 12.77s to ~1.44s and the control pass to ~0.51s.
func (s *Store) ensureR144ResearchSystemCollectionTotals(ctx context.Context) error {
	const latch = "r144_research_system_collection_totals_v1"
	var done int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM kv WHERE k=?`, latch).Scan(&done); err != nil {
		return fmt.Errorf("check R144 collection totals latch: %w", err)
	}
	if done > 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	// The outer read avoids taking a write lock on every normal boot. Recheck after BEGIN
	// IMMEDIATE so two startup callers that both observed an absent latch serialize cleanly: the
	// second caller sees the first caller's commit and becomes a no-op instead of recounting.
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM kv WHERE k=?`, latch).Scan(&done); err != nil {
		return fmt.Errorf("recheck R144 collection totals latch: %w", err)
	}
	if done > 0 {
		return tx.Commit()
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM research_system_collection_totals`); err != nil {
		return fmt.Errorf("reset R144 collection totals: %w", err)
	}
	// Controls do not need executable economics. Count them alone so the economic pass never has
	// to CASE-test the ~98% observer/control majority of the historical ledger.
	if _, err := tx.ExecContext(ctx, `INSERT INTO research_system_collection_totals(
system_id,economic_observations,candidates,controls,open_economic)
SELECT system_id,0,0,COUNT(*),0
FROM research_system_observations
WHERE observation_kind='control'
GROUP BY system_id`); err != nil {
		return fmt.Errorf("bootstrap R144 control totals: %w", err)
	}
	const economicTruth = `o.route!='observer' AND TRIM(o.source_clock_id)!='' AND
TRIM(o.book_source)!='' AND TRIM(o.fee_source)!='' AND o.size_units>0 AND o.executable_cost>0 AND
o.latency_known=1 AND o.quote_age_known=1 AND o.tick_known=1 AND o.depth_known=1 AND o.fee_known=1`
	if _, err := tx.ExecContext(ctx, `INSERT INTO research_system_collection_totals(
system_id,economic_observations,candidates,controls,open_economic)
SELECT o.system_id,COUNT(*),COALESCE(SUM(o.candidate=1),0),0,
COALESCE(SUM(o.outcome_status='open' AND NOT EXISTS(
  SELECT 1 FROM research_system_payoff_updates u WHERE u.observation_id=o.id
)),0)
FROM research_system_observations o
WHERE `+economicTruth+`
GROUP BY o.system_id
ON CONFLICT(system_id) DO UPDATE SET
 economic_observations=excluded.economic_observations,
 candidates=excluded.candidates,
 open_economic=excluded.open_economic`); err != nil {
		return fmt.Errorf("bootstrap R144 economic totals: %w", err)
	}
	// R175 raw report counters share this compact table. A database can legitimately reach this
	// older bootstrap after Open has already installed/backfilled the newer columns; because the
	// R144 recount starts with DELETE, repopulate the raw fields in this same exact snapshot rather
	// than erasing them while leaving the newer latch behind.
	if _, err := tx.ExecContext(ctx, `INSERT INTO research_system_collection_totals(
system_id,controls,raw_observations,raw_candidates,raw_negative,raw_open_envelopes)
SELECT system_id,
 COALESCE(SUM(observation_kind='control'),0),
 COUNT(*),
 COALESCE(SUM(candidate=1),0),
 COALESCE(SUM(observation_kind='negative'),0),
 COALESCE(SUM(outcome_status='open'),0)
FROM research_system_observations
GROUP BY system_id
ON CONFLICT(system_id) DO UPDATE SET
 controls=excluded.controls,
 raw_observations=excluded.raw_observations,
 raw_candidates=excluded.raw_candidates,
 raw_negative=excluded.raw_negative,
 raw_open_envelopes=excluded.raw_open_envelopes`); err != nil {
		return fmt.Errorf("bootstrap R144/R175 raw totals: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO kv(k,v,ts) VALUES(?,?,?)`,
		r175SystemRawTotalsLatch, "done", nowRFC()); err != nil {
		return fmt.Errorf("latch R144/R175 raw totals: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO kv(k,v,ts) VALUES(?,?,?)`, latch, "done", nowRFC()); err != nil {
		return fmt.Errorf("latch R144 collection totals: %w", err)
	}
	return tx.Commit()
}

// startR144ResearchSystemCollectionTotalsRetry owns the rare deadline/contention path. It retries
// with bounded transactions and backoff until the atomic latch commits or Store.Close cancels it.
// Keeping this off the collector initialization context means a reporting migration can warm in
// the background without disabling the collectors that generate its future rows.
func (s *Store) startR144ResearchSystemCollectionTotalsRetry() {
	s.r144CollectionBootstrapMu.Lock()
	if s.r144CollectionBootstrapRunning {
		s.r144CollectionBootstrapMu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.r144CollectionBootstrapRunning = true
	s.r144CollectionBootstrapCancel = cancel
	s.r144CollectionBootstrapWG.Add(1)
	s.r144CollectionBootstrapMu.Unlock()

	go func() {
		defer s.r144CollectionBootstrapWG.Done()
		defer func() {
			s.r144CollectionBootstrapMu.Lock()
			s.r144CollectionBootstrapRunning = false
			s.r144CollectionBootstrapCancel = nil
			s.r144CollectionBootstrapMu.Unlock()
		}()
		backoff := 250 * time.Millisecond
		for {
			attemptCtx, attemptCancel := context.WithTimeout(ctx, 12*time.Second)
			err := s.ensureR144ResearchSystemCollectionTotals(attemptCtx)
			attemptCancel()
			if err == nil || ctx.Err() != nil {
				return
			}
			timer := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			if backoff < 10*time.Second {
				backoff *= 2
			}
		}
	}()
}

func (s *Store) ensureR138MicrostructureProvenanceColumns(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx, `PRAGMA table_info(research_microstructure_frames)`)
	if err != nil {
		return err
	}
	existing := map[string]bool{}
	for rows.Next() {
		var cid, notNull, pk int
		var name, typ string
		var dflt any
		if err := rows.Scan(&cid, &name, &typ, &notNull, &dflt, &pk); err != nil {
			rows.Close()
			return err
		}
		existing[name] = true
	}
	if err := rows.Close(); err != nil {
		return err
	}
	columns := map[string]string{
		"received_ts":            "TEXT NOT NULL DEFAULT ''",
		"source_channel":         "TEXT NOT NULL DEFAULT ''",
		"source_generation":      "INTEGER NOT NULL DEFAULT 0 CHECK(source_generation>=0)",
		"source_subscription_id": "INTEGER NOT NULL DEFAULT 0 CHECK(source_subscription_id>=0)",
		"source_sequence":        "INTEGER NOT NULL DEFAULT 0 CHECK(source_sequence>=0)",
	}
	for _, name := range []string{"received_ts", "source_channel", "source_generation", "source_subscription_id", "source_sequence"} {
		if existing[name] {
			continue
		}
		if _, err := s.db.ExecContext(ctx, `ALTER TABLE research_microstructure_frames ADD COLUMN `+name+` `+columns[name]); err != nil {
			return fmt.Errorf("add microstructure provenance column %s: %w", name, err)
		}
	}
	return nil
}

func (s *Store) ensureR138SystemObservationTruthColumns(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx, `PRAGMA table_info(research_system_observations)`)
	if err != nil {
		return err
	}
	existing := map[string]bool{}
	for rows.Next() {
		var cid, notNull, pk int
		var name, typ string
		var dflt any
		if err := rows.Scan(&cid, &name, &typ, &notNull, &dflt, &pk); err != nil {
			rows.Close()
			return err
		}
		existing[name] = true
	}
	if err := rows.Close(); err != nil {
		return err
	}
	columns := map[string]string{
		"decision_latency_ms": "REAL NOT NULL DEFAULT 0 CHECK(decision_latency_ms>=0)",
		"latency_known":       "INTEGER NOT NULL DEFAULT 0 CHECK(latency_known IN (0,1))",
		"quote_age_known":     "INTEGER NOT NULL DEFAULT 0 CHECK(quote_age_known IN (0,1))",
		"tick_known":          "INTEGER NOT NULL DEFAULT 0 CHECK(tick_known IN (0,1))",
		"depth_known":         "INTEGER NOT NULL DEFAULT 0 CHECK(depth_known IN (0,1))",
		"fee_known":           "INTEGER NOT NULL DEFAULT 0 CHECK(fee_known IN (0,1))",
	}
	for _, name := range []string{"decision_latency_ms", "latency_known", "quote_age_known", "tick_known", "depth_known", "fee_known"} {
		if existing[name] {
			continue
		}
		if _, err := s.db.ExecContext(ctx, `ALTER TABLE research_system_observations ADD COLUMN `+name+` `+columns[name]); err != nil {
			return fmt.Errorf("add research system truth column %s: %w", name, err)
		}
	}
	return nil
}

type CapacityPoint struct {
	Size        float64 `json:"size"`
	Cost        float64 `json:"cost"`
	Fee         float64 `json:"fee"`
	PayoutFloor float64 `json:"payout_floor"`
	NetFloor    float64 `json:"net_floor"`
}

type ResearchSystemObservation struct {
	Observed, DecisionAt                                 time.Time
	SystemID, OpportunityID, Kind, Cohort                string
	CanonicalEventID, Venue, Route, Side                 string
	Ticker, CanonicalPayoffID                            string
	CertificateStatus, CertificateHash                   string
	SourceClockID, SourceArtifact, BookSource, FeeSource string
	Blocker, OutcomeStatus                               string
	ExperimentVersion, EventVersion, PayoffVersion       int
	InstrumentVersion                                    int
	QuoteAgeMax, TickMin, Size, Cost, Fee                float64
	PayoutLower, PayoutUpper, NetLower, NetUpper         float64
	VisibleCapacity, CapitalSeconds                      float64
	DecisionLatencyMS                                    float64
	CapacityCurve                                        []CapacityPoint
	Inputs                                               any
	Candidate, LatencyKnown, QuoteAgeKnown               bool
	TickKnown, DepthKnown, FeeKnown                      bool
}

func researchSystemIDKnown(id string) bool {
	for _, known := range ResearchExperimentIDs() {
		if id == known {
			return true
		}
	}
	return false
}

func finiteR138(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }

func firstNonEmptyStorage(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

// redundantFlowLabelObserver is the exact historical Step-7 double-write that is already owned in
// full by research_flow_direction_pairs. Keep this deliberately narrow: other observer controls
// (especially cross-venue identity/rule acceptances and rejections) are first-class route evidence.
func redundantFlowLabelObserver(o ResearchSystemObservation) bool {
	if o.SystemID != "flow-direction-integrity" || o.Route != "observer" || o.Kind != "control" ||
		o.Blocker != "label_audit_only_no_trade_authority" {
		return false
	}
	return o.Cohort == "kalshi-authoritative-aggressor-v1" || o.Cohort == "kalshi-inferred-aggressor-v1"
}

func validateCapacityCurve(points []CapacityPoint) error {
	lastSize, lastCost := 0.0, -1.0
	for _, p := range points {
		if !finiteR138(p.Size) || !finiteR138(p.Cost) || !finiteR138(p.Fee) ||
			!finiteR138(p.PayoutFloor) || !finiteR138(p.NetFloor) || p.Size <= lastSize ||
			p.Cost < 0 || p.Cost+1e-12 < lastCost || p.PayoutFloor < 0 ||
			p.NetFloor > p.PayoutFloor-p.Cost-p.Fee+1e-9 {
			return errors.New("invalid capacity curve")
		}
		lastSize, lastCost = p.Size, p.Cost
	}
	return nil
}

// InsertResearchSystemObservation retains the original current-instrument behavior used by
// ordinary collectors. Frozen projection jobs use insertResearchSystemObservationTx with the
// exact instrument version captured in their immutable payload instead.
func (s *Store) InsertResearchSystemObservation(ctx context.Context, o ResearchSystemObservation) (int64, bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, false, err
	}
	defer tx.Rollback()
	id, inserted, err := s.insertResearchSystemObservationTx(ctx, tx, o, -1)
	if err != nil {
		return 0, false, err
	}
	if err := tx.Commit(); err != nil {
		return 0, false, err
	}
	return id, inserted, nil
}

// insertResearchSystemObservationTx inserts an observation and its unified route mirror in the
// caller's transaction. instrumentVersion < 0 means resolve the current immutable instrument
// version (the legacy/public mode); zero is valid only for observers; a positive value requires
// that exact historical instrument version and never switches to a newer current version.
func (s *Store) insertResearchSystemObservationTx(ctx context.Context, tx *sql.Tx, o ResearchSystemObservation,
	instrumentVersion int) (int64, bool, error) {
	claimedInstrumentVersion := o.InstrumentVersion
	o.SystemID, o.OpportunityID = strings.TrimSpace(o.SystemID), strings.TrimSpace(o.OpportunityID)
	o.Kind, o.Route = strings.ToLower(strings.TrimSpace(o.Kind)), strings.ToLower(strings.TrimSpace(o.Route))
	o.CertificateStatus = strings.ToLower(strings.TrimSpace(o.CertificateStatus))
	if !researchSystemIDKnown(o.SystemID) || o.OpportunityID == "" ||
		(o.Kind != "candidate" && o.Kind != "negative" && o.Kind != "control") ||
		(o.Route != "observer" && o.Route != "taker" && o.Route != "maker" && o.Route != "maker-control" && o.Route != "rfq") ||
		(o.CertificateStatus != "verified" && o.CertificateStatus != "structural" && o.CertificateStatus != "unverified" &&
			o.CertificateStatus != "rejected" && o.CertificateStatus != "not_applicable") {
		return 0, false, errors.New("invalid research system identity")
	}
	if o.ExperimentVersion <= 0 {
		o.ExperimentVersion = 1
	}
	if o.EventVersion < 0 || o.QuoteAgeMax < 0 || o.TickMin < 0 || o.Size < 0 || o.Cost < 0 ||
		o.VisibleCapacity < 0 || o.CapitalSeconds < 0 || o.DecisionLatencyMS < 0 || o.PayoutLower > o.PayoutUpper || o.NetLower > o.NetUpper {
		return 0, false, errors.New("invalid research system bounds")
	}
	for _, v := range []float64{o.QuoteAgeMax, o.TickMin, o.Size, o.Cost, o.Fee, o.PayoutLower,
		o.PayoutUpper, o.NetLower, o.NetUpper, o.VisibleCapacity, o.CapitalSeconds, o.DecisionLatencyMS} {
		if !finiteR138(v) {
			return 0, false, errors.New("non-finite research system value")
		}
	}
	// Net bounds may be more conservative than the raw payoff-cost-fee identity (partial-fill,
	// opportunity-cost and censor reserves), but can never be rosier than it.
	if o.NetLower > o.PayoutLower-o.Cost-o.Fee+1e-9 || o.NetUpper > o.PayoutUpper-o.Cost-o.Fee+1e-9 {
		return 0, false, errors.New("research net envelope overstates payoff envelope")
	}
	if err := validateCapacityCurve(o.CapacityCurve); err != nil {
		return 0, false, err
	}
	if o.Observed.IsZero() {
		o.Observed = time.Now()
	}
	decisionClockProvided := !o.DecisionAt.IsZero()
	if !decisionClockProvided {
		o.DecisionAt = o.Observed
	} else if o.DecisionAt.Before(o.Observed) {
		return 0, false, errors.New("research decision clock precedes observation clock")
	}
	if decisionClockProvided && o.LatencyKnown &&
		math.Abs(o.DecisionAt.Sub(o.Observed).Seconds()*1000-o.DecisionLatencyMS) > 1 {
		return 0, false, errors.New("research decision clock contradicts measured latency")
	}
	if o.OutcomeStatus == "" {
		o.OutcomeStatus = "open"
	}
	if o.OutcomeStatus != "open" && o.OutcomeStatus != "settled" && o.OutcomeStatus != "voided" && o.OutcomeStatus != "censored" {
		return 0, false, errors.New("invalid payoff status")
	}
	if o.Route != "observer" && (o.BookSource == "" || o.FeeSource == "" || o.TickMin <= 0 || o.Size <= 0) {
		return 0, false, errors.New("route observation lacks book, fee, tick, or size truth")
	}
	if (o.QuoteAgeKnown && strings.TrimSpace(o.BookSource) == "") || (o.TickKnown && o.TickMin <= 0) ||
		(o.DepthKnown && o.VisibleCapacity < 0) || (o.FeeKnown && strings.TrimSpace(o.FeeSource) == "") {
		return 0, false, errors.New("research system known flag contradicts its evidence")
	}
	allRouteTruthKnown := o.LatencyKnown && o.QuoteAgeKnown && o.TickKnown && o.DepthKnown && o.FeeKnown
	if o.Candidate {
		if o.Kind != "candidate" || o.CertificateStatus != "verified" ||
			(o.Route != "taker" && o.Route != "maker" && o.Route != "rfq") || o.Blocker != "" ||
			!allRouteTruthKnown || strings.TrimSpace(o.SourceClockID) == "" ||
			strings.TrimSpace(o.BookSource) == "" || strings.TrimSpace(o.FeeSource) == "" ||
			o.TickMin <= 0 || o.VisibleCapacity < o.Size || o.Size <= 0 {
			return 0, false, errors.New("candidate bypasses preregistered executable-action contract")
		}
	} else if o.Kind == "candidate" {
		return 0, false, errors.New("candidate kind must set candidate")
	}
	curve, err := json.Marshal(o.CapacityCurve)
	if err != nil {
		return 0, false, err
	}
	inputs := []byte("{}")
	if o.Inputs != nil {
		inputs, err = json.Marshal(o.Inputs)
		if err != nil {
			return 0, false, err
		}
	}
	slot := researchSlot(o.Observed, 5*time.Minute)
	// A non-observer route is eligible for later terminal inference only when it freezes the exact
	// immutable venue-instrument -> event/payoff mapping. Ordinary inserts resolve the latest
	// version inside this transaction. Projection retries pass their captured positive version and
	// must continue to use that exact receipt even if a newer version is now current.
	if o.Route != "observer" {
		venue, ticker := strings.ToLower(strings.TrimSpace(o.Venue)), strings.TrimSpace(o.Ticker)
		if venue == "" || ticker == "" {
			return 0, false, errors.New("economic route observation lacks venue instrument identity")
		}
		if instrumentVersion == 0 {
			return 0, false, errors.New("economic route observation lacks frozen instrument version")
		}
		if claimedInstrumentVersion > 0 && instrumentVersion >= 0 &&
			claimedInstrumentVersion != instrumentVersion {
			return 0, false, errors.New("economic route observation conflicts with frozen instrument version")
		}
		var eventID, payoffID string
		var eventVersion, payoffVersion int
		query := `SELECT version,event_id,event_version,payoff_id,payoff_version
FROM research_instrument_specs i WHERE venue=? AND ticker=?`
		args := []any{venue, ticker}
		if instrumentVersion < 0 {
			query += ` AND version=(SELECT MAX(v.version) FROM research_instrument_specs v
 WHERE v.venue=i.venue AND v.ticker=i.ticker)`
		} else {
			query += ` AND version=?`
			args = append(args, instrumentVersion)
		}
		query += ` LIMIT 1`
		if err := tx.QueryRowContext(ctx, query, args...).Scan(&o.InstrumentVersion, &eventID,
			&eventVersion, &payoffID, &payoffVersion); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				if instrumentVersion < 0 {
					return 0, false, errors.New("economic route observation has no current canonical instrument/event/payoff version")
				}
				return 0, false, errors.New("economic route observation has no requested canonical instrument/event/payoff version")
			}
			return 0, false, err
		}
		if claimedInstrumentVersion > 0 && claimedInstrumentVersion != o.InstrumentVersion {
			return 0, false, errors.New("economic route observation conflicts with resolved instrument version")
		}
		if o.InstrumentVersion <= 0 {
			return 0, false, errors.New("economic route observation resolved invalid instrument version")
		}
		if (o.CanonicalEventID != "" && o.CanonicalEventID != eventID) ||
			(o.EventVersion > 0 && o.EventVersion != eventVersion) ||
			(o.CanonicalPayoffID != "" && o.CanonicalPayoffID != payoffID) ||
			(o.PayoffVersion > 0 && o.PayoffVersion != payoffVersion) {
			if instrumentVersion < 0 {
				return 0, false, errors.New("economic route observation conflicts with current canonical instrument/event/payoff version")
			}
			return 0, false, errors.New("economic route observation conflicts with current canonical instrument/event/payoff version")
		}
		o.CanonicalEventID, o.EventVersion = eventID, eventVersion
		o.CanonicalPayoffID, o.PayoffVersion = payoffID, payoffVersion
	} else {
		if instrumentVersion > 0 || o.InstrumentVersion > 0 {
			return 0, false, errors.New("observer projection cannot claim an instrument version")
		}
		o.InstrumentVersion = 0
	}
	res, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO research_system_observations(
observed_ts,decision_ts,observed_slot,system_id,experiment_version,opportunity_id,observation_kind,cohort,
canonical_event_id,event_version,canonical_payoff_id,payoff_version,instrument_version,venue,ticker,route,side,certificate_status,certificate_hash,source_clock_id,
source_artifact,book_source,fee_source,quote_age_max_s,tick_min,size_units,executable_cost,exact_fee,
payout_lower,payout_upper,net_lower,net_upper,visible_capacity,capital_seconds,decision_latency_ms,
latency_known,quote_age_known,tick_known,depth_known,fee_known,capacity_curve_json,inputs_json,
outcome_status,blocker,candidate,funded,paper_authority,live_authority)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0,0,0)`,
		o.Observed.UTC().Format(time.RFC3339Nano), o.DecisionAt.UTC().Format(time.RFC3339Nano),
		slot, o.SystemID, o.ExperimentVersion, o.OpportunityID,
		o.Kind, o.Cohort, o.CanonicalEventID, o.EventVersion, nullableResearchString(o.CanonicalPayoffID),
		nullableResearchVersion(o.PayoffVersion), o.InstrumentVersion, strings.ToLower(o.Venue), o.Ticker, o.Route,
		strings.ToUpper(o.Side), o.CertificateStatus, o.CertificateHash, o.SourceClockID,
		o.SourceArtifact, o.BookSource, o.FeeSource, o.QuoteAgeMax, o.TickMin, o.Size, o.Cost, o.Fee,
		o.PayoutLower, o.PayoutUpper, o.NetLower, o.NetUpper, o.VisibleCapacity, o.CapitalSeconds,
		o.DecisionLatencyMS, boolInt(o.LatencyKnown), boolInt(o.QuoteAgeKnown), boolInt(o.TickKnown),
		boolInt(o.DepthKnown), boolInt(o.FeeKnown), string(curve), string(inputs), o.OutcomeStatus,
		o.Blocker, boolInt(o.Candidate))
	if err != nil {
		return 0, false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, false, err
	}
	id := int64(0)
	if n > 0 {
		id, err = res.LastInsertId()
		if err != nil {
			return 0, false, err
		}
	} else {
		// INSERT OR IGNORE is safe only for a byte-equivalent semantic replay. The legacy UNIQUE
		// key predates experiment/instrument/cohort/side versioning; accepting any collision would
		// let an old row suppress a frozen projection and falsely complete its outbox job.
		err = tx.QueryRowContext(ctx, `SELECT id FROM research_system_observations
WHERE system_id=? AND opportunity_id=? AND observed_slot=? AND route=? AND size_units=?
 AND observation_kind=? AND observed_ts=?
 AND COALESCE(NULLIF(decision_ts,''),observed_ts)=? AND experiment_version=? AND cohort=?
 AND canonical_event_id=? AND event_version=? AND canonical_payoff_id IS ?
 AND payoff_version IS ? AND instrument_version=? AND venue=? AND ticker=? AND side=?
 AND certificate_status=? AND certificate_hash=? AND source_clock_id=? AND source_artifact=?
 AND book_source=? AND fee_source=? AND quote_age_max_s=? AND tick_min=?
 AND executable_cost=? AND exact_fee=? AND payout_lower=? AND payout_upper=?
 AND net_lower=? AND net_upper=? AND visible_capacity=? AND capital_seconds=?
 AND decision_latency_ms=? AND latency_known=? AND quote_age_known=? AND tick_known=?
 AND depth_known=? AND fee_known=? AND capacity_curve_json=? AND inputs_json=?
 AND outcome_status=? AND blocker=? AND candidate=?`,
			o.SystemID, o.OpportunityID, slot, o.Route, o.Size, o.Kind,
			o.Observed.UTC().Format(time.RFC3339Nano), o.DecisionAt.UTC().Format(time.RFC3339Nano),
			o.ExperimentVersion, o.Cohort,
			o.CanonicalEventID, o.EventVersion, nullableResearchString(o.CanonicalPayoffID),
			nullableResearchVersion(o.PayoffVersion), o.InstrumentVersion, strings.ToLower(o.Venue),
			o.Ticker, strings.ToUpper(o.Side), o.CertificateStatus, o.CertificateHash,
			o.SourceClockID, o.SourceArtifact, o.BookSource, o.FeeSource, o.QuoteAgeMax, o.TickMin,
			o.Cost, o.Fee, o.PayoutLower, o.PayoutUpper, o.NetLower, o.NetUpper,
			o.VisibleCapacity, o.CapitalSeconds, o.DecisionLatencyMS, boolInt(o.LatencyKnown),
			boolInt(o.QuoteAgeKnown), boolInt(o.TickKnown), boolInt(o.DepthKnown), boolInt(o.FeeKnown),
			string(curve), string(inputs), o.OutcomeStatus, o.Blocker, boolInt(o.Candidate)).Scan(&id)
		if errors.Is(err, sql.ErrNoRows) {
			return 0, false, errors.New("conflicting research system observation duplicate")
		}
		if err != nil {
			return 0, false, err
		}
	}
	// These two Step-7 flow label cohorts are already preserved field-for-field in the compact
	// research_flow_direction_pairs ledger. Mirroring them here used to duplicate the same JSON-heavy
	// evidence without adding a route, price, fill, or settlement fact; the production ledger owns
	// more than two million such copies. All other observer controls still mirror normally because
	// cross-venue identity/rule acceptances and rejections are canonical route evidence.
	if redundantFlowLabelObserver(o) {
		return id, n > 0, nil
	}
	// Mirror the exact same immutable decision into the unified route ledger in this transaction.
	// Multi-leg total cost may exceed the route ledger's single-share price field; that remains an
	// explicit blocked route with total cost preserved in evidence rather than a clipped fake price.
	var evidence map[string]any
	_ = json.Unmarshal(inputs, &evidence)
	if evidence == nil {
		evidence = map[string]any{}
	}
	evidence["system_observation_id"] = id
	evidence["executable_cost_total"] = o.Cost
	evidence["capacity_curve"] = o.CapacityCurve
	evidence["blocker"] = o.Blocker
	evidence["outcome_status"] = o.OutcomeStatus
	evidence["decision_latency_ms"] = o.DecisionLatencyMS
	evidence["decision_at"] = o.DecisionAt.UTC().Format(time.RFC3339Nano)
	evidence["decision_latency_known"] = o.LatencyKnown
	evidence["quote_age_known"] = o.QuoteAgeKnown
	evidence["tick_known"] = o.TickKnown
	evidence["depth_known"] = o.DepthKnown
	evidence["fee_known"] = o.FeeKnown
	evidenceJSON, _ := json.Marshal(evidence)
	identity := o.CertificateStatus
	if identity == "not_applicable" {
		identity = "unverified"
	}
	venue := strings.ToLower(strings.TrimSpace(o.Venue))
	if venue == "" {
		venue = "none"
	}
	side := strings.ToUpper(strings.TrimSpace(o.Side))
	if side == "" {
		side = "NONE"
	}
	route, action := o.Route, "reject"
	switch route {
	case "observer":
		route, action = "abstain", "control"
	case "maker-control":
		route, action = "control", "control"
	case "maker":
		action = "post"
	case "taker":
		action = "buy"
	case "rfq":
		action = "quote"
	}
	decision := "blocked"
	if o.Kind == "control" {
		decision = "control"
	}
	price := 0.0
	if o.Size > 0 {
		price = o.Cost / o.Size
	}
	decisionReason := o.Blocker
	if decisionReason == "" {
		decisionReason = "retained prospective research observation; zero authority"
	}
	if price > 1+1e-9 {
		price, action, decision = 0, "reject", "blocked"
		decisionReason = "multi-leg total cost is preserved in evidence; no false single-share price"
	} else if o.Candidate && allRouteTruthKnown {
		decision = "candidate"
	} else if o.Candidate {
		decision = "blocked"
		decisionReason = "positive structural lower bound retained, but decision latency was not measured"
	}
	if (action == "buy" || action == "post" || action == "quote") &&
		(strings.TrimSpace(o.Ticker) == "" || side == "NONE" || price <= 0 || o.VisibleCapacity <= 0 || o.Size <= 0) {
		action, decision = "reject", "blocked"
		decisionReason = "route identity/depth is incomplete; exact totals remain in immutable evidence"
	}
	quoteSource := o.BookSource
	if quoteSource == "" {
		quoteSource = firstNonEmptyStorage(o.SourceArtifact, "research-observer")
	}
	feeAmount, rebateAmount := o.Fee, 0.0
	if feeAmount < 0 {
		rebateAmount, feeAmount = -feeAmount, 0
	}
	feeAuthority := o.FeeSource
	if feeAuthority == "" {
		feeAuthority = "not_applicable"
	}
	partialWorst := -o.Cost - math.Max(o.Fee, 0)
	routeOpportunityID := ResearchRouteStableID(o.SystemID, o.OpportunityID, slot)
	routeID := ResearchRouteStableID(routeOpportunityID, o.Route, o.Side,
		fmt.Sprintf("%.9f", o.Size), o.Kind)
	_, err = insertResearchRouteOpportunityWith(ctx, tx, ResearchRouteOpportunity{
		OpportunityID: routeOpportunityID, RouteID: routeID, Observed: o.Observed, DecisionAt: o.DecisionAt,
		ExperimentID: o.SystemID, ExperimentVersion: o.ExperimentVersion, SystemName: o.SystemID,
		CanonicalEventID: o.CanonicalEventID, CanonicalPayoffID: o.CanonicalPayoffID,
		IdentityStatus: identity, Venue: venue, Ticker: o.Ticker, Side: side, Route: route, Action: action,
		QuoteSource: quoteSource, QuoteSequence: o.SourceClockID, QuoteAgeSeconds: o.QuoteAgeMax,
		QuoteAgeKnown: o.QuoteAgeKnown, DecisionLatencyMS: o.DecisionLatencyMS,
		LatencyKnown: o.LatencyKnown, TickSize: o.TickMin, TickKnown: o.TickKnown,
		ExecutablePrice: price, ExecutableDepth: o.VisibleCapacity, DepthKnown: o.DepthKnown,
		RequestedQty: o.Size, FeeAmount: feeAmount, RebateAmount: rebateAmount,
		FeeAuthority: feeAuthority, FeeKnown: o.FeeKnown,
		ExpectedPayoutLow: o.PayoutLower, ExpectedPayoutHigh: o.PayoutUpper,
		ExpectedNetLow: o.NetLower, ExpectedNetHigh: o.NetUpper, PartialFillWorst: partialWorst,
		CapitalSeconds: o.CapitalSeconds, Decision: decision, DecisionReason: decisionReason,
		AlternativeGroup: o.SystemID + "|" + o.OpportunityID, EvidenceJSON: string(evidenceJSON),
	})
	if err != nil {
		return 0, false, fmt.Errorf("unified route mirror: %w", err)
	}
	return id, n > 0, nil
}

func nullableResearchString(v string) any {
	if strings.TrimSpace(v) == "" {
		return nil
	}
	return strings.TrimSpace(v)
}

func nullableResearchVersion(v int) any {
	if v <= 0 {
		return nil
	}
	return v
}

type ResearchPayoffUpdate struct {
	ObservationID                      int64
	Observed                           time.Time
	Status                             string
	PayoutLower, PayoutUpper           float64
	RealizedNet                        *float64
	SourceArtifact, SourceHash, Reason string
}

func (s *Store) AppendResearchPayoffUpdate(ctx context.Context, u ResearchPayoffUpdate) (bool, error) {
	u.Status = strings.ToLower(strings.TrimSpace(u.Status))
	if u.ObservationID <= 0 || (u.Status != "settled" && u.Status != "voided" && u.Status != "censored") ||
		u.PayoutLower > u.PayoutUpper || u.SourceArtifact == "" || u.SourceHash == "" ||
		!finiteR138(u.PayoutLower) || !finiteR138(u.PayoutUpper) || (u.RealizedNet != nil && !finiteR138(*u.RealizedNet)) {
		return false, errors.New("invalid payoff update")
	}
	if u.Status == "censored" && u.PayoutLower == u.PayoutUpper {
		return false, errors.New("censored payoff cannot collapse to a fabricated point")
	}
	if u.Observed.IsZero() {
		u.Observed = time.Now()
	}
	res, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO research_system_payoff_updates(
observation_id,observed_ts,status,payout_lower,payout_upper,realized_net,source_artifact,source_hash,
reason,funded,paper_authority,live_authority) VALUES(?,?,?,?,?,?,?,?,?,0,0,0)`, u.ObservationID,
		u.Observed.UTC().Format(time.RFC3339Nano), u.Status, u.PayoutLower, u.PayoutUpper,
		u.RealizedNet, u.SourceArtifact, u.SourceHash, u.Reason)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil || n == 0 {
		return n > 0, err
	}
	var systemID, opportunityID, slot, route, side, kind, cohort, blocker string
	var size float64
	if err := s.db.QueryRowContext(ctx, `SELECT system_id,opportunity_id,observed_slot,route,side,size_units,observation_kind,cohort,blocker
FROM research_system_observations WHERE id=?`, u.ObservationID).Scan(&systemID, &opportunityID, &slot,
		&route, &side, &size, &kind, &cohort, &blocker); err == nil {
		// The two compact flow-label cohorts never own a route opportunity. Their terminal/censor
		// update remains fully preserved in research_system_payoff_updates and therefore must not
		// fabricate a grade event whose foreign key points at a deliberately suppressed duplicate.
		if redundantFlowLabelObserver(ResearchSystemObservation{SystemID: systemID, Route: route,
			Kind: kind, Cohort: cohort, Blocker: blocker}) {
			return true, nil
		}
		routeOpportunityID := ResearchRouteStableID(systemID, opportunityID, slot)
		routeID := ResearchRouteStableID(routeOpportunityID, route, side, fmt.Sprintf("%.9f", size), kind)
		var payout *float64
		if u.PayoutLower == u.PayoutUpper {
			v := u.PayoutLower
			payout = &v
		}
		evidence, _ := json.Marshal(map[string]any{"payoff_lower": u.PayoutLower,
			"payoff_upper": u.PayoutUpper, "source_hash": u.SourceHash})
		if _, routeErr := s.AppendResearchRouteEvent(ctx, ResearchRouteEvent{
			OpportunityID: routeOpportunityID, RouteID: routeID, EventType: "grade", Observed: u.Observed,
			Payout: payout, RealizedNet: u.RealizedNet, OutcomeStatus: u.Status, Reason: u.Reason,
			EvidenceJSON: string(evidence),
		}); routeErr != nil {
			return true, fmt.Errorf("unified route grade mirror: %w", routeErr)
		}
	}
	return true, nil
}

type ResearchBookLevel struct {
	Price float64 `json:"price"`
	Size  float64 `json:"size"`
}

type ResearchMicrostructureFrame struct {
	Observed, Received, CloseTime                          time.Time
	Ticker, CanonicalEventID, MarketStatus, BookSource     string
	FeeSource, SourceChannel                               string
	SourceGeneration, SourceSubscriptionID, SourceSequence int64
	QuoteAge, TickSize                                     float64
	YesBid, YesAsk, YesBidDepth, YesAskDepth               float64
	BidLevels, AskLevels                                   []ResearchBookLevel
	SignedFlow10, FlowUnits10, SignedFlow60, FlowUnits60   float64
	YesTakerFee, NoTakerFee, YesMakerFee, NoMakerFee       float64
}

func canonicalBookHash(tick float64, bids, asks []ResearchBookLevel) (string, []byte, []byte, error) {
	b, err := json.Marshal(bids)
	if err != nil {
		return "", nil, nil, err
	}
	a, err := json.Marshal(asks)
	if err != nil {
		return "", nil, nil, err
	}
	payload, _ := json.Marshal(struct {
		Tick float64             `json:"tick"`
		Bids []ResearchBookLevel `json:"bids"`
		Asks []ResearchBookLevel `json:"asks"`
	}{tick, bids, asks})
	h := sha256.Sum256(payload)
	return hex.EncodeToString(h[:]), b, a, nil
}

func classifyBookTransition(priorHash string, priorBid, priorAsk, priorBidDepth, priorAskDepth float64,
	curHash string, curBid, curAsk, curBidDepth, curAskDepth float64) string {
	if priorHash == "" {
		return "baseline"
	}
	if priorHash == curHash {
		return "unchanged"
	}
	depleted := func(prev, cur float64) bool { return prev-cur >= math.Max(1, .25*prev) }
	refilled := func(prev, cur float64) bool { return cur-prev >= math.Max(1, .25*math.Max(prev, 1)) }
	bd := curBid < priorBid-1e-12 || (math.Abs(curBid-priorBid) <= 1e-12 && depleted(priorBidDepth, curBidDepth))
	ad := curAsk > priorAsk+1e-12 || (math.Abs(curAsk-priorAsk) <= 1e-12 && depleted(priorAskDepth, curAskDepth))
	br := curBid > priorBid+1e-12 || (math.Abs(curBid-priorBid) <= 1e-12 && refilled(priorBidDepth, curBidDepth))
	ar := curAsk < priorAsk-1e-12 || (math.Abs(curAsk-priorAsk) <= 1e-12 && refilled(priorAskDepth, curAskDepth))
	classes := []string{}
	for name, yes := range map[string]bool{"bid_depletion": bd, "ask_depletion": ad, "bid_refill": br, "ask_refill": ar} {
		if yes {
			classes = append(classes, name)
		}
	}
	if len(classes) == 1 {
		return classes[0]
	}
	return "mixed"
}

func validateResearchLevels(levels []ResearchBookLevel, bids bool) error {
	if len(levels) == 0 || len(levels) > 10 {
		return errors.New("invalid exported depth")
	}
	last := 0.0
	for i, l := range levels {
		if l.Price <= 0 || l.Price >= 1 || l.Size <= 0 || !finiteR138(l.Price) || !finiteR138(l.Size) {
			return errors.New("invalid book level")
		}
		if i > 0 && ((bids && l.Price >= last) || (!bids && l.Price <= last)) {
			return errors.New("book levels not best-first")
		}
		last = l.Price
	}
	return nil
}

type researchMicrostructureExecer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type ResearchMicrostructureInsertResult struct {
	ID         int64
	Transition string
	Inserted   bool
}

func insertResearchMicrostructureFrameWith(ctx context.Context, q researchMicrostructureExecer,
	f ResearchMicrostructureFrame) (int64, string, bool, error) {
	if f.Observed.IsZero() || f.Received.IsZero() || f.SourceChannel == "" || f.SourceGeneration <= 0 ||
		f.SourceSubscriptionID <= 0 || f.SourceSequence <= 0 || f.Received.Before(f.Observed) {
		return 0, "", false, errors.New("microstructure frame lacks exact source/receipt sequence provenance")
	}
	if f.Ticker == "" || f.BookSource == "" || f.FeeSource == "" || f.QuoteAge < 0 || f.TickSize <= 0 ||
		f.YesBid <= 0 || f.YesAsk <= f.YesBid || f.YesAsk >= 1 || f.YesBidDepth < 0 || f.YesAskDepth < 0 ||
		f.FlowUnits10 < 0 || f.FlowUnits60 < 0 {
		return 0, "", false, errors.New("invalid microstructure frame")
	}
	if err := validateResearchLevels(f.BidLevels, true); err != nil {
		return 0, "", false, err
	}
	if err := validateResearchLevels(f.AskLevels, false); err != nil {
		return 0, "", false, err
	}
	for _, fee := range []float64{f.YesTakerFee, f.NoTakerFee, f.YesMakerFee, f.NoMakerFee} {
		if !finiteR138(fee) || fee <= -1 || fee >= 1 {
			return 0, "", false, errors.New("invalid route fee")
		}
	}
	hash, bids, asks, err := canonicalBookHash(f.TickSize, f.BidLevels, f.AskLevels)
	if err != nil {
		return 0, "", false, err
	}
	var priorHash string
	var priorBid, priorAsk, priorBidDepth, priorAskDepth float64
	err = q.QueryRowContext(ctx, `SELECT book_hash,yes_bid,yes_ask,yes_bid_depth,yes_ask_depth
FROM research_microstructure_frames WHERE ticker=? ORDER BY id DESC LIMIT 1`, f.Ticker).
		Scan(&priorHash, &priorBid, &priorAsk, &priorBidDepth, &priorAskDepth)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return 0, "", false, err
	}
	transition := classifyBookTransition(priorHash, priorBid, priorAsk, priorBidDepth, priorAskDepth,
		hash, f.YesBid, f.YesAsk, f.YesBidDepth, f.YesAskDepth)
	closeTS := ""
	if !f.CloseTime.IsZero() {
		closeTS = f.CloseTime.UTC().Format(time.RFC3339Nano)
	}
	res, err := q.ExecContext(ctx, `INSERT OR IGNORE INTO research_microstructure_frames(
observed_ts,received_ts,source_channel,source_generation,source_subscription_id,source_sequence,
ticker,canonical_event_id,market_status,close_ts,book_source,quote_age_s,tick_size,
yes_bid,yes_ask,yes_bid_depth,yes_ask_depth,bid_levels_json,ask_levels_json,book_hash,prior_book_hash,
transition_class,signed_flow_10s,flow_units_10s,signed_flow_60s,flow_units_60s,yes_taker_fee_1,
no_taker_fee_1,yes_maker_fee_1,no_maker_fee_1,fee_source,funded,paper_authority,live_authority)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0,0,0)`,
		f.Observed.UTC().Format(time.RFC3339Nano), f.Received.UTC().Format(time.RFC3339Nano),
		f.SourceChannel, f.SourceGeneration, f.SourceSubscriptionID, f.SourceSequence,
		f.Ticker, f.CanonicalEventID, f.MarketStatus, closeTS,
		f.BookSource, f.QuoteAge, f.TickSize, f.YesBid, f.YesAsk, f.YesBidDepth, f.YesAskDepth,
		string(bids), string(asks), hash, priorHash, transition, f.SignedFlow10, f.FlowUnits10,
		f.SignedFlow60, f.FlowUnits60, f.YesTakerFee, f.NoTakerFee, f.YesMakerFee, f.NoMakerFee, f.FeeSource)
	if err != nil {
		return 0, transition, false, err
	}
	n, err := res.RowsAffected()
	if err != nil || n == 0 {
		return 0, transition, false, err
	}
	id, err := res.LastInsertId()
	return id, transition, err == nil, err
}

func (s *Store) InsertResearchMicrostructureFrame(ctx context.Context, f ResearchMicrostructureFrame) (int64, string, bool, error) {
	return insertResearchMicrostructureFrameWith(ctx, s.db, f)
}

// InsertResearchMicrostructureFrames commits one explicitly bounded frame batch. Callers still
// own sampling and durable row budgets; this method rejects an accidental unbounded write storm.
func (s *Store) InsertResearchMicrostructureFrames(ctx context.Context,
	frames []ResearchMicrostructureFrame) ([]ResearchMicrostructureInsertResult, error) {
	if len(frames) == 0 {
		return nil, nil
	}
	if len(frames) > 60 {
		return nil, errors.New("microstructure batch exceeds 60-frame hard limit")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	out := make([]ResearchMicrostructureInsertResult, 0, len(frames))
	for _, frame := range frames {
		id, transition, inserted, err := insertResearchMicrostructureFrameWith(ctx, tx, frame)
		if err != nil {
			return nil, err
		}
		out = append(out, ResearchMicrostructureInsertResult{ID: id, Transition: transition, Inserted: inserted})
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return out, nil
}

// ResearchMicrostructureFrameCounts is the durable guard against restart-reset sampling budgets.
// The daily bound remains a hard sampler budget; the lifetime count is the hot working set and is
// reduced only by ArchiveResearchMicrostructure's two-phase move.
func (s *Store) ResearchMicrostructureFrameCounts(ctx context.Context, day time.Time) (total, today int, err error) {
	day = time.Date(day.UTC().Year(), day.UTC().Month(), day.UTC().Day(), 0, 0, 0, 0, time.UTC)
	err = s.db.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(SUM(observed_ts>=?),0)
FROM research_microstructure_frames`, day.Format(time.RFC3339Nano)).Scan(&total, &today)
	return
}

type ResearchMicrostructureDue struct {
	FrameID, Horizon int64
	Observed         time.Time
	Ticker           string
}

func (s *Store) ResearchMicrostructureHorizonsDue(ctx context.Context, now time.Time, limit int) ([]ResearchMicrostructureDue, error) {
	if limit <= 0 || limit > 1000 {
		limit = 200
	}
	rows, err := s.db.QueryContext(ctx, `WITH horizons(h) AS (VALUES(5),(30),(300))
SELECT f.id,h.h,f.observed_ts,f.ticker FROM research_microstructure_frames f CROSS JOIN horizons h
LEFT JOIN research_microstructure_horizons x ON x.frame_id=f.id AND x.horizon_s=h.h
WHERE x.id IS NULL AND f.observed_ts<=? AND f.observed_ts>=?
ORDER BY f.id,h.h LIMIT ?`, now.Add(-5*time.Second).UTC().Format(time.RFC3339Nano),
		now.Add(-20*time.Minute).UTC().Format(time.RFC3339Nano), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ResearchMicrostructureDue
	for rows.Next() {
		var r ResearchMicrostructureDue
		var observed string
		if err := rows.Scan(&r.FrameID, &r.Horizon, &observed, &r.Ticker); err != nil {
			return nil, err
		}
		r.Observed, _ = time.Parse(time.RFC3339Nano, observed)
		out = append(out, r)
	}
	return out, rows.Err()
}

type ResearchMicrostructureHorizon struct {
	FrameID, Horizon                int64
	Captured                        time.Time
	Status, Reason                  string
	YesBid, YesAsk, NoBid, NoAsk    *float64
	YesExitFee, NoExitFee, QuoteAge *float64
}

func (s *Store) InsertResearchMicrostructureHorizon(ctx context.Context, h ResearchMicrostructureHorizon) (bool, error) {
	if h.FrameID <= 0 || h.Horizon <= 0 || (h.Status != "captured" && h.Status != "missed") {
		return false, errors.New("invalid microstructure horizon")
	}
	if h.Captured.IsZero() {
		h.Captured = time.Now()
	}
	if h.Status == "captured" {
		vals := []*float64{h.YesBid, h.YesAsk, h.NoBid, h.NoAsk, h.YesExitFee, h.NoExitFee, h.QuoteAge}
		for _, v := range vals {
			if v == nil || !finiteR138(*v) {
				return false, errors.New("captured horizon lacks exact book/fee")
			}
		}
		if *h.YesBid <= 0 || *h.YesAsk <= *h.YesBid || *h.YesAsk >= 1 || *h.NoBid <= 0 ||
			*h.NoAsk <= *h.NoBid || *h.NoAsk >= 1 || *h.QuoteAge < 0 {
			return false, errors.New("invalid captured horizon book")
		}
	} else if h.Reason == "" {
		return false, errors.New("missed horizon needs reason")
	}
	res, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO research_microstructure_horizons(
frame_id,horizon_s,captured_ts,status,yes_bid,yes_ask,no_bid,no_ask,yes_exit_fee_1,no_exit_fee_1,
quote_age_s,reason,funded,paper_authority,live_authority) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,0,0,0)`,
		h.FrameID, h.Horizon, h.Captured.UTC().Format(time.RFC3339Nano), h.Status, h.YesBid, h.YesAsk,
		h.NoBid, h.NoAsk, h.YesExitFee, h.NoExitFee, h.QuoteAge, h.Reason)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

type OutcomeSetMember struct {
	Ticker string  `json:"ticker"`
	Status string  `json:"status"`
	Result string  `json:"result,omitempty"`
	Bid    float64 `json:"yes_bid,omitempty"`
	Ask    float64 `json:"yes_ask,omitempty"`
	Depth  float64 `json:"touch_depth,omitempty"`
}

type OutcomeSetFrame struct {
	Observed, SourceUpdated                        time.Time
	Venue, EventID, Title, SourceArtifact, Blocker string
	Members                                        []OutcomeSetMember
	MutuallyExclusive, Exhaustive, VoidVerified    bool
}

type OutcomeSetInsertResult struct {
	ID, Total, Executable int64
	ChangeClass           string
	Inserted              bool
}

func memberMap(rows []OutcomeSetMember) map[string]OutcomeSetMember {
	out := make(map[string]OutcomeSetMember, len(rows))
	for _, r := range rows {
		out[r.Ticker] = r
	}
	return out
}

func (s *Store) InsertOutcomeSetFrame(ctx context.Context, f OutcomeSetFrame) (OutcomeSetInsertResult, error) {
	if f.Observed.IsZero() {
		f.Observed = time.Now()
	}
	if f.Venue == "" || f.EventID == "" || f.SourceArtifact == "" || len(f.Members) < 2 {
		return OutcomeSetInsertResult{}, errors.New("invalid outcome-set frame")
	}
	sort.Slice(f.Members, func(i, j int) bool { return f.Members[i].Ticker < f.Members[j].Ticker })
	seen := map[string]bool{}
	executable := 0
	for _, m := range f.Members {
		if m.Ticker == "" || seen[m.Ticker] || m.Bid < 0 || m.Ask < 0 || m.Depth < 0 {
			return OutcomeSetInsertResult{}, errors.New("invalid outcome-set member")
		}
		seen[m.Ticker] = true
		if m.Bid > 0 && m.Ask > m.Bid && m.Ask < 1 && m.Depth >= 1 {
			executable++
		}
	}
	b, _ := json.Marshal(f.Members)
	h := sha256.Sum256(b)
	hash := hex.EncodeToString(h[:])
	var priorHash, priorJSON string
	err := s.db.QueryRowContext(ctx, `SELECT membership_hash,members_json FROM research_outcome_set_frames
WHERE venue=? AND event_id=? ORDER BY id DESC LIMIT 1`, strings.ToLower(f.Venue), f.EventID).Scan(&priorHash, &priorJSON)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return OutcomeSetInsertResult{}, err
	}
	var prior []OutcomeSetMember
	_ = json.Unmarshal([]byte(priorJSON), &prior)
	old, cur := memberMap(prior), memberMap(f.Members)
	added, removed, changed := []string{}, []string{}, []string{}
	for ticker, m := range cur {
		p, ok := old[ticker]
		if !ok {
			added = append(added, ticker)
		} else if p.Status != m.Status || p.Result != m.Result {
			changed = append(changed, ticker)
		}
	}
	for ticker := range old {
		if _, ok := cur[ticker]; !ok {
			removed = append(removed, ticker)
		}
	}
	sort.Strings(added)
	sort.Strings(removed)
	sort.Strings(changed)
	change := "unchanged"
	if priorHash == "" {
		change = "baseline"
	} else if len(added) > 0 && len(removed) == 0 && len(changed) == 0 {
		change = "expanded"
	} else if len(removed) > 0 && len(added) == 0 && len(changed) == 0 {
		change = "contracted"
	} else if len(changed) > 0 && len(added) == 0 && len(removed) == 0 {
		change = "status_change"
	} else if len(added)+len(removed)+len(changed) > 0 {
		change = "mixed"
	}
	addedJSON, _ := json.Marshal(added)
	removedJSON, _ := json.Marshal(removed)
	changedJSON, _ := json.Marshal(changed)
	sourceUpdated := ""
	if !f.SourceUpdated.IsZero() {
		sourceUpdated = f.SourceUpdated.UTC().Format(time.RFC3339Nano)
	}
	res, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO research_outcome_set_frames(
observed_ts,observed_slot,venue,event_id,title,source_updated_ts,source_artifact,membership_hash,
prior_membership_hash,members_json,added_json,removed_json,changed_json,change_class,mutually_exclusive,
exhaustive,void_policy_verified,executable_members,total_members,blocker,funded,paper_authority,live_authority)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0,0,0)`, f.Observed.UTC().Format(time.RFC3339Nano),
		researchSlot(f.Observed, 5*time.Minute), strings.ToLower(f.Venue), f.EventID, f.Title, sourceUpdated,
		f.SourceArtifact, hash, priorHash, string(b), string(addedJSON), string(removedJSON), string(changedJSON),
		change, boolInt(f.MutuallyExclusive), boolInt(f.Exhaustive), boolInt(f.VoidVerified), executable,
		len(f.Members), f.Blocker)
	if err != nil {
		return OutcomeSetInsertResult{}, err
	}
	n, err := res.RowsAffected()
	if err != nil || n == 0 {
		return OutcomeSetInsertResult{Total: int64(len(f.Members)), Executable: int64(executable), ChangeClass: change}, err
	}
	id, err := res.LastInsertId()
	return OutcomeSetInsertResult{ID: id, Total: int64(len(f.Members)), Executable: int64(executable), ChangeClass: change, Inserted: err == nil}, err
}

type OfficialReleaseFrame struct {
	Observed, SourceTime, ValidTime                  time.Time
	SourceID, ArtifactID, SchemaVersion, ClockStatus string
	ArtifactHash, Blocker                            string
	Values                                           any
	SettlementCompatible                             bool
}

type ResearchSystemRuntimeReceipt struct {
	Observed                     time.Time
	SystemID, State, Reason      string
	CollectorIDs, EvidenceTables []string
	Prerequisites                []string
}

// ResearchSystemRuntimeView is shared by the runtime-coverage and evidence surfaces so a
// registered zero-row system retains the same current liveness truth everywhere.
type ResearchSystemRuntimeView struct {
	SystemID, State, Reason, Observed           string
	CollectorIDs, EvidenceTables, Prerequisites []string
	Funded, PaperAuthority, LiveAuthority       bool
}

// effectiveResearchSystemRuntimeState derives the read-time state from the latest collector
// receipts. Runtime coverage receipts are append-only historical observations; they must not keep
// rendering COLLECTING after the collectors that justified that state have missed three cadences.
func effectiveResearchSystemRuntimeState(state, reason string, collectorIDs []string,
	byCollector map[string]CollectorLivenessView) (string, string, []string) {
	var blockers []string
	for _, id := range cleanStringSet(collectorIDs) {
		view, ok := byCollector[id]
		if !ok {
			blockers = append(blockers, id+": active collector contract/receipt missing")
			continue
		}
		if !view.Alert && !view.BootWarming {
			continue
		}
		detail := view.Status
		switch {
		case view.NeverRan:
			detail = "never ran"
		case view.BootWarming:
			detail = fmt.Sprintf("warming: no completion since process boot (bounded grace %.0fs)", view.BootGraceS)
		case view.NeverRanThisBoot:
			detail = "no completion since process boot"
		case view.EffectiveCadenceS > 0 && view.LagSeconds > 3*view.EffectiveCadenceS:
			detail = fmt.Sprintf("stale %.0fs (three-cadence limit %.0fs; effective scheduler cadence %.0fs)",
				view.LagSeconds, 3*view.EffectiveCadenceS, view.EffectiveCadenceS)
		case strings.TrimSpace(view.ErrorText) != "":
			detail = view.Status + ": " + view.ErrorText
		case strings.TrimSpace(view.ZeroReason) != "":
			detail = view.Status + ": " + view.ZeroReason
		}
		blockers = append(blockers, id+": "+detail)
	}
	if len(blockers) == 0 {
		return state, reason, nil
	}
	return "BLOCKED", "read-time collector liveness block: " + strings.Join(blockers, "; "), blockers
}

func (s *Store) InsertResearchSystemRuntimeReceipt(ctx context.Context, r ResearchSystemRuntimeReceipt) (bool, error) {
	r.SystemID, r.State, r.Reason = strings.TrimSpace(r.SystemID), strings.ToUpper(strings.TrimSpace(r.State)), strings.TrimSpace(r.Reason)
	if !researchSystemIDKnown(r.SystemID) || (r.State != "COLLECTING" && r.State != "COLLECTING_PARTIAL" && r.State != "BLOCKED") || r.Reason == "" {
		return false, errors.New("invalid system runtime receipt")
	}
	if r.State == "BLOCKED" && len(r.Prerequisites) == 0 {
		return false, errors.New("blocked system lacks exact prerequisites")
	}
	if r.Observed.IsZero() {
		r.Observed = time.Now()
	}
	r.CollectorIDs, r.EvidenceTables, r.Prerequisites = cleanStringSet(r.CollectorIDs), cleanStringSet(r.EvidenceTables), cleanStringSet(r.Prerequisites)
	collectors, _ := json.Marshal(r.CollectorIDs)
	tables, _ := json.Marshal(r.EvidenceTables)
	prereqs, _ := json.Marshal(r.Prerequisites)
	res, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO research_system_runtime_receipts(
observed_ts,observed_slot,system_id,state,collector_ids_json,evidence_tables_json,reason,
prerequisites_json,funded,paper_authority,live_authority) VALUES(?,?,?,?,?,?,?,?,0,0,0)`,
		r.Observed.UTC().Format(time.RFC3339Nano), researchSlot(r.Observed, 5*time.Minute), r.SystemID,
		r.State, string(collectors), string(tables), r.Reason, string(prereqs))
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

func (s *Store) ResearchSystemRuntimeReport(ctx context.Context) (map[string]any, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT r.system_id,r.state,r.collector_ids_json,r.evidence_tables_json,
r.reason,r.prerequisites_json,r.observed_ts,r.funded,r.paper_authority,r.live_authority
FROM research_system_runtime_receipts r WHERE r.id=(SELECT MAX(x.id) FROM research_system_runtime_receipts x WHERE x.system_id=r.system_id)
ORDER BY r.system_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ResearchSystemRuntimeView
	for rows.Next() {
		var v ResearchSystemRuntimeView
		var collectors, tables, prereqs string
		var funded, paper, live int
		if err := rows.Scan(&v.SystemID, &v.State, &collectors, &tables, &v.Reason, &prereqs,
			&v.Observed, &funded, &paper, &live); err != nil {
			return nil, err
		}
		_ = json.Unmarshal([]byte(collectors), &v.CollectorIDs)
		_ = json.Unmarshal([]byte(tables), &v.EvidenceTables)
		_ = json.Unmarshal([]byte(prereqs), &v.Prerequisites)
		v.Funded, v.PaperAuthority, v.LiveAuthority = funded != 0, paper != 0, live != 0
		out = append(out, v)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	liveness, err := s.CollectorLivenessReport(ctx)
	if err != nil {
		return nil, err
	}
	collectorViews, ok := liveness["collectors"].([]CollectorLivenessView)
	if !ok {
		return nil, errors.New("collector liveness report has unexpected shape")
	}
	byCollector := make(map[string]CollectorLivenessView, len(collectorViews))
	for _, collector := range collectorViews {
		byCollector[collector.CollectorID] = collector
	}
	counts := map[string]int{"COLLECTING": 0, "COLLECTING_PARTIAL": 0, "BLOCKED": 0}
	for i := range out {
		state, reason, blockers := effectiveResearchSystemRuntimeState(out[i].State, out[i].Reason,
			out[i].CollectorIDs, byCollector)
		out[i].State, out[i].Reason = state, reason
		if len(blockers) > 0 {
			out[i].Prerequisites = cleanStringSet(append(out[i].Prerequisites, blockers...))
		}
		counts[out[i].State]++
	}
	return map[string]any{"expected_systems": 19, "reported_systems": len(out), "counts": counts,
		"systems": out, "registry_alone_counts_as_implementation": false,
		"funded": false, "paper_authority": false, "live_authority": false}, nil
}

// ResearchSystemCollectionStat is the canonical input-funnel view used by the Systems tab and
// briefing. InputRows are durable collector/evidence rows, not settled bets, and therefore never
// enter P&L, proof, paper, or LIVE authority. Keeping those counters separate prevents an active
// collector from rendering as misleading n=0 while also preventing source rows from masquerading
// as economic outcomes.
type ResearchSystemCollectionStat struct {
	SystemID, State, Reason, Observed            string
	CollectorIDs                                 []string
	InputRows, InputCycles, EconomicObservations int
	Candidates, Controls, Open, CollectorAlerts  int
	// CurrentCycle* is activity from one latest bounded collector receipt. It is not
	// cumulative evidence and must never be added to economic observations, open rows, or n.
	CurrentCycleMatches, CurrentCycleInserted      int
	CurrentCycleDuplicates, CurrentCycleCandidates int
}

// compactCollectorLivenessLatest reads only the active contract and latest append-only receipt for
// collectors referenced by the compact system rows. It deliberately excludes lifetime aggregates:
// the digest needs current stale/error/boot truth, not another scan of collector history.
func (s *Store) compactCollectorLivenessLatest(ctx context.Context, collectorIDs []string) (map[string]CollectorLivenessView, error) {
	collectorIDs = cleanStringSet(collectorIDs)
	if len(collectorIDs) == 0 {
		return map[string]CollectorLivenessView{}, nil
	}
	args := make([]any, len(collectorIDs))
	for i := range collectorIDs {
		args[i] = collectorIDs[i]
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(collectorIDs)), ",")
	query := fmt.Sprintf(`WITH active_specs AS (
	SELECT c.collector_id,c.expected_cadence_s
	FROM research_collector_specs c
	WHERE c.active=1 AND c.collector_id IN (%s)
	AND c.version=(SELECT MAX(v.version) FROM research_collector_specs v
		WHERE v.collector_id=c.collector_id AND v.active=1)
)
SELECT c.collector_id,c.expected_cadence_s,COALESCE(r.status,''),COALESCE(r.completed_ts,''),
	COALESCE(r.zero_reason,''),COALESCE(r.error_text,'')
FROM active_specs c
LEFT JOIN research_collector_receipts r ON r.id=(
	SELECT x.id FROM research_collector_receipts x
	WHERE x.collector_id=c.collector_id ORDER BY x.id DESC LIMIT 1)
ORDER BY c.collector_id`, placeholders)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	now := time.Now()
	runtimeStarted := s.collectorRuntimeStartedAt()
	out := make(map[string]CollectorLivenessView, len(collectorIDs))
	for rows.Next() {
		var v CollectorLivenessView
		if err := rows.Scan(&v.CollectorID, &v.ExpectedCadenceS, &v.Status, &v.CompletedTS,
			&v.ZeroReason, &v.ErrorText); err != nil {
			return nil, err
		}
		applyCollectorLivenessClock(&v, now, runtimeStarted)
		out[v.CollectorID] = v
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// ResearchSystemCollectionStatsCompact is the default UI/digest reader. It joins only the latest
// per-system runtime and collector receipts to the insert-maintained economic counters; it
// deliberately skips all collector-lifetime aggregation. This makes exact rows/candidates/open
// counts plus truthful stale/blocked state visible without a WAL-pinning historical scan. Full
// collector cycles and input rows remain available through ResearchSystemCollectionStats on detail.
func (s *Store) ResearchSystemCollectionStatsCompact(ctx context.Context) ([]ResearchSystemCollectionStat, error) {
	ids := ResearchExperimentIDs()
	out := make([]ResearchSystemCollectionStat, len(ids))
	byIndex := make(map[string]int, len(ids))
	for i, id := range ids {
		out[i] = ResearchSystemCollectionStat{SystemID: id, State: "STARTING",
			Reason: "registered; waiting for first runtime collector receipt"}
		byIndex[id] = i
	}
	rows, err := s.db.QueryContext(ctx, `SELECT r.system_id,r.state,r.reason,r.observed_ts,r.collector_ids_json
FROM research_system_runtime_receipts r
WHERE r.id=(SELECT MAX(x.id) FROM research_system_runtime_receipts x WHERE x.system_id=r.system_id)
ORDER BY r.system_id`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var v ResearchSystemCollectionStat
		var collectors string
		if err := rows.Scan(&v.SystemID, &v.State, &v.Reason, &v.Observed, &collectors); err != nil {
			rows.Close()
			return nil, err
		}
		_ = json.Unmarshal([]byte(collectors), &v.CollectorIDs)
		if i, exists := byIndex[v.SystemID]; exists {
			out[i] = v
		}
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows, err = s.db.QueryContext(ctx, `SELECT system_id,economic_observations,candidates,controls,open_economic
FROM research_system_collection_totals ORDER BY system_id`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var id string
		var economics, candidates, controls, open int
		if err := rows.Scan(&id, &economics, &candidates, &controls, &open); err != nil {
			rows.Close()
			return nil, err
		}
		if i, exists := byIndex[id]; exists {
			out[i].EconomicObservations, out[i].Candidates = economics, candidates
			out[i].Controls, out[i].Open = controls, open
		}
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	var collectorIDs []string
	for i := range out {
		collectorIDs = append(collectorIDs, out[i].CollectorIDs...)
	}
	byCollector, err := s.compactCollectorLivenessLatest(ctx, collectorIDs)
	if err != nil {
		return nil, err
	}
	for i := range out {
		for _, id := range cleanStringSet(out[i].CollectorIDs) {
			if view, exists := byCollector[id]; exists && view.Alert {
				out[i].CollectorAlerts++
			}
		}
		state, reason, _ := effectiveResearchSystemRuntimeState(out[i].State, out[i].Reason,
			out[i].CollectorIDs, byCollector)
		out[i].State, out[i].Reason = state, reason
	}
	return out, nil
}

// ResearchSystemCollectionStats joins the latest immutable runtime contract to collector
// liveness and the common economic-observation ledger. Every registered R138 system is returned
// exactly once, including systems whose current funnel has only exclusions or negative controls.
func (s *Store) ResearchSystemCollectionStats(ctx context.Context) ([]ResearchSystemCollectionStat, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT r.system_id,r.state,r.reason,r.observed_ts,r.collector_ids_json
FROM research_system_runtime_receipts r
WHERE r.id=(SELECT MAX(x.id) FROM research_system_runtime_receipts x WHERE x.system_id=r.system_id)
ORDER BY r.system_id`)
	if err != nil {
		return nil, err
	}
	ids := ResearchExperimentIDs()
	out := make([]ResearchSystemCollectionStat, len(ids))
	byIndex := make(map[string]int, len(ids))
	for i, id := range ids {
		out[i] = ResearchSystemCollectionStat{SystemID: id, State: "STARTING",
			Reason: "registered; waiting for first runtime collector receipt"}
		byIndex[id] = i
	}
	for rows.Next() {
		var v ResearchSystemCollectionStat
		var collectors string
		if err := rows.Scan(&v.SystemID, &v.State, &v.Reason, &v.Observed, &collectors); err != nil {
			rows.Close()
			return nil, err
		}
		_ = json.Unmarshal([]byte(collectors), &v.CollectorIDs)
		if i, exists := byIndex[v.SystemID]; exists {
			out[i] = v
		}
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Rebuild pointers after append growth so the map always addresses the final backing array.
	bySystem := map[string]*ResearchSystemCollectionStat{}
	for i := range out {
		bySystem[out[i].SystemID] = &out[i]
	}
	// R144: observer funnel/control rows still prove only collection. Economic counters come from
	// the transactionally-maintained summary; never full-scan the live append-only evidence ledger
	// from a dashboard or briefing request.
	rows, err = s.db.QueryContext(ctx, `SELECT system_id,economic_observations,candidates,controls,open_economic
FROM research_system_collection_totals ORDER BY system_id`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var id string
		var n, candidates, controls, open int
		if err := rows.Scan(&id, &n, &candidates, &controls, &open); err != nil {
			rows.Close()
			return nil, err
		}
		if v := bySystem[id]; v != nil {
			v.EconomicObservations, v.Candidates, v.Controls, v.Open = n, candidates, controls, open
		}
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	liveness, err := s.CollectorLivenessReport(ctx)
	if err != nil {
		return nil, err
	}
	views, ok := liveness["collectors"].([]CollectorLivenessView)
	if !ok {
		return nil, errors.New("collector liveness report has unexpected shape")
	}
	byCollector := make(map[string]CollectorLivenessView, len(views))
	for _, view := range views {
		byCollector[view.CollectorID] = view
	}
	for i := range out {
		seen := map[string]bool{}
		for _, id := range out[i].CollectorIDs {
			if seen[id] {
				continue
			}
			seen[id] = true
			view, exists := byCollector[id]
			if !exists {
				continue
			}
			out[i].InputRows += int(view.LifetimeInserted)
			out[i].InputCycles += int(view.LifetimeCycles)
			if view.Alert {
				out[i].CollectorAlerts++
			}
		}
		state, reason, _ := effectiveResearchSystemRuntimeState(out[i].State, out[i].Reason,
			out[i].CollectorIDs, byCollector)
		out[i].State, out[i].Reason = state, reason
		// Economic observations stay separate. Collector LifetimeInserted already includes rows the
		// collector wrote to its system ledger; adding the observation count again would double-count
		// dedicated funnels and make healthy collection look larger than it is.
	}
	return out, nil
}

func (s *Store) InsertOfficialReleaseFrame(ctx context.Context, f OfficialReleaseFrame) (bool, error) {
	if f.Observed.IsZero() {
		f.Observed = time.Now()
	}
	if f.SourceID == "" || f.ArtifactID == "" || f.SchemaVersion == "" || f.ArtifactHash == "" ||
		(f.ClockStatus != "exact" && f.ClockStatus != "arrival_only" && f.ClockStatus != "blocked") {
		return false, errors.New("invalid official release frame")
	}
	if f.ClockStatus == "exact" && f.SourceTime.IsZero() {
		return false, errors.New("exact release lacks source timestamp")
	}
	if (f.ClockStatus == "blocked" || f.ClockStatus == "arrival_only") && f.Blocker == "" {
		return false, errors.New("non-exact release needs blocker")
	}
	values, err := json.Marshal(f.Values)
	if err != nil {
		return false, err
	}
	formatTime := func(t time.Time) string {
		if t.IsZero() {
			return ""
		}
		return t.UTC().Format(time.RFC3339Nano)
	}
	res, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO research_official_release_frames(
observed_ts,source_id,artifact_id,source_ts,valid_ts,schema_version,clock_status,artifact_hash,
values_json,settlement_compatible,blocker,funded,paper_authority,live_authority)
VALUES(?,?,?,?,?,?,?,?,?,?,?,0,0,0)`, formatTime(f.Observed), f.SourceID, f.ArtifactID,
		formatTime(f.SourceTime), formatTime(f.ValidTime), f.SchemaVersion, f.ClockStatus, f.ArtifactHash,
		string(values), boolInt(f.SettlementCompatible), f.Blocker)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

// VerifiedDeadlineRelation is deliberately narrow: only the latest fully verified immutable
// implication and instrument versions enter the runtime scanner. Unverified/title-derived rows
// stay outside the eligible universe and remain visible in collector receipts.
type VerifiedDeadlineRelation struct {
	RelationID, EventID, LeftPayoffID, RightPayoffID string
	LeftTicker, RightTicker                          string
	RelationVersion, EventVersion                    int
	EvidenceJSON, LeftPredicate, RightPredicate      string
	VoidPolicy, EventEvidenceJSON                    string
}

func (s *Store) VerifiedDeadlineRelations(ctx context.Context, venue string, limit int) ([]VerifiedDeadlineRelation, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `SELECT
r.relation_id,r.event_id,r.left_payoff_id,r.right_payoff_id,li.ticker,ri.ticker,
r.version,r.event_version,r.evidence_json,lp.predicate_json,rp.predicate_json,e.void_policy,e.evidence_json
FROM research_payoff_relations r
JOIN research_event_specs e ON e.event_id=r.event_id AND e.version=r.event_version
JOIN research_payoff_specs lp ON lp.event_id=r.event_id AND lp.event_version=r.event_version
 AND lp.payoff_id=r.left_payoff_id AND lp.version=r.left_payoff_version
JOIN research_payoff_specs rp ON rp.event_id=r.event_id AND rp.event_version=r.event_version
 AND rp.payoff_id=r.right_payoff_id AND rp.version=r.right_payoff_version
JOIN research_instrument_specs li ON li.event_id=r.event_id AND li.event_version=r.event_version
 AND li.payoff_id=r.left_payoff_id AND li.payoff_version=r.left_payoff_version AND li.venue=?
JOIN research_instrument_specs ri ON ri.event_id=r.event_id AND ri.event_version=r.event_version
 AND ri.payoff_id=r.right_payoff_id AND ri.payoff_version=r.right_payoff_version AND ri.venue=?
WHERE r.relation_type='implies' AND r.identity_status='verified'
AND lp.identity_status='verified' AND rp.identity_status='verified'
AND li.identity_status='verified' AND ri.identity_status='verified'
AND NOT EXISTS (SELECT 1 FROM research_payoff_relations newer
 WHERE newer.relation_id=r.relation_id AND newer.version>r.version)
AND NOT EXISTS (SELECT 1 FROM research_instrument_specs newer
 WHERE newer.venue=li.venue AND newer.ticker=li.ticker AND newer.version>li.version)
AND NOT EXISTS (SELECT 1 FROM research_instrument_specs newer
 WHERE newer.venue=ri.venue AND newer.ticker=ri.ticker AND newer.version>ri.version)
ORDER BY r.relation_id LIMIT ?`, venue, venue, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []VerifiedDeadlineRelation
	for rows.Next() {
		var r VerifiedDeadlineRelation
		if err := rows.Scan(&r.RelationID, &r.EventID, &r.LeftPayoffID, &r.RightPayoffID,
			&r.LeftTicker, &r.RightTicker, &r.RelationVersion, &r.EventVersion, &r.EvidenceJSON,
			&r.LeftPredicate, &r.RightPredicate, &r.VoidPolicy, &r.EventEvidenceJSON); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

const researchSystemEvidenceRawTotalsSQL = `SELECT system_id,raw_observations,raw_candidates,
raw_negative,controls,raw_open_envelopes
FROM research_system_collection_totals ORDER BY system_id`

func (s *Store) ResearchSystemEvidenceReport(ctx context.Context) (map[string]any, error) {
	counts := map[string]int64{}
	var payoffUpdates, bookFrames, bookHorizons, outcomeSetFrames int64
	var membershipChanges, officialReleases, blockedReleases int64
	if err := s.db.QueryRowContext(ctx, `SELECT
 (SELECT COUNT(*) FROM research_system_payoff_updates),
 (SELECT COUNT(*) FROM research_microstructure_frames),
 (SELECT COUNT(*) FROM research_microstructure_horizons),
 (SELECT COUNT(*) FROM research_outcome_set_frames),
 (SELECT COUNT(*) FROM research_outcome_set_frames
   WHERE change_class NOT IN ('baseline','unchanged')),
 (SELECT COUNT(*) FROM research_official_release_frames),
 (SELECT COUNT(*) FROM research_official_release_frames WHERE clock_status='blocked')`).Scan(
		&payoffUpdates, &bookFrames, &bookHorizons, &outcomeSetFrames, &membershipChanges,
		&officialReleases, &blockedReleases); err != nil {
		return nil, err
	}
	counts["payoff_updates"], counts["book_frames"] = payoffUpdates, bookFrames
	counts["book_horizons"], counts["outcome_set_frames"] = bookHorizons, outcomeSetFrames
	counts["membership_changes"], counts["official_releases"] = membershipChanges, officialReleases
	counts["blocked_releases"] = blockedReleases
	counts["observations"], counts["candidates"] = 0, 0
	counts["negative_controls"], counts["open_envelopes"] = 0, 0
	// The immutable registry is the denominator.  A GROUP BY over the observation ledger alone
	// silently drops a correctly registered system while its collector has produced no common
	// economic rows (payoff-constraint-solver was the first runtime example).  Zero is evidence
	// state, not absence: seed all 19 systems and then overlay durable observations and current
	// collector liveness below.
	type evidenceView struct {
		Rows       int64  `json:"rows"`
		Candidates int64  `json:"candidates"`
		Negative   int64  `json:"negative"`
		Controls   int64  `json:"controls"`
		State      string `json:"state"`
		Reason     string `json:"reason"`
		Blocker    string `json:"blocker"`
	}
	views := make(map[string]*evidenceView, len(ResearchExperimentIDs()))
	bySystem := make(map[string]any, len(ResearchExperimentIDs()))
	for _, id := range ResearchExperimentIDs() {
		reason := "registered; waiting for first runtime collector receipt"
		view := &evidenceView{State: "STARTING", Reason: reason, Blocker: reason}
		views[id] = view
		bySystem[id] = view
	}
	rows, err := s.db.QueryContext(ctx, researchSystemEvidenceRawTotalsSQL)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var id string
		var total, candidate, negative, control, rawOpen int64
		if err := rows.Scan(&id, &total, &candidate, &negative, &control, &rawOpen); err != nil {
			_ = rows.Close()
			return nil, err
		}
		counts["observations"] += total
		counts["candidates"] += candidate
		counts["negative_controls"] += total - candidate
		counts["open_envelopes"] += rawOpen
		if view := views[id]; view != nil {
			view.Rows, view.Candidates, view.Negative, view.Controls = total, candidate, negative, control
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	runtimeCoverage, err := s.ResearchSystemRuntimeReport(ctx)
	if err != nil {
		return nil, err
	}
	if runtime, ok := runtimeCoverage["systems"].([]ResearchSystemRuntimeView); ok {
		for _, state := range runtime {
			view := views[state.SystemID]
			if view == nil {
				continue
			}
			view.State, view.Reason = state.State, state.Reason
			view.Blocker = ""
			if state.State == "BLOCKED" || state.State == "STARTING" {
				view.Blocker = state.Reason
			}
		}
	}
	return map[string]any{
		"generated_at": time.Now().UTC().Format(time.RFC3339Nano), "schema": r138EvidenceSchemaVersion,
		"funded": false, "paper_authority": false, "live_authority": false,
		"counts": counts, "systems": bySystem, "runtime_coverage": runtimeCoverage,
		"money_truth":    "side-specific executable depth, exact route fees, quote age and ticks; open outcomes remain payoff intervals",
		"capacity_truth": "visible depth curves are mechanical ceilings, never assumed recurring fills",
	}, nil
}

func r138HashJSON(v any) string {
	b, _ := json.Marshal(v)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func R138HashJSON(v any) string { return r138HashJSON(v) }

func R138DeadlineFromPredicate(raw string) (time.Time, bool) {
	var v map[string]any
	if json.Unmarshal([]byte(raw), &v) != nil {
		return time.Time{}, false
	}
	for _, key := range []string{"deadline_ts", "deadline", "end_ts"} {
		if s, ok := v[key].(string); ok {
			if t, err := time.Parse(time.RFC3339, strings.TrimSpace(s)); err == nil {
				return t, true
			}
		}
	}
	return time.Time{}, false
}

func (s *Store) R138ObservationAuthorityCounts(ctx context.Context) (funded, paper, live int, err error) {
	err = s.db.QueryRowContext(ctx, `SELECT COALESCE(SUM(funded),0),COALESCE(SUM(paper_authority),0),
COALESCE(SUM(live_authority),0) FROM research_system_observations`).Scan(&funded, &paper, &live)
	return
}

func (s *Store) R138EvidenceRowCount(ctx context.Context, table string) (int, error) {
	allowed := map[string]bool{
		"research_system_observations": true, "research_system_payoff_updates": true,
		"research_microstructure_frames": true, "research_microstructure_horizons": true,
		"research_outcome_set_frames": true, "research_official_release_frames": true,
	}
	if !allowed[table] {
		return 0, fmt.Errorf("unsupported evidence table")
	}
	var n int
	err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&n)
	return n, err
}
