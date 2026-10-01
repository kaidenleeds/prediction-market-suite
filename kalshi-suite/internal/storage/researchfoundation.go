package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// R138 STEP 1: canonical event/payoff identity and immutable experiment governance.
//
// These rows are research specifications, never order authority. Semantic changes append a new
// version. SQLite triggers reject UPDATE/DELETE and the registration helpers reject a changed
// payload under an existing (id,version), so a result can never be made to fit a rewritten thesis.

type CanonicalEventSpec struct {
	EventID, EventType, Domain, Title               string
	StartTS, DeadlineTS, Timezone                   string
	SettlementSource, SourceArtifact, SourceClockID string
	BoundaryRule, RevisionPolicy, VoidPolicy        string
	OutcomeSetStatus, EvidenceJSON                  string
	// SourceObservedTS is provenance/liveness, not semantic identity.  It is deliberately
	// excluded from the immutable spec hash so a refresh does not manufacture a new version.
	SourceObservedTS string
}

type CanonicalPayoffSpec struct {
	PayoffID, EventID, Label, PredicateJSON, BoundaryRule string
	SettlementSource, SourceArtifact, IdentityStatus      string
	PayoutFloor, PayoutCeiling                            float64
	EvidenceJSON                                          string
	SourceObservedTS                                      string
}

type CanonicalInstrumentSpec struct {
	Venue, Ticker, EventID, PayoffID, NativeSide, Orientation string
	MarketKind, Scope, CloseTS, SettlementSource              string
	RulesArtifact, RulesHash, FeeAuthority, CrossVenueBasis   string
	IdentityStatus, EvidenceJSON                              string
	Line                                                      float64
	SourceObservedTS                                          string
}

type CanonicalRelationSpec struct {
	RelationID, EventID, LeftPayoffID, RightPayoffID string
	RelationType, IdentityStatus, EvidenceJSON       string
	PayoutFloor                                      *float64
}

type ResearchExperimentSpec struct {
	ExperimentID, SystemName, Mechanism, Hypothesis, NullHypothesis string
	Direction, CohortJSON, Route, HoldoutJSON, StoppingRule         string
	MultiplicityFamily, ParentExperiment, InitialState              string
	CellsJSON, PrimaryEstimand, HorizonRule, FeeRouteRule           string
	SourceSchemaJSON, InferenceJSON, ShrinkageJSON                  string
	MultiplicityMethod, CapacityMetric, CapitalTimeMetric           string
	SystemKillRule, CodeManifestHash, DataManifestHash              string
	Version, ParentVersion, RequiredDays                            int
}

type canonicalEventHash struct {
	EventID, EventType, Domain, Title               string
	StartTS, DeadlineTS, Timezone                   string
	SettlementSource, SourceArtifact, SourceClockID string
	BoundaryRule, RevisionPolicy, VoidPolicy        string
	OutcomeSetStatus, EvidenceJSON                  string
}

type canonicalPayoffHash struct {
	PayoffID, EventID, Label, PredicateJSON, BoundaryRule string
	SettlementSource, SourceArtifact, IdentityStatus      string
	PayoutFloor, PayoutCeiling                            float64
	EvidenceJSON                                          string
}

type canonicalInstrumentHash struct {
	Venue, Ticker, EventID, PayoffID, NativeSide, Orientation string
	MarketKind, Scope, CloseTS, SettlementSource              string
	RulesArtifact, RulesHash, FeeAuthority, CrossVenueBasis   string
	IdentityStatus, EvidenceJSON                              string
	Line                                                      float64
}

func eventSemanticHash(e CanonicalEventSpec) canonicalEventHash {
	return canonicalEventHash{e.EventID, e.EventType, e.Domain, e.Title, e.StartTS, e.DeadlineTS,
		e.Timezone, e.SettlementSource, e.SourceArtifact, e.SourceClockID, e.BoundaryRule,
		e.RevisionPolicy, e.VoidPolicy, e.OutcomeSetStatus, e.EvidenceJSON}
}

func payoffSemanticHash(p CanonicalPayoffSpec) canonicalPayoffHash {
	return canonicalPayoffHash{p.PayoffID, p.EventID, p.Label, p.PredicateJSON, p.BoundaryRule,
		p.SettlementSource, p.SourceArtifact, p.IdentityStatus, p.PayoutFloor, p.PayoutCeiling, p.EvidenceJSON}
}

func instrumentSemanticHash(i CanonicalInstrumentSpec) canonicalInstrumentHash {
	return canonicalInstrumentHash{i.Venue, i.Ticker, i.EventID, i.PayoffID, i.NativeSide, i.Orientation,
		i.MarketKind, i.Scope, i.CloseTS, i.SettlementSource, i.RulesArtifact, i.RulesHash,
		i.FeeAuthority, i.CrossVenueBasis, i.IdentityStatus, i.EvidenceJSON, i.Line}
}

type experimentHash struct {
	ExperimentID, SystemName, Mechanism, Hypothesis, NullHypothesis string
	Direction, CohortJSON, Route, HoldoutJSON, StoppingRule         string
	MultiplicityFamily, ParentExperiment, InitialState              string
	CellsJSON, PrimaryEstimand, HorizonRule, FeeRouteRule           string
	SourceSchemaJSON, InferenceJSON, ShrinkageJSON                  string
	MultiplicityMethod, CapacityMetric, CapitalTimeMetric           string
	SystemKillRule, CodeManifestHash, DataManifestHash              string
	Version, ParentVersion, RequiredDays                            int
}

type canonicalRelationHash struct {
	RelationID, EventID, LeftPayoffID, RightPayoffID    string
	RelationType, IdentityStatus, EvidenceJSON          string
	EventVersion, LeftPayoffVersion, RightPayoffVersion int
	PayoutFloor                                         *float64
}

func researchSpecHash(v any) (string, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:]), nil
}

func normalizedJSON(raw, fallback string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		raw = fallback
	}
	var v any
	if err := json.Unmarshal([]byte(raw), &v); err != nil {
		return "", err
	}
	b, err := json.Marshal(v)
	return string(b), err
}

func normalizeIdentityStatus(v string) string {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "verified", "structural", "unverified", "rejected":
		return strings.ToLower(strings.TrimSpace(v))
	default:
		return "unverified"
	}
}

func normalizeOutcomeSet(v string) string {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "complete", "incomplete", "unknown":
		return strings.ToLower(strings.TrimSpace(v))
	default:
		return "unknown"
	}
}

func researchFiveMinuteSlot(t time.Time) string {
	return t.UTC().Truncate(5 * time.Minute).Format(time.RFC3339)
}

func sourceObservedTime(raw string, fallback time.Time) time.Time {
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02 15:04:05"} {
		if t, err := time.Parse(layout, strings.TrimSpace(raw)); err == nil {
			return t.UTC()
		}
	}
	return fallback.UTC()
}

func nextVersion(ctx context.Context, tx *sql.Tx, table, idCol, id string) (int, error) {
	q := fmt.Sprintf("SELECT COALESCE(MAX(version),0)+1 FROM %s WHERE %s=?", table, idCol)
	var n int
	err := tx.QueryRowContext(ctx, q, id).Scan(&n)
	return n, err
}

func insertIdentitySighting(ctx context.Context, tx *sql.Tx, now time.Time, typ, key string, version int, source string) error {
	_, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO research_identity_sightings(
observed_ts,slot,object_type,object_key,object_version,source) VALUES(?,?,?,?,?,?)`,
		now.UTC().Format(time.RFC3339Nano), researchFiveMinuteSlot(now), typ, key, version, source)
	return err
}

func registerEventTx(ctx context.Context, tx *sql.Tx, now time.Time, e CanonicalEventSpec) (int, bool, error) {
	e.EventID, e.EventType = strings.TrimSpace(e.EventID), strings.TrimSpace(e.EventType)
	if e.EventID == "" {
		return 0, false, errors.New("canonical event id is required")
	}
	if e.EventType == "" {
		e.EventType = "unknown"
	}
	var err error
	e.EvidenceJSON, err = normalizedJSON(e.EvidenceJSON, "{}")
	if err != nil {
		return 0, false, fmt.Errorf("event %s evidence: %w", e.EventID, err)
	}
	e.OutcomeSetStatus = normalizeOutcomeSet(e.OutcomeSetStatus)
	h, err := researchSpecHash(eventSemanticHash(e))
	if err != nil {
		return 0, false, err
	}
	var version int
	err = tx.QueryRowContext(ctx, `SELECT version FROM research_event_specs WHERE event_id=? AND spec_hash=?`, e.EventID, h).Scan(&version)
	if err == nil {
		return version, false, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return 0, false, err
	}
	version, err = nextVersion(ctx, tx, "research_event_specs", "event_id", e.EventID)
	if err != nil {
		return 0, false, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO research_event_specs(
event_id,version,spec_hash,event_type,domain,title,start_ts,deadline_ts,timezone,settlement_source,
source_artifact,source_clock_id,boundary_rule,revision_policy,void_policy,outcome_set_status,evidence_json,created_ts)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, e.EventID, version, h, e.EventType, e.Domain, e.Title,
		e.StartTS, e.DeadlineTS, e.Timezone, e.SettlementSource, e.SourceArtifact, e.SourceClockID,
		e.BoundaryRule, e.RevisionPolicy, e.VoidPolicy, e.OutcomeSetStatus, e.EvidenceJSON,
		now.UTC().Format(time.RFC3339Nano))
	if err != nil {
		return 0, false, err
	}
	return version, true, insertIdentitySighting(ctx, tx, sourceObservedTime(e.SourceObservedTS, now), "event", e.EventID, version, e.SourceArtifact)
}

func currentEventVersion(ctx context.Context, tx *sql.Tx, eventID string) (int, error) {
	var v int
	err := tx.QueryRowContext(ctx, `SELECT version FROM research_event_specs WHERE event_id=? ORDER BY version DESC LIMIT 1`, eventID).Scan(&v)
	return v, err
}

func registerPayoffTx(ctx context.Context, tx *sql.Tx, now time.Time, p CanonicalPayoffSpec) (int, bool, error) {
	p.PayoffID, p.EventID = strings.TrimSpace(p.PayoffID), strings.TrimSpace(p.EventID)
	if p.PayoffID == "" || p.EventID == "" || p.PayoutFloor > p.PayoutCeiling {
		return 0, false, errors.New("invalid canonical payoff")
	}
	var err error
	p.PredicateJSON, err = normalizedJSON(p.PredicateJSON, "{}")
	if err != nil {
		return 0, false, fmt.Errorf("payoff %s predicate: %w", p.PayoffID, err)
	}
	p.EvidenceJSON, err = normalizedJSON(p.EvidenceJSON, "{}")
	if err != nil {
		return 0, false, fmt.Errorf("payoff %s evidence: %w", p.PayoffID, err)
	}
	p.IdentityStatus = normalizeIdentityStatus(p.IdentityStatus)
	eventVersion, err := currentEventVersion(ctx, tx, p.EventID)
	if err != nil {
		return 0, false, fmt.Errorf("payoff %s event: %w", p.PayoffID, err)
	}
	h, err := researchSpecHash(struct {
		Spec         canonicalPayoffHash
		EventVersion int
	}{payoffSemanticHash(p), eventVersion})
	if err != nil {
		return 0, false, err
	}
	var version int
	err = tx.QueryRowContext(ctx, `SELECT version FROM research_payoff_specs WHERE payoff_id=? AND spec_hash=?`, p.PayoffID, h).Scan(&version)
	if err == nil {
		return version, false, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return 0, false, err
	}
	version, err = nextVersion(ctx, tx, "research_payoff_specs", "payoff_id", p.PayoffID)
	if err != nil {
		return 0, false, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO research_payoff_specs(
payoff_id,version,spec_hash,event_id,event_version,label,predicate_json,boundary_rule,payout_floor,
payout_ceiling,settlement_source,source_artifact,identity_status,evidence_json,created_ts)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, p.PayoffID, version, h, p.EventID, eventVersion, p.Label,
		p.PredicateJSON, p.BoundaryRule, p.PayoutFloor, p.PayoutCeiling, p.SettlementSource,
		p.SourceArtifact, p.IdentityStatus, p.EvidenceJSON, now.UTC().Format(time.RFC3339Nano))
	if err != nil {
		return 0, false, err
	}
	return version, true, insertIdentitySighting(ctx, tx, sourceObservedTime(p.SourceObservedTS, now), "payoff", p.PayoffID, version, p.SourceArtifact)
}

func currentPayoffVersion(ctx context.Context, tx *sql.Tx, payoffID string) (int, error) {
	var v int
	err := tx.QueryRowContext(ctx, `SELECT version FROM research_payoff_specs WHERE payoff_id=? ORDER BY version DESC LIMIT 1`, payoffID).Scan(&v)
	return v, err
}

func registerInstrumentTx(ctx context.Context, tx *sql.Tx, now time.Time, i CanonicalInstrumentSpec) (int, bool, error) {
	i.Venue, i.Ticker = strings.ToLower(strings.TrimSpace(i.Venue)), strings.TrimSpace(i.Ticker)
	i.EventID, i.PayoffID = strings.TrimSpace(i.EventID), strings.TrimSpace(i.PayoffID)
	if i.Venue == "" || i.Ticker == "" || i.EventID == "" || i.PayoffID == "" {
		return 0, false, errors.New("invalid canonical instrument")
	}
	if i.NativeSide == "" {
		i.NativeSide = "YES"
	}
	if i.Orientation == "" {
		i.Orientation = "unknown"
	}
	i.IdentityStatus = normalizeIdentityStatus(i.IdentityStatus)
	var err error
	i.EvidenceJSON, err = normalizedJSON(i.EvidenceJSON, "{}")
	if err != nil {
		return 0, false, fmt.Errorf("instrument %s|%s evidence: %w", i.Venue, i.Ticker, err)
	}
	eventVersion, err := currentEventVersion(ctx, tx, i.EventID)
	if err != nil {
		return 0, false, err
	}
	var payoffEventID string
	var payoffEventVersion, payoffVersion int
	err = tx.QueryRowContext(ctx, `SELECT event_id,event_version,version FROM research_payoff_specs
WHERE payoff_id=? ORDER BY version DESC LIMIT 1`, i.PayoffID).Scan(&payoffEventID, &payoffEventVersion, &payoffVersion)
	if err != nil {
		return 0, false, err
	}
	if payoffEventID != i.EventID || payoffEventVersion != eventVersion {
		return 0, false, fmt.Errorf("instrument %s|%s payoff %s belongs to %s v%d, not %s v%d",
			i.Venue, i.Ticker, i.PayoffID, payoffEventID, payoffEventVersion, i.EventID, eventVersion)
	}
	h, err := researchSpecHash(struct {
		Spec          canonicalInstrumentHash
		EventVersion  int
		PayoffVersion int
	}{instrumentSemanticHash(i), eventVersion, payoffVersion})
	if err != nil {
		return 0, false, err
	}
	var version int
	err = tx.QueryRowContext(ctx, `SELECT version FROM research_instrument_specs WHERE venue=? AND ticker=? AND spec_hash=?`, i.Venue, i.Ticker, h).Scan(&version)
	key := i.Venue + "|" + i.Ticker
	if err == nil {
		return version, false, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return 0, false, err
	}
	var n int
	err = tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(version),0)+1 FROM research_instrument_specs WHERE venue=? AND ticker=?`, i.Venue, i.Ticker).Scan(&n)
	if err != nil {
		return 0, false, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO research_instrument_specs(
venue,ticker,version,spec_hash,event_id,event_version,payoff_id,payoff_version,native_side,
orientation,market_kind,scope,line,close_ts,settlement_source,rules_artifact,rules_hash,
fee_authority,cross_venue_basis,identity_status,evidence_json,created_ts)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, i.Venue, i.Ticker, n, h, i.EventID,
		eventVersion, i.PayoffID, payoffVersion, strings.ToUpper(i.NativeSide), i.Orientation,
		i.MarketKind, i.Scope, i.Line, i.CloseTS, i.SettlementSource, i.RulesArtifact, i.RulesHash,
		i.FeeAuthority, i.CrossVenueBasis, i.IdentityStatus, i.EvidenceJSON,
		now.UTC().Format(time.RFC3339Nano))
	if err != nil {
		return 0, false, err
	}
	return n, true, insertIdentitySighting(ctx, tx, sourceObservedTime(i.SourceObservedTS, now), "instrument", key, n, i.RulesArtifact)
}

type CanonicalBatchResult struct {
	EventsSeen          int `json:"events_seen"`
	EventsInserted      int `json:"events_inserted"`
	PayoffsSeen         int `json:"payoffs_seen"`
	PayoffsInserted     int `json:"payoffs_inserted"`
	InstrumentsSeen     int `json:"instruments_seen"`
	InstrumentsInserted int `json:"instruments_inserted"`
	RelationsSeen       int `json:"relations_seen"`
	RelationsInserted   int `json:"relations_inserted"`
}

// RegisterCanonicalBatch atomically registers a bounded point-in-time identity page. Existing
// identical specs are read-only no-ops; changed semantics append a new immutable version. Source
// freshness belongs to bounded collector receipts, never to rescan-time per-object writes.
func (s *Store) RegisterCanonicalBatch(ctx context.Context, events []CanonicalEventSpec, payoffs []CanonicalPayoffSpec, instruments []CanonicalInstrumentSpec, relationPages ...[]CanonicalRelationSpec) (CanonicalBatchResult, error) {
	var out CanonicalBatchResult
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return out, err
	}
	defer func() { _ = tx.Rollback() }()
	now := time.Now()
	for _, e := range events {
		out.EventsSeen++
		_, inserted, err := registerEventTx(ctx, tx, now, e)
		if err != nil {
			return out, err
		}
		if inserted {
			out.EventsInserted++
		}
	}
	for _, p := range payoffs {
		out.PayoffsSeen++
		_, inserted, err := registerPayoffTx(ctx, tx, now, p)
		if err != nil {
			return out, err
		}
		if inserted {
			out.PayoffsInserted++
		}
	}
	for _, i := range instruments {
		out.InstrumentsSeen++
		_, inserted, err := registerInstrumentTx(ctx, tx, now, i)
		if err != nil {
			return out, err
		}
		if inserted {
			out.InstrumentsInserted++
		}
	}
	for _, page := range relationPages {
		for _, rel := range page {
			out.RelationsSeen++
			inserted, err := registerPayoffRelationTx(ctx, tx, rel)
			if err != nil {
				return out, err
			}
			if inserted {
				out.RelationsInserted++
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return out, err
	}
	return out, nil
}

func registerPayoffRelationTx(ctx context.Context, tx *sql.Tx, r CanonicalRelationSpec) (bool, error) {
	if r.RelationID == "" || r.EventID == "" || r.LeftPayoffID == "" || r.RightPayoffID == "" {
		return false, errors.New("invalid payoff relation")
	}
	switch r.RelationType {
	case "implies", "excludes", "exhaustive_with", "basis", "correlated":
	default:
		return false, errors.New("invalid payoff relation type")
	}
	evidence, err := normalizedJSON(r.EvidenceJSON, "{}")
	if err != nil {
		return false, err
	}
	ev, err := currentEventVersion(ctx, tx, r.EventID)
	if err != nil {
		return false, err
	}
	var leftEvent, rightEvent string
	var leftEventVersion, rightEventVersion, lv, rv int
	if err := tx.QueryRowContext(ctx, `SELECT event_id,event_version,version FROM research_payoff_specs
WHERE payoff_id=? ORDER BY version DESC LIMIT 1`, r.LeftPayoffID).Scan(&leftEvent, &leftEventVersion, &lv); err != nil {
		return false, err
	}
	if err := tx.QueryRowContext(ctx, `SELECT event_id,event_version,version FROM research_payoff_specs
WHERE payoff_id=? ORDER BY version DESC LIMIT 1`, r.RightPayoffID).Scan(&rightEvent, &rightEventVersion, &rv); err != nil {
		return false, err
	}
	if leftEvent != r.EventID || rightEvent != r.EventID || leftEventVersion != ev || rightEventVersion != ev {
		return false, fmt.Errorf("relation %s crosses event versions: event=%s v%d left=%s v%d right=%s v%d",
			r.RelationID, r.EventID, ev, leftEvent, leftEventVersion, rightEvent, rightEventVersion)
	}
	status := normalizeIdentityStatus(r.IdentityStatus)
	h, err := researchSpecHash(canonicalRelationHash{
		RelationID: r.RelationID, EventID: r.EventID, LeftPayoffID: r.LeftPayoffID,
		RightPayoffID: r.RightPayoffID, RelationType: r.RelationType, IdentityStatus: status,
		EvidenceJSON: evidence, EventVersion: ev, LeftPayoffVersion: lv, RightPayoffVersion: rv,
		PayoutFloor: r.PayoutFloor,
	})
	if err != nil {
		return false, err
	}
	var existingVersion int
	err = tx.QueryRowContext(ctx, `SELECT version FROM research_payoff_relations WHERE relation_id=? AND spec_hash=?`,
		r.RelationID, h).Scan(&existingVersion)
	if err == nil {
		return false, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return false, err
	}
	var version int
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(version),0)+1 FROM research_payoff_relations WHERE relation_id=?`,
		r.RelationID).Scan(&version); err != nil {
		return false, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO research_payoff_relations(
relation_id,version,spec_hash,event_id,event_version,left_payoff_id,left_payoff_version,right_payoff_id,
right_payoff_version,relation_type,payout_floor,identity_status,evidence_json,created_ts)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, r.RelationID, version, h, r.EventID, ev, r.LeftPayoffID, lv,
		r.RightPayoffID, rv, r.RelationType, r.PayoutFloor, status, evidence, nowRFC())
	if err != nil {
		return false, err
	}
	if err := insertIdentitySighting(ctx, tx, time.Now(), "relation", r.RelationID, version, "payoff-certificate"); err != nil {
		return false, err
	}
	return true, nil
}

// RegisterPayoffRelation appends one evidence-bearing logical relation. relation_id is a stable
// logical key; changed evidence or semantics append a new immutable version.
func (s *Store) RegisterPayoffRelation(ctx context.Context, r CanonicalRelationSpec) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := registerPayoffRelationTx(ctx, tx, r); err != nil {
		return err
	}
	return tx.Commit()
}

func experimentSpecDigest(e ResearchExperimentSpec) (string, error) {
	return researchSpecHash(experimentHash(e))
}

func normalizeExperimentSpec(e ResearchExperimentSpec) (ResearchExperimentSpec, error) {
	e.ExperimentID, e.SystemName = strings.TrimSpace(e.ExperimentID), strings.TrimSpace(e.SystemName)
	if forbidden, class := ForbiddenResearchSystem(e.ExperimentID, e.SystemName); forbidden {
		return e, fmt.Errorf("permanently prohibited research system class %q", class)
	}
	if e.ExperimentID == "" || e.SystemName == "" || e.Mechanism == "" || e.Hypothesis == "" ||
		e.NullHypothesis == "" || e.StoppingRule == "" || e.MultiplicityFamily == "" ||
		e.PrimaryEstimand == "" || e.HorizonRule == "" || e.FeeRouteRule == "" ||
		e.MultiplicityMethod == "" || e.CapacityMetric == "" || e.CapitalTimeMetric == "" ||
		e.SystemKillRule == "" || e.CodeManifestHash == "" || e.DataManifestHash == "" {
		return e, errors.New("incomplete research experiment specification")
	}
	if e.Version <= 0 {
		e.Version = 1
	}
	if e.RequiredDays <= 0 {
		e.RequiredDays = 30
	}
	if e.Direction == "" {
		e.Direction = "two-sided"
	}
	if e.Route == "" {
		e.Route = "research-only"
	}
	if e.InitialState == "" {
		e.InitialState = "SPECIFIED_NOT_IMPLEMENTED"
	}
	if e.ParentExperiment != "" && e.ParentVersion <= 0 {
		e.ParentVersion = 1
	}
	var err error
	for name, field := range map[string]*string{
		"cohort": &e.CohortJSON, "holdout": &e.HoldoutJSON, "cells": &e.CellsJSON,
		"source_schema": &e.SourceSchemaJSON, "inference": &e.InferenceJSON,
		"shrinkage": &e.ShrinkageJSON,
	} {
		*field, err = normalizedJSON(*field, "{}")
		if err != nil {
			return e, fmt.Errorf("experiment %s %s: %w", e.ExperimentID, name, err)
		}
	}
	return e, nil
}

func registerExperimentSpecTx(ctx context.Context, tx *sql.Tx, e ResearchExperimentSpec) (bool, error) {
	var err error
	e, err = normalizeExperimentSpec(e)
	if err != nil {
		return false, err
	}
	h, err := experimentSpecDigest(e)
	if err != nil {
		return false, err
	}
	var existing string
	err = tx.QueryRowContext(ctx, `SELECT spec_hash FROM research_experiment_specs WHERE experiment_id=? AND version=?`, e.ExperimentID, e.Version).Scan(&existing)
	if err == nil {
		if existing != h {
			return false, fmt.Errorf("immutable experiment spec drift: %s v%d (bump version)", e.ExperimentID, e.Version)
		}
		return false, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return false, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO research_experiment_specs(
experiment_id,version,spec_hash,system_name,mechanism,hypothesis,null_hypothesis,direction,
cohort_json,route,holdout_json,stopping_rule,multiplicity_family,cells_json,primary_estimand,horizon_rule,
fee_route_rule,source_schema_json,inference_json,shrinkage_json,multiplicity_method,capacity_metric,
capital_time_metric,system_kill_rule,code_manifest_hash,data_manifest_hash,parent_experiment,parent_version,
required_days,initial_state,funded,paper_authority,live_authority,created_ts)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0,0,0,?)`, e.ExperimentID, e.Version, h, e.SystemName,
		e.Mechanism, e.Hypothesis, e.NullHypothesis, e.Direction, e.CohortJSON, e.Route,
		e.HoldoutJSON, e.StoppingRule, e.MultiplicityFamily, e.CellsJSON, e.PrimaryEstimand,
		e.HorizonRule, e.FeeRouteRule, e.SourceSchemaJSON, e.InferenceJSON, e.ShrinkageJSON,
		e.MultiplicityMethod, e.CapacityMetric, e.CapitalTimeMetric, e.SystemKillRule,
		e.CodeManifestHash, e.DataManifestHash, nullableString(e.ParentExperiment), nullablePositiveInt(e.ParentVersion),
		e.RequiredDays, e.InitialState, nowRFC())
	if err != nil {
		return false, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO research_experiment_events(
experiment_id,experiment_version,observed_ts,event_type,state,message,evidence_json,result_class,
code_manifest_hash,data_manifest_hash,source_manifest_hash,prior_event_id)
VALUES(?,?,?,'registered',?,'immutable blueprint registered','{}','',?,?,?,NULL)`,
		e.ExperimentID, e.Version, nowRFC(), e.InitialState, e.CodeManifestHash, e.DataManifestHash,
		e.DataManifestHash)
	if err != nil {
		return false, err
	}
	return true, nil
}

func nullableString(v string) any {
	if strings.TrimSpace(v) == "" {
		return nil
	}
	return strings.TrimSpace(v)
}

func nullablePositiveInt(v int) any {
	if v <= 0 {
		return nil
	}
	return v
}

func (s *Store) RegisterExperimentSpec(ctx context.Context, e ResearchExperimentSpec) (bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()
	inserted, err := registerExperimentSpecTx(ctx, tx, e)
	if err != nil {
		return false, err
	}
	return inserted, tx.Commit()
}

type ResearchExperimentEventInput struct {
	ExperimentID, EventType, State, Message, EvidenceJSON string
	ResultClass, CodeManifestHash, DataManifestHash       string
	SourceManifestHash                                    string
	ExperimentVersion                                     int
}

func validExperimentTransition(eventType, current, next string) bool {
	if eventType == "note" {
		return next == current
	}
	if eventType == "blocked" {
		return next == "BLOCKED"
	}
	if eventType == "killed" {
		return next == "KILLED"
	}
	switch eventType + "|" + next {
	case "collecting|COLLECTING":
		return current == "SPECIFIED_NOT_IMPLEMENTED" || current == "SPECIFIED" || current == "BLOCKED"
	case "result|TRAIN_RESULT_POSITIVE", "result|TRAIN_RESULT_NEGATIVE", "result|TRAIN_RESULT_INCONCLUSIVE":
		return current == "COLLECTING"
	case "collecting|VALIDATION_RUNNING":
		return current == "TRAIN_RESULT_POSITIVE"
	case "result|VALIDATION_RESULT_POSITIVE", "result|VALIDATION_RESULT_NEGATIVE", "result|VALIDATION_RESULT_INCONCLUSIVE":
		return current == "VALIDATION_RUNNING"
	case "collecting|HOLDOUT_RUNNING":
		return current == "VALIDATION_RESULT_POSITIVE"
	case "holdout_pass|REPLICATED_UNTOUCHED", "holdout_fail|HOLDOUT_FAILED":
		return current == "HOLDOUT_RUNNING"
	default:
		return false
	}
}

// AppendResearchExperimentEvent is the only supported result/state writer.  It validates legal
// transitions and provenance, including negative/inconclusive results.  Even a holdout pass cannot
// mutate the schema-enforced zero paper/LIVE authority fields.
func (s *Store) AppendResearchExperimentEvent(ctx context.Context, in ResearchExperimentEventInput) (int64, error) {
	in.ExperimentID = strings.TrimSpace(in.ExperimentID)
	in.EventType = strings.ToLower(strings.TrimSpace(in.EventType))
	in.State = strings.ToUpper(strings.TrimSpace(in.State))
	in.ResultClass = strings.ToLower(strings.TrimSpace(in.ResultClass))
	if in.ExperimentID == "" || in.EventType == "" || in.State == "" || strings.TrimSpace(in.Message) == "" {
		return 0, errors.New("incomplete research experiment event")
	}
	allowedType := map[string]bool{"collecting": true, "result": true, "holdout_pass": true,
		"holdout_fail": true, "killed": true, "blocked": true, "note": true}
	if !allowedType[in.EventType] {
		return 0, fmt.Errorf("unsupported research event type %q", in.EventType)
	}
	var err error
	in.EvidenceJSON, err = normalizedJSON(in.EvidenceJSON, "{}")
	if err != nil {
		return 0, err
	}
	requiresResult := in.EventType == "result" || in.EventType == "holdout_pass" || in.EventType == "holdout_fail" || in.EventType == "killed"
	if requiresResult {
		if !map[string]bool{"positive": true, "negative": true, "inconclusive": true, "data_quality": true}[in.ResultClass] ||
			in.EvidenceJSON == "{}" || in.CodeManifestHash == "" || in.DataManifestHash == "" || in.SourceManifestHash == "" {
			return 0, errors.New("result event requires class, non-empty evidence, and code/data/source manifest hashes")
		}
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()
	if in.ExperimentVersion <= 0 {
		if err := tx.QueryRowContext(ctx, `SELECT MAX(version) FROM research_experiment_specs WHERE experiment_id=?`, in.ExperimentID).Scan(&in.ExperimentVersion); err != nil {
			return 0, err
		}
	}
	var specCode, specData, initial string
	if err := tx.QueryRowContext(ctx, `SELECT code_manifest_hash,data_manifest_hash,initial_state
FROM research_experiment_specs WHERE experiment_id=? AND version=?`, in.ExperimentID, in.ExperimentVersion).
		Scan(&specCode, &specData, &initial); err != nil {
		return 0, err
	}
	var priorID int64
	var current string
	err = tx.QueryRowContext(ctx, `SELECT id,state FROM research_experiment_events
WHERE experiment_id=? AND experiment_version=? ORDER BY id DESC LIMIT 1`, in.ExperimentID, in.ExperimentVersion).
		Scan(&priorID, &current)
	if errors.Is(err, sql.ErrNoRows) {
		current, priorID = initial, 0
	} else if err != nil {
		return 0, err
	}
	if !validExperimentTransition(in.EventType, current, in.State) {
		return 0, fmt.Errorf("illegal research state transition %s --%s--> %s", current, in.EventType, in.State)
	}
	if requiresResult && (in.CodeManifestHash != specCode || in.DataManifestHash != specData) {
		return 0, errors.New("result manifests do not match frozen experiment specification")
	}
	res, err := tx.ExecContext(ctx, `INSERT INTO research_experiment_events(
experiment_id,experiment_version,observed_ts,event_type,state,message,evidence_json,result_class,
code_manifest_hash,data_manifest_hash,source_manifest_hash,prior_event_id)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`, in.ExperimentID, in.ExperimentVersion, nowRFC(), in.EventType,
		in.State, strings.TrimSpace(in.Message), in.EvidenceJSON, in.ResultClass, in.CodeManifestHash,
		in.DataManifestHash, in.SourceManifestHash, nullablePositiveInt(int(priorID)))
	if err != nil {
		return 0, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	return id, tx.Commit()
}

func commonHoldout() string {
	return `{"split":"canonical-event chronological train/validation/untouched-test","cluster":"canonical event + UTC day","purge_embargo":true,"hierarchical_shrinkage":true,"dependence_safe_multiplicity":true,"zero_opportunity_days":true,"maker_cancels_zero":true,"open_payoff_envelope":true,"minimum_days":30}`
}

func frozenManifestHash(kind, system string) string {
	h := sha256.Sum256([]byte("R138|" + kind + "|v1|" + system))
	return "sha256:" + hex.EncodeToString(h[:])
}

func completeExperimentContract(e ResearchExperimentSpec) ResearchExperimentSpec {
	// These are deliberately verbose, frozen contracts.  They are not evidence that the system
	// works; they make it impossible to choose cells, costs, horizons, or inference after seeing a
	// result.  A semantic change must create v2.
	e.CellsJSON = fmt.Sprintf(`{"system":%q,"axes":["venue","route","bought_side","direct_or_inverse","pre_or_live","category","liquidity_bin","quote_age_bin"],"separate_cells":true,"zero_opportunity_days":true}`, e.ExperimentID)
	e.PrimaryEstimand = "fee-and-rebate-net settled P&L cents per independently eligible one-share opportunity; secondary conservative Net/day includes zero-opportunity days"
	e.HorizonRule = "settlement is primary; frozen 10s/60s/5m executable markouts are diagnostics only; unresolved rows remain censor-safe payoff envelopes and never become scratches"
	e.FeeRouteRule = "freeze side-specific executable bid/ask, depth, quote age, tick structure, latency budget and authoritative fee/rebate version at decision time; maker and taker are separate experiments; unfilled or canceled maker attempts earn zero"
	e.SourceSchemaJSON = `{"required":["canonical_event_v1","canonical_payoff_v1","side_book_frame_v1","fee_authority_v1","route_ledger_v1"],"schema_drift":"block","source_clock_required":true}`
	e.InferenceJSON = `{"alpha":0.05,"interval":"one-sided 95% lower bound","resampling":"deterministic canonical-event-day block bootstrap","replicates":10000,"chronology":"train then validation then untouched test","purge_embargo":true,"selection_adjusted":true}`
	e.ShrinkageJSON = `{"model":"hierarchical partial pooling","levels":["system","venue","route","category","regime"],"variance_floor":true,"singletons_cannot_promote":true}`
	e.MultiplicityMethod = "predeclared family-wise Holm step-down over system cells, followed by untouched event-day replication"
	e.CapacityMetric = "fee-net lower-bound dollars/day across executable depth before edge decays, with one-share baseline and measured fill probability"
	e.CapitalTimeMetric = "integral of dollars committed over hours until fill/cancel/settlement, including partial and orphan exposure"
	e.SystemKillRule = e.StoppingRule + " System-specific kill: " + e.NullHypothesis
	e.CodeManifestHash = frozenManifestHash("code-contract", e.ExperimentID)
	e.DataManifestHash = frozenManifestHash("data-contract", e.ExperimentID)
	if e.ParentExperiment != "" && e.ParentVersion == 0 {
		e.ParentVersion = 1
	}
	return e
}

func r137ExperimentBlueprints() []ResearchExperimentSpec {
	hold := commonHoldout()
	stop := "No paper or LIVE authority until a later untouched event-day replication has a positive executable-edge lower bound; kill on source, identity, fee, or route invalidation."
	cohort := `{"venues":["kalshi","polyus"],"research_reference":["polymarket"],"maker_taker_separate":true,"direct_inverse_separate":true,"book_required":true,"exact_fee_required":true}`
	specs := []ResearchExperimentSpec{
		// Version 1 is immutable evidence. "Mode 4" is the historical registered label; current
		// operator surfaces translate that compatibility name to Adaptive Allocation Model.
		{ExperimentID: "proper-score-executor", SystemName: "proper-score-executor", Mechanism: "Convert a full probability forecast into Brier, log, or spherical position vectors, then admit only current fee-net executable book actions.", Hypothesis: "A frozen forecast persona has positive score advantage and fee-net executable Net/day versus equal-share, max-margin, Mode 4, and no-trade controls.", NullHypothesis: "Proper-score routing has non-positive executable edge or no advantage over controls.", Direction: "two-sided+abstain", CohortJSON: cohort, Route: "maker and taker separate", HoldoutJSON: hold, StoppingRule: stop, MultiplicityFamily: "forecast-transform", InitialState: "COLLECTING", Version: 1, RequiredDays: 30},
		{ExperimentID: "forecast-persona-router", SystemName: "forecast-persona-router", Mechanism: "Route frozen forecast personas to Brier, log, spherical, or abstain from prior event-grouped calibration behavior.", Hypothesis: "Persona-conditioned transforms improve untouched executable score and Net/day.", NullHypothesis: "Persona routing does not improve untouched executable economics.", Direction: "router", CohortJSON: cohort, Route: "research-only transform selection", HoldoutJSON: hold, StoppingRule: stop, MultiplicityFamily: "forecast-transform", ParentExperiment: "proper-score-executor", Version: 1, RequiredDays: 30},
		{ExperimentID: "payoff-constraint-solver", SystemName: "payoff-constraint-solver", Mechanism: "Compile verified payoff predicates into states and solve for a minimum-cost portfolio with a positive worst-state floor.", Hypothesis: "Verified logical constraints produce positive fee-net payoff floors at executable depth.", NullHypothesis: "Every certified portfolio has a non-positive executable payoff floor.", Direction: "state-vector", CohortJSON: cohort, Route: "all-leg taker or venue-native RFQ", HoldoutJSON: hold, StoppingRule: stop, MultiplicityFamily: "payoff-constraints", Version: 1, RequiredDays: 30},
		{ExperimentID: "outcome-set-expansion-shock", SystemName: "outcome-set-expansion-shock", Mechanism: "Measure probability redistribution after authoritative entrant additions, cuts, withdrawals, disqualifications, or removals.", Hypothesis: "A preregistered survivor residual persists after membership changes and all route costs.", NullHypothesis: "Executable survivor prices incorporate membership changes without positive residual.", Direction: "membership-shock", CohortJSON: cohort, Route: "maker and taker separate", HoldoutJSON: hold, StoppingRule: stop, MultiplicityFamily: "outcome-set", Version: 1, RequiredDays: 30},
		{ExperimentID: "settlement-latency-carry", SystemName: "settlement-latency-carry", Mechanism: "Price near-certain contracts net of adjudication delay, dispute/void reserve, and alternative capital-day return.", Hypothesis: "Some settlement delays leave a positive conservative carry lower bound.", NullHypothesis: "Observed discounts are fully explained by risk and capital time.", Direction: "carry", CohortJSON: cohort, Route: "taker and patient maker separate", HoldoutJSON: hold, StoppingRule: stop, MultiplicityFamily: "capital-mechanics", Version: 1, RequiredDays: 30},
		{ExperimentID: "deadline-hazard-surface", SystemName: "deadline-hazard-surface", Mechanism: "Fit monotone occurrence-by-deadline probabilities and detect fee-net violations across linked deadlines.", Hypothesis: "Executable books contain persistent violations of a source-compatible monotone hazard surface.", NullHypothesis: "All deviations lie inside uncertainty and route costs.", Direction: "monotone-residual", CohortJSON: cohort, Route: "constraint taker and maker separate", HoldoutJSON: hold, StoppingRule: stop, MultiplicityFamily: "time-surfaces", Version: 1, RequiredDays: 30},
		{ExperimentID: "score-state-surface", SystemName: "score-state-surface", Mechanism: "Infer one coherent game-state distribution across moneyline, spread, total, team-total, prop, and joint contracts.", Hypothesis: "Linked contracts expose executable marginal or joint residuals after correlation uncertainty.", NullHypothesis: "One coherent state surface prices all linked books within costs.", Direction: "joint-residual", CohortJSON: cohort, Route: "single-leg and multi-leg separate", HoldoutJSON: hold, StoppingRule: stop, MultiplicityFamily: "sports-state", Version: 1, RequiredDays: 30},
		{ExperimentID: "replenishment-fingerprint", SystemName: "replenishment-fingerprint", Mechanism: "Separate informed depletion from inventory refill using sequenced depth, cancels, trades, refill shape, confirmation, and markout.", Hypothesis: "Predeclared depletion/refill states predict route-specific fee-net markout and settlement.", NullHypothesis: "Replenishment states have no route-specific executable value.", Direction: "microstructure-router", CohortJSON: cohort, Route: "taker on persistent depletion; maker on verified refill", HoldoutJSON: hold, StoppingRule: stop, MultiplicityFamily: "microstructure", Version: 1, RequiredDays: 30},
		{ExperimentID: "clientele-clock-basis", SystemName: "clientele-clock-basis", Mechanism: "Model cross-venue basis by local hour, audience, market type, event phase, and liquidity.", Hypothesis: "A preregistered venue/time basis direction survives executable route costs.", NullHypothesis: "Clock-conditioned cross-venue basis has non-positive executable edge.", Direction: "clock-basis", CohortJSON: cohort, Route: "cross-venue maker/taker cells", HoldoutJSON: hold, StoppingRule: stop, MultiplicityFamily: "cross-venue-basis", Version: 1, RequiredDays: 30},
		{ExperimentID: "series-roll-anchor", SystemName: "series-roll-anchor", Mechanism: "Measure stale opening anchors in repeated daily or weekly listings conditional on prior issue, fair value, and liquidity.", Hypothesis: "Series-conditioned opening residuals predict fee-net repricing on untouched issues.", NullHypothesis: "New issues open at efficient executable prices.", Direction: "opening-residual", CohortJSON: cohort, Route: "maker and taker separate", HoldoutJSON: hold, StoppingRule: stop, MultiplicityFamily: "listing-lifecycle", Version: 1, RequiredDays: 30},
		{ExperimentID: "collateral-release-rotation", SystemName: "collateral-release-rotation", Mechanism: "Measure whether resolution-driven collateral release rotates into linked markets.", Hypothesis: "Large releases cause preregistered short-horizon linked-book flow or maker-income effects.", NullHypothesis: "Collateral release has no executable downstream effect.", Direction: "capital-flow", CohortJSON: cohort, Route: "maker and taker separate", HoldoutJSON: hold, StoppingRule: stop, MultiplicityFamily: "capital-mechanics", Version: 1, RequiredDays: 30},
		{ExperimentID: "semantic-complexity-premium", SystemName: "semantic-complexity-premium", Mechanism: "Model whether complex boundaries, sources, revisions, timezones, void rules, or discretion cause miscalibration or only a risk discount.", Hypothesis: "A predeclared complexity feature has positive executable residual value or improves abstention.", NullHypothesis: "Complexity has no executable value beyond a justified risk discount.", Direction: "residual-or-abstain", CohortJSON: cohort, Route: "research filter; maker/taker separate if identified", HoldoutJSON: hold, StoppingRule: stop, MultiplicityFamily: "semantic-risk", Version: 1, RequiredDays: 30},
		{ExperimentID: "attention-spillover-graph", SystemName: "attention-spillover-graph", Mechanism: "Measure delayed diffusion from prominent parent volume/news shocks into preregistered linked children.", Hypothesis: "Graph-conditioned child responses persist beyond feasible latency and all route costs.", NullHypothesis: "Children update synchronously or no positive residual remains.", Direction: "graph-diffusion", CohortJSON: cohort, Route: "maker and taker separate", HoldoutJSON: hold, StoppingRule: stop, MultiplicityFamily: "attention-graph", Version: 1, RequiredDays: 30},
		{ExperimentID: "flow-direction-integrity", SystemName: "flow-direction-integrity", Mechanism: "Join authoritative execution actions where available and segregate inferred aggressor direction as lower-authority data.", Hypothesis: "Authority-corrected direction materially changes flow/toxicity inference and downstream route economics.", NullHypothesis: "Inferred and authoritative flow labels yield equivalent decisions.", Direction: "data-control", CohortJSON: cohort, Route: "research-only label audit", HoldoutJSON: hold, StoppingRule: stop, MultiplicityFamily: "flow-integrity", Version: 1, RequiredDays: 30},
		{ExperimentID: "paired-bridge-inversion", SystemName: "paired-bridge-inversion", Mechanism: "Freeze direct and inverse executable asks, fees, depth, identity, and clocks under one bridge opportunity.", Hypothesis: "Paired evidence identifies a stable invertible lane rather than spread or orientation error.", NullHypothesis: "Direct/inverse differences are explained by route costs or identity defects.", Direction: "paired-direct-inverse", CohortJSON: cohort, Route: "maker and taker paired", HoldoutJSON: hold, StoppingRule: stop, MultiplicityFamily: "bridge-inversion", Version: 1, RequiredDays: 30},
		{ExperimentID: "side-normalized-crowding-fade", SystemName: "side-normalized-crowding-fade", Mechanism: "Express book imbalance, signed flow, whale direction, and concentration in the candidate bought-side coordinate system.", Hypothesis: "Predeclared agreement bins identify a fee-net direct or fade lane.", NullHypothesis: "Side-normalized crowding has no executable value.", Direction: "paired-direct-fade", CohortJSON: cohort, Route: "maker and taker paired", HoldoutJSON: hold, StoppingRule: stop, MultiplicityFamily: "behavioral-flow", Version: 1, RequiredDays: 30},
		{ExperimentID: "incentive-subsidized-structural-lock", SystemName: "incentive-subsidized-structural-lock", Mechanism: "Join certified lock floors with measured incentive competition, eligibility, fill probability, credited reward, adverse selection, and capital time.", Hypothesis: "Conservatively earned rewards turn some otherwise negative structural locks positive.", NullHypothesis: "Competition, fill loss, and capital time consume the advertised subsidy.", Direction: "subsidized-lock", CohortJSON: cohort, Route: "maker legs with complete partial-fill state", HoldoutJSON: hold, StoppingRule: stop, MultiplicityFamily: "incentive-lock", Version: 1, RequiredDays: 30},
		{ExperimentID: "identity-challenged-cross-venue-lock", SystemName: "identity-challenged-cross-venue-lock", Mechanism: "Rebuild apparent locks around canonical event/rules certificates and simultaneous fee-complete books; preserve matcher rejections.", Hypothesis: "A payoff-certified subset contains positive executable lock floors.", NullHypothesis: "Apparent gaps are identity, schema, staleness, or route artifacts.", Direction: "certified-lock", CohortJSON: cohort, Route: "all-leg taker or atomic venue route", HoldoutJSON: hold, StoppingRule: stop, MultiplicityFamily: "cross-venue-lock", Version: 1, RequiredDays: 30},
		{ExperimentID: "maker-salvage-matched-cohort", SystemName: "maker-salvage-matched-cohort", Mechanism: "Pair a preregistered post-only attempt with a matched no-order control when the same frozen model fails at the taker ask.", Hypothesis: "Natural fills have positive exact-fee settlement economics after adverse selection and capital time.", NullHypothesis: "Conditional fills erase the apparent maker salvage; cancels correctly earn zero.", Direction: "maker-vs-control", CohortJSON: cohort, Route: "post-only maker plus no-order control", HoldoutJSON: hold, StoppingRule: stop, MultiplicityFamily: "maker-salvage", Version: 1, RequiredDays: 30},
	}
	for i := range specs {
		specs[i] = completeExperimentContract(specs[i])
	}
	return specs
}

func r171ReplenishmentTimingBlueprint(base ResearchExperimentSpec) ResearchExperimentSpec {
	base.Version = 2
	base.Mechanism = "Separate informed depletion from inventory refill using sequenced depth, cancels, trades, refill shape, confirmation, and RAM-clocked fixed-horizon top-touch samples."
	base.Hypothesis = "Predeclared depletion/refill states predict route-specific fee-net markout and settlement when 5s/30s/300s samples are captured on the independent RAM clock."
	base.CohortJSON = `{"venues":["kalshi"],"research_reference":[],"maker_taker_separate":true,"direct_inverse_separate":true,"book_required":true,"exact_fee_required":true,"timing_cohort":"ram-fixed-horizon-v2","legacy_gate_delayed_marks_comparable":false}`
	base.HorizonRule = "settlement is primary; 5s/30s/300s diagnostics must freeze target, actual sample time, lateness, sequenced top touch and source clock in RAM before persistence; samples over the six-second lateness bound are controls only; version-1 gate-delayed marks are non-comparable"
	code := sha256.Sum256([]byte("R171|code-contract|replenishment-fingerprint|ram-fixed-horizon-v2"))
	data := sha256.Sum256([]byte("R171|data-contract|replenishment-fingerprint|ram-fixed-horizon-v2"))
	base.CodeManifestHash = "sha256:" + hex.EncodeToString(code[:])
	base.DataManifestHash = "sha256:" + hex.EncodeToString(data[:])
	return base
}

func r174AtomicProjectionBlueprint(base ResearchExperimentSpec) ResearchExperimentSpec {
	switch base.ExperimentID {
	case "replenishment-fingerprint":
		base.Version = 3
		base.Mechanism = "Separate informed depletion from inventory refill using sequenced depth, cancels, trades, refill shape, confirmation, RAM-clocked fixed-horizon top-touch samples, and an atomic frozen projection outbox."
		base.Hypothesis = "Predeclared depletion/refill states predict route-specific fee-net markout and settlement when every raw 5s/30s/300s sample and its byte-frozen candidate/control disposition commit together."
		base.CohortJSON = `{"venues":["kalshi"],"research_reference":[],"maker_taker_separate":true,"direct_inverse_separate":true,"book_required":true,"exact_fee_required":true,"timing_cohort":"ram-fixed-horizon-atomic-v3","legacy_gate_delayed_marks_comparable":false,"legacy_non_atomic_projection_comparable":false,"projection_contract":"step7-frozen-outbox-v1"}`
		base.HorizonRule = "settlement is primary; 5s/30s/300s diagnostics freeze target, sample time, lateness, sequenced book, identity version, tick, fee, and candidate/control disposition before one raw-plus-outbox commit; retry may consume only those bytes; versions 1-2 cannot enter this complete cohort"
		code := sha256.Sum256([]byte("R174|code-contract|replenishment-fingerprint|ram-fixed-horizon-atomic-v3|step7-frozen-outbox-v1"))
		data := sha256.Sum256([]byte("R174|data-contract|replenishment-fingerprint|ram-fixed-horizon-atomic-v3|step7-frozen-outbox-v1"))
		base.CodeManifestHash = "sha256:" + hex.EncodeToString(code[:])
		base.DataManifestHash = "sha256:" + hex.EncodeToString(data[:])
	case "flow-direction-integrity":
		base.Version = 2
		base.Mechanism = "Join authoritative execution actions, preserve inferred direction only as a label audit, and atomically bind each raw pair to a byte-frozen same-book candidate/control disposition."
		base.Hypothesis = "Authority-corrected direction materially changes fee-net route economics when the selected action and opposite control share one exact book, fee, identity version, and all-or-none projection."
		base.CohortJSON = `{"venues":["kalshi"],"research_reference":[],"maker_taker_separate":true,"direct_inverse_separate":true,"book_required":true,"exact_fee_required":true,"authoritative_trade_side_only":true,"inferred_side_can_select":false,"legacy_non_atomic_projection_comparable":false,"projection_contract":"step7-frozen-outbox-v1"}`
		base.Route = "research label audit plus same-clock paired taker candidate/control; zero authority"
		base.HorizonRule = "every raw authoritative/inferred pair commits with one immutable terminal or paired-route projection; delayed retries use only the captured identity version, book, fee, tick, clock, and decision timestamp"
		code := sha256.Sum256([]byte("R174|code-contract|flow-direction-integrity|atomic-paired-route-v2|step7-frozen-outbox-v1"))
		data := sha256.Sum256([]byte("R174|data-contract|flow-direction-integrity|atomic-paired-route-v2|step7-frozen-outbox-v1"))
		base.CodeManifestHash = "sha256:" + hex.EncodeToString(code[:])
		base.DataManifestHash = "sha256:" + hex.EncodeToString(data[:])
	}
	return base
}

func r175StructuralFlowProjectionBlueprint(base ResearchExperimentSpec) ResearchExperimentSpec {
	base.Version = Step7StructuralFlowExperimentVersion
	base.Mechanism = "Join authoritative Kalshi execution actions to one byte-frozen same-book YES/NO route pair; retain an exact venue-local structural action as a blocked negative until exact settlement, never as a verified candidate."
	base.Hypothesis = "Authority-corrected direction changes fee-net route economics when the selected action and opposite control share one exact Kalshi ticker/payoff version, book frame, tick, fee, and decision clock."
	base.CohortJSON = `{"venues":["kalshi"],"research_reference":[],"identity_scope":"kalshi_exact_ticker_payoff_route","structural_identity_action":"blocked_negative","cross_venue_equivalence_verified":false,"maker_taker_separate":true,"direct_inverse_separate":true,"book_required":true,"exact_fee_required":true,"exact_settlement_required":true,"authoritative_trade_side_only":true,"inferred_side_can_select":false,"legacy_non_atomic_projection_comparable":false,"projection_contract":"step7-frozen-outbox-v1","flow_projection_semantics":"kalshi-venue-local-structural-identity-v3","paper_authority":false,"live_authority":false}`
	base.Route = "research-only same-clock paired Kalshi taker observation; structural selected actions remain blocked and zero-authority"
	base.HorizonRule = "every raw pair commits with one immutable terminal or paired-route projection; structural identity may collect only a blocked selected action and matched control, then exact venue settlement; delayed retries use only captured identity, book, fee, tick, clock, and decision bytes"
	code := sha256.Sum256([]byte("R175|code-contract|flow-direction-integrity|kalshi-venue-local-structural-identity-v3|step7-frozen-outbox-v1"))
	data := sha256.Sum256([]byte("R175|data-contract|flow-direction-integrity|kalshi-venue-local-structural-identity-v3|step7-frozen-outbox-v1"))
	base.CodeManifestHash = "sha256:" + hex.EncodeToString(code[:])
	base.DataManifestHash = "sha256:" + hex.EncodeToString(data[:])
	return base
}

// EnsureR138ResearchBlueprints preserves the 19 immutable R137 version-1 system specifications,
// registers the corrected Step-7 timing cohort as replenishment-fingerprint v2, then registers the
// R174 atomic-projection cohorts. Pre-fix late or non-atomically projected rows remain immutable
// history but cannot mix with or authorize conclusions for the complete frozen-outbox cohorts.
func (s *Store) EnsureR138ResearchBlueprints(ctx context.Context) error {
	specs := r137ExperimentBlueprints()
	if len(specs) != 19 {
		return fmt.Errorf("R137 blueprint count=%d, want 19", len(specs))
	}
	seen := map[string]bool{}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	for _, e := range specs {
		if seen[e.ExperimentID] {
			return fmt.Errorf("duplicate R137 experiment id %s", e.ExperimentID)
		}
		seen[e.ExperimentID] = true
		if _, err := registerExperimentSpecTx(ctx, tx, e); err != nil {
			return err
		}
	}
	var timingV2 ResearchExperimentSpec
	for _, e := range specs {
		if e.ExperimentID == "replenishment-fingerprint" {
			timingV2 = r171ReplenishmentTimingBlueprint(e)
			break
		}
	}
	if timingV2.ExperimentID == "" {
		return errors.New("replenishment-fingerprint v1 blueprint missing")
	}
	if _, err := registerExperimentSpecTx(ctx, tx, timingV2); err != nil {
		return err
	}
	var replenishmentV3, flowV2, flowV3 ResearchExperimentSpec
	for _, e := range specs {
		switch e.ExperimentID {
		case "replenishment-fingerprint":
			replenishmentV3 = r174AtomicProjectionBlueprint(e)
		case "flow-direction-integrity":
			flowV2 = r174AtomicProjectionBlueprint(e)
			flowV3 = r175StructuralFlowProjectionBlueprint(e)
		}
	}
	if replenishmentV3.Version != 3 || flowV2.Version != 2 ||
		flowV3.Version != Step7StructuralFlowExperimentVersion {
		return errors.New("R174 Step-7 atomic projection blueprints missing")
	}
	if _, err := registerExperimentSpecTx(ctx, tx, replenishmentV3); err != nil {
		return err
	}
	if _, err := registerExperimentSpecTx(ctx, tx, flowV2); err != nil {
		return err
	}
	if _, err := registerExperimentSpecTx(ctx, tx, flowV3); err != nil {
		return err
	}
	return tx.Commit()
}

type StructuralIdentityRow struct {
	Venue, Ticker, GameID, MktType, YesTeam, NoTeam, Src string
	Line                                                 float64
	League, Away, Home, AwayName, HomeName, StartUTC     string
	SourceLastSeen                                       string
}

// StructuralIdentityPage reads only the venue-metadata-derived game joins. Fuzzy/title matches are
// intentionally absent: canonical research identity loses recall rather than fabricating payoff.
func (s *Store) StructuralIdentityPage(ctx context.Context, afterSeen, afterVenue, afterTicker string, limit int) ([]StructuralIdentityRow, error) {
	if limit <= 0 || limit > 1000 {
		limit = 250
	}
	rows, err := s.db.QueryContext(ctx, `SELECT m.venue,m.ticker,m.game_id,m.mkt_type,m.yes_team,m.no_team,m.line,m.src,
g.league,g.away,g.home,g.away_name,g.home_name,g.start_utc,
CASE WHEN m.last_seen>g.last_seen THEN m.last_seen ELSE g.last_seen END AS source_last_seen
FROM market_game m JOIN game_identity g ON g.game_id=m.game_id
WHERE m.src='struct' AND (
  source_last_seen>? OR
  (source_last_seen=? AND (m.venue>? OR (m.venue=? AND m.ticker>?)))
)
ORDER BY source_last_seen,m.venue,m.ticker LIMIT ?`, afterSeen, afterSeen, afterVenue, afterVenue, afterTicker, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []StructuralIdentityRow
	for rows.Next() {
		var r StructuralIdentityRow
		if err := rows.Scan(&r.Venue, &r.Ticker, &r.GameID, &r.MktType, &r.YesTeam, &r.NoTeam,
			&r.Line, &r.Src, &r.League, &r.Away, &r.Home, &r.AwayName, &r.HomeName, &r.StartUTC,
			&r.SourceLastSeen); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

type CatalogIdentityRow struct {
	Venue, Ticker, EventKey, Kind, Title, CloseTS, SourceLastSeen string
}

// CatalogIdentityPage gives every non-structural market a fail-closed venue-local event/payoff.
// It expands graph coverage without asserting cross-venue equivalence from titles.
func (s *Store) CatalogIdentityPage(ctx context.Context, afterSeen, afterVenue, afterTicker string, limit int) ([]CatalogIdentityRow, error) {
	if limit <= 0 || limit > 1000 {
		limit = 250
	}
	rows, err := s.db.QueryContext(ctx, `SELECT c.venue,c.ticker,c.event_key,c.kind,c.title,c.close_ts,c.last_seen
FROM market_catalog c LEFT JOIN market_game m ON m.venue=c.venue AND m.ticker=c.ticker AND m.src='struct'
WHERE m.ticker IS NULL AND (
 c.last_seen>? OR (c.last_seen=? AND (c.venue>? OR (c.venue=? AND c.ticker>?)))
)
ORDER BY c.last_seen,c.venue,c.ticker LIMIT ?`, afterSeen, afterSeen, afterVenue, afterVenue, afterTicker, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []CatalogIdentityRow
	for rows.Next() {
		var r CatalogIdentityRow
		if err := rows.Scan(&r.Venue, &r.Ticker, &r.EventKey, &r.Kind, &r.Title, &r.CloseTS, &r.SourceLastSeen); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// CatalogIdentityExact is the indexed, one-market counterpart to CatalogIdentityPage. It exists
// for current book-native collectors whose ticker can outrun the 500-row background identity
// cursor. The caller may construct only the same venue-local UNVERIFIED identity as the page
// builder; title or event-key similarity never grants cross-venue equivalence.
func (s *Store) CatalogIdentityExact(ctx context.Context, venue, ticker string) (CatalogIdentityRow, bool, error) {
	venue, ticker = strings.ToLower(strings.TrimSpace(venue)), strings.TrimSpace(ticker)
	if venue == "" || ticker == "" {
		return CatalogIdentityRow{}, false, nil
	}
	var row CatalogIdentityRow
	err := s.db.QueryRowContext(ctx, `SELECT venue,ticker,event_key,kind,title,close_ts,last_seen
FROM market_catalog WHERE venue=? AND ticker=? LIMIT 1`, venue, ticker).Scan(&row.Venue,
		&row.Ticker, &row.EventKey, &row.Kind, &row.Title, &row.CloseTS, &row.SourceLastSeen)
	if errors.Is(err, sql.ErrNoRows) {
		return CatalogIdentityRow{}, false, nil
	}
	return row, err == nil, err
}

type ResearchExperimentView struct {
	SystemID, ExperimentID, SystemName, Mechanism, Direction, Route string
	InitialState, CurrentState, MultiplicityFamily                  string
	RuntimeReason, RuntimeObserved                                  string
	CollectorIDs, EvidenceTables, Prerequisites                     []string
	PrimaryEstimand, HorizonRule, FeeRouteRule, SystemKillRule      string
	CodeManifestHash, DataManifestHash                              string
	Version, RequiredDays                                           int
	Funded, PaperAuthority, LiveAuthority                           bool
}

// ResearchFoundationReport is the human/API surface for Step 1. It exposes current versions and
// explicitly repeats the zero-authority contract; no route or placement function reads it.
func (s *Store) ResearchFoundationReport(ctx context.Context) (map[string]any, error) {
	counts := map[string]int{}
	for key, q := range map[string]string{
		"event_ids":           `SELECT COUNT(DISTINCT event_id) FROM research_event_specs`,
		"event_versions":      `SELECT COUNT(*) FROM research_event_specs`,
		"payoff_ids":          `SELECT COUNT(DISTINCT payoff_id) FROM research_payoff_specs`,
		"payoff_versions":     `SELECT COUNT(*) FROM research_payoff_specs`,
		"instrument_ids":      `SELECT COUNT(*) FROM (SELECT 1 FROM research_instrument_specs GROUP BY venue,ticker)`,
		"instrument_versions": `SELECT COUNT(*) FROM research_instrument_specs`,
		"relation_ids":        `SELECT COUNT(DISTINCT relation_id) FROM research_payoff_relations`,
		"relation_versions":   `SELECT COUNT(*) FROM research_payoff_relations`,
		"version_sightings":   `SELECT COUNT(*) FROM research_identity_sightings`,
		"experiment_ids":      `SELECT COUNT(DISTINCT experiment_id) FROM research_experiment_specs`,
		"experiment_versions": `SELECT COUNT(*) FROM research_experiment_specs`,
	} {
		var n int
		if err := s.db.QueryRowContext(ctx, q).Scan(&n); err != nil {
			return nil, err
		}
		counts[key] = n
	}
	// The persisted table name remains migration-compatible. Operator/API terminology is Systems;
	// the legacy experiment counters stay as aliases for old readers.
	counts["system_ids"] = counts["experiment_ids"]
	counts["system_versions"] = counts["experiment_versions"]
	rows, err := s.db.QueryContext(ctx, `SELECT e.experiment_id,e.system_name,e.mechanism,e.direction,e.route,
e.initial_state,COALESCE(rr.state,(SELECT x.state FROM research_experiment_events x
 WHERE x.experiment_id=e.experiment_id AND x.experiment_version=e.version ORDER BY x.id DESC LIMIT 1),e.initial_state),
COALESCE(rr.reason,''),COALESCE(rr.observed_ts,''),COALESCE(rr.collector_ids_json,'[]'),
COALESCE(rr.evidence_tables_json,'[]'),COALESCE(rr.prerequisites_json,'[]'),
e.multiplicity_family,e.primary_estimand,e.horizon_rule,e.fee_route_rule,e.system_kill_rule,
e.code_manifest_hash,e.data_manifest_hash,e.version,e.required_days,e.funded,e.paper_authority,e.live_authority
FROM research_experiment_specs e
LEFT JOIN research_system_runtime_receipts rr ON rr.id=(SELECT MAX(r2.id)
 FROM research_system_runtime_receipts r2 WHERE r2.system_id=e.experiment_id)
WHERE e.version=(SELECT MAX(v.version) FROM research_experiment_specs v WHERE v.experiment_id=e.experiment_id)
ORDER BY e.experiment_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var experiments []ResearchExperimentView
	for rows.Next() {
		var e ResearchExperimentView
		var collectors, tables, prereqs string
		var funded, paper, live int
		if err := rows.Scan(&e.ExperimentID, &e.SystemName, &e.Mechanism, &e.Direction, &e.Route,
			&e.InitialState, &e.CurrentState, &e.RuntimeReason, &e.RuntimeObserved, &collectors, &tables, &prereqs,
			&e.MultiplicityFamily, &e.PrimaryEstimand, &e.HorizonRule,
			&e.FeeRouteRule, &e.SystemKillRule, &e.CodeManifestHash, &e.DataManifestHash, &e.Version, &e.RequiredDays,
			&funded, &paper, &live); err != nil {
			return nil, err
		}
		_ = json.Unmarshal([]byte(collectors), &e.CollectorIDs)
		_ = json.Unmarshal([]byte(tables), &e.EvidenceTables)
		_ = json.Unmarshal([]byte(prereqs), &e.Prerequisites)
		e.Funded, e.PaperAuthority, e.LiveAuthority = funded != 0, paper != 0, live != 0
		e.SystemID = e.ExperimentID
		experiments = append(experiments, e)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	states := map[string]int{}
	for _, e := range experiments {
		states[e.CurrentState]++
	}
	return map[string]any{
		"generated_at":    time.Now().UTC().Format(time.RFC3339Nano),
		"step":            1,
		"funded":          false,
		"paper_authority": false,
		"live_authority":  false,
		"counts":          counts,
		"states":          states,
		"systems":         experiments,
		"experiments":     experiments, // backward-compatible API alias
		"required_contracts": map[string]any{
			"status":            "declared requirements; evidence is reported by route ledgers and experiment events, never inferred here",
			"actual_side_books": "required", "exact_fee_authority": "required",
			"maker_taker_separate": "required", "direct_inverse_separate": "required",
			"canonical_event_day_holdout": "required", "unresolved_payoff_envelopes": "required",
			"rank_order": "positive executable edge lower bound -> conservative Net/day -> capacity -> capital time -> correlation/diversification -> Adaptive Allocation Model sizing",
		},
	}, nil
}

// ResearchExperimentIDs is the legacy compatibility name. Public runtime/UI code should use
// SystemIDs; the returned order and membership remain identical for older persisted readers.
func ResearchExperimentIDs() []string {
	return SystemIDs()
}
