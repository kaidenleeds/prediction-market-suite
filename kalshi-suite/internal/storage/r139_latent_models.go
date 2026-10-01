package storage

// Durable inputs and fair cursors for the R139 score-state and outcome-expansion model collectors.
// The existing venue-frame tables remain append-only. Cursors are monotone processing bookmarks,
// never evidence and never authority.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	R139ScoreStateCollectorID       = "score-state-model-v1"
	R139ScoreStateCollectorSource   = "structural game joins plus current complete full-depth books; frozen leave-one-out marginal model"
	R139ScoreStateCollectorSchema   = "r139-score-state-loo-v1"
	R139OutcomeShockCollectorID     = "outcome-expansion-model-v1"
	R139OutcomeShockCollectorSource = "ordered immutable exhaustive outcome-set frames plus current complete full-depth books; frozen survivor redistribution"
	R139OutcomeShockCollectorSchema = "r139-outcome-expansion-v1"
)

func R139LatentCollectorSpecs() []CollectorSpec {
	return []CollectorSpec{
		{CollectorID: R139ScoreStateCollectorID, ExperimentID: "score-state-surface", ExperimentVersion: 1,
			Source: R139ScoreStateCollectorSource, SchemaVersion: R139ScoreStateCollectorSchema,
			ZeroPolicy:      "zero is healthy only after a fair bounded page of structural sports groups is attempted and every target has an explicit model, identity, clock, book, fee, or lower-residual exclusion",
			ExpectedCadence: 5 * time.Minute, Systems: []string{"score-state-surface"}},
		{CollectorID: R139OutcomeShockCollectorID, ExperimentID: "outcome-set-expansion-shock", ExperimentVersion: 1,
			Source: R139OutcomeShockCollectorSource, SchemaVersion: R139OutcomeShockCollectorSchema,
			ZeroPolicy:      "zero is healthy only when no unseen membership-change frame exists; every frame otherwise yields a candidate/control or a durable explicit blocker before the cursor advances",
			ExpectedCadence: 2 * time.Minute, Systems: []string{"outcome-set-expansion-shock"}},
	}
}

func (s *Store) EnsureR139LatentCollectorBlueprints(ctx context.Context) error {
	for _, spec := range R139LatentCollectorSpecs() {
		if _, err := s.RegisterCollectorSpec(ctx, spec); err != nil {
			return err
		}
	}
	return nil
}

type OutcomeSetFrameRecord struct {
	ID                                                    int64
	Observed, SourceUpdated                               time.Time
	Venue, EventID, Title, SourceArtifact, MembershipHash string
	ChangeClass, Blocker                                  string
	Members                                               []OutcomeSetMember
	MutuallyExclusive, Exhaustive, VoidVerified           bool
}

type OutcomeSetFramePair struct {
	Prior, Current OutcomeSetFrameRecord
}

func scanOutcomeSetFrame(scanner interface{ Scan(...any) error }) (OutcomeSetFrameRecord, error) {
	var out OutcomeSetFrameRecord
	var observed, sourceUpdated, membersJSON string
	var mutuallyExclusive, exhaustive, voidVerified int
	err := scanner.Scan(&out.ID, &observed, &sourceUpdated, &out.Venue, &out.EventID, &out.Title,
		&out.SourceArtifact, &out.MembershipHash, &membersJSON, &out.ChangeClass,
		&mutuallyExclusive, &exhaustive, &voidVerified, &out.Blocker)
	if err != nil {
		return OutcomeSetFrameRecord{}, err
	}
	out.Observed = sourceObservedTime(observed, time.Time{})
	if strings.TrimSpace(sourceUpdated) != "" {
		out.SourceUpdated = sourceObservedTime(sourceUpdated, time.Time{})
	}
	if out.Observed.IsZero() || json.Unmarshal([]byte(membersJSON), &out.Members) != nil {
		return OutcomeSetFrameRecord{}, errors.New("malformed immutable outcome-set frame")
	}
	out.MutuallyExclusive, out.Exhaustive, out.VoidVerified = mutuallyExclusive == 1, exhaustive == 1, voidVerified == 1
	return out, nil
}

const outcomeSetFrameSelect = `SELECT id,observed_ts,source_updated_ts,venue,event_id,title,
source_artifact,membership_hash,members_json,change_class,mutually_exclusive,exhaustive,
void_policy_verified,blocker FROM research_outcome_set_frames`

// OutcomeSetFrameChangesAfter returns an ID-ordered page. It never rotates or samples, so a busy
// event cannot starve another event; the caller advances only after writing an observation/receipt.
func (s *Store) OutcomeSetFrameChangesAfter(ctx context.Context, afterID int64, limit int) ([]OutcomeSetFramePair, error) {
	if afterID < 0 {
		afterID = 0
	}
	if limit <= 0 || limit > 100 {
		limit = 25
	}
	rows, err := s.db.QueryContext(ctx, outcomeSetFrameSelect+`
 WHERE id>? AND change_class NOT IN ('baseline','unchanged') ORDER BY id LIMIT ?`, afterID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var current []OutcomeSetFrameRecord
	for rows.Next() {
		row, err := scanOutcomeSetFrame(rows)
		if err != nil {
			return nil, err
		}
		current = append(current, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	out := make([]OutcomeSetFramePair, 0, len(current))
	for _, cur := range current {
		row := s.db.QueryRowContext(ctx, outcomeSetFrameSelect+`
 WHERE venue=? AND event_id=? AND id<? ORDER BY id DESC LIMIT 1`, cur.Venue, cur.EventID, cur.ID)
		prior, err := scanOutcomeSetFrame(row)
		if errors.Is(err, sql.ErrNoRows) {
			// A changed frame without a prior is a corrupt provenance gap. Preserve it as an error
			// instead of silently moving the fair cursor past unusable evidence.
			return nil, fmt.Errorf("outcome-set frame %d has no immutable prior", cur.ID)
		}
		if err != nil {
			return nil, err
		}
		out = append(out, OutcomeSetFramePair{Prior: prior, Current: cur})
	}
	return out, nil
}

func latentCursorKey(name string) string { return "r139:latent-cursor:" + strings.TrimSpace(name) }

func (s *Store) R139LatentCursor(ctx context.Context, name string) (int64, error) {
	var raw string
	err := s.db.QueryRowContext(ctx, `SELECT v FROM kv WHERE k=?`, latentCursorKey(name)).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	id, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || id < 0 {
		return 0, errors.New("malformed R139 latent cursor")
	}
	return id, nil
}

// AdvanceR139LatentCursor is monotone even if overlapping research sweeps finish out of order.
func (s *Store) AdvanceR139LatentCursor(ctx context.Context, name string, id int64) error {
	if strings.TrimSpace(name) == "" || id <= 0 {
		return errors.New("invalid R139 latent cursor advance")
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO kv(k,v,ts) VALUES(?,?,?)
ON CONFLICT(k) DO UPDATE SET v=excluded.v,ts=excluded.ts
WHERE CAST(kv.v AS INTEGER)<CAST(excluded.v AS INTEGER)`, latentCursorKey(name), strconv.FormatInt(id, 10), nowRFC())
	return err
}
