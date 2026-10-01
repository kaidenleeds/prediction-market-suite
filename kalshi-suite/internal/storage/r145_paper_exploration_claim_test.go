package storage

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

func insertR145PaperCandidate(t *testing.T, st *Store, systemID, suffix string, observed time.Time) int64 {
	t.Helper()
	eventID := "event:r145-paper-" + suffix
	ticker := "KXR145-" + suffix
	eventVersion, payoffID, payoffVersion := registerR138EvidenceInstrument(t, st, eventID, "kalshi", ticker)
	row := r138EvidenceFixture("candidate", true)
	row.Observed = observed.UTC()
	row.SystemID = systemID
	row.OpportunityID = "opp:r145-paper-" + suffix
	row.CanonicalEventID = eventID
	row.EventVersion = eventVersion
	row.CanonicalPayoffID = payoffID
	row.PayoffVersion = payoffVersion
	row.Ticker = ticker
	id, inserted, err := st.InsertResearchSystemObservation(context.Background(), row)
	if err != nil || !inserted {
		t.Fatalf("insert %s/%s: id=%d inserted=%v err=%v", systemID, suffix, id, inserted, err)
	}
	return id
}

func TestR145PaperExplorationClaimIsRestartSafeAndSingleUse(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	id := insertR145PaperCandidate(t, st, "deadline-hazard-surface", "claim", time.Now())
	rows, err := st.RecentResearchPaperCandidates(ctx, time.Now().Add(-time.Hour), 10)
	if err != nil || len(rows) != 1 || rows[0].ObservationID != id {
		t.Fatalf("candidates=%+v err=%v", rows, err)
	}
	source := fmt.Sprintf("r142x:taker:%d:deadline-hazard-surface", id)
	if claimed, err := st.ClaimResearchPaperCandidate(ctx, rows[0], source); err != nil || !claimed {
		t.Fatalf("first claim=%v err=%v", claimed, err)
	}
	if claimed, err := st.ClaimResearchPaperCandidate(ctx, rows[0], source); err != nil || claimed {
		t.Fatalf("duplicate claim=%v err=%v", claimed, err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	st, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if claimed, err := st.ClaimResearchPaperCandidate(ctx, rows[0], source); err != nil || claimed {
		t.Fatalf("restart replay claim=%v err=%v", claimed, err)
	}
	if rows, err := st.RecentResearchPaperCandidates(ctx, time.Now().Add(-time.Hour), 10); err != nil || len(rows) != 0 {
		t.Fatalf("claimed observation returned after restart: rows=%+v err=%v", rows, err)
	}
	if err := st.CompleteResearchPaperCandidate(ctx, id, true, "shared executor accepted current exact route"); err != nil {
		t.Fatal(err)
	}
	if err := st.CompleteResearchPaperCandidate(ctx, id, true, "duplicate completion"); err == nil {
		t.Fatal("an already-completed claim reported a successful second completion")
	}
	if err := st.CompleteResearchPaperCandidate(ctx, id+99999, false, "missing claim"); err == nil {
		t.Fatal("a missing claim reported a successful completion")
	}
}

func TestR145PaperExplorationCandidateScanIsFairAcrossSystemLanes(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	now := time.Now().UTC()
	// Five newer rows from one noisy lane must not starve the older second System.
	for i := 0; i < 5; i++ {
		insertR145PaperCandidate(t, st, "deadline-hazard-surface", fmt.Sprintf("noisy-%d", i),
			now.Add(time.Duration(i)*time.Millisecond))
	}
	insertR145PaperCandidate(t, st, "proper-score-executor", "quiet", now.Add(-time.Second))
	rows, err := st.RecentResearchPaperCandidates(context.Background(), now.Add(-time.Hour), 3)
	if err != nil {
		t.Fatal(err)
	}
	counts := map[string]int{}
	for _, row := range rows {
		counts[row.SystemID]++
	}
	if len(rows) != 3 || counts["deadline-hazard-surface"] != 2 || counts["proper-score-executor"] != 1 {
		t.Fatalf("unfair bounded scan rows=%+v counts=%v", rows, counts)
	}
}

func TestR166StaleDelayedPaperClaimBecomesNotObservedWithoutReopeningHistory(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	now := time.Now().UTC()
	staleID := insertR145PaperCandidate(t, st, "proper-score-executor", "stale-delayed", now)
	acceptedID := insertR145PaperCandidate(t, st, "proper-score-executor", "accepted-delayed", now)
	rows, err := st.RecentResearchPaperCandidates(ctx, now.Add(-time.Hour), 10)
	if err != nil {
		t.Fatal(err)
	}
	byID := map[int64]ResearchPromotionCandidate{}
	for _, row := range rows {
		byID[row.ObservationID] = row
	}
	for _, id := range []int64{staleID, acceptedID} {
		candidate, ok := byID[id]
		if !ok {
			t.Fatalf("candidate %d missing from scan", id)
		}
		source := fmt.Sprintf("r142x:taker:%d:proper-score-executor", id)
		if claimed, claimErr := st.ClaimResearchPaperCandidate(ctx, candidate, source); claimErr != nil || !claimed {
			t.Fatalf("claim %d=%v err=%v", id, claimed, claimErr)
		}
	}
	if err := st.CompleteResearchPaperCandidate(ctx, acceptedID, true,
		"delayed-paper PAPER-FILLED: exact fee"); err != nil {
		t.Fatal(err)
	}
	old := now.Add(-3 * time.Minute).Format(time.RFC3339Nano)
	if _, err := st.db.ExecContext(ctx, `UPDATE research_paper_exploration_claims SET claimed_ts=?`, old); err != nil {
		t.Fatal(err)
	}
	changed, err := st.CompleteStaleResearchPaperCandidates(ctx, now.Add(-2*time.Minute))
	if err != nil || changed != 1 {
		t.Fatalf("stale completion changed=%d err=%v", changed, err)
	}
	var staleState, staleReason, acceptedState string
	if err := st.db.QueryRowContext(ctx, `SELECT state,reason FROM research_paper_exploration_claims WHERE observation_id=?`,
		staleID).Scan(&staleState, &staleReason); err != nil {
		t.Fatal(err)
	}
	if err := st.db.QueryRowContext(ctx, `SELECT state FROM research_paper_exploration_claims WHERE observation_id=?`,
		acceptedID).Scan(&acceptedState); err != nil {
		t.Fatal(err)
	}
	if staleState != "rejected" || !strings.Contains(staleReason, "PAPER-NOT-OBSERVED") ||
		acceptedState != "accepted" {
		t.Fatalf("stale=%q/%q accepted=%q", staleState, staleReason, acceptedState)
	}
}
