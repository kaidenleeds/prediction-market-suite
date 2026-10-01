package storage

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// The three composite systems below do not originate an order from a raw observation. Their
// executable adapter is the common frozen-selector bridge: exact same-clock controls train on
// prior event-disjoint UTC days, and only a post-freeze observation matching the immutable arm is
// copied into the candidate lane. The shared Paper executor still refreshes the live book, fee,
// depth, lifecycle and route; this test pins only the candidate-side handoff contract.
func TestR145CompositeSystemsEmitOnlyPostFreezeExactSingleCandidates(t *testing.T) {
	for _, systemID := range []string{
		"attention-spillover-graph",
		"clientele-clock-basis",
		"collateral-release-rotation",
		"semantic-complexity-premium",
	} {
		systemID := systemID
		t.Run(systemID, func(t *testing.T) {
			st, err := Open(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			ctx := context.Background()
			base := time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC)
			cohort := "r145-composite-selector-fixture"

			insert := func(observed time.Time, eventID, ticker, side string, realized *float64) int64 {
				t.Helper()
				eventVersion, payoffID, payoffVersion := registerR138EvidenceInstrument(t, st,
					eventID, "kalshi", ticker)
				row := r138EvidenceFixture("control", false)
				row.Observed, row.SystemID = observed, systemID
				row.OpportunityID, row.Cohort = eventID+"|"+side, cohort
				row.CanonicalEventID, row.EventVersion = eventID, eventVersion
				row.CanonicalPayoffID, row.PayoffVersion = payoffID, payoffVersion
				row.Venue, row.Ticker, row.Route, row.Side = "kalshi", ticker, "taker", side
				row.Cost, row.Fee, row.PayoutLower, row.PayoutUpper = .40, .01, 0, 1
				row.NetLower, row.NetUpper, row.Blocker = -.41, .59, "selector training control"
				row.CapacityCurve = []CapacityPoint{{Size: 1, Cost: .40, Fee: .01,
					PayoutFloor: 0, NetFloor: -.41}}
				row.Inputs = map[string]any{
					"selector_training": true,
					"selector_scope":    "venue=kalshi",
					"selector_cell":     "r145-safe-composite-cell",
					"selector_arm":      side,
				}
				id, inserted, insertErr := st.InsertResearchSystemObservation(ctx, row)
				if insertErr != nil || !inserted {
					t.Fatalf("insert %s/%s=%v err=%v", eventID, side, inserted, insertErr)
				}
				if realized != nil {
					payout := *realized + row.Cost + row.Fee
					ok, updateErr := st.AppendResearchPayoffUpdate(ctx, ResearchPayoffUpdate{
						ObservationID: id, Observed: observed.Add(time.Hour), Status: "settled",
						PayoutLower: payout, PayoutUpper: payout, RealizedNet: realized,
						SourceArtifact: "fixture settlement", SourceHash: eventID + "|" + side,
					})
					if updateErr != nil || !ok {
						t.Fatalf("grade %s/%s=%v err=%v", eventID, side, ok, updateErr)
					}
				}
				return id
			}

			for i := 0; i < 20; i++ {
				observed := base.Add(time.Duration(i%7) * 24 * time.Hour)
				eventID, ticker := fmt.Sprintf("%s-event-%02d", systemID, i), fmt.Sprintf("KXR145%02d", i)
				yes, no := .20, -.10
				insert(observed, eventID, ticker, "YES", &yes)
				insert(observed, eventID, ticker, "NO", &no)
			}
			freezeAt := base.Add(8 * 24 * time.Hour)
			if frozen, freezeErr := st.FreezeEligibleResearchSelectors(ctx, freezeAt, 7, 20); freezeErr != nil || frozen != 1 {
				t.Fatalf("frozen=%d err=%v", frozen, freezeErr)
			}

			newEvent := systemID + "-post-freeze"
			yesID := insert(freezeAt.Add(time.Minute), newEvent, "KXR145NEW", "YES", nil)
			noID := insert(freezeAt.Add(time.Minute), newEvent, "KXR145NEW", "NO", nil)
			if emitted, emitErr := st.EmitFrozenSelectorCandidates(ctx, freezeAt.Add(2*time.Minute), 10); emitErr != nil || emitted != 1 {
				t.Fatalf("emitted=%d err=%v", emitted, emitErr)
			}

			var gotSystem, venue, side, route, sourceClock, bookSource, feeSource string
			var sourceID, candidate, latency, quoteAge, tick, depth, fee int
			if err := st.db.QueryRow(`SELECT o.system_id,o.venue,o.side,o.route,o.source_clock_id,
o.book_source,o.fee_source,e.source_observation_id,o.candidate,o.latency_known,o.quote_age_known,
o.tick_known,o.depth_known,o.fee_known FROM research_frozen_selector_emissions e
JOIN research_system_observations o ON o.id=e.candidate_observation_id`).Scan(
				&gotSystem, &venue, &side, &route, &sourceClock, &bookSource, &feeSource, &sourceID,
				&candidate, &latency, &quoteAge, &tick, &depth, &fee); err != nil {
				t.Fatal(err)
			}
			if gotSystem != systemID || venue != "kalshi" || side != "YES" || route != "taker" ||
				sourceID != int(yesID) || sourceID == int(noID) || candidate != 1 ||
				sourceClock == "" || bookSource == "" || feeSource == "" ||
				latency != 1 || quoteAge != 1 || tick != 1 || depth != 1 || fee != 1 {
				t.Fatalf("unsafe candidate: system=%s venue/side/route=%s/%s/%s source=%d yes/no=%d/%d "+
					"truth=%q/%q/%q flags=%d/%d/%d/%d/%d", gotSystem, venue, side, route,
					sourceID, yesID, noID, sourceClock, bookSource, feeSource,
					latency, quoteAge, tick, depth, fee)
			}
		})
	}
}

// The named identity-lock system currently owns observer controls, not a typed executable
// two-venue order. A control must never leak into the single-order candidate bridge. This remains
// true even when all of its book/fee flags are complete; a future adapter must be a typed bundle
// with explicit partial-fill and unwind semantics, not a relabelled single.
func TestR145IdentityLockObserverCannotMasqueradeAsSingleCandidate(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	eventVersion, payoffID, payoffVersion := registerR138EvidenceInstrument(t, st,
		"identity-lock-event", "kalshi", "KXIDENTITYLOCK")
	row := r138EvidenceFixture("control", false)
	row.SystemID = "identity-challenged-cross-venue-lock"
	row.OpportunityID, row.Cohort = "identity-lock-observer", "directional-gap-resolution-audit"
	row.CanonicalEventID, row.EventVersion = "identity-lock-event", eventVersion
	row.CanonicalPayoffID, row.PayoffVersion = payoffID, payoffVersion
	row.Venue, row.Ticker, row.Route, row.Side = "kalshi", "KXIDENTITYLOCK", "taker", "YES"
	row.Blocker = "observer control; no typed atomic or coordinated bundle"
	if _, inserted, insertErr := st.InsertResearchSystemObservation(ctx, row); insertErr != nil || !inserted {
		t.Fatalf("observer inserted=%v err=%v", inserted, insertErr)
	}
	candidates, err := st.RecentResearchPaperCandidates(ctx, time.Now().Add(-time.Hour), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(candidates) != 0 {
		t.Fatalf("identity observer leaked into single-order candidates: %+v", candidates)
	}
}

func TestR145CollateralNoReleaseControlsCannotFreezeAnOrderSelector(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	base := time.Date(2026, 2, 1, 1, 0, 0, 0, time.UTC)
	for i := 0; i < 20; i++ {
		for _, side := range []string{"YES", "NO"} {
			eventID := fmt.Sprintf("no-release-event-%02d", i)
			ticker := fmt.Sprintf("KXNORELEASE%02d", i)
			eventVersion, payoffID, payoffVersion := registerR138EvidenceInstrument(t, st,
				eventID, "kalshi", ticker)
			row := r138EvidenceFixture("control", false)
			row.Observed = base.Add(time.Duration(i%7) * 24 * time.Hour)
			row.SystemID, row.OpportunityID = "collateral-release-rotation", eventID+"|"+side
			row.Cohort = "deterministic-structurally-matched-no-release"
			row.CanonicalEventID, row.EventVersion = eventID, eventVersion
			row.CanonicalPayoffID, row.PayoffVersion = payoffID, payoffVersion
			row.Ticker, row.Side, row.Blocker = ticker, side, "matched no-release control"
			row.Inputs = map[string]any{
				"selector_training":             true, // old rows used true; the no-release flag must still quarantine them
				"selector_scope":                "venue=kalshi",
				"selector_cell":                 "no-release|child=spread",
				"selector_arm":                  side,
				"no_release_in_recent_30m_cell": true,
			}
			id, inserted, insertErr := st.InsertResearchSystemObservation(ctx, row)
			if insertErr != nil || !inserted {
				t.Fatalf("insert %s/%s=%v err=%v", eventID, side, inserted, insertErr)
			}
			realized := .1
			if side == "NO" {
				realized = -.1
			}
			payout := realized + row.Cost + row.Fee
			if ok, updateErr := st.AppendResearchPayoffUpdate(ctx, ResearchPayoffUpdate{
				ObservationID: id, Observed: row.Observed.Add(time.Hour), Status: "settled",
				PayoutLower: payout, PayoutUpper: payout, RealizedNet: &realized,
				SourceArtifact: "fixture settlement", SourceHash: eventID + "|" + side,
			}); updateErr != nil || !ok {
				t.Fatalf("grade %s/%s=%v err=%v", eventID, side, ok, updateErr)
			}
		}
	}
	if frozen, freezeErr := st.FreezeEligibleResearchSelectors(ctx, base.Add(8*24*time.Hour), 7, 20); freezeErr != nil || frozen != 0 {
		t.Fatalf("matched no-release controls froze %d order selectors; err=%v", frozen, freezeErr)
	}
}
