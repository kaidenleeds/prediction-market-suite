package storage

import (
	"reflect"
	"sort"
	"strings"
	"testing"
)

func TestSystemExecutionSpecsCoverCanonicalNineteenExactlyOnce(t *testing.T) {
	want := []string{
		"attention-spillover-graph",
		"clientele-clock-basis",
		"collateral-release-rotation",
		"deadline-hazard-surface",
		"flow-direction-integrity",
		"forecast-persona-router",
		"identity-challenged-cross-venue-lock",
		"incentive-subsidized-structural-lock",
		"maker-salvage-matched-cohort",
		"outcome-set-expansion-shock",
		"paired-bridge-inversion",
		"payoff-constraint-solver",
		"proper-score-executor",
		"replenishment-fingerprint",
		"score-state-surface",
		"semantic-complexity-premium",
		"series-roll-anchor",
		"settlement-latency-carry",
		"side-normalized-crowding-fade",
	}

	got := SystemIDs()
	if len(got) != 19 {
		t.Fatalf("SystemIDs() returned %d systems, want 19: %v", len(got), got)
	}
	if !sort.StringsAreSorted(got) {
		t.Fatalf("SystemIDs() must be stable and sorted: %v", got)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("canonical system IDs changed\n got: %v\nwant: %v", got, want)
	}
	seen := make(map[string]struct{}, len(got))
	for _, id := range got {
		if _, duplicate := seen[id]; duplicate {
			t.Fatalf("duplicate system ID %q", id)
		}
		seen[id] = struct{}{}
	}
	if compatibility := ResearchExperimentIDs(); !reflect.DeepEqual(compatibility, got) {
		t.Fatalf("ResearchExperimentIDs compatibility wrapper diverged\n got: %v\nwant: %v", compatibility, got)
	}
}

func TestSystemExecutionSpecsHaveTruthfulTypedHandoffs(t *testing.T) {
	validActions := map[SystemActionClass]bool{
		SystemActionSingle: true, SystemActionBundle: true,
		SystemActionComposite: true, SystemActionData: true,
	}
	validHandoffs := map[SystemHandoffClass]bool{
		SystemHandoffSingle: true, SystemHandoffBundle: true,
		SystemHandoffChild: true, SystemHandoffUnsupported: true,
	}
	validVenues := map[string]bool{"kalshi": true, "polyus": true}
	validSides := map[string]bool{"YES": true, "NO": true}
	validRoutes := map[string]bool{"maker": true, "taker": true, "rfq": true}

	for _, spec := range SystemExecutionSpecs() {
		spec := spec
		t.Run(spec.SystemID, func(t *testing.T) {
			if !validActions[spec.ActionClass] {
				t.Fatalf("invalid or missing action class %q", spec.ActionClass)
			}
			if !validHandoffs[spec.HandoffClass] {
				t.Fatalf("invalid or missing handoff class %q", spec.HandoffClass)
			}
			for field, value := range map[string]string{
				"trigger source":  spec.TriggerSource,
				"signal adapter":  spec.SignalAdapter,
				"Paper handoff":   spec.PaperHandoff,
				"LIVE handoff":    spec.LiveHandoff,
				"settlement path": spec.SettlementPath,
			} {
				if strings.TrimSpace(value) == "" {
					t.Fatalf("missing %s", field)
				}
			}
			if len(spec.UnsupportedReasons) == 0 {
				t.Fatal("every capability must state its current limitation explicitly")
			}
			for _, venue := range spec.Venues {
				if !validVenues[venue] {
					t.Fatalf("invalid executable venue %q", venue)
				}
			}
			for _, side := range spec.Sides {
				if !validSides[side] {
					t.Fatalf("invalid executable side %q", side)
				}
			}
			for _, route := range spec.Routes {
				if !validRoutes[route] {
					t.Fatalf("invalid executable route %q", route)
				}
			}

			variants := spec.ExactVariants()
			switch spec.HandoffClass {
			case SystemHandoffUnsupported:
				if len(spec.Routes) != 0 || len(variants) != 0 {
					t.Fatalf("unsupported system exposed order routes=%v variants=%v", spec.Routes, variants)
				}
				if spec.PaperHandoff != "unsupported" || spec.LiveHandoff != "unsupported" {
					t.Fatalf("unsupported system claims a handoff: Paper=%q LIVE=%q", spec.PaperHandoff, spec.LiveHandoff)
				}
			case SystemHandoffChild:
				if len(variants) == 0 {
					t.Fatal("child system must enumerate its named child's exact cells")
				}
				for _, variant := range variants {
					if spec.SingleOrderEligible(variant.Venue, variant.Side, variant.Route) {
						t.Fatalf("child guard masquerades as standalone order source: %+v", variant)
					}
				}
			case SystemHandoffBundle:
				if spec.ActionClass != SystemActionBundle || len(variants) == 0 {
					t.Fatalf("bundle handoff/action mismatch: action=%q variants=%v", spec.ActionClass, variants)
				}
				for _, variant := range variants {
					if spec.SingleOrderEligible(variant.Venue, variant.Side, variant.Route) {
						t.Fatalf("bundle route masquerades as single order: %+v", variant)
					}
				}
			case SystemHandoffSingle:
				if len(variants) == 0 {
					t.Fatal("single-order system has no exact executable cell")
				}
				for _, variant := range variants {
					if !spec.SingleOrderEligible(variant.Venue, variant.Side, variant.Route) {
						t.Fatalf("declared single-order cell is not eligible: %+v", variant)
					}
				}
			}
		})
	}
}

func TestSystemExecutionCapabilityDefensiveCopiesAndRejectsControls(t *testing.T) {
	specs := SystemExecutionSpecs()
	if len(specs) == 0 {
		t.Fatal("empty system registry")
	}
	originalID := specs[0].SystemID
	specs[0].SystemID = "mutated"
	specs[0].Venues[0] = "mutated"
	specs[0].UnsupportedReasons[0] = "mutated"

	got, ok := SystemExecutionCapability(originalID)
	if !ok {
		t.Fatalf("canonical system %q disappeared after mutating returned copy", originalID)
	}
	if got.SystemID != originalID || got.Venues[0] == "mutated" || got.UnsupportedReasons[0] == "mutated" {
		t.Fatalf("registry leaked a mutable backing slice: %+v", got)
	}
	got.Venues[0] = "mutated-again"
	again, _ := SystemExecutionCapability(originalID)
	if again.Venues[0] == "mutated-again" {
		t.Fatal("capability lookup did not return a defensive copy")
	}

	controls := []string{
		"side-control:spotlag", "counterfactual:kalshi-flow", "maker-control",
		"manual", "invert:invert:kalshi-flow", "control:proper-score-executor",
	}
	for _, control := range controls {
		if _, ok := SystemExecutionCapability(control); ok {
			t.Fatalf("internal control %q was registered as a system", control)
		}
	}
}

func TestSystemExecutionSpecsRepresentBothSidesAsExactChildren(t *testing.T) {
	for _, spec := range SystemExecutionSpecs() {
		for _, side := range spec.Sides {
			if side == "BOTH" || side == "B" {
				t.Fatalf("%s uses ambiguous aggregate side %q", spec.SystemID, side)
			}
		}
		if spec.SystemID == "payoff-constraint-solver" {
			if !spec.SupportsSide("YES") || spec.SupportsSide("NO") {
				t.Fatalf("payoff RFQ must expose only its venue-created YES security: %+v", spec.Sides)
			}
			continue
		}
		if !spec.SupportsSide("YES") || !spec.SupportsSide("NO") {
			t.Fatalf("%s must state both exact YES and NO capability cells", spec.SystemID)
		}
	}
}
