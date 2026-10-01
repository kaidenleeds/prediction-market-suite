package server

import (
	"strings"
	"testing"
)

func TestR147BundleProductsDoNotBorrowKalshiConjunctionOrSingleLegHandoffs(t *testing.T) {
	want := map[string]struct {
		kind     string
		decision bool
		role     r145CatalogExecutionRole
	}{
		"event-basket-lock":                    {"additive_lock_product", true, r145ExecutionOrderSystem},
		"nested-ladder-lock":                   {"additive_two_leg_lock", true, r145ExecutionOrderSystem},
		"time-nested-lock":                     {"additive_two_leg_lock", true, r145ExecutionOrderSystem},
		"xvlock":                               {"cross_venue_two_leg_lock", true, r145ExecutionUnsupported},
		"identity-challenged-cross-venue-lock": {"identity_and_route_control", false, r145ExecutionDataControl},
		"incentive-subsidized-structural-lock": {"reward_attribution_control", false, r145ExecutionDataControl},
		"joint-marginal-lock":                  {"joint_quote_comparator_control", false, r145ExecutionDataControl},
		"lockstack":                            {"paper_execution_ledger", false, r145ExecutionPortfolioEvidence},
	}
	for id, expected := range want {
		contract, ok := r147BundleProductContractFor(id)
		if !ok || contract.Kind != expected.kind || contract.DecisionSystem != expected.decision {
			t.Fatalf("%s contract=%+v ok=%v", id, contract, ok)
		}
		role, requires, reason := r145CatalogExecutionClassification(id)
		wantRequires := expected.role == r145ExecutionOrderSystem
		if role != expected.role || requires != wantRequires || strings.TrimSpace(reason) == "" {
			t.Fatalf("%s classification=%s requires=%v reason=%q", id, role, requires, reason)
		}
		if expected.decision && (!strings.Contains(contract.Blocker, "atomic") &&
			!strings.Contains(contract.Blocker, "same security")) {
			t.Fatalf("%s actual product lacks a precise route/payoff blocker: %q", id, contract.Blocker)
		}
	}
}

func TestR147BundleAliasesAndControlsNameTheirExecutableChild(t *testing.T) {
	for _, id := range []string{
		"identity-challenged-cross-venue-lock",
		"incentive-subsidized-structural-lock",
		"joint-marginal-lock",
		"lockstack",
	} {
		row, ok := r147BundleProductContractFor(id)
		if !ok || row.DecisionSystem || strings.TrimSpace(row.ConsumedBy) == "" {
			t.Fatalf("%s must be a non-originating control/ledger with a named child: %+v", id, row)
		}
		class, handoff, standalone := researchPromotionApplicability(id)
		if standalone || (class != "router_or_filter" && class != "portfolio_or_evidence_ledger") ||
			!strings.Contains(handoff, row.ConsumedBy) {
			t.Fatalf("%s promotion surface invented an order: class=%q handoff=%q standalone=%v", id, class, handoff, standalone)
		}
	}
}

func TestR147ActualBundleProductsUseExactProductVariantsWithoutFabricatedSingles(t *testing.T) {
	variants := r145KnownSystemVariants()
	for _, id := range []string{"event-basket-lock", "nested-ladder-lock", "time-nested-lock", "xvlock"} {
		contract, ok := r147BundleProductContractFor(id)
		if !ok || len(contract.Variants) == 0 || strings.TrimSpace(contract.TriggerSource) == "" ||
			strings.TrimSpace(contract.RequiredPrimitive) == "" {
			t.Fatalf("%s lacks explicit blocked product variants/fresh producer: %+v", id, contract)
		}
		connected := 0
		for _, productVariant := range contract.Variants {
			if productVariant.Atomic || (productVariant.State != "BLOCKED_EXTERNAL_PRODUCT" &&
				productVariant.State != "WIRED_DISABLED_EXACT_PROOF") ||
				len(productVariant.Venues) == 0 || len(productVariant.Legs) == 0 {
				t.Fatalf("%s product variant misstates route truth: %+v", id, productVariant)
			}
			if productVariant.State == "WIRED_DISABLED_EXACT_PROOF" {
				connected++
			}
		}
		class, handoff, standalone := researchPromotionApplicability(id)
		if id == "xvlock" {
			if connected != 0 || standalone || class != "externally_blocked_multi_leg_product" ||
				!strings.Contains(handoff, contract.RequiredPrimitive) {
				t.Fatalf("%s promotion surface hid its missing primitive: class=%q handoff=%q standalone=%v", id, class, handoff, standalone)
			}
		} else if connected == 0 || standalone || class != "wired_staged_multi_leg_product" ||
			!strings.Contains(handoff, "staged_fok_v1") {
			t.Fatalf("%s wired staged route is hidden: connected=%d class=%q handoff=%q standalone=%v", id, connected, class, handoff, standalone)
		}
		for _, row := range variants {
			if row.SystemID == id {
				t.Fatalf("%s received a fabricated venue/side/route cell: %+v", id, row)
			}
		}
	}
}

func TestR147XVLockNamesEveryCertifiedKPolyUSOrientation(t *testing.T) {
	row, ok := r147BundleProductContractFor("xvlock")
	if !ok || len(row.Variants) != 4 {
		t.Fatalf("xvlock variants=%+v", row.Variants)
	}
	if strings.Join(row.InputVenues, ",") != "kalshi,polyus,polymarket" ||
		strings.Join(row.ExecutionVenues, ",") != "kalshi,polyus" {
		t.Fatalf("xvlock input/execution topology conflated: inputs=%v execution=%v", row.InputVenues, row.ExecutionVenues)
	}
	want := map[string]bool{"k-yes-pus-no": false, "k-no-pus-yes": false,
		"k-yes-pus-yes-flipped-identity": false, "k-no-pus-no-flipped-identity": false}
	for _, variant := range row.Variants {
		if len(variant.Venues) != 2 || variant.Venues[0] != "kalshi" || variant.Venues[1] != "polyus" {
			t.Fatalf("xvlock variant is not exact K-PUS: %+v", variant)
		}
		if _, exists := want[variant.VariantID]; !exists {
			t.Fatalf("unexpected xvlock orientation: %+v", variant)
		}
		want[variant.VariantID] = true
	}
	for id, seen := range want {
		if !seen {
			t.Errorf("missing xvlock orientation %s", id)
		}
	}
}
