package server

import (
	"strings"
	"testing"
)

func TestR147OperatorFacingAllocationTerminology(t *testing.T) {
	for _, want := range []string{
		"Adaptive Allocation Model",
		"Allocation · proof-Kelly · bankroll-scaled cap",
		"C4 is ranking only—not Adaptive Allocation Model sizing",
		"r147OperatorTerms",
	} {
		if !strings.Contains(dashboardHTML, want) {
			t.Fatalf("dashboard omitted Adaptive Allocation Model wording %q", want)
		}
	}
	for _, retired := range []string{"Mode 4", "Mode-4", "MODE 4"} {
		if strings.Contains(dashboardHTML, retired) {
			t.Fatalf("dashboard still exposes retired operator term %q", retired)
		}
	}
}
