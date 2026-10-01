package server

import (
	"strings"
	"testing"
)

func TestR135LoadPaperUsesCoalescedJSONGets(t *testing.T) {
	start := strings.Index(dashboardHTML, "function loadPaper(){")
	if start < 0 {
		t.Fatal("dashboard missing loadPaper")
	}
	body := dashboardHTML[start:]
	if end := strings.Index(body, "\nfunction platLbl("); end >= 0 {
		body = body[:end]
	} else {
		t.Fatal("could not isolate loadPaper body")
	}
	for _, want := range []string{
		`jget("/api/pnl-series?session=1")`,
		`jget("/api/paper")`,
		`jget("/api/ml")`,
		`jget("/api/settings")`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("loadPaper missing coalesced request %s", want)
		}
	}
	for _, stale := range []string{
		`fetch("/api/pnl-series?session=1")`,
		`fetch("/api/paper")`,
		`fetch("/api/ml")`,
		`fetch("/api/settings")`,
	} {
		if strings.Contains(body, stale) {
			t.Fatalf("dashboard still bypasses jget with %s", stale)
		}
	}
}
