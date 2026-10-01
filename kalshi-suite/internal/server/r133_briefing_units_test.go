package server

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/paper"
)

func bookCountLine(txt, name string) string {
	lines := strings.Split(strings.TrimSpace(txt), "\n")
	for _, line := range lines {
		if strings.Contains(line, " "+name+" ") {
			if i := strings.Index(line, "🅾️"); i >= 0 {
				return strings.TrimSpace(line[i:])
			}
		}
	}
	return ""
}

// TestR133BriefingCombosUsesBetAndUnitDenominators pins the two different denominators in the
// phone book line. One closed ledger row is one bet; Contracts/Staked are the all-in combo/lock
// units that the strategy scoreboard grades. Product-study rows are not funded book positions and
// therefore must not enter either denominator.
func TestR133BriefingCombosUsesBetAndUnitDenominators(t *testing.T) {
	s := testServer(t)
	s.bootAt = time.Now().Add(-time.Minute)
	settled := time.Now().Format(time.RFC3339Nano)
	s.xvcBk = &xvcBook{Closed: []xvcClosed{
		{xvcPos: xvcPos{Contracts: 2, Expr: xvcExprSynth}, PnL: 0.50, SettledTS: settled},
		{xvcPos: xvcPos{Contracts: 100, Expr: xvcExprProduct}, PnL: 99, SettledTS: settled},
	}, Open: []xvcPos{
		{Contracts: 1, Expr: xvcExprSynth, TS: settled},
		{Contracts: 1, Expr: xvcExprSynth, TS: time.Now().Add(-2 * time.Minute).Format(time.RFC3339Nano)},
		{Contracts: 1, Expr: xvcExprProduct, TS: settled},
	}}
	s.xvlBook = &xvLockBook{
		Closed: []xvLockOpp{{Staked: 3, StakeFeesExact: true, StakedPnL: 0.15, SettledTS: settled}},
		Open: []xvLockOpp{
			{Staked: 2, FirstSeen: settled},
			{Staked: 0, FirstSeen: settled}, // log-only lock observation, not Combos-book money
		},
	}

	hdr := s.equityHeaderBlock(context.Background())
	line := headerLine(hdr, "Combos")
	if want := "🟢 Combos +$0.65 · +$0.33/bet · +13.0¢/u 🅾️2©️2"; line != want {
		t.Fatalf("Combos book line = %q, want %q", line, want)
	}
	if got, want := bookCountLine(hdr, "Combos"), "🅾️2©️2"; got != want {
		t.Fatalf("Combos count line = %q, want %q (product-study and pre-boot opens excluded)", got, want)
	}
}

// TestR133BriefingMissingUnitIsNA prevents a partially migrated/legacy close from making the
// briefing divide the whole book's P&L by only the rows whose unit count happened to survive.
func TestR133BriefingMissingUnitIsNA(t *testing.T) {
	s := testServer(t)
	s.bootAt = time.Now().Add(-time.Minute)
	settled := time.Now().Format(time.RFC3339Nano)
	s.xvcBk = &xvcBook{Closed: []xvcClosed{
		{xvcPos: xvcPos{Contracts: 0, Expr: xvcExprSynth}, PnL: 0.20, SettledTS: settled},
	}, Open: []xvcPos{{Contracts: 1, Expr: xvcExprSynth}}}

	hdr := s.equityHeaderBlock(context.Background())
	line := headerLine(hdr, "Combos")
	if !strings.Contains(line, "🟢 Combos +$0.20 · +$0.20/bet · n/a¢/u 🅾️n/a©️1") {
		t.Fatalf("missing unit count must render n/a instead of a partial/fabricated rate: %q", line)
	}
	if got := bookCountLine(hdr, "Combos"); got != "🅾️n/a©️1" {
		t.Fatalf("missing open timestamp must render n/a instead of a lifetime open count: %q", got)
	}
}

func TestR133BriefingVenueOpenCountsAreProcessScoped(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	// InsertPaperFill venue-stamps its own time. Put the old PUS lot down before the process epoch,
	// then add this-run K/book lots after it.
	if _, err := s.store.InsertPaperFill(ctx, paper.Fill{Platform: vbPolyus, Ticker: "P-OLD", Side: "YES", Action: "BUY", Price: 0.4, Contracts: 1}); err != nil {
		t.Fatal(err)
	}
	s.bootAt = time.Now().Add(time.Millisecond)
	time.Sleep(20 * time.Millisecond) // clear Windows' coarse clock tick so the next venue stamp is post-epoch
	if _, err := s.store.InsertPaperFill(ctx, paper.Fill{Platform: vbKalshi, Ticker: "K-NEW", Side: "YES", Action: "BUY", Price: 0.4, Contracts: 1}); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	s.kfBooks = &kflowBooks{Pre: kfBook{Open: []kfPos{{TS: now, Contracts: 1}}}}
	s.xvgBook = &kfBook{Open: []kfPos{{TS: now, Contracts: 1, Platform: vbPolyus}}}

	hdr := s.equityHeaderBlock(ctx)
	if got := bookCountLine(hdr, "Kalshi"); got != "🅾️2©️0" {
		t.Fatalf("Kalshi process counts = %q, want shared-auto + kflow opens only", got)
	}
	if got := bookCountLine(hdr, "PolyUS"); got != "🅾️1©️0" {
		t.Fatalf("PolyUS process counts = %q, want current xvgap open only (pre-boot auto excluded)", got)
	}
}

func TestR142ComboBriefingUsesSettledDenominatorsOnce(t *testing.T) {
	s := testServer(t)
	s.plabLoaded = true
	s.plabCand, s.plabGraded = 50, 50
	s.plabEnum = plabEnumStats{Representation: "manifest", PoolN: 254, RawSpaceDec: "268331787405251911"}
	s.plabStats = map[string]*plabAgg{
		"overlap|2leg|linked|unprobed":  {N: 7, SumReal: 7.0},
		"indep|2leg|unrelated|unprobed": {N: 43, SumReal: -21.5},
	}

	txt := s.BriefingText(context.Background())
	for _, want := range []string{"📊 Systems"} {
		if !strings.Contains(txt, want) {
			t.Fatalf("compact combo briefing missing %q:\n%s", want, txt)
		}
	}
	if strings.Contains(txt, "🎲 Combo Paper") {
		t.Fatalf("Combo Paper must not duplicate the portfolio header:\n%s", txt)
	}
	for _, misleading := range []string{"50 combos logged", "waiting on games to finish", "full manifest + bounded prospective grade sample"} {
		if strings.Contains(txt, misleading) {
			t.Fatalf("manifest briefing still uses misleading counter label %q:\n%s", misleading, txt)
		}
	}

	rr := httptest.NewRecorder()
	s.handleParlayLab(rr, httptest.NewRequest("GET", "/api/parlaylab", nil))
	var body map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body["current_representation"] != "manifest" || body["retained_graded"] != float64(50) || body["open_candidate_rows"] != float64(0) {
		t.Fatalf("manifest API counter semantics missing: %#v", body)
	}
}
