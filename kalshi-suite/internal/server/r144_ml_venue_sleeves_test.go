package server

import (
	"context"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestR144MLVenueBanksConserveOneGrant(t *testing.T) {
	for _, total := range []float64{600, 600.01, 777.77} {
		got := mlPaperVenueBanks(total)
		if math.Abs(got[vbKalshi]+got[vbPolyus]-total) > 0.011 {
			t.Fatalf("venue sleeves = %#v; sum must equal the one %.2f ML grant", got, total)
		}
	}
	got := mlPaperVenueBanks(600)
	if got[vbKalshi] != 300 || got[vbPolyus] != 300 {
		t.Fatalf("neutral default = %#v, want $300 Kalshi + $300 PolyUS", got)
	}
}

func TestR144MLAllocHandshakeCarriesVenueSleeves(t *testing.T) {
	s := testServer(t)
	s.writeMLAllocFile(context.Background())
	b, err := os.ReadFile(filepath.Join(s.cfg().DataDir, "ml_alloc.json"))
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		Total   float64            `json:"ml_bank_usd"`
		Sleeves map[string]float64 `json:"ml_venue_banks"`
	}
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	if got.Total != 600 || got.Sleeves[vbKalshi] != 300 || got.Sleeves[vbPolyus] != 300 {
		t.Fatalf("handshake = total %.2f sleeves %#v, want one $600 grant split 300/300", got.Total, got.Sleeves)
	}
	if got.Sleeves[vbKalshi]+got.Sleeves[vbPolyus] != got.Total {
		t.Fatalf("handshake minted money: total %.2f sleeves %#v", got.Total, got.Sleeves)
	}
}

func TestR144CurrentMLAPIKeepsVenueSleeveMetrics(t *testing.T) {
	raw := map[string]any{
		"bank0":    600.0,
		"epoch_id": "ml-v2-current",
		"open":     []any{}, "closed": []any{},
		"stats": map[string]any{"venue_sleeves": map[string]any{
			vbKalshi: map[string]any{"grant": 300.0, "available": 290.0},
			vbPolyus: map[string]any{"grant": 300.0, "available": 280.0},
		}},
		"venue_sleeves": map[string]any{
			vbKalshi: map[string]any{"grant": 300.0, "available": 290.0},
			vbPolyus: map[string]any{"grant": 300.0, "available": 280.0},
		},
	}
	view, ok := currentMLPaperView(raw).(map[string]any)
	if !ok {
		t.Fatal("current ML view is not an object")
	}
	sleeves, ok := view["venue_sleeves"].(map[string]any)
	if !ok || len(sleeves) != 2 {
		t.Fatalf("top-level venue sleeves missing from API view: %#v", view["venue_sleeves"])
	}
	stats, _ := view["stats"].(map[string]any)
	if nested, ok := stats["venue_sleeves"].(map[string]any); !ok || len(nested) != 2 {
		t.Fatalf("stats venue sleeves missing from API view: %#v", stats["venue_sleeves"])
	}
}

func TestR144MLVenueSleevesCompoundIndependently(t *testing.T) {
	life := map[string]any{
		"venue_net":      map[string]any{vbKalshi: 45.0, vbPolyus: -20.0},
		"venue_net_base": map[string]any{vbKalshi: 5.0, vbPolyus: 0.0},
	}
	got := mlVenueSleeveView(600, life,
		[]any{map[string]any{"platform": vbKalshi, "price": .50, "contracts": 10.0}},
		nil, nil)
	k := got[vbKalshi].(map[string]any)
	p := got[vbPolyus].(map[string]any)
	// Kalshi: 300 + (45-5) = 340, then $5 own deployment => $335 available.
	if mlVenueMapFloat(k, "sizing_balance") != 340 || mlVenueMapFloat(k, "available") != 335 {
		t.Fatalf("Kalshi sleeve did not compound its own gain: %#v", k)
	}
	// PolyUS: 300-20 = 280. Kalshi's +40 never leaks across.
	if mlVenueMapFloat(p, "sizing_balance") != 280 || mlVenueMapFloat(p, "available") != 280 {
		t.Fatalf("PolyUS sleeve changed by the other venue or ignored its own loss: %#v", p)
	}
}

func TestR144MLVenueSleevesRepairStaleSplitFromCompleteEpoch(t *testing.T) {
	life := map[string]any{
		"net": 96.61, "net_base": 0.0, "closed": 3.0, "closed_base": 0.0,
		"venue_net":      map[string]any{vbKalshi: 4.74, vbPolyus: 20.36},
		"venue_net_base": map[string]any{vbKalshi: 0.0, vbPolyus: 0.0},
	}
	closed := []any{
		map[string]any{"platform": vbKalshi, "pnl": 4.74},
		map[string]any{"platform": vbPolyus, "pnl": 20.36},
		map[string]any{"platform": vbPolyus, "pnl": 71.51},
	}
	got := mlVenueSleeveView(600, life, nil, closed, nil)
	k := got[vbKalshi].(map[string]any)
	p := got[vbPolyus].(map[string]any)
	if mlVenueMapFloat(k, "net") != 4.74 || mlVenueMapFloat(p, "net") != 91.87 {
		t.Fatalf("complete detail did not repair stale venue split: K=%#v PUS=%#v life=%#v", k, p, life)
	}
	if math.Abs(mlVenueMapFloat(k, "net")+mlVenueMapFloat(p, "net")-96.61) > 0.001 {
		t.Fatalf("venue sleeves do not reconcile to parent current-epoch net: K=%#v PUS=%#v", k, p)
	}
}

func TestR144MLVenueSleevesDoNotRepairFromPartialRing(t *testing.T) {
	life := map[string]any{
		"net": 96.61, "net_base": 0.0, "closed": 3.0, "closed_base": 0.0,
		"venue_net":      map[string]any{vbKalshi: 4.74, vbPolyus: 91.87},
		"venue_net_base": map[string]any{vbKalshi: 0.0, vbPolyus: 0.0},
	}
	// Only two of the three epoch closes are retained. Detail must not rewrite durable truth.
	mlEnsureVenueLifetime(life, []any{
		map[string]any{"platform": vbKalshi, "pnl": 4.74},
		map[string]any{"platform": vbPolyus, "pnl": 20.36},
	})
	net := life["venue_net"].(map[string]any)
	if mlVenueMapFloat(net, vbPolyus) != 91.87 {
		t.Fatalf("partial retained ring overwrote durable PolyUS net: %#v", life)
	}
}

func TestR144MLDurableVenueCountsSurviveFiveHundredRowRing(t *testing.T) {
	if got := mlRoundMoney(0.005); got != 0.01 {
		t.Fatalf("Go booked-cent rounding = %.4f, want +0.01", got)
	}
	if got := mlRoundMoney(-0.005); got != -0.01 {
		t.Fatalf("Go booked-cent rounding = %.4f, want -0.01", got)
	}
	life := map[string]any{
		"net": 0.0, "net_base": 0.0, "closed": 0.0, "closed_base": 0.0,
		"wins": 325.0, "wins_base": 0.0,
		"venue_net":            map[string]any{vbKalshi: 0.0, vbPolyus: 0.0},
		"venue_net_base":       map[string]any{vbKalshi: 0.0, vbPolyus: 0.0},
		"venue_closed":         map[string]any{vbKalshi: 0.0, vbPolyus: 0.0},
		"venue_closed_base":    map[string]any{vbKalshi: 0.0, vbPolyus: 0.0},
		"venue_contracts":      map[string]any{vbKalshi: 0.0, vbPolyus: 0.0},
		"venue_contracts_base": map[string]any{vbKalshi: 0.0, vbPolyus: 0.0},
	}
	all := make([]any, 0, 650)
	for i := 0; i < 650; i++ {
		venue := vbKalshi
		if i%2 == 1 {
			venue = vbPolyus
		}
		booked := mlRoundMoney(0.005)
		life["net"] = mlRoundMoney(currentMLFloat(life, "net") + booked)
		life["closed"] = currentMLFloat(life, "closed") + 1
		mlAddVenueClose(life, venue, booked, 2)
		all = append(all, map[string]any{
			"platform": venue, "pnl": booked, "contracts": 2.0, "won": float64(i % 2),
			"model_cohort": currentMLCohort, "epoch_id": "ml-v2-long",
		})
	}
	retained := all[len(all)-500:]
	raw := map[string]any{
		"bank0": 600.0, "epoch_id": "ml-v2-long", "open": []any{}, "closed": retained,
		"lifetime": life, "stats": map[string]any{},
	}
	view := currentMLPaperView(raw).(map[string]any)
	stats := view["stats"].(map[string]any)
	if got := currentMLFloat(stats, "closed"); got != 650 {
		t.Fatalf("parent closed=%v, want durable 650 (not retained 500)", got)
	}
	if got := currentMLFloat(stats, "contracts"); got != 1300 {
		t.Fatalf("parent contracts=%v, want durable 1300", got)
	}
	if got := currentMLFloat(stats, "net"); got != 6.50 {
		t.Fatalf("parent net=%v, want cent-booked 6.50", got)
	}
	if got := currentMLFloat(stats, "equity"); got != 606.50 {
		t.Fatalf("parent equity=%v, want durable 606.50", got)
	}
	sleeves := view["venue_sleeves"].(map[string]any)
	for _, venue := range []string{vbKalshi, vbPolyus} {
		row := sleeves[venue].(map[string]any)
		if currentMLFloat(row, "closed") != 325 || currentMLFloat(row, "contracts") != 650 ||
			currentMLFloat(row, "net") != 3.25 {
			t.Fatalf("%s durable sleeve was derived from the 500-row ring: %#v", venue, row)
		}
	}
	b, _ := json.Marshal(view)
	var brief mlPaperBrief
	if err := json.Unmarshal(b, &brief); err != nil {
		t.Fatal(err)
	}
	econ := mlCurrentEconomics(brief, time.Time{})
	if econ.SettledN != 650 || econ.Contracts != 1300 || econ.Net != 6.50 || econ.RealizedCents != 0.5 {
		t.Fatalf("briefing economics fell back to retained detail: %+v", econ)
	}
}

func TestR144MLBriefingShowsActualVenueSleeves(t *testing.T) {
	pf := mlPaperBrief{VenueSleeves: map[string]mlVenueSleeveBrief{
		vbKalshi: {Grant: 300, Net: 12, Equity: 312, Available: 300, Open: 2, Closed: 4},
		vbPolyus: {Grant: 300, Net: -5, Equity: 295, Available: 290, Open: 1, Closed: 3},
	}}
	text := mlCompactMainBrief(mlPredictionBrief{}, false, pf, true, mlBriefEconomics{})
	for _, want := range []string{
		"Kalshi · P&L +$12.00 · closed 4 · open 2",
		"PolyUS · P&L -$5.00 · closed 3 · open 1",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("New-ML briefing missing %q:\n%s", want, text)
		}
	}
}
