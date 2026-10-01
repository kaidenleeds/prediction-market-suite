package server

// R98 tests — DB-performance root fixes + the paper-book restructure:
//   1. SignalFamine's rewrite (per-family covering-index lookups) keeps its exact semantics:
//      newest = table MAX(ts); byFam carries ONLY requested families that fired inside the
//      window; an absent family is absent from the map (sse.go grades absence by cadence bar).
//   2. Shadow retirement (operator decision): header renders "Shadow (retired)" while open lots
//      are still settling out, drops the line once flat, and Reset P&L leaves the retired book
//      un-rebased (lots settle naturally; the record freezes readable).
//   3. Kflow twin freeze: no NEW lots while frozen; header keeps the line only while lots
//      remain ("(frozen)" marker), then drops it; Reset skips the twins' re-epoch.
//   Defaults (flags off) preserve every pre-R98 header/reset shape — the r92/r95/r97 pins run
//   against unchanged behavior.

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/paper"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestR98SignalFaminePerFamilyLookups(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	ins := func(fam, ticker string) {
		t.Helper()
		if err := s.store.InsertSignal(ctx, storage.Signal{Platform: "kalshi", Ticker: ticker, Title: "T " + ticker,
			Side: "YES", SignalType: fam, EntryPrice: 0.5}); err != nil {
			t.Fatalf("InsertSignal(%s): %v", fam, err)
		}
	}
	ins("polyus-flow", "R98-T1")
	ins("pmatch", "R98-T2")
	newest, byFam, err := s.store.SignalFamine(ctx, []string{"polyus-flow", "pmatch", "sharpline"})
	if err != nil {
		t.Fatalf("SignalFamine: %v", err)
	}
	if newest == "" {
		t.Fatalf("newest must carry the table MAX(ts)")
	}
	if _, ok := byFam["polyus-flow"]; !ok {
		t.Fatalf("polyus-flow fired and must be present: %v", byFam)
	}
	if _, ok := byFam["pmatch"]; !ok {
		t.Fatalf("pmatch fired and must be present: %v", byFam)
	}
	if _, ok := byFam["sharpline"]; ok {
		t.Fatalf("sharpline never fired — absence from byFam is the contract (sse grades it): %v", byFam)
	}
	if ts := byFam["polyus-flow"]; ts > newest {
		t.Fatalf("per-family ts %q cannot exceed table newest %q", ts, newest)
	}
}

// writeShadowBook drops a minimal ml_shadow.json into the test DataDir: bank0 + lifetime nets +
// n open $0.50 lots (shape-matched to what equityHeaderBlock's book() and resetMLBook read).
func writeShadowBook(t *testing.T, dir string, openN int, net, netBase float64) {
	t.Helper()
	opens := make([]map[string]any, 0, openN)
	for i := 0; i < openN; i++ {
		opens = append(opens, map[string]any{"ticker": "SH-" + string(rune('A'+i)), "side": "YES",
			"signal_type": "kalshi-whale", "title": "shadow lot", "price": 0.5, "contracts": 10.0,
			"opened": time.Now().Unix()})
	}
	b, err := json.Marshal(map[string]any{
		"bank0": 800.0,
		"open":  opens,
		"lifetime": map[string]any{"net": net, "net_base": netBase, "wins": 3.0, "closed": 5.0,
			"bank_ver": 3.0, "shadow_bank_ver": 4.0},
	})
	if err != nil {
		t.Fatalf("marshal shadow book: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "ml_shadow.json"), b, 0o644); err != nil {
		t.Fatalf("write shadow book: %v", err)
	}
}

// (TestR98ShadowRetiredHeaderLine removed in R127: the Shadow control book has no header line in
// the four-book block anymore — its retired body-line behavior stays pinned in r101_test.go.)

func TestR98KflowFrozenNoNewLotsAndHeader(t *testing.T) {
	s := testServer(t)
	c := *s.cfg()
	c.Auto.KflowBooksEnabled = true
	c.Auto.KflowBooksFrozen = true
	s.cfgP.Store(&c)

	// Frozen: placement is a no-op.
	s.kflowBookPlace(0, "KX-R98", "T", "YES", 0.50, 1)
	s.kfBookMu.Lock()
	preOpen := len(s.kfLoadLocked().Pre.Open)
	s.kfBookMu.Unlock()
	if preOpen != 0 {
		t.Fatalf("frozen twins must take no new lots, got %d", preOpen)
	}

	// (R127: the twins have no header lines anymore — they are roster subs of the Kalshi book;
	// the frozen/unfrozen placement gate is the surviving behavior under test.)
	s.kfBookMu.Lock()
	b := s.kfLoadLocked()
	b.Pre.Open = append(b.Pre.Open, kfPos{TS: time.Now().UTC().Format(time.RFC3339), Ticker: "KX-HOLD",
		Title: "T", Side: "YES", Price: 0.5, Contracts: 20, Fee: 0.10})
	s.kfBookMu.Unlock()

	// Unfrozen does not restore R166's retired assumed-fill path. The injected historical lot is
	// retained for settlement, but a signal alone cannot create a second lot.
	c2 := *s.cfg()
	c2.Auto.KflowBooksFrozen = false
	s.cfgP.Store(&c2)
	s.kflowBookPlace(0, "KX-R98B", "T", "YES", 0.50, 1)
	s.kfBookMu.Lock()
	preOpen = len(s.kfLoadLocked().Pre.Open)
	s.kfBookMu.Unlock()
	if preOpen != 1 {
		t.Fatalf("unfrozen twins must retain only the injected historical lot, got %d", preOpen)
	}
}

// TestR98FillsCacheDeltaRefresh — the last storm gap's fix: with a warm cache, a refresh reads
// ONLY id>maxID (new fills appear without a full-table scan); a stop edit busts to a full read.
func TestR98FillsCacheDeltaRefresh(t *testing.T) {
	s := testServer(t)
	s.fillsCacheTTL = -1 // every call refreshes (delta-mode path decides full vs delta)
	ctx := context.Background()
	buy := func(tk string) {
		t.Helper()
		if _, err := s.store.InsertPaperFill(ctx, paper.Fill{Platform: "kalshi", Ticker: tk, Title: "T " + tk,
			Side: "Yes", Action: "BUY", Price: 0.5, Contracts: 2, Fee: 0.01, Source: "manual"}); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}
	buy("R98-A")
	f, _, _, err := s.paperFillsCached(ctx) // cold → FULL read seeds cache + maxID + fullAt
	if err != nil || len(f) != 1 {
		t.Fatalf("seed read: n=%d err=%v", len(f), err)
	}
	buy("R98-B") // lands with a NEW id — the delta refresh must pick it up without a full read
	f, _, _, err = s.paperFillsCached(ctx)
	if err != nil || len(f) != 2 {
		t.Fatalf("delta refresh must surface the new fill: n=%d err=%v", len(f), err)
	}
	if f[0].Ticker != "R98-A" || f[1].Ticker != "R98-B" {
		t.Fatalf("merged order must stay (ts,id): %v %v", f[0].Ticker, f[1].Ticker)
	}
	// In-place stop edit mints no id — fillsBust forces the next refresh through the full read.
	if err := s.store.UpdatePositionStops(ctx, "kalshi", "R98-A", "Yes", 0.9, 0.1); err != nil {
		t.Fatalf("stops: %v", err)
	}
	s.fillsBust()
	f, _, _, err = s.paperFillsCached(ctx)
	if err != nil || len(f) != 2 {
		t.Fatalf("post-bust full read: n=%d err=%v", len(f), err)
	}
	var tp float64
	for _, x := range f {
		if x.Ticker == "R98-A" {
			tp = x.TP
		}
	}
	if tp != 0.9 {
		t.Fatalf("post-bust read must see the in-place stop edit (tp=%v)", tp)
	}
}

func TestR98ResetLeavesRetiredShadowAlone(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	dir := s.cfg().DataDir
	c := *s.cfg()
	c.Auto.ShadowBookRetired = true
	c.Auto.KflowBooksFrozen = true
	s.cfgP.Store(&c)

	writeShadowBook(t, dir, 2, 7.50, 0)
	if n, busy := s.doPaperReset(ctx); busy {
		t.Fatalf("reset reported lock busy in a hermetic test (closed %d)", n)
	}
	var pf map[string]any
	raw, err := os.ReadFile(filepath.Join(dir, "ml_shadow.json"))
	if err != nil || json.Unmarshal(raw, &pf) != nil {
		t.Fatalf("re-read shadow book: %v", err)
	}
	opens, _ := pf["open"].([]any)
	if len(opens) != 2 {
		t.Fatalf("retired shadow's open lots must ride the reset untouched (want 2, got %d)", len(opens))
	}
	life, _ := pf["lifetime"].(map[string]any)
	if nb, _ := life["net_base"].(float64); nb != 0 {
		t.Fatalf("retired shadow must NOT be re-based by the reset (net_base 0 → %v)", nb)
	}
}
