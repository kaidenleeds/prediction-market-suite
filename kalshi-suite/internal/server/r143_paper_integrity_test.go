package server

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/paper"
)

func TestR143AllNULLedgerIsPreservedAndRecovered(t *testing.T) {
	s := testServer(t)
	path := filepath.Join(s.cfg().DataDir, "favlong80_book.json")
	if err := os.WriteFile(path, make([]byte, 73), 0o644); err != nil {
		t.Fatal(err)
	}
	var dst map[string]any
	if s.readJSONLoose(path, &dst) {
		t.Fatal("all-NUL file must not parse")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("active crash artifact still exists: %v", err)
	}
	matches, err := filepath.Glob(filepath.Join(s.cfg().DataDir, "recovery", "favlong80_book.json.all-nul.*"))
	if err != nil || len(matches) != 1 {
		t.Fatalf("recovery copy count=%d err=%v", len(matches), err)
	}
	if st, err := os.Stat(matches[0]); err != nil || st.Size() != 73 {
		t.Fatalf("preserved artifact stat=%v err=%v", st, err)
	}
}

func TestR143MalformedNonNULLedgerStillFailsClosed(t *testing.T) {
	s := testServer(t)
	path := filepath.Join(s.cfg().DataDir, "favlong80_book.json")
	if err := os.WriteFile(path, []byte(`{"open":[`), 0o644); err != nil {
		t.Fatal(err)
	}
	var dst map[string]any
	if s.readJSONLoose(path, &dst) {
		t.Fatal("malformed file must not parse")
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("malformed but potentially recoverable file was discarded: %v", err)
	}
}

func TestR143BriefingPortfolioStartsAtDurableResetEpoch(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	s.bootAt = time.Now().Add(-time.Hour)
	insertRound := func(ticker string) {
		t.Helper()
		for _, fill := range []paper.Fill{
			{Platform: vbKalshi, Ticker: ticker, Side: "YES", Action: "BUY", Price: .40, Contracts: 1},
			{Platform: vbKalshi, Ticker: ticker, Side: "YES", Action: "SELL", Price: .50, Contracts: 1},
		} {
			if _, err := s.store.InsertPaperFill(ctx, fill); err != nil {
				t.Fatal(err)
			}
		}
	}
	insertRound("RESET-CLOSE")
	s.pnlMu.Lock()
	s.pnlEpoch = time.Now().UTC()
	s.pnlMu.Unlock()
	s.fillsBust()
	if m, ok := s.bookSessionClosedMetrics(ctx, vbKalshi); !ok || m.Bets != 0 || m.NetUSD != 0 {
		fills, _ := s.store.ListPaperFills(ctx)
		t.Fatalf("pre-reset close leaked into compact portfolio: %+v ok=%v epoch=%s trades=%+v", m, ok,
			s.pnlEpoch.Format(time.RFC3339Nano), paper.ClosedTrades(fills))
	}
	time.Sleep(20 * time.Millisecond)
	insertRound("POST-RESET")
	s.fillsBust()
	if m, ok := s.bookSessionClosedMetrics(ctx, vbKalshi); !ok || m.Bets != 1 || m.NetUSD <= 0 {
		t.Fatalf("post-reset close missing from compact portfolio: %+v ok=%v", m, ok)
	}
}
