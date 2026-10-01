package server

import (
	"bufio"
	"context"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func r166KFBook(name, platform string) kfBook {
	opened := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC).Format(time.RFC3339Nano)
	closedLot := kfPos{TS: opened, Ticker: name + "-CLOSED", Side: "YES", Price: .4,
		Contracts: 2, Fee: .02, Platform: platform}
	return kfBook{
		Bank: 600,
		Open: []kfPos{{TS: opened, Ticker: name + "-OPEN", Side: "NO", Price: .3,
			Contracts: 3, Fee: .03, Platform: platform}},
		Closed: []kfClosed{{kfPos: closedLot, Payout: 1, PnL: 1.18, Won: true,
			SettledTS: time.Date(2026, 7, 20, 13, 0, 0, 0, time.UTC).Format(time.RFC3339Nano)}},
		Net: 7.25, Wins: 4, Losses: 2,
		NetBase: 1.25, WinsBase: 1, LossesBase: 1,
		Equity: [][2]float64{{1, 6}},
	}
}

func TestR166AllKFSpecialistLedgersStartFlatAndArchiveOldOpens(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	s.kfBooks = &kflowBooks{Pre: r166KFBook("KFLOW-PRE", "kalshi"), Live: r166KFBook("KFLOW-LIVE", "kalshi")}
	rf := r166KFBook("RAWFLOW", "kalshi")
	wx := r166KFBook("WEATHER", "kalshi")
	fi := r166KFBook("FRESHINV", "kalshi")
	xvg := r166KFBook("XVGAP", "polyus")
	fl := r166KFBook("FAVLONG", "kalshi")
	s.rfBook, s.wxBook, s.fiBook, s.xvgBook, s.flBook = &rf, &wx, &fi, &xvg, &fl
	s.cbBook = &cbBook{K: r166KFBook("CHEAP-K", "kalshi"), P: r166KFBook("CHEAP-P", "polyus")}
	gk, gp := r166KFBook("GF-K", "kalshi"), r166KFBook("GF-P", "polyus")
	s.gfBook = &gfBookState{Subs: map[string]*kfBook{"gf:alpha-k": &gk, "gf:beta-p": &gp}}

	resetAt := time.Date(2026, 7, 21, 20, 45, 12, 345678900, time.UTC)
	got, err := s.resetAllKFSpecialistBooks(ctx, resetAt)
	if err != nil {
		t.Fatalf("reset all specialist ledgers: %v", err)
	}
	if got.Ledgers != 8 || got.SubBooks != 11 || got.ArchivedOpen != 11 || got.CanceledPending != 0 {
		t.Fatalf("reset summary = %+v; want 8 ledgers / 11 subs / 11 archived / 0 pending", got)
	}

	books := map[string]*kfBook{
		"kflow-pre": &s.kfBooks.Pre, "kflow-live": &s.kfBooks.Live,
		"rawflow": s.rfBook, "weather": s.wxBook, "freshinv": s.fiBook,
		"xvgap": s.xvgBook, "favlong80": s.flBook,
		"cheap-k": &s.cbBook.K, "cheap-p": &s.cbBook.P,
		"gf-k": s.gfBook.Subs["gf:alpha-k"], "gf-p": s.gfBook.Subs["gf:beta-p"],
	}
	for name, b := range books {
		if len(b.Open) != 0 {
			t.Errorf("%s still has %d current opens", name, len(b.Open))
		}
		if len(b.Closed) != 1 || b.Net != 7.25 || b.Wins != 4 || b.Losses != 2 {
			t.Errorf("%s lost lifetime evidence: %+v", name, b)
		}
		if b.NetBase != b.Net || b.WinsBase != b.Wins || b.LossesBase != b.Losses || len(b.Equity) != 0 {
			t.Errorf("%s did not start a clean baseline: %+v", name, b)
		}
		net, wins, losses := kfCurrentEpochStats(b)
		if math.Abs(net) > 1e-12 || wins != 0 || losses != 0 {
			t.Errorf("%s current display = %.4f/%d/%d, want 0/0/0", name, net, wins, losses)
		}
	}

	for _, file := range []string{"kflow_books.json", "rawflow_book.json", "weather_book.json",
		"freshinv_book.json", "xvgap_book.json", "favlong80_book.json", "cheapband_book.json", "genfollow_book.json"} {
		if st, err := os.Stat(filepath.Join(s.cfg().DataDir, file)); err != nil || st.Size() == 0 {
			t.Errorf("canonical reset ledger %s missing/empty: stat=%v err=%v", file, st, err)
		}
	}

	f, err := os.Open(filepath.Join(s.cfg().DataDir, kfPaperResetArchiveFile))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	snapshots, commits := 0, 0
	seenOpen := map[string]bool{}
	scan := bufio.NewScanner(f)
	for scan.Scan() {
		var row kfPaperResetArchiveRecord
		if err := json.Unmarshal(scan.Bytes(), &row); err != nil {
			t.Fatalf("decode archive row: %v", err)
		}
		if row.Schema != kfPaperResetArchiveSchema || row.Epoch != got.Epoch || row.ResetAt != resetAt.Format(time.RFC3339Nano) {
			t.Fatalf("archive epoch mismatch: %+v", row)
		}
		switch row.Record {
		case "snapshot":
			snapshots++
			if row.OpenCount != 1 || len(row.Open) != 1 {
				t.Fatalf("snapshot did not preserve its open: %+v", row)
			}
			seenOpen[row.Open[0].Ticker] = true
		case "canonical-commit":
			commits++
		default:
			t.Fatalf("unknown archive record kind %q", row.Record)
		}
	}
	if err := scan.Err(); err != nil {
		t.Fatal(err)
	}
	if snapshots != 11 || commits != 11 || len(seenOpen) != 11 {
		t.Fatalf("archive rows snapshots=%d commits=%d unique_opens=%d; want 11/11/11", snapshots, commits, len(seenOpen))
	}
}

func TestR166KFResetPoisonGuardLeavesCanonicalStateUntouched(t *testing.T) {
	s := testServer(t)
	b := r166KFBook("POISON", "kalshi")
	s.rfBook = &b
	s.rfPoisoned.Store(true)
	res, err := s.resetRawFlowBookAt(time.Now())
	if err == nil || res.Committed {
		t.Fatalf("poisoned reset res=%+v err=%v; want refused", res, err)
	}
	if len(s.rfBook.Open) != 1 || s.rfBook.NetBase != 1.25 || len(s.rfBook.Equity) != 1 {
		t.Fatalf("poisoned reset mutated memory: %+v", s.rfBook)
	}
	if _, err := os.Stat(filepath.Join(s.cfg().DataDir, "rawflow_book.json")); !os.IsNotExist(err) {
		t.Fatalf("poisoned reset overwrote canonical file: %v", err)
	}

	// The shared append-only archive has the same fail-closed rule: malformed prior history may
	// never be papered over by a new apparently valid epoch.
	s.rfPoisoned.Store(false)
	if err := os.WriteFile(filepath.Join(s.cfg().DataDir, kfPaperResetArchiveFile), []byte("not-json\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	res, err = s.resetRawFlowBookAt(time.Now())
	if err == nil || res.Committed || len(s.rfBook.Open) != 1 {
		t.Fatalf("poisoned archive reset res=%+v err=%v open=%d; want refused/intact", res, err, len(s.rfBook.Open))
	}
}

func TestR166KFResetArchiveAppendsAcrossEpochs(t *testing.T) {
	s := testServer(t)
	first := r166KFBook("FIRST", "kalshi")
	s.flBook = &first
	t0 := time.Date(2026, 7, 21, 21, 0, 0, 0, time.UTC)
	r1, err := s.resetFavLong80BookAt(t0)
	if err != nil || !r1.Committed {
		t.Fatalf("first reset res=%+v err=%v", r1, err)
	}
	s.flBookMu.Lock()
	s.flBook.Open = append(s.flBook.Open, kfPos{TS: t0.Add(time.Minute).Format(time.RFC3339Nano),
		Ticker: "SECOND-OPEN", Side: "YES", Price: .45, Contracts: 1, Platform: "kalshi"})
	s.flBookMu.Unlock()
	r2, err := s.resetFavLong80BookAt(t0.Add(2 * time.Minute))
	if err != nil || !r2.Committed {
		t.Fatalf("second reset res=%+v err=%v", r2, err)
	}

	f, err := os.Open(filepath.Join(s.cfg().DataDir, kfPaperResetArchiveFile))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	found := map[string]bool{}
	scan := bufio.NewScanner(f)
	for scan.Scan() {
		var row kfPaperResetArchiveRecord
		if err := json.Unmarshal(scan.Bytes(), &row); err != nil {
			t.Fatal(err)
		}
		if row.Record == "snapshot" && row.Ledger == "favlong80_book.json" && len(row.Open) == 1 {
			found[row.Open[0].Ticker] = true
		}
	}
	if err := scan.Err(); err != nil {
		t.Fatal(err)
	}
	if !found["FIRST-OPEN"] || !found["SECOND-OPEN"] || len(found) != 2 {
		t.Fatalf("append archive lost an epoch: %v", found)
	}
}
