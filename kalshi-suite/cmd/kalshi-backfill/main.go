// kalshi-backfill — standalone, operator-run puller for Kalshi's ARCHIVED public tape
// (R69 item 7; endpoints verified against docs.kalshi.com + live prod probes 2026-07-04:
// GET /historical/cutoff · /historical/markets?series_ticker= · /historical/trades?ticker=).
//
// NEVER run automatically. Read-only against the venue (public endpoints, no auth, no order
// writes possible), writes only the backfill_trades table + bkfl_* kv bookmarks in kalshi.db.
//
//	kalshi-backfill --series KXBTCD --days 30 [--config config.json] [--data ./data]
//
// Flow: cutoff → page the series' archived markets (newest window bounded by --days) → per
// market, page /historical/trades into backfill_trades (INSERT OR IGNORE on trade_id).
// RESUMABLE: per-ticker done-latches + a mid-ticker page cursor persist in kv under bkfl_*,
// alongside the trades_created_ts cutoff observed when the run started — re-running the same
// command continues where it stopped instead of re-pulling.
//
// The DB is opened in WAL mode (same as the suite) so running alongside the live server is
// safe, but prefer running it while the suite is idle: both processes share one SQLite write
// lock and a long backfill adds write contention the trading loops don't need.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/config"
	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "kalshi-backfill:", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		series  = flag.String("series", "", "series ticker to backfill (e.g. KXBTCD) — required")
		days    = flag.Int("days", 14, "how many days BACK from the cutoff to pull (bounded 1..120)")
		cfgPath = flag.String("config", "config.json", "suite config.json (data dir + rate limit); optional")
		dataDir = flag.String("data", "", "data dir override (default: config data_dir)")
		maxMkts = flag.Int("max-markets", 2000, "safety bound on archived markets to walk per run")
		reset   = flag.Bool("reset-bookmarks", false, "forget bkfl_* bookmarks for this series and start over (rows are kept; INSERT OR IGNORE dedups)")
	)
	flag.Parse()
	st := strings.ToUpper(strings.TrimSpace(*series))
	if st == "" {
		flag.Usage()
		return fmt.Errorf("--series is required")
	}
	if *days < 1 {
		*days = 1
	}
	if *days > 120 { // bounded by design: this is a research puller, not an archive mirror
		*days = 120
	}

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	if *dataDir != "" {
		cfg.DataDir = *dataDir
	}
	store, err := storage.Open(cfg.DataDir)
	if err != nil {
		return fmt.Errorf("open %s/kalshi.db: %w", cfg.DataDir, err)
	}
	defer store.Close()

	// Public endpoints only — no signer, prod host, the suite's own limiter/backoff. Half the
	// configured budget so a backfill run never crowds the (possibly running) live suite.
	rate := cfg.Kalshi.RateLimitPerSec / 2
	if rate <= 0 {
		rate = 5
	}
	client := kalshi.NewClient(kalshi.BaseProd, nil, rate, time.Duration(cfg.Kalshi.RequestTimeoutMs)*time.Millisecond)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop() // Ctrl-C = graceful: bookmarks make the next run resume

	kvDone := func(t string) string { return "bkfl_done_" + st + "_" + t }
	kvCur := func(t string) string { return "bkfl_cursor_" + st + "_" + t }
	kvCutoff := "bkfl_cutoff_" + st

	if *reset {
		kvs, err := store.KVPrefix(ctx, "bkfl_")
		if err != nil {
			return err
		}
		n := 0
		for k := range kvs {
			if strings.Contains(k, "_"+st+"_") || k == kvCutoff {
				_ = store.KVDel(ctx, k)
				n++
			}
		}
		fmt.Printf("cleared %d bookmark(s) for %s\n", n, st)
	}

	// 1) Cutoff — the newest instant the historical tier covers. Bookmarked so the report shows
	// which partition boundary a stored dataset was pulled against (cutoffs roll forward).
	cut, err := client.GetHistoricalCutoff(ctx)
	if err != nil {
		return fmt.Errorf("GET /historical/cutoff: %w", err)
	}
	cutTS, err := time.Parse(time.RFC3339, cut.TradesCreatedTS)
	if err != nil {
		return fmt.Errorf("parse trades_created_ts %q: %w", cut.TradesCreatedTS, err)
	}
	since := cutTS.AddDate(0, 0, -*days)
	if err := store.KVSet(ctx, kvCutoff, cut.TradesCreatedTS); err != nil {
		return fmt.Errorf("persist cutoff bookmark: %w", err)
	}
	fmt.Printf("historical cutoff (trades): %s · pulling window %s .. cutoff (%d day(s))\n",
		cut.TradesCreatedTS, since.Format("2006-01-02"), *days)

	// 2) Walk the series' archived markets; keep those whose close_time falls in the window.
	type mkt struct{ ticker, closeT string }
	var picks []mkt
	cursor, walked := "", 0
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		mkts, next, err := client.HistoricalMarketsPage(ctx, st, cursor, 1000)
		if err != nil {
			return fmt.Errorf("GET /historical/markets (series %s): %w", st, err)
		}
		walked += len(mkts)
		for _, m := range mkts {
			ct, e := time.Parse(time.RFC3339, m.CloseTime)
			if e != nil || ct.Before(since) {
				continue
			}
			picks = append(picks, mkt{ticker: m.Ticker, closeT: m.CloseTime})
		}
		if next == "" || len(mkts) == 0 || walked >= *maxMkts {
			break
		}
		cursor = next
	}
	if len(picks) == 0 {
		fmt.Printf("no archived %s markets closed within the window (%d walked) — nothing to do\n", st, walked)
		return nil
	}
	fmt.Printf("series %s: %d archived market(s) in window (of %d walked)\n", st, len(picks), walked)

	// 3) Per-market trade pages → backfill_trades. Resume: done-latch skips completed tickers;
	// a persisted page cursor resumes a ticker interrupted mid-pull.
	var totalNew, totalRows int64
	minTs := since.Unix()
	for i, m := range picks {
		if ctx.Err() != nil {
			fmt.Println("interrupted — bookmarks saved, re-run to resume")
			return nil
		}
		if v, ok := store.KVGet(ctx, kvDone(m.ticker)); ok && v == "1" {
			continue
		}
		pcur, _ := store.KVGet(ctx, kvCur(m.ticker))
		pages := 0
		for {
			trs, next, err := client.HistoricalTradesPage(ctx, m.ticker, pcur, 1000, minTs, 0)
			if err != nil {
				// transient venue/network error: leave the cursor bookmark as-is and stop the run
				// cleanly; the next invocation resumes this exact page.
				return fmt.Errorf("GET /historical/trades (%s): %w (resume with the same command)", m.ticker, err)
			}
			rows := make([]storage.BackfillTrade, 0, len(trs))
			for _, t := range trs {
				rows = append(rows, storage.BackfillTrade{
					TradeID: t.TradeID, Series: st, Ticker: t.Ticker, CreatedTime: t.CreatedTime,
					Count: t.CountFP.Float(), YesPrice: t.YesPriceD.Float(), NoPrice: t.NoPriceD.Float(),
					TakerSide: t.TakerSide,
				})
			}
			n, err := store.InsertBackfillTrades(ctx, rows)
			if err != nil {
				return fmt.Errorf("insert trades (%s): %w", m.ticker, err)
			}
			totalNew += n
			totalRows += int64(len(rows))
			pages++
			if next == "" || len(trs) == 0 {
				break
			}
			pcur = next
			if err := store.KVSet(ctx, kvCur(m.ticker), pcur); err != nil {
				return fmt.Errorf("persist page cursor (%s): %w", m.ticker, err)
			}
		}
		if err := store.KVSet(ctx, kvDone(m.ticker), "1"); err != nil {
			return fmt.Errorf("persist done latch (%s): %w", m.ticker, err)
		}
		_ = store.KVDel(ctx, kvCur(m.ticker))
		if pages > 0 {
			fmt.Printf("[%d/%d] %s (closed %s): %d page(s)\n", i+1, len(picks), m.ticker, m.closeT, pages)
		}
	}
	stored, _ := store.BackfillTradeCount(ctx, st)
	fmt.Printf("done: %d fetched rows this run (%d new after dedup) · %d total stored for %s\n",
		totalRows, totalNew, stored, st)
	return nil
}
