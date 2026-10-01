// Command showcase runs a safe, self-contained tour of the prediction-market suite.
// It uses a small SQLite database filled with synthetic data and never loads credentials,
// connects to an exchange, or places an order.
package main

import (
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"math"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

//go:embed web/*
var webAssets embed.FS

type marketRow struct {
	ID        string  `json:"id"`
	Question  string  `json:"question"`
	Venue     string  `json:"venue"`
	Yes       float64 `json:"yes"`
	Reference float64 `json:"reference"`
	Volume    float64 `json:"volume"`
}

type signalRow struct {
	ID       int64   `json:"id"`
	Created  string  `json:"created"`
	Family   string  `json:"family"`
	Market   string  `json:"market"`
	Side     string  `json:"side"`
	Strength float64 `json:"strength"`
	Summary  string  `json:"summary"`
}

type tradeRow struct {
	ID      int64   `json:"id"`
	Created string  `json:"created"`
	Market  string  `json:"market"`
	Side    string  `json:"side"`
	Entry   float64 `json:"entry"`
	Current float64 `json:"current"`
	Qty     int     `json:"qty"`
	Status  string  `json:"status"`
	PnL     float64 `json:"pnl"`
}

type researchRow struct {
	Name    string  `json:"name"`
	Sample  int     `json:"sample"`
	Edge    float64 `json:"edge"`
	Holdout string  `json:"holdout"`
	Status  string  `json:"status"`
}

type eventRow struct {
	Created string `json:"created"`
	Stage   string `json:"stage"`
	Message string `json:"message"`
}

type demoState struct {
	Mode       string        `json:"mode"`
	Paused     bool          `json:"paused"`
	Tick       int           `json:"tick"`
	Uptime     string        `json:"uptime"`
	Database   string        `json:"database"`
	Markets    []marketRow   `json:"markets"`
	Signals    []signalRow   `json:"signals"`
	Trades     []tradeRow    `json:"trades"`
	Research   []researchRow `json:"research"`
	Events     []eventRow    `json:"events"`
	PaperPnL   float64       `json:"paper_pnl"`
	OpenTrades int           `json:"open_trades"`
}

type demoServer struct {
	db      *sql.DB
	dbPath  string
	started time.Time
	mu      sync.Mutex
	paused  bool
	tick    int
}

func main() {
	addr := flag.String("addr", "127.0.0.1:8787", "local address for the demo dashboard")
	dbPath := flag.String("db", filepath.Join("demo-data", "showcase.db"), "path to the generated demo database")
	open := flag.Bool("open", true, "open the dashboard in the default browser")
	reset := flag.Bool("reset", false, "replace the demo database with a fresh sample")
	flag.Parse()

	if *reset {
		for _, suffix := range []string{"", "-wal", "-shm"} {
			if err := os.Remove(*dbPath + suffix); err != nil && !errors.Is(err, os.ErrNotExist) {
				log.Fatalf("reset demo database: %v", err)
			}
		}
	}
	if err := os.MkdirAll(filepath.Dir(*dbPath), 0o755); err != nil {
		log.Fatal(err)
	}

	db, err := sql.Open("sqlite", *dbPath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	srv := &demoServer{db: db, dbPath: *dbPath, started: time.Now()}
	if err := srv.prepare(context.Background()); err != nil {
		log.Fatalf("prepare demo: %v", err)
	}

	assets, err := fs.Sub(webAssets, "web")
	if err != nil {
		log.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/demo/state", srv.handleState)
	mux.HandleFunc("POST /api/demo/toggle", srv.handleToggle)
	mux.HandleFunc("POST /api/demo/step", srv.handleStep)
	mux.Handle("/", http.FileServer(http.FS(assets)))

	go srv.runTicker(context.Background())
	url := "http://" + *addr
	if *open {
		go func() {
			time.Sleep(500 * time.Millisecond)
			if err := openBrowser(url); err != nil {
				log.Printf("open %s in a browser", url)
			}
		}()
	}

	log.Printf("Prediction Market Suite demo: %s", url)
	log.Printf("Synthetic SQLite data: %s", *dbPath)
	log.Print("Press Ctrl+C to stop.")
	server := &http.Server{Addr: *addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

func (s *demoServer) prepare(ctx context.Context) error {
	statements := []string{
		`PRAGMA journal_mode=WAL`,
		`CREATE TABLE IF NOT EXISTS markets (
			id TEXT PRIMARY KEY, question TEXT NOT NULL, venue TEXT NOT NULL,
			yes_price REAL NOT NULL, reference_price REAL NOT NULL,
			volume REAL NOT NULL, updated_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS signals (
			id INTEGER PRIMARY KEY AUTOINCREMENT, created_at TEXT NOT NULL,
			family TEXT NOT NULL, market_id TEXT NOT NULL, side TEXT NOT NULL,
			strength REAL NOT NULL, summary TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS paper_trades (
			id INTEGER PRIMARY KEY AUTOINCREMENT, created_at TEXT NOT NULL,
			market_id TEXT NOT NULL, side TEXT NOT NULL, entry_price REAL NOT NULL,
			current_price REAL NOT NULL, quantity INTEGER NOT NULL,
			status TEXT NOT NULL, pnl REAL NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS research (
			name TEXT PRIMARY KEY, sample_size INTEGER NOT NULL, avg_edge REAL NOT NULL,
			holdout TEXT NOT NULL, status TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS system_events (
			id INTEGER PRIMARY KEY AUTOINCREMENT, created_at TEXT NOT NULL,
			stage TEXT NOT NULL, message TEXT NOT NULL
		)`,
	}
	for _, statement := range statements {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return err
		}
	}

	var count int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM markets`).Scan(&count); err != nil {
		return err
	}
	if count == 0 {
		return s.seed(ctx)
	}
	return nil
}

func (s *demoServer) seed(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	now := time.Now().UTC().Format(time.RFC3339)
	markets := []marketRow{
		{"fed-cut", "Will the Fed cut rates at its next meeting?", "Kalshi", .41, .47, 184200},
		{"btc-100k", "Will Bitcoin finish the month above $100,000?", "Kalshi", .56, .59, 99200},
		{"turnout", "Will national turnout exceed 62%?", "Polymarket US", .63, .60, 72100},
		{"denver-rain", "Will Denver record rain this weekend?", "Kalshi", .35, .38, 21800},
		{"playoffs", "Will Los Angeles reach the conference finals?", "Polymarket US", .71, .69, 131400},
		{"ai-benchmark", "Will the benchmark record be broken this quarter?", "Kalshi", .48, .52, 44700},
	}
	for _, row := range markets {
		if _, err := tx.ExecContext(ctx, `INSERT INTO markets(id,question,venue,yes_price,reference_price,volume,updated_at) VALUES(?,?,?,?,?,?,?)`,
			row.ID, row.Question, row.Venue, row.Yes, row.Reference, row.Volume, now); err != nil {
			return err
		}
	}
	research := []researchRow{
		{"Cross-market value", 248, 2.3, "Positive in both halves", "watch"},
		{"Spot lag", 192, 1.6, "Newer half still positive", "watch"},
		{"Order flow", 411, .8, "Mixed by venue", "collect"},
		{"Maker queue", 87, 1.2, "More fills needed", "collect"},
		{"Forecast scoring", 620, -.4, "Calibration improved", "score"},
	}
	for _, row := range research {
		if _, err := tx.ExecContext(ctx, `INSERT INTO research(name,sample_size,avg_edge,holdout,status) VALUES(?,?,?,?,?)`,
			row.Name, row.Sample, row.Edge, row.Holdout, row.Status); err != nil {
			return err
		}
	}
	seedSignals := []struct {
		family, market, side string
		strength             float64
		summary              string
	}{
		{"cross-market", "fed-cut", "YES", .78, "Reference markets imply 47¢ while the available ask is 41¢."},
		{"spot-lag", "btc-100k", "YES", .67, "The outside reference moved first and this contract has not caught up."},
		{"order-flow", "turnout", "NO", .61, "Recent selling and visible book size point in the same direction."},
	}
	for _, row := range seedSignals {
		if _, err := tx.ExecContext(ctx, `INSERT INTO signals(created_at,family,market_id,side,strength,summary) VALUES(?,?,?,?,?,?)`,
			now, row.family, row.market, row.side, row.strength, row.summary); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO paper_trades(created_at,market_id,side,entry_price,current_price,quantity,status,pnl) VALUES(?,?,?,?,?,?,?,?)`,
		now, "fed-cut", "YES", .41, .43, 10, "open", .20); err != nil {
		return err
	}
	for _, event := range []eventRow{
		{now, "feed", "Loaded six synthetic markets from the demo database."},
		{now, "signal", "Cross-market value found a 6¢ gap after its safety cushion."},
		{now, "risk", "Order passed cash, market, and portfolio limits."},
		{now, "paper", "Recorded a 10-contract paper fill at 41¢."},
	} {
		if _, err := tx.ExecContext(ctx, `INSERT INTO system_events(created_at,stage,message) VALUES(?,?,?)`, event.Created, event.Stage, event.Message); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *demoServer) runTicker(ctx context.Context) {
	ticker := time.NewTicker(1400 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.mu.Lock()
			paused := s.paused
			s.mu.Unlock()
			if !paused {
				_ = s.step(context.Background())
			}
		}
	}
}

func (s *demoServer) step(ctx context.Context) error {
	s.mu.Lock()
	s.tick++
	tick := s.tick
	s.mu.Unlock()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx, `SELECT id,yes_price FROM markets ORDER BY id`)
	if err != nil {
		return err
	}
	type quote struct {
		id  string
		yes float64
	}
	var quotes []quote
	for rows.Next() {
		var q quote
		if err := rows.Scan(&q.id, &q.yes); err != nil {
			rows.Close()
			return err
		}
		quotes = append(quotes, q)
	}
	rows.Close()
	now := time.Now().UTC().Format(time.RFC3339)
	deltas := []float64{.003, -.002, .001, .004, -.003, .002}
	for i, q := range quotes {
		delta := deltas[(tick+i)%len(deltas)]
		price := math.Max(.03, math.Min(.97, q.yes+delta))
		if _, err := tx.ExecContext(ctx, `UPDATE markets SET yes_price=?,volume=volume+?,updated_at=? WHERE id=?`, price, 120+17*i, now, q.id); err != nil {
			return err
		}
	}

	if tick%3 == 0 && len(quotes) > 0 {
		families := []string{"cross-market", "spot-lag", "order-flow", "book-skew", "weather", "crypto link"}
		q := quotes[(tick/3)%len(quotes)]
		family := families[(tick/3)%len(families)]
		strength := .58 + float64(tick%7)*.045
		side := "YES"
		if tick%2 == 0 {
			side = "NO"
		}
		summary := fmt.Sprintf("%s evidence reached %.0f%% strength; queued for fee and risk checks.", family, strength*100)
		if _, err := tx.ExecContext(ctx, `INSERT INTO signals(created_at,family,market_id,side,strength,summary) VALUES(?,?,?,?,?,?)`,
			now, family, q.id, side, strength, summary); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO system_events(created_at,stage,message) VALUES(?,?,?)`,
			now, "signal", fmt.Sprintf("%s produced a %s signal for %s.", family, side, q.id)); err != nil {
			return err
		}
	}

	if tick%6 == 0 && len(quotes) > 0 {
		q := quotes[(tick/6+1)%len(quotes)]
		qty := 5 + tick%8
		if _, err := tx.ExecContext(ctx, `INSERT INTO paper_trades(created_at,market_id,side,entry_price,current_price,quantity,status,pnl) VALUES(?,?,?,?,?,?,?,?)`,
			now, q.id, "YES", q.yes, q.yes, qty, "open", 0); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO system_events(created_at,stage,message) VALUES(?,?,?)`,
			now, "paper", fmt.Sprintf("Paper account filled %d YES contracts on %s after risk checks.", qty, q.id)); err != nil {
			return err
		}
	}

	if _, err := tx.ExecContext(ctx, `UPDATE paper_trades
		SET current_price=(SELECT yes_price FROM markets WHERE id=paper_trades.market_id),
		pnl=((SELECT yes_price FROM markets WHERE id=paper_trades.market_id)-entry_price)*quantity
		WHERE status='open'`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM signals WHERE id NOT IN (SELECT id FROM signals ORDER BY id DESC LIMIT 14)`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM paper_trades WHERE id NOT IN (SELECT id FROM paper_trades ORDER BY id DESC LIMIT 10)`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM system_events WHERE id NOT IN (SELECT id FROM system_events ORDER BY id DESC LIMIT 18)`); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *demoServer) snapshot(ctx context.Context) (demoState, error) {
	s.mu.Lock()
	paused, tick := s.paused, s.tick
	s.mu.Unlock()
	state := demoState{Mode: "synthetic demo", Paused: paused, Tick: tick, Uptime: time.Since(s.started).Round(time.Second).String(), Database: s.dbPath}

	rows, err := s.db.QueryContext(ctx, `SELECT id,question,venue,yes_price,reference_price,volume FROM markets ORDER BY volume DESC`)
	if err != nil {
		return state, err
	}
	for rows.Next() {
		var row marketRow
		if err := rows.Scan(&row.ID, &row.Question, &row.Venue, &row.Yes, &row.Reference, &row.Volume); err != nil {
			rows.Close()
			return state, err
		}
		state.Markets = append(state.Markets, row)
	}
	rows.Close()

	rows, err = s.db.QueryContext(ctx, `SELECT s.id,s.created_at,s.family,m.question,s.side,s.strength,s.summary
		FROM signals s JOIN markets m ON m.id=s.market_id ORDER BY s.id DESC LIMIT 8`)
	if err != nil {
		return state, err
	}
	for rows.Next() {
		var row signalRow
		if err := rows.Scan(&row.ID, &row.Created, &row.Family, &row.Market, &row.Side, &row.Strength, &row.Summary); err != nil {
			rows.Close()
			return state, err
		}
		state.Signals = append(state.Signals, row)
	}
	rows.Close()

	rows, err = s.db.QueryContext(ctx, `SELECT p.id,p.created_at,m.question,p.side,p.entry_price,p.current_price,p.quantity,p.status,p.pnl
		FROM paper_trades p JOIN markets m ON m.id=p.market_id ORDER BY p.id DESC LIMIT 7`)
	if err != nil {
		return state, err
	}
	for rows.Next() {
		var row tradeRow
		if err := rows.Scan(&row.ID, &row.Created, &row.Market, &row.Side, &row.Entry, &row.Current, &row.Qty, &row.Status, &row.PnL); err != nil {
			rows.Close()
			return state, err
		}
		state.Trades = append(state.Trades, row)
		state.PaperPnL += row.PnL
		if row.Status == "open" {
			state.OpenTrades++
		}
	}
	rows.Close()

	rows, err = s.db.QueryContext(ctx, `SELECT name,sample_size,avg_edge,holdout,status FROM research ORDER BY sample_size DESC`)
	if err != nil {
		return state, err
	}
	for rows.Next() {
		var row researchRow
		if err := rows.Scan(&row.Name, &row.Sample, &row.Edge, &row.Holdout, &row.Status); err != nil {
			rows.Close()
			return state, err
		}
		state.Research = append(state.Research, row)
	}
	rows.Close()

	rows, err = s.db.QueryContext(ctx, `SELECT created_at,stage,message FROM system_events ORDER BY id DESC LIMIT 10`)
	if err != nil {
		return state, err
	}
	for rows.Next() {
		var row eventRow
		if err := rows.Scan(&row.Created, &row.Stage, &row.Message); err != nil {
			rows.Close()
			return state, err
		}
		state.Events = append(state.Events, row)
	}
	rows.Close()
	return state, nil
}

func (s *demoServer) handleState(w http.ResponseWriter, r *http.Request) {
	state, err := s.snapshot(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(state)
}

func (s *demoServer) handleToggle(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	s.paused = !s.paused
	paused := s.paused
	s.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]bool{"paused": paused})
}

func (s *demoServer) handleStep(w http.ResponseWriter, r *http.Request) {
	if err := s.step(r.Context()); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func openBrowser(url string) error {
	var command *exec.Cmd
	switch runtime.GOOS {
	case "windows":
		command = exec.Command("rundll32", "url.dll,FileProtocolHandler", url)
	case "darwin":
		command = exec.Command("open", url)
	default:
		command = exec.Command("xdg-open", url)
	}
	return command.Start()
}
