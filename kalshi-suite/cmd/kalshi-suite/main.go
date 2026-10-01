// Command kalshi-suite is the single local binary for the Kalshi trading suite.
//
// Subcommands:
//
//	serve             (default) run the dashboard/API server until interrupted
//	ping              test connectivity (public), plus credentials if available
//	markets           list the most liquid markets closing soon (public)
//	set-credentials   encrypt and store the Kalshi Key ID + RSA private key
//	demo              verify the Kalshi DEMO key (env vars; no passphrase) + print balance
//	demo-order        DEMO read+write self-test: place a tiny post-only resting order + cancel it
//
// The passphrase that protects stored credentials is read from the environment
// variable KALSHI_SUITE_PASSPHRASE. It is never written to disk. The DEMO key, which
// guards only mock funds, needs no passphrase or encrypted store — it is read directly
// from KALSHI_DEMO_KEY_ID + KALSHI_DEMO_KEY_FILE.
//
// When config kalshi_key_file is set and exists, it becomes the Kalshi credential source (PEM,
// optional leading key_id: line; otherwise config kalshi_key_id) and wins over the encrypted
// store. polyus_key_file likewise holds the PolyUS base64 secret and wins over the
// POLY_US_SECRET(_FILE) env vars (config polyus_key_id — R88 — or the POLY_US_KEY_ID env
// fallback names the key). Key material is never
// logged — only the source and a sha256 fingerprint prefix of the file bytes.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"
	_ "time/tzdata" // embed the tz database so time.LoadLocation("America/New_York") works in the binary (Poly hourly/daily up/down slugs are ET-labeled)

	"github.com/kalshi-suite/kalshi-suite/internal/briefing"
	"github.com/kalshi-suite/kalshi-suite/internal/config"
	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/killswitch"
	"github.com/kalshi-suite/kalshi-suite/internal/logging"
	"github.com/kalshi-suite/kalshi-suite/internal/pmx"
	"github.com/kalshi-suite/kalshi-suite/internal/polymarketus"
	"github.com/kalshi-suite/kalshi-suite/internal/researchdr"
	"github.com/kalshi-suite/kalshi-suite/internal/secrets"
	"github.com/kalshi-suite/kalshi-suite/internal/server"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

// buildVersion and buildReleaseName are stamped at link time by build-suite.bat. The full
// version remains a precise sha/state/time diagnostic; the release name is the stable animal
// prefix carried by a clean commit (dirty builds deliberately refuse to inherit it).
var buildVersion = "dev"
var buildReleaseName string

// applyLaunchFlags keeps process-presentation flags separate from persisted trading state.
// In particular, -headless may suppress the browser but must never silently arm PAPER AUTO.
func applyLaunchFlags(cfg *config.Config, headless bool) {
	if cfg != nil && headless {
		cfg.OpenBrowser = false
	}
}

// childAbsolutePath resolves paths before the child changes its working directory to ml/.
// Config.Load intentionally interprets relative paths from the suite's launch directory, so the
// sidecar must receive that same resolved file rather than reinterpreting it from ml/.
func childAbsolutePath(path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return ""
	}
	if abs, err := filepath.Abs(path); err == nil {
		return abs
	}
	return filepath.Clean(path)
}

func mlSidecarArgs(cfg config.Config, cfgPath string) []string {
	args := []string{
		"live_ml.py", "--interval", "4",
		"--db", childAbsolutePath(filepath.Join(cfg.DataDir, "kalshi.db")),
	}
	if p := childAbsolutePath(cfgPath); p != "" {
		args = append(args, "--config", p)
	}
	return args
}

// runtimeBootRecord is the termination breadcrumb for unattended launches. A hard process-tree
// kill cannot close the record, so the next boot can distinguish an orderly shutdown from an
// incomplete exit without pretending it knows who/what ended the process.
type runtimeBootRecord struct {
	BootID    string `json:"boot_id"`
	StartedAt string `json:"started_at"`
	EndedAt   string `json:"ended_at,omitempty"`
	Exit      string `json:"exit,omitempty"` // shutdown_signal | server_returned | server_error | startup_error
	Build     string `json:"build"`
	PID       int    `json:"pid"`
	ParentPID int    `json:"parent_pid"`
	Headless  bool   `json:"headless"`
}

type runtimeBootHistoryEvent struct {
	ObservedAt string            `json:"observed_at"`
	State      string            `json:"state"` // completed | incomplete_exit_detected
	Receipt    runtimeBootRecord `json:"receipt"`
}

func writeRuntimeBoot(path string, rec runtimeBootRecord) error {
	b, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err = os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func appendRuntimeBootHistory(dir, state string, rec runtimeBootRecord) error {
	e := runtimeBootHistoryEvent{
		ObservedAt: time.Now().UTC().Format(time.RFC3339Nano), State: state, Receipt: rec,
	}
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(dir, "runtime_boot_history.jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Write(append(b, '\n'))
	return err
}

func beginRuntimeBoot(dir string, headless bool, log *slog.Logger) (string, runtimeBootRecord) {
	path := filepath.Join(dir, "runtime_boot.json")
	if prev, ok := readRuntimeBoot(path); ok && prev.BootID != "" && prev.EndedAt == "" {
		log.Warn("previous suite process ended without a completed exit receipt; cause is unknown (forced close, process kill, crash, or power/session loss are all possible)",
			"boot_id", prev.BootID, "pid", prev.PID, "parent_pid", prev.ParentPID,
			"started_at", prev.StartedAt, "headless", prev.Headless, "build", prev.Build)
		if err := appendRuntimeBootHistory(dir, "incomplete_exit_detected", prev); err != nil {
			log.Warn("could not persist incomplete runtime receipt to history", "err", err)
		}
	}
	now := time.Now().UTC()
	rec := runtimeBootRecord{BootID: fmt.Sprintf("%d-%d", now.UnixMilli(), os.Getpid()),
		StartedAt: now.Format(time.RFC3339Nano), Build: buildVersion, PID: os.Getpid(),
		ParentPID: os.Getppid(), Headless: headless}
	if err := writeRuntimeBoot(path, rec); err != nil {
		log.Warn("could not persist runtime boot record", "err", err)
	}
	log.Info("suite process identity", "boot_id", rec.BootID, "pid", rec.PID,
		"parent_pid", rec.ParentPID, "headless", rec.Headless, "build", rec.Build)
	return path, rec
}

func readRuntimeBoot(path string) (runtimeBootRecord, bool) {
	var rec runtimeBootRecord
	b, err := os.ReadFile(path)
	if err != nil || json.Unmarshal(b, &rec) != nil {
		return runtimeBootRecord{}, false
	}
	return rec, true
}

func endRuntimeBoot(path string, rec runtimeBootRecord, reason string, log *slog.Logger) {
	rec.EndedAt = time.Now().UTC().Format(time.RFC3339Nano)
	rec.Exit = reason
	if err := writeRuntimeBoot(path, rec); err != nil {
		log.Warn("could not close runtime boot record", "err", err)
	}
	if err := appendRuntimeBootHistory(filepath.Dir(path), "completed", rec); err != nil {
		log.Warn("could not append runtime exit history", "err", err)
	}
}

// runPMXProbe verifies the Polymarket US EXCHANGE gRPC stack end-to-end (R22): Auth0 token →
// health → authenticated instrument list → 10s live market-data stream, printing the venue's
// symbology. Requires the onboarding-issued PMX_* env vars (see internal/pmx docs); until those
// exist this prints exactly what's missing and exits.
func runPMXProbe() {
	cfg, ok := pmx.FromEnv()
	if !ok {
		fmt.Fprintln(os.Stderr, "PMX gRPC is not configured. Set PMX_AUTH0_DOMAIN, PMX_CLIENT_ID, PMX_AUDIENCE, PMX_KEY_FILE (+ optional PMX_PARTICIPANT_ID, PMX_GRPC_ADDR).")
		fmt.Fprintln(os.Stderr, "These are ONBOARDING-issued credentials (Auth0 client-credentials), not the retail Ed25519 key — request them via support@polymarket.us.")
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	c, err := pmx.Dial(ctx, cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "dial:", err)
		os.Exit(1)
	}
	defer c.Close()
	if err := c.Probe(ctx, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "probe:", err)
		os.Exit(1)
	}
}

func main() {
	disableQuickEdit() // Windows: stop the cmd window from PAUSING the server when it's clicked/selected
	cfgPath := flag.String("config", "config.json", "path to config file (JSON)")
	headless := flag.Bool("headless", false, "server/24-7 mode: do NOT open a browser (open the dashboard yourself from another machine at http://<server-ip>:<port>); preserves the persisted PAPER AUTO setting")
	flag.Parse()

	cmd := "serve"
	if flag.NArg() > 0 {
		cmd = flag.Arg(0)
	}

	cfg, defaulted, unknown, err := config.LoadWithReport(*cfgPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "config error:", err)
		if cmd == "serve" { // R86: a double-clicked serve boot must stay readable (was: window vanished / R79 batch pause)
			fatalBootHold()
		}
		os.Exit(1)
	}
	log := logging.New(cfg.LogLevel)
	// R76 (auditor bug 19): boot transparency — every knob the FILE didn't set runs on Default()'s
	// value; name them once at boot so effective-vs-file drift is visible, not archaeology.
	if len(defaulted) > 0 {
		log.Info("config: keys not in the file (running on defaults)", "n", len(defaulted), "keys", strings.Join(defaulted, " "))
	}
	// R79 (replaces the R76 unknown-key fail-stop): an unknown key is a LOUD WARN + audit +
	// CONTINUE — the typo is visible here AND as its real key in the defaults list above, while a
	// LIVE-money suite keeps running instead of fail-stopping invisibly. Invalid JSON still fails.
	if len(unknown) > 0 {
		log.Warn("config: UNKNOWN keys in the file — IGNORED (typo? retired knob?); each named key has NO effect and its real setting runs on the default", "n", len(unknown), "keys", strings.Join(unknown, " "))
	}

	// R135 (auditor r63): Auto.Enabled and Auto.Mode are persisted operator state. Boot used to
	// overwrite both, so clicking PAPER AUTO off never survived a restart. Headless now changes
	// presentation only; it must not grant trading authority or replace the chosen strategy mode.
	applyLaunchFlags(&cfg, *headless)
	if *headless {
		log.Info("headless mode: no browser will be opened; PAPER AUTO follows its persisted setting", "auto_enabled", cfg.Auto.Enabled, "auto_mode", cfg.Auto.Mode, "dashboard", "http://"+cfg.ServerAddr)
	}

	if err := os.MkdirAll(cfg.DataDir, 0o700); err != nil {
		log.Error("create data dir", "err", err)
		if cmd == "serve" { // R86: fatal boot errors hold the console readable (see fatalBootHold)
			fatalBootHold()
		}
		os.Exit(1)
	}
	if cmd == "research-snapshot" {
		if conn, dialErr := net.DialTimeout("tcp", cfg.ServerAddr, 500*time.Millisecond); dialErr == nil {
			_ = conn.Close()
			fmt.Fprintln(os.Stderr, "research snapshot refused: suite is listening on", cfg.ServerAddr, "(stop it first)")
			os.Exit(1)
		}
	}
	// DRIVESYNC: restore must either install one fully verified main+shadow generation or fail
	// before storage.Open can create a blank main DB and permanently mask the failed restore.
	if err := maybeRestoreFromSnapshot(cfg, log); err != nil {
		log.Error("automatic snapshot restore failed; storage was not opened", "err", err)
		if cmd == "serve" {
			fatalBootHold()
		}
		os.Exit(1)
	}
	var bootPath string
	var bootRec runtimeBootRecord
	if cmd == "serve" {
		bootPath, bootRec = beginRuntimeBoot(cfg.DataDir, *headless, log)
	}
	store, err := storage.Open(cfg.DataDir)
	if err != nil {
		log.Error("open storage", "err", err)
		if cmd == "serve" {
			endRuntimeBoot(bootPath, bootRec, "startup_error", log)
		}
		if cmd == "serve" { // R86: fatal boot errors hold the console readable (see fatalBootHold)
			fatalBootHold()
		}
		os.Exit(1)
	}
	defer store.Close()
	for _, w := range store.IndexWarnings { // R101 (auditor r28 bug 236): boot DDL failures are now VISIBLE (log + audit trail)
		log.Warn("storage boot DDL failed", "warn", w)
		_ = store.Audit(context.Background(), "warn", "storage", "boot DDL failed (bug 236 — silent full-scan risk)", w)
	}
	if len(unknown) > 0 { // R79: the unknown-key warning also lands in the persisted audit trail
		_ = store.Audit(context.Background(), "warn", "config", fmt.Sprintf("config.json has %d UNKNOWN key(s) — ignored, boot continued (R79; was a fail-stop)", len(unknown)), strings.Join(unknown, " "))
	}

	switch cmd {
	case "serve":
		exitReason, exitCode := runServe(cfg, *cfgPath, log, store)
		endRuntimeBoot(bootPath, bootRec, exitReason, log)
		if exitCode != 0 {
			os.Exit(exitCode)
		}
	case "ping":
		runPing(cfg, log, store)
	case "markets":
		runMarkets(cfg, log, store)
	case "briefing":
		runBriefingOnce(cfg, log, store)
	case "backup":
		runBackupOnce(cfg, log, store)
	case "research-snapshot":
		runResearchSnapshot(cfg, log, store)
	case "archive-ptt":
		runArchivePTT(cfg, log, store)
	case "set-credentials":
		runSetCredentials(cfg, log, store, flag.Args()[1:])
	case "demo":
		runDemoCheck(cfg, log)
	case "demo-order":
		runDemoOrder(cfg, log, flag.Args()[1:])
	case "ws":
		runWS(cfg, log)
	case "polyus":
		runPolyUS(cfg, log, flag.Args()[1:])
	case "pmx-probe":
		runPMXProbe()
	case "live-order":
		runLiveOrder(cfg, log, flag.Args()[1:])
	case "polyus-markets":
		runPolyUSMarkets(cfg, log, flag.Args()[1:])
	case "polyus-games":
		runPolyUSGames(cfg, log, flag.Args()[1:])
	case "polyus-book":
		runPolyUSBook(cfg, log, flag.Args()[1:])
	case "polyus-ws":
		runPolyUSWS(cfg, log, flag.Args()[1:])
	case "tier":
		runTier(cfg, log, flag.Args()[1:])
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q (want: serve | ping | markets | briefing | backup | research-snapshot | set-credentials | demo | demo-order | ws | polyus | polyus-markets | polyus-games | polyus-book | polyus-ws | tier | live-order)\n", cmd)
		os.Exit(2)
	}
}

// errCredsLocked — R86: the exact boot state after the shell that exported
// KALSHI_SUITE_PASSPHRASE dies (PC crash / closed terminal): credentials ARE stored but cannot
// be unsealed, so the suite boots public-only (WS 0 tickers, balance error). Surfaced as the RED
// "kalshi_auth" component in /api/ready + the bar1 🔑 chip so that state is loud, not archaeology.
var errCredsLocked = errors.New("credentials are stored but KALSHI_SUITE_PASSPHRASE is not set")

// errKeyFileNoKeyID — R87: the Kalshi key FILE exists but is PEM-only (no key_id:/keyid= first
// line) and config kalshi_key_id is empty. The suite never guesses a key id — this is a RED
// kalshi_auth state whose detail IS the fix.
var errKeyFileNoKeyID = errors.New("key file found but kalshi_key_id not set (Settings)")

// loadSigner builds a Kalshi signer. When cfg.KalshiKeyFile exists, parse it (an embedded
// key_id:/keyid= first
// line wins, else config kalshi_key_id; PEM-only with neither = errKeyFileNoKeyID) — on success
// the file WINS, and on failure it is a loud red state, never a silent fallback; (b) no key file
// → the encrypted store + KALSHI_SUITE_PASSPHRASE exactly as before; (c) neither → public-only
// (nil, "", nil). The returned source ("file"/"store") feeds the kalshi_auth readiness detail.
// SECURITY: key MATERIAL is never logged — callers log the source + keyFileFingerprint only.
func loadSigner(ctx context.Context, cfg config.Config, store *storage.Store) (*kalshi.Signer, string, error) {
	if path := strings.TrimSpace(cfg.KalshiKeyFile); path != "" {
		if fi, statErr := os.Stat(path); statErr == nil && !fi.IsDir() {
			s, err := kalshiSignerFromFile(path, cfg.KalshiKeyID)
			return s, "file", err
		}
	}
	cred, err := store.LoadCredential(ctx, string(cfg.Environment))
	if err != nil {
		return nil, "store", err
	}
	if cred == nil {
		return nil, "", nil
	}
	pass := os.Getenv("KALSHI_SUITE_PASSPHRASE")
	if pass == "" {
		return nil, "store", errCredsLocked
	}
	pemBytes, err := secrets.Open(pass, secrets.Sealed{
		Salt: cred.Salt, Nonce: cred.Nonce, Ciphertext: cred.Ciphertext,
	})
	if err != nil {
		return nil, "store", err
	}
	sg, err := kalshi.NewSigner(cred.KeyID, pemBytes)
	return sg, "store", err
}

// kalshiSignerFromFile builds the signer from the operator's plain key file (R87): the PEM RSA
// private key, optionally preceded by a key_id:/keyid= line (kalshi.ParseKeyFile tolerates
// BOM/CRLF/whitespace). A PEM-only file uses cfgKeyID (config kalshi_key_id). The file contents
// are NEVER logged or echoed — errors carry the path only.
func kalshiSignerFromFile(path, cfgKeyID string) (*kalshi.Signer, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read kalshi key file %q: %w", path, err)
	}
	keyID, pemBytes, err := kalshi.ParseKeyFile(b)
	if err != nil {
		return nil, fmt.Errorf("kalshi key file %q: %w", path, err)
	}
	if keyID == "" {
		keyID = strings.TrimSpace(cfgKeyID)
	}
	if keyID == "" {
		return nil, errKeyFileNoKeyID
	}
	return kalshi.NewSigner(keyID, pemBytes)
}

// keyFileFingerprint returns the first 8 hex chars of sha256(file bytes) — a log-safe identity
// for a key file (did the file change?) that reveals nothing about the key. This is the ONLY
// thing the suite may ever log about a key file's contents.
func keyFileFingerprint(path string) string {
	b, err := os.ReadFile(path)
	if err != nil {
		return "unreadable"
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:4])
}

// fatalBootHold — R86 (operator: "ctrl c isnt working"): crash visibility moved INTO the exe.
// The R79 batch wrapper (start cmd /c "kalshi-suite.exe serve & if errorlevel 1 ... pause") kept
// crash text on screen, but it put cmd's batch layer back in charge of the console — Ctrl+C hit
// the wrapper ("Terminate batch job (Y/N)?" / swallowed interrupt) instead of the suite. So
// build-suite.bat launches the exe DIRECTLY again, and on a fatal boot error (config parse, data
// dir, storage open, port bind) THIS prints a marker and holds the console open for 60s so a
// double-clicked / `start`ed window can be read before it vanishes. Windows-only: a headless
// Linux/systemd boot must fail fast so the supervisor's restart/backoff logic owns the timing.
// Ctrl+C during the hold still exits immediately (default signal disposition at these points).
func fatalBootHold() {
	if runtime.GOOS != "windows" {
		return
	}
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "[X] kalshi-suite failed to start - exiting in 60s - read the error above.")
	time.Sleep(60 * time.Second)
}

func newClient(cfg config.Config, signer *kalshi.Signer) *kalshi.Client {
	return kalshi.NewClient(
		kalshi.BaseURLFor(string(cfg.Environment)),
		signer,
		cfg.Kalshi.RateLimitPerSec,
		time.Duration(cfg.Kalshi.RequestTimeoutMs)*time.Millisecond,
	)
}

func awaitServeExitAndCancel(ctx context.Context, errCh <-chan error,
	stop func()) (exitReason string, srvFatal bool, serverErr error) {
	defer func() {
		if stop != nil {
			stop()
		}
	}()
	select {
	case <-ctx.Done():
		return "shutdown_signal", false, nil
	case err := <-errCh:
		if err != nil {
			return "server_error", true, err
		}
		return "server_returned", false, nil
	}
}

func runServe(cfg config.Config, cfgPath string, log *slog.Logger, store *storage.Store) (exitReason string, exitCode int) {
	// Serve runs for hours/days, so tee the operational log to a file too — it's captured even when
	// the console is closed/minimized/unfocused. (data dir already exists by now.)
	log = logging.NewWithFile(cfg.LogLevel, filepath.Join(cfg.DataDir, "kalshi-suite.log"))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// The kill switch is created before the server, so the trip callback late-binds through srvKS.
	// On trip it (1) logs/audits, then (2) runs the REAL halt — cancel all resting real-money orders
	// and disarm live writes — async so Trip() never blocks on network I/O. The trading loops
	// themselves check ks.Tripped() (ksBlocked) at every risk-opening entry point.
	var srvKS *server.Server
	ks := killswitch.New(func(reason string) {
		log.Warn("KILL SWITCH TRIPPED", "reason", reason)
		_ = store.Audit(context.Background(), "critical", "killswitch", "tripped", reason)
		if srvKS != nil {
			go srvKS.OnKillSwitchTrip(reason)
		}
	})

	signer, kalSrc, err := loadSigner(ctx, cfg, store)
	// R86 AUTH VISIBILITY: classify the signer outcome for /api/ready ("kalshi_auth") + the bar1
	// 🔑 chip. The operator's PC crash killed the terminal session holding KALSHI_SUITE_PASSPHRASE
	// and the next boot was public-only with ONLY the quiet warn below + a red balance line — he
	// had to diagnose it from symptoms. The boot log warn STAYS; the locked state is now also a
	// persistent RED readiness component + header chip until it's fixed + restart.
	// R87: the detail now names the SOURCE ("file"/"store"), the locked fix text offers BOTH
	// remedies (key file OR passphrase), and a key file without a usable key id is its own red
	// state ("key file found but kalshi_key_id not set (Settings)").
	authState, authDetail := "none", "no credentials stored"
	switch {
	case err != nil:
		switch {
		case errors.Is(err, errKeyFileNoKeyID):
			authDetail = "key file found but kalshi_key_id not set (Settings)"
		case errors.Is(err, errCredsLocked):
			authDetail = "credentials stored but locked - set KALSHI_SUITE_PASSPHRASE in the launching shell (or User env) and restart, OR point kalshi_key_file at your PEM key + set kalshi_key_id (Settings)"
		case kalSrc == "file":
			authDetail = "kalshi key file present but unusable: " + err.Error() // path/parse errors only — never key material
		default:
			authDetail = "credentials stored but failed to unlock: " + err.Error() // wrong passphrase / unreadable store — same fix path
		}
		authState = "locked"
		log.Warn("credentials not loaded; serving public endpoints only", "source", kalSrc, "err", err)
	case signer == nil:
		log.Info("no credentials stored yet; serving public endpoints only (set kalshi_key_file + kalshi_key_id in Settings, or run set-credentials)")
	default:
		authState, authDetail = "ok", "signer loaded (key "+signer.KeyID+", env "+string(cfg.Environment)+", source "+kalSrc+")"
		if kalSrc == "file" { // R87: log the source + a sha256 prefix of the FILE BYTES — never the key itself
			log.Info("credentials loaded", "key_id", signer.KeyID, "env", string(cfg.Environment), "source", "file", "sha256_8", keyFileFingerprint(cfg.KalshiKeyFile))
		} else {
			log.Info("credentials loaded", "key_id", signer.KeyID, "env", string(cfg.Environment), "source", kalSrc)
		}
	}

	// Prod market-data WebSocket needs an authenticated handshake. If a read-only PROD key is in env
	// (KALSHI_PROD_KEY_ID/FILE), use it for the prod client so the real-time ticker feed connects.
	// Never used for orders — it is never armed for writes, so writeAllowed() refuses any order on it.
	if ps, perr := prodSignerFromEnv(); perr == nil && ps != nil {
		signer = ps
		log.Info("prod market-data key loaded (real-time ticker WS enabled)")
	}
	kc := newClient(cfg, signer)
	srv := server.New(cfg, log, ks, kc, store)
	if err := srv.StartupError(); err != nil {
		log.Error("server startup blocked by a money-ledger repair failure", "err", err)
		_ = store.Audit(context.Background(), "critical", "settlement",
			"server startup blocked by a money-ledger repair failure", err.Error())
		return "startup settlement repair failed", 1
	}
	srvKS = srv                  // late-bind the kill-switch halt target now that the server exists
	srv.SetConfigPath(cfgPath)   // let the Settings modal persist edits to config.json
	srv.SetVersion(buildVersion) // R20: build stamp (git sha + state + time) shown in the dashboard header
	if err := srv.PrepareForwardResearchGeneration(ctx); err != nil {
		log.Error("forward-research generation preparation failed", "err", err)
		return "forward research generation failed", 1
	}
	srv.SetBuildReleaseName(buildReleaseName)      // stable clean-commit animal; dirty builds get a temporary name
	srv.SetKalshiAuth(authState, authDetail)       // R86: /api/ready "kalshi_auth" component + the 🔑 AUTH LOCKED chip
	srv.SetWindowFullscreenFn(appWindowFullscreen) // R67c: ⛶ fallback — user32 borderless-maximize of the app window (no-op off Windows)
	_ = store.Audit(ctx, "info", "system", "server starting", string(cfg.Environment))
	go kc.StartTickerStream(ctx, kalshi.WSURLFor(string(cfg.Environment))) // real-time prices; no-op without a signer
	go kc.StartBoardRefresher(ctx)                                         // keep the liquid-market board warm — request paths never pull it themselves (audit §4)

	// Wire the DEMO order client (live-sandbox mode). Built from the demo env key; demo-host-locked
	// so it can never touch real money. Absent key → live-sandbox is unavailable and we stay paper.
	if dsigner, derr := demoSignerFromEnv(); derr == nil && dsigner != nil {
		srv.SetDemoClient(kalshi.NewClient(
			kalshi.BaseDemo, dsigner, cfg.Kalshi.RateLimitPerSec,
			time.Duration(cfg.Kalshi.RequestTimeoutMs)*time.Millisecond,
		))
		log.Info("demo sandbox client ready", "mode", srv.ExecMode())
	} else {
		log.Info("no demo key in env; paper mode only (live-sandbox unavailable)")
	}

	errCh := make(chan error, 1)
	go func() { errCh <- srv.Start() }()

	if cfg.OpenBrowser {
		go func() {
			time.Sleep(700 * time.Millisecond)
			url := "http://" + cfg.ServerAddr
			log.Info("opening dashboard in browser", "url", url)
			openAppWindow(url) // TABCLOSE: app window so the dashboard can close its own tab when the suite stops
		}()
	}

	go runBriefingLoop(ctx, log, srv)         // NTFYALL · R67i: launches unconditionally, self-gates on the LIVE config each tick (Settings toggles apply without restart)
	go runTelegramBriefingLoop(ctx, log, srv) // R67i: independent periodic Telegram briefing (creds re-read per tick; R92: 5-min cadence)
	go runLegSimLoop(ctx, log, srv)           // ntfy leg-render audit across ALL markets (every 15m) → data/ntfy_legsim*.{jsonl,log}
	// MONITOR SUPERVISION (audit §6): an unrecovered panic in ANY goroutine kills the whole Go
	// process — on a live server that also strands resting real-money orders. Every monitor now
	// runs under a guard that logs+audits the panic and RESTARTS the monitor after 5s.
	guard := func(name string, fn func(context.Context)) {
		go func() {
			for ctx.Err() == nil {
				func() {
					defer func() {
						if r := recover(); r != nil {
							log.Error("PANIC in monitor — restarting in 5s", "monitor", name, "panic", fmt.Sprint(r))
							_ = store.Audit(context.Background(), "critical", "system", "panic in "+name+" (auto-restarted)", fmt.Sprint(r))
						}
					}()
					fn(ctx)
				}()
				if ctx.Err() == nil {
					time.Sleep(5 * time.Second) // panic OR unexpected return — back off, then restart
				}
			}
		}()
	}
	srv.BootResetPnL(ctx)                               // R30 (operator, reset_on_start switch): close ALL paper/ML positions + zero P&L/graphs BEFORE any trading loop starts — every boot begins from a clean slate
	guard("MonitorPaper", srv.MonitorPaper)             // auto-close paper positions on take-profit / stop-loss
	guard("MonitorAuto", srv.MonitorAuto)               // PAPER auto-pilot (off unless enabled from the dashboard)
	guard("MonitorAutoSell", srv.MonitorAutoSell)       // PAPER auto-sell: close consensus positions when the signal reverses
	guard("PolyLiveTrades", srv.StartPolyLiveTrades)    // RTDS WebSocket: real-time Polymarket trade stream
	guard("PolyCLOBMarketWS", srv.StartPolyCLOBWS)      // R70-B #10: CLOB market channel (custom features) — market_resolved PUSH-settles poly-int paper/signals ahead of the 20s poll sweep
	guard("MonitorPolyWhales", srv.MonitorPolyWhales)   // rebuild the Poly whale feed from the live buffer (server-side)
	guard("MonitorPolyTraders", srv.MonitorPolyTraders) // Phase 1b: persist ranked-wallet trade history → real per-whale hit rate
	guard("MonitorSignalLog", srv.MonitorSignalLog)     // ALWAYS log signals (kalshi-flow/whale + poly-consensus) even with dashboard closed / auto off
	guard("MonitorNetPnL", srv.MonitorNetPnL)           // sample live net P&L (real-time chart movement, not just on close)
	guard("MonitorPolyUS", srv.MonitorPolyUS)           // poll Poly US OWN game markets + prices (feeds the PolyUS panels)
	guard("MonitorParlays", srv.MonitorParlays)         // PAPER parlay book: settle combos once every leg resolves
	guard("MonitorLiveStale", srv.MonitorLiveStale)     // LIVE: cancel resting real-money orders that go stale (ML drops the pick, or market moves ≥1¢)
	guard("MonitorBookWS", srv.MonitorBookWS)           // R74: keep the orderbook_delta working set current (positions/rests/proposals/combo legs + top-volume fill)
	guard("MonitorLiveAuto", srv.MonitorLiveAuto)       // R41 AUTO LIVE: armed + AUTO → places the live proposals itself ($1/bet, $10 singles, $1/$5 combos)
	guard("MonitorRetention", srv.MonitorRetention)     // R70 (audit §e): bounded DB retention — audit/arb/maker-stats age out, signal_log path blobs trim, training rows NEVER deleted
	guard("MonitorPTTBackfill", srv.MonitorPTTBackfill) // R125: budgeted historical grader for the ptt unresolved backlog (gamma closed-markets root cause)
	guard("RESTHealthWatch", srv.RESTHealthWatch)       // R85: Kalshi REST wedge watchdog — no HTTP response >120s while WS alive ⇒ CRITICAL audit + one page/episode + limiter self-heal reset (never wedged for hours again)
	// R87: the PolyUS credential outcome is classified for /api/ready ("polyus_auth", mirroring
	// kalshi_auth) — configured-but-broken used to fall into the silent "off" branch below.
	pus, pusSrc, pusErr := polyUSLoad(cfg)
	switch {
	case pusErr != nil:
		pusDetail := "polyus credentials present but unusable: " + pusErr.Error() // path/config errors only — never the secret
		if errors.Is(pusErr, errPolyUSNoKeyID) {
			pusDetail = "polyus secret file found but no key id — set polyus_key_id (Settings) or POLY_US_KEY_ID (env)"
		}
		srv.SetPolyUSAuthState("error", pusDetail)
		log.Warn("Poly US credentials not loaded — real-time feed + live order path OFF", "source", pusSrc, "err", pusErr)
	case pus == nil:
		srv.SetPolyUSAuthState("none", "no PolyUS credentials configured (set polyus_key_file + polyus_key_id, or POLY_US_* env)")
		log.Info("Poly US WebSocket off — set polyus_key_file + polyus_key_id (or POLY_US_KEY_ID + POLY_US_SECRET(_FILE) env) to enable the real-time feed")
	default: // real-time Poly US feed (live price + taker-flow; no REST limit)
		srv.SetPolyUSAuthState("ok", "Ed25519 client ready (key "+pus.KeyID()+", source "+pusSrc+")")
		if pusSrc == "file" { // R87: log source + sha256 prefix of the FILE BYTES only — never the secret
			log.Info("Poly US key loaded", "key_id", pus.KeyID(), "source", "file", "sha256_8", keyFileFingerprint(cfg.PolyUSKeyFile))
		} else {
			log.Info("Poly US key loaded", "key_id", pus.KeyID(), "source", pusSrc)
		}
		pws := pus.NewMarketsWSWithFullBookCap(cfg.PolyUSBookWSCap)
		srv.SetPolyUSWS(pws)
		srv.SetPolyUSAuth(pus) // R8: the SAME key also signs POST /v1/orders — enables the guarded PolyUS live path
		go pws.Stream(ctx, srv.PolyUSSlugs)
		priv := pus.NewPrivateWS() // R15: push stream for OUR orders/fills/positions/balance (zero-poll live)
		srv.SetPolyUSPrivWS(priv)
		go priv.Stream(ctx)
		log.Info("Poly US WebSockets enabled: markets (price+flow) + PRIVATE (orders/fills/positions push); live order path armed+capped",
			"full_depth_cap", pws.FullBookCap())
	}
	if state := ks.State(); state.Tripped {
		// Restore-HALT recovery waits until both authenticated venue clients are attached. HTTP may
		// already answer read-only requests, but reset/arm stays fail-closed until this sweep finishes.
		srv.OnKillSwitchTrip(state.Reason)
	}
	guard("PrewarmSnapshots", srv.PrewarmSnapshots)     // R10: build EVERY tab's snapshot in parallel at boot + keep Portfolio/ML perpetually fresh (R70, audit §b P3: under the guard-supervisor like every other monitor)
	go runSnapshotLoop(ctx, cfg.Briefing.Dir, log, srv) // export decision snapshot for the GO/NO-GO gate
	go runMLAccuracyLoop(ctx, log, srv)                 // R117 Part 4: weekly real-bet accuracy report (data/ml_accuracy.json → /api/mlaccuracy → ML tab card)
	if cfg.Backup.Enabled && cfg.Backup.EveryMinutes > 0 {
		go runBackupLoop(ctx, cfg, log, store) // DRIVESYNC: periodic consistent DB+ML snapshot into cfg.Backup.Dir (a Google Drive folder)
	}
	job := newKillOnCloseJob(log)                            // MLPROC: Windows job so the ML sidecar dies with the suite no matter HOW it's closed
	mlSup := superviseMLSidecar(ctx, cfg, cfgPath, log, job) // MLHEAL: run the ML sidecar under a supervisor that auto-restarts it if it crashes (OOM mid-retrain, etc.)
	// MINIREVERT: the pcrypto mini-server is NO LONGER launched by the suite — co-launching it caused
	// resource contention / slow loads / WS disconnects. Run it STANDALONE via pcrypto-server/run.bat.
	// (startMiniServer is kept for reference but intentionally not called.)

	exitReason, srvFatal, serveErr := awaitServeExitAndCancel(ctx, errCh, stop)
	switch exitReason {
	case "shutdown_signal":
		log.Info("shutdown signal received")
	case "server_error":
		log.Error("server error", "err", serveErr)
	case "server_returned":
		log.Warn("server returned without an error or shutdown signal")
	}

	mlSup.stop()                                     // MLHEAL: stop the supervisor + kill the current sidecar child (no orphaned window, no restart)
	if cfg.Backup.Enabled && cfg.Backup.OnShutdown { // DRIVESYNC: write a fresh snapshot on the way out so switching PCs always carries the latest
		log.Info("writing final data snapshot before exit…")
		// R89 (auditor bug 71): BOUND the shutdown snapshot — the bare Background ctx let a hung
		// VACUUM block shutdown forever, and a console-X close (~5s Windows grace) truncated the
		// .tmp mid-copy (a 705MB kalshi.db.tmp was found abandoned on 07-05). 4 minutes covers a
		// healthy multi-GB VACUUM INTO; past that we keep the previous snapshot and exit clean.
		bctx, bcancel := context.WithTimeout(context.Background(), 4*time.Minute)
		// Close HTTP/cash admission, drain every comparison producer, and fence execution-shadow
		// before selecting the final backup generation. Shutdown repeats the idempotent boundary.
		if err := backupNowAfterExecutionShadowFence(bctx, cfg, store, log, srv); err != nil {
			log.Error("shutdown snapshot failed (previous snapshot kept)", "err", err)
		}
		bcancel()
	}
	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutCtx)
	_ = store.Audit(context.Background(), "info", "system", "server stopped", "")
	if srvFatal { // R86: bind-fail class errors used to exit 0 + vanish — hold readable, exit non-zero
		stop()        // restore default Ctrl+C disposition so the 60s hold can be skipped
		store.Close() // main's deferred Close never runs past os.Exit
		fatalBootHold()
		return exitReason, 1
	}
	log.Info("stopped cleanly")
	return exitReason, 0
}

// startMLSidecar launches the Python ML edge-scorer (ml/live_ml.py) as a CHILD process that shares
// the suite's output streams. Windows creates it with CREATE_NO_WINDOW, so a console-less suite
// cannot make Python allocate a Windows Terminal. Ctrl+C/shutdown of the suite kills it too.
// Best-effort: if Python isn't installed it logs and returns nil.
func startMLSidecar(cfg config.Config, cfgPath string, log *slog.Logger) *exec.Cmd {
	py := "python"
	if runtime.GOOS != "windows" {
		py = "python3"
	}
	cmd := exec.Command(py, mlSidecarArgs(cfg, cfgPath)...)
	// Python otherwise inherits the Windows ANSI code page even though its redirected log files
	// contain UTF-8 market titles. One ⇄ character used to abort every scoring cycle at print().
	cmd.Env = append(os.Environ(), "PYTHONUTF8=1", "PYTHONIOENCODING=utf-8")
	configureHeadlessChild(cmd)
	cmd.Dir = "ml" // run from the ml/ folder next to the suite exe
	childLogs := configureMLChildOutput(cmd, cfg.DataDir)
	if err := cmd.Start(); err != nil {
		closeChildLogs(childLogs)
		log.Info("ML sidecar not started (Python missing?) — skipping", "err", err)
		return nil
	}
	// StartProcess duplicated the inheritable file handles into the child. The parent copies are no
	// longer needed and closing them prevents one descriptor leak on every supervisor restart.
	closeChildLogs(childLogs)
	log.Info("ML edge-scorer started as a no-console child (book-native first model waits for 150 resolved snapshots; later fits coalesce until max(200 new, 0.2% of training N) or ~15m; prices re-score every 10s)")
	return cmd
}

func configureMLChildOutput(cmd *exec.Cmd, dataDir string) []*os.File {
	if runtime.GOOS != "windows" {
		cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
		return nil
	}
	// A console-less parent must not pass invalid console handles to Python. Direct files keep the
	// scorer auditable and avoid the hidden console host that Windows can create for stream repair.
	cmd.Stdout, cmd.Stderr = io.Discard, io.Discard
	var opened []*os.File
	if f, err := os.OpenFile(filepath.Join(dataDir, "ml_sidecar.out.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644); err == nil {
		cmd.Stdout = f
		opened = append(opened, f)
	}
	if f, err := os.OpenFile(filepath.Join(dataDir, "ml_sidecar.err.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644); err == nil {
		cmd.Stderr = f
		opened = append(opened, f)
	}
	return opened
}

func closeChildLogs(files []*os.File) {
	for _, f := range files {
		_ = f.Close()
	}
}

// mlSupervisor keeps the Python ML sidecar alive. If the child exits on its own (e.g. OOM-killed
// mid-retrain), it is automatically relaunched after a short delay, so scoring never silently stops.
// stop() sets the shutdown flag and kills the current child so it does NOT get restarted on exit.
type mlSupervisor struct {
	mu      sync.Mutex
	cur     *exec.Cmd
	stopped bool
}

// superviseMLSidecar launches the ML sidecar under a restart-on-crash loop and returns the supervisor.
func superviseMLSidecar(ctx context.Context, cfg config.Config, cfgPath string, log *slog.Logger, job *killOnCloseJob) *mlSupervisor {
	s := &mlSupervisor{}
	go func() {
		for {
			if ctx.Err() != nil {
				return
			}
			cmd := startMLSidecar(cfg, cfgPath, log)
			if cmd == nil {
				return // Python missing — don't spin restarting nothing
			}
			s.mu.Lock()
			if s.stopped { // shutdown raced the relaunch — kill it and bail
				s.mu.Unlock()
				if cmd.Process != nil {
					_ = cmd.Process.Kill()
				}
				return
			}
			s.cur = cmd
			s.mu.Unlock()
			if job != nil && cmd.Process != nil {
				job.assign(cmd.Process.Pid, "ml-sidecar", log)
			}
			_ = cmd.Wait() // blocks until the child exits (crash or kill)
			s.mu.Lock()
			stopped := s.stopped
			s.mu.Unlock()
			if stopped || ctx.Err() != nil {
				return
			}
			log.Warn("ML sidecar exited unexpectedly — auto-restarting in 5s")
			select {
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Second):
			}
		}
	}()
	return s
}

// stop halts the supervisor and takes the current sidecar child down with the suite.
func (s *mlSupervisor) stop() {
	s.mu.Lock()
	s.stopped = true
	c := s.cur
	s.mu.Unlock()
	if c != nil && c.Process != nil {
		_ = c.Process.Kill()
	}
}

// startMiniServer launches the standalone pcrypto mini-server (pcrypto-server/, dashboard on
// 127.0.0.1:8799) as a no-console CHILD process; shutdown of the suite kills it too (Process.Kill
// in main). We spawn the PREBUILT exe, not `go run .`, because
// `go run` would leave the compiled child orphaned when killed. Best-effort: if the exe isn't there
// (run build-suite.bat to build it) it logs + returns nil so the suite still runs fine.
func startMiniServer(log *slog.Logger) *exec.Cmd {
	dir := filepath.Join("..", "pcrypto-server")
	exe := filepath.Join(dir, "pcrypto-server.exe")
	if runtime.GOOS != "windows" {
		exe = filepath.Join(dir, "pcrypto-server")
	}
	if _, err := os.Stat(exe); err != nil {
		log.Info("pcrypto mini-server exe not found — skipping (run build-suite.bat to build it)", "path", exe)
		return nil
	}
	cmd := exec.Command(exe, "-headless") // paper mode, port 127.0.0.1:8799, no browser/window
	configureHeadlessChild(cmd)
	cmd.Dir = dir                                 // run from pcrypto-server/ so it finds its settings/state json
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr // preserve output without allowing another window
	if err := cmd.Start(); err != nil {
		log.Info("pcrypto mini-server not started — skipping", "err", err)
		return nil
	}
	log.Info("pcrypto mini-server started as a no-console child (dashboard 127.0.0.1:8799)")
	return cmd
}

// runBriefingLoop writes a fresh markdown briefing to the watch folder on startup
// and every Briefing.EveryMinutes, using the app's live market data. The forwarder
// picks up each new file and pushes it. Replaces the stale external scheduled task.
// pruneBriefings deletes briefing-*.md files older than an hour so the folder doesn't fill up.
// The forwarder sends each within seconds of it being written, so old ones are safe to remove.
func pruneBriefings(dir string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	cutoff := time.Now().Add(-time.Hour)
	for _, e := range entries {
		n := e.Name()
		isBrief := len(n) >= 9 && n[:9] == "briefing-"
		isML := len(n) >= 7 && n[:7] == "mlbook-"
		if e.IsDir() || filepath.Ext(n) != ".md" || (!isBrief && !isML) {
			continue
		}
		if info, ierr := e.Info(); ierr == nil && info.ModTime().Before(cutoff) {
			_ = os.Remove(filepath.Join(dir, n))
		}
	}
}

func runBriefingLoop(ctx context.Context, log *slog.Logger, srv *server.Server) {
	// R67i: the old loop captured cfg BY VALUE at launch (and main gated the launch on the boot
	// config), so toggling the ntfy briefing in Settings needed a restart. Now the loop launches
	// unconditionally and re-reads srv.BriefingCfg() EVERY tick: clearing enabled/dir stops the
	// .md drops (= stops the ntfy forwarder) on the next tick; enabling starts them the same way.
	// NOTE (R67i finding): ntfy itself is sent by the SEPARATE forwarder process, which reads
	// forwarder-config.json once at ITS start — the suite-side stop works by stopping the .md
	// feed the forwarder watches.
	write := func(dir string) {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			log.Error("briefing dir", "err", err)
			return
		}
		bctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		md := srv.BriefingText(bctx)
		// Unique name (the forwarder dedups by filename, so each gets sent), but written
		// ATOMICALLY via temp+rename so the forwarder never reads a half-written file — and the
		// folder is pruned so old briefings don't pile up.
		path := filepath.Join(dir, "briefing-"+time.Now().Format("2006-01-02-1504")+".md")
		tmp := path + ".tmp"
		if err := os.WriteFile(tmp, []byte(md), 0o644); err != nil {
			log.Error("write briefing", "err", err)
			return
		}
		if err := os.Rename(tmp, path); err != nil {
			log.Error("rename briefing", "err", err)
			return
		}
		pruneBriefings(dir)
		log.Info("wrote briefing", "file", path)
		// R65's Telegram mirror moved out: Telegram has its OWN 2-minute loop now (R67i).
	}
	time.Sleep(25 * time.Second) // let the first market pull + Kalshi event-title cache warm so the FIRST briefing's parlay matchups aren't blank
	var lastWrite time.Time
	t := time.NewTicker(30 * time.Second) // fast gate tick; the write cadence below honors EveryMinutes LIVE
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			bc := srv.BriefingCfg() // re-read the LIVE config — Settings changes apply without restart
			if !srv.NtfyDropsEnabled() {
				continue // R82 TELEGRAM-ONLY (default): no ntfy .md drops — the 2-min Telegram loop is the sole messenger; flip messenger_mode to ntfy/both to restore the drops
			}
			if !bc.Enabled || strings.TrimSpace(bc.Dir) == "" {
				continue
			}
			every := time.Duration(bc.EveryMinutes) * time.Minute
			if every <= 0 {
				every = 5 * time.Minute
			}
			if time.Since(lastWrite) < every {
				continue
			}
			lastWrite = time.Now()
			write(bc.Dir)
		}
	}
}

// runTelegramBriefingLoop — R67i: the Telegram briefing is its OWN ticker, independent of the
// ntfy .md cadence. R92 (operator: "the telegram 5 min updater"): the fixed cadence is now
// 5 MINUTES (was 2 — R67i), and every update opens with the equity header (💰 per-book cash +
// position value, built inside BriefingText). Creds are re-read from the live config every tick
// (TgSend reads s.cfg per call), so pasting creds starts sends and clearing either box stops
// them — no restart in either direction.
//
// R83 SILENCE-PROOFING (live incident 2026-07-05: sends stopped 40+ min while the auto loops were
// wedged in a Kalshi REST logjam — the transport itself tested fine, message_id delivered).
// BriefingText can block PAST its 30s ctx in ctx-free sections (mark/title helpers, venue REST
// with private deadlines), and the old loop ran the build INLINE — one wedged build silenced the
// channel indefinitely with zero trace. Now the build runs on a side goroutine (at most ONE in
// flight, so a wedge can never leak goroutines) and the loop NEVER blocks past 90s: a slow build
// triggers a degraded "briefing delayed" send immediately, and every later tick reports the
// in-flight age until the wedged build finally returns. Silence is impossible by construction.
//
// R83 AUDIT + HEARTBEAT (completing the incident fix): (1) the loop's LAUNCH is audited at boot —
// "loop never started" is now checkable in the trail; (2) EVERY skip reason is audited (throttled —
// creds-missing 1/30min, wedge 1/10min) in category "telegram"; (3) an EMPTY briefing still sends a
// minimal heartbeat line at most every 30 minutes (config telegram_heartbeat, default TRUE) — so a
// silent phone means the suite is down, full stop. Telegram delay notices are throttled to 1/10min
// (the audit trail gets every occurrence; the phone doesn't need 30 copies of the same wedge).
func runTelegramBriefingLoop(ctx context.Context, log *slog.Logger, srv *server.Server) {
	log.Info("telegram briefing loop started", "cadence", "5m")
	srv.AuditEvent("info", "telegram", "telegram briefing loop started (5-min cadence — R92; R83 boot audit)")
	time.Sleep(40 * time.Second) // same warmup grace as the ntfy briefing
	var (
		mu         sync.Mutex
		inFlight   bool
		startAt    time.Time
		delayPaged bool // R85: this build has already sent its wedge page (once per episode; repeats ≤1/15min)
	)
	lastSent := time.Now() // any Telegram activity from this loop (briefing/degraded/delay/heartbeat)
	var lastSkipAudit, lastDelayNotice time.Time
	t := time.NewTicker(5 * time.Minute) // R92: "the telegram 5 min updater" — was 2m (R67i)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if !srv.TelegramReady() {
				if time.Since(lastSkipAudit) > 30*time.Minute { // R83: creds-off is a legitimate state, but it must be VISIBLE in the trail
					lastSkipAudit = time.Now()
					srv.AuditEvent("info", "telegram", "telegram briefing skipped: bot token/chat id empty (paste creds in Settings to resume)")
				}
				continue // creds cleared in Settings → sends stop, no restart needed
			}
			mu.Lock()
			if inFlight { // previous build still wedged → say so instead of nothing
				age := time.Since(startAt).Round(time.Minute)
				// R85 hygiene: page ONCE per wedge episode (this build), then ≤1/15min while it
				// persists; the audit trail still records every wedged tick.
				fire := !delayPaged || time.Since(lastDelayNotice) >= 15*time.Minute
				if fire {
					delayPaged = true
				}
				mu.Unlock()
				log.Warn("telegram briefing build still in flight", "age", age.String())
				srv.AuditEvent("warn", "telegram", fmt.Sprintf("telegram briefing skipped: previous build still in flight %s (REST logjam?)", age))
				if fire {
					lastDelayNotice = time.Now()
					lastSent = time.Now()
					srv.TgSend(fmt.Sprintf("⚠️ briefing delayed: build in flight %s (REST logjam?) — suite is UP; check /api/ready + audit log. Repeats ≤1/15min while delayed.", age))
				}
				continue
			}
			inFlight = true
			startAt = time.Now()
			delayPaged = false
			mu.Unlock()
			done := make(chan string, 1) // buffered: a late build result never blocks the abandoned goroutine
			go func() {
				bctx, cancel := context.WithTimeout(ctx, 30*time.Second)
				defer cancel()
				md := srv.BriefingText(bctx)
				mu.Lock()
				inFlight = false
				wedgeDur, wasWedged := time.Since(startAt), delayPaged
				mu.Unlock()
				if wasWedged { // R85: a paged wedge must also page its RECOVERY — one line, with the total wedge time
					srv.AuditEvent("info", "telegram", fmt.Sprintf("briefing build recovered after %s", wedgeDur.Round(time.Second)))
					srv.TgSend(fmt.Sprintf("✅ briefing recovered (%s) — normal briefings resume", wedgeDur.Round(time.Second)))
				}
				done <- md
			}()
			select {
			case md := <-done:
				if strings.TrimSpace(md) == "" {
					// R83 HEARTBEAT: an empty briefing must NOT mean a silent phone. Audit the skip
					// every time; send the minimal proof-of-life line at most every 30 minutes
					// (config telegram_heartbeat, default true).
					srv.AuditEvent("info", "telegram", "telegram briefing skipped: briefing text empty this tick")
					if srv.TelegramHeartbeatOn() && time.Since(lastSent) > 30*time.Minute {
						lastSent = time.Now()
						srv.TgSend("♥ kalshi-suite up · " + time.Now().Format("15:04") + " · briefing had nothing to report this cycle")
					}
					continue
				}
				lastSent = time.Now()
				// R126 store-and-forward: the queue owns BriefingMarkSent now (R124 invariant —
				// the verdict snapshot advances ONLY on REAL delivery, direct or flushed after an
				// offline stretch), so the explicit MarkSent call moved into the queue layer.
				srv.TgSendBriefing("main", md)
			case <-time.After(90 * time.Second): // build blew through every deadline → degraded send NOW; ticks above track the wedge
				lastSent = time.Now()
				srv.AuditEvent("warn", "telegram", "telegram briefing build exceeded 90s — degraded notice sent; next completion resumes normal briefings")
				srv.TgSend("⚠️ briefing build slow (>90s) — normal briefings resume when it completes; suite is UP")
			case <-ctx.Done():
				return
			}
		}
	}
}

// runParlayBriefingLoop sends a SEPARATE ntfy every 10 min = the dedicated parlay message (open book +
// top suggestions per venue with pick/side/price + recent settled). Written as briefing-parlay-*.md so
// the existing forwarder + pruner (both keyed on the "briefing-" prefix) deliver + clean it up.
func runParlayBriefingLoop(ctx context.Context, cfg config.Config, log *slog.Logger, srv *server.Server) {
	write := func() {
		bctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		md := srv.ParlayBriefingText(bctx) // TEXT built identically in every messenger mode (R82)
		if srv.NtfyDropsEnabled() {        // R82 TELEGRAM-ONLY (default): skip the ntfy .md drop, keep the Telegram send below
			if err := os.MkdirAll(cfg.Briefing.Dir, 0o755); err != nil {
				return
			}
			path := filepath.Join(cfg.Briefing.Dir, "briefing-parlay-"+time.Now().Format("2006-01-02-1504")+".md")
			tmp := path + ".tmp"
			if err := os.WriteFile(tmp, []byte(md), 0o644); err != nil {
				return
			}
			if err := os.Rename(tmp, path); err != nil {
				return
			}
			log.Info("wrote parlay briefing", "file", path)
		}
		srv.TgSendBriefing("parlay", md) // R65 mirror; R82: no-ops in ntfy-only mode; R126: queues offline, latest parlay wins
	}
	// NO startup write — the main briefing fires at boot; the parlay push's first send is one interval in,
	// so the two never stack on top of each other at startup.
	t := time.NewTicker(10 * time.Minute)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			write()
		}
	}
}

// runLegSimLoop periodically renders the ntfy leg-line for EVERY live/recorded market (Kalshi + Poly US +
// Poly-int) and logs how each comes out + quality flags, so a future notification-render regression is
// caught quickly. Writes data/ntfy_legsim.jsonl (full snapshot) + data/ntfy_legsim_problems.log (trail).
func runLegSimLoop(ctx context.Context, log *slog.Logger, srv *server.Server) {
	run := func() {
		bctx, cancel := context.WithTimeout(ctx, 150*time.Second) // enough for all rate-limited title/event fetches
		defer cancel()
		total, flagged, _ := srv.NtfyLegSim(bctx)
		log.Info("ntfy leg-sim", "markets", total, "flagged", flagged)
	}
	select {
	case <-ctx.Done():
		return
	case <-time.After(60 * time.Second): // let the market caches warm first
	}
	run()
	t := time.NewTicker(15 * time.Minute)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			run()
		}
	}
}

// runMLBookLoop sends a SEPARATE ntfy message every 15 min with the ML paper book's standing
// (equity / net / ROI / win-rate / open + top live pick), written as its own mlbook-*.md file into
// the briefing dir so the forwarder delivers it independently of the main briefing.
func runMLBookLoop(ctx context.Context, cfg config.Config, log *slog.Logger, srv *server.Server) {
	write := func() {
		md := srv.MLBookBriefing() // TEXT built identically in every messenger mode (R82)
		if md == "" {
			return // ML sidecar hasn't produced data yet — don't send an empty message
		}
		if srv.NtfyDropsEnabled() { // R82 TELEGRAM-ONLY (default): skip the ntfy .md drop, keep the Telegram send below
			if err := os.MkdirAll(cfg.Briefing.Dir, 0o755); err != nil {
				return
			}
			path := filepath.Join(cfg.Briefing.Dir, "mlbook-"+time.Now().Format("2006-01-02-1504")+".md")
			tmp := path + ".tmp"
			if err := os.WriteFile(tmp, []byte(md), 0o644); err != nil {
				return
			}
			if err := os.Rename(tmp, path); err != nil {
				return
			}
			pruneBriefings(cfg.Briefing.Dir)
			log.Info("wrote ML book briefing", "file", path)
		}
		srv.TgSendBriefing("ml", md) // R65 mirror; R82: no-ops in ntfy-only mode; R126: queues offline, latest ML briefing wins
	}
	time.Sleep(25 * time.Second) // let the ML sidecar produce its first ml_paper.json
	write()
	t := time.NewTicker(15 * time.Minute)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			write()
		}
	}
}

// runMLAccuracyLoop refreshes the real-bet accuracy report. The five-minute wake is cheap because
// RefreshMLAccuracy content-addresses the exact settled cohort and returns immediately when its
// count/max-close/hash receipt is unchanged. First pass waits for books/store warm-up.
func runMLAccuracyLoop(ctx context.Context, log *slog.Logger, srv *server.Server) {
	run := func() {
		rctx, cancel := context.WithTimeout(ctx, 60*time.Second)
		defer cancel()
		if err := srv.RefreshMLAccuracy(rctx, false); err != nil && !shutdownQuietErr(ctx, err) {
			log.Warn("ml accuracy refresh failed", "err", err)
		}
	}
	select {
	case <-ctx.Done():
		return
	case <-time.After(2 * time.Minute): // warmup — let the books/store settle first
	}
	run()
	t := time.NewTicker(5 * time.Minute)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			run()
		}
	}
}

// shutdownQuietErr — R72-A #1 (shutdown alarm hygiene): true when err is nothing but the process
// tearing down — the loop's parent ctx is already canceled AND the error unwraps to
// context.Canceled/DeadlineExceeded. Loop alarms skip in that case; real mid-session failures
// (parent ctx alive) still log at full volume.
func shutdownQuietErr(ctx context.Context, err error) bool {
	if err == nil || ctx.Err() == nil {
		return false
	}
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// runSnapshotLoop exports a complete decision snapshot (portfolio + stats + risk +
// all live signals) to <dir>/decision-snapshot.json every few minutes, so an external
// GO/NO-GO gate can read the current state as a file. Reuses the briefing dir.
func runSnapshotLoop(ctx context.Context, dir string, log *slog.Logger, srv *server.Server) {
	if dir == "" {
		log.Info("snapshot export off (no briefing dir set)")
		return
	}
	write := func() {
		sctx, cancel := context.WithTimeout(ctx, 25*time.Second)
		defer cancel()
		b, err := srv.SnapshotJSON(sctx)
		if err != nil {
			if !shutdownQuietErr(ctx, err) { // R72-A #1: a snapshot canceled BY shutdown is not an alarm
				log.Error("build snapshot", "err", err)
			}
			return
		}
		if err := os.MkdirAll(dir, 0o755); err != nil {
			log.Error("snapshot dir", "err", err)
			return
		}
		// Atomic write: write to a temp file then rename, so the gate never reads a
		// half-written (truncated) snapshot. Rename is atomic on the same filesystem.
		path := filepath.Join(dir, "decision-snapshot.json")
		tmp := path + ".tmp"
		if err := os.WriteFile(tmp, b, 0o644); err != nil {
			log.Error("write snapshot", "err", err)
			return
		}
		if err := os.Rename(tmp, path); err != nil {
			log.Error("rename snapshot", "err", err)
		}
	}
	time.Sleep(8 * time.Second) // let the first market/signal pulls warm up
	write()
	// Every 10s. The gate runs every 5 min and the dashboard reads live /api endpoints, so nothing
	// needs sub-second snapshot freshness. The old 66ms (~15/s) cadence rewrote this ~1MB file so
	// fast that some synchronized mounts kept serving half-replaced or cached copies —
	// the gate then wasted MINUTES retrying truncated reads. 10s is plenty fresh and lets a reader
	// get one clean, stable copy.
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			write()
		}
	}
}

// runBriefingOnce generates a single briefing on demand (handy for testing): writes
// it to the configured dir (or current dir) and prints it to stdout.
func runBriefingOnce(cfg config.Config, log *slog.Logger, store *storage.Store) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	signer, _, _ := loadSigner(ctx, cfg, store)
	md := briefing.Generate(ctx, newClient(cfg, signer), time.Now())
	dir := cfg.Briefing.Dir
	if dir == "" {
		dir = "."
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		log.Error("briefing dir", "err", err)
		os.Exit(1)
	}
	name := "briefing-" + time.Now().Format("2006-01-02-1504") + ".md"
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(md), 0o644); err != nil {
		log.Error("write briefing", "err", err)
		os.Exit(1)
	}
	fmt.Println("Wrote", path)
	fmt.Println("----")
	fmt.Println(md)
	server.TelegramSend(cfg, log, md) // R65: the one-shot CLI publish mirrors to Telegram too (no-op without creds)
}

func runPing(cfg config.Config, log *slog.Logger, store *storage.Store) {
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()

	signer, _, err := loadSigner(ctx, cfg, store)
	if err != nil {
		log.Warn("could not load credentials; doing public check only", "err", err)
	}
	client := newClient(cfg, signer)

	st, err := client.GetExchangeStatus(ctx)
	if err != nil {
		log.Error("exchange status check failed", "err", err)
		os.Exit(1)
	}
	fmt.Printf("Exchange reachable (env=%s): exchange_active=%v trading_active=%v\n",
		cfg.Environment, st.ExchangeActive, st.TradingActive)

	if client.HasCredentials() {
		bal, err := client.GetBalance(ctx)
		if err != nil {
			log.Error("authenticated balance check failed", "err", err)
			os.Exit(1)
		}
		fmt.Printf("Authenticated OK. Account balance: $%.2f\n", float64(bal.Balance)/100.0)
	} else {
		fmt.Println("No credentials loaded. Run 'set-credentials' and set KALSHI_SUITE_PASSPHRASE for an authenticated check.")
	}
}

// demoSignerFromEnv builds a Kalshi DEMO signer straight from environment variables —
// no passphrase, no encrypted store. Demo keys guard only mock funds, so plaintext env
// vars are an appropriate (and far simpler) posture. Returns (nil, nil) when unset.
//
//	KALSHI_DEMO_KEY_ID    the demo API Key ID (UUID)
//	KALSHI_DEMO_KEY_FILE  path to the demo RSA private key file (.txt/.pem)
func demoSignerFromEnv() (*kalshi.Signer, error) {
	keyID := os.Getenv("KALSHI_DEMO_KEY_ID")
	keyFile := os.Getenv("KALSHI_DEMO_KEY_FILE")
	if keyID == "" || keyFile == "" {
		return nil, nil
	}
	pemBytes, err := os.ReadFile(keyFile)
	if err != nil {
		return nil, fmt.Errorf("read KALSHI_DEMO_KEY_FILE %q: %w", keyFile, err)
	}
	return kalshi.NewSigner(keyID, pemBytes)
}

// prodSignerFromEnv builds a PROD Kalshi signer from env — read-only market data, used only for the
// authenticated ticker WebSocket handshake. NEVER used for orders (demoOnly() gates writes to demo).
//
//	KALSHI_PROD_KEY_ID    the prod API Key ID (UUID)
//	KALSHI_PROD_KEY_FILE  path to the prod RSA private key file
func prodSignerFromEnv() (*kalshi.Signer, error) {
	keyID := os.Getenv("KALSHI_PROD_KEY_ID")
	keyFile := os.Getenv("KALSHI_PROD_KEY_FILE")
	if keyID == "" || keyFile == "" {
		return nil, nil
	}
	pemBytes, err := os.ReadFile(keyFile)
	if err != nil {
		return nil, fmt.Errorf("read KALSHI_PROD_KEY_FILE %q: %w", keyFile, err)
	}
	return kalshi.NewSigner(keyID, pemBytes)
}

// errPolyUSNoKeyID — R87/R88: the PolyUS SECRET file exists but no key id is configured (the
// key id the Ed25519 signature is labeled with) — neither config polyus_key_id (Settings) nor
// the POLY_US_KEY_ID env var. RED polyus_auth state; the detail is the fix.
var errPolyUSNoKeyID = errors.New("polyus secret file found but no key id set (Settings polyus_key_id, or POLY_US_KEY_ID env)")

// polyUSLoad builds a Polymarket US RETAIL client. The credential is the Key ID + base64
// Secret Key from polymarket.us/developer (Ed25519 request signing).
//
// R87 SOURCE ORDER (mirrors loadSigner): (a) cfg.PolyUSKeyFile EXISTS → its content is the
// base64 secret one-liner (trimmed; BOM tolerated) and WINS — POLY_US_KEY_ID env still names
// the key, and a broken/id-less file is a loud error, never a silent fallback; (b) no file →
// env exactly as before; (c) neither → (nil, "", nil) = public data only. Consumption is
// unchanged either way: polymarketus.NewClient(keyID, secret, 15s). The returned source
// ("file"/"env") feeds the polyus_auth readiness detail. NEVER log the secret.
//
//	polyus_key_id (config)  R88: your Key ID (developer portal) — WINS over the env var, so a
//	                        headless boot never depends on the launching process env again
//	                        (2026-07-05: User-scope POLY_US_KEY_ID was set, but the stale MCP
//	                        parent env didn't carry it → polyus_auth RED despite a valid file)
//	POLY_US_KEY_ID       env fallback for the Key ID — used with BOTH sources
//	POLY_US_SECRET       the base64 Secret Key (shown once)              — or —
//	POLY_US_SECRET_FILE  path to a file holding that base64 secret
func polyUSLoad(cfg config.Config) (*polymarketus.Client, string, error) {
	keyID := strings.TrimSpace(cfg.PolyUSKeyID) // R88: config first (Settings-persistable, survives any parent env)
	if keyID == "" {
		keyID = os.Getenv("POLY_US_KEY_ID")
	}
	if path := strings.TrimSpace(cfg.PolyUSKeyFile); path != "" {
		if fi, statErr := os.Stat(path); statErr == nil && !fi.IsDir() {
			b, err := os.ReadFile(path)
			if err != nil {
				return nil, "file", fmt.Errorf("read polyus key file %q: %w", path, err)
			}
			secret := strings.TrimSpace(strings.TrimPrefix(string(b), "\ufeff"))
			if secret == "" {
				return nil, "file", fmt.Errorf("polyus key file %q is empty", path)
			}
			if keyID == "" {
				return nil, "file", errPolyUSNoKeyID
			}
			c, err := polymarketus.NewClient(keyID, secret, 15*time.Second)
			return c, "file", err
		}
	}
	secret := os.Getenv("POLY_US_SECRET")
	if secret == "" {
		if f := os.Getenv("POLY_US_SECRET_FILE"); f != "" {
			b, err := os.ReadFile(f)
			if err != nil {
				return nil, "env", fmt.Errorf("read POLY_US_SECRET_FILE %q: %w", f, err)
			}
			secret = strings.TrimSpace(string(b))
		}
	}
	if keyID == "" || secret == "" {
		return nil, "", nil
	}
	c, err := polymarketus.NewClient(keyID, secret, 15*time.Second)
	return c, "env", err
}

// runPolyUS verifies the Polymarket US retail key WITHOUT placing any order: it signs a request with
// Ed25519 and calls /v1/portfolio/positions. A 200 proves the key + signing work (and the same key can
// trade). This is the safe "wire in / verify it" step before any live order entry.
func runPolyUS(cfg config.Config, log *slog.Logger, args []string) {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	c, _, err := polyUSLoad(cfg)
	if err != nil {
		log.Error("load Poly US key", "err", err)
		os.Exit(1)
	}
	if c == nil {
		fmt.Fprintln(os.Stderr, "Set polyus_key_file + polyus_key_id (or POLY_US_KEY_ID + POLY_US_SECRET(_FILE) env), then re-run 'polyus'.")
		os.Exit(2)
	}
	fmt.Printf("Polymarket US retail (api.polymarket.us): signing a test request (key_id=%s)...\n", c.KeyID())
	body, code, err := c.Positions(ctx)
	if err != nil {
		log.Error("request failed (network?)", "err", err)
		os.Exit(1)
	}
	preview := strings.TrimSpace(string(body))
	if len(preview) > 600 {
		preview = preview[:600] + "…"
	}
	fmt.Printf("GET /v1/portfolio/positions [%d]: %s\n", code, preview)
	switch {
	case code == 200:
		fmt.Println("RESULT: OK — Ed25519 signing works and the key is live. It can read positions and (same key) place real orders.")
	case code == 401 || code == 403:
		fmt.Println("RESULT: auth rejected (401/403) — check the Key ID + Secret match, the secret is the base64 from the portal, and the clock is NTP-synced (±30s).")
	default:
		fmt.Printf("RESULT: unexpected status %d — see the body above.\n", code)
	}
}

// runWS samples the Kalshi ticker WebSocket so you can verify the real-time price feed. Uses the
// PROD key if present (real prices), else the demo key (demo markets are quiet). The handshake is
// authenticated, so without a key it can't connect.
func runWS(cfg config.Config, log *slog.Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	signer, _ := prodSignerFromEnv()
	env := "prod"
	if signer == nil {
		signer, _ = demoSignerFromEnv()
		env = "demo"
	}
	if signer == nil {
		fmt.Fprintln(os.Stderr, "Set KALSHI_PROD_KEY_ID + KALSHI_PROD_KEY_FILE (real-time prod prices) — the WS handshake needs a key. Then re-run 'ws'.")
		os.Exit(2)
	}
	kc := kalshi.NewClient(
		kalshi.BaseURLFor(env), signer,
		cfg.Kalshi.RateLimitPerSec,
		time.Duration(cfg.Kalshi.RequestTimeoutMs)*time.Millisecond,
	)
	go kc.StartTickerStream(ctx, kalshi.WSURLFor(env))
	fmt.Printf("Kalshi %s ticker WS: sampling 15s...\n", env)
	for i := 0; i < 15; i++ {
		time.Sleep(time.Second)
		fmt.Printf("  live tickers: %d\n", kc.TickerCount())
	}
	fmt.Println("Done. A rising count means the real-time price feed is live.")
}

// runDemoCheck verifies the demo key end-to-end: it signs a request to the Kalshi DEMO
// host and prints the mock balance. This is read-only — no orders. It always talks to
// BaseDemo regardless of config.environment, so it won't disturb the prod data feed.
func runDemoCheck(cfg config.Config, log *slog.Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()

	signer, err := demoSignerFromEnv()
	if err != nil {
		log.Error("load demo key", "err", err)
		os.Exit(1)
	}
	if signer == nil {
		fmt.Fprintln(os.Stderr, "Set KALSHI_DEMO_KEY_ID and KALSHI_DEMO_KEY_FILE first, then re-run 'demo'.")
		os.Exit(2)
	}
	client := kalshi.NewClient(
		kalshi.BaseDemo, signer,
		cfg.Kalshi.RateLimitPerSec,
		time.Duration(cfg.Kalshi.RequestTimeoutMs)*time.Millisecond,
	)
	st, err := client.GetExchangeStatus(ctx)
	if err != nil {
		log.Error("demo exchange status failed", "err", err)
		os.Exit(1)
	}
	fmt.Printf("Kalshi DEMO reachable: exchange_active=%v trading_active=%v\n", st.ExchangeActive, st.TradingActive)

	bal, err := client.GetBalance(ctx)
	if err != nil {
		log.Error("demo balance check failed (key id / signature / clock?)", "err", err)
		os.Exit(1)
	}
	fmt.Printf("Authenticated OK on DEMO. Balance: $%.2f  (key_id=%s)\n", float64(bal.Balance)/100.0, signer.KeyID)
}

// runDemoOrder exercises the DEMO read+write loop end-to-end: it reads balance/orders/positions,
// then (only with --place) submits a tiny post-only resting order that CANNOT fill, lists resting
// orders, and cancels it. Default is a DRY RUN. Every write is hard-gated to the demo sandbox by
// kalshi.Client.demoOnly(), so this can never touch real money.
func runDemoOrder(cfg config.Config, log *slog.Logger, args []string) {
	fs := flag.NewFlagSet("demo-order", flag.ExitOnError)
	ticker := fs.String("ticker", "", "demo market ticker to test on (default: first liquid demo market)")
	place := fs.Bool("place", false, "actually place the tiny resting test order on the demo account, then cancel it")
	_ = fs.Parse(args)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	signer, err := demoSignerFromEnv()
	if err != nil {
		log.Error("load demo key", "err", err)
		os.Exit(1)
	}
	if signer == nil {
		fmt.Fprintln(os.Stderr, "Set KALSHI_DEMO_KEY_ID and KALSHI_DEMO_KEY_FILE first (see 'demo').")
		os.Exit(2)
	}
	client := kalshi.NewClient(
		kalshi.BaseDemo, signer,
		cfg.Kalshi.RateLimitPerSec,
		time.Duration(cfg.Kalshi.RequestTimeoutMs)*time.Millisecond,
	)

	if bal, err := client.GetBalance(ctx); err == nil {
		fmt.Printf("DEMO balance: $%.2f\n", float64(bal.Balance)/100.0)
	}
	if orders, err := client.GetOrders(ctx); err == nil {
		fmt.Printf("Resting orders: %d\n", len(orders))
	}
	if pos, err := client.GetPositions(ctx); err == nil {
		fmt.Printf("Open positions: %d\n", len(pos))
	}

	// Pick a non-crossing resting price: join the best YES bid (always rests as maker),
	// nudged under the best ask if the book is locked. ok=false when there's no room for a
	// safe resting bid (e.g. a deep-OTM market whose YES trades at 1c) — that was the
	// "post only cross" rejection on the auto-picked BTC strike.
	restingBid := func(ob *kalshi.Orderbook) (float64, bool) {
		if ob == nil || len(ob.YesBids) == 0 {
			return 0, false
		}
		p := ob.YesBids[0].Price
		if len(ob.YesAsks) > 0 && p >= ob.YesAsks[0].Price {
			p = ob.YesAsks[0].Price - 0.01
		}
		if p < 0.01 {
			return 0, false
		}
		return p, true
	}

	tk, price := *ticker, 0.0
	if tk != "" {
		ob, err := client.GetOrderbook(ctx, tk)
		if err != nil {
			log.Error("orderbook fetch failed", "ticker", tk, "err", err)
			os.Exit(1)
		}
		p, ok := restingBid(ob)
		if !ok {
			fmt.Fprintf(os.Stderr, "Market %s has no room for a safe resting bid (try a two-sided market).\n", tk)
			os.Exit(1)
		}
		price = p
	} else {
		mks, err := client.GetTopLiquidMarkets(ctx)
		if err != nil || len(mks) == 0 {
			fmt.Fprintln(os.Stderr, "No demo market found automatically (demo is sparse). Re-run with --ticker SOME_DEMO_TICKER.")
			os.Exit(1)
		}
		for i, m := range mks {
			if i >= 12 {
				break
			}
			ob, err := client.GetOrderbook(ctx, m.Ticker)
			if err != nil {
				continue
			}
			if p, ok := restingBid(ob); ok {
				tk, price = m.Ticker, p
				break
			}
		}
		if tk == "" {
			fmt.Fprintln(os.Stderr, "No two-sided demo market found to rest a test bid on. Re-run with --ticker SOME_DEMO_TICKER.")
			os.Exit(1)
		}
	}

	cents := int(price * 100.0) // floor to a whole-cent tick (valid on 1c and 0.1c markets; staying <= best bid keeps it non-crossing, never rounds up into the ask)
	if cents < 1 {
		cents = 1
	}
	priceStr := fmt.Sprintf("0.%02d", cents)
	req := kalshi.OrderRequest{
		Ticker:                  tk,
		ClientOrderID:           fmt.Sprintf("demo-test-%d", time.Now().UnixNano()),
		Side:                    "bid", // buy YES
		Count:                   "1",
		Price:                   priceStr,
		TimeInForce:             "good_till_canceled",
		SelfTradePreventionType: "taker_at_cross",
		PostOnly:                true, // maker-only: rejected rather than crossing → cannot fill
	}
	fmt.Printf("\nTest order → market %s: BUY 1 YES @ $%s, post-only (joins the bid, cannot fill).\n", tk, priceStr)

	if !*place {
		fmt.Println("DRY RUN — nothing sent. Re-run with --place to send it on the mock account, then auto-cancel.")
		return
	}

	ord, err := client.CreateOrder(ctx, req)
	if err != nil {
		log.Error("create demo order failed", "err", err)
		os.Exit(1)
	}
	fmt.Printf("Placed: order_id=%s fill_count=%s remaining=%s\n", ord.OrderID, ord.FillCount, ord.RemainingCount)
	if orders, err := client.GetOrders(ctx); err == nil {
		fmt.Printf("Resting orders now: %d\n", len(orders))
	}
	if err := client.CancelOrder(ctx, ord.OrderID); err != nil {
		log.Error("cancel failed — cancel it manually in the demo UI", "err", err, "order_id", ord.OrderID)
		os.Exit(1)
	}
	fmt.Printf("Canceled order_id=%s. Read+write loop verified on the demo account.\n", ord.OrderID)
}

// runLiveOrder is now read-only. The old diagnostic placed a real post-only GTC order and then
// canceled it, but post-only prevents crossing only at submission; it does not prevent a later
// maker fill. That bypassed the server's durable pre-send reservation, account recovery, writer
// fence, and kill-switch boundary. All PROD mutations must use the armed server pipeline.
func runLiveOrder(cfg config.Config, log *slog.Logger, args []string) {
	fs := flag.NewFlagSet("live-order", flag.ExitOnError)
	ticker := fs.String("ticker", "", "prod market ticker to test on (default: first liquid market)")
	place := fs.Bool("place", false, "retired: PROD writes are refused here; use the armed server pipeline")
	_ = fs.Parse(args)

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	signer, err := prodSignerFromEnv()
	if err != nil {
		log.Error("load prod key", "err", err)
		os.Exit(1)
	}
	if signer == nil {
		fmt.Fprintln(os.Stderr, "Set KALSHI_PROD_KEY_ID + KALSHI_PROD_KEY_FILE (your real Kalshi key) first, then re-run 'live-order'.")
		os.Exit(2)
	}
	client := kalshi.NewClient(kalshi.BaseProd, signer, cfg.Kalshi.RateLimitPerSec, time.Duration(cfg.Kalshi.RequestTimeoutMs)*time.Millisecond)

	bal, err := client.GetBalance(ctx)
	if err != nil {
		log.Error("prod balance check failed (key id / signature / clock?)", "err", err)
		os.Exit(1)
	}
	fmt.Printf("Kalshi PROD (REAL MONEY). Balance: $%.2f (key_id=%s)\n", float64(bal.Balance)/100.0, signer.KeyID)

	restingBid := func(ob *kalshi.Orderbook) (float64, bool) {
		if ob == nil || len(ob.YesBids) == 0 {
			return 0, false
		}
		p := ob.YesBids[0].Price
		if len(ob.YesAsks) > 0 && p >= ob.YesAsks[0].Price {
			p = ob.YesAsks[0].Price - 0.01
		}
		if p < 0.01 {
			return 0, false
		}
		return p, true
	}

	tk, price := *ticker, 0.0
	if tk != "" {
		ob, err := client.GetOrderbook(ctx, tk)
		if err != nil {
			log.Error("orderbook fetch failed", "ticker", tk, "err", err)
			os.Exit(1)
		}
		p, ok := restingBid(ob)
		if !ok {
			fmt.Fprintf(os.Stderr, "Market %s has no room for a safe resting bid (try a two-sided market).\n", tk)
			os.Exit(1)
		}
		price = p
	} else {
		mks, err := client.GetTopLiquidMarkets(ctx)
		if err != nil || len(mks) == 0 {
			fmt.Fprintln(os.Stderr, "No liquid market found automatically. Re-run with --ticker SOME_TICKER.")
			os.Exit(1)
		}
		for i, m := range mks {
			if i >= 12 {
				break
			}
			ob, err := client.GetOrderbook(ctx, m.Ticker)
			if err != nil {
				continue
			}
			if p, ok := restingBid(ob); ok {
				tk, price = m.Ticker, p
				break
			}
		}
		if tk == "" {
			fmt.Fprintln(os.Stderr, "No two-sided market found to rest a test bid on. Re-run with --ticker SOME_TICKER.")
			os.Exit(1)
		}
	}

	cents := int(price * 100.0)
	if cents < 1 {
		cents = 1
	}
	priceStr := fmt.Sprintf("0.%02d", cents)
	capUSD := cfg.Risk.LiveMaxOrderUSD
	if capUSD <= 0 {
		capUSD = 5
	}
	if float64(cents)/100.0 > capUSD {
		fmt.Fprintf(os.Stderr, "Test price $%s exceeds the $%.2f live cap; aborting.\n", priceStr, capUSD)
		os.Exit(1)
	}
	req := kalshi.OrderRequest{
		Ticker:                  tk,
		ClientOrderID:           fmt.Sprintf("live-test-%d", time.Now().UnixNano()),
		Side:                    "bid",
		Count:                   "1",
		Price:                   priceStr,
		TimeInForce:             "good_till_canceled",
		SelfTradePreventionType: "taker_at_cross",
		PostOnly:                true,
	}
	fmt.Printf("\nRead-only quote check -> %s: hypothetical BUY 1 YES @ $%s, post-only. REAL account.\n", tk, priceStr)

	_ = req // retain the exact hypothetical wire receipt for the read-only diagnostic above
	if *place {
		fmt.Fprintln(os.Stderr, "REFUSED: the live-order CLI cannot mutate PROD. Use the suite's ARM + LIVE AUTO/server order path so durable risk, recovery, and the kill switch apply.")
		return
	}
	fmt.Println("DRY RUN - nothing sent. PROD mutation is intentionally unavailable in this command.")
}

func trunc(s string, n int) string {
	s = strings.TrimSpace(s)
	if len(s) > n {
		return s[:n] + "..."
	}
	return s
}

// runPolyUSMarkets verifies the Poly US PUBLIC market-data read (no key): it lists live markets from
// gateway.polymarket.us, prints the slugs it can parse, and fetches the BBO for the first one to prove
// live prices read end-to-end. Pass --slug to fetch one market's BBO directly. This is the "set up
// reading from Poly US correctly" check before wiring the dashboard panels + signal mapping.
func runPolyUSMarkets(cfg config.Config, log *slog.Logger, args []string) {
	fs := flag.NewFlagSet("polyus-markets", flag.ExitOnError)
	slug := fs.String("slug", "", "fetch BBO for a specific Poly US market slug")
	query := fs.String("query", "active=true&closed=false&limit=15&orderBy=volumeNum&orderDirection=desc", "raw query for GET /v1/markets")
	rawDump := fs.Bool("raw", false, "also print the raw market JSON (to confirm field shapes)")
	_ = fs.Parse(args)

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	pc := polymarketus.NewPublicClient(15 * time.Second)

	if *slug != "" {
		b, raw, code, err := pc.MarketBBO(ctx, *slug)
		if err != nil {
			log.Error("bbo fetch failed", "err", err)
			os.Exit(1)
		}
		if b != nil {
			fmt.Printf("BBO %s [%d]: bestBid=%.3f bestAsk=%.3f last=%.3f current=%.3f\n", *slug, code, b.BestBid, b.BestAsk, b.LastTradePx, b.CurrentPx)
		}
		fmt.Println(trunc(string(raw), 900))
		return
	}

	mkts, raw, code, err := pc.MarketsList(ctx, *query)
	if err != nil {
		log.Error("markets fetch/parse failed", "err", err)
		if len(raw) > 0 {
			fmt.Println(trunc(string(raw), 1400))
		}
		os.Exit(1)
	}
	fmt.Printf("GET /v1/markets?%s [%d] -> %d markets\n", *query, code, len(mkts))
	for i, m := range mkts {
		if i >= 15 {
			break
		}
		t := m.LongTeam()
		fmt.Printf("  - %-44s | %-20s | %s %s | yes=%.3f | %s | ends %s\n",
			m.Slug, trunc(m.Question, 20), t.League, t.Abbreviation, m.YesPrice(), m.SportsType, m.EndDate)
	}
	if *rawDump {
		fmt.Println("--- raw (truncated) ---")
		fmt.Println(trunc(string(raw), 1800))
	}
	if len(mkts) == 0 {
		fmt.Println("No markets parsed. Try a different --query, or re-run with --raw to inspect the shape.")
		return
	}
	if b, _, bcode, berr := pc.MarketBBO(ctx, mkts[0].Slug); berr == nil && bcode == 200 && b != nil {
		fmt.Printf("BBO sample %s: bestBid=%.3f bestAsk=%.3f last=%.3f\n", mkts[0].Slug, b.BestBid, b.BestAsk, b.LastTradePx)
		fmt.Println("RESULT: OK - reading Poly US markets + live prices works.")
	} else {
		fmt.Printf("BBO sample for %s failed [%d] %v\n", mkts[0].Slug, bcode, berr)
	}
}

// runPolyUSGames verifies the authoritative games feed: GET /v2/leagues/{league}/events?type=sport.
// Lists today's games with live state (score/period) and each game's moneyline prices + teams — the
// exact data the PolyUS live-market section + flow window will render, and what the whale->Poly US
// mapping keys off (team + league + start-time).
func runPolyUSGames(cfg config.Config, log *slog.Logger, args []string) {
	fs := flag.NewFlagSet("polyus-games", flag.ExitOnError)
	league := fs.String("league", "mlb", "league slug: mlb, nba, nfl, nhl, wnba, ...")
	evType := fs.String("type", "sport", "sport (games) or futures")
	rawDump := fs.Bool("raw", false, "also print the raw events JSON")
	_ = fs.Parse(args)

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	pc := polymarketus.NewPublicClient(15 * time.Second)

	events, raw, code, err := pc.LeagueEvents(ctx, *league, *evType)
	if err != nil {
		log.Error("league events fetch/parse failed", "err", err)
		if len(raw) > 0 {
			fmt.Println(trunc(string(raw), 1400))
		}
		os.Exit(1)
	}
	fmt.Printf("GET /v2/leagues/%s/events?type=%s [%d] -> %d events\n", *league, *evType, code, len(events))
	for i, e := range events {
		if i >= 20 {
			break
		}
		state := "upcoming@" + e.StartTime
		if e.Live {
			state = "LIVE " + strings.TrimSpace(e.Score+" "+e.Period)
		} else if e.Ended {
			state = "ended " + e.Score
		}
		ml := ""
		for _, m := range e.Moneylines() {
			t := m.LongTeam()
			ml += fmt.Sprintf(" [%s bid=%.3f ask=%.3f]", t.Abbreviation, m.BestBid, m.BestAsk)
		}
		fmt.Printf("  - %-30s | %d mkts | %s |%s\n", trunc(e.Title, 30), len(e.Markets), state, ml)
	}
	if *rawDump {
		fmt.Println("--- raw (truncated) ---")
		fmt.Println(trunc(string(raw), 1800))
	}
	if len(events) == 0 {
		fmt.Println("No events. Try --type futures, or another --league (nba, nfl, nhl, wnba, fifa-wc).")
		return
	}
	fmt.Println("RESULT: OK - reading Poly US games + live state + moneyline prices.")
}

func runPolyUSBook(cfg config.Config, log *slog.Logger, args []string) {
	fs := flag.NewFlagSet("polyus-book", flag.ExitOnError)
	slug := fs.String("slug", "", "market slug (grab one from polyus-markets / polyus-games)")
	_ = fs.Parse(args)
	if *slug == "" {
		fmt.Fprintln(os.Stderr, "usage: kalshi-suite polyus-book --slug <market-slug>")
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	pc := polymarketus.NewPublicClient(15 * time.Second)
	raw, code, err := pc.Book(ctx, *slug)
	if err != nil {
		log.Error("book fetch failed", "err", err)
		os.Exit(1)
	}
	fmt.Printf("GET /v1/markets/%s/book [%d]\n", *slug, code)
	fmt.Println("--- raw (truncated) ---")
	fmt.Println(trunc(string(raw), 2000))
	state, fb, fo, nb, no := polymarketus.BookSample(raw)
	fmt.Printf("state=%s  bids=%d offers=%d\n", state, nb, no)
	if fb != "" {
		fmt.Println("sample bid level:", fb)
	}
	if fo != "" {
		fmt.Println("sample offer level:", fo)
	}
	if bid, ask, ok := polymarketus.BookDepth(raw); ok {
		imb := 0.0
		if bid+ask > 0 {
			imb = (bid - ask) / (bid + ask)
		}
		fmt.Printf("PARSED depth: bid=%.1f ask=%.1f imbalance=%+.2f (>0 = more resting bids = buy pressure)\n", bid, ask, imb)
		fmt.Println("RESULT: OK - order-book depth parses; ready to wire the Poly US order-flow signal.")
	} else {
		fmt.Println("PARSED depth: couldn't find bids/asks in the shape above - paste me the raw block and I'll lock the parser.")
	}
}

// runTier checks (and with --upgrade, requests) your Kalshi API usage tier. Default shows your current
// limits; `tier --upgrade` requests the permanent Advanced grant (~20/s -> ~30/s). Uses the prod key.
func runTier(cfg config.Config, log *slog.Logger, args []string) {
	fs := flag.NewFlagSet("tier", flag.ExitOnError)
	upgrade := fs.Bool("upgrade", false, "request the permanent Advanced usage-level upgrade")
	_ = fs.Parse(args)

	signer, err := prodSignerFromEnv()
	if err != nil || signer == nil {
		fmt.Fprintln(os.Stderr, "Set KALSHI_PROD_KEY_ID + KALSHI_PROD_KEY_FILE first (the account API needs your prod key).")
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	client := kalshi.NewClient(kalshi.BaseProd, signer, cfg.Kalshi.RateLimitPerSec, time.Duration(cfg.Kalshi.RequestTimeoutMs)*time.Millisecond)

	if *upgrade {
		out, err := client.UpgradeTier(ctx)
		if err != nil {
			fmt.Printf("Upgrade rejected: %v\n", err)
			fmt.Println("Most likely cause: none of your last 100 orders was API-created yet. Place the $1 live order first (arming), then re-run this. The grant is permanent once it goes through.")
			os.Exit(1)
		}
		fmt.Printf("Upgrade response: %v\n", out)
		fmt.Println("RESULT: Advanced requested. Now set \"rate_limit_per_sec\": 30 in config.json and rebuild.")
		return
	}
	out, err := client.GetAccountLimits(ctx)
	if err != nil {
		log.Error("get account limits failed", "err", err)
		os.Exit(1)
	}
	fmt.Printf("Account limits: %v\n", out)
	fmt.Println("To upgrade (after at least one API-created order exists): .\\kalshi-suite.exe tier --upgrade")
}

// runPolyUSWS verifies the Poly US markets WebSocket (the no-REST-limit live feed): authenticates with
// Ed25519, subscribes to live price + taker-flow for today's game slugs, and prints the first raw
// messages so we confirm auth + the wire format before wiring it into serve.
func runPolyUSWS(cfg config.Config, log *slog.Logger, args []string) {
	fs := flag.NewFlagSet("polyus-ws", flag.ExitOnError)
	slugCSV := fs.String("slug", "", "comma-separated market slugs (default: auto from today's MLB games)")
	secs := fs.Int("secs", 20, "seconds to listen")
	_ = fs.Parse(args)

	c, _, err := polyUSLoad(cfg)
	if err != nil || c == nil {
		fmt.Fprintln(os.Stderr, "Set polyus_key_file + POLY_US_KEY_ID (or POLY_US_SECRET(_FILE) env) first (the WS handshake is authenticated).")
		os.Exit(2)
	}
	var slugs []string
	if strings.TrimSpace(*slugCSV) != "" {
		for _, s := range strings.Split(*slugCSV, ",") {
			if s = strings.TrimSpace(s); s != "" {
				slugs = append(slugs, s)
			}
		}
	} else {
		pc := polymarketus.NewPublicClient(15 * time.Second)
		ctx0, cancel0 := context.WithTimeout(context.Background(), 15*time.Second)
		events, _, _, _ := pc.LeagueEvents(ctx0, "mlb", "sport")
		cancel0()
		for _, e := range events {
			if e.Ended {
				continue
			}
			for _, m := range e.Moneylines() {
				slugs = append(slugs, m.Slug)
			}
		}
	}
	if len(slugs) == 0 {
		fmt.Println("No slugs to subscribe (no live MLB games?). Pass --slug aec-mlb-... (grab one from polyus-games).")
		return
	}
	if len(slugs) > 100 {
		slugs = slugs[:100]
	}
	fmt.Printf("Connecting wss://api.polymarket.us/v1/ws/markets (key_id=%s), subscribing %d slugs for %ds...\n", c.KeyID(), len(slugs), *secs)
	ws := c.NewMarketsWS()
	n := 0
	ws.SetOnMsg(func(b []byte) {
		n++
		// Show trade messages in FULL (we need the taker block); lite messages truncated.
		if n <= 24 {
			lim := 360
			if strings.Contains(string(b), "\"trade\"") {
				lim = 900
			}
			fmt.Println(trunc(string(b), lim))
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*secs)*time.Second)
	defer cancel()
	ws.Stream(ctx, func() []string { return slugs })
	fmt.Printf("\nReceived %d messages. Fresh live markets: %d\n", n, ws.Count())
	if n == 0 {
		fmt.Println("RESULT: 0 messages — likely auth rejected (check key/clock ±30s) OR the subscribe shape is wrong (snake/numeric vs camel/string). Paste the output and I'll adjust.")
	} else {
		fmt.Println("RESULT: OK — WS authenticated + streaming. Paste a few sample messages so I lock the parser and wire it into serve.")
	}
}

func runSetCredentials(cfg config.Config, log *slog.Logger, store *storage.Store, args []string) {
	fs := flag.NewFlagSet("set-credentials", flag.ExitOnError)
	keyID := fs.String("key-id", "", "Kalshi API Key ID")
	keyFile := fs.String("key-file", "", "path to the PEM RSA private key file")
	envFlag := fs.String("env", string(cfg.Environment), "environment: demo|prod")
	_ = fs.Parse(args)

	if *keyID == "" || *keyFile == "" {
		fmt.Fprintln(os.Stderr, "usage: kalshi-suite set-credentials --key-id <id> --key-file <path.pem> [--env demo|prod]")
		os.Exit(2)
	}
	pass := os.Getenv("KALSHI_SUITE_PASSPHRASE")
	if pass == "" {
		fmt.Fprintln(os.Stderr, "set KALSHI_SUITE_PASSPHRASE first (it encrypts the key at rest)")
		os.Exit(2)
	}

	pemBytes, err := os.ReadFile(*keyFile)
	if err != nil {
		log.Error("read key file", "err", err)
		os.Exit(1)
	}
	// Validate the key parses before storing anything.
	if _, err := kalshi.NewSigner(*keyID, pemBytes); err != nil {
		log.Error("invalid RSA private key", "err", err)
		os.Exit(1)
	}
	sealed, err := secrets.Seal(pass, pemBytes)
	if err != nil {
		log.Error("encrypt key", "err", err)
		os.Exit(1)
	}

	ctx := context.Background()
	if err := store.SaveCredential(ctx, storage.Credential{
		Environment: *envFlag,
		KeyID:       *keyID,
		Salt:        sealed.Salt,
		Nonce:       sealed.Nonce,
		Ciphertext:  sealed.Ciphertext,
	}); err != nil {
		log.Error("save credential", "err", err)
		os.Exit(1)
	}
	_ = store.Audit(ctx, "info", "auth", "credentials stored (encrypted)", *envFlag)
	fmt.Printf("Stored encrypted credentials for env=%s (key_id=%s).\n", *envFlag, *keyID)
	fmt.Println("The private key was encrypted with your passphrase and never written in plaintext.")
}

func runMarkets(cfg config.Config, log *slog.Logger, store *storage.Store) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	signer, _, _ := loadSigner(ctx, cfg, store) // markets are public; credentials optional
	client := newClient(cfg, signer)

	markets, err := client.GetTopLiquidMarkets(ctx)
	if err != nil {
		log.Error("get markets", "err", err)
		os.Exit(1)
	}
	if len(markets) == 0 {
		fmt.Printf("No liquid markets found (env=%s). On demo this can be sparse; try env=prod.\n", cfg.Environment)
		return
	}
	fmt.Printf("Top liquid live markets (env=%s):\n", cfg.Environment)
	fmt.Printf("%-3s %-28s %8s %11s  %s\n", "#", "ticker", "implied", "vol24h", "title")
	for i, m := range markets {
		if i >= 20 {
			break
		}
		fmt.Printf("%-3d %-28s %7.0f%% %11.0f  %s\n",
			i+1, m.Ticker, m.ImpliedProbability()*100, m.Volume24h.Float(), trimStr(m.Title, 42))
	}
}

func trimStr(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n-1]) + "…"
}

// openBrowser opens url in the user's default browser. R67f: the implementation moved to
// server.OpenBrowser so the new POST /api/openurl endpoint (external links from the WebView2
// dashboard) and this boot path share ONE helper.
func openBrowser(url string) { server.OpenBrowser(url) }

// openAppWindow opens url as a TAB in the user's EXISTING (most-recent) Chrome window — NOT a new
// window (per preference: keep the dashboard + mini-server in the same window you're already in, e.g.
// alongside YouTube). Plain `chrome <url>` reuses the current window and adds a tab. Falls back to the
// default browser if Chrome isn't found. NOTE: a normal tab can't force-close itself, so on stop the
// heartbeat greys the page to a "stopped" banner instead of auto-closing.
func openAppWindow(url string) {
	// R53 (operator: "the app, not the chrome tab, needs to open. make it open by default"): the
	// native kalshi-app.exe window is the default viewport now. -attach = the suite is already
	// booting, so the app must only wait for /health, never spawn a second suite (port fight).
	// KALSHI_APP_SPAWNED=1 means the app launched US — it already owns a window; opening another
	// would loop app→suite→app. Chrome tab and default browser remain as fallbacks only.
	if os.Getenv("KALSHI_APP_SPAWNED") == "1" {
		return
	}
	if runtime.GOOS == "windows" {
		if exe, err := os.Executable(); err == nil {
			app := filepath.Join(filepath.Dir(exe), "kalshi-app.exe")
			if _, err := os.Stat(app); err == nil {
				if exec.Command(app, "-attach").Start() == nil {
					return
				}
			}
		}
		if chrome := chromeExe(); chrome != "" {
			if err := exec.Command(chrome, url).Start(); err == nil { // no --new-window → tab in the current window
				return
			}
		}
	}
	openBrowser(url)
}

// chromeExe returns the Google Chrome executable path on Windows, or "" if not found.
func chromeExe() string {
	if runtime.GOOS != "windows" {
		return ""
	}
	for _, env := range []string{"ProgramFiles", "ProgramFiles(x86)", "LocalAppData"} {
		if b := os.Getenv(env); b != "" {
			p := filepath.Join(b, `Google\Chrome\Application\chrome.exe`)
			if _, err := os.Stat(p); err == nil {
				return p
			}
		}
	}
	return ""
}

// ── DRIVESYNC: cross-PC data portability via a cloud-synced snapshot folder ──────────────────────
// The whole 718MB+ live SQLite DB is far too big for git, so the supported way to carry history/ML to
// another PC is: write a CONSISTENT snapshot (DB + ML state) into cfg.Backup.Dir, point Google Drive
// Desktop at that one folder, and let the suite auto-restore it on a fresh machine. Code/config/docs
// still travel via git.

// backupSnapshotDir returns the configured snapshot folder (default "ml/drive-sync").
func backupSnapshotDir(cfg config.Config) string {
	if d := strings.TrimSpace(cfg.Backup.Dir); d != "" {
		return d
	}
	return filepath.Join("ml", "drive-sync")
}

type finalSnapshotPreparer interface {
	PrepareFinalSnapshot(context.Context) error
}

// backupNowAfterExecutionShadowFence is the final-shutdown backup boundary. The preparer first
// closes HTTP/cash admission, joins the urgent schedulers, drains comparison producers, and fences
// execution-shadow. Periodic snapshots must call backupNow directly because this boundary is
// intentionally permanent.
func backupNowAfterExecutionShadowFence(ctx context.Context, cfg config.Config, store *storage.Store,
	log *slog.Logger, preparer finalSnapshotPreparer) error {
	if preparer == nil {
		return errors.New("final snapshot preparer is unavailable")
	}
	if err := preparer.PrepareFinalSnapshot(ctx); err != nil {
		return fmt.Errorf("suite did not quiesce; snapshot skipped: %w", err)
	}
	return backupNow(ctx, cfg, store, log)
}

const driveSnapshotSafetyMarginBytes uint64 = 1 << 30

type driveSnapshotBackupOps struct {
	availableBytes  func(string) (uint64, error)
	shadowWatermark func(context.Context) (storage.ExecutionShadowWatermark, error)
	backupMain      func(context.Context, string) error
	backupShadow    func(context.Context, string) error
}

func driveSnapshotSourceFileBytes(path string, required bool) (uint64, error) {
	info, err := os.Stat(path)
	if err != nil {
		if !required && errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}
		return 0, fmt.Errorf("stat snapshot source %s: %w", path, err)
	}
	if !info.Mode().IsRegular() || required && info.Size() <= 0 {
		return 0, fmt.Errorf("snapshot source is not a non-empty regular file: %s", path)
	}
	return uint64(info.Size()), nil
}

// requiredDriveSnapshotBytes deliberately uses the live database files plus their current WALs
// instead of guessing from the last backup. Both new SQLite artifacts coexist until the manifest
// is published, and the extra GiB covers journals, manifests, and filesystem accounting.
func requiredDriveSnapshotBytes(dataDir string) (uint64, error) {
	mainPath := filepath.Join(dataDir, "kalshi.db")
	shadowPath := filepath.Join(dataDir, storage.ExecutionShadowFileName)
	sources := []struct {
		path     string
		required bool
	}{
		{mainPath, true},
		{mainPath + "-wal", false},
		{shadowPath, true},
		{shadowPath + "-wal", false},
	}
	required := driveSnapshotSafetyMarginBytes
	for _, source := range sources {
		size, err := driveSnapshotSourceFileBytes(source.path, source.required)
		if err != nil {
			return 0, err
		}
		if size > ^uint64(0)-required {
			return 0, errors.New("snapshot source footprint overflows byte accounting")
		}
		required += size
	}
	return required, nil
}

func cleanupDriveSnapshotTemp(path string) {
	// VACUUM INTO normally uses a rollback journal, but remove every possible SQLite companion.
	// The path is a generation-scoped .tmp, never a published manifest target or legacy snapshot.
	for _, suffix := range []string{"-journal", "-wal", "-shm", ""} {
		_ = os.Remove(path + suffix)
	}
}

// backupNow writes consistent main and execution-shadow DB snapshots (VACUUM INTO, safe while
// running) plus the ML state and a copy of config.json + the mini-server state into the snapshot
// folder. The databases use immutable generation names and their verified manifest is published
// last, so restore can never combine files from two different snapshot runs.
func backupNow(ctx context.Context, cfg config.Config, store *storage.Store, log *slog.Logger) error {
	if store == nil {
		return errors.New("snapshot store is unavailable")
	}
	return backupNowWithOps(ctx, cfg, log, driveSnapshotBackupOps{
		availableBytes:  driveSnapshotAvailableBytes,
		shadowWatermark: store.CaptureExecutionShadowWatermark,
		backupMain:      store.Backup,
		backupShadow:    store.BackupExecutionShadow,
	})
}

func backupNowWithOps(ctx context.Context, cfg config.Config, log *slog.Logger,
	ops driveSnapshotBackupOps) error {
	if ops.availableBytes == nil || ops.shadowWatermark == nil ||
		ops.backupMain == nil || ops.backupShadow == nil {
		return errors.New("snapshot backup operation is incomplete")
	}
	dir := backupSnapshotDir(cfg)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("make snapshot dir: %w", err)
	}
	required, err := requiredDriveSnapshotBytes(cfg.DataDir)
	if err != nil {
		return fmt.Errorf("size snapshot sources: %w", err)
	}
	available, err := ops.availableBytes(dir)
	if err != nil {
		return err
	}
	if available < required {
		return fmt.Errorf(
			"snapshot refused: destination has %d bytes free, needs %d bytes "+
				"(main DB/WAL + execution-shadow DB/WAL + 1 GiB safety margin)",
			available, required)
	}
	// Capture one stable prefix before the main snapshot begins. This read touches only the
	// isolated telemetry database, never the cash database or order path. The later shadow VACUUM
	// may observe newer comparison rows, but they are removed from its unpublished artifact before
	// hashing, so the published pair can never contain a post-main attempt/event.
	shadowWatermark, err := ops.shadowWatermark(ctx)
	if err != nil {
		return fmt.Errorf("capture execution-shadow snapshot cutoff: %w", err)
	}
	generation := newDriveSnapshotGeneration()
	mainName := driveSnapshotMainName(generation)
	shadowName := driveSnapshotShadowName(generation)
	tmp := filepath.Join(dir, mainName+".tmp")
	final := filepath.Join(dir, mainName)
	shadowTmp := filepath.Join(dir, shadowName+".tmp")
	shadowFinal := filepath.Join(dir, shadowName)
	manifestFinal := filepath.Join(dir, driveSnapshotManifestName(generation))
	// Temporary generation files are never selectors, but VACUUM INTO still refuses to overwrite
	// one if a killed process happened to leave the same nanosecond/counter tuple behind.
	cleanupDriveSnapshotTemp(tmp)
	cleanupDriveSnapshotTemp(shadowTmp)
	defer cleanupDriveSnapshotTemp(tmp)
	defer cleanupDriveSnapshotTemp(shadowTmp)
	if err := ops.backupMain(ctx, tmp); err != nil {
		return fmt.Errorf("db snapshot: %w", err)
	}
	// INTEGRITY GATE (audit #5): a snapshot must PROVE it is a well-formed database before it
	// replaces the backup of record — a malformed file that becomes ml/drive-sync/kalshi.db would
	// be faithfully synced to Drive and auto-restored onto a fresh machine.
	if err := storage.QuickCheck(tmp); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("db snapshot FAILED quick_check — previous snapshot kept: %w", err)
	}
	if err := ops.backupShadow(ctx, shadowTmp); err != nil {
		return fmt.Errorf("execution-shadow db snapshot: %w", err)
	}
	if err := storage.TrimExecutionShadowSnapshotToWatermark(
		ctx, shadowTmp, shadowWatermark); err != nil {
		_ = os.Remove(tmp)
		_ = os.Remove(shadowTmp)
		return fmt.Errorf("execution-shadow db snapshot cutoff: %w", err)
	}
	if err := storage.QuickCheck(shadowTmp); err != nil {
		_ = os.Remove(tmp)
		_ = os.Remove(shadowTmp)
		return fmt.Errorf("execution-shadow db snapshot FAILED quick_check; previous snapshot kept: %w", err)
	}
	if err := os.Rename(tmp, final); err != nil {
		_ = os.Remove(shadowTmp)
		return fmt.Errorf("db snapshot rename: %w", err)
	}
	if err := os.Rename(shadowTmp, shadowFinal); err != nil {
		_ = os.Remove(final)
		return fmt.Errorf("execution-shadow db snapshot rename: %w", err)
	}
	cleanupUnpublished := func() {
		_ = os.Remove(final)
		_ = os.Remove(shadowFinal)
	}
	mainArtifact, err := driveSnapshotArtifactFromFile(final, mainName)
	if err != nil {
		cleanupUnpublished()
		return fmt.Errorf("main snapshot manifest evidence: %w", err)
	}
	shadowArtifact, err := driveSnapshotArtifactFromFile(shadowFinal, shadowName)
	if err != nil {
		cleanupUnpublished()
		return fmt.Errorf("execution-shadow snapshot manifest evidence: %w", err)
	}
	manifest := driveSnapshotManifest{
		SchemaVersion:         driveSnapshotManifestVersion,
		Generation:            generation,
		CreatedAt:             time.Now().UTC(),
		Main:                  mainArtifact,
		ExecutionShadow:       shadowArtifact,
		ExecutionShadowCutoff: shadowWatermark,
	}
	if err := writeDriveSnapshotManifestLast(manifestFinal, manifest); err != nil {
		cleanupUnpublished()
		return fmt.Errorf("publish complete snapshot generation: %w", err)
	}
	pruneDriveSnapshotGenerations(dir)
	// Small companions: ML book/predictions (copied only if they parse as JSON — a torn copy in
	// the snapshot dir used to make ALL THREE ML snapshots unloadable), suite settings, mini state.
	_ = copyJSONInto(filepath.Join(cfg.DataDir, "ml_paper.json"), dir)
	_ = copyJSONInto(filepath.Join(cfg.DataDir, "ml_predictions.json"), dir)
	_ = copyFileInto("config.json", dir)
	_ = copyFileInto(filepath.Join("..", "pcrypto-server", "pcrypto-state.json"), dir)
	_ = copyFileInto(filepath.Join("..", "pcrypto-server", "pcrypto-settings.json"), dir)
	_ = os.WriteFile(filepath.Join(dir, "SNAPSHOT.txt"),
		[]byte("last snapshot (local time): "+time.Now().Format("2006-01-02 15:04:05")+
			"\ngeneration: "+generation+
			"\nrestore: keep both generation-named databases and their snapshot manifest together; on an empty data/ the suite verifies and restores the newest complete pair automatically.\n"), 0o644)
	if fi, e := os.Stat(final); e == nil {
		log.Info("data snapshot written", "dir", dir, "generation", generation,
			"db_mb", fmt.Sprintf("%.0f", float64(fi.Size())/(1024*1024)))
	}
	return nil
}

// runBackupLoop snapshots on a timer (cfg.Backup.EveryMinutes). The on-shutdown snapshot in main
// covers the "switch PCs now" case; this is the backstop for hard kills between shutdowns.
func runBackupLoop(ctx context.Context, cfg config.Config, log *slog.Logger, store *storage.Store) {
	every := time.Duration(cfg.Backup.EveryMinutes) * time.Minute
	if every <= 0 {
		return
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			// R101 (auditor r28 bug 231): backupNow used to run on the UNDEADLINED app ctx — one
			// hung Drive-synced copy blocked every future backup AND pinned the WAL reader mark
			// until restart. 10min bounds the whole snapshot (the shutdown path was already 4min).
			bctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
			err := backupNow(bctx, cfg, store, log)
			cancel()
			if err != nil {
				if shutdownQuietErr(ctx, err) { // R72-A #1: shutdown mid-backup — the on-shutdown snapshot (context.Background) still runs
					return
				}
				log.Error("scheduled snapshot failed", "err", err)
				continue
			}
			// R101 (bug 230): the VACUUM-INTO copy pins the WAL reader mark for its whole run —
			// kick a TRUNCATE right after so the piled-up frames fold NOW, not ≤10min later.
			if busy, _, done, terr := store.WALCheckpointTruncate(ctx, 2*time.Second); terr == nil && busy == 0 {
				log.Info("post-backup WAL truncate", "checkpointed", done)
			}
		}
	}
}

// runArchivePTT is the `archive-ptt` subcommand (R124): drain the full resolved->30d ptt backlog
// into kalshi_archive.db right now and exit — the one-off bulk move, run with the suite STOPPED
// (the nightly MonitorRetention pass handles ongoing aging in-suite). Rows are MOVED, never
// deleted (retention_test.go pins the policy); progress prints per million so the operator run
// is observable; the summary lands in the audit trail.
func runArchivePTT(cfg config.Config, log *slog.Logger, store *storage.Store) {
	ctx := context.Background()
	if err := store.EnsureArchive(ctx); err != nil {
		log.Error("archive-ptt: ensure failed", "err", err)
		os.Exit(1)
	}
	start := time.Now()
	total, dedup, lastMark := int64(0), int64(0), int64(0)
	for {
		n, dup, err := store.ArchiveResolvedPTT(ctx, 0, 0) // defaults: 30d cutoff, 20k batch
		if err != nil {
			log.Error("archive-ptt: batch failed (rows already moved stay moved — rerun to resume)", "moved_so_far", total, "err", err)
			os.Exit(1)
		}
		if n == 0 && dup == 0 {
			break
		}
		total += n
		dedup += dup
		if total-lastMark >= 1_000_000 {
			lastMark = total
			fmt.Printf("archive-ptt: %d rows moved (%s elapsed)\n", total, time.Since(start).Round(time.Second))
		}
	}
	archN, _ := store.ArchivePTTCount(ctx)
	_ = store.Audit(ctx, "info", "system",
		fmt.Sprintf("ptt archive (bulk subcommand): moved %d aged resolved rows to %s in %s (+%d re-inserted duplicates reconciled) — archive now %d rows; research tape preserved (moved, never deleted)",
			total, storage.ArchiveFileName, time.Since(start).Round(time.Second), dedup, archN), "")
	fmt.Printf("archive-ptt: DONE — %d rows moved (+%d dupes reconciled) in %s; archive now holds %d rows\n", total, dedup, time.Since(start).Round(time.Second), archN)
}

// runBackupOnce is the `backup` subcommand: write a snapshot right now and exit.
func runBackupOnce(cfg config.Config, log *slog.Logger, store *storage.Store) {
	if err := backupNow(context.Background(), cfg, store, log); err != nil {
		log.Error("backup failed", "err", err)
		os.Exit(1)
	}
	fmt.Println("snapshot written to", backupSnapshotDir(cfg))
}

// runResearchSnapshot is deliberately CLI-only. main checked that the dashboard port was clear
// before opening storage, so the multi-gigabyte backup/hash/quick-check cannot compete with a live
// server's trading or research writers.
func runResearchSnapshot(cfg config.Config, log *slog.Logger, store *storage.Store) {
	mgr, err := researchdr.New(cfg.DataDir, store, 3)
	if err != nil {
		log.Error("research snapshot manager", "err", err)
		os.Exit(1)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	manifest, err := mgr.CreateSnapshot(ctx)
	if err != nil {
		log.Error("research snapshot failed", "err", err)
		os.Exit(1)
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(manifest); err != nil {
		log.Error("print research snapshot manifest", "err", err)
		os.Exit(1)
	}
}

// maybeRestoreFromSnapshot runs BEFORE the DB opens. On a fresh machine (live data/ DB missing or
// empty) where a Drive-synced snapshot exists, it restores the DB + ML state so "clone + run" comes up
// with full history. It NEVER overwrites a non-empty live DB. config.json + mini state come from git,
// so they are not restored here.
func maybeRestoreFromSnapshot(cfg config.Config, log *slog.Logger) error {
	dir := backupSnapshotDir(cfg)
	liveDB := filepath.Join(cfg.DataDir, "kalshi.db")
	liveShadowDB := filepath.Join(cfg.DataDir, storage.ExecutionShadowFileName)
	restorePath := filepath.Join(cfg.DataDir, driveSnapshotRestoreMarker)
	restore, restoring, err := loadDriveSnapshotRestore(restorePath)
	if err != nil {
		return fmt.Errorf("load interrupted snapshot restore: %w", err)
	}
	// A non-empty main DB is ordinary local data only when no restore marker owns it. With a
	// marker, this is the crash cut after both promotions and before cleanup: verify the exact pair
	// without relying on Drive still retaining the source, then finish the interrupted restore.
	if fileNonEmpty(liveDB) {
		if !restoring {
			return nil // already have local data — never clobber it
		}
		if err := finishDriveSnapshotRestore(cfg.DataDir, restore); err != nil {
			return fmt.Errorf("finish interrupted restore generation %s: %w",
				restore.Manifest.Generation, err)
		}
		log.Info("FINISHED interrupted history/execution-shadow restore",
			"generation", restore.Manifest.Generation)
		return nil
	}
	if fileNonEmpty(liveShadowDB) && !restoring {
		return fmt.Errorf("local execution-shadow DB exists while main DB is missing; refusing to mix snapshot generations: %s",
			liveShadowDB)
	}
	if !cfg.Backup.AutoRestore && !restoring {
		return nil
	}
	if !restoring {
		manifest, found, findErr := newestVerifiedDriveSnapshot(dir)
		if findErr != nil {
			return findErr
		}
		if !found {
			return nil // no snapshot material exists
		}
		restore = driveSnapshotRestore{
			SchemaVersion: driveSnapshotRestoreVersion,
			Manifest:      manifest,
		}
	}
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return fmt.Errorf("restore: make data dir: %w", err)
	}
	if !restoring {
		if err := writeDriveSnapshotRestore(restorePath, restore); err != nil {
			return fmt.Errorf("publish restore marker for generation %s: %w",
				restore.Manifest.Generation, err)
		}
	}
	// Both marker-owned staging files are copied and verified before either live name moves. On a
	// restart after shadow-first promotion, staging is rebuilt and the installed shadow is accepted
	// only when its exact hash belongs to this marker.
	if err := stageDriveSnapshotRestore(dir, cfg.DataDir, restore); err != nil {
		return fmt.Errorf("stage restore generation %s: %w", restore.Manifest.Generation, err)
	}
	if err := promoteDriveSnapshotRestore(cfg.DataDir, restore); err != nil {
		return fmt.Errorf("promote restore generation %s: %w", restore.Manifest.Generation, err)
	}
	if err := finishDriveSnapshotRestore(cfg.DataDir, restore); err != nil {
		return fmt.Errorf("finish restore generation %s: %w", restore.Manifest.Generation, err)
	}
	for _, n := range []string{"ml_paper.json", "ml_predictions.json"} {
		_ = copyFile(filepath.Join(dir, n), filepath.Join(cfg.DataDir, n))
	}
	log.Info("RESTORED history/execution-shadow/ML from Drive snapshot (fresh machine)",
		"from", filepath.Join(dir, restore.Manifest.Main.File), "to", liveDB,
		"generation", restore.Manifest.Generation,
		"execution_shadow_restored", true)
	return nil
}

// fileNonEmpty reports whether path exists and has >0 bytes.
func fileNonEmpty(path string) bool {
	fi, err := os.Stat(path)
	return err == nil && !fi.IsDir() && fi.Size() > 0
}

// copyFile streams src→dst (atomic via a .tmp + rename), creating parent dirs. Streamed so the 718MB
// DB never gets loaded fully into RAM.
func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	tmp := dst + ".tmp"
	out, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := out.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	_ = os.Remove(dst)
	return os.Rename(tmp, dst)
}

// copyFileInto copies src into directory dstDir (keeping the base name). Skips silently if src is
// missing so the snapshot of optional companions stays best-effort.
func copyFileInto(src, dstDir string) error {
	if !fileNonEmpty(src) {
		return nil
	}
	return copyFile(src, filepath.Join(dstDir, filepath.Base(src)))
}

// copyJSONInto copies src into dstDir ONLY if its contents parse as JSON (after trimming
// trailing NUL padding from cloud-sync mirrors). A torn read of a live ml_*.json used to be
// copied verbatim into the snapshot dir, making the backup of record unloadable (audit #5).
// The valid bytes are written atomically (tmp+rename), never a raw stream of the live file.
func copyJSONInto(src, dstDir string) error {
	b, err := os.ReadFile(src)
	if err != nil {
		return nil // no file → nothing to snapshot (best-effort companion)
	}
	b = bytes.TrimRight(b, "\x00")
	if len(b) == 0 || !json.Valid(b) {
		return fmt.Errorf("refusing to snapshot %s: not valid JSON (torn write?)", src)
	}
	dst := filepath.Join(dstDir, filepath.Base(src))
	tmp := dst + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, dst)
}
