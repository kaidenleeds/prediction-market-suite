package researchdr

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kalshi-suite/kalshi-suite/internal/researchreplay"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func freshManager(t *testing.T, keep int) (string, *storage.Store, *Manager) {
	t.Helper()
	dir := t.TempDir()
	st, err := storage.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	mgr, err := NewWithOptions(dir, st, keep, Options{AllowFreshMissing: true})
	if err != nil {
		t.Fatal(err)
	}
	return dir, st, mgr
}

func writeCompleteSources(t *testing.T, dir string) *sql.DB {
	t.Helper()
	archive := filepath.Join(dir, storage.ArchiveFileName)
	db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(archive)+"?_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE recovery_sentinel(v TEXT NOT NULL); INSERT INTO recovery_sentinel VALUES('committed-wal-row')`); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}

	replayDir := filepath.Join(dir, "research-replay")
	w, err := researchreplay.Open(researchreplay.Config{Dir: replayDir, MaxSegments: 8, MaxBytes: 8 << 20})
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	frame, err := researchreplay.NewFrame("test-source", "v1", "book", "event-1", "adversarial recovery fixture", map[string]any{"bid": 41, "ask": 43})
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := w.Queue(frame); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if _, wrote, err := w.Flush(context.Background()); err != nil || !wrote {
		_ = db.Close()
		t.Fatalf("replay flush wrote=%v err=%v", wrote, err)
	}
	w.Close()

	files := map[string]string{
		"ml_model_book_v2.pkl":   "opaque-test-model",
		"ml_model_export.json":   `{"model_version":"book-native-v2","weights":[1,2,3]}`,
		"ml_model_history.jsonl": "{\"version\":\"book-native-v2\",\"fit\":1}\n",
		"ml_eval_vectors.json":   `{"rows":1}`,
		"ml_accuracy.json":       `{"auc":0.5}`,
	}
	for name, body := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600); err != nil {
			_ = db.Close()
			t.Fatal(err)
		}
	}
	return db
}

func TestFreshTestSnapshotHasExplicitOptionalMissingStatuses(t *testing.T) {
	dir, st, mgr := freshManager(t, 2)
	if err := st.KVSet(context.Background(), "recovery-test", "present"); err != nil {
		t.Fatal(err)
	}
	manifest, err := mgr.CreateSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !manifest.FreshTestPolicy || !manifest.RequiredComplete || len(manifest.Artifacts) < 6 {
		t.Fatalf("fresh manifest did not make optional omissions explicit: %+v", manifest)
	}
	missing := 0
	for _, artifact := range manifest.Artifacts {
		if artifact.Status == "optional_missing" {
			missing++
			if artifact.Required || artifact.BundlePath != "" {
				t.Fatalf("unsafe missing artifact receipt: %+v", artifact)
			}
		}
	}
	if missing < 5 {
		t.Fatalf("optional missing receipts=%d artifacts=%+v", missing, manifest.Artifacts)
	}
	report := mgr.Verify(manifest.BundleName)
	if report.Error != "" || !report.Exists || !report.HashMatches || !report.SQLiteOK || !report.RestoreDrillOK {
		t.Fatalf("verify=%+v", report)
	}
	plan := mgr.PlanRestore(manifest.BundleName)
	if plan.Automatic || !plan.RequiresStopped || len(plan.Steps) < 6 || plan.Refusal == "" {
		t.Fatalf("plan=%+v", plan)
	}
	if _, err := os.Stat(filepath.Join(dir, "kalshi.db")); err != nil {
		t.Fatalf("live DB was disturbed: %v", err)
	}
}

func TestProductionPreflightFailsBeforePublishingWhenRequiredArtifactsMissing(t *testing.T) {
	dir := t.TempDir()
	st, err := storage.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	mgr, err := New(dir, st, 2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.CreateSnapshot(context.Background()); err == nil || !strings.Contains(err.Error(), "missing required artifacts") {
		t.Fatalf("strict snapshot did not fail closed: %v", err)
	}
	if entries, err := os.ReadDir(filepath.Join(dir, "recovery")); err == nil && len(entries) != 0 {
		t.Fatalf("preflight published partial recovery content: %v", entries)
	}
}

func TestCompleteBundleCapturesArchiveWALReplayAndMLAndDrillsRestoreCopy(t *testing.T) {
	dir := t.TempDir()
	st, err := storage.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	archiveHandle := writeCompleteSources(t, dir)
	defer archiveHandle.Close()
	mgr, err := New(dir, st, 2)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := mgr.CreateSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Version != currentManifestV3 || !manifest.Drill.Passed || manifest.Replay.IncludedSegments != 1 || !manifest.Replay.FullArchive {
		t.Fatalf("manifest=%+v", manifest)
	}
	roles := map[string]int{}
	for _, artifact := range manifest.Artifacts {
		roles[artifact.Role]++
		if artifact.Status == "present" && (artifact.SHA256 == "" || artifact.SizeBytes <= 0 || !strings.HasSuffix(artifact.QuickCheck, "=ok")) {
			t.Fatalf("artifact lacks verification receipt: %+v", artifact)
		}
	}
	for _, role := range []string{"main-db", "execution-shadow-db", "archive-db", "replay-index", "replay-segment", "ml-model", "ml-export", "ml-history"} {
		if roles[role] == 0 {
			t.Fatalf("required role %q missing: %v", role, roles)
		}
	}
	for _, artifact := range manifest.Artifacts {
		if artifact.Role == executionShadowRole && (!artifact.Required || artifact.Status != "present") {
			t.Fatalf("post-isolation shadow artifact is not required and present: %+v", artifact)
		}
	}
	shadowCopy := filepath.Join(dir, "recovery", manifest.BundleName, "database", storage.ExecutionShadowFileName)
	if err := storage.QuickCheck(shadowCopy); err != nil {
		t.Fatalf("execution-shadow recovery artifact failed quick_check: %v", err)
	}
	report := mgr.Verify(manifest.BundleName)
	if report.Error != "" || !report.HashMatches || !report.SQLiteOK || !report.RestoreDrillOK {
		t.Fatalf("complete bundle verify=%+v", report)
	}
	archiveCopy := filepath.Join(dir, "recovery", manifest.BundleName, "database", storage.ArchiveFileName)
	copyDB, err := sql.Open("sqlite", "file:"+filepath.ToSlash(archiveCopy)+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer copyDB.Close()
	var sentinel string
	if err := copyDB.QueryRow(`SELECT v FROM recovery_sentinel`).Scan(&sentinel); err != nil || sentinel != "committed-wal-row" {
		t.Fatalf("archive WAL row missing: value=%q err=%v", sentinel, err)
	}
}

func TestBundlePublicationRetriesTransientWindowsStyleSharingViolation(t *testing.T) {
	root := t.TempDir()
	stage := filepath.Join(root, "snapshot.bundle.building")
	final := filepath.Join(root, "snapshot.bundle")
	if err := os.Mkdir(stage, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stage, "proof"), []byte("complete"), 0o600); err != nil {
		t.Fatal(err)
	}
	attempts := 0
	transient := errors.New("simulated sharing violation")
	rename := func(oldPath, newPath string) error {
		attempts++
		if attempts <= 3 {
			return transient
		}
		return os.Rename(oldPath, newPath)
	}
	if err := renameBundleWithRetry(context.Background(), stage, final, rename); err != nil {
		t.Fatal(err)
	}
	if attempts != 4 {
		t.Fatalf("rename attempts=%d want=4", attempts)
	}
	if body, err := os.ReadFile(filepath.Join(final, "proof")); err != nil ||
		string(body) != "complete" {
		t.Fatalf("published tree body=%q err=%v", body, err)
	}
	if _, err := os.Stat(stage); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("staging tree still exists after publication: %v", err)
	}
}

func TestPostIsolationManifestCannotDowngradeExecutionShadow(t *testing.T) {
	dir, _, mgr := freshManager(t, 2)
	manifest, err := mgr.CreateSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Version != currentManifestV3 {
		t.Fatalf("new snapshot version=%d want=%d", manifest.Version, currentManifestV3)
	}
	bundle, err := mgr.bundleDir(manifest.BundleName)
	if err != nil {
		t.Fatal(err)
	}
	for i := range manifest.Artifacts {
		if manifest.Artifacts[i].Role == executionShadowRole {
			manifest.Artifacts[i].Required = false
		}
	}
	if err := atomicJSON(filepath.Join(bundle, manifestFileName), manifest); err != nil {
		t.Fatal(err)
	}
	report := mgr.Verify(manifest.BundleName)
	if report.Error == "" || !strings.Contains(report.Error, "execution-shadow") {
		t.Fatalf("post-isolation manifest without required shadow verified: %+v", report)
	}
	plan := mgr.PlanRestore(manifest.BundleName)
	if len(plan.Steps) == 0 || !strings.Contains(plan.Steps[0], "Do not restore") {
		t.Fatalf("unsafe post-isolation restore plan: %+v", plan)
	}
	if _, err := os.Stat(filepath.Join(dir, storage.ExecutionShadowFileName)); err != nil {
		t.Fatalf("verification disturbed live shadow database: %v", err)
	}
}

func TestPostIsolationManifestCannotOmitExecutionShadow(t *testing.T) {
	_, _, mgr := freshManager(t, 2)
	manifest, err := mgr.CreateSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	bundle, err := mgr.bundleDir(manifest.BundleName)
	if err != nil {
		t.Fatal(err)
	}
	kept := manifest.Artifacts[:0]
	for _, artifact := range manifest.Artifacts {
		if artifact.Role != executionShadowRole {
			kept = append(kept, artifact)
		}
	}
	manifest.Artifacts = kept
	if err := atomicJSON(filepath.Join(bundle, manifestFileName), manifest); err != nil {
		t.Fatal(err)
	}
	report := mgr.Verify(manifest.BundleName)
	if report.Error == "" || !strings.Contains(report.Error, "execution-shadow") {
		t.Fatalf("post-isolation manifest without shadow verified: %+v", report)
	}
}

func TestLegacyV2WithoutExecutionShadowRemainsExplicitlyCompatible(t *testing.T) {
	_, _, mgr := freshManager(t, 2)
	manifest, err := mgr.CreateSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	bundle, err := mgr.bundleDir(manifest.BundleName)
	if err != nil {
		t.Fatal(err)
	}
	manifest.Version = legacyManifestV2
	kept := manifest.Artifacts[:0]
	for _, artifact := range manifest.Artifacts {
		if artifact.Role == executionShadowRole {
			manifest.SizeBytes -= artifact.SizeBytes
			if err := os.Remove(filepath.Join(bundle, filepath.FromSlash(artifact.BundlePath))); err != nil {
				t.Fatal(err)
			}
			continue
		}
		kept = append(kept, artifact)
	}
	manifest.Artifacts = kept
	if err := atomicJSON(filepath.Join(bundle, manifestFileName), manifest); err != nil {
		t.Fatal(err)
	}
	report := mgr.Verify(manifest.BundleName)
	if report.Error != "" || !report.HashMatches || !report.SQLiteOK || !report.RestoreDrillOK {
		t.Fatalf("legacy v2 bundle no longer verifies: %+v", report)
	}
	if !report.LegacyCompatible || report.ExecutionShadowCovered {
		t.Fatalf("legacy compatibility was not explicit: %+v", report)
	}
	plan := mgr.PlanRestore(manifest.BundleName)
	if len(plan.Steps) < 2 || !strings.Contains(plan.Steps[0], "LEGACY COMPATIBILITY ONLY") ||
		!strings.Contains(strings.Join(plan.Steps, " "), "never combine") {
		t.Fatalf("legacy restore plan did not disclose missing shadow coverage: %+v", plan)
	}
}

func TestTamperedRequiredArtifactFailsClosed(t *testing.T) {
	dir, _, mgr := freshManager(t, 2)
	manifest, err := mgr.CreateSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(dir, "recovery", manifest.BundleName, filepath.FromSlash(manifest.DatabaseFile))
	f, err := os.OpenFile(p, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.Write([]byte("tamper"))
	_ = f.Close()
	report := mgr.Verify(manifest.BundleName)
	if report.Error == "" || report.HashMatches || report.RestoreDrillOK {
		t.Fatalf("tampered report=%+v", report)
	}
	if plan := mgr.PlanRestore(manifest.BundleName); len(plan.Steps) == 0 || plan.Automatic || !strings.Contains(plan.Steps[0], "Do not restore") {
		t.Fatalf("unsafe plan=%+v", plan)
	}
}

func TestReplayBoundFailsClosedInsteadOfSilentlyDroppingAllSegments(t *testing.T) {
	dir := t.TempDir()
	st, err := storage.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	archiveHandle := writeCompleteSources(t, dir)
	defer archiveHandle.Close()
	mgr, err := NewWithOptions(dir, st, 2, Options{MaxReplayBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.CreateSnapshot(context.Background()); err == nil || !strings.Contains(err.Error(), "exceeds recovery bound") {
		t.Fatalf("tiny replay bound did not fail closed: %v", err)
	}
}

func TestVerifyRejectsArtifactPathEscapeWithoutTouchingOutside(t *testing.T) {
	dir, _, mgr := freshManager(t, 2)
	name := bundlePrefix + "20260712T000000.000000000Z" + bundleSuffix
	bundle := filepath.Join(mgr.root, name)
	if err := os.MkdirAll(bundle, 0o700); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(dir, "outside")
	if err := os.WriteFile(outside, []byte("must-survive"), 0o600); err != nil {
		t.Fatal(err)
	}
	manifest := Manifest{Version: 2, BundleName: name, RequiredComplete: true, QuickCheck: "ok",
		Artifacts: []Artifact{{Role: "main-db", BundlePath: "../../outside", Required: true, Status: "present", SizeBytes: 12, SHA256: strings.Repeat("a", 64), QuickCheck: "sqlite=ok"}}}
	if err := atomicJSON(filepath.Join(bundle, manifestFileName), manifest); err != nil {
		t.Fatal(err)
	}
	report := mgr.Verify(name)
	if report.Error == "" || report.HashMatches {
		t.Fatalf("path escape verified: %+v", report)
	}
	if got, err := os.ReadFile(outside); err != nil || string(got) != "must-survive" {
		t.Fatalf("outside file changed: %q err=%v", got, err)
	}
}

func TestRotationBoundsBundlesAndNeverEscapesRecoveryDirectory(t *testing.T) {
	for _, keep := range []int{1, 2} {
		t.Run(string(rune('0'+keep)), func(t *testing.T) {
			dir, _, mgr := freshManager(t, keep)
			if err := os.MkdirAll(mgr.root, 0o700); err != nil {
				t.Fatal(err)
			}
			outside := filepath.Join(dir, bundlePrefix+"outside"+bundleSuffix)
			if err := os.WriteFile(outside, []byte("outside-must-survive"), 0o600); err != nil {
				t.Fatal(err)
			}
			for _, stamp := range []string{"0001", "0002", "0003"} {
				if err := os.Mkdir(filepath.Join(mgr.root, bundlePrefix+stamp+bundleSuffix), 0o700); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.WriteFile(filepath.Join(mgr.root, "unrelated.db"), []byte("keep"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Mkdir(filepath.Join(mgr.root, bundlePrefix+"stale"+bundleSuffix+buildingSuffix), 0o700); err != nil {
				t.Fatal(err)
			}
			mgr.cleanupBuildingLocked()
			if err := mgr.rotateLocked(); err != nil {
				t.Fatal(err)
			}
			entries, err := os.ReadDir(mgr.root)
			if err != nil {
				t.Fatal(err)
			}
			retained := 0
			for _, entry := range entries {
				if entry.IsDir() && validBundleName(entry.Name()) {
					retained++
				}
				if strings.HasSuffix(entry.Name(), buildingSuffix) {
					t.Fatalf("stale building dir survived cleanup: %s", entry.Name())
				}
			}
			if retained != keep {
				t.Fatalf("retained=%d want=%d entries=%v", retained, keep, entries)
			}
			if got, err := os.ReadFile(outside); err != nil || string(got) != "outside-must-survive" {
				t.Fatalf("rotation touched outside file: %q err=%v", got, err)
			}
			if _, err := os.Stat(filepath.Join(mgr.root, "unrelated.db")); err != nil {
				t.Fatalf("rotation removed unrelated file: %v", err)
			}
		})
	}
}

func TestListIsBoundedReceiptAndCorruptManifestIsReported(t *testing.T) {
	_, _, mgr := freshManager(t, 2)
	manifest, err := mgr.CreateSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	rows, err := mgr.List()
	if err != nil || len(rows) != 1 || rows[0].VerificationMode != "immutable-creation-receipt" || !rows[0].RestoreDrillOK {
		t.Fatalf("receipt list rows=%+v err=%v", rows, err)
	}
	bundle, _ := mgr.bundleDir(manifest.BundleName)
	if err := os.WriteFile(filepath.Join(bundle, manifestFileName), []byte(`{"version":2}`), 0o600); err != nil {
		t.Fatal(err)
	}
	rows, err = mgr.List()
	if err != nil || len(rows) != 1 || rows[0].Error == "" || rows[0].Exists {
		t.Fatalf("corrupt manifest rows=%+v err=%v", rows, err)
	}
	// Ensure malformed JSON is also a clean error rather than a panic.
	if err := os.WriteFile(filepath.Join(bundle, manifestFileName), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	rows, err = mgr.List()
	if err != nil || len(rows) != 1 || rows[0].Error == "" {
		t.Fatalf("empty manifest rows=%+v err=%v", rows, err)
	}
}

func TestManifestJSONRoundTripKeepsNoAbsoluteSourcePaths(t *testing.T) {
	_, _, mgr := freshManager(t, 2)
	manifest, err := mgr.CreateSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), mgr.dataDir) || strings.Contains(string(b), `C:\`) {
		t.Fatalf("manifest leaked absolute source path: %s", b)
	}
}
