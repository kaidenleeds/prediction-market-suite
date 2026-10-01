package main

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/config"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

type driveBackupTestFencer struct {
	store   *storage.Store
	attempt storage.ExecutionShadowAttempt
	err     error
	called  bool
}

func (f *driveBackupTestFencer) PrepareFinalSnapshot(ctx context.Context) error {
	f.called = true
	if f.err != nil {
		return f.err
	}
	inserted, err := f.store.InsertExecutionShadowAttempt(ctx, f.attempt)
	if err != nil {
		return err
	}
	if !inserted {
		return errors.New("fence fixture was not inserted")
	}
	return nil
}

func driveBackupTestAttempt(id string, observed time.Time) storage.ExecutionShadowAttempt {
	return storage.ExecutionShadowAttempt{
		AttemptID: id, SignalDecisionID: id + "-decision",
		ObservedAt: observed, TriggerUnixMS: observed.UnixMilli(), Venue: "kalshi",
		Ticker: "KX-DRIVE-BACKUP", Title: "Drive backup preserves comparison history",
		Side: "YES", Action: "BUY", SystemID: "spotlag", Route: "taker",
		SignalSource: "backup-test", InputTopology: "test->kalshi",
		SignalContract: "backup-test-v1", InputObservedAt: observed,
		SignalPrice: .42, QualificationBasis: "backup integration fixture",
	}
}

func seedDriveBackupShadowAttemptEvent(t *testing.T, store *storage.Store, id string,
	observed time.Time) {
	t.Helper()
	attempt := driveBackupTestAttempt(id, observed)
	if inserted, err := store.InsertExecutionShadowAttempt(t.Context(), attempt); err != nil ||
		!inserted {
		t.Fatalf("seed %s attempt inserted=%v err=%v", id, inserted, err)
	}
	eventAt := observed.Add(time.Millisecond)
	event := storage.ExecutionShadowEvent{
		EventID: id + "-event", AttemptID: id, At: eventAt,
		ElapsedFromTriggerMS: eventAt.UnixMilli() - attempt.TriggerUnixMS,
		Stage:                "snapshot-test", Outcome: "recorded",
		Evidence: map[string]any{"fixture": id},
	}
	if inserted, err := store.AppendExecutionShadowEvent(t.Context(), event); err != nil ||
		!inserted {
		t.Fatalf("seed %s event inserted=%v err=%v", id, inserted, err)
	}
}

func driveBackupTestConfig(root string) config.Config {
	return config.Config{
		DataDir: filepath.Join(root, "data"),
		Backup: config.BackupConfig{
			Enabled: true, Dir: filepath.Join(root, "drive"), AutoRestore: true,
		},
	}
}

func writeDriveSnapshotFootprint(t *testing.T, cfg config.Config) uint64 {
	t.Helper()
	if err := os.MkdirAll(cfg.DataDir, 0o700); err != nil {
		t.Fatal(err)
	}
	files := []struct {
		name string
		size int
	}{
		{"kalshi.db", 29},
		{"kalshi.db-wal", 17},
		{storage.ExecutionShadowFileName, 13},
		{storage.ExecutionShadowFileName + "-wal", 7},
	}
	var sourceBytes uint64
	for _, file := range files {
		if err := os.WriteFile(filepath.Join(cfg.DataDir, file.name),
			make([]byte, file.size), 0o600); err != nil {
			t.Fatal(err)
		}
		sourceBytes += uint64(file.size)
	}
	return sourceBytes + driveSnapshotSafetyMarginBytes
}

func writeValidLegacyDriveSnapshot(t *testing.T, dir string) string {
	t.Helper()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "kalshi.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE legacy_snapshot_marker(value TEXT NOT NULL);
INSERT INTO legacy_snapshot_marker(value) VALUES('preserve me')`); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

func assertNoUnpublishedSnapshotTemps(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".tmp") ||
			strings.HasSuffix(entry.Name(), ".tmp-journal") {
			t.Fatalf("unpublished snapshot temporary survived: %s", entry.Name())
		}
	}
}

func assertLegacyDriveSnapshotPreserved(t *testing.T, path string) {
	t.Helper()
	if err := storage.QuickCheck(path); err != nil {
		t.Fatalf("valid legacy snapshot was deleted or damaged: %v", err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var marker string
	if err := db.QueryRow(`SELECT value FROM legacy_snapshot_marker`).Scan(&marker); err != nil {
		t.Fatalf("read legacy snapshot marker: %v", err)
	}
	if marker != "preserve me" {
		t.Fatalf("legacy snapshot marker=%q", marker)
	}
}

func TestDriveSnapshotRefusesInsufficientDestinationSpaceBeforeBackup(t *testing.T) {
	cfg := driveBackupTestConfig(t.TempDir())
	required := writeDriveSnapshotFootprint(t, cfg)
	legacy := writeValidLegacyDriveSnapshot(t, cfg.Backup.Dir)
	mainCalled := false
	shadowCalled := false
	err := backupNowWithOps(t.Context(), cfg,
		slog.New(slog.NewTextHandler(io.Discard, nil)), driveSnapshotBackupOps{
			availableBytes: func(string) (uint64, error) { return required - 1, nil },
			shadowWatermark: func(context.Context) (storage.ExecutionShadowWatermark, error) {
				t.Fatal("insufficient-space preflight captured a shadow watermark")
				return storage.ExecutionShadowWatermark{}, nil
			},
			backupMain: func(context.Context, string) error {
				mainCalled = true
				return nil
			},
			backupShadow: func(context.Context, string) error {
				shadowCalled = true
				return nil
			},
		})
	if err == nil || !strings.Contains(err.Error(), "snapshot refused") ||
		!strings.Contains(err.Error(), "1 GiB safety margin") {
		t.Fatalf("insufficient-space error=%v", err)
	}
	if mainCalled || shadowCalled {
		t.Fatalf("insufficient-space preflight started backup main=%v shadow=%v",
			mainCalled, shadowCalled)
	}
	assertNoUnpublishedSnapshotTemps(t, cfg.Backup.Dir)
	assertLegacyDriveSnapshotPreserved(t, legacy)
}

func TestDriveSnapshotFailedMainBackupCleansTempAndJournal(t *testing.T) {
	cfg := driveBackupTestConfig(t.TempDir())
	_ = writeDriveSnapshotFootprint(t, cfg)
	legacy := writeValidLegacyDriveSnapshot(t, cfg.Backup.Dir)
	mainCalled := false
	shadowCalled := false
	err := backupNowWithOps(t.Context(), cfg,
		slog.New(slog.NewTextHandler(io.Discard, nil)), driveSnapshotBackupOps{
			availableBytes: func(string) (uint64, error) { return ^uint64(0), nil },
			shadowWatermark: func(context.Context) (storage.ExecutionShadowWatermark, error) {
				return storage.ExecutionShadowWatermark{CapturedAt: time.Now().UTC()}, nil
			},
			backupMain: func(_ context.Context, path string) error {
				mainCalled = true
				if err := os.WriteFile(path, []byte("partial main"), 0o600); err != nil {
					return err
				}
				if err := os.WriteFile(path+"-journal", []byte("partial journal"), 0o600); err != nil {
					return err
				}
				return errors.New("database or disk is full (13)")
			},
			backupShadow: func(context.Context, string) error {
				shadowCalled = true
				return nil
			},
		})
	if err == nil || !strings.Contains(err.Error(), "database or disk is full") {
		t.Fatalf("failed-main error=%v", err)
	}
	if !mainCalled || shadowCalled {
		t.Fatalf("failed-main flow main=%v shadow=%v", mainCalled, shadowCalled)
	}
	assertNoUnpublishedSnapshotTemps(t, cfg.Backup.Dir)
	assertLegacyDriveSnapshotPreserved(t, legacy)
}

func TestAwaitServeExitCancelsRuntimeOnUnexpectedServerReturn(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	wantErr := errors.New("listener failed")
	errCh <- wantErr
	reason, fatal, gotErr := awaitServeExitAndCancel(ctx, errCh, cancel)
	if reason != "server_error" || !fatal || !errors.Is(gotErr, wantErr) {
		t.Fatalf("exit classification=%q fatal=%v err=%v", reason, fatal, gotErr)
	}
	if ctx.Err() != context.Canceled {
		t.Fatalf("unexpected server exit left runtime context live: %v", ctx.Err())
	}
}

func closeAndRemoveLiveSnapshotPair(t *testing.T, store *storage.Store, dataDir string) {
	t.Helper()
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"kalshi.db", storage.ExecutionShadowFileName} {
		if err := os.Remove(filepath.Join(dataDir, name)); err != nil {
			t.Fatalf("remove live %s: %v", name, err)
		}
	}
}

func TestDriveSnapshotBacksUpAndFreshRestoresVerifiedGenerationPair(t *testing.T) {
	cfg := driveBackupTestConfig(t.TempDir())
	if err := os.MkdirAll(cfg.DataDir, 0o700); err != nil {
		t.Fatal(err)
	}
	store, err := storage.Open(cfg.DataDir)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	attempt := driveBackupTestAttempt("drive-backup-shadow-attempt", now)
	if inserted, err := store.InsertExecutionShadowAttempt(t.Context(), attempt); err != nil || !inserted {
		_ = store.Close()
		t.Fatalf("seed execution-shadow attempt inserted=%v err=%v", inserted, err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	if err := backupNow(t.Context(), cfg, store, log); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	manifest, found, err := newestVerifiedDriveSnapshot(cfg.Backup.Dir)
	if err != nil || !found {
		_ = store.Close()
		t.Fatalf("published generation found=%v err=%v", found, err)
	}
	if manifest.Main.File == "kalshi.db" ||
		manifest.ExecutionShadow.File == storage.ExecutionShadowFileName {
		_ = store.Close()
		t.Fatalf("snapshot did not use immutable generation names: %+v", manifest)
	}
	if _, err := os.Stat(filepath.Join(cfg.Backup.Dir,
		driveSnapshotManifestName(manifest.Generation))); err != nil {
		_ = store.Close()
		t.Fatalf("published manifest missing: %v", err)
	}
	closeAndRemoveLiveSnapshotPair(t, store, cfg.DataDir)

	if err := maybeRestoreFromSnapshot(cfg, log); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"kalshi.db", storage.ExecutionShadowFileName} {
		if err := storage.QuickCheck(filepath.Join(cfg.DataDir, name)); err != nil {
			t.Fatalf("restored %s failed quick_check: %v", name, err)
		}
	}
	restoredStore, err := storage.Open(cfg.DataDir)
	if err != nil {
		t.Fatal(err)
	}
	defer restoredStore.Close()
	views, err := restoredStore.ListExecutionShadowAttempts(t.Context(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(views) != 1 || views[0].Attempt.AttemptID != attempt.AttemptID {
		t.Fatalf("restored execution-shadow history=%+v", views)
	}
}

func TestPeriodicDriveSnapshotCutsOffShadowRowsWrittenAfterMainBoundary(t *testing.T) {
	cfg := driveBackupTestConfig(t.TempDir())
	if err := os.MkdirAll(cfg.DataDir, 0o700); err != nil {
		t.Fatal(err)
	}
	store, err := storage.Open(cfg.DataDir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Now().UTC().Truncate(time.Millisecond)
	const beforeID = "periodic-before-main"
	const afterID = "periodic-after-main"
	seedDriveBackupShadowAttemptEvent(t, store, beforeID, now)

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	if err := backupNowWithOps(t.Context(), cfg, log, driveSnapshotBackupOps{
		availableBytes:  func(string) (uint64, error) { return ^uint64(0), nil },
		shadowWatermark: store.CaptureExecutionShadowWatermark,
		backupMain: func(ctx context.Context, path string) error {
			if err := store.Backup(ctx, path); err != nil {
				return err
			}
			// Deterministically model telemetry accepted after the main DB's SQLite snapshot.
			// The later shadow VACUUM sees it; the published artifact must not.
			seedDriveBackupShadowAttemptEvent(t, store, afterID, now.Add(time.Second))
			return nil
		},
		backupShadow: store.BackupExecutionShadow,
	}); err != nil {
		t.Fatal(err)
	}
	manifest, found, err := newestVerifiedDriveSnapshot(cfg.Backup.Dir)
	if err != nil || !found {
		t.Fatalf("published generation found=%v err=%v", found, err)
	}
	if manifest.SchemaVersion != driveSnapshotManifestVersion ||
		manifest.ExecutionShadowCutoff.AttemptSequence != 1 ||
		manifest.ExecutionShadowCutoff.EventRowID != 1 {
		t.Fatalf("unexpected periodic cutoff manifest: %+v", manifest)
	}
	shadowDB, err := sql.Open("sqlite",
		filepath.Join(cfg.Backup.Dir, manifest.ExecutionShadow.File))
	if err != nil {
		t.Fatal(err)
	}
	defer shadowDB.Close()
	for id, want := range map[string]int{beforeID: 1, afterID: 0} {
		var attempts, events int
		if err := shadowDB.QueryRowContext(t.Context(),
			`SELECT COUNT(*) FROM execution_shadow_attempts WHERE attempt_id=?`, id,
		).Scan(&attempts); err != nil {
			t.Fatal(err)
		}
		if err := shadowDB.QueryRowContext(t.Context(),
			`SELECT COUNT(*) FROM execution_shadow_events WHERE attempt_id=?`, id,
		).Scan(&events); err != nil {
			t.Fatal(err)
		}
		if attempts != want || events != want {
			t.Fatalf("published %s attempts=%d events=%d want=%d",
				id, attempts, events, want)
		}
	}
	lossyCutoff := manifest.ExecutionShadowCutoff
	lossyCutoff.AttemptSequence++
	lossyCutoff.EventRowID++
	if err := storage.ValidateExecutionShadowSnapshotWatermark(
		t.Context(),
		filepath.Join(cfg.Backup.Dir, manifest.ExecutionShadow.File),
		lossyCutoff,
	); err == nil || !strings.Contains(err.Error(), "does not equal watermark prefix") {
		t.Fatalf("validator accepted a shadow artifact missing its declared endpoint: %v", err)
	}
	var liveAfter int
	if err := store.ExecutionShadowDBForTest().QueryRowContext(t.Context(),
		`SELECT COUNT(*) FROM execution_shadow_attempts WHERE attempt_id=?`, afterID,
	).Scan(&liveAfter); err != nil {
		t.Fatal(err)
	}
	if liveAfter != 1 {
		t.Fatalf("snapshot cutoff mutated live telemetry: count=%d", liveAfter)
	}
}

func TestFinalDriveSnapshotFencesAcceptedShadowTailBeforeBackup(t *testing.T) {
	cfg := driveBackupTestConfig(t.TempDir())
	if err := os.MkdirAll(cfg.DataDir, 0o700); err != nil {
		t.Fatal(err)
	}
	store, err := storage.Open(cfg.DataDir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	fencer := &driveBackupTestFencer{
		store: store,
		attempt: driveBackupTestAttempt("accepted-at-final-fence",
			time.Now().UTC().Truncate(time.Microsecond)),
	}
	if err := backupNowAfterExecutionShadowFence(t.Context(), cfg, store, log, fencer); err != nil {
		t.Fatal(err)
	}
	if !fencer.called {
		t.Fatal("final snapshot did not call the suite quiesce/fence boundary")
	}
	manifest, found, err := newestVerifiedDriveSnapshot(cfg.Backup.Dir)
	if err != nil || !found {
		t.Fatalf("published generation found=%v err=%v", found, err)
	}
	// Inspect the exact immutable shadow artifact selected by the published manifest.
	shadowDB, err := sql.Open("sqlite",
		filepath.Join(cfg.Backup.Dir, manifest.ExecutionShadow.File))
	if err != nil {
		t.Fatal(err)
	}
	defer shadowDB.Close()
	var count int
	if err := shadowDB.QueryRowContext(t.Context(),
		`SELECT COUNT(*) FROM execution_shadow_attempts WHERE attempt_id=?`,
		fencer.attempt.AttemptID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("final snapshot omitted accepted pre-fence tail: count=%d", count)
	}
}

func TestDriveSnapshotRestoreFallsBackToPreviousCompletePair(t *testing.T) {
	cfg := driveBackupTestConfig(t.TempDir())
	if err := os.MkdirAll(cfg.DataDir, 0o700); err != nil {
		t.Fatal(err)
	}
	store, err := storage.Open(cfg.DataDir)
	if err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	now := time.Now().UTC().Truncate(time.Microsecond)
	first := driveBackupTestAttempt("first-generation-attempt", now)
	if inserted, err := store.InsertExecutionShadowAttempt(t.Context(), first); err != nil || !inserted {
		t.Fatalf("seed first inserted=%v err=%v", inserted, err)
	}
	if err := backupNow(t.Context(), cfg, store, log); err != nil {
		t.Fatal(err)
	}
	firstManifest, found, err := newestVerifiedDriveSnapshot(cfg.Backup.Dir)
	if err != nil || !found {
		t.Fatalf("first generation found=%v err=%v", found, err)
	}

	second := driveBackupTestAttempt("second-generation-attempt", now.Add(time.Millisecond))
	if inserted, err := store.InsertExecutionShadowAttempt(t.Context(), second); err != nil || !inserted {
		t.Fatalf("seed second inserted=%v err=%v", inserted, err)
	}
	if err := backupNow(t.Context(), cfg, store, log); err != nil {
		t.Fatal(err)
	}
	latest, found, err := newestVerifiedDriveSnapshot(cfg.Backup.Dir)
	if err != nil || !found || latest.Generation == firstManifest.Generation {
		t.Fatalf("latest generation=%q first=%q found=%v err=%v",
			latest.Generation, firstManifest.Generation, found, err)
	}
	if err := os.WriteFile(filepath.Join(cfg.Backup.Dir, latest.ExecutionShadow.File),
		[]byte("corrupt newest shadow"), 0o600); err != nil {
		t.Fatal(err)
	}
	closeAndRemoveLiveSnapshotPair(t, store, cfg.DataDir)

	if err := maybeRestoreFromSnapshot(cfg, log); err != nil {
		t.Fatalf("restore did not fall back to previous complete pair: %v", err)
	}
	restored, err := storage.Open(cfg.DataDir)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	views, err := restored.ListExecutionShadowAttempts(t.Context(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(views) != 1 || views[0].Attempt.AttemptID != first.AttemptID {
		t.Fatalf("restore mixed generations or used corrupt newest pair: %+v", views)
	}
}

func TestSelectedSnapshotFailureReturnsErrorWithoutCreatingMainDB(t *testing.T) {
	cfg := driveBackupTestConfig(t.TempDir())
	if err := os.MkdirAll(cfg.DataDir, 0o700); err != nil {
		t.Fatal(err)
	}
	store, err := storage.Open(cfg.DataDir)
	if err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	now := time.Now().UTC().Truncate(time.Microsecond)
	attempt := driveBackupTestAttempt("failed-restore-attempt", now)
	if inserted, err := store.InsertExecutionShadowAttempt(t.Context(), attempt); err != nil || !inserted {
		t.Fatalf("seed inserted=%v err=%v", inserted, err)
	}
	if err := backupNow(t.Context(), cfg, store, log); err != nil {
		t.Fatal(err)
	}
	manifest, found, err := newestVerifiedDriveSnapshot(cfg.Backup.Dir)
	if err != nil || !found {
		t.Fatalf("generation found=%v err=%v", found, err)
	}
	shadowPath := filepath.Join(cfg.Backup.Dir, manifest.ExecutionShadow.File)
	goodShadow, err := os.ReadFile(shadowPath)
	if err != nil {
		t.Fatal(err)
	}
	closeAndRemoveLiveSnapshotPair(t, store, cfg.DataDir)
	if err := os.WriteFile(shadowPath, []byte("not sqlite"), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := maybeRestoreFromSnapshot(cfg, log); err == nil {
		t.Fatal("corrupt selected generation did not return a startup-blocking error")
	}
	if fileNonEmpty(filepath.Join(cfg.DataDir, "kalshi.db")) ||
		fileNonEmpty(filepath.Join(cfg.DataDir, storage.ExecutionShadowFileName)) {
		t.Fatal("failed restore poisoned the fresh data directory")
	}

	// Because the failed call did not create storage, repairing cloud sync allows the next boot to
	// retry the same generation instead of being suppressed by a blank kalshi.db sentinel.
	if err := os.WriteFile(shadowPath, goodShadow, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := maybeRestoreFromSnapshot(cfg, log); err != nil {
		t.Fatalf("repaired snapshot was not retryable: %v", err)
	}
	if !fileNonEmpty(filepath.Join(cfg.DataDir, "kalshi.db")) {
		t.Fatal("successful retry did not install the main database")
	}
}

func TestAutoRestoreDisabledStillRefusesOrphanExecutionShadow(t *testing.T) {
	for _, tc := range []struct {
		name       string
		shadowData []byte
		wantErr    bool
	}{
		{name: "non-empty shadow refuses mixed generation", shadowData: []byte("old shadow"), wantErr: true},
		{name: "empty fresh directory remains allowed", shadowData: nil, wantErr: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := driveBackupTestConfig(t.TempDir())
			cfg.Backup.AutoRestore = false
			if err := os.MkdirAll(cfg.DataDir, 0o700); err != nil {
				t.Fatal(err)
			}
			if tc.shadowData != nil {
				if err := os.WriteFile(filepath.Join(
					cfg.DataDir, storage.ExecutionShadowFileName), tc.shadowData, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			log := slog.New(slog.NewTextHandler(io.Discard, nil))
			err := maybeRestoreFromSnapshot(cfg, log)
			if tc.wantErr {
				if err == nil || !strings.Contains(err.Error(), "refusing to mix snapshot generations") {
					t.Fatalf("orphan shadow error=%v", err)
				}
			} else if err != nil {
				t.Fatalf("fresh AutoRestore=false startup refused: %v", err)
			}
			if fileNonEmpty(filepath.Join(cfg.DataDir, "kalshi.db")) {
				t.Fatal("pre-open restore check created a main DB")
			}
		})
	}
}

func prepareInterruptedDriveRestore(t *testing.T) (config.Config, driveSnapshotRestore,
	storage.ExecutionShadowAttempt, *slog.Logger) {
	t.Helper()
	cfg := driveBackupTestConfig(t.TempDir())
	if err := os.MkdirAll(cfg.DataDir, 0o700); err != nil {
		t.Fatal(err)
	}
	store, err := storage.Open(cfg.DataDir)
	if err != nil {
		t.Fatal(err)
	}
	attempt := driveBackupTestAttempt("interrupted-restore-attempt",
		time.Now().UTC().Truncate(time.Microsecond))
	if inserted, insertErr := store.InsertExecutionShadowAttempt(
		t.Context(), attempt); insertErr != nil || !inserted {
		_ = store.Close()
		t.Fatalf("seed interrupted restore inserted=%v err=%v", inserted, insertErr)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	if err := backupNow(t.Context(), cfg, store, log); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	manifest, found, err := newestVerifiedDriveSnapshot(cfg.Backup.Dir)
	if err != nil || !found {
		_ = store.Close()
		t.Fatalf("interrupted restore generation found=%v err=%v", found, err)
	}
	closeAndRemoveLiveSnapshotPair(t, store, cfg.DataDir)
	restore := driveSnapshotRestore{
		SchemaVersion: driveSnapshotRestoreVersion,
		Manifest:      manifest,
	}
	if err := writeDriveSnapshotRestore(
		filepath.Join(cfg.DataDir, driveSnapshotRestoreMarker), restore); err != nil {
		t.Fatal(err)
	}
	if err := stageDriveSnapshotRestore(cfg.Backup.Dir, cfg.DataDir, restore); err != nil {
		t.Fatal(err)
	}
	return cfg, restore, attempt, log
}

func assertInterruptedDriveRestoreCompleted(t *testing.T, cfg config.Config,
	attempt storage.ExecutionShadowAttempt) {
	t.Helper()
	if _, err := os.Stat(filepath.Join(cfg.DataDir, driveSnapshotRestoreMarker)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("completed restore left its in-progress marker or unreadable state: %v", err)
	}
	for _, name := range []string{"kalshi.db", storage.ExecutionShadowFileName} {
		if err := storage.QuickCheck(filepath.Join(cfg.DataDir, name)); err != nil {
			t.Fatalf("resumed restore %s failed quick_check: %v", name, err)
		}
	}
	restored, err := storage.Open(cfg.DataDir)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	views, err := restored.ListExecutionShadowAttempts(t.Context(), 10)
	if err != nil || len(views) != 1 || views[0].Attempt.AttemptID != attempt.AttemptID {
		t.Fatalf("resumed restore history=%+v err=%v", views, err)
	}
}

func TestDriveSnapshotRestoreResumesCrashAfterShadowPromotion(t *testing.T) {
	cfg, restore, attempt, log := prepareInterruptedDriveRestore(t)
	if err := promoteDriveSnapshotRestoreArtifact(cfg.DataDir,
		driveSnapshotRestoreStageShadow(restore.Manifest.Generation),
		storage.ExecutionShadowFileName, restore.Manifest.ExecutionShadow); err != nil {
		t.Fatal(err)
	}
	if fileNonEmpty(filepath.Join(cfg.DataDir, "kalshi.db")) {
		t.Fatal("shadow-first crash fixture unexpectedly published the main sentinel")
	}
	// The marker-owned main stage plus installed shadow are sufficient to resume even if cloud sync
	// removes the source generation before the next process starts.
	for _, name := range []string{restore.Manifest.Main.File, restore.Manifest.ExecutionShadow.File} {
		if err := os.Remove(filepath.Join(cfg.Backup.Dir, name)); err != nil {
			t.Fatal(err)
		}
	}

	if err := maybeRestoreFromSnapshot(cfg, log); err != nil {
		t.Fatalf("restore did not resume after shadow-first crash cut: %v", err)
	}
	assertInterruptedDriveRestoreCompleted(t, cfg, attempt)
}

func TestDriveSnapshotRestoreFinishesCrashAfterMainSentinelPromotion(t *testing.T) {
	cfg, restore, attempt, log := prepareInterruptedDriveRestore(t)
	if err := promoteDriveSnapshotRestore(cfg.DataDir, restore); err != nil {
		t.Fatal(err)
	}
	if !fileNonEmpty(filepath.Join(cfg.DataDir, "kalshi.db")) ||
		!fileNonEmpty(filepath.Join(cfg.DataDir, storage.ExecutionShadowFileName)) {
		t.Fatal("post-promotion crash fixture did not install the complete pair")
	}
	if !fileNonEmpty(filepath.Join(cfg.DataDir, driveSnapshotRestoreMarker)) {
		t.Fatal("post-promotion crash fixture lost its in-progress marker")
	}
	for _, name := range []string{restore.Manifest.Main.File, restore.Manifest.ExecutionShadow.File} {
		if err := os.Remove(filepath.Join(cfg.Backup.Dir, name)); err != nil {
			t.Fatal(err)
		}
	}

	if err := maybeRestoreFromSnapshot(cfg, log); err != nil {
		t.Fatalf("restore did not finish after main-sentinel crash cut: %v", err)
	}
	assertInterruptedDriveRestoreCompleted(t, cfg, attempt)
}
