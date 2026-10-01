package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

const (
	driveSnapshotManifestVersion = 2
	driveSnapshotManifestLegacy  = 1
	driveSnapshotManifestPrefix  = "snapshot-"
	driveSnapshotManifestSuffix  = ".json"
	driveSnapshotKeepGenerations = 2
	driveSnapshotRestoreVersion  = 1
	driveSnapshotRestoreMarker   = ".snapshot-restore-in-progress.json"
)

var driveSnapshotGenerationCounter atomic.Uint64

type driveSnapshotArtifact struct {
	File   string `json:"file"`
	Bytes  int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
}

// driveSnapshotManifest is the publication record for one immutable main+shadow pair.
// The two database files become visible first; this manifest becomes visible last. Restore
// therefore never infers a pair from whichever flat files happen to be present.
type driveSnapshotManifest struct {
	SchemaVersion         int                              `json:"schema_version"`
	Generation            string                           `json:"generation"`
	CreatedAt             time.Time                        `json:"created_at"`
	Main                  driveSnapshotArtifact            `json:"main"`
	ExecutionShadow       driveSnapshotArtifact            `json:"execution_shadow"`
	ExecutionShadowCutoff storage.ExecutionShadowWatermark `json:"execution_shadow_cutoff,omitempty"`
}

// driveSnapshotRestore records the exact verified generation whose local installation is in
// progress. It is published before either staged copy and removed only after the shadow companion
// and the main sentinel have both been promoted and reverified. A restart can therefore distinguish
// its own partial restore from unrelated local data and safely resume only the former.
type driveSnapshotRestore struct {
	SchemaVersion int                   `json:"schema_version"`
	Manifest      driveSnapshotManifest `json:"manifest"`
}

func newDriveSnapshotGeneration() string {
	return fmt.Sprintf("%s-%06d",
		time.Now().UTC().Format("20060102T150405.000000000Z"),
		driveSnapshotGenerationCounter.Add(1))
}

func driveSnapshotMainName(generation string) string {
	return "kalshi-" + generation + ".db"
}

func driveSnapshotShadowName(generation string) string {
	return "execution-shadow-" + generation + ".db"
}

func driveSnapshotManifestName(generation string) string {
	return driveSnapshotManifestPrefix + generation + driveSnapshotManifestSuffix
}

func driveSnapshotRestoreStageMain(generation string) string {
	return ".restore-" + driveSnapshotMainName(generation)
}

func driveSnapshotRestoreStageShadow(generation string) string {
	return ".restore-" + driveSnapshotShadowName(generation)
}

func validDriveSnapshotGeneration(generation string) bool {
	if generation == "" || len(generation) > 96 {
		return false
	}
	for _, r := range generation {
		if (r >= '0' && r <= '9') || r == 'T' || r == 'Z' || r == '.' || r == '-' {
			continue
		}
		return false
	}
	return true
}

func driveSnapshotArtifactFromFile(path, name string) (driveSnapshotArtifact, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return driveSnapshotArtifact{}, err
	}
	if !fi.Mode().IsRegular() || fi.Size() <= 0 {
		return driveSnapshotArtifact{}, fmt.Errorf("%s is not a non-empty regular file", path)
	}
	f, err := os.Open(path)
	if err != nil {
		return driveSnapshotArtifact{}, err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return driveSnapshotArtifact{}, err
	}
	return driveSnapshotArtifact{
		File: name, Bytes: fi.Size(), SHA256: hex.EncodeToString(h.Sum(nil)),
	}, nil
}

func verifyDriveSnapshotArtifact(dir string, artifact driveSnapshotArtifact, expectedName string) error {
	if artifact.File != expectedName || filepath.Base(artifact.File) != artifact.File {
		return fmt.Errorf("artifact name %q does not match generation name %q", artifact.File, expectedName)
	}
	if artifact.Bytes <= 0 || len(artifact.SHA256) != sha256.Size*2 {
		return fmt.Errorf("artifact %s has invalid size or hash metadata", artifact.File)
	}
	got, err := driveSnapshotArtifactFromFile(filepath.Join(dir, artifact.File), artifact.File)
	if err != nil {
		return fmt.Errorf("artifact %s: %w", artifact.File, err)
	}
	if got.Bytes != artifact.Bytes || !strings.EqualFold(got.SHA256, artifact.SHA256) {
		return fmt.Errorf("artifact %s hash/size mismatch", artifact.File)
	}
	if err := storage.QuickCheck(filepath.Join(dir, artifact.File)); err != nil {
		return fmt.Errorf("artifact %s quick_check: %w", artifact.File, err)
	}
	return nil
}

func validateDriveSnapshotManifest(dir string, manifest driveSnapshotManifest) error {
	if manifest.SchemaVersion != driveSnapshotManifestLegacy &&
		manifest.SchemaVersion != driveSnapshotManifestVersion {
		return fmt.Errorf("unsupported snapshot manifest version %d", manifest.SchemaVersion)
	}
	if !validDriveSnapshotGeneration(manifest.Generation) {
		return fmt.Errorf("invalid snapshot generation %q", manifest.Generation)
	}
	if manifest.CreatedAt.IsZero() {
		return errors.New("snapshot manifest has no creation time")
	}
	if err := verifyDriveSnapshotArtifact(dir, manifest.Main,
		driveSnapshotMainName(manifest.Generation)); err != nil {
		return fmt.Errorf("main database: %w", err)
	}
	if err := verifyDriveSnapshotArtifact(dir, manifest.ExecutionShadow,
		driveSnapshotShadowName(manifest.Generation)); err != nil {
		return fmt.Errorf("execution-shadow database: %w", err)
	}
	if manifest.SchemaVersion == driveSnapshotManifestVersion {
		if err := storage.ValidateExecutionShadowSnapshotWatermark(
			context.Background(),
			filepath.Join(dir, manifest.ExecutionShadow.File),
			manifest.ExecutionShadowCutoff,
		); err != nil {
			return fmt.Errorf("execution-shadow cutoff: %w", err)
		}
	}
	return nil
}

func validateDriveSnapshotRestoreShape(restore driveSnapshotRestore) error {
	if restore.SchemaVersion != driveSnapshotRestoreVersion {
		return fmt.Errorf("unsupported restore marker version %d", restore.SchemaVersion)
	}
	manifest := restore.Manifest
	if manifest.SchemaVersion != driveSnapshotManifestLegacy &&
		manifest.SchemaVersion != driveSnapshotManifestVersion {
		return fmt.Errorf("unsupported snapshot manifest version %d", manifest.SchemaVersion)
	}
	if !validDriveSnapshotGeneration(manifest.Generation) {
		return fmt.Errorf("invalid snapshot generation %q", manifest.Generation)
	}
	if manifest.CreatedAt.IsZero() {
		return errors.New("restore marker snapshot has no creation time")
	}
	for _, artifact := range []struct {
		got, want string
		meta      driveSnapshotArtifact
	}{
		{manifest.Main.File, driveSnapshotMainName(manifest.Generation), manifest.Main},
		{manifest.ExecutionShadow.File, driveSnapshotShadowName(manifest.Generation),
			manifest.ExecutionShadow},
	} {
		if artifact.got != artifact.want || filepath.Base(artifact.got) != artifact.got {
			return fmt.Errorf("restore marker artifact %q does not match %q",
				artifact.got, artifact.want)
		}
		if artifact.meta.Bytes <= 0 || len(artifact.meta.SHA256) != sha256.Size*2 {
			return fmt.Errorf("restore marker artifact %s has invalid size or hash metadata",
				artifact.got)
		}
		if _, err := hex.DecodeString(artifact.meta.SHA256); err != nil {
			return fmt.Errorf("restore marker artifact %s has invalid hash: %w",
				artifact.got, err)
		}
	}
	return nil
}

func loadDriveSnapshotRestore(path string) (driveSnapshotRestore, bool, error) {
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return driveSnapshotRestore{}, false, nil
	}
	if err != nil {
		return driveSnapshotRestore{}, false, err
	}
	var restore driveSnapshotRestore
	if err := json.Unmarshal(raw, &restore); err != nil {
		return driveSnapshotRestore{}, false, fmt.Errorf("invalid restore marker JSON: %w", err)
	}
	if err := validateDriveSnapshotRestoreShape(restore); err != nil {
		return driveSnapshotRestore{}, false, err
	}
	return restore, true, nil
}

func writeDriveSnapshotRestore(path string, restore driveSnapshotRestore) error {
	if err := validateDriveSnapshotRestoreShape(restore); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(restore, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	tmp := path + ".tmp"
	_ = os.Remove(tmp)
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	ok := false
	defer func() {
		_ = f.Close()
		if !ok {
			_ = os.Remove(tmp)
		}
	}()
	if _, err := f.Write(raw); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	ok = true
	return nil
}

func verifyDriveSnapshotRestoreArtifact(dataDir string, artifact driveSnapshotArtifact,
	localName string) error {
	local := artifact
	local.File = localName
	return verifyDriveSnapshotArtifact(dataDir, local, localName)
}

func syncDriveSnapshotRestoreFile(path string) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

func stageDriveSnapshotRestore(snapshotDir, dataDir string, restore driveSnapshotRestore) error {
	if err := validateDriveSnapshotRestoreShape(restore); err != nil {
		return err
	}
	mainStage := driveSnapshotRestoreStageMain(restore.Manifest.Generation)
	shadowStage := driveSnapshotRestoreStageShadow(restore.Manifest.Generation)
	for _, copy := range []struct {
		source, stage, live string
		artifact            driveSnapshotArtifact
	}{
		{restore.Manifest.Main.File, mainStage, "kalshi.db", restore.Manifest.Main},
		{restore.Manifest.ExecutionShadow.File, shadowStage, storage.ExecutionShadowFileName,
			restore.Manifest.ExecutionShadow},
	} {
		if fileNonEmpty(filepath.Join(dataDir, copy.live)) {
			if err := verifyDriveSnapshotRestoreArtifact(
				dataDir, copy.artifact, copy.live); err != nil {
				return fmt.Errorf("existing marker-owned %s is not the selected generation: %w",
					copy.live, err)
			}
			continue
		}
		if err := verifyDriveSnapshotRestoreArtifact(dataDir, copy.artifact, copy.stage); err == nil {
			continue
		}
		if err := verifyDriveSnapshotArtifact(snapshotDir, copy.artifact, copy.source); err != nil {
			return fmt.Errorf("restore source %s is no longer complete and its staged copy is unusable: %w",
				copy.source, err)
		}
		stagePath := filepath.Join(dataDir, copy.stage)
		if err := copyFile(filepath.Join(snapshotDir, copy.source), stagePath); err != nil {
			return fmt.Errorf("copy %s to restore staging: %w", copy.source, err)
		}
		if err := syncDriveSnapshotRestoreFile(stagePath); err != nil {
			return fmt.Errorf("sync staged %s: %w", copy.stage, err)
		}
		if err := verifyDriveSnapshotRestoreArtifact(dataDir, copy.artifact, copy.stage); err != nil {
			return fmt.Errorf("verify staged %s: %w", copy.stage, err)
		}
	}
	return nil
}

func promoteDriveSnapshotRestoreArtifact(dataDir, stageName, liveName string,
	artifact driveSnapshotArtifact) error {
	livePath := filepath.Join(dataDir, liveName)
	stagePath := filepath.Join(dataDir, stageName)
	if fileNonEmpty(livePath) {
		if err := verifyDriveSnapshotRestoreArtifact(dataDir, artifact, liveName); err != nil {
			return fmt.Errorf("existing marker-owned %s is not the selected generation: %w",
				liveName, err)
		}
		_ = os.Remove(stagePath)
		return nil
	}
	if err := os.Remove(livePath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove empty restore destination %s: %w", liveName, err)
	}
	if err := os.Rename(stagePath, livePath); err != nil {
		return fmt.Errorf("promote staged %s: %w", liveName, err)
	}
	if err := verifyDriveSnapshotRestoreArtifact(dataDir, artifact, liveName); err != nil {
		return fmt.Errorf("verify promoted %s: %w", liveName, err)
	}
	return nil
}

func promoteDriveSnapshotRestore(dataDir string, restore driveSnapshotRestore) error {
	// The companion is promoted first. The main DB is the fresh-machine sentinel and is therefore
	// promoted last, only after both staged files passed hash, size, and SQLite integrity checks.
	if err := promoteDriveSnapshotRestoreArtifact(dataDir,
		driveSnapshotRestoreStageShadow(restore.Manifest.Generation),
		storage.ExecutionShadowFileName, restore.Manifest.ExecutionShadow); err != nil {
		return err
	}
	if err := promoteDriveSnapshotRestoreArtifact(dataDir,
		driveSnapshotRestoreStageMain(restore.Manifest.Generation),
		"kalshi.db", restore.Manifest.Main); err != nil {
		return err
	}
	return verifyInstalledDriveSnapshotRestore(dataDir, restore)
}

func verifyInstalledDriveSnapshotRestore(dataDir string, restore driveSnapshotRestore) error {
	if err := validateDriveSnapshotRestoreShape(restore); err != nil {
		return err
	}
	if err := verifyDriveSnapshotRestoreArtifact(dataDir, restore.Manifest.ExecutionShadow,
		storage.ExecutionShadowFileName); err != nil {
		return fmt.Errorf("installed execution-shadow database: %w", err)
	}
	if err := verifyDriveSnapshotRestoreArtifact(dataDir, restore.Manifest.Main,
		"kalshi.db"); err != nil {
		return fmt.Errorf("installed main database: %w", err)
	}
	return nil
}

func finishDriveSnapshotRestore(dataDir string, restore driveSnapshotRestore) error {
	if err := verifyInstalledDriveSnapshotRestore(dataDir, restore); err != nil {
		return err
	}
	for _, name := range []string{
		driveSnapshotRestoreStageMain(restore.Manifest.Generation),
		driveSnapshotRestoreStageShadow(restore.Manifest.Generation),
	} {
		_ = os.Remove(filepath.Join(dataDir, name))
		_ = os.Remove(filepath.Join(dataDir, name+".tmp"))
	}
	markerPath := filepath.Join(dataDir, driveSnapshotRestoreMarker)
	if err := os.Remove(markerPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove completed restore marker: %w", err)
	}
	return nil
}

// newestVerifiedDriveSnapshot selects the newest manifest whose exact main+shadow pair passes
// hash, size and SQLite integrity checks. Invalid/incompletely-synced newer generations are ignored
// in favor of the previous complete generation. If snapshot material exists but no complete
// generation does, startup must fail rather than create a blank database.
func newestVerifiedDriveSnapshot(dir string) (driveSnapshotManifest, bool, error) {
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return driveSnapshotManifest{}, false, nil
	}
	if err != nil {
		return driveSnapshotManifest{}, false, err
	}
	var names []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasPrefix(name, driveSnapshotManifestPrefix) &&
			strings.HasSuffix(name, driveSnapshotManifestSuffix) {
			names = append(names, name)
		}
	}
	sort.Sort(sort.Reverse(sort.StringSlice(names)))
	var invalid []string
	for _, name := range names {
		raw, readErr := os.ReadFile(filepath.Join(dir, name))
		if readErr != nil {
			invalid = append(invalid, name+": "+readErr.Error())
			continue
		}
		var manifest driveSnapshotManifest
		if err := json.Unmarshal(raw, &manifest); err != nil {
			invalid = append(invalid, name+": invalid JSON")
			continue
		}
		if driveSnapshotManifestName(manifest.Generation) != name {
			invalid = append(invalid, name+": filename/generation mismatch")
			continue
		}
		if err := validateDriveSnapshotManifest(dir, manifest); err != nil {
			invalid = append(invalid, name+": "+err.Error())
			continue
		}
		return manifest, true, nil
	}
	if len(names) > 0 || fileNonEmpty(filepath.Join(dir, "kalshi.db")) ||
		fileNonEmpty(filepath.Join(dir, storage.ExecutionShadowFileName)) {
		detail := "legacy/unmanifested snapshot files are not restorable"
		if len(invalid) > 0 {
			detail = strings.Join(invalid, "; ")
		}
		return driveSnapshotManifest{}, false,
			fmt.Errorf("no verified complete main+execution-shadow snapshot generation: %s", detail)
	}
	return driveSnapshotManifest{}, false, nil
}

func writeDriveSnapshotManifestLast(path string, manifest driveSnapshotManifest) error {
	raw, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	tmp := path + ".tmp"
	_ = os.Remove(tmp)
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	ok := false
	defer func() {
		_ = f.Close()
		if !ok {
			_ = os.Remove(tmp)
		}
	}()
	if _, err := f.Write(raw); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	ok = true
	return nil
}

// pruneDriveSnapshotGenerations keeps the newest complete generations. It never removes files
// from a generation whose manifest cannot be parsed and matched, so a malformed cloud-sync tail
// cannot trick cleanup into deleting an unrelated path.
func pruneDriveSnapshotGenerations(dir string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	var names []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasPrefix(entry.Name(), driveSnapshotManifestPrefix) &&
			strings.HasSuffix(entry.Name(), driveSnapshotManifestSuffix) {
			names = append(names, entry.Name())
		}
	}
	sort.Sort(sort.Reverse(sort.StringSlice(names)))
	keptVerified := 0
	for _, name := range names {
		raw, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			continue
		}
		var manifest driveSnapshotManifest
		if json.Unmarshal(raw, &manifest) != nil ||
			driveSnapshotManifestName(manifest.Generation) != name ||
			!validDriveSnapshotGeneration(manifest.Generation) ||
			manifest.Main.File != driveSnapshotMainName(manifest.Generation) ||
			manifest.ExecutionShadow.File != driveSnapshotShadowName(manifest.Generation) {
			continue
		}
		// Only a genuinely restorable generation consumes a retention slot. A malformed or
		// half-synced newer manifest must never evict the previous known-good pair.
		if validateDriveSnapshotManifest(dir, manifest) != nil {
			continue
		}
		keptVerified++
		if keptVerified <= driveSnapshotKeepGenerations {
			continue
		}
		_ = os.Remove(filepath.Join(dir, manifest.Main.File))
		_ = os.Remove(filepath.Join(dir, manifest.ExecutionShadow.File))
		_ = os.Remove(filepath.Join(dir, name))
	}
}
