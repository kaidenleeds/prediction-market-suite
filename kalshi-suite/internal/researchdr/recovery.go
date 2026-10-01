// Package researchdr creates and verifies bounded, offline-only research recovery bundles. It
// intentionally has no restore function: replacing live state is an operator procedure, never an
// HTTP side effect or an automatic response to a transient error.
package researchdr

import (
	"bufio"
	"compress/gzip"
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
	"sync"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/researchreplay"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

const (
	defaultTimeout      = 30 * time.Minute
	defaultReplayFiles  = 32
	defaultReplayBytes  = int64(64 << 20)
	defaultMLBytes      = int64(128 << 20)
	maxSingleArtifact   = int64(64 << 20)
	manifestFileName    = "manifest.json"
	bundleSuffix        = ".bundle"
	bundlePrefix        = "kalshi-r138-"
	buildingSuffix      = ".building"
	verificationScope   = "sha256+sqlite-quick-check+typed-file-check+restore-copy-drill"
	legacyManifestV2    = 2
	currentManifestV3   = 3
	executionShadowRole = "execution-shadow-db"
	publishRenameBudget = 2 * time.Second
)

// Artifact is one immutable item inside a bundle. Missing optional entries are retained in the
// manifest so a fresh test database can never be mistaken for a complete production recovery set.
type Artifact struct {
	Role       string `json:"role"`
	SourceName string `json:"source_name"`
	BundlePath string `json:"bundle_path,omitempty"`
	Required   bool   `json:"required"`
	Status     string `json:"status"`
	SizeBytes  int64  `json:"size_bytes,omitempty"`
	SHA256     string `json:"sha256,omitempty"`
	QuickCheck string `json:"quick_check"`
}

type ReplaySelection struct {
	SourceSegments   int    `json:"source_segments"`
	IncludedSegments int    `json:"included_segments"`
	IncludedBytes    int64  `json:"included_bytes"`
	HeadSegmentHash  string `json:"head_segment_hash,omitempty"`
	FullArchive      bool   `json:"full_archive"`
	Policy           string `json:"policy"`
}

type RestoreDrill struct {
	Passed      bool   `json:"passed"`
	VerifiedAt  string `json:"verified_at"`
	Artifacts   int    `json:"artifacts"`
	BytesCopied int64  `json:"bytes_copied"`
	Scope       string `json:"scope"`
}

type Manifest struct {
	Version       int    `json:"version"`
	BundleName    string `json:"bundle_name"`
	CreatedAt     string `json:"created_at"`
	DatabaseFile  string `json:"database_file"`
	SizeBytes     int64  `json:"size_bytes"`
	SHA256        string `json:"sha256"`
	QuickCheck    string `json:"quick_check"`
	Method        string `json:"method"`
	ResearchOnly  bool   `json:"research_only"`
	RestoreIsAuto bool   `json:"restore_is_automatic"`

	Artifacts        []Artifact      `json:"artifacts"`
	RequiredComplete bool            `json:"required_artifacts_complete"`
	FreshTestPolicy  bool            `json:"fresh_test_missing_artifacts_allowed"`
	Replay           ReplaySelection `json:"replay_selection"`
	Drill            RestoreDrill    `json:"restore_copy_drill"`
}

type ArtifactVerify struct {
	Role        string `json:"role"`
	BundlePath  string `json:"bundle_path,omitempty"`
	Required    bool   `json:"required"`
	Status      string `json:"status"`
	Exists      bool   `json:"exists"`
	HashMatches bool   `json:"hash_matches"`
	QuickOK     bool   `json:"quick_check_ok"`
	Error       string `json:"error,omitempty"`
}

type VerifyReport struct {
	Manifest
	ManifestFile           string           `json:"manifest_file"`
	Exists                 bool             `json:"exists"`
	HashMatches            bool             `json:"hash_matches"`
	SQLiteOK               bool             `json:"sqlite_quick_check_ok"`
	RestoreDrillOK         bool             `json:"restore_copy_drill_ok"`
	ExecutionShadowCovered bool             `json:"execution_shadow_covered"`
	LegacyCompatible       bool             `json:"legacy_compatible"`
	VerificationMode       string           `json:"verification_mode"`
	ArtifactReports        []ArtifactVerify `json:"artifact_reports,omitempty"`
	Error                  string           `json:"error,omitempty"`
}

type RestorePlan struct {
	Automatic       bool     `json:"automatic"`
	RequiresStopped bool     `json:"requires_suite_stopped"`
	Snapshot        string   `json:"snapshot"`
	LiveDatabase    string   `json:"live_database"`
	Steps           []string `json:"steps"`
	Refusal         string   `json:"refusal"`
}

type Options struct {
	// AllowFreshMissing is only for isolated, newly-created test stores. It relaxes archive,
	// replay, and model provenance; the post-isolation execution-shadow database is always
	// required. Production callers use New.
	AllowFreshMissing bool
	MaxReplayFiles    int
	MaxReplayBytes    int64
	MaxMLBytes        int64
}

func (o Options) normalized() Options {
	if o.MaxReplayFiles <= 0 || o.MaxReplayFiles > 256 {
		o.MaxReplayFiles = defaultReplayFiles
	}
	if o.MaxReplayBytes <= 0 || o.MaxReplayBytes > 512<<20 {
		o.MaxReplayBytes = defaultReplayBytes
	}
	if o.MaxMLBytes <= 0 || o.MaxMLBytes > 512<<20 {
		o.MaxMLBytes = defaultMLBytes
	}
	return o
}

type Manager struct {
	root    string
	dataDir string
	liveDB  string
	store   *storage.Store
	maxKeep int
	opts    Options
	mu      sync.Mutex
}

func New(dataDir string, store *storage.Store, maxKeep int) (*Manager, error) {
	return NewWithOptions(dataDir, store, maxKeep, Options{})
}

func NewWithOptions(dataDir string, store *storage.Store, maxKeep int, opts Options) (*Manager, error) {
	if store == nil {
		return nil, errors.New("nil recovery store")
	}
	dataAbs, err := filepath.Abs(dataDir)
	if err != nil {
		return nil, err
	}
	if maxKeep <= 0 || maxKeep > 12 {
		maxKeep = 3
	}
	return &Manager{root: filepath.Join(dataAbs, "recovery"), dataDir: dataAbs,
		liveDB: filepath.Join(dataAbs, "kalshi.db"), store: store, maxKeep: maxKeep,
		opts: opts.normalized()}, nil
}

func validBundleName(name string) bool {
	return name != "" && filepath.Base(name) == name && !strings.ContainsAny(name, `/\`) &&
		strings.HasPrefix(name, bundlePrefix) && strings.HasSuffix(name, bundleSuffix)
}

func (m *Manager) bundleDir(name string) (string, error) {
	if !validBundleName(name) {
		return "", errors.New("invalid recovery bundle name")
	}
	root, err := filepath.Abs(m.root)
	if err != nil {
		return "", err
	}
	p, err := filepath.Abs(filepath.Join(root, name))
	if err != nil {
		return "", err
	}
	if filepath.Dir(p) != root {
		return "", errors.New("recovery path escaped recovery directory")
	}
	return p, nil
}

func childPath(root, rel string) (string, error) {
	clean := filepath.Clean(filepath.FromSlash(rel))
	if rel == "" || clean == "." || filepath.IsAbs(clean) || strings.HasPrefix(clean, ".."+string(os.PathSeparator)) || clean == ".." {
		return "", errors.New("invalid bundle artifact path")
	}
	rootAbs, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	p, err := filepath.Abs(filepath.Join(rootAbs, clean))
	if err != nil {
		return "", err
	}
	relCheck, err := filepath.Rel(rootAbs, p)
	if err != nil || relCheck == ".." || strings.HasPrefix(relCheck, ".."+string(os.PathSeparator)) {
		return "", errors.New("bundle artifact escaped bundle directory")
	}
	return p, nil
}

func fileSHA256(path string) (string, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), n, nil
}

func atomicJSON(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

// renameBundleWithRetry preserves the single atomic publication rename while tolerating the
// short sharing violations Windows can report immediately after SQLite/hash verification closes
// a newly-created directory tree. It refuses collisions or a vanished staging tree immediately
// and never retries beyond the caller's cancellation or the small offline-backup budget.
func renameBundleWithRetry(ctx context.Context, stage, final string,
	rename func(string, string) error,
) error {
	if rename == nil {
		return errors.New("recovery bundle rename is unavailable")
	}
	deadline := time.Now().Add(publishRenameBudget)
	delay := 10 * time.Millisecond
	var lastErr error
	for {
		if err := rename(stage, final); err == nil {
			return nil
		} else {
			lastErr = err
		}
		if _, err := os.Lstat(final); err == nil {
			return fmt.Errorf("recovery bundle destination already exists: %w", lastErr)
		} else if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("inspect recovery bundle destination: %w", err)
		}
		if _, err := os.Lstat(stage); err != nil {
			return fmt.Errorf("recovery bundle staging tree unavailable: %w", lastErr)
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return fmt.Errorf("publish recovery bundle after bounded rename retry: %w", lastErr)
		}
		if delay > remaining {
			delay = remaining
		}
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return fmt.Errorf("publish recovery bundle: %w", ctx.Err())
		}
		if delay < 200*time.Millisecond {
			delay *= 2
			if delay > 200*time.Millisecond {
				delay = 200 * time.Millisecond
			}
		}
	}
}

type sourceFile struct {
	role, sourceName, sourcePath, bundlePath, check string
	required                                        bool
}

type preflight struct {
	archivePath string
	shadowPath  string
	replayDir   string
	replayIndex researchreplay.Index
	replayFiles []sourceFile
	models      []sourceFile
	missing     []Artifact
	replay      ReplaySelection
}

func regularFile(path string) (os.FileInfo, error) {
	fi, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if fi.Mode()&os.ModeSymlink != 0 || !fi.Mode().IsRegular() {
		return nil, fmt.Errorf("artifact is not a regular non-symlink file")
	}
	return fi, nil
}

func (m *Manager) preflightInputs() (preflight, error) {
	p := preflight{archivePath: filepath.Join(m.dataDir, storage.ArchiveFileName),
		shadowPath: filepath.Join(m.dataDir, storage.ExecutionShadowFileName),
		replayDir:  filepath.Join(m.dataDir, "research-replay"), replay: ReplaySelection{
			Policy: "newest integrity-verified tail, at most 32 files/64 MiB by default; source index retained for provenance"}}
	var missing []string
	require := !m.opts.AllowFreshMissing

	if _, err := regularFile(p.archivePath); err != nil {
		p.missing = append(p.missing, Artifact{Role: "archive-db", SourceName: storage.ArchiveFileName,
			Required: require, Status: "optional_missing", QuickCheck: "not_applicable"})
		if require {
			missing = append(missing, "archive database")
		}
	}
	if _, err := regularFile(p.shadowPath); err != nil {
		return p, fmt.Errorf("execution-shadow database: %w", err)
	}

	indexPath := filepath.Join(p.replayDir, researchreplay.IndexFileName)
	indexBytes, err := os.ReadFile(indexPath)
	if err != nil {
		p.missing = append(p.missing, Artifact{Role: "replay-index", SourceName: "research-replay/index.json",
			Required: require, Status: "optional_missing", QuickCheck: "not_applicable"})
		if require {
			missing = append(missing, "research replay index")
		}
	} else {
		if _, err := regularFile(indexPath); err != nil {
			return p, fmt.Errorf("research replay index: %w", err)
		}
		if err := json.Unmarshal(indexBytes, &p.replayIndex); err != nil || p.replayIndex.Format != researchreplay.FormatVersion {
			return p, fmt.Errorf("research replay index is invalid or unsupported")
		}
		verifyN := m.opts.MaxReplayFiles
		if verifyN > len(p.replayIndex.Segments) {
			verifyN = len(p.replayIndex.Segments)
		}
		if _, err := researchreplay.VerifyDirectoryRecent(p.replayDir, verifyN); err != nil {
			return p, fmt.Errorf("research replay source verification: %w", err)
		}
		p.replayFiles = append(p.replayFiles, sourceFile{role: "replay-index", sourceName: "research-replay/index.json",
			sourcePath: indexPath, bundlePath: "replay/source-index.json", check: "json", required: true})
		p.replay.SourceSegments = len(p.replayIndex.Segments)
		p.replay.HeadSegmentHash = p.replayIndex.HeadSegmentHash
		var selected []sourceFile
		for i := len(p.replayIndex.Segments) - 1; i >= 0 && len(selected) < m.opts.MaxReplayFiles; i-- {
			meta := p.replayIndex.Segments[i]
			src, err := childPath(p.replayDir, meta.RelativePath)
			if err != nil {
				return p, fmt.Errorf("research replay segment path: %w", err)
			}
			fi, err := regularFile(src)
			if err != nil {
				return p, fmt.Errorf("research replay segment %s: %w", meta.SegmentID, err)
			}
			if fi.Size() != meta.CompressedBytes {
				return p, fmt.Errorf("research replay segment %s size differs from index", meta.SegmentID)
			}
			if fi.Size() > m.opts.MaxReplayBytes {
				return p, fmt.Errorf("newest research replay segment exceeds recovery bound")
			}
			if p.replay.IncludedBytes+fi.Size() > m.opts.MaxReplayBytes {
				break
			}
			selected = append(selected, sourceFile{role: "replay-segment", sourceName: filepath.ToSlash(filepath.Join("research-replay", meta.RelativePath)),
				sourcePath: src, bundlePath: filepath.ToSlash(filepath.Join("replay", meta.RelativePath)), check: "gzip-jsonl", required: true})
			p.replay.IncludedBytes += fi.Size()
		}
		for i, j := 0, len(selected)-1; i < j; i, j = i+1, j-1 {
			selected[i], selected[j] = selected[j], selected[i]
		}
		p.replayFiles = append(p.replayFiles, selected...)
		p.replay.IncludedSegments = len(selected)
		p.replay.FullArchive = len(selected) == len(p.replayIndex.Segments)
		if len(p.replayIndex.Segments) > 0 && len(selected) == 0 {
			return p, errors.New("research replay has segments but none fit the recovery bound")
		}
	}

	modelNames := []string{"ml_model_book_v2.pkl", "ml_model_book_v1.pkl", "ml_model.pkl"}
	var modelPath, modelName string
	for _, name := range modelNames {
		if _, err := regularFile(filepath.Join(m.dataDir, name)); err == nil {
			modelName, modelPath = name, filepath.Join(m.dataDir, name)
			break
		}
	}
	core := []struct{ role, name, check string }{
		{"ml-model", modelName, "opaque-model"},
		{"ml-export", "ml_model_export.json", "json"},
		{"ml-history", "ml_model_history.jsonl", "jsonl"},
	}
	var mlBytes int64
	for _, c := range core {
		path := filepath.Join(m.dataDir, c.name)
		if c.role == "ml-model" {
			path = modelPath
		}
		if c.name == "" {
			p.missing = append(p.missing, Artifact{Role: c.role, SourceName: "latest supported ML model",
				Required: require, Status: "optional_missing", QuickCheck: "not_applicable"})
			if require {
				missing = append(missing, c.role)
			}
			continue
		}
		fi, err := regularFile(path)
		if err != nil {
			p.missing = append(p.missing, Artifact{Role: c.role, SourceName: c.name,
				Required: require, Status: "optional_missing", QuickCheck: "not_applicable"})
			if require {
				missing = append(missing, c.role)
			}
			continue
		}
		if fi.Size() > maxSingleArtifact || mlBytes+fi.Size() > m.opts.MaxMLBytes {
			return p, fmt.Errorf("required %s exceeds ML recovery bound", c.role)
		}
		mlBytes += fi.Size()
		p.models = append(p.models, sourceFile{role: c.role, sourceName: c.name, sourcePath: path,
			bundlePath: filepath.ToSlash(filepath.Join("ml", c.name)), check: c.check, required: true})
	}
	for _, name := range []string{"ml_eval_vectors.json", "ml_accuracy.json", "ml_realization.json", "ml_alloc.json"} {
		path := filepath.Join(m.dataDir, name)
		fi, err := regularFile(path)
		if err != nil {
			p.missing = append(p.missing, Artifact{Role: "ml-provenance", SourceName: name,
				Required: false, Status: "optional_missing", QuickCheck: "not_applicable"})
			continue
		}
		if fi.Size() > maxSingleArtifact || mlBytes+fi.Size() > m.opts.MaxMLBytes {
			p.missing = append(p.missing, Artifact{Role: "ml-provenance", SourceName: name,
				Required: false, Status: "optional_omitted_bound", QuickCheck: "not_applicable"})
			continue
		}
		mlBytes += fi.Size()
		p.models = append(p.models, sourceFile{role: "ml-provenance", sourceName: name, sourcePath: path,
			bundlePath: filepath.ToSlash(filepath.Join("ml", name)), check: "json", required: false})
	}
	if len(missing) > 0 {
		return p, fmt.Errorf("production recovery preflight missing required artifacts: %s", strings.Join(missing, ", "))
	}
	return p, nil
}

func copyFileBounded(src, dst string, maxBytes int64) (int64, error) {
	before, err := regularFile(src)
	if err != nil {
		return 0, err
	}
	if maxBytes > 0 && before.Size() > maxBytes {
		return 0, fmt.Errorf("artifact size %d exceeds bound %d", before.Size(), maxBytes)
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
		return 0, err
	}
	in, err := os.Open(src)
	if err != nil {
		return 0, err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return 0, err
	}
	n, copyErr := io.Copy(out, io.LimitReader(in, before.Size()+1))
	closeErr := out.Close()
	if copyErr != nil {
		_ = os.Remove(dst)
		return n, copyErr
	}
	if closeErr != nil || n != before.Size() {
		_ = os.Remove(dst)
		if closeErr != nil {
			return n, closeErr
		}
		return n, errors.New("artifact changed size during copy")
	}
	after, err := regularFile(src)
	if err != nil || after.Size() != before.Size() || !after.ModTime().Equal(before.ModTime()) {
		_ = os.Remove(dst)
		return n, errors.New("artifact changed during recovery copy")
	}
	return n, nil
}

func validateJSON(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	dec := json.NewDecoder(f)
	var value any
	if err := dec.Decode(&value); err != nil {
		return err
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return errors.New("JSON artifact contains trailing data")
	}
	return nil
}

func validateJSONLReader(r io.Reader) error {
	scan := bufio.NewScanner(r)
	scan.Buffer(make([]byte, 64<<10), 2<<20)
	lines := 0
	for scan.Scan() {
		if len(strings.TrimSpace(scan.Text())) == 0 {
			continue
		}
		lines++
		if !json.Valid(scan.Bytes()) {
			return fmt.Errorf("invalid JSONL at non-empty line %d", lines)
		}
	}
	if err := scan.Err(); err != nil {
		return err
	}
	if lines == 0 {
		return errors.New("empty JSONL artifact")
	}
	return nil
}

func validateTyped(path, check string) error {
	switch check {
	case "sqlite":
		return storage.QuickCheck(path)
	case "json":
		return validateJSON(path)
	case "jsonl":
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()
		return validateJSONLReader(f)
	case "gzip-jsonl":
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()
		gz, err := gzip.NewReader(f)
		if err != nil {
			return err
		}
		err = validateJSONLReader(gz)
		if closeErr := gz.Close(); err == nil {
			err = closeErr
		}
		return err
	case "opaque-model":
		fi, err := regularFile(path)
		if err != nil {
			return err
		}
		if fi.Size() == 0 {
			return errors.New("empty opaque model")
		}
		return nil
	default:
		return fmt.Errorf("unknown artifact check %q", check)
	}
}

func addArtifact(root string, sf sourceFile, maxBytes int64) (Artifact, error) {
	dst, err := childPath(root, sf.bundlePath)
	if err != nil {
		return Artifact{}, err
	}
	if _, err := copyFileBounded(sf.sourcePath, dst, maxBytes); err != nil {
		return Artifact{}, err
	}
	if err := validateTyped(dst, sf.check); err != nil {
		return Artifact{}, fmt.Errorf("%s validation: %w", sf.role, err)
	}
	hash, size, err := fileSHA256(dst)
	if err != nil {
		return Artifact{}, err
	}
	return Artifact{Role: sf.role, SourceName: sf.sourceName, BundlePath: sf.bundlePath,
		Required: sf.required, Status: "present", SizeBytes: size, SHA256: hash,
		QuickCheck: sf.check + "=ok"}, nil
}

func addSQLiteArtifact(root, role, sourceName, rel string, required bool, backup func(string) error) (Artifact, error) {
	dst, err := childPath(root, rel)
	if err != nil {
		return Artifact{}, err
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
		return Artifact{}, err
	}
	if err := backup(dst); err != nil {
		return Artifact{}, err
	}
	if err := storage.QuickCheck(dst); err != nil {
		return Artifact{}, err
	}
	hash, size, err := fileSHA256(dst)
	if err != nil {
		return Artifact{}, err
	}
	return Artifact{Role: role, SourceName: sourceName, BundlePath: rel, Required: required,
		Status: "present", SizeBytes: size, SHA256: hash, QuickCheck: "sqlite=ok"}, nil
}

func restoreCopyDrill(bundleRoot string, artifacts []Artifact) (RestoreDrill, error) {
	tmp, err := os.MkdirTemp("", "kalshi-r138-restore-drill-")
	if err != nil {
		return RestoreDrill{}, err
	}
	defer os.RemoveAll(tmp)
	drill := RestoreDrill{VerifiedAt: time.Now().UTC().Format(time.RFC3339Nano), Scope: verificationScope}
	for _, artifact := range artifacts {
		if artifact.Status != "present" {
			continue
		}
		src, err := childPath(bundleRoot, artifact.BundlePath)
		if err != nil {
			return drill, err
		}
		dst, err := childPath(tmp, artifact.BundlePath)
		if err != nil {
			return drill, err
		}
		if _, err := copyFileBounded(src, dst, artifact.SizeBytes); err != nil {
			return drill, fmt.Errorf("restore-copy %s: %w", artifact.Role, err)
		}
		hash, size, err := fileSHA256(dst)
		if err != nil || hash != artifact.SHA256 || size != artifact.SizeBytes {
			return drill, fmt.Errorf("restore-copy %s hash/size mismatch", artifact.Role)
		}
		check := strings.TrimSuffix(artifact.QuickCheck, "=ok")
		if err := validateTyped(dst, check); err != nil {
			return drill, fmt.Errorf("restore-copy %s validation: %w", artifact.Role, err)
		}
		drill.Artifacts++
		drill.BytesCopied += size
	}
	drill.Passed = true
	return drill, nil
}

// CreateSnapshot first fails closed on missing production artifacts, then builds everything under
// one staging directory. Publication is a single directory rename after hashes, typed checks and a
// full restore-copy drill pass. It never closes, renames, truncates or replaces live state.
func (m *Manager) CreateSnapshot(ctx context.Context) (Manifest, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, defaultTimeout)
		defer cancel()
	}
	inputs, err := m.preflightInputs()
	if err != nil {
		return Manifest{}, err
	}
	if err := os.MkdirAll(m.root, 0o700); err != nil {
		return Manifest{}, err
	}
	m.cleanupBuildingLocked()
	stamp := time.Now().UTC().Format("20060102T150405.000000000Z")
	name := bundlePrefix + stamp + bundleSuffix
	final, err := m.bundleDir(name)
	if err != nil {
		return Manifest{}, err
	}
	stage := final + buildingSuffix
	if err := os.Mkdir(stage, 0o700); err != nil {
		return Manifest{}, err
	}
	defer os.RemoveAll(stage)

	mainArtifact, err := addSQLiteArtifact(stage, "main-db", "kalshi.db", "database/kalshi.db", true,
		func(dst string) error { return m.store.Backup(ctx, dst) })
	if err != nil {
		return Manifest{}, fmt.Errorf("consistent main backup: %w", err)
	}
	artifacts := []Artifact{mainArtifact}
	shadowArtifact, err := addSQLiteArtifact(stage, executionShadowRole,
		storage.ExecutionShadowFileName, "database/"+storage.ExecutionShadowFileName, true,
		func(dst string) error { return m.store.BackupExecutionShadow(ctx, dst) })
	if err != nil {
		return Manifest{}, fmt.Errorf("consistent execution-shadow backup: %w", err)
	}
	artifacts = append(artifacts, shadowArtifact)
	if _, err := regularFile(inputs.archivePath); err == nil {
		archiveArtifact, err := addSQLiteArtifact(stage, "archive-db", storage.ArchiveFileName,
			"database/"+storage.ArchiveFileName, true,
			func(dst string) error { return storage.BackupSQLiteFile(ctx, inputs.archivePath, dst) })
		if err != nil {
			return Manifest{}, fmt.Errorf("consistent archive backup: %w", err)
		}
		artifacts = append(artifacts, archiveArtifact)
	}
	for _, sf := range append(inputs.replayFiles, inputs.models...) {
		artifact, err := addArtifact(stage, sf, maxSingleArtifact)
		if err != nil {
			return Manifest{}, fmt.Errorf("copy %s: %w", sf.role, err)
		}
		artifacts = append(artifacts, artifact)
	}
	artifacts = append(artifacts, inputs.missing...)
	requiredComplete := true
	var total int64
	for _, artifact := range artifacts {
		if artifact.Status == "present" {
			total += artifact.SizeBytes
		}
		if artifact.Required && artifact.Status != "present" {
			requiredComplete = false
		}
	}
	if !requiredComplete {
		return Manifest{}, errors.New("required recovery artifacts are incomplete")
	}
	drill, err := restoreCopyDrill(stage, artifacts)
	if err != nil {
		return Manifest{}, fmt.Errorf("restore-copy drill: %w", err)
	}
	manifest := Manifest{Version: currentManifestV3, BundleName: name, CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
		DatabaseFile: mainArtifact.BundlePath, SizeBytes: total, SHA256: mainArtifact.SHA256,
		QuickCheck: "ok", Method: "SQLite VACUUM INTO for main+archive+required execution-shadow; selected immutable replay/ML copies; SHA-256; typed checks; temp restore-copy drill",
		ResearchOnly: true, RestoreIsAuto: false, Artifacts: artifacts, RequiredComplete: true,
		FreshTestPolicy: m.opts.AllowFreshMissing, Replay: inputs.replay, Drill: drill}
	if err := atomicJSON(filepath.Join(stage, manifestFileName), manifest); err != nil {
		return Manifest{}, err
	}
	if err := renameBundleWithRetry(ctx, stage, final, os.Rename); err != nil {
		return Manifest{}, err
	}
	if err := m.rotateLocked(); err != nil {
		return Manifest{}, fmt.Errorf("snapshot published but rotation failed: %w", err)
	}
	return manifest, nil
}

func (m *Manager) cleanupBuildingLocked() {
	entries, _ := os.ReadDir(m.root)
	for _, entry := range entries {
		if !entry.IsDir() || !strings.HasPrefix(entry.Name(), bundlePrefix) || !strings.HasSuffix(entry.Name(), bundleSuffix+buildingSuffix) {
			continue
		}
		p := filepath.Join(m.root, entry.Name())
		if filepath.Dir(p) == m.root {
			_ = os.RemoveAll(p)
		}
	}
}

func (m *Manager) rotateLocked() error {
	entries, err := os.ReadDir(m.root)
	if err != nil {
		return err
	}
	var names []string
	for _, entry := range entries {
		if entry.IsDir() && validBundleName(entry.Name()) {
			names = append(names, entry.Name())
		}
	}
	sort.Sort(sort.Reverse(sort.StringSlice(names)))
	if len(names) <= m.maxKeep {
		return nil
	}
	for _, name := range names[m.maxKeep:] {
		p, err := m.bundleDir(name)
		if err != nil {
			return err
		}
		if err := os.RemoveAll(p); err != nil {
			return err
		}
	}
	return nil
}

func readManifest(bundlePath string) (Manifest, error) {
	var manifest Manifest
	b, err := os.ReadFile(filepath.Join(bundlePath, manifestFileName))
	if err != nil {
		return manifest, err
	}
	if err := json.Unmarshal(b, &manifest); err != nil {
		return manifest, err
	}
	if (manifest.Version != legacyManifestV2 && manifest.Version != currentManifestV3) ||
		manifest.BundleName != filepath.Base(bundlePath) || len(manifest.Artifacts) == 0 {
		return manifest, errors.New("invalid recovery bundle manifest")
	}
	if manifest.Version == currentManifestV3 {
		count, covered := executionShadowCoverage(manifest)
		if count != 1 || !covered {
			return manifest, errors.New("post-isolation recovery manifest requires exactly one present, required execution-shadow database")
		}
	}
	return manifest, nil
}

func executionShadowCoverage(manifest Manifest) (int, bool) {
	const canonicalPath = "database/" + storage.ExecutionShadowFileName
	count := 0
	covered := false
	for _, artifact := range manifest.Artifacts {
		if artifact.Role != executionShadowRole {
			continue
		}
		count++
		if artifact.Required && artifact.Status == "present" &&
			filepath.ToSlash(artifact.BundlePath) == canonicalPath {
			covered = true
		}
	}
	return count, covered
}

func verifyArtifact(bundlePath string, artifact Artifact) ArtifactVerify {
	report := ArtifactVerify{Role: artifact.Role, BundlePath: artifact.BundlePath, Required: artifact.Required, Status: artifact.Status}
	if artifact.Status != "present" {
		report.Exists = false
		report.HashMatches = !artifact.Required
		report.QuickOK = !artifact.Required
		if artifact.Required {
			report.Error = "required artifact is not present"
		}
		return report
	}
	p, err := childPath(bundlePath, artifact.BundlePath)
	if err != nil {
		report.Error = err.Error()
		return report
	}
	if _, err := regularFile(p); err != nil {
		report.Error = err.Error()
		return report
	}
	report.Exists = true
	hash, size, err := fileSHA256(p)
	if err != nil {
		report.Error = err.Error()
		return report
	}
	report.HashMatches = hash == artifact.SHA256 && size == artifact.SizeBytes
	check := strings.TrimSuffix(artifact.QuickCheck, "=ok")
	report.QuickOK = validateTyped(p, check) == nil
	if !report.HashMatches || !report.QuickOK {
		report.Error = "artifact failed hash, size, or typed verification"
	}
	return report
}

// Verify is the explicit, expensive offline verification path. List intentionally reports the
// immutable creation receipt without re-reading multi-gigabyte files on a live dashboard refresh.
func (m *Manager) Verify(name string) VerifyReport {
	report := VerifyReport{ManifestFile: filepath.ToSlash(filepath.Join(name, manifestFileName)),
		VerificationMode: "full-offline"}
	bundle, err := m.bundleDir(name)
	if err != nil {
		report.Error = err.Error()
		return report
	}
	manifest, err := readManifest(bundle)
	if err != nil {
		report.Error = err.Error()
		return report
	}
	report.Manifest, report.Exists = manifest, true
	_, report.ExecutionShadowCovered = executionShadowCoverage(manifest)
	report.LegacyCompatible = manifest.Version == legacyManifestV2
	report.HashMatches, report.SQLiteOK = true, true
	seen := map[string]bool{}
	for _, artifact := range manifest.Artifacts {
		if artifact.Status == "present" {
			if artifact.BundlePath == "" || seen[artifact.BundlePath] {
				report.Error = "manifest has empty or duplicate artifact path"
				report.HashMatches, report.SQLiteOK = false, false
				return report
			}
			seen[artifact.BundlePath] = true
		}
		ar := verifyArtifact(bundle, artifact)
		report.ArtifactReports = append(report.ArtifactReports, ar)
		if !ar.HashMatches {
			report.HashMatches = false
		}
		if (artifact.Role == "main-db" || artifact.Role == "archive-db" || artifact.Role == executionShadowRole) && !ar.QuickOK {
			report.SQLiteOK = false
		}
		if ar.Error != "" && (artifact.Required || artifact.Status == "present") && report.Error == "" {
			report.Error = ar.Error
		}
	}
	if !manifest.RequiredComplete || !report.HashMatches || !report.SQLiteOK {
		if report.Error == "" {
			report.Error = "bundle failed completeness, hash, or SQLite verification"
		}
		return report
	}
	drill, err := restoreCopyDrill(bundle, manifest.Artifacts)
	report.RestoreDrillOK = err == nil && drill.Passed
	if !report.RestoreDrillOK {
		report.Error = fmt.Sprintf("restore-copy drill failed: %v", err)
	}
	return report
}

// List is deliberately receipt-only and bounded. Full hashing/quick_check/restore copying is an
// offline Verify operation; the live research UI cannot trigger it by refreshing this list.
func (m *Manager) List() ([]VerifyReport, error) {
	if err := os.MkdirAll(m.root, 0o700); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(m.root)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, entry := range entries {
		if entry.IsDir() && validBundleName(entry.Name()) {
			names = append(names, entry.Name())
		}
	}
	sort.Sort(sort.Reverse(sort.StringSlice(names)))
	out := make([]VerifyReport, 0, len(names))
	for _, name := range names {
		bundle, _ := m.bundleDir(name)
		manifest, err := readManifest(bundle)
		report := VerifyReport{ManifestFile: filepath.ToSlash(filepath.Join(name, manifestFileName)),
			Exists: err == nil, VerificationMode: "immutable-creation-receipt"}
		if err != nil {
			report.Error = err.Error()
		} else {
			report.Manifest = manifest
			_, report.ExecutionShadowCovered = executionShadowCoverage(manifest)
			report.LegacyCompatible = manifest.Version == legacyManifestV2
			report.HashMatches = manifest.RequiredComplete && manifest.Drill.Passed
			report.SQLiteOK = manifest.QuickCheck == "ok"
			report.RestoreDrillOK = manifest.Drill.Passed
		}
		out = append(out, report)
	}
	return out, nil
}

// PlanRestore is documentation generated only after a fresh full verification. It cannot perform
// any step and never treats a bounded replay tail as a drop-in replacement for full replay history.
func (m *Manager) PlanRestore(name string) RestorePlan {
	report := m.Verify(name)
	plan := RestorePlan{Automatic: false, RequiresStopped: true, Snapshot: name,
		LiveDatabase: m.liveDB, Refusal: "This suite deliberately has no automatic or HTTP restore operation."}
	if report.Error != "" || !report.HashMatches || !report.SQLiteOK || !report.RestoreDrillOK {
		plan.Steps = []string{"Do not restore: full offline bundle verification failed.", "Create or select a verified recovery bundle."}
		return plan
	}
	if report.LegacyCompatible && !report.ExecutionShadowCovered {
		plan.Steps = []string{
			"LEGACY COMPATIBILITY ONLY: this verified version-2 bundle predates the required isolated execution-shadow database and is not a complete post-isolation recovery point.",
			"Stop the suite and ML child; verify no process is listening on port 8787.",
			"Quarantine the current main, archive, and execution-shadow databases plus WAL/SHM and current replay/ML files.",
			"Re-run the legacy bundle's per-artifact SHA-256, SQLite quick_check, typed checks, and temporary restore-copy drill.",
			"Restore only the legacy main and archive databases through one-filesystem temporary files and atomic renames; never combine them with a current execution-shadow database.",
			"Keep the current execution-shadow database quarantined and let the current suite create a fresh isolated ledger and migrate legacy shadow rows from the restored main database.",
			"Start the suite disarmed; verify the legacy migration receipt, schema, credentials, venue reachability, research collectors, replay provenance, and ledger counts.",
			"If any check fails, stop and restore the quarantined pre-drill files.",
		}
		return plan
	}
	plan.Steps = []string{
		"Stop the suite and ML child; verify no process is listening on port 8787.",
		"Copy live main/archive/execution-shadow databases plus WAL/SHM and current replay/ML files to a separate quarantine directory.",
		"Re-run the bundle's per-artifact SHA-256, SQLite quick_check, typed checks, and temporary restore-copy drill.",
		"With the suite still stopped, restore main, archive, and execution-shadow databases through one-filesystem temporary files and atomic renames; never copy WAL/SHM from the bundle.",
		"Restore ML artifacts only after matching their recorded hashes; treat a bounded replay tail as forensic evidence unless replay_selection.full_archive is true.",
		"Start the suite disarmed; verify schema migration, credentials, venue reachability, research collectors, replay provenance, and ledger counts.",
		"If any check fails, stop and restore the quarantined pre-drill files.",
	}
	return plan
}
