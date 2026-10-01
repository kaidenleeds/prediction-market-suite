package server

// R166 clean specialist-Paper epochs.
//
// The older kfBook reset convention only moved the display baselines and left every unresolved
// lot in Open. That was not a clean portfolio reset: old exposure still consumed the new grant,
// still blocked conflicts, and its eventual result landed in the new epoch. This file gives every
// kfBook-backed ledger one durable boundary. Pre-reset opens are copied to an append-only archive
// before the canonical JSON is atomically replaced; the live ledger then starts flat while its
// lifetime closed evidence and aggregate counters remain intact.

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	kfPaperResetArchiveFile   = "paper_kf_reset_archive.jsonl"
	kfPaperResetArchiveSchema = "kf-paper-reset-archive-v1"
)

// One process owns these JSON ledgers. The extra mutex keeps snapshot/replace/commit records for
// two concurrently requested resets from interleaving in the shared append-only archive.
var kfPaperResetArchiveMu sync.Mutex

type kfPaperResetArchiveRecord struct {
	Schema        string  `json:"schema"`
	Record        string  `json:"record"` // snapshot | canonical-commit
	Epoch         string  `json:"epoch"`
	ResetAt       string  `json:"reset_at"`
	Ledger        string  `json:"ledger"`
	SubBook       string  `json:"sub_book"`
	Open          []kfPos `json:"open,omitempty"`
	OpenCount     int     `json:"open_count"`
	LifetimeNet   float64 `json:"lifetime_net"`
	LifetimeWins  int     `json:"lifetime_wins"`
	LifetimeLoss  int     `json:"lifetime_losses"`
	PriorNetBase  float64 `json:"prior_net_base"`
	PriorWinBase  int     `json:"prior_wins_base"`
	PriorLossBase int     `json:"prior_losses_base"`
}

type kfLedgerResetResult struct {
	Ledger       string
	SubBooks     int
	ArchivedOpen []kfPos
	Committed    bool
}

type kfPaperResetResult struct {
	Epoch           string
	Ledgers         int
	SubBooks        int
	ArchivedOpen    int
	CanceledPending int
}

func normalizeKFResetAt(resetAt time.Time) time.Time {
	if resetAt.IsZero() {
		resetAt = time.Now()
	}
	return resetAt.UTC()
}

func kfResetEpoch(resetAt time.Time) string {
	return "paper-reset-" + normalizeKFResetAt(resetAt).Format("20060102T150405.000000000Z")
}

// kfCurrentEpochStats is the only reset-relative aggregate calculation for a kfBook display.
// Lifetime counters can survive repair/migration oddities, so count deltas fail closed at zero.
func kfCurrentEpochStats(b *kfBook) (net float64, wins, losses int) {
	if b == nil {
		return 0, 0, 0
	}
	net = b.Net - b.NetBase
	wins, losses = b.Wins-b.WinsBase, b.Losses-b.LossesBase
	if wins < 0 {
		wins = 0
	}
	if losses < 0 {
		losses = 0
	}
	return net, wins, losses
}

// prepareKFBookCleanEpoch is deliberately pure. The caller persists the enclosing ledger before
// installing next in memory. Closed rows and lifetime aggregates survive; unresolved prior-epoch
// exposure moves only to the explicit reset archive.
func prepareKFBookCleanEpoch(b *kfBook, ledger, sub string, resetAt time.Time) (next kfBook, rec kfPaperResetArchiveRecord) {
	resetAt = normalizeKFResetAt(resetAt)
	if b != nil {
		next = *b
		next.Closed = append([]kfClosed(nil), b.Closed...)
	}
	priorOpen := append([]kfPos(nil), next.Open...)
	rec = kfPaperResetArchiveRecord{
		Schema: kfPaperResetArchiveSchema, Record: "snapshot", Epoch: kfResetEpoch(resetAt),
		ResetAt: resetAt.Format(time.RFC3339Nano), Ledger: ledger, SubBook: sub,
		Open: priorOpen, OpenCount: len(priorOpen), LifetimeNet: next.Net,
		LifetimeWins: next.Wins, LifetimeLoss: next.Losses,
		PriorNetBase: next.NetBase, PriorWinBase: next.WinsBase, PriorLossBase: next.LossesBase,
	}
	next.Open = nil
	next.NetBase, next.WinsBase, next.LossesBase = next.Net, next.Wins, next.Losses
	next.Equity = nil
	return next, rec
}

func appendKFResetArchive(path string, rows []kfPaperResetArchiveRecord) error {
	if len(rows) == 0 {
		return nil
	}
	if err := validateKFResetArchive(path); err != nil {
		return err
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	for _, row := range rows {
		if err := enc.Encode(row); err != nil {
			return fmt.Errorf("encode specialist reset archive: %w", err)
		}
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("open specialist reset archive: %w", err)
	}
	defer f.Close()
	if _, err := f.Write(buf.Bytes()); err != nil {
		return fmt.Errorf("append specialist reset archive: %w", err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("sync specialist reset archive: %w", err)
	}
	return nil
}

// A corrupt append-only archive is never silently extended. That would make a later reader treat
// the new reset as trustworthy even though the older boundary was unreadable.
func validateKFResetArchive(path string) error {
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("open existing specialist reset archive: %w", err)
	}
	defer f.Close()
	scan := bufio.NewScanner(f)
	scan.Buffer(make([]byte, 64*1024), 32*1024*1024)
	line := 0
	for scan.Scan() {
		line++
		if len(bytes.TrimSpace(scan.Bytes())) == 0 {
			continue
		}
		var row kfPaperResetArchiveRecord
		if err := json.Unmarshal(scan.Bytes(), &row); err != nil || row.Schema != kfPaperResetArchiveSchema ||
			(row.Record != "snapshot" && row.Record != "canonical-commit") || row.Epoch == "" || row.ResetAt == "" || row.Ledger == "" {
			if err == nil {
				err = errors.New("invalid reset archive envelope")
			}
			return fmt.Errorf("specialist reset archive is poisoned at line %d: %w", line, err)
		}
	}
	if err := scan.Err(); err != nil {
		return fmt.Errorf("read specialist reset archive: %w", err)
	}
	return nil
}

// commitKFResetLedger is a conservative two-file transaction. Snapshot rows are synced first,
// then the canonical JSON is atomically replaced, then small commit receipts are appended. A
// failed replace therefore never loses the old open lots: they remain both in the old canonical
// file and in a clearly uncommitted snapshot. committed reports whether the canonical replace
// happened even if the final receipt append itself failed.
func (s *Server) commitKFResetLedger(file string, next any, rows []kfPaperResetArchiveRecord) (committed bool, err error) {
	raw, err := json.Marshal(next)
	if err != nil {
		return false, fmt.Errorf("marshal %s reset: %w", file, err)
	}
	path := filepath.Join(s.cfg().DataDir, file)
	tmp := path + ".reset.tmp"
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return false, fmt.Errorf("write %s reset temp: %w", file, err)
	}
	removeTemp := true
	defer func() {
		if removeTemp {
			_ = os.Remove(tmp)
		}
	}()

	kfPaperResetArchiveMu.Lock()
	defer kfPaperResetArchiveMu.Unlock()
	archivePath := filepath.Join(s.cfg().DataDir, kfPaperResetArchiveFile)
	if err := appendKFResetArchive(archivePath, rows); err != nil {
		return false, err
	}
	if err := os.Rename(tmp, path); err != nil {
		return false, fmt.Errorf("replace %s at clean epoch: %w", file, err)
	}
	removeTemp, committed = false, true
	commits := make([]kfPaperResetArchiveRecord, 0, len(rows))
	for _, row := range rows {
		row.Record = "canonical-commit"
		row.Open = nil
		commits = append(commits, row)
	}
	if err := appendKFResetArchive(archivePath, commits); err != nil {
		return true, err
	}
	return true, nil
}

func (s *Server) resetKflowBooksAt(resetAt time.Time) (kfLedgerResetResult, error) {
	s.kfBookMu.Lock()
	defer s.kfBookMu.Unlock()
	b := s.kfLoadLocked()
	if s.kfPoisoned.Load() {
		return kfLedgerResetResult{}, errors.New("kflow ledger is poisoned")
	}
	next := *b
	var preRec, liveRec kfPaperResetArchiveRecord
	next.Pre, preRec = prepareKFBookCleanEpoch(&b.Pre, "kflow_books.json", "pre", resetAt)
	next.Live, liveRec = prepareKFBookCleanEpoch(&b.Live, "kflow_books.json", "live", resetAt)
	rows := []kfPaperResetArchiveRecord{preRec, liveRec}
	result := kfLedgerResetResult{Ledger: "kflow_books.json", SubBooks: 2,
		ArchivedOpen: append(append([]kfPos(nil), rows[0].Open...), rows[1].Open...)}
	committed, err := s.commitKFResetLedger(result.Ledger, &next, rows)
	result.Committed = committed
	if committed {
		*b, s.kfDirty = next, false
	}
	return result, err
}

func (s *Server) resetRawFlowBookAt(resetAt time.Time) (kfLedgerResetResult, error) {
	s.rfBookMu.Lock()
	defer s.rfBookMu.Unlock()
	b := s.rfLoadLocked()
	if s.rfPoisoned.Load() {
		return kfLedgerResetResult{}, errors.New("rawflow ledger is poisoned")
	}
	next, rec := prepareKFBookCleanEpoch(b, "rawflow_book.json", "rawflow", resetAt)
	next.Bank = s.rawFlowSeedBank()
	result := kfLedgerResetResult{Ledger: "rawflow_book.json", SubBooks: 1, ArchivedOpen: rec.Open}
	committed, err := s.commitKFResetLedger(result.Ledger, &next, []kfPaperResetArchiveRecord{rec})
	result.Committed = committed
	if committed {
		*b, s.rfDirty = next, false
	}
	return result, err
}

func (s *Server) resetWeatherBookAt(resetAt time.Time) (kfLedgerResetResult, error) {
	s.wxBookMu.Lock()
	defer s.wxBookMu.Unlock()
	b := s.wxBookLoadLocked()
	if s.wxBookPoisoned.Load() {
		return kfLedgerResetResult{}, errors.New("weather ledger is poisoned")
	}
	next, rec := prepareKFBookCleanEpoch(b, "weather_book.json", "weather", resetAt)
	next.Bank = s.weatherSeedBank()
	result := kfLedgerResetResult{Ledger: "weather_book.json", SubBooks: 1, ArchivedOpen: rec.Open}
	committed, err := s.commitKFResetLedger(result.Ledger, &next, []kfPaperResetArchiveRecord{rec})
	result.Committed = committed
	if committed {
		*b, s.wxBookDirty = next, false
	}
	return result, err
}

func (s *Server) resetFreshInvBookAt(resetAt time.Time) (kfLedgerResetResult, error) {
	s.fiBookMu.Lock()
	defer s.fiBookMu.Unlock()
	b := s.fiLoadLocked()
	if s.fiBookPoisoned.Load() {
		return kfLedgerResetResult{}, errors.New("freshinv ledger is poisoned")
	}
	next, rec := prepareKFBookCleanEpoch(b, "freshinv_book.json", "freshinv", resetAt)
	result := kfLedgerResetResult{Ledger: "freshinv_book.json", SubBooks: 1, ArchivedOpen: rec.Open}
	committed, err := s.commitKFResetLedger(result.Ledger, &next, []kfPaperResetArchiveRecord{rec})
	result.Committed = committed
	if committed {
		*b, s.fiBookDirty = next, false
	}
	return result, err
}

func (s *Server) resetXvgBookAt(resetAt time.Time) (kfLedgerResetResult, error) {
	s.xvgBookMu.Lock()
	defer s.xvgBookMu.Unlock()
	b := s.xvgLoadLocked()
	if s.xvgBookPoisoned.Load() {
		return kfLedgerResetResult{}, errors.New("xvgap ledger is poisoned")
	}
	next, rec := prepareKFBookCleanEpoch(b, "xvgap_book.json", "xvgap", resetAt)
	result := kfLedgerResetResult{Ledger: "xvgap_book.json", SubBooks: 1, ArchivedOpen: rec.Open}
	committed, err := s.commitKFResetLedger(result.Ledger, &next, []kfPaperResetArchiveRecord{rec})
	result.Committed = committed
	if committed {
		*b, s.xvgBookDirty = next, false
	}
	return result, err
}

func (s *Server) resetFavLong80BookAt(resetAt time.Time) (kfLedgerResetResult, error) {
	s.flBookMu.Lock()
	defer s.flBookMu.Unlock()
	b := s.flLoadLocked()
	if s.flBookPoisoned.Load() {
		return kfLedgerResetResult{}, errors.New("favlong80 ledger is poisoned")
	}
	next, rec := prepareKFBookCleanEpoch(b, "favlong80_book.json", "favlong80", resetAt)
	result := kfLedgerResetResult{Ledger: "favlong80_book.json", SubBooks: 1, ArchivedOpen: rec.Open}
	committed, err := s.commitKFResetLedger(result.Ledger, &next, []kfPaperResetArchiveRecord{rec})
	result.Committed = committed
	if committed {
		*b, s.flBookDirty = next, false
	}
	return result, err
}

func (s *Server) resetCheapbandBooksAt(resetAt time.Time) (kfLedgerResetResult, error) {
	s.cbBookMu.Lock()
	defer s.cbBookMu.Unlock()
	b := s.cbLoadLocked()
	if s.cbBookPoisoned.Load() {
		return kfLedgerResetResult{}, errors.New("cheapband ledger is poisoned")
	}
	next := *b
	var kr, pr kfPaperResetArchiveRecord
	next.K, kr = prepareKFBookCleanEpoch(&b.K, "cheapband_book.json", "kalshi", resetAt)
	next.P, pr = prepareKFBookCleanEpoch(&b.P, "cheapband_book.json", "polyus", resetAt)
	rows := []kfPaperResetArchiveRecord{kr, pr}
	result := kfLedgerResetResult{Ledger: "cheapband_book.json", SubBooks: 2,
		ArchivedOpen: append(append([]kfPos(nil), kr.Open...), pr.Open...)}
	committed, err := s.commitKFResetLedger(result.Ledger, &next, rows)
	result.Committed = committed
	if committed {
		*b, s.cbBookDirty = next, false
	}
	return result, err
}

func (s *Server) resetGenfollowBooksAt(resetAt time.Time) (kfLedgerResetResult, error) {
	s.gfBookMu.Lock()
	defer s.gfBookMu.Unlock()
	b := s.gfLoadLocked()
	if s.gfBookPoisoned.Load() {
		return kfLedgerResetResult{}, errors.New("genfollow ledger is poisoned")
	}
	next := gfBookState{Subs: make(map[string]*kfBook, len(b.Subs))}
	keys := make([]string, 0, len(b.Subs))
	for key := range b.Subs {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	rows := make([]kfPaperResetArchiveRecord, 0, len(keys))
	result := kfLedgerResetResult{Ledger: "genfollow_book.json", SubBooks: len(keys)}
	for _, key := range keys {
		n, rec := prepareKFBookCleanEpoch(b.Subs[key], result.Ledger, key, resetAt)
		next.Subs[key] = &n
		rows = append(rows, rec)
		result.ArchivedOpen = append(result.ArchivedOpen, rec.Open...)
	}
	// Even an empty roster receives an explicit epoch marker and a canonical clean file.
	if len(rows) == 0 {
		resetAt = normalizeKFResetAt(resetAt)
		rows = append(rows, kfPaperResetArchiveRecord{Schema: kfPaperResetArchiveSchema,
			Record: "snapshot", Epoch: kfResetEpoch(resetAt), ResetAt: resetAt.Format(time.RFC3339Nano),
			Ledger: result.Ledger, SubBook: "__empty__"})
	}
	committed, err := s.commitKFResetLedger(result.Ledger, &next, rows)
	result.Committed = committed
	if committed {
		*b, s.gfBookDirty = next, false
	}
	return result, err
}

func (s *Server) finishKFArchivedLots(ctx context.Context, lots []kfPos) error {
	if s.store == nil || len(lots) == 0 {
		return nil
	}
	ctx = context.WithoutCancel(ctx)
	var errs []error
	for _, lot := range lots {
		if id := strings.TrimSpace(lot.RelationReceiptID); id != "" {
			if _, err := s.store.RecordFundedRelationCancellation(ctx, id, "specialist Paper lot archived by clean epoch reset"); err != nil {
				errs = append(errs, fmt.Errorf("cancel relation %s: %w", id, err))
			}
		}
		if id := strings.TrimSpace(lot.PlacementID); id != "" {
			if _, err := s.store.QuarantineInversePaperPlacement(ctx, id, "valid inverse Paper lot archived by operator reset"); err != nil {
				errs = append(errs, fmt.Errorf("archive inverse placement %s: %w", id, err))
			}
		}
	}
	return errors.Join(errs...)
}

func (s *Server) finishKFResetResult(ctx context.Context, res kfLedgerResetResult, resetErr error) error {
	if !res.Committed {
		return resetErr
	}
	return errors.Join(resetErr, s.finishKFArchivedLots(ctx, res.ArchivedOpen))
}

func (s *Server) cancelKFSpecialistPendingMakers(ctx context.Context) (int, error) {
	target := map[string]bool{"rawflow": true, "weather": true, "freshinv": true, "favlong80": true}
	s.pendMu.Lock()
	keep := s.pendingMakers[:0]
	var canceled []*pendingMaker
	for _, p := range s.pendingMakers {
		if p != nil && target[p.book] {
			canceled = append(canceled, p)
			continue
		}
		keep = append(keep, p)
	}
	s.pendingMakers = keep
	s.pendMu.Unlock()
	if s.store == nil {
		return len(canceled), nil
	}
	ctx = context.WithoutCancel(ctx)
	var errs []error
	for _, p := range canceled {
		if err := s.store.MarkMakerExpiredRule(ctx, p.id, "paper-reset-epoch"); err != nil {
			errs = append(errs, fmt.Errorf("cancel pending maker %d: %w", p.id, err))
		}
		if p.relationReceiptID != "" {
			if _, err := s.store.RecordFundedRelationCancellation(ctx, p.relationReceiptID, "maker canceled by clean Paper reset"); err != nil {
				errs = append(errs, fmt.Errorf("cancel pending relation %s: %w", p.relationReceiptID, err))
			}
		}
	}
	return len(canceled), errors.Join(errs...)
}

// resetAllKFSpecialistBooks is the one integration point for Reset P&L. It deliberately runs for
// disabled, frozen, retired, and empty books too: those flags control new placements, not whether
// old Paper exposure is allowed to leak into a newly requested portfolio epoch.
func (s *Server) resetAllKFSpecialistBooks(ctx context.Context, resetAt time.Time) (kfPaperResetResult, error) {
	resetAt = normalizeKFResetAt(resetAt)
	out := kfPaperResetResult{Epoch: kfResetEpoch(resetAt)}
	steps := []func(time.Time) (kfLedgerResetResult, error){
		s.resetKflowBooksAt,
		s.resetRawFlowBookAt,
		s.resetWeatherBookAt,
		s.resetFreshInvBookAt,
		s.resetXvgBookAt,
		s.resetFavLong80BookAt,
		s.resetCheapbandBooksAt,
		s.resetGenfollowBooksAt,
	}
	var errs []error
	for _, step := range steps {
		res, err := step(resetAt)
		if res.Committed {
			out.Ledgers++
			out.SubBooks += res.SubBooks
			out.ArchivedOpen += len(res.ArchivedOpen)
			if ferr := s.finishKFArchivedLots(ctx, res.ArchivedOpen); ferr != nil {
				errs = append(errs, fmt.Errorf("%s reset follow-through: %w", res.Ledger, ferr))
			}
		}
		if err != nil {
			errs = append(errs, err)
		}
	}
	var pendingErr error
	out.CanceledPending, pendingErr = s.cancelKFSpecialistPendingMakers(ctx)
	if pendingErr != nil {
		errs = append(errs, pendingErr)
	}
	s.invalidatePortfolioEquityCache()
	if s.store != nil {
		level, msg := "info", fmt.Sprintf("reset P&L: %d specialist Paper ledgers / %d sub-books started flat; %d old opens archived; %d resting posts canceled; lifetime evidence kept",
			out.Ledgers, out.SubBooks, out.ArchivedOpen, out.CanceledPending)
		if len(errs) > 0 {
			level, msg = "error", msg+"; one or more ledger reset steps failed: "+errors.Join(errs...).Error()
		}
		_ = s.store.Audit(context.WithoutCancel(ctx), level, "paper", msg, out.Epoch)
	}
	return out, errors.Join(errs...)
}
