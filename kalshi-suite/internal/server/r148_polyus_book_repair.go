package server

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

const r148PolyUSSettlementRegrade = "R148 PolyUS settlement provenance regrade"

// r148ClosedAt accepts both the JSON books' RFC3339 stamps and RFC3339Nano. An unreadable close
// time cannot prove that a row happened after a later authoritative receipt, so it remains in the
// repair set (ClosureNeedsRepair treats the zero time as untrusted).
func r148ClosedAt(raw string) time.Time {
	raw = strings.TrimSpace(raw)
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
		if parsed, err := time.Parse(layout, raw); err == nil {
			return parsed.UTC()
		}
	}
	return time.Time{}
}

func r148KFPositionKey(p kfPos) string {
	if id := strings.TrimSpace(p.PlacementID); id != "" {
		return "placement:" + id
	}
	return strings.Join([]string{strings.ToLower(fiLotPlatform(p)), strings.TrimSpace(p.Ticker),
		strings.ToUpper(strings.TrimSpace(p.Side)), strings.TrimSpace(p.TS),
		fmt.Sprintf("%.12g", p.Price), fmt.Sprintf("%.12g", p.Contracts)}, "\x00")
}

func r148RemoveTerminalSettleMark(p *kfPos) {
	if n := len(p.Marks); n > 0 && strings.EqualFold(strings.TrimSpace(p.Marks[n-1].Src), "settle") {
		p.Marks = p.Marks[:n-1]
	}
}

func r148SettlementPnL(c kfClosed) float64 {
	return c.Contracts*(c.Payout-c.Price) - c.Fee
}

func r148KFHistoryTrimmed(b *kfBook) bool {
	return b != nil && b.SettlementEpoch != r148SettlementCleanEpoch && b.Wins+b.Losses > len(b.Closed)
}

func (s *Server) r148AuditCleanEpoch(ctx context.Context, ledger, detail string) {
	_ = s.store.Audit(context.WithoutCancel(ctx), "warn", "settlement",
		"R148 started a clean funded-ledger epoch; unverifiable legacy aggregate history is archived and excluded",
		ledger+": "+detail)
}

const r148SettlementCleanEpoch = "polyus-final-v2-clean-epoch-1"

// r148ArchiveFundedJSONCleanEpoch preserves the complete pre-mutation file twice: in an immutable
// SQLite quarantine table (the authority copy) and in a hash-named operator-readable archive.
// Tests may construct an in-memory book without a file; in that case the exact in-memory value is
// marshaled before mutation and receives the same durable treatment.
func (s *Server) r148ArchiveFundedJSONCleanEpoch(ctx context.Context, ledger, path string, value any) (string, error) {
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		raw, err = json.Marshal(value)
	}
	if err != nil {
		return "", err
	}
	if !json.Valid(raw) {
		return "", fmt.Errorf("%s pre-clean-epoch source is not valid JSON", ledger)
	}
	hash, _, err := s.store.QuarantinePolyUSFundedJSONEpoch(ctx, ledger, raw)
	if err != nil {
		return "", err
	}
	archive := path + ".r148-pre-clean-epoch-" + hash[:12] + ".json"
	if prior, readErr := os.ReadFile(archive); readErr == nil {
		priorHash := fmt.Sprintf("%x", sha256.Sum256(prior))
		if priorHash != hash {
			return "", fmt.Errorf("%s clean-epoch archive hash mismatch", ledger)
		}
	} else if errors.Is(readErr, os.ErrNotExist) {
		if err := r148WriteJSONBytes(archive, raw); err != nil {
			return "", err
		}
	} else {
		return "", readErr
	}
	return hash, nil
}

func r148WriteJSONBytes(path string, raw []byte) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o444); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

func r148ResetKFBookCleanEpoch(b *kfBook, archiveSHA string) {
	if b == nil {
		return
	}
	b.Closed = nil
	b.Net, b.Wins, b.Losses = 0, 0, 0
	b.NetBase, b.WinsBase, b.LossesBase = 0, 0, 0
	b.Equity = [][2]float64{{float64(time.Now().Unix()), 0}}
	b.SettlementEpoch = r148SettlementCleanEpoch
	b.SettlementEpochAt = time.Now().UTC().Format(time.RFC3339Nano)
	b.SettlementLegacyArchiveSHA = archiveSHA
	b.SettlementLegacyExcluded = true
}

func r148ValidateKFCounters(bookName, sub string, b *kfBook, rows []kfClosed) error {
	wins, losses := 0, 0
	for _, row := range rows {
		if row.Won {
			wins++
		} else {
			losses++
		}
	}
	if b.Wins < wins || b.Losses < losses {
		return fmt.Errorf("%s/%s terminal counters cannot reverse %d wins/%d losses from %d/%d",
			bookName, sub, wins, losses, b.Wins, b.Losses)
	}
	return nil
}

// r148RepairKFBook quarantines the exact terminal rows before mutating the in-memory projection.
// A crash after the database phase is harmless: immutable INSERT OR IGNORE receipts and the
// settled->committed inverse-journal transition are both idempotent, and the next boot completes
// the same JSON projection repair.
func (s *Server) r148RepairKFBook(ctx context.Context, bookName, sub string, b *kfBook,
	states map[string]storage.PolyUSSettlementRepairState) (int, error) {
	if b == nil || len(b.Closed) == 0 {
		return 0, nil
	}
	bad := make([]kfClosed, 0, 2)
	for _, closed := range b.Closed {
		state, affected := states[strings.TrimSpace(closed.Ticker)]
		if !affected || fiLotPlatform(closed.kfPos) != "polyus" ||
			!strings.EqualFold(strings.TrimSpace(closed.Reason), "settled") ||
			!state.ClosureNeedsRepair(r148ClosedAt(closed.SettledTS)) {
			continue
		}
		bad = append(bad, closed)
	}
	if len(bad) == 0 {
		return 0, nil
	}
	if err := r148ValidateKFCounters(bookName, sub, b, bad); err != nil {
		return 0, err
	}
	for _, closed := range bad {
		raw, err := json.Marshal(closed)
		if err != nil {
			return 0, fmt.Errorf("marshal %s/%s PolyUS terminal row: %w", bookName, sub, err)
		}
		if _, err := s.store.QuarantinePolyUSKFSettlement(ctx, bookName, sub, closed.Ticker,
			closed.Side, closed.SettledTS, string(raw)); err != nil {
			return 0, fmt.Errorf("quarantine %s/%s PolyUS terminal row: %w", bookName, sub, err)
		}
		if id := strings.TrimSpace(closed.PlacementID); id != "" {
			reopened, err := s.store.ReopenInversePaperPlacementForSettlementRepair(ctx, id)
			if err != nil {
				return 0, fmt.Errorf("reopen inverse placement %s: %w", id, err)
			}
			if !reopened {
				journal, found, err := s.store.InversePaperPlacement(ctx, id)
				if err != nil {
					return 0, fmt.Errorf("read inverse placement %s: %w", id, err)
				}
				if !found || journal.State != "committed" || journal.Detail != r148PolyUSSettlementRegrade {
					return 0, fmt.Errorf("inverse placement %s was not safely reopened", id)
				}
			}
		}
	}

	badKeys := make(map[string]bool, len(bad))
	openKeys := make(map[string]bool, len(b.Open)+len(bad))
	for _, open := range b.Open {
		openKeys[r148KFPositionKey(open)] = true
	}
	for _, closed := range bad {
		key := r148KFPositionKey(closed.kfPos)
		badKeys[key+"\x00"+closed.SettledTS] = true
		open := closed.kfPos
		r148RemoveTerminalSettleMark(&open)
		if !openKeys[key] {
			b.Open = append(b.Open, open)
			openKeys[key] = true
		}
		pnl := r148SettlementPnL(closed)
		b.Net -= pnl
		if closed.Won {
			b.Wins--
		} else {
			b.Losses--
		}
		closedAt := r148ClosedAt(closed.SettledTS)
		if !s.pnlEpoch.IsZero() && !closedAt.IsZero() && closedAt.Before(s.pnlEpoch) {
			b.NetBase -= pnl
			if closed.Won {
				b.WinsBase--
			} else {
				b.LossesBase--
			}
		}
	}
	kept := b.Closed[:0]
	for _, closed := range b.Closed {
		if badKeys[r148KFPositionKey(closed.kfPos)+"\x00"+closed.SettledTS] {
			continue
		}
		kept = append(kept, closed)
	}
	b.Closed = kept
	b.Equity = append(b.Equity, [2]float64{float64(time.Now().Unix()), math.Round((b.Net-b.NetBase)*100) / 100})
	if len(b.Equity) > 400 {
		b.Equity = b.Equity[len(b.Equity)-400:]
	}
	return len(bad), nil
}

func r148WriteJSON(path string, value any) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	tmp := path + ".r148.tmp"
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

func (s *Server) r148RepairGenfollow(ctx context.Context, states map[string]storage.PolyUSSettlementRepairState) (int, error) {
	s.gfBookMu.Lock()
	defer s.gfBookMu.Unlock()
	if s.gfBookPoisoned.Load() {
		return 0, errors.New("genfollow ledger is poisoned")
	}
	b := s.gfLoadLocked()
	total := 0
	trimmedSubs := map[string]bool{}
	for sub, book := range b.Subs {
		trimmedSubs[sub] = strings.HasSuffix(strings.ToLower(strings.TrimSpace(sub)), "-p") && r148KFHistoryTrimmed(book)
	}
	archiveSHA := ""
	for _, trimmed := range trimmedSubs {
		if trimmed {
			var err error
			archiveSHA, err = s.r148ArchiveFundedJSONCleanEpoch(ctx, "genfollow_book.json",
				filepath.Join(s.cfg().DataDir, "genfollow_book.json"), b)
			if err != nil {
				s.gfBookPoisoned.Store(true)
				return 0, err
			}
			break
		}
	}
	cleaned := 0
	for sub, book := range b.Subs {
		n, err := s.r148RepairKFBook(ctx, "genfollow_book.json", sub, book, states)
		if err != nil {
			s.gfBookPoisoned.Store(true)
			return total, err
		}
		total += n
		if trimmedSubs[sub] {
			r148ResetKFBookCleanEpoch(book, archiveSHA)
			cleaned++
			s.r148AuditCleanEpoch(ctx, "genfollow_book.json/"+sub,
				"bounded close tail; Paper continues from zero with legacy history excluded from proof")
		}
	}
	if total == 0 && cleaned == 0 {
		return 0, nil
	}
	if err := r148WriteJSON(filepath.Join(s.cfg().DataDir, "genfollow_book.json"), b); err != nil {
		s.gfBookPoisoned.Store(true)
		return 0, err
	}
	s.gfBookDirty = false
	return total, nil
}

func (s *Server) r148RepairCheapband(ctx context.Context, states map[string]storage.PolyUSSettlementRepairState) (int, error) {
	s.cbBookMu.Lock()
	defer s.cbBookMu.Unlock()
	if s.cbBookPoisoned.Load() {
		return 0, errors.New("cheapband ledger is poisoned")
	}
	b := s.cbLoadLocked()
	trimmed := r148KFHistoryTrimmed(&b.P)
	archiveSHA := ""
	if trimmed {
		var archiveErr error
		archiveSHA, archiveErr = s.r148ArchiveFundedJSONCleanEpoch(ctx, "cheapband_book.json",
			filepath.Join(s.cfg().DataDir, "cheapband_book.json"), b)
		if archiveErr != nil {
			s.cbBookPoisoned.Store(true)
			return 0, archiveErr
		}
	}
	n, err := s.r148RepairKFBook(ctx, "cheapband_book.json", "polyus", &b.P, states)
	if err != nil {
		s.cbBookPoisoned.Store(true)
		return 0, err
	}
	if trimmed {
		r148ResetKFBookCleanEpoch(&b.P, archiveSHA)
		s.r148AuditCleanEpoch(ctx, "cheapband_book.json/polyus",
			"bounded close tail; Paper continues from zero with legacy history excluded from proof")
	}
	if n == 0 && !trimmed {
		return 0, nil
	}
	if err := r148WriteJSON(filepath.Join(s.cfg().DataDir, "cheapband_book.json"), b); err != nil {
		s.cbBookPoisoned.Store(true)
		return 0, err
	}
	s.cbBookDirty = false
	return n, nil
}

func (s *Server) r148RepairFreshInv(ctx context.Context, states map[string]storage.PolyUSSettlementRepairState) (int, error) {
	s.fiBookMu.Lock()
	defer s.fiBookMu.Unlock()
	if s.fiBookPoisoned.Load() {
		return 0, errors.New("freshinv ledger is poisoned")
	}
	b := s.fiLoadLocked()
	trimmed := r148KFHistoryTrimmed(b)
	archiveSHA := ""
	if trimmed {
		var archiveErr error
		archiveSHA, archiveErr = s.r148ArchiveFundedJSONCleanEpoch(ctx, "freshinv_book.json",
			filepath.Join(s.cfg().DataDir, "freshinv_book.json"), b)
		if archiveErr != nil {
			s.fiBookPoisoned.Store(true)
			return 0, archiveErr
		}
	}
	n, err := s.r148RepairKFBook(ctx, "freshinv_book.json", "", b, states)
	if err != nil {
		s.fiBookPoisoned.Store(true)
		return 0, err
	}
	if trimmed {
		r148ResetKFBookCleanEpoch(b, archiveSHA)
		s.r148AuditCleanEpoch(ctx, "freshinv_book.json",
			"mixed-venue bounded close tail; ledger restarted from zero and all legacy results excluded from proof")
	}
	if n == 0 && !trimmed {
		return 0, nil
	}
	if err := r148WriteJSON(filepath.Join(s.cfg().DataDir, "freshinv_book.json"), b); err != nil {
		s.fiBookPoisoned.Store(true)
		return 0, err
	}
	s.fiBookDirty = false
	return n, nil
}

func (s *Server) r148RepairXvGap(ctx context.Context, states map[string]storage.PolyUSSettlementRepairState) (int, error) {
	s.xvgBookMu.Lock()
	defer s.xvgBookMu.Unlock()
	if s.xvgBookPoisoned.Load() {
		return 0, errors.New("xvgap ledger is poisoned")
	}
	b := s.xvgLoadLocked()
	trimmed := r148KFHistoryTrimmed(b)
	archiveSHA := ""
	if trimmed {
		var archiveErr error
		archiveSHA, archiveErr = s.r148ArchiveFundedJSONCleanEpoch(ctx, "xvgap_book.json",
			filepath.Join(s.cfg().DataDir, "xvgap_book.json"), b)
		if archiveErr != nil {
			s.xvgBookPoisoned.Store(true)
			return 0, archiveErr
		}
	}
	n, err := s.r148RepairKFBook(ctx, "xvgap_book.json", "polyus", b, states)
	if err != nil {
		s.xvgBookPoisoned.Store(true)
		return 0, err
	}
	if trimmed {
		r148ResetKFBookCleanEpoch(b, archiveSHA)
		s.r148AuditCleanEpoch(ctx, "xvgap_book.json/polyus",
			"bounded close tail; Paper continues from zero with legacy history excluded from proof")
	}
	if n == 0 && !trimmed {
		return 0, nil
	}
	if err := r148WriteJSON(filepath.Join(s.cfg().DataDir, "xvgap_book.json"), b); err != nil {
		s.xvgBookPoisoned.Store(true)
		return 0, err
	}
	s.xvgBookDirty = false
	return n, nil
}

func r148XvLockBadLeg(op xvLockOpp, states map[string]storage.PolyUSSettlementRepairState) (string, string, bool) {
	closedAt := r148ClosedAt(op.SettledTS)
	for _, leg := range []struct{ venue, ticker, side string }{
		{op.AVenue, op.AID, op.ASide}, {op.BVenue, op.BID, op.BSide},
	} {
		state, affected := states[strings.TrimSpace(leg.ticker)]
		if affected && strings.EqualFold(strings.TrimSpace(leg.venue), "polyus") && state.ClosureNeedsRepair(closedAt) {
			return leg.ticker, leg.side, true
		}
	}
	return "", "", false
}

func (s *Server) r148RepairXvLock(ctx context.Context, states map[string]storage.PolyUSSettlementRepairState) (int, error) {
	s.xvlMu.Lock()
	defer s.xvlMu.Unlock()
	if s.xvlPoisoned.Load() {
		return 0, errors.New("xvlock ledger is poisoned")
	}
	b := s.xvlLoadLocked()
	trimmed := b.SettlementEpoch != r148SettlementCleanEpoch &&
		b.SeenTotal > int64(len(b.Open)+len(b.Closed))
	archiveSHA := ""
	if trimmed {
		var archiveErr error
		archiveSHA, archiveErr = s.r148ArchiveFundedJSONCleanEpoch(ctx, "xvlock_book.json",
			filepath.Join(s.cfg().DataDir, "xvlock_book.json"), b)
		if archiveErr != nil {
			s.xvlPoisoned.Store(true)
			return 0, archiveErr
		}
	}
	type badRow struct {
		op           xvLockOpp
		ticker, side string
	}
	bad := make([]badRow, 0, 2)
	for _, op := range b.Closed {
		if ticker, side, affected := r148XvLockBadLeg(op, states); affected {
			bad = append(bad, badRow{op: op, ticker: ticker, side: side})
		}
	}
	if len(bad) == 0 && !trimmed {
		return 0, nil
	}
	for _, row := range bad {
		raw, err := json.Marshal(row.op)
		if err != nil {
			return 0, err
		}
		if _, err := s.store.QuarantinePolyUSKFSettlement(ctx, "xvlock_book.json", row.op.Key,
			row.ticker, row.side, row.op.SettledTS, string(raw)); err != nil {
			return 0, err
		}
	}
	badKeys := make(map[string]bool, len(bad))
	for _, row := range bad {
		badKeys[row.op.Key+"\x00"+row.op.SettledTS] = true
		if row.op.Mismatch && b.MismatchN > 0 {
			b.MismatchN--
		}
		open := row.op
		if strings.EqualFold(open.AVenue, "polyus") && open.AID == row.ticker {
			open.AWon = -1
		}
		if strings.EqualFold(open.BVenue, "polyus") && open.BID == row.ticker {
			open.BWon = -1
		}
		open.RealizedC, open.StakedPnL = 0, 0
		open.Mismatch, open.PairQuarantined, open.SettledTS = false, false, ""
		b.Open = append(b.Open, open)
	}
	kept := b.Closed[:0]
	for _, op := range b.Closed {
		if !badKeys[op.Key+"\x00"+op.SettledTS] {
			kept = append(kept, op)
		}
	}
	b.Closed = kept
	// Remove a quarantine created only by a false closure. A separate surviving mismatch on the
	// same semantic pair recreates it immediately; older durable pairs unrelated to this repair
	// remain untouched.
	for _, row := range bad {
		pairKey := xvlOppPairKey(row.op)
		stillMismatch := false
		for _, op := range b.Closed {
			if op.Mismatch && xvlOppPairKey(op) == pairKey {
				stillMismatch = true
				break
			}
		}
		if !stillMismatch {
			delete(b.QuarantinedPairs, pairKey)
		}
	}
	if xvlReconcileQuarantines(b) {
		s.xvlDirty = true
	}
	if trimmed {
		b.Closed = nil
		b.SeenTotal = int64(len(b.Open))
		b.MismatchN = 0
		b.SettlementEpoch = r148SettlementCleanEpoch
		b.SettlementEpochAt = time.Now().UTC().Format(time.RFC3339Nano)
		b.SettlementLegacyArchiveSHA = archiveSHA
		b.SettlementLegacyExcluded = true
		s.r148AuditCleanEpoch(ctx, "xvlock_book.json",
			"bounded close tail; visible open routes retained and legacy results excluded from proof")
	}
	if err := r148WriteJSON(filepath.Join(s.cfg().DataDir, "xvlock_book.json"), b); err != nil {
		s.xvlPoisoned.Store(true)
		return 0, err
	}
	s.xvlDirty = false
	return len(bad), nil
}

// repairR148PolyUSFundedJSONBooks runs before the HTTP server and sidecar start. Every visible
// funded terminal projection derived from a quarantined receipt is returned to its open state;
// no executor may settle it again until a versioned final PolyUS receipt exists.
func (s *Server) repairR148PolyUSFundedJSONBooks(ctx context.Context) error {
	states, err := s.store.PolyUSSettlementRepairStates(ctx)
	if err != nil || len(states) == 0 {
		return err
	}
	total := 0
	for _, repair := range []struct {
		name string
		fn   func(context.Context, map[string]storage.PolyUSSettlementRepairState) (int, error)
	}{
		{"genfollow", s.r148RepairGenfollow},
		{"cheapband", s.r148RepairCheapband},
		{"freshinv", s.r148RepairFreshInv},
		{"xvgap", s.r148RepairXvGap},
		{"xvlock", s.r148RepairXvLock},
	} {
		n, repairErr := repair.fn(ctx, states)
		if repairErr != nil {
			return fmt.Errorf("%s funded settlement repair: %w", repair.name, repairErr)
		}
		total += n
	}
	if total > 0 {
		detail := fmt.Sprintf("reopened=%d; exact JSON rows preserved in immutable quarantine", total)
		if err := s.store.Audit(context.WithoutCancel(ctx), "warn", "settlement",
			"R148 reopened funded PolyUS JSON-book closures for authoritative regrade", detail); err != nil {
			return err
		}
	}
	return nil
}
