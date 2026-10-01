package server

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// R166 temporarily retires every multi-leg Paper entry that still turns one point-in-time
// observation into a fill. The signal/research ledgers continue to collect, and existing positions
// continue to settle. A route may be re-enabled only after it observes a newer complete executable
// quote and records a terminal fill/zero-fill/not-observed result without inventing execution.
const r166VerifiedMultiLegPaperEntriesEnabled = false

const r166UnverifiedMultiLegPaperReason = "paper-not-observed: multi-leg route lacks delayed newer complete executable quote verification"

var r166MultiLegPaperTestBypass sync.Map

func (s *Server) r166CanBookVerifiedMultiLegPaper() bool {
	if r166VerifiedMultiLegPaperEntriesEnabled {
		return true
	}
	_, ok := r166MultiLegPaperTestBypass.Load(s)
	return ok
}

// r166AllowUnverifiedMultiLegPaperForTest keeps legacy execution-unit fixtures useful while the
// production collectors remain fail-closed. It is unexported and is called only by tests.
func r166AllowUnverifiedMultiLegPaperForTest(s *Server) {
	if s != nil {
		r166MultiLegPaperTestBypass.Store(s, struct{}{})
	}
}

// resetXvComboPaperAt starts the product-simulation ledger flat without deleting its evidence.
// Pre-reset opens move to an explicit history lane which settleXvComboBook continues to grade;
// those later results advance the lifetime baseline equally and therefore never enter current P&L.
func (s *Server) resetXvComboPaperAt(epoch time.Time) (int, error) {
	if s == nil {
		return 0, fmt.Errorf("xvcombo reset requires server")
	}
	if epoch.IsZero() {
		epoch = time.Now().UTC()
	} else {
		epoch = epoch.UTC()
	}
	s.xvcMu.Lock()
	defer s.xvcMu.Unlock()
	b := s.xvcLoadLocked()
	path := filepath.Join(s.cfg().DataDir, xvcFile)
	if raw, err := os.ReadFile(path); err == nil {
		var check xvcBook
		if json.Unmarshal(raw, &check) != nil {
			return 0, fmt.Errorf("xvcombo reset refused unreadable canonical ledger")
		}
	} else if !os.IsNotExist(err) {
		return 0, fmt.Errorf("read xvcombo canonical ledger: %w", err)
	}

	next := *b
	next.Open = append([]xvcPos(nil), b.Open...)
	next.ArchivedOpen = append([]xvcPos(nil), b.ArchivedOpen...)
	next.Closed = append([]xvcClosed(nil), b.Closed...)
	archived := len(next.Open)
	next.ArchivedOpen = append(next.ArchivedOpen, next.Open...)
	next.Open = nil
	next.ResetEpoch = epoch.Format(time.RFC3339Nano)
	next.NetBase, next.WinsBase, next.LossesBase = next.Net, next.Wins, next.Losses
	raw, err := json.Marshal(&next)
	if err != nil {
		return 0, fmt.Errorf("marshal xvcombo clean epoch: %w", err)
	}
	tmp := path + ".reset.tmp"
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return 0, fmt.Errorf("write xvcombo clean epoch: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return 0, fmt.Errorf("replace xvcombo clean epoch: %w", err)
	}
	*b = next
	s.xvcDirty = false
	s.invalidatePortfolioEquityCache()
	return archived, nil
}
