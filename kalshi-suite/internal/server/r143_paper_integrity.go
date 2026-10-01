package server

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// quarantineAllNULJSON preserves an unrecoverable crash artifact and removes it from the active
// ledger path.  It is deliberately narrower than generic JSON recovery: only a non-empty file
// whose every byte was NUL reaches this helper (readJSONLoose already trimmed and checked it).
func (s *Server) quarantineAllNULJSON(path string, size int) {
	recovery := filepath.Join(filepath.Dir(path), "recovery")
	if err := os.MkdirAll(recovery, 0o755); err != nil {
		s.log.Error("all-NUL JSON ledger detected but recovery directory could not be created", "path", path, "err", err)
		return
	}
	stamp := time.Now().UTC().Format("20060102T150405.000000000Z")
	dest := filepath.Join(recovery, filepath.Base(path)+".all-nul."+stamp)
	if err := os.Rename(path, dest); err != nil {
		// Another concurrent reader may already have quarantined it.  A missing source is success.
		if _, statErr := os.Stat(path); os.IsNotExist(statErr) {
			return
		}
		s.log.Error("all-NUL JSON ledger quarantine failed", "path", path, "err", err)
		return
	}
	s.log.Error("all-NUL JSON ledger quarantined; starting a fresh paper epoch", "path", path, "bytes", size, "recovery", dest)
	if s.store != nil {
		_ = s.store.Audit(context.Background(), "error", "paper",
			fmt.Sprintf("R143 recovered all-NUL crash artifact %s (%d bytes): preserved under data\\recovery; active ledger starts empty and requires Reset P&L", filepath.Base(path), size), "")
	}
}

type paperLedgerIntegrity struct {
	OK      bool
	Healthy int
	Missing int
	Broken  []string
}

// paperLedgerIntegrityView is a cheap disk check for the native strategy ledgers that can spend
// one of the four Paper portfolios.  Missing files are valid fresh ledgers.  Existing files must
// contain a JSON object; poison flags catch a parse failure already observed by a typed loader.
func (s *Server) paperLedgerIntegrityView() paperLedgerIntegrity {
	names := []string{
		"kflow_books.json", "rawflow_book.json", "weather_book.json", "freshinv_book.json",
		"xvgap_book.json", "favlong80_book.json", "cheapband_book.json", "genfollow_book.json",
		"xvcombo_book.json", "xvlock_book.json", "ml_paper.json",
	}
	out := paperLedgerIntegrity{OK: true}
	for _, name := range names {
		path := filepath.Join(s.cfg().DataDir, name)
		b, err := os.ReadFile(path)
		if os.IsNotExist(err) {
			out.Missing++
			continue
		}
		if err != nil {
			out.Broken = append(out.Broken, name+": unreadable")
			continue
		}
		trimmed := strings.TrimRight(string(b), "\x00")
		var obj map[string]json.RawMessage
		if strings.TrimSpace(trimmed) == "" || json.Unmarshal([]byte(trimmed), &obj) != nil || obj == nil {
			out.Broken = append(out.Broken, name+": invalid JSON")
			continue
		}
		out.Healthy++
	}
	if s.kfPoisoned.Load() || s.rfPoisoned.Load() || s.wxBookPoisoned.Load() ||
		s.fiBookPoisoned.Load() || s.xvgBookPoisoned.Load() || s.flBookPoisoned.Load() ||
		s.cbBookPoisoned.Load() || s.gfBookPoisoned.Load() || s.xvlPoisoned.Load() {
		out.Broken = append(out.Broken, "one or more native ledgers are quarantined in memory")
	}
	out.OK = len(out.Broken) == 0
	return out
}
