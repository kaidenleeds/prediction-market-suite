package storage

// P3 tests (audit §6/§7): the kv latch store — schema creation on a fresh DB, upsert semantics,
// prefix reload (with LIKE-metacharacter keys), and age-based pruning. Uses a real temp-dir DB so
// the full Open() path (schema.sql + migrations + indexes) is exercised too.

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func openTemp(t *testing.T) (*Store, string) {
	t.Helper()
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s, dir
}

func TestKVRoundtrip(t *testing.T) {
	s, _ := openTemp(t)
	ctx := context.Background()

	// upsert: second Set wins
	if err := s.KVSet(ctx, "gate_exec|buy|kalshi|T1|YES", "1"); err != nil {
		t.Fatalf("KVSet: %v", err)
	}
	if err := s.KVSet(ctx, "gate_exec|buy|kalshi|T1|YES", "2"); err != nil {
		t.Fatalf("KVSet upsert: %v", err)
	}
	if err := s.KVSet(ctx, "bridge_seen|0xabc", "2026-07-02T10:00:00Z"); err != nil {
		t.Fatalf("KVSet: %v", err)
	}

	// prefix reload returns only that family, with the prefix stripped; '_' in the
	// prefix must be treated literally (LIKE-escaped), not as a wildcard.
	m, err := s.KVPrefix(ctx, "gate_exec|")
	if err != nil {
		t.Fatalf("KVPrefix: %v", err)
	}
	if len(m) != 1 || m["buy|kalshi|T1|YES"] != "2" {
		t.Fatalf("prefix reload wrong: %#v", m)
	}

	// delete removes exactly one key
	if err := s.KVDel(ctx, "gate_exec|buy|kalshi|T1|YES"); err != nil {
		t.Fatalf("KVDel: %v", err)
	}
	if m, _ := s.KVPrefix(ctx, "gate_exec|"); len(m) != 0 {
		t.Fatalf("delete failed: %#v", m)
	}
	if m, _ := s.KVPrefix(ctx, "bridge_seen|"); len(m) != 1 {
		t.Fatalf("other family must be untouched: %#v", m)
	}
}

func TestKVPruneOlder(t *testing.T) {
	s, _ := openTemp(t)
	ctx := context.Background()
	if err := s.KVSet(ctx, "bridge_seen|old", "x"); err != nil {
		t.Fatalf("KVSet: %v", err)
	}
	// rows are stamped now — a future cutoff prunes them, a past cutoff keeps them
	if err := s.KVPruneOlder(ctx, "bridge_seen|", time.Now().Add(-time.Hour)); err != nil {
		t.Fatalf("KVPruneOlder: %v", err)
	}
	if m, _ := s.KVPrefix(ctx, "bridge_seen|"); len(m) != 1 {
		t.Fatal("past cutoff must keep fresh rows")
	}
	if err := s.KVPruneOlder(ctx, "bridge_seen|", time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("KVPruneOlder: %v", err)
	}
	if m, _ := s.KVPrefix(ctx, "bridge_seen|"); len(m) != 0 {
		t.Fatal("future cutoff must prune")
	}
}

// A fresh DB must pass its own integrity check (the backup gate relies on this).
func TestQuickCheckFreshDB(t *testing.T) {
	s, dir := openTemp(t)
	_ = s.Close() // release the writer before the read-only quick_check re-open
	if err := QuickCheck(filepath.Join(dir, "kalshi.db")); err != nil {
		t.Fatalf("QuickCheck on a fresh DB: %v", err)
	}
}
