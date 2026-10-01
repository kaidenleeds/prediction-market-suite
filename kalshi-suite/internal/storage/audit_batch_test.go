package storage

import (
	"context"
	"testing"
)

func TestAuditBatchCommitsEveryEntryInOneCall(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	entries := []AuditEntry{
		{TS: "2026-07-16T00:00:00Z", Level: "info", Category: "fixture", Message: "one", Detail: `{"n":1}`},
		{TS: "2026-07-16T00:00:01Z", Level: "warn", Category: "fixture", Message: "two", Detail: `{"n":2}`},
	}
	if err := store.AuditBatch(context.Background(), entries); err != nil {
		t.Fatal(err)
	}
	rows, err := store.ListAudit(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0].Message != "two" || rows[1].Message != "one" {
		t.Fatalf("batch rows=%+v", rows)
	}
}

func TestAuditBatchHonorsCanceledContextWithoutPartialRows(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := store.AuditBatch(ctx, []AuditEntry{{Level: "info", Category: "fixture", Message: "nope"}}); err == nil {
		t.Fatal("canceled batch unexpectedly committed")
	}
	rows, err := store.ListAudit(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Fatalf("canceled batch left partial rows: %+v", rows)
	}
}
