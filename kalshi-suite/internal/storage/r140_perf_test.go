package storage

import "testing"

func TestR140SQLiteMemoryAndConnectionBudget(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	var cacheKB, tempStore int
	if err := st.db.QueryRow(`PRAGMA cache_size`).Scan(&cacheKB); err != nil {
		t.Fatal(err)
	}
	if err := st.db.QueryRow(`PRAGMA temp_store`).Scan(&tempStore); err != nil {
		t.Fatal(err)
	}
	var mmapBytes int64
	if err := st.db.QueryRow(`PRAGMA mmap_size`).Scan(&mmapBytes); err != nil {
		t.Fatal(err)
	}
	if cacheKB != -32768 || mmapBytes != 268435456 || tempStore != 1 {
		t.Fatalf("sqlite memory budget cache_size=%d mmap_size=%d temp_store=%d, want -32768/268435456/FILE(1)", cacheKB, mmapBytes, tempStore)
	}
	if got := st.db.Stats().MaxOpenConnections; got != 8 {
		t.Fatalf("max open connections=%d, want 8", got)
	}
}
