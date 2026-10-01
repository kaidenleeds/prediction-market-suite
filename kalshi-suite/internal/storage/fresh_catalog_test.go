package storage

import "testing"

func TestFreshStoreCreatesMarketCatalogResearchIndex(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	var definition string
	err = store.db.QueryRow(`SELECT sql FROM sqlite_master WHERE type='index' AND name='idx_mcat_event_kind_close'`).Scan(&definition)
	if err != nil {
		t.Fatalf("fresh database is missing market catalog research index: %v", err)
	}
	if definition == "" {
		t.Fatal("market catalog research index has no definition")
	}
	for _, warning := range store.IndexWarnings {
		if warning != "" {
			t.Fatalf("fresh database reported an index warning: %s", warning)
		}
	}
}
