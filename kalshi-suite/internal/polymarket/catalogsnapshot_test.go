package polymarket

import (
	"path/filepath"
	"testing"
	"time"
)

func TestR133CompleteCatalogSnapshotIsSeparateHonestAndCopied(t *testing.T) {
	c := NewClient(time.Second)
	now := time.Now().Add(-time.Minute)
	c.mktMu.Lock()
	c.mktCache = []Market{{ConditionID: "partial"}}
	c.mktAt = time.Now()
	c.mktRefreshing = true
	c.mktMu.Unlock()
	c.evtMu.Lock()
	c.evtCache = []Market{{ConditionID: "complete-a"}, {ConditionID: "complete-b"}}
	c.evtAt = now
	c.evtRefreshing = true
	c.evtMu.Unlock()

	head := c.CachedMarketSnapshot()
	complete := c.CompleteCatalogSnapshot()
	if head.Complete || len(head.Markets) != 1 || !head.Refreshing {
		t.Fatalf("partial head mislabeled: %+v", head)
	}
	if !complete.Complete || len(complete.Markets) != 2 || !complete.Refreshing || !complete.At.Equal(now) {
		t.Fatalf("complete receipt wrong: %+v", complete)
	}
	complete.Markets[0].ConditionID = "mutated-copy"
	if got := c.CompleteCatalogSnapshot().Markets[0].ConditionID; got != "complete-a" {
		t.Fatalf("snapshot leaked mutable backing storage: %q", got)
	}
}

func TestR133CompleteCatalogPersistsAndWarmsColdRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "gamma-catalog.json")
	at := time.Now().Add(-time.Minute).UTC().Truncate(time.Millisecond)
	rows := []Market{{ConditionID: "persist-a", TokensRaw: `["ya","na"]`, Active: true, AcceptingOrders: true}}
	if err := persistCatalog(path, at, rows); err != nil {
		t.Fatal(err)
	}
	c := NewClient(time.Second)
	c.SetCatalogCachePath(path)
	got := c.CompleteCatalogSnapshot()
	if !got.Complete || len(got.Markets) != 1 || got.Markets[0].ConditionID != "persist-a" || !got.At.Equal(at) {
		t.Fatalf("cold restart did not restore last-good catalog: %+v", got)
	}
	c.clobMu.RLock()
	assets := append([]string(nil), c.clobAssets["persist-a"]...)
	c.clobMu.RUnlock()
	if len(assets) != 2 || assets[0] != "ya" || assets[1] != "na" {
		t.Fatalf("restored catalog did not warm token registry: %v", assets)
	}
}

func TestR133CompleteCatalogSnapshotNeverClaimsColdCache(t *testing.T) {
	c := NewClient(time.Second)
	c.evtMu.Lock()
	c.evtRefreshing = true
	c.evtMu.Unlock()
	got := c.CompleteCatalogSnapshot()
	if got.Complete || len(got.Markets) != 0 || !got.Refreshing {
		t.Fatalf("cold/in-flight crawl mislabeled complete: %+v", got)
	}
}
