package storage

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestVenueNoticeVersionsAndSightingsAreImmutable(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	observed := time.Date(2026, 7, 11, 12, 1, 0, 0, time.UTC)
	base := VenueNotice{Venue: "polyus", SourceName: "polyus-changelog-rss", SourceID: "official-guid",
		Title: "v0.0.68 - Weekly maintenance window moved", Summary: "Thursday 2am-6am ET",
		PublishedTS: "2026-07-09T06:48:54Z", SourceURL: "https://docs.polymarket.us/changelog#july-9-2026",
		SourceFeed: "https://docs.polymarket.us/changelog/rss.xml", Severity: "attention",
		Categories: []string{"Maintenance", "Retail API"}, Classes: []string{"maintenance"}, ArtifactHash: "artifact-v1"}
	got, err := st.InsertVenueNoticeBatch(ctx, observed, []VenueNotice{base})
	if err != nil || got.Inserted != 1 || got.Versioned != 0 || got.Sightings != 1 {
		t.Fatalf("first insert = %#v, err=%v", got, err)
	}
	got, err = st.InsertVenueNoticeBatch(ctx, observed.Add(time.Minute), []VenueNotice{base})
	if err != nil || got.Duplicates != 1 || got.Sightings != 0 {
		t.Fatalf("same-slot dedupe = %#v, err=%v", got, err)
	}
	got, err = st.InsertVenueNoticeBatch(ctx, observed.Add(16*time.Minute), []VenueNotice{base})
	if err != nil || got.Duplicates != 1 || got.Sightings != 1 {
		t.Fatalf("later sighting = %#v, err=%v", got, err)
	}
	edited := base
	edited.Summary = "Thursday 2am-6am ET, effective July 9"
	edited.ArtifactHash = "artifact-v2"
	got, err = st.InsertVenueNoticeBatch(ctx, observed.Add(31*time.Minute), []VenueNotice{edited})
	if err != nil || got.Inserted != 1 || got.Versioned != 1 || got.Sightings != 1 {
		t.Fatalf("semantic edit = %#v, err=%v", got, err)
	}
	var versions, sightings, funded, paper, live int
	if err := st.db.QueryRow(`SELECT COUNT(*),SUM(funded),SUM(paper_authority),SUM(live_authority) FROM research_venue_notices`).
		Scan(&versions, &funded, &paper, &live); err != nil {
		t.Fatal(err)
	}
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM research_venue_notice_sightings`).Scan(&sightings); err != nil {
		t.Fatal(err)
	}
	if versions != 2 || sightings != 3 || funded != 0 || paper != 0 || live != 0 {
		t.Fatalf("versions=%d sightings=%d authority=%d/%d/%d", versions, sightings, funded, paper, live)
	}
	report, err := st.VenueNoticeReport(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if report["counts_by_venue"].(map[string]int)["polyus"] != 1 ||
		report["versions_by_venue"].(map[string]int)["polyus"] != 2 ||
		len(report["recent"].([]VenueNoticeView)) != 1 || report["recent"].([]VenueNoticeView)[0].Version != 2 {
		t.Fatalf("report did not separate current notice from immutable versions: %#v", report)
	}
	if _, err := st.db.Exec(`UPDATE research_venue_notices SET title='mutated' WHERE id=1`); err == nil || !strings.Contains(err.Error(), "immutable") {
		t.Fatalf("notice UPDATE was not blocked: %v", err)
	}
	if _, err := st.db.Exec(`DELETE FROM research_venue_notice_sightings WHERE id=1`); err == nil || !strings.Contains(err.Error(), "append-only") {
		t.Fatalf("sighting DELETE was not blocked: %v", err)
	}
}

func TestVenueNoticeReportShowsVersionsClassesAndNoAuthority(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	_, err = st.InsertVenueNoticeBatch(ctx, time.Now(), []VenueNotice{
		{Venue: "kalshi", SourceName: "kalshi-changelog-rss", SourceID: "k1", Title: "Fee endpoint deprecated",
			Summary: "Fee schema endpoint deprecated", SourceURL: "https://docs.kalshi.com/changelog", SourceFeed: "https://docs.kalshi.com/changelog/rss.xml",
			Severity: "attention", Classes: []string{"fee", "schema"}},
		{Venue: "polymarket", SourceName: "polymarket-status-rss", SourceID: "p1", Title: "Scheduled maintenance",
			Summary: "Maintenance window", SourceURL: "https://status.polymarket.com", SourceFeed: "https://status.polymarket.com/history.rss",
			Severity: "attention", Classes: []string{"maintenance"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	report, err := st.VenueNoticeReport(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	venues := report["counts_by_venue"].(map[string]int)
	classes := report["counts_by_class"].(map[string]int)
	recent := report["recent"].([]VenueNoticeView)
	if venues["kalshi"] != 1 || venues["polymarket"] != 1 || classes["schema"] != 1 || classes["maintenance"] != 1 || len(recent) != 2 {
		t.Fatalf("unexpected report: %#v", report)
	}
	if report["research_only"] != true || report["automatic_config_changes"] != false || report["live_authority"] != false {
		t.Fatalf("authority contract missing: %#v", report)
	}
}
