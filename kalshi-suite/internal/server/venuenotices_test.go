package server

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

const polyUSPriorityRSSFixture = `<?xml version="1.0" encoding="UTF-8"?>
<rss xmlns:content="http://purl.org/rss/1.0/modules/content/" version="2.0"><channel>
<lastBuildDate>Sun, 12 Jul 2026 03:57:23 GMT</lastBuildDate>
<item><title><![CDATA[v0.0.69 - Liquidity rewards reductions effective Monday, July 13 at midnight ET]]></title>
<description><![CDATA[Liquidity rewards are reduced across several categories effective 12:00am ET, Monday July 13.]]></description>
<link>https://docs.polymarket.us/changelog#july-10-2026</link><guid isPermaLink="false">3b27fc3066a02a50</guid>
<category><![CDATA[Incentives]]></category><pubDate>Sat, 11 Jul 2026 18:22:25 GMT</pubDate></item>
<item><title><![CDATA[v0.0.68 - Weekly maintenance window moved to Thursday 2am-6am ET]]></title>
<description><![CDATA[The recurring weekly maintenance window is now every Thursday from 2:00am-6:00am ET, effective July 9, 2026. Previously, the window was every Thursday, 6:00am-8:00am ET. This affects both the Institutional API and Retail API.]]></description>
<link>https://docs.polymarket.us/changelog#july-9-2026</link><guid isPermaLink="false">169bd8523e5be9b3</guid>
<category><![CDATA[Maintenance]]></category><pubDate>Thu, 09 Jul 2026 06:48:54 GMT</pubDate></item>
<item><title><![CDATA[v0.0.62 - lastPriceSample field no longer supported as of July 3]]></title>
<description><![CDATA[The lastPriceSample field is being removed from full and lite Retail Markets WebSocket responses and BBO/book REST endpoints. Use longQuote/shortQuote instead.]]></description>
<link>https://docs.polymarket.us/changelog#july-2-2026</link><guid isPermaLink="false">07a98eb919d85b36</guid>
<category><![CDATA[Breaking Change]]></category><pubDate>Thu, 02 Jul 2026 05:02:30 GMT</pubDate></item>
<item><title><![CDATA[v0.0.57 - Upcoming deprecations: Subjects endpoints and legacy market/event fields]]></title>
<description><![CDATA[Subjects endpoints and legacy fields are deprecated. sportsMarketType replaces marketType and sportsMarketTypeV2. The old fields will be removed.]]></description>
<link>https://docs.polymarket.us/changelog#june-26-2026</link><guid isPermaLink="false">1e8ef58b573c9e34</guid>
<category><![CDATA[Deprecation]]></category><pubDate>Sat, 27 Jun 2026 01:14:30 GMT</pubDate></item>
</channel></rss>`

func hasVenueNoticeClass(classes []string, want string) bool {
	for _, class := range classes {
		if class == want {
			return true
		}
	}
	return false
}

func TestParseVenueNoticeRSSPreservesOfficialVersionAndNewestDate(t *testing.T) {
	src := venueNoticeSource{Venue: "polyus", Name: "polyus-changelog-rss", URL: "https://docs.polymarket.us/changelog/rss.xml", Kind: "rss"}
	rows, err := parseVenueNoticeRSS(src, []byte(polyUSPriorityRSSFixture), `"feed-v1"`, "Sun, 12 Jul 2026 03:57:23 GMT")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 4 {
		t.Fatalf("got %d rows, want 4", len(rows))
	}
	if !strings.HasPrefix(rows[0].Title, "v0.0.69") || rows[0].PublishedTS != "2026-07-11T18:22:25Z" {
		t.Fatalf("newest version/date lost or reordered: %#v", rows[0])
	}
	if rows[0].FeedETag != `"feed-v1"` || rows[0].SourceID != "3b27fc3066a02a50" {
		t.Fatalf("feed provenance missing: %#v", rows[0])
	}
	if !hasVenueNoticeClass(rows[1].Classes, "maintenance") || rows[1].Severity != "attention" {
		t.Fatalf("v0.0.68 maintenance classification wrong: %#v", rows[1])
	}
	for _, index := range []int{2, 3} {
		if !hasVenueNoticeClass(rows[index].Classes, "schema") || rows[index].Severity != "breaking" {
			t.Fatalf("schema removal must be breaking: %#v", rows[index])
		}
	}
}

func TestParseVenueNoticeFullChangelogRetainsRolledOffPartialContractNotice(t *testing.T) {
	fixture := `<Update label="June 9, 2026" description="v0.0.43" tags={["Upcoming", "Institutional API", "Retail API"]} rss={{ title: "v0.0.43 - Full partial-contract rollout moved to June 11", description: "All newly listed instruments will become partial-contract markets on Thursday, June 11, 2026 at 5:00 PM ET. Legacy markets can remain scale 1." }}>
All market makers and API users should support partial-contract instruments now.
</Update>`
	src := venueNoticeSource{Venue: "polyus", Name: "polyus-changelog-full", URL: "https://docs.polymarket.us/changelog.md", Kind: "changelog-markdown"}
	rows, err := parseVenueNoticeChangelogMarkdown(src, []byte(fixture), "", "Sun, 12 Jul 2026 03:57:23 GMT")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || !strings.Contains(rows[0].Title, "v0.0.43") || rows[0].PublishedTS != "2026-06-09T00:00:00Z" {
		t.Fatalf("rolled-off notice not preserved: %#v", rows)
	}
	if !hasVenueNoticeClass(rows[0].Classes, "market_structure") || rows[0].Severity != "attention" {
		t.Fatalf("partial-contract notice classification wrong: %#v", rows[0])
	}
}

func TestParseVenueNoticeFutureChangelogLabelIsEffectiveNotPublished(t *testing.T) {
	fixture := `<Update label="July 23, 2026" description="Upcoming rollout" tags={["WebSocket"]} rss={{ title: "New price level structures", description: "Pilot markets switch the week of July 27." }}>
Consume price_ranges dynamically.
</Update>`
	src := venueNoticeSource{Venue: "kalshi", Name: "kalshi-changelog-full", URL: "https://docs.kalshi.com/changelog.md", Kind: "changelog-markdown"}
	observed := time.Date(2026, 7, 12, 6, 0, 0, 0, time.UTC)
	rows, err := parseVenueNoticeChangelogMarkdownAt(src, []byte(fixture), "", "Sun, 12 Jul 2026 05:00:00 GMT", observed)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want one", len(rows))
	}
	if rows[0].PublishedTS != "2026-07-12T05:00:00Z" || rows[0].EffectiveTS != "2026-07-23T00:00:00Z" {
		t.Fatalf("future label clocks wrong: published=%q effective=%q", rows[0].PublishedTS, rows[0].EffectiveTS)
	}
}

func TestParsePolyUSRegulatoryNoticeIndexKeepsArtifactProvenance(t *testing.T) {
	fixture := `{"files":[{"filename":"Amended Market Liquidity Incentive Program (2026.04.07).pdf","size":364136,"lastModified":"2026-04-08T13:56:07.000Z","eTag":"ea8ba07c65bebaf23e9b2d3cdeee9157"}]}`
	src := venueNoticeSource{Venue: "polyus", Name: "polyus-regulatory-notices", URL: "https://www.polymarketexchange.com/files/notices", Kind: "notice-json"}
	rows, err := parsePolyUSNoticeIndex(src, []byte(fixture), "", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].ArtifactHash != "ea8ba07c65bebaf23e9b2d3cdeee9157" || rows[0].PublishedTS != "2026-04-07T00:00:00Z" {
		t.Fatalf("regulatory artifact provenance wrong: %#v", rows)
	}
	if strings.Contains(rows[0].SourceURL, " ") || !strings.Contains(rows[0].SourceURL, "/files/notices/") {
		t.Fatalf("notice URL not safely encoded: %q", rows[0].SourceURL)
	}
}

func TestFetchVenueNoticeSourceUsesConditionalCache(t *testing.T) {
	var requests int
	ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if requests == 2 {
			if got := r.Header.Get("If-None-Match"); got != `"v1"` {
				t.Errorf("If-None-Match=%q, want quoted v1", got)
			}
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("Content-Type", "application/rss+xml")
		w.Header().Set("ETag", `"v1"`)
		_, _ = w.Write([]byte(polyUSPriorityRSSFixture))
	}))
	defer ts.Close()
	src := venueNoticeSource{Venue: "polyus", Name: "fixture-rss", URL: ts.URL, Kind: "rss"}
	cache := &sync.Map{}
	first := fetchVenueNoticeSource(context.Background(), ts.Client(), cache, src)
	if first.ErrorClass != "" || len(first.Notices) != 4 {
		t.Fatalf("first fetch failed: %#v", first)
	}
	second := fetchVenueNoticeSource(context.Background(), ts.Client(), cache, src)
	if second.ErrorClass != "" || !second.NotModified || len(second.Notices) != 0 {
		t.Fatalf("conditional fetch failed: %#v", second)
	}
}

func TestFetchVenueNoticeSourceBoundsBodiesAndSanitizesHTTPError(t *testing.T) {
	t.Run("bounded", func(t *testing.T) {
		ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/markdown")
			_, _ = fmt.Fprint(w, strings.Repeat("x", venueNoticeMaxBody+1))
		}))
		defer ts.Close()
		result := fetchVenueNoticeSource(context.Background(), ts.Client(), &sync.Map{}, venueNoticeSource{
			Venue: "kalshi", Name: "large", URL: ts.URL, Kind: "markdown"})
		if result.ErrorClass != "body_too_large" || strings.Contains(result.SafeErrorText, ts.URL) {
			t.Fatalf("unsafe/unbounded result: %#v", result)
		}
	})
	t.Run("http", func(t *testing.T) {
		ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "secret upstream body", http.StatusBadGateway)
		}))
		defer ts.Close()
		result := fetchVenueNoticeSource(context.Background(), ts.Client(), &sync.Map{}, venueNoticeSource{
			Venue: "kalshi", Name: "bad", URL: ts.URL, Kind: "rss"})
		if result.ErrorClass != "http_502" || strings.Contains(result.SafeErrorText, "secret") || strings.Contains(result.SafeErrorText, ts.URL) {
			t.Fatalf("HTTP error was not sanitized: %#v", result)
		}
	})
}

func TestVenueNoticeDirectMoneyAndSchemaCoverage(t *testing.T) {
	sources := defaultVenueNoticeSources()
	if len(sources) != 26 {
		t.Fatalf("official watcher sources=%d, want 26", len(sources))
	}
	want := map[string]bool{
		"kalshi-event-fee-change-doc": false, "kalshi-series-fee-change-doc": false,
		"kalshi-incentives-doc": false, "kalshi-openapi": false, "kalshi-asyncapi": false,
		"polymarket-fees-doc": false, "polymarket-maker-rebates-doc": false,
		"polymarket-builder-fees-doc": false, "polymarket-data-openapi": false,
		"polyus-fees-doc": false, "polyus-incentive-programs-doc": false,
		"polyus-incentives-schema": false,
	}
	seen := map[string]bool{}
	for _, src := range sources {
		if seen[src.Name] || !strings.HasPrefix(src.URL, "https://") {
			t.Fatalf("duplicate or non-HTTPS watcher source: %+v", src)
		}
		seen[src.Name] = true
		if _, ok := want[src.Name]; ok {
			want[src.Name] = true
		}
	}
	for name, found := range want {
		if !found {
			t.Fatalf("direct money/schema source missing: %s", name)
		}
	}
}

func TestR139EveryVenueOfficialChangelogIsDirectlyWatched(t *testing.T) {
	want := map[string]struct {
		venue, url, kind string
	}{
		"kalshi-changelog-rss":      {"kalshi", "https://docs.kalshi.com/changelog/rss.xml", "rss"},
		"kalshi-changelog-full":     {"kalshi", "https://docs.kalshi.com/changelog.md", "changelog-markdown"},
		"polyus-changelog-rss":      {"polyus", "https://docs.polymarket.us/changelog/rss.xml", "rss"},
		"polyus-changelog-full":     {"polyus", "https://docs.polymarket.us/changelog.md", "changelog-markdown"},
		"polymarket-changelog-full": {"polymarket", "https://docs.polymarket.com/changelog.md", "changelog-markdown"},
	}
	for _, src := range defaultVenueNoticeSources() {
		expect, ok := want[src.Name]
		if !ok {
			continue
		}
		if src.Venue != expect.venue || src.URL != expect.url || src.Kind != expect.kind {
			t.Fatalf("official changelog source drifted: got=%+v want venue=%s url=%s kind=%s",
				src, expect.venue, expect.url, expect.kind)
		}
		delete(want, src.Name)
	}
	if len(want) != 0 {
		t.Fatalf("official changelog sources missing: %+v", want)
	}
}

func TestVenueNoticeCycleIDsPreserveEveryRestartAttempt(t *testing.T) {
	first := time.Date(2026, 7, 12, 7, 40, 0, 0, time.UTC)
	second := first.Add(20 * time.Second)
	if venueNoticeCycleID(first) == venueNoticeCycleID(second) {
		t.Fatal("two real watcher attempts inside one cadence bucket deduplicated into one receipt")
	}
	if !strings.Contains(venueNoticeCycleID(first), first.Format(time.RFC3339Nano)) {
		t.Fatalf("watcher cycle lost exact attempt time: %q", venueNoticeCycleID(first))
	}
}

func TestParseVenueNoticeArtifactHashesWholeSchema(t *testing.T) {
	src := venueNoticeSource{Venue: "kalshi", Name: "kalshi-openapi", URL: "https://docs.kalshi.com/openapi.yaml", Kind: "artifact"}
	first, err := parseVenueNoticeArtifact(src, []byte("openapi: 3.0.0\npaths: {}\n"), `"v1"`, "Sun, 12 Jul 2026 03:57:23 GMT")
	if err != nil || len(first) != 1 {
		t.Fatalf("artifact parse failed: rows=%+v err=%v", first, err)
	}
	second, err := parseVenueNoticeArtifact(src, []byte("openapi: 3.0.0\npaths: { /new: {} }\n"), `"v2"`, "Sun, 12 Jul 2026 04:57:23 GMT")
	if err != nil || len(second) != 1 || first[0].ArtifactHash == second[0].ArtifactHash {
		t.Fatalf("whole-schema byte change was not versioned: first=%+v second=%+v err=%v", first, second, err)
	}
	if !hasVenueNoticeClass(second[0].Classes, "schema") || second[0].Categories[0] != "authoritative-schema-artifact" {
		t.Fatalf("schema artifact classification wrong: %+v", second[0])
	}
}

// Opt-in live contract probe. It is skipped in normal/offline CI but gives a pass an exact way to
// verify that every pinned official source still serves the schema its fixture parser expects.
func TestVenueNoticeOfficialSourcesLive(t *testing.T) {
	if os.Getenv("KALSHI_SUITE_LIVE_NOTICE_TEST") != "1" {
		t.Skip("set KALSHI_SUITE_LIVE_NOTICE_TEST=1 for official-source contract probe")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	client := venueNoticeClient()
	cache := &sync.Map{}
	requiredCurrent := map[string][]string{
		"polyus-changelog-full":     {"v0.0.69", "v0.0.68"},
		"polymarket-changelog-full": {"sports taker fee", "0.05"},
		"kalshi-changelog-full":     {"rfq_id", "center_whole_edge_half_cent"},
	}
	for _, src := range defaultVenueNoticeSources() {
		t.Run(src.Name, func(t *testing.T) {
			result := fetchVenueNoticeSource(ctx, client, cache, src)
			if result.ErrorClass != "" || len(result.Notices) == 0 {
				t.Fatalf("official source contract failed: class=%q text=%q rows=%d bytes=%d", result.ErrorClass, result.SafeErrorText, len(result.Notices), result.Bytes)
			}
			if required := requiredCurrent[src.Name]; len(required) > 0 {
				var current strings.Builder
				for _, notice := range result.Notices {
					current.WriteString(strings.ToLower(notice.Title + " " + notice.Summary + "\n"))
				}
				for _, fragment := range required {
					if !strings.Contains(current.String(), strings.ToLower(fragment)) {
						t.Fatalf("official source parsed but current required notice %q is missing", fragment)
					}
				}
			}
		})
	}
}
