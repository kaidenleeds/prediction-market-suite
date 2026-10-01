package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"html"
	"io"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

const (
	venueNoticeMaxBody = 4 << 20
	venueNoticeCadence = 15 * time.Minute
)

type venueNoticeSource struct {
	Venue, Name, URL, Kind string
}

// These are intentionally fixed official public sources, not user/config supplied URLs. The
// watcher is advisory: it records and reports notices but cannot mutate any runtime setting.
func defaultVenueNoticeSources() []venueNoticeSource {
	return []venueNoticeSource{
		{Venue: "kalshi", Name: "kalshi-changelog-rss", URL: "https://docs.kalshi.com/changelog/rss.xml", Kind: "rss"},
		{Venue: "kalshi", Name: "kalshi-changelog-full", URL: "https://docs.kalshi.com/changelog.md", Kind: "changelog-markdown"},
		{Venue: "kalshi", Name: "kalshi-maintenance-doc", URL: "https://docs.kalshi.com/getting_started/maintenance_and_pauses.md", Kind: "markdown"},
		{Venue: "kalshi", Name: "kalshi-fee-rounding-doc", URL: "https://docs.kalshi.com/getting_started/fee_rounding.md", Kind: "markdown"},
		{Venue: "kalshi", Name: "kalshi-event-fee-change-doc", URL: "https://docs.kalshi.com/api-reference/events/get-event-fee-changes.md", Kind: "markdown"},
		{Venue: "kalshi", Name: "kalshi-series-fee-change-doc", URL: "https://docs.kalshi.com/api-reference/exchange/get-series-fee-changes.md", Kind: "markdown"},
		{Venue: "kalshi", Name: "kalshi-incentives-doc", URL: "https://docs.kalshi.com/api-reference/incentive-programs/get-incentives.md", Kind: "markdown"},
		{Venue: "kalshi", Name: "kalshi-openapi", URL: "https://docs.kalshi.com/openapi.yaml", Kind: "artifact"},
		{Venue: "kalshi", Name: "kalshi-asyncapi", URL: "https://docs.kalshi.com/asyncapi.yaml", Kind: "artifact"},
		{Venue: "kalshi", Name: "kalshi-doc-index", URL: "https://docs.kalshi.com/llms.txt", Kind: "markdown"},
		// Polymarket removed the Mintlify RSS route; the complete Markdown changelog immediately
		// below remains the official, content-addressable source. Keeping a permanent 404 here
		// would falsely mark the whole notice collector blocked.
		{Venue: "polymarket", Name: "polymarket-changelog-full", URL: "https://docs.polymarket.com/changelog.md", Kind: "changelog-markdown"},
		{Venue: "polymarket", Name: "polymarket-status-rss", URL: "https://status.polymarket.com/history.rss", Kind: "rss"},
		{Venue: "polymarket", Name: "polymarket-fees-doc", URL: "https://docs.polymarket.com/trading/fees.md", Kind: "markdown"},
		{Venue: "polymarket", Name: "polymarket-maker-rebates-doc", URL: "https://docs.polymarket.com/market-makers/maker-rebates.md", Kind: "markdown"},
		{Venue: "polymarket", Name: "polymarket-builder-fees-doc", URL: "https://docs.polymarket.com/builders/fees.md", Kind: "markdown"},
		{Venue: "polymarket", Name: "polymarket-data-openapi", URL: "https://docs.polymarket.com/api-spec/data-openapi.yaml", Kind: "artifact"},
		{Venue: "polymarket", Name: "polymarket-doc-index", URL: "https://docs.polymarket.com/llms.txt", Kind: "markdown"},
		{Venue: "polyus", Name: "polyus-changelog-rss", URL: "https://docs.polymarket.us/changelog/rss.xml", Kind: "rss"},
		{Venue: "polyus", Name: "polyus-changelog-full", URL: "https://docs.polymarket.us/changelog.md", Kind: "changelog-markdown"},
		{Venue: "polyus", Name: "polyus-status-rss", URL: "https://status.polymarketexchange.com/history.rss", Kind: "rss"},
		{Venue: "polyus", Name: "polyus-regulatory-notices", URL: "https://www.polymarketexchange.com/files/notices", Kind: "notice-json"},
		{Venue: "polyus", Name: "polyus-maintenance-faq", URL: "https://docs.polymarket.us/faqs/general-faqs.md", Kind: "markdown"},
		{Venue: "polyus", Name: "polyus-fees-doc", URL: "https://docs.polymarket.us/fees.md", Kind: "markdown"},
		{Venue: "polyus", Name: "polyus-incentive-programs-doc", URL: "https://docs.polymarket.us/api-reference/incentives/get-incentive-programs.md", Kind: "markdown"},
		{Venue: "polyus", Name: "polyus-incentives-schema", URL: "https://docs.polymarket.us/api-reference/oapi-schemas/incentives-schema.json", Kind: "artifact"},
		{Venue: "polyus", Name: "polyus-doc-index", URL: "https://docs.polymarket.us/llms.txt", Kind: "markdown"},
	}
}

var venueNoticeSourceProvider = defaultVenueNoticeSources

type venueNoticeHTTPCache struct {
	ETag, LastModified string
}

var venueNoticeCaches sync.Map // map[*Server]*sync.Map(source name -> venueNoticeHTTPCache)

var venueNoticeSweepSchedule = struct {
	sync.Mutex
	at map[*Server]time.Time
}{at: make(map[*Server]time.Time)}

func venueNoticeSweepDue(s *Server, now time.Time) bool {
	venueNoticeSweepSchedule.Lock()
	defer venueNoticeSweepSchedule.Unlock()
	if previous := venueNoticeSweepSchedule.at[s]; !previous.IsZero() && now.Sub(previous) < venueNoticeCadence {
		return false
	}
	venueNoticeSweepSchedule.at[s] = now
	return true
}

func venueNoticeCycleID(started time.Time) string {
	return "venue-notices-" + started.UTC().Format(time.RFC3339Nano)
}

func venueNoticeCacheFor(s *Server) *sync.Map {
	if v, ok := venueNoticeCaches.Load(s); ok {
		return v.(*sync.Map)
	}
	v, _ := venueNoticeCaches.LoadOrStore(s, &sync.Map{})
	return v.(*sync.Map)
}

type venueNoticeFetchError struct {
	Class, Text string
}

func (e *venueNoticeFetchError) Error() string { return e.Class + ": " + e.Text }

func noticeFetchError(class, text string) error {
	return &venueNoticeFetchError{Class: class, Text: text}
}

type venueNoticeFetchResult struct {
	Source                    venueNoticeSource
	Notices                   []storage.VenueNotice
	ETag, LastModified        string
	NotModified               bool
	Bytes                     int
	Duration                  time.Duration
	ErrorClass, SafeErrorText string
}

func venueNoticeClient() *http.Client {
	return &http.Client{
		Timeout: 15 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 3 {
				return noticeFetchError("redirect_policy", "official source exceeded three redirects")
			}
			if len(via) > 0 && !strings.EqualFold(req.URL.Hostname(), via[0].URL.Hostname()) {
				return noticeFetchError("redirect_policy", "official source redirected to another host")
			}
			if req.URL.Scheme != "https" {
				return noticeFetchError("redirect_policy", "official source redirected away from HTTPS")
			}
			if req.URL.User != nil || req.URL.RawQuery != "" {
				return noticeFetchError("redirect_policy", "official source redirected outside the pinned URL policy")
			}
			return nil
		},
	}
}

func safeVenueNoticeHTTPError(err error) (class, text string) {
	var safe *venueNoticeFetchError
	if errors.As(err, &safe) {
		return safe.Class, safe.Text
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout", "official source request timed out"
	}
	return "transport", "official source request failed"
}

func venueNoticeContentTypeOK(kind, contentType string) bool {
	contentType = strings.ToLower(strings.TrimSpace(strings.Split(contentType, ";")[0]))
	switch kind {
	case "rss":
		return contentType == "application/rss+xml" || contentType == "application/xml" || contentType == "text/xml"
	case "notice-json":
		return contentType == "application/json" || contentType == "text/json"
	case "markdown", "changelog-markdown":
		return contentType == "text/markdown" || contentType == "text/plain"
	case "artifact":
		return contentType == "application/json" || contentType == "text/json" ||
			contentType == "text/yaml" || contentType == "application/yaml" ||
			contentType == "application/x-yaml" || contentType == "text/plain"
	default:
		return false
	}
}

func fetchVenueNoticeSource(ctx context.Context, client *http.Client, cache *sync.Map, src venueNoticeSource) venueNoticeFetchResult {
	started := time.Now()
	out := venueNoticeFetchResult{Source: src}
	u, err := url.Parse(src.URL)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.RawQuery != "" || u.User != nil {
		out.ErrorClass, out.SafeErrorText = "source_policy", "source is not an approved HTTPS URL"
		return out
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		out.ErrorClass, out.SafeErrorText = "request", "official source request could not be created"
		return out
	}
	req.Header.Set("Accept", map[string]string{
		"rss":                "application/rss+xml, application/xml;q=0.9, text/xml;q=0.8",
		"notice-json":        "application/json",
		"markdown":           "text/markdown, text/plain;q=0.9",
		"changelog-markdown": "text/markdown, text/plain;q=0.9",
		"artifact":           "application/json, text/yaml;q=0.9, application/yaml;q=0.9, text/plain;q=0.8",
	}[src.Kind])
	req.Header.Set("User-Agent", "kalshi-suite-research-notice-watcher/1")
	if prior, ok := cache.Load(src.Name); ok {
		c := prior.(venueNoticeHTTPCache)
		if c.ETag != "" {
			req.Header.Set("If-None-Match", c.ETag)
		}
		if c.LastModified != "" {
			req.Header.Set("If-Modified-Since", c.LastModified)
		}
	}
	resp, err := client.Do(req)
	if err != nil {
		out.ErrorClass, out.SafeErrorText = safeVenueNoticeHTTPError(err)
		out.Duration = time.Since(started)
		return out
	}
	defer resp.Body.Close()
	out.ETag, out.LastModified = resp.Header.Get("ETag"), resp.Header.Get("Last-Modified")
	if resp.StatusCode == http.StatusNotModified {
		out.NotModified = true
		out.Duration = time.Since(started)
		return out
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		out.ErrorClass = fmt.Sprintf("http_%d", resp.StatusCode)
		out.SafeErrorText = fmt.Sprintf("official source returned HTTP %d", resp.StatusCode)
		out.Duration = time.Since(started)
		return out
	}
	if !venueNoticeContentTypeOK(src.Kind, resp.Header.Get("Content-Type")) {
		out.ErrorClass, out.SafeErrorText = "content_type", "official source returned an unexpected content type"
		out.Duration = time.Since(started)
		return out
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, venueNoticeMaxBody+1))
	if err != nil {
		out.ErrorClass, out.SafeErrorText = "read", "official source response could not be read"
		out.Duration = time.Since(started)
		return out
	}
	out.Bytes = len(body)
	if len(body) > venueNoticeMaxBody {
		out.ErrorClass, out.SafeErrorText = "body_too_large", "official source exceeded the four-megabyte safety bound"
		out.Duration = time.Since(started)
		return out
	}
	switch src.Kind {
	case "rss":
		out.Notices, err = parseVenueNoticeRSS(src, body, out.ETag, out.LastModified)
	case "notice-json":
		out.Notices, err = parsePolyUSNoticeIndex(src, body, out.ETag, out.LastModified)
	case "markdown":
		out.Notices, err = parseVenueNoticeMarkdown(src, body, out.ETag, out.LastModified)
	case "changelog-markdown":
		out.Notices, err = parseVenueNoticeChangelogMarkdown(src, body, out.ETag, out.LastModified)
	case "artifact":
		out.Notices, err = parseVenueNoticeArtifact(src, body, out.ETag, out.LastModified)
	default:
		err = errors.New("unknown source kind")
	}
	if err != nil {
		out.Notices = nil
		out.ErrorClass, out.SafeErrorText = "parse", "official source response did not match its pinned schema"
	} else {
		cache.Store(src.Name, venueNoticeHTTPCache{ETag: out.ETag, LastModified: out.LastModified})
	}
	out.Duration = time.Since(started)
	return out
}

type venueNoticeRSS struct {
	Channel struct {
		LastBuildDate string               `xml:"lastBuildDate"`
		Items         []venueNoticeRSSItem `xml:"item"`
	} `xml:"channel"`
}

type venueNoticeRSSItem struct {
	Title       string   `xml:"title"`
	Description string   `xml:"description"`
	Content     string   `xml:"http://purl.org/rss/1.0/modules/content/ encoded"`
	Link        string   `xml:"link"`
	GUID        string   `xml:"guid"`
	PubDate     string   `xml:"pubDate"`
	Categories  []string `xml:"category"`
}

var venueNoticeTagRE = regexp.MustCompile(`(?s)<[^>]*>`)
var venueNoticeSpaceRE = regexp.MustCompile(`\s+`)

func venueNoticePlainText(s string, limit int) string {
	s = venueNoticeTagRE.ReplaceAllString(s, " ")
	s = html.UnescapeString(s)
	s = venueNoticeSpaceRE.ReplaceAllString(s, " ")
	s = strings.TrimSpace(s)
	if limit > 0 && len(s) > limit {
		s = strings.TrimSpace(s[:limit])
	}
	return s
}

func venueNoticeHash(parts ...string) string {
	h := sha256.New()
	for _, p := range parts {
		_, _ = io.WriteString(h, p)
		_, _ = h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

func venueNoticeSafeSourceURL(raw, fallback string) string {
	for _, candidate := range []string{raw, fallback} {
		u, err := url.Parse(strings.TrimSpace(candidate))
		if err == nil && u.Scheme == "https" && u.Hostname() != "" && u.User == nil && u.RawQuery == "" {
			return u.String()
		}
	}
	return ""
}

func parseVenueNoticeTime(raw string) string {
	for _, layout := range []string{time.RFC1123Z, time.RFC1123, time.RFC3339Nano, time.RFC3339} {
		if t, err := time.Parse(layout, strings.TrimSpace(raw)); err == nil {
			return t.UTC().Format(time.RFC3339Nano)
		}
	}
	return ""
}

func parseVenueNoticeRSS(src venueNoticeSource, body []byte, etag, lastModified string) ([]storage.VenueNotice, error) {
	var feed venueNoticeRSS
	if err := xml.Unmarshal(body, &feed); err != nil {
		return nil, err
	}
	if len(feed.Channel.Items) == 0 {
		return nil, errors.New("RSS channel contained no items")
	}
	out := make([]storage.VenueNotice, 0, len(feed.Channel.Items))
	for _, item := range feed.Channel.Items {
		title := venueNoticePlainText(item.Title, 1000)
		summary := venueNoticePlainText(item.Description, 8000)
		if summary == "" {
			summary = venueNoticePlainText(item.Content, 8000)
		}
		published := parseVenueNoticeTime(item.PubDate)
		id := strings.TrimSpace(item.GUID)
		if id == "" {
			id = strings.TrimSpace(item.Link)
		}
		if id == "" {
			id = venueNoticeHash(title, published)
		}
		if title == "" || id == "" {
			continue
		}
		classes, severity := classifyVenueNotice(title + " " + summary + " " + strings.Join(item.Categories, " "))
		out = append(out, storage.VenueNotice{Venue: src.Venue, SourceName: src.Name, SourceID: id,
			Title: title, Summary: summary, PublishedTS: published, SourceURL: venueNoticeSafeSourceURL(item.Link, src.URL),
			SourceFeed: src.URL, Severity: severity, Categories: item.Categories, Classes: classes,
			ArtifactHash: venueNoticeHash(title, summary, item.Content, published, strings.Join(item.Categories, "\x1f")),
			FeedETag:     etag, FeedLastModified: lastModified})
	}
	if len(out) == 0 {
		return nil, errors.New("RSS channel had no valid items")
	}
	return out, nil
}

type polyUSNoticeIndex struct {
	Files []struct {
		Filename     string `json:"filename"`
		Size         int64  `json:"size"`
		LastModified string `json:"lastModified"`
		ETag         string `json:"eTag"`
	} `json:"files"`
}

var polyUSNoticeDateRE = regexp.MustCompile(`\((\d{4})\.(\d{2})\.(\d{2})\)`)

func parsePolyUSNoticeIndex(src venueNoticeSource, body []byte, etag, lastModified string) ([]storage.VenueNotice, error) {
	var index polyUSNoticeIndex
	if err := json.Unmarshal(body, &index); err != nil {
		return nil, err
	}
	if len(index.Files) == 0 {
		return nil, errors.New("regulatory notice index contained no files")
	}
	base, err := url.Parse(src.URL)
	if err != nil {
		return nil, err
	}
	base.Path = path.Dir(base.Path) + "/notices/"
	out := make([]storage.VenueNotice, 0, len(index.Files))
	for _, f := range index.Files {
		name := strings.TrimSpace(f.Filename)
		if name == "" || f.Size < 0 {
			continue
		}
		published := parseVenueNoticeTime(f.LastModified)
		if match := polyUSNoticeDateRE.FindStringSubmatch(name); len(match) == 4 {
			if t, err := time.Parse("2006-01-02", strings.Join(match[1:], "-")); err == nil {
				published = t.UTC().Format(time.RFC3339Nano)
			}
		}
		artifact := strings.TrimSpace(f.ETag)
		if artifact == "" {
			artifact = venueNoticeHash(name, fmt.Sprint(f.Size), f.LastModified)
		}
		classes, severity := classifyVenueNotice(name)
		u := *base
		u.Path += name
		out = append(out, storage.VenueNotice{Venue: src.Venue, SourceName: src.Name, SourceID: name,
			Title: strings.TrimSuffix(name, path.Ext(name)), PublishedTS: published, SourceURL: u.String(),
			SourceFeed: src.URL, Severity: severity, Categories: []string{"regulatory-notice"}, Classes: classes,
			ArtifactHash: artifact, FeedETag: etag, FeedLastModified: lastModified})
	}
	if len(out) == 0 {
		return nil, errors.New("regulatory notice index had no valid files")
	}
	return out, nil
}

func parseVenueNoticeMarkdown(src venueNoticeSource, body []byte, etag, lastModified string) ([]storage.VenueNotice, error) {
	text := venueNoticePlainText(string(body), 8000)
	if text == "" {
		return nil, errors.New("empty markdown document")
	}
	title := src.Name
	for _, line := range strings.Split(string(body), "\n") {
		line = strings.TrimSpace(strings.TrimLeft(strings.TrimSpace(line), "#"))
		if line != "" {
			title = venueNoticePlainText(line, 1000)
			break
		}
	}
	classes, severity := classifyVenueNotice(title + " " + text)
	return []storage.VenueNotice{{Venue: src.Venue, SourceName: src.Name, SourceID: src.URL,
		Title: title, Summary: text, PublishedTS: parseVenueNoticeTime(lastModified), SourceURL: src.URL,
		SourceFeed: src.URL, Severity: severity, Categories: []string{"authoritative-document"}, Classes: classes,
		ArtifactHash: venueNoticeHash(string(body)), FeedETag: etag, FeedLastModified: lastModified}}, nil
}

// Schema artifacts are hashed byte-for-byte while only a bounded text preview enters the advisory
// notice. A changed OpenAPI/AsyncAPI/JSON artifact therefore creates a new immutable version even
// when a venue omits the change from its changelog. It never changes runtime configuration.
func parseVenueNoticeArtifact(src venueNoticeSource, body []byte, etag, lastModified string) ([]storage.VenueNotice, error) {
	if len(body) == 0 {
		return nil, errors.New("empty schema artifact")
	}
	preview := venueNoticePlainText(string(body), 8000)
	if preview == "" {
		preview = "authoritative schema artifact changed"
	}
	classes, severity := classifyVenueNotice(src.Name + " schema API " + preview)
	return []storage.VenueNotice{{Venue: src.Venue, SourceName: src.Name, SourceID: src.URL,
		Title: src.Name, Summary: preview, PublishedTS: parseVenueNoticeTime(lastModified), SourceURL: src.URL,
		SourceFeed: src.URL, Severity: severity, Categories: []string{"authoritative-schema-artifact"}, Classes: classes,
		ArtifactHash: venueNoticeHash(string(body)), FeedETag: etag, FeedLastModified: lastModified}}, nil
}

var venueNoticeUpdateRE = regexp.MustCompile(`(?s)<Update\b(.*?)>(.*?)</Update>`)
var venueNoticeLabelRE = regexp.MustCompile(`(?s)\blabel\s*=\s*"([^"]+)"`)
var venueNoticeVersionRE = regexp.MustCompile(`(?s)\bdescription\s*=\s*"([^"]+)"`)
var venueNoticeTagsRE = regexp.MustCompile(`(?s)\btags\s*=\s*\{\[(.*?)\]\}`)
var venueNoticeQuotedRE = regexp.MustCompile(`"((?:\\.|[^"])*)"`)
var venueNoticeRSSTitleRE = regexp.MustCompile(`(?s)\btitle\s*:\s*"((?:\\.|[^"])*)"`)
var venueNoticeRSSDescriptionRE = regexp.MustCompile(`(?s)\bdescription\s*:\s*"((?:\\.|[^"])*)"`)

func venueNoticeUnquote(raw string) string {
	if value, err := strconv.Unquote(`"` + raw + `"`); err == nil {
		return value
	}
	return raw
}

func venueNoticeMatch(re *regexp.Regexp, raw string) string {
	match := re.FindStringSubmatch(raw)
	if len(match) < 2 {
		return ""
	}
	return venueNoticeUnquote(strings.TrimSpace(match[1]))
}

func parseVenueNoticeLabelTime(raw string) string {
	for _, layout := range []string{"January 2, 2006", "Jan 2, 2006", "2006-01-02"} {
		if t, err := time.Parse(layout, strings.TrimSpace(raw)); err == nil {
			return t.UTC().Format(time.RFC3339Nano)
		}
	}
	return ""
}

// Mintlify's full changelog is authoritative history beyond the bounded RSS window. Each Update
// becomes its own immutable notice so older schema cutovers do not disappear when the RSS rolls.
func parseVenueNoticeChangelogMarkdown(src venueNoticeSource, body []byte, etag, lastModified string) ([]storage.VenueNotice, error) {
	return parseVenueNoticeChangelogMarkdownAt(src, body, etag, lastModified, time.Now())
}

func parseVenueNoticeChangelogMarkdownAt(src venueNoticeSource, body []byte, etag, lastModified string, observed time.Time) ([]storage.VenueNotice, error) {
	if observed.IsZero() {
		observed = time.Now()
	}
	blocks := venueNoticeUpdateRE.FindAllSubmatch(body, -1)
	if len(blocks) == 0 {
		return nil, errors.New("full changelog contained no Update blocks")
	}
	out := make([]storage.VenueNotice, 0, len(blocks))
	for _, block := range blocks {
		attrs, content := string(block[1]), string(block[2])
		label := venueNoticeMatch(venueNoticeLabelRE, attrs)
		version := venueNoticeMatch(venueNoticeVersionRE, attrs)
		title := venueNoticeMatch(venueNoticeRSSTitleRE, attrs)
		description := venueNoticeMatch(venueNoticeRSSDescriptionRE, attrs)
		if title == "" {
			title = strings.TrimSpace(strings.Join([]string{version, label}, " "))
		}
		if title == "" {
			continue
		}
		categories := make([]string, 0)
		if tags := venueNoticeTagsRE.FindStringSubmatch(attrs); len(tags) == 2 {
			for _, match := range venueNoticeQuotedRE.FindAllStringSubmatch(tags[1], -1) {
				if len(match) == 2 {
					categories = append(categories, venueNoticeUnquote(match[1]))
				}
			}
		}
		plainContent := venueNoticePlainText(content, 8000)
		summary := venueNoticePlainText(description, 8000)
		if summary == "" {
			summary = plainContent
		} else if plainContent != "" && !strings.Contains(summary, plainContent) {
			summary = venueNoticePlainText(summary+" "+plainContent, 8000)
		}
		published, effective := parseVenueNoticeLabelTime(label), ""
		if labelTime, err := time.Parse(time.RFC3339Nano, published); err == nil {
			observedDay := time.Date(observed.UTC().Year(), observed.UTC().Month(), observed.UTC().Day(), 0, 0, 0, 0, time.UTC)
			if labelTime.After(observedDay) {
				// Mintlify can label an announced update with its future rollout date. A future
				// calendar label cannot be its publication clock; retain it as effective time and
				// use only the HTTP artifact clock (when present) for publication provenance.
				effective = published
				published = parseVenueNoticeTime(lastModified)
			}
		}
		if published == "" && effective == "" {
			published = parseVenueNoticeTime(lastModified)
		}
		identityPrefix := strings.TrimSpace(version + "|" + label)
		if identityPrefix == "|" || identityPrefix == "" {
			identityPrefix = "update"
		}
		titleHash := venueNoticeHash(title)
		sourceID := identityPrefix + "|" + titleHash[:16]
		classes, severity := classifyVenueNotice(title + " " + summary + " " + strings.Join(categories, " "))
		out = append(out, storage.VenueNotice{Venue: src.Venue, SourceName: src.Name, SourceID: sourceID,
			Title: venueNoticePlainText(title, 1000), Summary: summary, PublishedTS: published, EffectiveTS: effective,
			SourceURL: src.URL, SourceFeed: src.URL, Severity: severity, Categories: categories, Classes: classes,
			ArtifactHash: venueNoticeHash(attrs, content), FeedETag: etag, FeedLastModified: lastModified})
	}
	if len(out) == 0 {
		return nil, errors.New("full changelog had no valid Update blocks")
	}
	return out, nil
}

func classifyVenueNotice(raw string) ([]string, string) {
	text := strings.ToLower(raw)
	rules := []struct {
		Class string
		Words []string
	}{
		{"schema", []string{"schema", "api version", "api change", "websocket", "endpoint", "payload", "response field", "request field", "data field", "fields", "deprecated", "deprecation", "removed", "fix tag", "grpc", "protobuf", "signature", "pagination"}},
		{"fee", []string{"fee", "fees", "rebate", "commission", "liquidity reward", "incentive program", "discount factor"}},
		{"maintenance", []string{"maintenance", "downtime", "cutover", "restart", "maintenance window", "scheduled maintenance"}},
		{"outage", []string{"incident", "degraded", "unavailable", "service disruption", "outage"}},
		{"market_structure", []string{"tick size", "sub-cent", "subcent", "sub-penny", "subpenny", "collateral", "pusd", "orderbook", "order book", "matching engine", "partial-contract", "fractional", "quantity scale", "minimum trade", "margin", "rfq"}},
		{"compliance", []string{"regulatory", "rulebook", "compliance", "cftc", "clearing", "market maker program", "liquidity provider program"}},
	}
	set := map[string]bool{}
	for _, rule := range rules {
		for _, word := range rule.Words {
			if venueNoticeContains(text, word) {
				set[rule.Class] = true
				break
			}
		}
	}
	if len(set) == 0 {
		set["general"] = true
	}
	classes := make([]string, 0, len(set))
	for class := range set {
		classes = append(classes, class)
	}
	sort.Strings(classes)
	severity := "info"
	for _, phrase := range []string{"no backwards compatibility", "must migrate", "trading unavailable", "removed", "downtime", "service disruption", "outage"} {
		if strings.Contains(text, phrase) {
			return classes, "breaking"
		}
	}
	if set["schema"] || set["fee"] || set["maintenance"] || set["market_structure"] || set["outage"] {
		severity = "attention"
	}
	return classes, severity
}

func venueNoticeContains(text, keyword string) bool {
	if strings.Contains(keyword, " ") || strings.Contains(keyword, "-") {
		return strings.Contains(text, keyword)
	}
	for start := 0; ; {
		index := strings.Index(text[start:], keyword)
		if index < 0 {
			return false
		}
		index += start
		leftOK := index == 0 || !venueNoticeWordByte(text[index-1])
		right := index + len(keyword)
		rightOK := right == len(text) || !venueNoticeWordByte(text[right])
		if leftOK && rightOK {
			return true
		}
		start = index + 1
	}
}

func venueNoticeWordByte(b byte) bool {
	return b >= 'a' && b <= 'z' || b >= '0' && b <= '9' || b == '_'
}

func sweepVenueNoticeSources(ctx context.Context, s *Server, sources []venueNoticeSource, client *http.Client) ([]venueNoticeFetchResult, storage.VenueNoticeBatchResult, error) {
	cache := venueNoticeCacheFor(s)
	results := make([]venueNoticeFetchResult, len(sources))
	sem := make(chan struct{}, 3)
	var wg sync.WaitGroup
	for i, src := range sources {
		wg.Add(1)
		go func(i int, src venueNoticeSource) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				results[i] = venueNoticeFetchResult{Source: src, ErrorClass: "timeout", SafeErrorText: "notice sweep context ended"}
				return
			}
			results[i] = fetchVenueNoticeSource(ctx, client, cache, src)
		}(i, src)
	}
	wg.Wait()
	var notices []storage.VenueNotice
	for _, result := range results {
		notices = append(notices, result.Notices...)
	}
	if len(notices) == 0 {
		return results, storage.VenueNoticeBatchResult{}, nil
	}
	batch, err := s.store.InsertVenueNoticeBatch(ctx, time.Now(), notices)
	return results, batch, err
}

func (s *Server) sweepVenueNotices(ctx context.Context) {
	started := time.Now()
	// The in-process due gate prevents duplicate cadence work. The receipt identity must still name
	// the actual attempt: after a restart inside the same 15-minute bucket, truncation used to hide a
	// completed 27-source fetch behind the prior process's INSERT OR IGNORE row.
	cycleID := venueNoticeCycleID(started)
	results, batch, insertErr := sweepVenueNoticeSources(ctx, s, venueNoticeSourceProvider(), venueNoticeClient())
	metrics := map[string]any{}
	failed := make([]string, 0)
	for _, result := range results {
		metrics[result.Source.Name] = map[string]any{"venue": result.Source.Venue, "kind": result.Source.Kind,
			"items": len(result.Notices), "bytes": result.Bytes, "not_modified": result.NotModified,
			"duration_ms": float64(result.Duration.Microseconds()) / 1000, "error_class": result.ErrorClass}
		if result.ErrorClass != "" {
			failed = append(failed, result.Source.Name+":"+result.ErrorClass)
		}
	}
	status, zeroReason, errorClass, errorText := "healthy", "", "", ""
	if insertErr != nil {
		status, errorClass, errorText = "blocked", "storage", "immutable venue notice persistence failed"
	} else if len(failed) > 0 {
		status, errorClass = "blocked", "source_fetch"
		sort.Strings(failed)
		errorText = "official notice sources failed: " + strings.Join(failed, ",")
	} else if batch.Inserted == 0 {
		status, zeroReason = "healthy_empty", "all sources were unchanged or all parsed notices were already stored"
	}
	receipt := storage.CollectorReceipt{CollectorID: "venue-notices", CycleID: cycleID,
		Status: status, ZeroReason: zeroReason, ErrorClass: errorClass, ErrorText: errorText,
		Source:        "official venue notices plus direct fee, incentive, rule, documentation-index and API-schema artifacts",
		SchemaVersion: "venue-notices-r138-v2", Started: started, Completed: time.Now(),
		Eligible: batch.Seen, Attempted: len(results), Inserted: batch.Inserted, Duplicates: batch.Duplicates,
		ExpectedZero:    batch.Inserted == 0 && len(failed) == 0 && insertErr == nil,
		ExpectedCadence: venueNoticeCadence, Metrics: metrics, Systems: storage.ResearchExperimentIDs(),
		Exclusions: map[string]int{"source_fetch_failed": len(failed)}}
	if _, err := s.store.InsertCollectorReceipt(ctx, receipt); err != nil && s.log != nil {
		s.log.Warn("venue notice collector receipt failed", "err", err)
	}
	// This source is arrival-clock only. Preserve the actual cycle and partial failures without
	// inventing a publication timestamp for unchanged/undated official documents.
	s.recordR138SourceClock(ctx, "venue-notice-watcher", cycleID, time.Time{}, batch.Seen, nil, nil, nil, nil,
		map[string]any{"sources": len(results), "failed_sources": len(failed), "inserted": batch.Inserted,
			"duplicates": batch.Duplicates}, errorClass, errorText)
}

func (s *Server) handleVenueNotices(w http.ResponseWriter, r *http.Request) {
	limit := 100
	if raw := strings.TrimSpace(r.URL.Query().Get("limit")); raw != "" {
		var err error
		if limit, err = strconv.Atoi(raw); err != nil || limit <= 0 || limit > 500 {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "limit must be between 1 and 500"})
			return
		}
	}
	report, err := s.store.VenueNoticeReport(r.Context(), limit)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "venue notice report unavailable"})
		return
	}
	if liveness, err := s.store.CollectorLivenessReport(r.Context()); err == nil {
		if rows, ok := liveness["collectors"].([]storage.CollectorLivenessView); ok {
			for _, row := range rows {
				if row.CollectorID == "venue-notices" {
					report["collector"] = row
					break
				}
			}
		}
	}
	writeJSON(w, http.StatusOK, report)
}
