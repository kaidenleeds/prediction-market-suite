package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

// VenueNotice is one immutable version of an authoritative public venue notice. Fetch metadata is
// retained only for provenance/liveness; no settings, fees, routes, or order code reads this table.
type VenueNotice struct {
	Venue, SourceName, SourceID, Title, Summary        string
	PublishedTS, EffectiveTS, SourceURL, SourceFeed    string
	Severity, ArtifactHash, FeedETag, FeedLastModified string
	Categories, Classes                                []string
}

type VenueNoticeBatchResult struct {
	Seen, Inserted, Versioned, Duplicates, Sightings int
}

type venueNoticeSemantic struct {
	Venue, SourceName, SourceID, Title, Summary     string
	PublishedTS, EffectiveTS, SourceURL, SourceFeed string
	Severity, ArtifactHash                          string
	Categories, Classes                             []string
}

func venueNoticeStringSet(in []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(in))
	for _, value := range in {
		value = strings.ToLower(strings.TrimSpace(value))
		if value != "" && !seen[value] {
			seen[value] = true
			out = append(out, value)
		}
	}
	sort.Strings(out)
	return out
}

func venueNoticeDigest(v any) (string, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:]), nil
}

func normalizeVenueNotice(n VenueNotice) (VenueNotice, string, error) {
	n.Venue = strings.ToLower(strings.TrimSpace(n.Venue))
	n.SourceName, n.SourceID = strings.TrimSpace(n.SourceName), strings.TrimSpace(n.SourceID)
	n.Title, n.Summary = strings.TrimSpace(n.Title), strings.TrimSpace(n.Summary)
	n.SourceURL, n.SourceFeed = strings.TrimSpace(n.SourceURL), strings.TrimSpace(n.SourceFeed)
	switch n.Venue {
	case "kalshi", "polyus", "polymarket":
	default:
		return n, "", fmt.Errorf("unsupported notice venue %q", n.Venue)
	}
	if n.SourceName == "" || n.SourceID == "" || n.Title == "" || n.SourceFeed == "" {
		return n, "", errors.New("incomplete venue notice identity")
	}
	if !strings.HasPrefix(n.SourceFeed, "https://") || (n.SourceURL != "" && !strings.HasPrefix(n.SourceURL, "https://")) {
		return n, "", errors.New("venue notice source must use https")
	}
	if len(n.Title) > 1000 || len(n.Summary) > 8000 || len(n.SourceID) > 1000 {
		return n, "", errors.New("venue notice exceeds bounded field size")
	}
	n.Categories = venueNoticeStringSet(n.Categories)
	n.Classes = venueNoticeStringSet(n.Classes)
	if len(n.Classes) == 0 {
		n.Classes = []string{"general"}
	}
	allowed := map[string]bool{"schema": true, "fee": true, "maintenance": true, "market_structure": true,
		"outage": true, "compliance": true, "general": true}
	for _, class := range n.Classes {
		if !allowed[class] {
			return n, "", fmt.Errorf("unsupported notice class %q", class)
		}
	}
	n.Severity = strings.ToLower(strings.TrimSpace(n.Severity))
	switch n.Severity {
	case "info", "attention", "breaking":
	default:
		n.Severity = "info"
	}
	if n.ArtifactHash == "" {
		n.ArtifactHash, _ = venueNoticeDigest(struct{ Title, Summary string }{n.Title, n.Summary})
	}
	h, err := venueNoticeDigest(venueNoticeSemantic{Venue: n.Venue, SourceName: n.SourceName,
		SourceID: n.SourceID, Title: n.Title, Summary: n.Summary, PublishedTS: n.PublishedTS,
		EffectiveTS: n.EffectiveTS, SourceURL: n.SourceURL, SourceFeed: n.SourceFeed,
		Severity: n.Severity, ArtifactHash: n.ArtifactHash, Categories: n.Categories, Classes: n.Classes})
	return n, h, err
}

func insertVenueNoticeSighting(ctx context.Context, tx *sql.Tx, noticeID int64, observed time.Time, n VenueNotice) (bool, error) {
	res, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO research_venue_notice_sightings(
notice_id,observed_ts,slot,feed_etag,feed_last_modified) VALUES(?,?,?,?,?)`, noticeID,
		observed.UTC().Format(time.RFC3339Nano), observed.UTC().Truncate(15*time.Minute).Format(time.RFC3339),
		n.FeedETag, n.FeedLastModified)
	if err != nil {
		return false, err
	}
	rows, err := res.RowsAffected()
	return rows > 0, err
}

// InsertVenueNoticeBatch appends semantic versions and bounded liveness sightings atomically.
// An edited RSS/PDF/document item receives the next version; old bytes remain immutable.
func (s *Store) InsertVenueNoticeBatch(ctx context.Context, observed time.Time, notices []VenueNotice) (VenueNoticeBatchResult, error) {
	var out VenueNoticeBatchResult
	if observed.IsZero() {
		observed = time.Now()
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return out, err
	}
	defer func() { _ = tx.Rollback() }()
	for _, raw := range notices {
		out.Seen++
		n, hash, err := normalizeVenueNotice(raw)
		if err != nil {
			return out, err
		}
		categories, _ := json.Marshal(n.Categories)
		classes, _ := json.Marshal(n.Classes)
		var noticeID int64
		var version int
		err = tx.QueryRowContext(ctx, `SELECT id,version FROM research_venue_notices
WHERE venue=? AND source_name=? AND source_id=? AND content_hash=?`, n.Venue, n.SourceName, n.SourceID, hash).
			Scan(&noticeID, &version)
		if err == nil {
			out.Duplicates++
			if sighted, err := insertVenueNoticeSighting(ctx, tx, noticeID, observed, n); err != nil {
				return out, err
			} else if sighted {
				out.Sightings++
			}
			continue
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return out, err
		}
		if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(version),0)+1 FROM research_venue_notices
WHERE venue=? AND source_name=? AND source_id=?`, n.Venue, n.SourceName, n.SourceID).Scan(&version); err != nil {
			return out, err
		}
		res, err := tx.ExecContext(ctx, `INSERT INTO research_venue_notices(
venue,source_name,source_id,version,content_hash,title,summary,published_ts,effective_ts,
source_url,source_feed,categories_json,change_classes_json,severity,artifact_hash,first_seen_ts)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, n.Venue, n.SourceName, n.SourceID, version, hash,
			n.Title, n.Summary, n.PublishedTS, n.EffectiveTS, n.SourceURL, n.SourceFeed,
			string(categories), string(classes), n.Severity, n.ArtifactHash,
			observed.UTC().Format(time.RFC3339Nano))
		if err != nil {
			return out, err
		}
		noticeID, err = res.LastInsertId()
		if err != nil {
			return out, err
		}
		out.Inserted++
		if version > 1 {
			out.Versioned++
		}
		if sighted, err := insertVenueNoticeSighting(ctx, tx, noticeID, observed, n); err != nil {
			return out, err
		} else if sighted {
			out.Sightings++
		}
	}
	if err := tx.Commit(); err != nil {
		return out, err
	}
	return out, nil
}

type VenueNoticeView struct {
	Venue        string   `json:"venue"`
	SourceName   string   `json:"source_name"`
	SourceID     string   `json:"source_id"`
	Title        string   `json:"title"`
	Summary      string   `json:"summary"`
	PublishedTS  string   `json:"published_ts"`
	EffectiveTS  string   `json:"effective_ts"`
	SourceURL    string   `json:"source_url"`
	SourceFeed   string   `json:"source_feed"`
	Severity     string   `json:"severity"`
	ArtifactHash string   `json:"artifact_hash"`
	ContentHash  string   `json:"content_hash"`
	FirstSeenTS  string   `json:"first_seen_ts"`
	Version      int      `json:"version"`
	Sightings    int      `json:"sightings"`
	Categories   []string `json:"categories"`
	Classes      []string `json:"change_classes"`
}

func venueNoticeCounts(rows *sql.Rows) (map[string]int, error) {
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var key string
		var n int
		if err := rows.Scan(&key, &n); err != nil {
			return nil, err
		}
		out[key] = n
	}
	return out, rows.Err()
}

func (s *Store) VenueNoticeReport(ctx context.Context, limit int) (map[string]any, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `SELECT venue,COUNT(*) FROM research_venue_notices GROUP BY venue ORDER BY venue`)
	if err != nil {
		return nil, err
	}
	versionCounts, err := venueNoticeCounts(rows)
	if err != nil {
		return nil, err
	}
	rows, err = s.db.QueryContext(ctx, `SELECT n.venue,COUNT(*) FROM research_venue_notices n
WHERE n.version=(SELECT MAX(x.version) FROM research_venue_notices x
 WHERE x.venue=n.venue AND x.source_name=n.source_name AND x.source_id=n.source_id)
GROUP BY n.venue ORDER BY n.venue`)
	if err != nil {
		return nil, err
	}
	currentCounts, err := venueNoticeCounts(rows)
	if err != nil {
		return nil, err
	}
	rows, err = s.db.QueryContext(ctx, `SELECT j.value,COUNT(*) FROM research_venue_notices n,
json_each(n.change_classes_json) j
WHERE n.version=(SELECT MAX(x.version) FROM research_venue_notices x
 WHERE x.venue=n.venue AND x.source_name=n.source_name AND x.source_id=n.source_id)
GROUP BY j.value ORDER BY j.value`)
	if err != nil {
		return nil, err
	}
	classCounts, err := venueNoticeCounts(rows)
	if err != nil {
		return nil, err
	}
	recent, err := s.db.QueryContext(ctx, `SELECT n.venue,n.source_name,n.source_id,n.version,
n.content_hash,n.title,n.summary,n.published_ts,n.effective_ts,n.source_url,n.source_feed,
n.categories_json,n.change_classes_json,n.severity,n.artifact_hash,n.first_seen_ts,
(SELECT COUNT(*) FROM research_venue_notice_sightings x WHERE x.notice_id=n.id)
FROM research_venue_notices n
WHERE n.version=(SELECT MAX(v.version) FROM research_venue_notices v
 WHERE v.venue=n.venue AND v.source_name=n.source_name AND v.source_id=n.source_id)
ORDER BY COALESCE(NULLIF(n.published_ts,''),n.first_seen_ts) DESC,n.id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer recent.Close()
	views := make([]VenueNoticeView, 0, limit)
	for recent.Next() {
		var v VenueNoticeView
		var categories, classes string
		if err := recent.Scan(&v.Venue, &v.SourceName, &v.SourceID, &v.Version, &v.ContentHash,
			&v.Title, &v.Summary, &v.PublishedTS, &v.EffectiveTS, &v.SourceURL, &v.SourceFeed,
			&categories, &classes, &v.Severity, &v.ArtifactHash, &v.FirstSeenTS, &v.Sightings); err != nil {
			return nil, err
		}
		_ = json.Unmarshal([]byte(categories), &v.Categories)
		_ = json.Unmarshal([]byte(classes), &v.Classes)
		views = append(views, v)
	}
	if err := recent.Err(); err != nil {
		return nil, err
	}
	return map[string]any{
		"generated_at":  time.Now().UTC().Format(time.RFC3339Nano),
		"research_only": true, "funded": false, "paper_authority": false, "live_authority": false,
		"automatic_config_changes": false, "immutable_history": true,
		"policy":            "advisory evidence only; notices never mutate fees, schemas, settings, sizing, routes, or order authority",
		"counts_by_venue":   currentCounts,
		"versions_by_venue": versionCounts,
		"counts_by_class":   classCounts,
		"recent":            views,
	}, nil
}
