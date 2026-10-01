package storage

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// migrateGeneralRulePairSchema upgrades the original K-PUS-only rule ledger in place. The legacy
// columns remain populated so an older read-only tool can still inspect the database, but all new
// authority checks use the explicit venue-native left/right identity. Existing immutable rows are
// not reinterpreted: their generic fields are a lossless transcription of the old K-PUS columns.
func migrateGeneralRulePairSchema(db *sql.DB) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var artifactDDL string
	if err := tx.QueryRow(`SELECT sql FROM sqlite_master WHERE type='table' AND name='research_rule_artifacts'`).Scan(&artifactDDL); err != nil {
		return err
	}
	if !strings.Contains(strings.ToLower(artifactDDL), "'polymarket'") {
		for _, ddl := range []string{
			`DROP TRIGGER IF EXISTS research_rule_artifacts_no_update`,
			`DROP TRIGGER IF EXISTS research_rule_artifacts_no_delete`,
			`DROP INDEX IF EXISTS idx_rrule_artifact_latest`,
			`ALTER TABLE research_rule_artifacts RENAME TO research_rule_artifacts_r139_legacy`,
			`CREATE TABLE research_rule_artifacts (
venue TEXT NOT NULL CHECK(venue IN ('kalshi','polyus','polymarket')),instrument_id TEXT NOT NULL,
version INTEGER NOT NULL CHECK(version>0),artifact_hash TEXT NOT NULL CHECK(length(artifact_hash)=64),
observed_ts TEXT NOT NULL,source TEXT NOT NULL,raw_rules_json TEXT NOT NULL CHECK(json_valid(raw_rules_json)),
settlement_sources_json TEXT NOT NULL CHECK(json_valid(settlement_sources_json)),capture_state TEXT NOT NULL
CHECK(capture_state IN ('RAW_COMPLETE_REVIEW_REQUIRED','RAW_RULES_ONLY','SETTLEMENT_SOURCE_ONLY')),
funded INTEGER NOT NULL DEFAULT 0 CHECK(funded=0),paper_authority INTEGER NOT NULL DEFAULT 0 CHECK(paper_authority=0),
live_authority INTEGER NOT NULL DEFAULT 0 CHECK(live_authority=0),PRIMARY KEY(venue,instrument_id,version),
UNIQUE(venue,instrument_id,artifact_hash))`,
			`INSERT INTO research_rule_artifacts SELECT * FROM research_rule_artifacts_r139_legacy`,
			`DROP TABLE research_rule_artifacts_r139_legacy`,
			`CREATE INDEX idx_rrule_artifact_latest ON research_rule_artifacts(venue,instrument_id,version DESC)`,
			`CREATE TRIGGER research_rule_artifacts_no_update BEFORE UPDATE ON research_rule_artifacts BEGIN SELECT RAISE(ABORT,'immutable venue rule artifact'); END`,
			`CREATE TRIGGER research_rule_artifacts_no_delete BEFORE DELETE ON research_rule_artifacts BEGIN SELECT RAISE(ABORT,'immutable venue rule artifact'); END`,
		} {
			if _, err := tx.Exec(ddl); err != nil {
				return err
			}
		}
	}
	for _, trigger := range []string{
		"research_rule_pair_certificates_no_update", "research_rule_pair_certificates_no_delete",
		"research_rule_pair_reviews_no_update", "research_rule_pair_reviews_no_delete",
	} {
		if _, err := tx.Exec("DROP TRIGGER IF EXISTS " + trigger); err != nil {
			return err
		}
	}
	for table, columns := range map[string][]string{
		"research_rule_pair_certificates": {
			"left_venue TEXT NOT NULL DEFAULT ''", "left_instrument_id TEXT NOT NULL DEFAULT ''",
			"right_venue TEXT NOT NULL DEFAULT ''", "right_instrument_id TEXT NOT NULL DEFAULT ''",
			"orientation TEXT NOT NULL DEFAULT 'same'", "left_artifact_hash TEXT NOT NULL DEFAULT ''",
			"right_artifact_hash TEXT NOT NULL DEFAULT ''", "left_payoff_id TEXT NOT NULL DEFAULT ''",
			"right_payoff_id TEXT NOT NULL DEFAULT ''",
		},
		"research_rule_pair_reviews": {
			"left_venue TEXT NOT NULL DEFAULT ''", "left_instrument_id TEXT NOT NULL DEFAULT ''",
			"right_venue TEXT NOT NULL DEFAULT ''", "right_instrument_id TEXT NOT NULL DEFAULT ''",
			"orientation TEXT NOT NULL DEFAULT 'same'", "left_payoff_id TEXT NOT NULL DEFAULT ''",
			"right_payoff_id TEXT NOT NULL DEFAULT ''", "left_artifact_hash TEXT NOT NULL DEFAULT ''",
			"right_artifact_hash TEXT NOT NULL DEFAULT ''",
		},
	} {
		for _, column := range columns {
			if _, err := tx.Exec("ALTER TABLE " + table + " ADD COLUMN " + column); err != nil &&
				!strings.Contains(strings.ToLower(err.Error()), "duplicate column") {
				return err
			}
		}
	}
	if _, err := tx.Exec(`UPDATE research_rule_pair_certificates SET
left_venue='kalshi',left_instrument_id=kalshi_ticker,right_venue='polyus',
right_instrument_id=polyus_slug,orientation='same',left_artifact_hash=kalshi_artifact_hash,
right_artifact_hash=polyus_artifact_hash,left_payoff_id=canonical_payoff_id,
right_payoff_id=canonical_payoff_id WHERE left_venue=''`); err != nil {
		return err
	}
	if _, err := tx.Exec(`UPDATE research_rule_pair_reviews SET
left_venue='kalshi',left_instrument_id=kalshi_ticker,right_venue='polyus',
right_instrument_id=polyus_slug,orientation='same',left_artifact_hash=kalshi_artifact_hash,
right_artifact_hash=polyus_artifact_hash,left_payoff_id=canonical_payoff_id,
right_payoff_id=canonical_payoff_id WHERE left_venue=''`); err != nil {
		return err
	}
	for _, ddl := range []string{
		`CREATE INDEX IF NOT EXISTS idx_rrule_certificate_generic ON research_rule_pair_certificates(left_venue,left_instrument_id,right_venue,right_instrument_id,version DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_rrule_certificate_reverse ON research_rule_pair_certificates(right_venue,right_instrument_id,left_venue,left_instrument_id,version DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_rrule_review_generic ON research_rule_pair_reviews(left_venue,left_instrument_id,right_venue,right_instrument_id,id DESC)`,
		`CREATE TRIGGER IF NOT EXISTS research_rule_pair_certificates_no_update BEFORE UPDATE ON research_rule_pair_certificates BEGIN SELECT RAISE(ABORT,'immutable normalized rule certificate'); END`,
		`CREATE TRIGGER IF NOT EXISTS research_rule_pair_certificates_no_delete BEFORE DELETE ON research_rule_pair_certificates BEGIN SELECT RAISE(ABORT,'immutable normalized rule certificate'); END`,
		`CREATE TRIGGER IF NOT EXISTS research_rule_pair_reviews_no_update BEFORE UPDATE ON research_rule_pair_reviews BEGIN SELECT RAISE(ABORT,'append-only rule pair review'); END`,
		`CREATE TRIGGER IF NOT EXISTS research_rule_pair_reviews_no_delete BEFORE DELETE ON research_rule_pair_reviews BEGIN SELECT RAISE(ABORT,'append-only rule pair review'); END`,
	} {
		if _, err := tx.Exec(ddl); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// migratePolymarketGameAlias runs after storage.Open has created the legacy dynamic market_game
// table. Keeping it separate from the schema migration lets fresh databases boot cleanly.
func migratePolymarketGameAlias(db *sql.DB) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.Exec(`INSERT INTO market_game(venue,ticker,game_id,mkt_type,yes_team,no_team,line,src,first_seen,last_seen)
SELECT 'polymarket',ticker,game_id,mkt_type,yes_team,no_team,line,src,first_seen,? FROM market_game
WHERE venue='polyint'
ON CONFLICT(venue,ticker) DO UPDATE SET game_id=excluded.game_id,mkt_type=excluded.mkt_type,
yes_team=excluded.yes_team,no_team=excluded.no_team,line=excluded.line,src=excluded.src,
last_seen=excluded.last_seen`, nowRFC()); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM market_game WHERE venue='polyint'`); err != nil {
		return err
	}
	return tx.Commit()
}

type RuleSettlementSource struct {
	Name string `json:"name"`
	URL  string `json:"url"`
}

type VenueRuleArtifact struct {
	Venue, InstrumentID, Source                             string
	Primary, Secondary, EarlyClose, Description, SportsType string
	SettlementSources                                       []RuleSettlementSource
	Observed                                                time.Time
	Version                                                 int
	ArtifactHash, CaptureState                              string
	Funded, PaperAuthority, LiveAuthority                   bool
}

type venueRuleArtifactHash struct {
	Venue, InstrumentID, Source                             string
	Primary, Secondary, EarlyClose, Description, SportsType string
	SettlementSources                                       []RuleSettlementSource
}

func normalizeRuleSources(in []RuleSettlementSource) []RuleSettlementSource {
	out := make([]RuleSettlementSource, 0, len(in))
	seen := map[string]bool{}
	for _, source := range in {
		source.Name, source.URL = strings.TrimSpace(source.Name), strings.TrimSpace(source.URL)
		if source.Name == "" && source.URL == "" {
			continue
		}
		key := source.Name + "\x00" + source.URL
		if !seen[key] {
			seen[key] = true
			out = append(out, source)
		}
	}
	return out
}

func (s *Store) InsertVenueRuleArtifact(ctx context.Context, in VenueRuleArtifact) (VenueRuleArtifact, bool, error) {
	in.Venue, in.InstrumentID, in.Source = strings.ToLower(strings.TrimSpace(in.Venue)), strings.TrimSpace(in.InstrumentID), strings.TrimSpace(in.Source)
	in.Primary, in.Secondary = strings.TrimSpace(in.Primary), strings.TrimSpace(in.Secondary)
	in.EarlyClose, in.Description, in.SportsType = strings.TrimSpace(in.EarlyClose), strings.TrimSpace(in.Description), strings.TrimSpace(in.SportsType)
	in.SettlementSources = normalizeRuleSources(in.SettlementSources)
	if in.Observed.IsZero() {
		in.Observed = time.Now().UTC()
	}
	hasRules := in.Primary != "" || in.Secondary != "" || in.EarlyClose != "" || in.Description != ""
	if (in.Venue != "kalshi" && in.Venue != "polyus" && in.Venue != "polymarket") || in.InstrumentID == "" || in.Source == "" ||
		(!hasRules && len(in.SettlementSources) == 0) || in.Funded || in.PaperAuthority || in.LiveAuthority {
		return VenueRuleArtifact{}, false, errors.New("invalid raw venue rule artifact or authority")
	}
	switch {
	case hasRules && len(in.SettlementSources) > 0:
		in.CaptureState = "RAW_COMPLETE_REVIEW_REQUIRED"
	case hasRules:
		in.CaptureState = "RAW_RULES_ONLY"
	default:
		in.CaptureState = "SETTLEMENT_SOURCE_ONLY"
	}
	semantic := venueRuleArtifactHash{Venue: in.Venue, InstrumentID: in.InstrumentID, Source: in.Source,
		Primary: in.Primary, Secondary: in.Secondary, EarlyClose: in.EarlyClose,
		Description: in.Description, SportsType: in.SportsType, SettlementSources: in.SettlementSources}
	var err error
	in.ArtifactHash, err = researchSpecHash(semantic)
	if err != nil {
		return VenueRuleArtifact{}, false, err
	}
	var version int
	err = s.db.QueryRowContext(ctx, `SELECT version FROM research_rule_artifacts
WHERE venue=? AND instrument_id=? AND artifact_hash=?`, in.Venue, in.InstrumentID, in.ArtifactHash).Scan(&version)
	if err == nil {
		in.Version = version
		return in, false, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return VenueRuleArtifact{}, false, err
	}
	rawRules, _ := json.Marshal(map[string]string{"primary": in.Primary, "secondary": in.Secondary,
		"early_close": in.EarlyClose, "description": in.Description, "sports_market_type": in.SportsType})
	sources, _ := json.Marshal(in.SettlementSources)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return VenueRuleArtifact{}, false, err
	}
	defer func() { _ = tx.Rollback() }()
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(version),0)+1 FROM research_rule_artifacts
WHERE venue=? AND instrument_id=?`, in.Venue, in.InstrumentID).Scan(&in.Version); err != nil {
		return VenueRuleArtifact{}, false, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO research_rule_artifacts(venue,instrument_id,version,
artifact_hash,observed_ts,source,raw_rules_json,settlement_sources_json,capture_state,
funded,paper_authority,live_authority) VALUES(?,?,?,?,?,?,?,?,?,0,0,0)`, in.Venue, in.InstrumentID,
		in.Version, in.ArtifactHash, in.Observed.UTC().Format(time.RFC3339Nano), in.Source,
		string(rawRules), string(sources), in.CaptureState)
	if err != nil {
		return VenueRuleArtifact{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return VenueRuleArtifact{}, false, err
	}
	return in, true, nil
}

func scanVenueRuleArtifact(row interface{ Scan(...any) error }) (VenueRuleArtifact, error) {
	var out VenueRuleArtifact
	var observed, rawRules, sources string
	var funded, paper, live int
	err := row.Scan(&out.Venue, &out.InstrumentID, &out.Version, &out.ArtifactHash, &observed,
		&out.Source, &rawRules, &sources, &out.CaptureState, &funded, &paper, &live)
	if err != nil {
		return VenueRuleArtifact{}, err
	}
	var rules map[string]string
	_ = json.Unmarshal([]byte(rawRules), &rules)
	out.Primary, out.Secondary, out.EarlyClose = rules["primary"], rules["secondary"], rules["early_close"]
	out.Description, out.SportsType = rules["description"], rules["sports_market_type"]
	_ = json.Unmarshal([]byte(sources), &out.SettlementSources)
	out.Observed, _ = time.Parse(time.RFC3339Nano, observed)
	out.Funded, out.PaperAuthority, out.LiveAuthority = funded != 0, paper != 0, live != 0
	return out, nil
}

func (s *Store) LatestVenueRuleArtifact(ctx context.Context, venue, instrumentID string) (VenueRuleArtifact, bool, error) {
	row := s.db.QueryRowContext(ctx, `SELECT venue,instrument_id,version,artifact_hash,observed_ts,
source,raw_rules_json,settlement_sources_json,capture_state,funded,paper_authority,live_authority
FROM research_rule_artifacts WHERE venue=? AND instrument_id=? ORDER BY version DESC LIMIT 1`,
		strings.ToLower(strings.TrimSpace(venue)), strings.TrimSpace(instrumentID))
	out, err := scanVenueRuleArtifact(row)
	if errors.Is(err, sql.ErrNoRows) {
		return VenueRuleArtifact{}, false, nil
	}
	return out, err == nil, err
}

type StructuralRulePair struct {
	PairID, PairType, GameID, MarketType, Orientation string
	LeftMarketType, RightMarketType                   string
	LeftTitle, RightTitle                             string
	League, Away, Home, StartUTC                      string
	LeftBoundaryUTC, RightBoundaryUTC                 string
	LeftVenue, LeftInstrumentID, RightVenue           string
	RightInstrumentID, LeftYesTeam, LeftNoTeam        string
	RightYesTeam, RightNoTeam, LastSeen               string
	CanonicalEventID, CanonicalPayoffID               string
	LeftPayoffID, RightPayoffID                       string
	LeftLine, RightLine                               float64
	// Legacy aliases are populated only for the Kalshi-PolyUS pair.
	KalshiTicker, PolyUSSlug, YesTeam, NoTeam string
	Line                                      float64
}

func normalizeRuleVenue(venue string) string {
	venue = strings.ToLower(strings.TrimSpace(venue))
	if venue == "polyint" {
		return "polymarket"
	}
	return venue
}

func ruleVenueRank(venue string) int {
	switch normalizeRuleVenue(venue) {
	case "kalshi":
		return 1
	case "polyus":
		return 2
	case "polymarket":
		return 3
	default:
		return 99
	}
}

func rulePairType(leftVenue, rightVenue string) string {
	switch normalizeRuleVenue(leftVenue) + "|" + normalizeRuleVenue(rightVenue) {
	case "kalshi|polyus":
		return "K-PUS"
	case "kalshi|polymarket":
		return "K-PINT"
	case "polyus|polymarket":
		return "PUS-PINT"
	default:
		return ""
	}
}

func rulePairPrefix(leftVenue, rightVenue string) string {
	switch rulePairType(leftVenue, rightVenue) {
	case "K-PUS":
		return "k-pus"
	case "K-PINT":
		return "k-pint"
	case "PUS-PINT":
		return "pus-pint"
	default:
		return ""
	}
}

func canonicalRulePair(leftVenue, leftID, rightVenue, rightID string) (string, string, string, string, string, bool) {
	leftVenue, rightVenue = normalizeRuleVenue(leftVenue), normalizeRuleVenue(rightVenue)
	leftID, rightID = strings.TrimSpace(leftID), strings.TrimSpace(rightID)
	if leftID == "" || rightID == "" || leftVenue == rightVenue || ruleVenueRank(leftVenue) > 3 || ruleVenueRank(rightVenue) > 3 {
		return "", "", "", "", "", false
	}
	if ruleVenueRank(leftVenue) > ruleVenueRank(rightVenue) {
		leftVenue, rightVenue, leftID, rightID = rightVenue, leftVenue, rightID, leftID
	}
	prefix := rulePairPrefix(leftVenue, rightVenue)
	if prefix == "" {
		return "", "", "", "", "", false
	}
	return leftVenue, leftID, rightVenue, rightID, prefix + "|" + leftID + "|" + rightID, true
}

func structuralRuleOrientation(kind, leftYes, leftNo string, leftLine float64, rightYes, rightNo string, rightLine float64) (string, bool) {
	kind = strings.ToLower(strings.TrimSpace(kind))
	leftYes, leftNo = strings.ToUpper(strings.TrimSpace(leftYes)), strings.ToUpper(strings.TrimSpace(leftNo))
	rightYes, rightNo = strings.ToUpper(strings.TrimSpace(rightYes)), strings.ToUpper(strings.TrimSpace(rightNo))
	lineSame := leftLine-rightLine < 0.0000001 && rightLine-leftLine < 0.0000001
	lineInverse := leftLine+rightLine < 0.0000001 && -(leftLine+rightLine) < 0.0000001
	if leftYes == rightYes && leftYes != "" {
		switch kind {
		case "winner", "advance":
			if leftNo == "" || rightNo == "" || leftNo == rightNo {
				return "same", true
			}
		case "spread", "total":
			if lineSame && (leftNo == "" || rightNo == "" || leftNo == rightNo) {
				return "same", true
			}
		}
	}
	if leftYes != "" && rightYes != "" {
		switch kind {
		case "winner", "advance":
			if leftNo != "" && rightNo != "" && leftYes == rightNo && leftNo == rightYes {
				return "inverse", true
			}
		case "spread":
			if leftNo != "" && rightNo != "" && leftYes == rightNo && leftNo == rightYes && lineInverse {
				return "inverse", true
			}
		case "total":
			if lineSame && ((leftYes == "OVER" && rightYes == "UNDER") || (leftYes == "UNDER" && rightYes == "OVER")) {
				return "inverse", true
			}
		}
	}
	return "", false
}

type StructuralRuleCursor struct {
	LeftVenue         string `json:"left_venue,omitempty"`
	LeftInstrumentID  string `json:"left_instrument_id,omitempty"`
	RightVenue        string `json:"right_venue,omitempty"`
	RightInstrumentID string `json:"right_instrument_id,omitempty"`
}

type StructuralRuleCandidatePage struct {
	Rows            []StructuralRulePair
	Next            StructuralRuleCursor
	Total, Deferred int
	Wrapped         bool
}

func (s *Store) structuralRuleCandidatePageOnce(ctx context.Context, cutoffText string,
	after StructuralRuleCursor, limit int) ([]StructuralRulePair, error) {
	after.LeftVenue, after.LeftInstrumentID = normalizeRuleVenue(after.LeftVenue), strings.TrimSpace(after.LeftInstrumentID)
	after.RightVenue, after.RightInstrumentID = normalizeRuleVenue(after.RightVenue), strings.TrimSpace(after.RightInstrumentID)
	afterSet := after.LeftVenue != "" && after.LeftInstrumentID != "" && after.RightVenue != "" && after.RightInstrumentID != ""
	afterFlag := 0
	if afterSet {
		afterFlag = 1
	}
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `SELECT a.venue,a.ticker,a.game_id,a.mkt_type,a.yes_team,a.no_team,a.line,
b.venue,b.ticker,b.mkt_type,b.yes_team,b.no_team,b.line,
CASE WHEN a.last_seen>b.last_seen THEN a.last_seen ELSE b.last_seen END,
COALESCE(g.league,''),COALESCE(g.away,''),COALESCE(g.home,''),COALESCE(g.start_utc,''),
COALESCE(ca.close_ts,''),COALESCE(cb.close_ts,''),COALESCE(ca.title,''),COALESCE(cb.title,'')
FROM market_game a JOIN market_game b ON b.game_id=a.game_id AND b.src='struct'
 AND (lower(trim(b.mkt_type))=lower(trim(a.mkt_type)) OR
      (lower(trim(a.mkt_type)) IN ('prop','player_scorer','player_prop') AND
       lower(trim(b.mkt_type)) IN ('prop','player_scorer','player_prop')))
LEFT JOIN game_identity g ON g.game_id=a.game_id
LEFT JOIN market_catalog ca ON ca.venue=a.venue AND ca.ticker=a.ticker
LEFT JOIN market_catalog cb ON cb.venue=b.venue AND cb.ticker=b.ticker
WHERE a.src='struct' AND lower(trim(a.mkt_type)) IN ('winner','advance','spread','total','prop','player_scorer','player_prop')
 AND a.venue IN ('kalshi','polyus','polymarket','polyint')
 AND b.venue IN ('kalshi','polyus','polymarket','polyint')
 AND (CASE a.venue WHEN 'kalshi' THEN 1 WHEN 'polyus' THEN 2 ELSE 3 END) <
     (CASE b.venue WHEN 'kalshi' THEN 1 WHEN 'polyus' THEN 2 ELSE 3 END)
 AND (?='' OR a.last_seen>=? OR b.last_seen>=?)
 AND (?=0 OR a.venue>? OR (a.venue=? AND (a.ticker>? OR (a.ticker=? AND
      (b.venue>? OR (b.venue=? AND b.ticker>?))))))
ORDER BY a.venue,a.ticker,b.venue,b.ticker LIMIT ?`, cutoffText, cutoffText, cutoffText,
		afterFlag, after.LeftVenue, after.LeftVenue, after.LeftInstrumentID, after.LeftInstrumentID,
		after.RightVenue, after.RightVenue, after.RightInstrumentID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []StructuralRulePair
	for rows.Next() {
		var v StructuralRulePair
		if err := rows.Scan(&v.LeftVenue, &v.LeftInstrumentID, &v.GameID, &v.LeftMarketType,
			&v.LeftYesTeam, &v.LeftNoTeam, &v.LeftLine, &v.RightVenue, &v.RightInstrumentID,
			&v.RightMarketType, &v.RightYesTeam, &v.RightNoTeam, &v.RightLine, &v.LastSeen,
			&v.League, &v.Away, &v.Home, &v.StartUTC, &v.LeftBoundaryUTC, &v.RightBoundaryUTC,
			&v.LeftTitle, &v.RightTitle); err != nil {
			return nil, err
		}
		v.MarketType = v.LeftMarketType // legacy diagnostic alias; contracts use the per-leg fields.
		orientation, _ := structuralRuleOrientation(v.MarketType, v.LeftYesTeam, v.LeftNoTeam,
			v.LeftLine, v.RightYesTeam, v.RightNoTeam, v.RightLine)
		var canonicalOK bool
		v.LeftVenue, v.LeftInstrumentID, v.RightVenue, v.RightInstrumentID, v.PairID, canonicalOK =
			canonicalRulePair(v.LeftVenue, v.LeftInstrumentID, v.RightVenue, v.RightInstrumentID)
		if !canonicalOK {
			continue
		}
		v.PairType = rulePairType(v.LeftVenue, v.RightVenue)
		if orientation != "" {
			v.Orientation = orientation
		}
		v.CanonicalEventID = "sports:" + strings.TrimSpace(v.GameID)
		v.YesTeam, v.NoTeam, v.Line = v.LeftYesTeam, v.LeftNoTeam, v.LeftLine
		if v.PairType == "K-PUS" {
			v.KalshiTicker, v.PolyUSSlug = v.LeftInstrumentID, v.RightInstrumentID
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// StructuralRuleCandidatePageAfter is a bounded, deterministic and durable-pagination-friendly
// view of every plausible structured pair, including rejected orientations. Once the tail is
// reached it wraps to the first page. A caller that persists Next therefore reaches every current
// pair without an unbounded materialization or newest-first starvation.
func (s *Store) StructuralRuleCandidatePageAfter(ctx context.Context, cutoff time.Time,
	after StructuralRuleCursor, limit int) (StructuralRuleCandidatePage, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	cutoffText := ""
	if !cutoff.IsZero() {
		cutoffText = cutoff.UTC().Format(time.RFC3339Nano)
	}
	var total int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM market_game a
JOIN market_game b ON b.game_id=a.game_id AND b.src='struct'
 AND (lower(trim(b.mkt_type))=lower(trim(a.mkt_type)) OR
      (lower(trim(a.mkt_type)) IN ('prop','player_scorer','player_prop') AND
       lower(trim(b.mkt_type)) IN ('prop','player_scorer','player_prop')))
WHERE a.src='struct' AND lower(trim(a.mkt_type)) IN ('winner','advance','spread','total','prop','player_scorer','player_prop')
 AND a.venue IN ('kalshi','polyus','polymarket','polyint')
 AND b.venue IN ('kalshi','polyus','polymarket','polyint')
 AND (CASE a.venue WHEN 'kalshi' THEN 1 WHEN 'polyus' THEN 2 ELSE 3 END) <
     (CASE b.venue WHEN 'kalshi' THEN 1 WHEN 'polyus' THEN 2 ELSE 3 END)
 AND (?='' OR a.last_seen>=? OR b.last_seen>=?)`, cutoffText, cutoffText, cutoffText).Scan(&total)
	if err != nil {
		return StructuralRuleCandidatePage{}, err
	}
	rows, err := s.structuralRuleCandidatePageOnce(ctx, cutoffText, after, limit)
	if err != nil {
		return StructuralRuleCandidatePage{}, err
	}
	wrapped := false
	if len(rows) == 0 && total > 0 && after.LeftVenue != "" {
		rows, err = s.structuralRuleCandidatePageOnce(ctx, cutoffText, StructuralRuleCursor{}, limit)
		wrapped = true
		if err != nil {
			return StructuralRuleCandidatePage{}, err
		}
	}
	page := StructuralRuleCandidatePage{Rows: rows, Total: total, Wrapped: wrapped}
	if total > len(rows) {
		page.Deferred = total - len(rows)
	}
	if len(rows) > 0 {
		last := rows[len(rows)-1]
		page.Next = StructuralRuleCursor{LeftVenue: last.LeftVenue, LeftInstrumentID: last.LeftInstrumentID,
			RightVenue: last.RightVenue, RightInstrumentID: last.RightInstrumentID}
	}
	return page, nil
}

// StructuralRuleCandidates retains the old first-page API for diagnostics and tests. Runtime
// collection uses StructuralRuleCandidatePageAfter with a durable cursor.
func (s *Store) StructuralRuleCandidates(ctx context.Context, cutoff time.Time, limit int) ([]StructuralRulePair, error) {
	page, err := s.StructuralRuleCandidatePageAfter(ctx, cutoff, StructuralRuleCursor{}, limit)
	return page.Rows, err
}

// StructuralRulePairs is the compatible subset retained for old consumers. The rule collector
// uses StructuralRuleCandidates so rejected comparisons are durably visible.
func (s *Store) StructuralRulePairs(ctx context.Context, cutoff time.Time, limit int) ([]StructuralRulePair, error) {
	candidates, err := s.StructuralRuleCandidates(ctx, cutoff, limit)
	if err != nil {
		return nil, err
	}
	out := make([]StructuralRulePair, 0, len(candidates))
	for _, candidate := range candidates {
		if candidate.Orientation == "" {
			continue
		}
		out = append(out, candidate)
	}
	return out, nil
}

// StructuralKPolyRulePairs is retained for old tests and diagnostics. New collectors call the
// all-pair method above.
func (s *Store) StructuralKPolyRulePairs(ctx context.Context, cutoff time.Time, limit int) ([]StructuralRulePair, error) {
	pairs, err := s.StructuralRulePairs(ctx, cutoff, 500)
	if err != nil {
		return nil, err
	}
	out := make([]StructuralRulePair, 0, len(pairs))
	for _, pair := range pairs {
		if pair.PairType == "K-PUS" && pair.Orientation == "same" {
			out = append(out, pair)
			if limit > 0 && len(out) >= limit {
				break
			}
		}
	}
	return out, nil
}

func normalizeStructuralRulePair(pair StructuralRulePair) StructuralRulePair {
	if pair.LeftVenue == "" && pair.KalshiTicker != "" && pair.PolyUSSlug != "" {
		pair.LeftVenue, pair.LeftInstrumentID = "kalshi", pair.KalshiTicker
		pair.RightVenue, pair.RightInstrumentID = "polyus", pair.PolyUSSlug
		pair.PairType = "K-PUS"
		pair.Orientation = "same"
	}
	if pair.PairID == "" {
		_, _, _, _, pair.PairID, _ = canonicalRulePair(pair.LeftVenue, pair.LeftInstrumentID,
			pair.RightVenue, pair.RightInstrumentID)
	}
	return pair
}

type NormalizedRuleTerms struct {
	SettlementSourceID, SettlementSourceURL               string
	PredicateHash, RulesHash                              string
	VoidPolicy, ScalarPolicy, UnknownPolicy, TimingPolicy string
}

type RulePairCertificateSpec struct {
	PairID, PairType, LeftVenue, LeftInstrumentID      string
	RightVenue, RightInstrumentID, Orientation         string
	LeftArtifactHash, RightArtifactHash                string
	CanonicalEventID, CanonicalPayoffID                string
	LeftPayoffID, RightPayoffID                        string
	Left, Right                                        NormalizedRuleTerms
	ReviewMethod, ReviewProvenance, ReviewEvidenceHash string
	Version                                            int
	SpecHash, BasisID                                  string
	Funded, PaperAuthority, LiveAuthority              bool
	// Legacy aliases are retained for the original Kalshi-PolyUS API contract.
	KalshiTicker, PolyUSSlug, KalshiArtifactHash, PolyUSArtifactHash string
}

type rulePairCertificateHash struct {
	PairID, PairType, LeftVenue, LeftInstrumentID      string
	RightVenue, RightInstrumentID, Orientation         string
	LeftArtifactHash, RightArtifactHash                string
	CanonicalEventID, CanonicalPayoffID                string
	LeftPayoffID, RightPayoffID                        string
	Terms                                              NormalizedRuleTerms
	ReviewMethod, ReviewProvenance, ReviewEvidenceHash string
	Version                                            int
}

func validRuleHash(v string) bool {
	b, err := hex.DecodeString(strings.TrimSpace(v))
	return err == nil && len(b) == 32
}

func normalizeRuleTerms(v NormalizedRuleTerms) NormalizedRuleTerms {
	v.SettlementSourceID, v.SettlementSourceURL = strings.TrimSpace(v.SettlementSourceID), strings.TrimSpace(v.SettlementSourceURL)
	v.PredicateHash, v.RulesHash = strings.ToLower(strings.TrimSpace(v.PredicateHash)), strings.ToLower(strings.TrimSpace(v.RulesHash))
	v.VoidPolicy, v.ScalarPolicy = strings.ToLower(strings.TrimSpace(v.VoidPolicy)), strings.ToLower(strings.TrimSpace(v.ScalarPolicy))
	v.UnknownPolicy, v.TimingPolicy = strings.ToLower(strings.TrimSpace(v.UnknownPolicy)), strings.ToLower(strings.TrimSpace(v.TimingPolicy))
	return v
}

func completeNormalizedTerms(v NormalizedRuleTerms) bool {
	return v.SettlementSourceID != "" && v.SettlementSourceURL != "" && validRuleHash(v.PredicateHash) &&
		validRuleHash(v.RulesHash) && v.VoidPolicy != "" && v.ScalarPolicy != "" &&
		v.UnknownPolicy != "" && v.TimingPolicy != ""
}

func normalizeRulePairSpec(in RulePairCertificateSpec) (RulePairCertificateSpec, error) {
	legacy := strings.TrimSpace(in.LeftVenue) == "" && strings.TrimSpace(in.RightVenue) == "" &&
		strings.TrimSpace(in.KalshiTicker) != "" && strings.TrimSpace(in.PolyUSSlug) != ""
	if legacy {
		in.LeftVenue, in.LeftInstrumentID = "kalshi", strings.TrimSpace(in.KalshiTicker)
		in.RightVenue, in.RightInstrumentID = "polyus", strings.TrimSpace(in.PolyUSSlug)
		in.LeftArtifactHash, in.RightArtifactHash = in.KalshiArtifactHash, in.PolyUSArtifactHash
		if strings.TrimSpace(in.Orientation) == "" {
			in.Orientation = "same"
		}
	}
	originalLeftVenue, originalLeftID := normalizeRuleVenue(in.LeftVenue), strings.TrimSpace(in.LeftInstrumentID)
	leftVenue, leftID, rightVenue, rightID, expectedPairID, ok := canonicalRulePair(
		originalLeftVenue, originalLeftID, in.RightVenue, in.RightInstrumentID)
	if !ok {
		return RulePairCertificateSpec{}, errors.New("unsupported or incomplete venue rule pair")
	}
	if leftVenue != originalLeftVenue || leftID != originalLeftID {
		in.Left, in.Right = in.Right, in.Left
		in.LeftArtifactHash, in.RightArtifactHash = in.RightArtifactHash, in.LeftArtifactHash
		in.LeftPayoffID, in.RightPayoffID = in.RightPayoffID, in.LeftPayoffID
	}
	in.LeftVenue, in.LeftInstrumentID, in.RightVenue, in.RightInstrumentID = leftVenue, leftID, rightVenue, rightID
	in.PairID, in.PairType = strings.TrimSpace(in.PairID), rulePairType(leftVenue, rightVenue)
	if in.PairID == "" || in.PairID != expectedPairID {
		return RulePairCertificateSpec{}, fmt.Errorf("rule pair id must be %s", expectedPairID)
	}
	in.Orientation = strings.ToLower(strings.TrimSpace(in.Orientation))
	in.LeftArtifactHash, in.RightArtifactHash = strings.ToLower(strings.TrimSpace(in.LeftArtifactHash)), strings.ToLower(strings.TrimSpace(in.RightArtifactHash))
	in.CanonicalEventID, in.CanonicalPayoffID = strings.TrimSpace(in.CanonicalEventID), strings.TrimSpace(in.CanonicalPayoffID)
	in.LeftPayoffID, in.RightPayoffID = strings.TrimSpace(in.LeftPayoffID), strings.TrimSpace(in.RightPayoffID)
	in.ReviewMethod, in.ReviewProvenance = strings.ToLower(strings.TrimSpace(in.ReviewMethod)), strings.TrimSpace(in.ReviewProvenance)
	in.ReviewEvidenceHash = strings.ToLower(strings.TrimSpace(in.ReviewEvidenceHash))
	in.Left, in.Right = normalizeRuleTerms(in.Left), normalizeRuleTerms(in.Right)
	if in.PairType == "K-PUS" {
		in.KalshiTicker, in.PolyUSSlug = leftID, rightID
		in.KalshiArtifactHash, in.PolyUSArtifactHash = in.LeftArtifactHash, in.RightArtifactHash
	}
	return in, nil
}

func (s *Store) RegisterRulePairCertificate(ctx context.Context, in RulePairCertificateSpec) (RulePairCertificateSpec, bool, error) {
	var err error
	in, err = normalizeRulePairSpec(in)
	if err != nil {
		return RulePairCertificateSpec{}, false, err
	}
	if in.Version <= 0 || (in.Orientation != "same" && in.Orientation != "inverse") ||
		!validRuleHash(in.LeftArtifactHash) || !validRuleHash(in.RightArtifactHash) ||
		in.CanonicalEventID == "" || in.CanonicalPayoffID == "" || !completeNormalizedTerms(in.Left) ||
		in.Left != in.Right || (in.ReviewMethod != "human_review" && in.ReviewMethod != "structured_official_schema") ||
		in.ReviewProvenance == "" || !validRuleHash(in.ReviewEvidenceHash) || in.Funded || in.PaperAuthority || in.LiveAuthority {
		return RulePairCertificateSpec{}, false, errors.New("normalized rule certificate incomplete, incompatible, unreviewed, or authoritative")
	}
	leftArtifact, ok, err := s.LatestVenueRuleArtifact(ctx, in.LeftVenue, in.LeftInstrumentID)
	if err != nil || !ok || leftArtifact.ArtifactHash != in.LeftArtifactHash {
		return RulePairCertificateSpec{}, false, fmt.Errorf("%s raw rule artifact is absent or no longer current", in.LeftVenue)
	}
	rightArtifact, ok, err := s.LatestVenueRuleArtifact(ctx, in.RightVenue, in.RightInstrumentID)
	if err != nil || !ok || rightArtifact.ArtifactHash != in.RightArtifactHash {
		return RulePairCertificateSpec{}, false, fmt.Errorf("%s raw rule artifact is absent or no longer current", in.RightVenue)
	}
	inputs, err := s.ResolutionBasisInputsForKeys(ctx, []ResolutionBasisInstrumentKey{
		{Venue: in.LeftVenue, Ticker: in.LeftInstrumentID}, {Venue: in.RightVenue, Ticker: in.RightInstrumentID}})
	if err != nil {
		return RulePairCertificateSpec{}, false, err
	}
	left, leftOK := inputs[ResolutionBasisKey(in.LeftVenue, in.LeftInstrumentID)]
	right, rightOK := inputs[ResolutionBasisKey(in.RightVenue, in.RightInstrumentID)]
	if !leftOK || !rightOK || left.EventID != right.EventID || left.EventVersion != right.EventVersion || left.EventID != in.CanonicalEventID ||
		left.PayoffID != in.CanonicalPayoffID ||
		(in.Orientation == "same" && (left.PayoffID != right.PayoffID || left.PayoffVersion != right.PayoffVersion)) ||
		(in.Orientation == "inverse" && left.PayoffID == right.PayoffID) {
		return RulePairCertificateSpec{}, false, errors.New("certificate does not match current structural event/payoff identity")
	}
	if in.LeftPayoffID != "" && in.LeftPayoffID != left.PayoffID || in.RightPayoffID != "" && in.RightPayoffID != right.PayoffID {
		return RulePairCertificateSpec{}, false, errors.New("certificate payoff receipts changed during review")
	}
	in.LeftPayoffID, in.RightPayoffID = left.PayoffID, right.PayoffID
	semantic := rulePairCertificateHash{PairID: in.PairID, PairType: in.PairType,
		LeftVenue: in.LeftVenue, LeftInstrumentID: in.LeftInstrumentID, RightVenue: in.RightVenue,
		RightInstrumentID: in.RightInstrumentID, Orientation: in.Orientation,
		LeftArtifactHash: in.LeftArtifactHash, RightArtifactHash: in.RightArtifactHash,
		CanonicalEventID: in.CanonicalEventID, CanonicalPayoffID: in.CanonicalPayoffID,
		LeftPayoffID: in.LeftPayoffID, RightPayoffID: in.RightPayoffID,
		Terms: in.Left, ReviewMethod: in.ReviewMethod,
		ReviewProvenance: in.ReviewProvenance, ReviewEvidenceHash: in.ReviewEvidenceHash, Version: in.Version}
	in.SpecHash, err = researchSpecHash(semantic)
	if err != nil {
		return RulePairCertificateSpec{}, false, err
	}
	in.BasisID = "normalized-rule-certificate:" + in.PairID + ":v" + fmt.Sprint(in.Version) + ":" + in.SpecHash
	var existing string
	err = s.db.QueryRowContext(ctx, `SELECT spec_hash FROM research_rule_pair_certificates WHERE pair_id=? AND version=?`, in.PairID, in.Version).Scan(&existing)
	if err == nil {
		if existing != in.SpecHash {
			return RulePairCertificateSpec{}, false, fmt.Errorf("immutable normalized rule certificate drift: %s v%d", in.PairID, in.Version)
		}
		return in, false, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return RulePairCertificateSpec{}, false, err
	}
	legacyLeftID, legacyRightID := in.LeftInstrumentID, in.RightInstrumentID
	legacyLeftHash, legacyRightHash := in.LeftArtifactHash, in.RightArtifactHash
	_, err = s.db.ExecContext(ctx, `INSERT INTO research_rule_pair_certificates(pair_id,version,spec_hash,
left_venue,left_instrument_id,right_venue,right_instrument_id,orientation,left_artifact_hash,
right_artifact_hash,left_payoff_id,right_payoff_id,
kalshi_ticker,polyus_slug,kalshi_artifact_hash,polyus_artifact_hash,canonical_event_id,
canonical_payoff_id,normalized_predicate_hash,normalized_rules_hash,settlement_source_id,
settlement_source_url,void_policy,scalar_policy,unknown_policy,timing_policy,review_method,
review_provenance,review_evidence_hash,created_ts,funded,paper_authority,live_authority)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0,0,0)`, in.PairID, in.Version, in.SpecHash,
		in.LeftVenue, in.LeftInstrumentID, in.RightVenue, in.RightInstrumentID, in.Orientation,
		in.LeftArtifactHash, in.RightArtifactHash, in.LeftPayoffID, in.RightPayoffID,
		legacyLeftID, legacyRightID, legacyLeftHash, legacyRightHash,
		in.CanonicalEventID, in.CanonicalPayoffID, in.Left.PredicateHash, in.Left.RulesHash,
		in.Left.SettlementSourceID, in.Left.SettlementSourceURL, in.Left.VoidPolicy,
		in.Left.ScalarPolicy, in.Left.UnknownPolicy, in.Left.TimingPolicy, in.ReviewMethod,
		in.ReviewProvenance, in.ReviewEvidenceHash, nowRFC())
	return in, err == nil, err
}

func (s *Store) CompatibleRulePairCertificateFor(ctx context.Context, leftVenue, leftID, rightVenue, rightID string) (RulePairCertificateSpec, bool, error) {
	leftVenue, leftID, rightVenue, rightID, _, ok := canonicalRulePair(leftVenue, leftID, rightVenue, rightID)
	if !ok {
		return RulePairCertificateSpec{}, false, nil
	}
	var out RulePairCertificateSpec
	var terms NormalizedRuleTerms
	var funded, paper, live int
	err := s.db.QueryRowContext(ctx, `SELECT pair_id,version,spec_hash,left_venue,left_instrument_id,
right_venue,right_instrument_id,orientation,left_artifact_hash,right_artifact_hash,
left_payoff_id,right_payoff_id,canonical_event_id,canonical_payoff_id,
normalized_predicate_hash,normalized_rules_hash,settlement_source_id,settlement_source_url,
void_policy,scalar_policy,unknown_policy,timing_policy,review_method,review_provenance,
review_evidence_hash,funded,paper_authority,live_authority
FROM research_rule_pair_certificates WHERE left_venue=? AND left_instrument_id=?
AND right_venue=? AND right_instrument_id=? ORDER BY version DESC LIMIT 1`,
		leftVenue, leftID, rightVenue, rightID).Scan(&out.PairID, &out.Version,
		&out.SpecHash, &out.LeftVenue, &out.LeftInstrumentID, &out.RightVenue, &out.RightInstrumentID,
		&out.Orientation, &out.LeftArtifactHash, &out.RightArtifactHash,
		&out.LeftPayoffID, &out.RightPayoffID, &out.CanonicalEventID, &out.CanonicalPayoffID,
		&terms.PredicateHash, &terms.RulesHash, &terms.SettlementSourceID, &terms.SettlementSourceURL,
		&terms.VoidPolicy, &terms.ScalarPolicy, &terms.UnknownPolicy, &terms.TimingPolicy,
		&out.ReviewMethod, &out.ReviewProvenance, &out.ReviewEvidenceHash, &funded, &paper, &live)
	if errors.Is(err, sql.ErrNoRows) {
		return RulePairCertificateSpec{}, false, nil
	}
	if err != nil {
		return RulePairCertificateSpec{}, false, err
	}
	out.PairType = rulePairType(out.LeftVenue, out.RightVenue)
	if out.PairType == "K-PUS" {
		out.KalshiTicker, out.PolyUSSlug = out.LeftInstrumentID, out.RightInstrumentID
		out.KalshiArtifactHash, out.PolyUSArtifactHash = out.LeftArtifactHash, out.RightArtifactHash
	}
	out.Left, out.Right, out.BasisID = terms, terms,
		"normalized-rule-certificate:"+out.PairID+":v"+fmt.Sprint(out.Version)+":"+out.SpecHash
	out.Funded, out.PaperAuthority, out.LiveAuthority = funded != 0, paper != 0, live != 0
	leftArtifact, leftOK, err := s.LatestVenueRuleArtifact(ctx, out.LeftVenue, out.LeftInstrumentID)
	if err != nil {
		return RulePairCertificateSpec{}, false, err
	}
	rightArtifact, rightOK, err := s.LatestVenueRuleArtifact(ctx, out.RightVenue, out.RightInstrumentID)
	if err != nil {
		return RulePairCertificateSpec{}, false, err
	}
	if !leftOK || !rightOK || leftArtifact.ArtifactHash != out.LeftArtifactHash || rightArtifact.ArtifactHash != out.RightArtifactHash ||
		out.Funded || out.PaperAuthority || out.LiveAuthority {
		return out, false, nil
	}
	inputs, err := s.ResolutionBasisInputsForKeys(ctx, []ResolutionBasisInstrumentKey{
		{Venue: out.LeftVenue, Ticker: out.LeftInstrumentID}, {Venue: out.RightVenue, Ticker: out.RightInstrumentID}})
	if err != nil {
		return RulePairCertificateSpec{}, false, err
	}
	left := inputs[ResolutionBasisKey(out.LeftVenue, out.LeftInstrumentID)]
	right := inputs[ResolutionBasisKey(out.RightVenue, out.RightInstrumentID)]
	if left.EventID != out.CanonicalEventID || right.EventID != out.CanonicalEventID || left.EventVersion != right.EventVersion ||
		left.PayoffID != out.LeftPayoffID || right.PayoffID != out.RightPayoffID ||
		(out.Orientation == "same" && (left.PayoffID != right.PayoffID || left.PayoffVersion != right.PayoffVersion)) ||
		(out.Orientation == "inverse" && left.PayoffID == right.PayoffID) {
		return out, false, nil
	}
	return out, true, nil
}

func (s *Store) CompatibleRulePairCertificate(ctx context.Context, kalshiTicker, polyUSSlug string) (RulePairCertificateSpec, bool, error) {
	return s.CompatibleRulePairCertificateFor(ctx, "kalshi", kalshiTicker, "polyus", polyUSSlug)
}

// CompatibleRulePairCertificatesForInstrument returns the current, independently normalized
// cross-venue rule certificates that contain one exact venue instrument. It is the destination
// discovery boundary for portable signal variants: callers never title-match or infer a twin, and
// must refuse an empty or ambiguous result. The result is bounded because one venue instrument is
// expected to describe one canonical binary payoff; multiple current matches remain visible so the
// caller can fail closed instead of silently choosing one.
func (s *Store) CompatibleRulePairCertificatesForInstrument(ctx context.Context, venue, instrumentID,
	otherVenue string, limit int) ([]RulePairCertificateSpec, error) {
	venue, otherVenue = normalizeRuleVenue(venue), normalizeRuleVenue(otherVenue)
	instrumentID = strings.TrimSpace(instrumentID)
	if instrumentID == "" || venue == "" || otherVenue == "" || venue == otherVenue {
		return nil, nil
	}
	if limit <= 0 || limit > 16 {
		limit = 8
	}
	rows, err := s.db.QueryContext(ctx, `SELECT left_venue,left_instrument_id,right_venue,right_instrument_id
FROM research_rule_pair_certificates c
WHERE ((left_venue=? AND left_instrument_id=? AND right_venue=?) OR
       (right_venue=? AND right_instrument_id=? AND left_venue=?))
AND c.version=(SELECT MAX(x.version) FROM research_rule_pair_certificates x WHERE x.pair_id=c.pair_id)
ORDER BY pair_id LIMIT ?`, venue, instrumentID, otherVenue, venue, instrumentID, otherVenue, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	type pairKey struct{ lv, lid, rv, rid string }
	keys := make([]pairKey, 0, 2)
	for rows.Next() {
		var key pairKey
		if err := rows.Scan(&key.lv, &key.lid, &key.rv, &key.rid); err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	out := make([]RulePairCertificateSpec, 0, len(keys))
	for _, key := range keys {
		cert, current, err := s.CompatibleRulePairCertificateFor(ctx, key.lv, key.lid, key.rv, key.rid)
		if err != nil {
			return nil, err
		}
		if current {
			out = append(out, cert)
		}
	}
	return out, nil
}

type RulePairReview struct {
	Observed                                                         time.Time
	PairID, PairType, LeftVenue, LeftInstrumentID                    string
	RightVenue, RightInstrumentID, Orientation                       string
	CanonicalEventID, CanonicalPayoffID                              string
	LeftPayoffID, RightPayoffID                                      string
	LeftArtifactHash, RightArtifactHash                              string
	NormalizationState, ExactBlocker, CertificateHash                string
	KalshiTicker, PolyUSSlug, KalshiArtifactHash, PolyUSArtifactHash string
}

func (s *Store) RecordRulePairReview(ctx context.Context, pair StructuralRulePair, observed time.Time) (RulePairReview, bool, error) {
	pair = normalizeStructuralRulePair(pair)
	if observed.IsZero() {
		observed = time.Now().UTC()
	}
	review := RulePairReview{Observed: observed.UTC(), PairID: pair.PairID, PairType: pair.PairType,
		LeftVenue: pair.LeftVenue, LeftInstrumentID: pair.LeftInstrumentID,
		RightVenue: pair.RightVenue, RightInstrumentID: pair.RightInstrumentID, Orientation: pair.Orientation}
	if review.PairType == "K-PUS" {
		review.KalshiTicker, review.PolyUSSlug = review.LeftInstrumentID, review.RightInstrumentID
	}
	inputs, err := s.ResolutionBasisInputsForKeys(ctx, []ResolutionBasisInstrumentKey{
		{Venue: pair.LeftVenue, Ticker: pair.LeftInstrumentID}, {Venue: pair.RightVenue, Ticker: pair.RightInstrumentID}})
	if err == nil {
		left, leftOK := inputs[ResolutionBasisKey(pair.LeftVenue, pair.LeftInstrumentID)]
		right, rightOK := inputs[ResolutionBasisKey(pair.RightVenue, pair.RightInstrumentID)]
		if leftOK && rightOK && left.EventID == right.EventID &&
			((pair.Orientation == "same" && left.PayoffID == right.PayoffID) ||
				(pair.Orientation == "inverse" && left.PayoffID != right.PayoffID)) {
			review.CanonicalEventID, review.CanonicalPayoffID = left.EventID, left.PayoffID
			review.LeftPayoffID, review.RightPayoffID = left.PayoffID, right.PayoffID
		}
	}
	leftArtifact, leftOK, err := s.LatestVenueRuleArtifact(ctx, pair.LeftVenue, pair.LeftInstrumentID)
	if err != nil {
		return RulePairReview{}, false, err
	}
	rightArtifact, rightOK, err := s.LatestVenueRuleArtifact(ctx, pair.RightVenue, pair.RightInstrumentID)
	if err != nil {
		return RulePairReview{}, false, err
	}
	if leftOK {
		review.LeftArtifactHash = leftArtifact.ArtifactHash
	}
	if rightOK {
		review.RightArtifactHash = rightArtifact.ArtifactHash
	}
	review.KalshiArtifactHash, review.PolyUSArtifactHash = review.LeftArtifactHash, review.RightArtifactHash
	cert, currentCert, certErr := s.CompatibleRulePairCertificateFor(ctx, pair.LeftVenue, pair.LeftInstrumentID,
		pair.RightVenue, pair.RightInstrumentID)
	if certErr != nil {
		return RulePairReview{}, false, certErr
	}
	switch {
	case !leftOK:
		review.NormalizationState, review.ExactBlocker = "MISSING_KALSHI_RULES", "current "+pair.LeftVenue+" raw rule artifact was not captured"
	case !rightOK:
		review.NormalizationState, review.ExactBlocker = "MISSING_POLYUS_RULES", "current "+pair.RightVenue+" raw rule artifact was not captured"
	case currentCert:
		review.NormalizationState, review.ExactBlocker, review.CertificateHash = "COMPATIBLE_CERTIFIED", "normalized compatible certificate is current", cert.SpecHash
	case cert.PairID != "":
		review.NormalizationState, review.ExactBlocker, review.CertificateHash = "CERTIFICATE_STALE", "a raw venue artifact changed after the last normalization review", cert.SpecHash
	case len(leftArtifact.SettlementSources) == 0:
		review.NormalizationState, review.ExactBlocker = "MISSING_KALSHI_SETTLEMENT_SOURCE", pair.LeftVenue+" raw schema has not supplied an official settlement-source identifier/URL"
	case len(rightArtifact.SettlementSources) == 0:
		review.NormalizationState, review.ExactBlocker = "MISSING_POLYUS_SETTLEMENT_SOURCE", pair.RightVenue+" raw schema has not supplied an official settlement-source identifier/URL"
	default:
		review.NormalizationState, review.ExactBlocker = "RAW_CAPTURED_REVIEW_REQUIRED", "raw rules and source references exist, but predicate/timing/void/scalar/unknown equivalence has not been independently normalized"
	}
	res, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO research_rule_pair_reviews(observed_ts,
pair_id,left_venue,left_instrument_id,right_venue,right_instrument_id,orientation,left_payoff_id,
right_payoff_id,left_artifact_hash,right_artifact_hash,kalshi_ticker,polyus_slug,canonical_event_id,
canonical_payoff_id,kalshi_artifact_hash,polyus_artifact_hash,normalization_state,exact_blocker,
certificate_hash,funded,paper_authority,live_authority) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0,0,0)`,
		review.Observed.Format(time.RFC3339Nano), review.PairID, review.LeftVenue, review.LeftInstrumentID,
		review.RightVenue, review.RightInstrumentID, review.Orientation, review.LeftPayoffID, review.RightPayoffID,
		review.LeftArtifactHash, review.RightArtifactHash, review.LeftInstrumentID, review.RightInstrumentID,
		review.CanonicalEventID, review.CanonicalPayoffID, review.LeftArtifactHash, review.RightArtifactHash,
		review.NormalizationState, review.ExactBlocker, review.CertificateHash)
	if err != nil {
		return RulePairReview{}, false, err
	}
	n, err := res.RowsAffected()
	return review, n > 0, err
}

type ThreeWayRuleCertificateStatus struct {
	Current, OrientationConsistent                  bool
	KalshiTicker, PolyUSSlug, PolymarketConditionID string
	CanonicalEventID, CanonicalPayoffID, Blocker    string
	PairCertificateHashes                           map[string]string
}

func ruleOrientationBit(v string) int {
	if strings.EqualFold(strings.TrimSpace(v), "inverse") {
		return 1
	}
	return 0
}

// ThreeWayRuleCertificateStatus requires the complete pairwise semantic triangle. Two compatible
// edges do not imply the third: venue C can use a different cancellation source or the two flips
// can be orientation-inconsistent. This method is the only allowed three-way economic status.
func (s *Store) ThreeWayRuleCertificateStatus(ctx context.Context, kalshiTicker, polyUSSlug, polymarketConditionID string) (ThreeWayRuleCertificateStatus, error) {
	out := ThreeWayRuleCertificateStatus{KalshiTicker: strings.TrimSpace(kalshiTicker),
		PolyUSSlug: strings.TrimSpace(polyUSSlug), PolymarketConditionID: strings.TrimSpace(polymarketConditionID),
		PairCertificateHashes: map[string]string{}}
	pairs := []struct {
		name, lv, lid, rv, rid string
	}{
		{"K-PUS", "kalshi", out.KalshiTicker, "polyus", out.PolyUSSlug},
		{"K-PINT", "kalshi", out.KalshiTicker, "polymarket", out.PolymarketConditionID},
		{"PUS-PINT", "polyus", out.PolyUSSlug, "polymarket", out.PolymarketConditionID},
	}
	certs := make(map[string]RulePairCertificateSpec, 3)
	for _, pair := range pairs {
		cert, current, err := s.CompatibleRulePairCertificateFor(ctx, pair.lv, pair.lid, pair.rv, pair.rid)
		if err != nil {
			return out, err
		}
		if !current {
			out.Blocker = "missing current " + pair.name + " normalized rule certificate"
			return out, nil
		}
		certs[pair.name] = cert
		out.PairCertificateHashes[pair.name] = cert.SpecHash
	}
	kp, ki, pi := certs["K-PUS"], certs["K-PINT"], certs["PUS-PINT"]
	if kp.CanonicalEventID != ki.CanonicalEventID || kp.CanonicalEventID != pi.CanonicalEventID {
		out.Blocker = "pairwise certificates disagree on canonical event"
		return out, nil
	}
	if kp.LeftPayoffID != ki.LeftPayoffID || kp.RightPayoffID != pi.LeftPayoffID ||
		ki.RightPayoffID != pi.RightPayoffID {
		out.Blocker = "pairwise certificates disagree on one venue-native payoff receipt"
		return out, nil
	}
	out.OrientationConsistent = ruleOrientationBit(kp.Orientation)^ruleOrientationBit(ki.Orientation)^ruleOrientationBit(pi.Orientation) == 0
	if !out.OrientationConsistent {
		out.Blocker = "pairwise flip orientations are not transitive"
		return out, nil
	}
	out.Current, out.CanonicalEventID, out.CanonicalPayoffID = true, kp.CanonicalEventID, kp.LeftPayoffID
	return out, nil
}

func (s *Store) currentRuleCertificates(ctx context.Context, limit int) ([]RulePairCertificateSpec, error) {
	if limit <= 0 || limit > 2000 {
		limit = 500
	}
	rows, err := s.db.QueryContext(ctx, `SELECT left_venue,left_instrument_id,right_venue,right_instrument_id
FROM research_rule_pair_certificates c WHERE c.version=(SELECT MAX(x.version)
FROM research_rule_pair_certificates x WHERE x.pair_id=c.pair_id) ORDER BY pair_id LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	type key struct{ lv, lid, rv, rid string }
	var keys []key
	for rows.Next() {
		var k key
		if err := rows.Scan(&k.lv, &k.lid, &k.rv, &k.rid); err != nil {
			return nil, err
		}
		keys = append(keys, k)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	out := make([]RulePairCertificateSpec, 0, len(keys))
	for _, k := range keys {
		cert, current, err := s.CompatibleRulePairCertificateFor(ctx, k.lv, k.lid, k.rv, k.rid)
		if err != nil {
			return nil, err
		}
		if current {
			out = append(out, cert)
		}
	}
	return out, nil
}

func (s *Store) RuleArtifactReport(ctx context.Context, limit int) (map[string]any, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	counts := map[string]int{}
	for key, q := range map[string]string{
		"artifact_instruments":  `SELECT COUNT(*) FROM (SELECT 1 FROM research_rule_artifacts GROUP BY venue,instrument_id)`,
		"artifact_versions":     `SELECT COUNT(*) FROM research_rule_artifacts`,
		"structural_pairs_seen": `SELECT COUNT(DISTINCT pair_id) FROM research_rule_pair_reviews`,
		"certificate_versions":  `SELECT COUNT(*) FROM research_rule_pair_certificates`,
	} {
		var n int
		if err := s.db.QueryRowContext(ctx, q).Scan(&n); err != nil {
			return nil, err
		}
		counts[key] = n
	}
	currentCerts, err := s.currentRuleCertificates(ctx, 500)
	if err != nil {
		return nil, err
	}
	counts["compatible_certificates"] = len(currentCerts)
	pairTypes := map[string]int{"K-PUS": 0, "K-PINT": 0, "PUS-PINT": 0}
	for _, cert := range currentCerts {
		pairTypes[cert.PairType]++
	}
	structuralPairTypes := map[string]int{"K-PUS": 0, "K-PINT": 0, "PUS-PINT": 0}
	typeRows, err := s.db.QueryContext(ctx, `SELECT left_venue,right_venue,COUNT(DISTINCT pair_id)
FROM research_rule_pair_reviews GROUP BY left_venue,right_venue`)
	if err != nil {
		return nil, err
	}
	for typeRows.Next() {
		var leftVenue, rightVenue string
		var n int
		if err := typeRows.Scan(&leftVenue, &rightVenue, &n); err != nil {
			_ = typeRows.Close()
			return nil, err
		}
		if pairType := rulePairType(leftVenue, rightVenue); pairType != "" {
			structuralPairTypes[pairType] += n
		}
	}
	if err := typeRows.Close(); err != nil {
		return nil, err
	}
	threeWay := 0
	for _, kp := range currentCerts {
		if kp.PairType != "K-PUS" {
			continue
		}
		for _, ki := range currentCerts {
			if ki.PairType != "K-PINT" || ki.LeftInstrumentID != kp.LeftInstrumentID {
				continue
			}
			status, err := s.ThreeWayRuleCertificateStatus(ctx, kp.LeftInstrumentID, kp.RightInstrumentID, ki.RightInstrumentID)
			if err != nil {
				return nil, err
			}
			if status.Current {
				threeWay++
			}
		}
	}
	counts["three_way_current_certificates"] = threeWay
	rows, err := s.db.QueryContext(ctx, `SELECT r.observed_ts,r.pair_id,r.left_venue,r.left_instrument_id,
r.right_venue,r.right_instrument_id,r.orientation,r.left_payoff_id,r.right_payoff_id,
r.canonical_event_id,r.canonical_payoff_id,r.left_artifact_hash,r.right_artifact_hash,
r.normalization_state,r.exact_blocker,r.certificate_hash
FROM research_rule_pair_reviews r WHERE r.id=(SELECT MAX(x.id) FROM research_rule_pair_reviews x WHERE x.pair_id=r.pair_id)
ORDER BY r.observed_ts DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var recent []RulePairReview
	states := map[string]int{}
	for rows.Next() {
		var v RulePairReview
		var observed string
		if err := rows.Scan(&observed, &v.PairID, &v.LeftVenue, &v.LeftInstrumentID,
			&v.RightVenue, &v.RightInstrumentID, &v.Orientation, &v.LeftPayoffID, &v.RightPayoffID,
			&v.CanonicalEventID, &v.CanonicalPayoffID, &v.LeftArtifactHash,
			&v.RightArtifactHash, &v.NormalizationState, &v.ExactBlocker, &v.CertificateHash); err != nil {
			return nil, err
		}
		v.PairType = rulePairType(v.LeftVenue, v.RightVenue)
		if v.PairType == "K-PUS" {
			v.KalshiTicker, v.PolyUSSlug = v.LeftInstrumentID, v.RightInstrumentID
			v.KalshiArtifactHash, v.PolyUSArtifactHash = v.LeftArtifactHash, v.RightArtifactHash
		}
		v.Observed, _ = time.Parse(time.RFC3339Nano, observed)
		states[v.NormalizationState]++
		recent = append(recent, v)
	}
	decisions, decisionErr := s.CrossVenueDecisionReport(ctx, limit)
	if decisionErr != nil {
		return nil, decisionErr
	}
	return map[string]any{"counts": counts, "pair_types": pairTypes, "structural_pair_types": structuralPairTypes,
		"states": states, "recent_pairs": recent,
		"objective_match_decisions": decisions,
		"funded":                    false, "paper_authority": false, "live_authority": false,
		"contract": "every bounded structured K-PUS, K-PINT, and PUS-PINT candidate is logged accepted or rejected with raw IDs; objective structured predicates may auto-certify, review-only genres fail closed, and all three current pairwise certificates are required for any three-way economic status"}, rows.Err()
}
