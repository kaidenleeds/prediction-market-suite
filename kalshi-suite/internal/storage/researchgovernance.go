package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

var permanentlyForbiddenResearchPrefixes = []string{
	"spoof", "wash-trad", "market-manipulat", "mnpi", "nonpublic-information",
}

// migrateResearchGovernanceAuthority closes the one legacy research table that predated the
// three-column zero-authority contract. Existing rows receive literal zero and become immutable;
// the migration cannot create Paper/LIVE authority or alter any economic value.
func migrateResearchGovernanceAuthority(db *sql.DB) error {
	for _, ddl := range []string{
		`ALTER TABLE research_nested_ladders ADD COLUMN paper_authority INTEGER NOT NULL DEFAULT 0 CHECK(paper_authority=0)`,
		`ALTER TABLE research_nested_ladders ADD COLUMN live_authority INTEGER NOT NULL DEFAULT 0 CHECK(live_authority=0)`,
	} {
		if _, err := db.Exec(ddl); err != nil && !strings.Contains(strings.ToLower(err.Error()), "duplicate column") {
			return err
		}
	}
	_, err := db.Exec(`
CREATE TRIGGER IF NOT EXISTS research_nested_ladders_no_update BEFORE UPDATE ON research_nested_ladders BEGIN SELECT RAISE(ABORT,'immutable research nested ladder'); END;
CREATE TRIGGER IF NOT EXISTS research_nested_ladders_no_delete BEFORE DELETE ON research_nested_ladders BEGIN SELECT RAISE(ABORT,'immutable research nested ladder'); END;
CREATE TRIGGER IF NOT EXISTS research_nested_ladders_zero_authority BEFORE INSERT ON research_nested_ladders
WHEN NEW.funded!=0 OR NEW.paper_authority!=0 OR NEW.live_authority!=0
BEGIN SELECT RAISE(ABORT,'research nested ladder has zero authority'); END;`)
	return err
}

// ForbiddenResearchSystem is intentionally narrow: it rejects an experiment whose own stable ID
// or system name proposes a prohibited action. A defensive detector such as
// flow-direction-integrity is not rejected merely because its mechanism discusses abuse.
func ForbiddenResearchSystem(experimentID, systemName string) (bool, string) {
	for _, raw := range []string{experimentID, systemName} {
		n := strings.ToLower(strings.TrimSpace(raw))
		n = strings.TrimLeft(n, "_- ")
		for _, prefix := range permanentlyForbiddenResearchPrefixes {
			if strings.HasPrefix(n, prefix) {
				return true, prefix
			}
		}
	}
	return false, ""
}

type ResearchSecurityEvent struct {
	Observed     time.Time
	Category     string
	Severity     string
	Message      string
	EvidenceJSON string
}

func (s *Store) AppendResearchSecurityEvent(ctx context.Context, v ResearchSecurityEvent) (int64, error) {
	if v.Observed.IsZero() {
		v.Observed = time.Now().UTC()
	}
	v.Category = strings.ToLower(strings.TrimSpace(v.Category))
	v.Severity = strings.ToLower(strings.TrimSpace(v.Severity))
	if v.EvidenceJSON == "" {
		v.EvidenceJSON = "{}"
	}
	validCategory := map[string]bool{"policy_check": true, "forbidden_attempt": true,
		"credential_scope": true, "rate_limit": true, "recovery": true, "authority_check": true}
	validSeverity := map[string]bool{"info": true, "attention": true, "blocked": true, "critical": true}
	if !validCategory[v.Category] || !validSeverity[v.Severity] || strings.TrimSpace(v.Message) == "" ||
		!json.Valid([]byte(v.EvidenceJSON)) {
		return 0, fmt.Errorf("invalid research security event")
	}
	r, err := s.db.ExecContext(ctx, `INSERT INTO research_security_events(
observed_ts,category,severity,message,evidence_json) VALUES(?,?,?,?,?)`,
		v.Observed.UTC().Format(time.RFC3339Nano), v.Category, v.Severity, v.Message, v.EvidenceJSON)
	if err != nil {
		return 0, err
	}
	return r.LastInsertId()
}

type ResearchSecurityRecent struct {
	ObservedTS, Category, Severity, Message string
	EvidenceJSON                            string
}

// ResearchGovernanceReport verifies the structural guardrails that can be proven locally. API-key
// scopes cannot be inferred from key material, so that check is explicitly operator-verification
// required instead of being presented as green.
func (s *Store) ResearchGovernanceReport(ctx context.Context) (map[string]any, error) {
	// Discover authority-bearing tables from the live schema instead of maintaining a stale list.
	// New research lanes are added often (inference, payoff envelopes, EVI, microstructure and
	// multi-leg bundles); omitting one here would make the governance page claim broader coverage
	// than it actually checked. Table names come only from sqlite_master and are identifier-quoted
	// again before use.
	tableRows, err := s.db.QueryContext(ctx, `SELECT name FROM sqlite_master
WHERE type='table' AND name LIKE 'research\_%' ESCAPE '\' ORDER BY name`)
	if err != nil {
		return nil, err
	}
	var researchTables []string
	for tableRows.Next() {
		var name string
		if err := tableRows.Scan(&name); err != nil {
			_ = tableRows.Close()
			return nil, err
		}
		researchTables = append(researchTables, name)
	}
	if err := tableRows.Close(); err != nil {
		return nil, err
	}
	authorityTables := make([]string, 0, len(researchTables))
	partialAuthorityColumns := make([]string, 0)
	for _, table := range researchTables {
		quoted := `"` + strings.ReplaceAll(table, `"`, `""`) + `"`
		columns, err := s.db.QueryContext(ctx, `PRAGMA table_info(`+quoted+`)`)
		if err != nil {
			return nil, err
		}
		hasFunded, hasPaper, hasLive := false, false, false
		for columns.Next() {
			var cid, notNull, pk int
			var name, typ string
			var defaultValue any
			if err := columns.Scan(&cid, &name, &typ, &notNull, &defaultValue, &pk); err != nil {
				_ = columns.Close()
				return nil, err
			}
			switch strings.ToLower(name) {
			case "funded":
				hasFunded = true
			case "paper_authority":
				hasPaper = true
			case "live_authority":
				hasLive = true
			}
		}
		if err := columns.Close(); err != nil {
			return nil, err
		}
		present := 0
		for _, ok := range []bool{hasFunded, hasPaper, hasLive} {
			if ok {
				present++
			}
		}
		if present == 3 {
			authorityTables = append(authorityTables, table)
		} else if present != 0 {
			partialAuthorityColumns = append(partialAuthorityColumns, table)
		}
	}
	authorityViolations := 0
	for _, table := range authorityTables {
		var n int
		quoted := `"` + strings.ReplaceAll(table, `"`, `""`) + `"`
		q := fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE funded!=0 OR paper_authority!=0 OR live_authority!=0`, quoted)
		if err := s.db.QueryRowContext(ctx, q).Scan(&n); err != nil {
			return nil, err
		}
		authorityViolations += n
	}
	var forbidden int
	rows, err := s.db.QueryContext(ctx, `SELECT experiment_id,system_name FROM research_experiment_specs`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var experimentID, systemName string
		if err := rows.Scan(&experimentID, &systemName); err != nil {
			_ = rows.Close()
			return nil, err
		}
		if bad, _ := ForbiddenResearchSystem(experimentID, systemName); bad {
			forbidden++
		}
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	requiredTriggers := []string{
		"research_experiment_specs_no_update", "research_experiment_specs_no_delete",
		"research_experiment_events_no_update", "research_experiment_events_no_delete",
		"research_route_opportunities_no_update", "research_route_opportunities_no_delete",
		"research_route_events_no_update", "research_route_events_no_delete",
		"research_security_events_no_update", "research_security_events_no_delete",
	}
	triggerOK := 0
	for _, name := range requiredTriggers {
		var one int
		err := s.db.QueryRowContext(ctx, `SELECT 1 FROM sqlite_master WHERE type='trigger' AND name=?`, name).Scan(&one)
		if err == nil {
			triggerOK++
		} else if err != sql.ErrNoRows {
			return nil, err
		}
	}
	recentRows, err := s.db.QueryContext(ctx, `SELECT observed_ts,category,severity,message,evidence_json
FROM research_security_events ORDER BY id DESC LIMIT 50`)
	if err != nil {
		return nil, err
	}
	defer recentRows.Close()
	var recent []ResearchSecurityRecent
	for recentRows.Next() {
		var v ResearchSecurityRecent
		if err := recentRows.Scan(&v.ObservedTS, &v.Category, &v.Severity, &v.Message, &v.EvidenceJSON); err != nil {
			return nil, err
		}
		recent = append(recent, v)
	}
	if err := recentRows.Err(); err != nil {
		return nil, err
	}
	return map[string]any{
		"generated_at":                  time.Now().UTC().Format(time.RFC3339Nano),
		"research_authority_violations": authorityViolations,
		"research_tables_discovered":    len(researchTables),
		"authority_tables_checked":      len(authorityTables),
		"authority_table_names":         authorityTables,
		"partial_authority_columns":     partialAuthorityColumns,
		"forbidden_systems_registered":  forbidden,
		"immutable_trigger_checks":      map[string]int{"present": triggerOK, "required": len(requiredTriggers)},
		"permanent_bans":                []string{"spoofing", "wash trading", "market manipulation", "systems using material nonpublic information"},
		"credential_scope": map[string]any{
			"status":          "OPERATOR_VERIFICATION_REQUIRED",
			"rule":            "use venue keys with the narrowest trading scope available; never infer scopes from a key ID or secret file",
			"secrets_exposed": false,
		},
		"arm_auto_contract":         "LIVE AUTO must remain session-scoped and can be true only while LIVE is separately armed; research ledgers have no authority columns that can become true",
		"rate_limit_contract":       "venue collectors must use bounded cadence/backoff and a collector receipt; research load cannot be a placement dependency",
		"audit_contract":            "research experiment, route, source, notice, and security truth is append-only; the legacy retention-pruned audit_log is operational history, not the immutable research authority ledger",
		"compliant_for_research":    authorityViolations == 0 && len(partialAuthorityColumns) == 0 && forbidden == 0 && triggerOK == len(requiredTriggers),
		"live_expansion_authorized": false,
		"recent":                    recent,
	}, nil
}
