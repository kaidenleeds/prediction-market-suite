package storage

// This file owns the immutable, zero-authority forward-research manifest and its read-only
// coverage report.  The tables live in execution_shadow.db so research membership can be joined
// to the append-only execution evidence without touching the cash database.

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

const forwardResearchSchema = `
CREATE TABLE IF NOT EXISTS forward_research_generations (
 generation_id TEXT PRIMARY KEY,
 manifest_hash TEXT NOT NULL,
 detector_receipt_version TEXT NOT NULL DEFAULT '',
 event_identity_version TEXT NOT NULL DEFAULT '',
 opened_ts TEXT NOT NULL,
 contract_count INTEGER NOT NULL CHECK(contract_count>0),
 cash_authority INTEGER NOT NULL DEFAULT 0 CHECK(cash_authority=0),
 created_ts TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS forward_research_contracts (
 generation_id TEXT NOT NULL,
 contract_id TEXT NOT NULL,
 ordinal INTEGER NOT NULL CHECK(ordinal>=0),
 system_id TEXT NOT NULL,
 venue TEXT NOT NULL CHECK(venue IN ('kalshi','polyus')),
 side TEXT NOT NULL CHECK(side IN ('YES','NO')),
 action TEXT NOT NULL CHECK(action IN ('BUY','SELL')),
 route TEXT NOT NULL,
 input_topology TEXT NOT NULL,
 execution_class TEXT NOT NULL,
 realistic INTEGER NOT NULL CHECK(realistic IN (0,1)),
 cash_authority INTEGER NOT NULL DEFAULT 0 CHECK(cash_authority=0),
 PRIMARY KEY(generation_id,contract_id),
 UNIQUE(generation_id,ordinal),
 FOREIGN KEY(generation_id) REFERENCES forward_research_generations(generation_id)
);
CREATE TABLE IF NOT EXISTS forward_research_attempts (
 generation_id TEXT NOT NULL,
 attempt_id TEXT PRIMARY KEY,
 contract_id TEXT NOT NULL,
 detector_event_id TEXT NOT NULL UNIQUE,
 model_version TEXT NOT NULL,
 detector_receipt_version TEXT NOT NULL DEFAULT '',
 event_identity_version TEXT NOT NULL DEFAULT '',
 trigger_unix_ms INTEGER NOT NULL CHECK(trigger_unix_ms>0),
 event_cluster_id TEXT NOT NULL DEFAULT '',
 event_cluster_verified INTEGER NOT NULL DEFAULT 0 CHECK(event_cluster_verified IN (0,1)),
 book_transport TEXT NOT NULL DEFAULT '',
 book_generation INTEGER,
 book_subscription_id INTEGER,
 book_sequence INTEGER,
 observed_ts TEXT NOT NULL,
 cash_authority INTEGER NOT NULL DEFAULT 0 CHECK(cash_authority=0),
 FOREIGN KEY(generation_id,contract_id)
  REFERENCES forward_research_contracts(generation_id,contract_id),
 FOREIGN KEY(attempt_id) REFERENCES execution_shadow_attempts(attempt_id),
 FOREIGN KEY(detector_event_id) REFERENCES execution_shadow_events(event_id)
);
CREATE INDEX IF NOT EXISTS idx_forward_research_attempts_contract
 ON forward_research_attempts(generation_id,contract_id,trigger_unix_ms,attempt_id);
CREATE TABLE IF NOT EXISTS forward_research_proofs (
 generation_id TEXT NOT NULL,
 proof_kind TEXT NOT NULL CHECK(proof_kind IN ('stress','capacity')),
 proof_hash TEXT NOT NULL,
 observed_ts TEXT NOT NULL,
 evidence_json TEXT NOT NULL DEFAULT '{}',
 cash_authority INTEGER NOT NULL DEFAULT 0 CHECK(cash_authority=0),
 PRIMARY KEY(generation_id,proof_kind,proof_hash),
 FOREIGN KEY(generation_id) REFERENCES forward_research_generations(generation_id)
);

CREATE TRIGGER IF NOT EXISTS forward_research_generations_no_update
 BEFORE UPDATE ON forward_research_generations
 BEGIN SELECT RAISE(ABORT,'immutable forward-research generation'); END;
CREATE TRIGGER IF NOT EXISTS forward_research_generations_no_delete
 BEFORE DELETE ON forward_research_generations
 BEGIN SELECT RAISE(ABORT,'forward-research generations are append-preserved'); END;
CREATE TRIGGER IF NOT EXISTS forward_research_contracts_no_update
 BEFORE UPDATE ON forward_research_contracts
 BEGIN SELECT RAISE(ABORT,'immutable forward-research contract'); END;
CREATE TRIGGER IF NOT EXISTS forward_research_contracts_no_delete
 BEFORE DELETE ON forward_research_contracts
 BEGIN SELECT RAISE(ABORT,'forward-research contracts are append-preserved'); END;
CREATE TRIGGER IF NOT EXISTS forward_research_contracts_no_insert_after_complete
 BEFORE INSERT ON forward_research_contracts
 WHEN (SELECT COUNT(*) FROM forward_research_contracts c
       WHERE c.generation_id=NEW.generation_id) >=
      (SELECT g.contract_count FROM forward_research_generations g
       WHERE g.generation_id=NEW.generation_id)
 BEGIN SELECT RAISE(ABORT,'sealed forward-research contract membership'); END;
CREATE TRIGGER IF NOT EXISTS forward_research_attempts_no_update
 BEFORE UPDATE ON forward_research_attempts
 BEGIN SELECT RAISE(ABORT,'immutable forward-research attempt link'); END;
CREATE TRIGGER IF NOT EXISTS forward_research_attempts_no_delete
 BEFORE DELETE ON forward_research_attempts
 BEGIN SELECT RAISE(ABORT,'forward-research attempt links are append-preserved'); END;
CREATE TRIGGER IF NOT EXISTS forward_research_proofs_no_update
 BEFORE UPDATE ON forward_research_proofs
 BEGIN SELECT RAISE(ABORT,'immutable forward-research proof'); END;
CREATE TRIGGER IF NOT EXISTS forward_research_proofs_no_delete
 BEFORE DELETE ON forward_research_proofs
 BEGIN SELECT RAISE(ABORT,'forward-research proofs are append-preserved'); END;
`

const forwardResearchDetectorTriggerSchema = `
-- The detector event is already written by the isolated execution-shadow writer.  Index its
-- generation membership in that same transaction, after the parent attempt exists, so the hot
-- detector path does no extra database or network work.
DROP TRIGGER IF EXISTS forward_research_detector_event_after_insert;
CREATE TRIGGER forward_research_detector_event_after_insert
 AFTER INSERT ON execution_shadow_events
 WHEN COALESCE(json_extract(NEW.evidence_json,'$.forward_generation_id'),'')<>''
 BEGIN
  INSERT OR IGNORE INTO forward_research_attempts(
   generation_id,attempt_id,contract_id,detector_event_id,model_version,
   detector_receipt_version,event_identity_version,trigger_unix_ms,event_cluster_id,
   event_cluster_verified,book_transport,book_generation,book_subscription_id,book_sequence,
   observed_ts,cash_authority)
  SELECT
   c.generation_id, NEW.attempt_id, c.contract_id, NEW.event_id,
   COALESCE(json_extract(NEW.evidence_json,'$.execution_model_version'),
            json_extract(NEW.evidence_json,'$.model_version')),
   COALESCE(json_extract(NEW.evidence_json,'$.forward_detector_receipt_version'),''),
   COALESCE(json_extract(NEW.evidence_json,'$.event_cluster_identity_version'),''),
   CAST(json_extract(NEW.evidence_json,'$.trigger_unix_ms') AS INTEGER),
   CASE WHEN json_extract(NEW.evidence_json,'$.event_cluster_verified') IN (1,'true')
          AND TRIM(COALESCE(json_extract(NEW.evidence_json,'$.event_cluster_id'),''))<>''
          AND g.detector_receipt_version<>''
          AND g.event_identity_version<>''
          AND COALESCE(json_extract(NEW.evidence_json,'$.forward_detector_receipt_version'),'')=
              g.detector_receipt_version
          AND COALESCE(json_extract(NEW.evidence_json,'$.event_cluster_identity_version'),'')=
              g.event_identity_version
        THEN COALESCE(json_extract(NEW.evidence_json,'$.event_cluster_id'),'') ELSE '' END,
   CASE WHEN json_extract(NEW.evidence_json,'$.event_cluster_verified') IN (1,'true')
          AND TRIM(COALESCE(json_extract(NEW.evidence_json,'$.event_cluster_id'),''))<>''
          AND g.detector_receipt_version<>''
          AND g.event_identity_version<>''
          AND COALESCE(json_extract(NEW.evidence_json,'$.forward_detector_receipt_version'),'')=
              g.detector_receipt_version
          AND COALESCE(json_extract(NEW.evidence_json,'$.event_cluster_identity_version'),'')=
              g.event_identity_version
        THEN 1 ELSE 0 END,
   COALESCE(json_extract(NEW.evidence_json,'$.book_transport'),''),
   NEW.book_generation,NEW.book_subscription_id,NEW.book_sequence,NEW.event_ts,0
  FROM forward_research_contracts c
  JOIN forward_research_generations g ON g.generation_id=c.generation_id
  WHERE c.generation_id=json_extract(NEW.evidence_json,'$.forward_generation_id')
    AND c.contract_id=COALESCE(json_extract(NEW.evidence_json,'$.signal_contract_id'),
                              json_extract(NEW.evidence_json,'$.signal_contract'))
    AND COALESCE(json_extract(NEW.evidence_json,'$.execution_model_version'),
                 json_extract(NEW.evidence_json,'$.model_version'),'')<>''
    AND CAST(COALESCE(json_extract(NEW.evidence_json,'$.trigger_unix_ms'),0) AS INTEGER)>0
    AND julianday(NEW.event_ts)>=julianday(g.opened_ts)
    AND CAST(json_extract(NEW.evidence_json,'$.trigger_unix_ms') AS INTEGER)>=
        CAST((julianday(g.opened_ts)-2440587.5)*86400000 AS INTEGER);
 END;
`

func ensureForwardResearchSchema(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, forwardResearchSchema); err != nil {
		return err
	}
	// v1/v2 generations are immutable and remain blank. These additive columns let a new
	// generation freeze the exact detector and structural-identity contracts without rewriting
	// either legacy membership or evidence.
	for _, ddl := range []string{
		`ALTER TABLE forward_research_generations ADD COLUMN detector_receipt_version TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE forward_research_generations ADD COLUMN event_identity_version TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE forward_research_attempts ADD COLUMN detector_receipt_version TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE forward_research_attempts ADD COLUMN event_identity_version TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE forward_research_attempts ADD COLUMN event_cluster_verified INTEGER NOT NULL DEFAULT 0 CHECK(event_cluster_verified IN (0,1))`,
	} {
		if _, err := db.ExecContext(ctx, ddl); err != nil &&
			!strings.Contains(strings.ToLower(err.Error()), "duplicate column") {
			return err
		}
	}
	_, err := db.ExecContext(ctx, forwardResearchDetectorTriggerSchema)
	return err
}

const (
	ForwardResearchMinAttempts      = 200
	ForwardResearchMinEventClusters = 50
	ForwardResearchMinActiveDays    = 7
)

type ForwardResearchContract struct {
	ContractID     string `json:"contract_id"`
	SystemID       string `json:"system_id"`
	Venue          string `json:"venue"`
	Side           string `json:"side"`
	Action         string `json:"action"`
	Route          string `json:"route"`
	InputTopology  string `json:"input_topology"`
	ExecutionClass string `json:"execution_class"`
	Realistic      bool   `json:"realistic"`
	CashAuthority  bool   `json:"cash_authority"`
}

type ForwardResearchGeneration struct {
	GenerationID           string                    `json:"generation_id"`
	ManifestHash           string                    `json:"manifest_hash"`
	DetectorReceiptVersion string                    `json:"detector_receipt_version"`
	EventIdentityVersion   string                    `json:"event_identity_version"`
	OpenedAt               time.Time                 `json:"opened_at"`
	Contracts              []ForwardResearchContract `json:"contracts"`
	CashAuthority          bool                      `json:"cash_authority"`
}

type ForwardResearchContractReport struct {
	ForwardResearchContract
	Ordinal           int   `json:"ordinal"`
	ObservedAttempts  int64 `json:"observed_attempts"`
	EventRows         int64 `json:"event_rows"`
	Settled           int64 `json:"settled"`
	EconomicsRequired int64 `json:"economics_required"`
	EconomicsKnown    int64 `json:"economics_known"`
	EventClusters     int64 `json:"event_clusters"`
	ActiveUTCDays     int64 `json:"active_utc_days"`
	MissingCluster    int64 `json:"missing_event_cluster"`
	MissingBook       int64 `json:"missing_book_transport"`
	MissingSequence   int64 `json:"missing_book_sequence"`
	Unseen            bool  `json:"unseen"`
}

type ForwardResearchGates struct {
	MinimumAttempts      int64 `json:"minimum_attempts"`
	MinimumEventClusters int64 `json:"minimum_event_clusters"`
	MinimumActiveUTCDays int64 `json:"minimum_active_utc_days"`
	AttemptsPass         bool  `json:"attempts_pass"`
	EventClustersPass    bool  `json:"event_clusters_pass"`
	ActiveDaysPass       bool  `json:"active_days_pass"`
	GenerationProof      bool  `json:"generation_proof"`
	EventProof           bool  `json:"event_proof"`
	SettlementProof      bool  `json:"settlement_proof"`
	StressProof          bool  `json:"stress_proof"`
	CapacityProof        bool  `json:"capacity_proof"`
}

type ForwardResearchReport struct {
	GenerationID            string                          `json:"generation_id"`
	ManifestHash            string                          `json:"manifest_hash"`
	DetectorReceiptVersion  string                          `json:"detector_receipt_version"`
	EventIdentityVersion    string                          `json:"event_identity_version"`
	OpenedAt                string                          `json:"opened_at"`
	Verdict                 string                          `json:"verdict"`
	CashAuthority           bool                            `json:"cash_authority"`
	PositiveVerdict         bool                            `json:"positive_verdict"`
	ContractCount           int64                           `json:"contract_count"`
	RealisticContracts      int64                           `json:"realistic_contracts"`
	LogOnlyContracts        int64                           `json:"log_only_contracts"`
	ObservedAttempts        int64                           `json:"observed_attempts"`
	EventRows               int64                           `json:"event_rows"`
	Settled                 int64                           `json:"settled"`
	EconomicsRequired       int64                           `json:"economics_required"`
	EconomicsKnown          int64                           `json:"economics_known"`
	EventClusters           int64                           `json:"event_clusters"`
	ActiveUTCDays           int64                           `json:"active_utc_days"`
	UnseenContracts         int64                           `json:"unseen_contracts"`
	AttemptsMissingCluster  int64                           `json:"attempts_missing_event_cluster"`
	AttemptsMissingBook     int64                           `json:"attempts_missing_book_transport"`
	AttemptsMissingSequence int64                           `json:"attempts_missing_book_sequence"`
	Gates                   ForwardResearchGates            `json:"gates"`
	DataGaps                []string                        `json:"data_gaps"`
	Rows                    []ForwardResearchContractReport `json:"rows"`
}

func normalizeForwardContract(in ForwardResearchContract) (ForwardResearchContract, error) {
	in.ContractID = strings.TrimSpace(in.ContractID)
	in.SystemID = strings.ToLower(strings.TrimSpace(in.SystemID))
	in.Venue = strings.ToLower(strings.TrimSpace(in.Venue))
	in.Side = strings.ToUpper(strings.TrimSpace(in.Side))
	in.Action = strings.ToUpper(strings.TrimSpace(in.Action))
	in.Route = strings.ToLower(strings.TrimSpace(in.Route))
	in.InputTopology = strings.ToUpper(strings.TrimSpace(in.InputTopology))
	in.ExecutionClass = strings.TrimSpace(in.ExecutionClass)
	if in.ContractID == "" || in.SystemID == "" || (in.Venue != "kalshi" && in.Venue != "polyus") ||
		(in.Side != "YES" && in.Side != "NO") || (in.Action != "BUY" && in.Action != "SELL") ||
		in.Route == "" || in.InputTopology == "" || in.ExecutionClass == "" || in.CashAuthority {
		return in, errors.New("invalid or cash-authorized forward-research contract")
	}
	return in, nil
}

func ForwardResearchManifestHash(contracts []ForwardResearchContract) (string, error) {
	normalized := make([]ForwardResearchContract, 0, len(contracts))
	seen := map[string]bool{}
	for _, candidate := range contracts {
		row, err := normalizeForwardContract(candidate)
		if err != nil {
			return "", err
		}
		if seen[row.ContractID] {
			return "", fmt.Errorf("duplicate forward-research contract %q", row.ContractID)
		}
		seen[row.ContractID] = true
		normalized = append(normalized, row)
	}
	if len(normalized) == 0 {
		return "", errors.New("empty forward-research manifest")
	}
	sort.Slice(normalized, func(i, j int) bool { return normalized[i].ContractID < normalized[j].ContractID })
	var body strings.Builder
	for _, row := range normalized {
		fmt.Fprintf(&body, "%s\x00%s\x00%s\x00%s\x00%s\x00%s\x00%s\x00%s\x00%t\x000\n",
			row.ContractID, row.SystemID, row.Venue, row.Side, row.Action, row.Route,
			row.InputTopology, row.ExecutionClass, row.Realistic)
	}
	sum := sha256.Sum256([]byte(body.String()))
	return hex.EncodeToString(sum[:]), nil
}

func (s *Store) PrepareForwardResearchGeneration(ctx context.Context,
	in ForwardResearchGeneration) (ForwardResearchGeneration, error) {
	db, err := s.executionShadowHandle()
	if err != nil {
		return ForwardResearchGeneration{}, err
	}
	if err = ensureForwardResearchSchema(ctx, db); err != nil {
		return ForwardResearchGeneration{}, fmt.Errorf("migrate forward-research schema: %w", err)
	}
	in.GenerationID = strings.TrimSpace(in.GenerationID)
	in.DetectorReceiptVersion = strings.TrimSpace(in.DetectorReceiptVersion)
	in.EventIdentityVersion = strings.TrimSpace(in.EventIdentityVersion)
	if in.GenerationID == "" || in.DetectorReceiptVersion == "" ||
		in.EventIdentityVersion == "" || in.CashAuthority {
		return ForwardResearchGeneration{}, errors.New("invalid or cash-authorized forward-research generation")
	}
	normalized := make([]ForwardResearchContract, 0, len(in.Contracts))
	for _, candidate := range in.Contracts {
		row, nerr := normalizeForwardContract(candidate)
		if nerr != nil {
			return ForwardResearchGeneration{}, nerr
		}
		normalized = append(normalized, row)
	}
	sort.Slice(normalized, func(i, j int) bool { return normalized[i].ContractID < normalized[j].ContractID })
	hash, err := ForwardResearchManifestHash(normalized)
	if err != nil {
		return ForwardResearchGeneration{}, err
	}
	if strings.TrimSpace(in.ManifestHash) != "" && !strings.EqualFold(strings.TrimSpace(in.ManifestHash), hash) {
		return ForwardResearchGeneration{}, errors.New("forward-research manifest hash mismatch")
	}
	if in.OpenedAt.IsZero() {
		in.OpenedAt = time.Now().UTC()
	} else {
		in.OpenedAt = in.OpenedAt.UTC()
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return ForwardResearchGeneration{}, err
	}
	defer tx.Rollback()
	var priorHash, priorDetectorVersion, priorIdentityVersion, priorOpened string
	var priorCount int
	var priorCash bool
	err = tx.QueryRowContext(ctx, `SELECT manifest_hash,detector_receipt_version,
event_identity_version,opened_ts,contract_count,cash_authority
FROM forward_research_generations WHERE generation_id=?`, in.GenerationID).
		Scan(&priorHash, &priorDetectorVersion, &priorIdentityVersion, &priorOpened,
			&priorCount, &priorCash)
	if errors.Is(err, sql.ErrNoRows) {
		now := time.Now().UTC().Format(time.RFC3339Nano)
		if _, err = tx.ExecContext(ctx, `INSERT INTO forward_research_generations(
generation_id,manifest_hash,detector_receipt_version,event_identity_version,opened_ts,
contract_count,cash_authority,created_ts) VALUES(?,?,?,?,?,?,0,?)`,
			in.GenerationID, hash, in.DetectorReceiptVersion, in.EventIdentityVersion,
			in.OpenedAt.Format(time.RFC3339Nano), len(normalized), now); err != nil {
			return ForwardResearchGeneration{}, err
		}
		for ordinal, row := range normalized {
			if _, err = tx.ExecContext(ctx, `INSERT INTO forward_research_contracts(
generation_id,contract_id,ordinal,system_id,venue,side,action,route,input_topology,
execution_class,realistic,cash_authority) VALUES(?,?,?,?,?,?,?,?,?,?,?,0)`, in.GenerationID,
				row.ContractID, ordinal, row.SystemID, row.Venue, row.Side, row.Action, row.Route,
				row.InputTopology, row.ExecutionClass, row.Realistic); err != nil {
				return ForwardResearchGeneration{}, err
			}
		}
	} else if err != nil {
		return ForwardResearchGeneration{}, err
	} else {
		opened, parseErr := time.Parse(time.RFC3339Nano, priorOpened)
		if parseErr != nil || priorCash || priorHash != hash ||
			priorDetectorVersion != in.DetectorReceiptVersion ||
			priorIdentityVersion != in.EventIdentityVersion || priorCount != len(normalized) {
			return ForwardResearchGeneration{}, errors.New("immutable forward-research generation differs from current manifest")
		}
		in.OpenedAt = opened.UTC()
		var exact int
		for ordinal, row := range normalized {
			var stored ForwardResearchContract
			var storedOrdinal int
			if err = tx.QueryRowContext(ctx, `SELECT ordinal,system_id,venue,side,action,route,
input_topology,execution_class,realistic,cash_authority FROM forward_research_contracts
WHERE generation_id=? AND contract_id=?`, in.GenerationID, row.ContractID).Scan(&storedOrdinal,
				&stored.SystemID, &stored.Venue, &stored.Side, &stored.Action, &stored.Route,
				&stored.InputTopology, &stored.ExecutionClass, &stored.Realistic,
				&stored.CashAuthority); err != nil {
				return ForwardResearchGeneration{}, err
			}
			stored.ContractID = row.ContractID
			if storedOrdinal != ordinal || stored != row {
				return ForwardResearchGeneration{}, errors.New("immutable forward-research contract differs from current manifest")
			}
			exact++
		}
		if exact != priorCount {
			return ForwardResearchGeneration{}, errors.New("forward-research manifest contract count mismatch")
		}
	}
	var storedCount int
	if err = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM forward_research_contracts
WHERE generation_id=?`, in.GenerationID).Scan(&storedCount); err != nil {
		return ForwardResearchGeneration{}, err
	}
	if storedCount != len(normalized) {
		return ForwardResearchGeneration{}, errors.New("sealed forward-research membership count mismatch")
	}
	if err = tx.Commit(); err != nil {
		return ForwardResearchGeneration{}, err
	}
	in.ManifestHash, in.Contracts, in.CashAuthority = hash, normalized, false
	return in, nil
}

const forwardResearchReportSQL = `WITH cohort AS MATERIALIZED (
 SELECT x.attempt_id,x.contract_id,x.trigger_unix_ms,a.venue,c.realistic,
        CASE WHEN x.event_cluster_verified=1 AND TRIM(x.event_cluster_id)<>''
          AND g.detector_receipt_version<>'' AND g.event_identity_version<>''
          AND x.detector_receipt_version=g.detector_receipt_version
          AND x.event_identity_version=g.event_identity_version
        THEN x.event_cluster_id ELSE '' END event_cluster_id
 FROM forward_research_attempts x
 JOIN execution_shadow_attempts a ON a.attempt_id=x.attempt_id
 JOIN forward_research_contracts c
   ON c.generation_id=x.generation_id AND c.contract_id=x.contract_id
 JOIN forward_research_generations g ON g.generation_id=x.generation_id
 WHERE x.generation_id=?
), event_roll AS (
 SELECT e.attempt_id,COUNT(*) event_rows,MAX(e.settlement_known) settled,
        MAX(CASE WHEN f.realistic=1 AND e.settlement_known=1 AND e.shadow_net IS NOT NULL
          THEN 1 ELSE 0 END) economics_known,
        MAX(CASE WHEN e.stage='counterfactual-execution-terminal' AND
          e.shadow_state IN ('modeled_fill','modeled_zero_fill') AND e.book_source<>'' THEN 1 ELSE 0 END) execution_book,
        MAX(CASE WHEN e.stage='counterfactual-execution-terminal' AND
          e.shadow_state IN ('modeled_fill','modeled_zero_fill') AND e.book_sequence IS NOT NULL THEN 1 ELSE 0 END) execution_sequence
 FROM cohort f
 JOIN execution_shadow_events e ON e.attempt_id=f.attempt_id
 GROUP BY e.attempt_id
), attempt_roll AS (
 SELECT f.contract_id,COUNT(*) attempts,COALESCE(SUM(e.event_rows),0) event_rows,
        COALESCE(SUM(e.settled),0) settled,
        SUM(CASE WHEN f.realistic=1 THEN 1 ELSE 0 END) economics_required,
        COALESCE(SUM(e.economics_known),0) economics_known,
        COUNT(DISTINCT CASE WHEN f.event_cluster_id<>'' THEN f.event_cluster_id END) clusters,
        COUNT(DISTINCT date(f.trigger_unix_ms/1000,'unixepoch')) active_days,
        SUM(CASE WHEN f.event_cluster_id='' THEN 1 ELSE 0 END) missing_cluster,
        SUM(CASE WHEN COALESCE(e.execution_book,0)=0 THEN 1 ELSE 0 END) missing_book,
        SUM(CASE WHEN f.venue='kalshi' AND COALESCE(e.execution_sequence,0)=0 THEN 1 ELSE 0 END) missing_sequence
 FROM cohort f LEFT JOIN event_roll e ON e.attempt_id=f.attempt_id GROUP BY f.contract_id
)
SELECT c.contract_id,c.ordinal,c.system_id,c.venue,c.side,c.action,c.route,c.input_topology,
       c.execution_class,c.realistic,c.cash_authority,
       COALESCE(a.attempts,0),COALESCE(a.event_rows,0),COALESCE(a.settled,0),
       COALESCE(a.economics_required,0),COALESCE(a.economics_known,0),
       COALESCE(a.clusters,0),COALESCE(a.active_days,0),
       COALESCE(a.missing_cluster,0),COALESCE(a.missing_book,0),COALESCE(a.missing_sequence,0)
FROM forward_research_contracts c LEFT JOIN attempt_roll a ON a.contract_id=c.contract_id
WHERE c.generation_id=? ORDER BY c.ordinal`

func (s *Store) ForwardResearchReport(ctx context.Context, generationID string) (ForwardResearchReport, error) {
	var out ForwardResearchReport
	db, err := s.executionShadowHandle()
	if err != nil {
		return out, err
	}
	generationID = strings.TrimSpace(generationID)
	var cash bool
	if err = db.QueryRowContext(ctx, `SELECT generation_id,manifest_hash,detector_receipt_version,
event_identity_version,opened_ts,contract_count,cash_authority
FROM forward_research_generations WHERE generation_id=?`, generationID).Scan(
		&out.GenerationID, &out.ManifestHash, &out.DetectorReceiptVersion,
		&out.EventIdentityVersion, &out.OpenedAt, &out.ContractCount, &cash); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return out, errors.New("forward-research generation missing")
		}
		return out, err
	}
	if cash {
		return out, errors.New("forward-research generation illegally has cash authority")
	}
	manifestContracts := make([]ForwardResearchContract, 0, out.ContractCount)
	// Materialize the selected generation first, then probe the attempt-indexed event ledger.
	// The old event-first CTE grouped every historical event before applying generation_id. On a
	// production ledger with millions of unrelated rows, one dashboard read could time out and
	// pin the WAL long enough to crowd out settlement and collector persistence.
	rows, err := db.QueryContext(ctx, forwardResearchReportSQL, generationID, generationID)
	if err != nil {
		return out, err
	}
	for rows.Next() {
		var row ForwardResearchContractReport
		if err = rows.Scan(&row.ContractID, &row.Ordinal, &row.SystemID, &row.Venue, &row.Side,
			&row.Action, &row.Route, &row.InputTopology, &row.ExecutionClass, &row.Realistic,
			&row.CashAuthority, &row.ObservedAttempts, &row.EventRows, &row.Settled,
			&row.EconomicsRequired, &row.EconomicsKnown, &row.EventClusters,
			&row.ActiveUTCDays, &row.MissingCluster, &row.MissingBook,
			&row.MissingSequence); err != nil {
			return out, err
		}
		if row.CashAuthority {
			return out, errors.New("forward-research contract illegally has cash authority")
		}
		row.Unseen = row.ObservedAttempts == 0
		out.Rows = append(out.Rows, row)
		manifestContracts = append(manifestContracts, row.ForwardResearchContract)
		if row.Realistic {
			out.RealisticContracts++
		} else {
			out.LogOnlyContracts++
		}
		if row.Unseen {
			out.UnseenContracts++
		}
		out.ObservedAttempts += row.ObservedAttempts
		out.EventRows += row.EventRows
		out.Settled += row.Settled
		out.EconomicsRequired += row.EconomicsRequired
		out.EconomicsKnown += row.EconomicsKnown
		out.AttemptsMissingCluster += row.MissingCluster
		out.AttemptsMissingBook += row.MissingBook
		out.AttemptsMissingSequence += row.MissingSequence
	}
	if err = rows.Err(); err != nil {
		_ = rows.Close()
		return out, err
	}
	if err = rows.Close(); err != nil {
		return out, err
	}
	if int64(len(out.Rows)) != out.ContractCount {
		return out, errors.New("forward-research manifest report is incomplete")
	}
	recomputedHash, hashErr := ForwardResearchManifestHash(manifestContracts)
	manifestHashVerified := hashErr == nil && strings.EqualFold(recomputedHash, out.ManifestHash)
	_ = db.QueryRowContext(ctx, `SELECT
COUNT(DISTINCT CASE WHEN x.event_cluster_verified=1 AND TRIM(x.event_cluster_id)<>''
  AND g.detector_receipt_version<>'' AND g.event_identity_version<>''
  AND x.detector_receipt_version=g.detector_receipt_version
  AND x.event_identity_version=g.event_identity_version THEN x.event_cluster_id END),
COUNT(DISTINCT date(x.trigger_unix_ms/1000,'unixepoch'))
FROM forward_research_attempts x
JOIN forward_research_generations g ON g.generation_id=x.generation_id
WHERE x.generation_id=?`, generationID).Scan(&out.EventClusters, &out.ActiveUTCDays)
	var stress, capacity int
	_ = db.QueryRowContext(ctx, `SELECT
EXISTS(SELECT 1 FROM forward_research_proofs WHERE generation_id=? AND proof_kind='stress'),
EXISTS(SELECT 1 FROM forward_research_proofs WHERE generation_id=? AND proof_kind='capacity')`,
		generationID, generationID).Scan(&stress, &capacity)
	out.Gates = ForwardResearchGates{
		MinimumAttempts: ForwardResearchMinAttempts, MinimumEventClusters: ForwardResearchMinEventClusters,
		MinimumActiveUTCDays: ForwardResearchMinActiveDays,
		AttemptsPass:         out.ObservedAttempts >= ForwardResearchMinAttempts,
		EventClustersPass:    out.EventClusters >= ForwardResearchMinEventClusters,
		ActiveDaysPass:       out.ActiveUTCDays >= ForwardResearchMinActiveDays,
		GenerationProof: manifestHashVerified && out.DetectorReceiptVersion != "" &&
			out.EventIdentityVersion != "" && int64(len(out.Rows)) == out.ContractCount,
		EventProof: out.EventRows >= out.ObservedAttempts && out.AttemptsMissingCluster == 0,
		SettlementProof: out.ObservedAttempts > 0 && out.Settled == out.ObservedAttempts &&
			out.EconomicsRequired > 0 && out.EconomicsKnown == out.EconomicsRequired,
		StressProof: stress != 0, CapacityProof: capacity != 0,
	}
	addGap := func(ok bool, gap string) {
		if !ok {
			out.DataGaps = append(out.DataGaps, gap)
		}
	}
	addGap(out.Gates.GenerationProof, "generation-proof-missing")
	addGap(out.Gates.AttemptsPass, "minimum-200-attempts-not-met")
	addGap(out.Gates.EventClustersPass, "minimum-50-event-clusters-not-met")
	addGap(out.Gates.ActiveDaysPass, "minimum-7-active-utc-days-not-met")
	addGap(out.Gates.EventProof, "event-proof-incomplete")
	addGap(out.Gates.SettlementProof, "settlement-or-economics-proof-incomplete")
	addGap(out.Gates.StressProof, "stress-proof-missing")
	addGap(out.Gates.CapacityProof, "capacity-proof-missing")
	if out.UnseenContracts > 0 {
		out.DataGaps = append(out.DataGaps, "manifest-contracts-unseen")
	}
	if out.AttemptsMissingBook > 0 {
		out.DataGaps = append(out.DataGaps, "book-transport-proof-missing")
	}
	if out.AttemptsMissingSequence > 0 {
		out.DataGaps = append(out.DataGaps, "kalshi-book-sequence-proof-missing")
	}
	all := out.Gates.AttemptsPass && out.Gates.EventClustersPass && out.Gates.ActiveDaysPass &&
		out.Gates.GenerationProof && out.Gates.EventProof && out.Gates.SettlementProof &&
		out.Gates.StressProof && out.Gates.CapacityProof && out.UnseenContracts == 0 &&
		out.AttemptsMissingBook == 0 && out.AttemptsMissingSequence == 0
	// Meeting evidence gates permits review; this report never infers positive economics and can
	// never grant cash authority.
	out.PositiveVerdict, out.CashAuthority = false, false
	if all {
		out.Verdict = "EVIDENCE_COMPLETE_REVIEW_REQUIRED"
	} else {
		out.Verdict = "NOT_PROVEN"
	}
	return out, nil
}
