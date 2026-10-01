package storage

// Durable pre-send risk reservations close the gap between a venue accepting an order and that
// order/fill appearing in the authenticated account snapshot. The immutable intent, execution
// legs and cluster attributions are committed before the venue mutation; all later observations
// are append-only events. A restart can therefore never turn an ambiguous or merely lagging order
// into zero exposure.

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
)

const livePendingRiskSchema = `
CREATE TABLE IF NOT EXISTS live_pending_risk_intents (
 reservation_id TEXT PRIMARY KEY,
 created_ts TEXT NOT NULL,
 baseline_observed_ts TEXT NOT NULL,
 product TEXT NOT NULL CHECK(product IN ('single','combo','staged','reduce')),
 dispatch_source TEXT NOT NULL,
 system_id TEXT NOT NULL,
 route TEXT NOT NULL CHECK(route IN ('maker','taker','rfq','staged_fok','reduce_only_fok')),
 principal_usd REAL NOT NULL CHECK(principal_usd>=0),
 fee_usd REAL NOT NULL CHECK(fee_usd>=0),
 cost_usd REAL NOT NULL CHECK(cost_usd>=0 AND abs(cost_usd-principal_usd-fee_usd)<=0.00000001),
 rfq_id TEXT NOT NULL,
 quote_id TEXT NOT NULL,
 source_intent_id TEXT NOT NULL,
 bundle_id TEXT NOT NULL,
 execution_shadow_attempt_id TEXT NOT NULL DEFAULT '',
 request_hash TEXT NOT NULL UNIQUE CHECK(length(request_hash)=64),
 proof_json TEXT NOT NULL CHECK(json_valid(proof_json)),
 baseline_receipt_json TEXT NOT NULL CHECK(json_valid(baseline_receipt_json))
);
CREATE INDEX IF NOT EXISTS idx_live_pending_risk_intent_created
 ON live_pending_risk_intents(created_ts,reservation_id);
CREATE TRIGGER IF NOT EXISTS live_pending_risk_intents_no_update
 BEFORE UPDATE ON live_pending_risk_intents BEGIN
 SELECT RAISE(ABORT,'immutable live pending-risk intent'); END;
CREATE TRIGGER IF NOT EXISTS live_pending_risk_intents_no_delete
 BEFORE DELETE ON live_pending_risk_intents BEGIN
 SELECT RAISE(ABORT,'immutable live pending-risk intent'); END;

CREATE TABLE IF NOT EXISTS live_pending_risk_legs (
 reservation_id TEXT NOT NULL,
 leg_index INTEGER NOT NULL CHECK(leg_index>=0 AND leg_index<6),
 venue TEXT NOT NULL CHECK(venue IN ('kalshi','polyus')),
 ticker TEXT NOT NULL,
 side TEXT NOT NULL CHECK(side IN ('YES','NO')),
 action TEXT NOT NULL CHECK(action IN ('BUY','SELL')),
 client_order_id TEXT NOT NULL,
 quantity REAL NOT NULL CHECK(quantity>0),
 limit_price REAL NOT NULL CHECK(limit_price>0 AND limit_price<1),
 baseline_position_qty REAL NOT NULL,
 expected_position_qty REAL NOT NULL,
 baseline_resting_risk_usd REAL NOT NULL CHECK(baseline_resting_risk_usd>=0),
 PRIMARY KEY(reservation_id,leg_index),
 FOREIGN KEY(reservation_id) REFERENCES live_pending_risk_intents(reservation_id)
);
CREATE INDEX IF NOT EXISTS idx_live_pending_risk_leg_ticker
 ON live_pending_risk_legs(venue,ticker,reservation_id);
CREATE TRIGGER IF NOT EXISTS live_pending_risk_legs_no_update
 BEFORE UPDATE ON live_pending_risk_legs BEGIN
 SELECT RAISE(ABORT,'immutable live pending-risk leg'); END;
CREATE TRIGGER IF NOT EXISTS live_pending_risk_legs_no_delete
 BEFORE DELETE ON live_pending_risk_legs BEGIN
 SELECT RAISE(ABORT,'immutable live pending-risk leg'); END;

CREATE TABLE IF NOT EXISTS live_pending_risk_clusters (
 reservation_id TEXT NOT NULL,
 cluster_index INTEGER NOT NULL CHECK(cluster_index>=0 AND cluster_index<32),
 venue TEXT NOT NULL CHECK(venue IN ('kalshi','polyus')),
 cluster_key TEXT NOT NULL,
 mapping_version TEXT NOT NULL,
 reserved_usd REAL NOT NULL CHECK(reserved_usd>=0),
 PRIMARY KEY(reservation_id,cluster_index),
 UNIQUE(reservation_id,venue,cluster_key),
 FOREIGN KEY(reservation_id) REFERENCES live_pending_risk_intents(reservation_id)
);
CREATE INDEX IF NOT EXISTS idx_live_pending_risk_cluster
 ON live_pending_risk_clusters(venue,cluster_key,reservation_id);
CREATE TRIGGER IF NOT EXISTS live_pending_risk_clusters_no_update
 BEFORE UPDATE ON live_pending_risk_clusters BEGIN
 SELECT RAISE(ABORT,'immutable live pending-risk cluster'); END;
CREATE TRIGGER IF NOT EXISTS live_pending_risk_clusters_no_delete
 BEFORE DELETE ON live_pending_risk_clusters BEGIN
 SELECT RAISE(ABORT,'immutable live pending-risk cluster'); END;

CREATE TABLE IF NOT EXISTS live_pending_risk_events (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 reservation_id TEXT NOT NULL,
 event_seq INTEGER NOT NULL CHECK(event_seq>=1),
 observed_ts TEXT NOT NULL,
 event_type TEXT NOT NULL CHECK(event_type IN (
  'reserved','submit_started','ack','resting_visible','fill_seen','clean_rejected',
  'terminal_unfilled','account_visible','ambiguous','frozen','released')),
 leg_index INTEGER CHECK(leg_index IS NULL OR (leg_index>=0 AND leg_index<6)),
 attempt_key TEXT NOT NULL,
 action TEXT NOT NULL CHECK(action IN ('','BUY','SELL')),
 client_order_id TEXT NOT NULL,
 order_id TEXT NOT NULL,
 filled_qty REAL NOT NULL CHECK(filled_qty>=0),
 average_price REAL NOT NULL CHECK(average_price>=0 AND average_price<=1),
 fee_total REAL NOT NULL CHECK(fee_total>=0),
 account_observed_ts TEXT NOT NULL,
 receipt_source TEXT NOT NULL,
 reason TEXT NOT NULL,
 evidence_json TEXT NOT NULL CHECK(json_valid(evidence_json)),
 FOREIGN KEY(reservation_id) REFERENCES live_pending_risk_intents(reservation_id),
 UNIQUE(reservation_id,event_seq)
);
CREATE INDEX IF NOT EXISTS idx_live_pending_risk_event
 ON live_pending_risk_events(reservation_id,event_seq);
CREATE UNIQUE INDEX IF NOT EXISTS idx_live_pending_risk_one_reserved
 ON live_pending_risk_events(reservation_id) WHERE event_type='reserved';
CREATE UNIQUE INDEX IF NOT EXISTS idx_live_pending_risk_one_release
 ON live_pending_risk_events(reservation_id) WHERE event_type='released';
CREATE UNIQUE INDEX IF NOT EXISTS idx_live_pending_risk_attempt_singletons
 ON live_pending_risk_events(reservation_id,attempt_key,event_type)
 WHERE attempt_key<>'' AND event_type IN (
  'submit_started','ack','resting_visible','clean_rejected','terminal_unfilled','account_visible');
CREATE TRIGGER IF NOT EXISTS live_pending_risk_events_sequence BEFORE INSERT ON live_pending_risk_events
WHEN NEW.event_seq<>(SELECT COALESCE(MAX(event_seq),0)+1 FROM live_pending_risk_events
 WHERE reservation_id=NEW.reservation_id)
BEGIN SELECT RAISE(ABORT,'non-contiguous live pending-risk event sequence'); END;
CREATE TRIGGER IF NOT EXISTS live_pending_risk_events_reserved_first BEFORE INSERT ON live_pending_risk_events
WHEN (NEW.event_type='reserved' AND NEW.event_seq<>1) OR
     (NEW.event_type<>'reserved' AND NOT EXISTS(
       SELECT 1 FROM live_pending_risk_events WHERE reservation_id=NEW.reservation_id AND event_type='reserved'))
BEGIN SELECT RAISE(ABORT,'live pending-risk reservation event must be first'); END;
CREATE TRIGGER IF NOT EXISTS live_pending_risk_events_release_requires_terminal
 BEFORE INSERT ON live_pending_risk_events
WHEN NEW.event_type='released' AND EXISTS(
 SELECT 1 FROM live_pending_risk_events s
 WHERE s.reservation_id=NEW.reservation_id AND s.event_type='submit_started'
   AND NOT EXISTS(
    SELECT 1 FROM live_pending_risk_events t
    WHERE t.reservation_id=s.reservation_id AND t.attempt_key=s.attempt_key
      AND t.event_type IN ('clean_rejected','terminal_unfilled','account_visible')))
BEGIN SELECT RAISE(ABORT,'cannot release unresolved live pending-risk submit'); END;
CREATE TRIGGER IF NOT EXISTS live_pending_risk_events_no_update
 BEFORE UPDATE ON live_pending_risk_events BEGIN
 SELECT RAISE(ABORT,'append-only live pending-risk event'); END;
CREATE TRIGGER IF NOT EXISTS live_pending_risk_events_no_delete
 BEFORE DELETE ON live_pending_risk_events BEGIN
 SELECT RAISE(ABORT,'append-only live pending-risk event'); END;
`

func migrateLivePendingRiskSchema(db *sql.DB) error {
	if _, err := db.Exec(livePendingRiskSchema); err != nil {
		return err
	}
	// Legacy reservations predate the canonical detector-opportunity join. Blank preserves their
	// honest unknown lineage while every new automatic cash intent supplies the exact immutable ID.
	if _, err := db.Exec(`ALTER TABLE live_pending_risk_intents ADD COLUMN execution_shadow_attempt_id TEXT NOT NULL DEFAULT ''`); err != nil &&
		!strings.Contains(strings.ToLower(err.Error()), "duplicate column") {
		return err
	}
	_, err := db.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS idx_live_pending_risk_execution_shadow_attempt
 ON live_pending_risk_intents(execution_shadow_attempt_id)
 WHERE execution_shadow_attempt_id<>''`)
	return err
}

const (
	LivePendingRiskReserved         = "reserved"
	LivePendingRiskSubmitStarted    = "submit_started"
	LivePendingRiskAck              = "ack"
	LivePendingRiskRestingVisible   = "resting_visible"
	LivePendingRiskFillSeen         = "fill_seen"
	LivePendingRiskCleanRejected    = "clean_rejected"
	LivePendingRiskTerminalUnfilled = "terminal_unfilled"
	LivePendingRiskAccountVisible   = "account_visible"
	LivePendingRiskAmbiguous        = "ambiguous"
	LivePendingRiskFrozen           = "frozen"
	LivePendingRiskReleased         = "released"
)

type LivePendingRiskIntent struct {
	ReservationID                  string
	Created, BaselineObserved      time.Time
	Product, DispatchSource        string
	SystemID, Route                string
	PrincipalUSD, FeeUSD, CostUSD  float64
	RFQID, QuoteID                 string
	SourceIntentID, BundleID       string
	ExecutionShadowAttemptID       string
	RequestHash                    string
	ProofJSON, BaselineReceiptJSON string
}

type LivePendingRiskLeg struct {
	ReservationID                              string
	Index                                      int
	Venue, Ticker, Side, Action, ClientOrderID string
	Quantity, LimitPrice                       float64
	BaselinePositionQty, ExpectedPositionQty   float64
	BaselineRestingRiskUSD                     float64
}

type LivePendingRiskCluster struct {
	ReservationID                     string
	Index                             int
	Venue, ClusterKey, MappingVersion string
	ReservedUSD                       float64
}

type LivePendingRiskEvent struct {
	ID                                           int64
	ReservationID                                string
	Sequence                                     int
	Observed, AccountObserved                    time.Time
	EventType, AttemptKey, Action, ClientOrderID string
	OrderID, ReceiptSource, Reason, EvidenceJSON string
	LegIndex                                     *int
	FilledQty, AveragePrice, FeeTotal            float64
}

type LivePendingRiskReservation struct {
	Intent   LivePendingRiskIntent
	Legs     []LivePendingRiskLeg
	Clusters []LivePendingRiskCluster
	Events   []LivePendingRiskEvent
}

func finite(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }

func validRiskSHA256(v string) bool {
	if len(v) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(v)
	return err == nil
}

func canonicalRiskJSON(v string) (string, bool) {
	var raw any
	if err := json.Unmarshal([]byte(strings.TrimSpace(v)), &raw); err != nil {
		return "", false
	}
	b, err := json.Marshal(raw)
	return string(b), err == nil
}

// bindLivePendingRiskOpportunityEvidence makes the normalized immutable intent the sole source of
// opportunity lineage. Callers may repeat the same ID in diagnostic evidence, but they cannot
// omit it from a new automatic reservation or attach a different detector attempt later.
func bindLivePendingRiskOpportunityEvidence(raw, attemptID string) (string, bool) {
	canonical, ok := canonicalRiskJSON(raw)
	if !ok {
		return "", false
	}
	attemptID = strings.TrimSpace(attemptID)
	if attemptID == "" {
		return canonical, true
	}
	var decoded any
	if json.Unmarshal([]byte(canonical), &decoded) != nil {
		return "", false
	}
	var evidence map[string]any
	switch value := decoded.(type) {
	case nil:
		evidence = map[string]any{}
	case map[string]any:
		evidence = value
	default:
		return "", false
	}
	if prior, exists := evidence["execution_shadow_attempt_id"]; exists {
		priorID, stringID := prior.(string)
		if !stringID || strings.TrimSpace(priorID) != attemptID {
			return "", false
		}
	}
	evidence["execution_shadow_attempt_id"] = attemptID
	b, err := json.Marshal(evidence)
	return string(b), err == nil
}

func normalizeLivePendingRisk(in LivePendingRiskIntent, legs []LivePendingRiskLeg,
	clusters []LivePendingRiskCluster) (LivePendingRiskIntent, []LivePendingRiskLeg, []LivePendingRiskCluster, error) {
	in.ReservationID = strings.TrimSpace(in.ReservationID)
	in.Product = strings.ToLower(strings.TrimSpace(in.Product))
	in.DispatchSource = strings.TrimSpace(in.DispatchSource)
	in.SystemID = strings.TrimSpace(in.SystemID)
	in.Route = strings.ToLower(strings.TrimSpace(in.Route))
	in.RFQID, in.QuoteID = strings.TrimSpace(in.RFQID), strings.TrimSpace(in.QuoteID)
	in.SourceIntentID, in.BundleID = strings.TrimSpace(in.SourceIntentID), strings.TrimSpace(in.BundleID)
	in.ExecutionShadowAttemptID = strings.TrimSpace(in.ExecutionShadowAttemptID)
	in.RequestHash = strings.ToLower(strings.TrimSpace(in.RequestHash))
	var ok bool
	if in.ProofJSON, ok = canonicalRiskJSON(in.ProofJSON); !ok {
		return in, nil, nil, errors.New("invalid live pending-risk proof JSON")
	}
	if in.ProofJSON, ok = bindLivePendingRiskOpportunityEvidence(
		in.ProofJSON, in.ExecutionShadowAttemptID); !ok {
		return in, nil, nil, errors.New("live pending-risk proof conflicts with its immutable opportunity id")
	}
	if in.BaselineReceiptJSON, ok = canonicalRiskJSON(in.BaselineReceiptJSON); !ok {
		return in, nil, nil, errors.New("invalid live pending-risk baseline receipt JSON")
	}
	validProduct := in.Product == "single" || in.Product == "combo" || in.Product == "staged" || in.Product == "reduce"
	validRoute := in.Route == "maker" || in.Route == "taker" || in.Route == "rfq" ||
		in.Route == "staged_fok" || in.Route == "reduce_only_fok"
	if in.ReservationID == "" || in.Created.IsZero() || in.BaselineObserved.IsZero() || !validProduct ||
		in.DispatchSource == "" || in.SystemID == "" || !validRoute || !validRiskSHA256(in.RequestHash) ||
		!finite(in.PrincipalUSD) || !finite(in.FeeUSD) || !finite(in.CostUSD) ||
		in.PrincipalUSD < 0 || in.FeeUSD < 0 || in.CostUSD < 0 ||
		math.Abs(in.CostUSD-in.PrincipalUSD-in.FeeUSD) > 1e-8 {
		return in, nil, nil, errors.New("invalid live pending-risk intent")
	}
	if (in.Product == "combo") != (in.Route == "rfq") ||
		(in.Product == "staged") != (in.Route == "staged_fok") ||
		(in.Product == "reduce") != (in.Route == "reduce_only_fok") ||
		(in.Product == "combo" && (in.RFQID == "" || in.QuoteID == "")) {
		return in, nil, nil, errors.New("live pending-risk product/route identity is inconsistent")
	}
	if len(legs) == 0 || len(legs) > 6 || len(clusters) == 0 || len(clusters) > 32 {
		return in, nil, nil, errors.New("live pending-risk reservation needs bounded legs and clusters")
	}
	legs = append([]LivePendingRiskLeg(nil), legs...)
	sort.Slice(legs, func(i, j int) bool { return legs[i].Index < legs[j].Index })
	for i := range legs {
		leg := &legs[i]
		leg.ReservationID = in.ReservationID
		leg.Venue = strings.ToLower(strings.TrimSpace(leg.Venue))
		leg.Ticker = strings.TrimSpace(leg.Ticker)
		leg.Side = strings.ToUpper(strings.TrimSpace(leg.Side))
		leg.Action = strings.ToUpper(strings.TrimSpace(leg.Action))
		leg.ClientOrderID = strings.TrimSpace(leg.ClientOrderID)
		if leg.Index != i || (leg.Venue != "kalshi" && leg.Venue != "polyus") || leg.Ticker == "" ||
			(leg.Side != "YES" && leg.Side != "NO") || (leg.Action != "BUY" && leg.Action != "SELL") ||
			!finite(leg.Quantity) || leg.Quantity <= 0 || !finite(leg.LimitPrice) ||
			leg.LimitPrice <= 0 || leg.LimitPrice >= 1 || !finite(leg.BaselinePositionQty) ||
			!finite(leg.ExpectedPositionQty) || !finite(leg.BaselineRestingRiskUSD) || leg.BaselineRestingRiskUSD < 0 {
			return in, nil, nil, fmt.Errorf("invalid live pending-risk leg %d", i)
		}
		if leg.Venue == "kalshi" && in.Route != "rfq" && leg.ClientOrderID == "" {
			return in, nil, nil, fmt.Errorf("Kalshi live pending-risk leg %d lacks deterministic client order id", i)
		}
	}
	if in.Product != "staged" && len(legs) != 1 {
		return in, nil, nil, errors.New("only staged reservations may contain multiple execution legs")
	}
	clusters = append([]LivePendingRiskCluster(nil), clusters...)
	sort.Slice(clusters, func(i, j int) bool { return clusters[i].Index < clusters[j].Index })
	seenCluster := map[string]bool{}
	for i := range clusters {
		cluster := &clusters[i]
		cluster.ReservationID = in.ReservationID
		cluster.Venue = strings.ToLower(strings.TrimSpace(cluster.Venue))
		cluster.ClusterKey = strings.TrimSpace(cluster.ClusterKey)
		cluster.MappingVersion = strings.TrimSpace(cluster.MappingVersion)
		key := cluster.Venue + "\x00" + cluster.ClusterKey
		if cluster.Index != i || (cluster.Venue != "kalshi" && cluster.Venue != "polyus") ||
			cluster.ClusterKey == "" || cluster.MappingVersion == "" || !finite(cluster.ReservedUSD) ||
			cluster.ReservedUSD < 0 || cluster.ReservedUSD > in.CostUSD+1e-8 || seenCluster[key] {
			return in, nil, nil, fmt.Errorf("invalid live pending-risk cluster %d", i)
		}
		seenCluster[key] = true
	}
	return in, legs, clusters, nil
}

func sameRiskFloat(a, b float64) bool { return math.Abs(a-b) <= 1e-9 }

func equalLivePendingRiskIntent(a, b LivePendingRiskIntent) bool {
	return a.ReservationID == b.ReservationID && a.Created.Equal(b.Created) &&
		a.BaselineObserved.Equal(b.BaselineObserved) && a.Product == b.Product &&
		a.DispatchSource == b.DispatchSource && a.SystemID == b.SystemID && a.Route == b.Route &&
		sameRiskFloat(a.PrincipalUSD, b.PrincipalUSD) && sameRiskFloat(a.FeeUSD, b.FeeUSD) &&
		sameRiskFloat(a.CostUSD, b.CostUSD) && a.RFQID == b.RFQID && a.QuoteID == b.QuoteID &&
		a.SourceIntentID == b.SourceIntentID && a.BundleID == b.BundleID &&
		a.ExecutionShadowAttemptID == b.ExecutionShadowAttemptID && a.RequestHash == b.RequestHash &&
		a.ProofJSON == b.ProofJSON && a.BaselineReceiptJSON == b.BaselineReceiptJSON
}

func equalLivePendingRiskLeg(a, b LivePendingRiskLeg) bool {
	return a.ReservationID == b.ReservationID && a.Index == b.Index && a.Venue == b.Venue &&
		a.Ticker == b.Ticker && a.Side == b.Side && a.Action == b.Action && a.ClientOrderID == b.ClientOrderID &&
		sameRiskFloat(a.Quantity, b.Quantity) && sameRiskFloat(a.LimitPrice, b.LimitPrice) &&
		sameRiskFloat(a.BaselinePositionQty, b.BaselinePositionQty) &&
		sameRiskFloat(a.ExpectedPositionQty, b.ExpectedPositionQty) &&
		sameRiskFloat(a.BaselineRestingRiskUSD, b.BaselineRestingRiskUSD)
}

func equalLivePendingRiskCluster(a, b LivePendingRiskCluster) bool {
	return a.ReservationID == b.ReservationID && a.Index == b.Index && a.Venue == b.Venue &&
		a.ClusterKey == b.ClusterKey && a.MappingVersion == b.MappingVersion &&
		sameRiskFloat(a.ReservedUSD, b.ReservedUSD)
}

type riskQuerier interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func loadLivePendingRisk(ctx context.Context, q riskQuerier, id string) (LivePendingRiskReservation, bool, error) {
	var out LivePendingRiskReservation
	var created, baseline string
	err := q.QueryRowContext(ctx, `SELECT reservation_id,created_ts,baseline_observed_ts,product,
	dispatch_source,system_id,route,principal_usd,fee_usd,cost_usd,rfq_id,quote_id,source_intent_id,
	bundle_id,execution_shadow_attempt_id,request_hash,proof_json,baseline_receipt_json FROM live_pending_risk_intents WHERE reservation_id=?`,
		id).Scan(&out.Intent.ReservationID, &created, &baseline, &out.Intent.Product,
		&out.Intent.DispatchSource, &out.Intent.SystemID, &out.Intent.Route, &out.Intent.PrincipalUSD,
		&out.Intent.FeeUSD, &out.Intent.CostUSD, &out.Intent.RFQID, &out.Intent.QuoteID,
		&out.Intent.SourceIntentID, &out.Intent.BundleID, &out.Intent.ExecutionShadowAttemptID,
		&out.Intent.RequestHash, &out.Intent.ProofJSON,
		&out.Intent.BaselineReceiptJSON)
	if errors.Is(err, sql.ErrNoRows) {
		return out, false, nil
	}
	if err != nil {
		return out, false, err
	}
	if out.Intent.Created, err = time.Parse(time.RFC3339Nano, created); err != nil {
		return out, false, errors.New("stored live pending-risk creation timestamp is invalid")
	}
	if out.Intent.BaselineObserved, err = time.Parse(time.RFC3339Nano, baseline); err != nil {
		return out, false, errors.New("stored live pending-risk baseline timestamp is invalid")
	}
	rows, err := q.QueryContext(ctx, `SELECT reservation_id,leg_index,venue,ticker,side,action,
client_order_id,quantity,limit_price,baseline_position_qty,expected_position_qty,baseline_resting_risk_usd
FROM live_pending_risk_legs WHERE reservation_id=? ORDER BY leg_index`, id)
	if err != nil {
		return out, false, err
	}
	for rows.Next() {
		var v LivePendingRiskLeg
		if err := rows.Scan(&v.ReservationID, &v.Index, &v.Venue, &v.Ticker, &v.Side, &v.Action,
			&v.ClientOrderID, &v.Quantity, &v.LimitPrice, &v.BaselinePositionQty,
			&v.ExpectedPositionQty, &v.BaselineRestingRiskUSD); err != nil {
			rows.Close()
			return out, false, err
		}
		out.Legs = append(out.Legs, v)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return out, false, err
	}
	if err := rows.Close(); err != nil {
		return out, false, err
	}
	rows, err = q.QueryContext(ctx, `SELECT reservation_id,cluster_index,venue,cluster_key,
mapping_version,reserved_usd FROM live_pending_risk_clusters WHERE reservation_id=? ORDER BY cluster_index`, id)
	if err != nil {
		return out, false, err
	}
	for rows.Next() {
		var v LivePendingRiskCluster
		if err := rows.Scan(&v.ReservationID, &v.Index, &v.Venue, &v.ClusterKey,
			&v.MappingVersion, &v.ReservedUSD); err != nil {
			rows.Close()
			return out, false, err
		}
		out.Clusters = append(out.Clusters, v)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return out, false, err
	}
	if err := rows.Close(); err != nil {
		return out, false, err
	}
	rows, err = q.QueryContext(ctx, `SELECT id,reservation_id,event_seq,observed_ts,event_type,
leg_index,attempt_key,action,client_order_id,order_id,filled_qty,average_price,fee_total,
account_observed_ts,receipt_source,reason,evidence_json
FROM live_pending_risk_events WHERE reservation_id=? ORDER BY event_seq`, id)
	if err != nil {
		return out, false, err
	}
	for rows.Next() {
		var v LivePendingRiskEvent
		var observed, account string
		var leg sql.NullInt64
		if err := rows.Scan(&v.ID, &v.ReservationID, &v.Sequence, &observed, &v.EventType, &leg,
			&v.AttemptKey, &v.Action, &v.ClientOrderID, &v.OrderID, &v.FilledQty,
			&v.AveragePrice, &v.FeeTotal, &account, &v.ReceiptSource, &v.Reason, &v.EvidenceJSON); err != nil {
			rows.Close()
			return out, false, err
		}
		v.Observed, err = time.Parse(time.RFC3339Nano, observed)
		if err != nil {
			rows.Close()
			return out, false, errors.New("stored live pending-risk event timestamp is invalid")
		}
		if account != "" {
			v.AccountObserved, err = time.Parse(time.RFC3339Nano, account)
			if err != nil {
				rows.Close()
				return out, false, errors.New("stored live pending-risk account timestamp is invalid")
			}
		}
		if leg.Valid {
			i := int(leg.Int64)
			v.LegIndex = &i
		}
		out.Events = append(out.Events, v)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return out, false, err
	}
	if err := rows.Close(); err != nil {
		return out, false, err
	}
	return out, true, validateLoadedLivePendingRisk(out)
}

func validRiskEventType(v string) bool {
	switch v {
	case LivePendingRiskReserved, LivePendingRiskSubmitStarted, LivePendingRiskAck,
		LivePendingRiskRestingVisible, LivePendingRiskFillSeen, LivePendingRiskCleanRejected,
		LivePendingRiskTerminalUnfilled, LivePendingRiskAccountVisible, LivePendingRiskAmbiguous,
		LivePendingRiskFrozen, LivePendingRiskReleased:
		return true
	}
	return false
}

func riskAttemptTerminal(v string) bool {
	return v == LivePendingRiskCleanRejected || v == LivePendingRiskTerminalUnfilled ||
		v == LivePendingRiskAccountVisible
}

func validateLoadedLivePendingRisk(v LivePendingRiskReservation) error {
	norm, legs, clusters, err := normalizeLivePendingRisk(v.Intent, v.Legs, v.Clusters)
	if err != nil || !equalLivePendingRiskIntent(norm, v.Intent) || len(legs) != len(v.Legs) ||
		len(clusters) != len(v.Clusters) {
		return errors.New("stored live pending-risk immutable reservation is invalid")
	}
	for i := range legs {
		if !equalLivePendingRiskLeg(legs[i], v.Legs[i]) {
			return errors.New("stored live pending-risk leg is invalid")
		}
	}
	for i := range clusters {
		if !equalLivePendingRiskCluster(clusters[i], v.Clusters[i]) {
			return errors.New("stored live pending-risk cluster is invalid")
		}
	}
	if len(v.Events) == 0 || v.Events[0].EventType != LivePendingRiskReserved || v.Events[0].Sequence != 1 {
		return errors.New("stored live pending-risk reservation lacks its first event")
	}
	type attemptState struct {
		started, acked, terminal       bool
		leg                            int
		action, clientOrderID, orderID string
		filledQty, feeTotal            float64
	}
	attempts := map[string]attemptState{}
	released := false
	for i, event := range v.Events {
		if event.ReservationID != v.Intent.ReservationID || event.Sequence != i+1 || event.Observed.IsZero() ||
			!validRiskEventType(event.EventType) || !finite(event.FilledQty) || !finite(event.AveragePrice) ||
			!finite(event.FeeTotal) || event.FilledQty < 0 || event.AveragePrice < 0 ||
			event.AveragePrice > 1 || event.FeeTotal < 0 || strings.TrimSpace(event.ReceiptSource) == "" ||
			strings.TrimSpace(event.Reason) == "" {
			return errors.New("stored live pending-risk event is invalid")
		}
		if _, ok := canonicalRiskJSON(event.EvidenceJSON); !ok || released {
			return errors.New("stored live pending-risk event evidence/state is invalid")
		}
		if event.EventType == LivePendingRiskReserved {
			if i != 0 || event.LegIndex != nil || event.AttemptKey != "" || event.Action != "" {
				return errors.New("stored live pending-risk reserved event is invalid")
			}
			continue
		}
		if event.EventType == LivePendingRiskReleased {
			for _, state := range attempts {
				if state.started && !state.terminal {
					return errors.New("live pending-risk reservation released before every submit was terminal")
				}
			}
			released = true
			continue
		}
		if event.EventType == LivePendingRiskFrozen && event.AttemptKey == "" {
			continue
		}
		if event.EventType == LivePendingRiskAmbiguous && event.AttemptKey == "" {
			continue
		}
		if event.LegIndex == nil || *event.LegIndex < 0 || *event.LegIndex >= len(v.Legs) ||
			strings.TrimSpace(event.AttemptKey) == "" || (event.Action != "BUY" && event.Action != "SELL") {
			return errors.New("stored live pending-risk attempt identity is invalid")
		}
		state := attempts[event.AttemptKey]
		leg := v.Legs[*event.LegIndex]
		if event.FilledQty > leg.Quantity+1e-9 || (event.FilledQty > 0 &&
			(event.AveragePrice <= 0 || event.AveragePrice >= 1)) {
			return errors.New("stored live pending-risk fill exceeds its leg or lacks exact price")
		}
		switch event.EventType {
		case LivePendingRiskSubmitStarted:
			if state.started {
				return errors.New("duplicate live pending-risk submit attempt")
			}
			identityMismatch := event.Action != leg.Action || event.ClientOrderID != leg.ClientOrderID
			// Staged packages may append a separately idempotent SELL unwind for an immutable BUY
			// leg. Every single/combo/reduce attempt must match exactly; the staged exception still
			// requires the declared BUY leg and a distinct non-empty client identity for the unwind.
			stagedUnwind := v.Intent.Product == "staged" && leg.Action == "BUY" &&
				event.Action == "SELL" && event.ClientOrderID != "" && event.ClientOrderID != leg.ClientOrderID
			if identityMismatch && !stagedUnwind {
				return errors.New("live pending-risk submit differs from immutable leg action/client identity")
			}
			if leg.Venue == "kalshi" && v.Intent.Route != "rfq" && event.ClientOrderID == "" {
				return errors.New("Kalshi live pending-risk submit lacks deterministic client order id")
			}
			state.started, state.leg, state.action, state.clientOrderID = true, *event.LegIndex,
				event.Action, event.ClientOrderID
		case LivePendingRiskAck:
			if !state.started || state.acked || event.OrderID == "" || state.leg != *event.LegIndex ||
				state.action != event.Action || state.clientOrderID != event.ClientOrderID {
				return errors.New("live pending-risk acknowledgement is out of order")
			}
			state.acked, state.orderID = true, event.OrderID
		case LivePendingRiskCleanRejected:
			if !state.started || state.terminal || state.leg != *event.LegIndex ||
				state.action != event.Action || state.clientOrderID != event.ClientOrderID ||
				event.FilledQty != 0 || event.FeeTotal != 0 {
				return errors.New("live pending-risk clean rejection is out of order")
			}
			state.terminal = true
		case LivePendingRiskRestingVisible, LivePendingRiskFillSeen,
			LivePendingRiskTerminalUnfilled, LivePendingRiskAccountVisible:
			if !state.acked || state.terminal || event.OrderID == "" || state.leg != *event.LegIndex ||
				state.action != event.Action || state.clientOrderID != event.ClientOrderID ||
				state.orderID != event.OrderID || event.FilledQty+1e-9 < state.filledQty ||
				event.FeeTotal+1e-9 < state.feeTotal {
				return errors.New("live pending-risk authoritative receipt is out of order")
			}
			if event.EventType == LivePendingRiskTerminalUnfilled &&
				(event.FilledQty != 0 || event.FeeTotal != 0) {
				return errors.New("terminal-unfilled live pending-risk receipt contains a fill")
			}
			if event.EventType == LivePendingRiskAccountVisible && event.AccountObserved.IsZero() {
				return errors.New("live pending-risk account-visible receipt lacks account timestamp")
			}
			if event.EventType == LivePendingRiskAccountVisible && event.FilledQty <= 0 {
				return errors.New("account-visible live pending-risk receipt lacks an exact fill")
			}
			state.filledQty, state.feeTotal = event.FilledQty, event.FeeTotal
			if riskAttemptTerminal(event.EventType) {
				state.terminal = true
			}
		case LivePendingRiskAmbiguous, LivePendingRiskFrozen:
			if !state.started {
				return errors.New("live pending-risk ambiguity precedes submit")
			}
		}
		attempts[event.AttemptKey] = state
	}
	return nil
}

func (s *Store) InsertLivePendingRisk(ctx context.Context, in LivePendingRiskIntent,
	legs []LivePendingRiskLeg, clusters []LivePendingRiskCluster) (bool, error) {
	in, legs, clusters, err := normalizeLivePendingRisk(in, legs, clusters)
	if err != nil {
		return false, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	var existingID string
	err = tx.QueryRowContext(ctx, `SELECT reservation_id FROM live_pending_risk_intents
WHERE reservation_id=? OR request_hash=? LIMIT 1`, in.ReservationID, in.RequestHash).Scan(&existingID)
	if err == nil {
		existing, ok, loadErr := loadLivePendingRisk(ctx, tx, existingID)
		if loadErr != nil || !ok {
			return false, firstRiskErr(loadErr, errors.New("existing live pending-risk reservation is unreadable"))
		}
		same := equalLivePendingRiskIntent(existing.Intent, in) && len(existing.Legs) == len(legs) &&
			len(existing.Clusters) == len(clusters)
		for i := range legs {
			same = same && equalLivePendingRiskLeg(existing.Legs[i], legs[i])
		}
		for i := range clusters {
			same = same && equalLivePendingRiskCluster(existing.Clusters[i], clusters[i])
		}
		if !same {
			return false, errors.New("live pending-risk reservation id/hash reused with changed immutable money fields")
		}
		return false, tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return false, err
	}
	// Exact venue+ticker claims are mutually exclusive while unreleased. This check lives inside
	// the same transaction as insertion, so two concurrent HTTP handlers cannot both pass a
	// process-memory guard and create restart-surviving duplicate exposure on one contract.
	for _, leg := range legs {
		// A reduce-only mutation may close an in-flight entry, but it may not overlap another exit
		// or staged coordinator on the same contract. Two ordinary SELL requests are not magically
		// venue-reduce-only, so allowing both could oversell/reverse exposure while account state lags.
		var activeID string
		err = tx.QueryRowContext(ctx, `SELECT l.reservation_id
FROM live_pending_risk_legs l
JOIN live_pending_risk_intents i ON i.reservation_id=l.reservation_id
WHERE lower(l.venue)=lower(?) AND lower(l.ticker)=lower(?)
  AND NOT EXISTS(SELECT 1 FROM live_pending_risk_events e
                 WHERE e.reservation_id=l.reservation_id AND e.event_type='released')
  AND (?<>'reduce' OR i.product IN ('reduce','staged'))
LIMIT 1`, leg.Venue, leg.Ticker, in.Product).Scan(&activeID)
		if err == nil {
			return false, fmt.Errorf("active live pending-risk reservation %s already owns %s:%s",
				activeID, leg.Venue, leg.Ticker)
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return false, err
		}
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO live_pending_risk_intents(
reservation_id,created_ts,baseline_observed_ts,product,dispatch_source,system_id,route,
principal_usd,fee_usd,cost_usd,rfq_id,quote_id,source_intent_id,bundle_id,execution_shadow_attempt_id,
request_hash,proof_json,baseline_receipt_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		in.ReservationID, in.Created.UTC().Format(time.RFC3339Nano),
		in.BaselineObserved.UTC().Format(time.RFC3339Nano), in.Product, in.DispatchSource,
		in.SystemID, in.Route, in.PrincipalUSD, in.FeeUSD, in.CostUSD, in.RFQID, in.QuoteID,
		in.SourceIntentID, in.BundleID, in.ExecutionShadowAttemptID, in.RequestHash, in.ProofJSON,
		in.BaselineReceiptJSON)
	if err != nil {
		return false, err
	}
	for _, leg := range legs {
		_, err = tx.ExecContext(ctx, `INSERT INTO live_pending_risk_legs(
reservation_id,leg_index,venue,ticker,side,action,client_order_id,quantity,limit_price,
baseline_position_qty,expected_position_qty,baseline_resting_risk_usd) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`,
			leg.ReservationID, leg.Index, leg.Venue, leg.Ticker, leg.Side, leg.Action,
			leg.ClientOrderID, leg.Quantity, leg.LimitPrice, leg.BaselinePositionQty,
			leg.ExpectedPositionQty, leg.BaselineRestingRiskUSD)
		if err != nil {
			return false, err
		}
	}
	for _, cluster := range clusters {
		_, err = tx.ExecContext(ctx, `INSERT INTO live_pending_risk_clusters(
reservation_id,cluster_index,venue,cluster_key,mapping_version,reserved_usd) VALUES(?,?,?,?,?,?)`,
			cluster.ReservationID, cluster.Index, cluster.Venue, cluster.ClusterKey,
			cluster.MappingVersion, cluster.ReservedUSD)
		if err != nil {
			return false, err
		}
	}
	reservedEvidence, ok := bindLivePendingRiskOpportunityEvidence(`{}`, in.ExecutionShadowAttemptID)
	if !ok {
		return false, errors.New("live pending-risk reserved evidence could not bind opportunity id")
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO live_pending_risk_events(
reservation_id,event_seq,observed_ts,event_type,leg_index,attempt_key,action,client_order_id,
order_id,filled_qty,average_price,fee_total,account_observed_ts,receipt_source,reason,evidence_json)
VALUES(?,?,?,?,NULL,'','','','',0,0,0,'',?,?,?)`, in.ReservationID, 1,
		in.Created.UTC().Format(time.RFC3339Nano), LivePendingRiskReserved,
		"pre-send-reservation", "immutable risk and identity reserved before any venue mutation", reservedEvidence)
	if err != nil {
		return false, err
	}
	if err = tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

func firstRiskErr(a, b error) error {
	if a != nil {
		return a
	}
	return b
}

func normalizeRiskEvent(e LivePendingRiskEvent) (LivePendingRiskEvent, error) {
	e.ReservationID = strings.TrimSpace(e.ReservationID)
	e.EventType = strings.ToLower(strings.TrimSpace(e.EventType))
	e.AttemptKey = strings.TrimSpace(e.AttemptKey)
	e.Action = strings.ToUpper(strings.TrimSpace(e.Action))
	e.ClientOrderID = strings.TrimSpace(e.ClientOrderID)
	e.OrderID = strings.TrimSpace(e.OrderID)
	e.ReceiptSource = strings.TrimSpace(e.ReceiptSource)
	e.Reason = strings.TrimSpace(e.Reason)
	var ok bool
	if e.EvidenceJSON, ok = canonicalRiskJSON(e.EvidenceJSON); !ok {
		return e, errors.New("invalid live pending-risk event evidence JSON")
	}
	if e.ReservationID == "" || e.Observed.IsZero() || !validRiskEventType(e.EventType) ||
		!finite(e.FilledQty) || !finite(e.AveragePrice) || !finite(e.FeeTotal) ||
		e.FilledQty < 0 || e.AveragePrice < 0 || e.AveragePrice > 1 || e.FeeTotal < 0 ||
		e.ReceiptSource == "" || e.Reason == "" {
		return e, errors.New("invalid live pending-risk event")
	}
	if e.EventType == LivePendingRiskReserved {
		return e, errors.New("reserved event is created atomically with the immutable intent")
	}
	if e.EventType == LivePendingRiskReleased {
		if e.LegIndex != nil || e.AttemptKey != "" || e.Action != "" || e.OrderID != "" {
			return e, errors.New("released event must be reservation-scoped")
		}
		return e, nil
	}
	if (e.EventType == LivePendingRiskFrozen || e.EventType == LivePendingRiskAmbiguous) && e.AttemptKey == "" {
		if e.LegIndex != nil || e.Action != "" {
			return e, errors.New("reservation-scoped freeze/ambiguity cannot name a leg/action")
		}
		return e, nil
	}
	if e.LegIndex == nil || *e.LegIndex < 0 || e.AttemptKey == "" ||
		(e.Action != "BUY" && e.Action != "SELL") {
		return e, errors.New("live pending-risk attempt event lacks leg/attempt/action identity")
	}
	if (e.EventType == LivePendingRiskAck || e.EventType == LivePendingRiskRestingVisible ||
		e.EventType == LivePendingRiskFillSeen || e.EventType == LivePendingRiskTerminalUnfilled ||
		e.EventType == LivePendingRiskAccountVisible) && e.OrderID == "" {
		return e, errors.New("live pending-risk authoritative event lacks order identity")
	}
	if e.EventType == LivePendingRiskAccountVisible && e.AccountObserved.IsZero() {
		return e, errors.New("account-visible live pending-risk event lacks account timestamp")
	}
	return e, nil
}

func equalRiskEventPayload(a, b LivePendingRiskEvent) bool {
	legSame := a.LegIndex == nil && b.LegIndex == nil
	if a.LegIndex != nil && b.LegIndex != nil {
		legSame = *a.LegIndex == *b.LegIndex
	}
	return a.ReservationID == b.ReservationID && a.EventType == b.EventType && legSame &&
		a.AttemptKey == b.AttemptKey && a.Action == b.Action && a.ClientOrderID == b.ClientOrderID &&
		a.OrderID == b.OrderID && sameRiskFloat(a.FilledQty, b.FilledQty) &&
		sameRiskFloat(a.AveragePrice, b.AveragePrice) && sameRiskFloat(a.FeeTotal, b.FeeTotal) &&
		a.AccountObserved.Equal(b.AccountObserved) && a.ReceiptSource == b.ReceiptSource &&
		a.Reason == b.Reason && a.EvidenceJSON == b.EvidenceJSON
}

func equalRiskEventAttemptIdentity(a, b LivePendingRiskEvent) bool {
	legSame := a.LegIndex == nil && b.LegIndex == nil
	if a.LegIndex != nil && b.LegIndex != nil {
		legSame = *a.LegIndex == *b.LegIndex
	}
	return legSame && a.AttemptKey == b.AttemptKey && a.Action == b.Action &&
		a.ClientOrderID == b.ClientOrderID && a.OrderID == b.OrderID
}

// kalshiCreateAverageFeeEnvelope recognizes the one deliberately lower-precision receipt in the
// pending-risk journal. Kalshi's create-order response exposes average_fee_paid per contract as a
// fixed-point string, while the order-scoped fills endpoint exposes fee_cost for every fill. The
// former can be truncated by the venue (for example 0.0174 for a 12-contract total of 0.2097), so
// multiplying it by quantity is not immutable cumulative fee truth.
//
// Keep this exception narrow: the source must be the create acknowledgement, the raw fixed-point
// value and fill count must still be present in its canonical evidence, and the stored total must
// exactly be the multiplication callers historically performed. The returned half-open envelope
// is the only total that the truncated string could represent. No generic fee tolerance is used.
func kalshiCreateAverageFeeEnvelope(e LivePendingRiskEvent) (float64, float64, bool) {
	if e.ReceiptSource != "kalshi-create-order-receipt" || e.FilledQty <= 0 {
		return 0, 0, false
	}
	var evidence struct {
		Response struct {
			FillCount      string `json:"fill_count"`
			AverageFeePaid string `json:"average_fee_paid"`
		} `json:"response"`
	}
	if json.Unmarshal([]byte(e.EvidenceJSON), &evidence) != nil {
		return 0, 0, false
	}
	fillCount, err := strconv.ParseFloat(strings.TrimSpace(evidence.Response.FillCount), 64)
	if err != nil || !sameRiskFloat(fillCount, e.FilledQty) {
		return 0, 0, false
	}
	raw := strings.TrimSpace(evidence.Response.AverageFeePaid)
	feePC, err := strconv.ParseFloat(raw, 64)
	if err != nil || !finite(feePC) || feePC < 0 || !sameRiskFloat(feePC*e.FilledQty, e.FeeTotal) {
		return 0, 0, false
	}
	dot := strings.IndexByte(raw, '.')
	if dot < 0 || dot == len(raw)-1 || strings.ContainsAny(raw, "eE") {
		return 0, 0, false
	}
	digits := len(raw) - dot - 1
	if digits <= 0 || digits > 9 {
		return 0, 0, false
	}
	for _, ch := range raw[dot+1:] {
		if ch < '0' || ch > '9' {
			return 0, 0, false
		}
	}
	quantum := math.Pow10(-digits)
	return feePC * e.FilledQty, (feePC + quantum) * e.FilledQty, true
}

// kalshiScopedFeeRefinesCreateAverage permits one append-only precision upgrade, never a changed
// economic retry. The new receipt must be the authoritative order-scoped fills aggregation and
// must land inside the exact envelope encoded by the venue's earlier truncated average. Once that
// scoped receipt exists, any later price/fee disagreement is rejected by the ordinary immutable
// comparison below.
func kalshiScopedFeeRefinesCreateAverage(prior, next LivePendingRiskEvent) bool {
	if next.ReceiptSource != "kalshi-get-order+order-scoped-fills" &&
		next.ReceiptSource != "kalshi-create-ack+order-scoped-fills" {
		return false
	}
	if !equalRiskEventAttemptIdentity(prior, next) ||
		!sameRiskFloat(prior.FilledQty, next.FilledQty) ||
		!sameRiskFloat(prior.AveragePrice, next.AveragePrice) {
		return false
	}
	low, high, ok := kalshiCreateAverageFeeEnvelope(prior)
	return ok && next.FeeTotal+1e-9 >= low && next.FeeTotal < high+1e-9
}

func (s *Store) AppendLivePendingRiskEvent(ctx context.Context, e LivePendingRiskEvent) (bool, error) {
	e, err := normalizeRiskEvent(e)
	if err != nil {
		return false, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	current, ok, err := loadLivePendingRisk(ctx, tx, e.ReservationID)
	if err != nil || !ok {
		return false, firstRiskErr(err, errors.New("live pending-risk reservation not found"))
	}
	if e.EvidenceJSON, ok = bindLivePendingRiskOpportunityEvidence(
		e.EvidenceJSON, current.Intent.ExecutionShadowAttemptID); !ok {
		return false, errors.New("live pending-risk event evidence conflicts with immutable opportunity id")
	}
	for _, prior := range current.Events {
		if prior.EventType != e.EventType || prior.AttemptKey != e.AttemptKey {
			continue
		}
		// Pollers and push verifiers can observe the same resting order concurrently. Resting is a
		// one-time semantic fact bound to the exact attempt/order, not a mutable snapshot payload.
		if e.EventType == LivePendingRiskRestingVisible {
			if !equalRiskEventAttemptIdentity(prior, e) {
				return false, errors.New("resting receipt retry changed immutable attempt/order identity")
			}
			return false, tx.Commit()
		}
		// fill_seen is cumulative. Replaying an unchanged cumulative venue receipt is idempotent even
		// when a REST poll and WS verifier attach different diagnostic prose. The same quantity with
		// changed price/fee/identity is contradictory money truth and fails closed.
		if e.EventType == LivePendingRiskFillSeen && sameRiskFloat(prior.FilledQty, e.FilledQty) &&
			equalRiskEventAttemptIdentity(prior, e) && sameRiskFloat(prior.AveragePrice, e.AveragePrice) &&
			sameRiskFloat(prior.FeeTotal, e.FeeTotal) {
			return false, tx.Commit()
		}
		singleton := e.EventType != LivePendingRiskFillSeen && e.EventType != LivePendingRiskAmbiguous &&
			e.EventType != LivePendingRiskFrozen
		if singleton || equalRiskEventPayload(prior, e) {
			if !equalRiskEventPayload(prior, e) {
				return false, errors.New("live pending-risk event retry changed immutable receipt fields")
			}
			return false, tx.Commit()
		}
	}
	if e.EventType == LivePendingRiskFillSeen {
		// A fill is cumulative. Examine every receipt at this quantity before appending so an exact
		// replay remains idempotent even when an older low-precision create receipt precedes it.
		// The sole allowed mismatch is the narrowly proved create-average -> scoped-fill upgrade.
		for _, prior := range current.Events {
			if prior.EventType != e.EventType || prior.AttemptKey != e.AttemptKey ||
				!sameRiskFloat(prior.FilledQty, e.FilledQty) {
				continue
			}
			if !kalshiScopedFeeRefinesCreateAverage(prior, e) {
				return false, errors.New("cumulative fill retry changed immutable execution economics")
			}
		}
		// A matching create-average receipt reaches here only after proving the scoped precision
		// upgrade. Loaded-state validation below still enforces monotone cumulative fee.
	}
	if e.Sequence == 0 {
		e.Sequence = len(current.Events) + 1
	}
	if e.Sequence != len(current.Events)+1 {
		return false, errors.New("live pending-risk event sequence is not next")
	}
	test := current
	test.Events = append(append([]LivePendingRiskEvent(nil), current.Events...), e)
	if err := validateLoadedLivePendingRisk(test); err != nil {
		return false, err
	}
	var leg any
	if e.LegIndex != nil {
		leg = *e.LegIndex
	}
	account := ""
	if !e.AccountObserved.IsZero() {
		account = e.AccountObserved.UTC().Format(time.RFC3339Nano)
	}
	res, err := tx.ExecContext(ctx, `INSERT INTO live_pending_risk_events(
reservation_id,event_seq,observed_ts,event_type,leg_index,attempt_key,action,client_order_id,
order_id,filled_qty,average_price,fee_total,account_observed_ts,receipt_source,reason,evidence_json)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, e.ReservationID, e.Sequence,
		e.Observed.UTC().Format(time.RFC3339Nano), e.EventType, leg, e.AttemptKey, e.Action,
		e.ClientOrderID, e.OrderID, e.FilledQty, e.AveragePrice, e.FeeTotal, account,
		e.ReceiptSource, e.Reason, e.EvidenceJSON)
	if err != nil {
		return false, err
	}
	id, err := res.LastInsertId()
	if err != nil || id <= 0 {
		return false, firstRiskErr(err, errors.New("live pending-risk event insert lacked row id"))
	}
	if err = tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Store) LivePendingRiskByID(ctx context.Context, id string) (LivePendingRiskReservation, bool, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return LivePendingRiskReservation{}, false, err
	}
	defer tx.Rollback()
	v, ok, err := loadLivePendingRisk(ctx, tx, strings.TrimSpace(id))
	if err != nil {
		return v, ok, err
	}
	return v, ok, tx.Commit()
}

// LivePendingRisksByExecutionShadowAttempts returns the complete immutable risk rows whose
// evidence names one of the requested detector attempts. It includes released rows: boot repair
// may need a direct zero-fill acknowledgement after its money reservation was already closed.
func (s *Store) LivePendingRisksByExecutionShadowAttempts(ctx context.Context,
	attemptIDs []string) ([]LivePendingRiskReservation, error) {
	unique := make(map[string]struct{}, len(attemptIDs))
	for _, attemptID := range attemptIDs {
		if attemptID = strings.TrimSpace(attemptID); attemptID != "" {
			unique[attemptID] = struct{}{}
		}
	}
	if len(unique) == 0 {
		return []LivePendingRiskReservation{}, nil
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	ids := make(map[string]struct{})
	const queryChunk = 400
	batch := make([]string, 0, queryChunk)
	queryBatch := func() error {
		if len(batch) == 0 {
			return nil
		}
		placeholders := strings.TrimSuffix(strings.Repeat("?,", len(batch)), ",")
		args := make([]any, 0, len(batch)*2)
		for i := range batch {
			args = append(args, batch[i])
		}
		for i := range batch {
			args = append(args, batch[i])
		}
		rows, queryErr := tx.QueryContext(ctx, `SELECT reservation_id
FROM live_pending_risk_intents
WHERE execution_shadow_attempt_id IN (`+placeholders+`)
UNION
SELECT DISTINCT e.reservation_id
FROM live_pending_risk_events e
JOIN live_pending_risk_intents i ON i.reservation_id=e.reservation_id
WHERE i.execution_shadow_attempt_id=''
AND json_extract(e.evidence_json,'$.execution_shadow_attempt_id') IN (`+
			placeholders+`)`, args...)
		if queryErr != nil {
			return queryErr
		}
		for rows.Next() {
			var id string
			if scanErr := rows.Scan(&id); scanErr != nil {
				rows.Close()
				return scanErr
			}
			ids[id] = struct{}{}
		}
		if rowsErr := rows.Err(); rowsErr != nil {
			rows.Close()
			return rowsErr
		}
		return rows.Close()
	}
	for attemptID := range unique {
		batch = append(batch, attemptID)
		if len(batch) == cap(batch) {
			if err := queryBatch(); err != nil {
				return nil, err
			}
			batch = batch[:0]
		}
	}
	if err := queryBatch(); err != nil {
		return nil, err
	}
	ordered := make([]string, 0, len(ids))
	for id := range ids {
		ordered = append(ordered, id)
	}
	sort.Strings(ordered)
	out := make([]LivePendingRiskReservation, 0, len(ordered))
	for _, id := range ordered {
		row, ok, loadErr := loadLivePendingRisk(ctx, tx, id)
		if loadErr != nil || !ok {
			return nil, firstRiskErr(loadErr,
				errors.New("execution-attempt live pending-risk reservation is unreadable"))
		}
		out = append(out, row)
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return out, nil
}

// ListLivePendingRisksWithExecutionShadowAttempts is the restart bridge from real-money state back
// to the exact detector decision. Active/ambiguous reservations are returned first; released rows
// remain eligible so a crash after venue POST but before the handler's shadow receipt can still be
// repaired after account reconciliation closed the conservative risk overlay.
func (s *Store) ListLivePendingRisksWithExecutionShadowAttempts(ctx context.Context,
	limit int) ([]LivePendingRiskReservation, error) {
	if limit <= 0 || limit > 2000 {
		limit = 2000
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT i.reservation_id
FROM live_pending_risk_intents i
WHERE i.execution_shadow_attempt_id<>''
ORDER BY EXISTS(
 SELECT 1 FROM live_pending_risk_events e
 WHERE e.reservation_id=i.reservation_id AND e.event_type='released'
) ASC, i.created_ts DESC, i.reservation_id
LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0)
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	if err = rows.Close(); err != nil {
		return nil, err
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	out := make([]LivePendingRiskReservation, 0, len(ids))
	for _, id := range ids {
		row, found, loadErr := loadLivePendingRisk(ctx, tx, id)
		if loadErr != nil || !found {
			return nil, firstRiskErr(loadErr,
				errors.New("opportunity-linked live pending-risk reservation is unreadable"))
		}
		out = append(out, row)
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return out, nil
}

// ActiveLivePendingRiskProduct is the money-safety reader for one execution product.  The
// autocommit EXISTS probe is deliberate: this database uses _txlock=immediate, so opening even a
// read-only transaction can wait behind an unrelated startup writer.  Empty ledgers are the
// normal case and must not acquire that write lock.  A matching row still enters the immutable
// transactional loader below; corruption or an unreadable active reservation therefore continues
// to fail closed.
func (s *Store) ActiveLivePendingRiskProduct(ctx context.Context, product string) ([]LivePendingRiskReservation, error) {
	product = strings.ToLower(strings.TrimSpace(product))
	switch product {
	case "single", "combo", "staged", "reduce":
		return s.activeLivePendingRisk(ctx, product)
	default:
		return nil, errors.New("invalid live pending-risk product filter")
	}
}

func (s *Store) ActiveLivePendingRisk(ctx context.Context) ([]LivePendingRiskReservation, error) {
	return s.activeLivePendingRisk(ctx, "")
}

// HasActiveLivePendingRiskProduct is a presence-only safety probe.  It never opens the
// _txlock=immediate transaction required to load a complete immutable reservation, so callers can
// distinguish "the empty ledger could not be read yet" from "a durable money row exists and its
// full reconciliation failed".
func (s *Store) HasActiveLivePendingRiskProduct(ctx context.Context, product string) (bool, error) {
	product = strings.ToLower(strings.TrimSpace(product))
	switch product {
	case "single", "combo", "staged", "reduce":
	default:
		return false, errors.New("invalid live pending-risk product filter")
	}
	presenceSQL := `SELECT EXISTS(SELECT 1 FROM live_pending_risk_intents i
WHERE NOT EXISTS(SELECT 1 FROM live_pending_risk_events e
	 WHERE e.reservation_id=i.reservation_id AND e.event_type='released')
	AND i.product=? LIMIT 1)`
	var present int
	if err := s.db.QueryRowContext(ctx, presenceSQL, product).Scan(&present); err != nil {
		return false, err
	}
	return present != 0, nil
}

func (s *Store) activeLivePendingRisk(ctx context.Context, product string) ([]LivePendingRiskReservation, error) {
	if product != "" {
		present, err := s.HasActiveLivePendingRiskProduct(ctx, product)
		if err != nil {
			return nil, err
		}
		if !present {
			return []LivePendingRiskReservation{}, nil
		}
	} else {
		var present int
		if err := s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM live_pending_risk_intents i
WHERE NOT EXISTS(SELECT 1 FROM live_pending_risk_events e
 WHERE e.reservation_id=i.reservation_id AND e.event_type='released') LIMIT 1)`).Scan(&present); err != nil {
			return nil, err
		}
		if present == 0 {
			return []LivePendingRiskReservation{}, nil
		}
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	query := `SELECT i.reservation_id FROM live_pending_risk_intents i
WHERE NOT EXISTS(SELECT 1 FROM live_pending_risk_events e
	 WHERE e.reservation_id=i.reservation_id AND e.event_type='released')`
	args := []any{}
	if product != "" {
		query += ` AND i.product=?`
		args = append(args, product)
	}
	query += ` ORDER BY i.created_ts,i.reservation_id`
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	out := make([]LivePendingRiskReservation, 0, len(ids))
	for _, id := range ids {
		v, ok, err := loadLivePendingRisk(ctx, tx, id)
		if err != nil || !ok {
			return nil, firstRiskErr(err, errors.New("active live pending-risk reservation is unreadable"))
		}
		out = append(out, v)
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return out, nil
}
