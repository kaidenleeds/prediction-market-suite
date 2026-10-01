package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const polyUSMLSettlementQuarantineDDL = `
CREATE TABLE IF NOT EXISTS polyus_ml_relation_outcome_quarantine (
 receipt_id TEXT PRIMARY KEY,
 outcome_id INTEGER NOT NULL,
 outcome_status TEXT NOT NULL,
 settled_ts TEXT NOT NULL,
 pnl_dollars REAL NOT NULL,
 result_source TEXT NOT NULL,
 created_ts TEXT NOT NULL,
 reason TEXT NOT NULL,
 quarantined_at TEXT NOT NULL
);
CREATE TRIGGER IF NOT EXISTS polyus_ml_relation_outcome_quarantine_no_update
BEFORE UPDATE ON polyus_ml_relation_outcome_quarantine
BEGIN SELECT RAISE(ABORT,'immutable PolyUS ML outcome quarantine'); END;
CREATE TRIGGER IF NOT EXISTS polyus_ml_relation_outcome_quarantine_no_delete
BEFORE DELETE ON polyus_ml_relation_outcome_quarantine
BEGIN SELECT RAISE(ABORT,'immutable PolyUS ML outcome quarantine'); END;`

type polyUSMLSettlementQuarantineFile struct {
	Reason       string           `json:"reason"`
	SourceSHA256 string           `json:"source_sha256"`
	CreatedAt    string           `json:"created_at"`
	Rows         []map[string]any `json:"rows"`
}

func jsonFloat(m map[string]any, key string) float64 {
	v, _ := m[key].(float64)
	return v
}

func jsonMap(m map[string]any, key string) map[string]any {
	v, _ := m[key].(map[string]any)
	if v == nil {
		v = map[string]any{}
		m[key] = v
	}
	return v
}

func adjustJSONNumber(m map[string]any, key string, delta float64) {
	m[key] = jsonFloat(m, key) + delta
}

func cloneJSONMap(m map[string]any) map[string]any {
	raw, _ := json.Marshal(m)
	var out map[string]any
	_ = json.Unmarshal(raw, &out)
	return out
}

func mlSettlementQuarantineKey(row map[string]any) string {
	// The funded relation receipt is the strongest lot identity. Legacy lots may lack it, so the
	// fallback includes every stable execution coordinate; ticker+close time alone can collide for
	// simultaneous YES/NO or repeated lots and would silently discard immutable evidence.
	if receipt, _ := row["relation_receipt_id"].(string); strings.TrimSpace(receipt) != "" {
		receipt = strings.TrimSpace(receipt)
		return "receipt:" + receipt
	}
	parts := []string{"lot", strings.ToLower(strings.TrimSpace(fmt.Sprint(row["platform"]))),
		strings.TrimSpace(fmt.Sprint(row["ticker"])), strings.ToUpper(strings.TrimSpace(fmt.Sprint(row["side"]))),
		fmt.Sprint(row["opened"]), fmt.Sprint(row["closed_ts"]), fmt.Sprint(row["contracts"]),
		fmt.Sprint(row["price"]), fmt.Sprint(row["settlement_payout"])}
	return strings.Join(parts, "|")
}

func atomicWriteJSON(path string, value any) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func reverseMLTerminalCounters(book, row map[string]any, resetUnix float64) {
	life := jsonMap(book, "lifetime")
	pnl, contracts, closedAt := jsonFloat(row, "pnl"), jsonFloat(row, "contracts"), jsonFloat(row, "closed_ts")
	won := jsonFloat(row, "won")
	adjustJSONNumber(life, "net", -pnl)
	adjustJSONNumber(life, "closed", -1)
	adjustJSONNumber(life, "wins", -won)
	for field, delta := range map[string]float64{"venue_net": -pnl, "venue_closed": -1, "venue_contracts": -contracts} {
		v := jsonMap(life, field)
		adjustJSONNumber(v, "polyus", delta)
	}
	if resetUnix > 0 && closedAt > 0 && closedAt < resetUnix {
		adjustJSONNumber(life, "net_base", -pnl)
		adjustJSONNumber(life, "closed_base", -1)
		adjustJSONNumber(life, "wins_base", -won)
		for field, delta := range map[string]float64{"venue_net_base": -pnl, "venue_closed_base": -1, "venue_contracts_base": -contracts} {
			v := jsonMap(life, field)
			adjustJSONNumber(v, "polyus", delta)
		}
	}
}

func recomputeMLBookCounters(book map[string]any) {
	life, stats := jsonMap(book, "lifetime"), jsonMap(book, "stats")
	bank := jsonFloat(book, "bank0")
	if bank <= 0 {
		bank = jsonFloat(stats, "bank0")
	}
	net := jsonFloat(life, "net") - jsonFloat(life, "net_base")
	closed := jsonFloat(life, "closed") - jsonFloat(life, "closed_base")
	wins := jsonFloat(life, "wins") - jsonFloat(life, "wins_base")
	stats["net"], stats["closed"], stats["wins"] = net, closed, wins
	if closed > 0 {
		stats["win_rate"] = wins / closed
	} else {
		stats["win_rate"] = 0.0
	}
	stats["bank0"], stats["equity"] = bank, bank+net
	if bank > 0 {
		stats["roi"] = net / bank
	}
	open, _ := book["open"].([]any)
	stats["open_n"] = float64(len(open))
	venueNet, venueNetBase := jsonMap(life, "venue_net"), jsonMap(life, "venue_net_base")
	venueClosed, venueClosedBase := jsonMap(life, "venue_closed"), jsonMap(life, "venue_closed_base")
	venueContracts, venueContractsBase := jsonMap(life, "venue_contracts"), jsonMap(life, "venue_contracts_base")
	sleeves := jsonMap(book, "venue_sleeves")
	for _, venue := range []string{"kalshi", "polyus"} {
		sleeve := jsonMap(sleeves, venue)
		grant := jsonFloat(sleeve, "grant")
		if grant <= 0 && bank > 0 {
			grant = bank / 2
		}
		vnet := jsonFloat(venueNet, venue) - jsonFloat(venueNetBase, venue)
		vclosed := jsonFloat(venueClosed, venue) - jsonFloat(venueClosedBase, venue)
		vcontracts := jsonFloat(venueContracts, venue) - jsonFloat(venueContractsBase, venue)
		deployed, openN := 0.0, 0
		for _, raw := range open {
			lot, _ := raw.(map[string]any)
			if strings.EqualFold(strings.TrimSpace(fmt.Sprint(lot["platform"])), venue) {
				deployed += jsonFloat(lot, "contracts")*jsonFloat(lot, "price") + jsonFloat(lot, "fee")
				openN++
			}
		}
		equity := grant + vnet
		sleeve["grant"], sleeve["net"], sleeve["equity"], sleeve["sizing_balance"] = grant, vnet, equity, equity
		sleeve["deployed"], sleeve["reserved"], sleeve["available"] = deployed, 0.0, equity-deployed
		sleeve["open"], sleeve["closed"], sleeve["contracts"] = float64(openN), vclosed, vcontracts
	}
	stats["venue_sleeves"] = sleeves
	book["stats"], book["venue_sleeves"], book["equity"] = stats, sleeves, bank+net
	book["updated"] = float64(time.Now().Unix())
}

func archiveR148MLCleanEpoch(ctx context.Context, db *sql.DB, path string, raw []byte) (string, error) {
	hash, _, err := quarantinePolyUSFundedJSONEpoch(ctx, db, "ml_paper.json", raw)
	if err != nil {
		return "", err
	}
	archive := path + ".r148-pre-clean-epoch-" + hash[:12] + ".json"
	if prior, readErr := os.ReadFile(archive); readErr == nil {
		priorHash := sha256.Sum256(prior)
		if hex.EncodeToString(priorHash[:]) != hash {
			return "", errors.New("ML clean-epoch archive hash mismatch")
		}
	} else if errors.Is(readErr, os.ErrNotExist) {
		tmp := archive + ".tmp"
		if err := os.WriteFile(tmp, raw, 0o400); err != nil {
			return "", err
		}
		if err := os.Rename(tmp, archive); err != nil {
			_ = os.Remove(tmp)
			return "", err
		}
	} else {
		return "", readErr
	}
	return hash, nil
}

func resetR148MLCleanEpoch(book map[string]any, archiveSHA string) {
	life := jsonMap(book, "lifetime")
	for _, field := range []string{"net", "net_base", "closed", "closed_base", "wins", "wins_base"} {
		life[field] = 0.0
	}
	for _, field := range []string{"venue_net", "venue_net_base", "venue_closed", "venue_closed_base",
		"venue_contracts", "venue_contracts_base"} {
		m := jsonMap(life, field)
		m["kalshi"], m["polyus"] = 0.0, 0.0
	}
	book["closed"] = []any{}
	book["settlement_epoch"] = "polyus-final-v2-clean-epoch-1"
	book["settlement_epoch_at"] = nowRFC()
	book["settlement_legacy_archive_sha256"] = archiveSHA
	book["settlement_legacy_excluded"] = true
	delete(book, "settlement_repair_blocked")
	delete(book, "settlement_repair_blocked_at")
}

func quarantineMLRelationOutcomes(db *sql.DB, rows []map[string]any) error {
	if _, err := db.Exec(polyUSMLSettlementQuarantineDDL); err != nil {
		return err
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`DROP TRIGGER IF EXISTS funded_relation_outcomes_no_delete`); err != nil {
		return err
	}
	for _, row := range rows {
		receipt := strings.TrimSpace(fmt.Sprint(row["relation_receipt_id"]))
		if len(receipt) != 64 {
			continue
		}
		result, err := tx.Exec(`INSERT OR IGNORE INTO polyus_ml_relation_outcome_quarantine(
receipt_id,outcome_id,outcome_status,settled_ts,pnl_dollars,result_source,created_ts,reason,quarantined_at)
SELECT receipt_id,outcome_id,outcome_status,settled_ts,pnl_dollars,result_source,created_ts,?,?
			FROM funded_relation_outcomes WHERE receipt_id=?`, polyUSFractionalSettlementReason, nowRFC(), receipt)
		if err != nil {
			return err
		}
		// Delete only the exact false outcome that was newly copied into immutable quarantine in
		// this transaction. On later boots INSERT OR IGNORE affects zero rows; a corrected final
		// outcome may then exist under the same receipt id and must never be deleted again.
		inserted, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if inserted == 0 {
			continue
		}
		if _, err := tx.Exec(`DELETE FROM funded_relation_outcomes WHERE receipt_id=?`, receipt); err != nil {
			return err
		}
	}
	if _, err := tx.Exec(`CREATE TRIGGER IF NOT EXISTS funded_relation_outcomes_no_delete BEFORE DELETE ON funded_relation_outcomes
BEGIN SELECT RAISE(ABORT,'immutable funded relation outcome'); END`); err != nil {
		return err
	}
	return tx.Commit()
}

// repairPolyUSMLFractionalSettlements runs before the sidecar starts, so no second JSON writer is
// active. The exact bad rows are preserved in a quarantine file, terminal P&L is reversed, and the
// lots are reopened for a later authoritative 0/1 settlement. The linked relation outcome is also
// copied to immutable quarantine before removal, allowing the original receipt to settle correctly
// later instead of leaving false dependent/independent P&L in the briefing.
func repairPolyUSMLFractionalSettlements(db *sql.DB, dataDir string) error {
	path := filepath.Join(dataDir, "ml_paper.json")
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var book map[string]any
	if err := json.Unmarshal(raw, &book); err != nil {
		return fmt.Errorf("decode ML paper for settlement repair: %w", err)
	}
	repairStates, err := polyUSSettlementRepairStates(context.Background(), db)
	if err != nil {
		return fmt.Errorf("load PolyUS settlement repair states: %w", err)
	}
	closed, _ := book["closed"].([]any)
	open, _ := book["open"].([]any)
	visiblePolyUSClosed := 0
	for _, rawLot := range closed {
		if lot, ok := rawLot.(map[string]any); ok &&
			strings.EqualFold(strings.TrimSpace(fmt.Sprint(lot["platform"])), "polyus") {
			visiblePolyUSClosed++
		}
	}
	lifetimePolyUSClosed := jsonFloat(jsonMap(jsonMap(book, "lifetime"), "venue_closed"), "polyus")
	legacyBlocked := strings.TrimSpace(fmt.Sprint(book["settlement_repair_blocked"]))
	if legacyBlocked == "<nil>" {
		legacyBlocked = ""
	}
	needsCleanEpoch := len(repairStates) > 0 &&
		strings.TrimSpace(fmt.Sprint(book["settlement_epoch"])) != "polyus-final-v2-clean-epoch-1" &&
		(lifetimePolyUSClosed > float64(visiblePolyUSClosed)+1e-9 || legacyBlocked != "")
	cleanEpochSHA := ""
	if needsCleanEpoch {
		cleanEpochSHA, err = archiveR148MLCleanEpoch(context.Background(), db, path, raw)
		if err != nil {
			return fmt.Errorf("archive ML clean epoch: %w", err)
		}
	}
	var resetUnix float64
	if reset, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(fmt.Sprint(book["reset_at"]))); err == nil {
		resetUnix = float64(reset.Unix())
	}
	seenOpen := map[string]bool{}
	for _, rawLot := range open {
		lot, _ := rawLot.(map[string]any)
		seenOpen[fmt.Sprint(lot["ticker"])+"|"+fmt.Sprint(lot["side"])+"|"+fmt.Sprint(lot["opened"])] = true
	}
	kept := make([]any, 0, len(closed))
	bad := make([]map[string]any, 0, 2)
	for _, rawLot := range closed {
		lot, ok := rawLot.(map[string]any)
		payout := jsonFloat(lot, "settlement_payout")
		ticker := strings.TrimSpace(fmt.Sprint(lot["ticker"]))
		closedAt := time.Time{}
		if closedUnix := jsonFloat(lot, "closed_ts"); closedUnix > 0 {
			closedAt = time.Unix(int64(closedUnix), 0).UTC()
		}
		state, legacyTicker := repairStates[ticker]
		legacyClosure := legacyTicker && state.ClosureNeedsRepair(closedAt)
		fractionalClosure := payout > 0 && payout < 1
		if !ok || !strings.EqualFold(strings.TrimSpace(fmt.Sprint(lot["platform"])), "polyus") ||
			strings.TrimSpace(fmt.Sprint(lot["terminal_reason"])) != "settlement" ||
			(!fractionalClosure && !legacyClosure) {
			kept = append(kept, rawLot)
			continue
		}
		bad = append(bad, cloneJSONMap(lot))
		reverseMLTerminalCounters(book, lot, resetUnix)
		key := fmt.Sprint(lot["ticker"]) + "|" + fmt.Sprint(lot["side"]) + "|" + fmt.Sprint(lot["opened"])
		for _, field := range []string{"won", "pnl", "closed_ts", "realized_cents_per_share",
			"settlement_payout", "final_result", "close_market_price", "clv", "terminal_reason", "relation_outcome_synced"} {
			delete(lot, field)
		}
		lot["settlement_regrade_reason"] = polyUSUntrustedSettlementReason
		if !seenOpen[key] {
			open = append(open, lot)
			seenOpen[key] = true
		}
	}
	qPath := filepath.Join(dataDir, "ml_paper.r148-settlement-quarantine.json")
	var q polyUSMLSettlementQuarantineFile
	if qRaw, qErr := os.ReadFile(qPath); qErr == nil {
		_ = json.Unmarshal(qRaw, &q)
	}
	if len(bad) > 0 {
		h := sha256.Sum256(raw)
		q.Reason, q.SourceSHA256 = polyUSUntrustedSettlementReason, hex.EncodeToString(h[:])
		if q.CreatedAt == "" {
			q.CreatedAt = nowRFC()
		}
		known := map[string]bool{}
		for _, row := range q.Rows {
			known[mlSettlementQuarantineKey(row)] = true
		}
		for _, row := range bad {
			key := mlSettlementQuarantineKey(row)
			if !known[key] {
				q.Rows = append(q.Rows, row)
				known[key] = true
			}
		}
		if err := atomicWriteJSON(qPath, q); err != nil {
			return fmt.Errorf("write ML settlement quarantine: %w", err)
		}
		book["closed"], book["open"] = kept, open
		recomputeMLBookCounters(book)
	}
	if needsCleanEpoch {
		book["open"] = open
		resetR148MLCleanEpoch(book, cleanEpochSHA)
		recomputeMLBookCounters(book)
	}
	if len(bad) > 0 || needsCleanEpoch {
		if err := atomicWriteJSON(path, book); err != nil {
			return fmt.Errorf("write repaired/blocked ML paper: %w", err)
		}
		_ = os.Remove(filepath.Join(dataDir, "ml_accuracy.json"))
	}
	// Always replay the durable quarantine file into SQLite. This closes the small crash window
	// between the two atomic file writes and the database transaction.
	if len(q.Rows) > 0 {
		return quarantineMLRelationOutcomes(db, q.Rows)
	}
	return nil
}
