package storage

// The generic inverse Paper book is a JSON projection, while its exact signal attribution lives
// in SQLite.  A durable placement journal bridges those two stores so a process crash can never
// leave a funded-looking JSON lot that has no independently named, side-specific signal fill.

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
)

const inversePaperPlacementSchema = `
CREATE TABLE IF NOT EXISTS inverse_paper_placement_journal (
 placement_id TEXT PRIMARY KEY CHECK(length(placement_id)=64),
 created_ts TEXT NOT NULL,
 updated_ts TEXT NOT NULL,
 signal_id INTEGER NOT NULL,
 signal_ts TEXT NOT NULL,
 decision_ts TEXT NOT NULL,
 platform TEXT NOT NULL CHECK(platform IN ('kalshi','polyus')),
 ticker TEXT NOT NULL,
 side TEXT NOT NULL CHECK(side IN ('YES','NO')),
 strategy_family TEXT NOT NULL CHECK(strategy_family LIKE 'invert:%' AND strategy_family NOT LIKE 'invert:invert:%'),
 fill_price REAL NOT NULL CHECK(fill_price>0 AND fill_price<1),
 fee_pc REAL NOT NULL CHECK(fee_pc>=0),
 contracts REAL NOT NULL CHECK(contracts>0),
 state TEXT NOT NULL CHECK(state IN ('prepared','ledger_persisted','committed','settled','rolled_back','quarantined')),
 detail TEXT NOT NULL DEFAULT '',
 FOREIGN KEY(signal_id) REFERENCES signal_log(id)
);
CREATE INDEX IF NOT EXISTS idx_inverse_paper_journal_state ON inverse_paper_placement_journal(state,updated_ts);
CREATE UNIQUE INDEX IF NOT EXISTS idx_inverse_paper_journal_active_signal
 ON inverse_paper_placement_journal(signal_id)
 WHERE state IN ('prepared','ledger_persisted','committed');
CREATE TRIGGER IF NOT EXISTS inverse_paper_journal_safe_update BEFORE UPDATE ON inverse_paper_placement_journal
WHEN NEW.placement_id<>OLD.placement_id OR NEW.created_ts<>OLD.created_ts OR
 NEW.signal_id<>OLD.signal_id OR NEW.signal_ts<>OLD.signal_ts OR NEW.decision_ts<>OLD.decision_ts OR
 NEW.platform<>OLD.platform OR NEW.ticker<>OLD.ticker OR NEW.side<>OLD.side OR
 NEW.strategy_family<>OLD.strategy_family OR NEW.fill_price<>OLD.fill_price OR
 NEW.fee_pc<>OLD.fee_pc OR NEW.contracts<>OLD.contracts OR NOT (
  (OLD.state='prepared' AND NEW.state IN ('ledger_persisted','rolled_back','quarantined')) OR
  (OLD.state='ledger_persisted' AND NEW.state IN ('committed','rolled_back','quarantined')) OR
  (OLD.state='committed' AND NEW.state IN ('settled','quarantined')) OR
  (OLD.state='settled' AND NEW.state='committed' AND NEW.detail='R148 PolyUS settlement provenance regrade') OR
  (OLD.state=NEW.state)
 )
BEGIN SELECT RAISE(ABORT,'unsafe inverse Paper placement journal update'); END;
CREATE TRIGGER IF NOT EXISTS inverse_paper_journal_no_delete BEFORE DELETE ON inverse_paper_placement_journal
BEGIN SELECT RAISE(ABORT,'immutable inverse Paper placement journal'); END;
`

func migrateInversePaperPlacementSchema(db *sql.DB) error {
	// Recreate the trigger so existing databases receive newly allowed, tightly-scoped repair
	// transitions instead of retaining the first IF-NOT-EXISTS definition forever.
	if _, err := db.Exec(`DROP TRIGGER IF EXISTS inverse_paper_journal_safe_update`); err != nil {
		return err
	}
	_, err := db.Exec(inversePaperPlacementSchema)
	return err
}

type InversePaperPlacement struct {
	PlacementID, SignalTS, DecisionTS, Platform, Ticker, Side, StrategyFamily, State, Detail string
	SignalID                                                                                 int64
	FillPrice, FeePC, Contracts                                                              float64
}

func validInversePaperPlacementInput(platform, ticker, side, family string, fillPrice, feePC, contracts float64) bool {
	platform = strings.ToLower(strings.TrimSpace(platform))
	side = strings.ToUpper(strings.TrimSpace(side))
	family = strings.ToLower(strings.TrimSpace(family))
	return (platform == "kalshi" || platform == "polyus") && strings.TrimSpace(ticker) != "" &&
		(side == "YES" || side == "NO") && strings.HasPrefix(family, "invert:") &&
		!strings.HasPrefix(family, "invert:invert:") && fillPrice > 0 && fillPrice < 1 &&
		!math.IsNaN(fillPrice) && !math.IsInf(fillPrice, 0) && feePC >= 0 &&
		!math.IsNaN(feePC) && !math.IsInf(feePC, 0) && contracts > 0 &&
		!math.IsNaN(contracts) && !math.IsInf(contracts, 0)
}

// PrepareInversePaperPlacement durably binds a prospective lot to the exact, still-unfilled
// signal row before the JSON projection is allowed to change.
func (s *Store) PrepareInversePaperPlacement(ctx context.Context, platform, ticker, side, family string,
	fillPrice, feePC, contracts float64, decision time.Time) (InversePaperPlacement, error) {
	var out InversePaperPlacement
	platform, side, family = strings.ToLower(strings.TrimSpace(platform)), strings.ToUpper(strings.TrimSpace(side)), strings.ToLower(strings.TrimSpace(family))
	if decision.IsZero() || !validInversePaperPlacementInput(platform, ticker, side, family, fillPrice, feePC, contracts) {
		return out, errors.New("invalid inverse Paper placement identity or economics")
	}
	decision = decision.UTC()
	decisionTS := decision.Format(time.RFC3339Nano)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return out, err
	}
	defer func() { _ = tx.Rollback() }()
	var signalTS string
	err = tx.QueryRowContext(ctx, `SELECT id,ts FROM signal_log
WHERE platform=? AND ticker=? AND UPPER(side)=? AND signal_type=? AND resolved=0 AND fill_price=0
 AND ABS((julianday(ts)-julianday(?))*86400.0)<=5.0
ORDER BY ABS((julianday(ts)-julianday(?))*86400.0),id DESC LIMIT 1`,
		platform, ticker, side, family, decisionTS, decisionTS).Scan(&out.SignalID, &signalTS)
	if errors.Is(err, sql.ErrNoRows) {
		return out, errors.New("exact inverse signal receipt is missing or stale")
	}
	if err != nil {
		return out, err
	}
	raw := fmt.Sprintf("%d|%s|%s|%s|%s|%s|%.12f|%.12f|%.12f", out.SignalID,
		platform, ticker, side, family, decisionTS, fillPrice, feePC, contracts)
	h := sha256.Sum256([]byte(raw))
	out = InversePaperPlacement{PlacementID: hex.EncodeToString(h[:]), SignalID: out.SignalID,
		SignalTS: signalTS, DecisionTS: decisionTS, Platform: platform, Ticker: ticker, Side: side,
		StrategyFamily: family, FillPrice: fillPrice, FeePC: feePC, Contracts: contracts, State: "prepared"}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	res, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO inverse_paper_placement_journal(
placement_id,created_ts,updated_ts,signal_id,signal_ts,decision_ts,platform,ticker,side,strategy_family,
fill_price,fee_pc,contracts,state,detail) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,'prepared','')`,
		out.PlacementID, now, now, out.SignalID, signalTS, decisionTS, platform, ticker, side, family,
		fillPrice, feePC, contracts)
	if err != nil {
		return InversePaperPlacement{}, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return InversePaperPlacement{}, err
	}
	if n != 1 {
		return InversePaperPlacement{}, errors.New("inverse Paper placement journal id already exists")
	}
	if err := tx.Commit(); err != nil {
		return InversePaperPlacement{}, err
	}
	return out, nil
}

func (s *Store) transitionInversePaperPlacement(ctx context.Context, id, from, to, detail string) (bool, error) {
	res, err := s.db.ExecContext(ctx, `UPDATE inverse_paper_placement_journal
SET state=?,updated_ts=?,detail=? WHERE placement_id=? AND state=?`, to,
		time.Now().UTC().Format(time.RFC3339Nano), strings.TrimSpace(detail), strings.TrimSpace(id), from)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n == 1, err
}

func (s *Store) MarkInversePaperLedgerPersisted(ctx context.Context, id string) (bool, error) {
	return s.transitionInversePaperPlacement(ctx, id, "prepared", "ledger_persisted", "JSON lot durably projected")
}

// CommitInversePaperPlacement atomically stamps the exact signal fill and commits journal
// authority. A JSON lot may settle only after this transaction succeeds.
func (s *Store) CommitInversePaperPlacement(ctx context.Context, id string) (bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()
	var p InversePaperPlacement
	err = tx.QueryRowContext(ctx, `SELECT placement_id,signal_id,signal_ts,decision_ts,platform,ticker,side,
strategy_family,fill_price,fee_pc,contracts,state,detail FROM inverse_paper_placement_journal
WHERE placement_id=?`, strings.TrimSpace(id)).Scan(&p.PlacementID, &p.SignalID, &p.SignalTS,
		&p.DecisionTS, &p.Platform, &p.Ticker, &p.Side, &p.StrategyFamily, &p.FillPrice,
		&p.FeePC, &p.Contracts, &p.State, &p.Detail)
	if errors.Is(err, sql.ErrNoRows) || p.State != "ledger_persisted" {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	res, err := tx.ExecContext(ctx, `UPDATE signal_log SET fill_price=?,fee_pc=?
WHERE id=? AND platform=? AND ticker=? AND UPPER(side)=? AND signal_type=?
 AND resolved=0 AND fill_price=0 AND ts=?`, p.FillPrice, p.FeePC, p.SignalID,
		p.Platform, p.Ticker, p.Side, p.StrategyFamily, p.SignalTS)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil || n != 1 {
		return false, err
	}
	res, err = tx.ExecContext(ctx, `UPDATE inverse_paper_placement_journal
SET state='committed',updated_ts=?,detail='exact signal fill and JSON lot committed'
WHERE placement_id=? AND state='ledger_persisted'`, time.Now().UTC().Format(time.RFC3339Nano), p.PlacementID)
	if err != nil {
		return false, err
	}
	n, err = res.RowsAffected()
	if err != nil || n != 1 {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Store) RollbackInversePaperPlacement(ctx context.Context, id, reason string) (bool, error) {
	if ok, err := s.transitionInversePaperPlacement(ctx, id, "prepared", "rolled_back", reason); ok || err != nil {
		return ok, err
	}
	return s.transitionInversePaperPlacement(ctx, id, "ledger_persisted", "rolled_back", reason)
}

func (s *Store) QuarantineInversePaperPlacement(ctx context.Context, id, reason string) (bool, error) {
	for _, from := range []string{"prepared", "ledger_persisted", "committed"} {
		if ok, err := s.transitionInversePaperPlacement(ctx, id, from, "quarantined", reason); ok || err != nil {
			return ok, err
		}
	}
	return false, nil
}

func (s *Store) SettleInversePaperPlacement(ctx context.Context, id string) (bool, error) {
	return s.transitionInversePaperPlacement(ctx, id, "committed", "settled", "JSON lot durably settled")
}

// ReopenInversePaperPlacementForSettlementRepair is the sole settled->committed transition. It
// requires that the journal's own PolyUS ticker has immutable R148 receipt evidence, so ordinary
// trading code cannot resurrect a legitimately settled lot.
func (s *Store) ReopenInversePaperPlacementForSettlementRepair(ctx context.Context, id string) (bool, error) {
	res, err := s.db.ExecContext(ctx, `UPDATE inverse_paper_placement_journal
SET state='committed',updated_ts=?,detail='R148 PolyUS settlement provenance regrade'
WHERE placement_id=? AND state='settled' AND platform='polyus'
AND EXISTS (SELECT 1 FROM polyus_settlement_quarantine q
 WHERE q.platform='polyus' AND q.ticker=inverse_paper_placement_journal.ticker)`,
		time.Now().UTC().Format(time.RFC3339Nano), strings.TrimSpace(id))
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n == 1, err
}

func (s *Store) InversePaperPlacement(ctx context.Context, id string) (InversePaperPlacement, bool, error) {
	var p InversePaperPlacement
	err := s.db.QueryRowContext(ctx, `SELECT placement_id,signal_id,signal_ts,decision_ts,platform,ticker,side,
strategy_family,fill_price,fee_pc,contracts,state,detail FROM inverse_paper_placement_journal
WHERE placement_id=?`, strings.TrimSpace(id)).Scan(&p.PlacementID, &p.SignalID, &p.SignalTS,
		&p.DecisionTS, &p.Platform, &p.Ticker, &p.Side, &p.StrategyFamily, &p.FillPrice,
		&p.FeePC, &p.Contracts, &p.State, &p.Detail)
	if errors.Is(err, sql.ErrNoRows) {
		return p, false, nil
	}
	return p, err == nil, err
}

func (s *Store) ActiveInversePaperPlacements(ctx context.Context) ([]InversePaperPlacement, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT placement_id,signal_id,signal_ts,decision_ts,platform,ticker,side,
strategy_family,fill_price,fee_pc,contracts,state,detail FROM inverse_paper_placement_journal
WHERE state IN ('prepared','ledger_persisted','committed') ORDER BY created_ts,placement_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []InversePaperPlacement
	for rows.Next() {
		var p InversePaperPlacement
		if err := rows.Scan(&p.PlacementID, &p.SignalID, &p.SignalTS, &p.DecisionTS, &p.Platform,
			&p.Ticker, &p.Side, &p.StrategyFamily, &p.FillPrice, &p.FeePC, &p.Contracts,
			&p.State, &p.Detail); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}
