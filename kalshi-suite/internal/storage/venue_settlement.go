package storage

// Durable, venue-scoped terminal truth. Signal rows remain the feature/training ledger, but they
// are not a safe position ledger: a routed destination contract may have no signal row at all.
// Funded books therefore settle from this receipt first and use signal_log only as a legacy
// fallback. Final outcomes are immutable; the same receipt may be replayed after any restart,
// while a contradictory value is surfaced instead of silently rewriting P&L.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
)

// PolyUSFinalBookSettlementV2 and PolyUSFinalEndpointSettlementV2 are the only
// durable PolyUS settlement provenances accepted by the storage boundary.  A
// binary-looking 0/1 is not sufficient: older BookFull snapshots exposed
// preliminary marks that could also happen to equal an endpoint.
const (
	PolyUSFinalBookSettlementV2     = "polyus-final-v2:event-tier-1-book"
	PolyUSFinalEndpointSettlementV2 = "polyus-final-v2:dedicated-settlement-endpoint"
)

func TrustedPolyUSFinalSettlementSource(sourceArtifact string) bool {
	switch strings.TrimSpace(sourceArtifact) {
	case PolyUSFinalBookSettlementV2, PolyUSFinalEndpointSettlementV2:
		return true
	default:
		return false
	}
}

func normalizeSettlementVenue(platform string) string {
	platform = strings.ToLower(strings.TrimSpace(platform))
	if platform == "" {
		return "kalshi"
	}
	return platform
}

func authoritativeVenueSettlementValue(platform string, yesValue float64) bool {
	if yesValue < 0 || yesValue > 1 || math.IsNaN(yesValue) || math.IsInf(yesValue, 0) {
		return false
	}
	// PolyUS contracts are binary. Fractional settlementPx values are preliminary/intraday
	// marks, not final payouts; only the strict public parser may turn final EVENT_TIER_1 0/1
	// receipts into durable settlement truth.
	return normalizeSettlementVenue(platform) != "polyus" || yesValue == 0 || yesValue == 1
}

func authoritativeVenueSettlementReceipt(platform string, yesValue float64, sourceArtifact string) bool {
	if !authoritativeVenueSettlementValue(platform, yesValue) {
		return false
	}
	return normalizeSettlementVenue(platform) != "polyus" ||
		TrustedPolyUSFinalSettlementSource(sourceArtifact)
}

// RecordVenueSettlement persists one exact venue+ticker YES payout. First final receipt wins;
// replaying the same value is a successful no-op and a conflicting value fails loudly.
func (s *Store) RecordVenueSettlement(ctx context.Context, platform, ticker string, yesValue float64,
	resolvedAt time.Time, sourceArtifact string) (bool, error) {
	platform = normalizeSettlementVenue(platform)
	ticker = strings.TrimSpace(ticker)
	sourceArtifact = strings.TrimSpace(sourceArtifact)
	if (platform != "kalshi" && platform != "polyus" && platform != "polymarket") || ticker == "" ||
		sourceArtifact == "" || !authoritativeVenueSettlementReceipt(platform, yesValue, sourceArtifact) {
		return false, errors.New("invalid venue settlement receipt")
	}
	if resolvedAt.IsZero() {
		resolvedAt = time.Now().UTC()
	}
	res, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO venue_settlements(
platform,ticker,yes_value,resolved_at,source_artifact) VALUES(?,?,?,?,?)`,
		platform, ticker, yesValue, resolvedAt.UTC().Format(time.RFC3339Nano), sourceArtifact)
	if err != nil {
		return false, err
	}
	if n, _ := res.RowsAffected(); n > 0 {
		return true, nil
	}
	var prior float64
	if err := s.db.QueryRowContext(ctx, `SELECT yes_value FROM venue_settlements
WHERE platform=? AND ticker=?`, platform, ticker).Scan(&prior); err != nil {
		return false, err
	}
	if math.Abs(prior-yesValue) > 1e-9 {
		return false, fmt.Errorf("conflicting final settlement for %s %s: durable=%g incoming=%g",
			platform, ticker, prior, yesValue)
	}
	return false, nil
}

// ResolvedYesForVenueWithError reads the durable receipt first and preserves the distinction
// between "not settled" and "the database could not answer." Callers which build durable source
// receipts must use this form: treating a timeout as an unresolved market can make a previously
// graded row disappear and manufacture a false source change.
func (s *Store) ResolvedYesForVenueWithError(ctx context.Context, platform, ticker string) (float64, bool, error) {
	platform = normalizeSettlementVenue(platform)
	ticker = strings.TrimSpace(ticker)
	if ticker == "" {
		return 0, false, nil
	}
	var yesValue float64
	var sourceArtifact string
	err := s.db.QueryRowContext(ctx, `SELECT yes_value,source_artifact FROM venue_settlements
WHERE platform=? AND ticker=?`, platform, ticker).Scan(&yesValue, &sourceArtifact)
	if err == nil && authoritativeVenueSettlementReceipt(platform, yesValue, sourceArtifact) {
		return yesValue, true, nil
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return 0, false, err
	}
	// A legacy PolyUS signal row carries no final-source provenance. Current
	// writers always persist the trusted venue receipt before mirroring a grade,
	// so falling back here would re-authorize the exact legacy rows R148 is
	// quarantining.
	if platform == "polyus" {
		return 0, false, nil
	}
	var sv sql.NullFloat64
	var won sql.NullInt64
	var side sql.NullString
	err = s.db.QueryRowContext(ctx, `SELECT settle_val,won,side FROM signal_log
WHERE platform=? AND ticker=? AND resolved=1 ORDER BY resolved_at DESC,id DESC LIMIT 1`,
		platform, ticker).Scan(&sv, &won, &side)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, false, nil
		}
		return 0, false, err
	}
	if sv.Valid && authoritativeVenueSettlementValue(platform, sv.Float64) {
		return sv.Float64, true, nil
	}
	if !won.Valid || !side.Valid {
		return 0, false, nil
	}
	switch strings.ToUpper(strings.TrimSpace(side.String)) {
	case "YES", "UP":
		if won.Int64 >= 1 {
			return 1, true, nil
		}
		return 0, true, nil
	case "NO", "DOWN":
		if won.Int64 >= 1 {
			return 0, true, nil
		}
		return 1, true, nil
	default:
		return 0, false, nil
	}
}

// ResolvedYesForVenue is the compatibility form used by non-receipt consumers. It deliberately
// retains the historical two-result contract; money/report source receipts use the error-aware
// form above so transient database failures cannot be mistaken for terminal absence.
func (s *Store) ResolvedYesForVenue(ctx context.Context, platform, ticker string) (float64, bool) {
	yes, resolved, _ := s.ResolvedYesForVenueWithError(ctx, platform, ticker)
	return yes, resolved
}

// ResolveSignalsForVenue records terminal truth even when there is no signal row, then mirrors it
// into only that venue's feature/maker ledgers. This is the restart-safe replacement for the
// legacy ticker-only ResolveSignals at every call site that knows the venue.
func (s *Store) ResolveSignalsForVenue(ctx context.Context, platform, ticker string, yesValue float64,
	sourceArtifact string) error {
	platform = normalizeSettlementVenue(platform)
	if _, err := s.RecordVenueSettlement(ctx, platform, ticker, yesValue, time.Now().UTC(), sourceArtifact); err != nil {
		return err
	}
	yesWon := 0
	if yesValue >= 0.5 {
		yesWon = 1
	}
	stamp := nowRFC()
	if _, err := s.db.ExecContext(ctx, `UPDATE signal_log SET resolved=1,resolved_at=?,settle_val=?,
 won=CASE WHEN (UPPER(TRIM(side))='YES' AND ?=1) OR
               (UPPER(TRIM(side))='NO' AND ?=0) THEN 1 ELSE 0 END
WHERE platform=? AND ticker=? AND resolved=0 AND
 (platform NOT IN ('kalshi','polyus') OR UPPER(TRIM(side)) IN ('YES','NO'))`,
		stamp, yesValue, yesWon, yesWon, platform, ticker); err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE signal_log SET resolved=-1,resolved_at=?
WHERE platform=? AND ticker=? AND resolved=0 AND platform IN ('kalshi','polyus')
 AND UPPER(TRIM(side)) NOT IN ('YES','NO')`, stamp, platform, ticker); err != nil {
		return fmt.Errorf("quarantine leg: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE maker_fill_stats SET settle_val=?,settle_mirrored_at=?,
 outcome_won=CASE WHEN (UPPER(TRIM(side))='YES' AND ?=1) OR
                       (UPPER(TRIM(side))='NO' AND ?=0) THEN 1 ELSE 0 END
WHERE platform=? AND ticker=? AND settle_val IS NULL`, yesValue, stamp, yesWon, yesWon,
		platform, ticker); err != nil {
		return fmt.Errorf("would-gate grading leg: %w", err)
	}
	if platform == "kalshi" {
		if err := s.ResolveSubcentGolfTicker(ctx, ticker, yesValue); err != nil {
			return fmt.Errorf("subcent-golf grading leg: %w", err)
		}
	}
	return nil
}
