package storage

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/objectiveidentity"
)

type CrossVenueMatchDecision struct {
	Observed                                                   time.Time
	CycleID, PairID, PairType                                  string
	LeftVenue, LeftInstrumentID, RightVenue, RightInstrumentID string
	FriendlyLeft, FriendlyRight                                string
	CertificateHash                                            string
	Left, Right                                                objectiveidentity.Contract
	Verdict                                                    objectiveidentity.Verdict
	Funded, PaperAuthority, LiveAuthority                      bool
}

func (s *Store) InsertCrossVenueMatchDecision(ctx context.Context, in CrossVenueMatchDecision) (bool, error) {
	if in.Observed.IsZero() {
		in.Observed = time.Now().UTC()
	}
	in.CycleID, in.PairID, in.PairType = strings.TrimSpace(in.CycleID), strings.TrimSpace(in.PairID), strings.TrimSpace(in.PairType)
	in.LeftVenue, in.RightVenue = normalizeRuleVenue(in.LeftVenue), normalizeRuleVenue(in.RightVenue)
	in.LeftInstrumentID, in.RightInstrumentID = strings.TrimSpace(in.LeftInstrumentID), strings.TrimSpace(in.RightInstrumentID)
	_, _, _, _, expectedID, ok := canonicalRulePair(in.LeftVenue, in.LeftInstrumentID, in.RightVenue, in.RightInstrumentID)
	if !ok || expectedID != in.PairID || rulePairType(in.LeftVenue, in.RightVenue) != in.PairType ||
		in.CycleID == "" || in.Verdict.TaxonomyVersion != objectiveidentity.TaxonomyVersion || in.Verdict.RiskTier == "" ||
		len(in.Verdict.EvidenceHash) != 64 || in.Verdict.Compatible != strings.HasPrefix(in.Verdict.ReasonCode, "ACCEPT_") ||
		(in.Verdict.LockEligible && !in.Verdict.Compatible) ||
		(!strings.HasPrefix(in.Verdict.ReasonCode, "ACCEPT_") && !strings.HasPrefix(in.Verdict.ReasonCode, "REJECT_")) ||
		in.Funded || in.PaperAuthority || in.LiveAuthority {
		return false, errors.New("invalid or authoritative cross-venue match decision")
	}
	leftJSON, err := json.Marshal(in.Verdict.NormalizedLeft)
	if err != nil {
		return false, err
	}
	rightJSON, err := json.Marshal(in.Verdict.NormalizedRight)
	if err != nil {
		return false, err
	}
	res, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO research_crossvenue_match_decisions(
observed_ts,cycle_id,pair_id,pair_type,left_venue,left_instrument_id,right_venue,right_instrument_id,
friendly_left,friendly_right,genre,market_type,orientation,accepted,lock_eligible,reason_code,lock_reason_code,risk_tier,taxonomy_version,
predicate_hash,rules_hash,evidence_hash,left_contract_json,right_contract_json,certificate_hash,
funded,paper_authority,live_authority) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0,0,0)`,
		in.Observed.UTC().Format(time.RFC3339Nano), in.CycleID, in.PairID, in.PairType,
		in.LeftVenue, in.LeftInstrumentID, in.RightVenue, in.RightInstrumentID,
		strings.TrimSpace(in.FriendlyLeft), strings.TrimSpace(in.FriendlyRight),
		in.Verdict.NormalizedLeft.Genre, in.Verdict.NormalizedLeft.MarketType, in.Verdict.Orientation,
		boolInt(in.Verdict.Compatible), boolInt(in.Verdict.LockEligible), in.Verdict.ReasonCode,
		in.Verdict.LockReasonCode, in.Verdict.RiskTier, in.Verdict.TaxonomyVersion,
		in.Verdict.PredicateHash, in.Verdict.RulesHash, in.Verdict.EvidenceHash,
		string(leftJSON), string(rightJSON), strings.TrimSpace(in.CertificateHash))
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

// NextRulePairCertificateVersion is used only after a complete objective-equivalence verdict.
// RegisterRulePairCertificate still owns immutable drift checks and all zero-authority guards.
func (s *Store) NextRulePairCertificateVersion(ctx context.Context, pairID string) (int, error) {
	var next int
	err := s.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(version),0)+1 FROM research_rule_pair_certificates WHERE pair_id=?`, strings.TrimSpace(pairID)).Scan(&next)
	return next, err
}

type CrossVenueDecisionView struct {
	Observed, PairID, PairType, LeftVenue, LeftInstrumentID                               string
	RightVenue, RightInstrumentID, FriendlyLeft, FriendlyRight                            string
	Genre, MarketType, Orientation, ReasonCode, LockReasonCode, RiskTier, CertificateHash string
	Accepted, LockEligible                                                                bool
}

func (s *Store) CrossVenueDecisionReport(ctx context.Context, limit int) (map[string]any, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	counts := map[string]int{}
	var total, accepted, lockEligible int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(SUM(accepted),0),COALESCE(SUM(lock_eligible),0)
FROM research_crossvenue_match_decisions`).Scan(&total, &accepted, &lockEligible); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT reason_code,COUNT(*) FROM research_crossvenue_match_decisions GROUP BY reason_code ORDER BY reason_code`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var code string
		var n int
		if err := rows.Scan(&code, &n); err != nil {
			_ = rows.Close()
			return nil, err
		}
		counts[code] = n
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	recentRows, err := s.db.QueryContext(ctx, `SELECT observed_ts,pair_id,pair_type,left_venue,left_instrument_id,
right_venue,right_instrument_id,friendly_left,friendly_right,genre,market_type,orientation,accepted,
lock_eligible,reason_code,lock_reason_code,risk_tier,certificate_hash
FROM research_crossvenue_match_decisions ORDER BY id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer recentRows.Close()
	var recent []CrossVenueDecisionView
	for recentRows.Next() {
		var v CrossVenueDecisionView
		var accepted, lockEligible int
		if err := recentRows.Scan(&v.Observed, &v.PairID, &v.PairType, &v.LeftVenue, &v.LeftInstrumentID,
			&v.RightVenue, &v.RightInstrumentID, &v.FriendlyLeft, &v.FriendlyRight, &v.Genre,
			&v.MarketType, &v.Orientation, &accepted, &lockEligible, &v.ReasonCode,
			&v.LockReasonCode, &v.RiskTier, &v.CertificateHash); err != nil {
			return nil, err
		}
		v.Accepted = accepted != 0
		v.LockEligible = lockEligible != 0
		recent = append(recent, v)
	}
	return map[string]any{"taxonomy_version": objectiveidentity.TaxonomyVersion, "reason_counts": counts,
		"total": total, "accepted_directional": accepted, "lock_eligible": lockEligible,
		"recent": recent, "friendly_names_display_only": true, "funded": false,
		"paper_authority": false, "live_authority": false}, recentRows.Err()
}
