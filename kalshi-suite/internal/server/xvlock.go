package server

// R125 XVLOCK — the operator's LOCK idea, log-only: buy YES on venue A at ask_A and NO on venue B
// at ask_B; when ask_A + ask_B < $1 − fees(both), settlement pays $1 on exactly one leg and the
// margin is locked REGARDLESS of outcome — IF the two venues settle by equivalent rules. That
// "if" is the whole risk, so every graded lock is scored from EACH venue's OWN settlement: a lock
// where both legs lose (or both win) is a settlement-rules mismatch and fires an ERROR-level
// audit alarm — the empirical feedback loop for the R124 xvgRulesEquiv equivalence guard.
//
// Pairs scanned (all structural, never fuzz):
//   K↔PUS  — gameident twins (winner/total/spread/advance; R106 flip semantics handled by
//            ORIENTATION: a flip twin's YES_A lock leg is YES_B, not NO_B);
//   K↔Pint — xvident twins priced from the gamma cache (zero new API; poly-int depth is not
//            observable → depth −1 "unknown"; poly-int is venue-locked, so these locks are
//            research-only by construction);
//   PUS↔Pint — TRANSITIVE through a shared Kalshi twin (both legs structural). Sports live only
//            in gameident and non-sports only in xvident today, so this pair's count is expected
//            ~0 until poly-int sports are anchored — surfaced honestly in /api/xvpairs.
//
// LOG-ONLY: no placement path exists in this file; the ledger grades what WOULD have happened if
// both legs filled at first-sight asks (1 contract/leg). Leg-risk is measured, not assumed: each
// later scan checks whether leg B could still be completed within +0.5¢ of its first-sight ask
// (BSurvived/Scans = the honest "second leg survives" rate).

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/resolutionbasis"
)

const (
	xvlMinMarginC   = 0.5 // log locks with ≥0.5¢ post-fee margin
	xvlOpenCap      = 400
	xvlClosedCap    = 600
	xvlPintBudget   = 8 // settle-pass gamma lookups for Pint legs
	xvlLegRiskTolPr = 0.005

	// R129 LOCKSTACK — paper-STAKE the depth-verified slice of the ledger (operator pre-auth:
	// locks are near-riskless; evidence at ship: 67 settled locks +4.22¢/lock mean, 0 settlement
	// mismatches, but only ~14% of sightings had both legs ≥1 contract at the ask — so staking is
	// gated on the measured leg-fill risk, exactly the data the scanner records):
	//   pair K↔PUS only (both venues bettable; PINT is venue-locked = research-only) ·
	//   first-sight margin ≥ 2¢ · BOTH legs ≥1 contract visible at the ask · px-offline gates ·
	//   contracts = min(both depths, 10) capped by the Combos-book wall + the lockstack
	//   scoreboard-weight share. Staked rows grade as their own verdict family "lockstack"
	//   (¢/lock scoreboard line); the log-only ledger and its stats are UNCHANGED.
	lockStackMinMarginC = 2.0
	lockStackMaxCts     = 10
)

// xvLockOpp is one observed lock opportunity (and, once both legs settle, its grade).
type xvLockOpp struct {
	Key        string  `json:"key"`  // pair|aid|bid|orient
	Pair       string  `json:"pair"` // K-PUS | K-PINT | PUS-PINT
	Orient     string  `json:"orient"`
	AVenue     string  `json:"a_venue"`
	AID        string  `json:"a_id"`
	AName      string  `json:"a_name,omitempty"`
	ASide      string  `json:"a_side"` // YES|NO bought on A
	BVenue     string  `json:"b_venue"`
	BID        string  `json:"b_id"`
	BName      string  `json:"b_name,omitempty"`
	BSide      string  `json:"b_side"`
	AAsk       float64 `json:"a_ask"` // first-sight entry asks (prob units)
	BAsk       float64 `json:"b_ask"`
	FeeA       float64 `json:"fee_a"` // $ per contract, taker
	FeeB       float64 `json:"fee_b"`
	FeeAKnown  bool    `json:"fee_a_known,omitempty"`
	FeeBKnown  bool    `json:"fee_b_known,omitempty"`
	FeeASource string  `json:"fee_a_source,omitempty"`
	FeeBSource string  `json:"fee_b_source,omitempty"`
	MarginC    float64 `json:"margin_c"` // first-sight post-fee margin, ¢ per contract-pair
	MaxMargC   float64 `json:"max_margin_c"`
	ADepth     float64 `json:"a_depth"` // contracts at the ask (−1 unknown)
	BDepth     float64 `json:"b_depth"`
	FirstSeen  string  `json:"first_seen"`
	LastSeen   string  `json:"last_seen"`
	Scans      int     `json:"scans"`
	BSurvived  int     `json:"b_survived"` // scans where leg B was still fillable ≤ first ask+0.5¢
	Ended      string  `json:"ended,omitempty"`
	AWon       float64 `json:"a_won"` // −1 unknown, else exact side payout 0..1 from venue A's settlement
	BWon       float64 `json:"b_won"`
	RealizedC  float64 `json:"realized_c"`
	Mismatch   bool    `json:"mismatch"`
	SettledTS  string  `json:"settled_ts,omitempty"`
	// PairQuarantined is durable feedback from an observed payout mismatch on either orientation
	// of this exact venue-instrument pair. Quarantined rows remain forensic history but never enter
	// proof, staking, or future opportunity generation.
	PairQuarantined bool    `json:"pair_quarantined,omitempty"`
	Staked          float64 `json:"staked,omitempty"` // R129 LOCKSTACK: contracts paper-staked from the Combos book (0 = log-only row)
	// R132: a staked order's venue fees are rounded on the aggregate order, not one contract at a
	// time. FeeA/FeeB stay the one-contract research values so the original lock ledger remains
	// comparable. These fields are the actual total entry fees and realized dollars for the staked
	// slice. StakeFeesExact distinguishes a legitimate $0 aggregate fee from a pre-R132 row.
	StakeFeeA float64 `json:"stake_fee_a,omitempty"`
	StakeFeeB float64 `json:"stake_fee_b,omitempty"`
	// StakeFeesExact means aggregate-order rounding was used. The per-leg Known+Source fields
	// separately certify the venue schedule; arithmetic exactness is not fee authority.
	StakeFeesExact  bool    `json:"stake_fees_exact,omitempty"`
	StakeFeeAKnown  bool    `json:"stake_fee_a_known,omitempty"`
	StakeFeeBKnown  bool    `json:"stake_fee_b_known,omitempty"`
	StakeFeeASource string  `json:"stake_fee_a_source,omitempty"`
	StakeFeeBSource string  `json:"stake_fee_b_source,omitempty"`
	StakedMarginC   float64 `json:"staked_margin_c,omitempty"` // exact aggregate margin per pair
	StakedPnL       float64 `json:"staked_pnl,omitempty"`      // exact aggregate realized dollars
	// ResolutionBasis freezes both venues' immutable rules/source/version/void/scalar/unknown
	// semantics at first sight. Nil marks a legacy row and is never proof- or stake-eligible.
	ResolutionBasis *resolutionbasis.Certificate `json:"resolution_basis,omitempty"`
}

// xvlProofEligible separates an auditable historical observation from evidence that may prove the
// lock strategy. Rows remain byte-for-byte in the ledger and keep their own venue settlements, but
// a structurally anchored class whose settlement authorities are not equivalent cannot influence
// the xvlock verdict, sizing proof, or forward summary. Parsing either leg is enough to fail closed;
// legacy K↔PINT weather rows always retain the Kalshi ticker even when the old PINT id was a slug.
func xvlProofEligible(op xvLockOpp) bool {
	if op.Mismatch || op.PairQuarantined {
		return false
	}
	if op.ResolutionBasis == nil || resolutionbasis.Verify(*op.ResolutionBasis) != nil {
		return false
	}
	for _, leg := range []struct{ venue, id string }{{op.AVenue, op.AID}, {op.BVenue, op.BID}} {
		var key, kind string
		var anchored bool
		switch strings.ToLower(strings.TrimSpace(leg.venue)) {
		case "kalshi":
			key, kind, anchored = xvKalshiAnchor(leg.id)
		case "polymarket":
			// Weather slugs encode their date and do not need endDate. Condition-id-only rows are
			// still caught by the Kalshi leg; no live path can create a new weather row after R133.
			key, kind, anchored = xvPolyAnchor(leg.id, "")
		}
		if anchored && !xvKeyRulesEquivalent(kind, key) {
			return false
		}
	}
	return true
}

// stakedCostUSD is the Combos-book cost basis. Pre-R132 rows did not persist aggregate venue
// fees, so they retain their historical one-contract-fee x contracts accounting.
func (op xvLockOpp) stakedCostUSD() float64 {
	if op.Staked <= 0 {
		return 0
	}
	fees := (op.FeeA + op.FeeB) * op.Staked
	if op.StakeFeesExact {
		fees = op.StakeFeeA + op.StakeFeeB
	}
	return (op.AAsk+op.BAsk)*op.Staked + fees
}

// stakedNetUSD is the money result of the staked slice. RealizedC remains the one-contract
// research grade; exact R132 rows use the aggregate receipt stored at settlement.
func (op xvLockOpp) stakedNetUSD() float64 {
	if op.Staked <= 0 {
		return 0
	}
	if op.StakeFeesExact {
		return op.StakedPnL
	}
	return op.RealizedC / 100 * op.Staked
}

type xvlStakeTerms struct {
	FeeA, FeeB             float64
	FeeAKnown, FeeBKnown   bool
	FeeASource, FeeBSource string
	Cost, MarginC          float64
}

// xvlFeeReceipt returns the route's aggregate fee plus historical authority. Fallback fee models
// stay useful for opportunity discovery, but they cannot certify a Systems row or a lockstack lot.
func (s *Server) xvlFeeReceipt(venue, id string, contracts, price float64) (float64, string, bool) {
	if contracts <= 0 || price <= 0 || price >= 1 {
		return 0, "", false
	}
	switch strings.ToLower(strings.TrimSpace(venue)) {
	case "kalshi":
		fee, known, why := s.kalFeeExact(id, false, contracts, price)
		if !known || fee < 0 || math.IsNaN(fee) || math.IsInf(fee, 0) {
			return 0, "", false
		}
		return fee, "kalshi:" + strings.TrimSpace(why), true
	case "polyus":
		fee, source, known := s.polyUSFeeExactAuthority(id, false, contracts, price)
		if !known || fee < 0 {
			return 0, "", false
		}
		return fee, source, true
	case "polymarket":
		if s.poly == nil {
			return 0, "", false
		}
		fee, known := s.poly.ClobFeeUSD(id, contracts, price, true)
		if !known || fee < 0 || math.IsNaN(fee) || math.IsInf(fee, 0) {
			return 0, "", false
		}
		return fee, "polyint:clob-fee-schedule", true
	default:
		return 0, "", false
	}
}

// xvlAggregateStake prices the actual n-contract orders at each venue's aggregate rounding rule.
// The scanner's FeeA/FeeB and MarginC deliberately remain its one-contract research quote.
func (s *Server) xvlAggregateStake(c xvlCandidate, o xvlOrient, contracts float64) xvlStakeTerms {
	if contracts <= 0 {
		return xvlStakeTerms{}
	}
	fa := s.blendedFee(c.aVenue, c.aID, "", false, contracts, o.aAsk)
	fb := s.blendedFee(c.bVenue, c.bID, "", false, contracts, o.bAsk)
	faExact, faSource, faKnown := s.xvlFeeReceipt(c.aVenue, c.aID, contracts, o.aAsk)
	fbExact, fbSource, fbKnown := s.xvlFeeReceipt(c.bVenue, c.bID, contracts, o.bAsk)
	if faKnown {
		fa = faExact
	}
	if fbKnown {
		fb = fbExact
	}
	cost := contracts*(o.aAsk+o.bAsk) + fa + fb
	marginC := (contracts - cost) / contracts * 100
	return xvlStakeTerms{FeeA: fa, FeeB: fb, FeeAKnown: faKnown, FeeBKnown: fbKnown,
		FeeASource: faSource, FeeBSource: fbSource, Cost: cost, MarginC: marginC}
}

type xvLockBook struct {
	Open                       []xvLockOpp                 `json:"open"`
	Closed                     []xvLockOpp                 `json:"closed"`
	SeenTotal                  int64                       `json:"seen_total"`
	MismatchN                  int64                       `json:"mismatch_n"`
	QuarantinedPairs           map[string]xvLockQuarantine `json:"quarantined_pairs,omitempty"`
	SettlementEpoch            string                      `json:"settlement_epoch,omitempty"`
	SettlementEpochAt          string                      `json:"settlement_epoch_at,omitempty"`
	SettlementLegacyArchiveSHA string                      `json:"settlement_legacy_archive_sha256,omitempty"`
	SettlementLegacyExcluded   bool                        `json:"settlement_legacy_excluded,omitempty"`
}

// xvLockQuarantine survives restarts independently of the bounded closed-row tail. One mismatch
// proves that the apparent complementary payoff was not actually complementary; future scans of
// the exact semantic pair therefore fail closed rather than repeatedly rediscovering it.
type xvLockQuarantine struct {
	Pair          string `json:"pair"`
	AVenue        string `json:"a_venue"`
	AID           string `json:"a_id"`
	BVenue        string `json:"b_venue"`
	BID           string `json:"b_id"`
	Reason        string `json:"reason"`
	FirstMismatch string `json:"first_mismatch"`
	LastMismatch  string `json:"last_mismatch"`
	MismatchN     int    `json:"mismatch_n"`
}

func xvlSemanticPairKey(aVenue, aID, bVenue, bID string) string {
	a := strings.ToLower(strings.TrimSpace(aVenue)) + "|" + strings.ToLower(strings.TrimSpace(aID))
	b := strings.ToLower(strings.TrimSpace(bVenue)) + "|" + strings.ToLower(strings.TrimSpace(bID))
	if b < a {
		a, b = b, a
	}
	return a + "<->" + b
}

func xvlCandidatePairKey(c xvlCandidate) string {
	return xvlSemanticPairKey(c.aVenue, c.aID, c.bVenue, c.bID)
}

func xvlOppPairKey(op xvLockOpp) string {
	return xvlSemanticPairKey(op.AVenue, op.AID, op.BVenue, op.BID)
}

func xvlPairQuarantined(b *xvLockBook, c xvlCandidate) bool {
	if b == nil || len(b.QuarantinedPairs) == 0 {
		return false
	}
	_, blocked := b.QuarantinedPairs[xvlCandidatePairKey(c)]
	return blocked
}

// xvlRecordMismatchQuarantine records a newly observed mismatch before the bounded closed tail can
// rotate it away. Reconciliation below remains responsible for migrating legacy books and marking
// every historical orientation of the pair.
func xvlRecordMismatchQuarantine(b *xvLockBook, op *xvLockOpp) {
	if b == nil || op == nil {
		return
	}
	if b.QuarantinedPairs == nil {
		b.QuarantinedPairs = map[string]xvLockQuarantine{}
	}
	key := xvlOppPairKey(*op)
	q, exists := b.QuarantinedPairs[key]
	if !exists {
		q = xvLockQuarantine{Pair: op.Pair, AVenue: op.AVenue, AID: op.AID,
			BVenue: op.BVenue, BID: op.BID, Reason: "observed non-complementary venue payouts"}
	}
	q.MismatchN++
	if q.FirstMismatch == "" {
		q.FirstMismatch = op.SettledTS
	}
	q.LastMismatch = op.SettledTS
	b.QuarantinedPairs[key] = q
	op.PairQuarantined = true
}

// xvlReconcileQuarantines backfills old mismatch rows on first load, preserves counts after the
// closed tail rotates, and marks every orientation of a poisoned pair as proof-ineligible. Caller
// holds xvlMu. It returns whether the persisted book changed.
func xvlReconcileQuarantines(b *xvLockBook) bool {
	if b == nil {
		return false
	}
	changed := false
	if b.QuarantinedPairs == nil {
		b.QuarantinedPairs = map[string]xvLockQuarantine{}
	}
	type aggregate struct {
		row         xvLockOpp
		count       int
		first, last string
	}
	agg := map[string]aggregate{}
	for _, op := range b.Closed {
		if !op.Mismatch {
			continue
		}
		key := xvlOppPairKey(op)
		a := agg[key]
		a.row, a.count = op, a.count+1
		at := strings.TrimSpace(op.SettledTS)
		if a.first == "" || (at != "" && at < a.first) {
			a.first = at
		}
		if at > a.last {
			a.last = at
		}
		agg[key] = a
	}
	for key, a := range agg {
		q, exists := b.QuarantinedPairs[key]
		if !exists {
			q = xvLockQuarantine{Pair: a.row.Pair, AVenue: a.row.AVenue, AID: a.row.AID,
				BVenue: a.row.BVenue, BID: a.row.BID, Reason: "observed non-complementary venue payouts"}
			changed = true
		}
		if a.first != "" && (q.FirstMismatch == "" || a.first < q.FirstMismatch) {
			q.FirstMismatch, changed = a.first, true
		}
		if a.last > q.LastMismatch {
			q.LastMismatch, changed = a.last, true
		}
		if a.count > q.MismatchN {
			q.MismatchN, changed = a.count, true
		}
		b.QuarantinedPairs[key] = q
	}
	mark := func(rows []xvLockOpp) {
		for i := range rows {
			_, blocked := b.QuarantinedPairs[xvlOppPairKey(rows[i])]
			if blocked && !rows[i].PairQuarantined {
				rows[i].PairQuarantined, changed = true, true
			}
		}
	}
	mark(b.Open)
	mark(b.Closed)
	return changed
}

func (s *Server) xvlLoadLocked() *xvLockBook {
	if s.xvlBook != nil {
		return s.xvlBook
	}
	b := &xvLockBook{}
	path := filepath.Join(s.cfg().DataDir, "xvlock_book.json")
	if !s.readJSONLoose(path, b) {
		if st, err := os.Stat(path); err == nil && st.Size() > 0 {
			s.xvlPoisoned.Store(true) // bug-52 guard, same as every book
			s.log.Error("xvlock_book.json exists but failed to parse — lock-ledger flushes DISABLED this session", "path", path)
		}
	}
	if xvlReconcileQuarantines(b) {
		s.xvlDirty = true
	}
	s.xvlBook = b
	return b
}

func (s *Server) xvlFlush() {
	defer s.invalidatePortfolioEquityCache()
	s.xvlMu.Lock()
	defer s.xvlMu.Unlock()
	if s.xvlPoisoned.Load() || !s.xvlDirty || s.xvlBook == nil {
		return
	}
	out, err := json.Marshal(s.xvlBook)
	if err != nil {
		return
	}
	path := filepath.Join(s.cfg().DataDir, "xvlock_book.json")
	tmp := path + ".tmp"
	if os.WriteFile(tmp, out, 0o644) == nil {
		if os.Rename(tmp, path) == nil {
			s.xvlDirty = false
		}
	}
}

// xvlCandidate — one live matched pair with both sides' prices, pre-orientation.
type xvlCandidate struct {
	pair, aVenue, aID, bVenue, bID string
	sameSide                       bool
	aBid, aAsk, bBid, bAsk         float64 // prob units, venue-native YES prices
	aAskSz, aBidSz                 float64 // contracts at the touch (−1 unknown)
	bAskSz, bBidSz                 float64
	aNoAsk, bNoAsk                 float64 // exact opposite-token ask where the venue has one
	aNoAskSz, bNoAskSz             float64
}

// xvlFriendlyName keeps the raw venue IDs as identity while giving the API and audit trail the
// same readable market names used elsewhere in the suite. It is cache-only and never participates
// in matching, side orientation, settlement, or deduplication.
func (s *Server) xvlFriendlyName(venue, id string) string {
	venue, id = strings.ToLower(strings.TrimSpace(venue)), strings.TrimSpace(id)
	if id == "" {
		return ""
	}
	switch venue {
	case "kalshi":
		s.metaMu.Lock()
		m, ok := s.kmkts[id]
		s.metaMu.Unlock()
		if ok {
			return friendlyName(id, m.Title, m.YesSubTitle)
		}
	case "polyus":
		for _, m := range s.polyUSSnapshot() {
			if m.Slug == id {
				title := strings.TrimSpace(m.Game)
				if q := strings.TrimSpace(m.Question); q != "" && !strings.Contains(strings.ToLower(title), strings.ToLower(q)) {
					title = strings.TrimSpace(title + " · " + q)
				}
				return friendlyName(id, title, m.TeamName)
			}
		}
	case "polymarket":
		if s.poly != nil {
			if m, ok := s.poly.CachedMarketByCondition(id); ok {
				return friendlyName(m.Slug, m.Question, "")
			}
		}
	}
	return id
}

// xvlPolyUSQuote returns a current two-sided executable book. The markets-WS BBO is preferred;
// the already-fetched snapshot is accepted only while its all-board receipt is fresh. This keeps
// a stalled feed from repeatedly logging locks at an old touch.
func (s *Server) xvlPolyUSQuote(m polyUSMarket) (bid, ask, bidDepth, askDepth float64, ok bool) {
	yesAsk, yad, _, yesOK := s.executableAsk(context.Background(), "polyus", m.Slug, "YES", false)
	noAsk, nbd, _, noOK := s.executableAsk(context.Background(), "polyus", m.Slug, "NO", false)
	if yesOK && noOK {
		bid = 1 - noAsk
		if bid > 0 && yesAsk > bid && yesAsk < 1 {
			if nbd <= 0 {
				nbd = -1
			}
			if yad <= 0 {
				yad = -1
			}
			return bid, yesAsk, nbd, yad, true
		}
	}
	s.polyUSMu.Lock()
	at := s.polyUSAt
	s.polyUSMu.Unlock()
	if at.IsZero() || time.Since(at) > 2*time.Minute || m.Bid <= 0 || m.Ask <= m.Bid || m.Ask >= 1 {
		return 0, 0, 0, 0, false
	}
	return m.Bid, m.Ask, m.BidSz, m.AskSz, true
}

func xvlFriendlyLeg(name, id string) string {
	name, id = strings.TrimSpace(name), strings.TrimSpace(id)
	if name == "" || strings.EqualFold(name, id) {
		return id
	}
	return name + " [" + id + "]"
}

// xvlLockContext bounds contention on the shared lock ledger. Scans are periodic, so missing one
// pass is safer than queueing every verdict/briefing caller behind a sick pass. The short wait
// admits normal file-load/settlement critical sections while cancellation remains immediate.
func (s *Server) xvlLockContext(ctx context.Context, maxWait time.Duration) bool {
	if s.xvlMu.TryLock() {
		return true
	}
	if maxWait <= 0 {
		return false
	}
	t := time.NewTimer(maxWait)
	defer t.Stop()
	poll := time.NewTicker(2 * time.Millisecond)
	defer poll.Stop()
	for {
		select {
		case <-ctx.Done():
			return false
		case <-t.C:
			return false
		case <-poll.C:
			if s.xvlMu.TryLock() {
				return true
			}
		}
	}
}

// xvLockScan — the 60s sweep: enumerate structural pairs, compute both lock orientations, track
// the ledger. Registered in MonitorPaper's run() table.
func (s *Server) xvLockScan(ctx context.Context) {
	if s.xvlPoisoned.Load() {
		return
	}
	if time.Since(s.xvlSweepAt) < 45*time.Second {
		return
	}
	s.xvlSweepAt = time.Now()
	var cands []xvlCandidate

	// ── K↔PUS from gameident (structural; prop-class refused by twinLocked) ──
	pusPx := map[string]polyUSMarket{}
	for _, m := range s.polyUSSnapshot() {
		pusPx[m.Slug] = m
	}
	type kp struct {
		kt, slug string
		sameSide bool
	}
	var kpPairs []kp
	st := s.gi()
	s.giMu.Lock()
	for mk, ref := range st.byMkt {
		if ref == nil || ref.venue != "kalshi" || !strings.HasPrefix(mk, "kalshi|") {
			continue
		}
		if slug, sameSide, ok := st.twinLocked(ref, "polyus"); ok {
			kpPairs = append(kpPairs, kp{kt: ref.id, slug: slug, sameSide: sameSide})
		}
	}
	s.giMu.Unlock()
	for _, p := range kpPairs {
		pm, ok := pusPx[p.slug]
		if !ok {
			continue
		}
		pb, pa, pbs, pas, freshPUS := s.xvlPolyUSQuote(pm)
		if !freshPUS {
			continue
		}
		kb, ka, fresh := s.xvlKalshiQuote(p.kt)
		if !fresh {
			continue
		}
		c := xvlCandidate{pair: "K-PUS", aVenue: "kalshi", aID: p.kt, bVenue: "polyus", bID: p.slug,
			sameSide: p.sameSide, aBid: kb, aAsk: ka, bBid: pb, bAsk: pa,
			aAskSz: -1, aBidSz: -1, bAskSz: pas, bBidSz: pbs}
		if s.kal != nil { // YES-ask liquidity = the NO-bid ladder at 1−ask (and vice versa)
			if sz, ok := s.kal.BookLevelSize(p.kt, true, 1-ka, 5*time.Second); ok {
				c.aAskSz = sz
			}
			if sz, ok := s.kal.BookLevelSize(p.kt, false, kb, 5*time.Second); ok {
				c.aBidSz = sz
			}
		}
		cands = append(cands, c)

		// ── PUS↔Pint transitive through this Kalshi twin (expected ~0 today; counted honestly) ──
		if _, pintID, _, ok3 := xvTwin("kalshi", p.kt); ok3 {
			if gm, okc := s.xvlPintCached(pintID); okc && gm.BestBid > 0 && gm.BestAsk > 0 {
				cands = append(cands, xvlCandidate{pair: "PUS-PINT", aVenue: "polyus", aID: p.slug,
					bVenue: "polymarket", bID: gm.ConditionID, sameSide: p.sameSide,
					aBid: pb, aAsk: pa, bBid: gm.BestBid, bAsk: gm.BestAsk,
					aAskSz: pas, aBidSz: pbs,
					bAskSz: gm.BestAskDepth, bBidSz: gm.BestBidDepth,
					bNoAsk: gm.NoBestAsk, bNoAskSz: gm.NoBestAskDepth})
			}
		}
	}

	// ── K↔Pint and PUS↔Pint from the dynamic identity registry (zero API) ──
	for _, gm := range s.pintCatalogMarkets() {
		if gm.Slug == "" || gm.Closed {
			continue
		}
		kt, pusSlug := pintDynamicXVTwins(gm.ConditionID, gm.Slug)
		pq, quoted := s.xvlPintMarketQuote(gm)
		if !quoted {
			continue
		}
		if kt != "" {
			if kb, ka, fresh := s.xvlKalshiQuote(kt); fresh {
				// xvident keys encode identical direction (same-side by construction)
				cands = append(cands, xvlCandidate{pair: "K-PINT", aVenue: "kalshi", aID: kt,
					bVenue: "polymarket", bID: gm.ConditionID, sameSide: true,
					aBid: kb, aAsk: ka, bBid: pq.BestBid, bAsk: pq.BestAsk,
					aAskSz: -1, aBidSz: -1, bAskSz: pq.BestAskDepth, bBidSz: pq.BestBidDepth,
					bNoAsk: pq.NoBestAsk, bNoAskSz: pq.NoBestAskDepth})
			}
		}
		if pm, ok := pusPx[pusSlug]; pusSlug != "" && ok {
			pb, pa, pbs, pas, freshPUS := s.xvlPolyUSQuote(pm)
			if !freshPUS {
				continue
			}
			cands = append(cands, xvlCandidate{pair: "PUS-PINT", aVenue: "polyus", aID: pusSlug,
				bVenue: "polymarket", bID: gm.ConditionID, sameSide: true,
				aBid: pb, aAsk: pa, bBid: pq.BestBid, bAsk: pq.BestAsk,
				aAskSz: pas, aBidSz: pbs,
				bAskSz: pq.BestAskDepth, bBidSz: pq.BestBidDepth,
				bNoAsk: pq.NoBestAsk, bNoAskSz: pq.NoBestAskDepth})
		}
	}

	// ── R127 poly-int SPORTS joins (pintsports.go) — CONSUME-GATED: returns nil until
	// auto.pint_sports_consume is armed (default OFF; anchor/log runs regardless) ──
	cands = append(cands, s.pintLockCandidates()...)

	// R129 LOCKSTACK pre-compute (NEVER under xvlMu — bookAvailableUSD reads the ledger too):
	// the Combos-book wall + the lockstack scoreboard-weight share; a cold weight table stakes
	// nothing this pass (the explore pool funds the line within ~1 min of boot).
	lockAvail, lockShare := 0.0, 0.0
	if s.lockStackOn() && !s.ksBlocked() {
		lockAvail = s.bookAvailableUSD(ctx, vbCombos)
		if sh, ok := s.subShareUSD("lockstack"); ok {
			lockShare = sh
		}
	}
	// Snapshot only the ledger identities needed to decide which candidates require a new immutable
	// certificate. No database call is allowed while xvlMu is held: the pre-R146 implementation did
	// one certificate query per candidate inside this critical section while cloning the whole
	// batch map for each query. With the expanded catalog that became O(N^2) and stalled xvlock,
	// verdicts, and briefing previews together.
	if !s.xvlLockContext(ctx, 25*time.Millisecond) {
		return
	}
	snapshotBook := s.xvlLoadLocked()
	snapshotOpen := make(map[string]struct{}, len(snapshotBook.Open))
	for i := range snapshotBook.Open {
		snapshotOpen[snapshotBook.Open[i].Key] = struct{}{}
	}
	snapshotQuarantined := make(map[string]struct{}, len(snapshotBook.QuarantinedPairs))
	for key := range snapshotBook.QuarantinedPairs {
		snapshotQuarantined[key] = struct{}{}
	}
	newSlots := xvlOpenCap - len(snapshotBook.Open)
	if newSlots < 0 {
		newSlots = 0
	}
	s.xvlMu.Unlock()

	resolutionInputs := s.xvlResolutionInputs(ctx, cands...)
	if ctx.Err() != nil {
		return
	}
	type preparedOrientation struct {
		o                    xvlOrient
		key, aName, bName    string
		certificate          resolutionbasis.Certificate
		certificateAvailable bool
	}
	type preparedCandidate struct {
		candidate    xvlCandidate
		orientations []preparedOrientation
	}
	prepared := make([]preparedCandidate, 0, len(cands))
	plannedNew := 0
	for _, c := range cands {
		if ctx.Err() != nil {
			return
		}
		if _, blocked := snapshotQuarantined[xvlCandidatePairKey(c)]; blocked {
			continue
		}
		pc := preparedCandidate{candidate: c}
		for _, o := range s.xvlOrientations(c) {
			if ctx.Err() != nil {
				return
			}
			po := preparedOrientation{o: o, key: c.pair + "|" + c.aID + "|" + c.bID + "|" + o.orient}
			_, alreadyOpen := snapshotOpen[po.key]
			if !alreadyOpen && o.marginC >= xvlMinMarginC && plannedNew < newSlots {
				po.certificate = s.xvlCertifiedResolutionCertificate(ctx, c, o, resolutionInputs)
				if ctx.Err() != nil {
					return
				}
				po.certificateAvailable = true
				po.aName = s.xvlFriendlyName(c.aVenue, c.aID)
				po.bName = s.xvlFriendlyName(c.bVenue, c.bID)
				plannedNew++
			}
			pc.orientations = append(pc.orientations, po)
		}
		prepared = append(prepared, pc)
	}
	observedAt := time.Now().UTC()
	now := observedAt.Format(time.RFC3339)
	seen := map[string]bool{}
	var stakeNotes []string
	var typedXVCandidates []xvLockOpp
	if !s.xvlLockContext(ctx, 25*time.Millisecond) {
		return
	}
	b := s.xvlLoadLocked()
	openByKey := map[string]int{}
	stakedHeld := 0.0 // current staked open exposure (counts against the lockstack share)
	for i := range b.Open {
		openByKey[b.Open[i].Key] = i
		if op := b.Open[i]; op.Staked > 0 {
			stakedHeld += op.stakedCostUSD()
		}
	}
	// bookAvailableUSD already subtracts every existing open position, including stakedHeld.
	// Only newly opened rows consume this incremental remainder; the lockstack share remains an
	// absolute cap and therefore compares against stakedHeld + the candidate's exact cost.
	bookRemaining := lockAvail
	completePass := true
apply:
	for _, pc := range prepared {
		if ctx.Err() != nil {
			completePass = false
			break
		}
		c := pc.candidate
		if xvlPairQuarantined(b, c) {
			continue
		}
		for _, po := range pc.orientations {
			if ctx.Err() != nil {
				completePass = false
				break apply
			}
			o, key := po.o, po.key
			seen[key] = true
			if idx, ok := openByKey[key]; ok { // persistence + leg-risk tracking
				op := &b.Open[idx]
				op.LastSeen, op.Scans = now, op.Scans+1
				if o.marginC > op.MaxMargC {
					op.MaxMargC = o.marginC
				}
				if o.bAsk <= op.BAsk+xvlLegRiskTolPr {
					op.BSurvived++
				}
				if o.marginC < xvlMinMarginC && op.Ended == "" {
					op.Ended = now
				}
				s.xvlDirty = true
				continue
			}
			if o.marginC < xvlMinMarginC {
				continue
			}
			if len(b.Open) >= xvlOpenCap {
				continue // runaway guard — capacity reported on /api/xvlock
			}
			// A row that became new after the pre-lock snapshot waits for the next pass. It must never
			// be inserted without the independently read immutable certificate.
			if !po.certificateAvailable {
				continue
			}
			b.SeenTotal++
			op := xvLockOpp{Key: key, Pair: c.pair, Orient: o.orient,
				AVenue: c.aVenue, AID: c.aID, AName: po.aName, ASide: o.aSide,
				BVenue: c.bVenue, BID: c.bID, BName: po.bName, BSide: o.bSide,
				AAsk: o.aAsk, BAsk: o.bAsk, FeeA: o.feeA, FeeB: o.feeB,
				FeeAKnown: o.feeAKnown, FeeBKnown: o.feeBKnown,
				FeeASource: o.feeASource, FeeBSource: o.feeBSource,
				MarginC: o.marginC, MaxMargC: o.marginC, ADepth: o.aDepth, BDepth: o.bDepth,
				FirstSeen: now, LastSeen: now, Scans: 1, BSurvived: 1, AWon: -1, BWon: -1,
				ResolutionBasis: &po.certificate}
			if op.Pair == "K-PUS" && xvlProofEligible(op) && op.FeeAKnown && op.FeeBKnown &&
				op.ADepth >= 1 && op.BDepth >= 1 && op.MarginC > 0 {
				typedXVCandidates = append(typedXVCandidates, op)
			}
			// R129 LOCKSTACK: stake the depth-verified slice at FIRST SIGHT only (the asks the
			// margin was measured at). Gate ladder: knob/kill (lockAvail>0 embeds both) · K↔PUS
			// only · margin floor · both legs ≥1 contract visible · both venues' feeds alive.
			if r148LegacyLockstackFundingAllowed() && lockAvail > 0 && lockShare > 0 && c.pair == "K-PUS" && xvlProofEligible(op) &&
				o.marginC >= lockStackMinMarginC && o.aDepth >= 1 && o.bDepth >= 1 &&
				s.pxOfflineReason("kalshi", c.aID) == "" && s.pxOfflineReason("polyus", c.bID) == "" {
				maxN := int(math.Floor(math.Min(o.aDepth, o.bDepth)))
				if maxN > lockStackMaxCts {
					maxN = lockStackMaxCts
				}
				// Aggregate fee rounding is nonlinear, so solve the small (1..10) sizing problem
				// exactly instead of dividing the wall by a one-contract cost.
				for ni := maxN; ni >= 1; ni-- {
					n := float64(ni)
					terms := s.xvlAggregateStake(c, o, n)
					if !terms.FeeAKnown || !terms.FeeBKnown || strings.TrimSpace(terms.FeeASource) == "" ||
						strings.TrimSpace(terms.FeeBSource) == "" || terms.MarginC+1e-9 < lockStackMinMarginC ||
						terms.Cost > bookRemaining+1e-9 ||
						stakedHeld+terms.Cost > lockShare+1e-9 {
						continue
					}
					op.Staked = n
					op.StakeFeeA, op.StakeFeeB = terms.FeeA, terms.FeeB
					op.StakeFeesExact = true
					op.StakeFeeAKnown, op.StakeFeeBKnown = terms.FeeAKnown, terms.FeeBKnown
					op.StakeFeeASource, op.StakeFeeBSource = terms.FeeASource, terms.FeeBSource
					op.StakedMarginC = math.Round(terms.MarginC*100) / 100
					stakedHeld += terms.Cost
					bookRemaining -= terms.Cost
					stakeNotes = append(stakeNotes, fmt.Sprintf(
						"LOCKSTACK OPEN %s %s: %s %s + %s %s ×%.0f — margin %.2f¢/pair, cost $%.2f (depths %.0f/%.0f)",
						op.Pair, op.Orient, op.ASide, xvlFriendlyLeg(op.AName, op.AID), op.BSide,
						xvlFriendlyLeg(op.BName, op.BID), n, op.StakedMarginC, terms.Cost, o.aDepth, o.bDepth))
					break
				}
			}
			b.Open = append(b.Open, op)
			openByKey[key] = len(b.Open) - 1
			s.xvlDirty = true
		}
	}
	// Only a complete pass can declare a pair absent. Cancellation preserves the last complete
	// visibility receipt rather than ending every unvisited row from a partial scan.
	if completePass {
		for i := range b.Open {
			if !seen[b.Open[i].Key] && b.Open[i].Ended == "" {
				b.Open[i].Ended = now
				s.xvlDirty = true
			}
		}
	}
	s.xvlMu.Unlock()
	s.xvlFlush()
	for _, op := range typedXVCandidates {
		inserted, duplicate, err := s.captureR148XVLockBundle(ctx, op, observedAt)
		if err != nil {
			_ = s.store.Audit(ctx, "info", "xvlock-typed-route-rejected", op.Key+": "+err.Error(), "")
			continue
		}
		if inserted > 0 || duplicate > 0 {
			_ = s.store.Audit(ctx, "info", "xvlock-typed-route", fmt.Sprintf("%s: typed=%d duplicate=%d", op.Key, inserted, duplicate), "")
		}
	}
	for _, n := range stakeNotes {
		_ = s.store.Audit(ctx, "info", "lockstack", n, "")
	}
}

// lockStackOn — config kill for the staked lock expression (lockstack_enabled, nil/absent = ON).
func (s *Server) lockStackOn() bool {
	v := s.cfg().Auto.LockStackEnabled
	return v == nil || *v
}

type xvlOrient struct {
	orient, aSide, bSide    string
	aAsk, bAsk, feeA, feeB  float64
	feeAKnown, feeBKnown    bool
	feeASource, feeBSource  string
	marginC, aDepth, bDepth float64
}

// xvlOrientations computes both lock orientations for a candidate pair. Complement semantics:
// sameSide pairs lock as YES_A+NO_B / NO_A+YES_B; flip pairs (A YES ≡ B NO, R106) lock as
// YES_A+YES_B / NO_A+NO_B. Complementary single-instrument venues derive NO from YES; Poly-int
// supplies the separately subscribed opposite-token ask and its own depth.
func (s *Server) xvlOrientations(c xvlCandidate) []xvlOrient {
	yesAsk := func(ask float64) float64 { return ask }
	noAsk := func(exactAsk, yesBid float64) float64 {
		if exactAsk > 0 && exactAsk < 1 {
			return exactAsk
		}
		return 1 - yesBid
	}
	noDepth := func(exactAsk, exactDepth, yesBidDepth float64) float64 {
		if exactAsk > 0 && exactAsk < 1 {
			return exactDepth
		}
		return yesBidDepth
	}
	fee := func(venue, id string, px float64) (float64, string, bool) {
		if px <= 0 || px >= 1 {
			return 0, "", false
		}
		fallback := s.blendedFee(venue, id, "", false, 1, px)
		if exact, source, known := s.xvlFeeReceipt(venue, id, 1, px); known {
			return exact, source, true
		}
		return fallback, "", false
	}
	var out []xvlOrient
	build := func(orient, aSide, bSide string, aAsk, bAsk, aDepth, bDepth float64) {
		if aAsk <= 0 || aAsk >= 1 || bAsk <= 0 || bAsk >= 1 {
			return
		}
		fa, faSource, faKnown := fee(c.aVenue, c.aID, aAsk)
		fb, fbSource, fbKnown := fee(c.bVenue, c.bID, bAsk)
		m := (1 - aAsk - bAsk - fa - fb) * 100
		out = append(out, xvlOrient{orient: orient, aSide: aSide, bSide: bSide,
			aAsk: aAsk, bAsk: bAsk, feeA: fa, feeB: fb,
			feeAKnown: faKnown, feeBKnown: fbKnown, feeASource: faSource, feeBSource: fbSource,
			marginC: math.Round(m*100) / 100,
			aDepth:  aDepth, bDepth: bDepth})
	}
	if c.sameSide {
		build("YESa+NOb", "YES", "NO", yesAsk(c.aAsk), noAsk(c.bNoAsk, c.bBid), c.aAskSz, noDepth(c.bNoAsk, c.bNoAskSz, c.bBidSz))
		build("NOa+YESb", "NO", "YES", noAsk(c.aNoAsk, c.aBid), yesAsk(c.bAsk), noDepth(c.aNoAsk, c.aNoAskSz, c.aBidSz), c.bAskSz)
	} else {
		build("YESa+YESb", "YES", "YES", yesAsk(c.aAsk), yesAsk(c.bAsk), c.aAskSz, c.bAskSz)
		build("NOa+NOb", "NO", "NO", noAsk(c.aNoAsk, c.aBid), noAsk(c.bNoAsk, c.bBid), noDepth(c.aNoAsk, c.aNoAskSz, c.aBidSz), noDepth(c.bNoAsk, c.bNoAskSz, c.bBidSz))
	}
	return out
}

// xvlKalshiQuote — fresh, executable Kalshi YES bid/ask from the actual book plus a live,
// unsettled market certificate from metadata. The old path priced a lock from a ≤2-minute REST
// market snapshot; that can manufacture an arbitrage after the touch has already moved.
func (s *Server) xvlKalshiQuote(ticker string) (bid, ask float64, ok bool) {
	yesAsk, _, _, yesOK := s.executableAsk(context.Background(), "kalshi", ticker, "YES", false)
	noAsk, _, _, noOK := s.executableAsk(context.Background(), "kalshi", ticker, "NO", false)
	if !yesOK || !noOK {
		return 0, 0, false
	}
	bookBid := 1 - noAsk
	if bookBid <= 0 || yesAsk <= bookBid || yesAsk >= 1 {
		return 0, 0, false
	}
	s.metaMu.Lock()
	km, okM := s.kmkts[ticker]
	at, okA := s.kmktsAt[ticker]
	s.metaMu.Unlock()
	if !okM || !okA || time.Since(at) > 2*time.Minute {
		return 0, 0, false
	}
	if km.Result != "" {
		return 0, 0, false
	}
	if status := strings.ToLower(strings.TrimSpace(km.Status)); status != "active" {
		return 0, 0, false
	}
	return bookBid, yesAsk, true
}

// settleXvLocks — grade ended/open locks from EACH venue's OWN settlement (settlement tick).
func (s *Server) settleXvLocks(ctx context.Context) {
	if s.xvlPoisoned.Load() {
		return
	}
	// One shared bounded venue budget keeps direct terminal checks from flooding any venue while
	// ensuring Kalshi/PolyUS legs that disappeared from active caches can still close. Previously
	// only Poly-int had a direct fallback, which left old K/PUS locks at -1 indefinitely whenever
	// their signal-log ticker had not yet reached the broad round-robin drain.
	venueBudget := xvlPintBudget
	now := time.Now().UTC().Format(time.RFC3339)
	var alarms, closeNotes []string
	// Snapshot under the ledger mutex, then perform every database/venue lookup outside it. The
	// previous implementation held xvlMu across ResolvedYesForVenue and direct venue fallbacks;
	// one slow SQLite page read therefore froze /api/xvlock and made Combo portfolio metrics render
	// n/a. Settlement is a merge, not a reason to monopolize the in-memory ledger.
	s.xvlMu.Lock()
	b := s.xvlLoadLocked()
	snapshot := append([]xvLockOpp(nil), b.Open...)
	s.xvlMu.Unlock()
	snapshot = xvlArmedSettlementSnapshot(snapshot, s.researchLiveActive())
	if len(snapshot) == 0 {
		return
	}
	legWon := s.xvlLegWon
	if s.xvlLegWonFn != nil {
		legWon = s.xvlLegWonFn
	}
	type legResult struct{ a, b float64 }
	resolved := make(map[string]legResult, len(snapshot))
	for _, op := range snapshot {
		if op.AWon < 0 {
			op.AWon = legWon(ctx, op.AVenue, op.AID, op.ASide, &venueBudget)
		}
		if op.BWon < 0 {
			op.BWon = legWon(ctx, op.BVenue, op.BID, op.BSide, &venueBudget)
		}
		resolved[op.Key] = legResult{a: op.AWon, b: op.BWon}
		if ctx.Err() != nil {
			break
		}
	}
	if !s.xvlLockContext(ctx, 25*time.Millisecond) {
		return
	}
	b = s.xvlLoadLocked()
	still := make([]xvLockOpp, 0, len(b.Open))
	for _, op := range b.Open {
		result, checked := resolved[op.Key]
		if !checked {
			still = append(still, op)
			continue
		}
		if op.AWon < 0 {
			op.AWon = result.a
		}
		if op.BWon < 0 {
			op.BWon = result.b
		}
		if op.AWon < 0 || op.BWon < 0 {
			still = append(still, op)
			continue
		}
		pay := op.AWon + op.BWon
		op.RealizedC = math.Round((pay-op.AAsk-op.BAsk-op.FeeA-op.FeeB)*10000) / 100
		op.Mismatch = math.Abs(pay-1) > 1e-9
		op.SettledTS = now
		if op.Staked > 0 {
			if op.StakeFeesExact {
				op.StakedPnL = math.Round((pay*op.Staked-(op.AAsk+op.BAsk)*op.Staked-op.StakeFeeA-op.StakeFeeB)*1e6) / 1e6
			} else {
				// A pre-R132 row has no persisted aggregate receipt, so retain its historical
				// one-contract-fee x contracts result rather than guessing from today's schedule.
				op.StakedPnL = op.RealizedC / 100 * op.Staked
			}
		}
		if op.Staked > 0 { // R129 LOCKSTACK: the staked slice's realized $ (grades as family "lockstack")
			closeNotes = append(closeNotes, fmt.Sprintf("LOCKSTACK CLOSE %s %s ×%.0f — realized %+.2f¢/pair = $%+.2f",
				op.Pair, op.Orient, op.Staked, op.stakedNetUSD()/op.Staked*100, op.stakedNetUSD()))
		}
		if op.Mismatch {
			b.MismatchN++
			xvlRecordMismatchQuarantine(b, &op)
			alarms = append(alarms, fmt.Sprintf(
				"XVLOCK SETTLEMENT-RULES MISMATCH: %s %s — leg payouts %.4g/%.4g (%s %s %s ⇄ %s %s %s) realized %+.1f¢ — equivalence guard feedback",
				op.Pair, op.Orient, op.AWon, op.BWon, op.AVenue, op.ASide, op.AID, op.BVenue, op.BSide, op.BID, op.RealizedC))
		}
		b.Closed = append(b.Closed, op)
		s.xvlDirty = true
	}
	b.Open = still
	if xvlReconcileQuarantines(b) {
		s.xvlDirty = true
	}
	if len(b.Closed) > xvlClosedCap {
		b.Closed = b.Closed[len(b.Closed)-xvlClosedCap:]
	}
	s.xvlMu.Unlock()
	for _, a := range alarms {
		s.log.Error(a)
		_ = s.store.Audit(ctx, "error", "xvlock", a, "")
	}
	for _, n := range closeNotes {
		_ = s.store.Audit(ctx, "info", "lockstack", n, "")
	}
	s.xvlFlush()
}

func xvlArmedSettlementSnapshot(snapshot []xvLockOpp, liveActive bool) []xvLockOpp {
	if !liveActive {
		return snapshot
	}
	// Unstaked locks are one-unit research observations. Keep only funded Paper lock legs in the
	// armed-session pass; their capital must still be released, while broad research polling waits.
	funded := snapshot[:0]
	for _, op := range snapshot {
		if op.Staked > 0 {
			funded = append(funded, op)
		}
	}
	return funded
}

func xvlSidePayout(yesValue float64, side string) (float64, bool) {
	if yesValue < 0 || yesValue > 1 || math.IsNaN(yesValue) || math.IsInf(yesValue, 0) {
		return 0, false
	}
	if strings.EqualFold(strings.TrimSpace(side), "YES") {
		return yesValue, true
	}
	if strings.EqualFold(strings.TrimSpace(side), "NO") {
		return 1 - yesValue, true
	}
	return 0, false
}

// xvlLegWon resolves one leg from ITS OWN venue: exact side payout 0..1 once settled, −1 while
// unknown. Keeping partial settlement values matters for void/refund/scalar outcomes; rounding
// 0.50 to a binary win can manufacture a settlement-rules mismatch and the wrong P&L.
func (s *Server) xvlLegWon(ctx context.Context, venue, id, side string, venueBudget *int) float64 {
	yesValue := -1.0
	switch venue {
	case "kalshi":
		s.metaMu.Lock()
		km, ok := s.kmkts[id]
		s.metaMu.Unlock()
		if ok {
			yesValue = km.SettledYes()
		}
		if yesValue < 0 {
			if yv, res := s.store.ResolvedYesForVenue(ctx, "kalshi", id); res {
				yesValue = yv
			}
		}
		if yesValue < 0 && venueBudget != nil && *venueBudget > 0 && ctx.Err() == nil {
			*venueBudget--
			if yv, res := s.resolveLegYes(ctx, "kalshi", id); res {
				yesValue = yv
			}
		}
	case "polyus":
		if yv, res := s.store.ResolvedYesForVenue(ctx, "polyus", id); res {
			yesValue = yv
		}
		if yesValue < 0 && venueBudget != nil && *venueBudget > 0 && ctx.Err() == nil {
			*venueBudget--
			if yv, res := s.resolveLegYes(ctx, "polyus", id); res {
				yesValue = yv
			}
		}
	case "polymarket":
		if yv, res := s.store.ResolvedYesForVenue(ctx, "polymarket", id); res {
			yesValue = yv
		} else if venueBudget != nil && *venueBudget > 0 {
			*venueBudget--
			m, ok := s.poly.MarketByCondition(ctx, id)
			if !ok { // compatibility for pre-R133 ledger rows that stored a Gamma slug
				m, ok = s.poly.MarketBySlug(ctx, id)
			}
			if ok {
				if win, done := m.Resolved(); done {
					w := strings.ToUpper(strings.TrimSpace(win))
					if w == "YES" || w == "UP" {
						yesValue = 1
					} else if w == "NO" || w == "DOWN" {
						yesValue = 0
					} else if outs := m.Outcomes(); len(outs) == 2 { // outcome-label binary
						if strings.EqualFold(outs[0], win) {
							yesValue = 1
						} else {
							yesValue = 0
						}
					}
				}
			}
		}
	}
	if yesValue >= 0 {
		if venue == "polyus" {
			// resolveLegYes is the only PolyUS network path above and has already
			// persisted the strict parser's versioned book/endpoint provenance.
			// Never relabel that receipt with a generic cross-venue source.
			durable, ok := s.store.ResolvedYesForVenue(ctx, venue, id)
			if !ok || math.Abs(durable-yesValue) > 1e-9 {
				s.settleErrOnce("receipt-xvlock-"+venue, id,
					fmt.Errorf("PolyUS leg lacks matching trusted final receipt"))
				return -1
			}
		} else if _, err := s.store.RecordVenueSettlement(ctx, venue, id, yesValue, time.Now().UTC(),
			"cross-venue lock authoritative leg settlement"); err != nil {
			s.settleErrOnce("receipt-xvlock-"+venue, id, err)
			return -1
		}
	}
	payout, ok := xvlSidePayout(yesValue, side)
	if !ok {
		return -1
	}
	return payout
}

// handleXvLock (GET /api/xvlock) — ledger + first-hours summary stats (opps/hour, margin
// distribution, depth capacity, persistence, per-pair counts, leg-risk survival).
func (s *Server) handleXvLock(w http.ResponseWriter, r *http.Request) {
	s.xvlMu.Lock()
	b := s.xvlLoadLocked()
	all := make([]xvLockOpp, 0, len(b.Open)+len(b.Closed))
	all = append(all, b.Open...)
	all = append(all, b.Closed...)
	openN, closedN, seen, mism := len(b.Open), len(b.Closed), b.SeenTotal, b.MismatchN
	quarantinedPairsN := len(b.QuarantinedPairs)
	stakedOpen, stakedClosed, stakedNet := 0, 0, 0.0 // R129 LOCKSTACK receipts
	for _, op := range b.Open {
		if op.Staked > 0 {
			stakedOpen++
		}
	}
	for _, op := range b.Closed {
		if op.Staked > 0 {
			stakedClosed++
			stakedNet += op.stakedNetUSD()
		}
	}
	s.xvlMu.Unlock()
	perPair := map[string]int{}
	proofPerPair := map[string]int{}
	var margins, persistMin, capUSD []float64
	survScans, survOK := 0, 0
	proofN, proofExcluded := 0, 0
	first := time.Now()
	for _, op := range all {
		perPair[op.Pair]++
		if !xvlProofEligible(op) {
			proofExcluded++
			continue
		}
		proofN++
		proofPerPair[op.Pair]++
		margins = append(margins, op.MarginC)
		if ft, err := time.Parse(time.RFC3339, op.FirstSeen); err == nil {
			if ft.Before(first) {
				first = ft
			}
			endT := time.Now()
			if op.Ended != "" {
				if et, err2 := time.Parse(time.RFC3339, op.Ended); err2 == nil {
					endT = et
				}
			} else if lt, err2 := time.Parse(time.RFC3339, op.LastSeen); err2 == nil {
				endT = lt
			}
			persistMin = append(persistMin, endT.Sub(ft).Minutes())
		}
		if op.ADepth > 0 && op.BDepth > 0 {
			d := math.Min(op.ADepth, op.BDepth)
			capUSD = append(capUSD, d*(op.AAsk+op.BAsk))
		}
		survScans += op.Scans
		survOK += op.BSurvived
	}
	sort.Float64s(margins)
	med := func(xs []float64) float64 {
		if len(xs) == 0 {
			return 0
		}
		return xs[len(xs)/2]
	}
	hours := math.Max(time.Since(first).Hours(), 0.01)
	out := map[string]any{
		"open": openN, "closed": closedN, "seen_total": seen, "mismatch_n": mism,
		"quarantined_pairs_n": quarantinedPairsN,
		"per_pair":            perPair, "proof_per_pair": proofPerPair,
		"proof_n": proofN, "proof_excluded_n": proofExcluded,
		"opps_per_hour": math.Round(float64(proofN)/hours*100) / 100,
		"margin_med_c":  med(margins), "margin_max_c": func() float64 {
			if len(margins) == 0 {
				return 0
			}
			return margins[len(margins)-1]
		}(),
		"persist_med_min": med(persistMin), "cap_med_usd_1x": med(capUSD),
		"legB_survival_rate": func() float64 {
			if survScans == 0 {
				return 0
			}
			return math.Round(float64(survOK)/float64(survScans)*1000) / 1000
		}(),
		"note": "log-only lock scanner (R125): 1 contract/leg hypothetical fills at first-sight asks; mismatch_n>0 permanently quarantines that exact semantic pair; empirically incompatible predicate classes and quarantined pairs remain in history but are excluded from future matching, proof metrics, staking, and verdicts",
		// R129 LOCKSTACK: the depth-verified staked slice (K↔PUS, margin ≥2¢, both legs ≥1 ct,
		// Combos-book funded, verdict family "lockstack").
		"lockstack": map[string]any{"enabled": s.lockStackOn(), "staked_open": stakedOpen,
			"staked_closed": stakedClosed, "staked_net_usd": math.Round(stakedNet*100) / 100,
			"min_margin_c": lockStackMinMarginC, "max_contracts": lockStackMaxCts},
	}
	s.xvlMu.Lock()
	out["open_rows"] = s.xvlLoadLocked().Open
	closedTail := s.xvlLoadLocked().Closed
	if len(closedTail) > 50 {
		closedTail = closedTail[len(closedTail)-50:]
	}
	out["closed_tail"] = closedTail
	s.xvlMu.Unlock()
	writeJSON(w, http.StatusOK, out)
}

// handleXVPairs (GET /api/xvpairs) — per-venue-combination matcher coverage + realized-edge stats
// (R125 Part 3.3): matches per pair, flip-only counts, and the graded family numbers that price
// each pair (from the 60s verdict cache).
func (s *Server) handleXVPairs(w http.ResponseWriter, r *http.Request) {
	// K↔PUS from gameident
	type ptStat struct{ Matched, Flip int }
	kpus := map[string]*ptStat{}
	threeWayDiscovered, threeWayCertified := 0, 0
	st := s.gi()
	s.giMu.Lock()
	type kpIdentity struct{ kalshi, polyus string }
	var kpIDs []kpIdentity
	for _, ref := range st.byMkt {
		if ref == nil || ref.venue != "kalshi" {
			continue
		}
		if polyUSSlug, sameSide, ok := st.twinLocked(ref, "polyus"); ok {
			p := kpus[ref.mktType]
			if p == nil {
				p = &ptStat{}
				kpus[ref.mktType] = p
			}
			p.Matched++
			if !sameSide {
				p.Flip++
			}
			kpIDs = append(kpIDs, kpIdentity{kalshi: ref.id, polyus: polyUSSlug})
		}
	}
	s.giMu.Unlock()
	kpusTotal, kpusFlip := 0, 0
	for _, p := range kpus {
		kpusTotal += p.Matched
		kpusFlip += p.Flip
	}
	// K↔Pint from xvident. "matched" means safe for member-level lock comparison; structural
	// same-shape anchors whose settlement rules are unproven (notably NWS-vs-Wunderground weather)
	// are reported separately and never counted as locks.
	kpint := map[string]int{}
	kpintAnchorOnly := map[string]int{}
	kpintTotal := 0
	kpintAnchorOnlyTotal := 0
	xvReg.mu.Lock()
	for key, e := range xvReg.byKey {
		if e == nil {
			continue
		}
		if len(e.ids["kalshi"]) > 0 && len(e.ids["polymarket"]) > 0 {
			if xvKeyRulesEquivalent(e.kind, key) {
				kpint[e.kind]++
				kpintTotal++
			} else {
				kpintAnchorOnly[e.kind]++
				kpintAnchorOnlyTotal++
			}
		}
	}
	xvReg.mu.Unlock()
	// PUS↔Pint transitive (shared Kalshi twin)
	for _, pair := range kpIDs {
		if _, pintID, _, ok := xvTwin("kalshi", pair.kalshi); ok {
			threeWayDiscovered++
			if status, err := s.store.ThreeWayRuleCertificateStatus(r.Context(), pair.kalshi, pair.polyus, pintID); err == nil && status.Current {
				threeWayCertified++
			}
		}
	}
	// realized edge per pair from the verdict cache (honest units: see each family's Unit)
	fams := map[string]any{}
	for _, v := range s.computeExperimentVerdicts(r.Context()) {
		switch v.Family {
		case "xvgap", "xvlag", "xmatch", "xvlock", "book:xvgap":
			fams[v.Family] = map[string]any{"mean": v.Mean, "n": v.N, "unit": v.Unit, "state": v.State, "venue_locked": v.Locked}
		}
	}
	// R127: pint sports coverage + N random join samples (?samples=N — the 30-sample
	// hand-verification surface). Anchor-only until auto.pint_sports_consume arms consumers.
	samplesN := 0
	if v := r.URL.Query().Get("samples"); v != "" {
		samplesN, _ = strconv.Atoi(v)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"k_pus": map[string]any{"matched": kpusTotal, "flip_matched": kpusFlip, "by_type": kpus},
		"k_pint": map[string]any{"matched": kpintTotal, "predicate_matched": kpintTotal, "by_kind": kpint,
			"anchor_only": kpintAnchorOnlyTotal, "anchor_only_by_kind": kpintAnchorOnly},
		"pus_pint": map[string]any{"matched_transitive": threeWayDiscovered, "current_rule_certified": threeWayCertified,
			"note": "structural transitivity is discovery only; three-way economic status requires current K-PUS, K-PINT, and PUS-PINT rule certificates with consistent reviewed flips"},
		"three_way": map[string]any{"structural_discovered": threeWayDiscovered,
			"economic_current_certified": threeWayCertified, "requires_all_pairwise_current_certificates": true},
		"identity_note": "matched means exact structured payoff-predicate candidate, not a risk-free lock; the accepted/rejected objective ledger exposes directional vs lock-safe tiers",
		"families":      fams,
		"pint_sports":   s.pintPairsPayload(r.Context(), samplesN),
		"pint_clob":     s.pintCLOBPlanView(),
	})
}
