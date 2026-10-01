package server

// R144 LIVE daily-loss durability.
//
// A venue's current-position response is an exposure view, not an account ledger: fully closed
// positions disappear. The old implementation subtracted two such snapshots, so a close could
// erase a loss, a restart reset the baseline, and a Kalshi read error prevented PolyUS from being
// checked at all. This file instead persists Kalshi's cumulative total-traded ledger and immutable
// PolyUS activity receipts inside one UTC-day state. The venues are refreshed and gated independently.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/polymarketus"
)

const (
	liveLossPolyUSWindow                 = 100   // documented/default retail activities window
	liveLossKalshiSettlementWindow       = 100   // one authenticated page every 30s; continuity is proved by overlap/older-day receipt
	liveLossKalshiBootstrapWindow        = 10000 // first/rebuild read must cover the UTC day or expose an older-day cursor row
	liveLossTruthMaxAge                  = 90 * time.Second
	liveLossUnitsPerUSD            int64 = 1_000_000 // microdollars; fixed-point venues can realize sub-cent partial-quantity economics
)

type liveLossReceipt struct {
	ID     string
	DeltaC int64
}

func liveLossUSDToUnits(value float64) int64 {
	return int64(math.Round(value * float64(liveLossUnitsPerUSD)))
}

func liveLossUnitsToUSD(value int64) float64 {
	return float64(value) / float64(liveLossUnitsPerUSD)
}

func liveLossDay(now time.Time) string { return now.UTC().Format("2006-01-02") }

func liveLossDayStart(day string) time.Time {
	start, _ := time.Parse("2006-01-02", strings.TrimSpace(day))
	return start.UTC()
}

func liveLossReceiptID(parts ...string) string {
	h := sha256.Sum256([]byte(strings.Join(parts, "\x1f")))
	return hex.EncodeToString(h[:12])
}

func liveLossReceiptTime(raw string) (time.Time, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, errors.New("receipt has no venue timestamp")
	}
	for _, layout := range []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02 15:04:05.999999999Z07:00",
		"2006-01-02 15:04:05Z07:00",
	} {
		if t, err := time.Parse(layout, raw); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("unparseable venue receipt timestamp %q", raw)
}

func (s *Server) liveLossSeenCopy(venue string) map[string]bool {
	s.liveMu.Lock()
	defer s.liveMu.Unlock()
	out := map[string]bool{}
	for id := range s.liveLossSeenV[venue] {
		out[id] = true
	}
	return out
}

// kalshiCumulativeLossValues consumes count_filter=total_traded, not the open-position view. The
// venue contract says those rows remain for every ever-traded market, including a flat close.
// realized_pnl and fees_paid are cumulative account values, so persisting the last value gives an
// exact restart-safe delta without reconstructing lots from fills.
func kalshiCumulativeLossValues(rows []kalshi.MarketPosition) (map[string]int64, error) {
	out := make(map[string]int64, len(rows))
	for _, row := range rows {
		ticker := strings.TrimSpace(row.Ticker)
		if ticker == "" {
			return nil, errors.New("Kalshi total-traded position has no ticker")
		}
		if _, dup := out[ticker]; dup {
			return nil, fmt.Errorf("duplicate Kalshi total-traded position %s", ticker)
		}
		if !row.FeesKnown() {
			return nil, fmt.Errorf("Kalshi total-traded position %s has no fees_paid_dollars truth", ticker)
		}
		net := row.RealizedUSD() - row.FeesUSD()
		if math.IsNaN(net) || math.IsInf(net, 0) {
			return nil, fmt.Errorf("non-finite Kalshi cumulative economics for %s", ticker)
		}
		out[ticker] = liveLossUSDToUnits(net)
	}
	return out, nil
}

// kalshiFreshDayOpenFeeValues reconstructs a nonterminal ticker only when the venue proves that its
// entire lifetime traded notional and cumulative fee equal the complete set of timestamped fills
// from today. That proves the ticker has no pre-day history, so realized_pnl-fees is exactly today's
// value even after a partial close. Terminal markets are owned by settlement receipts. A reused or
// incomplete ticker fails closed instead of letting prior lifetime profit hide today's loss.
func kalshiFreshDayOpenFeeValues(day string, rows []kalshi.MarketPosition, fills []kalshi.Fill,
	finals map[string]kalshiLossSettlementFinal) (map[string]int64, error) {
	byTicker := make(map[string]kalshi.MarketPosition, len(rows))
	open := map[string]bool{}
	for _, row := range rows {
		ticker := strings.TrimSpace(row.Ticker)
		if ticker == "" {
			return nil, errors.New("Kalshi total-traded position has no ticker")
		}
		byTicker[ticker] = row
		if row.PositionQty() != 0 {
			open[ticker] = true
		}
	}
	fees, todayNotional, fillCount := map[string]float64{}, map[string]float64{}, map[string]int{}
	seenFill := map[string]bool{}
	for _, fill := range fills {
		at, err := liveLossReceiptTime(fill.CreatedTime)
		if err != nil {
			return nil, fmt.Errorf("Kalshi fill %s: %w", fill.FillID, err)
		}
		if liveLossDay(at) != day {
			continue
		}
		ticker := strings.TrimSpace(fill.Ticker)
		if ticker == "" || strings.TrimSpace(fill.FillID) == "" {
			return nil, errors.New("current-day Kalshi fill omitted ticker or fill_id")
		}
		if seenFill[fill.FillID] {
			return nil, fmt.Errorf("duplicate current-day Kalshi fill %s", fill.FillID)
		}
		seenFill[fill.FillID] = true
		if _, terminal := finals[ticker]; terminal {
			continue
		}
		if _, known := byTicker[ticker]; !known {
			return nil, fmt.Errorf("current-day Kalshi fill %s has no total-traded row", fill.FillID)
		}
		if !fill.FeeKnown() {
			return nil, fmt.Errorf("current-day Kalshi fill %s has no fee_cost truth", fill.FillID)
		}
		qty := fill.Qty()
		price := fill.YesPriceUSD()
		if strings.EqualFold(fill.SideYesNo(), "no") {
			price = fill.NoPriceUSD()
		}
		if qty <= 0 || price <= 0 || price >= 1 || math.IsNaN(qty) || math.IsInf(qty, 0) ||
			math.IsNaN(price) || math.IsInf(price, 0) {
			return nil, fmt.Errorf("current-day Kalshi fill %s has invalid quantity/side price", fill.FillID)
		}
		fees[ticker] += fill.FeeUSD()
		todayNotional[ticker] += qty * price
		fillCount[ticker]++
	}
	out := map[string]int64{}
	for ticker, row := range byTicker {
		if _, terminal := finals[ticker]; terminal {
			continue
		}
		if fillCount[ticker] == 0 {
			continue
		}
		if math.Abs(row.TradedUSDs.Float()-todayNotional[ticker]) > 0.000001 {
			return nil, fmt.Errorf("current-day Kalshi ticker %s has pre-day or incomplete lifetime notional", ticker)
		}
		if !row.FeesKnown() || math.Abs(row.FeesUSD()-fees[ticker]) > 0.000001 {
			return nil, fmt.Errorf("current-day Kalshi ticker %s cumulative fee does not match timestamped fills", ticker)
		}
		net := row.RealizedUSD() - row.FeesUSD()
		if math.IsNaN(net) || math.IsInf(net, 0) {
			return nil, fmt.Errorf("non-finite Kalshi current-day economics for %s", ticker)
		}
		out[ticker] = liveLossUSDToUnits(net)
	}
	return out, nil
}

func polyUSLossReceipts(day string, rows []polymarketus.PUSActivity, prior map[string]bool) ([]liveLossReceipt, error) {
	out := make([]liveLossReceipt, 0, len(rows))
	windowContinuous := len(rows) < liveLossPolyUSWindow
	overlapsPrior := false
	duplicates := map[string]int{}
	for _, row := range rows {
		at, err := liveLossReceiptTime(row.Time)
		if err != nil {
			return nil, err
		}
		rowDay := liveLossDay(at)
		if rowDay < day {
			windowContinuous = true
			continue
		}
		if rowDay > day {
			return nil, fmt.Errorf("future PolyUS activity receipt %s at %s", row.Slug, row.Time)
		}
		typ := strings.ToUpper(strings.TrimSpace(row.Type))
		if typ != "TRADE" && typ != "POSITION_RESOLUTION" {
			continue
		}
		// A trade fee is realized cash even when the trade itself realizes no prior position. Missing
		// commission presence is not silently treated as zero; that would overstate compounding and
		// understate the hard loss rail.
		if typ == "TRADE" && !row.CommissionKnown {
			return nil, fmt.Errorf("PolyUS trade receipt %s at %s has no commission truth", row.Slug, row.Time)
		}
		delta := row.PnL
		if typ == "TRADE" {
			delta -= row.CommissionUSD
		}
		if math.IsNaN(delta) || math.IsInf(delta, 0) {
			return nil, fmt.Errorf("non-finite PolyUS activity economics for %s", row.Slug)
		}
		base := strings.Join([]string{"polyus", typ, row.Slug, row.Time,
			strconv.FormatFloat(row.Price, 'g', -1, 64), strconv.FormatFloat(row.Qty, 'g', -1, 64),
			strconv.FormatFloat(row.PnL, 'g', -1, 64), strconv.FormatBool(row.CommissionKnown),
			strconv.FormatFloat(row.CommissionUSD, 'g', -1, 64)}, "\x1f")
		// Retail activity payloads do not expose a stable activity id. Preserve multiplicity when two
		// genuinely identical fills share a timestamp by assigning a deterministic occurrence ordinal.
		ordinal := duplicates[base]
		duplicates[base] = ordinal + 1
		id := liveLossReceiptID(base, strconv.Itoa(ordinal))
		if prior[id] {
			overlapsPrior = true
		}
		out = append(out, liveLossReceipt{ID: id, DeltaC: liveLossUSDToUnits(delta)})
	}
	if !windowContinuous && !overlapsPrior {
		return nil, fmt.Errorf("PolyUS activities window is full and has no persisted overlap; UTC-day continuity is unproved")
	}
	return out, nil
}

func (s *Server) readKalshiLossReceipts(ctx context.Context) ([]kalshi.MarketPosition, error) {
	if s.liveLossKalshiRead != nil {
		return s.liveLossKalshiRead(ctx)
	}
	if s.kal == nil || !s.kal.HasCredentials() {
		return nil, errors.New("Kalshi total-traded ledger unavailable: authenticated client missing")
	}
	return s.kal.GetTradedPositions(ctx)
}

func (s *Server) readKalshiLossSettlements(ctx context.Context) ([]kalshi.Settlement, bool, error) {
	if s.liveLossKalshiSettlementRead != nil {
		rows, err := s.liveLossKalshiSettlementRead(ctx)
		return rows, len(rows) < liveLossKalshiSettlementWindow, err
	}
	if s.kal == nil || !s.kal.HasCredentials() {
		return nil, false, errors.New("Kalshi settlement ledger unavailable: authenticated client missing")
	}
	// Settlement receipts are part of the normal daily-loss truth, not merely a repair for rows
	// that disappear from total_traded. A fill can open and settle entirely between two 30-second
	// position polls; one newest page plus durable overlap detects that case without a 10-page scan.
	s.liveMu.Lock()
	bootstrap := !s.liveLossBootV["kalshi-settlements"]
	s.liveMu.Unlock()
	limit := liveLossKalshiSettlementWindow
	if bootstrap {
		limit = liveLossKalshiBootstrapWindow
	}
	return s.kal.GetSettlementsWindow(ctx, limit)
}

func (s *Server) readKalshiLossFills(ctx context.Context) ([]kalshi.Fill, error) {
	if s.liveLossKalshiFillRead != nil {
		return s.liveLossKalshiFillRead(ctx)
	}
	if s.kal == nil || !s.kal.HasCredentials() {
		return nil, errors.New("Kalshi fill ledger unavailable: authenticated client missing")
	}
	return s.getKalshiFillsObserved(ctx)
}

func (s *Server) readPolyUSLossReceipts(ctx context.Context) ([]polymarketus.PUSActivity, error) {
	if s.liveLossPolyUSRead != nil {
		return s.liveLossPolyUSRead(ctx)
	}
	// Reuse only a separately timestamped successful activities response. pusLiveAt advances even
	// when one member of the REST trio fails, so it is deliberately not money authority here.
	s.pusLiveMu.Lock()
	if !s.pusHistAt.IsZero() && time.Since(s.pusHistAt) <= 45*time.Second {
		rows := append([]polymarketus.PUSActivity(nil), s.pusHistC...)
		s.pusLiveMu.Unlock()
		return rows, nil
	}
	s.pusLiveMu.Unlock()
	if s.polyUSAuth == nil {
		return nil, errors.New("PolyUS activity ledger unavailable: authenticated client missing")
	}
	rows, err := s.polyUSAuth.Activities(ctx)
	if err != nil {
		s.pusLiveMu.Lock()
		s.pusHistErrC = err.Error()
		if strings.Contains(err.Error(), "429") {
			s.pusHist429Til = time.Now().Add(2 * time.Minute)
		}
		s.pusLiveMu.Unlock()
		return nil, err
	}
	s.pusLiveMu.Lock()
	s.pusHistC = append([]polymarketus.PUSActivity(nil), rows...)
	s.pusHistAt = time.Now()
	s.pusHistErrC = ""
	s.pusLiveMu.Unlock()
	return rows, nil
}

func (s *Server) prepareLiveLossLedgerForArm() {
	day := liveLossDay(time.Now())
	s.liveMu.Lock()
	if s.liveLossDay != day || !s.liveLossHasBase {
		s.liveLossDay = day
		// The configured rail is a UTC-day loss cap, not a since-ARM cap. A missing/corrupt KV or
		// first deployment must therefore rebuild from midnight rather than forgive losses already
		// realized earlier today.
		s.liveLossEpochAt = liveLossDayStart(day)
		s.liveLossBaseC = 0
		s.liveLossBaseV = nil
		s.liveLossDeltaV = map[string]int64{"kalshi": 0, "polyus": 0}
		s.liveLossSeenV = map[string]map[string]bool{"kalshi": {}, "polyus": {}}
		s.liveLossLastV = map[string]map[string]int64{"kalshi": {}, "polyus": {}}
		s.liveLossFinalV = map[string]map[string]int64{"kalshi": {}, "polyus": {}}
		s.liveLossBootV = map[string]bool{}
		s.liveLossHasBase = true
	}
	s.liveLossTruthV = map[string]bool{"kalshi": false, "polyus": false}
	s.liveLossTruthAtV = map[string]time.Time{}
	s.liveLossErrV = map[string]string{
		"kalshi": "awaiting fresh authoritative total-traded cumulative read",
		"polyus": "awaiting fresh authoritative activity receipt read",
	}
	s.liveLossCheckAt = time.Time{}
	s.liveMu.Unlock()
	if err := s.persistLiveLossLedger(context.Background()); err != nil {
		s.markLiveLossUnavailable("kalshi", fmt.Errorf("persist daily-loss ledger: %w", err))
		s.markLiveLossUnavailable("polyus", fmt.Errorf("persist daily-loss ledger: %w", err))
	}
}

func (s *Server) markLiveLossUnavailable(venue string, err error) {
	s.liveMu.Lock()
	if s.liveLossTruthV == nil {
		s.liveLossTruthV = map[string]bool{}
	}
	if s.liveLossErrV == nil {
		s.liveLossErrV = map[string]string{}
	}
	s.liveLossTruthV[venue] = false
	if err != nil {
		s.liveLossErrV[venue] = err.Error()
	} else {
		s.liveLossErrV[venue] = "authoritative daily-loss truth unavailable"
	}
	s.liveMu.Unlock()
}

func (s *Server) applyLiveLossReceipts(day, venue string, receipts []liveLossReceipt) {
	now := time.Now()
	s.liveMu.Lock()
	if s.liveLossDay != day || !s.liveLossHasBase {
		s.liveLossDay = day
		s.liveLossEpochAt = liveLossDayStart(day)
		s.liveLossBaseC = 0
		s.liveLossBaseV = nil
		s.liveLossDeltaV = map[string]int64{"kalshi": 0, "polyus": 0}
		s.liveLossSeenV = map[string]map[string]bool{"kalshi": {}, "polyus": {}}
		s.liveLossLastV = map[string]map[string]int64{"kalshi": {}, "polyus": {}}
		s.liveLossFinalV = map[string]map[string]int64{"kalshi": {}, "polyus": {}}
		s.liveLossBootV = map[string]bool{}
		s.liveLossHasBase = true
	}
	if s.liveLossDeltaV == nil {
		s.liveLossDeltaV = map[string]int64{}
	}
	if s.liveLossSeenV == nil {
		s.liveLossSeenV = map[string]map[string]bool{}
	}
	if s.liveLossSeenV[venue] == nil {
		s.liveLossSeenV[venue] = map[string]bool{}
	}
	for _, receipt := range receipts {
		if receipt.ID == "" || s.liveLossSeenV[venue][receipt.ID] {
			continue
		}
		s.liveLossSeenV[venue][receipt.ID] = true
		s.liveLossDeltaV[venue] += receipt.DeltaC
	}
	if s.liveLossTruthV == nil {
		s.liveLossTruthV = map[string]bool{}
	}
	if s.liveLossTruthAtV == nil {
		s.liveLossTruthAtV = map[string]time.Time{}
	}
	if s.liveLossErrV == nil {
		s.liveLossErrV = map[string]string{}
	}
	s.liveLossTruthV[venue] = true
	s.liveLossTruthAtV[venue] = now
	delete(s.liveLossErrV, venue)
	s.liveMu.Unlock()
}

func (s *Server) applyKalshiCumulativeLoss(day string, current map[string]int64) error {
	return s.applyKalshiCumulativeLossBootstrap(day, current, nil)
}

func (s *Server) applyKalshiCumulativeLossBootstrap(day string, current, currentDayBootstrap map[string]int64) error {
	now := time.Now()
	s.liveMu.Lock()
	defer s.liveMu.Unlock()
	if s.liveLossDay != day || !s.liveLossHasBase {
		s.liveLossDay = day
		s.liveLossEpochAt = liveLossDayStart(day)
		s.liveLossBaseC = 0
		s.liveLossBaseV = nil
		s.liveLossDeltaV = map[string]int64{"kalshi": 0, "polyus": 0}
		s.liveLossSeenV = map[string]map[string]bool{"kalshi": {}, "polyus": {}}
		s.liveLossLastV = map[string]map[string]int64{"kalshi": {}, "polyus": {}}
		s.liveLossFinalV = map[string]map[string]int64{"kalshi": {}, "polyus": {}}
		s.liveLossBootV = map[string]bool{}
		s.liveLossHasBase = true
	}
	if s.liveLossLastV == nil {
		s.liveLossLastV = map[string]map[string]int64{}
	}
	if s.liveLossFinalV == nil {
		s.liveLossFinalV = map[string]map[string]int64{}
	}
	if s.liveLossFinalV["kalshi"] == nil {
		s.liveLossFinalV["kalshi"] = map[string]int64{}
	}
	if s.liveLossBootV == nil {
		s.liveLossBootV = map[string]bool{}
	}
	// Settlement truth owns terminal markets even on the first cumulative baseline. This prevents
	// a current-day pre-ARM settlement from entering both FinalV and LastV, then looking like a
	// discontinuity on the next poll.
	for ticker := range s.liveLossFinalV["kalshi"] {
		delete(current, ticker)
	}
	prior := s.liveLossLastV["kalshi"]
	if !s.liveLossBootV["kalshi"] {
		// The first cumulative read is a watermark, never today's P&L. total_traded is an
		// ever-traded lifetime value; summing it during a migration can charge prior-day history
		// to the current daily rail. Version migrations preserve the prior watermark/delta and
		// rebuild missing current-day terminal rows from authenticated settlements instead.
		delete(s.liveLossBootV, "kalshi-day-rebuild")
		var delta int64
		for ticker, cents := range currentDayBootstrap {
			if _, finalized := s.liveLossFinalV["kalshi"][ticker]; !finalized {
				delta += cents
			}
		}
		if s.liveLossDeltaV == nil {
			s.liveLossDeltaV = map[string]int64{}
		}
		s.liveLossDeltaV["kalshi"] += delta
		s.liveLossLastV["kalshi"] = cloneInt64Map(current)
		s.liveLossBootV["kalshi"] = true
	} else {
		// A finalized row is allowed to remain briefly in total_traded after its authenticated
		// settlement, or disappear immediately. Settlement truth owns it in either case; ignoring
		// the stale cumulative echo prevents the final value being counted twice.
		// Any other missing row still proves an incomplete snapshot. The caller gets one chance to
		// supply the exact authenticated settlement before entering this function.
		for ticker := range prior {
			if _, finalized := s.liveLossFinalV["kalshi"][ticker]; finalized {
				continue
			}
			if _, ok := current[ticker]; !ok {
				return fmt.Errorf("Kalshi total-traded ledger lost prior market %s; refusing a partial snapshot", ticker)
			}
		}
		var delta int64
		for ticker, cents := range current {
			before, existed := prior[ticker]
			if existed {
				delta += cents - before
			} else {
				// A newly traded market appeared after today's baseline. Its cumulative fee/net value
				// begins at zero for this ledger and must not be silently baselined away.
				delta += cents
			}
		}
		if s.liveLossDeltaV == nil {
			s.liveLossDeltaV = map[string]int64{}
		}
		s.liveLossDeltaV["kalshi"] += delta
		s.liveLossLastV["kalshi"] = cloneInt64Map(current)
	}
	if s.liveLossTruthV == nil {
		s.liveLossTruthV = map[string]bool{}
	}
	if s.liveLossTruthAtV == nil {
		s.liveLossTruthAtV = map[string]time.Time{}
	}
	if s.liveLossErrV == nil {
		s.liveLossErrV = map[string]string{}
	}
	s.liveLossTruthV["kalshi"] = true
	s.liveLossTruthAtV["kalshi"] = now
	delete(s.liveLossErrV, "kalshi")
	return nil
}

type kalshiLossSettlementFinal struct {
	Cents int64
	Day   string
	At    time.Time
}

// kalshiLossSettlementWindow decodes current-day terminal receipts and says whether the returned
// page itself reaches an older day/end-of-list. A full current-day page is still continuous after
// startup when it overlaps any durable receipt from the preceding poll.
func kalshiLossSettlementWindow(day string, rows []kalshi.Settlement, endpointComplete bool) (map[string]kalshiLossSettlementFinal, bool, error) {
	out := make(map[string]kalshiLossSettlementFinal)
	windowComplete := endpointComplete
	for _, row := range rows {
		ticker := strings.TrimSpace(row.Ticker)
		if ticker == "" {
			return nil, false, errors.New("Kalshi settlement has no ticker")
		}
		at, err := liveLossReceiptTime(row.SettledTime)
		if err != nil {
			return nil, false, fmt.Errorf("Kalshi settlement %s: %w", ticker, err)
		}
		rowDay := liveLossDay(at)
		if rowDay < day {
			windowComplete = true
			continue
		}
		if rowDay > day {
			return nil, false, fmt.Errorf("future Kalshi settlement %s at %s", ticker, row.SettledTime)
		}
		if !row.FeeKnown() {
			return nil, false, fmt.Errorf("Kalshi settlement %s has no fee_cost truth", ticker)
		}
		cost := row.YesTotalCost.Float() + row.NoTotalCost.Float()
		net := row.RevenueUSD() - cost - row.FeeUSD()
		if math.IsNaN(net) || math.IsInf(net, 0) {
			return nil, false, fmt.Errorf("non-finite Kalshi settlement economics for %s", ticker)
		}
		final := kalshiLossSettlementFinal{Cents: liveLossUSDToUnits(net), Day: rowDay, At: at}
		if prior, exists := out[ticker]; exists && prior != final {
			return nil, false, fmt.Errorf("conflicting Kalshi settlement receipts for %s", ticker)
		}
		out[ticker] = final
	}
	return out, windowComplete, nil
}

// applyKalshiSettlementWindow owns the terminal side of the Kalshi daily-loss ledger. The first
// authenticated window rebuilds every settlement from the current UTC day, including activity
// before ARM. Later unseen receipts add final-minus-last-cumulative, or the entire final result when
// a position opened and settled between polls. FinalV is both the durable dedupe set and the
// tombstone that prevents a lagging total_traded echo from counting the same market twice.
func (s *Server) applyKalshiSettlementWindow(day string, finals map[string]kalshiLossSettlementFinal, windowComplete bool) error {
	s.liveMu.Lock()
	defer s.liveMu.Unlock()
	if s.liveLossDay != day || !s.liveLossHasBase {
		return errors.New("Kalshi settlement window has no matching daily-loss baseline")
	}
	if s.liveLossFinalV == nil {
		s.liveLossFinalV = map[string]map[string]int64{}
	}
	if s.liveLossFinalV["kalshi"] == nil {
		s.liveLossFinalV["kalshi"] = map[string]int64{}
	}
	if s.liveLossLastV == nil {
		s.liveLossLastV = map[string]map[string]int64{}
	}
	if s.liveLossLastV["kalshi"] == nil {
		s.liveLossLastV["kalshi"] = map[string]int64{}
	}
	if s.liveLossBootV == nil {
		s.liveLossBootV = map[string]bool{}
	}
	const settlementBootKey = "kalshi-settlements"
	if !s.liveLossBootV[settlementBootKey] {
		if !windowComplete {
			return errors.New("initial Kalshi settlement rebuild did not reach the end of the UTC-day window")
		}
		var delta int64
		for ticker, final := range finals {
			if prior, seen := s.liveLossFinalV["kalshi"][ticker]; seen {
				if prior != final.Cents {
					return fmt.Errorf("Kalshi settlement final changed for %s (%+.6f -> %+.6f USD)", ticker,
						liveLossUnitsToUSD(prior), liveLossUnitsToUSD(final.Cents))
				}
				delete(s.liveLossLastV["kalshi"], ticker)
				continue
			}
			before := s.liveLossLastV["kalshi"][ticker]
			if !s.liveLossEpochAt.IsZero() && !final.At.Before(s.liveLossEpochAt) {
				delta += final.Cents - before
			}
			s.liveLossFinalV["kalshi"][ticker] = final.Cents
			delete(s.liveLossLastV["kalshi"], ticker)
		}
		if s.liveLossDeltaV == nil {
			s.liveLossDeltaV = map[string]int64{}
		}
		s.liveLossDeltaV["kalshi"] += delta
		s.liveLossBootV[settlementBootKey] = true
		return nil
	}
	overlap := false
	for ticker, final := range finals {
		if prior, seen := s.liveLossFinalV["kalshi"][ticker]; seen {
			overlap = true
			if prior != final.Cents {
				return fmt.Errorf("Kalshi settlement final changed for %s (%+.6f -> %+.6f USD)", ticker,
					liveLossUnitsToUSD(prior), liveLossUnitsToUSD(final.Cents))
			}
		}
	}
	if !windowComplete && !overlap {
		return errors.New("Kalshi settlement window is full and has no durable overlap; UTC-day continuity is unproved")
	}
	var delta int64
	for ticker, final := range finals {
		if _, seen := s.liveLossFinalV["kalshi"][ticker]; seen {
			continue
		}
		before := s.liveLossLastV["kalshi"][ticker]
		delta += final.Cents - before
		s.liveLossFinalV["kalshi"][ticker] = final.Cents
		delete(s.liveLossLastV["kalshi"], ticker)
	}
	if s.liveLossDeltaV == nil {
		s.liveLossDeltaV = map[string]int64{}
	}
	s.liveLossDeltaV["kalshi"] += delta
	return nil
}

func kalshiLossSettlementFinals(rows []kalshi.Settlement, wanted []string) (map[string]kalshiLossSettlementFinal, error) {
	want := make(map[string]bool, len(wanted))
	for _, ticker := range wanted {
		want[strings.TrimSpace(ticker)] = true
	}
	out := make(map[string]kalshiLossSettlementFinal, len(want))
	for _, row := range rows {
		ticker := strings.TrimSpace(row.Ticker)
		if !want[ticker] {
			continue
		}
		if ticker == "" {
			return nil, errors.New("Kalshi settlement has no ticker")
		}
		at, err := liveLossReceiptTime(row.SettledTime)
		if err != nil {
			return nil, fmt.Errorf("Kalshi settlement %s: %w", ticker, err)
		}
		if !row.FeeKnown() {
			return nil, fmt.Errorf("Kalshi settlement %s has no fee_cost truth", ticker)
		}
		cost := row.YesTotalCost.Float() + row.NoTotalCost.Float()
		net := row.RevenueUSD() - cost - row.FeeUSD()
		if math.IsNaN(net) || math.IsInf(net, 0) {
			return nil, fmt.Errorf("non-finite Kalshi settlement economics for %s", ticker)
		}
		final := kalshiLossSettlementFinal{Cents: liveLossUSDToUnits(net), Day: liveLossDay(at), At: at}
		if prior, exists := out[ticker]; exists && prior != final {
			return nil, fmt.Errorf("conflicting Kalshi settlement receipts for %s", ticker)
		}
		out[ticker] = final
	}
	return out, nil
}

// kalshiMissingCumulativeTickers identifies only non-final rows. It is a read-only preflight used
// to avoid polling the settlement endpoint on healthy cycles.
func (s *Server) kalshiMissingCumulativeTickers(current map[string]int64) []string {
	s.liveMu.Lock()
	defer s.liveMu.Unlock()
	if !s.liveLossBootV["kalshi"] {
		return nil
	}
	final := s.liveLossFinalV["kalshi"]
	missing := make([]string, 0)
	for ticker := range s.liveLossLastV["kalshi"] {
		if _, ok := current[ticker]; ok {
			continue
		}
		if _, done := final[ticker]; done {
			continue
		}
		missing = append(missing, ticker)
	}
	sort.Strings(missing)
	return missing
}

// applyKalshiSettlementFinals replaces a disappearing cumulative position with its authenticated,
// fee-known terminal receipt exactly once. A prior-day receipt may tombstone a baseline row only
// when it agrees exactly; it cannot manufacture money in today's loss window.
func (s *Server) applyKalshiSettlementFinals(day string, missing []string, finals map[string]kalshiLossSettlementFinal) error {
	if len(missing) == 0 {
		return nil
	}
	s.liveMu.Lock()
	defer s.liveMu.Unlock()
	if s.liveLossDay != day || !s.liveLossHasBase {
		return errors.New("Kalshi settlement finalization has no matching daily-loss baseline")
	}
	if s.liveLossFinalV == nil {
		s.liveLossFinalV = map[string]map[string]int64{}
	}
	if s.liveLossFinalV["kalshi"] == nil {
		s.liveLossFinalV["kalshi"] = map[string]int64{}
	}
	prior := s.liveLossLastV["kalshi"]
	var delta int64
	for _, ticker := range missing {
		before, tracked := prior[ticker]
		if !tracked {
			continue
		}
		final, ok := finals[ticker]
		if !ok {
			return fmt.Errorf("Kalshi cumulative row %s disappeared without an authenticated settlement receipt", ticker)
		}
		if final.Day != day && final.Cents != before {
			return fmt.Errorf("Kalshi prior-day settlement %s changed today's cumulative value (%+.6f -> %+.6f USD)", ticker,
				liveLossUnitsToUSD(before), liveLossUnitsToUSD(final.Cents))
		}
		if existing, done := s.liveLossFinalV["kalshi"][ticker]; done && existing != final.Cents {
			return fmt.Errorf("Kalshi settlement final changed for %s (%+.6f -> %+.6f USD)", ticker,
				liveLossUnitsToUSD(existing), liveLossUnitsToUSD(final.Cents))
		}
		if final.Day == day {
			delta += final.Cents - before
		}
		s.liveLossFinalV["kalshi"][ticker] = final.Cents
		delete(prior, ticker)
	}
	if s.liveLossDeltaV == nil {
		s.liveLossDeltaV = map[string]int64{}
	}
	s.liveLossDeltaV["kalshi"] += delta
	return nil
}

func cloneInt64Map(in map[string]int64) map[string]int64 {
	out := make(map[string]int64, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

// refreshLiveLossLedger performs the authoritative venue reads and durable reconciliation without
// granting any order authority. Keeping it independent from ARM lets a restarted process rebuild
// today's loss before the first real-money write can become possible.
func (s *Server) refreshLiveLossLedger(ctx context.Context) float64 {
	day := liveLossDay(time.Now())
	// Read independently and concurrently: a hung/failed venue cannot suppress the other venue's
	// fresh receipt truth. Each gets its own bounded child context.
	type result struct {
		venue string
		kal   []kalshi.MarketPosition
		pus   []polymarketus.PUSActivity
		err   error
	}
	results := make(chan result, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		cctx, cancel := context.WithTimeout(ctx, 12*time.Second)
		defer cancel()
		rows, err := s.readKalshiLossReceipts(cctx)
		results <- result{venue: "kalshi", kal: rows, err: err}
	}()
	go func() {
		defer wg.Done()
		cctx, cancel := context.WithTimeout(ctx, 12*time.Second)
		defer cancel()
		rows, err := s.readPolyUSLossReceipts(cctx)
		results <- result{venue: "polyus", pus: rows, err: err}
	}()
	wg.Wait()
	close(results)

	for got := range results {
		if got.err != nil {
			s.markLiveLossUnavailable(got.venue, got.err)
			continue
		}
		if got.venue == "kalshi" {
			var values map[string]int64
			values, got.err = kalshiCumulativeLossValues(got.kal)
			var currentDayBootstrap map[string]int64
			s.liveMu.Lock()
			needsCumulativeBootstrap := !s.liveLossBootV["kalshi"]
			s.liveMu.Unlock()
			var finals map[string]kalshiLossSettlementFinal
			if got.err == nil {
				cctx, cancel := context.WithTimeout(ctx, 12*time.Second)
				var settlements []kalshi.Settlement
				var endpointComplete bool
				settlements, endpointComplete, got.err = s.readKalshiLossSettlements(cctx)
				cancel()
				if got.err == nil {
					var complete bool
					finals, complete, got.err = kalshiLossSettlementWindow(day, settlements, endpointComplete)
					if got.err == nil {
						got.err = s.applyKalshiSettlementWindow(day, finals, complete)
					}
				}
			}
			if got.err == nil && needsCumulativeBootstrap {
				cctx, cancel := context.WithTimeout(ctx, 12*time.Second)
				var fills []kalshi.Fill
				fills, got.err = s.readKalshiLossFills(cctx)
				cancel()
				if got.err == nil {
					currentDayBootstrap, got.err = kalshiFreshDayOpenFeeValues(day, got.kal, fills, finals)
				}
			}
			if got.err == nil {
				got.err = s.applyKalshiCumulativeLossBootstrap(day, values, currentDayBootstrap)
			}
		} else {
			prior := s.liveLossSeenCopy(got.venue)
			var receipts []liveLossReceipt
			receipts, got.err = polyUSLossReceipts(day, got.pus, prior)
			if got.err == nil {
				s.applyLiveLossReceipts(day, got.venue, receipts)
			}
		}
		if got.err != nil {
			s.markLiveLossUnavailable(got.venue, got.err)
			continue
		}
	}
	if err := s.persistLiveLossLedger(context.Background()); err != nil {
		// Without a durable watermark, a restart could forget today's losses or double-count
		// receipts. Both money paths therefore fail closed until a later poll persists cleanly.
		s.markLiveLossUnavailable("kalshi", fmt.Errorf("persist daily-loss ledger: %w", err))
		s.markLiveLossUnavailable("polyus", fmt.Errorf("persist daily-loss ledger: %w", err))
	}

	s.liveMu.Lock()
	deltaC := s.liveLossDeltaV["kalshi"] + s.liveLossDeltaV["polyus"]
	s.liveMu.Unlock()
	return liveLossUnitsToUSD(deltaC)
}

func (s *Server) pollLiveLossLedger(ctx context.Context) {
	r := s.cfg().Risk
	maxDay, limitOK := liveRailLimit(s.liveArmTotalBankroll(), r.LiveDailyLossPct, r.LiveMaxDailyLossUSD)
	if !limitOK || s.ksBlocked() {
		return
	}
	s.liveMu.Lock()
	armed := s.liveArmed
	throttled := time.Since(s.liveLossCheckAt) < 30*time.Second
	if !throttled {
		s.liveLossCheckAt = time.Now()
	}
	s.liveMu.Unlock()
	if !armed || throttled {
		return
	}

	rawDeltaUSD := s.refreshLiveLossLedger(ctx)
	deltaUSD, period := s.liveLossAfterOperatorReset(rawDeltaUSD)
	if deltaUSD <= -maxDay {
		s.setLiveAutoSafetyPause("daily-loss", fmt.Sprintf("durable fee-net realized %+.2f USD %s breaches active loss rail %.2f", deltaUSD, period, maxDay))
	} else {
		s.clearLiveAutoSafetyPause("daily-loss", "authoritative daily fee-net loss is back inside the configured rail")
	}
}

// liveLossAfterOperatorReset keeps the venue's immutable UTC-day history while allowing an
// explicit intraday risk reset. On the reset date, admission uses only the change after the
// captured day-P&L receipt. At the next UTC date the ordinary midnight reset takes over.
func (s *Server) liveLossAfterOperatorReset(dayPnL float64) (float64, string) {
	r := s.cfg().Risk
	at, err := time.Parse(time.RFC3339, r.LiveRiskEpochAt)
	if err == nil && at.UTC().Format("2006-01-02") == time.Now().UTC().Format("2006-01-02") {
		return dayPnL - r.LiveRiskEpochDayPnLUSD, "since the operator risk reset"
	}
	return dayPnL, "today"
}

// preArmLiveLossReason runs after authenticated bankroll reads but before either venue's write
// gate is armed. It closes the restart window where an already-breached durable day could otherwise
// enable AUTO before the normal 30-second poll.
func (s *Server) preArmLiveLossReason(ctx context.Context, bankroll float64, requiredVenues []string) string {
	r := s.cfg().Risk
	maxDay, ok := liveRailLimit(bankroll, r.LiveDailyLossPct, r.LiveMaxDailyLossUSD)
	if !ok {
		return "daily-loss rail cannot be calculated from the authenticated ARM bankroll"
	}
	rawDeltaUSD := s.refreshLiveLossLedger(ctx)
	deltaUSD, period := s.liveLossAfterOperatorReset(rawDeltaUSD)
	for _, venue := range requiredVenues {
		if why := s.liveLossTruthReason(venue); why != "" {
			return why
		}
	}
	if deltaUSD <= -maxDay {
		reason := fmt.Sprintf("durable fee-net result %+.2f USD %s breaches the %.2f USD daily-loss rail", deltaUSD, period, maxDay)
		s.setLiveAutoSafetyPause("daily-loss", reason)
		return reason
	}
	s.clearLiveAutoSafetyPause("daily-loss", "pre-ARM authoritative daily fee-net result is inside the configured rail")
	return ""
}

func (s *Server) liveLossTruthReason(venue string) string {
	venue = strings.ToLower(strings.TrimSpace(venue))
	s.liveMu.Lock()
	initialized := s.liveLossHasBase
	ok, at, why := s.liveLossTruthV[venue], s.liveLossTruthAtV[venue], s.liveLossErrV[venue]
	s.liveMu.Unlock()
	if !initialized {
		return fmt.Sprintf("%s live daily-loss ledger is not initialized", venue)
	}
	if ok && !at.IsZero() && time.Since(at) <= liveLossTruthMaxAge {
		return ""
	}
	if why == "" {
		if ok {
			why = fmt.Sprintf("last authoritative read is %s old", time.Since(at).Round(time.Second))
		} else {
			why = "authoritative daily-loss receipt read has not succeeded"
		}
	}
	return fmt.Sprintf("%s live daily-loss truth unavailable (%s)", venue, why)
}

// liveLossVenueGate is applied only to production-style armed sessions that initialized the
// durable ledger. Bare unit-test Servers that directly flip liveArmed without the ARM handshake
// retain their historical test semantics; the real ARM path always calls prepareLiveLossLedgerForArm.
func (s *Server) liveLossVenueGate(venue string) string {
	venue = strings.ToLower(strings.TrimSpace(venue))
	s.liveMu.Lock()
	armed, initialized := s.liveArmed, s.liveLossHasBase
	s.liveMu.Unlock()
	if !armed || !initialized {
		return ""
	}
	why := s.liveLossTruthReason(venue)
	if why == "" {
		return ""
	}
	return fmt.Sprintf("%s — %s orders fail-closed; the other venue remains independent", why, venue)
}
