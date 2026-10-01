package server

// R165 GENERIC SIGNAL FOLLOWER — corrected Paper research collector.
//
// Historical UnitTrial, Paper, replay, model, and visible-book rows are hypotheses only. Their
// reported sign or magnitude cannot add/remove a route, resize a probe, flip a side, promote a
// system, or authorize LIVE. Every eligible exact (family, venue, side, taker-route) detector lane
// instead gets the same one-share corrected-Paper probe. Each probe must pass current complete-book,
// fee, time-window, conflict, and delayed final-book checks, and it records an explicit modeled IOC
// fill or zero-fill. A dedicated executor still owns its own lane so the generic follower does not
// duplicate it. Venue-locked and ML-only rows remain research-only.
//
// Only a separately tagged, route-exact, fill-conditioned authenticated exchange cohort may later
// affect allocation or defeat its own route. No such current cohort authorizes automatic cash in
// R165: all model/Paper-to-LIVE bridges are hard retired. Any opposite-side idea must be a separately
// named system that reads the real opposite book; algebraic 1-price inversion has no order authority.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/paper"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

const (
	gfSpreadCapC      = 6.0  // sane-spread screen (cheapband precedent)
	gfMaxOpenPerSub   = 100  // runaway guard per follower sub
	gfPriceLo         = 0.01 // follow only real two-sided prices
	gfPriceHi         = 0.97
	gfPaperPointState = "HYPOTHESIS ONLY" // old assumed-fill point may nominate research, never size it
	gfTurnoverMinMult = 1.00              // small C4 nudge; current c/ct remains the primary allocator
	gfTurnoverMaxMult = 1.08
)

// gfDedicatedDirectExpression reports only a COMPLETE dedicated expression for this exact
// family+venue. It must not be a family-wide approximation: xvgap's specialist is PolyUS-only;
// FavLong80 covers only one Kalshi price band; weather/kflow are frozen; and legacy "arb" signal
// rows are a directional cheap-side study, not the separately implemented two-leg lock. Those
// partial/frozen/different expressions stay in the generic follower so no positive scoreboard
// SIGNAL silently loses its PAPER route (and therefore its prospective LIVE candidate route).
func gfDedicatedDirectExpression(fam, platform string) bool {
	fam = strings.ToLower(strings.TrimSpace(fam))
	platform = strings.ToLower(strings.TrimSpace(platform))
	switch fam {
	case "xvgap":
		return platform == "polyus" // xvgapexec.go is exactly the PUS pocket
	case "freshlist":
		return platform == "polyus" // FreshInv's PUS lane follows fresh listings directly
	default:
		return false
	}
}

// gfDedicatedInvertExpression reports the two venue/family inversions already expressed by the
// venue-split FreshInv book. Every other qualifying inversion — including inversions of xvgap,
// weather, kflow, favlong and arb — gets its own generic-follower sub-strategy.
func gfDedicatedInvertExpression(fam, platform string) bool {
	// Only Kalshi's tagged FreshInv NO book owns this exact identity. PolyUS FreshInv lots express
	// direct freshlist YES; a separate invert:freshfade route must keep its own generic ledger.
	return fam == "freshlist" && platform == "kalshi"
}

// gfDedicatedExpression recognizes both a direct family and an independently named inverse that
// is already fully expressed by a specialist. This prevents the shared follower from double
// funding FreshInv while leaving every other exact invert:<family> route eligible.
func gfDedicatedExpression(fam, platform string) bool {
	if base, inverse := strings.CutPrefix(strings.ToLower(strings.TrimSpace(fam)), "invert:"); inverse {
		return gfDedicatedInvertExpression(base, strings.ToLower(strings.TrimSpace(platform)))
	}
	return gfDedicatedDirectExpression(fam, platform)
}

// gfMLFamily is a defense-in-depth generic-follower exclusion. Current book-native-v2 ML rows have
// their own funded Paper portfolio and guarded future LIVE bridge; an accidental signal_log family
// spelling must not duplicate them in the venue books. Legacy ML remains ineligible everywhere.
func gfMLFamily(fam string) bool {
	f := strings.ToLower(strings.TrimSpace(fam))
	return f == "ml" || f == "auto-ml" || strings.HasPrefix(f, "ml-") || strings.HasPrefix(f, "ml:")
}

// gfBookState — one ledger file, one kfBook per roster key ("gf:<family>-k"/"-p").
type gfBookState struct {
	Subs map[string]*kfBook `json:"subs"`
}

type gfRouteAuditStamp struct {
	At          time.Time
	Fingerprint string
}

// gfRouteAuditDue keeps one durable receipt for every distinct direct or inverse decision state without
// writing the same unchanged 30-second retry hundreds of times. A new price, episode, rejection
// reason, or 10-minute signal-log window is a new receipt; accepted fills bypass this throttle.
func (s *Server) gfRouteAuditDue(key, fingerprint string, now time.Time) bool {
	s.gfInvAuditMu.Lock()
	defer s.gfInvAuditMu.Unlock()
	if s.gfInvAudit == nil {
		s.gfInvAudit = map[string]gfRouteAuditStamp{}
	}
	prior, found := s.gfInvAudit[key]
	if found && prior.Fingerprint == fingerprint && now.Sub(prior.At) < 10*time.Minute {
		return false
	}
	s.gfInvAudit[key] = gfRouteAuditStamp{At: now, Fingerprint: fingerprint}
	if len(s.gfInvAudit) > 12000 {
		s.gfInvAudit = map[string]gfRouteAuditStamp{key: {At: now, Fingerprint: fingerprint}}
	}
	return true
}

// genfollowOn — config kill (genfollow_enabled, nil/absent = ON).
func (s *Server) genfollowOn() bool {
	v := s.cfg().Auto.GenfollowEnabled
	return v == nil || *v
}

// gfLoadLocked lazy-loads genfollow_book.json. Caller holds gfBookMu (bug-52 poison guard).
func (s *Server) gfLoadLocked() *gfBookState {
	if s.gfBook != nil {
		return s.gfBook
	}
	b := &gfBookState{}
	path := filepath.Join(s.cfg().DataDir, "genfollow_book.json")
	if !s.readJSONLoose(path, b) {
		if st, err := os.Stat(path); err == nil && st.Size() > 0 {
			s.gfBookPoisoned.Store(true)
			s.log.Error("genfollow_book.json exists but failed to parse — follower flushes DISABLED this session (bug-52 guard)", "path", path, "size", st.Size())
			_ = s.store.Audit(context.Background(), "error", "genfollow", "Follower book file unreadable — flushes disabled this session; inspect data\\genfollow_book.json", "")
		}
	}
	if b.Subs == nil {
		b.Subs = map[string]*kfBook{}
	}
	s.gfBook = b
	return b
}

// genfollowFlush persists the ledger if dirty (atomic tmp+rename; poison-guarded; 454 rule).
func (s *Server) genfollowFlush() error {
	defer s.invalidatePortfolioEquityCache()
	s.gfBookMu.Lock()
	defer s.gfBookMu.Unlock()
	if s.gfBookPoisoned.Load() {
		return fmt.Errorf("follower ledger is poisoned")
	}
	if !s.gfBookDirty || s.gfBook == nil {
		return nil
	}
	out, err := json.Marshal(s.gfBook)
	if err != nil {
		return fmt.Errorf("marshal follower ledger: %w", err)
	}
	path := filepath.Join(s.cfg().DataDir, "genfollow_book.json")
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, out, 0o644); err != nil {
		return fmt.Errorf("write follower ledger temp: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("rename follower ledger: %w", err)
	}
	s.gfBookDirty = false
	return nil
}

// rollbackInverseGenfollowPlacement removes an uncommitted JSON projection before closing its
// durable journal.  A failed rollback flush poisons the executor: retaining an ambiguous lot is
// safer than allowing either another placement or settlement to treat it as committed money.
func (s *Server) rollbackInverseGenfollowPlacement(ctx context.Context, key string,
	p storage.InversePaperPlacement, reason string) error {
	if p.PlacementID == "" {
		return nil
	}
	s.gfBookMu.Lock()
	if h := s.gfLoadLocked().Subs[key]; h != nil {
		for i := len(h.Open) - 1; i >= 0; i-- {
			if h.Open[i].PlacementID == p.PlacementID {
				h.Open = append(h.Open[:i], h.Open[i+1:]...)
				s.gfBookDirty = true
				break
			}
		}
	}
	s.gfBookMu.Unlock()
	if err := s.genfollowFlush(); err != nil {
		s.gfBookPoisoned.Store(true)
		_, _ = s.store.QuarantineInversePaperPlacement(context.WithoutCancel(ctx), p.PlacementID,
			"JSON rollback persistence failed: "+reason)
		return err
	}
	_, err := s.store.RollbackInversePaperPlacement(context.WithoutCancel(ctx), p.PlacementID, reason)
	return err
}

func inversePaperPlacementMatchesLot(p storage.InversePaperPlacement, lot kfPos, family string) bool {
	platform := lot.Platform
	if platform == "" {
		platform = "kalshi"
	}
	feePC := 0.0
	if lot.Contracts > 0 {
		feePC = lot.Fee / lot.Contracts
	}
	return p.PlacementID != "" && p.PlacementID == lot.PlacementID &&
		strings.EqualFold(p.Platform, platform) && p.Ticker == lot.Ticker &&
		strings.EqualFold(p.Side, lot.Side) && strings.EqualFold(p.StrategyFamily, family) &&
		p.SignalTS == lot.SignalTS && p.DecisionTS == lot.DecisionTS &&
		math.Abs(p.FillPrice-lot.Price) < 1e-9 && math.Abs(p.FeePC-feePC) < 1e-9 &&
		math.Abs(p.Contracts-lot.Contracts) < 1e-9
}

// reconcileInverseGenfollowPlacements is both the boot repair and the pre-settlement gate. New
// inverse lots require an exact committed journal; an incomplete crash-window projection is
// removed before it can affect P&L. Legacy ID-less lots remain only when their exact historical
// signal-fill receipt validates. SQLite reads occur outside gfBookMu.
func (s *Server) reconcileInverseGenfollowPlacements(ctx context.Context) error {
	type lotRef struct {
		sub    string
		lot    kfPos
		closed bool
		pnl    float64
		won    bool
	}
	var refs []lotRef
	s.gfBookMu.Lock()
	if s.gfBookPoisoned.Load() {
		s.gfBookMu.Unlock()
		return errors.New("follower ledger is poisoned")
	}
	for sub, b := range s.gfLoadLocked().Subs {
		for _, lot := range b.Open {
			if strings.HasPrefix(strings.ToLower(lotFamilyTag(lot, "")), "invert:") {
				refs = append(refs, lotRef{sub: sub, lot: lot})
			}
		}
		for _, closed := range b.Closed {
			if strings.HasPrefix(strings.ToLower(lotFamilyTag(closed.kfPos, "")), "invert:") && closed.PlacementID != "" {
				refs = append(refs, lotRef{sub: sub, lot: closed.kfPos, closed: true, pnl: closed.PnL, won: closed.Won})
			}
		}
	}
	s.gfBookMu.Unlock()

	removeOpen, removeClosed := map[string]bool{}, map[string]bool{}
	present := map[string]bool{}
	settleIDs, quarantine := map[string]bool{}, map[string]string{}
	active, err := s.store.ActiveInversePaperPlacements(ctx)
	if err != nil {
		return err
	}
	activeByID := make(map[string]storage.InversePaperPlacement, len(active))
	for _, journal := range active {
		activeByID[journal.PlacementID] = journal
	}
	refKey := func(r lotRef) string {
		return r.sub + "\x00" + r.lot.Ticker + "\x00" + r.lot.Side + "\x00" +
			r.lot.TS + "\x00" + r.lot.PlacementID
	}
	for _, ref := range refs {
		family := lotFamilyTag(ref.lot, "")
		if ref.lot.PlacementID == "" {
			ok, err := s.store.SignalFamilyFillMatchesAt(ctx, firstNonEmpty(ref.lot.Platform, "kalshi"),
				ref.lot.Ticker, ref.lot.Side, family, ref.lot.Price, ref.lot.SignalTS, true)
			if err != nil {
				return err
			}
			if !ok {
				if ref.closed {
					removeClosed[refKey(ref)] = true
				} else {
					removeOpen[refKey(ref)] = true
				}
			}
			continue
		}
		present[ref.lot.PlacementID] = true
		journal, ok := activeByID[ref.lot.PlacementID]
		// A closed lot normally owns a terminal settled journal, so it deliberately drops out
		// of the active scan. Avoid rereading every historical close on every settlement tick.
		// Open lots can never be terminal and still remain valid, so an absent active row must
		// be distinguished from a missing/rolled-back/quarantined journal before it is trusted.
		if ref.closed && !ok {
			continue
		}
		if !ref.closed && !ok {
			var err error
			journal, ok, err = s.store.InversePaperPlacement(ctx, ref.lot.PlacementID)
			if err != nil {
				return err
			}
		}
		trusted := ok && inversePaperPlacementMatchesLot(journal, ref.lot, family)
		if ref.closed && trusted && (journal.State == "committed" || journal.State == "settled") {
			if journal.State == "committed" {
				settleIDs[journal.PlacementID] = true
			}
			continue
		}
		if !ref.closed && trusted && journal.State == "committed" {
			continue
		}
		if ref.closed {
			removeClosed[refKey(ref)] = true
		} else {
			removeOpen[refKey(ref)] = true
		}
		if ok && (journal.State == "prepared" || journal.State == "ledger_persisted" || journal.State == "committed") {
			quarantine[journal.PlacementID] = "JSON projection was not an exact committed inverse placement"
		}
	}
	for _, journal := range active {
		if !present[journal.PlacementID] {
			quarantine[journal.PlacementID] = "active inverse journal has no JSON open/closed projection"
		}
	}

	changed := len(removeOpen) > 0 || len(removeClosed) > 0
	if changed {
		s.gfBookMu.Lock()
		st := s.gfLoadLocked()
		for sub, b := range st.Subs {
			open := b.Open[:0]
			for _, lot := range b.Open {
				ref := lotRef{sub: sub, lot: lot}
				if removeOpen[refKey(ref)] {
					continue
				}
				open = append(open, lot)
			}
			b.Open = open
			closed := b.Closed[:0]
			corrected := false
			for _, lot := range b.Closed {
				ref := lotRef{sub: sub, lot: lot.kfPos, closed: true, pnl: lot.PnL, won: lot.Won}
				if removeClosed[refKey(ref)] {
					b.Net -= lot.PnL
					if lot.Won && b.Wins > 0 {
						b.Wins--
					} else if !lot.Won && b.Losses > 0 {
						b.Losses--
					}
					corrected = true
					continue
				}
				closed = append(closed, lot)
			}
			b.Closed = closed
			if corrected {
				b.Equity = append(b.Equity, [2]float64{float64(time.Now().Unix()), math.Round((b.Net-b.NetBase)*100) / 100})
			}
		}
		s.gfBookDirty = true
		s.gfBookMu.Unlock()
		if err := s.genfollowFlush(); err != nil {
			s.gfBookPoisoned.Store(true)
			return err
		}
	}
	for id, reason := range quarantine {
		_, _ = s.store.QuarantineInversePaperPlacement(context.WithoutCancel(ctx), id, reason)
		_ = s.store.Audit(context.WithoutCancel(ctx), "warn", "genfollow",
			"Quarantined incomplete inverse Paper placement", id+": "+reason)
	}
	for id := range settleIDs {
		if ok, err := s.store.SettleInversePaperPlacement(context.WithoutCancel(ctx), id); err != nil || !ok {
			return fmt.Errorf("settle inverse placement journal %s: ok=%v err=%w", id, ok, err)
		}
	}
	return nil
}

// reconcileGenfollowFundedRelationOutcomes repairs only outcomes backed by a retained closed
// generic-follower lot and its exact immutable receipt ID. It never rewrites the first outcome:
// storage appends a correction only when a generic contract sweep previously won the race. This
// is safe to repeat at boot and lets old affected rows recover without inventing missing history.
func (s *Server) reconcileGenfollowFundedRelationOutcomes(ctx context.Context) error {
	type closeReceipt struct {
		lot     kfPos
		pnl     float64
		settled time.Time
		source  string
	}
	var receipts []closeReceipt
	s.gfBookMu.Lock()
	if s.gfBookPoisoned.Load() {
		s.gfBookMu.Unlock()
		return errors.New("follower ledger is poisoned")
	}
	for _, book := range s.gfLoadLocked().Subs {
		for _, closed := range book.Closed {
			if closed.RelationReceiptID == "" {
				continue
			}
			settled, err := time.Parse(time.RFC3339Nano, closed.SettledTS)
			if err != nil {
				s.gfBookMu.Unlock()
				return fmt.Errorf("parse funded follower settlement %s: %w", closed.RelationReceiptID, err)
			}
			source := "genfollow-settlement"
			if closed.Reason == "stop-loss" {
				source = "genfollow-stop-loss"
			}
			receipts = append(receipts, closeReceipt{lot: closed.kfPos, pnl: closed.PnL,
				settled: settled, source: source})
		}
	}
	s.gfBookMu.Unlock()
	for _, receipt := range receipts {
		if _, _, err := s.store.ReconcileFundedSpecialistRelationOutcome(ctx,
			receipt.lot.RelationReceiptID, fiLotPlatform(receipt.lot), receipt.lot.Ticker,
			receipt.lot.Side, receipt.lot.Contracts, receipt.lot.Price, receipt.lot.Fee,
			receipt.settled, receipt.pnl, receipt.source); err != nil {
			return fmt.Errorf("reconcile funded follower outcome %s: %w",
				receipt.lot.RelationReceiptID, err)
		}
	}
	return nil
}

// genfollowWindingDown — disabled with lots still open: keep settling (268 doctrine).
func (s *Server) genfollowWindingDown() bool {
	s.gfBookMu.Lock()
	defer s.gfBookMu.Unlock()
	for _, b := range s.gfLoadLocked().Subs {
		if len(b.Open) > 0 {
			return true
		}
	}
	return false
}

// gfRosterKey — the follower sub's roster/verdict family key for one (family, venue).
func gfRosterKey(fam, platform, side, route string, inputTopology ...string) string {
	suffix := "-k"
	if platform == "polyus" {
		suffix = "-p"
	}
	key := "gf:" + strings.ToLower(strings.TrimSpace(fam)) + ":" +
		strings.ToLower(strings.TrimSpace(side)) + ":" + strings.ToLower(strings.TrimSpace(route))
	if len(inputTopology) > 0 {
		if topology := strings.ToLower(strings.TrimSpace(inputTopology[0])); topology != "" {
			key += ":" + topology
		}
	}
	return key + suffix
}

// gfHypothesisPoint preserves old observed-book estimates only for naming research candidates.
// These magnitudes are not profit evidence and may not size Paper or authorize LIVE.
func gfHypothesisPoint(v verdictEnt) (float64, int, bool) {
	if v.N > 0 && !math.IsNaN(v.Mean) && !math.IsInf(v.Mean, 0) {
		return v.Mean, v.Markets, true
	}
	if v.ContractMarkets > 0 && !math.IsNaN(v.PaperPointMean) && !math.IsInf(v.PaperPointMean, 0) {
		return v.PaperPointMean, v.ContractMarkets, true
	}
	return 0, 0, false
}

// gfPaperPoint is money-facing Paper evidence. Historical assumed-fill/unit-trial rows are
// explicitly void and therefore fail here; only a separately tagged profit-evidence cohort may
// provide a magnitude.
func gfPaperPoint(v verdictEnt) (float64, int, bool) {
	if !verdictHasAuthenticatedProfitEvidence(v) {
		return 0, 0, false
	}
	return gfHypothesisPoint(v)
}

// gfPaperPointCosts keeps the entry-price/fee baseline in the same cohort as gfPaperPoint. Mixing
// an event-cluster edge with pre-R144 contract costs (or the reverse) would misstate the current
// repricing penalty and could approve a Paper order for the wrong reason.
func gfPaperPointCosts(v verdictEnt) (meanAsk, meanFee float64) {
	if v.N > 0 {
		return v.MeanAsk, v.FeePC
	}
	return v.PaperPointMeanAsk, v.PaperPointMeanFeePC
}

// gfExactCell accepts only a prospective executable one-share cell. It never falls back to a
// signal-time percentage or manufactures the opposite side. Strategy-origin cells are returned
// for coverage, but their originating executor already owns placement and must not be duplicated.
func gfExactCell(v verdictEnt) (family, platform, side, origin, route string, ok bool) {
	if v.Group != "taker" && v.Group != "strategy" {
		return "", "", "", "", "", false
	}
	family = strings.ToLower(strings.TrimSpace(v.SourceFamily))
	platform = strings.ToLower(strings.TrimSpace(v.Platform))
	side = strings.ToUpper(strings.TrimSpace(v.Side))
	origin = strings.ToLower(strings.TrimSpace(v.OriginLayer))
	route = strings.ToLower(strings.TrimSpace(v.Route))
	if route == "" {
		route = "taker"
	}
	if origin == "" {
		if v.Group == "strategy" {
			origin = "strategy"
		} else {
			origin = "model"
		}
	}
	ok = family != "" && (platform == "kalshi" || platform == "polyus") &&
		(side == "YES" || side == "NO") && route == "taker"
	return
}

// gfVenueMode is retained for historical replay/tests only. Funded Paper no longer consumes this
// signal-level sign rule; exact route registration and authenticated evidence live below.
func gfVenueMode(v verdictEnt, fam, platform string) (inverted bool, score venueVerdict, ok bool) {
	if fam == "" || v.Group != "signal" || v.Locked || gfMLFamily(fam) {
		return false, venueVerdict{}, false
	}
	vv, exists := v.Venues[platform]
	if !exists || vv.N <= 0 || math.IsNaN(vv.Mean) || math.IsInf(vv.Mean, 0) {
		return false, venueVerdict{}, false
	}
	if vv.Mean > 0 {
		if gfDedicatedDirectExpression(fam, platform) {
			return false, venueVerdict{}, false
		}
		vv.State = gfPaperPointState
		return false, vv, true
	}
	// A losing direct ledger is discovery evidence only. The opposite side cannot enter an
	// executable roster until its own actual ask/depth/fee/settlement route proves independently.
	return false, venueVerdict{}, false
}

// gfPaperCoverageRow makes the execution mapping auditable. Historical point estimates remain
// visible as research, but their sign cannot decide whether corrected Paper collects the route.
// LIVE authorization is deliberately absent from this structure.
type gfPaperCoverageRow struct {
	Family       string  `json:"family"`
	Platform     string  `json:"platform"`
	Side         string  `json:"side"`
	OriginLayer  string  `json:"origin_layer"`
	Route        string  `json:"route"`
	N            int     `json:"n"`
	Markets      int     `json:"unique_markets,omitempty"`
	PointMarkets int     `json:"point_contract_markets,omitempty"`
	CurrentCents float64 `json:"current_cents_per_contract"`
	Direction    string  `json:"direction,omitempty"`
	Executor     string  `json:"paper_executor,omitempty"`
	Excluded     string  `json:"excluded,omitempty"`
}

func gfPaperExecutionCoverage(verds []verdictEnt) []gfPaperCoverageRow {
	var out []gfPaperCoverageRow
	for _, v := range verds {
		family, platform, side, origin, route, ok := gfExactCell(v)
		if !ok {
			continue
		}
		point, pointMarkets, _ := gfHypothesisPoint(v)
		moneyPoint, _, moneyPointOK := gfPaperPoint(v)
		r := gfPaperCoverageRow{Family: family, Platform: platform, Side: side,
			OriginLayer: origin, Route: route, N: v.N, Markets: v.Markets,
			PointMarkets: pointMarkets, CurrentCents: vRnd4(point * 100)}
		switch {
		case v.Locked:
			r.Excluded = "venue-locked"
		case strings.HasPrefix(family, "side-control:"):
			r.Excluded = "opposite-side-control-has-no-order-authority"
		case gfMLFamily(family):
			r.Excluded = "ml-not-generic-follower"
		case moneyPointOK && moneyPoint <= 0:
			r.Excluded = "authenticated-profit-evidence-nonpositive"
		case origin == "strategy":
			r.Direction, r.Executor = "direct", "originating-strategy"
		case gfDedicatedExpression(family, platform):
			r.Direction, r.Executor = "direct", "dedicated"
		default:
			r.Direction, r.Executor = "direct", "generic-follower"
		}
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Family != out[j].Family {
			return out[i].Family < out[j].Family
		}
		if out[i].Platform != out[j].Platform {
			return out[i].Platform < out[j].Platform
		}
		if out[i].Side != out[j].Side {
			return out[i].Side < out[j].Side
		}
		return out[i].OriginLayer < out[j].OriginLayer
	})
	return out
}

// gfProfitRateMultiplier is retained only for historical replay/tests of the pre-R158 allocator.
// Funded Paper sizing no longer calls it: the conservative Net/day share already contains the
// route's profit rate, so applying a second speed term would double-count that evidence.
func gfProfitRateMultiplier(resolveHours, exponent float64) float64 {
	return clampF(evDayRankMult(resolveHours, exponent), gfTurnoverMinMult, gfTurnoverMaxMult)
}

// gfCurrentRouteEdge conservatively carries a settled exact-route mean to a new quote. Better
// price/fee movement does not inflate the historical estimate; worse movement must be paid in
// full. Because executableContracts caps size to the visible touch, modeled slippage is zero and
// any quantity beyond that touch is refused rather than guessed.
func gfCurrentRouteEdge(mean, historicalAsk, currentAsk, historicalFeePC, currentFee, contracts float64) (float64, bool) {
	if contracts <= 0 || historicalAsk <= 0 || historicalAsk >= 1 || currentAsk <= 0 || currentAsk >= 1 ||
		math.IsNaN(mean) || math.IsInf(mean, 0) || math.IsNaN(historicalFeePC) || math.IsInf(historicalFeePC, 0) ||
		math.IsNaN(currentFee) || math.IsInf(currentFee, 0) {
		return 0, false
	}
	edge := mean - math.Max(0, currentAsk-historicalAsk) -
		math.Max(0, currentFee/contracts-historicalFeePC)
	return edge, !math.IsNaN(edge) && !math.IsInf(edge, 0)
}

// gfDiscoveryLineage preserves the detector identity even after an inverse has been normalized to
// its independently quoted side. The inverse identity freezes these fields before clearing the
// discovery price, so the eventual fill receipt can be joined back without guessing.
func gfDiscoveryLineage(sig storage.Signal) (family, emittedSide string, discoveryPrice float64, invertedSide string) {
	family = strings.ToLower(strings.TrimSpace(sig.SignalType))
	emittedSide = strings.ToUpper(strings.TrimSpace(sig.Side))
	discoveryPrice = sig.EntryPrice
	base, inverse := strings.CutPrefix(family, "invert:")
	if !inverse {
		return family, emittedSide, discoveryPrice, "none"
	}
	family, invertedSide = base, emittedSide
	for _, part := range strings.Split(sig.ExecExpr, "/") {
		if value, ok := strings.CutPrefix(part, "emitted="); ok {
			if side := strings.ToUpper(strings.TrimSpace(value)); side == "YES" || side == "NO" {
				emittedSide = side
			}
		}
		if value, ok := strings.CutPrefix(part, "discovery-price="); ok {
			if price, err := strconv.ParseFloat(strings.TrimSpace(value), 64); err == nil && price > 0 && price < 1 {
				discoveryPrice = price
			}
		}
	}
	return family, emittedSide, discoveryPrice, invertedSide
}

// gfDynamicSubs — the AUTOMATIC roster lines (R130): one bookSub per registered exact
// (family, venue, side, route). Historical assumed-fill sign is research telemetry and cannot add
// or remove a corrected-Paper route. Only authenticated fill-conditioned profit evidence may
// defeat a route; all other registered routes enter equal one-share exploration.
func gfDynamicSubs(verds []verdictEnt) []bookSub {
	var out []bookSub
	seen := map[string]bool{}
	for _, v := range verds {
		fam, venue, side, origin, route, ok := gfExactCell(v)
		moneyPoint, _, moneyPointOK := gfPaperPoint(v)
		if !ok || v.Locked || origin != "model" || (moneyPointOK && moneyPoint <= 0) || gfMLFamily(fam) ||
			strings.HasPrefix(fam, "side-control:") || gfDedicatedExpression(fam, venue) {
			continue
		}
		key := gfRosterKey(fam, venue, side, route)
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, bookSub{Book: venue,
			Plain:  "follow: " + famPlainName(fam) + " [" + side + " " + route + "] (" + venue + ")",
			Family: key, Seed: v.Family,
			SeedMean: 0, SeedN: v.N, SeedState: gfPaperPointState})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Family < out[j].Family })
	return out
}

// venueBookSubsAll — static mapping + the R130 automatic follower lines, from one verdict slice
// (both consumers — scoreWeights and bookRosterLines — already hold a fresh compute).
func (s *Server) venueBookSubsAll(verds []verdictEnt) []bookSub {
	return append(venueBookSubs(), gfDynamicSubs(verds)...)
}

// gfModeCached — cache-only exact-route registry (cold/stale cache fails CLOSED, never a cold
// compute inside a producer pass). A non-profit hypothesis may register a route for neutral
// one-share collection, but its magnitude/sign is cleared before the value reaches money code.
// Authenticated fill-conditioned profit evidence may still defeat its own exact route.
func (s *Server) gfModeCached(fam, platform, side string) (verdictEnt, bool) {
	s.verdMu.Lock()
	cache, at := s.verdCache, s.verdAt
	s.verdMu.Unlock()
	if cache == nil || time.Since(at) > swStale {
		return verdictEnt{}, false
	}
	for _, v := range cache {
		cellFam, cellPlatform, cellSide, origin, route, valid := gfExactCell(v)
		moneyPoint, _, moneyPointOK := gfPaperPoint(v)
		if valid && !v.Locked && origin == "model" && cellFam == strings.ToLower(strings.TrimSpace(fam)) &&
			cellPlatform == strings.ToLower(strings.TrimSpace(platform)) &&
			cellSide == strings.ToUpper(strings.TrimSpace(side)) && !(moneyPointOK && moneyPoint <= 0) &&
			!gfMLFamily(cellFam) && !strings.HasPrefix(cellFam, "side-control:") &&
			!gfDedicatedExpression(cellFam, cellPlatform) {
			v.SourceFamily, v.Platform, v.Side, v.OriginLayer, v.Route = cellFam, cellPlatform, cellSide, origin, route
			if moneyPointOK {
				v.Mean = moneyPoint
				v.MeanAsk, v.FeePC = gfPaperPointCosts(v)
			} else {
				// Keep the route identity, discard the research economics. Corrected Paper uses a
				// current complete book, exact fee and fixed one-share probe instead.
				v.Mean, v.MeanAsk, v.FeePC = 0, 0, 0
			}
			return v, true
		}
	}
	return verdictEnt{}, false
}

// gfRegisteredSignalRoute binds a detector row to the code-owned contract registry before Paper
// looks at any statistical history. This prevents a brand-new route, or a route whose leaderboard
// cache is temporarily absent/stale, from disappearing before the realistic execution model can
// record it. Fresh authenticated exchange-fill evidence may still defeat this exact route.
type gfRegisteredSignalRoute struct {
	Verdict  verdictEnt
	Contract r147SystemVariantSignalContract
	Static   bool
}

func (s *Server) gfDedicatedContractActive(contract r147SystemVariantSignalContract) bool {
	switch strings.ToLower(strings.TrimSpace(contract.Handoff)) {
	case r145HandoffFreshInv:
		return s.freshInvActive()
	case r145HandoffXVGap:
		return s.xvgActive()
	default:
		return false
	}
}

func gfSignalContractBinding(sig storage.Signal) (r147SystemVariantSignalContract, string, bool) {
	family := strings.ToLower(strings.TrimSpace(sig.SignalType))
	platform := strings.ToLower(strings.TrimSpace(sig.Platform))
	side := strings.ToUpper(strings.TrimSpace(sig.Side))
	matches := r147ExactSignalContracts(family, platform, side, "taker")
	if len(matches) == 0 {
		return r147SystemVariantSignalContract{}, "exact-static-signal-contract-absent", false
	}
	candidate := liveMirrorCandidate{Family: family, Platform: platform, Side: side,
		InputTopology: r147SignalInputTopology(sig)}
	_, contract, why := r147BindSignalContract(candidate, "taker")
	if why != "" {
		return r147SystemVariantSignalContract{}, why, true
	}
	return contract, "", true
}

// bindStaticTakerSignalCandidate freezes the exact code-owned signal contract before an
// opportunity ID or dedup key is created. A multi-topology family may never first enter the
// immutable ledger under a blank contract and then acquire a different identity later during
// LIVE preflight.
func bindStaticTakerSignalCandidate(c liveMirrorCandidate, sig storage.Signal) (
	liveMirrorCandidate, string, bool) {
	contract, why, declared := gfSignalContractBinding(sig)
	if !declared {
		return c, "", false
	}
	if why != "" {
		return c, why, true
	}
	c.InputTopology = contract.InputTopology
	c.SignalContractID = r147SignalContractIdentity(contract)
	return c, "", true
}

func gfRosterTopology(contract r147SystemVariantSignalContract) string {
	if len(r147ExactSignalContracts(contract.SystemID, contract.ExecutionVenue,
		contract.Side, contract.Route)) > 1 {
		return contract.InputTopology
	}
	return ""
}

func (s *Server) gfRegisteredRouteForSignal(sig storage.Signal) (gfRegisteredSignalRoute, string) {
	family := strings.ToLower(strings.TrimSpace(sig.SignalType))
	platform := strings.ToLower(strings.TrimSpace(sig.Platform))
	side := strings.ToUpper(strings.TrimSpace(sig.Side))
	contract, bindWhy, staticallyDeclared := gfSignalContractBinding(sig)
	if staticallyDeclared {
		if bindWhy != "" {
			return gfRegisteredSignalRoute{}, bindWhy
		}
		if gfMLFamily(family) {
			return gfRegisteredSignalRoute{}, "exact-route-owned-by-book-native-ml-executor"
		}
		if s.gfDedicatedContractActive(contract) {
			return gfRegisteredSignalRoute{}, "exact-route-owned-by-active-dedicated-executor"
		}

		// A missing or stale verdict is not a reason to erase a code-connected observation. Only a
		// fresh, exact, authenticated exchange-fill cohort can stop neutral Paper collection.
		s.verdMu.Lock()
		cache, at := append([]verdictEnt(nil), s.verdCache...), s.verdAt
		s.verdMu.Unlock()
		registration := verdictEnt{Family: "taker:" + family + "@" + platform,
			SourceFamily: family, Platform: platform, OriginLayer: "model", Route: "taker",
			Side: side, Group: "taker"}
		if cache != nil && time.Since(at) <= swStale {
			for _, v := range cache {
				cellFamily, cellPlatform, cellSide, _, route, valid := gfExactCell(v)
				if !valid || cellFamily != family || cellPlatform != platform || cellSide != side || route != "taker" {
					continue
				}
				if point, _, ok := gfPaperPoint(v); ok && point <= 0 {
					return gfRegisteredSignalRoute{}, "fresh-authenticated-exchange-profit-evidence-nonpositive"
				}
				registration = v
				registration.SourceFamily, registration.Platform, registration.Side = family, platform, side
				registration.OriginLayer, registration.Route = "model", "taker"
				registration.Mean, registration.MeanAsk, registration.FeePC = 0, 0, 0
			}
		}
		return gfRegisteredSignalRoute{Verdict: registration, Contract: contract, Static: true}, ""
	}

	// Compatibility for hermetic fixtures and an old locally registered route. Production routes
	// must live in the exact contract registry; this branch cannot rescue an ambiguous static cell.
	if cell, ok := s.gfModeCached(family, platform, side); ok {
		return gfRegisteredSignalRoute{Verdict: cell}, ""
	}
	return gfRegisteredSignalRoute{}, bindWhy
}

// publishActualBeforeMirror makes the ordering executable, not merely conventional. Every caller
// completes the actual LIVE enqueue/log attempt before it may publish fake mirror work.
func publishActualBeforeMirror(actual func() bool, mirror func()) bool {
	accepted := false
	if actual != nil {
		accepted = actual()
	}
	if mirror != nil {
		mirror()
	}
	return accepted
}

// genfollowTriggeredSystems publishes one already-identified route. A separately quoted inverse is
// passed through this function only after collectIndependentInverseSystem has stored its own exact
// opposite-book receipt. Keeping that sequencing outside this hot function lets the emitted route
// publish LIVE before inverse REST/SQLite or any Paper work.
func (s *Server) genfollowTriggeredSystems(ctx context.Context, sig storage.Signal) {
	s.genfollowTriggeredSystemsAt(ctx, sig, time.Now().UTC())
}

func (s *Server) genfollowTriggeredSystemsAt(ctx context.Context, sig storage.Signal, triggerAt time.Time) {
	type candidate struct {
		sig   storage.Signal
		point float64 // always zero unless route-exact authenticated profit evidence is added later
	}
	candidates := make([]candidate, 0, 1)
	if _, _, declared := gfSignalContractBinding(sig); declared {
		candidates = append(candidates, candidate{sig: sig})
	} else if _, ok := s.gfModeCached(sig.SignalType, sig.Platform, sig.Side); ok {
		// Hermetic/legacy registration compatibility. Every production route is expected to use
		// the static exact contract branch above.
		candidates = append(candidates, candidate{sig: sig})
	}
	// Assumed-fill point magnitudes are hypothesis labels, not allocation authority. Preserve a
	// deterministic identity order instead of letting the old cents/share choose who gets scarce
	// Paper capacity or the same-market duplicate seat.
	sort.SliceStable(candidates, func(i, j int) bool {
		leftInverse := strings.HasPrefix(strings.ToLower(candidates[i].sig.SignalType), "invert:")
		rightInverse := strings.HasPrefix(strings.ToLower(candidates[j].sig.SignalType), "invert:")
		if leftInverse != rightInverse {
			// The actual emitted detector owns the portfolio seat. Its independently quoted inverse
			// is still tracked, but cannot front-run the source and create a guaranteed hedge loss.
			return !leftInverse
		}
		li := strings.ToLower(candidates[i].sig.SignalType) + "\x00" + strings.ToUpper(candidates[i].sig.Side)
		lj := strings.ToLower(candidates[j].sig.SignalType) + "\x00" + strings.ToUpper(candidates[j].sig.Side)
		return li < lj
	})
	paperSignals := make([]genfollowPaperSignal, 0, len(candidates))
	for _, c := range candidates {
		// Every exact taker opportunity receives its immutable ID before LIVE selection. Selection
		// decides whether the LIVE branch queues cash; it never decides whether Detector, Shadow, or
		// Paper are allowed to remember the opportunity.
		signalAt := triggerAt
		if signalAt.IsZero() {
			signalAt = time.Now().UTC()
		}
		// Claim the exact economic opportunity before minting its immutable id. Previously only the
		// selected LIVE queue performed this 500 ms claim, so an unselected detector could mint two
		// Detector/Paper/Shadow samples for the same book pulse. Research-promotion producers share
		// this same claim below, which also prevents two producer paths from double-counting one
		// contract while retaining independent seats for distinct input topologies.
		if !s.claimCanonicalSignalOpportunity(c.sig, signalAt) {
			continue
		}
		shadowAttemptID := s.executionShadowSignalAttemptID(c.sig, c.point, signalAt)
		schedulePaperNow := true
		selected := liveCandidateFromSignalIntent(liveSignalIntent{
			Signal: c.sig, At: signalAt, ShadowAttemptID: shadowAttemptID,
		})
		if bound, why, declared := bindStaticTakerSignalCandidate(selected, c.sig); declared && why == "" {
			selected = bound
		}
		selectionReason := s.liveSystemSelectionReason(selected, "taker")
		// A current detector opportunity needs a full-depth seat for both the money-free execution
		// shadow and corrected Paper. This hint grants no cash authority.
		s.noteLiveSignalDepthBookHint(c.sig)
		if selectionReason == "" {
			// Publish actual LIVE first. The independent fake-money mirror is strictly second and
			// nonblocking, so its experiment can never delay the real account's fresh execution check.
			paperAfterTerminal := genfollowPaperSignal{
				Signal: c.sig, SignalAt: signalAt, ShadowAttemptID: shadowAttemptID,
			}
			// Register before publishing to the asynchronous cash queue. A very fast early rejection
			// can otherwise produce its terminal before Paper knows which immutable signal it belongs
			// to. The queue receives fundedPaperPreScheduled=true so no later preflight duplicates it.
			s.rememberGenfollowPaperAfterLiveTerminal(paperAfterTerminal)
			publishShadow, liveAccepted := false, false
			publishActualBeforeMirror(
				func() bool {
					liveAccepted, publishShadow = s.queueLiveSignalIntentPreparedAfterClaim(
						c.sig, c.point, signalAt, shadowAttemptID, true)
					if publishShadow {
						s.executionShadowPublishSignal(c.sig, c.point, signalAt, shadowAttemptID, false)
					}
					return liveAccepted
				},
				func() {
					if publishShadow {
						s.queueLivePolicyMirrorSignalPrepared(c.sig, c.point, signalAt, shadowAttemptID)
					}
				},
			)
			if liveAccepted {
				// The exact LIVE terminal owns Paper fanout for this attempt, including an early local
				// preflight rejection. This preserves LIVE-first without changing Paper's sample.
				schedulePaperNow = false
			} else {
				// A synchronous no-send may already have consumed the registration and queued Paper.
				// If it did not, remove the orphan registration and enqueue the same signal below.
				_, stillWaiting := s.genfollowPaperPassedSignals.LoadAndDelete(shadowAttemptID)
				if !stillWaiting && publishShadow {
					schedulePaperNow = false
				}
			}
			if !publishShadow {
				// No parent means this callback was coalesced into the already-published economic
				// opportunity. Do not let Paper count a second sample that LIVE and Shadow did not.
				_, _ = s.genfollowPaperPassedSignals.LoadAndDelete(shadowAttemptID)
				schedulePaperNow = false
				shadowAttemptID = ""
			}
		} else if shadowAttemptID != "" {
			// The route was genuinely detected but has no current cash authority. Persist that as an
			// explicit branch decision, then let Paper and the portfolio-independent q1 execution
			// shadow observe the same ID. No LIVE queue, account read, reservation, or venue POST occurs.
			paperAfterTerminal := genfollowPaperSignal{
				Signal: c.sig, SignalAt: signalAt, ShadowAttemptID: shadowAttemptID,
			}
			s.rememberGenfollowPaperAfterLiveTerminal(paperAfterTerminal)
			s.executionShadowPublishSignalDisposition(c.sig, c.point, signalAt,
				shadowAttemptID, false, false, selectionReason)
			s.executionShadowRecordPaperFanout(shadowAttemptID, time.Now().UTC(), signalAt, c.sig)
			s.executionShadowRecordDrop(selected, "live-selection", "route-not-selected-for-live:"+
				strings.TrimSpace(selectionReason), time.Now().UTC(), map[string]any{
				"cash_authority": false, "selection_reason": strings.TrimSpace(selectionReason),
			})
			s.enqueueCanonicalSystemExecutionShadow(c.sig, signalAt, shadowAttemptID)
			// recordDrop synchronously releases the registered same-ID Paper branch.
			schedulePaperNow = false
		}
		if schedulePaperNow {
			paperSignals = append(paperSignals, genfollowPaperSignal{
				Signal: c.sig, SignalAt: signalAt, ShadowAttemptID: shadowAttemptID,
			})
		}
	}
	// Every LIVE candidate in this detection batch is now published. Paper runs later on its own
	// bounded worker and preserves deterministic hypothesis identity order inside the batch.
	s.enqueueGenfollowPaperIntent(paperSignals)
}

const liveSignalIntentCap = 256
const liveSignalIntentBurstDedup = 500 * time.Millisecond

func canonicalSignalOpportunityDedupWindow(sig storage.Signal) time.Duration {
	if strings.EqualFold(strings.TrimSpace(sig.SignalType), "invert:kalshi-flow") {
		// Event-driven and periodic inverse producers can describe one source episode several
		// seconds apart. Keep the existing 30-second inverse episode at the shared pre-id boundary.
		return liveMirrorDedup
	}
	return liveSignalIntentBurstDedup
}

type liveSignalIntent struct {
	Signal                  storage.Signal
	Point                   float64
	At                      time.Time
	ShadowAttemptID         string
	Generation              uint64
	GenerationBound         bool
	PreflightAdmitted       bool
	FundedPaperPreScheduled bool
}

const (
	liveIntentGenerationChangedReason  = "operator-auto-generation-changed"
	liveIntentGenerationRequiredReason = "AUTO candidate omitted its operator generation"
)

func (s *Server) liveIntentGenerationReason(generation uint64, bound bool) string {
	if bound && generation != s.liveSignalIntentGeneration.Load() {
		return liveIntentGenerationChangedReason
	}
	return ""
}

func (s *Server) liveIntentGenerationGateReason(auto bool, generation uint64, bound, required bool) string {
	if !auto {
		return ""
	}
	if required && !bound {
		return liveIntentGenerationRequiredReason
	}
	return s.liveIntentGenerationReason(generation, bound)
}

func (s *Server) liveSignalIntentChannel() chan liveSignalIntent {
	s.liveSignalIntentOnce.Do(func() {
		s.liveSignalIntentCh = make(chan liveSignalIntent, liveSignalIntentCap)
	})
	return s.liveSignalIntentCh
}

// claimCanonicalSignalOpportunity is the shared, pre-id burst claim for every exact taker
// producer. The key is bound to the static signal contract before it enters the map, so K-PINT and
// K-PUS remain independent while truly duplicate genfollow/research observations coalesce. The
// claim performs no persistence, account work, or venue I/O.
func (s *Server) claimCanonicalSignalOpportunity(sig storage.Signal, signalAt time.Time) bool {
	now := time.Now()
	if signalAt.IsZero() {
		signalAt = now
	}
	c := liveCandidateFromSignalIntent(liveSignalIntent{Signal: sig, At: signalAt})
	key := c.key()
	if strings.TrimSpace(key) == "" {
		return false
	}
	s.liveMirrorMu.Lock()
	if s.liveSignalIntentSeen == nil {
		s.liveSignalIntentSeen = map[string]time.Time{}
	}
	dedupWindow := canonicalSignalOpportunityDedupWindow(sig)
	if seenAt, exists := s.liveSignalIntentSeen[key]; exists && now.Sub(seenAt) >= 0 &&
		now.Sub(seenAt) <= dedupWindow {
		s.liveMirrorMu.Unlock()
		// Moving the claim ahead of immutable-id creation must not erase the operator's durable
		// explanation for a selected LIVE signal that was coalesced. Unselected research traffic
		// remains outside the LIVE candidate audit, and no second detector attempt is manufactured.
		if s.liveOperatorAutoOn() && s.liveSystemSelectionReason(c, "taker") == "" {
			s.recordLiveCandidateDrop(c, "AUTO-LIVE-SIGNAL-DROP", "signal-queue",
				"duplicate-coalesced-live-signal-intent", now,
				map[string]any{"coalesced_with_age_ms": now.Sub(seenAt).Milliseconds()})
		}
		return false
	}
	s.liveSignalIntentSeen[key] = now
	if len(s.liveSignalIntentSeen) > liveSignalIntentCap*2 {
		for seenKey, seenAt := range s.liveSignalIntentSeen {
			if now.Sub(seenAt) > liveMirrorDedup {
				delete(s.liveSignalIntentSeen, seenKey)
			}
		}
	}
	s.liveMirrorMu.Unlock()
	return true
}

// queueLiveSignalIntent is deliberately nonblocking. It performs only cheap exact-identity checks;
// MonitorLiveAuto owns the venue I/O. A full bounded queue is visible rather than blocking a feed
// producer or recreating the former unbounded subscription/goroutine flood.
func (s *Server) queueLiveSignalIntent(sig storage.Signal, measuredPoint float64) bool {
	return s.queueLiveSignalIntentAt(sig, measuredPoint, time.Now())
}

// queueLiveSignalIntentAt is the event-source form of queueLiveSignalIntent. signalAt is the
// first local source observation, not the worker-start clock, so downstream latency receipts cover
// detector wake + arbitration + preflight rather than hiding those stages.
func (s *Server) queueLiveSignalIntentAt(sig storage.Signal, measuredPoint float64, signalAt time.Time) bool {
	if !s.claimCanonicalSignalOpportunity(sig, signalAt) {
		return false
	}
	shadowAttemptID := s.executionShadowSignalAttemptID(sig, measuredPoint, signalAt)
	accepted, publishShadow := s.queueLiveSignalIntentPreparedAfterClaim(
		sig, measuredPoint, signalAt, shadowAttemptID, false)
	if publishShadow {
		s.executionShadowPublishSignal(sig, measuredPoint, signalAt, shadowAttemptID, false)
	}
	return accepted
}

// queueLiveSignalIntentPreparedAfterClaim must be called only after
// claimCanonicalSignalOpportunity succeeds. Keeping the claim outside this helper guarantees the
// immutable id is minted only once and prevents selected direct callers from claiming twice.
func (s *Server) queueLiveSignalIntentPreparedAfterClaim(sig storage.Signal, measuredPoint float64,
	signalAt time.Time, shadowAttemptID string,
	fundedPaperPreScheduled bool) (accepted, publishShadow bool) {
	// Raw strategy traffic remains invisible while either operator switch is OFF. Once both are
	// ON, every refusal below becomes a durable LIVE-candidate receipt.
	generation := s.liveSignalIntentGeneration.Load()
	operatorAutoOn := s.liveOperatorAutoOn()
	now := time.Now()
	if signalAt.IsZero() {
		signalAt = now
	}
	c := liveMirrorCandidate{Platform: strings.ToLower(strings.TrimSpace(sig.Platform)),
		Ticker: strings.TrimSpace(sig.Ticker), Side: strings.ToUpper(strings.TrimSpace(sig.Side)),
		Source: "auto-cons-" + strings.TrimSpace(sig.SignalType), Family: strings.TrimSpace(sig.SignalType),
		Price: sig.EntryPrice, At: signalAt, InputTopology: r147SignalInputTopology(sig),
		InputObservedAt: r147SignalInputObservedAt(sig), ShadowAttemptID: shadowAttemptID,
		LiveIntentGeneration: generation, LiveIntentGenerationBound: true}
	if bound, why, declared := bindStaticTakerSignalCandidate(c, sig); declared {
		if why != "" {
			if operatorAutoOn {
				s.recordLiveCandidateDrop(c, "AUTO-LIVE-SIGNAL-DROP", "signal-contract",
					why, now, nil)
				return false, true
			}
			return false, false
		}
		c = bound
	}
	if operatorAutoOn && !s.liveMirrorEnabled() {
		s.recordLiveCandidateDrop(c, "AUTO-LIVE-SIGNAL-DROP", "signal-gate",
			"live-runtime-gate-disabled", now, nil)
		return false, true
	}
	if math.IsNaN(measuredPoint) || math.IsInf(measuredPoint, 0) {
		if operatorAutoOn {
			s.recordLiveCandidateDrop(c, "AUTO-LIVE-SIGNAL-DROP", "signal-selection",
				"invalid-research-point", now, map[string]any{"measured_point": measuredPoint})
			return false, true
		}
		return false, false
	}
	if why := s.liveSystemSelectionReason(c, "taker"); why != "" {
		// Paper-only systems remain visible in their normal Paper/research ledgers. They are not
		// LIVE candidates, so do not create a candidate-audit row or execution-shadow orphan here.
		return false, false
	}
	if operatorAutoOn {
		if why := s.liveIntentGenerationReason(generation, true); why != "" {
			s.recordLiveCandidateDrop(c, "AUTO-LIVE-SIGNAL-DROP", "signal-generation", why, time.Now(), nil)
			return false, true
		}
	}
	if !operatorAutoOn {
		// The first exact selected observation still belongs to the disarmed policy-mirror
		// comparison. Repeats inside the burst window were coalesced above, before a detector group
		// could be published. Give funded Paper an explicit durable no-send terminal as well: a
		// later ARM/AUTO session never replays this attempt, so it is safe only as a labelled
		// counterfactual rather than an unknown LIVE outcome.
		s.executionShadowRecordDrop(c, "live-operator-gate", "ARM/AUTO was off at detection",
			now, map[string]any{"operator_auto_on": false})
		return false, true
	}
	in := liveSignalIntent{Signal: sig, Point: measuredPoint, At: signalAt,
		ShadowAttemptID: shadowAttemptID, Generation: generation, GenerationBound: true,
		FundedPaperPreScheduled: fundedPaperPreScheduled}
	select {
	case s.liveSignalIntentChannel() <- in:
		// The diagnostic writer sees cash priority from the first successful publication, before
		// asynchronous preflight or the later HTTP handler can touch SQLite.
		s.liveDispatchShadowLivePriorityAt.Store(now.UnixNano())
		return true, true
	default:
		s.recordLiveCandidateDrop(c, "AUTO-LIVE-SIGNAL-DROP", "signal-queue",
			"live-signal-queue-full", time.Now(),
			map[string]any{"queue_capacity": liveSignalIntentCap})
		return false, true
	}
}

func (s *Server) rejectLiveSignalIntent(sig storage.Signal, reason string) bool {
	return s.rejectLiveSignalIntentPrepared(sig, reason,
		s.executionShadowAttemptForSignal(sig), time.Now())
}

func (s *Server) rejectLiveSignalIntentPrepared(sig storage.Signal, reason,
	shadowAttemptID string, signalAt time.Time) bool {
	return s.rejectLiveSignalIntentPreparedMode(sig, reason, shadowAttemptID, signalAt, false)
}

func (s *Server) rejectLiveSignalIntentPreparedMode(sig storage.Signal, reason,
	shadowAttemptID string, signalAt time.Time, admittedBeforeDisable bool) bool {
	now := time.Now()
	if signalAt.IsZero() {
		signalAt = now
	}
	s.recordLiveCandidateDrop(liveMirrorCandidate{
		Platform: strings.ToLower(strings.TrimSpace(sig.Platform)), Ticker: strings.TrimSpace(sig.Ticker),
		Title: sig.Title, Side: strings.ToUpper(strings.TrimSpace(sig.Side)), Price: sig.EntryPrice,
		Family: strings.TrimSpace(sig.SignalType), Source: "auto-cons-" + strings.TrimSpace(sig.SignalType),
		At: signalAt, InputTopology: r147SignalInputTopology(sig),
		InputObservedAt: r147SignalInputObservedAt(sig), ShadowAttemptID: shadowAttemptID,
		LivePreflightAdmitted: admittedBeforeDisable,
	}, "AUTO-LIVE-SIGNAL-DROP", "live-first-preflight", reason, now, nil)
	return false
}

// enqueueLiveAllowlistedSignalIntent records the exact current one-unit intent that feeds the
// prospective LIVE proof lane. It deliberately sits beside (not inside) the funded Paper ledger:
// Paper portfolio diversification and sub-allocation are simulation policies, not facts about the
// real account. The helper never grants money authority; enqueue/dispatch/handler repeat the exact
// signal contract, executable book, statistical proof, sizing, account and risk checks.
func (s *Server) enqueueLiveAllowlistedSignalIntent(ctx context.Context, sig storage.Signal, measuredPoint float64) bool {
	return s.enqueueLiveAllowlistedSignalIntentAt(ctx, sig, measuredPoint, time.Now())
}

// enqueueLiveAllowlistedSignalIntentAt preserves the first in-process signal receipt through every
// later queue. Resetting At after the book preflight hid seconds of scheduler delay and extended a
// delayed signal's execution lease. The separate ArbitrationAt remains the age of the current
// executable quote/proof receipt.
func (s *Server) enqueueLiveAllowlistedSignalIntentAt(ctx context.Context, sig storage.Signal,
	measuredPoint float64, signalAt time.Time) bool {
	return s.processLiveAllowlistedSignalIntentAtGeneration(ctx, sig, measuredPoint, signalAt, false,
		s.executionShadowAttemptForSignal(sig), s.liveSignalIntentGeneration.Load(), true, false, false)
}

func (s *Server) processLiveAllowlistedSignalIntentJob(ctx context.Context,
	intent liveSignalIntent) bool {
	return s.processLiveAllowlistedSignalIntentAtGeneration(ctx, intent.Signal, intent.Point,
		intent.At, false, intent.ShadowAttemptID, intent.Generation, intent.GenerationBound,
		intent.FundedPaperPreScheduled, intent.PreflightAdmitted)
}

func (s *Server) publishLivePriorityProofReceipt(ctx context.Context, candidate liveMirrorCandidate,
	quote liveMirrorQuote, fee float64, feeSource string, at time.Time, mirrorOnly bool) error {
	if mirrorOnly {
		return nil
	}
	_, err := s.insertCanonicalUnitTrial(ctx, storage.UnitTrial{OpenedTS: at.UTC(),
		Family: liveMirrorFamily(candidate), Platform: candidate.Platform, Ticker: candidate.Ticker,
		Side: candidate.Side, Episode: livePriorityProofGenerationEpisode,
		Ask: quote.Price, FeePC: fee, FeeKnown: true,
		FeeSource: feeSource, Depth: quote.Depth,
		QuoteSource: livePriorityProofQuoteSource(quote.BookSource, candidate.SignalContractID)})
	if err != nil {
		return err
	}
	s.liveMirrorMu.Lock()
	if s.liveAllocationSignal == nil {
		s.liveAllocationSignal = map[string]time.Time{}
	}
	s.liveAllocationSignal[liveAllocationSignalKey(candidate)] = time.Now()
	s.liveMirrorMu.Unlock()
	return nil
}

// processLiveAllowlistedSignalIntentAt is the single exact preflight used by both money LIVE and
// the isolated policy mirror. mirrorOnly is accepted only by the mirror's separate worker: it may
// collect the same proof while ARM/AUTO are off, but it can never enqueue an actual LIVE candidate.
func (s *Server) processLiveAllowlistedSignalIntentAt(ctx context.Context, sig storage.Signal,
	measuredPoint float64, signalAt time.Time, mirrorOnly bool, shadowAttemptID string) bool {
	return s.processLiveAllowlistedSignalIntentAtGeneration(ctx, sig, measuredPoint, signalAt,
		mirrorOnly, shadowAttemptID, 0, false, false, false)
}

func (s *Server) processLiveAllowlistedSignalIntentAtGeneration(ctx context.Context, sig storage.Signal,
	measuredPoint float64, signalAt time.Time, mirrorOnly bool, shadowAttemptID string,
	generation uint64, generationBound, fundedPaperPreScheduled, admittedBeforeDisable bool) bool {
	if !mirrorOnly {
		endCashPriority := s.beginLiveCashPriority()
		defer endCashPriority()
	}
	if !mirrorOnly {
		if why := s.liveIntentGenerationReason(generation, generationBound); why != "" {
			return s.rejectLiveSignalIntentPreparedMode(sig, why, shadowAttemptID, signalAt,
				admittedBeforeDisable)
		}
	}
	if !mirrorOnly && (!s.liveOperatorAutoOn() || !s.liveMirrorEnabled()) {
		// ARM/AUTO can change after signal publication. Preserve the observation in the independent
		// mirror queue; never let a formerly-disarmed signal enter the later real-money queue.
		s.queueLivePolicyMirrorSignalPrepared(sig, measuredPoint, signalAt, shadowAttemptID)
		return s.rejectLiveSignalIntentPreparedMode(sig,
			"live-runtime-gate-disabled-after-preflight-admission",
			shadowAttemptID, signalAt, admittedBeforeDisable)
	}
	reject := func(reason string) bool {
		if mirrorOnly {
			s.recordLivePolicyMirrorRejection(ctx, sig, reason, signalAt, shadowAttemptID)
			return false
		}
		return publishActualBeforeMirror(
			func() bool {
				return s.rejectLiveSignalIntentPreparedMode(sig, reason, shadowAttemptID,
					signalAt, admittedBeforeDisable)
			},
			func() {
				s.enqueueLivePolicyMirrorRejectionPrepared(sig, reason, signalAt, shadowAttemptID)
			},
		)
	}
	if math.IsNaN(measuredPoint) || math.IsInf(measuredPoint, 0) {
		return reject("invalid-research-point")
	}
	plat := strings.ToLower(strings.TrimSpace(sig.Platform))
	side := strings.ToUpper(strings.TrimSpace(sig.Side))
	if (plat != "kalshi" && plat != "polyus") || (side != "YES" && side != "NO") || strings.TrimSpace(sig.Ticker) == "" {
		return reject("invalid-live-signal-identity")
	}
	if !mirrorOnly && plat == "kalshi" {
		ctx = r159KalshiResidentAdmissionContext(ctx)
	}
	if signalAt.IsZero() {
		signalAt = time.Now()
	}
	candidate := liveMirrorCandidate{Platform: plat, Ticker: strings.TrimSpace(sig.Ticker), Title: sig.Title,
		Side: side, Source: "auto-cons-" + strings.TrimSpace(sig.SignalType), Price: sig.EntryPrice, At: signalAt,
		Inverted:      strings.HasPrefix(strings.ToLower(strings.TrimSpace(sig.SignalType)), "invert:"),
		InputTopology: r147SignalInputTopology(sig), InputObservedAt: r147SignalInputObservedAt(sig),
		ShadowAttemptID: shadowAttemptID, LiveIntentGeneration: generation,
		LiveIntentGenerationBound: generationBound,
		LivePreflightAdmitted:     admittedBeforeDisable}
	if why := s.liveSystemSelectionReason(candidate, "taker"); why != "" {
		return reject(why)
	}
	hours := sig.ResolveHours
	if !paperEntryHorizonValueKnown(hours) {
		hours = s.paperEntryResolveHours(ctx, plat, sig.Ticker, sig.Title)
	}
	if !s.paperEntryHorizonOK(hours, sig.Ticker, sig.Title) {
		return reject("current-funded-entry-horizon-unavailable-or-exceeded")
	}
	// LIVE owns this first current-book boundary. Kalshi lifecycle and full depth ride the client's
	// reserved money-data tier; Paper's separate cache-only evaluation cannot consume or delay it.
	var (
		quote     liveMirrorQuote
		quoteWhy  string
		fee       float64
		feeSource string
		feeKnown  bool
	)
	if mirrorOnly {
		// The fake lane is cache-only: no Kalshi priority REST, route scheduler, account snapshot,
		// or other resource used by the real-money hot path.
		quote, quoteWhy = s.livePolicyMirrorCachedQuote(candidate)
		if quoteWhy == "" {
			fee, feeSource, feeKnown = s.fillFeeReceipt("kalshi", sig.Ticker, false, 1, quote.Price)
		}
	} else if s.completeBookSignalFn != nil { // immutable hermetic replay/test seam only
		quoted, source, ok := s.completeBookSignal(sig)
		if ok && quoted.FeePC != nil {
			quote = liveMirrorQuote{Price: quoted.EntryPrice, Depth: quoted.BookDepth,
				SpreadCents: quoted.SpreadCents, BookSource: quoted.BookSource}
			fee, feeSource, feeKnown = *quoted.FeePC, source, true
		} else {
			quoteWhy = "test-book-seam-incomplete"
		}
	} else {
		quote, quoteWhy = s.liveMirrorExecutableAfterPromotion(ctx, candidate)
		if quoteWhy == "" {
			fee, feeSource, feeKnown = s.fillFeeReceipt(plat, sig.Ticker, false, 1, quote.Price)
		}
	}
	if quoteWhy != "" {
		return reject(quoteWhy)
	}
	if quote.Maker {
		return reject("prospective-allocation-taker-route-required")
	}
	if quote.Price < gfPriceLo || quote.Price > gfPriceHi {
		return reject("current-executable-price-outside-system-range")
	}
	if quote.SpreadCents > gfSpreadCapC {
		return reject("current-spread-too-wide")
	}
	if quote.Depth < 1 {
		return reject("current-touch-depth-below-one-contract")
	}
	if !feeKnown || strings.TrimSpace(feeSource) == "" {
		return reject("current-route-fee-unavailable")
	}
	// liveMirrorExecutable already returned a current, lifecycle-valid full book from the money
	// feed. pxOfflineReason inspects older background price/cache clocks, so consulting it here can
	// reject an exact fresh quote merely because an unrelated collector is late.
	// Re-read authoritative lifecycle after the complete book operation, immediately before the
	// candidate timestamp. This is the same current funded horizon used at the final LIVE handoff.
	if !s.paperEntryHorizonNow(ctx, plat, sig.Ticker, sig.Title) {
		return reject("current-funded-entry-horizon-changed-before-candidate")
	}
	cell, cellOK := s.gfModeCached(sig.SignalType, plat, side)
	if !cellOK {
		return reject("current-exact-system-verdict-unavailable-or-stale")
	}
	candidate.Price = quote.Price
	if lineage, crossVenue := r148CrossVenueLineage(sig); crossVenue {
		candidate.CrossVenueSourceVenue = lineage.SourceVenue
		candidate.CrossVenueSourceTicker = lineage.SourceTicker
		candidate.CrossVenueSourceSide = lineage.SourceSide
		candidate.CrossVenueCertificateHash = lineage.CertificateHash
	}
	bound, _, bindWhy := s.liveAllocationBindSignalContract(candidate, "taker", false)
	if bindWhy != "" {
		return reject(bindWhy)
	}
	candidate = bound
	var sizingBank float64
	if mirrorOnly {
		portfolio, portfolioErr := s.livePolicyMirrorPortfolio(ctx)
		if portfolioErr == nil && portfolio.NAVKnown {
			sizingBank = portfolio.NAV
		}
	} else {
		sizingBank, _ = s.liveCachedSizingBankroll(plat)
	}
	if sizingBank <= 0 && s.completeBookSignalFn != nil {
		// Hermetic replay/tests replace the whole production book boundary and do not run the
		// authenticated account monitor. Give only that explicit seam a deterministic sizing
		// reference and matching ARM baseline; production can never enter this branch.
		sizingBank = 1000
		s.liveMu.Lock()
		if s.liveBankArm == nil {
			s.liveBankArm = map[string]float64{}
		}
		if s.liveBankArm[plat] <= 0 {
			s.liveBankArm[plat] = sizingBank
		}
		s.liveMu.Unlock()
	}
	if sizingBank <= 0 {
		return reject("current-authenticated-sizing-bankroll-unavailable")
	}
	// Search the exact whole-quantity fee schedule before rejecting the point edge. Kalshi's
	// balance-alignment rounding is non-monotone per contract, so a one-contract estimate cannot
	// stand in for a full order. This is local fee math over the already-fresh book and cached
	// bankroll; it adds no venue or account network call ahead of LIVE.
	if verdictHasAuthenticatedProfitEvidence(cell) &&
		!s.liveProspectivePointQuantityAvailable(candidate, quote, sizingBank, cell) {
		return reject("current-executable-price-or-fee-erases-point-edge")
	}
	// Persist and publish the exact current signal receipt before asking the prospective proof lane
	// to consume it. The old order asked proof for a receipt that this same path did not publish
	// until after proof, so a first independent LIVE-first signal was structurally impossible.
	// Recording the receipt does not grant money authority: proof, arbitration and final dispatch
	// still independently require positive current economics and every account/risk check.
	quoteReceiptAt := time.Now()
	if err := s.publishLivePriorityProofReceipt(ctx, candidate, quote, fee, feeSource,
		quoteReceiptAt, mirrorOnly); err != nil {
		return reject("current-one-unit-receipt-storage-failed")
	}
	// The signal has now passed the shared identity, horizon, full-book, depth, fee, sizing-bank
	// and current-edge boundary. Register the corrected Paper comparison before the cash-only proof
	// decision so a local cash refusal still produces an exact same-opportunity observation. The
	// Paper worker remains downstream of the durable LIVE terminal callback, so this registration
	// neither reads another book nor delays or authorizes a real-money order.
	paperAfterTerminal := !mirrorOnly && !fundedPaperPreScheduled
	if paperAfterTerminal {
		s.rememberGenfollowPaperAfterLiveTerminal(genfollowPaperSignal{
			Signal: sig, SignalAt: signalAt, ShadowAttemptID: shadowAttemptID,
		})
	}
	plan, planWhy := s.liveProspectivePlan(ctx, candidate, quote, sizingBank, cell, mirrorOnly)
	if planWhy != "" || !plan.valid() {
		if strings.TrimSpace(planWhy) == "" {
			planWhy = "current-exact-route-proof-lower-bound-nonpositive"
		}
		return reject(planWhy)
	}
	applyLiveProspectivePlan(&candidate, plan)
	candidate.ArbitrationQuote = quote
	candidate.ArbitrationBasis = plan.Basis
	candidate.ArbitrationMean = plan.Mean
	candidate.ArbitrationLower = plan.Lower
	candidate.ArbitrationFee = plan.ProofFeePC
	candidate.ArbitrationAt = quoteReceiptAt
	candidate.LiveFirstUnitPersisted = !mirrorOnly
	if mirrorOnly {
		s.simulateLivePolicyMirror(ctx, candidate)
		return true
	}
	// Publish only the cash candidate here. The already-registered execution comparison remains
	// blocked on this attempt's exact LIVE terminal, so it cannot overlap or get ahead of the order.
	if !s.enqueueLiveMirrorCandidate(candidate) {
		if paperAfterTerminal {
			s.genfollowPaperPassedSignals.Delete(shadowAttemptID)
		}
		// enqueueLiveMirrorCandidate owns the exact durable reason; emitting a second generic row
		// here would count one refusal twice.
		return false
	}
	s.executionShadowRecordQuote(candidate, "live-first-preflight", "passed",
		plan.TimeInForce+": "+plan.Reason, quote, plan.RequestedFee,
		plan.FeeSource, plan.Qty)
	// Persist the identical signal for restart recovery. In-memory admission waits for this
	// attempt's exact LIVE terminal.
	s.executionShadowRecordPaperFanout(shadowAttemptID, time.Now(), signalAt, sig)
	// The terminal callback offers that same immutable signal to funded Paper without competing
	// with the cash preflight.
	return true
}

func (s *Server) rememberGenfollowPaperAfterLiveTerminal(signal genfollowPaperSignal) {
	attemptID := strings.TrimSpace(signal.ShadowAttemptID)
	if attemptID == "" {
		return
	}
	now := time.Now()
	s.genfollowPaperPassedSignals.Range(func(key, value any) bool {
		queued, ok := value.(genfollowPaperSignal)
		if !ok || queued.SignalAt.IsZero() || now.Sub(queued.SignalAt) > liveMirrorClaimTTL {
			s.genfollowPaperPassedSignals.Delete(key)
		}
		return true
	})
	s.genfollowPaperPassedSignals.Store(attemptID, signal)
}

func (s *Server) enqueueGenfollowPaperAfterLiveTerminal(event storage.ExecutionShadowEvent) {
	attemptID := strings.TrimSpace(event.AttemptID)
	if attemptID == "" {
		return
	}
	terminal := fundedPaperLiveTerminalReceiptFromEvent(event)
	if terminal.Kind == fundedPaperLiveTerminalAmbiguous {
		return
	}
	raw, found := s.genfollowPaperPassedSignals.LoadAndDelete(attemptID)
	if !found {
		return
	}
	queued, ok := raw.(genfollowPaperSignal)
	if !ok {
		return
	}
	queued.LiveTerminal = terminal
	queued.VenuePriority = event.VenueAttempted
	s.enqueueGenfollowPaperIntent([]genfollowPaperSignal{queued})
}

// gfConflictReason closes the one path where the follower bypasses autoPlace: it supplies the
// common guard with the shared auto-paper positions, not only the specialist-book ledgers. The
// cache-backed read is fail-closed so storage blindness cannot create an accidental hedge.
func (s *Server) gfConflictReason(ctx context.Context, platform, ticker, side string) string {
	fills, _, _, err := s.paperFillsCached(ctx)
	if err != nil {
		return "paper-ledger-blind"
	}
	positions, _ := paper.Aggregate(fills)
	return s.betConflictReason(platform, ticker, side, "auto-cons-genfollow", positions)
}

func paperTakerQuoteFromSignal(sig storage.Signal) (liveMirrorQuote, bool) {
	if sig.BookBid == nil || sig.BookAsk == nil || sig.BookBidDepth == nil ||
		sig.BookAskDepth == nil || sig.BookQuoteAgeS == nil ||
		sig.BookTakerTick == nil || strings.TrimSpace(sig.BookSource) == "" {
		return liveMirrorQuote{}, false
	}
	checkedAt := time.Now().UTC()
	observedAt := checkedAt.Add(-time.Duration(*sig.BookQuoteAgeS * float64(time.Second)))
	return liveMirrorQuote{
		Price: sig.EntryPrice, Depth: sig.BookDepth, Tick: *sig.BookTakerTick,
		SpreadCents: sig.SpreadCents, BookSource: sig.BookSource, ObservedAt: observedAt,
		CheckedAt: checkedAt,
	}, true
}

// genfollowPaperWireQuote consumes the complete cached-book receipt already obtained by funded
// Paper. Kalshi's cached Paper boundary stamps the ladder and its WebSocket provenance atomically;
// do not perform another client/REST lookup here because that could bind a different ladder to the
// receipt or turn a valid hermetic/cache-only execution into a false zero-fill.
func (s *Server) genfollowPaperWireQuote(sig storage.Signal) (liveMirrorQuote, string) {
	embedded, ok := paperTakerQuoteFromSignal(sig)
	if !ok {
		return liveMirrorQuote{}, "paper-final-wire-book-receipt-incomplete"
	}
	if !strings.EqualFold(strings.TrimSpace(sig.Platform), "kalshi") {
		return embedded, ""
	}
	if livePolicyMirrorBookReceiptFrom(embedded.BookSource).ok {
		return embedded, ""
	}
	return liveMirrorQuote{}, "paper-final-wire-kalshi-book-provenance-incomplete"
}

// fundedPaperSignalFromCanonicalExecution turns the one shared canonical q1 execution terminal
// into the immutable execution economics consumed by funded Paper. Portfolio admission may still
// refuse the opportunity, but it must never start a second book clock or substitute a later quote.
// The canonical terminal carries taker-side depth only; bid depth is deliberately recorded as
// unavailable (zero) in the legacy JSON audit fields rather than copied from a later book.
func fundedPaperSignalFromCanonicalExecution(sig storage.Signal,
	event *storage.ExecutionShadowEvent) (storage.Signal, string, bool) {
	if event == nil || event.ShadowFilledQty == nil || *event.ShadowFilledQty < 1 ||
		event.ShadowFillPrice == nil || event.ShadowFee == nil ||
		strings.TrimSpace(event.ShadowFeeSource) == "" ||
		event.SideBid == nil || event.SideAsk == nil || event.VisibleDepth == nil ||
		event.TickSize == nil || event.SpreadCents == nil ||
		strings.TrimSpace(event.BookSource) == "" {
		return storage.Signal{}, "", false
	}
	qty := *event.ShadowFilledQty
	ask, bid, depth, tick := *event.ShadowFillPrice, *event.SideBid, *event.VisibleDepth, *event.TickSize
	if qty < 1 || ask <= 0 || ask >= 1 || bid <= 0 || bid >= ask ||
		depth < 1 || tick <= 0 || tick > 1 {
		return storage.Signal{}, "", false
	}
	bidDepth := 0.0
	quoteAgeS := 0.0
	// Preserve age at the canonical wire decision. Paper may consume the terminal seconds later;
	// adding that queue delay would falsely describe the due-time execution book as stale.
	if event.BookAgeMS != nil && *event.BookAgeMS >= 0 {
		quoteAgeS = *event.BookAgeMS / 1000
	}
	feePC := *event.ShadowFee / qty
	sig.EntryPrice, sig.BookDepth, sig.SpreadCents = ask, depth, *event.SpreadCents
	sig.FeePC = &feePC
	sig.BookBid, sig.BookAsk = &bid, &ask
	sig.BookBidDepth, sig.BookAskDepth = &bidDepth, &depth
	sig.BookQuoteAgeS, sig.BookTakerTick = &quoteAgeS, &tick
	sig.BookSource = strings.TrimSpace(event.BookSource)
	sig.PricingVersion = canonicalSystemShadowModelVersion + "/funded-paper-fanout"
	return sig, strings.TrimSpace(event.ShadowFeeSource), true
}

// genfollowConsider routes one freshly-logged signal row into the follower when its family is a
// measured-positive roster member. Called from the insertSignal choke point on EVERY row — the
// gate ladder is ordered cheapest-first and self-refuses almost always.
func (s *Server) genfollowConsider(ctx context.Context, sig storage.Signal) {
	s.genfollowConsiderWithShadow(ctx, sig, s.executionShadowAttemptForSignal(sig))
}

func (s *Server) genfollowConsiderWithShadow(ctx context.Context, sig storage.Signal,
	shadowAttemptID string) {
	s.genfollowConsiderWithShadowTerminal(ctx, sig, shadowAttemptID,
		fundedPaperLiveTerminalReceipt{})
}

func (s *Server) genfollowConsiderWithShadowTerminal(ctx context.Context, sig storage.Signal,
	shadowAttemptID string, liveTerminal fundedPaperLiveTerminalReceipt) {
	s.genfollowConsiderWithShadowTerminalAt(ctx, sig, shadowAttemptID, liveTerminal, time.Now().UTC())
}

func (s *Server) genfollowConsiderWithShadowTerminalAt(ctx context.Context, sig storage.Signal,
	shadowAttemptID string, liveTerminal fundedPaperLiveTerminalReceipt,
	signalAt time.Time) (terminal genfollowPaperTerminalResult) {
	return s.genfollowConsiderWithShadowTerminalCanonicalAt(ctx, sig, shadowAttemptID,
		liveTerminal, nil, signalAt)
}

func (s *Server) genfollowConsiderWithShadowTerminalCanonicalAt(ctx context.Context, sig storage.Signal,
	shadowAttemptID string, liveTerminal fundedPaperLiveTerminalReceipt,
	canonicalExecution *storage.ExecutionShadowEvent,
	signalAt time.Time) (terminal genfollowPaperTerminalResult) {
	if signalAt.IsZero() {
		signalAt = time.Now().UTC()
	} else {
		signalAt = signalAt.UTC()
	}
	plat := strings.ToLower(strings.TrimSpace(sig.Platform))
	side := strings.ToUpper(strings.TrimSpace(sig.Side))
	originalSignal, originalSide, discoveryPrice, invertedSide := gfDiscoveryLineage(sig)
	isInverse := strings.HasPrefix(strings.ToLower(strings.TrimSpace(sig.SignalType)), "invert:")
	if isInverse {
		s.gfJournalMu.Lock()
		defer s.gfJournalMu.Unlock()
	}
	auditState, auditReason := "REJECTED", "invalid-venue"
	var reference, decision storage.Signal
	var referenceFeeSource, decisionFeeSource, feeSource string
	var contracts, fee, currentNetEdge float64
	var attemptID string
	var attemptAt time.Time
	var attemptedContracts, attemptedLimitPrice float64
	var configuredExecutionDelay, observedExecutionDelay time.Duration
	var configuredWireDelay, observedWireDelay time.Duration
	var registeredRoute gfRegisteredSignalRoute
	_, _, staticRouteDeclared := gfSignalContractBinding(sig)
	// Every direct and independently named inverse Paper decision gets one durable structured
	// receipt, including refusals. Accepted fills also remain in the portfolio ledger and settlement
	// trail; this audit row explains exactly where each signal left the collection funnel.
	defer func() {
		snapshot := decision
		if snapshot.BookAsk == nil {
			snapshot = reference
		}
		fingerprint := fmt.Sprintf("%s|%s|%.6f|%d|%s|%.6f|%.6f", auditState, auditReason,
			discoveryPrice, sig.Episode, sig.ExecExpr, snapshot.EntryPrice, currentNetEdge)
		filledContracts := 0.0
		fillPrice, fillFee, fillFeeSource := 0.0, 0.0, ""
		if auditState == "PAPER-FILLED" {
			filledContracts, fillPrice, fillFee, fillFeeSource =
				contracts, snapshot.EntryPrice, fee, feeSource
		}
		terminal = genfollowPaperTerminalResult{State: auditState, Reason: auditReason,
			AttemptID: attemptID, FeeSource: fillFeeSource,
			AttemptedContracts: attemptedContracts, LimitPrice: attemptedLimitPrice,
			FilledContracts: filledContracts, FillPrice: fillPrice, Fee: fillFee}
		terminalAt := time.Now().UTC()
		// Liveness only: this records the already-decided realistic Paper result under the exact
		// family+venue+side+route+input-topology contract and cannot alter routing or allocation.
		s.noteR147PaperRouteOutcome(sig, "taker", auditState, auditReason, terminalAt)
		s.executionShadowRecordPaperAttempt(shadowAttemptID, auditState, auditReason, attemptID, terminalAt,
			attemptedContracts, attemptedLimitPrice, filledContracts, fillPrice, fillFee,
			fillFeeSource, snapshot)
		inputTopology := r147SignalInputTopology(sig)
		signalContractID := ""
		if registeredRoute.Static {
			inputTopology = registeredRoute.Contract.InputTopology
			signalContractID = r147SignalContractIdentity(registeredRoute.Contract)
		}
		if auditState == "REJECTED" && !staticRouteDeclared && !s.gfRouteAuditDue(
			strings.ToLower(sig.SignalType)+"\x00"+plat+"\x00"+sig.Ticker+"\x00"+side+"\x00"+inputTopology,
			fingerprint, time.Now()) {
			return
		}
		detail := map[string]any{
			"system": sig.SignalType, "venue": plat, "ticker": sig.Ticker,
			"market_title": sig.Title, "market_type": sig.MarketType,
			"original_signal": originalSignal, "original_side": originalSide,
			"discovery_price": discoveryPrice, "inverted_side": invertedSide,
			"state": auditState, "reason": auditReason,
			"book_source": snapshot.BookSource, "ask": snapshot.BookAsk, "bid": snapshot.BookBid,
			"ask_depth": snapshot.BookAskDepth, "bid_depth": snapshot.BookBidDepth,
			"spread_cents": snapshot.SpreadCents, "quote_age_s": snapshot.BookQuoteAgeS,
			"taker_tick": snapshot.BookTakerTick, "contracts": contracts,
			"attempt_id": attemptID, "attempted_contracts": attemptedContracts,
			"filled_contracts": filledContracts, "limit_price": attemptedLimitPrice,
			"configured_execution_delay_ms": float64(configuredExecutionDelay) / float64(time.Millisecond),
			"observed_execution_delay_ms":   float64(observedExecutionDelay) / float64(time.Millisecond),
			"configured_wire_delay_ms":      float64(configuredWireDelay) / float64(time.Millisecond),
			"observed_wire_delay_ms":        float64(observedWireDelay) / float64(time.Millisecond),
			"trigger_unix_ms":               signalAt.UnixMilli(),
			"input_topology":                inputTopology,
			"signal_contract":               signalContractID,
			"static_contract_declared":      staticRouteDeclared,
			"static_contract_bound":         registeredRoute.Static,
			"execution_generation":          fundedPaperCorrectedExecutionGenerationV1,
			"book_taker_fee_pc":             snapshot.BookTakerFeePC, "book_maker_fee_pc": snapshot.BookMakerFeePC,
			"fee": fee, "fee_source": feeSource, "reference_fee_source": referenceFeeSource,
			"decision_fee_source":      decisionFeeSource,
			"predicted_net_edge_cents": currentNetEdge * 100,
			"expected_slippage":        0, "slippage_model": "delayed-two-touch-wire-ioc-v2",
			"resolve_hours": sig.ResolveHours, "market_status_required": "open-and-accepting-orders",
			"settlement_identity": "same-instrument-complementary-binary-outcome",
			"order_result":        auditState,
		}
		if auditState == "PAPER-FILLED" {
			detail["fill_price"] = snapshot.EntryPrice
		}
		if !attemptAt.IsZero() {
			detail["attempt_at"] = attemptAt.Format(time.RFC3339Nano)
		}
		encoded, _ := json.Marshal(detail)
		category := "system-route"
		if isInverse {
			category = "inverse-system"
		}
		auditCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		defer cancel()
		_ = s.store.Audit(auditCtx, "info", category,
			fmt.Sprintf("%s %s %s %s: %s", auditState, sig.SignalType, plat, sig.Ticker, auditReason), string(encoded))
	}()
	if plat != "kalshi" && plat != "polyus" { // poly-int is venue-locked by construction
		return
	}
	if sig.Ticker == "" {
		auditReason = "missing-ticker"
		return
	}
	if !s.genfollowOn() {
		auditReason = "paper-executor-disabled"
		return
	}
	if s.ksBlocked() {
		auditReason = "kill-switch-tripped"
		return
	}
	if s.gfBookPoisoned.Load() {
		auditReason = "paper-ledger-unreadable"
		return
	}
	// Some high-speed producers deliberately use cache-only metadata. A missing cached clock is
	// represented as zero; it is not evidence that the market is closed. Resolve that UNKNOWN once
	// through the authoritative money clock before rejecting. Positive cached clocks still retain
	// the cheap screen, and the executor repeats the authoritative check immediately before fill.
	if !paperEntryHorizonValueKnown(sig.ResolveHours) {
		sig.ResolveHours = s.paperEntryResolveHours(ctx, plat, sig.Ticker, sig.Title)
	}
	if !s.paperEntryHorizonOK(sig.ResolveHours, sig.Ticker, sig.Title) {
		auditReason = "outside-paper-entry-horizon"
		return
	}
	if side != "YES" && side != "NO" {
		auditReason = "invalid-inverted-side"
		return
	}
	var routeWhy string
	registeredRoute, routeWhy = s.gfRegisteredRouteForSignal(sig)
	if routeWhy != "" {
		auditReason = routeWhy
		return
	}
	cell := registeredRoute.Verdict
	// Convert the discovery row into a complete executable reference receipt. A shared canonical
	// terminal is already the execution receipt; funded Paper consumes it directly and must not
	// replace it with a later resident book. Legacy/hermetic callers without a canonical terminal
	// retain the old cache-only path.
	var referenceOK bool
	if canonicalExecution != nil {
		reference, referenceFeeSource, referenceOK =
			fundedPaperSignalFromCanonicalExecution(sig, canonicalExecution)
	} else {
		reference, referenceFeeSource, referenceOK = s.completeBookSignalCached(sig)
	}
	if !referenceOK || reference.EntryPrice < gfPriceLo || reference.EntryPrice > gfPriceHi ||
		reference.SpreadCents > gfSpreadCapC || reference.FeePC == nil {
		auditReason = "reference-book-incomplete-stale-wide-or-out-of-range"
		return
	}
	sig = reference
	ref := sig.EntryPrice
	// completeBookSignalCached already requires a current lifecycle-valid full book with both
	// touches, depth, ticks and exact fee fields. Do not override that exact receipt with
	// pxOfflineReason's older background collector clocks.
	if r := s.gfConflictReason(ctx, plat, sig.Ticker, side); r != "" {
		auditReason = "position-conflict:" + r
		return
	}
	key := gfRosterKey(sig.SignalType, plat, side, cell.Route, gfRosterTopology(registeredRoute.Contract))
	book := vbKalshi
	if plat == "polyus" {
		book = vbPolyus
	}
	// One open lot per market across the WHOLE follower (two families sharing a detection must
	// not stack the same market) + per-sub runaway guard — BEFORE any pricing work.
	s.gfBookMu.Lock()
	subHeld := 0.0
	{
		st := s.gfLoadLocked()
		for _, b := range st.Subs {
			for _, p := range b.Open {
				if p.Ticker == sig.Ticker {
					s.gfBookMu.Unlock()
					auditReason = "market-already-open-in-paper-follower"
					return
				}
			}
		}
		if b := st.Subs[key]; b != nil {
			if len(b.Open) >= gfMaxOpenPerSub {
				s.gfBookMu.Unlock()
				auditReason = "per-system-open-position-cap"
				return
			}
			subHeld = kfLotsExposure(b.Open)
		}
	}
	s.gfBookMu.Unlock()
	// R128 book gate: venue-book wall + the scoreboard-weight share (never under gfBookMu —
	// bookOpenExposure takes the sub-book mutexes, including ours). A roster line that doesn't
	// exist yet (family qualified between sweeps) refuses here and places on the next sweep.
	subCap, okShare := s.subShareUSD(key)
	if registeredRoute.Static && (!okShare || subCap <= 0) {
		// A first-ever exact route cannot have a score-table allocation yet. Give it a neutral,
		// bounded research slice; the executor still places exactly one share and the venue-wide
		// portfolio wall below remains authoritative.
		subCap = 0.05 * s.bookSizingEquityUSD(ctx, book)
		okShare = subCap > 0
	}
	if !okShare || subCap <= 0 {
		auditReason = "no-positive-paper-allocation"
		return
	}
	venueAvail := s.bookAvailableUSD(ctx, book)
	if s.bookSizingEquityUSD(ctx, book) <= 0 || venueAvail <= 0 || subHeld >= subCap {
		auditReason = "paper-portfolio-capital-unavailable"
		return
	}
	// TAKER at the live ask, 3¢ never-chase against the signal's (possibly inverted) reference.
	// The discovery row's resolve-hours value is an early cheap screen. Re-read authoritative
	// venue time after the current quote/fee check so a stale signal cannot fund a late entry.
	if canonicalExecution == nil && !s.paperEntryHorizonNow(ctx, plat, sig.Ticker, sig.Title) {
		auditReason = "authoritative-time-remaining-outside-entry-horizon"
		return
	}
	// Reprice only after every potentially slow lifecycle check. This is a second complete receipt;
	// the earlier reference cannot authorize a later stale, wider or differently ticked book.
	var quoteOK bool
	if canonicalExecution != nil {
		decision, decisionFeeSource, quoteOK = reference, referenceFeeSource, true
	} else {
		decision, decisionFeeSource, quoteOK = s.completeBookSignalCached(sig)
	}
	if !quoteOK || decision.BookBid == nil || decision.BookAsk == nil ||
		decision.BookBidDepth == nil || decision.BookAskDepth == nil ||
		decision.BookQuoteAgeS == nil || decision.BookTakerTick == nil {
		auditReason = "final-book-reprice-incomplete-or-stale"
		return
	}
	ask, depth := decision.EntryPrice, decision.BookDepth
	if ask <= 0 || ask > ref+0.03 || ask < gfPriceLo || ask > gfPriceHi ||
		decision.SpreadCents > gfSpreadCapC {
		auditReason = "final-price-moved-too-far-wide-or-out-of-range"
		return
	}
	// Old assumed-fill cents/share may nominate this lane as a hypothesis, but cannot size it.
	// Corrected Paper therefore collects exactly one share under the explicit exploration slice.
	stake := math.Min(subCap-subHeld, venueAvail)
	contracts = 1
	contracts = executableContracts(contracts, depth)
	if contracts < 1 {
		auditReason = "opposite-side-touch-depth-below-one-share"
		return
	}
	// The exploration rail remains fee-inclusive and still requires the exact venue fee.
	var feeKnown bool
	if canonicalExecution != nil {
		// The canonical execution-only lane is explicitly q1. Reuse its exact terminal fee; asking
		// the fee service again here would create another branch that Paper and Shadow could split on.
		fee, feeSource, feeKnown = *canonicalExecution.ShadowFee,
			strings.TrimSpace(canonicalExecution.ShadowFeeSource), true
	} else {
		for attempt := 0; attempt < 4; attempt++ {
			fee, feeSource, feeKnown = s.fillFeeReceipt(plat, sig.Ticker, false, contracts, ask)
			if !feeKnown || strings.TrimSpace(feeSource) == "" {
				auditReason = "exact-sized-taker-fee-unavailable"
				return
			}
			if contracts*ask+fee <= stake+1e-9 {
				break
			}
			next := math.Floor(stake / (ask + fee/contracts))
			if next >= contracts {
				next = contracts - 1
			}
			contracts = executableContracts(next, depth)
			if contracts < 1 {
				auditReason = "fee-inclusive-sizing-rail-cannot-afford-one-share"
				return
			}
		}
	}
	if !feeKnown || strings.TrimSpace(feeSource) == "" {
		auditReason = "exact-sized-taker-fee-unavailable"
		return
	}
	cost := contracts*ask + fee
	if cost > stake+1e-9 {
		auditReason = "exact-fee-sizing-did-not-converge"
		return
	}
	currentNetEdge = 0 // hypothesis magnitude is deliberately void as allocation/profit authority
	// R154 delayed IOC simulation. This is reached only on the dedicated Paper worker, after the
	// LIVE-first intent handoff. Persist the attempted order before waiting; a crash during the wait
	// therefore leaves an honest attempt with no lot, never a silent instant fill.
	attemptedContracts, attemptedLimitPrice = contracts, ask
	configuredExecutionDelay = s.genfollowPaperTakerDelay()
	attemptAt = time.Now().UTC()
	if canonicalExecution != nil {
		// The canonical worker owned the only execution clock. Its due time is deterministic from
		// the shared trigger, even though funded Paper may read the durable terminal slightly later.
		attemptAt = signalAt.Add(configuredExecutionDelay).UTC()
	}
	attemptID = fmt.Sprintf("gf-%d-%s-%s", attemptAt.UnixNano(), plat, sig.Ticker)
	if err := s.recordGenfollowPaperTakerAttempt(ctx, sig, signalAt, attemptID,
		shadowAttemptID, attemptAt,
		configuredExecutionDelay, attemptedLimitPrice, attemptedContracts, fee, feeSource, decision); err != nil {
		auditState = "PAPER-ERROR"
		auditReason = "paper-taker-attempt-receipt-failed:" + err.Error()
		return
	}
	auditState, auditReason = "PAPER-ATTEMPTED", "waiting-for-delayed-executable-book"
	zeroFill := func(reason string) {
		auditState, auditReason = "PAPER-ZERO-FILL", reason
		contracts, fee, currentNetEdge = 0, 0, 0
	}
	notObserved := func(reason string) {
		auditState, auditReason = "PAPER-NOT-OBSERVED", reason
		contracts, fee, currentNetEdge = 0, 0, 0
	}
	var initialWireQuote liveMirrorQuote
	var initialExecution storage.Signal
	var wireWhy string
	if canonicalExecution != nil {
		initialExecution = decision
		var canonicalWhy string
		initialWireQuote, canonicalWhy = s.genfollowPaperWireQuote(decision)
		if canonicalWhy != "" {
			zeroFill("canonical-execution-receipt-incomplete:" + canonicalWhy)
			return
		}
		observedExecutionDelay = canonicalExecution.At.Sub(signalAt)
		if observedExecutionDelay < 0 {
			observedExecutionDelay = 0
		}
		configuredWireDelay = s.livePolicyMirrorFinalWireDelay()
		observedWireDelay = configuredWireDelay
		auditReason = "shared-canonical-execution-receipt"
	} else {
		if ctx.Err() != nil {
			notObserved("execution-context-ended-before-delayed-book-observation")
			return
		}
		timer := time.NewTimer(configuredExecutionDelay)
		select {
		case <-timer.C:
			observedExecutionDelay = time.Since(attemptAt)
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			observedExecutionDelay = time.Since(attemptAt)
			notObserved("execution-delay-canceled-before-book-recheck")
			return
		}
		// Time and book are both re-read after the measured wait. A second cache-only read after the
		// simulated wire delay owns the fill decision below; funded Paper never turns this first
		// post-delay touch directly into a lot.
		if ctx.Err() != nil {
			notObserved("execution-context-ended-before-book-recheck")
			return
		}
		if !s.paperEntryHorizonNow(ctx, plat, sig.Ticker, sig.Title) {
			if ctx.Err() != nil {
				notObserved("execution-context-ended-during-time-recheck")
				return
			}
			zeroFill("execution-time-remaining-outside-entry-horizon")
			return
		}
		execution, executionFeeSource, executionOK := s.completeBookSignalCached(sig)
		decision, decisionFeeSource = execution, executionFeeSource
		if !executionOK || execution.BookBid == nil || execution.BookAsk == nil ||
			execution.BookBidDepth == nil || execution.BookAskDepth == nil || execution.BookQuoteAgeS == nil ||
			execution.BookTakerTick == nil {
			if ctx.Err() != nil {
				notObserved("execution-context-ended-before-first-book-completed")
				return
			}
			zeroFill("execution-book-incomplete-or-stale-after-delay")
			return
		}
		initialWireQuote, wireWhy = s.genfollowPaperWireQuote(execution)
		if wireWhy != "" {
			if ctx.Err() != nil {
				notObserved("execution-context-ended-before-first-book-completed")
				return
			}
			zeroFill(wireWhy)
			return
		}
		initialExecution = execution
		configuredWireDelay = s.livePolicyMirrorFinalWireDelay()
		wireStarted := time.Now()
		if !waitLivePolicyMirror(ctx, configuredWireDelay) {
			observedWireDelay = time.Since(wireStarted)
			notObserved("execution-wire-delay-canceled-before-final-book-recheck")
			return
		}
		observedWireDelay = time.Since(wireStarted)
		if ctx.Err() != nil {
			notObserved("execution-context-ended-before-final-book-recheck")
			return
		}
		if !s.paperEntryHorizonNow(ctx, plat, sig.Ticker, sig.Title) {
			if ctx.Err() != nil {
				notObserved("execution-context-ended-during-final-time-recheck")
				return
			}
			zeroFill("execution-time-remaining-outside-entry-horizon-at-final-wire")
			return
		}
		finalExecution, finalExecutionFeeSource, finalExecutionOK :=
			s.completeBookSignalCached(sig)
		if !finalExecutionOK || finalExecution.BookBid == nil || finalExecution.BookAsk == nil ||
			finalExecution.BookBidDepth == nil || finalExecution.BookAskDepth == nil || finalExecution.BookQuoteAgeS == nil ||
			finalExecution.BookTakerTick == nil {
			if ctx.Err() != nil {
				notObserved("execution-context-ended-before-final-book-completed")
				return
			}
			zeroFill("execution-book-incomplete-or-stale-at-final-wire")
			return
		}
		finalWireQuote, wireWhy := s.genfollowPaperWireQuote(finalExecution)
		if wireWhy != "" {
			if ctx.Err() != nil {
				notObserved("execution-context-ended-before-final-book-completed")
				return
			}
			zeroFill(wireWhy)
			return
		}
		if strings.EqualFold(plat, "kalshi") {
			if zeroFillWhy := s.fundedPaperLiveZeroFillReason(
				shadowAttemptID, sig.Ticker, side, finalWireQuote.Price,
				finalWireQuote); zeroFillWhy != "" {
				zeroFill(zeroFillWhy)
				return
			}
		}
		if strings.EqualFold(plat, "kalshi") &&
			!livePolicyMirrorBookUsableAtWire(initialWireQuote, finalWireQuote) {
			zeroFill("execution-final-wire-book-provenance-regressed")
			return
		}
		finalExecution.BookSource = finalWireQuote.BookSource
		execution, executionFeeSource = finalExecution, finalExecutionFeeSource
		decision, decisionFeeSource = execution, executionFeeSource
		executionAsk, executionDepth := execution.EntryPrice, execution.BookDepth
		if executionAsk <= 0 || executionAsk < gfPriceLo || executionAsk > gfPriceHi ||
			execution.SpreadCents > gfSpreadCapC {
			zeroFill("execution-price-wide-or-out-of-range-at-final-wire")
			return
		}
		if executionAsk > attemptedLimitPrice+1e-9 {
			zeroFill("execution-ask-above-original-limit-at-final-wire")
			return
		}
		contracts = executableContracts(attemptedContracts, executionDepth)
		if contracts < 1 {
			zeroFill("execution-touch-depth-below-one-share-at-final-wire")
			return
		}
		ask, depth = executionAsk, executionDepth
		for attempt := 0; attempt < 4; attempt++ {
			fee, feeSource, feeKnown = s.fillFeeReceipt(plat, sig.Ticker, false, contracts, ask)
			if !feeKnown || strings.TrimSpace(feeSource) == "" {
				zeroFill("execution-exact-sized-taker-fee-unavailable")
				return
			}
			if contracts*ask+fee <= stake+1e-9 {
				break
			}
			next := math.Floor(stake / (ask + fee/contracts))
			if next >= contracts {
				next = contracts - 1
			}
			contracts = executableContracts(next, executionDepth)
			if contracts < 1 {
				zeroFill("execution-fee-inclusive-sizing-cannot-afford-one-share")
				return
			}
		}
		cost = contracts*ask + fee
		if cost > stake+1e-9 {
			zeroFill("execution-exact-fee-sizing-did-not-converge")
			return
		}
		if canonicalExecution != nil {
			if canonicalExecution.ShadowFilledQty == nil ||
				*canonicalExecution.ShadowFilledQty+1e-9 < contracts ||
				canonicalExecution.ShadowFillPrice == nil ||
				math.Abs(*canonicalExecution.ShadowFillPrice-ask) > 1e-9 ||
				canonicalExecution.ShadowFee == nil ||
				math.Abs(*canonicalExecution.ShadowFee-fee) > 1e-9 ||
				strings.TrimSpace(canonicalExecution.ShadowFeeSource) == "" ||
				strings.TrimSpace(canonicalExecution.BookSource) == "" ||
				strings.TrimSpace(canonicalExecution.BookSource) != strings.TrimSpace(decision.BookSource) {
				zeroFill("paper-execution-does-not-match-canonical-shadow-fill")
				return
			}
		}
	}
	currentNetEdge = 0
	auditState, auditReason = "PAPER-ERROR", "delayed-book-was-executable-but-ledger-write-did-not-complete"
	s.autoMu.Lock()
	slRatio, slMode := s.autoSLTPRatio, s.autoSLTPMode
	s.autoMu.Unlock()
	sl := 0.0
	if slMode != "ride" && slRatio > 0 {
		sl = math.Max(0.01, ask-slRatio*ask)
	}
	fam := sig.SignalType
	routeFamily := fam
	lineage := strings.NewReplacer(" ", "_", "\t", "_", "\r", "_", "\n", "_").Replace(strings.TrimSpace(sig.ExecExpr))
	if lineage == "" {
		lineage = "direct-emitted-side=" + side
	}
	route := "fam:" + routeFamily
	route += " origin=" + cell.OriginLayer
	route += " route=" + cell.Route
	route += " side=" + side
	route += " original_signal=" + originalSignal
	route += " original_side=" + originalSide
	route += fmt.Sprintf(" discovery_price=%.6f", discoveryPrice)
	route += " inverted_side=" + invertedSide
	route += " lineage=" + lineage
	route += " book=" + decision.BookSource
	route += fmt.Sprintf(" ask=%.6f bid=%.6f spread_c=%.4f quote_age_s=%.4f tick=%.6f", ask,
		*decision.BookBid, decision.SpreadCents, *decision.BookQuoteAgeS, *decision.BookTakerTick)
	route += fmt.Sprintf(" depth_visible=%.4f depth_used=%.0f fee=%.6f fee_pc=%.6f", depth, contracts, fee, fee/contracts)
	route += " fee_source=" + feeSource
	route += " reference_fee_source=" + referenceFeeSource
	route += fmt.Sprintf(" paper_attempt=%s original_limit=%.6f attempted_depth=%.0f execution_delay_ms=%.3f wire_delay_ms=%.3f",
		attemptID, attemptedLimitPrice, attemptedContracts,
		float64(observedExecutionDelay)/float64(time.Millisecond),
		float64(observedWireDelay)/float64(time.Millisecond))
	route += fmt.Sprintf(" evidence_mean_ask=%.6f evidence_mean_fee_pc=%.6f reference_ask=%.6f predicted_current_net_edge_c=%.4f",
		cell.MeanAsk, cell.FeePC, ref, currentNetEdge*100)
	route += " allocation=conservative-net-calendar-day-share"
	if isInverse {
		// Freeze the final repriced book onto the already-durable named signal immediately before the
		// Paper ledger write. This both proves ordering and gives the later exact-family fill update a
		// current five-second decision receipt even when earlier lifecycle checks took several seconds.
		decision.PricingVersion = "independent-inverse-executable-v1"
		decision.ExecExpr = sig.ExecExpr + "/book=" + decision.BookSource + "/fee=" + decisionFeeSource
		refreshed, refreshErr := s.store.RefreshSignalFamilyExecutionReceipt(ctx, decision)
		if refreshErr != nil {
			auditReason = "inverse-final-book-receipt-storage-error:" + refreshErr.Error()
			return
		}
		if !refreshed {
			auditReason = "inverse-final-book-receipt-missing-filled-or-stale"
			return
		}
	}
	decisionAt := time.Now().UTC()
	nowTS := decisionAt.Format(time.RFC3339Nano)
	var inversePlacement storage.InversePaperPlacement
	if isInverse {
		// The prepared row is the durable outbox boundary. It binds the exact independently named
		// signal before the JSON book can show a position. Only a later atomic signal-fill+journal
		// commit gives that lot settlement authority.
		var prepareErr error
		inversePlacement, prepareErr = s.store.PrepareInversePaperPlacement(ctx, plat, sig.Ticker,
			side, routeFamily, ask, fee/contracts, contracts, decisionAt)
		if prepareErr != nil {
			auditReason = "inverse-placement-journal-prepare-failed:" + prepareErr.Error()
			return
		}
		nowTS = inversePlacement.DecisionTS

		// Preparing the durable outbox can wait on SQLite. Nothing priced before that wait is allowed
		// to authorize money. Re-check authoritative time first, then make the complete book read the
		// final operation before the in-memory lot is appended. The immutable journal already binds
		// ask/fee/size, so any changed economics are rolled back and retried on a later signal rather
		// than quietly mutating the prepared order.
		rollbackPrepared := func(reason string) {
			if _, rollbackErr := s.store.RollbackInversePaperPlacement(context.WithoutCancel(ctx),
				inversePlacement.PlacementID, reason); rollbackErr != nil {
				_ = s.store.Audit(context.WithoutCancel(ctx), "error", "inverse-system",
					"Inverse pre-submit refusal could not roll back its prepared journal", rollbackErr.Error())
			}
		}
		if canonicalExecution != nil {
			// The journal is only a durable portfolio outbox. Its write latency cannot reopen the
			// execution decision: compare it to the immutable shared terminal, then project that
			// same receipt without a fresh fee or book lookup.
			if math.Abs(ask-inversePlacement.FillPrice) > 1e-9 ||
				math.Abs(fee/contracts-inversePlacement.FeePC) > 1e-9 ||
				math.Abs(contracts-inversePlacement.Contracts) > 1e-9 {
				reason := "canonical-economics-do-not-match-prepared-inverse-placement"
				rollbackPrepared(reason)
				zeroFill(reason)
				return
			}
			route += " submit_source=shared-canonical-execution-terminal"
		} else {
			if !s.paperEntryHorizonNow(ctx, plat, sig.Ticker, sig.Title) {
				reason := "authoritative-time-changed-after-journal-prepare"
				rollbackPrepared(reason)
				zeroFill(reason)
				return
			}
			submission, submissionDecisionFeeSource, submissionOK := s.completeBookSignalCached(sig)
			decision, decisionFeeSource = submission, submissionDecisionFeeSource
			if !submissionOK || submission.BookBid == nil || submission.BookAsk == nil ||
				submission.BookBidDepth == nil || submission.BookAskDepth == nil ||
				submission.BookQuoteAgeS == nil || submission.BookTakerTick == nil || submission.FeePC == nil {
				reason := "submission-book-incomplete-or-stale-after-journal-prepare"
				rollbackPrepared(reason)
				zeroFill(reason)
				return
			}
			submissionAsk, submissionDepth := submission.EntryPrice, submission.BookDepth
			if submissionAsk <= 0 || submissionAsk < gfPriceLo || submissionAsk > gfPriceHi ||
				submissionAsk > ref+0.03 || submission.SpreadCents > gfSpreadCapC ||
				submissionDepth+1e-9 < contracts {
				reason := "submission-price-spread-or-depth-failed-after-journal-prepare"
				rollbackPrepared(reason)
				zeroFill(reason)
				return
			}
			submissionFee, submissionFeeSource, submissionFeeKnown := s.fillFeeReceipt(
				plat, sig.Ticker, false, contracts, submissionAsk)
			if !submissionFeeKnown || strings.TrimSpace(submissionFeeSource) == "" ||
				submissionFee < 0 {
				reason := "submission-exact-fee-failed-after-journal-prepare"
				rollbackPrepared(reason)
				zeroFill(reason)
				return
			}
			if math.Abs(submissionAsk-inversePlacement.FillPrice) > 1e-9 ||
				math.Abs(submissionFee/contracts-inversePlacement.FeePC) > 1e-9 ||
				math.Abs(contracts-inversePlacement.Contracts) > 1e-9 {
				reason := "submission-economics-changed-after-journal-prepare"
				rollbackPrepared(reason)
				zeroFill(reason)
				return
			}
			ask, depth, fee, feeSource, currentNetEdge = submissionAsk, submissionDepth,
				submissionFee, submissionFeeSource, 0
			route += fmt.Sprintf(" submit_book=%s submit_bid=%.6f submit_ask=%.6f submit_spread_c=%.4f submit_depth_visible=%.4f submit_quote_age_s=%.4f submit_tick=%.6f submit_fee_source=%s",
				decision.BookSource, *decision.BookBid, ask, decision.SpreadCents, depth,
				*decision.BookQuoteAgeS, *decision.BookTakerTick, feeSource)
		}
	}
	// The exact settlement certificate is money truth, not collector metadata. Re-read it after
	// every potentially slow sizing/book/journal step and immediately before the Paper projection.
	// If raw venue rules changed, direct routes refuse and inverse routes roll back their outbox;
	// stale Paper evidence can therefore never become future LIVE proof.
	if ctx.Err() != nil {
		if isInverse {
			_, _ = s.store.RollbackInversePaperPlacement(context.WithoutCancel(ctx),
				inversePlacement.PlacementID, "original Paper signal deadline ended before final identity check")
		}
		auditState = "PAPER-NOT-OBSERVED"
		auditReason = "paper-original-signal-deadline-before-final-identity"
		return
	}
	if crossReason := s.paperR148CrossVenueVariantReason(ctx, sig, isInverse); crossReason != "" {
		if isInverse {
			_, _ = s.store.RollbackInversePaperPlacement(context.WithoutCancel(ctx),
				inversePlacement.PlacementID, "cross-venue certificate changed before Paper submit")
		}
		auditReason = "cross-venue-final-paper-identity-check:" + crossReason
		return
	}
	commitWireQuote, commitWireWhy := s.genfollowPaperWireQuote(decision)
	if strings.EqualFold(plat, "kalshi") && commitWireWhy != "" {
		if isInverse {
			_, _ = s.store.RollbackInversePaperPlacement(context.WithoutCancel(ctx),
				inversePlacement.PlacementID, "final Paper commit book receipt incomplete")
		}
		zeroFill(commitWireWhy)
		return
	}
	if ctx.Err() != nil {
		if isInverse {
			_, _ = s.store.RollbackInversePaperPlacement(context.WithoutCancel(ctx),
				inversePlacement.PlacementID, "original Paper signal deadline ended before final projection")
		}
		auditState = "PAPER-NOT-OBSERVED"
		auditReason = "paper-original-signal-deadline-before-final-projection"
		return
	}
	if !isInverse {
		// Settlement takes the same journal mutex. Hold it only across the final in-memory
		// projection and durable file replace so a deadline rollback cannot race settlement.
		s.gfJournalMu.Lock()
		defer s.gfJournalMu.Unlock()
	}
	s.gfBookMu.Lock()
	st := s.gfLoadLocked()
	b := st.Subs[key]
	if b == nil {
		b = &kfBook{}
		st.Subs[key] = b
	}
	held := kfLotsExposure(b.Open)
	dup := false
	for _, sb := range st.Subs {
		for _, p := range sb.Open {
			if p.Ticker == sig.Ticker {
				dup = true
			}
		}
	}
	if dup || held+cost > subCap || cost > venueAvail {
		s.gfBookMu.Unlock()
		if isInverse {
			_, _ = s.store.RollbackInversePaperPlacement(context.WithoutCancel(ctx),
				inversePlacement.PlacementID, "capacity or duplicate changed before JSON projection")
		}
		if dup {
			auditReason = "market-became-duplicate-before-submit"
		} else {
			auditReason = "paper-capacity-changed-before-submit"
		}
		return
	}
	executionFinalAt := time.Now().UTC()
	pos := kfPos{TS: nowTS, Ticker: sig.Ticker,
		Title: sig.Title, Side: side, Price: ask, Contracts: contracts, Fee: fee, SL: sl,
		FillKind: "taker", FillRule: "genfollow", FeeKnown: true, FeeSource: feeSource, RouteReason: route,
		Platform: plat, SignalTS: signalAt.Format(time.RFC3339Nano), DecisionTS: nowTS,
		FillTS:                   executionFinalAt.Format(time.RFC3339Nano),
		ExecutionShadowAttemptID: shadowAttemptID, ExecutionBookSource: commitWireQuote.BookSource,
		ExecutionGeneration:    fundedPaperCorrectedExecutionGenerationV1,
		ExecutionTriggerUnixMS: signalAt.UnixMilli(), ExecutionAttemptUnixMS: attemptAt.UnixMilli(),
		ExecutionFinalUnixMS:       executionFinalAt.UnixMilli(),
		ExecutionInitialBookSource: initialWireQuote.BookSource,
		ExecutionInitialBid:        *initialExecution.BookBid, ExecutionInitialAsk: *initialExecution.BookAsk,
		ExecutionInitialBidDepth:  *initialExecution.BookBidDepth,
		ExecutionInitialAskDepth:  *initialExecution.BookAskDepth,
		ExecutionInitialQuoteAgeS: *initialExecution.BookQuoteAgeS,
		ExecutionFinalBid:         *decision.BookBid, ExecutionFinalAsk: *decision.BookAsk,
		ExecutionFinalBidDepth: *decision.BookBidDepth, ExecutionFinalAskDepth: *decision.BookAskDepth,
		ExecutionFinalQuoteAgeS: *decision.BookQuoteAgeS}
	s.fundedPaperBookTruthMu.RLock()
	if strings.EqualFold(plat, "kalshi") {
		if zeroFillWhy := s.fundedPaperLiveZeroFillReasonLocked(
			shadowAttemptID, sig.Ticker, side, ask, commitWireQuote); zeroFillWhy != "" {
			s.fundedPaperBookTruthMu.RUnlock()
			s.gfBookMu.Unlock()
			if isInverse {
				_, _ = s.store.RollbackInversePaperPlacement(context.WithoutCancel(ctx),
					inversePlacement.PlacementID, "authoritative LIVE zero-fill disproved Paper book")
			}
			zeroFill(zeroFillWhy)
			return
		}
	}
	if shadowAttemptID != "" {
		if liveTerminal.EventID == "" || liveTerminal.State == "" ||
			(liveTerminal.Kind != fundedPaperLiveTerminalNoSend &&
				liveTerminal.Kind != fundedPaperLiveTerminalFill) {
			s.fundedPaperBookTruthMu.RUnlock()
			s.gfBookMu.Unlock()
			if isInverse {
				_, _ = s.store.RollbackInversePaperPlacement(context.WithoutCancel(ctx),
					inversePlacement.PlacementID, "durable LIVE terminal missing or unsafe before Paper submit")
			}
			auditReason = "durable-live-terminal-missing-or-unsafe-before-paper-submit"
			return
		}
		pos.ExecutionTruthContract = fundedPaperLiveTruthContractV1
		pos.ExecutionLiveTerminalKind = liveTerminal.Kind
		pos.ExecutionLiveTerminal = liveTerminal.State
		pos.ExecutionLiveTerminalID = liveTerminal.EventID
	}
	s.fundedPaperBookTruthMu.RUnlock()
	if isInverse {
		pos.SignalTS = inversePlacement.SignalTS
		pos.PlacementID = inversePlacement.PlacementID
	}
	if relationOK, relationReason := s.stampFundedKFRelationReason(
		ctx, &pos, strings.TrimPrefix(key, "gf:"), "taker"); !relationOK {
		s.gfBookMu.Unlock()
		if isInverse {
			_, _ = s.store.RollbackInversePaperPlacement(context.WithoutCancel(ctx), inversePlacement.PlacementID, "relation receipt failed")
		}
		if strings.Contains(relationReason, "funded Paper admission refused:") {
			auditState = "REJECTED"
			auditReason = "paper-portfolio-relation-admission-refused"
		} else {
			auditReason = "relation-receipt-failed"
		}
		return
	}
	if ctx.Err() != nil {
		s.gfBookMu.Unlock()
		if isInverse {
			_, _ = s.store.RollbackInversePaperPlacement(context.WithoutCancel(ctx),
				inversePlacement.PlacementID, "original Paper signal deadline ended before lot append")
		}
		auditState = "PAPER-NOT-OBSERVED"
		auditReason = "paper-original-signal-deadline-before-lot-append"
		return
	}
	// Linearize the funded Paper fill against a direct LIVE zero-fill publication. The read lock
	// covers only this final memory check and slice append; all Paper storage/network work happened
	// before it, so the LIVE response path can never wait behind slow fake execution work.
	s.fundedPaperBookTruthMu.RLock()
	if strings.EqualFold(plat, "kalshi") {
		if zeroFillWhy := s.fundedPaperLiveZeroFillReasonLocked(
			shadowAttemptID, sig.Ticker, side, ask, commitWireQuote); zeroFillWhy != "" {
			s.fundedPaperBookTruthMu.RUnlock()
			s.gfBookMu.Unlock()
			if isInverse {
				_, _ = s.store.RollbackInversePaperPlacement(context.WithoutCancel(ctx),
					inversePlacement.PlacementID, "authoritative LIVE zero-fill disproved Paper book")
			}
			zeroFill(zeroFillWhy)
			return
		}
	}
	b.Open = append(b.Open, pos)
	s.fundedPaperBookTruthMu.RUnlock()
	s.gfBookDirty = true
	s.gfBookMu.Unlock()
	rollbackLateProjection := func(reason string) error {
		if isInverse {
			return s.rollbackInverseGenfollowPlacement(
				context.WithoutCancel(ctx), key, inversePlacement, reason)
		}
		s.gfBookMu.Lock()
		if h := s.gfLoadLocked().Subs[key]; h != nil {
			for i := len(h.Open) - 1; i >= 0; i-- {
				if h.Open[i].TS == nowTS && h.Open[i].Ticker == sig.Ticker &&
					h.Open[i].Side == side &&
					h.Open[i].ExecutionShadowAttemptID == shadowAttemptID {
					h.Open = append(h.Open[:i], h.Open[i+1:]...)
					s.gfBookDirty = true
					break
				}
			}
		}
		s.gfBookMu.Unlock()
		if err := s.genfollowFlush(); err != nil {
			s.gfBookPoisoned.Store(true)
			return err
		}
		return nil
	}
	if ctx.Err() != nil {
		if rollbackErr := rollbackLateProjection(
			"original Paper signal deadline ended before ledger commit"); rollbackErr != nil {
			auditState = "PAPER-ERROR"
			auditReason = "paper-deadline-rollback-failed:" + rollbackErr.Error()
		} else {
			auditState = "PAPER-NOT-OBSERVED"
			auditReason = "paper-original-signal-deadline-before-ledger-commit"
		}
		return
	}
	if flushErr := s.genfollowFlush(); flushErr != nil { // 454 rule: durable before accepted
		if isInverse {
			if rollbackErr := s.rollbackInverseGenfollowPlacement(ctx, key, inversePlacement,
				"initial JSON projection failed"); rollbackErr != nil {
				_ = s.store.Audit(context.WithoutCancel(ctx), "error", "genfollow",
					"Inverse placement and rollback persistence failed; executor poisoned", rollbackErr.Error())
			}
		} else {
			s.gfBookMu.Lock()
			if h := s.gfLoadLocked().Subs[key]; h != nil {
				for i := len(h.Open) - 1; i >= 0; i-- {
					if h.Open[i].TS == nowTS && h.Open[i].Ticker == sig.Ticker && h.Open[i].Side == side {
						h.Open = append(h.Open[:i], h.Open[i+1:]...)
						break
					}
				}
			}
			s.gfBookDirty = true
			s.gfBookMu.Unlock()
			if rollbackErr := s.genfollowFlush(); rollbackErr != nil {
				s.gfBookPoisoned.Store(true)
				_ = s.store.Audit(context.WithoutCancel(ctx), "error", "genfollow",
					"Follower ledger placement and rollback persistence failed; executor poisoned", rollbackErr.Error())
			}
		}
		auditReason = "paper-ledger-persistence-failed:" + flushErr.Error()
		return
	}
	if ctx.Err() != nil {
		if rollbackErr := rollbackLateProjection(
			"original Paper signal deadline ended during ledger commit"); rollbackErr != nil {
			auditState = "PAPER-ERROR"
			auditReason = "paper-deadline-post-commit-rollback-failed:" + rollbackErr.Error()
		} else {
			auditState = "PAPER-NOT-OBSERVED"
			auditReason = "paper-original-signal-deadline-during-ledger-commit"
		}
		return
	}
	if isInverse {
		ledgerMarked, journalErr := s.store.MarkInversePaperLedgerPersisted(ctx, inversePlacement.PlacementID)
		if journalErr != nil || !ledgerMarked {
			// SQLite may report an ambiguous commit/connection error after applying the transition.
			// Re-read durable truth before deleting a valid JSON projection.
			actual, found, readErr := s.store.InversePaperPlacement(context.WithoutCancel(ctx), inversePlacement.PlacementID)
			if readErr != nil || !found || (actual.State != "ledger_persisted" && actual.State != "committed") {
				_ = s.rollbackInverseGenfollowPlacement(ctx, key, inversePlacement,
					"could not mark durable JSON projection")
				auditReason = "inverse-placement-journal-ledger-transition-failed"
				if journalErr != nil {
					auditReason += ":" + journalErr.Error()
				}
				return
			}
			if actual.State == "committed" {
				journalErr, ledgerMarked = nil, true
			}
		}
		committed, commitErr := true, error(nil)
		actual, found, _ := s.store.InversePaperPlacement(context.WithoutCancel(ctx), inversePlacement.PlacementID)
		if !found || actual.State != "committed" {
			committed, commitErr = s.store.CommitInversePaperPlacement(ctx, inversePlacement.PlacementID)
		}
		if commitErr != nil || !committed {
			actual, found, readErr := s.store.InversePaperPlacement(context.WithoutCancel(ctx), inversePlacement.PlacementID)
			if readErr != nil || !found || actual.State != "committed" {
				_ = s.rollbackInverseGenfollowPlacement(ctx, key, inversePlacement,
					"exact signal fill and journal commit failed")
				auditReason = "inverse-placement-journal-commit-failed"
				if commitErr != nil {
					auditReason += ":" + commitErr.Error()
				}
				return
			}
		}
	}
	auditState, auditReason = "PAPER-FILLED", "accepted-at-current-book-and-recorded"
	_ = s.store.Audit(ctx, "info", "genfollow",
		fmt.Sprintf("Follower OPEN %s %s %s ×%.0f @ %.1f¢ ($%.2f, fee $%.2f, sl %.1f¢) %s", plat, sig.Ticker, side, contracts, ask*100, cost, fee, sl*100, route), "")
	// The shared detector already gave the selected LIVE branch first use of this signal before
	// funded Paper began. A later simulated Paper fill is evidence for that same attempt, never a
	// second source of LIVE authority. Re-enqueuing here used to duplicate selected candidates and
	// let unselected Paper-only systems create execution-shadow/audit traffic after they filled.
	return
}

// settleGenfollowBook — resolutions first (signal_log authority — the origin families logged the
// rows), then the frozen ratio stop against the cached venue mark. Every sub, both venues.
func (s *Server) settleGenfollowBook(ctx context.Context) {
	if s.gfBookPoisoned.Load() {
		return
	}
	if !s.genfollowOn() && !s.genfollowWindingDown() {
		return
	}
	s.gfJournalMu.Lock()
	defer s.gfJournalMu.Unlock()
	if err := s.reconcileInverseGenfollowPlacements(ctx); err != nil {
		_ = s.store.Audit(context.WithoutCancel(ctx), "error", "genfollow",
			"Inverse placement journal reconciliation failed; settlement skipped", err.Error())
		return
	}
	now := time.Now()
	var closedNotes []string
	var settledPlacementIDs []string
	type inverseCloseReceipt struct {
		Family, Platform, Ticker, Side, Reason string
		PnL, Payout, CLV                       float64
		CLVKnown                               bool
	}
	var inverseCloses []inverseCloseReceipt
	var relationOutcomes []fundedKFOutcome
	type settleProbe struct {
		resolved bool
		yesWon   float64
		markOK   bool
		sidePx   float64
		exitFee  float64
	}
	lotKey := func(sub string, p kfPos) string {
		return sub + "\x00" + p.Ticker + "\x00" + p.TS + "\x00" + p.Side
	}

	// Snapshot under the ledger mutex, then do every SQLite/mark/fee lookup without it. A placement
	// may run while probes are calculated, but it cannot replace an existing ticker; the apply phase
	// matches the original lot identity and leaves any newly-created lot untouched.
	snap := make(map[string][]kfPos)
	s.gfBookMu.Lock()
	st := s.gfLoadLocked()
	for k, h := range st.Subs {
		if len(h.Open) > 0 {
			snap[k] = append([]kfPos(nil), h.Open...)
		}
	}
	s.gfBookMu.Unlock()

	probes := make(map[string]settleProbe)
	for k, open := range snap {
		for _, p := range open {
			probe := settleProbe{}
			if yv, res := s.store.ResolvedYesForVenue(ctx, fiLotPlatform(p), p.Ticker); res {
				probe.resolved, probe.yesWon = true, yv
			} else if sidePx, ok := s.fiSideMark(p); ok {
				probe.markOK, probe.sidePx = true, sidePx
				if p.SL > 0 && sidePx <= p.SL {
					plat := p.Platform
					if plat == "" {
						plat = "kalshi"
					}
					probe.exitFee = s.blendedFeeAction(plat, p.Ticker, "", false, p.Contracts, sidePx, false)
				}
			}
			if probe.markOK {
				family := lotFamilyTag(p, "")
				if strings.HasPrefix(strings.ToLower(family), "invert:") {
					platform := p.Platform
					if platform == "" {
						platform = "kalshi"
					}
					_ = s.store.AppendSignalFamilyPostPath(ctx, platform, p.Ticker, p.Side, family, probe.sidePx)
				}
			}
			probes[lotKey(k, p)] = probe
		}
	}

	s.gfBookMu.Lock()
	s.fundedPaperBookTruthMu.RLock()
	st = s.gfLoadLocked()
	keys := make([]string, 0, len(st.Subs))
	for k := range st.Subs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		h := st.Subs[k]
		if len(h.Open) == 0 {
			continue
		}
		still := h.Open[:0:0]
		changed := false
		for _, p := range h.Open {
			// A new selected-attempt Paper lot may settle only under its explicit durable LIVE
			// terminal contract. Recheck the exact zero-fill tombstone at this same open->closed
			// boundary; if settlement arrived first while LIVE was still unknown, leave the lot
			// untouched so the later authoritative correction removes it without ever creating
			// Closed P&L or funded-relation outcomes.
			if s.fundedPaperLotCloseBlockReasonLocked(p) != "" {
				still = append(still, p)
				continue
			}
			probe, probed := probes[lotKey(k, p)]
			if !probed {
				still = append(still, p)
				continue
			}
			if probe.resolved {
				payout, pnl, won := fiSettlePnL(p.Side, probe.yesWon, p.Price, p.Contracts, p.Fee)
				family := lotFamilyTag(p, "")
				if strings.HasPrefix(strings.ToLower(family), "invert:") {
					platform := p.Platform
					if platform == "" {
						platform = "kalshi"
					}
					receipt := inverseCloseReceipt{Family: family, Platform: platform, Ticker: p.Ticker,
						Side: p.Side, Reason: "settled", PnL: pnl, Payout: payout}
					if len(p.Marks) > 0 {
						receipt.CLV, receipt.CLVKnown = p.Marks[len(p.Marks)-1].Px-p.Price, true
					}
					inverseCloses = append(inverseCloses, receipt)
				}
				kfMarksClose(&p, payout, "settle")
				h.Net += pnl
				if won {
					h.Wins++
				} else {
					h.Losses++
				}
				h.Closed = append(h.Closed, kfClosed{kfPos: p, Payout: payout,
					PnL: math.Round(pnl*100) / 100, Won: won, SettledTS: now.UTC().Format(time.RFC3339), Reason: "settled"})
				relationOutcomes = append(relationOutcomes, fundedKFOutcome{Position: p, PnL: pnl, Source: "genfollow-settlement"})
				if p.PlacementID != "" {
					settledPlacementIDs = append(settledPlacementIDs, p.PlacementID)
				}
				closedNotes = append(closedNotes, fmt.Sprintf("%s %s settled %+0.2f", k, p.Ticker, pnl))
				s.gfBookDirty, changed = true, true
				continue
			}
			if probe.markOK { // platform-aware mark was captured outside gfBookMu
				sidePx := probe.sidePx
				nMk := len(p.Marks)
				kfStampMark(&p, sidePx, "mark")
				if len(p.Marks) != nMk {
					s.gfBookDirty = true
				}
				if p.SL > 0 && sidePx <= p.SL {
					family := lotFamilyTag(p, "")
					kfMarksClose(&p, sidePx, "stop")
					pnl := p.Contracts*(sidePx-p.Price) - p.Fee - probe.exitFee
					if strings.HasPrefix(strings.ToLower(family), "invert:") {
						platform := p.Platform
						if platform == "" {
							platform = "kalshi"
						}
						inverseCloses = append(inverseCloses, inverseCloseReceipt{Family: family,
							Platform: platform, Ticker: p.Ticker, Side: p.Side, Reason: "stop-loss",
							PnL: pnl, Payout: sidePx, CLV: sidePx - p.Price, CLVKnown: true})
					}
					h.Net += pnl
					h.Losses++
					h.Closed = append(h.Closed, kfClosed{kfPos: p, Payout: sidePx,
						PnL: math.Round(pnl*100) / 100, Won: false, SettledTS: now.UTC().Format(time.RFC3339), Reason: "stop-loss"})
					relationOutcomes = append(relationOutcomes, fundedKFOutcome{Position: p, PnL: pnl, Source: "genfollow-stop-loss"})
					if p.PlacementID != "" {
						settledPlacementIDs = append(settledPlacementIDs, p.PlacementID)
					}
					closedNotes = append(closedNotes, fmt.Sprintf("%s %s stop-loss %+0.2f", k, p.Ticker, pnl))
					s.gfBookDirty, changed = true, true
					continue
				}
			}
			still = append(still, p)
		}
		h.Open = still
		if changed {
			if len(h.Closed) > 300 {
				h.Closed = h.Closed[len(h.Closed)-300:]
			}
			h.Equity = append(h.Equity, [2]float64{float64(now.Unix()), math.Round((h.Net-h.NetBase)*100) / 100})
			if len(h.Equity) > 400 {
				h.Equity = h.Equity[len(h.Equity)-400:]
			}
		}
	}
	s.fundedPaperBookTruthMu.RUnlock()
	s.gfBookMu.Unlock()
	for _, n := range closedNotes {
		_ = s.store.Audit(ctx, "info", "genfollow", "Follower CLOSE "+n, "")
	}
	for _, receipt := range inverseCloses {
		detail := map[string]any{"system": receipt.Family, "venue": receipt.Platform,
			"ticker": receipt.Ticker, "side": receipt.Side, "settlement_result": receipt.Reason,
			"payout": receipt.Payout, "realized_pnl": receipt.PnL, "clv": nil}
		if receipt.CLVKnown {
			detail["clv"] = receipt.CLV
		}
		encoded, _ := json.Marshal(detail)
		_ = s.store.Audit(ctx, "info", "inverse-system", "CLOSED "+receipt.Family+" "+receipt.Ticker, string(encoded))
	}
	if err := s.genfollowFlush(); err != nil {
		s.gfBookPoisoned.Store(true)
		_ = s.store.Audit(context.WithoutCancel(ctx), "error", "genfollow",
			"Follower settlement ledger failed to persist; executor poisoned", err.Error())
		return
	}
	s.settleFundedKFOutcomes(context.WithoutCancel(ctx), relationOutcomes)
	// The JSON close is durable before the journal becomes terminal. A crash between these steps is
	// repaired on boot by reconcileInverseGenfollowPlacements, which sees the committed closed lot.
	for _, id := range settledPlacementIDs {
		if ok, err := s.store.SettleInversePaperPlacement(context.WithoutCancel(ctx), id); err != nil || !ok {
			_ = s.store.Audit(context.WithoutCancel(ctx), "error", "genfollow",
				"Inverse JSON close persisted but journal settlement is pending reconciliation",
				fmt.Sprintf("placement=%s ok=%v err=%v", id, ok, err))
		}
	}
}

// gfBookOpenSide — cross-book hedge leg (platform-aware: -k subs guard kalshi, -p subs polyus).
func (s *Server) gfBookOpenSide(platform, ticker string) string {
	suffix := "-k"
	switch platform {
	case "kalshi":
	case "polyus":
		suffix = "-p"
	default:
		return ""
	}
	s.gfBookMu.Lock()
	defer s.gfBookMu.Unlock()
	for k, b := range s.gfLoadLocked().Subs {
		if !strings.HasSuffix(k, suffix) {
			continue
		}
		for _, p := range b.Open {
			if p.Ticker == ticker {
				return p.Side
			}
		}
	}
	return ""
}

// gfExposureByVenue — Σ open cost + lot count of the follower's subs on one venue book
// (books.go exposure arm). Caller must NOT hold gfBookMu.
func (s *Server) gfExposureByVenue(book string) (usd float64, lots int, ok bool) {
	suffix := "-k"
	if book == vbPolyus {
		suffix = "-p"
	}
	s.gfBookMu.Lock()
	defer s.gfBookMu.Unlock()
	st := s.gfLoadLocked()
	if s.gfBookPoisoned.Load() {
		return 0, 0, false
	}
	for k, b := range st.Subs {
		if strings.HasSuffix(k, suffix) {
			usd += kfLotsExposure(b.Open)
			lots += len(b.Open)
		}
	}
	return usd, lots, true
}

// gfNetByVenue — current-reset-epoch net of the follower's subs on one venue book.
func (s *Server) gfNetByVenue(book string) (net float64) {
	suffix := "-k"
	if book == vbPolyus {
		suffix = "-p"
	}
	s.gfBookMu.Lock()
	defer s.gfBookMu.Unlock()
	for k, b := range s.gfLoadLocked().Subs {
		if strings.HasSuffix(k, suffix) {
			net += b.Net - b.NetBase
		}
	}
	return net
}

// handleGenfollow (GET /api/genfollow) — the automatic roster (live from the weight table), every
// sub ledger, and the follow rules in plain words.
func (s *Server) handleGenfollow(w http.ResponseWriter, r *http.Request) {
	out := map[string]any{
		"enabled": s.genfollowOn(),
		"rules":   "PAPER registers each exact prospective family + venue + side + taker cell, never a pooled signal-time price and never a mechanical opposite. Historical assumed-fill signs and magnitudes stay visible as research but cannot add, remove, rank, or size routes. Every non-profit-evidence route receives equal one-share exploration inside its venue book; only route-exact authenticated fill-conditioned exchange-profit evidence may later change that route. Strategy-origin cells stay with their existing executor. Every attempted fill still uses two current complete-book touches and rechecks the exact side, visible depth, authoritative fee, freshness, horizon, chase, conflicts, and portfolio cash. ML remains a sensor/ranker. LIVE remains cash-closed without its separate authenticated-profit authority.",
		"allocation_basis": map[string]any{
			"score":                         "equal exploration for every registered route without authenticated profit evidence",
			"order_size_shares":             1,
			"historical_points_money_input": false,
			"cache_only_on_execution_path":  true,
			"minimum_funded_share_usd":      0,
			"live_behavior_changed":         false,
		},
	}
	verds := s.computeExperimentVerdicts(r.Context())
	out["paper_execution_coverage"] = gfPaperExecutionCoverage(verds)
	// live roster lines (cache-only weight table)
	s.swMu.Lock()
	t := s.swTable
	s.swMu.Unlock()
	s.researchDigestMu.Lock()
	allocationErr := s.gfAllocationErr
	s.researchDigestMu.Unlock()
	roster := []map[string]any{}
	if t != nil {
		snapshot := map[string]any{"frozen": t.AllocationFrozen}
		if t.AllocationFrozen {
			snapshot["as_of"] = t.AllocationAsOf.Format(time.RFC3339Nano)
			snapshot["utc_day"] = t.AllocationDay
			snapshot["built_at"] = t.AllocationBuiltAt.Format(time.RFC3339Nano)
			snapshot["rollover_pending"] = t.AllocationDay != gfAllocationDayStart(time.Now()).Format("2006-01-02")
		}
		if allocationErr != "" {
			snapshot["last_refresh_error"] = allocationErr
		}
		out["allocation_snapshot"] = snapshot
		for _, book := range []string{vbKalshi, vbPolyus} {
			for _, e := range t.Books[book] {
				if strings.HasPrefix(e.Family, "gf:") {
					roster = append(roster, map[string]any{"book": book, "family": e.Family, "plain": e.Plain,
						"cents_per_unit": sanF(e.Cents), "n": e.N, "weight": sanF(e.Weight),
						"share_usd": math.Round(e.Weight*s.bookSizingEquityUSD(r.Context(), book)*100) / 100,
						"explore":   e.Explore, "allocation_score": sanF(e.AllocationScore),
						"allocation_n": e.AllocationN, "allocation_ready": e.AllocationReady})
				}
			}
		}
	} else {
		out["allocation_snapshot"] = map[string]any{"frozen": false}
	}
	out["roster"] = roster
	subs := map[string]any{}
	s.gfBookMu.Lock()
	for k, h := range s.gfLoadLocked().Subs {
		netSince, wins, losses := kfCurrentEpochStats(h)
		wr := 0.0
		if n := wins + losses; n > 0 {
			wr = float64(wins) / float64(n)
		}
		openCost := 0.0
		lots := make([]map[string]any, 0, len(h.Open))
		for _, p := range h.Open {
			openCost += p.Contracts*p.Price + p.Fee
			lot := map[string]any{"ticker": p.Ticker, "title": p.Title, "side": p.Side,
				"price": p.Price, "contracts": p.Contracts, "route": p.RouteReason, "platform": p.Platform}
			if sidePx, ok := s.fiSideMark(p); ok {
				lot["cur_price"] = sidePx
				lot["unrealized"] = math.Round((p.Contracts*(sidePx-p.Price)-p.Fee)*100) / 100
			}
			lots = append(lots, lot)
		}
		subs[k] = map[string]any{
			"accounting": "current-reset-epoch",
			"net":        sanF(math.Round(netSince*100) / 100), "closed": wins + losses, "wins": wins,
			"win_rate": sanF(wr), "open": len(h.Open), "open_cost": sanF(math.Round(openCost*100) / 100),
			"lifetime": map[string]any{"net": sanF(math.Round(h.Net*100) / 100), "wins": h.Wins, "losses": h.Losses},
		}
		if len(lots) > 0 {
			subs[k].(map[string]any)["open_lots"] = lots
		}
	}
	s.gfBookMu.Unlock()
	out["subs"] = subs
	writeJSON(w, http.StatusOK, out)
}
