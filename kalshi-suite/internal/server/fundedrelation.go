package server

// Decision-time dependence plumbing for funded Paper entries. Relation is evaluated against the
// immutable canonical event/version frozen at the decision, never against a later friendly-name
// rematch. Unknown identity stays unknown; it can never inflate independent performance.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/paper"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

type fundedRelationCandidateLeg struct {
	Venue, Ticker, Side string
}

type fundedRelationDecision struct {
	s                 *Server
	ctx               context.Context
	decision          time.Time
	portfolio, system string
	route             string
	legs              []storage.FundedRelationLeg
	state             string
	matchedPositions  []string
	matchedEvents     []string
	executionIdentity bool
	identityIssues    []string
	persisted         bool
	receiptID         string
}

// Every funded Paper ledger persists its decision receipt through allowBeforePlacement. Serialize
// the final open-receipt check with that insert so two specialist/generic/ML workers cannot both
// admit sibling outcomes from one exact Kalshi event_ticker between read and write.
var fundedKalshiEventAdmissionMu sync.Mutex

func relationHash(parts ...string) string {
	h := sha256.New()
	for _, p := range parts {
		_, _ = h.Write([]byte(strconv.Itoa(len(p))))
		_, _ = h.Write([]byte{':'})
		_, _ = h.Write([]byte(p))
		_, _ = h.Write([]byte{'|'})
	}
	return hex.EncodeToString(h.Sum(nil))
}

func fundedPortfolioFor(platform, source string) string {
	platform, source = strings.ToLower(strings.TrimSpace(platform)), strings.ToLower(strings.TrimSpace(source))
	if source == "auto-ml" || source == "ml-book" || strings.HasPrefix(source, "ml-book-v2") ||
		strings.HasPrefix(source, "new-ml") || strings.Contains(source, "book-native-v2") {
		if platform == vbPolyus {
			return "ml-polyus"
		}
		return "ml-kalshi"
	}
	if platform == vbPolyus {
		return vbPolyus
	}
	return vbKalshi
}

func contractRelationKey(venue, ticker, side string) string {
	return strings.ToLower(strings.TrimSpace(venue)) + "|" + strings.TrimSpace(ticker) + "|" + strings.ToUpper(strings.TrimSpace(side))
}

func uniqueSortedStrings(in []string) []string {
	seen := map[string]struct{}{}
	for _, v := range in {
		if v = strings.TrimSpace(v); v != "" {
			seen[v] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for v := range seen {
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

func (s *Server) beginFundedRelation(ctx context.Context, portfolio, system, route string,
	candidates []fundedRelationCandidateLeg) *fundedRelationDecision {
	d := &fundedRelationDecision{s: s, ctx: ctx, decision: time.Now(), portfolio: strings.ToLower(strings.TrimSpace(portfolio)),
		system: strings.TrimSpace(system), route: strings.TrimSpace(route), state: "independent", executionIdentity: true}
	if len(candidates) == 0 {
		d.executionIdentity = false
		d.identityIssues = append(d.identityIssues, "missing-candidate-legs")
		d.state = "unknown"
	}
	open, openErr := s.store.OpenFundedRelationLegs(ctx)
	if openErr != nil {
		d.state = "unknown"
	}
	eventContracts := map[string]map[string]struct{}{}
	eventPositions := map[string][]string{}
	for _, current := range open {
		if current.CanonicalEventID == "" || current.EventVersion <= 0 {
			continue
		}
		event := fmt.Sprintf("%s@%d", current.CanonicalEventID, current.EventVersion)
		if eventContracts[event] == nil {
			eventContracts[event] = map[string]struct{}{}
		}
		eventContracts[event][contractRelationKey(current.Venue, current.Ticker, current.Side)] = struct{}{}
		eventPositions[event] = append(eventPositions[event], current.PositionFingerprint)
	}
	for _, candidate := range candidates {
		candidate.Venue = strings.ToLower(strings.TrimSpace(candidate.Venue))
		candidate.Ticker = strings.TrimSpace(candidate.Ticker)
		candidate.Side = strings.ToUpper(strings.TrimSpace(candidate.Side))
		leg := storage.FundedRelationLeg{Venue: candidate.Venue, Ticker: candidate.Ticker, Side: candidate.Side,
			RelationState: "independent"}
		var identityIssues []string
		if candidate.Venue == "" {
			identityIssues = append(identityIssues, "missing-venue")
		} else if candidate.Venue != vbKalshi && candidate.Venue != vbPolyus {
			identityIssues = append(identityIssues, "nonfunded-venue")
		} else if (d.portfolio == vbKalshi || d.portfolio == "ml-kalshi") && candidate.Venue != vbKalshi ||
			(d.portfolio == vbPolyus || d.portfolio == "ml-polyus") && candidate.Venue != vbPolyus {
			identityIssues = append(identityIssues, "portfolio-venue-mismatch")
		}
		if candidate.Ticker == "" {
			identityIssues = append(identityIssues, "missing-ticker")
		}
		if candidate.Side != "YES" && candidate.Side != "NO" {
			identityIssues = append(identityIssues, "nonbinary-side")
		}
		if len(identityIssues) > 0 {
			// This is still a real rejected candidate, but it is not a funded binary relation leg.
			// Preserve its raw labels in the unclassified rejection lane; never call it independent.
			leg.RelationState = "unknown"
			d.state = "unknown"
			d.executionIdentity = false
			d.identityIssues = append(d.identityIssues, identityIssues...)
			d.legs = append(d.legs, leg)
			continue
		}
		// Do not wait for the rotating background foundation sweep before classifying a funded
		// decision.  The venue catalog already carries the authoritative native event container;
		// materialize that exact venue+ticker identity on demand so known siblings never fall into
		// the unknown lane merely because the process has just restarted.
		identity, ok, err := s.ensureExactCatalogCanonicalInstrument(ctx, candidate.Venue, candidate.Ticker)
		if err != nil || !ok || identity.EventID == "" || identity.EventVersion <= 0 {
			leg.RelationState = "unknown"
			d.state = "unknown"
			d.legs = append(d.legs, leg)
			continue
		}
		leg.CanonicalEventID, leg.EventVersion = identity.EventID, identity.EventVersion
		event := fmt.Sprintf("%s@%d", identity.EventID, identity.EventVersion)
		if eventContracts[event] == nil {
			eventContracts[event] = map[string]struct{}{}
		}
		eventContracts[event][contractRelationKey(candidate.Venue, candidate.Ticker, candidate.Side)] = struct{}{}
		leg.MatchedPositionIDs = uniqueSortedStrings(eventPositions[event])
		d.legs = append(d.legs, leg)
		d.matchedPositions = append(d.matchedPositions, leg.MatchedPositionIDs...)
		d.matchedEvents = append(d.matchedEvents, event)
	}
	if d.state != "unknown" {
		for i := range d.legs {
			leg := &d.legs[i]
			event := fmt.Sprintf("%s@%d", leg.CanonicalEventID, leg.EventVersion)
			if len(eventContracts[event]) >= 2 {
				leg.RelationState = "dependent"
				d.state = "dependent"
			}
		}
	}
	d.matchedPositions = uniqueSortedStrings(d.matchedPositions)
	d.matchedEvents = uniqueSortedStrings(d.matchedEvents)
	return d
}

func (d *fundedRelationDecision) persist(allowed bool, reason string, quantity, price, fee float64) (string, error) {
	if d == nil || d.persisted {
		if d == nil {
			return "", fmt.Errorf("nil funded relation decision")
		}
		return d.receiptID, nil
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "unspecified-decision"
	}
	if !d.executionIdentity {
		if allowed {
			return "", fmt.Errorf("funded placement lacks executable relation identity: %s",
				strings.Join(uniqueSortedStrings(d.identityIssues), ","))
		}
		first := storage.FundedRelationLeg{}
		if len(d.legs) > 0 {
			first = d.legs[0]
		}
		id, inserted, err := d.s.store.InsertFundedRelationIdentityRejection(d.ctx,
			storage.FundedRelationIdentityRejection{Decision: d.decision, Venue: first.Venue,
				Ticker: first.Ticker, Side: first.Side, SystemID: d.system, RouteKind: d.route,
				Reason: reason, IdentityIssue: strings.Join(uniqueSortedStrings(d.identityIssues), ","),
				RawLegs: append([]storage.FundedRelationLeg(nil), d.legs...)})
		if err != nil {
			return "", err
		}
		d.persisted, d.receiptID = true, id
		if inserted {
			_ = d.s.store.Audit(d.ctx, "info", "relation", "unclassified funded candidate rejected",
				fmt.Sprintf("receipt=%s system=%s venue=%s ticker=%s side=%s reason=%s identity=%s",
					id[:12], d.system, first.Venue, first.Ticker, first.Side, reason,
					strings.Join(uniqueSortedStrings(d.identityIssues), ",")))
		}
		return id, nil
	}
	legKeys := make([]string, 0, len(d.legs))
	for _, l := range d.legs {
		legKeys = append(legKeys, contractRelationKey(l.Venue, l.Ticker, l.Side)+"|"+l.CanonicalEventID+"|"+strconv.Itoa(l.EventVersion))
	}
	sort.Strings(legKeys)
	// Rejected hot-loop candidates dedupe per minute/reason. Accepted entries retain an exact
	// decision timestamp so two real fills cannot collapse into one receipt.
	decisionKey := d.decision.UTC().Format(time.RFC3339Nano)
	if !allowed {
		decisionKey = d.decision.UTC().Truncate(time.Minute).Format(time.RFC3339)
	}
	fingerprint := relationHash("decision-v1", decisionKey, d.portfolio, d.system, d.route,
		strings.Join(legKeys, ";"), strconv.FormatBool(allowed), reason)
	positionFingerprint := relationHash("position-v1", d.portfolio, strings.Join(legKeys, ";"))
	first := storage.FundedRelationLeg{}
	if len(d.legs) > 0 {
		first = d.legs[0]
	}
	receipt := storage.FundedRelationReceipt{DecisionFingerprint: fingerprint,
		PositionFingerprint: positionFingerprint, Decision: d.decision, Portfolio: d.portfolio,
		CandidateVenue: first.Venue, CandidateTicker: first.Ticker, CandidateSide: first.Side,
		SystemID: d.system, RouteKind: d.route, CanonicalEventID: first.CanonicalEventID,
		EventVersion: first.EventVersion, RelationState: d.state, MatchedPositionIDs: d.matchedPositions,
		MatchedEventIDs: d.matchedEvents, Allowed: allowed, Reason: reason, Quantity: quantity,
		EntryPrice: price, EntryFee: fee, Legs: append([]storage.FundedRelationLeg(nil), d.legs...)}
	id, _, err := d.s.store.InsertFundedRelationReceipt(d.ctx, receipt)
	if err != nil {
		return "", err
	}
	d.persisted, d.receiptID = true, id
	decision := "rejected"
	if allowed {
		decision = "allowed"
	}
	_ = d.s.store.Audit(d.ctx, "info", "relation", fmt.Sprintf(
		"funded %s: %s relation · %s · %s", decision, d.state, d.portfolio, reason),
		fmt.Sprintf("receipt=%s system=%s legs=%d matched=%d", id[:12], d.system, len(d.legs), len(d.matchedPositions)))
	return id, nil
}

func (d *fundedRelationDecision) allowBeforePlacement(reason string, quantity, price, fee float64) (string, error) {
	if d == nil {
		return "", fmt.Errorf("nil funded relation decision")
	}
	if d.persisted {
		return d.receiptID, nil
	}
	fundedKalshiEventAdmissionMu.Lock()
	defer fundedKalshiEventAdmissionMu.Unlock()
	if conflict, err := d.kalshiEventHeaderConflictNow(); err != nil {
		_, persistErr := d.persist(false, "kalshi-event-header-identity-unavailable", 0, 0, 0)
		if persistErr != nil {
			return "", fmt.Errorf("Kalshi event-header admission unreadable: %v (rejection persistence: %w)", err, persistErr)
		}
		return "", fmt.Errorf("Kalshi event-header admission unreadable: %w", err)
	} else if conflict != "" {
		_, persistErr := d.persist(false, conflict, 0, 0, 0)
		if persistErr != nil {
			return "", fmt.Errorf("%s (rejection persistence: %w)", conflict, persistErr)
		}
		return "", fmt.Errorf("funded Paper admission refused: %s", conflict)
	}
	return d.persist(true, reason, quantity, price, fee)
}

// kalshiEventHeaderConflictNow resolves Kalshi's native EventTicker and the official Milestone
// relationship directly from authenticated/current venue metadata. Native sibling headers remain
// exclusive. Separate winner/spread/total/prop headers from one Sports occurrence are admitted only
// when the purchased payoff intersection is positively certified; unknown relationships fail closed.
func (d *fundedRelationDecision) kalshiEventHeaderConflictNow() (string, error) {
	if d == nil || d.s == nil || d.s.store == nil {
		return "", fmt.Errorf("funded relation store unavailable")
	}
	candidates := make([]kalshiEventExposure, 0, len(d.legs))
	for _, leg := range d.legs {
		if !strings.EqualFold(strings.TrimSpace(leg.Venue), vbKalshi) {
			continue
		}
		if d.s.allowSyntheticKalshiEventIdentity {
			if market, cached := d.s.kmkt(leg.Ticker); !cached || strings.TrimSpace(market.EventTicker) == "" {
				continue
			}
		}
		event, err := d.s.kalshiEventTickerStrict(d.ctx, leg.Ticker)
		if err != nil {
			return "", fmt.Errorf("candidate %s lacks authoritative Kalshi event_ticker: %w", leg.Ticker, err)
		}
		candidates = append(candidates, kalshiEventExposure{Ticker: leg.Ticker, EventTicker: event,
			Side: strings.ToUpper(strings.TrimSpace(leg.Side)), Kind: "candidate"})
	}
	if len(candidates) == 0 {
		return "", nil
	}
	open, err := d.s.store.OpenFundedRelationLegs(d.ctx)
	if err != nil {
		return "", err
	}
	held := make([]kalshiEventExposure, 0, len(open)+len(candidates))
	for _, leg := range open {
		if !strings.EqualFold(strings.TrimSpace(leg.Venue), vbKalshi) {
			continue
		}
		if d.s.allowSyntheticKalshiEventIdentity {
			if market, cached := d.s.kmkt(leg.Ticker); !cached || strings.TrimSpace(market.EventTicker) == "" {
				continue
			}
		}
		event, readErr := d.s.kalshiEventTickerStrict(d.ctx, leg.Ticker)
		if readErr != nil {
			return "", fmt.Errorf("open funded ticker %s lacks authoritative Kalshi event_ticker: %w", leg.Ticker, readErr)
		}
		held = append(held, kalshiEventExposure{Ticker: leg.Ticker, EventTicker: event,
			Side: strings.ToUpper(strings.TrimSpace(leg.Side)), Kind: "position"})
	}
	for _, candidate := range candidates {
		if why := kalshiEventExposureConflict(candidate.Ticker, candidate.EventTicker, held); why != "" {
			return "kalshi-event-header-already-funded", nil
		}
		if why := d.s.kalshiSemanticExposureConflict(d.ctx, candidate.Ticker, candidate.Side, held); why != "" {
			return why, nil
		}
		held = append(held, candidate)
	}
	return "", nil
}

func (d *fundedRelationDecision) reject(reason string) {
	if d == nil || d.persisted {
		return
	}
	if _, err := d.persist(false, reason, 0, 0, 0); err != nil {
		_ = d.s.store.Audit(d.ctx, "error", "relation", "funded rejection relation receipt failed", err.Error())
	}
}

func (s *Server) fundedSingleRelation(ctx context.Context, platform, ticker, side, source, route string) *fundedRelationDecision {
	d := s.beginFundedRelation(ctx, fundedPortfolioFor(platform, source), source, route,
		[]fundedRelationCandidateLeg{{Venue: platform, Ticker: ticker, Side: side}})
	platform = strings.ToLower(strings.TrimSpace(platform))
	if platform != vbKalshi && platform != vbPolyus {
		// Poly-int and any future research-only venue have no funded single-order portfolio here.
		// Their rejected candidates belong in the raw identity-rejection lane, never Kalshi by
		// default and never an independent/dependent economic cell.
		d.executionIdentity = false
		d.state = "unknown"
		d.identityIssues = append(d.identityIssues, "nonfunded-venue")
		for i := range d.legs {
			d.legs[i].RelationState = "unknown"
		}
	}
	return d
}

func (s *Server) stampFundedKFRelation(ctx context.Context, p *kfPos, system, route string) bool {
	ok, _ := s.stampFundedKFRelationReason(ctx, p, system, route)
	return ok
}

func (s *Server) stampFundedKFRelationReason(ctx context.Context, p *kfPos,
	system, route string) (bool, string) {
	if p == nil {
		return false, "nil-funded-paper-position"
	}
	platform := fiLotPlatform(*p)
	decision := s.fundedSingleRelation(ctx, platform, p.Ticker, p.Side, system, route)
	id, err := decision.allowBeforePlacement("passed-current-specialist-book-checks", p.Contracts, p.Price, p.Fee)
	if err != nil {
		_ = s.store.Audit(ctx, "error", "relation", "specialist funded relation receipt failed", err.Error())
		return false, err.Error()
	}
	p.RelationReceiptID = id
	return true, ""
}

func (s *Server) settleFundedKFRelation(ctx context.Context, p kfPos, pnl float64, source string) {
	if p.RelationReceiptID == "" {
		return // legacy lot; never backfill a current identity onto an old outcome
	}
	if _, _, err := s.store.ReconcileFundedSpecialistRelationOutcome(ctx, p.RelationReceiptID,
		fiLotPlatform(p), p.Ticker, p.Side, p.Contracts, p.Price, p.Fee, time.Now(), pnl, source); err != nil {
		_ = s.store.Audit(ctx, "error", "relation", "specialist funded relation outcome failed", err.Error())
	}
}

type fundedKFOutcome struct {
	Position kfPos
	PnL      float64
	Source   string
}

func (s *Server) settleFundedKFOutcomes(ctx context.Context, outcomes []fundedKFOutcome) {
	for _, outcome := range outcomes {
		s.settleFundedKFRelation(ctx, outcome.Position, outcome.PnL, outcome.Source)
	}
}

func comboRelationLegs(legs []paper.Leg) []fundedRelationCandidateLeg {
	out := make([]fundedRelationCandidateLeg, 0, len(legs))
	for _, l := range legs {
		out = append(out, fundedRelationCandidateLeg{Venue: l.Platform, Ticker: l.Ticker, Side: l.Side})
	}
	return out
}

func (s *Server) settleFundedContractRelation(ctx context.Context, portfolio, venue, ticker, side,
	systemID string, pnl float64, source string) {
	if _, err := s.store.RecordFundedRelationOutcomesForSystemContract(ctx, portfolio, venue, ticker, side,
		systemID, time.Now(), pnl, source); err != nil {
		_ = s.store.Audit(ctx, "error", "relation", "funded relation outcome failed", err.Error())
	}
}

func (s *Server) handleFundedRelationPerformance(w http.ResponseWriter, r *http.Request) {
	epoch := s.pnlEpoch
	if raw := strings.TrimSpace(r.URL.Query().Get("epoch")); raw != "" {
		if parsed, err := time.Parse(time.RFC3339Nano, raw); err == nil {
			epoch = parsed
		} else {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "epoch must be RFC3339"})
			return
		}
	}
	if epoch.IsZero() {
		epoch = time.Unix(0, 0).UTC()
	}
	portfolio, err := s.store.FundedRelationPerformance(r.Context(), epoch)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	proper, properErr := s.store.ProperScoreRelationPerformance(r.Context(), epoch)
	if properErr != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": properErr.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"epoch": epoch.UTC().Format(time.RFC3339Nano), "portfolios": portfolio,
		"proper_betting_research_net_not_portfolio_pnl": proper,
		"definition": "independent = one distinct contract per frozen event/version; dependent = two or more; unknown is separate and never counts independent",
	})
}

func (s *Server) handleFundedSystemPerformance(w http.ResponseWriter, r *http.Request) {
	now := time.Now().UTC()
	epoch := s.bootAt
	s.pnlMu.Lock()
	if !s.pnlEpoch.IsZero() {
		epoch = s.pnlEpoch
	}
	s.pnlMu.Unlock()
	rows, err := s.store.FundedSystemPerformanceRowsSince(r.Context(), epoch, now)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	positive := 0
	attributed := map[string]float64{}
	for _, row := range rows {
		if row.N > 0 && row.TotalRealizedProfitDollars > 0 {
			positive++
		}
		book := row.Portfolio
		if strings.HasPrefix(book, "ml-") {
			book = vbML
		}
		attributed[book] += row.TotalRealizedProfitDollars
	}
	type reconciliation struct {
		Portfolio                 string  `json:"portfolio"`
		PortfolioRealizedDollars  float64 `json:"portfolio_realized_dollars"`
		SystemAttributedDollars   float64 `json:"system_attributed_dollars"`
		LegacyUnattributedDollars float64 `json:"legacy_unattributed_dollars"`
		DollarCoveragePct         float64 `json:"absolute_dollar_coverage_pct"`
		AttributionComplete       bool    `json:"system_attribution_complete"`
	}
	reconciled := make([]reconciliation, 0, 4)
	for _, book := range []string{vbKalshi, vbPolyus, vbCombos, vbML} {
		metrics, ok := s.bookSessionClosedMetrics(r.Context(), book)
		if !ok {
			continue
		}
		total := metrics.NetUSD
		a := attributed[book]
		gap := total - a
		coverage := 100.0
		if abs := math.Abs(total); abs > 0.005 {
			coverage = math.Min(100, math.Abs(a)/abs*100)
		} else if math.Abs(gap) > 0.005 {
			coverage = 0
		}
		reconciled = append(reconciled, reconciliation{Portfolio: book,
			PortfolioRealizedDollars: total, SystemAttributedDollars: a,
			LegacyUnattributedDollars: gap, DollarCoveragePct: coverage,
			AttributionComplete: math.Abs(gap) <= 0.005})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"as_of": now.Format(time.RFC3339Nano), "epoch": epoch.UTC().Format(time.RFC3339Nano),
		"economics_scope": "funded-paper-current-reset-epoch",
		"rows":            rows, "positive_rows": positive,
		"portfolio_reconciliation": reconciled,
		"definition":               "sized simulated Paper outcome from immutable modeled-placement receipts and settlements; n is settled accepted simulations, unique_positions de-duplicates repeated entries into the same frozen position, and modeled Net/d includes quiet time since the first accepted simulation",
		"historical_attribution":   "portfolio_realized_dollars is the complete durable current-reset portfolio total; legacy_unattributed_dollars is epoch money without an immutable per-system receipt and is never guessed into a system",
		"unit_comparison":          "Systems Leaderboard rows labelled one-share-unit are bankroll-free one-share evidence, not this actual portfolio P&L",
	})
}

func (s *Server) handleFundedRelationReceipts(w http.ResponseWriter, r *http.Request) {
	// The summary endpoint intentionally exposes performance and the durable audit rows stay in
	// SQLite/export. This route returns open identities for operational debugging without scanning
	// the immutable history on every dashboard poll.
	open, err := s.store.OpenFundedRelationLegs(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	limit := 500
	if raw := r.URL.Query().Get("limit"); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 && n <= 2000 {
			limit = n
		}
	}
	if len(open) > limit {
		open = open[len(open)-limit:]
	}
	writeJSON(w, http.StatusOK, map[string]any{"open_legs": open, "count": len(open)})
}

// handleFundedRelationOutcome is the token-authenticated sidecar bridge. New ML owns its Paper
// JSON writer, so it reports a terminal lot only after the atomic book save succeeds. The storage
// insert is idempotent; a retry after a response loss cannot double-count P&L.
func (s *Server) handleFundedRelationOutcome(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ReceiptID string  `json:"receipt_id"`
		PnL       float64 `json:"pnl_dollars"`
		Source    string  `json:"source"`
		ClosedTS  int64   `json:"closed_ts"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || len(strings.TrimSpace(body.ReceiptID)) != 64 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "valid receipt_id and terminal economics required"})
		return
	}
	if strings.TrimSpace(body.Source) == "" {
		body.Source = "new-ml-paper-settlement"
	}
	closed := time.Now()
	if body.ClosedTS > 0 {
		closed = time.Unix(body.ClosedTS, 0)
	}
	inserted, err := s.store.RecordFundedRelationOutcome(r.Context(), body.ReceiptID, closed, body.PnL, body.Source)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "inserted": inserted})
}

// marshalFundedRelation is used only by tests/debug exporters; keeping it here guarantees the
// exact user-visible state names cannot drift from the stored contract.
func marshalFundedRelation(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}
