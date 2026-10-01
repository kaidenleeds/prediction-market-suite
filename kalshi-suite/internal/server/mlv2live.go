package server

// New-ML Paper -> guarded LIVE AUTO bridge.
//
// The Python sidecar owns the book-native-v2 Paper ledger. Its accepted taker fills land in
// ml_paper.json; accepted maker posts live in pendingMakers until a real simulated touch fills
// them. This file turns only those recent, current-cohort Paper decisions into short-lived opaque
// capabilities for liveautomirror.go. A capability is not money authority: dispatch still needs
// ARM + LIVE AUTO, a clear kill switch, a fresh matching model receipt, positive fee-known Paper
// results on the same venue/route, current lifecycle/book/depth/fee proof, account/cluster/risk
// checks, and the venue handler's final wire-price check.

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const newMLV2LiveCashRetiredReason = "new-ml-v2-positive-settled-authenticated-live-profit-cohort-unavailable"

func newMLV2LiveCashAuthorityReady() bool { return false }

const (
	newMLV2LiveSourcePrefix = "mlv2-live:"
	newMLV2ModelLineage     = "book-native-v2|book-features:1|label:kalshi_start_clock_v2|calibration:v1"
)

// A point-positive result (especially one win) is not a LIVE proof.  The model receipt must first
// carry the separately sealed untouched-replication authority, then this exact
// model/venue/side/route must own enough independent settled markets for a one-sided lower bound.
const newMLV2MinProofMarkets = 20

type newMLV2LiveCapability struct {
	server       *Server
	created      time.Time
	platform     string
	ticker       string
	side         string
	route        string
	modelVersion string
	modelLineage string
	paperUnits   float64
	// reservedUSD is a conservative worst-case principal reservation. Positive current edge
	// implies all-in risk below $1/contract, so reserving one dollar per proved Paper unit prevents
	// simultaneous capabilities from spending the same destination sleeve.
	reservedUSD float64
	handlerUsed bool
}

var newMLV2LiveCapabilities = struct {
	sync.Mutex
	rows map[string]*newMLV2LiveCapability
}{rows: map[string]*newMLV2LiveCapability{}}

type newMLV2PredictionReceipt struct {
	GeneratedAt      int64  `json:"generated_at"`
	FeatureSchema    string `json:"feature_schema"`
	ModelCohort      string `json:"model_cohort"`
	ModelVersion     string `json:"model_version"`
	ModelLineage     string `json:"model_lineage"`
	ExecutionEnabled bool   `json:"execution_enabled"`
	PaperAuthority   bool   `json:"paper_authority"`
	LiveAuthority    bool   `json:"live_authority"`
	LiveValidation   struct {
		State            string `json:"state"`
		Authority        bool   `json:"authority"`
		ModelVersion     string `json:"model_version"`
		ReplicationID    string `json:"replication_id"`
		FrozenBeforeTest bool   `json:"frozen_before_test"`
		Untouched        bool   `json:"untouched"`
	} `json:"live_validation"`
	Predictions []struct {
		Platform  string   `json:"platform"`
		Ticker    string   `json:"ticker"`
		Side      string   `json:"side"`
		PWin      float64  `json:"p_win"`
		LivePrice float64  `json:"live_price"`
		EV        float64  `json:"ev_per_contract"`
		EVNet     *float64 `json:"ev_net"`
		EVNetLive *float64 `json:"ev_net_live"`
	} `json:"predictions"`
}

type newMLV2PaperLot struct {
	Platform     string   `json:"platform"`
	Ticker       string   `json:"ticker"`
	Title        string   `json:"title"`
	Side         string   `json:"side"`
	Model        string   `json:"model_cohort"`
	ModelVersion string   `json:"model_version"`
	ModelLineage string   `json:"model_lineage"`
	EpochID      string   `json:"epoch_id"`
	FillKind     string   `json:"fill_kind"`
	PxSrc        string   `json:"px_src"`
	Price        float64  `json:"price"`
	PWin         float64  `json:"p_win"`
	EVNet        *float64 `json:"ev_net"`
	Contracts    float64  `json:"contracts"`
	PnL          float64  `json:"pnl"`
	Opened       int64    `json:"opened"`
	Decision     int64    `json:"decision_ts"`
	ResolveHours float64  `json:"entry_resolve_hours"`
	IsCrypto     bool     `json:"entry_is_crypto"`
	FeeKnown     bool     `json:"fee_known"`
	Reset        flexBool `json:"reset_close"`
}

func newMLV2ReceiptHasSealedLiveAuthority(r newMLV2PredictionReceipt) bool {
	v := r.LiveValidation
	return r.LiveAuthority && v.Authority && v.FrozenBeforeTest && v.Untouched &&
		strings.EqualFold(strings.TrimSpace(v.State), "SEALED_UNTOUCHED_PASS") &&
		strings.TrimSpace(v.ReplicationID) != "" && strings.TrimSpace(r.ModelVersion) != "" &&
		r.ModelLineage == newMLV2ModelLineage &&
		strings.TrimSpace(v.ModelVersion) == strings.TrimSpace(r.ModelVersion)
}

type newMLV2PaperView struct {
	Open               []newMLV2PaperLot `json:"open"`
	Closed             []newMLV2PaperLot `json:"closed"`
	EpochID            string            `json:"epoch_id"`
	ResetAt            string            `json:"reset_at"`
	CurrentModelCohort string            `json:"current_model_cohort"`
}

func isNewMLV2LiveSource(source string) bool {
	return strings.HasPrefix(strings.ToLower(strings.TrimSpace(source)), newMLV2LiveSourcePrefix)
}

func newMLV2Route(route string) string {
	route = strings.ToLower(strings.TrimSpace(route))
	if route == "maker" || route == "taker" {
		return route
	}
	return ""
}

func newMLV2DecisionTime(lot newMLV2PaperLot) time.Time {
	ts := lot.Decision
	if ts <= 0 {
		ts = lot.Opened
	}
	if ts <= 0 {
		return time.Time{}
	}
	return time.Unix(ts, 0)
}

// newMLV2Forecast proves that the decision still exists in a fresh, current-cohort sidecar
// receipt. LIVE authority is deliberately two-keyed: the current model receipt must carry a
// separately frozen untouched PASS, and the exact model/venue/side/route Paper cohort must later
// clear its own fee-net confidence bound. Global ARM + LIVE AUTO remain the operator handshake.
func (s *Server) newMLV2Forecast(platform, ticker, side string) (pWin, liveEdge float64, modelVersion, why string) {
	path := filepath.Join(s.cfg().DataDir, "ml_predictions.json")
	fi, err := os.Stat(path)
	if err != nil || time.Since(fi.ModTime()) > 3*time.Minute || fi.ModTime().After(time.Now().Add(5*time.Second)) {
		return 0, 0, "", "new-ml-signal-stream-stale"
	}
	var receipt newMLV2PredictionReceipt
	if !s.readJSONLoose(path, &receipt) {
		return 0, 0, "", "new-ml-signal-receipt-unreadable"
	}
	now := time.Now().Unix()
	if receipt.GeneratedAt <= 0 || now-receipt.GeneratedAt > int64((3*time.Minute)/time.Second) || receipt.GeneratedAt > now+5 {
		return 0, 0, "", "new-ml-signal-receipt-stale"
	}
	if receipt.FeatureSchema != currentMLCohort || receipt.ModelCohort != currentMLCohort ||
		strings.TrimSpace(receipt.ModelVersion) == "" || receipt.ModelLineage != newMLV2ModelLineage ||
		!receipt.ExecutionEnabled || !receipt.PaperAuthority {
		return 0, 0, "", "current-book-native-v2-paper-authority-required"
	}
	if !newMLV2ReceiptHasSealedLiveAuthority(receipt) {
		return 0, 0, "", "current-new-ml-sealed-untouched-live-authority-required"
	}
	platform = strings.ToLower(strings.TrimSpace(platform))
	side = strings.ToUpper(strings.TrimSpace(side))
	for _, p := range receipt.Predictions {
		if strings.ToLower(strings.TrimSpace(p.Platform)) != platform ||
			!strings.EqualFold(strings.TrimSpace(p.Ticker), strings.TrimSpace(ticker)) ||
			strings.ToUpper(strings.TrimSpace(p.Side)) != side || p.PWin <= 0 || p.PWin >= 1 ||
			p.LivePrice <= 0 || p.LivePrice >= 1 || p.EVNetLive == nil || *p.EVNetLive <= 0 ||
			math.IsNaN(*p.EVNetLive) || math.IsInf(*p.EVNetLive, 0) {
			continue
		}
		if *p.EVNetLive > liveEdge {
			pWin, liveEdge = p.PWin, *p.EVNetLive
		}
	}
	if pWin <= 0 {
		return 0, 0, "", "fresh-current-new-ml-prediction-missing"
	}
	return pWin, liveEdge, receipt.ModelVersion, ""
}

func (s *Server) newMLV2PaperView() (newMLV2PaperView, bool) {
	var raw map[string]any
	if !s.readJSONLoose(filepath.Join(s.cfg().DataDir, "ml_paper.json"), &raw) {
		return newMLV2PaperView{}, false
	}
	view := currentMLPaperView(raw)
	b, err := json.Marshal(view)
	if err != nil {
		return newMLV2PaperView{}, false
	}
	var out newMLV2PaperView
	if json.Unmarshal(b, &out) != nil {
		return newMLV2PaperView{}, false
	}
	return out, true
}

// newMLV2PositivePaperRoute applies the economic proof to one exact current-model cohort. Repeated
// entries in one market collapse to one independent market result before the bound is calculated.
// No result may borrow evidence across model generations, venue, side, or maker/taker route.
func (s *Server) newMLV2PositivePaperRoute(platform, side, route, modelVersion string) (meanPC, lowerPC float64, markets int, why string) {
	view, ok := s.newMLV2PaperView()
	if !ok {
		return 0, 0, 0, "new-ml-paper-ledger-unavailable"
	}
	platform = strings.ToLower(strings.TrimSpace(platform))
	side = strings.ToUpper(strings.TrimSpace(side))
	route = newMLV2Route(route)
	modelVersion = strings.TrimSpace(modelVersion)
	type marketNet struct{ contracts, net float64 }
	byMarket := map[string]marketNet{}
	for _, lot := range view.Closed {
		if bool(lot.Reset) || lot.Model != currentMLCohort || strings.TrimSpace(lot.ModelVersion) != modelVersion ||
			strings.ToLower(strings.TrimSpace(lot.Platform)) != platform ||
			strings.ToUpper(strings.TrimSpace(lot.Side)) != side || newMLV2Route(lot.FillKind) != route ||
			strings.TrimSpace(lot.Ticker) == "" || !lot.FeeKnown || lot.Contracts <= 0 ||
			math.IsNaN(lot.PnL) || math.IsInf(lot.PnL, 0) {
			continue
		}
		key := strings.ToLower(strings.TrimSpace(lot.Ticker))
		a := byMarket[key]
		a.contracts += lot.Contracts
		a.net += lot.PnL
		byMarket[key] = a
	}
	if len(byMarket) == 0 {
		return 0, 0, 0, "no-completed-fee-known-exact-new-ml-paper-cohort"
	}
	sum, sum2 := 0.0, 0.0
	for _, a := range byMarket {
		if a.contracts <= 0 {
			continue
		}
		x := a.net / a.contracts
		if math.IsNaN(x) || math.IsInf(x, 0) {
			continue
		}
		sum += x
		sum2 += x * x
		markets++
	}
	if markets < newMLV2MinProofMarkets {
		return 0, 0, markets, "insufficient-independent-markets-for-new-ml-live-proof"
	}
	meanPC = sum / float64(markets)
	lowerPC = evLowerBound(sum, sum2, markets)
	if meanPC <= 0 || lowerPC <= 0 || math.IsNaN(meanPC) || math.IsInf(meanPC, 0) ||
		math.IsNaN(lowerPC) || math.IsInf(lowerPC, 0) {
		return meanPC, lowerPC, markets, "new-ml-exact-paper-cohort-lower-bound-not-positive-after-fees"
	}
	return meanPC, lowerPC, markets, ""
}

// newMLV2PositivePaperLineageRoute fixes the online-refit starvation in the original exact-hash
// proof above. A fitted generation remains the provenance of the *current* signal, but settlement
// evidence pools only generations from one explicit feature/label/calibration lineage and one
// reset epoch. This excludes pre-book, legacy signal-price, pre-reset, unknown-fee, and structurally
// incompatible rows without demanding that a fast refit wait for every old market to settle.
func (s *Server) newMLV2PositivePaperLineageRoute(platform, side, route, lineage string) (meanPC, lowerPC float64, markets int, why string) {
	view, ok := s.newMLV2PaperView()
	if !ok {
		return 0, 0, 0, "new-ml-paper-ledger-unavailable"
	}
	platform = strings.ToLower(strings.TrimSpace(platform))
	side = strings.ToUpper(strings.TrimSpace(side))
	route = newMLV2Route(route)
	lineage = strings.TrimSpace(lineage)
	resetAt, resetErr := time.Parse(time.RFC3339Nano, strings.TrimSpace(view.ResetAt))
	if route == "" || lineage != newMLV2ModelLineage || strings.TrimSpace(view.EpochID) == "" ||
		view.CurrentModelCohort != currentMLCohort || resetErr != nil {
		return 0, 0, 0, "new-ml-current-epoch-lineage-unavailable"
	}
	type marketNet struct{ contracts, net float64 }
	byMarket := map[string]marketNet{}
	for _, lot := range view.Closed {
		decisionAt := newMLV2DecisionTime(lot)
		// Migration allowance is deliberately narrow: R144 lots written before the lineage field
		// may join only when their exact generation is book-v2, they carry this current epoch id,
		// and their decision occurred after the epoch reset. Missing model_version stays excluded.
		lineageOK := lot.ModelLineage == lineage || (strings.TrimSpace(lot.ModelLineage) == "" &&
			strings.HasPrefix(strings.TrimSpace(lot.ModelVersion), "book-v2-"))
		if bool(lot.Reset) || lot.Model != currentMLCohort || !lineageOK ||
			strings.TrimSpace(lot.ModelVersion) == "" || strings.TrimSpace(lot.EpochID) != view.EpochID ||
			lot.ResolveHours <= 0 || (!lot.IsCrypto && lot.ResolveHours > 4) || (lot.IsCrypto && lot.ResolveHours > 2) ||
			decisionAt.IsZero() || decisionAt.Before(resetAt) ||
			strings.ToLower(strings.TrimSpace(lot.Platform)) != platform ||
			strings.ToUpper(strings.TrimSpace(lot.Side)) != side || newMLV2Route(lot.FillKind) != route ||
			strings.TrimSpace(lot.Ticker) == "" || !lot.FeeKnown || lot.Contracts <= 0 ||
			math.IsNaN(lot.PnL) || math.IsInf(lot.PnL, 0) {
			continue
		}
		key := strings.ToLower(strings.TrimSpace(lot.Ticker))
		a := byMarket[key]
		a.contracts += lot.Contracts
		a.net += lot.PnL
		byMarket[key] = a
	}
	if len(byMarket) == 0 {
		return 0, 0, 0, "no-completed-fee-known-current-lineage-new-ml-paper-cohort"
	}
	sum, sum2 := 0.0, 0.0
	for _, a := range byMarket {
		if a.contracts <= 0 {
			continue
		}
		x := a.net / a.contracts
		if math.IsNaN(x) || math.IsInf(x, 0) {
			continue
		}
		sum += x
		sum2 += x * x
		markets++
	}
	if markets < newMLV2MinProofMarkets {
		return 0, 0, markets, "insufficient-independent-markets-for-new-ml-live-proof"
	}
	meanPC = sum / float64(markets)
	lowerPC = evLowerBound(sum, sum2, markets)
	if meanPC <= 0 || lowerPC <= 0 || math.IsNaN(meanPC) || math.IsInf(meanPC, 0) ||
		math.IsNaN(lowerPC) || math.IsInf(lowerPC, 0) {
		return meanPC, lowerPC, markets, "new-ml-current-lineage-route-lower-bound-not-positive-after-fees"
	}
	return meanPC, lowerPC, markets, ""
}

// newMLV2SizingBankrollFor is the Paper-capacity authority for a New-ML LIVE capability. Each
// destination venue owns one sleeve of the ML Paper portfolio; its available balance after open
// lots, resting fees, and other outstanding capabilities decides whether the route has capacity.
// It is deliberately not the LIVE dollar bankroll: newMLV2LiveSizingBankrollFor separately reads
// authenticated destination NAV, while paperUnits remains the final proof-capacity ceiling.
func (s *Server) newMLV2SizingBankrollFor(platform, ownSource string) (available, reference float64, source string) {
	pf, ok := s.currentMLPaperBrief()
	if !ok {
		return 0, 0, "new-ml-current-paper-ledger-unavailable"
	}
	platform = strings.ToLower(strings.TrimSpace(platform))
	sleeve, ok := pf.VenueSleeves[platform]
	if !ok {
		return 0, 0, "new-ml-destination-venue-sleeve-missing"
	}
	vals := []float64{sleeve.Grant, sleeve.Net, sleeve.Equity, sleeve.SizingBalance, sleeve.Deployed, sleeve.Reserved, sleeve.Available}
	for _, v := range vals {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return 0, 0, "new-ml-destination-venue-sleeve-invalid"
		}
	}
	if sleeve.Grant <= 0 || sleeve.Equity <= 0 || sleeve.SizingBalance <= 0 || sleeve.Deployed < 0 || sleeve.Reserved < 0 || sleeve.Available < 0 ||
		math.Abs(sleeve.SizingBalance-sleeve.Equity) > 0.011 || sleeve.Available > sleeve.SizingBalance+0.011 {
		return 0, 0, "new-ml-destination-venue-sleeve-invalid"
	}
	available = sleeve.Available
	now := time.Now()
	newMLV2LiveCapabilities.Lock()
	for key, cap := range newMLV2LiveCapabilities.rows {
		if cap == nil || now.Sub(cap.created) > liveMirrorTTL {
			delete(newMLV2LiveCapabilities.rows, key)
			continue
		}
		if cap.server == s && cap.platform == platform && key != ownSource {
			available -= cap.reservedUSD
		}
	}
	newMLV2LiveCapabilities.Unlock()
	if available <= 0 {
		return 0, sleeve.Grant, "current-new-ml-" + platform + "-sleeve-no-unreserved-available"
	}
	return available, sleeve.Grant, "current-new-ml-" + platform + "-sleeve-available"
}

func (s *Server) newMLV2SizingBankroll(platform string) (available, reference float64, source string) {
	return s.newMLV2SizingBankrollFor(platform, "")
}

// newMLV2LiveSizingBankrollFor keeps two separate truths separate:
//   - the Paper sleeve proves that this exact New-ML route has current capacity; and
//   - the authenticated destination venue NAV supplies the dollars that LIVE may size.
//
// The Paper sleeve must never become a hidden fixed LIVE bankroll. Conversely, a larger venue
// balance cannot manufacture model capacity: dispatch still caps contracts by the originating
// Paper capability's paperUnits after this helper returns.
func (s *Server) newMLV2LiveSizingBankrollFor(ctx context.Context, platform, ownSource string) (bank float64, source string) {
	available, _, paperSource := s.newMLV2SizingBankrollFor(platform, ownSource)
	if available <= 0 {
		return 0, paperSource
	}
	bank, liveSource := s.liveSizingBankroll(ctx, platform)
	if bank <= 0 {
		return 0, liveSource + ";paper-capacity=" + paperSource
	}
	return bank, liveSource + ";paper-capacity=" + paperSource
}

func registerNewMLV2LiveCapability(s *Server, c liveMirrorCandidate, route, modelVersion string, paperUnits float64) (liveMirrorCandidate, bool) {
	route = newMLV2Route(route)
	platform := strings.ToLower(strings.TrimSpace(c.Platform))
	side := strings.ToUpper(strings.TrimSpace(c.Side))
	modelVersion = strings.TrimSpace(modelVersion)
	if s == nil || !s.liveNewMLVenueEnabled(platform) || route == "" || (platform != "kalshi" && platform != "polyus") || c.Ticker == "" ||
		(side != "YES" && side != "NO") || modelVersion == "" || c.Price <= 0 || c.Price >= 1 ||
		paperUnits < 1 || math.IsNaN(paperUnits) || math.IsInf(paperUnits, 0) {
		return c, false
	}
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		return c, false
	}
	source := newMLV2LiveSourcePrefix + hex.EncodeToString(token[:])
	now := time.Now()
	pf, sleeveOK := s.currentMLPaperBrief()
	sleeve, sleeveOK := pf.VenueSleeves[platform]
	// Positive fee-net probability edge implies principal+fee < $1 per unit. Reserving a full
	// dollar per originating Paper unit is therefore conservative without guessing a stale price.
	reservation := paperUnits
	if !sleeveOK || sleeve.Available <= 0 {
		return c, false
	}
	newMLV2LiveCapabilities.Lock()
	reserved := 0.0
	for key, cap := range newMLV2LiveCapabilities.rows {
		if cap == nil || now.Sub(cap.created) > liveMirrorTTL {
			delete(newMLV2LiveCapabilities.rows, key)
			continue
		}
		if cap.server == s && cap.platform == platform {
			reserved += cap.reservedUSD
		}
	}
	if reservation > sleeve.Available-reserved+1e-9 {
		newMLV2LiveCapabilities.Unlock()
		return c, false
	}
	newMLV2LiveCapabilities.rows[source] = &newMLV2LiveCapability{
		server: s, created: now, platform: platform,
		ticker: strings.TrimSpace(c.Ticker), side: side,
		route: route, modelVersion: modelVersion, modelLineage: newMLV2ModelLineage,
		paperUnits: paperUnits, reservedUSD: reservation,
	}
	newMLV2LiveCapabilities.Unlock()
	c.Source = source
	c.Family = "new-ml-v2"
	return c, true
}

func (s *Server) newMLV2Capability(c liveMirrorCandidate, route string) (newMLV2LiveCapability, bool) {
	if !isNewMLV2LiveSource(c.Source) {
		return newMLV2LiveCapability{}, false
	}
	newMLV2LiveCapabilities.Lock()
	defer newMLV2LiveCapabilities.Unlock()
	cap := newMLV2LiveCapabilities.rows[c.Source]
	if cap == nil || cap.server != s || time.Since(cap.created) > liveMirrorTTL ||
		cap.platform != strings.ToLower(strings.TrimSpace(c.Platform)) ||
		!strings.EqualFold(cap.ticker, strings.TrimSpace(c.Ticker)) ||
		cap.side != strings.ToUpper(strings.TrimSpace(c.Side)) ||
		cap.modelLineage != newMLV2ModelLineage ||
		(route != "" && cap.route != newMLV2Route(route)) {
		return newMLV2LiveCapability{}, false
	}
	return *cap, true
}

func releaseNewMLV2Capability(s *Server, source string) {
	newMLV2LiveCapabilities.Lock()
	if cap := newMLV2LiveCapabilities.rows[source]; cap != nil && cap.server == s && !cap.handlerUsed {
		delete(newMLV2LiveCapabilities.rows, source)
	}
	newMLV2LiveCapabilities.Unlock()
}

func (s *Server) authorizeNewMLV2LiveHandler(source, platform, ticker, side, route string) (bool, string) {
	// Current New-ML economics come from simulated Paper lots. Keep every model/Paper collector,
	// but do not let that lineage cross the final venue boundary until a separate preregistered,
	// settled, authenticated LIVE cohort has a positive fee-net profit-rate bound.
	if !newMLV2LiveCashAuthorityReady() {
		return false, newMLV2LiveCashRetiredReason
	}
	if !s.liveNewMLVenueEnabled(platform) {
		return false, "current-New-ML LIVE disabled for destination venue"
	}
	c := liveMirrorCandidate{Platform: platform, Ticker: ticker, Side: side, Source: source}
	cap, ok := s.newMLV2Capability(c, route)
	if !ok {
		return false, "valid internal current-New-ML Paper capability required"
	}
	_, _, modelVersion, why := s.newMLV2Forecast(platform, ticker, side)
	if why != "" || modelVersion != cap.modelVersion {
		if why == "" {
			why = "new-ml-model-generation-changed"
		}
		return false, why
	}
	if _, _, _, why := s.newMLV2PositivePaperLineageRoute(platform, side, route, cap.modelLineage); why != "" {
		return false, why
	}
	newMLV2LiveCapabilities.Lock()
	defer newMLV2LiveCapabilities.Unlock()
	stored := newMLV2LiveCapabilities.rows[source]
	if stored == nil || stored.server != s || stored.handlerUsed || stored.modelVersion != cap.modelVersion {
		return false, "New-ML capability already used or expired"
	}
	stored.handlerUsed = true
	return true, ""
}

func (s *Server) enqueueNewMLV2PaperDecision(lot newMLV2PaperLot, at time.Time, route string) bool {
	if lot.Model != currentMLCohort || (lot.Platform != "kalshi" && lot.Platform != "polyus") ||
		lot.Ticker == "" || (strings.ToUpper(lot.Side) != "YES" && strings.ToUpper(lot.Side) != "NO") ||
		strings.TrimSpace(lot.ModelVersion) == "" || lot.Price <= 0 || lot.Price >= 1 || lot.PWin <= 0 || lot.PWin >= 1 || lot.EVNet == nil || *lot.EVNet <= 0 || lot.Contracts < 1 ||
		at.IsZero() || time.Since(at) > liveMirrorTTL || at.After(time.Now().Add(5*time.Second)) {
		return false
	}
	_, _, modelVersion, why := s.newMLV2Forecast(lot.Platform, lot.Ticker, lot.Side)
	if why != "" || modelVersion != strings.TrimSpace(lot.ModelVersion) {
		return false
	}
	c := liveMirrorCandidate{Platform: lot.Platform, Ticker: lot.Ticker, Title: lot.Title,
		Side: lot.Side, Price: lot.Price, At: at, Family: "new-ml-v2",
		LiveIntentGeneration: s.liveSignalIntentGeneration.Load(), LiveIntentGenerationBound: true}
	var ok bool
	c, ok = registerNewMLV2LiveCapability(s, c, route, modelVersion, lot.Contracts)
	if !ok {
		return false
	}
	if !s.enqueueLiveMirrorCandidate(c) {
		releaseNewMLV2Capability(s, c.Source)
		return false
	}
	return true
}

// enqueueNewMLV2LiveCandidates scans only the two honest New-ML Paper acceptance surfaces. It is
// called by the armed live-mirror consumer, so decisions made while LIVE is off are never replayed
// later. Old ML files, Poly-int, stale decisions, and predictions not present in the fresh current
// receipt cannot mint a capability.
func (s *Server) enqueueNewMLV2LiveCandidates(ctx context.Context) int {
	_ = ctx
	if !s.liveMirrorEnabled() {
		return 0
	}
	queued := 0
	if view, ok := s.newMLV2PaperView(); ok {
		for _, lot := range view.Open {
			route := newMLV2Route(lot.FillKind)
			if route != "" && s.enqueueNewMLV2PaperDecision(lot, newMLV2DecisionTime(lot), route) {
				queued++
			}
		}
	}
	// Maker posts are accepted Paper orders before they become open fills. Copy under pendMu, then
	// decode and validate without holding the pending-maker lock across file reads.
	s.pendMu.Lock()
	pending := make([]struct {
		at    time.Time
		price float64
		raw   []byte
	}, 0)
	for _, p := range s.pendingMakers {
		if p != nil && p.book == "ml" && time.Since(p.postedAt) <= liveMirrorTTL {
			pending = append(pending, struct {
				at    time.Time
				price float64
				raw   []byte
			}{at: p.postedAt, price: p.px, raw: append([]byte(nil), p.mlRow...)})
		}
	}
	s.pendMu.Unlock()
	for _, p := range pending {
		var lot newMLV2PaperLot
		if json.Unmarshal(p.raw, &lot) != nil {
			continue
		}
		lot.Price = p.price // the accepted Paper maker boundary, not the earlier ask
		if s.enqueueNewMLV2PaperDecision(lot, p.at, "maker") {
			queued++
		}
	}
	return queued
}

func newMLV2ConservativeLiveEdges(currentPoint, paperMean, paperLower float64) (meanNow, lowerNow float64) {
	return math.Min(currentPoint, paperMean), math.Min(currentPoint, paperLower)
}

func (s *Server) newMLV2LiveProof(c liveMirrorCandidate, price float64, maker bool) (bool, string, float64, float64, float64) {
	// A positive Paper/model cohort is not authenticated exchange profit evidence.
	if !newMLV2LiveCashAuthorityReady() {
		return false, newMLV2LiveCashRetiredReason, 0, 0, 0
	}
	route := "taker"
	if maker {
		route = "maker"
	}
	cap, ok := s.newMLV2Capability(c, route)
	if !ok {
		return false, "invalid-or-expired-new-ml-v2-capability", 0, 0, 0
	}
	if !s.liveNewMLVenueEnabled(c.Platform) {
		return false, "current-New-ML LIVE disabled for destination venue", 0, 0, 0
	}
	pWin, _, modelVersion, why := s.newMLV2Forecast(c.Platform, c.Ticker, c.Side)
	if why != "" || modelVersion != cap.modelVersion {
		if why == "" {
			why = "new-ml-model-generation-changed"
		}
		return false, why, 0, 0, 0
	}
	meanPC, lowerPC, markets, why := s.newMLV2PositivePaperLineageRoute(c.Platform, c.Side, route, cap.modelLineage)
	if why != "" {
		return false, why, 0, 0, 0
	}
	feePC := s.liveMirrorFeePC(c, maker, price)
	if math.IsNaN(feePC) || math.IsInf(feePC, 0) {
		return false, "current-new-ml-route-fee-unknown", 0, 0, feePC
	}
	currentEdge := pWin - price - feePC
	if currentEdge < s.liveMirrorEdgeFloor() {
		return false, "current-new-ml-fee-adjusted-edge-below-floor", currentEdge, currentEdge, feePC
	}
	meanNow, lowerNow := newMLV2ConservativeLiveEdges(currentEdge, meanPC, lowerPC)
	if lowerNow < s.liveMirrorEdgeFloor() {
		return false, "current-new-ml-exact-cohort-lower-bound-below-floor", meanNow, lowerNow, feePC
	}
	basis := fmt.Sprintf("new-ml-v2-sealed:%s:lineage=%s:%s/%s/%s:m%d:mean%+.4f:lower%+.4f",
		modelVersion, cap.modelLineage, c.Platform, strings.ToUpper(c.Side), route, markets, meanPC, lowerPC)
	return true, basis, meanNow, lowerNow, feePC
}
