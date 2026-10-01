package server

// This file is the venue-neutral state machine for the four non-atomic additive products.  It is
// intentionally separate from the venue adapters: the engine proves that intent is durable before
// a write, never treats an acknowledgement as a fill, and cannot advance past an ambiguous order.
// A production adapter still has to supply current authenticated books, exact fees, risk/ARM gates,
// FOK submission, authoritative reconciliation and the persistent kill-switch freeze.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

var (
	errR148StagedNoProof       = errors.New("no sealed untouched route proof for every execution venue")
	errR148StagedAmbiguous     = errors.New("ambiguous staged order state")
	errR148StagedUnwindOutside = errors.New("current unwind is worse than the certified bound")
)

type r148StagedReceiptState string

const (
	r148ReceiptFilled    r148StagedReceiptState = "filled"
	r148ReceiptUnfilled  r148StagedReceiptState = "unfilled"
	r148ReceiptPending   r148StagedReceiptState = "pending"
	r148ReceiptAmbiguous r148StagedReceiptState = "ambiguous"
)

type r148StagedQuote struct {
	Price, Available, Fee, Tick float64
	Source                      string
}

type r148StagedReceipt struct {
	OrderID                 string
	State                   r148StagedReceiptState
	FilledQty, AveragePrice float64
	FeeTotal                float64
	Authoritative           bool
	Source, Reason          string
}

// r148StagedBundleBroker is deliberately strict. Gate is called after the whole unfinished bundle
// has been repriced and is where the production adapter must enforce ARM + LIVE AUTO, kill switch,
// exact system allowlist, venue maintenance, horizon, balance/exposure and order-size constraints.
type r148StagedBundleBroker interface {
	Admission(context.Context, storage.ResearchRouteBundle, storage.StagedBundleExecutionProof) (map[int]r148StagedQuote, error)
	Quote(context.Context, storage.ResearchRouteBundleLeg, string, float64) (r148StagedQuote, error)
	Gate(context.Context, storage.ResearchRouteBundle, storage.ResearchRouteBundleLeg, string, r148StagedQuote) error
	SubmitFOK(context.Context, storage.ResearchRouteBundle, storage.ResearchRouteBundleLeg, string, float64, float64, string) (r148StagedReceipt, error)
	Reconcile(context.Context, storage.ResearchRouteBundleLeg, string, string) (r148StagedReceipt, error)
	Freeze(context.Context, string) error
}

// r148StagedBundleSizer is an optional production capability.  Research bundles are deliberately
// recorded at a small comparison size; a money adapter must turn that frozen ratio into a current
// NAV/depth-bounded execution plan before the immutable venue intent is written.  Hermetic state-
// machine brokers may omit this interface and retain the source bundle's exact quantity.
type r148StagedBundleSizer interface {
	SizeCurrent(context.Context, storage.ResearchRouteBundle, storage.StagedBundleExecutionProof) (storage.ResearchRouteBundle, r148StagedExecutionPlan, error)
}

// r163StagedMoneyPolicySource binds a production-sized package to the exact LIVE authority/risk
// publication it used. Hermetic research brokers omit it and remain money-inert.
type r163StagedMoneyPolicySource interface {
	stagedLiveMoneyPolicyGeneration() uint64
}

// r148StagedIntentReconciler closes the only unsafe crash boundary on venues without a client
// supplied order id.  It must decide an already-durable intent from account receipts; it is never
// permission to submit again.  A deterministic synthetic lookup id is acceptable while the venue
// outcome is still pending, provided Reconcile can continue that same immutable lookup later.
type r148StagedIntentReconciler interface {
	ReconcileIntent(context.Context, storage.ResearchRouteBundle, storage.ResearchRouteBundleLeg,
		string, string, time.Time) (r148StagedReceipt, error)
}

// r148StagedRiskManager is the durable package-level exposure overlay used by the production
// adapter. One immutable reservation covers every leg and canonical cluster before leg one can
// cross a venue boundary. Hermetic state-machine brokers may omit it; the production broker may
// not submit a staged intent without it.
type r148StagedRiskManager interface {
	ReserveStagedPackage(context.Context, string, storage.ResearchRouteBundle, map[int]r148StagedQuote) error
	RecordStagedRiskReceipt(context.Context, string, storage.ResearchRouteBundleLeg, string, r148StagedReceipt) error
	ReleaseStagedRisk(context.Context, string, string, any) error
	FreezeStagedRisk(context.Context, string, string, any) error
}

// r148StagedExecutionPlan is persisted inside proof_json.  Keeping it in the immutable intent
// makes recovery independent of today's NAV or books: restart resumes the exact quantities that
// were admitted, never silently resizes an already-started bundle.
type r148StagedExecutionPlan struct {
	Version                   int                `json:"version"`
	PackageMultiplier         int                `json:"package_multiplier"`
	Size                      float64            `json:"size"`
	Cost                      float64            `json:"cost"`
	Fee                       float64            `json:"fee"`
	PayoutFloor               float64            `json:"payout_floor"`
	NetFloor                  float64            `json:"net_floor"`
	PartialFillWorst          float64            `json:"partial_fill_worst"`
	UnwindWorst               float64            `json:"unwind_worst"`
	LegQuantities             []float64          `json:"leg_quantities"`
	NAV                       float64            `json:"nav,omitempty"`
	ArmBaseline               float64            `json:"arm_baseline,omitempty"`
	KellyFraction             float64            `json:"kelly_fraction,omitempty"`
	TargetUSD                 float64            `json:"target_usd,omitempty"`
	VenueHeadroom             float64            `json:"venue_headroom,omitempty"`
	RelatedHeadroom           float64            `json:"related_headroom,omitempty"`
	RequiredEdge              float64            `json:"required_edge,omitempty"`
	VenueNAV                  map[string]float64 `json:"venue_nav,omitempty"`
	VenueArmBaseline          map[string]float64 `json:"venue_arm_baseline,omitempty"`
	VenueTargetUSD            map[string]float64 `json:"venue_target_usd,omitempty"`
	VenueHeadroomUSD          map[string]float64 `json:"venue_headroom_usd,omitempty"`
	RelatedHeadroomUSD        map[string]float64 `json:"related_headroom_usd,omitempty"`
	LiveMoneyPolicyBound      bool               `json:"live_money_policy_bound,omitempty"`
	LiveMoneyPolicyGeneration uint64             `json:"live_money_policy_generation,omitempty"`
}

type r148StagedDurableProof struct {
	Sealed storage.StagedBundleExecutionProof `json:"sealed_proof"`
	Plan   r148StagedExecutionPlan            `json:"execution_plan"`
}

func r148DefaultExecutionPlan(b storage.ResearchRouteBundle) r148StagedExecutionPlan {
	q := make([]float64, len(b.Legs))
	for i := range b.Legs {
		q[i] = b.Legs[i].Quantity
	}
	return r148StagedExecutionPlan{Version: 1, PackageMultiplier: 1, Size: b.Size,
		Cost: b.Cost, Fee: b.Fee, PayoutFloor: b.PayoutFloor, NetFloor: b.NetFloor,
		PartialFillWorst: b.PartialFillWorst, UnwindWorst: b.UnwindWorst, LegQuantities: q}
}

func r148ValidExecutionPlan(source storage.ResearchRouteBundle, p r148StagedExecutionPlan) bool {
	if p.Version != 1 || p.PackageMultiplier < 1 || len(p.LegQuantities) != len(source.Legs) ||
		!finitePositive(p.Size) || !finitePositive(p.Cost) || p.Fee < 0 || p.PayoutFloor < 0 ||
		p.NetFloor <= 0 || p.PartialFillWorst > 0 || p.UnwindWorst > 0 {
		return false
	}
	for i, q := range p.LegQuantities {
		if !finitePositive(q) || math.Abs(q-source.Legs[i].Quantity*float64(p.PackageMultiplier)) > 1e-8 {
			return false
		}
	}
	return math.Abs(p.Size-source.Size*float64(p.PackageMultiplier)) <= 1e-8 &&
		math.Abs(p.PayoutFloor-source.PayoutFloor*float64(p.PackageMultiplier)) <= 1e-8 &&
		math.Abs(p.PartialFillWorst-source.PartialFillWorst*float64(p.PackageMultiplier)) <= 1e-8 &&
		math.Abs(p.UnwindWorst-source.UnwindWorst*float64(p.PackageMultiplier)) <= 1e-8 &&
		math.Abs(p.NetFloor-(p.PayoutFloor-p.Cost-p.Fee)) <= 1e-7
}

func r148ApplyExecutionPlan(source storage.ResearchRouteBundle, p r148StagedExecutionPlan) (storage.ResearchRouteBundle, error) {
	if !r148ValidExecutionPlan(source, p) {
		return storage.ResearchRouteBundle{}, errors.New("invalid immutable staged execution sizing plan")
	}
	out := source
	out.Size, out.Cost, out.Fee = p.Size, p.Cost, p.Fee
	out.PayoutFloor, out.NetFloor = p.PayoutFloor, p.NetFloor
	out.PartialFillWorst, out.UnwindWorst = p.PartialFillWorst, p.UnwindWorst
	out.Legs = append([]storage.ResearchRouteBundleLeg(nil), source.Legs...)
	for i := range out.Legs {
		out.Legs[i].Quantity = p.LegQuantities[i]
	}
	return out, nil
}

func r148ExecutionBundleHash(source storage.ResearchRouteBundle, p r148StagedExecutionPlan) string {
	return r148Hash(map[string]any{"source": r148BundleIdentityHash(source), "plan": p})
}

func r148DecodeDurablePlan(raw any) (r148StagedDurableProof, bool) {
	var out r148StagedDurableProof
	b, err := json.Marshal(raw)
	if err != nil || json.Unmarshal(b, &out) != nil || out.Plan.Version != 1 {
		return out, false
	}
	return out, true
}

type r148StagedBundleCoordinator struct {
	store          *storage.Store
	broker         r148StagedBundleBroker
	now            func() time.Time
	sourceBundleFn func(context.Context, string) (storage.ResearchRouteBundle, bool, error) // hermetic read-failure seam
}

func (c *r148StagedBundleCoordinator) sourceBundle(ctx context.Context, id string) (storage.ResearchRouteBundle, bool, error) {
	if c.sourceBundleFn != nil {
		return c.sourceBundleFn(ctx, id)
	}
	return c.store.ResearchRouteBundleByID(ctx, id)
}

func r148Hash(value any) string {
	raw, _ := json.Marshal(value)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func r148StagedLegOrder(b storage.ResearchRouteBundle, quotes map[int]r148StagedQuote) []int {
	order := make([]int, len(b.Legs))
	for i := range b.Legs {
		order[i] = i
	}
	// Put the least liquid leg first.  A later failure is then more likely to leave a liquid hedge
	// available. Stable identity tie-breakers make the order reproducible after a restart.
	sort.Slice(order, func(i, j int) bool {
		a, z := b.Legs[order[i]], b.Legs[order[j]]
		ar := quotes[order[i]].Available / math.Max(a.Quantity, 1e-12)
		zr := quotes[order[j]].Available / math.Max(z.Quantity, 1e-12)
		if math.Abs(ar-zr) > 1e-12 {
			return ar < zr
		}
		if a.Venue != z.Venue {
			return a.Venue < z.Venue
		}
		if a.Ticker != z.Ticker {
			return a.Ticker < z.Ticker
		}
		return a.Index < z.Index
	})
	return order
}

func r148ValidStagedLegOrder(order []int, legs int) bool {
	if legs < 2 || legs > 6 || len(order) != legs {
		return false
	}
	seen := make([]bool, legs)
	for _, idx := range order {
		if idx < 0 || idx >= legs || seen[idx] {
			return false
		}
		seen[idx] = true
	}
	return true
}

func r148AdmissionAllIn(b storage.ResearchRouteBundle, quotes map[int]r148StagedQuote) (float64, error) {
	if b.Size <= 0 || len(quotes) != len(b.Legs) {
		return 0, errors.New("whole-bundle admission did not quote every leg")
	}
	total := 0.0
	for i, leg := range b.Legs {
		q, ok := quotes[i]
		if !ok || q.Price <= 0 || q.Price >= 1 || q.Available+1e-9 < leg.Quantity || q.Fee < 0 ||
			strings.TrimSpace(q.Source) == "" || math.IsNaN(q.Price) || math.IsInf(q.Price, 0) ||
			math.IsNaN(q.Available) || math.IsInf(q.Available, 0) || math.IsNaN(q.Fee) || math.IsInf(q.Fee, 0) {
			return 0, fmt.Errorf("admission quote for leg %d lacks current executable depth or exact fee authority", i)
		}
		total += q.Price*leg.Quantity + q.Fee
	}
	unit := total / b.Size
	original := (b.Cost + b.Fee) / b.Size
	if !finitePositive(unit) || unit > original+1e-9 || b.PayoutFloor-total <= 0 {
		return 0, errors.New("current admitted bundle is no longer positive within its frozen quote cap")
	}
	return unit, nil
}

func finitePositive(v float64) bool {
	return v > 0 && !math.IsNaN(v) && !math.IsInf(v, 0)
}

func r148BundleIdentityHash(b storage.ResearchRouteBundle) string {
	return r148Hash(map[string]any{"bundle": b.BundleID, "certificate": b.CertificateHash,
		"state": b.StateVectorHash, "system": b.SystemID, "version": b.ExperimentVersion})
}

// AdmitCurrent writes the whole-bundle route intent before any venue write. It refuses rolling
// leaderboard evidence: only a sealed untouched positive result for every destination venue can
// reach the broker's ARM/risk admission gate.
func (c *r148StagedBundleCoordinator) AdmitCurrent(ctx context.Context, b storage.ResearchRouteBundle) (string, error) {
	if !r165StagedBundleNewCashAuthorityReady() {
		return "", errR165StagedBundleCashRetired
	}
	proof, ok, err := c.store.CurrentStagedBundleExecutionProof(ctx, b)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", errR148StagedNoProof
	}
	// Research quantity is evidence collection, not a bankroll instruction.  A production broker
	// resolves current authenticated NAV, risk headroom, exact depth/fees and quantity steps here,
	// before any immutable intent exists.  The resulting plan is then frozen for recovery.
	executionBundle := b
	plan := r148DefaultExecutionPlan(b)
	policySource, bindsMoneyPolicy := c.broker.(r163StagedMoneyPolicySource)
	var admittedMoneyPolicyGeneration uint64
	if bindsMoneyPolicy {
		admittedMoneyPolicyGeneration = policySource.stagedLiveMoneyPolicyGeneration()
	}
	if sizer, ok := c.broker.(r148StagedBundleSizer); ok {
		executionBundle, plan, err = sizer.SizeCurrent(ctx, b, proof)
		if err != nil {
			return "", err
		}
	}
	if !r148ValidExecutionPlan(b, plan) {
		return "", errors.New("staged execution sizing did not produce one valid immutable package")
	}
	quotes, err := c.broker.Admission(ctx, executionBundle, proof)
	if err != nil {
		return "", err
	}
	if bindsMoneyPolicy {
		if current := policySource.stagedLiveMoneyPolicyGeneration(); current != admittedMoneyPolicyGeneration {
			return "", errors.New("LIVE authority or risk policy changed during staged admission")
		}
		plan.LiveMoneyPolicyBound = true
		plan.LiveMoneyPolicyGeneration = admittedMoneyPolicyGeneration
	}
	maxAllIn, err := r148AdmissionAllIn(executionBundle, quotes)
	if err != nil {
		return "", err
	}
	order := r148StagedLegOrder(executionBundle, quotes)
	if len(order) < 2 {
		return "", errors.New("staged bundle requires at least two legs")
	}
	bundleHash, orderHash := r148ExecutionBundleHash(b, plan), r148Hash(order)
	durableProof := r148StagedDurableProof{Sealed: proof, Plan: plan}
	executionID := "staged:" + r148Hash(map[string]any{"bundle_hash": bundleHash, "proof": durableProof, "order": order})
	now := time.Now().UTC()
	if c.now != nil {
		now = c.now().UTC()
	}
	inserted, err := c.store.InsertStagedBundleExecutionIntent(ctx, storage.StagedBundleExecutionIntent{
		ExecutionID: executionID, BundleID: b.BundleID, SystemID: b.SystemID, BundleHash: bundleHash,
		OrderedLegs: order, OrderedLegsHash: orderHash, RoutePolicy: "staged_fok_v1", Created: now, Proof: durableProof,
		RequestedSize: executionBundle.Size, MaxAllInUnit: maxAllIn, FirstLegIndex: order[0],
	})
	if err != nil {
		return "", err
	}
	if !inserted {
		// bundle_id is intentionally unique. A newer sealed proof may produce a different
		// deterministic execution id for a bundle whose durable intent already exists; returning
		// that unpersisted id would make recovery look in the wrong journal. Resume the one durable
		// identity instead, and refuse if the immutable bundle identity itself drifted.
		existing, ok, readErr := c.store.StagedBundleExecutionIntentByBundleID(ctx, b.BundleID)
		if readErr != nil {
			return "", readErr
		}
		if !ok || existing.BundleHash != bundleHash || existing.SystemID != b.SystemID {
			return "", errors.New("existing staged bundle intent identity mismatch")
		}
		return existing.ExecutionID, nil
	}
	if inserted {
		_, err = c.append(ctx, executionID, "admitted", nil, "", "", r148StagedReceipt{},
			"sealed proof, current-NAV sizing and whole-bundle admission passed",
			map[string]any{"order": order, "proof": proof, "execution_plan": plan})
	}
	return executionID, err
}

func (c *r148StagedBundleCoordinator) append(ctx context.Context, executionID, eventType string,
	leg *storage.ResearchRouteBundleLeg, action, orderID string, receipt r148StagedReceipt, reason string, evidence any) (storage.StagedBundleExecutionEvent, error) {
	now := time.Now().UTC()
	if c.now != nil {
		now = c.now().UTC()
	}
	raw, err := json.Marshal(evidence)
	if err != nil {
		return storage.StagedBundleExecutionEvent{}, err
	}
	if len(raw) == 0 || string(raw) == "null" {
		raw = []byte("{}")
	}
	e := storage.StagedBundleExecutionEvent{ExecutionID: executionID, EventType: eventType,
		Observed: now, Action: action, OrderID: orderID, RequestedQty: 0, FilledQty: receipt.FilledQty,
		AveragePrice: receipt.AveragePrice, FeeTotal: receipt.FeeTotal, ReceiptSource: receipt.Source,
		Reason: reason, EvidenceJSON: string(raw)}
	if leg != nil {
		idx := leg.Index
		e.LegIndex, e.Venue, e.Ticker, e.Side, e.RequestedQty = &idx, leg.Venue, leg.Ticker, leg.Side, leg.Quantity
	}
	return c.store.AppendStagedBundleExecutionEvent(ctx, e)
}

func r148Terminal(events []storage.StagedBundleExecutionEvent) bool {
	for _, e := range events {
		if e.EventType == "completed" || e.EventType == "rejected" || e.EventType == "frozen" {
			return true
		}
	}
	return false
}

func r148LegState(events []storage.StagedBundleExecutionEvent) (filled map[int]storage.StagedBundleExecutionEvent,
	failed bool, fillOrder []int) {
	filled = map[int]storage.StagedBundleExecutionEvent{}
	for _, e := range events {
		if e.LegIndex == nil {
			continue
		}
		switch e.EventType {
		case "leg_filled":
			if _, exists := filled[*e.LegIndex]; !exists {
				fillOrder = append(fillOrder, *e.LegIndex)
			}
			filled[*e.LegIndex] = e
		case "leg_unfilled":
			failed = true
		case "unwind_filled":
			delete(filled, *e.LegIndex)
		case "unwind_unfilled":
			failed = true
		}
	}
	return filled, failed, fillOrder
}

type r148OutstandingOrder struct {
	legIndex int
	action   string
	orderID  string
	observed time.Time
}

func r148Outstanding(events []storage.StagedBundleExecutionEvent) (r148OutstandingOrder, bool) {
	resolved := map[string]bool{}
	for i := len(events) - 1; i >= 0; i-- {
		e := events[i]
		if e.LegIndex == nil {
			continue
		}
		key := fmt.Sprintf("%s:%d", e.Action, *e.LegIndex)
		switch e.EventType {
		case "leg_filled", "leg_unfilled", "unwind_filled", "unwind_unfilled":
			resolved[key] = true
		case "leg_submitted", "unwind_submitted":
			if !resolved[key] {
				return r148OutstandingOrder{legIndex: *e.LegIndex, action: e.Action, orderID: e.OrderID}, true
			}
		}
	}
	return r148OutstandingOrder{}, false
}

func r148IntentWithoutReceipt(events []storage.StagedBundleExecutionEvent) (r148OutstandingOrder, bool) {
	// An intent is written immediately before crossing the venue boundary. If the process dies
	// after venue acceptance but before the returned order id is journaled, absence of a submitted
	// event does NOT prove absence of an order (PolyUS has no client-order-id lookup). Fail closed;
	// a production recovery adapter must prove the venue ledger is flat before an operator clears it.
	state := map[string]r148OutstandingOrder{}
	for _, e := range events {
		if e.LegIndex == nil {
			continue
		}
		key := fmt.Sprintf("%s:%d", e.Action, *e.LegIndex)
		switch e.EventType {
		case "leg_intent", "unwind_intent":
			state[key] = r148OutstandingOrder{legIndex: *e.LegIndex, action: e.Action, observed: e.Observed}
		case "leg_submitted", "unwind_submitted", "leg_filled", "leg_unfilled", "unwind_filled", "unwind_unfilled":
			delete(state, key)
		}
	}
	if len(state) == 0 {
		return r148OutstandingOrder{}, false
	}
	keys := make([]string, 0, len(state))
	for key := range state {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return state[keys[0]], true
}

func (c *r148StagedBundleCoordinator) freeze(ctx context.Context, executionID, reason string, evidence any) error {
	// Freeze the money boundary first. A journal write failure must never leave trading enabled.
	freezeErr := c.broker.Freeze(ctx, reason)
	if freezeErr != nil {
		// Do not write a terminal event for a freeze that did not take effect. With the journal
		// still nonterminal, the next Resume is forced through this safety boundary again.
		return freezeErr
	}
	if risk, ok := c.broker.(r148StagedRiskManager); ok {
		if err := risk.FreezeStagedRisk(ctx, executionID, reason, evidence); err != nil {
			return err
		}
	}
	_, journalErr := c.append(ctx, executionID, "frozen", nil, "", "", r148StagedReceipt{}, reason, evidence)
	return journalErr
}

// retireFlatBeforeFirstVenueWrite closes a legacy admitted intent that never reached a venue.
// It deliberately does not call Freeze: there is no exposure to unwind and no ambiguity to turn
// into a global stop. Any package-level reservation is released before the terminal journal row;
// a failed append therefore remains retryable on the next recovery pass.
func (c *r148StagedBundleCoordinator) retireFlatBeforeFirstVenueWrite(ctx context.Context,
	executionID, reason string) error {
	if risk, ok := c.broker.(r148StagedRiskManager); ok {
		if err := risk.ReleaseStagedRisk(ctx, executionID, reason,
			map[string]any{"flat": true, "venue_attempted": false}); err != nil &&
			!errors.Is(err, errR148StagedRiskReservationMissing) {
			return err
		}
	}
	_, err := c.append(ctx, executionID, "rejected", nil, "", "", r148StagedReceipt{},
		reason, map[string]any{"flat": true, "venue_attempted": false,
			"cash_authority": "retired-r165"})
	return err
}

func r148ValidReceipt(r r148StagedReceipt, requested float64) bool {
	if r.State != r148ReceiptFilled && r.State != r148ReceiptUnfilled && r.State != r148ReceiptPending && r.State != r148ReceiptAmbiguous {
		return false
	}
	knownNoSend := r.State == r148ReceiptUnfilled && r.Authoritative &&
		r163StagedKnownNoSendSource(r.Source)
	if r.State != r148ReceiptAmbiguous && strings.TrimSpace(r.OrderID) == "" && !knownNoSend {
		// A pending or terminal venue result without a durable lookup key cannot survive a restart.
		return false
	}
	return strings.TrimSpace(r.Source) != "" &&
		!math.IsNaN(r.FilledQty) && !math.IsInf(r.FilledQty, 0) && r.FilledQty >= 0 && r.FilledQty <= requested+1e-9 &&
		!math.IsNaN(r.AveragePrice) && !math.IsInf(r.AveragePrice, 0) && r.AveragePrice >= 0 && r.AveragePrice <= 1 &&
		!math.IsNaN(r.FeeTotal) && !math.IsInf(r.FeeTotal, 0) && r.FeeTotal >= 0
}

func (c *r148StagedBundleCoordinator) recordReceipt(ctx context.Context, executionID string,
	leg storage.ResearchRouteBundleLeg, action string, r r148StagedReceipt, reconciled bool) error {
	if !r148ValidReceipt(r, leg.Quantity) || (r.State != r148ReceiptPending && r.State != r148ReceiptAmbiguous && !r.Authoritative) {
		return c.freeze(ctx, executionID, "non-authoritative or malformed venue receipt", r)
	}
	if risk, ok := c.broker.(r148StagedRiskManager); ok {
		if err := risk.RecordStagedRiskReceipt(ctx, executionID, leg, action, r); err != nil {
			return c.freeze(ctx, executionID, "durable staged risk receipt failed: "+err.Error(), r)
		}
	}
	if reconciled {
		if r.State == r148ReceiptPending {
			events, err := c.store.StagedBundleExecutionEvents(ctx, executionID)
			if err != nil {
				return err
			}
			for i := len(events) - 1; i >= 0; i-- {
				e := events[i]
				if e.EventType != "reconciled" || e.OrderID != r.OrderID || e.LegIndex == nil ||
					*e.LegIndex != leg.Index || e.Action != action {
					continue
				}
				var prior r148StagedReceipt
				if json.Unmarshal([]byte(e.EvidenceJSON), &prior) == nil && prior.State == r.State &&
					prior.OrderID == r.OrderID && prior.FilledQty == r.FilledQty &&
					prior.AveragePrice == r.AveragePrice && prior.FeeTotal == r.FeeTotal &&
					prior.Authoritative == r.Authoritative && prior.Source == r.Source && prior.Reason == r.Reason {
					return nil
				}
				break
			}
		}
		if _, err := c.append(ctx, executionID, "reconciled", &leg, action, r.OrderID, r,
			"authoritative restart reconciliation", r); err != nil {
			return err
		}
	}
	if r.State == r148ReceiptAmbiguous {
		return c.freeze(ctx, executionID, errR148StagedAmbiguous.Error(), r)
	}
	if r.State == r148ReceiptPending {
		return nil
	}
	if r.State == r148ReceiptFilled && math.Abs(r.FilledQty-leg.Quantity) > 1e-9 {
		return c.freeze(ctx, executionID, "FOK returned a partial fill", r)
	}
	eventType := "leg_unfilled"
	if action == "SELL" {
		eventType = "unwind_unfilled"
	}
	if r.State == r148ReceiptFilled {
		eventType = "leg_filled"
		if action == "SELL" {
			eventType = "unwind_filled"
		}
	}
	_, err := c.append(ctx, executionID, eventType, &leg, action, r.OrderID, r, r.Reason, r)
	if err != nil {
		return err
	}
	if eventType == "unwind_unfilled" {
		return c.freeze(ctx, executionID, "bounded unwind did not fill", r)
	}
	return nil
}

func (c *r148StagedBundleCoordinator) currentBuyQuotes(ctx context.Context, in storage.StagedBundleExecutionIntent,
	b storage.ResearchRouteBundle, filled map[int]storage.StagedBundleExecutionEvent) (map[int]r148StagedQuote, error) {
	quotes := map[int]r148StagedQuote{}
	total := 0.0
	for i, leg := range b.Legs {
		if fill, ok := filled[i]; ok {
			total += fill.FilledQty*fill.AveragePrice + fill.FeeTotal
			continue
		}
		q, err := c.broker.Quote(ctx, leg, "BUY", leg.Quantity)
		if err != nil {
			return nil, err
		}
		if q.Price <= 0 || q.Price >= 1 || q.Available+1e-9 < leg.Quantity || q.Fee < 0 || strings.TrimSpace(q.Source) == "" {
			return nil, errors.New("current buy book lacks executable depth or exact fee authority")
		}
		quotes[i] = q
		total += q.Price*leg.Quantity + q.Fee
	}
	if total/b.Size > in.MaxAllInUnit+1e-9 || b.PayoutFloor-total <= 0 {
		return nil, errors.New("repriced bundle no longer has positive certified value")
	}
	return quotes, nil
}

func (c *r148StagedBundleCoordinator) currentUnwindQuotes(ctx context.Context, b storage.ResearchRouteBundle,
	filled map[int]storage.StagedBundleExecutionEvent) (map[int]r148StagedQuote, error) {
	quotes, entry, exit := map[int]r148StagedQuote{}, 0.0, 0.0
	for i, fill := range filled {
		leg := b.Legs[i]
		q, err := c.broker.Quote(ctx, leg, "SELL", fill.FilledQty)
		if err != nil {
			return nil, err
		}
		if q.Price <= 0 || q.Price >= 1 || q.Available+1e-9 < fill.FilledQty || q.Fee < 0 || strings.TrimSpace(q.Source) == "" {
			return nil, errors.New("current unwind book lacks executable depth or exact fee authority")
		}
		quotes[i] = q
		entry += fill.FilledQty*fill.AveragePrice + fill.FeeTotal
		exit += fill.FilledQty*q.Price - q.Fee
	}
	if exit-entry < b.UnwindWorst-1e-9 {
		return nil, errR148StagedUnwindOutside
	}
	return quotes, nil
}

// Resume advances at most one money-writing transition. Calling it repeatedly is safe; a restart
// first reconciles any submitted order and cannot submit the next leg while the prior state is
// pending or ambiguous.
func (c *r148StagedBundleCoordinator) Resume(ctx context.Context, executionID string) error {
	in, ok, err := c.store.StagedBundleExecutionIntentByID(ctx, executionID)
	if err != nil || !ok {
		if err == nil {
			err = errors.New("staged execution intent not found")
		}
		return err
	}
	sourceBundle, ok, err := c.sourceBundle(ctx, in.BundleID)
	if err != nil {
		// Storage availability is not evidence that immutable identity changed. Recovery's outer
		// state machine pauses every new dispatch and retries transient reads; only a successful
		// read proving the source is missing may freeze globally.
		return err
	}
	if !ok {
		return c.freeze(ctx, executionID, "immutable source bundle missing", map[string]any{"bundle_id": in.BundleID})
	}
	// New production intents carry the exact sized plan inside proof_json.  The legacy fallback is
	// limited to the original unchanged research quantity; no missing plan may reconstruct a larger
	// order or re-read current NAV on restart.
	b := sourceBundle
	bundleHash := r148BundleIdentityHash(sourceBundle)
	var durablePlan r148StagedExecutionPlan
	if durable, hasPlan := r148DecodeDurablePlan(in.Proof); hasPlan {
		durablePlan = durable.Plan
		b, err = r148ApplyExecutionPlan(sourceBundle, durable.Plan)
		if err != nil {
			return c.freeze(ctx, executionID, "immutable staged sizing plan is unreadable", err.Error())
		}
		bundleHash = r148ExecutionBundleHash(sourceBundle, durable.Plan)
	} else if math.Abs(in.RequestedSize-sourceBundle.Size) > 1e-8 {
		return c.freeze(ctx, executionID, "sized staged intent lacks its immutable execution plan",
			map[string]any{"requested_size": in.RequestedSize, "source_size": sourceBundle.Size})
	}
	order := append([]int(nil), in.OrderedLegs...)
	if !r148ValidStagedLegOrder(order, len(b.Legs)) || in.BundleHash != bundleHash ||
		in.OrderedLegsHash != r148Hash(order) || in.FirstLegIndex != order[0] {
		return c.freeze(ctx, executionID, "immutable staged route hash mismatch", map[string]any{"persisted_order": order})
	}
	events, err := c.store.StagedBundleExecutionEvents(ctx, executionID)
	if err != nil || r148Terminal(events) {
		return err
	}
	if orphan, exists := r148IntentWithoutReceipt(events); exists {
		reconciler, supported := c.broker.(r148StagedIntentReconciler)
		if !supported {
			return c.freeze(ctx, executionID,
				"durable intent has no venue order receipt; prior acceptance cannot be disproved",
				map[string]any{"leg_index": orphan.legIndex, "action": orphan.action, "manual_venue_reconciliation_required": true})
		}
		leg := b.Legs[orphan.legIndex]
		idem := r148Hash(map[string]any{"execution": executionID, "leg": orphan.legIndex, "action": orphan.action})
		r, reconcileErr := reconciler.ReconcileIntent(ctx, b, leg, orphan.action, idem, orphan.observed)
		if reconcileErr != nil {
			if r148TransientRecoveryError(reconcileErr) {
				return reconcileErr
			}
			// A lost response is a money-safety boundary. A partial/failed history read cannot prove
			// whether the deterministic client order reached the venue, so freeze immediately rather
			// than leaving a possible fill untracked or retrying the write.
			return c.freeze(ctx, executionID, "intent-only venue recovery failed: "+reconcileErr.Error(),
				map[string]any{"leg_index": orphan.legIndex, "action": orphan.action,
					"idempotency_key": idem, "manual_venue_reconciliation_required": true})
		}
		if !r148ValidReceipt(r, leg.Quantity) {
			return c.freeze(ctx, executionID, "intent-only recovery returned a malformed receipt", r)
		}
		eventType := "leg_submitted"
		if orphan.action == "SELL" {
			eventType = "unwind_submitted"
		}
		if _, err := c.append(ctx, executionID, eventType, &leg, orphan.action, r.OrderID, r,
			"intent-only venue ledger recovery; no resubmit", map[string]any{"receipt": r, "idempotency_key": idem}); err != nil {
			return err
		}
		return c.recordReceipt(ctx, executionID, leg, orphan.action, r, true)
	}
	if pending, exists := r148Outstanding(events); exists {
		r, reconcileErr := c.broker.Reconcile(ctx, b.Legs[pending.legIndex], pending.action, pending.orderID)
		if reconcileErr != nil {
			if r148TransientRecoveryError(reconcileErr) {
				return reconcileErr
			}
			return c.freeze(ctx, executionID, "venue reconciliation failed after a submitted order", reconcileErr.Error())
		}
		return c.recordReceipt(ctx, executionID, b.Legs[pending.legIndex], pending.action, r, true)
	}
	filled, failed, fillOrder := r148LegState(events)
	if failed {
		if len(filled) == 0 {
			_, err = c.append(ctx, executionID, "rejected", nil, "", "", r148StagedReceipt{},
				"staged leg did not fill; no exposure remains", map[string]any{"flat": true})
			if err == nil {
				if risk, ok := c.broker.(r148StagedRiskManager); ok {
					err = risk.ReleaseStagedRisk(ctx, executionID, "flat staged rejection", map[string]any{"flat": true})
				}
			}
			return err
		}
		quotes, quoteErr := c.currentUnwindQuotes(ctx, b, filled)
		if quoteErr != nil {
			return c.freeze(ctx, executionID, "bounded unwind unavailable: "+quoteErr.Error(), map[string]any{"filled_legs": len(filled)})
		}
		for i := len(fillOrder) - 1; i >= 0; i-- {
			idx := fillOrder[i]
			fill, exists := filled[idx]
			if !exists {
				continue
			}
			leg, q := b.Legs[idx], quotes[idx]
			if err := c.broker.Gate(ctx, b, leg, "SELL", q); err != nil {
				return c.freeze(ctx, executionID, "unwind risk gate failed: "+err.Error(), q)
			}
			idem := r148Hash(map[string]any{"execution": executionID, "leg": idx, "action": "SELL"})
			if _, err := c.append(ctx, executionID, "unwind_intent", &leg, "SELL", "", r148StagedReceipt{},
				"bounded reverse-order unwind", map[string]any{"quote": q, "idempotency_key": idem}); err != nil {
				return err
			}
			r, submitErr := c.broker.SubmitFOK(ctx, b, leg, "SELL", fill.FilledQty, q.Price, idem)
			if submitErr != nil {
				return c.freeze(ctx, executionID, "ambiguous unwind submit: "+submitErr.Error(), map[string]any{"idempotency_key": idem})
			}
			if r.OrderID != "" {
				if _, err := c.append(ctx, executionID, "unwind_submitted", &leg, "SELL", r.OrderID, r,
					"venue accepted unwind request; fill not implied", r); err != nil {
					freezeErr := c.freeze(ctx, executionID, "venue accepted unwind but its durable receipt could not be journaled", r)
					return errors.Join(err, freezeErr)
				}
			}
			return c.recordReceipt(ctx, executionID, leg, "SELL", r, false)
		}
		_, err = c.append(ctx, executionID, "rejected", nil, "", "", r148StagedReceipt{},
			"staged route failed and all known fills were authoritatively unwound", map[string]any{"flat": true})
		if err == nil {
			if risk, ok := c.broker.(r148StagedRiskManager); ok {
				err = risk.ReleaseStagedRisk(ctx, executionID, "staged route authoritatively unwound", map[string]any{"flat": true})
			}
		}
		return err
	}
	if len(filled) == len(b.Legs) {
		_, err = c.append(ctx, executionID, "completed", nil, "", "", r148StagedReceipt{},
			"all FOK legs authoritatively filled", map[string]any{"filled_legs": len(filled)})
		if err == nil {
			if risk, ok := c.broker.(r148StagedRiskManager); ok {
				err = risk.ReleaseStagedRisk(ctx, executionID, "staged package is account-visible", map[string]any{"filled_legs": len(filled)})
			}
		}
		return err
	}
	quotes, quoteErr := c.currentBuyQuotes(ctx, in, b, filled)
	if quoteErr != nil {
		// This is a clean pre-write refusal. If earlier legs filled, mark this leg failed so the next
		// resume enters the bounded unwind branch.
		for _, idx := range order {
			if _, exists := filled[idx]; exists {
				continue
			}
			_, err = c.append(ctx, executionID, "leg_unfilled", &b.Legs[idx], "BUY", "", r148StagedReceipt{}, quoteErr.Error(), map[string]any{"prewrite": true})
			return err
		}
	}
	if len(filled) == 0 {
		if risk, ok := c.broker.(r148StagedRiskManager); ok {
			if err := risk.ReserveStagedPackage(ctx, executionID, b, quotes); err != nil {
				return c.freeze(ctx, executionID, "whole-package pending-risk reservation failed: "+err.Error(),
					map[string]any{"bundle_id": b.BundleID})
			}
		}
	}
	ctx = r163WithStagedMoneyPolicy(ctx, durablePlan.LiveMoneyPolicyBound,
		durablePlan.LiveMoneyPolicyGeneration)
	for _, idx := range order {
		if _, exists := filled[idx]; exists {
			continue
		}
		leg, q := b.Legs[idx], quotes[idx]
		if err := c.broker.Gate(ctx, b, leg, "BUY", q); err != nil {
			_, appendErr := c.append(ctx, executionID, "leg_unfilled", &leg, "BUY", "", r148StagedReceipt{},
				"pre-submit gate: "+err.Error(), map[string]any{"prewrite": true, "quote": q})
			return appendErr
		}
		idem := r148Hash(map[string]any{"execution": executionID, "leg": idx, "action": "BUY"})
		if _, err := c.append(ctx, executionID, "leg_intent", &leg, "BUY", "", r148StagedReceipt{},
			"durable intent before venue write", map[string]any{"quote": q, "idempotency_key": idem}); err != nil {
			return err
		}
		r, submitErr := c.broker.SubmitFOK(ctx, b, leg, "BUY", leg.Quantity, q.Price, idem)
		if submitErr != nil {
			return c.freeze(ctx, executionID, "ambiguous leg submit: "+submitErr.Error(), map[string]any{"idempotency_key": idem})
		}
		if r.OrderID != "" {
			if _, err := c.append(ctx, executionID, "leg_submitted", &leg, "BUY", r.OrderID, r,
				"venue accepted FOK request; fill not implied", r); err != nil {
				freezeErr := c.freeze(ctx, executionID, "venue accepted leg but its durable receipt could not be journaled", r)
				return errors.Join(err, freezeErr)
			}
		}
		return c.recordReceipt(ctx, executionID, leg, "BUY", r, false)
	}
	return nil
}
