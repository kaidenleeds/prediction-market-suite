package server

// verdicts.go — R115 STATISTICAL VERDICT ENGINE (operator Part 1; replaces fixed-n verdicts).
//
// One shared engine summarizes EVERY running experiment continuously. The confidence-sequence
// math is valid for the observations supplied to it, but the observation source decides what the
// result means: simulated/assumed fills are research only, while only an authenticated,
// fill-conditioned exchange cohort may be described as profit evidence.
//
// SEQUENTIAL-TESTING CHOICE (documented plainly, per the operator's ask): we use a Robbins-style
// normal-mixture CONFIDENCE SEQUENCE (the "always-valid CI" of Howard/Ramdas et al.) instead of a
// fixed-n t/Wilson interval. A fixed-n 95% CI is only honest if you look ONCE at a pre-chosen n;
// checking it every sweep inflates false positives badly (peek often enough and a null experiment
// will eventually "prove" itself). The mixture CS radius
//
//	r(n) = sd · sqrt( (2·(n·ρ+1)/(n²·ρ)) · ln( sqrt(n·ρ+1)/α ) ),  ρ=1, α=0.05
//
// is wider than the t-interval at any given n — that width is the price of the guarantee that
// with 95% probability the TRUE mean is inside the interval AT EVERY n SIMULTANEOUSLY. So a
// verdict fired at n=40 is exactly as trustworthy as one fired at n=4,000.
//
// R116 RANGE CORRECTION (found by the ordered null-stream stress test): the sample-SD plug-in
// UNDERSTATES risk for skewed bounded payoffs (a 90–95¢ contract wins tiny and often, loses big
// and rarely — early samples can show almost no variance). 1,000 simulated ZERO-edge streams
// through the R115 rule gave 9.5–11.1% false-PROVEN on skewed nulls (vs the 5% promise). Since
// every observation here lives inside a $1 range, we add the empirical-Bernstein-style bounded-
// range term  + 0.5·ln(sqrt(n+1)/α)/n  to the radius. c=0.5 is the smallest constant that held
// ALL null shapes ≤5% in simulation (sym 0.7% · skew90 1.1% · skew95 1.7% · gauss 0.0%; c=0 fails
// at 9.5–11.1%). The term decays as ln(n)/n: invisible at n≈100k (+0.00005), decisive exactly in
// the small-n/skewed regime where the plug-in SD lies (n=32: +0.074). Sim: r116_scratch/.
//
// INTERNAL MATH STATES per experiment:
//	COLLECTING — the CS still spans 0
//	PROVEN+    — the supplied observations' CS sits above 0
//	PROVEN−    — the supplied observations' CS sits below 0
//	FUTILE     — n ≥ 300 and the supplied observations' CS is tightly pinned around 0
// These internal labels are not automatically money claims. operatorVerdict converts every row
// without explicit profit_evidence into RESEARCH labels and retains the internal state separately
// as simulation_state. That prevents a valid calculation over assumed fills from being presented
// as valid exchange execution.
//
// DIAGNOSTIC INVERT RULE: an internally negative family may spawn a LOG-ONLY arithmetic twin —
// same detections, opposite side. Inversion is not free: the twin pays the fees the original
// paid PLUS its own, PLUS (R118) a SPREAD HAIRCUT for actually crossing to the other side —
// the honest inverted edge is (−mean − 2·feePC − haircut). The haircut is the family's mean
// per-row half-spread from signal_log (entry_price ≈ mid, so other-side ask ≈ mid + spread/2);
// rows/families with no recorded spread charge a conservative 2¢ default (the R116 "spread-free
// upper bound" is retired). The taker fee needs no inverted-side recompute: 0.07·p·(1−p) is
// symmetric in p ↔ 1−p, so the inverted side's fee equals the original's. The engine computes
// the net edge BEFORE spawning; only families whose losses are deeper than the full round-trip
// drag (2·fee + haircut) qualify. The twin is a counterfactual ledger graded continuously from
// the same rows with the sign flipped and the drag subtracted. It is explicitly tagged as
// counterfactual_same_row_arithmetic and can never be profit evidence or cash authority. A real
// inverse hypothesis must collect its own opposite-side book, fee, fillability, and settlement.
//
// SURFACES: GET /api/verdicts · Experiments panel (dashboard WREG 'verdicts') · a telegram/audit
// line whenever any experiment CHANGES state (persisted in kv verdict_state:<family>) · the
// auditor-readable state file data/verdicts.json (edge_snapshot.json pattern).

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

const (
	verdictTelegramBatchKV     = "verdict_telegram_batch_v1"
	verdictTelegramBatchWindow = 15 * time.Minute
	verdictProofMethodDefault  = "market-cell-cs-v1"
	// R145: the operator removed canonical-event clusters from display and trading authority.
	// They remain stored as relationship diagnostics, while executable route evidence uses one
	// observation per distinct venue+ticker contract.  This method token prevents an old
	// canonical-event verdict receipt from silently masquerading as the new contract-cell cohort.
	verdictProofMethodContract = "distinct-contract-cs-v1"
)

type verdictTelegramBatch struct {
	Started string                           `json:"started"`
	Lines   []string                         `json:"lines,omitempty"` // backward-compatible v1 queue
	Entries map[string]verdictTelegramChange `json:"entries,omitempty"`
}

type verdictTelegramChange struct {
	Key      string `json:"key"`
	Kind     string `json:"kind,omitempty"` // verdict | proof-method-reset
	Line     string `json:"line,omitempty"`
	Previous string `json:"previous,omitempty"`
	Current  string `json:"current,omitempty"`
	Markets  int    `json:"markets,omitempty"`
}

// verdictStateReceipt replaces the old plain state string while remaining able to read it. The
// method tag prevents a proof-definition migration from masquerading as many independent market
// verdict changes. Old values such as "PROVEN+" decode with an empty ProofMethod.
type verdictStateReceipt struct {
	State       string `json:"state"`
	ProofMethod string `json:"proof_method,omitempty"`
}

type verdictEnt struct {
	Family string `json:"family"`
	// SourceFamily/Platform/OriginLayer/Route make executable one-share cells explicit. Family
	// remains the stable display key (for example taker:kflow@kalshi); these fields prevent
	// consumers from reverse-parsing that label and accidentally pooling venue, side, or route.
	SourceFamily       string `json:"source_family,omitempty"`
	Platform           string `json:"platform,omitempty"`
	OriginLayer        string `json:"origin_layer,omitempty"`
	Route              string `json:"route,omitempty"`
	EvidenceTier       string `json:"evidence_tier,omitempty"`
	FillConditioned    bool   `json:"fill_conditioned"`
	ProfitEvidence     bool   `json:"profit_evidence"`
	VoidReason         string `json:"void_reason,omitempty"`
	LiveAuthorizes     bool   `json:"live_authorizes"`
	Side               string `json:"side,omitempty"`           // exact route outcome side; blank only when the experiment truly pools sides
	Group              string `json:"group"`                    // signal | taker | maker | book | rfq | parlay | invert
	N                  int    `json:"n"`                        // statistical sample used by the verdict; see Markets/SettledRows
	Markets            int    `json:"unique_markets,omitempty"` // independent proof cells; canonical event clusters for UnitTrial routes
	EventClusters      int    `json:"canonical_event_clusters,omitempty"`
	ContractMarkets    int    `json:"unique_contract_markets,omitempty"`
	UnclusteredMarkets int    `json:"unclustered_markets,omitempty"`
	SettledRows        int    `json:"settled_rows,omitempty"`
	// PaperPointMean/SD retain the contract-market point estimate for PAPER exploration when
	// pre-R144 rows cannot enter dependence-safe proof because they lack an entry-time canonical
	// event identity. They never change N/Markets, the confidence sequence, or LIVE authority.
	PaperPointMean      float64                 `json:"paper_point_mean,omitempty"`
	PaperPointSD        float64                 `json:"paper_point_sd,omitempty"`
	PaperPointMeanAsk   float64                 `json:"paper_point_mean_ask,omitempty"`
	PaperPointMeanFeePC float64                 `json:"paper_point_mean_fee_pc,omitempty"`
	Mean                float64                 `json:"mean"` // fee-net mean per unit (see Unit)
	SD                  float64                 `json:"sd"`
	Lo                  float64                 `json:"ci_lo"` // 95% always-valid confidence sequence
	Hi                  float64                 `json:"ci_hi"`
	FeePC               float64                 `json:"fee_pc"`                     // mean fee drag per unit (INVERT hurdle = 2×this)
	MeanAsk             float64                 `json:"mean_ask,omitempty"`         // mean executable entry ask behind an exact route's measured edge
	Unit                string                  `json:"unit"`                       // "$/contract" | "$/round" | "$/quote" | "$/$1"
	State               string                  `json:"state"`                      // operator state; research-only rows never use PROVEN labels
	SimulationState     string                  `json:"simulation_state,omitempty"` // raw confidence-sequence state for non-profit research
	SDs                 float64                 `json:"sds_from_zero"`
	PValue              float64                 `json:"p_naive"`                // naive (non-sequential) two-sided p — context only, the CS is the verdict
	Invert              float64                 `json:"invert_edge"`            // >0 only on a qualifying PROVEN− family: −mean − 2·fee − spread haircut (R118 honest cost model, see header)
	InvHC               float64                 `json:"invert_spread_haircut"`  // R118: the per-contract spread haircut the twin is charged (family mean half-spread; 2¢ default when unrecorded)
	Locked              bool                    `json:"venue_locked"`           // R116: measured at poly-int prices (unbettable venue) — NEVER arm; invert twins inherit
	KSh                 float64                 `json:"kalshi_share,omitempty"` // R130: share of graded rows on kalshi (signal families; the follower's venue membership)
	PSh                 float64                 `json:"polyus_share,omitempty"` // R130: share of graded rows on polyus; invert twins inherit both
	Venues              map[string]venueVerdict `json:"venues,omitempty"`       // R132: independent executable-venue truth
	SeedSource          string                  `json:"seed_source,omitempty"`  // exact legacy route used only until this named system settles its own rows
}

func verdictHasAuthenticatedProfitEvidence(v verdictEnt) bool {
	if !v.ProfitEvidence || !v.FillConditioned {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(v.EvidenceTier)) {
	case "authenticated_live_fill", "authenticated_exchange_fill", "exchange_live":
		return true
	default:
		return false
	}
}

// operatorVerdict prevents mathematically strong results over the wrong observation class from
// masquerading as exchange profit. It deliberately preserves the raw state and raw numbers for
// audit/research, but changes the operator-facing label, removes inverse/LIVE implications, and
// supplies an explicit evidence description. This is a presentation and authority boundary; the
// underlying research computation remains queryable and continues collecting.
func operatorVerdict(v verdictEnt) verdictEnt {
	if verdictHasAuthenticatedProfitEvidence(v) {
		return v
	}
	v.ProfitEvidence = false
	if strings.TrimSpace(v.SimulationState) == "" {
		v.SimulationState = strings.TrimSpace(v.State)
	}
	if strings.TrimSpace(v.EvidenceTier) == "" {
		v.EvidenceTier = "research_model_or_simulation"
	}
	if strings.TrimSpace(v.VoidReason) == "" {
		v.VoidReason = "no authenticated exchange-fill cohort backs this result"
	}
	v.FillConditioned = false
	v.LiveAuthorizes = false
	v.Invert = 0
	switch strings.TrimSpace(v.State) {
	case "VOID ASSUMED-FILL HISTORY":
		// The most important legacy class already has the clearest possible label.
	case "PROVEN+":
		v.State = "RESEARCH+"
	case "PROVEN-":
		v.State = "RESEARCH-"
	case "FUTILE":
		v.State = "RESEARCH-INCONCLUSIVE"
	default:
		v.State = "RESEARCH-COLLECTING"
	}
	if len(v.Venues) > 0 {
		venues := make(map[string]venueVerdict, len(v.Venues))
		for venue, vv := range v.Venues {
			vv.Invert = 0
			switch strings.TrimSpace(vv.State) {
			case "PROVEN+":
				vv.State = "RESEARCH+"
			case "PROVEN-":
				vv.State = "RESEARCH-"
			case "FUTILE":
				vv.State = "RESEARCH-INCONCLUSIVE"
			default:
				vv.State = "RESEARCH-COLLECTING"
			}
			venues[venue] = vv
		}
		v.Venues = venues
	}
	return v
}

func operatorVerdicts(vs []verdictEnt) []verdictEnt {
	out := make([]verdictEnt, len(vs))
	for i := range vs {
		out[i] = operatorVerdict(vs[i])
	}
	return out
}

type venueVerdict struct {
	N           int     `json:"n"`
	Markets     int     `json:"unique_markets,omitempty"`
	SettledRows int     `json:"settled_rows,omitempty"`
	Mean        float64 `json:"mean"`
	SD          float64 `json:"sd"`
	Lo          float64 `json:"ci_lo"`
	Hi          float64 `json:"ci_hi"`
	FeePC       float64 `json:"fee_pc"`
	InvHC       float64 `json:"invert_spread_haircut"`
	State       string  `json:"state"`
	Invert      float64 `json:"invert_edge,omitempty"`
}

func verdictStableKey(v verdictEnt) string {
	key := strings.TrimSpace(v.Family)
	if side := strings.ToUpper(strings.TrimSpace(v.Side)); side == "YES" || side == "NO" {
		key += "|side=" + side
	}
	return key
}

func verdictDisplayName(v verdictEnt) string {
	name := strings.TrimSpace(v.Family)
	if side := strings.ToUpper(strings.TrimSpace(v.Side)); side == "YES" || side == "NO" {
		name += " [" + side + "]"
	}
	return name
}

func verdictProofMethod(v verdictEnt) string {
	group := strings.ToLower(strings.TrimSpace(v.Group))
	if strings.EqualFold(strings.TrimSpace(v.Route), "taker") &&
		(group == "taker" || group == "strategy") {
		return verdictProofMethodContract
	}
	return verdictProofMethodDefault
}

func decodeVerdictStateReceipt(raw string) (verdictStateReceipt, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return verdictStateReceipt{}, false
	}
	var receipt verdictStateReceipt
	if strings.HasPrefix(raw, "{") && json.Unmarshal([]byte(raw), &receipt) == nil &&
		strings.TrimSpace(receipt.State) != "" {
		receipt.State = strings.TrimSpace(receipt.State)
		receipt.ProofMethod = strings.TrimSpace(receipt.ProofMethod)
		return receipt, false
	}
	// Backward compatibility: every release before this receipt stored only the state token.
	return verdictStateReceipt{State: raw}, true
}

func encodeVerdictStateReceipt(v verdictEnt) string {
	receipt := verdictStateReceipt{State: strings.TrimSpace(v.State), ProofMethod: verdictProofMethod(v)}
	encoded, err := json.Marshal(receipt)
	if err != nil { // strings cannot fail JSON encoding; retain a safe fallback if that ever changes.
		return receipt.State
	}
	return string(encoded)
}

func isVerdictProofMethodReset(previous verdictStateReceipt, v verdictEnt) bool {
	return previous.State != "" && previous.State != v.State && v.State == "COLLECTING" &&
		v.N == 0 && v.SettledRows > 0 && v.UnclusteredMarkets > 0 &&
		verdictProofMethod(v) == verdictProofMethodContract &&
		previous.ProofMethod != verdictProofMethodContract
}

func verdictTransitionLine(v verdictEnt) string {
	rangeText := fmt.Sprintf("95%% range %+.3f..%+.3f", v.Lo, v.Hi)
	if v.N < 2 || math.IsNaN(v.Lo) || math.IsNaN(v.Hi) || math.IsInf(v.Lo, 0) ||
		math.IsInf(v.Hi, 0) || math.Abs(v.Lo) >= 9000 || math.Abs(v.Hi) >= 9000 {
		rangeText = "95% range unavailable"
	}
	label := "System result"
	metric := "exchange edge"
	if !verdictHasAuthenticatedProfitEvidence(v) {
		label = "Research-only result"
		metric = "modeled edge"
	}
	return fmt.Sprintf("%s: %s is now %s (n=%d, %s %+.3f %s, %s)",
		label, verdictDisplayName(v), v.State, v.N, metric, v.Mean, v.Unit, rangeText)
}

func verdictTelegramChangeFor(previous verdictStateReceipt, v verdictEnt) verdictTelegramChange {
	change := verdictTelegramChange{Key: verdictStableKey(v), Kind: "verdict",
		Previous: previous.State, Current: v.State, Line: verdictTransitionLine(v)}
	if isVerdictProofMethodReset(previous, v) {
		change.Kind = "proof-method-reset"
		change.Line = ""
		change.Markets = v.UnclusteredMarkets
	}
	return change
}

func venueVerdictOf(v verdictEnt) venueVerdict {
	return venueVerdict{N: v.N, Mean: v.Mean, SD: v.SD, Lo: v.Lo, Hi: v.Hi,
		Markets: v.Markets, SettledRows: v.SettledRows,
		FeePC: v.FeePC, InvHC: v.InvHC, State: v.State, Invert: v.Invert}
}

// invDefaultSpreadHaircutPC — R118: the conservative per-contract spread haircut an invert twin
// pays when no per-row spread data exists (signal families with unrecorded spread_cents, and all
// book/maker/rfq/parlay experiments). Documented constant, not a silent zero.
const invDefaultSpreadHaircutPC = 0.02

// invertDrag — the FULL round-trip cost an inverted twin pays on top of the sign flip:
// the original side's fee + the inverted side's fee (equal — 0.07·p·(1−p) is symmetric) + the
// spread haircut to cross to the other side. The invert SPAWN threshold is exactly loss > this.
func invertDrag(feePC, spreadHaircutPC float64) float64 {
	if spreadHaircutPC <= 0 {
		spreadHaircutPC = invDefaultSpreadHaircutPC
	}
	return 2*feePC + spreadHaircutPC
}

// csRadius — the always-valid confidence-sequence radius (see file header for the math + why).
// R116: + bounded-range correction c·L/n (c=0.5, sim-validated) so skewed families can't
// false-prove off an understated early sample SD.
func csRadius(n int, sd float64) float64 {
	if n < 2 || sd <= 0 {
		return math.Inf(1)
	}
	nn := float64(n)
	const rho, alpha = 1.0, 0.05
	L := math.Log(math.Sqrt(nn*rho+1) / alpha)
	return sd*math.Sqrt((2*(nn*rho+1))/(nn*nn*rho)*L) + 0.5*L/nn
}

// verdictFrom classifies one experiment from its (n, mean, sd, fee, spread-haircut) summary.
// spreadHaircutPC feeds the INVERT qualification only (≤0 ⇒ the 2¢ documented default).
func verdictFrom(family, group, unit string, n int, mean, sd, feePC, spreadHaircutPC float64) verdictEnt {
	if spreadHaircutPC <= 0 {
		spreadHaircutPC = invDefaultSpreadHaircutPC
	}
	v := verdictEnt{Family: family, Group: group, Unit: unit, N: n,
		SettledRows: n,
		Mean:        vRnd4(mean), SD: vRnd4(sd), FeePC: vRnd4(feePC), InvHC: vRnd4(spreadHaircutPC), State: "COLLECTING"}
	r := csRadius(n, sd)
	if math.IsInf(r, 1) {
		// n<2 or sd=0: the CS is unbounded. SENTINELS, not ±Inf — an Inf value poisons the whole
		// JSON encode (the exact failure class of auditor bug 331 / the R114 +Inf cluster cap).
		v.Lo, v.Hi = -9999, 9999
		return v
	}
	v.Lo, v.Hi = vRnd4(mean-r), vRnd4(mean+r)
	if sd > 0 {
		se := sd / math.Sqrt(float64(n))
		v.SDs = vRnd4(mean / se)
		v.PValue = vRnd4(2 * (1 - normCDF(math.Abs(mean)/se)))
	}
	minN := 20
	switch {
	case n >= minN && v.Lo > 0:
		v.State = "PROVEN+"
	case n >= minN && v.Hi < 0:
		v.State = "PROVEN-"
		if inv := -mean - invertDrag(feePC, spreadHaircutPC); inv > 0 {
			v.Invert = vRnd4(inv) // qualifies for the log-only inverted twin (loss > 2·fee + spread haircut)
		}
	case n >= 300 && (v.Hi-v.Lo) < math.Max(0.02, 2*feePC) && v.Lo < 0 && v.Hi > 0:
		v.State = "FUTILE"
	}
	return v
}

// unitTrialVerdict converts one observed-book quote route into contract-cell simulated-fill
// evidence. Unit trials observe ask, visible depth and fee, but never submit an order or receive an
// exchange fill. Their confidence sequence can describe the simulation; it cannot become PROVEN
// execution evidence, generate an automatic inverse, or authorize LIVE.
// R145 operator rule: n is one distinct settled venue+ticker contract, regardless of retries,
// shares, snapshots, or repeated entries. Canonical event clusters stay attached for research
// diagnostics only and never choose the point estimate, confidence sequence, or trading state.
// A contract-cell CS does not claim that different tickers are statistically independent; LIVE
// authorization remains a separate sealed untouched-holdout decision in liveMirrorProof.
func unitTrialVerdict(u storage.UnitTrialStat) verdictEnt {
	origin, group, prefix := strings.ToLower(strings.TrimSpace(u.OriginLayer)), "taker", "taker"
	if origin == "strategy" {
		group, prefix = "strategy", "strategy"
	} else {
		origin = "model"
	}
	platform := strings.ToLower(strings.TrimSpace(u.Platform))
	v := verdictFrom(prefix+":"+u.Family+"@"+platform, group,
		"$/contract", u.SettledMarkets, u.MeanPC,
		u.SDPC, u.MeanFeePC, 0)
	v.Markets, v.EventClusters, v.ContractMarkets =
		u.SettledMarkets, u.SettledEventClusters, u.SettledMarkets
	v.UnclusteredMarkets, v.SettledRows = u.UnclusteredSettledMarkets, u.N
	v.PaperPointMean, v.PaperPointSD = v.Mean, v.SD
	v.PaperPointMeanAsk, v.PaperPointMeanFeePC = u.MeanAsk, u.MeanFeePC
	v.MeanAsk = u.MeanAsk
	v.SourceFamily, v.Platform, v.OriginLayer, v.Route = u.Family, platform, origin, "taker"
	v.Side = strings.ToUpper(strings.TrimSpace(u.Side))
	v.EvidenceTier, v.FillConditioned, v.ProfitEvidence, v.LiveAuthorizes = "historical_assumed_fill_simulation_void", false, false, false
	v.VoidReason = "no exchange order, acknowledgement, or fill was observed"
	v.Invert = 0
	v.SimulationState = v.State
	v.State = "VOID ASSUMED-FILL HISTORY"
	return v
}

func vRnd4(f float64) float64 { return math.Round(f*1e4) / 1e4 }

// normCDF — standard normal CDF via erf (context p-value only; the CS is the verdict).
func normCDF(z float64) float64 { return 0.5 * (1 + math.Erf(z/math.Sqrt2)) }

// meanSD reduces an independent observation vector.
func meanSD(xs []float64) (n int, mean, sd float64) {
	n = len(xs)
	if n == 0 {
		return
	}
	for _, x := range xs {
		mean += x
	}
	mean /= float64(n)
	if n > 1 {
		var ss float64
		for _, x := range xs {
			ss += (x - mean) * (x - mean)
		}
		sd = math.Sqrt(ss / float64(n-1))
	}
	return
}

// verdictMarketEvidence keeps the economic receipt count separate from the independent market
// count used by confidence. Multiple lots/attempts on one venue+ticker are averaged first; rows
// without a durable market identity remain visible as receipts but cannot manufacture proof.
type verdictMarketEvidence struct {
	Rows    int
	Markets map[string][]float64
}

func (e *verdictMarketEvidence) Add(key string, value float64) {
	e.Rows++
	key = strings.ToLower(strings.TrimSpace(key))
	if key == "" || math.IsNaN(value) || math.IsInf(value, 0) {
		return
	}
	if e.Markets == nil {
		e.Markets = map[string][]float64{}
	}
	e.Markets[key] = append(e.Markets[key], value)
}

func (e verdictMarketEvidence) Means() []float64 {
	out := make([]float64, 0, len(e.Markets))
	for _, rows := range e.Markets {
		if n, mean, _ := meanSD(rows); n > 0 {
			out = append(out, mean)
		}
	}
	return out
}

func verdictFromMarketEvidence(family, group, unit string, e verdictMarketEvidence, feePC float64) verdictEnt {
	n, mean, sd := meanSD(e.Means())
	v := verdictFrom(family, group, unit, n, mean, sd, feePC, 0)
	v.Markets, v.SettledRows = n, e.Rows
	return v
}

// connectedOutcomeMeans collapses rows connected by any shared canonical resolution key into one
// block mean. A chain A-B, B-C is one block; combo permutations and overlapping baskets therefore
// cannot manufacture independent proof. Raw receipt rows remain separately visible.
func connectedOutcomeMeans(values []float64, groups [][]string) []float64 {
	if len(values) == 0 || len(values) != len(groups) {
		return nil
	}
	parent := make([]int, len(values))
	for i := range parent {
		parent[i] = i
	}
	var find func(int) int
	find = func(x int) int {
		if parent[x] != x {
			parent[x] = find(parent[x])
		}
		return parent[x]
	}
	union := func(a, b int) {
		ra, rb := find(a), find(b)
		if ra != rb {
			parent[rb] = ra
		}
	}
	owner := map[string]int{}
	for i, keys := range groups {
		for _, raw := range keys {
			key := strings.TrimSpace(raw)
			if key == "" {
				continue
			}
			if prior, ok := owner[key]; ok {
				union(i, prior)
			} else {
				owner[key] = i
			}
		}
	}
	type cell struct {
		sum float64
		n   int
	}
	blocks := map[int]*cell{}
	for i, value := range values {
		root := find(i)
		if blocks[root] == nil {
			blocks[root] = &cell{}
		}
		blocks[root].sum += value
		blocks[root].n++
	}
	out := make([]float64, 0, len(blocks))
	for _, block := range blocks {
		out = append(out, block.sum/float64(block.n))
	}
	return out
}

// computeExperimentVerdicts gathers every running experiment and classifies it. Cached ~60s.
func (s *Server) computeExperimentVerdicts(ctx context.Context) []verdictEnt {
	s.verdMu.Lock()
	if time.Since(s.verdAt) < time.Minute && s.verdCache != nil {
		out := append([]verdictEnt(nil), s.verdCache...)
		s.verdMu.Unlock()
		return out
	}
	// Keep an immutable last-good snapshot. The signal and exact-route aggregates are independent
	// critical groups: a timeout in either query must not replace a healthy roster with an empty
	// partial result. Fresh successful zero rows still replace the group; only an actual error falls
	// back. This is deliberately in-memory and does not pretend a failed read is new evidence.
	prior := append([]verdictEnt(nil), s.verdCache...)
	s.verdMu.Unlock()
	// A restart has no in-memory last-good value. Seed it from the prior durable sweep before any
	// database work so one busy/cancelled cold-boot query cannot replace a healthy exact-route
	// roster with an unrelated partial group. Fresh successful groups below still replace their own
	// prior rows; the file is recovery evidence, never newer market data.
	if len(prior) == 0 {
		var disk struct {
			Experiments []verdictEnt `json:"experiments"`
		}
		if s.readJSONLoose(filepath.Join(s.cfg().DataDir, "verdicts.json"), &disk) {
			prior = append(prior, disk.Experiments...)
		}
	}

	var out []verdictEnt
	add := func(v verdictEnt) {
		if v.N > 0 {
			out = append(out, v)
			// INVERT twin: log-only counterfactual — same rows, sign flipped, full round-trip drag
			// off (R118: 2×fee + spread haircut — the R116 "spread-free upper bound" is retired;
			// the twin's mean is now the honest cost-model edge, exactly v.Invert).
			if v.Invert > 0 && !strings.HasPrefix(strings.ToLower(strings.TrimSpace(v.Family)), "invert:") {
				// `invert:<family>` is reserved for the independently collected executable system.
				// This row is only same-row arithmetic and must never share its namespace, stable key,
				// promotion identity, or Paper state with that real route.
				inv := verdictFrom("counterfactual:invert:"+v.Family, "invert", v.Unit, v.N,
					-v.Mean-invertDrag(v.FeePC, v.InvHC), v.SD, v.FeePC, v.InvHC)
				inv.EvidenceTier = "counterfactual_same_row_arithmetic"
				inv.FillConditioned, inv.ProfitEvidence, inv.LiveAuthorizes = false, false, false
				inv.VoidReason = "same-row sign flip with modeled drag; no opposite-side exchange order or fill"
				inv.Locked = v.Locked // R116: an unbettable family's twin is just as unbettable
				inv.Markets, inv.SettledRows = v.Markets, v.SettledRows
				inv.SourceFamily, inv.Platform, inv.OriginLayer, inv.Route, inv.Side =
					v.SourceFamily, v.Platform, v.OriginLayer, v.Route, v.Side
				inv.KSh, inv.PSh = v.KSh, v.PSh // R130: same rows, same venues
				if len(v.Venues) > 0 {
					inv.Venues = map[string]venueVerdict{}
					for venue, vv := range v.Venues {
						iv := verdictFrom(inv.Family, "invert", v.Unit, vv.N,
							-vv.Mean-invertDrag(vv.FeePC, vv.InvHC), vv.SD, vv.FeePC, vv.InvHC)
						iv.Markets, iv.SettledRows = vv.Markets, vv.SettledRows
						inv.Venues[venue] = venueVerdictOf(iv)
					}
				}
				out = append(out, inv)
			}
		}
	}
	preserveGroups := func(groups ...string) {
		keep := make(map[string]bool, len(groups))
		for _, group := range groups {
			keep[group] = true
		}
		for _, v := range prior {
			if !keep[v.Group] {
				continue
			}
			if v.Group == "signal" {
				add(v) // re-derive (rather than duplicate) any diagnostic invert twin
			} else {
				out = append(out, v)
			}
		}
	}

	// 1) signal_log families (xvgap, freshfade, wxedge, kflow, meanrev, …) — trailing 90d.
	// Exact venue/side/route economics are the money-decision roster, so build them before the
	// much larger detector-history aggregates. The old order let a cold-boot signal scan consume
	// the lane deadline; UnitTrialStatsAt then inherited a cancelled context and the suite replaced
	// thousands of valid routes with a book-only snapshot. Relationship diagnostics remain on the
	// full leaderboard, but the route snapshot uses the fast contract-cell query because n is one
	// distinct venue+ticker contract and canonical clusters do not authorize this Paper point.
	if units, err := s.store.UnitTrialExecutionStatsAt(ctx, time.Now().UTC()); err == nil {
		for _, u := range units {
			if u.N > 0 && u.SettledMarkets > 0 {
				out = append(out, unitTrialVerdict(u))
			}
		}
	} else {
		preserveGroups("taker", "strategy")
	}

	cut := time.Now().AddDate(0, 0, -90)
	venueCells := map[string]map[string]venueVerdict{}
	cells, signalErr := s.store.FamilyVenueEdgeStats(ctx, cut)
	if signalErr == nil {
		for _, c := range cells {
			vv := verdictFrom(c.Family, "signal", "$/contract", c.N, c.Mean, c.SD, c.FeePC, c.InvHaircutPC)
			vv.Markets, vv.SettledRows = c.N, c.Rows
			if venueCells[c.Family] == nil {
				venueCells[c.Family] = map[string]venueVerdict{}
			}
			venueCells[c.Family][c.Platform] = venueVerdictOf(vv)
		}
	}
	var fams []storage.FamilyEdge
	if signalErr == nil {
		fams, signalErr = s.store.FamilyEdgeStats(ctx, cut)
	}
	if signalErr == nil {
		for _, f := range fams {
			v := verdictFrom(f.Family, "signal", "$/contract", f.N, f.Mean, f.SD, f.FeePC, f.InvHaircutPC)
			v.Markets, v.SettledRows = f.N, f.Rows
			// R116 VENUE-LOCKED: detections priced on poly-int (platform 'polymarket') can NEVER be
			// executed — we hold no rail there. The verdict stays (it grades the signal), but the
			// panel/state file tag it so nobody arms a family whose venue we cannot bet.
			v.Locked = f.PolyShare > 0.5
			v.KSh, v.PSh = vRnd4(f.KalShare), vRnd4(f.PusShare) // R130: follower venue membership
			v.Venues = venueCells[f.Family]
			add(v)
		}
	} else {
		preserveGroups("signal")
	}
	// Prospective taker route: exact executable asks, no paper allocation/bankroll gate. These rows
	// deliberately stand beside (not replace) each detector strategy: one answers whether the idea
	// predicts; this one answers whether crossing the actual book leaves money after fees. Return/$
	// and capital-day live on /api/unittrials; the scoreboard's common unit is ¢/contract.
	// 2) the four kfBook experiments (RawFlow / Weather / kflow pre / kflow live) — per-round $
	// from the retained closed tail (aggregates keep lifetime history; variance needs the rows).
	// R133: maker route evidence is separate from executable-ask/taker evidence. Its canonical
	// family+venue rows include canceled attempts as zero-PnL observations.
	out = append(out, s.makerRouteVerdicts(ctx)...)

	bookObsWhere := func(closed []kfClosed, accept func(kfPos) bool) verdictMarketEvidence {
		var evidence verdictMarketEvidence
		for _, c := range closed {
			if accept != nil && !accept(c.kfPos) {
				continue
			}
			platform := strings.ToLower(strings.TrimSpace(fiLotPlatform(c.kfPos)))
			evidence.Add(platform+"|"+strings.TrimSpace(c.Ticker), c.PnL)
		}
		return evidence
	}
	s.rfBookMu.Lock()
	rfClosed := fundedPaperClosedSnapshot(s.rfLoadLocked())
	s.rfBookMu.Unlock()
	s.wxBookMu.Lock()
	wxClosed := fundedPaperClosedSnapshot(s.wxBookLoadLocked())
	s.wxBookMu.Unlock()
	s.kfBookMu.Lock()
	kb := s.kfLoadLocked()
	kfPreClosed, kfLiveClosed := fundedPaperClosedSnapshot(&kb.Pre), fundedPaperClosedSnapshot(&kb.Live)
	s.kfBookMu.Unlock()
	// R117: the FreshInv book's OWN settled lots — the promotion pipeline's grow/retire rules
	// read exactly this family ("book:freshinv", the executor registry's BookFamily).
	s.fiBookMu.Lock()
	fiClosed := fundedPaperClosedSnapshot(s.fiLoadLocked())
	s.fiBookMu.Unlock()
	s.xvgBookMu.Lock()
	xvgClosed := fundedPaperClosedSnapshot(s.xvgLoadLocked())
	s.xvgBookMu.Unlock()
	s.flBookMu.Lock()
	flClosed := fundedPaperClosedSnapshot(s.flLoadLocked())
	s.flBookMu.Unlock()
	s.cbBookMu.Lock()
	cbB := s.cbLoadLocked()
	cbKClosed, cbPClosed := fundedPaperClosedSnapshot(&cbB.K), fundedPaperClosedSnapshot(&cbB.P)
	s.cbBookMu.Unlock()
	s.gfBookMu.Lock()
	gfClosed := make(map[string][]kfClosed, len(s.gfLoadLocked().Subs))
	for k, b := range s.gfLoadLocked().Subs {
		gfClosed[k] = fundedPaperClosedSnapshot(b)
	}
	s.gfBookMu.Unlock()

	allClosed := [][]kfClosed{rfClosed, wxClosed, kfPreClosed, kfLiveClosed, fiClosed,
		xvgClosed, flClosed, cbKClosed, cbPClosed}
	for _, closed := range gfClosed {
		allClosed = append(allClosed, closed)
	}
	proofs, _ := s.fundedPaperImmutableProofs(ctx, fundedPaperLots(allClosed...))
	bookObs := func(closed []kfClosed) verdictMarketEvidence {
		return bookObsWhere(closed, proofs.accepts)
	}
	rf, wx := bookObs(rfClosed), bookObs(wxClosed)
	kfPre, kfLive := bookObs(kfPreClosed), bookObs(kfLiveClosed)
	fi := bookObs(fiClosed)
	var fiK, fiP verdictMarketEvidence
	for _, c := range fiClosed {
		if !proofs.accepts(c.kfPos) {
			continue
		}
		if fiLotPlatform(c.kfPos) == vbPolyus {
			fiP.Add("polyus|"+strings.TrimSpace(c.Ticker), c.PnL)
		} else {
			fiK.Add("kalshi|"+strings.TrimSpace(c.Ticker), c.PnL)
		}
	}
	xvg, fl := bookObs(xvgClosed), bookObs(flClosed)
	cbK, cbP := bookObs(cbKClosed), bookObs(cbPClosed)
	for _, e := range []struct {
		name string
		obs  verdictMarketEvidence
	}{{"rawflow", rf}, {"weather", wx}, {"kflow-pre", kfPre}, {"kflow-live", kfLive},
		{"book:freshinv", fi}, {"book:freshinv-k", fiK}, {"book:freshlist-p", fiP},
		{"book:xvgap", xvg}, {"book:favlong80", fl}, {"book:cheapband-k", cbK}, {"book:cheapband-p", cbP}} {
		add(verdictFromMarketEvidence(e.name, "book", "$/round", e.obs, 0.05)) // ~5¢/round maker drag estimate; default 2¢ invert haircut
	}
	// R130: every follower sub grades as its own book family ("gf:<family>-k"/"-p") — the honest
	// self-measurement that takes over the roster weight from the seeded family ¢.
	gfObs := map[string]verdictMarketEvidence{}
	for k, closed := range gfClosed {
		gfObs[k] = bookObs(closed)
	}
	gfKeys := make([]string, 0, len(gfObs))
	for k := range gfObs {
		gfKeys = append(gfKeys, k)
	}
	sort.Strings(gfKeys)
	for _, k := range gfKeys {
		add(verdictFromMarketEvidence(k, "book", "$/round", gfObs[k], 0.05))
	}

	// 2b) R125 lock ledger — realized $ per 1-contract lock (both legs at first-sight asks, each
	// leg graded by its OWN venue's settlement; ~$0+margin by construction UNLESS the venues'
	// settlement rules disagree — mismatches also fire the xvlock audit alarm). R133: a class proven
	// settlement-incompatible remains in the ledger/audit but is quarantined from strategy proof.
	var xvl, lks verdictMarketEvidence
	s.xvlMu.Lock()
	for _, op := range s.xvlLoadLocked().Closed {
		if !xvlProofEligible(op) {
			continue
		}
		legs := []string{
			strings.ToLower(strings.TrimSpace(op.AVenue)) + "|" + strings.TrimSpace(op.AID),
			strings.ToLower(strings.TrimSpace(op.BVenue)) + "|" + strings.TrimSpace(op.BID),
		}
		sort.Strings(legs)
		marketKey := strings.Join(legs, "+")
		xvl.Add(marketKey, op.RealizedC/100)
		if op.Staked > 0 { // R129: the depth-verified STAKED slice grades as its own family
			lks.Add(marketKey, op.stakedNetUSD()/op.Staked)
		}
	}
	s.xvlMu.Unlock()
	add(verdictFromMarketEvidence("xvlock", "book", "$/lock", xvl, 0))
	add(verdictFromMarketEvidence("lockstack", "book", "$/lock", lks, 0))

	// 2c) R126/R127 combo ledger — per-contract fee-net PnL of settled 2-leg correlated combos,
	// split by EXPRESSION tag (R127 item 5): product-sim (and legacy untagged) rows grade under
	// the original "combo-overlay" family; the Combos book's money lots grade under "combo-synth"
	// (leg-stack) and "combo-rfq" (venue-quote, n≈0 expected). Semantics per family unchanged.
	var xvc, xvcSyn, xvcRfq verdictMarketEvidence
	s.xvcMu.Lock()
	for _, cp := range s.xvcLoadLocked().Closed {
		if cp.Contracts <= 0 {
			continue
		}
		legs := []string{strings.TrimSpace(cp.LegA), strings.TrimSpace(cp.LegB)}
		sort.Strings(legs)
		marketKey := "kalshi|" + strings.Join(legs, "+")
		switch cp.Expr {
		case xvcExprSynth:
			xvcSyn.Add(marketKey, cp.PnL/cp.Contracts)
		case xvcExprRFQ:
			xvcRfq.Add(marketKey, cp.PnL/cp.Contracts)
		default: // "" (legacy) + product-sim — the original study family
			xvc.Add(marketKey, cp.PnL/cp.Contracts)
		}
	}
	s.xvcMu.Unlock()
	add(verdictFromMarketEvidence("combo-overlay", "book", "$/combo-ct", xvc, 0))
	add(verdictFromMarketEvidence("combo-synth", "book", "$/combo-ct", xvcSyn, 0))
	add(verdictFromMarketEvidence("combo-rfq", "book", "$/combo-ct", xvcRfq, 0))

	// 3) ML paper book — per-contract fee-net pnl on epoch closes (reset_close marks excluded).
	// Repeated lots in one venue+ticker are averaged before confidence; raw lots remain visible.
	var mlObs []float64
	var pf struct {
		Closed []struct {
			PnL        *float64 `json:"pnl"`
			Contracts  float64  `json:"contracts"`
			Ticker     string   `json:"ticker"`
			Platform   string   `json:"platform"`
			Model      string   `json:"model_cohort"`
			ResetClose any      `json:"reset_close"`
		} `json:"closed"`
	}
	type mlMarketCell struct {
		sum float64
		n   int
	}
	mlMarkets := map[string]*mlMarketCell{}
	if s.readJSONLoose(filepath.Join(s.cfg().DataDir, "ml_paper.json"), &pf) {
		for _, c := range pf.Closed {
			if c.PnL != nil && c.Contracts > 0 && c.ResetClose == nil && c.Model == currentMLCohort {
				value := *c.PnL / c.Contracts
				mlObs = append(mlObs, value)
				if ticker := strings.TrimSpace(c.Ticker); ticker != "" {
					key := strings.ToLower(strings.TrimSpace(c.Platform)) + "|" + ticker
					cell := mlMarkets[key]
					if cell == nil {
						cell = &mlMarketCell{}
						mlMarkets[key] = cell
					}
					cell.sum += value
					cell.n++
				}
			}
		}
	}
	mlMarketMeans := make([]float64, 0, len(mlMarkets))
	for _, cell := range mlMarkets {
		mlMarketMeans = append(mlMarketMeans, cell.sum/float64(cell.n))
	}
	n, m, sd := meanSD(mlMarketMeans)
	mlVerdict := verdictFrom("ml-book", "book", "$/contract", n, m, sd, 0.01, 0)
	mlVerdict.Markets, mlVerdict.SettledRows = n, len(mlObs)
	add(mlVerdict)

	// 4) settled maker fills (the maker-vs-taker tag family).
	if markets, rows, mm, msd, err := s.store.MakerSettledMarketEdge(ctx); err == nil && markets > 0 {
		v := verdictFrom("maker-fills", "maker", "$/contract", markets, mm, msd, 0.0, 0)
		v.Markets, v.SettledRows = markets, rows
		add(v)
	}

	// 5) RFQ would-quote sim — adverse-fill $ per graded quote (in-memory tail, PnLAdverse doctrine).
	s.wqMu.Lock()
	var wq verdictMarketEvidence
	for _, g := range s.wqGrades {
		if v, ok := g["pnl_adverse"].(float64); ok {
			ticker, _ := g["ticker"].(string)
			wq.Add("kalshi|"+strings.TrimSpace(ticker), v)
		}
	}
	s.wqMu.Unlock()
	add(verdictFromMarketEvidence("rfq-sim", "rfq", "$/quote", wq, 0))

	// 6) Combo Lab — current immutable all-in-v2 receipts only. The retired JSONL fields used a
	// bare-$1 denominator and could report impossible losses below -100%; they are never proof.
	// Confidence uses connected resolution-block means, while raw receipt rows and unique market
	// keys remain visible as separate denominators.
	if receipts, err := s.store.PlabGradeReceipts(ctx); err == nil {
		type comboEvidence struct {
			values []float64
			groups [][]string
		}
		byLegs := map[int]*comboEvidence{}
		for _, receipt := range receipts {
			legs := int(receipt.NLegs)
			if legs < 2 || legs > parlayLegLimit || len(receipt.IndependenceKeys) == 0 {
				continue
			}
			cell := byLegs[legs]
			if cell == nil {
				cell = &comboEvidence{}
				byLegs[legs] = cell
			}
			value := receipt.RealizedReturn
			if receipt.MVEValid {
				value = receipt.RealizedMVEReturn
			}
			cell.values = append(cell.values, value)
			cell.groups = append(cell.groups, append([]string(nil), receipt.IndependenceKeys...))
		}
		for legs := 2; legs <= parlayLegLimit; legs++ {
			cell := byLegs[legs]
			if cell == nil {
				continue
			}
			blocks := connectedOutcomeMeans(cell.values, cell.groups)
			n, mean, sd := meanSD(blocks)
			v := verdictFrom(fmt.Sprintf("parlay-%dleg", legs), "parlay", "$/$1", n, mean, sd, 0.02, 0)
			v.Markets, v.SettledRows = plabUniqueMarkets(cell.groups), len(cell.values)
			add(v)
		}
	}

	sort.Slice(out, func(i, j int) bool { return out[i].N > out[j].N })
	s.verdMu.Lock()
	s.verdCache, s.verdAt = out, time.Now()
	s.verdMu.Unlock()
	return out
}

// sweepVerdicts — the engine's 2-min pass: recompute, persist the auditor state file, and audit
// every state transition immediately. Phone notifications are durably collected into one
// 15-minute batch so a cluster of related verdict changes cannot spam the operator.
func (s *Server) sweepVerdicts(ctx context.Context) {
	raw := s.computeExperimentVerdicts(ctx)
	vs := operatorVerdicts(raw)
	if len(vs) == 0 {
		return
	}
	s.scoreWeights(ctx) // R128: refresh the scoreboard-weight allocation table on the same cadence
	// auditor-readable state file (edge_snapshot.json pattern: overwrite each pass)
	if b, err := json.MarshalIndent(map[string]any{"ts": time.Now().UTC().Format(time.RFC3339),
		"method":      "confidence sequence over each row's stated observation class; simulated or assumed fills remain research-only and are not exchange profit evidence",
		"states":      "RESEARCH+/RESEARCH-/RESEARCH-COLLECTING describe modeled or simulated observations only; PROVEN labels require an authenticated fill-conditioned exchange cohort",
		"experiments": vs}, "", " "); err == nil {
		_ = os.WriteFile(filepath.Join(s.cfg().DataDir, "verdicts.json"), b, 0o644)
	}
	var changed []verdictTelegramChange
	for _, v := range vs {
		key := "verdict_state:" + verdictStableKey(v)
		rawPrevious, _ := s.store.KVGet(ctx, key)
		previous, _ := decodeVerdictStateReceipt(rawPrevious)
		next := encodeVerdictStateReceipt(v)
		if previous.State == v.State {
			// Upgrade old plain-state receipts silently. A storage-format migration is not a market
			// verdict, but recording the proof method makes future method changes distinguishable.
			if strings.TrimSpace(rawPrevious) != next {
				_ = s.store.KVSet(ctx, key, next)
			}
			continue
		}
		_ = s.store.KVSet(ctx, key, next)
		if !v.ProfitEvidence {
			// Keep research transitions in the local audit trail without sending a phone alert that
			// could be mistaken for cash-quality proof or a promotion instruction.
			_ = s.store.Audit(ctx, "info", "verdicts", verdictTransitionLine(v), "")
			continue
		}
		if previous.State == "" && v.State == "COLLECTING" {
			continue // first sighting of a still-undecided experiment: no noise
		}
		change := verdictTelegramChangeFor(previous, v)
		if change.Kind == "proof-method-reset" {
			line := fmt.Sprintf("System evidence reset: %s %s -> COLLECTING; proof n=0; %d old settled contract markets lack frozen entry-time event identity and remain excluded; method=%s",
				verdictDisplayName(v), previous.State, v.UnclusteredMarkets, verdictProofMethod(v))
			_ = s.store.Audit(ctx, "info", "verdicts", line, "")
			changed = append(changed, change)
			continue
		}
		if v.Locked {
			change.Line += " — VENUE-LOCKED: measured at poly-int prices we cannot bet; log-only, never arm"
		}
		if v.State == "PROVEN-" {
			change.Line += " — " + s.invertVerdictStatus(v)
		}
		_ = s.store.Audit(ctx, "info", "verdicts", change.Line, "")
		changed = append(changed, change)
	}
	s.queueVerdictTelegramBatch(ctx, changed, time.Now().UTC())
}

func legacyVerdictLineKey(line string, index int) string {
	plain := trimVerdictLineLabel(line)
	if name, _, ok := strings.Cut(plain, " is now "); ok && strings.TrimSpace(name) != "" {
		return "legacy:" + strings.TrimSpace(name)
	}
	return fmt.Sprintf("legacy:%d", index)
}

// trimVerdictLineLabel accepts already queued pre-R145 messages while all newly emitted operator
// text uses System terminology. Keeping this compatibility at the queue boundary avoids dropping
// a durable verdict transition solely because its display label changed.
func trimVerdictLineLabel(line string) string {
	line = strings.TrimSpace(line)
	line = strings.TrimSpace(strings.TrimPrefix(line, "System result:"))
	line = strings.TrimSpace(strings.TrimPrefix(line, "Experiment verdict:"))
	return line
}

func sanitizeLegacyVerdictLine(line string) string {
	if !strings.Contains(line, "9999") {
		return line
	}
	if start := strings.Index(line, "95% range "); start >= 0 {
		if tail := strings.Index(line[start:], ")"); tail >= 0 {
			end := start + tail
			return line[:start] + "95% range unavailable" + line[end:]
		}
	}
	return strings.ReplaceAll(strings.ReplaceAll(line, "-9999.000..+9999.000", "unavailable"),
		"-9999..+9999", "unavailable")
}

func (b *verdictTelegramBatch) normalize() {
	if b.Entries == nil {
		b.Entries = map[string]verdictTelegramChange{}
	}
	// Migrate an already queued v1 string batch. Unbounded n=0 lines are proof-reset noise, so
	// retain their count but fold them into the single reset summary instead of leaking sentinels.
	for i, line := range b.Lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		key := legacyVerdictLineKey(line, i)
		change := verdictTelegramChange{Key: key, Kind: "verdict", Line: sanitizeLegacyVerdictLine(line)}
		if strings.Contains(line, "n=0") && strings.Contains(line, "9999") {
			change.Kind, change.Line = "proof-method-reset", ""
		}
		b.Entries[key] = change
	}
	b.Lines = nil
}

func (b *verdictTelegramBatch) merge(changes []verdictTelegramChange) {
	b.normalize()
	for i, change := range changes {
		key := strings.TrimSpace(change.Key)
		if key == "" {
			key = fmt.Sprintf("anonymous:%d", i)
		}
		change.Key = key
		b.Entries[key] = change // latest state wins inside the 15-minute phone window
	}
	if len(b.Entries) <= 200 {
		return
	}
	keys := make([]string, 0, len(b.Entries))
	for key := range b.Entries {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys[:len(keys)-200] {
		delete(b.Entries, key)
	}
}

func (s *Server) queueVerdictTelegramBatch(ctx context.Context, changed []verdictTelegramChange, now time.Time) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	var batch verdictTelegramBatch
	if raw, ok := s.store.KVGet(ctx, verdictTelegramBatchKV); ok && strings.TrimSpace(raw) != "" {
		_ = json.Unmarshal([]byte(raw), &batch)
	}
	started, err := time.Parse(time.RFC3339Nano, batch.Started)
	if err != nil || started.After(now) {
		started = now
		batch.Started = started.Format(time.RFC3339Nano)
	}
	batch.merge(changed)
	if now.Sub(started) < verdictTelegramBatchWindow || len(batch.Entries) == 0 {
		if encoded, err := json.Marshal(batch); err == nil {
			_ = s.store.KVSet(ctx, verdictTelegramBatchKV, string(encoded))
		}
		return
	}
	s.TgSend(verdictTelegramBatchMessage(batch.Entries))
	_ = s.store.KVSet(ctx, verdictTelegramBatchKV, `{"started":"`+now.Format(time.RFC3339Nano)+`","entries":{}}`)
}

func verdictTelegramBatchMessage(entries map[string]verdictTelegramChange) string {
	keys := make([]string, 0, len(entries))
	for key := range entries {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	resetCount, resetMarkets := 0, 0
	var lines []string
	for _, key := range keys {
		change := entries[key]
		if change.Kind == "proof-method-reset" {
			resetCount++
			resetMarkets += change.Markets
			continue
		}
		if line := strings.TrimSpace(change.Line); line != "" {
			lines = append(lines, line)
		}
	}
	var b strings.Builder
	b.WriteString(fmt.Sprintf("⚙️ System results · %d change", len(entries)))
	if len(entries) != 1 {
		b.WriteString("s")
	}
	b.WriteString(" in the last 15 minutes\n")
	shown := 0
	if resetCount > 0 {
		marketText := ""
		if resetMarkets > 0 {
			marketText = fmt.Sprintf("; %d old settled route-market cells remain visible but excluded", resetMarkets)
		}
		candidate := fmt.Sprintf("• %d exact routes reset to prospective evidence: old rows lacked entry-time event identity%s; collecting new independent outcomes\n",
			resetCount, marketText)
		if b.Len()+len(candidate) <= 3600 {
			b.WriteString(candidate)
			shown += resetCount
		}
	}
	for _, line := range lines {
		line = trimVerdictLineLabel(line)
		candidate := "• " + line + "\n"
		if b.Len()+len(candidate) > 3600 {
			break
		}
		b.WriteString(candidate)
		shown++
	}
	if shown < len(entries) {
		b.WriteString(fmt.Sprintf("• %d more audited changes are on the Systems dashboard", len(entries)-shown))
	}
	return strings.TrimSpace(b.String())
}

// inversePaperRuntime reports wiring truth separately from economic evidence. A positive exact
// route is eligible evidence; it is not an "active" strategy until the named executor ledger has
// accepted at least one actual Paper lot.
func (s *Server) inversePaperRuntime(v verdictEnt) (string, string) {
	family := strings.ToLower(strings.TrimSpace(v.SourceFamily))
	platform := strings.ToLower(strings.TrimSpace(v.Platform))
	side := strings.ToUpper(strings.TrimSpace(v.Side))
	if family == "" || platform == "" || (side != "YES" && side != "NO") {
		return "PAPER-BLOCKED", "receiving=false; executable=false; missing exact family/venue/side identity; LIVE armed=false (sealed)"
	}
	if s.ksBlocked() {
		return "PAPER-BLOCKED", "receiving=true; executable=false; kill switch is tripped; LIVE armed=false (sealed)"
	}
	if gfDedicatedExpression(family, platform) {
		if s.fiBookPoisoned.Load() {
			return "PAPER-BLOCKED", "receiving=true; executable=false; dedicated ledger is unreadable; LIVE armed=false (sealed)"
		}
		if !s.freshInvActive() {
			return "PAPER-PAUSED", "receiving=true; executable=per-candidate preflight; dedicated Paper executor is not enabled; LIVE armed=false (sealed)"
		}
		s.fiBookMu.Lock()
		b := s.fiLoadLocked()
		open, closed := 0, 0
		for _, p := range b.Open {
			pPlatform := strings.ToLower(strings.TrimSpace(p.Platform))
			if pPlatform == "" {
				pPlatform = "kalshi"
			}
			if pPlatform == platform && strings.EqualFold(p.Side, side) {
				open++
			}
		}
		for _, p := range b.Closed {
			pPlatform := strings.ToLower(strings.TrimSpace(p.Platform))
			if pPlatform == "" {
				pPlatform = "kalshi"
			}
			if pPlatform == platform && strings.EqualFold(p.Side, side) {
				closed++
			}
		}
		s.fiBookMu.Unlock()
		if open+closed > 0 {
			return "PAPER-ACTIVE", fmt.Sprintf("receiving=true; executable=complete-book preflight; paper-enabled=true; named lots open=%d closed=%d; LIVE armed=false (sealed)", open, closed)
		}
		return "PAPER-ENABLED", "receiving=true; executable=per-candidate complete-book preflight; paper-enabled=true; no named lot accepted yet; LIVE armed=false (sealed)"
	}
	if !s.genfollowOn() {
		return "PAPER-PAUSED", "receiving=true; executable=per-candidate preflight; generic Paper executor is disabled; LIVE armed=false (sealed)"
	}
	if s.gfBookPoisoned.Load() {
		return "PAPER-BLOCKED", "receiving=true; executable=false; generic Paper ledger is unreadable; LIVE armed=false (sealed)"
	}
	key := gfRosterKey(family, platform, side, v.Route)
	s.gfBookMu.Lock()
	b := s.gfLoadLocked().Subs[key]
	open, closed := 0, 0
	if b != nil {
		open, closed = len(b.Open), len(b.Closed)
	}
	s.gfBookMu.Unlock()
	if s.gfBookPoisoned.Load() {
		return "PAPER-BLOCKED", "receiving=true; executable=false; generic Paper ledger failed to load; LIVE armed=false (sealed)"
	}
	if share, ok := s.subShareUSD(key); !ok || share <= 0 {
		return "PAPER-PAUSED", fmt.Sprintf("receiving=true; executable=per-candidate complete-book preflight; allocator has no current positive capacity; named lots open=%d closed=%d; LIVE armed=false (sealed)", open, closed)
	}
	if open+closed > 0 {
		return "PAPER-ACTIVE", fmt.Sprintf("receiving=true; executable=complete-book preflight; paper-enabled=true; named lots open=%d closed=%d; LIVE armed=false (sealed)", open, closed)
	}
	return "PAPER-ENABLED", "receiving=true; executable=per-candidate complete-book preflight; paper-enabled=true; no named lot accepted yet; LIVE armed=false (sealed)"
}

// verdictSourceSystemFamily returns the system that owns an evidence row. Group describes the
// evidence/route (signal, taker, strategy, maker, book, RFQ, ...); it does NOT say whether the
// owning system has an execution path. SourceFamily is authoritative for exact route rows.
func verdictSourceSystemFamily(v verdictEnt) string {
	if family := strings.ToLower(strings.TrimSpace(v.SourceFamily)); family != "" {
		return family
	}
	family := strings.ToLower(strings.TrimSpace(v.Family))
	if strings.HasPrefix(family, "strategy:") {
		family = strings.TrimPrefix(family, "strategy:")
	}
	return sideSelectionBaseFamily(family)
}

func verdictInternalControlFamily(family string) bool {
	family = strings.ToLower(strings.TrimSpace(family))
	return strings.HasPrefix(family, "side-control:") || strings.HasPrefix(family, "control:")
}

// invertVerdictStatus gives every PROVEN- phone alert the decision the operator actually needs:
// the opposite-side edge AFTER both sides' fees and the crossing-spread haircut, plus the status
// of a separately measured opposite-side system when one applies. Evidence group names never
// decide execution capability. A counterfactual/control is never described as running money, and
// no result bypasses the existing Paper proof, current-book, ARM, or LIVE AUTO gates.
func (s *Server) invertVerdictStatus(v verdictEnt) string {
	unit := "unit"
	switch v.Unit {
	case "$/contract", "$/combo-ct":
		unit = "ct"
	case "$/round":
		unit = "bet"
	case "$/quote":
		unit = "quote"
	case "$/$1":
		unit = "$1"
	case "$/lock":
		unit = "lock"
	}
	family := verdictSourceSystemFamily(v)
	if v.Group == "invert" || strings.HasPrefix(family, "counterfactual:") {
		return "diagnostic counterfactual only; it has no order authority and cannot be recursively inverted; LIVE remains sealed"
	}
	if strings.HasPrefix(family, "invert:") {
		return fmt.Sprintf("named inverse %s is %+.1f¢/%s; recursive inversion is forbidden and its existing execution state is unchanged; LIVE remains sealed",
			family, v.Mean*100, unit)
	}
	inv := -v.Mean - invertDrag(v.FeePC, v.InvHC)
	prefix := fmt.Sprintf("fee/spread-adjusted inverted edge %+.1f¢/%s", inv*100, unit)
	if verdictInternalControlFamily(family) {
		return prefix + "; this is an opposite-side control, not an order-producing system; controls have no Paper/LIVE order authority"
	}
	if v.Locked {
		return prefix + "; the system is BLOCKED because the evidence is venue-locked/unbettable"
	}
	sideClass, _, _ := systemSideClassification(family)
	switch sideClass {
	case sideClassMulti:
		return prefix + "; single-leg inversion does not apply to this multi-leg system; any complementary position must own a separately quoted payoff-certified combo/RFQ route; LIVE remains sealed"
	case sideClassRouter:
		role := "router/filter"
		if family == "flow-direction-integrity" {
			role = "data guard"
		}
		return prefix + "; this " + role + " operates through an executable child system, so no standalone inverse order applies; LIVE remains sealed"
	}

	// A signal, taker route, or accepted strategy route can all belong to the same real system.
	// Search only independently collected exact opposite-book routes; never reuse this row's
	// algebraic complement or a side-control/counterfactual result.
	if family != "" {
		inverseFamily := "invert:" + family
		baseVenue := strings.ToLower(strings.TrimSpace(v.Platform))
		if baseVenue == "" {
			baseVenue = verdictRouteVenue(v)
		}
		baseSide := strings.ToUpper(strings.TrimSpace(v.Side))
		inverseSide := concreteOppositeSide(baseSide)
		baseRoute := strings.ToLower(strings.TrimSpace(v.Route))
		if baseRoute == "" && (v.Group == "maker" || v.Group == "taker") {
			baseRoute = v.Group
		}
		if baseRoute == "" {
			baseRoute = "taker"
		}
		s.verdMu.Lock()
		cache := append([]verdictEnt(nil), s.verdCache...)
		s.verdMu.Unlock()
		best, found := verdictEnt{}, false
		for _, route := range cache {
			routeVenue := strings.ToLower(strings.TrimSpace(route.Platform))
			if routeVenue == "" {
				routeVenue = verdictRouteVenue(route)
			}
			routeSide := strings.ToUpper(strings.TrimSpace(route.Side))
			routeKind := strings.ToLower(strings.TrimSpace(route.Route))
			if routeKind == "" && (route.Group == "maker" || route.Group == "taker") {
				routeKind = route.Group
			}
			// An inverse result is usable only for the exact venue, opposite side and maker/taker
			// route implied by this alert. Unknown identity fails closed instead of borrowing the
			// best-looking cell from another venue/side/route.
			if strings.ToLower(strings.TrimSpace(route.SourceFamily)) != inverseFamily ||
				(baseVenue == "" || routeVenue != baseVenue) ||
				(inverseSide == "" || routeSide != inverseSide) || routeKind != baseRoute ||
				(route.Group != "taker" && route.Group != "maker" && route.Group != "strategy") ||
				route.N <= 0 {
				continue
			}
			if !found || route.Mean > best.Mean {
				best, found = route, true
			}
		}
		if found && best.Mean > 0 {
			seed := "its own exact opposite-book history"
			if best.SeedSource != "" {
				seed = "the exact pre-R144 opposite-book ledger (temporary seed until the named route settles)"
			}
			runtime, detail := s.inversePaperRuntime(best)
			return fmt.Sprintf("%s; independent %s [%s] is %s at %+.1f cents/ct from %s (n=%d); %s",
				prefix, inverseFamily, best.Side, runtime, best.Mean*100, seed, best.N, detail)
		}
		if found {
			return fmt.Sprintf("%s; independent %s is COLLECTING/PAUSED because its own exact-book result is %+.1f cents/ct (n=%d); LIVE remains sealed",
				prefix, inverseFamily, best.Mean*100, best.N)
		}
	}
	if inv <= 0 {
		if family != "" {
			return prefix + "; independent invert:" + family + " remains COLLECTING only because the diagnostic direct-row transform does not clear costs"
		}
		return prefix + "; no inverse system is activated because the diagnostic direct-row transform does not clear costs"
	}
	if family == "" {
		return prefix + "; no inverse system identity is available; LIVE remains sealed"
	}
	return prefix + "; independent invert:" + family + " system is COLLECTING its own actual opposite asks/depth/fees/settlements; Paper activates only from its own positive exact route and LIVE remains sealed"
}

// handleVerdicts — GET /api/verdicts: the Experiments panel + auditor endpoint.
func (s *Server) handleVerdicts(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	vs := operatorVerdicts(s.computeExperimentVerdicts(ctx))
	depthNow, _ := s.store.MakerDepthSrcCounts(ctx, time.Now().Add(-24*time.Hour))
	writeJSON(w, http.StatusOK, map[string]any{
		"method":          "confidence sequence over the stated observation class; research/simulation is not authenticated exchange profit evidence",
		"profit_contract": "only fill-conditioned authenticated exchange cohorts may use PROVEN labels or authorize cash",
		"systems":         vs,
		"experiments":     vs,
		"depth_src_24h":   depthNow, // Part 3 coverage measure rides along
	})
}
