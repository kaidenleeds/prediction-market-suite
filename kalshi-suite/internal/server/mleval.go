package server

// mleval.go — R123 Part 1: NEAR-ZERO RE-SCORING (operator: "as fast as possible, closest to 0
// seconds"). Held-lot p_win used to refresh only when the Python sidecar republished
// (~6-minute effective staleness). Now the sidecar exports its trained model after every
// retrain (data/ml_model_export.json: 3 CalibratedClassifierCV folds of boosted trees + each
// fold's calibrator) and the SIGNAL-SIDE feature vectors for every row that matters
// (data/ml_eval_vectors.json: held ML-book lots + actionable preds), and THIS file re-scores
// p_win in microseconds on every WS price tick — pure tree walks, no CGo, no network.
//
// PARITY IS THE HARD REQUIREMENT, in two honest layers (measured live 2026-07-09):
//   1. IMPLEMENTATION parity — the export carries up to 1,000 (feature-vector → the sidecar's
//      own FLAT evaluation) pairs; THIS loader REFUSES any export whose pairs don't replay
//      within 1e-6 (same arithmetic in Go lands ~1e-15). Go can never drift from the export.
//   2. MODEL-COPY fidelity — the sidecar measures |flat − predict_proba| on the same 1,000
//      real rows, publishes the distribution (median 0 · p95 2.0e-8 · p99 4.0e-8 · max 4.2e-6
//      on 3/1000 rows at steep isotonic boundaries — XGBoost's GPU/SIMD float32 margin order,
//      not portably reproducible) and refuses to publish beyond p99 2.5e-7 / max 5e-5.
// Arithmetic is pinned to the sidecar's _flat_pwin twin: float32 SEQUENTIAL leaf-sum for
// xgboost folds (float64 drifts to 1.4e-6 — measured), float64 sigmoid → float32 cast for
// proba-calibrated folds (sklearn feeds the calibrator XGBoost's float32 probability), raw
// margin for decision_function-calibrated folds (HGB), np.interp isotonic, folds averaged.
//
// FEATURE HONESTY (the hybrid, flagged per-feature in /api/mleval): Go patches only the live
// values the suite actually has (price, momentum family, spread, depth, horizon clocks) and
// recomputes the extra_* terms derived from them; every other feature stays FROZEN at the
// sidecar's last score (trader/flow/holder aggregates live only in the DB at signal time).
// The sidecar remains the trainer and ground truth: every vectors publish OVERWRITES the tick
// scores with the sidecar's own numbers, and two drift metrics ride /api/mleval + /api/ready:
//   - model_drift:   |Go eval of the sidecar's UNPATCHED vector − sidecar p_win| — the alarm
//                     metric (≈ parity ulps when export/version are in sync; growth = skew);
//   - refresh_delta: |tick-fresh score − sidecar's fresh score| — informational; this is the
//                     price/feature movement the tick re-scorer exists to capture.
//
// Consumers: mlPredPWin (r115.go) overlays tick-fresh scores → fill-time recheck + edge-died
// post pulls + mlMakerFill drift stamps; tickFreshPWin feeds the quarantine/border checks;
// enrichMLURLs stamps p_win_tick on the Top-Scored rows; GET /api/mleval is the telemetry.

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
	"sync"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

const (
	mlEvalExportFile = "ml_model_export.json"
	mlEvalVecsFile   = "ml_eval_vectors.json"
	mlEvalFreshFor   = 90 * time.Second       // a tick score older than this no longer overlays the file p_win
	mlEvalParityTol  = 1e-6                   // THE hard requirement (operator spec)
	mlEvalKeyRate    = 250 * time.Millisecond // per-key re-score rate limit on hot tickers
)

// ---- export format (must mirror live_ml.py _export_flat_model exactly) ----------------------

type mlEvalTree struct {
	F []int32   `json:"f"` // split feature index (unused at leaves)
	T []float64 `json:"t"` // split threshold; at leaves (L[i]<0) the leaf value
	L []int32   `json:"l"` // left child (-1 = leaf)
	R []int32   `json:"r"` // right child
}

type mlEvalCalib struct {
	Method string    `json:"method"` // isotonic | sigmoid
	X      []float64 `json:"x"`
	Y      []float64 `json:"y"`
	A      float64   `json:"a"`
	B      float64   `json:"b"`
}

type mlEvalFold struct {
	Cmp      string       `json:"cmp"`       // lt (xgb) | le (hgb)
	Acc      string       `json:"acc"`       // f32 | f64
	Bias     float64      `json:"bias"`      // logit(base_score) (xgb) | baseline_prediction (hgb)
	CalInput string       `json:"cal_input"` // proba | margin — WHAT sklearn fed the calibrator
	Calib    mlEvalCalib  `json:"calib"`
	Trees    []mlEvalTree `json:"trees"`
}

type mlEvalExport struct {
	Version       string       `json:"version"`
	FeatureSchema string       `json:"feature_schema"`
	GeneratedAt   int64        `json:"generated_at"`
	Backend       string       `json:"backend"`
	CalibMethod   string       `json:"calib_method"`
	NFeatures     int          `json:"n_features"`
	FeatureNames  []string     `json:"feature_names"`
	Folds         []mlEvalFold `json:"folds"`
	Parity        struct {
		N           int                `json:"n"`
		SelfMaxDiff float64            `json:"self_max_diff"`
		Fidelity    map[string]float64 `json:"fidelity_vs_predict_proba"` // R123: measured |flat−predict_proba| median/p95/p99/max
		Vectors     [][]float64        `json:"vectors"`
		PWin        []float64          `json:"pwin"` // the sidecar's own FLAT evaluations (implementation-parity ground truth)
	} `json:"parity"`
}

type mlEvalModel struct {
	version   string
	backend   string
	calib     string
	nf        int
	folds     []mlEvalFold
	idx       map[string]int // feature name → column
	parityN   int
	parityMax float64            // loader replay max |diff| vs the pairs (≤ 1e-6 or the model was refused)
	selfMax   float64            // the sidecar's own measured max |flat − predict_proba|
	fidelity  map[string]float64 // the full measured fidelity distribution (median/p95/p99/max)
	loadedAt  time.Time
}

// mlEvalVec is one sidecar-published SIGNAL-SIDE feature row (ml_eval_vectors.json).
type mlEvalVec struct {
	Ticker   string    `json:"ticker"`
	Side     string    `json:"side"`
	Platform string    `json:"platform"`
	PWin     float64   `json:"p_win"` // the sidecar's own fresh score — drift ground truth
	SigPrice float64   `json:"sig_price"`
	SigTS    int64     `json:"sig_ts"`
	CloseTS  int64     `json:"close_ts"`
	StartTS  int64     `json:"start_ts"`
	Held     bool      `json:"held"`
	X        []float64 `json:"x"`
}

type mlEvalScoreEnt struct {
	pwin    float64
	sidecar float64 // sidecar p_win from the vector row (ground truth at last publish)
	px      float64 // side price the score was computed at
	at      time.Time
	src     string // tick | load | sidecar
}

func mlEvalVectorVersionOK(model *mlEvalModel, vectorVersion string) bool {
	return model != nil && strings.TrimSpace(vectorVersion) != "" && vectorVersion == model.version
}

type mlEvalDriftRing struct {
	v     [256]float64
	n     int
	total int64
}

func (d *mlEvalDriftRing) add(x float64) {
	d.v[d.total%int64(len(d.v))] = x
	d.total++
	if d.n < len(d.v) {
		d.n++
	}
}

func (d *mlEvalDriftRing) stats() (n int, med, max float64) {
	if d.n == 0 {
		return 0, 0, 0
	}
	cp := make([]float64, d.n)
	copy(cp, d.v[:d.n])
	sort.Float64s(cp)
	for _, x := range cp {
		if x > max {
			max = x
		}
	}
	return d.n, cp[d.n/2], max
}

// mlEvalState — all R123 tick re-scorer state, one lock (hot path holds it only for map ops;
// tree walks run outside it on a copied vector).
type mlEvalState struct {
	mu           sync.Mutex
	model        *mlEvalModel
	modelMt      time.Time
	modelChk     time.Time
	modelBusy    bool
	lastErr      string
	lastErrAt    time.Time
	errAuditAt   time.Time
	vecs         map[string]*mlEvalVec // platform|ticker|SIDE → row
	byTicker     map[string][]string   // platform|ticker → keys
	vecsMt       time.Time
	vecsChk      time.Time
	vecsBusy     bool
	vecsGen      int64
	scores       map[string]mlEvalScoreEnt
	scoreAt      map[string]time.Time
	latNs        [256]int64
	latN         int
	latTotal     int64
	evalCount    int64
	mdrift       mlEvalDriftRing
	rdelta       mlEvalDriftRing
	driftWarnAt  time.Time
	loadAuditNew string // last version announced to the audit log (once per version)
}

// ---- evaluation core -------------------------------------------------------------------------

func (f *mlEvalFold) eval(x []float64) float64 {
	var margin float64
	if f.Acc == "f32" {
		var s float32
		for ti := range f.Trees {
			t := &f.Trees[ti]
			i := 0
			for t.L[i] >= 0 {
				if float32(x[t.F[i]]) < float32(t.T[i]) {
					i = int(t.L[i])
				} else {
					i = int(t.R[i])
				}
			}
			s += float32(t.T[i]) // SEQUENTIAL float32 accumulation — parity-pinned
		}
		margin = float64(s) + f.Bias
	} else {
		s := 0.0
		for ti := range f.Trees {
			t := &f.Trees[ti]
			i := 0
			for t.L[i] >= 0 {
				if x[t.F[i]] <= t.T[i] {
					i = int(t.L[i])
				} else {
					i = int(t.R[i])
				}
			}
			s += t.T[i]
		}
		margin = s + f.Bias
	}
	pin := margin // cal_input=margin (decision_function-calibrated folds, e.g. HGB)
	if f.CalInput != "margin" {
		// proba-calibrated folds see XGBoost's FLOAT32 probability — the cast is load-bearing
		pin = float64(float32(1.0 / (1.0 + math.Exp(-margin))))
	}
	return f.Calib.apply(pin)
}

// apply — np.interp semantics for isotonic (clip at end thresholds; y = slope·(p−x0)+y0),
// Platt for sigmoid. Pinned to live_ml.py _flat_calibrate.
func (c *mlEvalCalib) apply(p float64) float64 {
	if c.Method == "sigmoid" {
		return 1.0 / (1.0 + math.Exp(c.A*p+c.B))
	}
	x, y := c.X, c.Y
	n := len(x)
	if n == 0 {
		return p
	}
	if p <= x[0] {
		return y[0]
	}
	if p >= x[n-1] {
		return y[n-1]
	}
	j := sort.SearchFloat64s(x, p) // smallest j with x[j] >= p
	if j < n && x[j] == p {
		return y[j]
	}
	j-- // now the largest j with x[j] < p (bisect_right(x,p)-1 for strictly-increasing x)
	if j >= n-1 {
		return y[n-1]
	}
	slope := (y[j+1] - y[j]) / (x[j+1] - x[j])
	return slope*(p-x[j]) + y[j]
}

func (m *mlEvalModel) pwin(x []float64) float64 {
	tot := 0.0
	for fi := range m.folds {
		tot += m.folds[fi].eval(x)
	}
	return tot / float64(len(m.folds))
}

// mlEvalParse validates + parity-gates an export. Any pair replaying outside 1e-6 rejects the
// whole export (the operator's hard requirement) — the caller keeps the previous model.
func mlEvalParse(b []byte) (*mlEvalModel, error) {
	var e mlEvalExport
	if err := json.Unmarshal(b, &e); err != nil {
		return nil, fmt.Errorf("unmarshal: %w", err)
	}
	if e.FeatureSchema != mlBookFeatureSchema {
		return nil, fmt.Errorf("model feature schema %q is not %q; legacy export is reference-only", e.FeatureSchema, mlBookFeatureSchema)
	}
	if len(e.Folds) == 0 || e.NFeatures <= 0 || len(e.FeatureNames) != e.NFeatures {
		return nil, fmt.Errorf("malformed export: folds=%d nf=%d names=%d", len(e.Folds), e.NFeatures, len(e.FeatureNames))
	}
	for fi := range e.Folds {
		f := &e.Folds[fi]
		for ti := range f.Trees {
			t := &f.Trees[ti]
			n := len(t.T)
			if n == 0 || len(t.F) != n || len(t.L) != n || len(t.R) != n {
				return nil, fmt.Errorf("fold %d tree %d: ragged arrays", fi, ti)
			}
			for i := 0; i < n; i++ {
				if t.L[i] >= 0 && (int(t.L[i]) >= n || int(t.R[i]) >= n || t.F[i] < 0 || int(t.F[i]) >= e.NFeatures) {
					return nil, fmt.Errorf("fold %d tree %d node %d: out-of-range child/feature", fi, ti, i)
				}
			}
		}
	}
	m := &mlEvalModel{version: e.Version, backend: e.Backend, calib: e.CalibMethod,
		nf: e.NFeatures, folds: e.Folds, idx: make(map[string]int, e.NFeatures),
		parityN: e.Parity.N, selfMax: e.Parity.SelfMaxDiff, fidelity: e.Parity.Fidelity,
		loadedAt: time.Now()}
	for i, nm := range e.FeatureNames {
		m.idx[nm] = i
	}
	if e.Parity.N == 0 || len(e.Parity.Vectors) != e.Parity.N || len(e.Parity.PWin) != e.Parity.N {
		return nil, fmt.Errorf("export carries no parity pairs — refused (parity is the contract)")
	}
	for i := 0; i < e.Parity.N; i++ {
		if len(e.Parity.Vectors[i]) != e.NFeatures {
			return nil, fmt.Errorf("parity vector %d: width %d != %d", i, len(e.Parity.Vectors[i]), e.NFeatures)
		}
		d := math.Abs(m.pwin(e.Parity.Vectors[i]) - e.Parity.PWin[i])
		if d > m.parityMax {
			m.parityMax = d
		}
		if d > mlEvalParityTol {
			return nil, fmt.Errorf("PARITY FAIL: pair %d off by %.3e > 1e-6 (version %s) — export refused", i, d, e.Version)
		}
	}
	return m, nil
}

// ---- file watching ---------------------------------------------------------------------------

// mlEvalTickMaint schedules async model/vector reloads (rate-limited stats; parses run off the
// WS tick goroutine so a 4.5MB export can never stall the book feed).
func (s *Server) mlEvalTickMaint() {
	st := &s.mlEval
	now := time.Now()
	st.mu.Lock()
	loadModel := !st.modelBusy && now.Sub(st.modelChk) > 5*time.Second
	if loadModel {
		st.modelChk = now
		st.modelBusy = true
	}
	loadVecs := !st.vecsBusy && now.Sub(st.vecsChk) > 2*time.Second
	if loadVecs {
		st.vecsChk = now
		st.vecsBusy = true
	}
	st.mu.Unlock()
	if loadModel {
		go s.mlEvalLoadModel()
	}
	if loadVecs {
		go s.mlEvalLoadVecs()
	}
}

func (s *Server) mlEvalLoadModel() {
	st := &s.mlEval
	defer func() {
		st.mu.Lock()
		st.modelBusy = false
		st.mu.Unlock()
	}()
	path := filepath.Join(s.cfg().DataDir, mlEvalExportFile)
	fi, err := os.Stat(path)
	if err != nil {
		return // no export yet — sidecar publishes after its first retrain/boot
	}
	st.mu.Lock()
	same := fi.ModTime().Equal(st.modelMt)
	st.mu.Unlock()
	if same {
		return
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return
	}
	m, perr := mlEvalParse(b)
	st.mu.Lock()
	st.modelMt = fi.ModTime()
	if perr != nil {
		if strings.Contains(perr.Error(), "legacy export is reference-only") {
			// Expected once during the book-v1 warmup. Keep the old artifact for research/provenance,
			// but do not page or retain it as an active evaluator.
			st.model = nil
			st.lastErr = ""
			st.mu.Unlock()
			return
		}
		st.lastErr, st.lastErrAt = perr.Error(), time.Now()
		auditIt := time.Since(st.errAuditAt) > 10*time.Minute
		if auditIt {
			st.errAuditAt = time.Now()
		}
		st.mu.Unlock()
		if auditIt {
			actx, acancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = s.store.Audit(actx, "error", "ml", "R123 go-eval model export REFUSED — keeping previous model + file fallback", perr.Error())
			acancel()
		}
		return
	}
	announce := st.loadAuditNew != m.version
	if announce {
		st.loadAuditNew = m.version
	}
	oldVersion := ""
	if st.model != nil {
		oldVersion = st.model.version
	}
	st.model = m
	if oldVersion != m.version {
		// Model and vectors are one atomic semantic generation even though they arrive in two files.
		// Never serve scores or drift samples from the prior generation while waiting for matching
		// vectors; vecsMt=zero makes the watcher retry the current vector file after the new export.
		st.vecs, st.byTicker, st.scores, st.scoreAt = nil, nil, nil, nil
		st.vecsMt = time.Time{}
		st.mdrift, st.rdelta = mlEvalDriftRing{}, mlEvalDriftRing{}
		st.latN, st.latTotal, st.evalCount = 0, 0, 0
	}
	st.lastErr = ""
	st.mu.Unlock()
	if announce {
		actx, acancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = s.store.Audit(actx, "info", "ml", fmt.Sprintf("R123 go-eval model loaded: %s (%s/%s, parity replay max %.2e on %d pairs)",
			m.version, m.backend, m.calib, m.parityMax, m.parityN), "")
		acancel()
	}
}

func (s *Server) mlEvalLoadVecs() {
	st := &s.mlEval
	defer func() {
		st.mu.Lock()
		st.vecsBusy = false
		st.mu.Unlock()
	}()
	path := filepath.Join(s.cfg().DataDir, mlEvalVecsFile)
	fi, err := os.Stat(path)
	if err != nil {
		return
	}
	st.mu.Lock()
	same := fi.ModTime().Equal(st.vecsMt)
	model := st.model
	st.mu.Unlock()
	if same {
		return
	}
	var doc struct {
		GeneratedAt int64        `json:"generated_at"`
		Version     string       `json:"version"`
		Authority   *bool        `json:"authority"`
		Rows        []*mlEvalVec `json:"rows"`
	}
	if !s.readJSONLoose(path, &doc) {
		return
	}
	// R138 governance: an explicit research-only receipt revokes every prior tick score even when
	// its model/vector generation is empty or different. Returning on version mismatch used to
	// leave the old in-memory scores warm enough to authorize live combo legs.
	if doc.Authority == nil || !*doc.Authority {
		st.mu.Lock()
		st.vecs, st.byTicker = map[string]*mlEvalVec{}, map[string][]string{}
		st.scores, st.scoreAt = map[string]mlEvalScoreEnt{}, map[string]time.Time{}
		st.vecsMt, st.vecsGen = fi.ModTime(), doc.GeneratedAt
		st.mu.Unlock()
		return
	}
	// R135: the sidecar fast-start path used to publish version="" after loading a persisted
	// bundle while Go kept the prior flat export. That paired p_win from one model with another
	// model's trees and manufactured a 3–10% "drift" alarm. Reject empty/mismatched generations;
	// the sidecar now republishes the exact persisted model before it emits usable vectors.
	if !mlEvalVectorVersionOK(model, doc.Version) {
		return
	}
	vecs := make(map[string]*mlEvalVec, len(doc.Rows))
	byTicker := make(map[string][]string, len(doc.Rows))
	for _, v := range doc.Rows {
		if v == nil || v.Ticker == "" || len(v.X) == 0 {
			continue
		}
		plat := strings.ToLower(v.Platform)
		key := plat + "|" + v.Ticker + "|" + strings.ToUpper(v.Side)
		vecs[key] = v
		tk := plat + "|" + v.Ticker
		byTicker[tk] = append(byTicker[tk], key)
	}
	now := time.Now()
	st.mu.Lock()
	// DRIFT — measured at the sidecar-overwrite moment (the sidecar is ground truth):
	//   refresh_delta: our latest tick score vs the sidecar's fresh score, same key;
	//   model_drift:   Go eval of the UNPATCHED new vector vs the sidecar's p_win (sampled).
	if st.scores != nil {
		for key, v := range vecs {
			if sc, ok := st.scores[key]; ok && sc.src == "tick" && now.Sub(sc.at) < 3*time.Minute {
				st.rdelta.add(math.Abs(sc.pwin - v.PWin))
			}
		}
	}
	sampled := 0
	if model != nil {
		for _, v := range vecs {
			if len(v.X) != model.nf {
				continue
			}
			st.mdrift.add(math.Abs(model.pwin(v.X) - v.PWin))
			sampled++
			if sampled >= 24 {
				break
			}
		}
	}
	st.vecs = vecs
	st.byTicker = byTicker
	st.vecsMt = fi.ModTime()
	st.vecsGen = doc.GeneratedAt
	if st.scores == nil {
		st.scores = map[string]mlEvalScoreEnt{}
	}
	// sidecar OVERWRITE: every publish resets each key to the sidecar's own number; ticks
	// re-freshen from there. Keys that left the file drop out entirely.
	for key := range st.scores {
		if _, ok := vecs[key]; !ok {
			delete(st.scores, key)
			delete(st.scoreAt, key)
		}
	}
	for key, v := range vecs {
		st.scores[key] = mlEvalScoreEnt{pwin: v.PWin, sidecar: v.PWin, px: v.SigPrice, at: now, src: "sidecar"}
	}
	mdN, mdMed, _ := st.mdrift.stats()
	warn := false
	warnTol := s.cfg().Auto.GoEvalDriftWarn
	if warnTol <= 0 {
		warnTol = 0.02
	}
	if mdN >= 8 && mdMed > warnTol && time.Since(st.driftWarnAt) > 30*time.Minute {
		st.driftWarnAt = now
		warn = true
	}
	model2 := st.model
	st.mu.Unlock()
	if warn {
		actx, acancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = s.store.Audit(actx, "warn", "ml", fmt.Sprintf("R123 go-eval MODEL DRIFT: median |go−sidecar| %.4f > %.4f on same-vector evals (n=%d) — export/version skew suspected", mdMed, warnTol, mdN), "")
		acancel()
	}
	// warm the fresh rows immediately off the WS cache (held lots must not wait for a tick)
	if model2 != nil {
		warmed := 0
		for key, v := range vecs {
			if !v.Held && warmed >= 64 {
				continue
			}
			if !strings.EqualFold(v.Platform, "kalshi") || s.kal == nil {
				continue
			}
			if yes, ok := s.kal.LivePrice(v.Ticker); ok && yes > 0 && yes < 1 {
				sidePx := yes
				if sd := strings.ToUpper(v.Side); sd == "NO" || sd == "DOWN" {
					sidePx = 1 - yes
				}
				s.mlEvalScoreOne(model2, key, v, sidePx, "load")
				warmed++
			}
		}
	}
}

// ---- live feature patch + scoring --------------------------------------------------------------

// Per-feature policy (the honest hybrid table, surfaced on /api/mleval):
var mlEvalLiveFeats = []string{ // patched from live suite state on every re-score
	"book_maker_price", "book_taker_price", "book_maker_depth", "book_taker_depth",
	"book_quote_age_s", "book_maker_tick", "book_taker_tick", "book_maker_fee_pc",
	"book_taker_fee_pc", "book_spread_cents", "book_touch_imbalance", "book_maker_all_in",
	"book_taker_all_in", "legacy_to_taker_gap_cents", "book_is_tapered",
					"momentum", "mom_1h", "mom_4h", "dist_hi", "dist_lo", "resolve_hours", "secs_to_start"}
var mlEvalDerivedFeats = []string{ // recomputed from the patched values (formulas = extra_feats)
	"extra_1", "extra_2", "extra_3", "extra_4", "extra_5", "extra_6", "extra_7", "extra_8",
	"extra_10", "extra_11", "extra_12"}

// mlEvalPatch overlays the live features onto a COPY of the sidecar vector and recomputes the
// derived extra_* terms. Anything it can't faithfully construct stays frozen (documented).
func (s *Server) mlEvalPatch(m *mlEvalModel, v *mlEvalVec, x []float64, sidePx float64) bool {
	set := func(name string, val float64) {
		if i, ok := m.idx[name]; ok && i < len(x) {
			x[i] = val
		}
	}
	get := func(name string) float64 {
		if i, ok := m.idx[name]; ok && i < len(x) {
			return x[i]
		}
		return 0
	}
	_ = sidePx // ticker mid is discovery only; executable side book below is authoritative.
	bs := storage.Signal{Platform: strings.ToLower(v.Platform), Ticker: v.Ticker, Side: v.Side}
	s.stampMLBookSnapshot(&bs)
	if bs.BookFeatureVer != mlBookFeatureVersion || bs.BookBid == nil || bs.BookAsk == nil ||
		bs.BookBidDepth == nil || bs.BookAskDepth == nil || bs.BookQuoteAgeS == nil ||
		bs.BookMakerTick == nil || bs.BookTakerTick == nil ||
		bs.BookMakerFeePC == nil || bs.BookTakerFeePC == nil {
		return false
	}
	set("book_feature_ver", float64(bs.BookFeatureVer))
	set("book_maker_price", *bs.BookBid)
	set("book_taker_price", *bs.BookAsk)
	set("book_maker_depth", *bs.BookBidDepth)
	set("book_taker_depth", *bs.BookAskDepth)
	set("book_quote_age_s", *bs.BookQuoteAgeS)
	set("book_maker_tick", *bs.BookMakerTick)
	set("book_taker_tick", *bs.BookTakerTick)
	set("book_maker_fee_pc", *bs.BookMakerFeePC)
	set("book_taker_fee_pc", *bs.BookTakerFeePC)
	set("book_spread_cents", (*bs.BookAsk-*bs.BookBid)*100)
	set("book_touch_imbalance", (*bs.BookBidDepth-*bs.BookAskDepth)/(*bs.BookBidDepth+*bs.BookAskDepth))
	set("book_maker_all_in", *bs.BookBid+*bs.BookMakerFeePC)
	set("book_taker_all_in", *bs.BookAsk+*bs.BookTakerFeePC)
	set("legacy_to_taker_gap_cents", (*bs.BookAsk-get("legacy_signal_price"))*100)
	tapered := 0.0
	if math.Min(*bs.BookMakerTick, *bs.BookTakerTick) < .01 {
		tapered = 1
	}
	set("book_is_tapered", tapered)
	set("book_latency_ms", 0)
	set("book_latency_missing", 1)
	if v.CloseTS > 0 {
		rh := time.Until(time.Unix(v.CloseTS, 0)).Hours()
		if rh < 0 {
			rh = 0
		}
		set("resolve_hours", rh)
	}
	if v.StartTS > 0 {
		set("secs_to_start", time.Until(time.Unix(v.StartTS, 0)).Seconds())
	}
	plat := strings.ToLower(v.Platform)
	if plat == "" {
		plat = "kalshi"
	}
	if mom, ok := s.pxRingMom5m(plat, v.Ticker); ok {
		set("momentum", mom)
	}
	m1, m4, dh, dl := s.pxRingFeats(plat, v.Ticker)
	if m1 != nil {
		set("mom_1h", *m1)
	}
	if m4 != nil {
		set("mom_4h", *m4)
	}
	if dh != nil {
		set("dist_hi", *dh)
	}
	if dl != nil {
		set("dist_lo", *dl)
	}
	// derived extras (formulas verbatim from live_ml.py extra_feats; frozen: extra_0/9/13/14
	// — their inputs (notional, strength×notional, trader_count, underlying) are not live-known)
	p, strg, imb := get("book_taker_price"), get("strength"), get("imbalance")
	mom, rh, sp, dep := get("momentum"), get("resolve_hours"), get("spread_cents"), get("book_depth")
	set("extra_1", math.Abs(p-0.5))
	set("extra_2", p*strg)
	set("extra_3", p*imb)
	set("extra_4", mom*rh)
	set("extra_5", p*(1-p))
	set("extra_6", math.Min(p, 1-p))
	set("extra_7", math.Log1p(math.Max(0, rh)))
	set("extra_8", mom*imb)
	set("extra_10", sp/100.0)
	tight := 0.0
	if sp > 0 && sp <= 3 {
		tight = 1.0
	}
	set("extra_11", tight)
	set("extra_12", math.Log1p(math.Max(0, dep)))
	return true
}

func (s *Server) mlEvalScoreOne(m *mlEvalModel, key string, v *mlEvalVec, sidePx float64, src string) {
	if len(v.X) != m.nf {
		return // vector width from a different model generation — wait for the next publish
	}
	x := make([]float64, len(v.X))
	copy(x, v.X)
	if !s.mlEvalPatch(m, v, x, sidePx) {
		return
	}
	t0 := time.Now()
	p := m.pwin(x)
	ns := time.Since(t0).Nanoseconds()
	st := &s.mlEval
	st.mu.Lock()
	if st.scores == nil {
		st.scores = map[string]mlEvalScoreEnt{}
	}
	st.scores[key] = mlEvalScoreEnt{pwin: p, sidecar: v.PWin, px: sidePx, at: time.Now(), src: src}
	st.latNs[st.latTotal%int64(len(st.latNs))] = ns
	st.latTotal++
	if st.latN < len(st.latNs) {
		st.latN++
	}
	st.evalCount++
	st.mu.Unlock()
}

// mlEvalOnTick — THE hook: called from onKalshiTick/onPolyUSTick on every top-of-book change.
func (s *Server) mlEvalOnTick(platform, ticker string, yes float64) {
	if !s.cfg().Auto.GoEvalEnabled {
		return
	}
	s.mlEvalTickMaint()
	st := &s.mlEval
	st.mu.Lock()
	m := st.model
	if m == nil || st.byTicker == nil {
		st.mu.Unlock()
		return
	}
	keys := st.byTicker[strings.ToLower(platform)+"|"+ticker]
	if len(keys) == 0 {
		st.mu.Unlock()
		return
	}
	now := time.Now()
	if st.scoreAt == nil {
		st.scoreAt = map[string]time.Time{}
	}
	type job struct {
		key string
		vec *mlEvalVec
	}
	var work []job
	for _, k := range keys {
		if t, ok := st.scoreAt[k]; ok && now.Sub(t) < mlEvalKeyRate {
			continue
		}
		st.scoreAt[k] = now
		if v := st.vecs[k]; v != nil {
			work = append(work, job{k, v})
		}
	}
	st.mu.Unlock()
	for _, w := range work {
		sidePx := yes
		if sd := strings.ToUpper(w.vec.Side); sd == "NO" || sd == "DOWN" {
			sidePx = 1 - yes
		}
		s.mlEvalScoreOne(m, w.key, w.vec, sidePx, "tick")
	}
}

// ---- consumers ---------------------------------------------------------------------------------

// goEvalScore returns the current Go-eval score for platform|ticker|side (binary complement
// when only the opposite signal side has a vector — mirrors the sidecar's all_scored math).
func (s *Server) goEvalScore(platform, ticker, side string) (float64, time.Time, bool) {
	if !s.cfg().Auto.GoEvalEnabled {
		return 0, time.Time{}, false
	}
	st := &s.mlEval
	plat := strings.ToLower(platform)
	sd := strings.ToUpper(side)
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.scores == nil {
		return 0, time.Time{}, false
	}
	if sc, ok := st.scores[plat+"|"+ticker+"|"+sd]; ok {
		return sc.pwin, sc.at, true
	}
	opp := map[string]string{"YES": "NO", "NO": "YES", "UP": "DOWN", "DOWN": "UP"}[sd]
	if opp != "" {
		if sc, ok := st.scores[plat+"|"+ticker+"|"+opp]; ok {
			return 1 - sc.pwin, sc.at, true
		}
	}
	return 0, time.Time{}, false
}

// tickFreshPWin — R123: the border/quarantine checks' overlay. Returns the tick-fresh Go-eval
// p_win when one exists and is fresh; otherwise the caller's file-based value, unchanged.
func (s *Server) tickFreshPWin(platform, ticker, side string, filePW float64) float64 {
	if pw, at, ok := s.goEvalScore(platform, ticker, side); ok && time.Since(at) <= mlEvalFreshFor {
		return pw
	}
	return filePW
}

// mlEvalReady — the /api/ready component. Additive feature: absence of an export is a neutral
// state (sidecar publishes minutes after boot); RED only on parity refusal or drift alarm.
func (s *Server) mlEvalReady() (bool, string) {
	if !s.cfg().Auto.GoEvalEnabled {
		return true, "disabled (go_eval_enabled=false)"
	}
	var cohort struct {
		Status   string `json:"model_status"`
		Schema   string `json:"feature_schema"`
		Resolved int    `json:"book_v1_resolved"`
		Need     int    `json:"min_train_required"`
	}
	if s.readJSONLoose(filepath.Join(s.cfg().DataDir, "ml_predictions.json"), &cohort) &&
		cohort.Schema == mlBookFeatureSchema && strings.HasPrefix(cohort.Status, "WARMING") {
		return true, fmt.Sprintf("book-v2 holdout warming %d/%d resolved; legacy export ignored; zero authority", cohort.Resolved, cohort.Need)
	}
	st := &s.mlEval
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.lastErr != "" && time.Since(st.lastErrAt) < time.Hour {
		return false, "export refused: " + st.lastErr
	}
	if st.model == nil {
		return true, "no export yet (sidecar publishes after boot/retrain)"
	}
	mdN, mdMed, _ := st.mdrift.stats()
	warnTol := s.cfg().Auto.GoEvalDriftWarn
	if warnTol <= 0 {
		warnTol = 0.02
	}
	if mdN >= 8 && mdMed > warnTol {
		return false, fmt.Sprintf("model drift median %.4f > %.4f (n=%d)", mdMed, warnTol, mdN)
	}
	return true, fmt.Sprintf("model %s · %d vectors · %d evals · drift med %.5f (n=%d)",
		st.model.version, len(st.vecs), st.evalCount, mdMed, mdN)
}

// mlEvalMLPayload — compact stats block for the /api/ml payload (dashboard-visible).
func (s *Server) mlEvalMLPayload() map[string]any {
	st := &s.mlEval
	st.mu.Lock()
	defer st.mu.Unlock()
	out := map[string]any{"enabled": s.cfg().Auto.GoEvalEnabled}
	if st.model != nil {
		out["version"] = st.model.version
		out["parity_max"] = st.model.parityMax
		out["vectors"] = len(st.vecs)
		out["evals"] = st.evalCount
	}
	if st.lastErr != "" {
		out["last_err"] = st.lastErr
	}
	return out
}

// handleMLEval (GET /api/mleval) — full telemetry: parity, latency, drift, the per-feature
// live/derived/frozen policy table, and every held lot's tick-fresh score.
func (s *Server) handleMLEval(w http.ResponseWriter, r *http.Request) {
	depECE, depN, depBins, depOK := s.mlDeployedCalibNow()
	calRed, calWhy := s.mlCalibGate()
	st := &s.mlEval
	st.mu.Lock()
	out := map[string]any{
		"note":    "R123 go-eval: sidecar-exported model re-scored per WS tick; sidecar stays trainer + ground truth (its publishes overwrite tick scores). Parity gate 1e-6 on load; frozen features are honest hybrid, listed below.",
		"enabled": s.cfg().Auto.GoEvalEnabled,
	}
	out["maker_calibration"] = map[string]any{"deployed_ece": depECE, "n": depN, "bins": depBins,
		"available": depOK, "maker_paused": calRed, "reason": calWhy, "window_days": 14, "min_n": 100}
	if st.model != nil {
		out["model"] = map[string]any{
			"version": st.model.version, "feature_schema": mlBookFeatureSchema,
			"backend": st.model.backend, "calib": st.model.calib,
			"n_features": st.model.nf, "folds": len(st.model.folds),
			"parity_pairs": st.model.parityN, "parity_replay_max": st.model.parityMax,
			"fidelity_vs_predict_proba": st.model.fidelity, // measured |flat−proba| median/p95/p99/max — the honest copy-quality table
			"sidecar_self_max":          st.model.selfMax, "loaded_at": st.model.loadedAt.UTC().Format(time.RFC3339),
		}
	}
	if st.lastErr != "" {
		out["last_err"] = st.lastErr
		out["last_err_at"] = st.lastErrAt.UTC().Format(time.RFC3339)
	}
	if st.latN > 0 {
		cp := make([]int64, st.latN)
		copy(cp, st.latNs[:st.latN])
		sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
		out["eval_latency"] = map[string]any{
			"n_window": st.latN, "evals_total": st.evalCount,
			"median_us": float64(cp[st.latN/2]) / 1e3,
			"p95_us":    float64(cp[(st.latN*95)/100]) / 1e3,
		}
	}
	mdN, mdMed, mdMax := st.mdrift.stats()
	rdN, rdMed, rdMax := st.rdelta.stats()
	out["model_drift"] = map[string]any{"n": mdN, "median": mdMed, "max": mdMax,
		"what": "|go eval of sidecar's UNPATCHED vector − sidecar p_win| — alarm metric (≈0 when in sync)"}
	out["refresh_delta"] = map[string]any{"n": rdN, "median": rdMed, "max": rdMax,
		"what": "|tick-fresh score − sidecar's fresh score| at publish — the movement tick re-scoring captures"}
	frozen := []string{}
	if st.model != nil {
		liveSet := map[string]bool{}
		for _, nm := range mlEvalLiveFeats {
			liveSet[nm] = true
		}
		for _, nm := range mlEvalDerivedFeats {
			liveSet[nm] = true
		}
		names := make([]string, 0, st.model.nf)
		for nm := range st.model.idx {
			names = append(names, nm)
		}
		sort.Strings(names)
		for _, nm := range names {
			if !liveSet[nm] {
				frozen = append(frozen, nm)
			}
		}
	}
	out["features"] = map[string]any{"live": mlEvalLiveFeats, "derived": mlEvalDerivedFeats,
		"frozen_n": len(frozen), "frozen": frozen}
	held := []map[string]any{}
	now := time.Now()
	for key, v := range st.vecs {
		if !v.Held {
			continue
		}
		row := map[string]any{"ticker": v.Ticker, "side": v.Side, "platform": v.Platform,
			"sidecar_p_win": v.PWin}
		if sc, ok := st.scores[key]; ok {
			row["p_win_tick"] = math.Round(sc.pwin*1e4) / 1e4
			row["tick_age_s"] = math.Round(now.Sub(sc.at).Seconds()*10) / 10
			row["tick_px"] = sc.px
			row["src"] = sc.src
			row["delta_vs_sidecar"] = math.Round((sc.pwin-v.PWin)*1e4) / 1e4
		}
		held = append(held, row)
	}
	sort.Slice(held, func(i, j int) bool { return held[i]["ticker"].(string) < held[j]["ticker"].(string) })
	out["held_lots"] = held
	out["scores_n"] = len(st.scores)
	out["vectors_n"] = len(st.vecs)
	out["vectors_generated_at"] = st.vecsGen
	st.mu.Unlock()
	writeJSON(w, http.StatusOK, out)
}
