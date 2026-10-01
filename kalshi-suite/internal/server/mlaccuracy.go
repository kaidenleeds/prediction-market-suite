package server

// mlaccuracy.go — ML calibration measured mainly on settled book-native-v2 Paper simulations,
// separate from the sidecar's provisional/untouched model holdout. The active Paper reset epoch
// contains closed ml_paper lots plus same-epoch modeled maker rows joined to an outcome.
// Explicitly v2-provenanced settled LIVE combos are a separate persistent lane. Retired shadow,
// backup, legacy model, and pre-reset rows remain archived and cannot re-enter the New ML card.
// Books without model p_win provenance (rawflow/weather/kflow/freshinv) are also excluded. Output
// is data/ml_accuracy.json (atomic), served at GET /api/mlaccuracy and rendered as Paper-model
// calibration; it is not exchange-profit evidence.

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// mlAccBet is one settled calibration row: p = model win prob at the modeled/real decision, y = outcome
// (1 win / 0 loss; live combos use the per-contract payout clamped to [0,1]), r = realized
// $/contract when the book carries P&L (rank-IC target, mirrors the sidecar's R115 ICIR), else y.
type mlAccBet struct {
	P        float64
	Y        float64
	R        float64
	TS       int64 // settle unix ts (0 = unknown → excluded from daily rank-IC only)
	SourceTS int64 // durable input-row clock (maker fill time when settlement time is unavailable)
	Book     string
	// R118 genre×venue breakdown fields (empty on rows that never carried them, e.g. live combos)
	Ticker string
	Side   string
	Venue  string // "kalshi" | "polyus" (normalized; polyus also detected by ticker prefix)
	Opened int64  // open unix ts — half of the (ticker,side,opened) dedupe key across book files
	// Populated only for combo prediction units. BundleKey is the canonical sorted ticker|side
	// set; ResolutionKeys identify shared legs so overlapping bundles are not called independent.
	BundleKey      string
	ResolutionKeys []string
}

// mlAccVersion gates data/ml_accuracy.json. The current source receipt, not file age, decides
// reuse: a newly settled row invalidates the report within the next refresh even in the same epoch.
const mlAccVersion = 148

// mlAccEpoch is the same durable New-ML Paper boundary used by the briefing: reset_at is the
// temporal floor, epoch_id rejects late writers from an older reset, and the final reset_close
// marker is the ordered fallback for books which predate explicit epoch IDs.
type mlAccEpoch struct {
	ID        string
	ResetAt   string
	ResetUnix float64
	Valid     bool
	Reason    string
}

// mlAccSourceReceipt is the durable identity of the exact settled rows behind one report. Count
// and max-close make changes legible; SHA-256 catches replacements/corrections with equal counts.
// The per-lane counts explain what moved without weakening the combined hash contract.
type mlAccSourceReceipt struct {
	Version           int    `json:"version"`
	Valid             bool   `json:"valid"`
	EpochID           string `json:"epoch_id"`
	ResetAt           string `json:"reset_at"`
	Count             int    `json:"settled_count"`
	MaxCloseUnix      int64  `json:"max_close_unix"`
	MaxSourceUnix     int64  `json:"max_source_unix"`
	PaperMaxCloseUnix int64  `json:"paper_max_close_unix"`
	MakerMaxFillUnix  int64  `json:"maker_max_fill_unix"`
	SHA256            string `json:"sha256"`
	PaperCount        int    `json:"paper_count"`
	MakerFillCount    int    `json:"maker_fill_count"`
	LiveComboCount    int    `json:"live_combo_count"`
}

// mlAccPolyUSTicker — PolyUS game tickers carry the aec-/tsc-/asc-/atc- prefixes (venue ID
// convention); used as a venue fallback when a lot's platform field is missing.
func mlAccPolyUSTicker(t string) bool {
	lt := strings.ToLower(t)
	for _, p := range []string{"aec-", "tsc-", "asc-", "atc-"} {
		if strings.HasPrefix(lt, p) {
			return true
		}
	}
	return false
}

// mlAccVenue normalizes a lot's venue to kalshi/polyus (the only two bettable venues that place).
func mlAccVenue(platform, ticker string) string {
	if strings.ToLower(strings.TrimSpace(platform)) == "polyus" || mlAccPolyUSTicker(ticker) {
		return "polyus"
	}
	return "kalshi"
}

// mlAccFoldGenre folds canonGenre's open set onto the card's fixed six + Other (auto-created
// venue genres and Other/Unknown all read as Other here — the card is a coarse contrast, not
// the market tree).
func mlAccFoldGenre(g string) string {
	switch g {
	case "Sports", "Crypto", "Weather", "Politics", "Economics", "Entertainment":
		return g
	}
	return "Other"
}

// mlAccGenreOf resolves a placed/universe row to its card genre: polyus rows are Sports by
// construction (the venue only lists games); Kalshi rows go series → category map (longest
// hyphen-prefix = the segment before the first '-', same parse as the market tree) → canonGenre.
func (s *Server) mlAccGenreOf(ticker, venue string, cats map[string]string) string {
	if venue == "polyus" {
		return "Sports"
	}
	if ticker == "" {
		return "Other"
	}
	return mlAccFoldGenre(s.mtGenreOfSeries(kalSeriesOf(ticker), cats))
}

// mlAccKey is the cross-file dedupe key for book lots: paper/shadow (authoritative) load first,
// then the .bak snapshot only adds keys they no longer carry (truncated tails).
func mlAccKey(b mlAccBet) string {
	return b.Ticker + "|" + strings.ToUpper(b.Side) + "|" + fmt.Sprint(b.Opened)
}

// mlAccIndependentMarkets removes repeated entries/shares/sides from statistical n. The earliest
// placed decision per venue+instrument wins because it is least exposed to later outcome
// information. Raw settled rows remain in the source receipt and economic audit counts; model
// calibration and its sample-strength label use only these independent instrument markets.
func mlAccIndependentMarkets(in []mlAccBet) []mlAccBet {
	chosen := map[string]mlAccBet{}
	orderTS := func(b mlAccBet) int64 {
		if b.Opened > 0 {
			return b.Opened
		}
		if b.SourceTS > 0 {
			return b.SourceTS
		}
		return b.TS
	}
	for _, b := range in {
		key := strings.ToLower(strings.TrimSpace(b.Venue)) + "|" + strings.TrimSpace(b.Ticker)
		if b.Ticker == "" {
			key = b.Book + "|" + mlAccKey(b)
		}
		old, exists := chosen[key]
		if !exists || orderTS(b) < orderTS(old) ||
			(orderTS(b) == orderTS(old) && strings.ToUpper(b.Side) < strings.ToUpper(old.Side)) {
			chosen[key] = b
		}
	}
	out := make([]mlAccBet, 0, len(chosen))
	for _, b := range chosen {
		out = append(out, b)
	}
	sort.Slice(out, func(i, j int) bool {
		ki := strings.ToLower(out[i].Venue) + "|" + out[i].Ticker
		kj := strings.ToLower(out[j].Venue) + "|" + out[j].Ticker
		return ki < kj
	})
	return out
}

// mlAccComboIdentity converts both historical object legs and the current canonical string-leg
// signature into one durable bundle identity. It deliberately does not use the RFQ display market
// alone: distinct leg sets can be quoted under the same collection/display market.
func mlAccComboIdentity(raw any, market string, rowID int64) (string, []string) {
	_ = rowID // retained in the helper signature for stable test/caller diagnostics; never an evidence key
	var body []byte
	switch v := raw.(type) {
	case json.RawMessage:
		body = append([]byte(nil), v...)
	case []byte:
		body = append([]byte(nil), v...)
	case string:
		body = []byte(v)
	default:
		body, _ = json.Marshal(v)
	}
	var values []any
	_ = json.Unmarshal(body, &values)
	legs := make([]string, 0, len(values))
	resolution := make([]string, 0, len(values))
	for _, value := range values {
		var ticker, side string
		switch v := value.(type) {
		case string:
			parts := strings.Split(v, "|")
			if len(parts) >= 2 {
				ticker, side = strings.Join(parts[:len(parts)-1], "|"), parts[len(parts)-1]
			} else {
				ticker = v
			}
		case map[string]any:
			for _, key := range []string{"ticker", "market_ticker", "marketTicker", "market"} {
				if text, ok := v[key].(string); ok && strings.TrimSpace(text) != "" {
					ticker = text
					break
				}
			}
			for _, key := range []string{"side", "outcome"} {
				if text, ok := v[key].(string); ok && strings.TrimSpace(text) != "" {
					side = text
					break
				}
			}
		}
		ticker = strings.ToUpper(strings.TrimSpace(ticker))
		side = strings.ToUpper(strings.TrimSpace(side))
		if ticker == "" {
			continue
		}
		if side != "YES" && side != "NO" {
			side = "UNKNOWN"
		}
		legs = append(legs, ticker+"|"+side)
		resolution = append(resolution, "kalshi|"+ticker)
	}
	sort.Strings(legs)
	sort.Strings(resolution)
	legs = mlAccCompactStrings(legs)
	resolution = mlAccCompactStrings(resolution)
	if len(legs) == 0 {
		fallback := strings.ToUpper(strings.TrimSpace(market))
		if fallback == "" {
			return "", nil // malformed identity stays in storage but cannot enter calibration proof
		}
		legs = []string{"MARKET|" + fallback}
		resolution = []string{"kalshi|" + fallback}
	}
	return strings.Join(legs, ","), resolution
}

func mlAccCompactStrings(in []string) []string {
	if len(in) < 2 {
		return in
	}
	out := in[:1]
	for _, value := range in[1:] {
		if value != out[len(out)-1] {
			out = append(out, value)
		}
	}
	return out
}

// mlAccUniqueComboBundles de-duplicates retries/replacements of the exact same prediction unit.
// It does not pretend overlapping but non-identical bundles are independent; that count is
// computed separately by mlAccComboIndependenceBlocks.
func mlAccUniqueComboBundles(in []mlAccBet) []mlAccBet {
	chosen := map[string]mlAccBet{}
	orderTS := func(b mlAccBet) int64 {
		if b.Opened > 0 {
			return b.Opened
		}
		if b.SourceTS > 0 {
			return b.SourceTS
		}
		return b.TS
	}
	for _, b := range in {
		key := b.BundleKey
		if key == "" {
			key = strings.TrimSpace(b.Ticker)
		}
		if key == "" {
			continue
		}
		if old, ok := chosen[key]; !ok || orderTS(b) < orderTS(old) {
			chosen[key] = b
		}
	}
	out := make([]mlAccBet, 0, len(chosen))
	for _, b := range chosen {
		out = append(out, b)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].BundleKey < out[j].BundleKey })
	return out
}

// mlAccComboIndependenceBlocks counts connected components of combo bundles sharing any
// resolution key. It is the honest sample-strength n; unique bundle count remains the number of
// separately scored prediction units.
func mlAccComboIndependenceBlocks(in []mlAccBet) int {
	if len(in) == 0 {
		return 0
	}
	parent := make([]int, len(in))
	for i := range parent {
		parent[i] = i
	}
	var find func(int) int
	find = func(i int) int {
		if parent[i] != i {
			parent[i] = find(parent[i])
		}
		return parent[i]
	}
	union := func(a, b int) {
		ra, rb := find(a), find(b)
		if ra != rb {
			parent[rb] = ra
		}
	}
	owner := map[string]int{}
	for i, b := range in {
		keys := b.ResolutionKeys
		if len(keys) == 0 {
			keys = []string{"bundle|" + b.BundleKey}
		}
		for _, key := range keys {
			if old, ok := owner[key]; ok {
				union(i, old)
			} else {
				owner[key] = i
			}
		}
	}
	roots := map[int]bool{}
	for i := range in {
		roots[find(i)] = true
	}
	return len(roots)
}

func mlAccSampleBucket(n int) string {
	bucket := evidenceSampleBucket(n)
	return bucket.Emoji + " " + bucket.Label
}

func mlAccReceipt(epoch mlAccEpoch, bets []mlAccBet) mlAccSourceReceipt {
	r := mlAccSourceReceipt{Version: 1, Valid: epoch.Valid, EpochID: epoch.ID, ResetAt: epoch.ResetAt}
	if !epoch.Valid {
		return r
	}
	type row struct {
		Book, Ticker, Side, Venue, Bundle, Resolution string
		Opened, Closed, Source                        int64
		P, Y, Realized                                float64
	}
	encoded := make([]string, 0, len(bets))
	for _, b := range bets {
		switch b.Book {
		case "ml_paper":
			r.PaperCount++
		case "maker_fills":
			r.MakerFillCount++
		case "live_combos":
			r.LiveComboCount++
		}
		if b.TS > r.MaxCloseUnix {
			r.MaxCloseUnix = b.TS
		}
		sourceTS := b.SourceTS
		if sourceTS == 0 {
			sourceTS = b.TS
		}
		if sourceTS > r.MaxSourceUnix {
			r.MaxSourceUnix = sourceTS
		}
		if b.Book == "ml_paper" && b.TS > r.PaperMaxCloseUnix {
			r.PaperMaxCloseUnix = b.TS
		}
		if b.Book == "maker_fills" && sourceTS > r.MakerMaxFillUnix {
			r.MakerMaxFillUnix = sourceTS
		}
		body, _ := json.Marshal(row{Book: b.Book, Ticker: b.Ticker,
			Side: strings.ToUpper(strings.TrimSpace(b.Side)), Venue: b.Venue,
			Bundle: b.BundleKey, Resolution: strings.Join(b.ResolutionKeys, ","),
			Opened: b.Opened, Closed: b.TS, Source: sourceTS, P: b.P, Y: b.Y, Realized: b.R})
		encoded = append(encoded, string(body))
	}
	sort.Strings(encoded)
	payload, _ := json.Marshal(struct {
		Version int      `json:"version"`
		EpochID string   `json:"epoch_id"`
		ResetAt string   `json:"reset_at"`
		Rows    []string `json:"rows"`
	}{1, epoch.ID, epoch.ResetAt, encoded})
	h := sha256.Sum256(payload)
	r.Count, r.SHA256 = len(bets), fmt.Sprintf("%x", h[:])
	return r
}

// mlAccDecile is one 10%-of-predicted-probability calibration bucket.
type mlAccDecile struct {
	Lo   float64 `json:"lo"`
	Hi   float64 `json:"hi"`
	N    int     `json:"n"`
	Pred float64 `json:"pred"` // mean predicted p in the bucket
	Real float64 `json:"real"` // realized win rate in the bucket
	Gap  float64 `json:"gap"`  // realized − predicted (negative = overconfident)
}

// mlAccDeciles buckets bets by predicted p into 10 fixed bins [0,.1)…[.9,1] (top bin inclusive,
// same edge rule as the sidecar's ECE at live_ml.py:1937-1946). Empty bins are omitted.
func mlAccDeciles(bets []mlAccBet) []mlAccDecile {
	var out []mlAccDecile
	for i := 0; i < 10; i++ {
		lo, hi := float64(i)/10, float64(i+1)/10
		var n int
		var sp, sy float64
		for _, b := range bets {
			in := b.P >= lo && (b.P < hi || (i == 9 && b.P <= hi))
			if !in {
				continue
			}
			n++
			sp += b.P
			sy += b.Y
		}
		if n == 0 {
			continue
		}
		pred, real := sp/float64(n), sy/float64(n)
		out = append(out, mlAccDecile{Lo: lo, Hi: hi, N: n,
			Pred: math.Round(pred*1000) / 1000, Real: math.Round(real*1000) / 1000,
			Gap: math.Round((real-pred)*1000) / 1000})
	}
	return out
}

// mlAccECE is the n-weighted mean |realized − predicted| over the deciles — the same Expected
// Calibration Error formula the sidecar publishes, so the two columns compare like-for-like.
func mlAccECE(deciles []mlAccDecile, total int) float64 {
	if total <= 0 {
		return 0
	}
	var s float64
	for _, d := range deciles {
		s += math.Abs(d.Real-d.Pred) * float64(d.N)
	}
	return s / float64(total)
}

// mlAccMAE is the plain per-bet mean |p − y| (Brier-adjacent; NOT the same thing as ECE — a
// perfectly calibrated 60% model still scores ~0.48 here because single outcomes are 0/1).
func mlAccMAE(bets []mlAccBet) float64 {
	if len(bets) == 0 {
		return 0
	}
	var s float64
	for _, b := range bets {
		s += math.Abs(b.P - b.Y)
	}
	return s / float64(len(bets))
}

// mlAccBrier is the binary Brier score on settled, actually placed lots:
// mean((p_win - outcome)^2). It is deliberately separate from the sidecar's
// provisional/holdout Brier: callers choose the placed-lot cohort explicitly.
func mlAccBrier(bets []mlAccBet) (float64, bool) {
	if len(bets) == 0 {
		return 0, false
	}
	var sum float64
	for _, b := range bets {
		d := b.P - b.Y
		sum += d * d
	}
	return sum / float64(len(bets)), true
}

// mlAccLogLoss is binary cross-entropy on settled, actually placed lots. Exact
// 0/1 probabilities are clipped so one overconfident miss is finite and the
// report remains valid JSON while still receiving a very large penalty.
func mlAccLogLoss(bets []mlAccBet) (float64, bool) {
	if len(bets) == 0 {
		return 0, false
	}
	const eps = 1e-15
	var sum float64
	for _, b := range bets {
		p := math.Min(1-eps, math.Max(eps, b.P))
		sum -= b.Y*math.Log(p) + (1-b.Y)*math.Log(1-p)
	}
	return sum / float64(len(bets)), true
}

// mlAccAUC is the rank-based (Mann-Whitney) AUC: P(random winner's p > random loser's p), ties
// counted 0.5. Returns -1 when there is no winner/loser pair to rank.
func mlAccAUC(bets []mlAccBet) float64 {
	var wins, losses []float64
	for _, b := range bets {
		if b.Y >= 0.5 {
			wins = append(wins, b.P)
		} else {
			losses = append(losses, b.P)
		}
	}
	if len(wins) == 0 || len(losses) == 0 {
		return -1
	}
	var s float64
	for _, w := range wins {
		for _, l := range losses {
			switch {
			case w > l:
				s += 1
			case w == l:
				s += 0.5
			}
		}
	}
	return s / float64(len(wins)*len(losses))
}

// mlAccDisc reports discrimination: win rate of confident-yes bets (p>0.60) vs confident-no
// bets (p<0.40). A model with real signal shows a wide spread between the two.
func mlAccDisc(bets []mlAccBet) (hiN int, hiWR float64, loN int, loWR float64) {
	var hiW, loW float64
	for _, b := range bets {
		if b.P > 0.60 {
			hiN++
			hiW += b.Y
		} else if b.P < 0.40 {
			loN++
			loW += b.Y
		}
	}
	if hiN > 0 {
		hiWR = hiW / float64(hiN)
	}
	if loN > 0 {
		loWR = loW / float64(loN)
	}
	return
}

// mlAccSpearman is the sidecar's R115 tie-aware Spearman (live_ml.py:1538-1557) ported verbatim:
// average ranks on ties, Pearson on the ranks. Returns ok=false on zero variance.
func mlAccSpearman(x, y []float64) (float64, bool) {
	rank := func(v []float64) []float64 {
		order := make([]int, len(v))
		for i := range order {
			order[i] = i
		}
		sort.SliceStable(order, func(a, b int) bool { return v[order[a]] < v[order[b]] })
		r := make([]float64, len(v))
		for i := 0; i < len(order); {
			j := i
			for j+1 < len(order) && v[order[j+1]] == v[order[i]] {
				j++
			}
			for k := i; k <= j; k++ {
				r[order[k]] = float64(i+j) / 2
			}
			i = j + 1
		}
		return r
	}
	rx, ry := rank(x), rank(y)
	n := float64(len(x))
	var mx, my float64
	for i := range rx {
		mx += rx[i]
		my += ry[i]
	}
	mx /= n
	my /= n
	var sxy, sxx, syy float64
	for i := range rx {
		sxy += (rx[i] - mx) * (ry[i] - my)
		sxx += (rx[i] - mx) * (rx[i] - mx)
		syy += (ry[i] - my) * (ry[i] - my)
	}
	if sxx <= 0 || syy <= 0 {
		return 0, false
	}
	return sxy / math.Sqrt(sxx*syy), true
}

// mlAccICIR reuses the sidecar's R115 formula on the real-bet subset: per-day Spearman rank-IC
// (predicted p vs realized $/contract) over bets settling in the trailing 30 days, days need ≥5
// bets to rank; ICIR = mean(IC)/std(IC, n−1) over ≥5 qualifying days. Returns (nil, reason) when
// the real-bet subset is too thin — expected early, so the reason string is user-facing.
func mlAccICIR(bets []mlAccBet, now int64) (*float64, string) {
	byDay := map[int64][][2]float64{}
	for _, b := range bets {
		if b.TS <= 0 || now-b.TS > 30*86400 {
			continue
		}
		d := b.TS / 86400
		byDay[d] = append(byDay[d], [2]float64{b.P, b.R})
	}
	var days []int64
	for d := range byDay {
		days = append(days, d)
	}
	sort.Slice(days, func(a, b int) bool { return days[a] < days[b] })
	var ics []float64
	for _, d := range days {
		pairs := byDay[d]
		if len(pairs) < 5 { // need a few lots to rank meaningfully (same floor as the sidecar)
			continue
		}
		x := make([]float64, len(pairs))
		y := make([]float64, len(pairs))
		for i, p := range pairs {
			x[i], y[i] = p[0], p[1]
		}
		if ic, ok := mlAccSpearman(x, y); ok {
			ics = append(ics, ic)
		}
	}
	if len(ics) < 5 {
		return nil, fmt.Sprintf("real-bet sample too thin for daily rank-IC: %d day(s) had ≥5 settled bets in the last 30d, need ≥5 days", len(ics))
	}
	var m float64
	for _, v := range ics {
		m += v
	}
	m /= float64(len(ics))
	var sd float64
	for _, v := range ics {
		sd += (v - m) * (v - m)
	}
	sd = math.Sqrt(sd / float64(len(ics)-1))
	if sd <= 1e-9 {
		return nil, "rank-IC variance ~0 across days — ICIR undefined"
	}
	icir := math.Round(m/sd*100) / 100
	return &icir, fmt.Sprintf("daily rank-IC over %d day(s), trailing 30d", len(ics))
}

// mlAccPlain writes the operator's one-sentence answer from the deciles: what the model says vs
// what actually happened, in points, plain words.
func mlAccPlain(deciles []mlAccDecile, total int) string {
	if total == 0 || len(deciles) == 0 {
		return "No settled Paper-model decisions with a probability yet — nothing to grade."
	}
	var sp, sy float64
	for _, d := range deciles {
		sp += d.Pred * float64(d.N)
		sy += d.Real * float64(d.N)
	}
	pred, real := sp/float64(total), sy/float64(total)
	gap := (pred - real) * 100 // positive = overconfident
	verdict := "about right — the confidence numbers match reality"
	switch {
	case gap >= 2:
		verdict = fmt.Sprintf("about %.0f points overconfident", gap)
	case gap <= -2:
		verdict = fmt.Sprintf("about %.0f points underconfident", -gap)
	}
	return fmt.Sprintf("Across %d settled Paper-model decisions, when the model says %.0f%% the selected sides settle true about %.0f%% — %s. This grades probability calibration, not exchange profit.",
		total, pred*100, real*100, verdict)
}

// mlAccReadBook loads settled lots with a model p_win from an ml book JSON (ml_paper.json /
// ml_shadow.json shape: {"closed":[...]}). Loose reader: trailing NULs trimmed, lots missing
// p_win/won skipped, reset_close markers (bank events, not bets) skipped.
func mlAccReadBook(path, book string) []mlAccBet {
	bets, _ := mlAccReadBookEpoch(path, book)
	return bets
}

func mlAccReadBookEpoch(path, book string) ([]mlAccBet, mlAccEpoch) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, mlAccEpoch{Reason: "paper book is unavailable"}
	}
	var doc struct {
		EpochID string           `json:"epoch_id"`
		ResetAt string           `json:"reset_at"`
		Closed  []map[string]any `json:"closed"`
	}
	if json.Unmarshal([]byte(strings.Trim(strings.TrimSpace(string(b)), "\x00")), &doc) != nil {
		return nil, mlAccEpoch{Reason: "paper book JSON is malformed"}
	}
	epoch := mlAccEpoch{ID: strings.TrimSpace(doc.EpochID), ResetAt: strings.TrimSpace(doc.ResetAt)}
	if epoch.ID == "" {
		epoch.Reason = "epoch_id is missing"
		return nil, epoch
	}
	if !strings.HasPrefix(epoch.ID, "ml-v2-") || len(epoch.ID) <= len("ml-v2-") {
		epoch.Reason = "epoch_id is malformed"
		return nil, epoch
	}
	cut, parseErr := time.Parse(time.RFC3339Nano, epoch.ResetAt)
	if parseErr != nil || cut.UnixNano() <= 0 {
		epoch.Reason = "reset_at is missing or malformed"
		return nil, epoch
	}
	epoch.ResetUnix = float64(cut.UnixNano()) / 1e9
	epoch.Valid = true
	// Match currentMLPaperView: everything through the final reset marker is archive history.
	// Explicit epoch/timestamp checks below also fail closed against an older writer appending
	// stale v2 rows after that marker.
	start := 0
	for i, lot := range doc.Closed {
		if rc, ok := lot["reset_close"].(bool); ok && rc {
			start = i + 1
		}
	}
	num := func(m map[string]any, k string) (float64, bool) {
		v, ok := m[k].(float64)
		return v, ok
	}
	var out []mlAccBet
	for _, lot := range doc.Closed[start:] {
		if rc, ok := lot["reset_close"].(bool); ok && rc {
			continue
		}
		cohort, _ := lot["model_cohort"].(string)
		if cohort != currentMLCohort {
			continue // legacy/v1 accuracy is archived and never labeled as New ML
		}
		lotEpoch, _ := lot["epoch_id"].(string)
		if lotEpoch != epoch.ID {
			continue
		}
		opened, openedOK := num(lot, "opened")
		if epoch.ResetUnix > 0 && (!openedOK || opened <= epoch.ResetUnix) {
			continue
		}
		p, okP := num(lot, "p_win")
		y, okY := num(lot, "won")
		if !okP || !okY || p < 0 || p > 1 || y < 0 || y > 1 {
			continue
		}
		bet := mlAccBet{P: p, Y: y, R: y, Book: book}
		if ts, ok := num(lot, "closed_ts"); ok {
			bet.TS = int64(ts)
			bet.SourceTS = bet.TS
		}
		bet.Ticker, _ = lot["ticker"].(string)
		bet.Side, _ = lot["side"].(string)
		plat, _ := lot["platform"].(string)
		bet.Venue = mlAccVenue(plat, bet.Ticker)
		if openedOK {
			bet.Opened = int64(opened)
		}
		if pnl, ok := num(lot, "pnl"); ok {
			if c, ok2 := num(lot, "contracts"); ok2 && c > 0 {
				bet.R = pnl / c // realized $/contract — the sidecar's R115 rank-IC target
			}
		}
		out = append(out, bet)
	}
	return out, epoch
}

// mlAccReadMakerFills loads data/ml_maker_fills.jsonl — the append-only maker fill log, one JSON
// per line {"row":{...},"seq","ts"}. Rows carry the model p_win at fill but NO outcome; the caller
// joins outcomes from signal_log (ResolvedYes) and drops fills whose (ticker,side) is already
// booked (paper lots ARE booked maker fills — the log would double-count them). Y is returned -1
// (unjoined sentinel).
func mlAccReadMakerFills(path string) []mlAccBet {
	return mlAccReadMakerFillsEpoch(path, mlAccEpoch{})
}

func mlAccReadMakerFillsEpoch(path string, epoch mlAccEpoch) []mlAccBet {
	if !epoch.Valid {
		return nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var out []mlAccBet
	for _, ln := range strings.Split(string(b), "\n") {
		ln = strings.TrimSpace(strings.Trim(ln, "\x00"))
		if ln == "" || ln[0] != '{' {
			continue
		}
		var rec struct {
			Row map[string]any `json:"row"`
		}
		if json.Unmarshal([]byte(ln), &rec) != nil || rec.Row == nil {
			continue
		}
		cohort, _ := rec.Row["model_cohort"].(string)
		if cohort != currentMLCohort {
			continue
		}
		rowEpoch, _ := rec.Row["epoch_id"].(string)
		if rowEpoch != epoch.ID {
			continue
		}
		stamp, stampOK := rec.Row["fill_ts"].(float64)
		if !stampOK || stamp <= 0 {
			stamp, stampOK = rec.Row["opened"].(float64)
		}
		if epoch.ResetUnix > 0 && (!stampOK || stamp <= epoch.ResetUnix) {
			continue
		}
		p, okP := rec.Row["p_win"].(float64)
		tk, _ := rec.Row["ticker"].(string)
		if !okP || p < 0 || p > 1 || tk == "" {
			continue
		}
		bet := mlAccBet{P: p, Y: -1, Book: "maker_fills", Ticker: tk, SourceTS: int64(stamp)}
		bet.Side, _ = rec.Row["side"].(string)
		plat, _ := rec.Row["platform"].(string)
		bet.Venue = mlAccVenue(plat, tk)
		if op, ok := rec.Row["opened"].(float64); ok {
			bet.Opened = int64(op)
		}
		out = append(out, bet)
	}
	return out
}

// mlAccGatherLiveCombos pulls settled, confirmed exact-fee real-money RFQ combos: fair = model
// p_win for the combo, payout is the binary outcome, realized/contracts is fee-net $/ct.
// Legacy fee-unknown rows and transport-ambiguous accepts are not confirmed placed bets.
func (s *Server) mlAccGatherLiveCombos(ctx context.Context) ([]mlAccBet, error) {
	rows, err := s.store.ListLiveCombosAll(ctx, 2000)
	if err != nil {
		return nil, err
	}
	var out []mlAccBet
	for _, r := range rows {
		settled, _ := r["settled"].(int)
		reportable, _ := r["fee_net_reportable"].(bool)
		if settled != 1 || !reportable {
			continue
		}
		cohort, _ := r["model_cohort"].(string)
		if cohort != currentMLCohort {
			continue
		}
		fair, _ := r["fair"].(float64)
		payout, _ := r["payout"].(float64)
		if fair <= 0 || fair > 1 {
			continue // no model probability recorded — can't grade
		}
		y := math.Min(1, math.Max(0, payout))
		market, _ := r["market"].(string)
		rowID, _ := r["id"].(int64)
		bundle, resolution := mlAccComboIdentity(r["legs"], market, rowID)
		if bundle == "" || len(resolution) == 0 {
			continue
		}
		bet := mlAccBet{
			P: fair, Y: y, R: y, Book: "live_combos", Venue: "kalshi", Side: "COMBO",
			Ticker: "combo:" + bundle, BundleKey: bundle, ResolutionKeys: resolution,
		}
		if realized, ok := r["realized"].(float64); ok {
			if c, ok2 := r["contracts"].(float64); ok2 && c > 0 {
				bet.R = realized / c
			}
		}
		if ts, ok := r["settled_ts"].(string); ok && ts != "" {
			if t, err := time.Parse(time.RFC3339Nano, ts); err == nil {
				bet.TS = t.Unix()
			}
		}
		if ts, ok := r["ts"].(string); ok && ts != "" {
			if t, err := time.Parse(time.RFC3339Nano, ts); err == nil {
				bet.Opened = t.Unix()
				bet.SourceTS = bet.Opened
			}
		}
		if bet.SourceTS == 0 {
			bet.SourceTS = bet.TS
		}
		out = append(out, bet)
	}
	return out, nil
}

// mlAccAllSignals reads the LAST valid line of ml_model_history.jsonl — the sidecar's latest
// all-signals walk-forward numbers (oos_auc / brier / ece / icir when present) for the
// side-by-side column. Loose reader, same as handleMLHistory.
func mlAccAllSignals(path string) map[string]any {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	lines := strings.Split(string(b), "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		ln := strings.TrimSpace(strings.Trim(lines[i], "\x00"))
		if ln == "" || ln[0] != '{' {
			continue
		}
		var m map[string]any
		if json.Unmarshal([]byte(ln), &m) != nil {
			continue
		}
		schema, _ := m["feature_schema"].(string)
		if schema != currentMLCohort {
			continue
		}
		scope, _ := m["metric_scope"].(string)
		keys := map[string]string{"oos_auc": "auc", "brier": "brier", "oos_log_loss": "log_loss",
			"ece": "ece", "icir": "icir"}
		if scope == "provisional_paper" {
			keys = map[string]string{"provisional_auc": "auc", "provisional_brier": "brier",
				"provisional_log_loss": "log_loss", "provisional_ece": "ece", "icir": "icir"}
		}
		out := map[string]any{"metric_scope": scope}
		for src, dst := range keys {
			if v, ok := m[src]; ok && v != nil {
				out[dst] = v
			}
		}
		if len(out) > 1 {
			return out
		}
	}
	return nil
}

// mlAccCell is one genre×venue cell (placed or universe). Cells with n<50 are flagged thin —
// their numbers render but must not be read as signal.
type mlAccCell struct {
	Genre string  `json:"genre"`
	Venue string  `json:"venue"`
	N     int     `json:"n"`
	Pred  float64 `json:"pred"`
	Real  float64 `json:"real"`
	Gap   float64 `json:"gap"`
	HiN   int     `json:"hi_n"`
	HiWR  float64 `json:"hi_wr"`
	LoN   int     `json:"lo_n"`
	LoWR  float64 `json:"lo_wr"`
	AUC   any     `json:"auc,omitempty"` // placed cells only (universe cells skip pairwise AUC)
	Thin  bool    `json:"thin"`
}

func mlAccR3(v float64) float64 { return math.Round(v*1000) / 1000 }

func mlAccSortCells(cells []mlAccCell) {
	sort.Slice(cells, func(a, b int) bool {
		if cells[a].N != cells[b].N {
			return cells[a].N > cells[b].N
		}
		if cells[a].Genre != cells[b].Genre {
			return cells[a].Genre < cells[b].Genre
		}
		return cells[a].Venue < cells[b].Venue
	})
}

// mlAccPlacedCells groups the placed bets by genre×venue and computes each cell's calibration +
// discrimination numbers (pairwise AUC is fine here — placed cells are hundreds of rows, not 312k).
func (s *Server) mlAccPlacedCells(bets []mlAccBet, cats map[string]string) []mlAccCell {
	groups := map[[2]string][]mlAccBet{}
	for _, b := range bets {
		g := s.mlAccGenreOf(b.Ticker, b.Venue, cats)
		k := [2]string{g, b.Venue}
		groups[k] = append(groups[k], b)
	}
	var out []mlAccCell
	for k, gb := range groups {
		var sp, sy float64
		for _, b := range gb {
			sp += b.P
			sy += b.Y
		}
		n := float64(len(gb))
		hiN, hiWR, loN, loWR := mlAccDisc(gb)
		c := mlAccCell{Genre: k[0], Venue: k[1], N: len(gb),
			Pred: mlAccR3(sp / n), Real: mlAccR3(sy / n), Gap: mlAccR3((sy - sp) / n),
			HiN: hiN, HiWR: mlAccR3(hiWR), LoN: loN, LoWR: mlAccR3(loWR), Thin: len(gb) < 50}
		if auc := mlAccAUC(gb); auc >= 0 {
			c.AUC = mlAccR3(auc)
		}
		out = append(out, c)
	}
	mlAccSortCells(out)
	return out
}

// mlAccUniverse builds the market-implied contrast block from the storage aggregates: deciles +
// ECE from the decile sums, AUC exactly from the price grid (ties within a 0.1¢ price bin count
// 0.5 — prices sit on the cent grid, so this is the Mann-Whitney AUC, not an approximation), and
// genre×venue cells folded from the per-series cells. NO model probability exists here: the
// "prediction" is the market price itself, and the block is labeled that way.
func (s *Server) mlAccUniverse(ctx context.Context, cats map[string]string) (map[string]any, error) {
	pts, rawCells, err := s.store.MLUniverseCalibration(ctx)
	if err != nil {
		return nil, err
	}
	if len(pts) == 0 {
		return nil, nil
	}
	// deciles
	type agg struct {
		n      int
		sp, sy float64
	}
	var decs [10]agg
	total := 0
	var sumP, sumY float64
	for _, p := range pts {
		if p.Dec < 0 || p.Dec > 9 {
			continue
		}
		decs[p.Dec].n += p.N
		decs[p.Dec].sp += p.SumP
		decs[p.Dec].sy += p.SumY
		total += p.N
		sumP += p.SumP
		sumY += p.SumY
	}
	var deciles []mlAccDecile
	for i, d := range decs {
		if d.n == 0 {
			continue
		}
		pred, real := d.sp/float64(d.n), d.sy/float64(d.n)
		deciles = append(deciles, mlAccDecile{Lo: float64(i) / 10, Hi: float64(i+1) / 10, N: d.n,
			Pred: mlAccR3(pred), Real: mlAccR3(real), Gap: mlAccR3(real - pred)})
	}
	// AUC from the price grid: walk prices ascending, count win-above-loss pairs
	grid := map[int][2]int{} // pk → {wins, losses}
	for _, p := range pts {
		g := grid[p.PK]
		g[0] += p.Wins
		g[1] += p.N - p.Wins
		grid[p.PK] = g
	}
	pks := make([]int, 0, len(grid))
	for pk := range grid {
		pks = append(pks, pk)
	}
	sort.Ints(pks)
	var pairS, cumLoss, totW, totL float64
	for _, pk := range pks {
		w, l := float64(grid[pk][0]), float64(grid[pk][1])
		pairS += w*cumLoss + 0.5*w*l
		cumLoss += l
		totW += w
		totL += l
	}
	var aucOut any
	if totW > 0 && totL > 0 {
		aucOut = mlAccR3(pairS / (totW * totL))
	}
	// genre×venue cells (fold series → genre; polyus folds to Sports by venue rule)
	folded := map[[2]string]*mlAccCell{}
	for _, rc := range rawCells {
		venue := rc.Venue
		if venue != "polyus" {
			venue = "kalshi"
		}
		g := "Sports"
		if venue != "polyus" {
			g = mlAccFoldGenre(s.mtGenreOfSeries(rc.Series, cats))
		}
		k := [2]string{g, venue}
		c := folded[k]
		if c == nil {
			c = &mlAccCell{Genre: g, Venue: venue}
			folded[k] = c
		}
		c.N += rc.N
		c.Pred += rc.SumP // sums for now; normalized below
		c.Real += rc.SumY
		c.HiN += rc.HiN
		c.HiWR += float64(rc.HiW) // win count for now
		c.LoN += rc.LoN
		c.LoWR += float64(rc.LoW)
	}
	var cells []mlAccCell
	for _, c := range folded {
		n := float64(c.N)
		c.Pred, c.Real = mlAccR3(c.Pred/n), mlAccR3(c.Real/n)
		c.Gap = mlAccR3(c.Real - c.Pred)
		if c.HiN > 0 {
			c.HiWR = mlAccR3(c.HiWR / float64(c.HiN))
		}
		if c.LoN > 0 {
			c.LoWR = mlAccR3(c.LoWR / float64(c.LoN))
		}
		c.Thin = c.N < 50
		cells = append(cells, *c)
	}
	mlAccSortCells(cells)
	if total == 0 {
		return nil, nil
	}
	pred, real := sumP/float64(total), sumY/float64(total)
	return map[string]any{
		"note":    "market price calibration (no model p) — probability = entry price of every resolved logged signal, NOT a model prediction; contrast baseline only",
		"n":       total,
		"pred":    mlAccR3(pred),
		"real":    mlAccR3(real),
		"gap":     mlAccR3(real - pred),
		"ece":     math.Round(mlAccECE(deciles, total)*10000) / 10000,
		"auc":     aucOut,
		"deciles": deciles,
		"cells":   cells,
	}, nil
}

// mlAccPlainExtras extends the plain-words answer with the R118 asks — every clause is generated
// from the computed numbers (nothing hard-coded) and skipped when its data is missing/thin.
func mlAccPlainExtras(byBook map[string][]mlAccBet, placedCells []mlAccCell, deciles []mlAccDecile, uni map[string]any) string {
	var parts []string
	bookStat := func(bets []mlAccBet) (gapPt, auc float64, ok bool) {
		if len(bets) < 20 {
			return 0, 0, false
		}
		var sp, sy float64
		for _, b := range bets {
			sp += b.P
			sy += b.Y
		}
		return (sy - sp) / float64(len(bets)) * 100, mlAccAUC(bets), true
	}
	if pg, pa, ok1 := bookStat(byBook["ml_paper"]); ok1 {
		if sg, sa, ok2 := bookStat(byBook["ml_shadow"]); ok2 && pg < sg {
			parts = append(parts, fmt.Sprintf("The weak spot is the current paper book (%.1f pts overconfident, AUC %.2f) — the retired shadow book was better calibrated (%.1f pts, AUC %.2f).", -pg, pa, -sg, sa))
		}
	}
	// where in the probability range the overconfidence sits (top bins .8–1.0)
	var hn int
	var hgap float64
	for _, d := range deciles {
		if d.Lo >= 0.8 {
			hn += d.N
			hgap += d.Gap * float64(d.N)
		}
	}
	if hn >= 50 && hgap/float64(hn) < -0.05 {
		parts = append(parts, fmt.Sprintf("Most of the overconfidence sits in the 80–100%% picks: they win about %.0f pts less than stated.", -hgap/float64(hn)*100))
	}
	// genres: best-calibrated big cell vs Sports on both venues
	var best *mlAccCell
	var sports []mlAccCell
	for i, c := range placedCells {
		if c.Thin {
			continue
		}
		if best == nil || math.Abs(c.Gap) < math.Abs(best.Gap) {
			best = &placedCells[i]
		}
		if c.Genre == "Sports" {
			sports = append(sports, c)
		}
	}
	if best != nil && best.Genre != "Sports" && len(sports) > 0 {
		var sgap float64
		var sv []string
		for _, c := range sports {
			sgap += math.Abs(c.Gap)
			sv = append(sv, fmt.Sprintf("%s %.0f pts", c.Venue, -c.Gap*100))
		}
		sgap /= float64(len(sports))
		clause := fmt.Sprintf("%s bets are the best calibrated (%.0f pt gap)", best.Genre, math.Abs(best.Gap)*100)
		if math.Abs(best.Gap) > 0 && sgap/math.Abs(best.Gap) >= 1.5 {
			clause += fmt.Sprintf("; Sports runs about %.0fx more overconfident (%s)", sgap/math.Abs(best.Gap), strings.Join(sv, ", "))
		} else if len(sv) > 0 {
			clause += fmt.Sprintf("; Sports: %s overconfident", strings.Join(sv, ", "))
		}
		parts = append(parts, clause+".")
	}
	if uni != nil {
		if g, ok := uni["gap"].(float64); ok {
			if n, ok2 := uni["n"].(int); ok2 {
				parts = append(parts, fmt.Sprintf("Market prices themselves are calibrated to within about %.1f pt across %d resolved markets — the gap is the model's, not the venue's.", math.Abs(g)*100, n))
			}
		}
	}
	return strings.Join(parts, " ")
}

// RefreshMLAccuracy computes the real-bet accuracy report and atomically writes
// data/ml_accuracy.json. Reuse is content-addressed: the current settled-row count/max-close/hash
// must match, so a same-epoch close or corrected row invalidates immediately rather than waiting
// for an age window. Exported for the main.go loop and the read endpoint.
func (s *Server) RefreshMLAccuracy(ctx context.Context, force bool) error {
	// This report is analytic and can scan the full settled signal universe. Non-forced production
	// refreshes use the shared heavy lane, so they cannot overlap another analytic scan and their
	// context is canceled if LIVE becomes armed mid-refresh. A deferral preserves the last-good
	// file. Forced refresh remains an explicit test/offline tool.
	if !force {
		var refreshErr error
		ran := s.tryRunHeavyResearch(ctx, "ml-accuracy", 0, func(workCtx context.Context) {
			refreshErr = s.refreshMLAccuracy(workCtx, false)
		})
		if !ran {
			if s.researchLiveActive() {
				return nil
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			return nil
		}
		return refreshErr
	}
	return s.refreshMLAccuracy(ctx, true)
}

func (s *Server) refreshMLAccuracy(ctx context.Context, force bool) error {
	// The endpoint and background loop may notice the same new close concurrently. One writer owns
	// the fixed atomic temp path and receipt comparison at a time.
	s.mlAccMu.Lock()
	defer s.mlAccMu.Unlock()
	dir := s.cfg().DataDir
	outPath := filepath.Join(dir, "ml_accuracy.json")
	paper, paperEpoch := mlAccReadBookEpoch(filepath.Join(dir, "ml_paper.json"), "ml_paper")
	// R143: current New ML means explicit book-native-v2 provenance inside the current Paper reset
	// epoch only. Legacy/pre-reset paper and maker rows remain archived but cannot re-enter this
	// report merely because they also carry the v2 cohort label.
	seen := map[string]bool{}
	bookedSide := map[string]bool{} // ticker|SIDE — paper lots ARE booked maker fills
	var bets []mlAccBet
	addBook := func(src []mlAccBet) int {
		n := 0
		// Closed ledgers are append ordered. If a durable correction repeats an identity, the last
		// row is authoritative and its changed probability/outcome must enter the receipt hash.
		for i := len(src) - 1; i >= 0; i-- {
			b := src[i]
			if k := mlAccKey(b); b.Ticker != "" && seen[k] {
				continue
			} else if b.Ticker != "" {
				seen[k] = true
			}
			bookedSide[b.Ticker+"|"+strings.ToUpper(b.Side)] = true
			bets = append(bets, b)
			n++
		}
		return n
	}
	nPaper := addBook(paper)
	nFills := 0
	for _, f := range mlAccReadMakerFillsEpoch(filepath.Join(dir, "ml_maker_fills.jsonl"), paperEpoch) {
		side := strings.ToUpper(strings.TrimSpace(f.Side))
		if bookedSide[f.Ticker+"|"+side] || (side != "YES" && side != "NO") {
			continue
		}
		yes, ok, err := s.store.ResolvedYesForVenueWithError(ctx, f.Venue, f.Ticker)
		if err != nil {
			// Preserve the last complete report. A timeout is not evidence that this maker fill
			// became unresolved; omitting it would create a false source-receipt change.
			return fmt.Errorf("resolve maker fill %s/%s: %w", f.Venue, f.Ticker, err)
		}
		if !ok {
			continue // never resolved in signal_log — no outcome to grade against
		}
		y := yes
		if side == "NO" {
			y = 1 - yes
		}
		if y >= 0.5 { // threshold 0.5 → binary outcome, same semantics as the books' won flag
			f.Y = 1
		} else {
			f.Y = 0
		}
		f.R = f.Y
		bookedSide[f.Ticker+"|"+side] = true
		bets = append(bets, f)
		nFills++
	}
	var combos []mlAccBet
	if paperEpoch.Valid {
		var err error
		combos, err = s.mlAccGatherLiveCombos(ctx)
		if err != nil {
			return fmt.Errorf("read settled live combos: %w", err)
		}
	}
	// Single-market calibration and live-combo calibration are different prediction units. Keep
	// one content receipt over both durable lanes, but never pool combo outcomes into single-market
	// AUC/Brier/ECE. Combo bundles get their own explicitly dependent scorecard below.
	receiptRows := append(append([]mlAccBet(nil), bets...), combos...)
	receipt := mlAccReceipt(paperEpoch, receiptRows)
	settledRows := append([]mlAccBet(nil), bets...)
	bets = mlAccIndependentMarkets(settledRows)
	comboBundles := mlAccUniqueComboBundles(combos)
	comboBlocks := mlAccComboIndependenceBlocks(comboBundles)
	if !force {
		var cached struct {
			Version       int                `json:"version"`
			ModelCohort   string             `json:"model_cohort"`
			SourceReceipt mlAccSourceReceipt `json:"source_receipt"`
		}
		if b, err := os.ReadFile(outPath); err == nil && json.Unmarshal(b, &cached) == nil &&
			cached.Version >= mlAccVersion && cached.ModelCohort == currentMLCohort &&
			cached.SourceReceipt == receipt {
			return nil
		}
	}
	cats := s.mtSeriesCategory()
	placedCells := s.mlAccPlacedCells(bets, cats)
	universe, err := s.mlAccUniverse(ctx, cats)
	if err != nil {
		// A ready report is one atomic comparison. Never replace it with a new receipt whose
		// whole-ledger calibration block is absent merely because either grouped scan timed out.
		return fmt.Errorf("read ML calibration universe: %w", err)
	}

	now := time.Now().Unix()
	deciles := mlAccDeciles(bets)
	if deciles == nil {
		deciles = []mlAccDecile{}
	}
	ece := mlAccECE(deciles, len(bets))
	mae := mlAccMAE(bets)
	hiN, hiWR, loN, loWR := mlAccDisc(bets)
	icir, icirNote := mlAccICIR(bets, now)
	var aucOut any
	if auc := mlAccAUC(bets); auc >= 0 {
		aucOut = math.Round(auc*1000) / 1000
	}
	var icirOut any
	if icir != nil {
		icirOut = *icir
	}
	byBook := map[string][]mlAccBet{}
	for _, b := range bets {
		byBook[b.Book] = append(byBook[b.Book], b)
	}
	paperPlaced := byBook["ml_paper"]
	var bookV2Brier, bookV2LogLoss any
	if score, ok := mlAccBrier(paperPlaced); ok {
		bookV2Brier = math.Round(score*10000) / 10000
	}
	if score, ok := mlAccLogLoss(paperPlaced); ok {
		bookV2LogLoss = math.Round(score*10000) / 10000
	}
	var comboBrier, comboLogLoss, comboAUC any
	if score, ok := mlAccBrier(comboBundles); ok {
		comboBrier = math.Round(score*10000) / 10000
	}
	if score, ok := mlAccLogLoss(comboBundles); ok {
		comboLogLoss = math.Round(score*10000) / 10000
	}
	if score := mlAccAUC(comboBundles); score >= 0 {
		comboAUC = math.Round(score*1000) / 1000
	}
	plain := mlAccPlain(deciles, len(bets))
	if extra := mlAccPlainExtras(byBook, placedCells, deciles, universe); extra != "" {
		plain += " " + extra
	}
	doc := map[string]any{
		"version":                         mlAccVersion,
		"ready":                           paperEpoch.Valid,
		"computed_at":                     time.Now().UTC().Format(time.RFC3339),
		"n_bets":                          len(bets), // compatibility: statistical n is unique instrument markets
		"settled_rows":                    len(settledRows),
		"n_unique_markets":                len(bets),
		"sample_bucket":                   mlAccSampleBucket(len(bets)),
		"sample_rule":                     "single-market statistical n counts the earliest placed decision per venue+ticker; repeated entries, sides, snapshots, shares, and combo bundles never increase this n",
		"model_cohort":                    currentMLCohort,
		"paper_epoch_id":                  paperEpoch.ID,
		"paper_reset_at":                  paperEpoch.ResetAt,
		"epoch_error":                     paperEpoch.Reason,
		"source_receipt":                  receipt,
		"singles_profit_evidence":         false,
		"singles_execution_evidence_tier": "paper_simulation",
		"by_book": map[string]int{"ml_paper": len(byBook["ml_paper"]), "maker_fills": len(byBook["maker_fills"]),
			"live_combos": len(comboBundles)},
		"settled_rows_by_book": map[string]int{"ml_paper": nPaper, "maker_fills": nFills,
			"live_combos": len(combos)},
		"sources": "current-reset book-native-v2 singles only for the main calibration: current ML paper lots plus same-epoch v2 maker fills; settled live combos are separately persistent and scored only in combo_accuracy",
		"combo_accuracy": map[string]any{
			"prediction_unit":     "one settled exact-fee LIVE RFQ leg bundle",
			"settled_rows":        len(combos),
			"unique_bundles":      len(comboBundles),
			"independence_blocks": comboBlocks,
			"sample_bucket":       mlAccSampleBucket(comboBlocks),
			"brier":               comboBrier,
			"log_loss":            comboLogLoss,
			"auc":                 comboAUC,
			"scope":               "descriptive scores use unique canonical leg bundles; sample strength uses connected resolution blocks; combo scores are never pooled into single-market AUC/Brier/ECE",
		},
		// honesty: the sidecar truncates closed tails (paper keeps 500, shadow 1000) and ~2,200
		// lifetime closed lots' p_win is unrecoverable — this card covers every resolved placed
		// bet that SURVIVES in the files, not every bet ever placed.
		"truncation_note": "current v2 paper keeps a bounded closed ring; legacy/v1 rows are archived and excluded",
		"placed_cells":    placedCells,
		"universe":        universe,
		// books excluded because their lots never carried a model p_win — count them into
		// nothing, but say so, so this is never mistaken for all-books accuracy:
		"excluded_books": "legacy/v1 ML, retired shadow, rawflow, weather, kflow, freshinv, and rows without explicit book-native-v2 provenance",
		// Actual placed-bet score, not the sidecar's model-validation score. Keep n next to
		// the nullable value so an empty cohort can never render as a perfect zero.
		"book_v2_brier":          bookV2Brier,
		"book_v2_brier_n":        len(paperPlaced),
		"book_v2_brier_scope":    "settled current-reset book-native-v2 ml_paper lots only; mean squared error of p_win versus binary outcome; distinct from provisional/holdout model Brier",
		"book_v2_log_loss":       bookV2LogLoss,
		"book_v2_log_loss_n":     len(paperPlaced),
		"book_v2_log_loss_scope": "settled current-reset book-native-v2 ml_paper lots only; clipped binary cross-entropy of p_win versus outcome; distinct from model-training metrics",
		"deciles":                deciles,
		"ece":                    math.Round(ece*10000) / 10000, // n-weighted mean |realized − predicted| over deciles (sidecar formula)
		"mae":                    math.Round(mae*10000) / 10000, // plain mean |p − outcome| per bet (Brier-adjacent, NOT comparable to ECE)
		"auc":                    aucOut,
		"icir":                   icirOut,
		"icir_note":              icirNote,
		"disc": map[string]any{
			"hi": map[string]any{"n": hiN, "win_rate": math.Round(hiWR*1000) / 1000},
			"lo": map[string]any{"n": loN, "win_rate": math.Round(loWR*1000) / 1000},
		},
		"allsignals":   mlAccAllSignals(filepath.Join(dir, "ml_model_history.jsonl")),
		"plain_answer": plain,
	}
	b, err := json.Marshal(doc)
	if err != nil {
		return err
	}
	tmp := outPath + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, outPath); err != nil {
		return err
	}
	s.log.Info("ml_accuracy.json refreshed", "cohort", currentMLCohort,
		"unique_markets", len(bets), "settled_rows", len(settledRows),
		"paper_rows", nPaper, "maker_fill_rows", nFills, "live_combo_rows", len(combos),
		"live_combo_bundles", len(comboBundles), "live_combo_blocks", comboBlocks)
	return nil
}

// handleMLAccuracy (GET /api/mlaccuracy) content-checks the source before serving. The report file
// cache remains an I/O optimization only; it is never permission to serve an older settled cohort.
// Transient database failures leave the atomic report untouched, so a still-current last-good
// report remains available instead of flapping the endpoint to ready=false.
func (s *Server) handleMLAccuracy(w http.ResponseWriter, r *http.Request) {
	refreshErr := s.RefreshMLAccuracy(r.Context(), false)
	path := filepath.Join(s.cfg().DataDir, "ml_accuracy.json")
	paperPath := filepath.Join(s.cfg().DataDir, "ml_paper.json")
	makerPath := filepath.Join(s.cfg().DataDir, "ml_maker_fills.jsonl")
	stamp := ""
	if fi, err := os.Stat(path); err == nil {
		stamp = fmt.Sprintf("%d|%d", fi.ModTime().UnixNano(), fi.Size())
	}
	if fi, err := os.Stat(paperPath); err == nil {
		stamp += fmt.Sprintf("|paper:%d|%d", fi.ModTime().UnixNano(), fi.Size())
	}
	if fi, err := os.Stat(makerPath); err == nil {
		stamp += fmt.Sprintf("|maker:%d|%d", fi.ModTime().UnixNano(), fi.Size())
	}
	s.mlAccMu.Lock()
	if stamp != "" && stamp == s.mlAccStamp && s.mlAccBody != nil {
		body := s.mlAccBody
		s.mlAccMu.Unlock()
		if refreshErr != nil {
			w.Header().Set("X-ML-Accuracy-Refresh", "last-good")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
		return
	}
	s.mlAccMu.Unlock()
	b, err := os.ReadFile(path)
	if err != nil || !json.Valid(b) {
		writeJSON(w, http.StatusOK, map[string]any{"ready": false, "note": "not computed yet — first pass runs ~2 min after boot"})
		return
	}
	var meta struct {
		Version       int                `json:"version"`
		Ready         bool               `json:"ready"`
		ModelCohort   string             `json:"model_cohort"`
		PaperEpochID  string             `json:"paper_epoch_id"`
		PaperResetAt  string             `json:"paper_reset_at"`
		SourceReceipt mlAccSourceReceipt `json:"source_receipt"`
	}
	_, currentEpoch := mlAccReadBookEpoch(paperPath, "ml_paper")
	if json.Unmarshal(b, &meta) != nil || !currentEpoch.Valid || !meta.Ready ||
		!meta.SourceReceipt.Valid || meta.Version < mlAccVersion ||
		meta.ModelCohort != currentMLCohort || meta.PaperEpochID != currentEpoch.ID ||
		meta.PaperResetAt != currentEpoch.ResetAt || meta.SourceReceipt.EpochID != currentEpoch.ID ||
		meta.SourceReceipt.ResetAt != currentEpoch.ResetAt {
		writeJSON(w, http.StatusOK, map[string]any{
			"ready": false, "model_cohort": currentMLCohort,
			"note": "new book-v2 placed-bet accuracy is waiting for a valid reset epoch; legacy accuracy is archived",
		})
		return
	}
	if stamp != "" {
		s.mlAccMu.Lock()
		s.mlAccStamp, s.mlAccBody = stamp, b
		s.mlAccMu.Unlock()
	}
	if refreshErr != nil {
		w.Header().Set("X-ML-Accuracy-Refresh", "last-good")
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(b)
}
