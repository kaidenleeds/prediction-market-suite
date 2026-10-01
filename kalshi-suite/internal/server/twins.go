package server

// twins.go — R112 Part 3: VERIFIED-twin registry (operator re-audit order: "99% sure they're
// not all the same" — no more sampling). trueRestatementKey collisions are now only a twin
// CANDIDATE; a dual-series pair is dup-blocked ONLY after the venue's own market documents
// prove byte-identical payoff (same strike, direction, open/close/expiration window, and
// rules text). Anything unverified or differing in ANY field is DISTINCT — the operator's
// default: every market slug the API serves is a bet. Verdicts persist in kv ("twinv:") and
// are refreshed by a budgeted background sweep over the dual-series crypto families.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"strings"
	"time"
)

// twinSeries — the known dual-series families (base + trailing-D daily twin). The sweep only
// ever needs pairs whose keys can collide (same series modulo the trailing D).
// (R112 catalog audit: the live dual-series families are crypto + treasury-rate pairs.)
var twinSeries = []string{
	"KXBTC", "KXBTCD", "KXETH", "KXETHD", "KXSOL", "KXSOLD",
	"KXXRP", "KXXRPD", "KXDOGE", "KXDOGED", "KXBNB", "KXBNBD",
	"KXHYPE", "KXHYPED", "KXSHIBA", "KXSHIBAD",
	"KXUST2A", "KXUST2AD", "KXUST10A", "KXUST10AD", "KXUST30A", "KXUST30AD",
}

var twinSeriesSet = func() map[string]struct{} {
	out := make(map[string]struct{}, len(twinSeries))
	for _, series := range twinSeries {
		out[series] = struct{}{}
	}
	return out
}()

// twinCmpFields — the payoff-defining fields compared byte-for-byte between group members.
var twinCmpFields = []string{
	"floor_strike", "cap_strike", "strike_type", "open_time", "close_time",
	"expiration_time", "expected_expiration_time", "rules_primary", "rules_secondary",
}

const (
	twinSweepBudget     = 60 // groups verified per sweep (2 GETs each, under the AIMD limiter)
	twinEvidenceVersion = 1
)

// twinVerdictRecord binds a verdict to the exact venue evidence that produced it. A schema
// version bump, membership change, or payoff-document hash change forces a fresh comparison.
type twinVerdictRecord struct {
	Version      int       `json:"version"`
	Verdict      string    `json:"verdict"`
	Members      []string  `json:"members"`
	DocumentHash string    `json:"document_hash"`
	PayoffFields int       `json:"payoff_fields"`
	CheckedAt    time.Time `json:"checked_at"`
}

func twinMembers(tickers map[string]bool) []string {
	out := make([]string, 0, len(tickers))
	for ticker := range tickers {
		out = append(out, ticker)
	}
	sort.Strings(out)
	return out
}

func sameTwinMembers(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func validTwinRecord(rec twinVerdictRecord) bool {
	if rec.Version != twinEvidenceVersion || (rec.Verdict != "identical" && rec.Verdict != "distinct") ||
		len(rec.Members) < 2 || rec.PayoffFields < 1 || rec.PayoffFields > len(twinCmpFields) || rec.CheckedAt.IsZero() {
		return false
	}
	hashBytes, err := hex.DecodeString(rec.DocumentHash)
	if err != nil || len(hashBytes) != sha256.Size {
		return false
	}
	uniq := make(map[string]bool, len(rec.Members))
	for _, ticker := range rec.Members {
		if ticker == "" || uniq[ticker] {
			return false
		}
		uniq[ticker] = true
	}
	return sameTwinMembers(rec.Members, twinMembers(uniq))
}

func twinRecordMatchesKey(key string, rec twinVerdictRecord) bool {
	for _, ticker := range rec.Members {
		if trueRestatementKey("kalshi", ticker) != key {
			return false
		}
	}
	return true
}

func decodeTwinRecord(raw string) (twinVerdictRecord, bool) {
	var rec twinVerdictRecord
	if json.Unmarshal([]byte(raw), &rec) != nil || !validTwinRecord(rec) {
		return twinVerdictRecord{}, false
	}
	return rec, true
}

// twinLoad restores persisted verdicts once per boot. A failed read leaves the registry
// unloaded so the next sweep retries; twinKeyGate fail-closes the known twin families while
// that persisted safety evidence is unavailable.
func (s *Server) twinLoad(ctx context.Context) bool {
	s.twinMu.Lock()
	if s.twinLoaded {
		s.twinMu.Unlock()
		return true
	}
	if s.twinLoading {
		s.twinMu.Unlock()
		return false
	}
	s.twinLoading = true
	s.twinLoadAttempts++
	s.twinLoadAt = time.Now().UTC()
	s.twinMu.Unlock()

	rows, err := s.store.KVPrefix(ctx, "twinv:")
	s.twinMu.Lock()
	defer s.twinMu.Unlock()
	s.twinLoading = false
	if err != nil {
		s.twinLoadErr = err.Error()
		return false
	}
	verdicts := make(map[string]string, len(rows))
	evidence := make(map[string]twinVerdictRecord, len(rows))
	for k, v := range rows {
		key := strings.TrimPrefix(k, "twinv:")
		rec, valid := decodeTwinRecord(v)
		if !valid || !twinRecordMatchesKey(key, rec) {
			// Legacy plain-string verdicts and records from another evidence version are not
			// trusted silently. Keep the key fail-closed and prioritize it for revalidation.
			verdicts[key] = "stale"
			continue
		}
		verdicts[key] = rec.Verdict
		evidence[key] = rec
	}
	s.twinVerdict = verdicts
	s.twinEvidence = evidence
	s.twinLoaded = true
	s.twinLoadErr = ""
	s.twinLoadedAt = time.Now().UTC()
	return true
}

// twinFamilyAffected reports whether a ticker belongs to a known dual-series family whose
// persisted verdicts are money-path safety evidence.
func twinFamilyAffected(platform, ticker string) bool {
	if platform != "kalshi" {
		return false
	}
	series := strings.ToUpper(strings.SplitN(ticker, "-", 2)[0])
	_, ok := twinSeriesSet[series]
	return ok
}

// twinKeyGate returns tkey only when it is a VERIFIED identical twin. Before the persisted
// registry has loaded successfully, known twin families fail closed: candidate restatement keys
// are returned conservatively so a transient DB read cannot permit doubled identical exposure.
// Once loaded, the normal operator rule resumes: unverified/ambiguous = DISTINCT (bettable).
func (s *Server) twinKeyGate(platform, ticker string) string {
	tkey := trueRestatementKey(platform, ticker)
	if tkey == "" {
		return ""
	}
	s.twinMu.Lock()
	loaded := s.twinLoaded && s.twinVerdict != nil
	verdict := s.twinVerdict[tkey]
	s.twinMu.Unlock()
	if (loaded && (verdict == "identical" || verdict == "stale")) ||
		(!loaded && twinFamilyAffected(platform, ticker)) {
		return tkey
	}
	return ""
}

type twinSweepCandidate struct {
	key      string
	tickers  map[string]bool
	priority int
	checked  time.Time
}

func twinSeriesPairObserved(tkey string, seriesOK map[string]bool) bool {
	parts := strings.SplitN(tkey, ":", 3)
	if len(parts) != 3 || parts[0] != "ktr" {
		return false
	}
	base := parts[1]
	return seriesOK[base] && seriesOK[base+"D"]
}

// twinSweep — background verifier: enumerate open markets across the dual-series families,
// group by trueRestatementKey, and revalidate a fair budget of groups against current venue
// documents. Stale/version-mismatched/member-changed evidence goes first; then the oldest valid
// document checks rotate, so a later payoff-document change cannot remain trusted forever.
func (s *Server) twinSweep(ctx context.Context) {
	if s.kal == nil {
		return
	}
	if !s.twinLoad(ctx) {
		return
	}
	groups := map[string]map[string]bool{}
	seriesOK := map[string]bool{}
	for _, ser := range twinSeries {
		ms, err := s.kal.MarketsBySeries(ctx, ser)
		if err != nil {
			continue // venue hiccup: verify what we can this pass
		}
		seriesOK[ser] = true
		for _, m := range ms {
			if k := trueRestatementKey("kalshi", m.Ticker); k != "" {
				if groups[k] == nil {
					groups[k] = map[string]bool{}
				}
				groups[k][m.Ticker] = true
			}
		}
	}

	var candidates []twinSweepCandidate
	var staleKeys []string
	s.twinMu.Lock()
	// Only a complete read of both series can prove that membership changed. A transient failure
	// of either list endpoint must preserve the last-good record, not manufacture staleness.
	for key, rec := range s.twinEvidence {
		if twinSeriesPairObserved(key, seriesOK) && !sameTwinMembers(rec.Members, twinMembers(groups[key])) {
			if s.twinVerdict[key] != "stale" {
				staleKeys = append(staleKeys, key)
			}
			s.twinVerdict[key] = "stale"
		}
	}
	for key, tickers := range groups {
		if len(tickers) < 2 || !twinSeriesPairObserved(key, seriesOK) {
			continue
		}
		rec, hasEvidence := s.twinEvidence[key]
		verdict, known := s.twinVerdict[key]
		membersMatch := hasEvidence && sameTwinMembers(rec.Members, twinMembers(tickers))
		priority := 1
		checked := rec.CheckedAt
		if !known || verdict == "stale" || !membersMatch {
			priority = 0
			if known && !membersMatch {
				s.twinVerdict[key] = "stale"
			}
		}
		candidates = append(candidates, twinSweepCandidate{
			key: key, tickers: tickers, priority: priority, checked: checked,
		})
	}
	s.twinMu.Unlock()
	// Persist invalidation before any slower document re-fetch. If the process exits during
	// revalidation, the next boot must not resurrect the superseded member set.
	for _, key := range staleKeys {
		_ = s.store.KVSet(ctx, "twinv:"+key, "stale")
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].priority != candidates[j].priority {
			return candidates[i].priority < candidates[j].priority
		}
		if !candidates[i].checked.Equal(candidates[j].checked) {
			return candidates[i].checked.Before(candidates[j].checked)
		}
		return candidates[i].key < candidates[j].key
	})
	if len(candidates) > twinSweepBudget {
		candidates = candidates[:twinSweepBudget]
	}
	for _, candidate := range candidates {
		rec, ok := s.twinVerify(ctx, candidate.tickers)
		if !ok {
			continue // fetch/shape failure: stale stays fail-closed; new stays unverified
		}
		// Windows wall-clock reads can repeat within a very fast revalidation. Preserve a strictly
		// increasing evidence clock so a changed document cannot be indistinguishable from the
		// superseded check in storage, audit output, or deterministic tests.
		if !candidate.checked.IsZero() && !rec.CheckedAt.After(candidate.checked) {
			rec.CheckedAt = candidate.checked.Add(time.Nanosecond)
		}
		raw, err := json.Marshal(rec)
		if err != nil || s.store.KVSet(ctx, "twinv:"+candidate.key, string(raw)) != nil {
			s.twinMu.Lock()
			s.twinVerdict[candidate.key] = "stale"
			s.twinMu.Unlock()
			continue
		}
		s.twinMu.Lock()
		s.twinVerdict[candidate.key] = rec.Verdict
		if s.twinEvidence == nil {
			s.twinEvidence = map[string]twinVerdictRecord{}
		}
		s.twinEvidence[candidate.key] = rec
		s.twinMu.Unlock()
	}
}

func twinPayoffFieldPresent(v any) bool {
	if v == nil {
		return false
	}
	if text, ok := v.(string); ok {
		return strings.TrimSpace(text) != ""
	}
	return true // numeric zero is valid strike evidence; absence/null is handled above
}

// twinRecordFromDocuments compares only payoff-defining fields and hashes their canonical
// document projection. Every member must expose at least one substantive payoff field; two empty
// response shapes are insufficient evidence and can never become "identical".
func twinRecordFromDocuments(docs map[string]map[string]any, checkedAt time.Time) (twinVerdictRecord, bool) {
	if checkedAt.IsZero() {
		return twinVerdictRecord{}, false
	}
	tickers := make(map[string]bool, len(docs))
	for ticker := range docs {
		tickers[ticker] = true
	}
	members := twinMembers(tickers)
	if len(members) < 2 {
		return twinVerdictRecord{}, false
	}
	h := sha256.New()
	ref := ""
	verdict := "identical"
	minFields := len(twinCmpFields) + 1
	for _, ticker := range members {
		doc := docs[ticker]
		if doc == nil {
			return twinVerdictRecord{}, false
		}
		cmp := make(map[string]any, len(twinCmpFields))
		fields := 0
		for _, field := range twinCmpFields {
			value := doc[field]
			cmp[field] = value
			if twinPayoffFieldPresent(value) {
				fields++
			}
		}
		if fields == 0 {
			return twinVerdictRecord{}, false
		}
		if fields < minFields {
			minFields = fields
		}
		canonical, err := json.Marshal(cmp)
		if err != nil {
			return twinVerdictRecord{}, false
		}
		_, _ = h.Write([]byte(ticker))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write(canonical)
		_, _ = h.Write([]byte{0})
		if ref == "" {
			ref = string(canonical)
		} else if ref != string(canonical) {
			verdict = "distinct"
		}
	}
	return twinVerdictRecord{
		Version: twinEvidenceVersion, Verdict: verdict, Members: members,
		DocumentHash: hex.EncodeToString(h.Sum(nil)), PayoffFields: minFields,
		CheckedAt: checkedAt.UTC(),
	}, true
}

// twinVerify fetches every member market document and produces versioned evidence.
func (s *Server) twinVerify(ctx context.Context, tickers map[string]bool) (twinVerdictRecord, bool) {
	docs := make(map[string]map[string]any, len(tickers))
	for _, ticker := range twinMembers(tickers) {
		cctx, cancel := context.WithTimeout(ctx, 8*time.Second)
		raw, err := s.kal.RawGET(cctx, "/markets/"+ticker)
		cancel()
		if err != nil {
			return twinVerdictRecord{}, false // no answer — not cacheable
		}
		var doc struct {
			Market map[string]any `json:"market"`
		}
		if json.Unmarshal(raw, &doc) != nil || doc.Market == nil {
			return twinVerdictRecord{}, false
		}
		docs[ticker] = doc.Market
	}
	return twinRecordFromDocuments(docs, time.Now().UTC())
}

// twinStats — registry telemetry for the pass report / API.
func (s *Server) twinStats() map[string]any {
	s.twinMu.Lock()
	defer s.twinMu.Unlock()
	ident, dist, stale := 0, 0, 0
	for _, v := range s.twinVerdict {
		switch v {
		case "identical":
			ident++
		case "distinct":
			dist++
		default:
			stale++
		}
	}
	state := "pending"
	if s.twinLoading {
		state = "loading"
	} else if s.twinLoaded {
		state = "loaded"
	} else if s.twinLoadErr != "" {
		state = "error"
	}
	lastAttempt, loadedAt := "", ""
	if !s.twinLoadAt.IsZero() {
		lastAttempt = s.twinLoadAt.UTC().Format(time.RFC3339Nano)
	}
	if !s.twinLoadedAt.IsZero() {
		loadedAt = s.twinLoadedAt.UTC().Format(time.RFC3339Nano)
	}
	return map[string]any{
		"verified_identical": ident,
		"verified_distinct":  dist,
		"stale_evidence":     stale,
		"evidence_version":   twinEvidenceVersion,
		"load_state":         state,
		"load_error":         s.twinLoadErr,
		"load_attempts":      s.twinLoadAttempts,
		"last_load_attempt":  lastAttempt,
		"loaded_at":          loadedAt,
		"load_fail_closed":   !s.twinLoaded,
	}
}
