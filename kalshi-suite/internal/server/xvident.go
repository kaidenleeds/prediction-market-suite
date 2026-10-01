package server

// xvident.go — R107 STRUCTURAL CROSS-VENUE IDENTITY for NON-SPORTS genres (operator order:
// gameident proved sports; everything else still joins kalshi↔poly-int by title fuzz).
//
// Anchors come from venue metadata/identifiers ONLY — Kalshi series/event/strike ticker fields
// and Polymarket canonical MARKET slugs under STRICT grammars. NEVER free-text title fuzz: a
// slug/ticker that doesn't match a grammar is NOT anchored (R90 doctrine — a missing observation
// beats a fabricated join). Every grammar below was written against REAL rows sampled from the
// market_catalog DB copy + live gamma probes, 2026-07-07:
//
// CRYPTO — canonical key `crypto|<UND>|<windowEndUTC RFC3339>|<strike descriptor>|<dir>`:
//
//	Kalshi thresh   KXBTCD-26JUL0713-T65899.99 ("$65,900 or above", closes 17:05Z) — event seg
//	                yyMONddHH is the observation hour in ET; the venue's x99.99 T-value means
//	                "the next round strike or above", so the descriptor is T<value+1ulp>:
//	                T65899.99→T65900 · KXXRPD T1.7999→T1.8 · KXDOGED T0.1949999→T0.195.
//	Kalshi range    KXBTC-26JUL0713-B62550 ("$62,500 to 62,599.99") → B62550 (venue bucket id).
//	                Range-series T-tails (KXETH-26JUL0713-T1030 = "$1,029.99 or BELOW") have
//	                direction only in the subtitle, not the ticker → REFUSED, never guessed.
//	Kalshi up/down  KXBTC15M-26JUL071215-15 ("BTC price up in next 15 mins?", closes 16:20Z) —
//	                event seg yyMONddHHMM is the window END in ET → descriptor updown15m, dir up.
//	poly above      bitcoin-above-56k-on-july-10-2026 · xrp-above-0pt6-on-july-8-2026 →
//	                T56000/T0.6 (52k→52000, 0pt6→0.6), end = endDate (noon-ET convention),
//	                UTC date must equal the slug date or the row is refused.
//	poly up/down    btc-updown-15m-1783440000 (epoch = window START; end = epoch+15m = its
//	                close 16:15Z) · bitcoin-up-or-down-july-7-2026-12pm-et (hour window; end =
//	                start+1h = 17:00Z) · bitcoin-up-or-down-on-july-8-2026 (daily, end=endDate).
//	poly range      will-the-price-of-bitcoin-be-between-52000-54000-on-july-10-2026 → R52000-54000;
//	                …-less-than-52000-… → U52000 (down); greater-than → G<v> (up). Kalshi buckets
//	                are $100-wide vs poly's $2,000 — descriptors never collide, so no false joins.
//	poly touch      will-bitcoin-reach-95000-by-december-31-2026-from-june-8 → H95000 (dip-to →
//	                L<v>). No Kalshi counterpart series in the catalog → anchor-only.
//
//	The REAL cross-venue join surfaces: KXxxxD T-strikes ↔ poly "<coin>-above-*" (same UTC end +
//	same normalized strike), and KX<coin>15M ↔ poly "<coin>-updown-15m-<epoch>" (same UTC end).
//
// WEATHER — key `wx|<STATION>|<metric>|<date>|<bin>`:
//
//	Kalshi          KXHIGHMIA-26JUL08-B84.5 (bins are 2°F wide: 84-85°) · KXHIGHT<X>/KXLOWT<X>
//	                series batch (KXHIGHTATL, KXLOWTSEA…) · KXTEMPNYCH (hourly NYC threshold).
//	                B<v.5> → bin "<v-.5>-<v+.5>F"; T-tails ("<95°" vs ">78°") have direction only
//	                in the subtitle → bin "T<v>" which by construction never equals a poly tail.
//	poly            highest-temperature-in-miami-on-july-8-2026-84-85f → 84-85F (US cities are °F
//	                with the same visible 2° bins, but NOT settlement-equivalent: Kalshi resolves
//	                from the NWS Climatological Report while Poly-int resolves from Wunderground) ·
//	                highest-temperature-in-london-on-july-9-2026-30corbelow → T30C-below
//	                (international cities are °C, exact-degree bins — anchor-only).
//	City → station: nyc→NYC miami→MIA chicago→CHI austin→AUS… (the venue's own KXHIGH* codes);
//	                unknown cities anchor verbatim (LONDON, SEOUL…) and join nothing.
//
// ECON — key `econ|<series>|<period>` (ladder-level: every rung shares the key):
//
//	Kalshi          KXCPIYOY-26JUN-T3.9 → econ|us-cpi-yoy|2026-06 · KXCBDECISIONEU-26JUL23-H25 →
//	                econ|cb-eu|2026-07 (month grain: one policy meeting/month) ·
//	                KXJOBLESSCLAIMS-26JUL09-225000 → date grain · KXUE-AUS26JUN-4.0 →
//	                econ|unemployment-aus|2026-06 (country-prefixed event seg).
//	poly            will-the-fed-decrease-interest-rates-by-25-bps-after-the-july-2026-meeting →
//	                econ|cb-us|2026-07 · will-annual-inflation-be-3pt9-in-june-<noise> →
//	                econ|us-cpi-yoy|2026-06 (period year from endDate: back a year when the
//	                report month exceeds the close month).
//	KXFED/KXFEDDECISION → cb-us is pre-mapped so the fed-decision join lights up the moment the
//	venue re-lists the series (absent from the current 90d catalog window).
//
// POLITICS — key `pol|<race-or-person>|<year>` — CONSERVATIVE: the current Kalshi catalog window
// carries no parseable race identifiers (KXAPRPOTUS is an approval ladder), so only three
// unambiguous poly grammars ship (next-prime-minister-of-<country>, <state>-governor-election-
// winner, <party>-presidential-nominee-<year>) and they are anchor-only; everything else is
// skipped rather than mis-anchored.
//
// Consumers (wiring in server.go): bestKalshiMatchFam and consensusKalshiMatch consult
// xvTwin FIRST; the existing fuzzy path stays as tagged fallback (xvNoteFuzz), mirroring
// gameident's struct-first/fuzz-tagged pattern and its giNoteStruct/giNoteFuzz counters.

import (
	"context"
	"fmt"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ── registry ────────────────────────────────────────────────────────────────────────────────────

// xvEntry is one canonical identity: kind + the per-venue market ids that anchor to it.
type xvEntry struct {
	kind string              // crypto | wx | econ | pol
	ids  map[string][]string // venue → market ids (ladder-level keys carry many per venue)
}

// xvKindStat is the per-kind coverage snapshot recomputed on every rebuild.
type xvKindStat struct {
	KalMkts, PolyMkts int      // anchored market ids per venue
	KalKeys, PolyKeys int      // distinct canonical keys per venue
	JoinedKeys        int      // keys carrying BOTH venues
	Examples          []string // up to 5 joined keys (sorted — deterministic payload)
}

// xvState is the whole registry (one lock: lookups are two map hits; rebuild swaps whole maps).
type xvState struct {
	mu          sync.Mutex
	byKey       map[string]*xvEntry
	keyOf       map[string]string // "venue|id" → canonical key (poly conditionIds alias here too)
	stats       map[string]*xvKindStat
	orderedKeys []string // immutable sorted snapshot for bounded fair-rotation consumers
	kalSeen     int      // rows considered on the last rebuild (live board + catalog breadth)
	polySeen    int
	builtAt     time.Time
	building    bool
	// serve-rate counters (lifetime, survive rebuilds — gameident's structHits/fuzzHits mirror)
	structHits, fuzzHits map[string]int64
	// liveAnchors (R119): deterministically-fetched poly live-window markets registered via
	// xvAnchorPolyLive, kept on a TTL so the 5-min rebuild's map swap re-ingests them instead of
	// wiping them. Without this the rebuild snapshot (and its stats) never held the short-lived
	// windows — joined_keys read 0 even while live joins were being served between ticks.
	liveAnchors map[string]xvLiveAnchor // lower(slug) → anchor
}

// xvLiveAnchor is one live-registered poly market retained across rebuilds until expiry.
type xvLiveAnchor struct {
	key, kind, conditionID string
	at                     time.Time
}

// xvLiveAnchorTTL: how long a live-registered poly window stays re-ingestable. Longest live
// up/down window fetched is 1d; 26h covers it with slack, and expired windows are settled
// markets anyway (joins on them are historical, not actionable).
const xvLiveAnchorTTL = 26 * time.Hour

func newXVState() *xvState {
	return &xvState{byKey: map[string]*xvEntry{}, keyOf: map[string]string{},
		stats: map[string]*xvKindStat{}, structHits: map[string]int64{}, fuzzHits: map[string]int64{},
		liveAnchors: map[string]xvLiveAnchor{}}
}

var xvReg = newXVState()

// add registers one venue market id under a canonical key. First anchor wins for an id (an id
// can carry ONE identity); ids append per venue under the key.
func (st *xvState) add(venue, id, key, kind string) {
	st.mu.Lock()
	st.addLocked(venue, id, key, kind)
	st.mu.Unlock()
}

func (st *xvState) addLocked(venue, id, key, kind string) {
	if venue == "" || id == "" || key == "" {
		return
	}
	vk := venue + "|" + id
	if _, dup := st.keyOf[vk]; dup {
		return
	}
	st.keyOf[vk] = key
	e := st.byKey[key]
	if e == nil {
		e = &xvEntry{kind: kind, ids: map[string][]string{}}
		st.byKey[key] = e
	}
	e.ids[venue] = append(e.ids[venue], id)
}

// xvAnchorPolyLive (R118) registers a deterministically-fetched poly market (the
// CryptoUpDownMarket slug path) into the LIVE registry so xvTwin can serve current-window
// conditionIds the gamma top-volume list hasn't ranked yet. R117's xinv-pcrypto bridge logged
// ZERO rows because the 5-min rebuild only ingests CachedMarkets (top-volume), which a 15-min
// window enters only late/after its life — the two venues' snapshots never held the same window
// at the same time (kalshi_anchored 2,634 vs polyint_anchored 97, joined_keys=0). Called every
// consensus tick, so the rebuild's map swap wiping these entries is harmless: they re-register
// before each xvTwin call. Same strict xvPolyAnchor grammar — no fabricated joins (R90 doctrine).
func xvAnchorPolyLive(slug, endDate, conditionID string) {
	key, kind, ok := xvPolyAnchor(slug, endDate)
	if !ok {
		return
	}
	xvReg.mu.Lock()
	xvReg.addLocked("polymarket", strings.ToLower(slug), key, kind)
	if conditionID != "" {
		if _, dup := xvReg.keyOf["polymarket|"+conditionID]; !dup {
			xvReg.keyOf["polymarket|"+conditionID] = key
		}
	}
	// R119: retain on the TTL side-map so the next rebuild re-ingests this window and the
	// coverage stats reflect the joins actually being served.
	xvReg.liveAnchors[strings.ToLower(slug)] = xvLiveAnchor{key: key, kind: kind, conditionID: conditionID, at: time.Now()}
	xvReg.mu.Unlock()
}

// reset clears identity maps (rebuild + tests). Serve-rate counters intentionally survive.
func (st *xvState) reset() {
	st.mu.Lock()
	st.byKey, st.keyOf, st.stats, st.orderedKeys = map[string]*xvEntry{}, map[string]string{}, map[string]*xvKindStat{}, nil
	st.liveAnchors = map[string]xvLiveAnchor{}
	st.builtAt = time.Time{}
	st.mu.Unlock()
}

// xvMemberRulesEquivalent is a legacy-named predicate-candidate boundary, not a final rule
// certificate. Crypto pins the same strike/window/direction in the key and may be compared in the
// directional-only tier; every risk-free lock still needs a current normalized rule certificate.
// Weather deliberately does NOT pass: the Jul 9, 2026 live
// grades proved that same station/date/bin can settle differently because Kalshi's authority is
// the NWS Climatological Report while Poly-int's is Wunderground. Econ/politics are ladder-level.
// Future kinds remain anchor-only until their settlement authority is explicitly verified.
func xvMemberRulesEquivalent(kind string) bool {
	return strings.EqualFold(strings.TrimSpace(kind), "crypto")
}

// xvKeyRulesEquivalent is the predicate-level safety boundary. The broader crypto family remains
// structurally useful, but the settled lock ledger has repeatedly shown that Kalshi and Poly-int
// 15-minute up/down contracts can pay the same side on both venues. Their displayed window and
// direction therefore do not establish equivalent settlement semantics. Keep those instruments
// anchored for venue-local research while refusing every cross-venue twin consumer. A future
// independently reviewed rule model may narrow or remove this fail-closed exception.
func xvKeyRulesEquivalent(kind, key string) bool {
	if !xvMemberRulesEquivalent(kind) {
		return false
	}
	return !strings.Contains(strings.ToLower(strings.TrimSpace(key)), "|updown15m|")
}

// xvAnchorKind reports whether an id is structurally anchored even when it is intentionally barred
// from member-level matching. Callers that otherwise fall back to fuzzy titles use this to refuse a
// known weather/econ/politics anchor instead of routing around the settlement-rules boundary.
func xvAnchorKind(venue, id string) (kind string, ok bool) {
	_, kind, ok = xvAnchorIdentity(venue, id)
	return kind, ok
}

// xvAnchorIdentity returns the full structural key as well as its broad kind. Consumers that
// enforce predicate-specific settlement exclusions must inspect the key rather than treating an
// entire broad family as homogeneous.
func xvAnchorIdentity(venue, id string) (key, kind string, ok bool) {
	venue = strings.ToLower(strings.TrimSpace(venue))
	id = strings.TrimSpace(id)
	if venue == "" || id == "" {
		return "", "", false
	}
	xvReg.mu.Lock()
	defer xvReg.mu.Unlock()
	key = xvReg.keyOf[venue+"|"+id]
	if key == "" {
		key = xvReg.keyOf[venue+"|"+strings.ToLower(id)]
	}
	e := xvReg.byKey[key]
	if e == nil || e.kind == "" {
		return "", "", false
	}
	return key, e.kind, true
}

// xvTwin maps a venue market id to its OTHER-venue exact-predicate candidate. The legacy helper
// name above must not be read as lock-safe equivalence: currently crypto pins
// strike/window/direction, while a separate current certificate owns material settlement rules.
// Weather/econ/pol remain useful structural
// anchors for coverage, but never become twins. Econ/pol keys are LADDER-level by design (a Kalshi "above 3.9%"
// rung and a poly "3.9% exact" bin share econ|us-cpi-yoy|2026-06 but are DIFFERENT contracts —
// the identifiers carry no bin semantics), so those kinds join for coverage only and always
// refuse here. Within an eligible kind, the other venue must hold EXACTLY ONE id under the key —
// several candidates is ambiguous and REFUSES (R90 doctrine), it never picks.
func xvTwin(venue, id string) (otherVenue, otherID, kind string, ok bool) {
	venue = strings.ToLower(strings.TrimSpace(venue))
	id = strings.TrimSpace(id)
	if venue == "" || id == "" {
		return "", "", "", false
	}
	xvReg.mu.Lock()
	defer xvReg.mu.Unlock()
	key := xvReg.keyOf[venue+"|"+id]
	if key == "" {
		key = xvReg.keyOf[venue+"|"+strings.ToLower(id)]
	}
	e := xvReg.byKey[key]
	if e == nil {
		return "", "", "", false
	}
	if !xvKeyRulesEquivalent(e.kind, key) {
		return "", "", "", false // structural anchor only; settlement/member equivalence unproven
	}
	for v, ids := range e.ids {
		if v == venue {
			continue
		}
		if len(ids) == 1 {
			return v, ids[0], e.kind, true
		}
	}
	return "", "", "", false
}

// xvNoteStruct / xvNoteFuzz — struct-vs-fuzz serve counters per matcher family (giNoteStruct/
// giNoteFuzz mirrors; the /api/xvident payload exposes both so the auditor can grade how often
// non-sports joins are still fuzz-served).
func xvNoteStruct(family string) {
	xvReg.mu.Lock()
	xvReg.structHits[family]++
	xvReg.mu.Unlock()
}

func xvNoteFuzz(family string) {
	xvReg.mu.Lock()
	xvReg.fuzzHits[family]++
	xvReg.mu.Unlock()
}

// ── Kalshi anchors (ticker grammars) ────────────────────────────────────────────────────────────

// xvKalCrypto: crypto series → underlying + ladder style (census over the 90d catalog window;
// counts in the R107 pass notes). "thresh" = daily/hourly T-threshold ladders; "range" =
// B-bucket ladders; "updown15" = 15-minute up/down.
var xvKalCrypto = map[string]struct{ und, style string }{
	"KXBTCD": {"BTC", "thresh"}, "KXETHD": {"ETH", "thresh"}, "KXSOLD": {"SOL", "thresh"},
	"KXXRPD": {"XRP", "thresh"}, "KXDOGED": {"DOGE", "thresh"}, "KXBNBD": {"BNB", "thresh"},
	"KXHYPED": {"HYPE", "thresh"}, "KXSHIBAD": {"SHIBA", "thresh"},
	"KXBTC": {"BTC", "range"}, "KXETH": {"ETH", "range"}, "KXSOLE": {"SOL", "range"},
	"KXXRP": {"XRP", "range"}, "KXDOGE": {"DOGE", "range"}, "KXBNB": {"BNB", "range"},
	"KXHYPE": {"HYPE", "range"}, "KXSHIBA": {"SHIBA", "range"},
	"KXBTC15M": {"BTC", "updown15"}, "KXETH15M": {"ETH", "updown15"}, "KXSOL15M": {"SOL", "updown15"},
	"KXXRP15M": {"XRP", "updown15"}, "KXDOGE15M": {"DOGE", "updown15"}, "KXBNB15M": {"BNB", "updown15"},
	"KXHYPE15M": {"HYPE", "updown15"}, "KXNEAR15M": {"NEAR", "updown15"}, "KXZEC15M": {"ZEC", "updown15"},
}

// xvKalWxStations: the venue's own city codes (KXHIGH*/KXLOWT* series census). NY normalizes to
// NYC so KXHIGHNY and KXLOWTNYC land on one station.
var xvKalWxStations = map[string]string{
	"NY": "NYC", "NYC": "NYC", "CHI": "CHI", "AUS": "AUS", "DEN": "DEN", "LAX": "LAX",
	"MIA": "MIA", "PHIL": "PHIL", "ATL": "ATL", "BOS": "BOS", "DAL": "DAL", "DC": "DC",
	"HOU": "HOU", "LV": "LV", "MIN": "MIN", "NOLA": "NOLA", "OKC": "OKC", "PHX": "PHX",
	"SATX": "SATX", "SEA": "SEA", "SFO": "SFO",
}

// xvKalEcon: macro series → canonical econ series id + event-segment grain. Discovered from the
// live catalog (there is no KXNFP/KXGDP/KXFEDDECISION in the 90d window — KXFED* rows are
// pre-mapped so the poly fed-decision join lights up if/when the venue lists them).
var xvKalEcon = map[string]struct{ id, grain string }{
	"KXCPI": {"us-cpi-mom", "yymon"}, "KXCPICORE": {"us-cpi-core-mom", "yymon"},
	"KXCPIYOY": {"us-cpi-yoy", "yymon"}, "KXCPICOREYOY": {"us-cpi-core-yoy", "yymon"},
	"KXFED": {"cb-us", "month"}, "KXFEDDECISION": {"cb-us", "month"},
	"KXCBDECISIONEU": {"cb-eu", "month"}, "KXCBDECISIONCANADA": {"cb-canada", "month"},
	"KXCBDECISIONKOREA": {"cb-korea", "month"}, "KXCBDECISIONRUSSIA": {"cb-russia", "month"},
	"KXCBDISRAEL": {"cb-israel", "month"}, "KXCBDSA": {"cb-southafrica", "month"},
	"KXJOBLESSCLAIMS": {"us-jobless-claims", "date"}, "KXUSRETAIL": {"us-retail-mom", "date"},
	"KXUSPPIYOY": {"us-ppi-yoy", "date"}, "KXHOUSINGSTART": {"us-housing-starts", "date"},
	"KXEHSALES": {"us-existing-home-sales", "date"}, "KXBUILDPERMS": {"us-building-permits", "date"},
	"KXCHGDPYOY": {"china-gdp-yoy", "date"},
	"KXUST10A":   {"us-10y-yield", "date"}, "KXUST2A": {"us-2y-yield", "date"}, "KXUST30A": {"us-30y-yield", "date"},
	"KXUE": {"unemployment", "ue"},
}

var (
	xvMonAlt      = `JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC`
	xvKalEvDayRe  = regexp.MustCompile(`^(\d{2})(` + xvMonAlt + `)(\d{2})$`)               // 26JUL08
	xvKalEvHourRe = regexp.MustCompile(`^(\d{2})(` + xvMonAlt + `)(\d{2})(\d{2})$`)        // 26JUL0713
	xvKalEvQHRe   = regexp.MustCompile(`^(\d{2})(` + xvMonAlt + `)(\d{2})(\d{2})(\d{2})$`) // 26JUL071215
	xvKalEvMonRe  = regexp.MustCompile(`^(\d{2})(` + xvMonAlt + `)$`)                      // 26JUN
	xvKalEvUERe   = regexp.MustCompile(`^([A-Z]{2,6})(\d{2})(` + xvMonAlt + `)$`)          // AUS26JUN
	xvNumRe       = regexp.MustCompile(`^\d+(?:\.\d+)?$`)
)

// xvETTime builds the UTC instant for a Kalshi ET-embedded wall time (venue anchor —
// probes: KXBTCD-26JUL0713 closes 17:05Z = 1pm EDT + 5min settle buffer).
func xvETTime(yy, mon, dd, hh, mm string) (time.Time, bool) {
	et := etLocation()
	if et == nil {
		return time.Time{}, false
	}
	y, _ := strconv.Atoi(yy)
	d, _ := strconv.Atoi(dd)
	h, _ := strconv.Atoi(hh)
	mi, _ := strconv.Atoi(mm)
	m, okM := kalshiMonths[mon]
	if !okM || d < 1 || d > 31 || h > 23 || mi > 59 {
		return time.Time{}, false
	}
	return time.Date(2000+y, m, d, h, mi, 0, 0, et).UTC(), true
}

// xvStrikeUp bumps a threshold value by one unit in its own last decimal place — the venue's
// x99.99 convention read back as the round strike it means ("$65,900 or above" = T65899.99):
// 65899.99→65900 · 1.7999→1.8 · 0.1949999→0.195 · 74.9999→75 · 599.99→600. String/integer math
// only (no float rounding); "" = not a plain positive decimal.
func xvStrikeUp(v string) string {
	if v == "" || !xvNumRe.MatchString(v) {
		return ""
	}
	whole, frac := v, ""
	if i := strings.IndexByte(v, '.'); i >= 0 {
		whole, frac = v[:i], v[i+1:]
	}
	n, err := strconv.ParseUint(whole+frac, 10, 64)
	if err != nil {
		return ""
	}
	n++
	out := strconv.FormatUint(n, 10)
	if d := len(frac); d > 0 {
		for len(out) <= d {
			out = "0" + out
		}
		out = out[:len(out)-d] + "." + out[len(out)-d:]
		out = strings.TrimRight(out, "0")
		out = strings.TrimSuffix(out, ".")
	}
	return out
}

// xvKalWxSeries resolves a weather series ticker to (station, metric). The venue ships two
// vintages (KXHIGHMIA and KXHIGHTATL both exist) — known codes match directly or behind the 'T',
// and an UNSEEN T-prefixed code still parses (future city = new node, join-free until the
// station table learns it).
func xvKalWxSeries(series string) (station, metric string, ok bool) {
	var rest string
	switch {
	case series == "KXTEMPNYCH": // hourly NYC temp threshold ladder (792 rows) — anchor-only
		return "NYC", "temp", true
	case strings.HasPrefix(series, "KXHIGH"):
		rest, metric = series[len("KXHIGH"):], "high"
	case strings.HasPrefix(series, "KXLOW"):
		rest, metric = series[len("KXLOW"):], "low"
	default:
		return "", "", false
	}
	if st, hit := xvKalWxStations[rest]; hit {
		return st, metric, true
	}
	if len(rest) > 1 && rest[0] == 'T' {
		if st, hit := xvKalWxStations[rest[1:]]; hit {
			return st, metric, true
		}
		if len(rest) > 3 {
			return rest[1:], metric, true
		}
		return "", "", false
	}
	if len(rest) >= 2 {
		return rest, metric, true
	}
	return "", "", false
}

// xvKalWxBin canonicalizes a Kalshi temperature strike segment. B<v.5> = the venue's 2°F bin
// ("84-85°") → "84-85F", which is EXACTLY poly's US-city bin token. T-tails carry their
// direction only in the subtitle (probed: KXHIGHAUS T95 = "<95°" but KXLOWTAUS T78 = ">78°") →
// "T<v>", a form that by construction never equals a poly tail ("T81F-below") — tails anchor
// but never join, rather than join wrong.
func xvKalWxBin(seg string) (string, bool) {
	if len(seg) < 2 {
		return "", false
	}
	val := seg[1:]
	if !xvNumRe.MatchString(val) {
		return "", false
	}
	switch seg[0] {
	case 'B':
		if f, err := strconv.ParseFloat(val, 64); err == nil {
			lo := f - 0.5
			if lo == float64(int64(lo)) { // x.5 midpoint → the venue's integer 2° bin
				return fmt.Sprintf("%d-%dF", int64(lo), int64(f+0.5)), true
			}
		}
		return "B" + val, true
	case 'T':
		return "T" + val, true
	}
	return "", false
}

// xvKalshiAnchor parses one Kalshi ticker into its canonical non-sports identity.
// Pure (ticker-only): the event segment already embeds the venue's ET wall time.
func xvKalshiAnchor(ticker string) (key, kind string, ok bool) {
	tk := strings.ToUpper(strings.TrimSpace(ticker))
	parts := strings.Split(tk, "-")
	if len(parts) < 2 || parts[0] == "" {
		return "", "", false
	}
	series := parts[0]

	if cs, hit := xvKalCrypto[series]; hit {
		if len(parts) != 3 || len(parts[2]) < 2 {
			return "", "", false
		}
		suffix := parts[2]
		switch cs.style {
		case "thresh":
			m := xvKalEvHourRe.FindStringSubmatch(parts[1])
			if m == nil || suffix[0] != 'T' {
				return "", "", false
			}
			end, okT := xvETTime(m[1], m[2], m[3], m[4], "00")
			if !okT {
				return "", "", false
			}
			strike := xvStrikeUp(suffix[1:])
			if strike == "" {
				return "", "", false
			}
			return "crypto|" + cs.und + "|" + end.Format(time.RFC3339) + "|T" + strike + "|up", "crypto", true
		case "range":
			m := xvKalEvHourRe.FindStringSubmatch(parts[1])
			if m == nil || suffix[0] != 'B' || !xvNumRe.MatchString(suffix[1:]) {
				return "", "", false // T-tails on range series: direction not in the ticker — REFUSE
			}
			end, okT := xvETTime(m[1], m[2], m[3], m[4], "00")
			if !okT {
				return "", "", false
			}
			return "crypto|" + cs.und + "|" + end.Format(time.RFC3339) + "|B" + suffix[1:] + "|range", "crypto", true
		case "updown15":
			m := xvKalEvQHRe.FindStringSubmatch(parts[1])
			if m == nil {
				return "", "", false
			}
			end, okT := xvETTime(m[1], m[2], m[3], m[4], m[5]) // embedded time = window END (close probe: end+5min)
			if !okT {
				return "", "", false
			}
			return "crypto|" + cs.und + "|" + end.Format(time.RFC3339) + "|updown15m|up", "crypto", true
		}
		return "", "", false
	}

	if station, metric, hit := xvKalWxSeries(series); hit {
		if len(parts) != 3 {
			return "", "", false
		}
		if series == "KXTEMPNYCH" { // hourly threshold: yyMONddHH event + T-strike
			m := xvKalEvHourRe.FindStringSubmatch(parts[1])
			if m == nil || parts[2][0] != 'T' || !xvNumRe.MatchString(parts[2][1:]) {
				return "", "", false
			}
			end, okT := xvETTime(m[1], m[2], m[3], m[4], "00")
			if !okT {
				return "", "", false
			}
			return "wx|" + station + "|" + metric + "|" + end.Format(time.RFC3339) + "|T" + parts[2][1:], "wx", true
		}
		m := xvKalEvDayRe.FindStringSubmatch(parts[1])
		if m == nil {
			return "", "", false
		}
		mon, okM := kalshiMonths[m[2]]
		if !okM {
			return "", "", false
		}
		bin, okB := xvKalWxBin(parts[2])
		if !okB {
			return "", "", false
		}
		date := fmt.Sprintf("20%s-%02d-%s", m[1], int(mon), m[3])
		return "wx|" + station + "|" + metric + "|" + date + "|" + bin, "wx", true
	}

	if es, hit := xvKalEcon[series]; hit {
		id, period := es.id, ""
		switch es.grain {
		case "yymon": // KXCPIYOY-26JUN → 2026-06
			m := xvKalEvMonRe.FindStringSubmatch(parts[1])
			if m == nil {
				return "", "", false
			}
			period = fmt.Sprintf("20%s-%02d", m[1], int(kalshiMonths[m[2]]))
		case "month": // KXCBDECISIONEU-26JUL23 → 2026-07 (one policy meeting a month — poly's grain)
			m := xvKalEvDayRe.FindStringSubmatch(parts[1])
			if m == nil {
				return "", "", false
			}
			period = fmt.Sprintf("20%s-%02d", m[1], int(kalshiMonths[m[2]]))
		case "date": // KXJOBLESSCLAIMS-26JUL09 → 2026-07-09
			m := xvKalEvDayRe.FindStringSubmatch(parts[1])
			if m == nil {
				return "", "", false
			}
			period = fmt.Sprintf("20%s-%02d-%s", m[1], int(kalshiMonths[m[2]]), m[3])
		case "ue": // KXUE-AUS26JUN → unemployment-aus | 2026-06
			m := xvKalEvUERe.FindStringSubmatch(parts[1])
			if m == nil {
				return "", "", false
			}
			id = id + "-" + strings.ToLower(m[1])
			period = fmt.Sprintf("20%s-%02d", m[2], int(kalshiMonths[m[3]]))
		default:
			return "", "", false
		}
		return "econ|" + id + "|" + period, "econ", true
	}

	return "", "", false
}

// ── Polymarket anchors (strict market-slug grammars) ────────────────────────────────────────────

var xvPolyCoin = map[string]string{
	"bitcoin": "BTC", "btc": "BTC", "ethereum": "ETH", "eth": "ETH", "solana": "SOL", "sol": "SOL",
	"xrp": "XRP", "dogecoin": "DOGE", "doge": "DOGE", "bnb": "BNB", "hype": "HYPE",
	"near": "NEAR", "zec": "ZEC", "shiba": "SHIBA",
}

var xvPolyMonths = map[string]time.Month{
	"january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
	"july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12,
}

const (
	xvCoinAlt  = `bitcoin|btc|ethereum|eth|solana|sol|xrp|dogecoin|doge|bnb|hype|near|zec|shiba`
	xvMonthAlt = `january|february|march|april|may|june|july|august|september|october|november|december`
)

var (
	// crypto — real slugs in the header comment
	xvRePolyUpdownEpoch = regexp.MustCompile(`^(` + xvCoinAlt + `)-updown-(5m|15m|4h)-(\d{9,11})$`)
	xvRePolyUpdownDay   = regexp.MustCompile(`^(` + xvCoinAlt + `)-up-or-down-on-(` + xvMonthAlt + `)-(\d{1,2})-(\d{4})$`)
	xvRePolyUpdownHour  = regexp.MustCompile(`^(` + xvCoinAlt + `)-up-or-down-(` + xvMonthAlt + `)-(\d{1,2})-(\d{4})-(\d{1,2})(am|pm)-et$`)
	xvRePolyAbove       = regexp.MustCompile(`^(` + xvCoinAlt + `)-above-(\d+(?:pt\d+)?k?)-on-(` + xvMonthAlt + `)-(\d{1,2})-(\d{4})$`)
	xvRePolyBetween     = regexp.MustCompile(`^will-the-price-of-(` + xvCoinAlt + `)-be-between-(\d+)-(\d+)-on-(` + xvMonthAlt + `)-(\d{1,2})-(\d{4})$`)
	xvRePolyLessGreater = regexp.MustCompile(`^will-the-price-of-(` + xvCoinAlt + `)-be-(less|greater)-than-(\d+)-on-(` + xvMonthAlt + `)-(\d{1,2})-(\d{4})$`)
	xvRePolyTouch       = regexp.MustCompile(`^will-(` + xvCoinAlt + `)-(reach|dip-to)-(\d+(?:pt\d+)?k?)-by-(` + xvMonthAlt + `)-(\d{1,2})-(\d{4})(?:-from-(?:` + xvMonthAlt + `)-\d{1,2})?(?:-\d+)?$`)
	// weather
	xvRePolyWx    = regexp.MustCompile(`^(highest|lowest)-temperature-in-([a-z0-9]+(?:-[a-z0-9]+)*?)-on-(` + xvMonthAlt + `)-(\d{1,2})-(\d{4})-(\d{1,3}(?:-\d{1,3})?[cf](?:or(?:below|above|higher))?)$`)
	xvRePolyWxBin = regexp.MustCompile(`^(\d{1,3})(?:-(\d{1,3}))?([cf])(?:or(below|above|higher))?$`)
	// econ
	xvRePolyFedBps = regexp.MustCompile(`^will-the-fed-(?:decrease|increase)-interest-rates-by-\d+-bps-after-the-(` + xvMonthAlt + `)-(\d{4})-meeting(?:-\d+)?$`)
	xvRePolyFedNC  = regexp.MustCompile(`^will-there-be-no-change-in-fed-interest-rates-after-the-(` + xvMonthAlt + `)-(\d{4})-meeting(?:-\d+)?$`)
	xvRePolyInfl   = regexp.MustCompile(`^will-annual-inflation-be-\d+pt\d+(?:-or-(?:less|more))?-in-(` + xvMonthAlt + `)(?:-\d+)?$`)
	// politics (anchor-only this pass — no parseable Kalshi race identifiers in the catalog window)
	xvRePolyPM      = regexp.MustCompile(`^next-prime-minister-of-([a-z]+(?:-[a-z]+)*?)(?:-\d+)?$`)
	xvRePolyGov     = regexp.MustCompile(`^([a-z]+(?:-[a-z]+)*?)-governor-election-winner(?:-\d+)?$`)
	xvRePolyNominee = regexp.MustCompile(`^(democratic|republican)-presidential-nominee-(\d{4})(?:-\d+)?$`)
)

// xvPolyNum canonicalizes a poly slug number token onto Kalshi's normalized strike format:
// "52k"→52000 · "0pt6"→0.6 · "1"→1 · "56000"→56000. "" = not a number token.
func xvPolyNum(tok string) string {
	if k := strings.TrimSuffix(tok, "k"); k != tok {
		n, err := strconv.ParseUint(k, 10, 64)
		if err != nil {
			return ""
		}
		return strconv.FormatUint(n*1000, 10)
	}
	if i := strings.Index(tok, "pt"); i > 0 {
		w, f := tok[:i], tok[i+2:]
		if w == "" || f == "" || !xvNumRe.MatchString(w) || !xvNumRe.MatchString(f) {
			return ""
		}
		out := strings.TrimRight(w+"."+f, "0")
		return strings.TrimSuffix(out, ".")
	}
	if !xvNumRe.MatchString(tok) {
		return ""
	}
	return tok
}

// xvPolyCityStation: poly city slug → the Kalshi station code (the venue's own KXHIGH*/KXLOWT*
// vocabulary) for the majors BOTH venues list. Unknown cities (london, seoul, …) anchor verbatim
// — their °C exact-degree ladders have no Kalshi series to join anyway.
var xvPolyCityStation = map[string]string{
	"nyc": "NYC", "new-york": "NYC", "new-york-city": "NYC", "miami": "MIA", "chicago": "CHI",
	"austin": "AUS", "denver": "DEN", "los-angeles": "LAX", "philadelphia": "PHIL",
	"atlanta": "ATL", "boston": "BOS", "dallas": "DAL", "washington": "DC", "washington-dc": "DC",
	"houston": "HOU", "las-vegas": "LV", "minneapolis": "MIN", "new-orleans": "NOLA",
	"oklahoma-city": "OKC", "phoenix": "PHX", "san-antonio": "SATX", "seattle": "SEA",
	"san-francisco": "SFO",
}

// xvPolyWxBinNorm canonicalizes a poly temperature bin token: "84-85f"→"84-85F" (the Kalshi
// B-bin form — REAL joins) · "81forbelow"→"T81F-below" · "29c"→"29C" · "30corbelow"→"T30C-below".
func xvPolyWxBinNorm(tok string) (string, bool) {
	m := xvRePolyWxBin.FindStringSubmatch(tok)
	if m == nil {
		return "", false
	}
	unit := strings.ToUpper(m[3])
	switch {
	case m[2] != "": // integer range bin
		return m[1] + "-" + m[2] + unit, true
	case m[4] == "below":
		return "T" + m[1] + unit + "-below", true
	case m[4] != "": // above|higher
		return "T" + m[1] + unit + "-above", true
	default: // exact-degree bin
		return m[1] + unit, true
	}
}

// xvPolyDateUTC formats a slug month-day-year, and xvPolyEndMatchesDate verifies the venue
// endDate lands on the SAME UTC calendar date — a stale/mislabeled row anchors nowhere.
func xvPolyDateUTC(mon string, day, year int) (string, bool) {
	m, okM := xvPolyMonths[mon]
	if !okM || day < 1 || day > 31 || year < 2020 || year > 2100 {
		return "", false
	}
	return fmt.Sprintf("%04d-%02d-%02d", year, int(m), day), true
}

func xvPolyEndMatchesDate(endDate, date string) (time.Time, bool) {
	t, err := time.Parse(time.RFC3339, strings.TrimSpace(endDate))
	if err != nil {
		return time.Time{}, false
	}
	t = t.UTC()
	return t, t.Format("2006-01-02") == date
}

// xvPolyAnchor parses one Polymarket MARKET slug (+ the row's endDate where the slug alone
// doesn't pin the window instant) into its canonical identity. STRICT: no grammar, no anchor.
func xvPolyAnchor(slug, endDate string) (key, kind string, ok bool) {
	sl := strings.ToLower(strings.TrimSpace(slug))
	if sl == "" {
		return "", "", false
	}

	// crypto — up/down window slugs pin the window from the slug itself
	if m := xvRePolyUpdownEpoch.FindStringSubmatch(sl); m != nil {
		epoch, err := strconv.ParseInt(m[3], 10, 64)
		if err != nil {
			return "", "", false
		}
		dur := 5 * time.Minute
		switch m[2] {
		case "15m":
			dur = 15 * time.Minute
		case "4h": // R119: pcrypto fetches 4h windows but they were never anchored (silent hole)
			dur = 4 * time.Hour
		}
		end := time.Unix(epoch, 0).UTC().Add(dur) // slug epoch = window START; close probe = start+dur
		return "crypto|" + xvPolyCoin[m[1]] + "|" + end.Format(time.RFC3339) + "|updown" + m[2] + "|up", "crypto", true
	}
	if m := xvRePolyUpdownHour.FindStringSubmatch(sl); m != nil {
		et := etLocation()
		if et == nil {
			return "", "", false
		}
		day, _ := strconv.Atoi(m[3])
		year, _ := strconv.Atoi(m[4])
		hr, _ := strconv.Atoi(m[5])
		mon, okM := xvPolyMonths[m[2]]
		if !okM || day < 1 || day > 31 || hr < 1 || hr > 12 {
			return "", "", false
		}
		if m[6] == "pm" && hr != 12 {
			hr += 12
		} else if m[6] == "am" && hr == 12 {
			hr = 0
		}
		end := time.Date(year, mon, day, hr, 0, 0, 0, et).Add(time.Hour).UTC() // slug hour = window START (probe: 12pm-et ends 17:00Z)
		return "crypto|" + xvPolyCoin[m[1]] + "|" + end.Format(time.RFC3339) + "|updown1h|up", "crypto", true
	}
	if m := xvRePolyUpdownDay.FindStringSubmatch(sl); m != nil {
		day, _ := strconv.Atoi(m[3])
		year, _ := strconv.Atoi(m[4])
		date, okD := xvPolyDateUTC(m[2], day, year)
		if !okD {
			return "", "", false
		}
		end, okE := xvPolyEndMatchesDate(endDate, date)
		if !okE {
			return "", "", false
		}
		return "crypto|" + xvPolyCoin[m[1]] + "|" + end.Format(time.RFC3339) + "|updown1d|up", "crypto", true
	}
	if m := xvRePolyAbove.FindStringSubmatch(sl); m != nil {
		strike := xvPolyNum(m[2])
		day, _ := strconv.Atoi(m[4])
		year, _ := strconv.Atoi(m[5])
		date, okD := xvPolyDateUTC(m[3], day, year)
		if strike == "" || !okD {
			return "", "", false
		}
		end, okE := xvPolyEndMatchesDate(endDate, date) // noon-ET close convention rides the venue's own endDate
		if !okE {
			return "", "", false
		}
		return "crypto|" + xvPolyCoin[m[1]] + "|" + end.Format(time.RFC3339) + "|T" + strike + "|up", "crypto", true
	}
	if m := xvRePolyBetween.FindStringSubmatch(sl); m != nil {
		day, _ := strconv.Atoi(m[5])
		year, _ := strconv.Atoi(m[6])
		date, okD := xvPolyDateUTC(m[4], day, year)
		if !okD {
			return "", "", false
		}
		end, okE := xvPolyEndMatchesDate(endDate, date)
		if !okE {
			return "", "", false
		}
		return "crypto|" + xvPolyCoin[m[1]] + "|" + end.Format(time.RFC3339) + "|R" + m[2] + "-" + m[3] + "|range", "crypto", true
	}
	if m := xvRePolyLessGreater.FindStringSubmatch(sl); m != nil {
		day, _ := strconv.Atoi(m[5])
		year, _ := strconv.Atoi(m[6])
		date, okD := xvPolyDateUTC(m[4], day, year)
		if !okD {
			return "", "", false
		}
		end, okE := xvPolyEndMatchesDate(endDate, date)
		if !okE {
			return "", "", false
		}
		desc, dir := "U"+m[3], "down"
		if m[2] == "greater" {
			desc, dir = "G"+m[3], "up"
		}
		return "crypto|" + xvPolyCoin[m[1]] + "|" + end.Format(time.RFC3339) + "|" + desc + "|" + dir, "crypto", true
	}
	if m := xvRePolyTouch.FindStringSubmatch(sl); m != nil {
		strike := xvPolyNum(m[3])
		end, err := time.Parse(time.RFC3339, strings.TrimSpace(endDate))
		if strike == "" || err != nil {
			return "", "", false
		}
		desc, dir := "H"+strike, "up" // reach = touch-high
		if m[2] == "dip-to" {
			desc, dir = "L"+strike, "down"
		}
		return "crypto|" + xvPolyCoin[m[1]] + "|" + end.UTC().Format(time.RFC3339) + "|" + desc + "|" + dir, "crypto", true
	}

	// weather
	if m := xvRePolyWx.FindStringSubmatch(sl); m != nil {
		metric := "high"
		if m[1] == "lowest" {
			metric = "low"
		}
		day, _ := strconv.Atoi(m[4])
		year, _ := strconv.Atoi(m[5])
		date, okD := xvPolyDateUTC(m[3], day, year)
		bin, okB := xvPolyWxBinNorm(m[6])
		if !okD || !okB {
			return "", "", false
		}
		station := xvPolyCityStation[m[2]]
		if station == "" {
			station = strings.ToUpper(strings.ReplaceAll(m[2], "-", "")) // own node, join-free
		}
		return "wx|" + station + "|" + metric + "|" + date + "|" + bin, "wx", true
	}

	// econ
	if m := xvRePolyFedBps.FindStringSubmatch(sl); m != nil {
		return "econ|cb-us|" + m[2] + fmt.Sprintf("-%02d", int(xvPolyMonths[m[1]])), "econ", true
	}
	if m := xvRePolyFedNC.FindStringSubmatch(sl); m != nil {
		return "econ|cb-us|" + m[2] + fmt.Sprintf("-%02d", int(xvPolyMonths[m[1]])), "econ", true
	}
	if m := xvRePolyInfl.FindStringSubmatch(sl); m != nil {
		end, err := time.Parse(time.RFC3339, strings.TrimSpace(endDate))
		if err != nil {
			return "", "", false
		}
		end = end.UTC()
		pm := int(xvPolyMonths[m[1]])
		year := end.Year()
		if pm > int(end.Month()) { // December print settles in January — period year backs up one
			year--
		}
		return fmt.Sprintf("econ|us-cpi-yoy|%04d-%02d", year, pm), "econ", true
	}

	// politics — conservative, anchor-only (see header)
	if m := xvRePolyNominee.FindStringSubmatch(sl); m != nil {
		return "pol|" + m[1] + "-presidential-nominee|" + m[2], "pol", true
	}
	if m := xvRePolyPM.FindStringSubmatch(sl); m != nil {
		end, err := time.Parse(time.RFC3339, strings.TrimSpace(endDate))
		if err != nil {
			return "", "", false
		}
		return "pol|pm-" + m[1] + "|" + strconv.Itoa(end.UTC().Year()), "pol", true
	}
	if m := xvRePolyGov.FindStringSubmatch(sl); m != nil {
		end, err := time.Parse(time.RFC3339, strings.TrimSpace(endDate))
		if err != nil {
			return "", "", false
		}
		return "pol|gov-" + m[1] + "|" + strconv.Itoa(end.UTC().Year()), "pol", true
	}

	return "", "", false
}

// ── rebuild + endpoint ──────────────────────────────────────────────────────────────────────────

// xvRebuild re-anchors the registry from the SAME snapshots the market tree reads (Kalshi
// TopLiquidCached + poly CachedMarkets) plus the catalog reader for Kalshi breadth (rows the
// 10–15k live window has aged out but the venue still lists). Poly-int catalog rows carry
// conditionId + EVENT slug only (no market slug — the grammar-bearing identifier), so poly
// breadth is the live gamma cache; documented gap, not an oversight. Self-throttled to 5 min +
// single-flight; kicked from mtBuild and (wiring) the 5-min catalog sweep.
func (s *Server) xvRebuildCoordinated(ctx context.Context) {
	s.tryRunHeavyResearch(ctx, "xvident", 0, s.xvRebuild)
}

func (s *Server) xvRebuild(ctx context.Context) {
	// catalogSweep already calls this inside the shared research lane; direct diagnostics use the
	// coordinated wrapper above. This final guard also prevents an accidental future direct call
	// from starting the catalog breadth scan after the operator has armed LIVE.
	if s.researchLiveActive() {
		return
	}
	xvReg.mu.Lock()
	if xvReg.building || time.Since(xvReg.builtAt) < 5*time.Minute {
		xvReg.mu.Unlock()
		return
	}
	xvReg.building = true
	xvReg.mu.Unlock()
	defer func() {
		xvReg.mu.Lock()
		xvReg.building = false
		xvReg.mu.Unlock()
	}()

	fresh := newXVState() // built lock-free, swapped in whole at the end
	kalSeen, polySeen := 0, 0

	if s.kal != nil {
		kmkts, _ := s.kal.TopLiquidCached()
		for _, m := range kmkts {
			if m.Ticker == "" {
				continue
			}
			kalSeen++
			if key, kind, ok := xvKalshiAnchor(m.Ticker); ok {
				fresh.addLocked("kalshi", m.Ticker, key, kind)
			}
		}
	}
	if s.store != nil { // catalog breadth: open rows beyond the live window (weather/econ ladders age out fast)
		if rows, err := s.store.CatalogUniverse(ctx, "kalshi", time.Now()); err == nil {
			for i := range rows {
				tk := rows[i].Ticker
				if tk == "" {
					continue
				}
				if _, dup := fresh.keyOf["kalshi|"+tk]; dup {
					continue
				}
				kalSeen++
				if key, kind, ok := xvKalshiAnchor(tk); ok {
					fresh.addLocked("kalshi", tk, key, kind)
				}
			}
		}
	}
	if s.poly != nil {
		for _, m := range s.poly.CachedMarkets() {
			if m.Slug == "" || m.Closed {
				continue
			}
			polySeen++
			if key, kind, ok := xvPolyAnchor(m.Slug, m.EndDate); ok {
				fresh.addLocked("polymarket", strings.ToLower(m.Slug), key, kind)
				if m.ConditionID != "" { // lookup alias — consumers hold either identifier
					fresh.keyOf["polymarket|"+m.ConditionID] = key
				}
			}
		}
	}

	// R119: re-ingest live-registered poly windows (TTL-pruned) — the gamma top-volume cache
	// never holds short-lived up/down windows in time, so without this the swap wiped them and
	// joined_keys under-reported the joins the live path serves.
	xvReg.mu.Lock()
	liveCopy := make(map[string]xvLiveAnchor, len(xvReg.liveAnchors))
	for slug, la := range xvReg.liveAnchors {
		if time.Since(la.at) > xvLiveAnchorTTL {
			delete(xvReg.liveAnchors, slug)
			continue
		}
		liveCopy[slug] = la
	}
	xvReg.mu.Unlock()
	for slug, la := range liveCopy {
		fresh.addLocked("polymarket", slug, la.key, la.kind)
		if la.conditionID != "" {
			if _, dup := fresh.keyOf["polymarket|"+la.conditionID]; !dup {
				fresh.keyOf["polymarket|"+la.conditionID] = la.key
			}
		}
	}

	// per-kind coverage stats (deterministic example order)
	stats := map[string]*xvKindStat{}
	keys := make([]string, 0, len(fresh.byKey))
	for k := range fresh.byKey {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		e := fresh.byKey[k]
		st := stats[e.kind]
		if st == nil {
			st = &xvKindStat{}
			stats[e.kind] = st
		}
		nk, np := len(e.ids["kalshi"]), len(e.ids["polymarket"])
		st.KalMkts += nk
		st.PolyMkts += np
		if nk > 0 {
			st.KalKeys++
		}
		if np > 0 {
			st.PolyKeys++
		}
		if nk > 0 && np > 0 {
			st.JoinedKeys++
			if len(st.Examples) < 5 {
				st.Examples = append(st.Examples, k)
			}
		}
	}

	xvReg.mu.Lock()
	xvReg.byKey, xvReg.keyOf, xvReg.stats, xvReg.orderedKeys = fresh.byKey, fresh.keyOf, stats, append([]string(nil), keys...)
	xvReg.kalSeen, xvReg.polySeen = kalSeen, polySeen
	xvReg.builtAt = time.Now()
	xvReg.mu.Unlock()
}

// handleXVIdent — GET /api/xvident: per-kind anchor + join coverage, serve-rate counters and
// joined-key examples. coverage_pct definition rides in the payload (auditor-facing).
func (s *Server) handleXVIdent(w http.ResponseWriter, r *http.Request) {
	s.xvRebuildCoordinated(r.Context()) // cached/live-only while armed; cold diagnostics rebuild when admitted

	xvReg.mu.Lock()
	kinds := map[string]any{}
	for kind, st := range xvReg.stats {
		cov := 0.0
		if st.KalKeys > 0 {
			cov = 100 * float64(st.JoinedKeys) / float64(st.KalKeys)
		}
		exs := make([]map[string]any, 0, len(st.Examples))
		for _, k := range st.Examples {
			e := xvReg.byKey[k]
			if e == nil {
				continue
			}
			exs = append(exs, map[string]any{"key": k, "kalshi": e.ids["kalshi"], "polymarket": e.ids["polymarket"]})
		}
		kinds[kind] = map[string]any{
			"kalshi_anchored": st.KalMkts, "polyint_anchored": st.PolyMkts,
			"kalshi_keys": st.KalKeys, "polyint_keys": st.PolyKeys,
			"joined_keys": st.JoinedKeys, "coverage_pct": cov, "examples": exs,
		}
	}
	payload := map[string]any{
		"kinds":        kinds,
		"seen":         map[string]int{"kalshi": xvReg.kalSeen, "polymarket": xvReg.polySeen},
		"hits":         map[string]any{"struct": copyInt64Map(xvReg.structHits), "fuzz": copyInt64Map(xvReg.fuzzHits)},
		"built_at":     xvReg.builtAt.UTC().Format(time.RFC3339),
		"coverage_def": "coverage_pct = 100*joined_keys/kalshi_keys — of the Kalshi-side canonical keys this registry could anchor from venue identifiers (the kalshi-anchored-side eligible set), the share whose poly-int twin is ALSO structurally anchored; fuzzy fallback serves part of the remainder and is counted under hits.fuzz",
		"note":         "anchors from venue identifiers under strict grammars only (series/event/strike ticker fields; canonical market slugs); unparseable = unanchored, never title-fuzzed. Sports identity lives in gameident (/api/gametree).",
	}
	xvReg.mu.Unlock()
	writeJSON(w, http.StatusOK, payload)
}
