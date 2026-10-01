package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

const researchIdentityCursorKey = "r138:canonical-identity-cursor"
const researchCatalogCursorKey = "r138:canonical-catalog-cursor"

type researchIdentityCursor struct {
	SourceSeen string `json:"source_seen"`
	Venue      string `json:"venue"`
	Ticker     string `json:"ticker"`
}

func structuralPayoffKey(r storage.StructuralIdentityRow) (payoffID, label, boundary, scope, status string, predicateJSON string) {
	eventID := "sports:" + strings.TrimSpace(r.GameID)
	kind := strings.ToLower(strings.TrimSpace(r.MktType))
	yesTeam, noTeam := strings.ToUpper(strings.TrimSpace(r.YesTeam)), strings.ToUpper(strings.TrimSpace(r.NoTeam))
	line := strconv.FormatFloat(r.Line, 'f', -1, 64)
	scope, status = "full_game", "structural"
	if kind == "" || kind == "prop" {
		// Props and any scope the structural matcher deliberately demoted may share a game container
		// but do not share a proven payoff. Venue+ticker keeps them distinct until rule evidence arrives.
		kind, scope, status = "prop", "unknown", "unverified"
		payoffID = eventID + "|unverified|" + strings.ToLower(r.Venue) + "|" + r.Ticker
	} else if kind == "winner" || kind == "advance" {
		// A YES payoff is about the selected team.  The opposing label is venue orientation,
		// not payoff identity; including it split equivalent Kalshi/PolyUS propositions.
		payoffID = strings.Join([]string{eventID, scope, kind, yesTeam}, "|")
	} else {
		payoffID = strings.Join([]string{eventID, scope, kind, yesTeam, noTeam, line}, "|")
	}
	label = strings.TrimSpace(strings.Join([]string{kind, yesTeam, noTeam, line}, " "))
	switch kind {
	case "winner", "advance":
		boundary = "YES iff the structurally identified team satisfies the venue's full-event winner/advance rule"
	case "total":
		boundary = "YES iff the full-event total satisfies the signed over/under predicate at the stored line"
	case "spread":
		boundary = "YES iff the structurally identified team covers the stored signed full-event spread"
	default:
		boundary = "unverified venue-specific proposition; rule document required"
	}
	predicate, _ := json.Marshal(map[string]any{
		"kind": kind, "scope": scope, "yes_team": yesTeam, "no_team": noTeam,
		"line": r.Line, "source": r.Src,
	})
	return payoffID, label, boundary, scope, status, string(predicate)
}

// sweepResearchFoundation incrementally folds only structural matcher truth into the immutable
// research graph. It reads no venue API and deliberately excludes fuzzy/title-only matches.
func (s *Server) sweepResearchFoundation(ctx context.Context) {
	started := time.Now()
	cur := researchIdentityCursor{}
	if raw, ok := s.store.KVGet(ctx, researchIdentityCursorKey); ok && raw != "" {
		_ = json.Unmarshal([]byte(raw), &cur)
	}
	cycleID := fmt.Sprintf("%s|%s|%s", researchFiveMinuteCycle(started), cur.Venue, cur.Ticker)
	record := func(status string, eligible, attempted, inserted, duplicates int, expectedZero bool,
		zeroReason, errorClass, errorText string, exclusions map[string]int, metrics map[string]any) {
		dctx, cancel := researchDurabilityContext(ctx)
		defer cancel()
		_, _ = s.store.InsertCollectorReceipt(dctx, storage.CollectorReceipt{
			CollectorID: "canonical-identity", CycleID: cycleID, Status: status,
			Started: started, Completed: time.Now(), Eligible: eligible, Attempted: attempted,
			Inserted: inserted, Duplicates: duplicates, ExpectedZero: expectedZero,
			ZeroReason: zeroReason, ErrorClass: errorClass, ErrorText: errorText,
			Source: "game_identity+market_game structural joins", SchemaVersion: "r138-v1",
			ExpectedCadence: 5 * time.Minute, Exclusions: exclusions, Metrics: metrics,
			Systems: storage.ResearchExperimentIDs(),
		})
	}
	rows, err := s.store.StructuralIdentityPage(ctx, cur.SourceSeen, cur.Venue, cur.Ticker, 500)
	if err != nil {
		record("error", 0, 0, 0, 0, false, "", "storage_read", err.Error(), nil, nil)
		s.log.Warn("research foundation identity page failed", "err", err)
		return
	}
	if len(rows) == 0 {
		// Keep the source watermark. A later source update advances last_seen and becomes eligible;
		// historical rows are never rescanned merely to fabricate fresh liveness.
		record("healthy_empty", 0, 0, 0, 0, true, "no structural source rows newer than durable watermark", "", "", nil,
			map[string]any{"source_watermark": cur.SourceSeen})
		return
	}
	ruleArtifacts := s.researchRuleArtifacts(rows)
	eventMap := map[string]storage.CanonicalEventSpec{}
	payoffMap := map[string]storage.CanonicalPayoffSpec{}
	relationMap := map[string]storage.CanonicalRelationSpec{}
	instruments := make([]storage.CanonicalInstrumentSpec, 0, len(rows))
	unverified := 0
	for _, r := range rows {
		eventID := "sports:" + strings.TrimSpace(r.GameID)
		title := strings.TrimSpace(strings.Join([]string{r.AwayName, "at", r.HomeName}, " "))
		if title == "at" {
			title = strings.TrimSpace(strings.Join([]string{r.Away, "at", r.Home}, " "))
		}
		evidence, _ := json.Marshal(map[string]any{
			"game_id": r.GameID, "league": r.League, "away": r.Away, "home": r.Home,
			"source": "game_identity joined to market_game", "title_matching": false,
		})
		eventMap[eventID] = storage.CanonicalEventSpec{
			EventID: eventID, EventType: "sports-game", Domain: strings.ToLower(r.League), Title: title,
			StartTS: r.StartUTC, Timezone: "UTC", SourceArtifact: "game_identity/market_game",
			SourceClockID: "venue-metadata-start", OutcomeSetStatus: "incomplete",
			BoundaryRule:   "atomic game state; listed propositions are registered as separate payoffs",
			RevisionPolicy: "append a new immutable event version when structural venue metadata changes",
			VoidPolicy:     "", EvidenceJSON: string(evidence),
			SourceObservedTS: r.SourceLastSeen,
		}
		payoffID, label, boundary, scope, status, predicate := structuralPayoffKey(r)
		if status == "unverified" {
			unverified++
		}
		payEvidence, _ := json.Marshal(map[string]any{
			"venue_metadata_source": r.Src, "title_matching": false,
			"rule_document_verified": false, "scope_fail_closed": scope == "unknown",
		})
		payoffMap[payoffID] = storage.CanonicalPayoffSpec{
			PayoffID: payoffID, EventID: eventID, Label: label, PredicateJSON: predicate,
			BoundaryRule: boundary, PayoutFloor: 0, PayoutCeiling: 1,
			SourceArtifact: "market_game:struct", IdentityStatus: status, EvidenceJSON: string(payEvidence),
			SourceObservedTS: r.SourceLastSeen,
		}
		complementID := payoffID + "|NOT"
		complementPredicate, _ := json.Marshal(map[string]any{"not_payoff_id": payoffID, "native_side": "NO"})
		payoffMap[complementID] = storage.CanonicalPayoffSpec{
			PayoffID: complementID, EventID: eventID, Label: "NOT (" + label + ")",
			PredicateJSON: string(complementPredicate),
			BoundaryRule:  "logical complement of the registered venue-binary YES proposition; void economics remain separate",
			PayoutFloor:   0, PayoutCeiling: 1, SourceArtifact: "market_game:struct",
			IdentityStatus: status, EvidenceJSON: string(payEvidence), SourceObservedTS: r.SourceLastSeen,
		}
		relationID := "complement:" + payoffID
		relationMap[relationID] = storage.CanonicalRelationSpec{
			RelationID: relationID, EventID: eventID, LeftPayoffID: payoffID, RightPayoffID: complementID,
			RelationType: "exhaustive_with", IdentityStatus: status,
			EvidenceJSON: `{"basis":"venue-native binary YES/NO logical complement","void_payoff_floor_asserted":false}`,
		}
		basis := "unverified; same canonical payoff key is necessary but rule documents are still required"
		if status == "unverified" {
			basis = "unverified distinct proposition; never cross-venue equivalent"
		}
		instEvidenceMap := map[string]any{
			"game_id": r.GameID, "market_type": r.MktType, "yes_team": r.YesTeam,
			"no_team": r.NoTeam, "line": r.Line, "source": r.Src,
			"void_policy_status": "unknown until explicit venue rule text is captured and normalized",
		}
		artifact := ruleArtifacts[storage.ResolutionBasisKey(r.Venue, r.Ticker)]
		instEvidence, rulesArtifact, rulesHash, settlementSource := "", "", "", ""
		if artifact.Source != "" {
			instEvidence = attachResearchRuleArtifact(instEvidenceMap, artifact)
			rulesArtifact, rulesHash, settlementSource = artifact.Source, artifact.Hash, artifact.SettlementSource
		} else {
			raw, _ := json.Marshal(instEvidenceMap)
			instEvidence = string(raw)
		}
		instruments = append(instruments, storage.CanonicalInstrumentSpec{
			Venue: r.Venue, Ticker: r.Ticker, EventID: eventID, PayoffID: payoffID,
			NativeSide: "YES", Orientation: "same", MarketKind: strings.ToLower(r.MktType),
			Scope: scope, Line: r.Line, RulesArtifact: rulesArtifact, RulesHash: rulesHash,
			SettlementSource: settlementSource,
			CrossVenueBasis:  basis, IdentityStatus: status, EvidenceJSON: string(instEvidence),
			SourceObservedTS: r.SourceLastSeen,
		})
	}
	events := make([]storage.CanonicalEventSpec, 0, len(eventMap))
	for _, e := range eventMap {
		events = append(events, e)
	}
	payoffs := make([]storage.CanonicalPayoffSpec, 0, len(payoffMap))
	for _, p := range payoffMap {
		payoffs = append(payoffs, p)
	}
	sort.Slice(events, func(i, j int) bool { return events[i].EventID < events[j].EventID })
	sort.Slice(payoffs, func(i, j int) bool { return payoffs[i].PayoffID < payoffs[j].PayoffID })
	relations := make([]storage.CanonicalRelationSpec, 0, len(relationMap))
	for _, rel := range relationMap {
		relations = append(relations, rel)
	}
	sort.Slice(relations, func(i, j int) bool { return relations[i].RelationID < relations[j].RelationID })
	result, err := s.store.RegisterCanonicalBatch(ctx, events, payoffs, instruments, relations)
	if err != nil {
		record("error", len(rows), len(rows), 0, 0, false, "", "identity_write", err.Error(),
			map[string]int{"unverified_payoff": unverified}, nil)
		s.log.Warn("research foundation identity register failed", "err", err, "rows", len(rows))
		return
	}
	last := rows[len(rows)-1]
	next, _ := json.Marshal(researchIdentityCursor{SourceSeen: last.SourceLastSeen, Venue: last.Venue, Ticker: last.Ticker})
	if err := s.store.KVSet(ctx, researchIdentityCursorKey, string(next)); err != nil {
		record("error", len(rows), len(rows), result.InstrumentsInserted,
			len(rows)-result.InstrumentsInserted, false, "", "cursor_write", err.Error(),
			map[string]int{"unverified_payoff": unverified}, nil)
		s.log.Warn("research foundation cursor persist failed", "err", err)
		return
	}
	record("healthy", len(rows), len(rows), result.InstrumentsInserted,
		len(rows)-result.InstrumentsInserted, false, "", "", "",
		map[string]int{"unverified_payoff": unverified}, map[string]any{
			"events_inserted": result.EventsInserted, "payoffs_inserted": result.PayoffsInserted,
			"instruments_inserted": result.InstrumentsInserted, "relations_inserted": result.RelationsInserted,
			"source_watermark": last.SourceLastSeen,
		})
	if result.EventsInserted+result.PayoffsInserted+result.InstrumentsInserted > 0 {
		_ = s.store.Audit(ctx, "info", "research", "R138 canonical identity page registered",
			fmt.Sprintf(`{"rows":%d,"events":%d,"payoffs":%d,"instruments":%d}`,
				len(rows), result.EventsInserted, result.PayoffsInserted, result.InstrumentsInserted))
	}
}

// venueLocalCatalogIdentitySpecs is the single builder for catalog-only identity. Both the
// background cursor and an exact current-ticker catch-up use this function, so priority cannot
// silently acquire stronger semantics. Every product is venue-local and UNVERIFIED; titles and
// event keys never assert cross-venue equivalence.
func venueLocalCatalogIdentitySpecs(r storage.CatalogIdentityRow) (storage.CanonicalEventSpec,
	[]storage.CanonicalPayoffSpec, storage.CanonicalInstrumentSpec, storage.CanonicalRelationSpec) {
	venue := strings.ToLower(strings.TrimSpace(r.Venue))
	eventKey := strings.TrimSpace(r.EventKey)
	if eventKey == "" {
		eventKey = r.Ticker
	}
	eventID := "venue:" + venue + ":" + eventKey
	evidence, _ := json.Marshal(map[string]any{"venue": venue, "event_key": eventKey,
		"title_matching": false, "cross_venue_equivalence": false, "source_last_seen": r.SourceLastSeen})
	event := storage.CanonicalEventSpec{
		EventID: eventID, EventType: "venue-event", Domain: venue, Title: eventKey,
		SourceArtifact: "market_catalog", SourceClockID: "venue-catalog-last-seen",
		BoundaryRule:   "venue-local container only; no title-derived payoff equivalence",
		RevisionPolicy: "append immutable version on venue structural metadata change",
		VoidPolicy:     "unknown until authoritative market rules attach", OutcomeSetStatus: "unknown",
		EvidenceJSON: string(evidence), SourceObservedTS: r.SourceLastSeen,
	}
	payoffID := eventID + "|market:" + r.Ticker + "|YES"
	predicate, _ := json.Marshal(map[string]any{"venue": venue, "ticker": r.Ticker, "native_side": "YES"})
	yesPayoff := storage.CanonicalPayoffSpec{
		PayoffID: payoffID, EventID: eventID, Label: r.Title, PredicateJSON: string(predicate),
		BoundaryRule: "unverified venue-market YES proposition; authoritative rule artifact required",
		PayoutFloor:  0, PayoutCeiling: 1, SourceArtifact: "market_catalog",
		IdentityStatus: "unverified", EvidenceJSON: string(evidence), SourceObservedTS: r.SourceLastSeen,
	}
	complementID := payoffID + "|NOT"
	complementPredicate, _ := json.Marshal(map[string]any{"not_payoff_id": payoffID, "native_side": "NO"})
	noPayoff := storage.CanonicalPayoffSpec{
		PayoffID: complementID, EventID: eventID, Label: "NOT (" + r.Title + ")",
		PredicateJSON: string(complementPredicate),
		BoundaryRule:  "logical complement of the venue-native binary YES proposition; void economics remain separate",
		PayoutFloor:   0, PayoutCeiling: 1, SourceArtifact: "market_catalog",
		IdentityStatus: "unverified", EvidenceJSON: string(evidence), SourceObservedTS: r.SourceLastSeen,
	}
	relation := storage.CanonicalRelationSpec{
		RelationID: "complement:" + payoffID, EventID: eventID, LeftPayoffID: payoffID,
		RightPayoffID: complementID, RelationType: "exhaustive_with", IdentityStatus: "unverified",
		EvidenceJSON: `{"basis":"venue-native binary YES/NO logical complement","void_payoff_floor_asserted":false}`,
	}
	instrument := storage.CanonicalInstrumentSpec{
		Venue: venue, Ticker: r.Ticker, EventID: eventID, PayoffID: payoffID, NativeSide: "YES",
		Orientation: "unknown", MarketKind: r.Kind, Scope: "unknown", CloseTS: r.CloseTS,
		RulesArtifact: "market_catalog", CrossVenueBasis: "none; venue-local identity only",
		IdentityStatus: "unverified", EvidenceJSON: string(evidence), SourceObservedTS: r.SourceLastSeen,
	}
	return event, []storage.CanonicalPayoffSpec{yesPayoff, noPayoff}, instrument, relation
}

func (s *Server) ensureExactCatalogCanonicalInstrument(ctx context.Context, venue, ticker string) (
	storage.CurrentCanonicalInstrument, bool, error) {
	identity, ok, err := s.store.CurrentCanonicalInstrument(ctx, venue, ticker)
	if err != nil || ok {
		return identity, ok, err
	}
	row, catalogOK, err := s.store.CatalogIdentityExact(ctx, venue, ticker)
	if err != nil || !catalogOK {
		return storage.CurrentCanonicalInstrument{}, false, err
	}
	event, payoffs, instrument, relation := venueLocalCatalogIdentitySpecs(row)
	if _, err := s.store.RegisterCanonicalBatch(ctx, []storage.CanonicalEventSpec{event}, payoffs,
		[]storage.CanonicalInstrumentSpec{instrument}, []storage.CanonicalRelationSpec{relation}); err != nil {
		return storage.CurrentCanonicalInstrument{}, false, err
	}
	return s.store.CurrentCanonicalInstrument(ctx, venue, ticker)
}

// sweepCatalogFoundation covers the rest of the venue universe with venue-local, unverified
// identities.  It intentionally sacrifices cross-venue recall: titles never create equivalence.
func (s *Server) sweepCatalogFoundation(ctx context.Context) {
	started := time.Now()
	cur := researchIdentityCursor{}
	if raw, ok := s.store.KVGet(ctx, researchCatalogCursorKey); ok && raw != "" {
		_ = json.Unmarshal([]byte(raw), &cur)
	}
	cycleID := fmt.Sprintf("%s|catalog|%s|%s|%s", researchFiveMinuteCycle(started), cur.SourceSeen, cur.Venue, cur.Ticker)
	record := func(status string, eligible, inserted int, expectedZero bool, zeroReason, errorClass, errorText string, metrics map[string]any) {
		dctx, cancel := researchDurabilityContext(ctx)
		defer cancel()
		_, _ = s.store.InsertCollectorReceipt(dctx, storage.CollectorReceipt{
			CollectorID: "canonical-catalog-identity", CycleID: cycleID, Status: status,
			Started: started, Completed: time.Now(), Eligible: eligible, Attempted: eligible,
			Inserted: inserted, Duplicates: eligible - inserted, ExpectedZero: expectedZero,
			ZeroReason: zeroReason, ErrorClass: errorClass, ErrorText: errorText,
			Source: "market_catalog excluding structural game joins", SchemaVersion: "r138-v1",
			ExpectedCadence: 5 * time.Minute, Metrics: metrics, Systems: storage.ResearchExperimentIDs(),
		})
	}
	rows, err := s.store.CatalogIdentityPage(ctx, cur.SourceSeen, cur.Venue, cur.Ticker, 500)
	if err != nil {
		record("error", 0, 0, false, "", "storage_read", err.Error(), nil)
		return
	}
	if len(rows) == 0 {
		record("healthy_empty", 0, 0, true, "no non-structural catalog rows newer than durable watermark", "", "",
			map[string]any{"source_watermark": cur.SourceSeen})
		return
	}
	events := make(map[string]storage.CanonicalEventSpec)
	payoffs := make([]storage.CanonicalPayoffSpec, 0, len(rows))
	instruments := make([]storage.CanonicalInstrumentSpec, 0, len(rows))
	relations := make([]storage.CanonicalRelationSpec, 0, len(rows))
	for _, r := range rows {
		event, rowPayoffs, instrument, relation := venueLocalCatalogIdentitySpecs(r)
		events[event.EventID] = event
		payoffs = append(payoffs, rowPayoffs...)
		instruments = append(instruments, instrument)
		relations = append(relations, relation)
	}
	eventList := make([]storage.CanonicalEventSpec, 0, len(events))
	for _, e := range events {
		eventList = append(eventList, e)
	}
	sort.Slice(eventList, func(i, j int) bool { return eventList[i].EventID < eventList[j].EventID })
	sort.Slice(payoffs, func(i, j int) bool { return payoffs[i].PayoffID < payoffs[j].PayoffID })
	sort.Slice(relations, func(i, j int) bool { return relations[i].RelationID < relations[j].RelationID })
	result, err := s.store.RegisterCanonicalBatch(ctx, eventList, payoffs, instruments, relations)
	if err != nil {
		record("error", len(rows), 0, false, "", "identity_write", err.Error(), nil)
		return
	}
	last := rows[len(rows)-1]
	next, _ := json.Marshal(researchIdentityCursor{SourceSeen: last.SourceLastSeen, Venue: last.Venue, Ticker: last.Ticker})
	if err := s.store.KVSet(ctx, researchCatalogCursorKey, string(next)); err != nil {
		record("error", len(rows), result.InstrumentsInserted, false, "", "cursor_write", err.Error(), nil)
		return
	}
	record("healthy", len(rows), result.InstrumentsInserted, false, "", "", "", map[string]any{
		"events_inserted": result.EventsInserted, "payoffs_inserted": result.PayoffsInserted,
		"instruments_inserted": result.InstrumentsInserted, "relations_inserted": result.RelationsInserted,
		"source_watermark": last.SourceLastSeen,
	})
}

func researchFiveMinuteCycle(t time.Time) string {
	return t.UTC().Truncate(5 * time.Minute).Format(time.RFC3339)
}

func (s *Server) handleResearchFoundation(w http.ResponseWriter, r *http.Request) {
	report, err := s.store.ResearchFoundationReport(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, report)
}
