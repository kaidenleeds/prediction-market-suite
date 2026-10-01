package storage

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestR139LegacyRuleArtifactMigrationAddsPolymarketAndRestoresImmutability(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", "file:"+filepath.Join(dir, "kalshi.db"))
	if err != nil {
		t.Fatal(err)
	}
	for _, ddl := range []string{
		`DROP TRIGGER IF EXISTS research_rule_artifacts_no_update`,
		`DROP TRIGGER IF EXISTS research_rule_artifacts_no_delete`,
		`DROP INDEX IF EXISTS idx_rrule_artifact_latest`,
		`ALTER TABLE research_rule_artifacts RENAME TO research_rule_artifacts_current`,
		`CREATE TABLE research_rule_artifacts (venue TEXT NOT NULL CHECK(venue IN ('kalshi','polyus')),
instrument_id TEXT NOT NULL,version INTEGER NOT NULL CHECK(version>0),artifact_hash TEXT NOT NULL CHECK(length(artifact_hash)=64),
observed_ts TEXT NOT NULL,source TEXT NOT NULL,raw_rules_json TEXT NOT NULL CHECK(json_valid(raw_rules_json)),
settlement_sources_json TEXT NOT NULL CHECK(json_valid(settlement_sources_json)),capture_state TEXT NOT NULL
CHECK(capture_state IN ('RAW_COMPLETE_REVIEW_REQUIRED','RAW_RULES_ONLY','SETTLEMENT_SOURCE_ONLY')),
funded INTEGER NOT NULL DEFAULT 0 CHECK(funded=0),paper_authority INTEGER NOT NULL DEFAULT 0 CHECK(paper_authority=0),
live_authority INTEGER NOT NULL DEFAULT 0 CHECK(live_authority=0),PRIMARY KEY(venue,instrument_id,version),
UNIQUE(venue,instrument_id,artifact_hash))`,
		`INSERT INTO research_rule_artifacts VALUES('kalshi','LEGACY-K',1,'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','2026-07-12T00:00:00Z','legacy','{"primary":"legacy"}','[]','RAW_RULES_ONLY',0,0,0)`,
		`DROP TABLE research_rule_artifacts_current`,
		`INSERT INTO market_game(venue,ticker,game_id,mkt_type,yes_team,no_team,line,src,first_seen,last_seen)
VALUES('polyint','0xlegacy-game','G-LEGACY','winner','PHI','KC',0,'struct','2026-07-12T00:00:00Z','2026-07-12T00:00:00Z')`,
	} {
		if _, err := db.Exec(ddl); err != nil {
			_ = db.Close()
			t.Fatalf("legacy fixture %q: %v", ddl, err)
		}
	}
	_ = db.Close()
	st, err = Open(dir)
	if err != nil {
		t.Fatalf("legacy upgrade: %v", err)
	}
	defer st.Close()
	if _, ok, err := st.LatestVenueRuleArtifact(context.Background(), "kalshi", "LEGACY-K"); err != nil || !ok {
		t.Fatalf("legacy artifact lost ok=%v err=%v", ok, err)
	}
	if _, inserted, err := st.InsertVenueRuleArtifact(context.Background(), VenueRuleArtifact{
		Venue: "polymarket", InstrumentID: "0xlegacy-upgrade", Source: "Gamma",
		Description: "official description", SettlementSources: []RuleSettlementSource{{Name: "Gamma resolutionSource", URL: "https://official.example"}}}); err != nil || !inserted {
		t.Fatalf("upgraded CHECK rejected polymarket inserted=%v err=%v", inserted, err)
	}
	if _, err := st.db.Exec(`UPDATE research_rule_artifacts SET source='mutable' WHERE instrument_id='LEGACY-K'`); err == nil {
		t.Fatal("artifact update trigger was not restored by legacy migration")
	}
	var publicRows, aliasRows int
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM market_game WHERE venue='polymarket' AND ticker='0xlegacy-game'`).Scan(&publicRows); err != nil {
		t.Fatal(err)
	}
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM market_game WHERE venue='polyint'`).Scan(&aliasRows); err != nil {
		t.Fatal(err)
	}
	if publicRows != 1 || aliasRows != 0 {
		t.Fatalf("legacy Poly-int venue alias was not canonicalized: polymarket=%d polyint=%d", publicRows, aliasRows)
	}
}

func seedRuleCanonical(t *testing.T, st *Store, kTicker, pSlug string) {
	t.Helper()
	event := CanonicalEventSpec{EventID: "sports:game-rule", EventType: "sports-game", Domain: "mlb",
		OutcomeSetStatus: "incomplete", EvidenceJSON: `{}`}
	payoff := CanonicalPayoffSpec{PayoffID: "sports:game-rule|full_game|winner|PHI", EventID: event.EventID,
		Label: "winner PHI", PredicateJSON: `{"kind":"winner","yes_team":"PHI"}`,
		PayoutCeiling: 1, IdentityStatus: "structural", EvidenceJSON: `{}`}
	instruments := []CanonicalInstrumentSpec{
		{Venue: "kalshi", Ticker: kTicker, EventID: event.EventID, PayoffID: payoff.PayoffID,
			NativeSide: "YES", Orientation: "same", IdentityStatus: "structural", EvidenceJSON: `{}`},
		{Venue: "polyus", Ticker: pSlug, EventID: event.EventID, PayoffID: payoff.PayoffID,
			NativeSide: "YES", Orientation: "same", IdentityStatus: "structural", EvidenceJSON: `{}`},
	}
	if _, err := st.RegisterCanonicalBatch(context.Background(), []CanonicalEventSpec{event},
		[]CanonicalPayoffSpec{payoff}, instruments); err != nil {
		t.Fatal(err)
	}
}

func TestR139RuleArtifactsAreExactVersionedAppendOnlyAndAuthorityZero(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx, now := context.Background(), time.Now().UTC()
	in := VenueRuleArtifact{Venue: "kalshi", InstrumentID: "KX-RULE", Source: "GET /markets/{ticker}",
		Primary: "If Team A wins, Yes.", Secondary: "Postponement clause.",
		EarlyClose: "After winner declared.", SettlementSources: []RuleSettlementSource{{Name: "Official League", URL: "https://league.example/rules"}}, Observed: now}
	first, inserted, err := st.InsertVenueRuleArtifact(ctx, in)
	if err != nil || !inserted || first.Version != 1 || len(first.ArtifactHash) != 64 ||
		first.CaptureState != "RAW_COMPLETE_REVIEW_REQUIRED" {
		t.Fatalf("first artifact=%+v inserted=%v err=%v", first, inserted, err)
	}
	second, inserted, err := st.InsertVenueRuleArtifact(ctx, in)
	if err != nil || inserted || second.Version != 1 || second.ArtifactHash != first.ArtifactHash {
		t.Fatalf("dedup artifact=%+v inserted=%v err=%v", second, inserted, err)
	}
	in.Primary += " Changed."
	third, inserted, err := st.InsertVenueRuleArtifact(ctx, in)
	if err != nil || !inserted || third.Version != 2 || third.ArtifactHash == first.ArtifactHash {
		t.Fatalf("changed artifact=%+v inserted=%v err=%v", third, inserted, err)
	}
	for _, q := range []string{
		`UPDATE research_rule_artifacts SET raw_rules_json='{}' WHERE venue='kalshi' AND instrument_id='KX-RULE'`,
		`DELETE FROM research_rule_artifacts WHERE venue='kalshi' AND instrument_id='KX-RULE'`,
	} {
		if _, err := st.db.Exec(q); err == nil {
			t.Fatalf("raw rule artifact mutation succeeded: %s", q)
		}
	}
	authorized := in
	authorized.InstrumentID, authorized.LiveAuthority = "KX-BAD", true
	if _, _, err := st.InsertVenueRuleArtifact(ctx, authorized); err == nil {
		t.Fatal("rule artifact gained LIVE authority")
	}
}

func TestR139StructuralRulePairQueryIsExactAndBounded(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	insert := func(venue, ticker, game, kind, yes, no string, line float64, src string) {
		t.Helper()
		_, err := st.db.Exec(`INSERT INTO market_game(venue,ticker,game_id,mkt_type,yes_team,no_team,line,src,first_seen,last_seen)
VALUES(?,?,?,?,?,?,?,?,?,?)`, venue, ticker, game, kind, yes, no, line, src, now, now)
		if err != nil {
			t.Fatal(err)
		}
	}
	insert("kalshi", "KX-1", "G1", "winner", "PHI", "KC", 0, "struct")
	insert("polyus", "pus-1", "G1", "winner", "PHI", "KC", 0, "struct")
	insert("polyus", "pus-wrong-team", "G1", "winner", "KC", "PHI", 0, "struct")
	insert("polyus", "pus-fuzzy", "G1", "winner", "PHI", "KC", 0, "fuzzy")
	insert("kalshi", "KX-SPREAD", "G1", "spread", "PHI", "KC", -1.5, "struct")
	insert("polyus", "pus-wrong-line", "G1", "spread", "PHI", "KC", -2.5, "struct")
	insert("polymarket", "0xpint-1", "G1", "winner", "PHI", "KC", 0, "struct")
	pairs, err := st.StructuralKPolyRulePairs(context.Background(), time.Now().Add(-time.Hour), 10)
	if err != nil || len(pairs) != 1 || pairs[0].KalshiTicker != "KX-1" || pairs[0].PolyUSSlug != "pus-1" {
		t.Fatalf("exact structural pairs=%+v err=%v", pairs, err)
	}
	allPairs, err := st.StructuralRulePairs(context.Background(), time.Now().Add(-time.Hour), 20)
	if err != nil {
		t.Fatal(err)
	}
	types := map[string]bool{}
	for _, pair := range allPairs {
		types[pair.PairType] = true
	}
	if !types["K-PUS"] || !types["K-PINT"] || !types["PUS-PINT"] {
		t.Fatalf("all-pair structural funnel omitted a venue combination: types=%v pairs=%+v", types, allPairs)
	}
}

func TestStructuralRuleCandidatesNeverSilentlyDropSideOrLineMismatches(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	insert := func(venue, ticker, yes, no string, line float64) {
		t.Helper()
		if _, err := st.db.Exec(`INSERT INTO market_game(venue,ticker,game_id,mkt_type,yes_team,no_team,line,src,first_seen,last_seen)
VALUES(?,?,?,?,?,?,?,?,?,?)`, venue, ticker, "G-MISMATCH", "spread", yes, no, line, "struct", now, now); err != nil {
			t.Fatal(err)
		}
	}
	insert("kalshi", "K-MISMATCH", "PHI", "KC", -1.5)
	insert("polyus", "P-LINE", "PHI", "KC", -2.5)
	insert("polymarket", "I-SIDE", "NYM", "ATL", -1.5)
	page, err := st.StructuralRuleCandidatePageAfter(context.Background(), time.Now().Add(-time.Hour), StructuralRuleCursor{}, 20)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]StructuralRulePair{}
	for _, pair := range page.Rows {
		seen[pair.PairType] = pair
	}
	for _, pairType := range []string{"K-PUS", "K-PINT", "PUS-PINT"} {
		if pair, ok := seen[pairType]; !ok {
			t.Fatalf("%s structurally considered mismatch vanished: rows=%+v", pairType, page.Rows)
		} else if pair.Orientation != "" {
			t.Fatalf("%s unsafe mismatch received a structural orientation: %+v", pairType, pair)
		}
	}
}

func TestStructuralRuleCandidatesJoinOrdinaryPlayerPropAliasesAndRawTitles(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	for _, row := range []struct {
		venue, ticker, kind, title string
	}{
		{"kalshi", "K-PLAYER", "prop", "Lakers at Celtics — LeBron James over 25.5 points (full game)"},
		{"polyus", "P-PLAYER", "player_prop", "Will LeBron James record over 25.5 points in the full game?"},
	} {
		if _, err := st.db.Exec(`INSERT INTO market_game(venue,ticker,game_id,mkt_type,yes_team,no_team,line,src,first_seen,last_seen)
VALUES(?,?,?,?,?,?,?,?,?,?)`, row.venue, row.ticker, "G-PLAYER", row.kind, "", "", 25.5, "struct", now, now); err != nil {
			t.Fatal(err)
		}
		if err := st.UpsertMarketCatalog(ctx, []CatalogRow{{Venue: row.venue, Ticker: row.ticker,
			EventKey: "G-PLAYER", Kind: "prop", Title: row.title}}); err != nil {
			t.Fatal(err)
		}
	}
	page, err := st.StructuralRuleCandidatePageAfter(ctx, time.Now().Add(-time.Hour), StructuralRuleCursor{}, 10)
	if err != nil || len(page.Rows) != 1 {
		t.Fatalf("player alias candidate rows=%+v err=%v", page.Rows, err)
	}
	pair := page.Rows[0]
	if pair.LeftMarketType != "prop" || pair.RightMarketType != "player_prop" ||
		!strings.Contains(pair.LeftTitle, "LeBron James") || !strings.Contains(pair.RightTitle, "LeBron James") {
		t.Fatalf("per-leg player metadata was not retained: %+v", pair)
	}
}

func TestStructuralRuleCandidatePagesRotateWithoutNewestTailStarvation(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	for i := 0; i < 5; i++ {
		game := "ROT-" + string(rune('A'+i))
		for _, row := range []struct{ venue, id string }{{"kalshi", "K-" + game}, {"polyus", "P-" + game}} {
			if _, err := st.db.Exec(`INSERT INTO market_game(venue,ticker,game_id,mkt_type,yes_team,no_team,line,src,first_seen,last_seen)
VALUES(?,?,?,?,?,?,?,?,?,?)`, row.venue, row.id, game, "winner", "PHI", "KC", 0, "struct", now, now); err != nil {
				t.Fatal(err)
			}
		}
	}
	ctx := context.Background()
	cursor := StructuralRuleCursor{}
	seen := map[string]bool{}
	for pageN := 0; pageN < 3; pageN++ {
		page, err := st.StructuralRuleCandidatePageAfter(ctx, time.Now().Add(-time.Hour), cursor, 2)
		if err != nil {
			t.Fatal(err)
		}
		if page.Total != 5 || len(page.Rows) == 0 || len(page.Rows) > 2 {
			t.Fatalf("page %d=%+v", pageN, page)
		}
		for _, row := range page.Rows {
			seen[row.PairID] = true
		}
		cursor = page.Next
	}
	if len(seen) != 5 {
		t.Fatalf("rotation saw %d/5 pairs: %v", len(seen), seen)
	}
	wrapped, err := st.StructuralRuleCandidatePageAfter(ctx, time.Now().Add(-time.Hour), cursor, 2)
	if err != nil {
		t.Fatal(err)
	}
	if !wrapped.Wrapped || len(wrapped.Rows) != 2 {
		t.Fatalf("tail did not wrap deterministically: %+v", wrapped)
	}
}

func TestR139RulePairCertificateNeedsCurrentRawArtifactsAndIndependentEqualNormalization(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx, now := context.Background(), time.Now().UTC()
	kTicker, pSlug := "KX-CERT", "pus-cert"
	seedRuleCanonical(t, st, kTicker, pSlug)
	k, _, err := st.InsertVenueRuleArtifact(ctx, VenueRuleArtifact{Venue: "kalshi", InstrumentID: kTicker,
		Source: "kalshi market+event REST", Primary: "Team wins under official rules.",
		SettlementSources: []RuleSettlementSource{{Name: "League", URL: "https://league.example/box"}}, Observed: now})
	if err != nil {
		t.Fatal(err)
	}
	p, _, err := st.InsertVenueRuleArtifact(ctx, VenueRuleArtifact{Venue: "polyus", InstrumentID: pSlug,
		Source: "polyus market REST", Description: "Winner under house rules.", Observed: now})
	if err != nil {
		t.Fatal(err)
	}
	pair := StructuralRulePair{PairID: "k-pus|" + kTicker + "|" + pSlug, KalshiTicker: kTicker, PolyUSSlug: pSlug}
	review, _, err := st.RecordRulePairReview(ctx, pair, now)
	if err != nil || review.NormalizationState != "MISSING_POLYUS_SETTLEMENT_SOURCE" {
		t.Fatalf("pre-review blocker=%+v err=%v", review, err)
	}
	terms := NormalizedRuleTerms{SettlementSourceID: "official-league-box-score",
		SettlementSourceURL: "https://league.example/box", PredicateHash: strings.Repeat("1", 64),
		RulesHash: strings.Repeat("2", 64), VoidPolicy: "refund-principal", ScalarPolicy: "binary-0-or-1",
		UnknownPolicy: "refund-principal", TimingPolicy: "official-final-including-overtime"}
	spec := RulePairCertificateSpec{PairID: pair.PairID, Version: 1, KalshiTicker: kTicker, PolyUSSlug: pSlug,
		KalshiArtifactHash: k.ArtifactHash, PolyUSArtifactHash: p.ArtifactHash,
		CanonicalEventID: "sports:game-rule", CanonicalPayoffID: "sports:game-rule|full_game|winner|PHI",
		Left: terms, Right: terms, ReviewMethod: "human_review", ReviewProvenance: "signed review R139",
		ReviewEvidenceHash: strings.Repeat("3", 64)}
	bad := spec
	bad.Right.VoidPolicy = "cancel-as-loss"
	if _, _, err := st.RegisterRulePairCertificate(ctx, bad); err == nil {
		t.Fatal("mismatched normalized venue policies were certified")
	}
	bad = spec
	bad.LiveAuthority = true
	if _, _, err := st.RegisterRulePairCertificate(ctx, bad); err == nil {
		t.Fatal("normalized certificate gained LIVE authority")
	}
	cert, inserted, err := st.RegisterRulePairCertificate(ctx, spec)
	if err != nil || !inserted || len(cert.SpecHash) != 64 || !strings.Contains(cert.BasisID, cert.SpecHash) {
		t.Fatalf("compatible cert=%+v inserted=%v err=%v", cert, inserted, err)
	}
	review, _, err = st.RecordRulePairReview(ctx, pair, now.Add(time.Second))
	if err != nil || review.NormalizationState != "COMPATIBLE_CERTIFIED" || review.CertificateHash != cert.SpecHash {
		t.Fatalf("certified review=%+v err=%v", review, err)
	}
	if _, err := st.db.Exec(`UPDATE research_rule_pair_certificates SET void_policy='loss' WHERE pair_id=?`, pair.PairID); err == nil {
		t.Fatal("normalized certificate mutation succeeded")
	}
	// A raw venue revision invalidates the certificate without deleting history.
	p.Description = "Winner under changed house rules."
	if _, inserted, err := st.InsertVenueRuleArtifact(ctx, p); err != nil || !inserted {
		t.Fatalf("revised raw artifact inserted=%v err=%v", inserted, err)
	}
	if stale, current, err := st.CompatibleRulePairCertificate(ctx, kTicker, pSlug); err != nil || current || stale.SpecHash != cert.SpecHash {
		t.Fatalf("stale certificate current=%v cert=%+v err=%v", current, stale, err)
	}
	review, _, err = st.RecordRulePairReview(ctx, pair, now.Add(2*time.Second))
	if err != nil || review.NormalizationState != "CERTIFICATE_STALE" {
		t.Fatalf("stale review=%+v err=%v", review, err)
	}
}

func TestR139AllVenuePairsPersistReviewedFlipAndThreeWayRequiresCompleteCurrentTriangle(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	event := CanonicalEventSpec{EventID: "sports:triple-rule", EventType: "sports-game", Domain: "mlb",
		OutcomeSetStatus: "complete", EvidenceJSON: `{}`}
	payA := CanonicalPayoffSpec{PayoffID: event.EventID + "|winner|PHI", EventID: event.EventID,
		Label: "PHI wins", PredicateJSON: `{"winner":"PHI"}`, PayoutCeiling: 1, IdentityStatus: "structural", EvidenceJSON: `{}`}
	payB := CanonicalPayoffSpec{PayoffID: event.EventID + "|winner|KC", EventID: event.EventID,
		Label: "KC wins", PredicateJSON: `{"winner":"KC"}`, PayoutCeiling: 1, IdentityStatus: "structural", EvidenceJSON: `{}`}
	instruments := []CanonicalInstrumentSpec{
		{Venue: "kalshi", Ticker: "K-TRIPLE", EventID: event.EventID, PayoffID: payA.PayoffID, NativeSide: "YES", Orientation: "same", IdentityStatus: "structural", EvidenceJSON: `{}`},
		{Venue: "polyus", Ticker: "pus-triple", EventID: event.EventID, PayoffID: payA.PayoffID, NativeSide: "YES", Orientation: "same", IdentityStatus: "structural", EvidenceJSON: `{}`},
		{Venue: "polymarket", Ticker: "0xtriple", EventID: event.EventID, PayoffID: payB.PayoffID, NativeSide: "YES", Orientation: "same", IdentityStatus: "structural", EvidenceJSON: `{}`},
	}
	if _, err := st.RegisterCanonicalBatch(ctx, []CanonicalEventSpec{event}, []CanonicalPayoffSpec{payA, payB}, instruments); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	artifacts := map[string]VenueRuleArtifact{}
	for _, key := range []struct{ venue, id string }{{"kalshi", "K-TRIPLE"}, {"polyus", "pus-triple"}, {"polymarket", "0xtriple"}} {
		artifact, _, err := st.InsertVenueRuleArtifact(ctx, VenueRuleArtifact{Venue: key.venue,
			InstrumentID: key.id, Source: "official cached schema", Description: "exact raw rule " + key.venue,
			SettlementSources: []RuleSettlementSource{{Name: "Official", URL: "https://official.example/final"}}, Observed: now})
		if err != nil {
			t.Fatal(err)
		}
		artifacts[key.venue] = artifact
	}
	terms := NormalizedRuleTerms{SettlementSourceID: "official-final", SettlementSourceURL: "https://official.example/final",
		PredicateHash: strings.Repeat("b", 64), RulesHash: strings.Repeat("c", 64), VoidPolicy: "refund",
		ScalarPolicy: "binary", UnknownPolicy: "refund", TimingPolicy: "official-final"}
	register := func(pairID, lv, lid, rv, rid, orientation, canonicalPayoff string) RulePairCertificateSpec {
		t.Helper()
		cert, inserted, err := st.RegisterRulePairCertificate(ctx, RulePairCertificateSpec{PairID: pairID, Version: 1,
			LeftVenue: lv, LeftInstrumentID: lid, RightVenue: rv, RightInstrumentID: rid, Orientation: orientation,
			LeftArtifactHash: artifacts[lv].ArtifactHash, RightArtifactHash: artifacts[rv].ArtifactHash,
			CanonicalEventID: event.EventID, CanonicalPayoffID: canonicalPayoff, Left: terms, Right: terms,
			ReviewMethod: "human_review", ReviewProvenance: "signed independent triple review",
			ReviewEvidenceHash: strings.Repeat("d", 64)})
		if err != nil || !inserted {
			t.Fatalf("register %s inserted=%v err=%v cert=%+v", pairID, inserted, err, cert)
		}
		return cert
	}
	register("k-pus|K-TRIPLE|pus-triple", "kalshi", "K-TRIPLE", "polyus", "pus-triple", "same", payA.PayoffID)
	register("k-pint|K-TRIPLE|0xtriple", "kalshi", "K-TRIPLE", "polymarket", "0xtriple", "inverse", payA.PayoffID)
	status, err := st.ThreeWayRuleCertificateStatus(ctx, "K-TRIPLE", "pus-triple", "0xtriple")
	if err != nil || status.Current || !strings.Contains(status.Blocker, "PUS-PINT") {
		t.Fatalf("two edges incorrectly made three-way economic: %+v err=%v", status, err)
	}
	pi := register("pus-pint|pus-triple|0xtriple", "polyus", "pus-triple", "polymarket", "0xtriple", "inverse", payA.PayoffID)
	if pi.Orientation != "inverse" || pi.PairType != "PUS-PINT" {
		t.Fatalf("reviewed flip not persisted: %+v", pi)
	}
	status, err = st.ThreeWayRuleCertificateStatus(ctx, "K-TRIPLE", "pus-triple", "0xtriple")
	if err != nil || !status.Current || !status.OrientationConsistent || len(status.PairCertificateHashes) != 3 {
		t.Fatalf("complete current triangle rejected: %+v err=%v", status, err)
	}
	changed := artifacts["polymarket"]
	changed.Description += " revised"
	if _, inserted, err := st.InsertVenueRuleArtifact(ctx, changed); err != nil || !inserted {
		t.Fatalf("stale fixture inserted=%v err=%v", inserted, err)
	}
	status, err = st.ThreeWayRuleCertificateStatus(ctx, "K-TRIPLE", "pus-triple", "0xtriple")
	if err != nil || status.Current || !strings.Contains(status.Blocker, "K-PINT") {
		t.Fatalf("stale venue artifact did not revoke triangle: %+v err=%v", status, err)
	}
}
