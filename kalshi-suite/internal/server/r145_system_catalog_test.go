package server

import (
	"reflect"
	"strings"
	"testing"
)

func TestR145CanonicalSystemCatalogCounts(t *testing.T) {
	got := r145CanonicalSystemCatalogCounts()
	if got.BaseSystems != 119 {
		t.Fatalf("base systems=%d, want 119", got.BaseSystems)
	}
	if got.DeclaredTypedVariants != 302 || got.KnownExactExecutionVariants != 302 {
		t.Fatalf("declared typed variants=%d connected compatibility=%d, want 302/302",
			got.DeclaredTypedVariants, got.KnownExactExecutionVariants)
	}
	if got.CodePathConnectedVariants != 302 || got.DeclaredWithoutCodePathVariants != 0 {
		t.Fatalf("code-path-connected=%d disconnected=%d, want 302/0",
			got.CodePathConnectedVariants, got.DeclaredWithoutCodePathVariants)
	}
	if got.BundleProductSystems != 4 || got.DeclaredBundleProductVariants != 10 ||
		got.ConnectedStagedBundleVariants != 4 || got.ExternallyBlockedBundleVariants != 6 {
		t.Fatalf("bundle variant counts changed unexpectedly: %+v", got)
	}
	if got.MissingTypedVariantSystems != 28 || len(got.MissingVariantSystemIDs) != 28 {
		t.Fatalf("missing typed systems=%d ids=%d, want 28", got.MissingTypedVariantSystems, len(got.MissingVariantSystemIDs))
	}
	wantMissing := []string{
		"adaptive-whale-scorer", "basket", "behavioral-bias-regime", "combo-overlay", "combo-rfq",
		"combo-synth", "divergence", "fade", "fee-rounding-batch",
		"identity-challenged-cross-venue-lock", "incentive-maker",
		"incentive-subsidized-structural-lock", "independent-probabilistic-weather", "insider",
		"joint-marginal-lock", "kflow-live", "kflow-pre", "lifecycle-reopen", "lockstack",
		"pcrypto", "pflow", "poly-consensus", "poly-whale",
		"queue-priority", "skillbuy", "subcent-golf", "weather", "whale-exit",
	}
	if !reflect.DeepEqual(got.MissingVariantSystemIDs, wantMissing) {
		t.Fatalf("missing typed systems mismatch\n got: %v\nwant: %v", got.MissingVariantSystemIDs, wantMissing)
	}
}

func TestR147CatalogClassifiesNonOrderNamesInsteadOfFabricatingVariants(t *testing.T) {
	got := r145CanonicalSystemCatalogCounts()
	if got.OrderOriginatingBaseSystems != 89 || got.ChildOverlayBaseSystems != 1 ||
		got.ClassifiedNonOrderBaseSystems != 30 || got.ResearchVenueBaseSystems != 10 ||
		got.DataControlBaseSystems != 9 || got.ResearchCohortBaseSystems != 2 ||
		got.UnsupportedProductBaseSystems != 1 || got.FrozenBaseSystems != 1 ||
		got.PortfolioEvidenceBaseSystems != 6 {
		t.Fatalf("execution classification counts changed unexpectedly: %+v", got)
	}
	if got.MissingExecutableVariantSystems != 0 || len(got.MissingExecutableSystemIDs) != 0 {
		t.Fatalf("order-originating systems without an exact route: %v", got.MissingExecutableSystemIDs)
	}
	if got.ExternallyBlockedDecisionSystems != 1 || !reflect.DeepEqual(got.ExternallyBlockedDecisionIDs,
		[]string{"xvlock"}) {
		t.Fatalf("externally blocked payoff systems are hidden: %v", got.ExternallyBlockedDecisionIDs)
	}
	for _, row := range r145CanonicalBaseSystems() {
		if row.ExecutionRole == "" || strings.TrimSpace(row.Rationale) == "" {
			t.Fatalf("base lacks execution classification: %+v", row)
		}
	}
}

func TestR147NativeVariantsRequireNamedAdapters(t *testing.T) {
	seen := map[string]r145SystemCatalogVariant{}
	for _, row := range r145KnownSystemVariants() {
		key := row.SystemID + "|" + row.Venue + "|" + row.Side + "|" + row.Route
		seen[key] = row
		if row.Handoff == "standalone" || row.Handoff == "" || row.Handoff == r145HandoffUnregistered {
			t.Fatalf("declared variant lacks a named adapter: %+v", row)
		}
	}
	for _, key := range []string{
		"arb|polyus|NO|taker",
		"freshfade|kalshi|NO|taker", "invert:freshlist|kalshi|NO|taker",
		"freshlist|kalshi|YES|taker", "invert:pfbridge|polyus|NO|taker",
		"fbridge|kalshi|NO|taker", "fbridge|kalshi|YES|taker", "fbridge|polyus|NO|taker",
		"pfbridge|kalshi|NO|taker", "pfbridge|kalshi|YES|taker", "pfbridge|polyus|NO|taker",
		"pmatch|kalshi|NO|taker", "pmatch|kalshi|YES|taker", "pmatch|polyus|NO|taker",
		"notail|polyus|NO|taker", "wxedge|kalshi|YES|taker",
		"xvlag|kalshi|YES|taker", "xvlag|polyus|NO|taker",
	} {
		if _, ok := seen[key]; !ok {
			t.Errorf("real emitted route missing from catalog: %s", key)
		}
	}
	if _, falseMaker := seen["invert:kthresh|kalshi|NO|maker"]; falseMaker {
		t.Fatal("independent inverse still fabricates a maker route")
	}
	if r145CatalogVariantHasCodePath(r145SystemCatalogVariant{SystemID: "invert:kthresh",
		Venue: "kalshi", Side: "NO", Route: "maker", Handoff: "standalone"}) {
		t.Fatal("legacy standalone marker still bypasses the adapter contract")
	}
	for _, cell := range r145NativeKnownVariantCells {
		direct, ok := r145ParseCatalogVariantCell(cell, "")
		if !ok || direct.Route != "taker" || strings.HasPrefix(direct.SystemID, "invert:") ||
			!r145PolicySignalProducer(direct.SystemID) {
			continue
		}
		inverseSide := "YES"
		if direct.Side == "YES" {
			inverseSide = "NO"
		}
		key := "invert:" + direct.SystemID + "|" + direct.Venue + "|" + inverseSide + "|taker"
		if _, ok := seen[key]; !ok {
			t.Errorf("central signal choke inverse is missing from catalog: %s", key)
		}
	}
	for key := range seen {
		if strings.HasPrefix(key, "invert:invert:") {
			t.Errorf("catalog contains recursive inverse: %s", key)
		}
	}
}

func TestR145DeclaredVariantsDoNotMasqueradeAsConnected(t *testing.T) {
	disconnected := map[string]int{}
	for _, row := range r145KnownSystemVariants() {
		if !r145CatalogVariantHasCodePath(row) {
			disconnected[row.SystemID]++
		}
	}
	want := map[string]int{}
	if !reflect.DeepEqual(disconnected, want) {
		t.Fatalf("disconnected declared cells mismatch\n got: %v\nwant: %v", disconnected, want)
	}
}

func TestR145TracedExactExecutionVariants(t *testing.T) {
	want := []string{
		"confluence|kalshi|YES|taker", "confluence|polyus|NO|taker", "confluence|polyus|YES|taker",
		"cross|kalshi|YES|taker",
		"fundtilt|kalshi|NO|taker", "fundtilt|kalshi|YES|taker",
		"fundtilt|polyus|NO|taker", "fundtilt|polyus|YES|taker",
		"meanrev|kalshi|NO|taker", "meanrev|kalshi|YES|taker", "meanrev|polyus|NO|taker", "meanrev|polyus|YES|taker",
		"ml-book|kalshi|NO|maker", "ml-book|kalshi|NO|taker", "ml-book|kalshi|YES|maker", "ml-book|kalshi|YES|taker",
		"ml-book|polyus|NO|maker", "ml-book|polyus|NO|taker", "ml-book|polyus|YES|maker", "ml-book|polyus|YES|taker",
		"rawflow|kalshi|NO|maker", "rawflow|kalshi|NO|taker", "rawflow|kalshi|YES|maker", "rawflow|kalshi|YES|taker",
		"sharpline|kalshi|NO|taker", "sharpline|kalshi|YES|taker", "sharpline|polyus|NO|taker", "sharpline|polyus|YES|taker",
		"whale-exit-hold-bridge|kalshi|NO|taker", "whale-exit-hold-bridge|kalshi|YES|taker",
		"whale-exit-hold-bridge|polyus|NO|taker", "whale-exit-hold-bridge|polyus|YES|taker",
	}
	wantSystems := map[string]bool{
		"confluence": true, "cross": true, "fundtilt": true, "meanrev": true,
		"ml-book": true, "rawflow": true, "sharpline": true, "whale-exit-hold-bridge": true,
	}
	got := make([]string, 0, len(want))
	for _, row := range r145KnownSystemVariants() {
		if wantSystems[row.SystemID] {
			got = append(got, row.SystemID+"|"+row.Venue+"|"+row.Side+"|"+row.Route)
		}
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("traced exact variants mismatch\n got: %v\nwant: %v", got, want)
	}
}

func TestR145CanonicalSystemCatalogRolesDoNotOverlap(t *testing.T) {
	seen := map[string]r145CatalogRole{}
	for _, rows := range [][]r145SystemCatalogEntry{
		r145CanonicalBaseSystems(), r145CanonicalSystemAliases(), r145CanonicalSystemComponents(),
	} {
		for _, row := range rows {
			if prior, ok := seen[row.ID]; ok {
				t.Fatalf("catalog id %q has two roles: %s and %s", row.ID, prior, row.Role)
			}
			seen[row.ID] = row.Role
		}
	}
	for _, id := range []string{"adaptive-whale-scorer", "behavioral-bias-regime", "cheapband", "favlong80"} {
		if seen[id] != r145CatalogBase {
			t.Fatalf("operator/dedicated system %q role=%q, want base", id, seen[id])
		}
	}
	for _, id := range []string{"raw-flow-observer", "proper-score-brier", "parlay-6leg"} {
		if seen[id] != r145CatalogComponent {
			t.Fatalf("diagnostic/cohort %q role=%q, want component", id, seen[id])
		}
	}
}

func TestR145CanonicalSystemAliasNormalization(t *testing.T) {
	tests := []struct {
		name, venue, side, route string
		want                     r145SystemCatalogVariant
	}{
		{"strategy:kalshi-flow@kalshi", "", "YES", "taker", r145SystemCatalogVariant{SystemID: "kalshi-flow", Venue: "kalshi", Side: "YES", Route: "taker"}},
		{"taker:kalshi-flow@kalshi [NO]", "", "", "", r145SystemCatalogVariant{SystemID: "kalshi-flow", Venue: "kalshi", Side: "NO", Route: "taker"}},
		{"gf:invert:kcrypto:yes:taker-k", "", "", "", r145SystemCatalogVariant{SystemID: "invert:kcrypto", Venue: "kalshi", Side: "YES", Route: "taker"}},
		{"book:favlong80", "", "YES", "maker", r145SystemCatalogVariant{SystemID: "favlong80", Venue: "kalshi", Side: "YES", Route: "maker"}},
		{"book:cheapband-p", "", "NO", "taker", r145SystemCatalogVariant{SystemID: "cheapband", Venue: "polyus", Side: "NO", Route: "taker"}},
		{"book:freshinv-k", "", "NO", "taker", r145SystemCatalogVariant{SystemID: "invert:freshlist", Venue: "kalshi", Side: "NO", Route: "taker"}},
		{"book:xvgap", "", "YES", "taker", r145SystemCatalogVariant{SystemID: "xvgap", Venue: "polyus", Side: "YES", Route: "taker"}},
	}
	for _, tt := range tests {
		got, ok := r145CanonicalSystemIdentity(tt.name, tt.venue, tt.side, tt.route)
		if !ok || got.SystemID != tt.want.SystemID || got.Venue != tt.want.Venue ||
			got.Side != tt.want.Side || got.Route != tt.want.Route {
			t.Errorf("identity(%q)=(%+v,%v), want %+v", tt.name, got, ok, tt.want)
		}
	}
}

func TestR145CanonicalSystemIdentityFailsClosed(t *testing.T) {
	tests := []struct{ name, venue, side, route string }{
		{"maker:kalshi-flow@kalshi [YES]", "kalshi", "YES", "taker"},
		{"strategy:kalshi-flow@polyus", "kalshi", "YES", "taker"},
		{"taker:kalshi-flow@kalshi [NO]", "kalshi", "YES", "taker"},
		{"book:unknown", "kalshi", "YES", "taker"},
		{"side-control:kalshi-flow", "kalshi", "NO", "taker"},
		{"counterfactual:invert:kalshi-flow", "kalshi", "NO", "taker"},
		{"invert:invert:kalshi-flow", "kalshi", "YES", "taker"},
	}
	for _, tt := range tests {
		if got, ok := r145CanonicalSystemIdentity(tt.name, tt.venue, tt.side, tt.route); ok {
			t.Errorf("identity(%q) unexpectedly accepted: %+v", tt.name, got)
		}
	}
}

func TestR145KnownVariantsHaveCanonicalBasesAndExactCells(t *testing.T) {
	bases := r145CatalogBaseSet()
	seen := map[string]bool{}
	for _, row := range r145KnownSystemVariants() {
		if _, ok := bases[row.SystemID]; !ok {
			t.Errorf("variant has non-canonical base: %+v", row)
		}
		if row.Venue != "kalshi" && row.Venue != "polyus" {
			t.Errorf("variant has non-tradeable venue: %+v", row)
		}
		if row.Side != "YES" && row.Side != "NO" {
			t.Errorf("variant has non-exact side: %+v", row)
		}
		if row.Route != "maker" && row.Route != "taker" && row.Route != "rfq" {
			t.Errorf("variant has non-exact route: %+v", row)
		}
		key := row.SystemID + "|" + row.Venue + "|" + row.Side + "|" + row.Route
		if seen[key] {
			t.Errorf("duplicate exact variant %s", key)
		}
		seen[key] = true
	}
}
