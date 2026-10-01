package server

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/paper"
)

func TestAuditor67TwinLoadFailureRetriesAndRecovers(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	base := "KXETH-26JUL0817-T2509.99"
	twin := "KXETHD-26JUL0817-T2509.99"
	key := trueRestatementKey("kalshi", base)
	rec, ok := twinRecordFromDocuments(map[string]map[string]any{
		base: {"rules_primary": "same payoff"}, twin: {"rules_primary": "same payoff"},
	}, time.Date(2026, 7, 11, 1, 0, 0, 0, time.UTC))
	if !ok {
		t.Fatal("valid twin fixture did not produce evidence")
	}
	raw, err := json.Marshal(rec)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.store.KVSet(ctx, "twinv:"+key, string(raw)); err != nil {
		t.Fatal(err)
	}

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if s.twinLoad(canceled) {
		t.Fatal("canceled KV read must not mark the twin registry loaded")
	}
	stats := s.twinStats()
	if stats["load_state"] != "error" || stats["load_error"] == "" ||
		stats["load_attempts"] != 1 || stats["load_fail_closed"] != true {
		t.Fatalf("failed-load telemetry = %+v", stats)
	}
	pos := []paper.Position{{Platform: "kalshi", Ticker: base, Contracts: 1, CostBasis: .40}}
	if got := s.betConflictReason("kalshi", twin, "YES", "gate", pos); got != "dup-underlying" {
		t.Fatalf("failed registry read must fail closed for a known twin family, got %q", got)
	}

	if !s.twinLoad(ctx) {
		t.Fatal("healthy retry must load persisted twin verdicts")
	}
	stats = s.twinStats()
	if stats["load_state"] != "loaded" || stats["load_error"] != "" ||
		stats["load_attempts"] != 2 || stats["load_fail_closed"] != false ||
		stats["verified_identical"] != 1 {
		t.Fatalf("recovered-load telemetry = %+v", stats)
	}
	if got := s.betConflictReason("kalshi", twin, "YES", "gate", pos); got != "dup-underlying" {
		t.Fatalf("persisted identical verdict was not restored after retry, got %q", got)
	}
}

func TestAuditor67TwinGateFailsClosedOnlyForAffectedFamiliesUntilLoad(t *testing.T) {
	s := testServer(t)
	base := "KXBTC-26JUL0817-T108000"
	twin := "KXBTCD-26JUL0817-T108000"
	pos := []paper.Position{{Platform: "kalshi", Ticker: base, Contracts: 1, CostBasis: .40}}
	if got := s.betConflictReason("kalshi", twin, "YES", "gate", pos); got != "dup-underlying" {
		t.Fatalf("pending registry must fail closed for known KXBTC/KXBTCD family, got %q", got)
	}
	if got := s.twinKeyGate("kalshi", "KXMLBGAMED-26JUL11AB-YES"); got != "" {
		t.Fatalf("unrelated trailing-D series must not enter twin fail-closed scope, got %q", got)
	}
	if got := s.twinKeyGate("polyus", "KXBTCD-26JUL0817-T108000"); got != "" {
		t.Fatalf("non-Kalshi market must not enter twin fail-closed scope, got %q", got)
	}

	if !s.twinLoad(context.Background()) {
		t.Fatal("empty persisted twin registry must load")
	}
	if got := s.betConflictReason("kalshi", twin, "YES", "gate", pos); got != "" {
		t.Fatalf("successfully loaded but unverified twin must restore distinct/bettable rule, got %q", got)
	}
}

func TestAuditor67TwinEvidenceRequiresPayoffFieldsAndVersions(t *testing.T) {
	base := "KXETH-26JUL0817-T2509.99"
	twin := "KXETHD-26JUL0817-T2509.99"
	now := time.Date(2026, 7, 11, 2, 0, 0, 0, time.UTC)
	if rec, ok := twinRecordFromDocuments(map[string]map[string]any{
		base: {}, twin: {},
	}, now); ok || rec.Verdict == "identical" {
		t.Fatalf("empty market documents became twin evidence: %+v ok=%v", rec, ok)
	}
	rec, ok := twinRecordFromDocuments(map[string]map[string]any{
		base: {"floor_strike": 0.0, "rules_primary": "same"},
		twin: {"floor_strike": 0.0, "rules_primary": "same"},
	}, now)
	if !ok || rec.Verdict != "identical" || !validTwinRecord(rec) || rec.DocumentHash == "" || rec.PayoffFields != 2 {
		t.Fatalf("substantive equal documents did not produce valid evidence: %+v ok=%v", rec, ok)
	}
	oldHash := rec.DocumentHash
	changed, ok := twinRecordFromDocuments(map[string]map[string]any{
		base: {"floor_strike": 0.0, "rules_primary": "same"},
		twin: {"floor_strike": 0.0, "rules_primary": "changed"},
	}, now.Add(time.Minute))
	if !ok || changed.Verdict != "distinct" || changed.DocumentHash == oldHash {
		t.Fatalf("payoff-document change was not re-hashed/re-decided: old=%+v changed=%+v ok=%v", rec, changed, ok)
	}

	s := testServer(t)
	legacyKey := trueRestatementKey("kalshi", base)
	badVersionKey := trueRestatementKey("kalshi", "KXBTC-26JUL0817-T108000")
	if err := s.store.KVSet(context.Background(), "twinv:"+legacyKey, "identical"); err != nil {
		t.Fatal(err)
	}
	rec.Version++
	badVersion, _ := json.Marshal(rec)
	if err := s.store.KVSet(context.Background(), "twinv:"+badVersionKey, string(badVersion)); err != nil {
		t.Fatal(err)
	}
	if !s.twinLoad(context.Background()) {
		t.Fatal("KV read should succeed even though both evidence values are stale")
	}
	stats := s.twinStats()
	if stats["stale_evidence"] != 2 || stats["verified_identical"] != 0 ||
		stats["evidence_version"] != twinEvidenceVersion {
		t.Fatalf("legacy/version telemetry = %+v", stats)
	}
	if got := s.twinKeyGate("kalshi", twin); got != legacyKey {
		t.Fatalf("legacy evidence must be stale/fail-closed until rechecked, got %q", got)
	}
}

func TestAuditor67TwinSweepRevalidatesDocumentAndMembershipChanges(t *testing.T) {
	base := "KXETH-26JUL0817-T2509.99"
	twin := "KXETHD-26JUL0817-T2509.99"
	key := trueRestatementKey("kalshi", base)
	var mu sync.Mutex
	twinListed, changedRule := true, false
	venue := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		mu.Lock()
		listed, changed := twinListed, changedRule
		mu.Unlock()
		switch {
		case r.URL.Path == "/markets":
			series := r.URL.Query().Get("series_ticker")
			switch series {
			case "KXETH":
				_, _ = fmt.Fprintf(w, `{"markets":[{"ticker":%q}]}`, base)
			case "KXETHD":
				if listed {
					_, _ = fmt.Fprintf(w, `{"markets":[{"ticker":%q}]}`, twin)
				} else {
					_, _ = w.Write([]byte(`{"markets":[]}`))
				}
			default:
				_, _ = w.Write([]byte(`{"markets":[]}`))
			}
		case strings.HasPrefix(r.URL.Path, "/markets/"):
			ticker := strings.TrimPrefix(r.URL.Path, "/markets/")
			rule := "same payoff"
			if changed && ticker == twin {
				rule = "different payoff"
			}
			_, _ = fmt.Fprintf(w, `{"market":{"ticker":%q,"floor_strike":2509.99,"rules_primary":%q}}`, ticker, rule)
		default:
			http.NotFound(w, r)
		}
	}))
	defer venue.Close()
	privateKey, err := rsa.GenerateKey(rand.Reader, 1024)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := kalshi.NewSigner("twin-test", pem.EncodeToMemory(&pem.Block{
		Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(privateKey),
	}))
	if err != nil {
		t.Fatal(err)
	}
	s := testServer(t)
	s.kal = kalshi.NewClient(venue.URL, signer, 1000, 2*time.Second)
	s.twinSweep(context.Background())
	s.twinMu.Lock()
	first := s.twinEvidence[key]
	firstVerdict := s.twinVerdict[key]
	s.twinMu.Unlock()
	if firstVerdict != "identical" || !validTwinRecord(first) {
		t.Fatalf("initial sweep evidence=%+v verdict=%q", first, firstVerdict)
	}

	mu.Lock()
	changedRule = true
	mu.Unlock()
	s.twinSweep(context.Background())
	s.twinMu.Lock()
	changed := s.twinEvidence[key]
	changedVerdict := s.twinVerdict[key]
	s.twinMu.Unlock()
	if changedVerdict != "distinct" || changed.DocumentHash == first.DocumentHash || !changed.CheckedAt.After(first.CheckedAt) {
		t.Fatalf("document change was not revalidated: first=%+v changed=%+v verdict=%q", first, changed, changedVerdict)
	}
	if got := s.twinKeyGate("kalshi", twin); got != "" {
		t.Fatalf("fresh distinct verdict remained blocked, key=%q", got)
	}

	mu.Lock()
	twinListed = false
	changedRule = false
	mu.Unlock()
	s.twinSweep(context.Background())
	s.twinMu.Lock()
	stale := s.twinVerdict[key]
	s.twinMu.Unlock()
	if stale != "stale" || s.twinKeyGate("kalshi", base) != key {
		t.Fatalf("membership change did not stale/fail-close evidence: verdict=%q", stale)
	}

	mu.Lock()
	twinListed = true
	mu.Unlock()
	s.twinSweep(context.Background())
	s.twinMu.Lock()
	restored := s.twinEvidence[key]
	restoredVerdict := s.twinVerdict[key]
	s.twinMu.Unlock()
	if restoredVerdict != "identical" || !sameTwinMembers(restored.Members, []string{base, twin}) ||
		!restored.CheckedAt.After(changed.CheckedAt) {
		t.Fatalf("restored membership was not revalidated: %+v verdict=%q", restored, restoredVerdict)
	}
}
