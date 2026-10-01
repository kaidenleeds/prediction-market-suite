package server

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestR133MLBookSideNormalizationUsesBothExecutableSides(t *testing.T) {
	yes, ok := mlNormalizeBookSide("YES", .41, .44, 12, 7)
	if !ok || yes.MakerPrice != .41 || yes.TakerPrice != .44 || yes.MakerDepth != 12 || yes.TakerDepth != 7 {
		t.Fatalf("YES touch=%+v ok=%v", yes, ok)
	}
	no, ok := mlNormalizeBookSide("NO", .41, .44, 12, 7)
	if !ok || math.Abs(no.MakerPrice-.56) > 1e-12 || math.Abs(no.TakerPrice-.59) > 1e-12 || no.MakerDepth != 7 || no.TakerDepth != 12 {
		t.Fatalf("NO touch=%+v ok=%v", no, ok)
	}
	if _, ok := mlNormalizeBookSide("YES", .41, 0, 12, 0); ok {
		t.Fatal("one-sided visible price must not fabricate an executable book-v1 snapshot")
	}
	if _, ok := mlNormalizeBookSide("TEAM A", .41, .44, 12, 7); ok {
		t.Fatal("non-canonical outcome side must fail closed")
	}
}

func TestR133BookV2WarmupIsFreshReadyButHasZeroAuthority(t *testing.T) {
	s := testServer(t)
	cfg := *s.cfg()
	cfg.Auto.GoEvalEnabled = true
	s.cfgP.Store(&cfg)
	b := []byte(`{"model_status":"WARMING","feature_schema":"book-native-v2","model_cohort":"book-native-v2","book_feature_version":1,"book_v1_resolved":7,"book_v1_open":13,"min_train_required":150,"actionable_predictions":0,"live_authority":false,"predictions":[]}`)
	if err := os.WriteFile(filepath.Join(s.cfg().DataDir, "ml_predictions.json"), b, 0o644); err != nil {
		t.Fatal(err)
	}
	s.mlEval.lastErr = "model feature schema legacy refused"
	s.mlEval.lastErrAt = time.Now()
	ok, detail := s.mlEvalReady()
	if !ok || !strings.Contains(detail, "warming 7/150") || !strings.Contains(detail, "zero authority") {
		t.Fatalf("warm readiness ok=%v detail=%q", ok, detail)
	}
}

func TestR133GoEvalRefusesLegacyFeatureSchema(t *testing.T) {
	var body map[string]any
	if err := json.Unmarshal(synthExport(t, false), &body); err != nil {
		t.Fatal(err)
	}
	delete(body, "feature_schema")
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mlEvalParse(b); err == nil {
		t.Fatal("legacy export without the current book-native schema must be reference-only")
	}
}
