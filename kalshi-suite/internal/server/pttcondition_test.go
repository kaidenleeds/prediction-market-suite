package server

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

type fakeConditionResolver struct {
	calls  map[string]int
	cached map[string]string
}

func (f *fakeConditionResolver) ConditionByToken(_ context.Context, token string) (string, bool, error) {
	f.calls[token]++
	return "0x" + strings.Repeat(fmt.Sprintf("%02x", len(f.calls)), 32), true, nil
}

func (f *fakeConditionResolver) CachedConditionByToken(token string) (string, bool, bool) {
	id, ok := f.cached[token]
	return id, ok, ok
}

func TestR132ConditionRecoveryIsUniqueAndBudgetBounded(t *testing.T) {
	f := &fakeConditionResolver{calls: map[string]int{}}
	left := 10
	byToken := map[string]string{}
	tried := map[string]bool{}
	bad := "0x" + strings.Repeat("a", 62)
	cachedID := "0x" + strings.Repeat("ef", 32)
	f.cached = map[string]string{"cached-token": cachedID}
	if got, recovered := recoverPTTConditionID(context.Background(), f, bad, "cached-token", &left, byToken, tried); recovered || got != cachedID || left != 10 {
		t.Fatalf("cached recovery got=%q recovered=%v left=%d", got, recovered, left)
	}

	first, recovered := recoverPTTConditionID(context.Background(), f, bad, "token-0", &left, byToken, tried)
	if !recovered || len(first) != 66 {
		t.Fatalf("first recovery=%q recovered=%v", first, recovered)
	}
	again, recovered := recoverPTTConditionID(context.Background(), f, bad, "token-0", &left, byToken, tried)
	if recovered || again != first || f.calls["token-0"] != 1 || left != 9 {
		t.Fatalf("cache reuse again=%q recovered=%v calls=%d left=%d", again, recovered, f.calls["token-0"], left)
	}

	for i := 1; i <= 11; i++ {
		_, _ = recoverPTTConditionID(context.Background(), f, bad, fmt.Sprintf("token-%d", i), &left, byToken, tried)
	}
	if got := len(f.calls); got != 10 {
		t.Fatalf("unique HTTP recoveries=%d, want hard budget 10", got)
	}
	if left != 0 || tried["token-10"] || tried["token-11"] {
		t.Fatalf("budget state left=%d tried10=%v tried11=%v", left, tried["token-10"], tried["token-11"])
	}
}
