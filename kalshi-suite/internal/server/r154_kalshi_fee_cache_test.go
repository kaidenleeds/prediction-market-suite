package server

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/config"
	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
)

func r154FeeCacheServer(t *testing.T, dir string) *Server {
	t.Helper()
	s := &Server{log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	cfg := config.Config{DataDir: dir}
	s.cfgP.Store(&cfg)
	return s
}

func r154PutFeeSnapshot(t *testing.T, dir string, snap kalFeeRegistrySnapshot) {
	t.Helper()
	raw, err := json.Marshal(snap)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "kalshi_fee_registry_v1_00000000000000000001.json"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestR154FeeRegistrySnapshotSurvivesRestartWithoutNetwork(t *testing.T) {
	dir := t.TempDir()
	first := r154FeeCacheServer(t, dir)
	fetchedAt := time.Now().Add(-time.Hour)
	series := map[string]kalFeeInfo{
		"KXATPSETWINNER": kalFeeInfoFromVenue("quadratic", 1),
	}
	events := map[string]kalFeeInfo{
		"KXATPSETWINNER-26JUL17DZUMOL": kalFeeInfoFromVenue("quadratic_with_maker_fees", 2),
	}
	first.persistKalFeeRegistrySnapshot(series, events, map[string]string{
		"KXATPSETWINNER-26JUL17DZUMOL": "KXATPSETWINNER",
	}, fetchedAt, time.Now().Add(time.Hour))

	restarted := r154FeeCacheServer(t, dir)
	restarted.kalFeesLoadOnce.Do(restarted.loadKalFeeRegistrySnapshot)

	if restarted.kalFeesAt.IsZero() || len(restarted.kalFees) != 1 || len(restarted.kalEventFees) != 1 {
		t.Fatalf("durable registry not restored: at=%v series=%d events=%d", restarted.kalFeesAt, len(restarted.kalFees), len(restarted.kalEventFees))
	}
	fee, known, reason := restarted.kalFeeExact("KXATPSETWINNER-26JUL17DZUMOL-2-MOL", false, 1, 0.5)
	if !known || reason != "quadratic_with_maker_fees" || fee <= 0 {
		t.Fatalf("restored exact event fee = %.4f known=%v reason=%q", fee, known, reason)
	}
}

func TestR154FeeRegistrySnapshotRejectsStaleDueAndMalformedAuthority(t *testing.T) {
	tests := []struct {
		name string
		snap kalFeeRegistrySnapshot
	}{
		{
			name: "stale",
			snap: kalFeeRegistrySnapshot{
				Version:   kalFeeRegistrySnapshotVersion,
				FetchedAt: time.Now().Add(-kalFeeRegistryMaxAge - time.Minute),
				Series:    map[string]kalFeeRegistrySnapshotRow{"KXTEST": {FeeType: "quadratic", FeeMultiplier: 1}},
			},
		},
		{
			name: "activation due",
			snap: kalFeeRegistrySnapshot{
				Version:        kalFeeRegistrySnapshotVersion,
				FetchedAt:      time.Now().Add(-time.Hour),
				NextActivation: time.Now().Add(-time.Second),
				Series:         map[string]kalFeeRegistrySnapshotRow{"KXTEST": {FeeType: "quadratic", FeeMultiplier: 1}},
			},
		},
		{
			name: "malformed multiplier",
			snap: kalFeeRegistrySnapshot{
				Version:   kalFeeRegistrySnapshotVersion,
				FetchedAt: time.Now().Add(-time.Hour),
				Series:    map[string]kalFeeRegistrySnapshotRow{"KXTEST": {FeeType: "quadratic", FeeMultiplier: -1}},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			s := r154FeeCacheServer(t, dir)
			r154PutFeeSnapshot(t, dir, tc.snap)
			s.kalFeesLoadOnce.Do(s.loadKalFeeRegistrySnapshot)
			if s.kalFees != nil {
				t.Fatalf("unsafe snapshot loaded: %#v", s.kalFees)
			}
			if _, known, reason := s.kalFeeExact("KXTEST-26JUL17", false, 1, 0.5); known || !strings.Contains(reason, "not loaded") {
				t.Fatalf("fee authority known=%v reason=%q; want fail-closed not loaded", known, reason)
			}
		})
	}
}

func TestR154FeeRegistrySnapshotPreservesFutureActivationFence(t *testing.T) {
	dir := t.TempDir()
	s := r154FeeCacheServer(t, dir)
	next := time.Now().Add(30 * time.Minute)
	r154PutFeeSnapshot(t, dir, kalFeeRegistrySnapshot{
		Version:        kalFeeRegistrySnapshotVersion,
		FetchedAt:      time.Now().Add(-time.Hour),
		NextActivation: next,
		Series:         map[string]kalFeeRegistrySnapshotRow{"KXTEST": {FeeType: "quadratic", FeeMultiplier: 1}},
	})
	s.kalFeesLoadOnce.Do(s.loadKalFeeRegistrySnapshot)
	if s.kalFeesNext.IsZero() || s.kalFeesNext.Sub(next) > time.Second || next.Sub(s.kalFeesNext) > time.Second {
		t.Fatalf("next activation = %v, want %v", s.kalFeesNext, next)
	}
	if _, known, reason := s.kalFeeExact("KXTEST-26JUL17", false, 1, 0.5); !known || reason != "quadratic" {
		t.Fatalf("fresh pre-activation fee known=%v reason=%q", known, reason)
	}
}

func TestR154FeeRegistryReadinessUsesExactFreshness(t *testing.T) {
	now := time.Now()
	tests := []struct {
		name string
		at   time.Time
		next time.Time
		fees map[string]kalFeeInfo
		ok   bool
	}{
		{name: "missing", ok: false},
		{name: "fresh", at: now.Add(-time.Hour), fees: map[string]kalFeeInfo{"KXTEST": kalFeeInfoFromVenue("quadratic", 1)}, ok: true},
		{name: "stale", at: now.Add(-kalFeeRegistryMaxAge - time.Second), fees: map[string]kalFeeInfo{"KXTEST": kalFeeInfoFromVenue("quadratic", 1)}, ok: false},
		{name: "future", at: now.Add(time.Second), fees: map[string]kalFeeInfo{"KXTEST": kalFeeInfoFromVenue("quadratic", 1)}, ok: false},
		{name: "activation due", at: now.Add(-time.Hour), next: now, fees: map[string]kalFeeInfo{"KXTEST": kalFeeInfoFromVenue("quadratic", 1)}, ok: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := &Server{kalFees: tc.fees, kalFeesAt: tc.at, kalFeesNext: tc.next, kalFeesSource: "venue"}
			got, detail := s.kalFeeRegistryReadiness(now)
			if got != tc.ok {
				t.Fatalf("readiness=%v want=%v detail=%q", got, tc.ok, detail)
			}
			if !strings.Contains(detail, "series=") {
				t.Fatalf("detail lacks fee receipt: %q", detail)
			}
		})
	}
}

func TestR154FeeRegistryRefreshFetchesIndependentEndpointsConcurrently(t *testing.T) {
	var arrived atomic.Int32
	allArrived := make(chan struct{})
	var closeOnce sync.Once
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if arrived.Add(1) == 3 {
			closeOnce.Do(func() { close(allArrived) })
		}
		select {
		case <-allArrived:
		case <-time.After(time.Second):
			http.Error(w, "fee endpoints were fetched sequentially", http.StatusGatewayTimeout)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/series":
			_, _ = io.WriteString(w, `{"series":[{"ticker":"KXTEST","fee_type":"quadratic","fee_multiplier":1}]}`)
		case "/series/fee_changes":
			_, _ = io.WriteString(w, `{"series_fee_change_arr":[]}`)
		case "/events/fee_changes":
			_, _ = io.WriteString(w, `{"event_fee_changes":[],"cursor":""}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	dir := t.TempDir()
	s := r154FeeCacheServer(t, dir)
	s.kal = kalshi.NewClient(ts.URL, nil, 1000, 2*time.Second)
	s.refreshKalFees()

	if len(s.kalFees) != 1 || s.kalFeesSource != "venue" {
		t.Fatalf("parallel refresh did not publish exact registry: count=%d source=%q", len(s.kalFees), s.kalFeesSource)
	}
	files, err := filepath.Glob(filepath.Join(dir, kalFeeRegistrySnapshotGlob))
	if err != nil || len(files) != 1 {
		t.Fatalf("durable generations=%d err=%v, want 1", len(files), err)
	}
}

func TestR154FeeRegistryEmptyVenueResponsePreservesLastGood(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/series":
			_, _ = io.WriteString(w, `{"series":[]}`)
		case "/series/fee_changes":
			_, _ = io.WriteString(w, `{"series_fee_change_arr":[]}`)
		case "/events/fee_changes":
			_, _ = io.WriteString(w, `{"event_fee_changes":[],"cursor":""}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	s := r154FeeCacheServer(t, t.TempDir())
	before := time.Now().Add(-time.Hour)
	s.kal = kalshi.NewClient(ts.URL, nil, 1000, 2*time.Second)
	s.kalFees = map[string]kalFeeInfo{"KXOLD": kalFeeInfoFromVenue("quadratic", 1)}
	s.kalFeesAt, s.kalFeesSource = before, "disk"
	s.refreshKalFees()

	if _, ok := s.kalFees["KXOLD"]; !ok || len(s.kalFees) != 1 {
		t.Fatalf("empty venue response replaced last-good: %#v", s.kalFees)
	}
	if !s.kalFeesAt.Equal(before) || s.kalFeesSource != "disk" {
		t.Fatalf("last-good authority changed: at=%v source=%q", s.kalFeesAt, s.kalFeesSource)
	}
}

func TestR154FeeRegistryCandidateRejectsPartialOrMalformedReplacement(t *testing.T) {
	good := map[string]kalFeeInfo{
		"KXA": kalFeeInfoFromVenue("quadratic", 1),
		"KXB": kalFeeInfoFromVenue("quadratic", 1),
	}
	if err := validateKalFeeRegistryCandidate(good, nil, nil, 2, 2); err != nil {
		t.Fatalf("valid candidate rejected: %v", err)
	}
	if err := validateKalFeeRegistryCandidate(map[string]kalFeeInfo{
		"KXA": kalFeeInfoFromVenue("quadratic", 1),
	}, nil, nil, 1, 4); err == nil || !strings.Contains(err.Error(), "catastrophic") {
		t.Fatalf("partial replacement err=%v, want catastrophic shrink", err)
	}
	if err := validateKalFeeRegistryCandidate(good, nil, nil, 3, 2); err == nil || !strings.Contains(err.Error(), "raw count") {
		t.Fatalf("silently normalized replacement err=%v, want raw-count refusal", err)
	}
	if err := validateKalFeeRegistryCandidate(map[string]kalFeeInfo{
		"KXA": {typ: "quadratic", multiplier: -1},
	}, nil, nil, 1, 0); err == nil || !strings.Contains(err.Error(), "multiplier") {
		t.Fatalf("malformed metadata err=%v, want multiplier refusal", err)
	}
}

func TestR154FeeRegistryRefreshStartsBeforeAuthorityExpires(t *testing.T) {
	if kalFeeRegistryRefreshAge >= kalFeeRegistryMaxAge {
		t.Fatalf("refresh age %s must be earlier than hard authority %s", kalFeeRegistryRefreshAge, kalFeeRegistryMaxAge)
	}
}

func TestR154FeeRegistryOwnsOnlyOneRefreshTimer(t *testing.T) {
	s := &Server{}
	s.scheduleKalFeeRefresh(time.Hour)
	first := s.kalFeesTimer
	s.scheduleKalFeeRefresh(2 * time.Hour)
	if s.kalFeesTimer == nil || s.kalFeesTimer == first || s.kalFeesTimerGen != 2 {
		t.Fatalf("timer replacement failed: timer=%p first=%p generation=%d", s.kalFeesTimer, first, s.kalFeesTimerGen)
	}
	s.kalFeesTimer.Stop()
}
