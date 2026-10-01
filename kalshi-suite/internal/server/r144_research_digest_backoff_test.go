package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestR144ResearchDigestRetryBackoffIsBounded(t *testing.T) {
	want := []time.Duration{
		15 * time.Second,
		15 * time.Second,
		30 * time.Second,
		60 * time.Second,
		2 * time.Minute,
		4 * time.Minute,
		5 * time.Minute,
		5 * time.Minute,
	}
	for failures, expected := range want {
		if got := researchDigestRetryBackoff(failures); got != expected {
			t.Fatalf("failures=%d backoff=%s want %s", failures, got, expected)
		}
	}
	if got := researchDigestRetryBackoff(100); got != researchDigestRetryMax {
		t.Fatalf("large failure count backoff=%s want cap %s", got, researchDigestRetryMax)
	}

	s := &Server{}
	now := time.Now()
	for failure, expected := range want[1:] {
		got := s.scheduleResearchDigestRetryLocked(now)
		if got != expected || s.researchDigestFailures != failure+1 || s.researchDigestNext != now.Add(expected) {
			t.Fatalf("scheduled failure=%d delay=%s count=%d next=%s", failure+1, got,
				s.researchDigestFailures, s.researchDigestNext.Sub(now))
		}
	}
	s.researchDigestFailures = 0 // the complete-refresh path resets the sequence
	if got := s.scheduleResearchDigestRetryLocked(now); got != researchDigestRetryDelay {
		t.Fatalf("post-success retry=%s want reset %s", got, researchDigestRetryDelay)
	}
}

func TestR144ResearchDigestReportsCurrentBackoffInsteadOfFixedRetry(t *testing.T) {
	now := time.Now()
	s := &Server{
		researchDigestJSON:     []byte(`{"state":"READY","systems":{"top":[]}}`),
		researchDigestAt:       now,
		researchDigestNext:     now.Add(2 * time.Minute),
		researchDigestErr:      "database busy",
		researchDigestFailures: 4,
	}
	rec := httptest.NewRecorder()
	s.handleResearchDigest(rec, httptest.NewRequest(http.MethodGet, "/api/research/digest", nil))
	var got map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode digest: %v (%s)", err, rec.Body.String())
	}
	retry, ok := got["retry_seconds"].(float64)
	if !ok || retry < 118 || retry > 120 {
		t.Fatalf("retry_seconds=%v, want current ~120s backoff", got["retry_seconds"])
	}
	if got["refresh_state"] != "RETRYING" {
		t.Fatalf("refresh_state=%v", got["refresh_state"])
	}
}
