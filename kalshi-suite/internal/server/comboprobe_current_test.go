package server

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestR140ComboLegalityUsesCurrentCreateEndpointNotDeprecatedLookup(t *testing.T) {
	hits := 0
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		if r.Method != http.MethodPost || r.URL.Path != "/multivariate_event_collections/COL" ||
			r.URL.Path == "/multivariate_event_collections/COL/lookup" {
			t.Fatalf("deprecated or unexpected combo oracle request=%s %s", r.Method, r.URL.Path)
		}
		_, _ = io.WriteString(w, `{"event_ticker":"MVE-E","market_ticker":"MVE-M"}`)
	}))
	defer api.Close()
	st, err := storage.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	s := &Server{kal: kalshi.NewClient(api.URL, queueTestSigner(t), 1000, time.Second), store: st,
		log: slog.New(slog.NewTextHandler(io.Discard, nil)), cprobeVerdict: map[string]cprobeVerdict{},
		cprobeQueue: map[string]cprobeCand{}, cprobeReasons: map[string]int{}}
	legs := []plabLeg{{Ticker: "KXEV1-MKT", Side: "YES", Platform: "kalshi"},
		{Ticker: "KXEV2-MKT", Side: "NO", Platform: "kalshi"}}
	col := kalshi.MVCollection{CollectionTicker: "COL", Events: map[string]bool{"KXEV1": true, "KXEV2": true}}
	key := cprobeKey(legs)
	spent, done := s.cprobeOne(context.Background(), key, cprobeCand{Legs: legs, EV: .02}, []kalshi.MVCollection{col}, 2)
	got, found := s.comboVerdict(legs)
	if spent != 1 || !done || hits != 1 || !found || got.Verdict != "legal_probed" ||
		got.Collection != "COL" || s.cprobeCreateUsed != 1 {
		t.Fatalf("current combo oracle spent=%d done=%v hits=%d found=%v verdict=%+v create_used=%d",
			spent, done, hits, found, got, s.cprobeCreateUsed)
	}
}

func TestR170CanceledComboProbeSpendsNoCreateAllowance(t *testing.T) {
	hits := 0
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		_, _ = io.WriteString(w, `{"event_ticker":"MVE-E","market_ticker":"MVE-M"}`)
	}))
	defer api.Close()
	st, err := storage.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	s := &Server{kal: kalshi.NewClient(api.URL, queueTestSigner(t), 1000, time.Second), store: st,
		log: slog.New(slog.NewTextHandler(io.Discard, nil)), cprobeVerdict: map[string]cprobeVerdict{},
		cprobeQueue: map[string]cprobeCand{}, cprobeReasons: map[string]int{}}
	legs := []plabLeg{{Ticker: "KXEV1-MKT", Side: "YES", Platform: "kalshi"},
		{Ticker: "KXEV2-MKT", Side: "NO", Platform: "kalshi"}}
	col := kalshi.MVCollection{CollectionTicker: "COL", Events: map[string]bool{"KXEV1": true, "KXEV2": true}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	spent, done := s.cprobeOne(ctx, cprobeKey(legs), cprobeCand{Legs: legs, EV: .02},
		[]kalshi.MVCollection{col}, 2)
	if spent != 0 || done || hits != 0 || s.cprobeCreateUsed != 0 {
		t.Fatalf("canceled combo probe spent=%d done=%v hits=%d create_used=%d; want no dispatch or allowance spend",
			spent, done, hits, s.cprobeCreateUsed)
	}
}

func TestR140ComboLegalityExhaustsEligibleCollectionsBeforeGlobalRefusal(t *testing.T) {
	hits := []string{}
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits = append(hits, r.URL.Path)
		if r.Method != http.MethodPost {
			t.Fatalf("unexpected method %s", r.Method)
		}
		if r.URL.Path != "/multivariate_event_collections/C4" {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = io.WriteString(w, `{"error":{"code":"invalid_parameters","message":"collection refused"}}`)
			return
		}
		_, _ = io.WriteString(w, `{"event_ticker":"MVE-E","market_ticker":"MVE-M"}`)
	}))
	defer api.Close()
	st, err := storage.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	s := &Server{kal: kalshi.NewClient(api.URL, queueTestSigner(t), 1000, time.Second), store: st,
		log: slog.New(slog.NewTextHandler(io.Discard, nil)), cprobeVerdict: map[string]cprobeVerdict{},
		cprobeQueue: map[string]cprobeCand{}, cprobeReasons: map[string]int{}}
	legs := []plabLeg{{Ticker: "KXEV1-MKT", Side: "YES", Platform: "kalshi"},
		{Ticker: "KXEV2-MKT", Side: "NO", Platform: "kalshi"}}
	cols := []kalshi.MVCollection{}
	for _, name := range []string{"C3", "C1", "C4", "C2"} {
		cols = append(cols, kalshi.MVCollection{CollectionTicker: name,
			Events: map[string]bool{"KXEV1": true, "KXEV2": true}})
	}
	spent, done := s.cprobeOne(context.Background(), cprobeKey(legs),
		cprobeCand{Legs: legs, EV: .02}, cols, len(cols))
	got, found := s.comboVerdict(legs)
	if spent != 4 || !done || !found || got.Verdict != "legal_probed" || got.Collection != "C4" {
		t.Fatalf("collection-scoped probe spent=%d done=%v found=%v verdict=%+v hits=%v", spent, done, found, got, hits)
	}
	want := []string{"/multivariate_event_collections/C1", "/multivariate_event_collections/C2",
		"/multivariate_event_collections/C3", "/multivariate_event_collections/C4"}
	if len(hits) != len(want) {
		t.Fatalf("did not exhaust all eligible collections: hits=%v", hits)
	}
	for i := range want {
		if hits[i] != want[i] {
			t.Fatalf("collection order/hits=%v want=%v", hits, want)
		}
	}
}
