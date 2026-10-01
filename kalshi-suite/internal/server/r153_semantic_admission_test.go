package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/paper"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

const (
	r153GameEvent      = "KXNBASUMMERGAME-26JUL16BKNHOU"
	r153BKNWinner      = r153GameEvent + "-BKN"
	r153HOUWinner      = r153GameEvent + "-HOU"
	r153SpreadEvent    = "KXNBASUMMERSPREAD-26JUL16BKNHOU"
	r153HOUOver35      = r153SpreadEvent + "-HOU4"
	r153TotalEvent     = "KXNBASUMMERTOTAL-26JUL16BKNHOU"
	r153Over1825       = r153TotalEvent + "-183"
	r153UnknownEvent   = "KXNBASUMMERPLAYERPTS-26JUL16BKNHOU"
	r153UnknownProp    = r153UnknownEvent + "-PLAYER1"
	r153FirstHalfEvent = "KXNBASUMMER1HSPREAD-26JUL16BKNHOU"
	r153FirstHalfHOU35 = r153FirstHalfEvent + "-HOU4"
)

type r153SemanticVenue struct {
	t              *testing.T
	server         *httptest.Server
	mu             sync.RWMutex
	markets        map[string]map[string]any
	events         map[string]map[string]any
	related        []string
	pos            []map[string]any
	orders         []map[string]any
	marketCalls    map[string]int
	boardCalls     int
	eventCalls     map[string]int
	milestoneCalls int
}

func newR153SemanticVenue(t *testing.T) *r153SemanticVenue {
	t.Helper()
	const (
		houID    = "team-houston"
		bknID    = "team-brooklyn"
		playerID = "player-one"
	)
	v := &r153SemanticVenue{t: t, marketCalls: map[string]int{}, eventCalls: map[string]int{}}
	v.related = []string{r153GameEvent, r153SpreadEvent, r153TotalEvent, r153UnknownEvent, r153FirstHalfEvent}
	v.events = map[string]map[string]any{
		r153GameEvent:      {"event_ticker": r153GameEvent, "series_ticker": "KXNBASUMMERGAME", "category": "Sports", "product_metadata": map[string]any{"competition_scope": "Full Game"}},
		r153SpreadEvent:    {"event_ticker": r153SpreadEvent, "series_ticker": "KXNBASUMMERSPREAD", "category": "Sports", "product_metadata": map[string]any{"competition_scope": "Full Game"}},
		r153TotalEvent:     {"event_ticker": r153TotalEvent, "series_ticker": "KXNBASUMMERTOTAL", "category": "Sports", "product_metadata": map[string]any{"competition_scope": "Full Game"}},
		r153UnknownEvent:   {"event_ticker": r153UnknownEvent, "series_ticker": "KXNBASUMMERPLAYERPTS", "category": "Sports", "product_metadata": map[string]any{"competition_scope": "Full Game"}},
		r153FirstHalfEvent: {"event_ticker": r153FirstHalfEvent, "series_ticker": "KXNBASUMMER1HSPREAD", "category": "Sports", "product_metadata": map[string]any{"competition_scope": "First Half"}},
	}
	v.markets = map[string]map[string]any{
		r153BKNWinner:      {"ticker": r153BKNWinner, "event_ticker": r153GameEvent, "custom_strike": map[string]any{"basketball_team": bknID}},
		r153HOUWinner:      {"ticker": r153HOUWinner, "event_ticker": r153GameEvent, "custom_strike": map[string]any{"basketball_team": houID}},
		r153HOUOver35:      {"ticker": r153HOUOver35, "event_ticker": r153SpreadEvent, "floor_strike": 3.5, "custom_strike": map[string]any{"basketball_team": houID}},
		r153Over1825:       {"ticker": r153Over1825, "event_ticker": r153TotalEvent, "floor_strike": 182.5},
		r153UnknownProp:    {"ticker": r153UnknownProp, "event_ticker": r153UnknownEvent, "floor_strike": 20.5, "custom_strike": map[string]any{"player": playerID}},
		r153FirstHalfHOU35: {"ticker": r153FirstHalfHOU35, "event_ticker": r153FirstHalfEvent, "floor_strike": 3.5, "custom_strike": map[string]any{"basketball_team": houID}},
	}
	v.server = httptest.NewServer(http.HandlerFunc(v.serveHTTP))
	t.Cleanup(v.server.Close)
	return v
}

func (v *r153SemanticVenue) serveHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	write := func(value any) {
		v.t.Helper()
		if err := json.NewEncoder(w).Encode(value); err != nil {
			v.t.Errorf("encode %s: %v", r.URL.Path, err)
		}
	}
	switch {
	case strings.HasPrefix(r.URL.Path, "/markets/"):
		ticker := strings.TrimPrefix(r.URL.Path, "/markets/")
		v.mu.Lock()
		v.marketCalls[ticker]++
		v.mu.Unlock()
		market, ok := v.markets[ticker]
		if !ok {
			http.NotFound(w, r)
			return
		}
		write(map[string]any{"market": market})
	case r.URL.Path == "/markets":
		v.mu.Lock()
		v.boardCalls++
		rows := make([]map[string]any, 0, len(v.markets))
		for _, market := range v.markets {
			rows = append(rows, market)
		}
		v.mu.Unlock()
		write(map[string]any{"markets": rows, "cursor": ""})
	case strings.HasPrefix(r.URL.Path, "/events/"):
		eventTicker := strings.TrimPrefix(r.URL.Path, "/events/")
		v.mu.Lock()
		v.eventCalls[eventTicker]++
		v.mu.Unlock()
		event, ok := v.events[eventTicker]
		if !ok {
			http.NotFound(w, r)
			return
		}
		write(map[string]any{"event": event})
	case r.URL.Path == "/milestones":
		v.mu.Lock()
		v.milestoneCalls++
		v.mu.Unlock()
		requested := r.URL.Query().Get("related_event_ticker")
		if r.URL.Query().Get("limit") != "500" || v.events[requested] == nil {
			http.Error(w, "unexpected milestone filter", http.StatusBadRequest)
			return
		}
		write(map[string]any{"milestones": []map[string]any{{
			"id": "sports-game-bkn-hou", "category": "Sports", "type": "game",
			"details":               map[string]any{"home_team_id": "team-houston", "away_team_id": "team-brooklyn"},
			"primary_event_tickers": []string{r153GameEvent}, "related_event_tickers": v.related,
		}}, "cursor": ""})
	case r.URL.Path == "/portfolio/positions":
		v.mu.RLock()
		rows := append([]map[string]any{}, v.pos...)
		v.mu.RUnlock()
		write(map[string]any{"market_positions": rows, "cursor": ""})
	case r.URL.Path == "/portfolio/orders":
		v.mu.RLock()
		rows := append([]map[string]any{}, v.orders...)
		v.mu.RUnlock()
		write(map[string]any{"orders": rows, "cursor": ""})
	default:
		http.NotFound(w, r)
	}
}

func (v *r153SemanticVenue) account(positions, orders []map[string]any) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.pos = append([]map[string]any(nil), positions...)
	v.orders = append([]map[string]any(nil), orders...)
}

func newR153SemanticServer(t *testing.T) (*Server, *r153SemanticVenue) {
	t.Helper()
	venue := newR153SemanticVenue(t)
	s := testServer(t)
	s.allowSyntheticKalshiEventIdentity = false
	s.kal = kalshi.NewClient(venue.server.URL, queueTestSigner(t), 10_000, 2*time.Second)
	s.kmkts = map[string]kalshi.Market{}
	s.kmktsAt = map[string]time.Time{}
	return s, venue
}

func r153Held(ticker, eventTicker, side, kind string) []kalshiEventExposure {
	return []kalshiEventExposure{{Ticker: ticker, EventTicker: eventTicker, Side: side, Kind: kind}}
}

func TestR153SemanticAdmissionOfficialEndpoints(t *testing.T) {
	s, _ := newR153SemanticServer(t)
	ctx := context.Background()
	tests := []struct {
		name                  string
		candidate, side       string
		heldTicker, heldEvent string
		heldSide, expectedWhy string
	}{
		{"BKN winner then HOU over 3.5", r153HOUOver35, "YES", r153BKNWinner, r153GameEvent, "YES", "kalshi-logical-payoff-conflict"},
		{"HOU over 3.5 then BKN winner", r153BKNWinner, "YES", r153HOUOver35, r153SpreadEvent, "YES", "kalshi-logical-payoff-conflict"},
		{"winner and total remain compatible", r153Over1825, "YES", r153BKNWinner, r153GameEvent, "YES", ""},
		{"same-team winner and spread remain compatible", r153HOUOver35, "YES", r153HOUWinner, r153GameEvent, "YES", ""},
		{"NO side is inverted before comparison", r153HOUOver35, "YES", r153HOUWinner, r153GameEvent, "NO", "kalshi-logical-payoff-conflict"},
		{"candidate NO is not treated as candidate YES", r153HOUOver35, "NO", r153BKNWinner, r153GameEvent, "YES", ""},
		{"unknown same-milestone prop fails closed", r153UnknownProp, "YES", r153BKNWinner, r153GameEvent, "YES", "kalshi-related-payoff-unclassified"},
		{"different official period remains compatible", r153FirstHalfHOU35, "YES", r153BKNWinner, r153GameEvent, "YES", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := s.kalshiSemanticExposureConflict(ctx, tc.candidate, tc.side,
				r153Held(tc.heldTicker, tc.heldEvent, tc.heldSide, "position"))
			if got != tc.expectedWhy {
				t.Fatalf("semantic conflict = %q, want %q", got, tc.expectedWhy)
			}
		})
	}
}

func TestR153SemanticPeriodUsesSolverFullGameVocabulary(t *testing.T) {
	event := kalshi.Event{SeriesTicker: "KXNBASUMMERGAME"}
	if got := semanticPeriod(event); got != "full_game" {
		t.Fatalf("full-game adapter period = %q, want full_game", got)
	}
}

func TestR153SemanticOfficialFetchesSingleflightSameEvent(t *testing.T) {
	s, venue := newR153SemanticServer(t)
	const callers = 24
	var wg sync.WaitGroup
	errCh := make(chan error, callers*2)
	for i := 0; i < callers; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, err := s.kalshiSemanticEvent(context.Background(), r153GameEvent)
			errCh <- err
		}()
		go func() {
			defer wg.Done()
			_, err := s.kalshiSemanticMilestone(context.Background(), r153GameEvent)
			errCh <- err
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent official fetch: %v", err)
		}
	}
	venue.mu.RLock()
	eventCalls := venue.eventCalls[r153GameEvent]
	milestoneCalls := venue.milestoneCalls
	venue.mu.RUnlock()
	if eventCalls != 1 || milestoneCalls != 1 {
		t.Fatalf("same-event official fetches event=%d milestone=%d, want one each", eventCalls, milestoneCalls)
	}
}

func TestR153SemanticAdmissionLivePositionAndOrderEndpoints(t *testing.T) {
	t.Run("authenticated position", func(t *testing.T) {
		s, venue := newR153SemanticServer(t)
		venue.account([]map[string]any{{"ticker": r153BKNWinner, "position_fp": "1.00",
			"last_updated_ts": time.Now().UTC().Format(time.RFC3339Nano)}}, nil)
		if got := s.liveKalshiEventHeaderGuard(context.Background(), r153HOUOver35, "YES"); got != "kalshi-logical-payoff-conflict" {
			t.Fatalf("LIVE position guard = %q, want semantic payoff conflict", got)
		}
	})

	t.Run("authenticated resting order", func(t *testing.T) {
		s, venue := newR153SemanticServer(t)
		venue.account(nil, []map[string]any{{
			"order_id": "order-bkn", "client_order_id": "client-bkn", "ticker": r153BKNWinner,
			"status": "resting", "type": "limit", "side": "bid", "action": "buy",
			"outcome_side": "yes", "book_side": "bid", "yes_price_dollars": "0.40",
			"no_price_dollars": "0.60", "fill_count_fp": "0.00", "remaining_count_fp": "1.00",
			"initial_count_fp": "1.00", "created_time": time.Now().UTC().Format(time.RFC3339Nano),
			"last_update_time": time.Now().UTC().Format(time.RFC3339Nano),
		}})
		if got := s.liveKalshiEventHeaderGuard(context.Background(), r153HOUOver35, "YES"); got != "kalshi-logical-payoff-conflict" {
			t.Fatalf("LIVE order guard = %q, want semantic payoff conflict", got)
		}
	})
}

func TestR153SemanticAdmissionExpandsDurableHeldComboLegs(t *testing.T) {
	s, venue := newR153SemanticServer(t)
	const comboTicker = "KXMVE-R153-BKN-AND-OVER"
	s.liveMu.Lock()
	s.comboLegReg = map[string][]kalshi.MVELeg{comboTicker: {
		{MarketTicker: r153BKNWinner, EventTicker: r153GameEvent, Side: "yes"},
		{MarketTicker: r153Over1825, EventTicker: r153TotalEvent, Side: "yes"},
	}}
	s.liveMu.Unlock()
	venue.account([]map[string]any{{"ticker": comboTicker, "position_fp": "1.00",
		"last_updated_ts": time.Now().UTC().Format(time.RFC3339Nano)}}, nil)
	if got := s.liveKalshiEventHeaderGuard(context.Background(), r153HOUOver35, "YES"); got != "kalshi-logical-payoff-conflict" {
		t.Fatalf("held combo leg guard = %q, want semantic payoff conflict", got)
	}
}

func TestR153SemanticAdmissionRecoversOrBlocksUnknownHeldCombo(t *testing.T) {
	t.Run("authoritative market legs are recovered durably", func(t *testing.T) {
		s, venue := newR153SemanticServer(t)
		const comboTicker = "KXMVE-R153-RECOVER"
		venue.mu.Lock()
		venue.markets[comboTicker] = map[string]any{
			"ticker": comboTicker, "event_ticker": "KXMVE-EVENT-R153",
			"mve_selected_legs": []map[string]any{
				{"market_ticker": r153BKNWinner, "event_ticker": r153GameEvent, "side": "yes"},
				{"market_ticker": r153Over1825, "event_ticker": r153TotalEvent, "side": "yes"},
			},
		}
		venue.mu.Unlock()
		venue.account([]map[string]any{{"ticker": comboTicker, "position_fp": "1.00",
			"last_updated_ts": time.Now().UTC().Format(time.RFC3339Nano)}}, nil)
		if got := s.liveKalshiEventHeaderGuard(context.Background(), r153HOUOver35, "YES"); got != "kalshi-logical-payoff-conflict" {
			t.Fatalf("recovered combo guard = %q, want semantic payoff conflict", got)
		}
		s.liveMu.Lock()
		recovered := len(s.comboLegReg[comboTicker])
		s.liveMu.Unlock()
		if recovered != 2 {
			t.Fatalf("recovered combo legs=%d, want 2", recovered)
		}
	})

	t.Run("unknown held combo fails closed", func(t *testing.T) {
		s, venue := newR153SemanticServer(t)
		venue.account([]map[string]any{{"ticker": "KXMVE-R153-UNKNOWN-POS", "position_fp": "1.00"}}, nil)
		if got := s.liveKalshiEventHeaderGuard(context.Background(), r153HOUOver35, "YES"); got != "kalshi-held-event-identity-unavailable" {
			t.Fatalf("unknown held combo guard = %q", got)
		}
	})

	t.Run("unknown resting combo fails closed", func(t *testing.T) {
		s, venue := newR153SemanticServer(t)
		venue.account(nil, []map[string]any{{
			"order_id": "unknown-combo-order", "ticker": "KXMVE-R153-UNKNOWN-ORDER",
			"status": "resting", "action": "buy", "outcome_side": "yes",
			"remaining_count_fp": "1.00", "initial_count_fp": "1.00",
		}})
		if got := s.liveKalshiEventHeaderGuard(context.Background(), r153HOUOver35, "YES"); got != "kalshi-held-event-identity-unavailable" {
			t.Fatalf("unknown resting combo guard = %q", got)
		}
	})
}

func insertR153SemanticReservation(t *testing.T, s *Server, ticker string) {
	t.Helper()
	now := time.Now().UTC().Truncate(time.Microsecond)
	id := "risk-r153-semantic"
	inserted, err := s.store.InsertLivePendingRisk(context.Background(), storage.LivePendingRiskIntent{
		ReservationID: id, Created: now, BaselineObserved: now.Add(-time.Second), Product: "single",
		DispatchSource: "test", SystemID: "semantic-test", Route: "taker", PrincipalUSD: 0.40,
		FeeUSD: 0.01, CostUSD: 0.41, SourceIntentID: "signal-r153", RequestHash: strings.Repeat("c", 64),
		ProofJSON: `{}`, BaselineReceiptJSON: `{}`,
	}, []storage.LivePendingRiskLeg{{
		Index: 0, Venue: "kalshi", Ticker: ticker, Side: "YES", Action: "BUY",
		ClientOrderID: "client-r153", Quantity: 1, LimitPrice: 0.40, ExpectedPositionQty: 1,
	}}, []storage.LivePendingRiskCluster{{
		Index: 0, Venue: "kalshi", ClusterKey: "sports-game-bkn-hou",
		MappingVersion: r148LiveRiskMappingVersion, ReservedUSD: 0.41,
	}})
	if err != nil || !inserted {
		t.Fatalf("insert semantic reservation=%v err=%v", inserted, err)
	}
}

func TestR153SemanticAdmissionIncludesDurableReservation(t *testing.T) {
	s, venue := newR153SemanticServer(t)
	venue.account(nil, nil)
	insertR153SemanticReservation(t, s, r153BKNWinner)
	if got := s.liveKalshiEventHeaderGuard(context.Background(), r153HOUOver35, "YES"); got != "kalshi-logical-payoff-conflict" {
		t.Fatalf("LIVE reservation guard = %q, want semantic payoff conflict", got)
	}
}

func TestR153SemanticAdmissionPackageAndPaperPaths(t *testing.T) {
	t.Run("package rejects jointly impossible legs", func(t *testing.T) {
		s, venue := newR153SemanticServer(t)
		venue.account(nil, nil)
		got := s.liveKalshiPackageEventHeaderConflict(context.Background(), []kalshiPackageLeg{
			{Ticker: r153BKNWinner, Side: "YES"}, {Ticker: r153HOUOver35, Side: "YES"},
		}, "")
		if got != "kalshi-logical-payoff-conflict" {
			t.Fatalf("package contradiction = %q, want semantic payoff conflict", got)
		}
	})

	t.Run("package allows compatible winner and total", func(t *testing.T) {
		s, venue := newR153SemanticServer(t)
		venue.account(nil, nil)
		got := s.liveKalshiPackageEventHeaderConflict(context.Background(), []kalshiPackageLeg{
			{Ticker: r153BKNWinner, Side: "YES"}, {Ticker: r153Over1825, Side: "YES"},
		}, "")
		if got != "" {
			t.Fatalf("compatible package refused: %q", got)
		}
	})

	t.Run("Paper uses the same official relationship", func(t *testing.T) {
		s, _ := newR153SemanticServer(t)
		got := s.paperKalshiEventHeaderConflict(context.Background(), r153HOUOver35, "YES", []paper.Position{{
			Platform: "kalshi", Ticker: r153BKNWinner, Side: "YES", Contracts: 1,
		}})
		if got != "kalshi-logical-payoff-conflict" {
			t.Fatalf("Paper semantic guard = %q, want semantic payoff conflict", got)
		}
	})
}
