package kalshi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestGetEventDecodesOfficialMilestoneMetadataShape(t *testing.T) {
	const eventTicker = "KXNBASUMMERTOTAL-26JUL16BKNHOU"
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/events/"+eventTicker || r.URL.RawQuery != "" {
			t.Fatalf("request=%s %s", r.Method, r.URL.RequestURI())
		}
		fmt.Fprint(w, `{
  "event": {
    "event_ticker": "KXNBASUMMERTOTAL-26JUL16BKNHOU",
    "series_ticker": "KXNBASUMMERTOTAL",
    "sub_title": "BKN at HOU (Jul 16)",
    "title": "Brooklyn at Houston: Point Total",
    "collateral_return_type": "binary",
    "mutually_exclusive": true,
    "available_on_brokers": true,
    "product_metadata": {
      "competition": "  Pro Basketball Summer League  ",
      "competition_scope": "Point Total",
      "round_number": 4,
      "optional": null
    },
    "category": "Sports",
    "strike_date": "2026-07-16T22:00:00Z",
    "strike_period": "game",
    "markets": [],
    "last_updated_ts": "2026-07-16T19:20:30Z"
  },
  "markets": []
}`)
	}))
	defer ts.Close()

	c := NewClient(ts.URL, nil, 1000, time.Second)
	event, err := c.GetEvent(context.Background(), "  "+eventTicker+"  ")
	if err != nil {
		t.Fatal(err)
	}
	if event.EventTicker != eventTicker || event.SeriesTicker != "KXNBASUMMERTOTAL" ||
		event.SubTitle != "BKN at HOU (Jul 16)" || event.Title != "Brooklyn at Houston: Point Total" ||
		event.Category != "Sports" || event.StrikePeriod != "game" || !event.MutuallyExclusive ||
		event.LastUpdatedTS != "2026-07-16T19:20:30Z" {
		t.Fatalf("event=%+v", event)
	}
	if got := event.ProductString("competition"); got != "Pro Basketball Summer League" {
		t.Fatalf("competition=%q", got)
	}
	if got := event.ProductString("competition_scope"); got != "Point Total" {
		t.Fatalf("competition_scope=%q", got)
	}
	for _, key := range []string{"round_number", "optional", "missing"} {
		if got := event.ProductString(key); got != "" {
			t.Fatalf("ProductString(%q)=%q", key, got)
		}
	}
}

func TestGetMilestonesForEventDecodesOfficialShapeAndEscapesQuery(t *testing.T) {
	const eventTicker = "KX EVENT+?&/1"
	wantedQuery := "limit=500&related_event_ticker=" + url.QueryEscape(eventTicker)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/milestones" {
			t.Fatalf("request=%s %s", r.Method, r.URL.RequestURI())
		}
		if r.URL.RawQuery != wantedQuery || r.URL.Query().Get("limit") != "500" ||
			r.URL.Query().Get("related_event_ticker") != eventTicker {
			t.Fatalf("query=%q parsed=%v", r.URL.RawQuery, r.URL.Query())
		}
		fmt.Fprint(w, `{
  "milestones": [
    {
      "id": "9ea73a39-90f8-4d5c-9396-40f0f67ccaf5",
      "category": "Sports",
      "type": "basketball_game",
      "start_date": "2026-07-16T22:00:00Z",
      "related_event_tickers": [
        "  kx event+?&/1  ",
        "KXNBASUMMERSPREAD-26JUL16BKNHOU",
        "KXNBASUMMERTOTAL-26JUL16BKNHOU"
      ],
      "title": "Brooklyn at Houston",
      "notification_message": "Game started",
      "details": {
        "home_team_id": "  70b9924e-bf08-4fc9-9fae-8ea395063485  ",
        "away_team_id": "31036362-6450-4583-9c23-f5841afb3c22",
        "period": 4,
        "nullable": null
      },
      "primary_event_tickers": ["KXNBASUMMERGAME-26JUL16BKNHOU"],
      "last_updated_ts": "2026-07-16T19:20:30Z",
      "end_date": "2026-07-17T01:00:00Z",
      "source_id": "stats-perform-game-123",
      "source_ids": {
        "stats_perform": "sp-123",
        "numeric_source": 123
      }
    }
  ],
  "cursor": ""
}`)
	}))
	defer ts.Close()

	c := NewClient(ts.URL, nil, 1000, time.Second)
	rows, err := c.GetMilestonesForEvent(context.Background(), "  "+eventTicker+"  ")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows=%+v", rows)
	}
	milestone := rows[0]
	if milestone.ID != "9ea73a39-90f8-4d5c-9396-40f0f67ccaf5" || milestone.Category != "Sports" ||
		milestone.Type != "basketball_game" || milestone.Title != "Brooklyn at Houston" ||
		milestone.StartDate != "2026-07-16T22:00:00Z" || milestone.EndDate != "2026-07-17T01:00:00Z" ||
		milestone.SourceID != "stats-perform-game-123" || milestone.LastUpdatedTS != "2026-07-16T19:20:30Z" ||
		len(milestone.PrimaryEventTickers) != 1 || len(milestone.RelatedEventTickers) != 3 ||
		!milestone.ContainsEvent(strings.ToUpper(eventTicker)) {
		t.Fatalf("milestone=%+v", milestone)
	}
	if got := milestone.DetailString("home_team_id"); got != "70b9924e-bf08-4fc9-9fae-8ea395063485" {
		t.Fatalf("home_team_id=%q", got)
	}
	if got := milestone.DetailString("away_team_id"); got != "31036362-6450-4583-9c23-f5841afb3c22" {
		t.Fatalf("away_team_id=%q", got)
	}
	for _, key := range []string{"period", "nullable", "missing"} {
		if got := milestone.DetailString(key); got != "" {
			t.Fatalf("DetailString(%q)=%q", key, got)
		}
	}
	var sourceID string
	if err := json.Unmarshal(milestone.SourceIDs["stats_perform"], &sourceID); err != nil || sourceID != "sp-123" {
		t.Fatalf("source_ids=%s value=%q err=%v", milestone.SourceIDs["stats_perform"], sourceID, err)
	}
}

func TestMilestoneEndpointsRejectBadIdentityAndPagination(t *testing.T) {
	t.Run("event identity mismatch", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			fmt.Fprint(w, `{"event":{"event_ticker":"KX-WRONG"}}`)
		}))
		defer ts.Close()
		event, err := NewClient(ts.URL, nil, 1000, time.Second).GetEvent(context.Background(), "KX-WANTED")
		if err == nil || event.EventTicker != "" || !strings.Contains(err.Error(), "identity mismatch") {
			t.Fatalf("event=%+v err=%v", event, err)
		}
	})

	t.Run("milestone omits requested event", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			fmt.Fprint(w, `{"milestones":[{"id":"m1","related_event_tickers":["KX-OTHER"]}],"cursor":""}`)
		}))
		defer ts.Close()
		rows, err := NewClient(ts.URL, nil, 1000, time.Second).GetMilestonesForEvent(context.Background(), "KX-WANTED")
		if err == nil || rows != nil || !strings.Contains(err.Error(), "requested event identity") {
			t.Fatalf("rows=%+v err=%v", rows, err)
		}
	})

	t.Run("filtered response unexpectedly paginates", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			fmt.Fprint(w, `{"milestones":[],"cursor":"next-page"}`)
		}))
		defer ts.Close()
		rows, err := NewClient(ts.URL, nil, 1000, time.Second).GetMilestonesForEvent(context.Background(), "KX-WANTED")
		if err == nil || rows != nil || !strings.Contains(err.Error(), "paginated") {
			t.Fatalf("rows=%+v err=%v", rows, err)
		}
	})
}

func TestMilestoneEndpointsPropagateHTTPErrorStatus(t *testing.T) {
	t.Run("event", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, `{"error":{"code":"not_found","message":"event missing"}}`)
		}))
		defer ts.Close()
		_, err := NewClient(ts.URL, nil, 1000, time.Second).GetEvent(context.Background(), "KX-MISSING")
		if err == nil || !strings.Contains(err.Error(), "status 404") || !strings.Contains(err.Error(), "event missing") {
			t.Fatalf("err=%v", err)
		}
	})

	t.Run("milestones", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprint(w, `{"error":{"code":"unavailable","message":"try later"}}`)
		}))
		defer ts.Close()
		rows, err := NewClient(ts.URL, nil, 1000, time.Second).GetMilestonesForEvent(context.Background(), "KX-EVENT")
		if err == nil || rows != nil || !strings.Contains(err.Error(), "status 503") || !strings.Contains(err.Error(), "try later") {
			t.Fatalf("rows=%+v err=%v", rows, err)
		}
	})
}
