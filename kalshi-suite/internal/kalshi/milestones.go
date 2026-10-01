package kalshi

// Current official occurrence metadata used by the funded sports-exposure guard.
//
// Kalshi event_ticker is the native mutually-exclusive market container. A Milestone is the
// broader real-world occurrence container: one game milestone can own separate winner, spread,
// total, period, and prop event tickers. Money code must use that official relationship instead
// of guessing from friendly titles.

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"
)

type Event struct {
	EventTicker       string                     `json:"event_ticker"`
	SeriesTicker      string                     `json:"series_ticker"`
	SubTitle          string                     `json:"sub_title"`
	Title             string                     `json:"title"`
	Category          string                     `json:"category"`
	StrikePeriod      string                     `json:"strike_period"`
	ProductMetadata   map[string]json.RawMessage `json:"product_metadata"`
	MutuallyExclusive bool                       `json:"mutually_exclusive"`
	LastUpdatedTS     string                     `json:"last_updated_ts"`
}

type Milestone struct {
	ID                  string                     `json:"id"`
	Category            string                     `json:"category"`
	Type                string                     `json:"type"`
	Title               string                     `json:"title"`
	StartDate           string                     `json:"start_date"`
	EndDate             string                     `json:"end_date"`
	SourceID            string                     `json:"source_id"`
	SourceIDs           map[string]json.RawMessage `json:"source_ids"`
	Details             map[string]json.RawMessage `json:"details"`
	PrimaryEventTickers []string                   `json:"primary_event_tickers"`
	RelatedEventTickers []string                   `json:"related_event_tickers"`
	LastUpdatedTS       string                     `json:"last_updated_ts"`
}

func (m Milestone) ContainsEvent(eventTicker string) bool {
	eventTicker = strings.TrimSpace(eventTicker)
	for _, ticker := range m.RelatedEventTickers {
		if strings.EqualFold(strings.TrimSpace(ticker), eventTicker) {
			return true
		}
	}
	for _, ticker := range m.PrimaryEventTickers {
		if strings.EqualFold(strings.TrimSpace(ticker), eventTicker) {
			return true
		}
	}
	return false
}

func (m Milestone) DetailString(key string) string {
	raw, ok := m.Details[key]
	if !ok || len(raw) == 0 {
		return ""
	}
	var value string
	if json.Unmarshal(raw, &value) != nil {
		return ""
	}
	return strings.TrimSpace(value)
}

func (e Event) ProductString(key string) string {
	raw, ok := e.ProductMetadata[key]
	if !ok || len(raw) == 0 {
		return ""
	}
	var value string
	if json.Unmarshal(raw, &value) != nil {
		return ""
	}
	return strings.TrimSpace(value)
}

func (c *Client) GetEvent(ctx context.Context, eventTicker string) (Event, error) {
	eventTicker = strings.TrimSpace(eventTicker)
	if eventTicker == "" {
		return Event{}, errors.New("empty Kalshi event ticker")
	}
	var resp struct {
		Event Event `json:"event"`
	}
	if err := c.do(ctx, http.MethodGet, "/events/"+url.PathEscape(eventTicker), &resp, false); err != nil {
		return Event{}, err
	}
	if !strings.EqualFold(strings.TrimSpace(resp.Event.EventTicker), eventTicker) {
		return Event{}, errors.New("Kalshi event response identity mismatch")
	}
	return resp.Event, nil
}

// GetMilestonesForEvent asks the official relationship endpoint for every occurrence tied to one
// exact event ticker. The server selects one game/match-level Sports milestone conservatively;
// this client method deliberately preserves all rows so ambiguity cannot be hidden here.
func (c *Client) GetMilestonesForEvent(ctx context.Context, eventTicker string) ([]Milestone, error) {
	eventTicker = strings.TrimSpace(eventTicker)
	if eventTicker == "" {
		return nil, errors.New("empty Kalshi milestone event ticker")
	}
	var resp struct {
		Milestones []Milestone `json:"milestones"`
		Cursor     string      `json:"cursor"`
	}
	path := "/milestones?limit=500&related_event_ticker=" + url.QueryEscape(eventTicker)
	if err := c.do(ctx, http.MethodGet, path, &resp, false); err != nil {
		return nil, err
	}
	if strings.TrimSpace(resp.Cursor) != "" {
		return nil, errors.New("Kalshi milestone event filter unexpectedly paginated beyond 500 rows")
	}
	out := make([]Milestone, 0, len(resp.Milestones))
	for _, milestone := range resp.Milestones {
		if strings.TrimSpace(milestone.ID) == "" || !milestone.ContainsEvent(eventTicker) {
			return nil, errors.New("Kalshi milestone response omitted its requested event identity")
		}
		out = append(out, milestone)
	}
	return out, nil
}
