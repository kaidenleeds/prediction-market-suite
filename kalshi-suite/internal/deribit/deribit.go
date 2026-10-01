// Package deribit reads the public option-summary surface for independent crypto distribution
// research. It has no account, order, or authenticated API capability.
package deribit

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

const (
	DefaultBaseURL = "https://www.deribit.com/api/v2"
	maxResponse    = 4 << 20
)

type OptionSummary struct {
	InstrumentName      string  `json:"instrument_name"`
	UnderlyingIndex     string  `json:"underlying_index"`
	BaseCurrency        string  `json:"base_currency"`
	QuoteCurrency       string  `json:"quote_currency"`
	BidPrice            float64 `json:"bid_price"`
	AskPrice            float64 `json:"ask_price"`
	MidPrice            float64 `json:"mid_price"`
	MarkPrice           float64 `json:"mark_price"`
	MarkIV              float64 `json:"mark_iv"`
	UnderlyingPrice     float64 `json:"underlying_price"`
	EstimatedDelivery   float64 `json:"estimated_delivery_price"`
	OpenInterest        float64 `json:"open_interest"`
	Volume              float64 `json:"volume"`
	VolumeUSD           float64 `json:"volume_usd"`
	CreationTimestampMS int64   `json:"creation_timestamp"`
}

type Snapshot struct {
	Currency   string          `json:"currency"`
	ServerTime time.Time       `json:"server_time"`
	Rows       []OptionSummary `json:"rows"`
	Bytes      int64           `json:"bytes"`
	URL        string          `json:"url"`
}

// Instrument is the exact public contract metadata needed to interpret an option summary. The
// summary endpoint intentionally does not repeat strike, option type, or expiry, so deriving those
// fields from the display name would make the research adapter depend on an undocumented parser.
type Instrument struct {
	InstrumentName      string  `json:"instrument_name"`
	Kind                string  `json:"kind"`
	BaseCurrency        string  `json:"base_currency"`
	QuoteCurrency       string  `json:"quote_currency"`
	PriceIndex          string  `json:"price_index"`
	OptionType          string  `json:"option_type"`
	SettlementPeriod    string  `json:"settlement_period"`
	Strike              float64 `json:"strike"`
	ExpirationTimestamp int64   `json:"expiration_timestamp"`
	CreationTimestamp   int64   `json:"creation_timestamp"`
	IsActive            bool    `json:"is_active"`
}

type InstrumentSnapshot struct {
	Currency   string       `json:"currency"`
	ServerTime time.Time    `json:"server_time"`
	Rows       []Instrument `json:"rows"`
	Bytes      int64        `json:"bytes"`
	URL        string       `json:"url"`
}

// ThresholdPoint is a frozen, research-only risk-neutral terminal distribution point. It is not
// a forecast of the physical probability and cannot itself authorize an order. Each point retains
// the exact option metadata and server clock so a later prediction-market join can require an
// identical underlying, threshold, deadline, index, and settlement basis.
type ThresholdPoint struct {
	Currency         string    `json:"currency"`
	Expiry           time.Time `json:"expiry"`
	Strike           float64   `json:"strike"`
	ProbabilityAbove float64   `json:"probability_above"`
	ProbabilityLow   float64   `json:"probability_low"`
	ProbabilityHigh  float64   `json:"probability_high"`
	UnderlyingPrice  float64   `json:"underlying_price"`
	MarkIVPercent    float64   `json:"mark_iv_percent"`
	OpenInterest     float64   `json:"open_interest"`
	PriceIndex       string    `json:"price_index"`
	Contributors     []string  `json:"contributors"`
	SourceTime       time.Time `json:"source_time"`
	Model            string    `json:"model"`
}

type Client struct {
	BaseURL string
	HTTP    *http.Client
}

func NewClient(timeout time.Duration) *Client {
	return &Client{BaseURL: DefaultBaseURL, HTTP: &http.Client{Timeout: timeout}}
}

func decodeRPCResponse(resp *http.Response, target any, label string) (time.Time, int64, error) {
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK || resp.ContentLength > maxResponse {
		return time.Time{}, 0, fmt.Errorf("%s status=%d length=%d", label, resp.StatusCode, resp.ContentLength)
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxResponse+1))
	if err != nil {
		return time.Time{}, 0, err
	}
	if len(raw) == 0 || len(raw) > maxResponse {
		return time.Time{}, 0, fmt.Errorf("%s invalid bytes=%d", label, len(raw))
	}
	var envelope struct {
		JSONRPC string          `json:"jsonrpc"`
		Result  json.RawMessage `json:"result"`
		UsOut   int64           `json:"usOut"`
		Error   json.RawMessage `json:"error"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return time.Time{}, 0, fmt.Errorf("%s schema: %w", label, err)
	}
	if len(envelope.Error) > 0 && string(envelope.Error) != "null" {
		return time.Time{}, 0, fmt.Errorf("%s JSON-RPC error: %.300s", label, envelope.Error)
	}
	serverTime := time.UnixMicro(envelope.UsOut).UTC()
	if envelope.JSONRPC != "2.0" || envelope.UsOut <= 0 || serverTime.Before(time.Now().UTC().Add(-5*time.Minute)) ||
		serverTime.After(time.Now().UTC().Add(30*time.Second)) {
		return time.Time{}, 0, fmt.Errorf("%s missing/stale server clock usOut=%d", label, envelope.UsOut)
	}
	if err := json.Unmarshal(envelope.Result, target); err != nil {
		return time.Time{}, 0, fmt.Errorf("%s result schema: %w", label, err)
	}
	return serverTime, int64(len(raw)), nil
}

func (c *Client) OptionSummary(ctx context.Context, currency string) (Snapshot, error) {
	if c == nil || c.HTTP == nil {
		return Snapshot{}, fmt.Errorf("invalid Deribit public client")
	}
	currency = strings.ToUpper(strings.TrimSpace(currency))
	if currency != "BTC" && currency != "ETH" {
		return Snapshot{}, fmt.Errorf("unsupported Deribit currency %q", currency)
	}
	q := url.Values{"currency": {currency}, "kind": {"option"}}
	endpoint := strings.TrimRight(c.BaseURL, "/") + "/public/get_book_summary_by_currency?" + q.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return Snapshot{}, err
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return Snapshot{}, err
	}
	var decoded []OptionSummary
	serverTime, bytesRead, err := decodeRPCResponse(resp, &decoded, "Deribit option summary")
	if err != nil {
		return Snapshot{}, err
	}
	rows := decoded[:0]
	for _, row := range decoded {
		if row.InstrumentName == "" || row.UnderlyingPrice <= 0 || row.MarkIV <= 0 || row.CreationTimestampMS <= 0 {
			continue
		}
		rows = append(rows, row)
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].OpenInterest == rows[j].OpenInterest {
			return rows[i].InstrumentName < rows[j].InstrumentName
		}
		return rows[i].OpenInterest > rows[j].OpenInterest
	})
	return Snapshot{Currency: currency, ServerTime: serverTime, Rows: rows, Bytes: bytesRead, URL: endpoint}, nil
}

// OptionInstruments fetches the authoritative option metadata in one bounded public request. It is
// paired with OptionSummary by instrument_name; no display-name parsing is used for money truth.
func (c *Client) OptionInstruments(ctx context.Context, currency string) (InstrumentSnapshot, error) {
	if c == nil || c.HTTP == nil {
		return InstrumentSnapshot{}, fmt.Errorf("invalid Deribit public client")
	}
	currency = strings.ToUpper(strings.TrimSpace(currency))
	if currency != "BTC" && currency != "ETH" {
		return InstrumentSnapshot{}, fmt.Errorf("unsupported Deribit currency %q", currency)
	}
	q := url.Values{"currency": {currency}, "kind": {"option"}, "expired": {"false"}}
	endpoint := strings.TrimRight(c.BaseURL, "/") + "/public/get_instruments?" + q.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return InstrumentSnapshot{}, err
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return InstrumentSnapshot{}, err
	}
	var decoded []Instrument
	serverTime, bytesRead, err := decodeRPCResponse(resp, &decoded, "Deribit option instruments")
	if err != nil {
		return InstrumentSnapshot{}, err
	}
	rows := decoded[:0]
	for _, row := range decoded {
		row.Kind = strings.ToLower(strings.TrimSpace(row.Kind))
		row.OptionType = strings.ToLower(strings.TrimSpace(row.OptionType))
		if !row.IsActive || row.Kind != "option" || (row.OptionType != "call" && row.OptionType != "put") ||
			row.InstrumentName == "" || row.Strike <= 0 || row.ExpirationTimestamp <= serverTime.UnixMilli() {
			continue
		}
		rows = append(rows, row)
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].ExpirationTimestamp != rows[j].ExpirationTimestamp {
			return rows[i].ExpirationTimestamp < rows[j].ExpirationTimestamp
		}
		if rows[i].Strike != rows[j].Strike {
			return rows[i].Strike < rows[j].Strike
		}
		return rows[i].InstrumentName < rows[j].InstrumentName
	})
	return InstrumentSnapshot{Currency: currency, ServerTime: serverTime, Rows: rows, Bytes: bytesRead, URL: endpoint}, nil
}

func normalCDF(x float64) float64 { return 0.5 * (1 + math.Erf(x/math.Sqrt2)) }

// ThresholdSurface turns current implied volatilities into a risk-neutral terminal CDF under a
// frozen zero-rate lognormal model. This is pure mathematics, not another neural network. The
// min/max contributor envelope is retained so skew disagreement cannot be hidden by averaging.
func ThresholdSurface(summary Snapshot, instruments InstrumentSnapshot) ([]ThresholdPoint, error) {
	if summary.Currency == "" || !strings.EqualFold(summary.Currency, instruments.Currency) ||
		summary.ServerTime.IsZero() || instruments.ServerTime.IsZero() ||
		math.Abs(summary.ServerTime.Sub(instruments.ServerTime).Seconds()) > 30 {
		return nil, fmt.Errorf("Deribit summary/instrument snapshots are not same-currency same-clock")
	}
	meta := make(map[string]Instrument, len(instruments.Rows))
	for _, row := range instruments.Rows {
		meta[row.InstrumentName] = row
	}
	type accumulator struct {
		point        ThresholdPoint
		weightedP    float64
		weightedIV   float64
		weightedSpot float64
		weight       float64
	}
	groups := map[string]*accumulator{}
	for _, row := range summary.Rows {
		m, ok := meta[row.InstrumentName]
		if !ok || row.MarkIV <= 0 || row.UnderlyingPrice <= 0 || m.Strike <= 0 {
			continue
		}
		expiry := time.UnixMilli(m.ExpirationTimestamp).UTC()
		years := expiry.Sub(summary.ServerTime).Hours() / (24 * 365.25)
		sigma := row.MarkIV / 100
		if years <= 0 || sigma <= 0 {
			continue
		}
		d2 := (math.Log(row.UnderlyingPrice/m.Strike) - 0.5*sigma*sigma*years) / (sigma * math.Sqrt(years))
		p := normalCDF(d2)
		if math.IsNaN(p) || math.IsInf(p, 0) || p < 0 || p > 1 {
			continue
		}
		weight := math.Max(row.OpenInterest, 1)
		key := fmt.Sprintf("%s|%d|%.9f|%s", strings.ToUpper(summary.Currency), m.ExpirationTimestamp, m.Strike, m.PriceIndex)
		a := groups[key]
		if a == nil {
			a = &accumulator{point: ThresholdPoint{Currency: strings.ToUpper(summary.Currency), Expiry: expiry,
				Strike: m.Strike, ProbabilityLow: p, ProbabilityHigh: p, PriceIndex: m.PriceIndex,
				SourceTime: summary.ServerTime, Model: "Black-Scholes risk-neutral N(d2), zero rate; research input only"}}
			groups[key] = a
		}
		a.weight += weight
		a.weightedP += weight * p
		a.weightedIV += weight * row.MarkIV
		a.weightedSpot += weight * row.UnderlyingPrice
		a.point.OpenInterest += math.Max(row.OpenInterest, 0)
		a.point.ProbabilityLow = math.Min(a.point.ProbabilityLow, p)
		a.point.ProbabilityHigh = math.Max(a.point.ProbabilityHigh, p)
		a.point.Contributors = append(a.point.Contributors, row.InstrumentName)
	}
	out := make([]ThresholdPoint, 0, len(groups))
	for _, a := range groups {
		if a.weight <= 0 {
			continue
		}
		a.point.ProbabilityAbove = a.weightedP / a.weight
		a.point.MarkIVPercent = a.weightedIV / a.weight
		a.point.UnderlyingPrice = a.weightedSpot / a.weight
		sort.Strings(a.point.Contributors)
		out = append(out, a.point)
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].Expiry.Equal(out[j].Expiry) {
			return out[i].Expiry.Before(out[j].Expiry)
		}
		return out[i].Strike < out[j].Strike
	})
	return out, nil
}
