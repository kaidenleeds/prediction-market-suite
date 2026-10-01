package kalshi

// Authenticated Forecast Graph percentile history. This is the venue's documented
// /series/{series}/events/{event}/forecast_percentile_history surface; it is intentionally kept
// separate from ordinary market discovery because every call consumes the private REST budget.
// IMPORTANT: Kalshi's help center says Forecast Graph values are derived from collective trades.
// This transport is therefore market-derived telemetry, not an independent weather forecast.

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
)

type ForecastPercentilePoint struct {
	Percentile           int     `json:"percentile"` // basis points of percentile: 0..9999
	RawNumericalForecast float64 `json:"raw_numerical_forecast"`
	NumericalForecast    float64 `json:"numerical_forecast"`
	FormattedForecast    string  `json:"formatted_forecast"`
}

type ForecastPercentileFrame struct {
	EventTicker    string                    `json:"event_ticker"`
	EndPeriodTS    int64                     `json:"end_period_ts"`
	PeriodInterval int                       `json:"period_interval"`
	Points         []ForecastPercentilePoint `json:"percentile_points"`
}

// ForecastPercentileHistory performs one bounded authenticated read. percentiles are encoded as
// repeated form keys (OpenAPI style=form, explode=true), capped at the venue's ten-value limit.
func (c *Client) ForecastPercentileHistory(ctx context.Context, series, event string, percentiles []int, startTS, endTS int64, periodInterval int) ([]ForecastPercentileFrame, error) {
	path, err := forecastPercentileHistoryPath(series, event, percentiles, startTS, endTS, periodInterval)
	if err != nil {
		return nil, err
	}
	if c == nil {
		return nil, fmt.Errorf("nil Kalshi client")
	}
	var resp struct {
		History []ForecastPercentileFrame `json:"forecast_history"`
	}
	if err := c.do(ctx, http.MethodGet, path, &resp, true); err != nil {
		return nil, err
	}
	sort.Slice(resp.History, func(i, j int) bool { return resp.History[i].EndPeriodTS < resp.History[j].EndPeriodTS })
	return resp.History, nil
}

func forecastPercentileHistoryPath(series, event string, percentiles []int, startTS, endTS int64, periodInterval int) (string, error) {
	series, event = strings.TrimSpace(series), strings.TrimSpace(event)
	if series == "" || event == "" || startTS <= 0 || endTS < startTS {
		return "", fmt.Errorf("invalid forecast history identity or time range")
	}
	if len(percentiles) == 0 || len(percentiles) > 10 {
		return "", fmt.Errorf("forecast history requires 1..10 percentiles")
	}
	switch periodInterval {
	case 0, 1, 60, 1440:
	default:
		return "", fmt.Errorf("unsupported forecast period interval %d", periodInterval)
	}
	// Live GET probes on 2026-07-11 proved the service rejects otherwise-valid requests with a
	// generic HTTP 400 when either boundary is off the selected period grid. The public schema
	// lists the allowed intervals but does not state this alignment constraint. Normalize centrally
	// so every caller signs and sends the exact wire contract: five-second boundaries for interval
	// 0, minute boundaries for 1, and the corresponding minute multiple for 60/1440.
	stepSeconds := int64(5)
	if periodInterval > 0 {
		stepSeconds = int64(periodInterval) * 60
	}
	startTS -= startTS % stepSeconds
	endTS -= endTS % stepSeconds
	q := url.Values{}
	seen := make(map[int]struct{}, len(percentiles))
	for _, p := range percentiles {
		if p < 0 || p > 9999 {
			return "", fmt.Errorf("forecast percentile %d outside 0..9999", p)
		}
		if _, duplicate := seen[p]; duplicate {
			return "", fmt.Errorf("duplicate forecast percentile %d", p)
		}
		seen[p] = struct{}{}
		q.Add("percentiles", strconv.Itoa(p))
	}
	q.Set("start_ts", strconv.FormatInt(startTS, 10))
	q.Set("end_ts", strconv.FormatInt(endTS, 10))
	q.Set("period_interval", strconv.Itoa(periodInterval))
	return "/series/" + url.PathEscape(series) + "/events/" + url.PathEscape(event) + "/forecast_percentile_history?" + q.Encode(), nil
}
