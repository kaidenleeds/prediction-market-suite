package server

// /api/profit-funnel is an aggregate-only, read-only view over execution_shadow.db. Keeping the
// handler separate from execution_shadow_ledger.go prevents reporting changes from touching the
// order-path producers.

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

type profitFunnelHTTPResponse struct {
	storage.ProfitFunnelReport
	Telemetry map[string]any `json:"telemetry"`
	Semantics map[string]any `json:"semantics"`
}

func profitFunnelQueryValues(values url.Values, name string) []string {
	var out []string
	for _, raw := range values[name] {
		for _, value := range strings.Split(raw, ",") {
			value = strings.TrimSpace(value)
			if value != "" {
				out = append(out, value)
			}
		}
	}
	return out
}

func profitFunnelTimeParameter(values url.Values, textNames []string,
	millisName string) (*time.Time, error) {
	var textValue string
	for _, name := range textNames {
		if value := strings.TrimSpace(values.Get(name)); value != "" {
			if textValue != "" {
				return nil, fmt.Errorf("use only one of %s", strings.Join(textNames, "/"))
			}
			textValue = value
		}
	}
	millisValue := strings.TrimSpace(values.Get(millisName))
	if textValue != "" && millisValue != "" {
		return nil, fmt.Errorf("use RFC3339 time or %s, not both", millisName)
	}
	if textValue != "" {
		parsed, err := time.Parse(time.RFC3339Nano, textValue)
		if err != nil {
			return nil, fmt.Errorf("invalid RFC3339 time %q: %w", textValue, err)
		}
		parsed = parsed.UTC()
		return &parsed, nil
	}
	if millisValue != "" {
		millis, err := strconv.ParseInt(millisValue, 10, 64)
		if err != nil || millis <= 0 {
			return nil, fmt.Errorf("invalid positive Unix millisecond %q", millisValue)
		}
		parsed := time.UnixMilli(millis).UTC()
		return &parsed, nil
	}
	return nil, nil
}

func parseProfitFunnelQuery(values url.Values) (storage.ProfitFunnelQuery, error) {
	var out storage.ProfitFunnelQuery
	var err error
	out.FromInclusive, err = profitFunnelTimeParameter(values,
		[]string{"from", "start"}, "from_ms")
	if err != nil {
		return out, err
	}
	out.ToExclusive, err = profitFunnelTimeParameter(values,
		[]string{"to", "end"}, "to_ms")
	if err != nil {
		return out, err
	}
	out.Systems = append(profitFunnelQueryValues(values, "system"),
		profitFunnelQueryValues(values, "system_id")...)
	out.Venues = profitFunnelQueryValues(values, "venue")
	out.Sides = profitFunnelQueryValues(values, "side")
	out.Actions = profitFunnelQueryValues(values, "action")
	out.Routes = profitFunnelQueryValues(values, "route")
	out.InputTopologies = append(profitFunnelQueryValues(values, "input_topology"),
		profitFunnelQueryValues(values, "topology")...)
	if _, present := values["group_by"]; present {
		out.GroupBy = profitFunnelQueryValues(values, "group_by")
		if strings.EqualFold(strings.TrimSpace(values.Get("group_by")), "none") {
			out.GroupBy = []string{}
		}
	}
	return out, nil
}

func (s *Server) handleProfitFunnel(w http.ResponseWriter, r *http.Request) {
	query, err := parseProfitFunnelQuery(r.URL.Query())
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	// The trigger index makes bounded/current-window reports fast. An explicit full-history audit
	// can still need to decode years of append-only events, so give that honest read enough time to
	// finish instead of returning a misleading partial answer at the old 30-second deadline.
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Minute)
	defer cancel()
	report, err := s.store.ExecutionShadowProfitFunnel(ctx, query)
	if err != nil {
		status := http.StatusServiceUnavailable
		if errors.Is(err, storage.ErrInvalidProfitFunnelQuery) {
			status = http.StatusBadRequest
		}
		writeJSON(w, status, map[string]any{"error": err.Error()})
		return
	}
	occupancy := s.executionShadowOccupancy.Load()
	inFlight := s.executionShadowInFlightCount.Load()
	response := profitFunnelHTTPResponse{
		ProfitFunnelReport: report,
		Telemetry: map[string]any{
			"dropped_current_process": s.executionShadowDropped.Load(),
			"dropped_by_reason": map[string]uint64{
				"invalid":    s.executionShadowDropInvalid.Load(),
				"contention": s.executionShadowDropContention.Load(),
				"stopping":   s.executionShadowDropStopping.Load(),
				"capacity":   s.executionShadowDropCapacity.Load(),
				"orphan":     s.executionShadowDropOrphan.Load(),
			},
			"pending": max(occupancy-inFlight, 0), "in_flight": inFlight,
			"capacity":            s.executionShadowCapacity(),
			"stopping":            s.executionShadowStoppingAtomic.Load(),
			"contention_rerouted": s.executionShadowRerouted.Load(),
			"storage":             s.store.ExecutionShadowStatus(ctx),
		},
		Semantics: map[string]any{
			"selection_clock":              "attempt.trigger_unix_ms (from inclusive, to exclusive)",
			"detected":                     "one immutable execution-shadow attempt",
			"book_valid":                   "an event froze an executable side quote, positive visible depth, original limit, and named book source",
			"fee_valid":                    "the same executable-book event froze a nonnegative fee with a named source",
			"taken":                        "a canonical LIVE terminal proves positive filled quantity",
			"skipped":                      "a canonical LIVE terminal proves zero fill or an explicit no-send",
			"unknown":                      "LIVE has no canonical terminal or its terminal is ambiguous; it is neither taken nor skipped",
			"branch_gaps":                  "missing Paper or Shadow branches and missing terminals are explicit gaps, never synthesized terminal outcomes",
			"skipped_comparable_economics": "known only from a settled canonical delayed execution-shadow fill; Paper or a later quote is never substituted",
			"unknown_policy":               "missing or non-authoritative evidence remains null/unknown and is never converted to zero",
			"detail_limit":                 "none; limit query parameters do not affect aggregate totals",
		},
	}
	writeJSON(w, http.StatusOK, response)
}
