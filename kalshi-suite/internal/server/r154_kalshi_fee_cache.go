package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// The exact Kalshi fee registry is public venue metadata, not a secret. Keeping immutable,
// validated generation files lets a brief public-API outage after restart preserve fee authority
// without guessing a fee, contending with SQLite, or adding a network call to the LIVE order path.
const (
	kalFeeRegistrySnapshotVersion = 1
	kalFeeRegistryMaxAge          = 6 * time.Hour
	kalFeeRegistryRefreshAge      = 5 * time.Hour
	kalFeeRegistrySnapshotGlob    = "kalshi_fee_registry_v1_*.json"
)

type kalFeeRegistrySnapshotRow struct {
	FeeType       string  `json:"fee_type"`
	FeeMultiplier float64 `json:"fee_multiplier"`
	ParentSeries  string  `json:"parent_series,omitempty"`
}

type kalFeeRegistrySnapshot struct {
	Version        int                                  `json:"version"`
	FetchedAt      time.Time                            `json:"fetched_at"`
	NextActivation time.Time                            `json:"next_activation,omitempty"`
	Series         map[string]kalFeeRegistrySnapshotRow `json:"series"`
	Events         map[string]kalFeeRegistrySnapshotRow `json:"events,omitempty"`
}

func encodeKalFeeRegistrySnapshot(series, events map[string]kalFeeInfo, eventParents map[string]string, fetchedAt, nextActivation time.Time) ([]byte, error) {
	if fetchedAt.IsZero() || len(series) == 0 {
		return nil, errors.New("empty fee registry snapshot")
	}
	out := kalFeeRegistrySnapshot{
		Version:        kalFeeRegistrySnapshotVersion,
		FetchedAt:      fetchedAt.UTC(),
		NextActivation: nextActivation.UTC(),
		Series:         make(map[string]kalFeeRegistrySnapshotRow, len(series)),
		Events:         make(map[string]kalFeeRegistrySnapshotRow, len(events)),
	}
	for ticker, info := range series {
		key, row, err := kalFeeSnapshotRow(ticker, info)
		if err != nil {
			return nil, fmt.Errorf("series %q: %w", ticker, err)
		}
		out.Series[key] = row
	}
	for ticker, info := range events {
		key, row, err := kalFeeSnapshotRow(ticker, info)
		if err != nil {
			return nil, fmt.Errorf("event %q: %w", ticker, err)
		}
		parent := strings.ToUpper(strings.TrimSpace(eventParents[key]))
		if parent == "" {
			return nil, fmt.Errorf("event %q: missing authoritative parent series", ticker)
		}
		if _, ok := out.Series[parent]; !ok {
			return nil, fmt.Errorf("event %q: parent series %q is absent", ticker, parent)
		}
		row.ParentSeries = parent
		out.Events[key] = row
	}
	return json.Marshal(out)
}

func kalFeeSnapshotRow(ticker string, info kalFeeInfo) (string, kalFeeRegistrySnapshotRow, error) {
	key := strings.ToUpper(strings.TrimSpace(ticker))
	typ := strings.ToLower(strings.TrimSpace(info.typ))
	if key == "" || typ == "" {
		return "", kalFeeRegistrySnapshotRow{}, errors.New("blank ticker or fee type")
	}
	if math.IsNaN(info.multiplier) || math.IsInf(info.multiplier, 0) || info.multiplier < 0 {
		return "", kalFeeRegistrySnapshotRow{}, errors.New("invalid fee multiplier")
	}
	return key, kalFeeRegistrySnapshotRow{FeeType: typ, FeeMultiplier: info.multiplier}, nil
}

func validateKalFeeRegistryCandidate(series, events map[string]kalFeeInfo, eventParents map[string]string, rawSeriesCount, lastGoodCount int) error {
	if len(series) == 0 {
		return errors.New("empty normalized series registry")
	}
	if rawSeriesCount != len(series) {
		return fmt.Errorf("normalized series count %d differs from raw count %d", len(series), rawSeriesCount)
	}
	if lastGoodCount >= 2 && len(series)*2 < lastGoodCount {
		return fmt.Errorf("catastrophic series shrink %d -> %d", lastGoodCount, len(series))
	}
	for ticker, info := range series {
		key, _, err := kalFeeSnapshotRow(ticker, info)
		if err != nil {
			return fmt.Errorf("series %q: %w", ticker, err)
		}
		if key != ticker {
			return fmt.Errorf("series ticker %q is not canonical", ticker)
		}
	}
	for ticker, info := range events {
		key, _, err := kalFeeSnapshotRow(ticker, info)
		if err != nil {
			return fmt.Errorf("event %q: %w", ticker, err)
		}
		if key != ticker {
			return fmt.Errorf("event ticker %q is not canonical", ticker)
		}
		parent := strings.ToUpper(strings.TrimSpace(eventParents[ticker]))
		if parent == "" || parent != eventParents[ticker] {
			return fmt.Errorf("event %q has invalid parent", ticker)
		}
		if _, ok := series[parent]; !ok {
			return fmt.Errorf("event %q parent %q is absent", ticker, parent)
		}
	}
	if len(eventParents) != len(events) {
		return fmt.Errorf("event parent count %d differs from override count %d", len(eventParents), len(events))
	}
	return nil
}

func decodeKalFeeRegistrySnapshot(raw string, now time.Time) (map[string]kalFeeInfo, map[string]kalFeeInfo, time.Time, time.Time, error) {
	var in kalFeeRegistrySnapshot
	if err := json.Unmarshal([]byte(raw), &in); err != nil {
		return nil, nil, time.Time{}, time.Time{}, fmt.Errorf("decode: %w", err)
	}
	if in.Version != kalFeeRegistrySnapshotVersion {
		return nil, nil, time.Time{}, time.Time{}, fmt.Errorf("unsupported version %d", in.Version)
	}
	fetchedAt := in.FetchedAt.UTC()
	if fetchedAt.IsZero() {
		return nil, nil, time.Time{}, time.Time{}, errors.New("missing fetched_at")
	}
	if fetchedAt.After(now) {
		return nil, nil, time.Time{}, time.Time{}, errors.New("fetched_at is in the future")
	}
	if now.Sub(fetchedAt) > kalFeeRegistryMaxAge {
		return nil, nil, time.Time{}, time.Time{}, errors.New("snapshot is stale")
	}
	nextActivation := in.NextActivation.UTC()
	if !nextActivation.IsZero() && !now.Before(nextActivation) {
		return nil, nil, time.Time{}, time.Time{}, errors.New("scheduled fee activation is due")
	}
	if len(in.Series) == 0 {
		return nil, nil, time.Time{}, time.Time{}, errors.New("empty series registry")
	}
	series, err := decodeKalFeeSnapshotRows(in.Series)
	if err != nil {
		return nil, nil, time.Time{}, time.Time{}, fmt.Errorf("series: %w", err)
	}
	events, err := decodeKalFeeSnapshotRows(in.Events)
	if err != nil {
		return nil, nil, time.Time{}, time.Time{}, fmt.Errorf("events: %w", err)
	}
	for event, row := range in.Events {
		parent := strings.ToUpper(strings.TrimSpace(row.ParentSeries))
		if parent == "" || parent != row.ParentSeries {
			return nil, nil, time.Time{}, time.Time{}, fmt.Errorf("event %q has invalid parent series", event)
		}
		if _, ok := series[parent]; !ok {
			return nil, nil, time.Time{}, time.Time{}, fmt.Errorf("event %q parent series %q is absent", event, parent)
		}
	}
	return series, events, fetchedAt, nextActivation, nil
}

func decodeKalFeeSnapshotRows(rows map[string]kalFeeRegistrySnapshotRow) (map[string]kalFeeInfo, error) {
	out := make(map[string]kalFeeInfo, len(rows))
	for ticker, row := range rows {
		key := strings.ToUpper(strings.TrimSpace(ticker))
		typ := strings.ToLower(strings.TrimSpace(row.FeeType))
		if key == "" || typ == "" {
			return nil, errors.New("blank ticker or fee type")
		}
		if key != ticker {
			return nil, fmt.Errorf("ticker %q is not canonical", ticker)
		}
		if math.IsNaN(row.FeeMultiplier) || math.IsInf(row.FeeMultiplier, 0) || row.FeeMultiplier < 0 {
			return nil, fmt.Errorf("%s has invalid fee multiplier", key)
		}
		// Rebuild coefficients from venue metadata. Stored arithmetic is never trusted.
		out[key] = kalFeeInfoFromVenue(typ, row.FeeMultiplier)
	}
	return out, nil
}

func (s *Server) loadKalFeeRegistrySnapshot() {
	if s == nil {
		return
	}
	dir := strings.TrimSpace(s.cfg().DataDir)
	if dir == "" {
		return
	}
	paths, err := filepath.Glob(filepath.Join(dir, kalFeeRegistrySnapshotGlob))
	if err != nil || len(paths) == 0 {
		return
	}
	now := time.Now()
	var (
		bestSeries, bestEvents        map[string]kalFeeInfo
		bestFetchedAt, bestActivation time.Time
		bestPath                      string
		lastErr                       error
	)
	for _, path := range paths {
		raw, readErr := os.ReadFile(path)
		if readErr != nil {
			lastErr = readErr
			continue
		}
		series, events, fetchedAt, nextActivation, decodeErr := decodeKalFeeRegistrySnapshot(string(raw), now)
		if decodeErr != nil {
			lastErr = decodeErr
			continue
		}
		if bestFetchedAt.IsZero() || fetchedAt.After(bestFetchedAt) {
			bestSeries, bestEvents = series, events
			bestFetchedAt, bestActivation, bestPath = fetchedAt, nextActivation, path
		}
	}
	if bestFetchedAt.IsZero() {
		if s.log != nil {
			s.log.Warn("durable Kalshi fee registry ignored; LIVE remains fail-closed until venue refresh", "reason", lastErr)
		}
		return
	}
	s.kalFeeMu.Lock()
	// A successful network refresh can win a race with a slow local read. Never replace newer
	// authority with the older durable copy.
	if s.kalFeesAt.IsZero() || bestFetchedAt.After(s.kalFeesAt) {
		s.kalFees, s.kalEventFees = bestSeries, bestEvents
		s.kalFeesAt, s.kalFeesNext = bestFetchedAt, bestActivation
		s.kalFeesRetryAt = time.Time{}
		s.kalFeesSource = "disk"
	}
	s.kalFeeMu.Unlock()
	if s.log != nil {
		s.log.Info("restored durable Kalshi fee registry", "series", len(bestSeries), "event_overrides", len(bestEvents),
			"fetched_at", bestFetchedAt, "next_activation", bestActivation, "file", filepath.Base(bestPath))
	}
}

func (s *Server) kalFeeRegistryReadiness(now time.Time) (bool, string) {
	s.kalFeeMu.Lock()
	count, at, next := len(s.kalFees), s.kalFeesAt, s.kalFeesNext
	busy, retryAt, source := s.kalFeesBusy, s.kalFeesRetryAt, s.kalFeesSource
	s.kalFeeMu.Unlock()

	if source == "" {
		source = "none"
	}
	base := fmt.Sprintf("series=%d source=%s refresh_busy=%v", count, source, busy)
	usable, authority := kalFeeRegistryAuthority(count, at, next, now)
	if count == 0 || at.IsZero() {
		if !retryAt.IsZero() {
			base += fmt.Sprintf(" retry_in=%s", retryAt.Sub(now).Truncate(time.Second))
		}
		return false, base + " exact fee registry not loaded"
	}
	age := now.Sub(at)
	base += fmt.Sprintf(" age=%s", age.Truncate(time.Second))
	if !usable && authority == "future" {
		return false, base + " fetched_at is in the future"
	}
	if !usable && authority == "stale" {
		return false, base + " exact fee registry is stale"
	}
	if !next.IsZero() {
		base += fmt.Sprintf(" next_activation=%s", next.UTC().Format(time.RFC3339))
		if !usable && authority == "activation-due" {
			return false, base + " scheduled activation is due"
		}
	}
	return usable, base
}

func kalFeeRegistryAuthority(count int, at, next, now time.Time) (bool, string) {
	if count == 0 || at.IsZero() {
		return false, "not-loaded"
	}
	age := now.Sub(at)
	if age < 0 {
		return false, "future"
	}
	if age > kalFeeRegistryMaxAge {
		return false, "stale"
	}
	if !next.IsZero() && !now.Before(next) {
		return false, "activation-due"
	}
	return true, "current"
}

func (s *Server) persistKalFeeRegistrySnapshot(series, events map[string]kalFeeInfo, eventParents map[string]string, fetchedAt, nextActivation time.Time) {
	if s == nil {
		return
	}
	dir := strings.TrimSpace(s.cfg().DataDir)
	if dir == "" {
		return
	}
	raw, err := encodeKalFeeRegistrySnapshot(series, events, eventParents, fetchedAt, nextActivation)
	if err != nil {
		if s.log != nil {
			s.log.Error("Kalshi fee registry persistence encoding failed", "err", err)
		}
		return
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		if s.log != nil {
			s.log.Error("Kalshi fee registry persistence failed; current process remains exact", "err", err)
		}
		return
	}
	// Immutable generation files avoid rename-over-existing behavior on Windows. A crash can leave
	// one malformed newest generation; boot validates every generation and falls back to the prior
	// complete receipt. No database writer or LIVE order path is touched.
	path := filepath.Join(dir, fmt.Sprintf("kalshi_fee_registry_v1_%020d.json", fetchedAt.UnixNano()))
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err == nil {
		var written int
		if written, err = f.Write(raw); err == nil && written != len(raw) {
			err = io.ErrShortWrite
		}
		if err == nil {
			err = f.Sync()
		}
		if closeErr := f.Close(); err == nil {
			err = closeErr
		}
	}
	if err != nil {
		_ = os.Remove(path)
		if s.log != nil {
			s.log.Error("Kalshi fee registry persistence failed; current process remains exact, next restart may need venue refresh", "err", err)
		}
		return
	}
	paths, _ := filepath.Glob(filepath.Join(dir, kalFeeRegistrySnapshotGlob))
	sort.Strings(paths)
	if len(paths) > 3 {
		for _, old := range paths[:len(paths)-3] {
			_ = os.Remove(old)
		}
	}
}
