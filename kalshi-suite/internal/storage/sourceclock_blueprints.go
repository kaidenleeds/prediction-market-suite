package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"time"
)

func sourceSchemaFingerprint(schema string, required ...string) string {
	fields := append([]string(nil), required...)
	sort.Strings(fields)
	b, _ := json.Marshal(struct {
		Schema   string   `json:"schema"`
		Required []string `json:"required"`
	}{schema, fields})
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func r138SourceClockBlueprints() []SourceClockSpec {
	return []SourceClockSpec{
		{SourceID: "kalshi-orderbook-ws", DisplayName: "Kalshi sequenced order-book WebSocket",
			AuthorityURL: "https://docs.kalshi.com/websockets/orderbook-updates", SchemaVersion: "orderbook-v2-fixed-point",
			SchemaHash: sourceSchemaFingerprint("orderbook-v2-fixed-point", "type", "sid", "seq", "msg.market_ticker", "msg.yes_dollars_fp", "msg.no_dollars_fp"),
			ClockKind:  "monotonic_sequence", SequenceField: "seq", Timezone: "UTC",
			ExpectedCadence: time.Minute, GapTolerance: 5 * time.Minute,
			RevisionPolicy:          "snapshot establishes a new baseline; deltas require an unbroken per-sid sequence; a gap invalidates until resnapshot",
			SettlementCompatibility: "book evidence only; never a settlement source",
			CachePolicy:             "retain only sequence-valid current books; selected replay frames are batched"},
		{SourceID: "kalshi-ticker-ws", DisplayName: "Kalshi public ticker WebSocket",
			AuthorityURL: "https://docs.kalshi.com/websockets/ticker", SchemaVersion: "ticker-fixed-point-time-v1",
			SchemaHash: sourceSchemaFingerprint("ticker-fixed-point-time-v1", "type", "msg.market_ticker", "msg.time", "msg.yes_bid_dollars", "msg.yes_ask_dollars"),
			ClockKind:  "source_timestamp", TimestampField: "msg.time", Timezone: "UTC",
			ExpectedCadence: time.Second, GapTolerance: 15 * time.Second, MaxLag: time.Minute,
			RevisionPolicy:          "append venue timestamped ticks; corrections are new frames",
			SettlementCompatibility: "price discovery only; lifecycle settlement remains authoritative",
			CachePolicy:             "rolling latest tick per market plus bounded selected replay"},
		{SourceID: "kalshi-market-lifecycle-ws", DisplayName: "Kalshi market lifecycle WebSocket",
			AuthorityURL: "https://docs.kalshi.com/getting_started/market_lifecycle", SchemaVersion: "market-lifecycle-v2-sequence",
			SchemaHash: sourceSchemaFingerprint("market-lifecycle-v2-sequence", "type", "sid", "seq", "msg.event_type", "msg.market_ticker", "msg.settlement_value"),
			ClockKind:  "monotonic_sequence", SequenceField: "seq", Timezone: "UTC", Version: 2,
			ExpectedCadence: 24 * time.Hour, GapTolerance: 7 * 24 * time.Hour,
			RevisionPolicy:          "each explicit lifecycle transition appends inside its generation/sid sequence domain; optional venue timestamps remain optional evidence and are never imputed",
			SettlementCompatibility: "settled/determined values are authoritative only under the event rule and scalar/binary schema",
			CachePolicy:             "persist selected transitions and aggregate baseline/duplicate liveness receipts"},
		{SourceID: "polyus-market-ws-full", DisplayName: "PolyUS full market WebSocket",
			AuthorityURL: "https://docs.polymarket.us/api-reference/websocket/markets", SchemaVersion: "polyus-market-data-v1",
			SchemaHash: sourceSchemaFingerprint("polyus-market-data-v1", "type", "instrumentId", "transactTime", "bids", "offers", "marketState"),
			ClockKind:  "source_timestamp", TimestampField: "transactTime", Timezone: "UTC",
			ExpectedCadence: time.Second, GapTolerance: 15 * time.Second, MaxLag: time.Minute,
			RevisionPolicy:          "complete MARKET_DATA replaces the prior instrument snapshot; later messages append",
			SettlementCompatibility: "market state/settlement must be joined to authenticated instrument truth",
			CachePolicy:             "full depth only for bounded priority instruments; all-board LITE remains separate"},
		{SourceID: "polyus-market-ws-lite", DisplayName: "PolyUS all-board LITE arrival clock",
			AuthorityURL: "https://docs.polymarket.us/api-reference/websocket/markets", SchemaVersion: "polyus-lite-v1",
			SchemaHash: sourceSchemaFingerprint("polyus-lite-v1", "type", "instrumentId", "bestBid", "bestOffer", "marketState"),
			ClockKind:  "arrival_only", Timezone: "UTC", ExpectedCadence: 5 * time.Second,
			GapTolerance:            30 * time.Second,
			RevisionPolicy:          "arrival receipts append; absence of a venue timestamp is preserved and never imputed",
			SettlementCompatibility: "discovery/liveness only; cannot prove execution or settlement",
			CachePolicy:             "one latest all-board quote per instrument; no source-clock claim"},
		{SourceID: "polyint-clob-ws", DisplayName: "Polymarket Global CLOB WebSocket",
			AuthorityURL: "https://docs.polymarket.com/developers/CLOB/websocket/market-channel", SchemaVersion: "clob-market-v2",
			SchemaHash: sourceSchemaFingerprint("clob-market-v2", "event_type", "asset_id", "timestamp", "best_bid", "best_ask"),
			ClockKind:  "source_timestamp", TimestampField: "timestamp", Timezone: "UTC",
			ExpectedCadence: 2 * time.Second, GapTolerance: 30 * time.Second, MaxLag: time.Minute,
			RevisionPolicy:          "append timestamped market events; malformed explicit timestamps block freshness",
			SettlementCompatibility: "research reference only; never a tradeable settlement authority",
			CachePolicy:             "bounded research CLOB cache; no account/auth payloads in replay"},
		{SourceID: "noaa-nbm", DisplayName: "NOAA National Blend of Models",
			AuthorityURL: "https://vlab.noaa.gov/web/mdl/nbm-text-products", Version: 2, SchemaVersion: "nbm-probabilistic-text-v2",
			SchemaHash: sourceSchemaFingerprint("nbm-probabilistic-text-v2", "station header", "NBM run UTC", "UTC", "FHR", "TXNMN", "TXNSD", "TXNP1", "TXNP2", "TXNP5", "TXNP7", "TXNP9"),
			ClockKind:  "source_timestamp", TimestampField: "station bulletin NBM run UTC", Timezone: "UTC",
			ExpectedCadence: 6 * time.Hour, GapTolerance: 8 * time.Hour, MaxLag: 8 * time.Hour,
			PublicationLag:          2 * time.Hour,
			RevisionPolicy:          "retain every official probabilistic station bulletin by model run; derive valid UTC from UTC/FHR and never overwrite the forecast available at decision time",
			SettlementCompatibility: "independent forecast only; venue-designated observation source and boundary decide settlement",
			CachePolicy:             "stream the bounded official all-station bulletin and retain only requested station percentile rows plus artifact hash"},
		{SourceID: "venue-notice-watcher", DisplayName: "Official venue/API notice watcher",
			AuthorityURL: "official Kalshi, PolyUS and Polymarket changelogs/status feeds", SchemaVersion: "venue-notice-v1",
			SchemaHash: sourceSchemaFingerprint("venue-notice-v1", "venue", "published_at|observed_at", "notice_type", "artifact_url", "content_hash"),
			ClockKind:  "arrival_only", Timezone: "UTC", ExpectedCadence: 6 * time.Hour,
			GapTolerance:            24 * time.Hour,
			RevisionPolicy:          "content-hash changes append a new artifact; prior notice bytes remain immutable",
			SettlementCompatibility: "governance/schema input only; never market settlement evidence",
			CachePolicy:             "conditional GET where supported; retain hash and sanitized artifact metadata"},
	}
}

func (s *Store) EnsureR138SourceClockBlueprints(ctx context.Context) error {
	specs := r138SourceClockBlueprints()
	if len(specs) != 8 {
		return fmt.Errorf("R138 source-clock blueprint count=%d, want 8", len(specs))
	}
	seen := map[string]bool{}
	// Preserve the immutable R138 v1 timestamp contract before registering the corrected sequence
	// contract as v2. Production lifecycle frames commonly omit ts/ts_ms but do carry sid/seq; v1
	// remains audit history while the latest-version report uses v2.
	legacyLifecycle := SourceClockSpec{SourceID: "kalshi-market-lifecycle-ws", DisplayName: "Kalshi market lifecycle WebSocket",
		AuthorityURL: "https://docs.kalshi.com/getting_started/market_lifecycle", SchemaVersion: "market-lifecycle-v2",
		SchemaHash: sourceSchemaFingerprint("market-lifecycle-v2", "type", "msg.event_type", "msg.market_ticker", "msg.ts|msg.ts_ms", "msg.settlement_value"),
		ClockKind:  "source_timestamp", TimestampField: "msg.ts_ms|msg.ts", Timezone: "UTC", Version: 1,
		ExpectedCadence: 24 * time.Hour, GapTolerance: 7 * 24 * time.Hour,
		RevisionPolicy:          "each explicit lifecycle transition appends; determinations, disputes and later settlement are distinct evidence",
		SettlementCompatibility: "settled/determined values are authoritative only under the event rule and scalar/binary schema",
		CachePolicy:             "persist selected transitions and aggregate baseline/duplicate liveness receipts"}
	if _, _, err := s.RegisterSourceClockSpec(ctx, legacyLifecycle); err != nil {
		return err
	}
	legacyNBM := SourceClockSpec{SourceID: "noaa-nbm", DisplayName: "NOAA National Blend of Models",
		AuthorityURL: "https://vlab.noaa.gov/web/mdl/nbm-documentation", SchemaVersion: "nbm-publication-v1",
		SchemaHash: sourceSchemaFingerprint("nbm-publication-v1", "model_run", "valid_time", "station|grid", "variable", "value", "unit"),
		ClockKind:  "source_timestamp", TimestampField: "model_run", Timezone: "UTC", Version: 1,
		ExpectedCadence: time.Hour, GapTolerance: 2 * time.Hour, MaxLag: 2 * time.Hour,
		PublicationLag:          15 * time.Minute,
		RevisionPolicy:          "retain every model run and later correction; never overwrite the forecast available at decision time",
		SettlementCompatibility: "independent forecast only; venue-designated observation source and boundary decide settlement",
		CachePolicy:             "immutable by run/station/variable with bounded local download cache"}
	if _, _, err := s.RegisterSourceClockSpec(ctx, legacyNBM); err != nil {
		return err
	}
	for _, spec := range specs {
		if seen[spec.SourceID] {
			return fmt.Errorf("duplicate source-clock blueprint %s", spec.SourceID)
		}
		seen[spec.SourceID] = true
		if spec.Version == 0 {
			spec.Version = 1
		}
		if _, _, err := s.RegisterSourceClockSpec(ctx, spec); err != nil {
			return err
		}
	}
	return nil
}

func R138SourceClockIDs() []string {
	specs := r138SourceClockBlueprints()
	out := make([]string, 0, len(specs))
	for _, spec := range specs {
		out = append(out, spec.SourceID)
	}
	sort.Strings(out)
	return out
}

// R138SourceClockSpec returns the current pinned contract. Older immutable versions remain in the
// database and are never rewritten.
func R138SourceClockSpec(sourceID string) (SourceClockSpec, bool) {
	for _, spec := range r138SourceClockBlueprints() {
		if spec.SourceID == sourceID {
			if spec.Version == 0 {
				spec.Version = 1
			}
			spec.Active = true
			return spec, true
		}
	}
	return SourceClockSpec{}, false
}
