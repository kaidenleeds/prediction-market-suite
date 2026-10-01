// durability.go — P3 (audit §6 + §9): the restart-safety layer.
//
//  1. Settlement journal: an append-only JSONL written AT RESOLVE TIME for every settlement
//     (paper positions, parlays, live RFQ combos). Survives venue purges (the Poly US case where a
//     market disappears after long downtime), DB rewrites, and resets — a grep-able forensic trail.
//  2. Latch persistence: the dedup/progress latches that were memory-only (gateExecDone,
//     bridgeSeen, freeRoll, scaleStep) now write through to the DB kv table at mutation time and
//     reload at boot, so a restart can't re-fire a gate order / re-buy a bridge / re-free-roll.
//     liveArmed is DELIBERATELY excluded: real-money arming must never survive a restart.
//  3. Latch hygiene: full position close clears that key's trailPeak/scaleStep/freeRoll (the audit
//     bug where a re-entry inherited a stale peak and instant-exited), and boot/daily pruning
//     bounds the kv table and the in-memory caches.
//  4. Idempotency keys: deterministic client/order keys so a double-click or a retry after a
//     timeout can't place the same order twice (Kalshi dedupes on client_order_id).
package server

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// latch key prefixes in the kv table.
const (
	kvGateExec   = "gate_exec|"   // gate buy signature → "1"
	kvBridgeSeen = "bridge_seen|" // consensus-bridge key → RFC3339 first-seen
	kvFreeRoll   = "free_roll|"   // "platform|ticker|side" → "1"
	kvScaleStep  = "scale_step|"  // "platform|ticker|side" → step count
	// Today's accepted live-order gross turnover {day, spend, per-venue}. It resets at UTC midnight
	// and backs the automatic daily capital-churn admission gate.
	kvLiveDaySpend = "live_day_spend"
	// R144: UTC-day authoritative settlement/activity accumulator. Unlike current positions, these
	// receipts survive a full close and a process restart.
	kvLiveLossLedger = "live_loss_ledger_v2"
)

type liveLossPersisted struct {
	Version int                         `json:"version"`
	Day     string                      `json:"day"`
	Epoch   string                      `json:"epoch_at"`
	DeltaV  map[string]int64            `json:"delta_microdollars_by_venue"`
	SeenV   map[string]map[string]bool  `json:"seen_receipts_by_venue"`
	LastV   map[string]map[string]int64 `json:"last_cumulative_microdollars_by_venue"`
	FinalV  map[string]map[string]int64 `json:"settlement_final_microdollars_by_venue,omitempty"`
	BootV   map[string]bool             `json:"cumulative_baseline_captured_by_venue"`
}

func (s *Server) persistLiveLossLedger(ctx context.Context) error {
	if s.store == nil {
		return errors.New("live loss ledger store unavailable")
	}
	s.liveMu.Lock()
	epoch := s.liveLossEpochAt
	if epoch.IsZero() {
		epoch, _ = time.Parse("2006-01-02", s.liveLossDay)
	}
	snap := liveLossPersisted{
		Version: 4,
		Day:     s.liveLossDay,
		Epoch:   epoch.UTC().Format(time.RFC3339Nano),
		DeltaV:  map[string]int64{},
		SeenV:   map[string]map[string]bool{},
		LastV:   map[string]map[string]int64{},
		FinalV:  map[string]map[string]int64{},
		BootV:   map[string]bool{},
	}
	for venue, cents := range s.liveLossDeltaV {
		snap.DeltaV[venue] = cents
	}
	for venue, ids := range s.liveLossSeenV {
		snap.SeenV[venue] = map[string]bool{}
		for id := range ids {
			snap.SeenV[venue][id] = true
		}
	}
	for venue, values := range s.liveLossLastV {
		snap.LastV[venue] = map[string]int64{}
		for key, cents := range values {
			snap.LastV[venue][key] = cents
		}
	}
	for venue, values := range s.liveLossFinalV {
		snap.FinalV[venue] = map[string]int64{}
		for key, cents := range values {
			snap.FinalV[venue][key] = cents
		}
	}
	for venue, ok := range s.liveLossBootV {
		snap.BootV[venue] = ok
	}
	s.liveMu.Unlock()
	if snap.Day == "" {
		return errors.New("live loss ledger day is empty")
	}
	b, err := json.Marshal(snap)
	if err != nil {
		return err
	}
	pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return s.store.KVSet(pctx, kvLiveLossLedger, string(b))
}

// journalSettle appends one JSON line to data/settlement_journal.jsonl. Failures are swallowed —
// the journal is an audit trail, never a gate on the settlement itself.
func (s *Server) journalSettle(kind string, rec map[string]any) {
	if rec == nil {
		rec = map[string]any{}
	}
	rec["ts"] = time.Now().UTC().Format(time.RFC3339)
	rec["kind"] = kind
	b, err := json.Marshal(rec)
	if err != nil {
		return
	}
	s.journalMu.Lock()
	defer s.journalMu.Unlock()
	path := filepath.Join(s.cfg().DataDir, "settlement_journal.jsonl")
	// R89 (auditor bug 55): the journal was append-only FOREVER. Rotate at 4MB keeping the newest
	// half (same bound pattern as the legsim problems trail) — forensic tail preserved, file bounded.
	if fi, e := os.Stat(path); e == nil && fi.Size() > 4<<20 {
		if old, e2 := os.ReadFile(path); e2 == nil {
			half := old[len(old)/2:]
			if i := bytes.IndexByte(half, '\n'); i >= 0 && i+1 < len(half) {
				half = half[i+1:] // start at a whole line
			}
			_ = os.WriteFile(path, half, 0o644)
		}
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return
	}
	_, _ = f.Write(append(b, '\n'))
	_ = f.Close()
}

// loadLatches restores the persisted latches into their in-memory maps at boot (called from New,
// before any sweep goroutine starts — no lock ordering issues; the recovery retry goroutine takes
// the maps' own locks). R76 (auditor bug 6): errors no longer degrade to silent empty maps — the
// first failure is RETURNED so the caller can fail-closed (latchesOK stays false and every latch
// consumer refuses to act) instead of booting into a wedge with amnesia and re-firing real orders.
func (s *Server) loadLatches(ctx context.Context) error {
	m, err := s.store.KVPrefix(ctx, kvGateExec)
	if err != nil {
		return err
	}
	s.autoMu.Lock()
	if s.gateExecDone == nil {
		s.gateExecDone = map[string]time.Time{}
	}
	for k := range m {
		// R70: value is now the latch time (age-prunable). KVPrefix doesn't carry ts, so boot
		// reload stamps now — entries age out 7d from THIS boot, matching the KV prune below.
		s.gateExecDone[k] = time.Now()
	}
	s.autoMu.Unlock()
	m, err = s.store.KVPrefix(ctx, kvBridgeSeen)
	if err != nil {
		return err
	}
	s.bridgeMu.Lock()
	s.bridgeSeen = map[string]time.Time{}
	for k, v := range m {
		if t, err := time.Parse(time.RFC3339, v); err == nil && time.Since(t) < 48*time.Hour {
			s.bridgeSeen[k] = t
		}
	}
	s.bridgeMu.Unlock()
	m, err = s.store.KVPrefix(ctx, kvFreeRoll)
	if err != nil {
		return err
	}
	s.scaleMu.Lock()
	s.freeRoll = map[string]bool{}
	for k := range m {
		s.freeRoll[k] = true
	}
	s.scaleMu.Unlock()
	m, err = s.store.KVPrefix(ctx, kvScaleStep)
	if err != nil {
		return err
	}
	s.scaleMu.Lock()
	s.scaleStep = map[string]int{}
	for k, v := range m {
		n := 0
		for _, c := range v {
			if c < '0' || c > '9' {
				n = 0
				break
			}
			n = n*10 + int(c-'0')
		}
		if n > 0 {
			s.scaleStep[k] = n
		}
	}
	s.scaleMu.Unlock()
	// Restore today's gross-turnover observation; a new UTC day starts fresh.
	if v, ok := s.store.KVGet(ctx, kvLiveDaySpend); ok {
		var snap struct {
			Day   string             `json:"day"`
			Spend float64            `json:"spend"`
			V     map[string]float64 `json:"v"`
		}
		if json.Unmarshal([]byte(v), &snap) == nil && snap.Day == time.Now().UTC().Format("2006-01-02") {
			s.liveMu.Lock()
			s.liveDayKey, s.liveDaySpend = snap.Day, snap.Spend
			s.liveDaySpendV = snap.V
			s.liveMu.Unlock()
		}
	}
	// R144: restore the durable receipt watermark and realized fixed-point money. Freshness itself never
	// survives a process boundary: each venue remains independently fail-closed until its first
	// successful authoritative read after boot.
	if v, ok := s.store.KVGet(ctx, kvLiveLossLedger); ok {
		var snap liveLossPersisted
		if err := json.Unmarshal([]byte(v), &snap); err != nil {
			return fmt.Errorf("decode durable live loss ledger: %w", err)
		}
		if snap.Day == time.Now().UTC().Format("2006-01-02") {
			migration := snap.Version < 4
			epoch, epochErr := time.Parse(time.RFC3339Nano, snap.Epoch)
			if epochErr != nil || epoch.IsZero() {
				if !migration {
					return fmt.Errorf("durable live loss ledger omitted a valid epoch")
				}
				epoch, _ = time.Parse("2006-01-02", snap.Day)
			}
			if snap.DeltaV == nil {
				snap.DeltaV = map[string]int64{}
			}
			if snap.SeenV == nil {
				snap.SeenV = map[string]map[string]bool{}
			}
			if snap.LastV == nil {
				snap.LastV = map[string]map[string]int64{}
			}
			if snap.FinalV == nil {
				snap.FinalV = map[string]map[string]int64{}
			}
			if snap.BootV == nil {
				snap.BootV = map[string]bool{}
			}
			for _, venue := range []string{"kalshi", "polyus"} {
				if snap.SeenV[venue] == nil {
					snap.SeenV[venue] = map[string]bool{}
				}
				if snap.LastV[venue] == nil {
					snap.LastV[venue] = map[string]int64{}
				}
				if snap.FinalV[venue] == nil {
					snap.FinalV[venue] = map[string]int64{}
				}
			}
			if migration {
				// Versions <=3 stored whole cents and could baseline earlier same-day activity at ARM.
				// Those aggregate watermarks cannot be safely converted or merged: a lifetime ticker
				// may contain both prior-day and current-day P&L. Rebuild the UTC day from exact
				// settlement receipts, timestamped Kalshi fill fees, and PolyUS activity receipts.
				// Any partial close that the venue schemas cannot day-split fails pre-ARM instead of
				// inheriting a false baseline.
				epoch = liveLossDayStart(snap.Day)
				snap.DeltaV = map[string]int64{"kalshi": 0, "polyus": 0}
				snap.SeenV = map[string]map[string]bool{"kalshi": {}, "polyus": {}}
				snap.LastV = map[string]map[string]int64{"kalshi": {}, "polyus": {}}
				snap.FinalV = map[string]map[string]int64{"kalshi": {}, "polyus": {}}
				snap.BootV = map[string]bool{}
			}
			s.liveMu.Lock()
			s.liveLossDay = snap.Day
			s.liveLossEpochAt = epoch.UTC()
			s.liveLossBaseC = 0
			s.liveLossBaseV = nil
			s.liveLossDeltaV = snap.DeltaV
			s.liveLossSeenV = snap.SeenV
			s.liveLossLastV = snap.LastV
			s.liveLossFinalV = snap.FinalV
			s.liveLossBootV = snap.BootV
			s.liveLossTruthV = map[string]bool{"kalshi": false, "polyus": false}
			s.liveLossTruthAtV = map[string]time.Time{}
			s.liveLossErrV = map[string]string{
				"kalshi": "restart restored the ledger; awaiting a fresh Kalshi total-traded read",
				"polyus": "restart restored the ledger; awaiting a fresh PolyUS activity read",
			}
			s.liveLossHasBase = true
			s.liveMu.Unlock()
		}
	}
	// bound the table: gate signatures are day-scoped, bridges 48h; a week covers every consumer.
	_ = s.store.KVPruneOlder(ctx, kvGateExec, time.Now().Add(-7*24*time.Hour))
	_ = s.store.KVPruneOlder(ctx, kvBridgeSeen, time.Now().Add(-7*24*time.Hour))
	// R70 (audit §b P2): combo_legs_* was the one KV family with NO prune — settled combos now
	// delete their row (sweepLiveCombos), and this age prune catches anything that slipped (a
	// real-money combo open past 90 days does not exist; sports/crypto legs settle in hours).
	_ = s.store.KVPruneOlder(ctx, "combo_legs_", time.Now().Add(-90*24*time.Hour))
	return nil
}

// clearPositionLatches drops every per-position latch for a key ("platform|ticker|side") when the
// position FULLY closes. Fixes the audit bug where trailPeak/scaleStep survived the close, so a
// re-entry inherited a stale peak (instant trailing-stop exit) or skipped ladder steps.
func (s *Server) clearPositionLatches(ctx context.Context, key string) {
	s.trailMu.Lock()
	delete(s.trailPeak, key)
	s.trailMu.Unlock()
	s.scaleMu.Lock()
	delete(s.scaleStep, key)
	delete(s.freeRoll, key)
	s.scaleMu.Unlock()
	_ = s.store.KVDel(ctx, kvFreeRoll+key)
	_ = s.store.KVDel(ctx, kvScaleStep+key)
}

// idemKey builds a deterministic idempotency key: prefix + sha1(parts)[:20]. The SAME intent
// (ticker/side/price/count within the same time bucket) always yields the SAME key, so venue-side
// client_order_id dedup rejects the duplicate instead of double-filling a nervous double-click or
// a timeout retry. sha1 is fine here — this is a dedup key, not a security boundary.
func idemKey(prefix string, parts ...string) string {
	h := sha1.Sum([]byte(strings.Join(parts, "|")))
	return prefix + hex.EncodeToString(h[:])[:20]
}

// pruneTimeMap deletes entries older than maxAge once a map outgrows softCap. Callers own the map
// (single-goroutine or already holding its lock) — this just bounds the audit's "grow unbounded"
// caches (bridgeSeen, xvLast, kobCache-style time-stamped maps) without changing semantics.
func pruneTimeMap[V any](m map[string]V, at func(V) time.Time, softCap int, maxAge time.Duration) {
	if len(m) <= softCap {
		return
	}
	cut := time.Now().Add(-maxAge)
	for k, v := range m {
		if at(v).Before(cut) {
			delete(m, k)
		}
	}
}

// capMap hard-bounds a map that has no usable timestamp: past hardCap it is simply reset and
// left to rebuild from the live feed within one refresh (the values are rolling baselines).
func capMap[V any](m map[string]V, hardCap int) map[string]V {
	if len(m) <= hardCap {
		return m
	}
	return make(map[string]V, hardCap/4)
}

// capSyncMap is capMap for sync.Map caches (evTitle / mktTS — R70, audit §b P2: Store-only, never
// Deleted). Counting stops at hardCap+1, so the usual under-cap call is O(cap) worst case and the
// writes it guards are rare (network cache-miss fills). Over the cap the whole map is cleared —
// entries are re-fetchable display labels, exactly capMap's rebuild-from-live contract.
func capSyncMap(m *sync.Map, hardCap int) {
	n := 0
	m.Range(func(k, _ any) bool {
		n++
		return n <= hardCap
	})
	if n <= hardCap {
		return
	}
	m.Range(func(k, _ any) bool {
		m.Delete(k)
		return true
	})
}
