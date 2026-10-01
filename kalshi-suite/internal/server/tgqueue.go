package server

// tgqueue.go — R126 Part 6.1: Telegram store-and-forward.
//
// Before this file, a failed Telegram send was audit-logged and DROPPED (server.go TgSend). The
// operator is on a hotspot that drops for minutes at a time — messages must queue on failure and
// flush IN ORDER on reconnect. This file owns all of that behavior so wiring stays minimal.
//
// PARENT WIRING — exactly two server.go changes (do them outside this file):
//  1. TgSend body becomes a delegate:            s.TgSendClass("alert", text)
//  2. Server startup launches the flush loop:    go s.tgQueueLoop(ctx)
//
// Semantics:
//   - TgSendClass gates exactly like TgSend (ntfy mode / empty token/chat/text → silent no-op).
//   - If the queue is non-empty, new messages ENQUEUE behind it (order preserved). Otherwise a
//     direct send is attempted; failure audits (as before) and enqueues.
//   - Briefings dedupe in-queue per stream (only the LATEST main/parlay/ml briefing survives —
//     collapsed count kept). Alerts are never dropped except at the 300-entry cap (oldest out).
//   - The queue persists to <DataDir>/telegram_queue.json (atomic tmp+rename, xvgFlush pattern)
//     after every mutation, and reloads at boot — a restart while offline loses nothing.
//   - tgQueueLoop retries every 30s: catch-up header first ("⏪ N updates queued while offline…"),
//     then entries strictly in order with 250ms spacing; the first failure stops the pass with
//     everything remaining still queued. briefing:main deliveries advance BriefingMarkSent —
//     the verdict-transition snapshot moves only on REAL delivery (R124 invariant preserved).

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// tgTransport is the injectable wire call (tests stub it; production is tgSend from server.go).
var tgTransport = tgSend

const (
	tgQueueCap     = 300                    // hard cap; overflow drops the OLDEST entry
	tgQueueFile    = "telegram_queue.json"  // persisted under DataDir
	tgFlushSpacing = 250 * time.Millisecond // gap between queued sends (Telegram flood control)
	tgClassAlert   = "alert"
	tgClassBrief   = "briefing:" // prefix; streams: briefing:main / briefing:parlay / briefing:ml
	tgClassMain    = "briefing:main"
)

// tgEntry is one queued message. Seq is a process-local ordering/identity handle (renumbered on
// reload); TS is the original enqueue time (drives the catch-up header's "oldest HH:MM UTC").
type tgEntry struct {
	Seq   int64     `json:"seq"`
	TS    time.Time `json:"ts"`
	Class string    `json:"class"`
	Text  string    `json:"text"`
}

// tgQueueBlob is the persisted shape — entries plus the counters the catch-up header reports.
type tgQueueBlob struct {
	Entries   []tgEntry `json:"entries"`
	Seq       int64     `json:"seq"`
	Collapsed int64     `json:"collapsed"`
	Dropped   int64     `json:"dropped"`
}

// tgQueue is the store-and-forward buffer. The suite runs one process, so a package singleton
// (tgq) backs the Server methods; tests construct their own instances for isolation.
type tgQueue struct {
	mu        sync.Mutex
	entries   []tgEntry
	seq       int64
	collapsed int64 // briefings removed by same-stream dedupe since the last full drain
	dropped   int64 // entries lost to the overflow cap since the last full drain
	loaded    bool  // load() ran (once per process — boot only)

	// onBriefingSent fires after every successfully DELIVERED briefing:main message (direct or
	// flushed). The Server wires it to BriefingMarkSent; tests wire a boolean/counter.
	onBriefingSent func()
}

// tgq is the process-wide queue behind the Server methods.
var tgq = &tgQueue{}

// setBriefingHookIfNil wires onBriefingSent exactly once (idempotent under the lock — TgSendClass
// and tgQueueLoop both call it, whichever runs first wins; they wire the same Server anyway).
func (q *tgQueue) setBriefingHookIfNil(fn func()) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.onBriefingSent == nil {
		q.onBriefingSent = fn
	}
}

func (q *tgQueue) briefingHook() func() {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.onBriefingSent
}

// persistLocked writes the whole queue atomically (tmp+rename — the xvgFlush pattern). Callers
// hold q.mu. An empty dataDir (mis-wired test/CLI path) skips persistence rather than littering
// the working directory.
func (q *tgQueue) persistLocked(dataDir string) {
	if strings.TrimSpace(dataDir) == "" {
		return
	}
	out, err := json.Marshal(tgQueueBlob{Entries: q.entries, Seq: q.seq, Collapsed: q.collapsed, Dropped: q.dropped})
	if err != nil {
		return
	}
	path := filepath.Join(dataDir, tgQueueFile)
	tmp := path + ".tmp"
	if os.WriteFile(tmp, out, 0o644) == nil {
		_ = os.Rename(tmp, path)
	}
}

// load restores the persisted queue ONCE (boot). Anything already enqueued in-memory before load
// (sends failing during warmup) is kept AFTER the persisted backlog — persisted entries are older.
// Entries are renumbered so Seq identity is collision-free after the merge.
func (q *tgQueue) load(dataDir string, log *slog.Logger) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.loaded || strings.TrimSpace(dataDir) == "" {
		return
	}
	q.loaded = true
	b, err := os.ReadFile(filepath.Join(dataDir, tgQueueFile))
	if err != nil {
		return // no file = clean start
	}
	var blob tgQueueBlob
	if err := json.Unmarshal(b, &blob); err != nil {
		if log != nil {
			log.Warn("telegram queue file unreadable — starting empty", "err", err)
		}
		return
	}
	if len(blob.Entries) > 0 {
		q.entries = append(blob.Entries, q.entries...)
	}
	q.collapsed += blob.Collapsed
	q.dropped += blob.Dropped
	for i := range q.entries {
		q.entries[i].Seq = int64(i + 1)
	}
	q.seq = int64(len(q.entries))
	q.persistLocked(dataDir)
}

// enqueueLocked appends one entry, applying briefing dedupe (same-class queued briefings collapse
// to the newest) and the overflow cap (oldest entry of ANY class drops). Persists. Callers hold q.mu.
func (q *tgQueue) enqueueLocked(dataDir, class, text string) {
	if strings.HasPrefix(class, tgClassBrief) {
		kept := q.entries[:0]
		for _, e := range q.entries {
			if e.Class == class {
				q.collapsed++
				continue
			}
			kept = append(kept, e)
		}
		q.entries = kept
	}
	q.seq++
	q.entries = append(q.entries, tgEntry{Seq: q.seq, TS: time.Now().UTC(), Class: class, Text: text})
	for len(q.entries) > tgQueueCap {
		q.entries = q.entries[1:]
		q.dropped++
	}
	q.persistLocked(dataDir)
}

// sendClass is the instance-level core of TgSendClass: direct send when the line is clear, queue
// otherwise. onFail receives the transport error of a failed DIRECT send (the Server audits it);
// flush-pass failures stay quiet here (tgSend's own throttled log still fires).
func (q *tgQueue) sendClass(log *slog.Logger, token, chatID, dataDir, class, text string, onFail func(error)) {
	if class == "" {
		class = tgClassAlert
	}
	q.mu.Lock()
	if len(q.entries) > 0 { // backlog exists — never jump the line, order is the contract
		q.enqueueLocked(dataDir, class, text)
		q.mu.Unlock()
		return
	}
	q.mu.Unlock()
	if err := tgTransport(log, token, chatID, text); err != nil {
		if onFail != nil {
			onFail(err)
		}
		q.mu.Lock()
		q.enqueueLocked(dataDir, class, text)
		q.mu.Unlock()
		return
	}
	if class == tgClassMain {
		if fn := q.briefingHook(); fn != nil {
			fn()
		}
	}
}

// flushOnce runs one reconnect pass: catch-up header first; if that fails we are still offline and
// NOTHING is touched. On header success, entries go out strictly in order with `spacing` between
// sends; the first failure ends the pass with the remainder (and counters) intact. A full drain
// resets the counters and persists the empty queue. Returns (entries sent, fully drained).
func (q *tgQueue) flushOnce(log *slog.Logger, token, chatID, dataDir string, spacing time.Duration) (sent int, drained bool) {
	q.mu.Lock()
	n := len(q.entries)
	if n == 0 {
		q.mu.Unlock()
		return 0, true
	}
	collapsed, dropped := q.collapsed, q.dropped
	oldest := q.entries[0].TS
	q.mu.Unlock()

	header := fmt.Sprintf("⏪ %d updates queued while offline (oldest %s UTC)", n, oldest.UTC().Format("15:04"))
	if collapsed > 0 {
		header += fmt.Sprintf(", %d briefings collapsed to latest", collapsed)
	}
	if dropped > 0 {
		header += fmt.Sprintf(", %d dropped", dropped)
	}
	if err := tgTransport(log, token, chatID, header); err != nil {
		return 0, false // still offline — retry next tick, nothing lost
	}
	for {
		q.mu.Lock()
		if len(q.entries) == 0 { // fully drained — counters reset, empty state persisted
			q.collapsed, q.dropped = 0, 0
			q.persistLocked(dataDir)
			q.mu.Unlock()
			return sent, true
		}
		e := q.entries[0]
		q.mu.Unlock()
		if spacing > 0 {
			time.Sleep(spacing)
		}
		if err := tgTransport(log, token, chatID, e.Text); err != nil {
			return sent, false // link dropped mid-flush — remainder stays queued, counters intact
		}
		q.mu.Lock() // pop by Seq — a concurrent same-class briefing collapse may have removed it already
		for i := range q.entries {
			if q.entries[i].Seq == e.Seq {
				q.entries = append(q.entries[:i], q.entries[i+1:]...)
				break
			}
		}
		q.persistLocked(dataDir)
		q.mu.Unlock()
		sent++
		if e.Class == tgClassMain {
			if fn := q.briefingHook(); fn != nil {
				fn()
			}
		}
	}
}

// status returns (queued, collapsed, dropped, oldest-entry time; zero time when empty).
func (q *tgQueue) status() (int, int64, int64, time.Time) {
	q.mu.Lock()
	defer q.mu.Unlock()
	var oldest time.Time
	if len(q.entries) > 0 {
		oldest = q.entries[0].TS
	}
	return len(q.entries), q.collapsed, q.dropped, oldest
}

// ── Server surface ───────────────────────────────────────────────────────────────────────────────

// TgSendClass sends one Telegram message with store-and-forward. Gate is EXACTLY TgSend's: ntfy
// messenger mode, or empty token/chat/text, is a silent no-op (nothing queued — not-configured is
// a state, not an outage). class "" defaults to "alert"; briefings use TgSendBriefing.
func (s *Server) TgSendClass(class, text string) {
	if s.MessengerMode() == "ntfy" { // R82: ntfy-only mode suppresses every Telegram send
		return
	}
	token := strings.TrimSpace(s.cfg().TelegramBotToken)
	chatID := strings.TrimSpace(s.cfg().TelegramChatID)
	if token == "" || chatID == "" || strings.TrimSpace(text) == "" {
		return // tgSend's not-configured gate, applied before queueing
	}
	tgq.setBriefingHookIfNil(func() { s.BriefingMarkSent(context.Background()) })
	tgq.sendClass(s.log, token, chatID, s.cfg().DataDir, class, text, func(err error) {
		// Mirror of TgSend's R80 audit (silent non-delivery stays impossible) + the queue fact.
		_ = s.store.Audit(context.Background(), "warn", "telegram",
			"telegram send FAILED: "+truncStr(err.Error(), 400)+" — queued for retry", "")
	})
}

// TgSendBriefing routes one briefing to its dedupe stream ("main" / "parlay" / "ml"): while
// offline only the LATEST briefing per stream is kept — the operator reconnecting after an hour
// gets one fresh briefing per stream, not twelve stale ones. main-stream deliveries advance
// BriefingMarkSent (the queue owns that hook now; main.go no longer calls it).
func (s *Server) TgSendBriefing(stream, text string) {
	s.TgSendClass(tgClassBrief+stream, text)
}

// TgQueueStatus exposes the queue for /api surfaces: depth, collapse/drop counters since the last
// full drain, and the oldest queued timestamp (zero when empty).
func (s *Server) TgQueueStatus() (queued int, collapsed, dropped int64, oldest time.Time) {
	return tgq.status()
}

// tgQueueLoop is the reconnect pump: restore the persisted backlog once at boot, then every 30s
// attempt a full ordered flush (no-op while empty or while Telegram is gated off). Launched by
// server wiring as `go s.tgQueueLoop(ctx)`.
func (s *Server) tgQueueLoop(ctx context.Context) {
	tgq.setBriefingHookIfNil(func() { s.BriefingMarkSent(context.Background()) })
	tgq.load(s.cfg().DataDir, s.log)
	t := time.NewTicker(30 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if s.MessengerMode() == "ntfy" {
				continue // sends are suppressed in ntfy mode; hold the queue rather than leak into the wrong mode
			}
			token := strings.TrimSpace(s.cfg().TelegramBotToken)
			chatID := strings.TrimSpace(s.cfg().TelegramChatID)
			if token == "" || chatID == "" {
				continue // not configured — keep the backlog for when creds return
			}
			tgq.flushOnce(s.log, token, chatID, s.cfg().DataDir, tgFlushSpacing)
		}
	}
}
