package server

// tgqueue_test.go — R126 Part 6.1: Telegram store-and-forward coverage. All tests stub tgTransport
// (no network) and drive tgQueue instances directly for isolation; the Server-level gate/wiring is
// covered against the hermetic testServer from r70_test.go. Run: go test ./internal/server -run TgQueue

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestR139TelegramChunksPreserveCompleteEmojiBriefing(t *testing.T) {
	line := "🧪 system · cycles 12 · inputs 345 · state collecting\n"
	text := strings.Repeat(line, 180)
	parts := tgMessageChunks(text, 4096)
	if len(parts) < 2 {
		t.Fatalf("long briefing was not chunked: bytes=%d", len(text))
	}
	var rebuilt strings.Builder
	for i, part := range parts {
		if len(part) > 4096 || !utf8.ValidString(part) {
			t.Fatalf("part %d bytes=%d utf8=%v", i, len(part), utf8.ValidString(part))
		}
		prefix := fmt.Sprintf("📨 %d/%d\n", i+1, len(parts))
		if !strings.HasPrefix(part, prefix) {
			t.Fatalf("part %d prefix=%q", i, part[:min(len(part), 24)])
		}
		rebuilt.WriteString(strings.TrimPrefix(part, prefix))
	}
	if rebuilt.String() != text {
		t.Fatal("chunking dropped or changed briefing content")
	}
}

func tgTestLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// stubTgTransport swaps the package transport for the test and restores it on cleanup.
func stubTgTransport(t *testing.T, fn func(log *slog.Logger, token, chatID, text string) error) {
	t.Helper()
	old := tgTransport
	tgTransport = fn
	t.Cleanup(func() { tgTransport = old })
}

func tgFail(_ *slog.Logger, _, _, _ string) error { return errors.New("offline: connection refused") }

// seedOutageQueue reproduces the spec scenario under a dead link: alerts A1,A2, main-stream
// briefings B1,B2,B3, alert A3 → queue A1,A2,B3,A3 with collapsed=2.
func seedOutageQueue(t *testing.T, q *tgQueue, dir string) {
	t.Helper()
	send := func(class, text string) { q.sendClass(tgTestLogger(), "tok", "chat", dir, class, text, nil) }
	send("alert", "A1")
	send("alert", "A2")
	send("briefing:main", "B1")
	send("briefing:main", "B2")
	send("briefing:main", "B3")
	send("alert", "A3")
}

func tgQueueTexts(q *tgQueue) []string {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]string, 0, len(q.entries))
	for _, e := range q.entries {
		out = append(out, e.Text)
	}
	return out
}

func assertTexts(t *testing.T, got, want []string, label string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: got %d messages %v, want %d %v", label, len(got), got, len(want), want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%s: message[%d] = %q, want %q (full: %v)", label, i, got[i], want[i], got)
		}
	}
}

// ── 1. Simulated outage: order + briefing collapse + persistence survives a restart ─────────────

func TestTgQueueOutageQueuesCollapsesPersists(t *testing.T) {
	dir := t.TempDir()
	stubTgTransport(t, tgFail)
	q := &tgQueue{}
	seedOutageQueue(t, q, dir)

	assertTexts(t, tgQueueTexts(q), []string{"A1", "A2", "B3", "A3"}, "queued after outage")
	if q.collapsed != 2 {
		t.Fatalf("collapsed = %d, want 2 (B1,B2 superseded by B3)", q.collapsed)
	}
	if q.dropped != 0 {
		t.Fatalf("dropped = %d, want 0", q.dropped)
	}

	// Persisted file exists and reloads into a FRESH instance with identical content (restart survival).
	if _, err := os.Stat(filepath.Join(dir, tgQueueFile)); err != nil {
		t.Fatalf("persisted queue file missing: %v", err)
	}
	q2 := &tgQueue{}
	q2.load(dir, tgTestLogger())
	assertTexts(t, tgQueueTexts(q2), []string{"A1", "A2", "B3", "A3"}, "reloaded queue")
	q2.mu.Lock()
	classes := []string{}
	for _, e := range q2.entries {
		classes = append(classes, e.Class)
	}
	c2, d2 := q2.collapsed, q2.dropped
	q2.mu.Unlock()
	assertTexts(t, classes, []string{"alert", "alert", "briefing:main", "alert"}, "reloaded classes")
	if c2 != 2 || d2 != 0 {
		t.Fatalf("reloaded counters = (collapsed %d, dropped %d), want (2, 0)", c2, d2)
	}
}

// ── 2. Reconnect flush: header first, then strict order, counters reset, MarkSent hook fires ────

func TestTgQueueFlushHeaderOrderAndReset(t *testing.T) {
	dir := t.TempDir()
	stubTgTransport(t, tgFail)
	q := &tgQueue{}
	marks := 0
	q.onBriefingSent = func() { marks++ }
	seedOutageQueue(t, q, dir)
	if marks != 0 {
		t.Fatalf("MarkSent hook fired %d times during the outage, want 0 (no real delivery)", marks)
	}

	var sent []string
	stubTgTransport(t, func(_ *slog.Logger, _, _, text string) error { sent = append(sent, text); return nil })
	n, drained := q.flushOnce(tgTestLogger(), "tok", "chat", dir, 0)
	if !drained || n != 4 {
		t.Fatalf("flushOnce = (sent %d, drained %v), want (4, true)", n, drained)
	}
	if len(sent) != 5 {
		t.Fatalf("wire saw %d messages %v, want 5 (header + 4 entries)", len(sent), sent)
	}
	if !strings.Contains(sent[0], "4 updates") || !strings.Contains(sent[0], "2 briefings collapsed") {
		t.Fatalf("catch-up header = %q, want it to contain \"4 updates\" and \"2 briefings collapsed\"", sent[0])
	}
	if !strings.Contains(sent[0], "UTC") {
		t.Fatalf("catch-up header = %q, want the oldest HH:MM UTC stamp", sent[0])
	}
	assertTexts(t, sent[1:], []string{"A1", "A2", "B3", "A3"}, "flushed order")
	if marks != 1 {
		t.Fatalf("MarkSent hook fired %d times, want 1 (exactly one briefing:main delivered)", marks)
	}
	queued, collapsed, dropped, oldest := q.status()
	if queued != 0 || collapsed != 0 || dropped != 0 || !oldest.IsZero() {
		t.Fatalf("post-drain status = (%d, %d, %d, %v), want all zero", queued, collapsed, dropped, oldest)
	}
	// Persisted state must be the drained one — a restart right now must not resurrect the backlog.
	q3 := &tgQueue{}
	q3.load(dir, tgTestLogger())
	if got := tgQueueTexts(q3); len(got) != 0 {
		t.Fatalf("reload after drain = %v, want empty", got)
	}
}

// ── 3. Partial flush: dies after header+first entry → nothing lost, second pass completes ───────

func TestTgQueuePartialFlushRetainsRemainder(t *testing.T) {
	dir := t.TempDir()
	stubTgTransport(t, tgFail)
	q := &tgQueue{}
	seedOutageQueue(t, q, dir)

	var sent []string
	calls := 0
	stubTgTransport(t, func(_ *slog.Logger, _, _, text string) error {
		calls++
		if calls > 2 { // header + A1 make it out, then the link dies again
			return errors.New("offline again")
		}
		sent = append(sent, text)
		return nil
	})
	n, drained := q.flushOnce(tgTestLogger(), "tok", "chat", dir, 0)
	if drained || n != 1 {
		t.Fatalf("partial flushOnce = (sent %d, drained %v), want (1, false)", n, drained)
	}
	assertTexts(t, sent[1:], []string{"A1"}, "delivered before the drop")
	assertTexts(t, tgQueueTexts(q), []string{"A2", "B3", "A3"}, "retained remainder in order")
	if q.collapsed != 2 {
		t.Fatalf("collapsed = %d after partial flush, want 2 (counters intact until full drain)", q.collapsed)
	}

	// Heal fully: the second pass re-headers for the remaining 3 and completes.
	sent = nil
	stubTgTransport(t, func(_ *slog.Logger, _, _, text string) error { sent = append(sent, text); return nil })
	n, drained = q.flushOnce(tgTestLogger(), "tok", "chat", dir, 0)
	if !drained || n != 3 {
		t.Fatalf("second flushOnce = (sent %d, drained %v), want (3, true)", n, drained)
	}
	if !strings.Contains(sent[0], "3 updates") {
		t.Fatalf("second header = %q, want \"3 updates\"", sent[0])
	}
	assertTexts(t, sent[1:], []string{"A2", "B3", "A3"}, "second-pass order")
	if queued, collapsed, dropped, _ := q.status(); queued != 0 || collapsed != 0 || dropped != 0 {
		t.Fatalf("post-drain status = (%d, %d, %d), want zeros", queued, collapsed, dropped)
	}
}

// Header failure = still offline: the pass must touch nothing.
func TestTgQueueHeaderFailureLosesNothing(t *testing.T) {
	dir := t.TempDir()
	stubTgTransport(t, tgFail)
	q := &tgQueue{}
	seedOutageQueue(t, q, dir)
	n, drained := q.flushOnce(tgTestLogger(), "tok", "chat", dir, 0)
	if drained || n != 0 {
		t.Fatalf("header-fail flushOnce = (sent %d, drained %v), want (0, false)", n, drained)
	}
	assertTexts(t, tgQueueTexts(q), []string{"A1", "A2", "B3", "A3"}, "queue untouched after header failure")
	if q.collapsed != 2 || q.dropped != 0 {
		t.Fatalf("counters = (collapsed %d, dropped %d), want (2, 0)", q.collapsed, q.dropped)
	}
}

// ── 4. Overflow: cap 300, oldest dropped (any class), dropped counter increments ────────────────

func TestTgQueueOverflowDropsOldest(t *testing.T) {
	dir := t.TempDir()
	stubTgTransport(t, tgFail)
	q := &tgQueue{}
	for i := 1; i <= tgQueueCap+5; i++ {
		q.sendClass(tgTestLogger(), "tok", "chat", dir, "alert", fmt.Sprintf("M%04d", i), nil)
	}
	queued, _, dropped, _ := q.status()
	if queued != tgQueueCap {
		t.Fatalf("queued = %d, want the cap %d", queued, tgQueueCap)
	}
	if dropped != 5 {
		t.Fatalf("dropped = %d, want 5", dropped)
	}
	texts := tgQueueTexts(q)
	if texts[0] != "M0006" || texts[len(texts)-1] != fmt.Sprintf("M%04d", tgQueueCap+5) {
		t.Fatalf("window = [%s … %s], want [M0006 … M%04d] (oldest five dropped)",
			texts[0], texts[len(texts)-1], tgQueueCap+5)
	}
	// The persisted blob carries the drop counter across a restart.
	q2 := &tgQueue{}
	q2.load(dir, tgTestLogger())
	if _, _, d2, _ := q2.status(); d2 != 5 {
		t.Fatalf("reloaded dropped = %d, want 5", d2)
	}
}

// ── Server surface: TgSendClass gates exactly like TgSend; failures audit + queue ───────────────

func TestTgQueueServerGates(t *testing.T) {
	calls := 0
	stubTgTransport(t, func(_ *slog.Logger, _, _, _ string) error { calls++; return errors.New("offline") })
	oldQ := tgq
	tgq = &tgQueue{}
	t.Cleanup(func() { tgq = oldQ })

	s := testServer(t)

	// Not configured (empty token/chat): silent no-op — no transport attempt, nothing queued.
	s.TgSendClass("alert", "hello")
	if q, _, _, _ := s.TgQueueStatus(); calls != 0 || q != 0 {
		t.Fatalf("unconfigured send: transport calls %d, queued %d — want 0, 0", calls, q)
	}

	// ntfy mode with creds: still a no-op (R82 gate).
	cfg := *s.cfg()
	cfg.TelegramBotToken, cfg.TelegramChatID = "tok", "chat"
	cfg.MessengerMode = "ntfy"
	s.cfgP.Store(&cfg)
	s.TgSendClass("alert", "hello")
	if q, _, _, _ := s.TgQueueStatus(); calls != 0 || q != 0 {
		t.Fatalf("ntfy-mode send: transport calls %d, queued %d — want 0, 0", calls, q)
	}

	// telegram mode: the failed send is attempted once, then queued.
	cfg2 := cfg
	cfg2.MessengerMode = "telegram"
	s.cfgP.Store(&cfg2)
	s.TgSendClass("alert", "hello")
	if q, _, _, _ := s.TgQueueStatus(); calls != 1 || q != 1 {
		t.Fatalf("failed send: transport calls %d, queued %d — want 1, 1", calls, q)
	}
	// Backlog present: the next send must enqueue WITHOUT hitting the wire (order preserved).
	s.TgSendBriefing("ml", "ml briefing")
	if q, _, _, _ := s.TgQueueStatus(); calls != 1 || q != 2 {
		t.Fatalf("send behind backlog: transport calls %d, queued %d — want 1, 2", calls, q)
	}
	texts := tgQueueTexts(tgq)
	assertTexts(t, texts, []string{"hello", "ml briefing"}, "server-queued order")
}
