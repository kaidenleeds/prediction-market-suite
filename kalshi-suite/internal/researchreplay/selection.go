package researchreplay

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
)

// SelectionQueue is an in-memory, coalescing admission queue for normalized replay frames. It is
// deliberately independent of venue clients and disk I/O. A hot WebSocket may offer every frame,
// but the queue retains only a small, deterministic research sample and never creates a new feed.
type SelectionQueue struct {
	mu       sync.Mutex
	max      int
	maxBytes int
	bytes    int
	serial   uint64
	entries  map[string]selectionEntry
	dropped  uint64
	evicted  uint64
}

type selectionEntry struct {
	key      string
	frame    Frame
	bytes    int
	priority int
	score    float64
	serial   uint64
}

type SelectionStats struct {
	Pending int    `json:"pending"`
	Bytes   int    `json:"bytes"`
	Dropped uint64 `json:"dropped"`
	Evicted uint64 `json:"evicted"`
}

func NewSelectionQueue(maxFrames, maxBytes int) *SelectionQueue {
	if maxFrames <= 0 {
		maxFrames = 96
	}
	if maxBytes <= 0 {
		maxBytes = 512 << 10
	}
	return &SelectionQueue{max: maxFrames, maxBytes: maxBytes, entries: map[string]selectionEntry{}}
}

// Offer retains one frame per selection key. A newer/higher-scoring same-key frame replaces its
// predecessor. When full, only a strictly better frame can evict the weakest entry. This provides
// hard CPU/memory bounds without silently turning selected replay into an unbounded raw feed.
func (q *SelectionQueue) Offer(key string, frame Frame, priority int, score float64) (bool, error) {
	key = strings.TrimSpace(key)
	if key == "" {
		return false, errors.New("research replay selection key is required")
	}
	if math.IsNaN(score) || math.IsInf(score, 0) {
		return false, errors.New("research replay selection score must be finite")
	}
	prepared, bytesN, err := validateAndCopyFrame(frame, q.maxBytes)
	if err != nil {
		return false, err
	}
	if bytesN > q.maxBytes {
		return false, fmt.Errorf("%w: selected frame bytes=%d max=%d", ErrBatchFull, bytesN, q.maxBytes)
	}

	q.mu.Lock()
	defer q.mu.Unlock()
	q.serial++
	incoming := selectionEntry{key: key, frame: prepared, bytes: bytesN, priority: priority, score: score, serial: q.serial}
	if old, ok := q.entries[key]; ok {
		if weaker(incoming, old) {
			q.dropped++
			return false, nil
		}
		q.bytes -= old.bytes
		q.entries[key] = incoming
		q.bytes += bytesN
		q.trimBytesLocked(key)
		_, retained := q.entries[key]
		return retained, nil
	}
	if len(q.entries) >= q.max || q.bytes+bytesN > q.maxBytes {
		victim, ok := q.weakestLocked()
		if !ok || !better(incoming, victim) {
			q.dropped++
			return false, nil
		}
		delete(q.entries, victim.key)
		q.bytes -= victim.bytes
		q.evicted++
	}
	q.entries[key] = incoming
	q.bytes += bytesN
	q.trimBytesLocked(key)
	_, retained := q.entries[key]
	return retained, nil
}

func better(a, b selectionEntry) bool {
	if a.priority != b.priority {
		return a.priority > b.priority
	}
	if a.score != b.score {
		return a.score > b.score
	}
	return a.serial > b.serial
}

func weaker(a, b selectionEntry) bool { return !better(a, b) }

func (q *SelectionQueue) weakestLocked() (selectionEntry, bool) {
	var weakest selectionEntry
	ok := false
	for _, candidate := range q.entries {
		if !ok || better(weakest, candidate) {
			weakest, ok = candidate, true
		}
	}
	return weakest, ok
}

func (q *SelectionQueue) trimBytesLocked(preferred string) {
	for q.bytes > q.maxBytes && len(q.entries) > 0 {
		victim, ok := q.weakestLocked()
		if !ok {
			break
		}
		delete(q.entries, victim.key)
		q.bytes -= victim.bytes
		q.evicted++
	}
}

// Drain returns selected frames in observation/arrival order and empties the queue. The writer
// applies its own independent frame/byte bound before any immutable segment is committed.
func (q *SelectionQueue) Drain() []Frame {
	q.mu.Lock()
	defer q.mu.Unlock()
	entries := make([]selectionEntry, 0, len(q.entries))
	for _, entry := range q.entries {
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].frame.ObservedAt.Equal(entries[j].frame.ObservedAt) {
			return entries[i].serial < entries[j].serial
		}
		return entries[i].frame.ObservedAt.Before(entries[j].frame.ObservedAt)
	})
	out := make([]Frame, 0, len(entries))
	for _, entry := range entries {
		out = append(out, entry.frame)
	}
	q.entries = map[string]selectionEntry{}
	q.bytes = 0
	return out
}

func (q *SelectionQueue) Stats() SelectionStats {
	q.mu.Lock()
	defer q.mu.Unlock()
	return SelectionStats{Pending: len(q.entries), Bytes: q.bytes, Dropped: q.dropped, Evicted: q.evicted}
}
