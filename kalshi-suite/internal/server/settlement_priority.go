package server

// R142 settlement_priority.go keeps the positions Paper actually owns ahead of the broad
// research-signal settlement backlog.  The generic signal drain still grades every observation,
// but a funded/book lot must not wait behind thousands of unrelated historical tickers.

import (
	"context"
	"sort"
	"strings"
	"time"
)

const (
	priorityPaperSettlementCursorKV = "r142:paper-settlement-priority-cursor"
	priorityPaperSettlementCap      = 16
)

type priorityPaperSettlementTarget struct {
	Platform string
	Ticker   string
}

func (t priorityPaperSettlementTarget) key() string {
	return strings.ToLower(strings.TrimSpace(t.Platform)) + "\x00" + strings.TrimSpace(t.Ticker)
}

// prioritySettlementWindow is a durable keyset rotation.  A disappearing/resolved head cannot
// reset progress, and an unresolved long-dated legacy lot cannot permanently starve later keys.
func prioritySettlementWindow(keys []string, after string, capN int) []string {
	if len(keys) == 0 || capN <= 0 {
		return nil
	}
	sort.Strings(keys)
	start := sort.SearchStrings(keys, after)
	for start < len(keys) && keys[start] <= after {
		start++
	}
	if start >= len(keys) {
		start = 0
	}
	if capN > len(keys) {
		capN = len(keys)
	}
	out := make([]string, 0, capN)
	for i := 0; i < capN; i++ {
		out = append(out, keys[(start+i)%len(keys)])
	}
	return out
}

// priorityPaperSettlementTargets snapshots all JSON-backed Paper books without retaining a book
// mutex across SQLite or venue I/O.  Main paper_fills and ML positions already have an even higher
// priority path in sweepSettlements; this fills the historical gap for strategy books, synthetic
// Kalshi combos, and cross-venue locks.
func (s *Server) priorityPaperSettlementTargets() []priorityPaperSettlementTarget {
	byKey := map[string]priorityPaperSettlementTarget{}
	add := func(platform, ticker string) {
		platform = strings.ToLower(strings.TrimSpace(platform))
		ticker = strings.TrimSpace(ticker)
		if platform == "" {
			platform = "kalshi"
		}
		if ticker == "" || (platform != "kalshi" && platform != "polyus") {
			return // Poly-int retains its separately budgeted authoritative resolver.
		}
		t := priorityPaperSettlementTarget{Platform: platform, Ticker: ticker}
		byKey[t.key()] = t
	}
	addLots := func(defaultPlatform string, lots []kfPos) {
		for _, p := range lots {
			platform := p.Platform
			if platform == "" {
				platform = defaultPlatform
			}
			add(platform, p.Ticker)
		}
	}

	s.kfBookMu.Lock()
	kf := s.kfLoadLocked()
	addLots("kalshi", kf.Pre.Open)
	addLots("kalshi", kf.Live.Open)
	s.kfBookMu.Unlock()

	s.rfBookMu.Lock()
	addLots("kalshi", s.rfLoadLocked().Open)
	s.rfBookMu.Unlock()

	s.wxBookMu.Lock()
	addLots("kalshi", s.wxBookLoadLocked().Open)
	s.wxBookMu.Unlock()

	s.fiBookMu.Lock()
	addLots("kalshi", s.fiLoadLocked().Open)
	s.fiBookMu.Unlock()

	s.xvgBookMu.Lock()
	addLots("polyus", s.xvgLoadLocked().Open)
	s.xvgBookMu.Unlock()

	s.flBookMu.Lock()
	addLots("kalshi", s.flLoadLocked().Open)
	s.flBookMu.Unlock()

	s.cbBookMu.Lock()
	cb := s.cbLoadLocked()
	addLots("kalshi", cb.K.Open)
	addLots("polyus", cb.P.Open)
	s.cbBookMu.Unlock()

	s.gfBookMu.Lock()
	for _, book := range s.gfLoadLocked().Subs {
		addLots("kalshi", book.Open)
	}
	s.gfBookMu.Unlock()

	// ML lots live in ml_paper.json rather than paper_fills. In particular, PolyUS ML positions
	// previously had no priority REST settlement path at all and could wait behind thousands of
	// unrelated signal rows after a restart. The cached reader is tiny and includes both sleeves.
	for key := range s.mlBookOpenSides() {
		platform, ticker, ok := strings.Cut(key, "|")
		if ok {
			add(platform, ticker)
		}
	}

	s.xvcMu.Lock()
	xvc := s.xvcLoadLocked()
	for _, p := range append(append([]xvcPos(nil), xvc.Open...), xvc.ArchivedOpen...) {
		add("kalshi", p.LegA)
		add("kalshi", p.LegB)
	}
	s.xvcMu.Unlock()

	s.xvlMu.Lock()
	for _, p := range s.xvlLoadLocked().Open {
		if p.AWon < 0 {
			add(p.AVenue, p.AID)
		}
		if p.BWon < 0 {
			add(p.BVenue, p.BID)
		}
	}
	s.xvlMu.Unlock()

	out := make([]priorityPaperSettlementTarget, 0, len(byKey))
	for _, t := range byKey {
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].key() < out[j].key() })
	return out
}

// sweepPriorityPaperSettlements actively asks each target venue for a small rotating slice, then
// fans authoritative YES values into signal_log.  The existing book settlers consume those values
// in the same independent lane.  Nothing is inferred from close time, midpoint, or catalog state.
func (s *Server) sweepPriorityPaperSettlements(ctx context.Context) {
	targets := s.priorityPaperSettlementTargets()
	if len(targets) == 0 || ctx.Err() != nil {
		return
	}
	byKey := make(map[string]priorityPaperSettlementTarget, len(targets))
	keys := make([]string, 0, len(targets))
	for _, t := range targets {
		key := t.key()
		keys = append(keys, key)
		byKey[key] = t
	}
	after, _ := s.store.KVGet(ctx, priorityPaperSettlementCursorKV)
	window := prioritySettlementWindow(keys, after, priorityPaperSettlementCap)
	last := ""
	for _, key := range window {
		if ctx.Err() != nil {
			break
		}
		t := byKey[key]
		_, _ = s.resolveLegYes(ctx, t.Platform, t.Ticker)
		last = key
	}
	if last == "" {
		return
	}
	// Cursor durability must survive a canceled settlement pass; detach only within a hard bound.
	writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	if err := s.store.KVSet(writeCtx, priorityPaperSettlementCursorKV, last); err != nil && s.log != nil {
		s.log.Warn("paper settlement priority cursor write failed", "err", err)
	}
}
