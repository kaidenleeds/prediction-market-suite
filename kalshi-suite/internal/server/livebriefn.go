package server

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/polymarketus"
)

const (
	kalshiLiveSettleSeenCap = 400
	polyUSLiveHistoryCap    = 40
)

type liveBriefN struct {
	Open, Closed     int
	OpenOK, ClosedOK bool
}

func parseLiveBriefTime(raw string) (time.Time, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, false
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
		if t, err := time.Parse(layout, raw); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

// closedSinceBoot reports whether a bounded newest-first history reaches the process boundary.
// A full cache whose oldest row is newer than boot is truncated, so the honest display is n/a.
func closedSinceBoot(times []time.Time, cap int, boot time.Time) (n int, complete bool) {
	if boot.IsZero() || cap <= 0 {
		return 0, false
	}
	oldest := time.Time{}
	for _, at := range times {
		if at.IsZero() {
			return 0, false
		}
		if oldest.IsZero() || at.Before(oldest) {
			oldest = at
		}
		if !at.Before(boot) {
			n++
		}
	}
	if len(times) < cap {
		return n, true
	}
	return n, !oldest.IsZero() && !oldest.After(boot)
}

func countKalshiLiveOpen(rows []kalshi.MarketPosition) int {
	n := 0
	for _, p := range rows {
		if p.PositionQty() != 0 {
			n++
		}
	}
	return n
}

// observeKalshiLiveOpenCount returns true exactly when a fresh authenticated snapshot has fewer
// open positions than the preceding one. The caller uses that edge to bypass the ordinary
// five-minute settlement-journal cadence; growth and identical snapshots do not create traffic.
func (s *Server) observeKalshiLiveOpenCount(openN int) bool {
	if openN < 0 {
		return false
	}
	s.liveMu.Lock()
	defer s.liveMu.Unlock()
	shrunk := s.liveSettleOpenOK && openN < s.liveSettleOpenN
	s.liveSettleOpenN, s.liveSettleOpenOK = openN, true
	if shrunk {
		s.liveSettleAt = time.Time{}
	}
	return shrunk
}

func (s *Server) kalshiLiveBriefCounts(ctx context.Context, positions []kalshi.MarketPosition, positionsOK bool) liveBriefN {
	out := liveBriefN{Open: countKalshiLiveOpen(positions), OpenOK: positionsOK}
	if s == nil || s.store == nil {
		return out
	}
	raw, _ := s.store.KVGet(ctx, "live_settlements_seen")
	seen := map[string]bool{}
	if strings.TrimSpace(raw) != "" && json.Unmarshal([]byte(raw), &seen) != nil {
		return out
	}
	times := make([]time.Time, 0, len(seen))
	for key := range seen {
		cut := strings.LastIndex(key, "|")
		if cut < 0 {
			return out
		}
		at, ok := parseLiveBriefTime(key[cut+1:])
		if !ok {
			return out
		}
		times = append(times, at)
	}
	out.Closed, out.ClosedOK = closedSinceBoot(times, kalshiLiveSettleSeenCap, s.bootAt)
	return out
}

func polyUSLiveBriefCounts(positions []polymarketus.PUSPosition, history []polymarketus.PUSActivity,
	boot time.Time, positionsOK, historyOK bool) liveBriefN {
	out := liveBriefN{OpenOK: positionsOK}
	if positionsOK {
		for _, p := range positions {
			if !p.Expired && p.Net != 0 {
				out.Open++
			}
		}
	}
	if !historyOK {
		return out
	}
	resolutionTimes := make([]time.Time, 0, len(history))
	allTimes := make([]time.Time, 0, len(history))
	for _, h := range history {
		at, ok := parseLiveBriefTime(h.Time)
		if !ok {
			return out
		}
		allTimes = append(allTimes, at)
		if strings.EqualFold(h.Type, "POSITION_RESOLUTION") {
			resolutionTimes = append(resolutionTimes, at)
		}
	}
	// History is capped after fetching all activity types. If the mixed cache has fewer than the
	// cap, every resolution is covered. At the cap, the oldest activity (not merely resolution)
	// must reach boot; the caller passes all parseable activity timestamps via the sentinel below.
	out.Closed, _ = closedSinceBoot(resolutionTimes, polyUSLiveHistoryCap, boot)
	if len(history) < polyUSLiveHistoryCap {
		out.ClosedOK = true
		return out
	}
	_, out.ClosedOK = closedSinceBoot(allTimes, polyUSLiveHistoryCap, boot)
	return out
}

func liveBriefNSuffix(n liveBriefN) string {
	open, closed := "n/a", "n/a"
	if n.OpenOK {
		open = strconv.Itoa(n.Open)
	}
	if n.ClosedOK {
		closed = strconv.Itoa(n.Closed)
	}
	return "🅾️" + open + "©️" + closed
}
