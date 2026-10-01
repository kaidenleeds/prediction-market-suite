package server

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

// Settled route statistics do not change between a signal's LIVE-first preflight and its final
// handler a few milliseconds later. Cache only that historical aggregate; current book, fee,
// input freshness, lower-bound deterioration, and every account/risk check still run normally.
const r154LiveRouteProofCacheMaxAge = 2 * time.Second

type r154LiveRouteProofCacheEntry struct {
	at         time.Time
	stat       storage.UnitTrialLeaderboardStat
	found      bool
	generation uint64
}

type r154LiveRouteProofCacheFlight struct {
	generation uint64
	done       chan struct{}
	stat       storage.UnitTrialLeaderboardStat
	found      bool
	err        error
}

type r154LiveRouteProofCache struct {
	mu         sync.Mutex
	entries    map[string]r154LiveRouteProofCacheEntry
	flights    map[string]*r154LiveRouteProofCacheFlight
	generation uint64
}

func r154LiveRouteProofKey(family, platform, origin, side string) string {
	return strings.Join([]string{
		strings.ToLower(strings.TrimSpace(family)),
		strings.ToLower(strings.TrimSpace(platform)),
		strings.ToLower(strings.TrimSpace(origin)),
		strings.ToUpper(strings.TrimSpace(side)),
	}, "\x1f")
}

func (c *r154LiveRouteProofCache) get(ctx context.Context, now time.Time, key string,
	load func() (storage.UnitTrialLeaderboardStat, bool, error)) (
	storage.UnitTrialLeaderboardStat, bool, error) {
	if c == nil || now.IsZero() || strings.TrimSpace(key) == "" || load == nil {
		return storage.UnitTrialLeaderboardStat{}, false, errors.New("route proof cache input unavailable")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for attempt := 0; attempt < 2; attempt++ {
		if err := ctx.Err(); err != nil {
			return storage.UnitTrialLeaderboardStat{}, false, err
		}
		c.mu.Lock()
		generation := c.generation
		if entry, ok := c.entries[key]; ok && entry.generation == generation {
			age := now.Sub(entry.at)
			if age >= 0 && age <= r154LiveRouteProofCacheMaxAge {
				c.mu.Unlock()
				return entry.stat, entry.found, nil
			}
		}
		if flight, ok := c.flights[key]; ok && flight.generation == generation {
			done := flight.done
			c.mu.Unlock()
			select {
			case <-done:
			case <-ctx.Done():
				return storage.UnitTrialLeaderboardStat{}, false, ctx.Err()
			}
			c.mu.Lock()
			current := c.generation == generation
			c.mu.Unlock()
			if !current {
				continue
			}
			return flight.stat, flight.found, flight.err
		}
		if c.flights == nil {
			c.flights = make(map[string]*r154LiveRouteProofCacheFlight)
		}
		flight := &r154LiveRouteProofCacheFlight{
			generation: generation,
			done:       make(chan struct{}),
		}
		c.flights[key] = flight
		c.mu.Unlock()

		stat, found, err := load()
		c.mu.Lock()
		flight.stat, flight.found, flight.err = stat, found, err
		if current, ok := c.flights[key]; ok && current == flight {
			delete(c.flights, key)
		}
		if err != nil {
			close(flight.done)
			c.mu.Unlock()
			return stat, found, err
		}
		if c.generation != generation {
			close(flight.done)
			c.mu.Unlock()
			continue
		}
		if c.entries == nil {
			c.entries = make(map[string]r154LiveRouteProofCacheEntry)
		}
		c.entries[key] = r154LiveRouteProofCacheEntry{
			at: now, stat: stat, found: found, generation: generation,
		}
		if len(c.entries) > 128 {
			for cachedKey, entry := range c.entries {
				if age := now.Sub(entry.at); age < 0 || age > r154LiveRouteProofCacheMaxAge ||
					entry.generation != generation {
					delete(c.entries, cachedKey)
				}
			}
		}
		close(flight.done)
		c.mu.Unlock()
		return stat, found, nil
	}
	return storage.UnitTrialLeaderboardStat{}, false,
		errors.New("route proof changed while the current proof was loading")
}

func (c *r154LiveRouteProofCache) invalidate() {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.generation++
	c.entries = nil
	c.mu.Unlock()
}

func (s *Server) r154UnitTrialRouteLeaderboard(ctx context.Context, now time.Time,
	family, platform, origin, side string) (storage.UnitTrialLeaderboardStat, bool, error) {
	if s == nil || s.store == nil {
		return storage.UnitTrialLeaderboardStat{}, false, errors.New("route proof store unavailable")
	}
	key := r154LiveRouteProofKey(family, platform, origin, side)
	return s.liveRouteProofCache.get(ctx, now, key, func() (storage.UnitTrialLeaderboardStat, bool, error) {
		return s.store.UnitTrialRouteLeaderboard(ctx, now, family, platform, origin, side)
	})
}
