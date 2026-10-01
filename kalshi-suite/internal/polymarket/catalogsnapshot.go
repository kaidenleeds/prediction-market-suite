package polymarket

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// CatalogSnapshot is a cache-only Gamma discovery receipt. Complete is true only for the event
// keyset crawler after it reached an empty next_cursor; the volume-head cache is intentionally
// never labeled complete because a timeout may publish only the pages received so far.
type CatalogSnapshot struct {
	Markets         []Market
	At              time.Time
	Refreshing      bool
	Complete        bool
	CheckpointRows  int
	CheckpointPage  int
	CheckpointLimit int
	LastError       string
}

type persistedCatalog struct {
	At      time.Time `json:"at"`
	Markets []Market  `json:"markets"`
}

// SetCatalogCachePath enables restart-safe last-good breadth. Loading is fail-closed: a missing,
// torn, empty, or malformed file leaves the in-memory cache untouched. The keyset crawler remains
// nonblocking and is still the only authority allowed to replace this snapshot.
func (c *Client) SetCatalogCachePath(path string) {
	path = strings.TrimSpace(path)
	if path == "" {
		return
	}
	c.evtMu.Lock()
	c.evtCachePath = path
	already := len(c.evtCache) > 0
	c.evtMu.Unlock()
	if already {
		return
	}
	body, err := os.ReadFile(path)
	if err != nil {
		return
	}
	var p persistedCatalog
	if json.Unmarshal(body, &p) != nil || p.At.IsZero() || len(p.Markets) == 0 {
		return
	}
	c.evtMu.Lock()
	if len(c.evtCache) == 0 {
		c.evtCache, c.evtAt = append([]Market(nil), p.Markets...), p.At
	}
	c.evtMu.Unlock()
	c.registerMarkets(p.Markets)
}

func persistCatalog(path string, at time.Time, markets []Market) error {
	if strings.TrimSpace(path) == "" || at.IsZero() || len(markets) == 0 {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	body, err := json.Marshal(persistedCatalog{At: at, Markets: markets})
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, body, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// getCatalogPage gives the unusually heavy nested-events endpoint its own bounded retry/timeout
// policy. The shared client is 15s, but a live `limit=2` page took 4.4s at R133; `limit=100`
// therefore timed out repeatedly and every old crawl discarded its work. A copied client preserves
// the transport/pool while allowing 45s for this one background request.
func (c *Client) getCatalogPage(ctx context.Context, urlStr string, out any) error {
	client := *c.http
	if client.Timeout < 45*time.Second {
		client.Timeout = 45 * time.Second
	}
	var last error
	for attempt := 0; attempt < 3; attempt++ {
		t0 := time.Now()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, urlStr, nil)
		if err != nil {
			return err
		}
		req.Header.Set("Accept", "application/json")
		req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36")
		resp, err := client.Do(req)
		d := time.Since(t0)
		c.noteRTT(d)
		if RESTObs != nil {
			RESTObs(d)
		}
		if err == nil {
			body, readErr := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			switch {
			case readErr != nil:
				err = readErr
			case resp.StatusCode >= 400 && resp.StatusCode != http.StatusTooManyRequests && resp.StatusCode < 500:
				return fmt.Errorf("polymarket catalog GET %s: status %d", urlStr, resp.StatusCode)
			case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500:
				err = fmt.Errorf("polymarket catalog GET %s: status %d", urlStr, resp.StatusCode)
			case json.Unmarshal(body, out) != nil:
				err = fmt.Errorf("polymarket catalog GET %s: invalid JSON", urlStr)
			default:
				return nil
			}
		}
		last = err
		if attempt == 2 {
			break
		}
		wait := time.Duration(attempt+1) * time.Second
		if resp != nil {
			if sec, e := strconv.Atoi(strings.TrimSpace(resp.Header.Get("Retry-After"))); e == nil && sec > 0 && sec <= 30 {
				wait = time.Duration(sec) * time.Second
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
	}
	return last
}

// CachedMarketSnapshot returns the current fast volume-head/merged cache without network I/O.
// It is useful for fresher overlays and cold-start expansion, but it is not authoritative breadth.
func (c *Client) CachedMarketSnapshot() CatalogSnapshot {
	c.mktMu.Lock()
	out := CatalogSnapshot{Markets: append([]Market(nil), c.mktCache...), At: c.mktAt,
		Refreshing: c.mktRefreshing, Complete: false}
	c.mktMu.Unlock()
	return out
}

// CompleteCatalogSnapshot returns the last successfully completed events/keyset crawl without
// network I/O. A failed or partial refresh never clears/replaces this snapshot, so callers can
// retain breadth through Gamma timeouts while Refreshing honestly reports the in-flight retry.
func (c *Client) CompleteCatalogSnapshot() CatalogSnapshot {
	c.evtMu.Lock()
	out := CatalogSnapshot{Markets: append([]Market(nil), c.evtCache...), At: c.evtAt,
		Refreshing: c.evtRefreshing, CheckpointRows: len(c.evtPartial), CheckpointPage: c.evtPages,
		CheckpointLimit: c.evtPageLimit, LastError: c.evtLastErr}
	out.Complete = !out.At.IsZero() && len(out.Markets) > 0
	c.evtMu.Unlock()
	return out
}
