package server

import (
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/researchreplay"
)

type replayVerificationCache struct {
	sync.Mutex
	at        time.Time
	indexMod  time.Time
	indexSize int64
	report    researchreplay.VerificationReport
}

var replayVerificationCaches sync.Map // map[*Server]*replayVerificationCache

func replayVerificationCacheFor(s *Server) *replayVerificationCache {
	if value, ok := replayVerificationCaches.Load(s); ok {
		return value.(*replayVerificationCache)
	}
	created := &replayVerificationCache{}
	value, _ := replayVerificationCaches.LoadOrStore(s, created)
	return value.(*replayVerificationCache)
}

func (s *Server) boundedReplayVerification(dir string, now time.Time) researchreplay.VerificationReport {
	cache := replayVerificationCacheFor(s)
	cache.Lock()
	defer cache.Unlock()
	info, _ := os.Stat(filepath.Join(dir, researchreplay.IndexFileName))
	var mod time.Time
	var size int64
	if info != nil {
		mod, size = info.ModTime(), info.Size()
	}
	age := now.Sub(cache.at)
	unchanged := mod.Equal(cache.indexMod) && size == cache.indexSize
	if !cache.at.IsZero() && (age < 30*time.Second || unchanged && age < 5*time.Minute) {
		report := cache.report
		report.Cached = true
		return report
	}
	report, _ := researchreplay.VerifyDirectoryRecent(dir, 16)
	cache.at, cache.indexMod, cache.indexSize, cache.report = now, mod, size, report
	return report
}

func (s *Server) handleResearchSourceClocks(w http.ResponseWriter, r *http.Request) {
	report, err := s.store.SourceClockReport(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, report)
}

// handleResearchReplay verifies files on demand without opening a writer or touching kalshi.db.
// Integrity failure remains a readable 200 report so the research UI can display the exact broken
// chain/orphan condition instead of replacing it with a generic request error.
func (s *Server) handleResearchReplay(w http.ResponseWriter, r *http.Request) {
	dir := filepath.Join(s.cfg().DataDir, "research-replay")
	report := s.boundedReplayVerification(dir, time.Now())
	stats := s.selectedResearchReplayStats()
	report.PendingSelectedFrames, report.PendingSelectedBytes = stats.Pending, stats.Bytes
	report.SelectionDropped, report.SelectionEvicted = stats.Dropped, stats.Evicted
	writeJSON(w, http.StatusOK, report)
}
