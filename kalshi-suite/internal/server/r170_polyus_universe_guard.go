package server

// R170 durable PolyUS full-board guard.
//
// The public market gateway can return a syntactically successful but catastrophically truncated
// board. In-memory comparison alone is insufficient because every suite restart erased the last
// good count. This receipt survives restarts, requires corroboration on a genuinely new install,
// and has a bounded recovery path for a real mass closure without ever accepting tiny repeated
// gateway fragments as a complete universe.

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

const (
	polyUSUniverseReceiptFile         = "polyus_full_universe_receipt.json"
	polyUSUniverseColdMinimum         = 1000
	polyUSUniverseColdConfirmations   = 2
	polyUSUniverseShrinkConfirmations = 3
	polyUSUniverseConfirmSpan         = 30 * time.Second
	polyUSUniverseOldBaseline         = 7 * 24 * time.Hour
)

type polyUSFullUniverseReceipt struct {
	Version              int       `json:"version"`
	LastGoodCount        int       `json:"last_good_count"`
	LastGoodAt           time.Time `json:"last_good_at"`
	PendingCount         int       `json:"pending_count,omitempty"`
	PendingFirstAt       time.Time `json:"pending_first_at,omitempty"`
	PendingConfirmations int       `json:"pending_confirmations,omitempty"`
	UpdatedAt            time.Time `json:"updated_at"`
}

func polyUSUniverseCountsAgree(a, b int) bool {
	if a <= 0 || b <= 0 {
		return false
	}
	large, small := a, b
	if small > large {
		large, small = small, large
	}
	return int64(small)*100 >= int64(large)*95
}

func notePolyUSUniversePending(r polyUSFullUniverseReceipt, candidate int,
	now time.Time) polyUSFullUniverseReceipt {
	if !polyUSUniverseCountsAgree(r.PendingCount, candidate) {
		r.PendingCount = candidate
		r.PendingFirstAt = now
		r.PendingConfirmations = 1
	} else {
		r.PendingConfirmations++
	}
	r.Version = 1
	r.UpdatedAt = now
	return r
}

// evaluatePolyUSFullUniverseCandidate is pure so the cold-boot and recovery policy can be tested
// without a filesystem or server. accepted means the caller may expose candidate as the complete
// board. The returned receipt must be durably written before doing so.
func evaluatePolyUSFullUniverseCandidate(r polyUSFullUniverseReceipt, inMemoryLastGood,
	candidate int, now time.Time) (next polyUSFullUniverseReceipt, accepted bool, reason string) {
	now = now.UTC()
	r.Version = 1
	if candidate <= 0 {
		r.UpdatedAt = now
		return r, false, "no proven-open markets"
	}
	baseline := inMemoryLastGood
	if r.LastGoodCount > baseline {
		baseline = r.LastGoodCount
	}
	if baseline <= 0 {
		if candidate < polyUSUniverseColdMinimum {
			r.UpdatedAt = now
			return r, false, fmt.Sprintf("cold board below corroboration floor: %d", candidate)
		}
		r = notePolyUSUniversePending(r, candidate, now)
		if r.PendingConfirmations < polyUSUniverseColdConfirmations ||
			now.Sub(r.PendingFirstAt) < polyUSFullUniverseRetryDelay {
			return r, false, fmt.Sprintf("cold board awaiting corroboration: %d confirmation(s)",
				r.PendingConfirmations)
		}
		r.LastGoodCount, r.LastGoodAt = candidate, now
		r.PendingCount, r.PendingConfirmations, r.PendingFirstAt = 0, 0, time.Time{}
		r.UpdatedAt = now
		return r, true, ""
	}
	if int64(candidate)*100 >= int64(baseline)*polyUSFullUniverseMinRetainedPercent {
		r.LastGoodCount, r.LastGoodAt = candidate, now
		r.PendingCount, r.PendingConfirmations, r.PendingFirstAt = 0, 0, time.Time{}
		r.UpdatedAt = now
		return r, true, ""
	}

	// A real mass closure has a bounded path: three stable crawls spanning at least 30 seconds.
	// A recent baseline may fall only as far as half; after seven days offline the floor relaxes
	// to one tenth. Both paths retain an absolute 1,000-market floor, so repeated 2/42-row gateway
	// fragments can never become "complete" merely by agreeing with themselves.
	minimum := baseline / 2
	if !r.LastGoodAt.IsZero() && now.Sub(r.LastGoodAt) >= polyUSUniverseOldBaseline {
		minimum = baseline / 10
	}
	if minimum < polyUSUniverseColdMinimum {
		minimum = polyUSUniverseColdMinimum
	}
	if candidate < minimum {
		r.PendingCount, r.PendingConfirmations, r.PendingFirstAt = 0, 0, time.Time{}
		r.UpdatedAt = now
		return r, false, fmt.Sprintf("catastrophic open-universe shrink %d -> %d", baseline, candidate)
	}
	r = notePolyUSUniversePending(r, candidate, now)
	if r.PendingConfirmations < polyUSUniverseShrinkConfirmations ||
		now.Sub(r.PendingFirstAt) < polyUSUniverseConfirmSpan {
		return r, false, fmt.Sprintf(
			"large open-universe shrink %d -> %d awaiting corroboration: %d confirmation(s)",
			baseline, candidate, r.PendingConfirmations)
	}
	r.LastGoodCount, r.LastGoodAt = candidate, now
	r.PendingCount, r.PendingConfirmations, r.PendingFirstAt = 0, 0, time.Time{}
	r.UpdatedAt = now
	return r, true, ""
}

func loadPolyUSFullUniverseReceipt(path string) (polyUSFullUniverseReceipt, error) {
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return polyUSFullUniverseReceipt{}, nil
	}
	if err != nil {
		return polyUSFullUniverseReceipt{}, err
	}
	var receipt polyUSFullUniverseReceipt
	if len(raw) == 0 || json.Unmarshal(raw, &receipt) != nil || receipt.Version != 1 ||
		receipt.LastGoodCount < 0 || receipt.PendingCount < 0 ||
		receipt.PendingConfirmations < 0 {
		return polyUSFullUniverseReceipt{}, errors.New("invalid durable PolyUS full-universe receipt")
	}
	return receipt, nil
}

func persistPolyUSFullUniverseReceipt(path string, receipt polyUSFullUniverseReceipt) error {
	raw, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	return nil
}

func (s *Server) validateAndPersistPolyUSFullUniverseCandidate(lastGood, candidate int,
	now time.Time) error {
	if s == nil {
		return errors.New("server unavailable")
	}
	path := filepath.Join(s.cfg().DataDir, polyUSUniverseReceiptFile)
	receipt, err := loadPolyUSFullUniverseReceipt(path)
	if err != nil {
		return fmt.Errorf("durable full-universe baseline unavailable: %w", err)
	}
	next, accepted, reason := evaluatePolyUSFullUniverseCandidate(receipt, lastGood, candidate, now)
	if err := persistPolyUSFullUniverseReceipt(path, next); err != nil {
		return fmt.Errorf("durable full-universe receipt write failed: %w", err)
	}
	if !accepted {
		return errors.New(reason)
	}
	return nil
}
