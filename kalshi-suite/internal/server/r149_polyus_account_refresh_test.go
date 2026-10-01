package server

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestR149PolyUSHistory429DoesNotPoisonExposureReceipt(t *testing.T) {
	got := classifyPolyUSAccountRefresh(nil, nil, errors.New("polyus activities page 2: status 429"))
	if got.ExposureErr != "" || got.ExposureRateLimited {
		t.Fatalf("history-only 429 poisoned exposure truth: %+v", got)
	}
	if !got.HistoryRateLimited || !strings.Contains(got.HistoryErr, "activities") {
		t.Fatalf("history 429 was not retained on its own lane: %+v", got)
	}
	if why := livePolyUSExposureReceiptReason(time.Now(), got.ExposureErr, true, time.Now()); why != "" {
		t.Fatalf("fresh push+position receipt was rejected because history failed: %q", why)
	}
}

func TestR149PolyUSPosition429StillFailsExposureClosed(t *testing.T) {
	got := classifyPolyUSAccountRefresh(nil, errors.New("polyus positions page 1: status 429"), nil)
	if !got.ExposureRateLimited || got.ExposureErr == "" || got.HistoryErr != "" {
		t.Fatalf("position 429 did not stay safety-critical: %+v", got)
	}
	if why := livePolyUSExposureReceiptReason(time.Now(), got.ExposureErr, true, time.Now()); why == "" {
		t.Fatal("position failure was treated as readable exposure")
	}
}

func TestR149PolyUSHistoryAndExposureUseIndependentBackoffFields(t *testing.T) {
	s := &Server{}
	now := time.Now()
	s.pus429Til = now.Add(30 * time.Second)
	s.pusHist429Til = now.Add(2 * time.Minute)
	if !now.Before(s.pus429Til) || !now.Before(s.pusHist429Til) || s.pus429Til.Equal(s.pusHist429Til) {
		t.Fatalf("account/history backoffs were not independent: exposure=%v history=%v", s.pus429Til, s.pusHist429Til)
	}
}
