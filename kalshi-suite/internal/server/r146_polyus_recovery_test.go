package server

import (
	"testing"
	"time"
)

func TestR146PolyUSCompleteCrawlScheduleIsIndependentAndSingleFlight(t *testing.T) {
	now := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	if !polyUSFullUniverseSweepDue(now, time.Time{}, false) {
		t.Fatal("cold complete-universe crawl was not due")
	}
	if polyUSFullUniverseSweepDue(now, now.Add(-time.Hour), true) {
		t.Fatal("busy complete-universe crawl allowed an overlapping request")
	}
	if polyUSFullUniverseSweepDue(now, now.Add(-polyUSFullUniverseSweepCadence), false) {
		t.Fatal("cadence boundary should wait for the next scheduler tick")
	}
	if !polyUSFullUniverseSweepDue(now.Add(time.Nanosecond), now.Add(-polyUSFullUniverseSweepCadence), false) {
		t.Fatal("complete-universe crawl did not become due after its cadence")
	}
	if polyUSFullUniverseScheduleTick >= polyUSFullUniverseSweepCadence {
		t.Fatal("independent scheduler cannot observe the due time promptly")
	}
}

func TestR146PolyUSTransientFailureRetriesInFifteenSeconds(t *testing.T) {
	now := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	anchor := polyUSFullUniverseRetryAnchor(now)
	if got := polyUSFullUniverseSweepCadence - now.Sub(anchor); got != polyUSFullUniverseRetryDelay {
		t.Fatalf("retry delay=%v, want %v", got, polyUSFullUniverseRetryDelay)
	}
	if polyUSFullUniverseSweepDue(now.Add(polyUSFullUniverseRetryDelay), anchor, false) {
		t.Fatal("exact retry boundary should wait for the next scheduler tick")
	}
	if !polyUSFullUniverseSweepDue(now.Add(polyUSFullUniverseRetryDelay+time.Nanosecond), anchor, false) {
		t.Fatal("transient failure did not become retryable after the short delay")
	}
}

func TestR170PolyUSCompleteUniverseRejectsCatastrophicSuccessfulShrink(t *testing.T) {
	now := time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)
	if _, accepted, _ := evaluatePolyUSFullUniverseCandidate(
		polyUSFullUniverseReceipt{}, 0, 37, now); accepted {
		t.Fatal("tiny cold-boot fragment became a complete universe")
	}
	cold, accepted, _ := evaluatePolyUSFullUniverseCandidate(
		polyUSFullUniverseReceipt{}, 0, 15881, now)
	if accepted {
		t.Fatal("one uncorroborated cold crawl became a complete universe")
	}
	cold, accepted, reason := evaluatePolyUSFullUniverseCandidate(
		cold, 0, 15790, now.Add(polyUSFullUniverseRetryDelay+time.Second))
	if !accepted {
		t.Fatalf("second agreeing cold crawl was not accepted: %s", reason)
	}
	if next, accepted, reason := evaluatePolyUSFullUniverseCandidate(
		cold, 15881, 12000, now.Add(time.Minute)); !accepted {
		t.Fatalf("ordinary lifecycle churn rejected: %s (%+v)", reason, next)
	}
	if _, accepted, _ := evaluatePolyUSFullUniverseCandidate(
		cold, 15881, 11290, now.Add(time.Minute)); accepted {
		t.Fatal("gateway truncation replaced the last complete universe")
	}
	if _, accepted, _ := evaluatePolyUSFullUniverseCandidate(
		polyUSFullUniverseReceipt{Version: 1, LastGoodCount: 1306, LastGoodAt: now},
		1306, 2, now.Add(time.Minute)); accepted {
		t.Fatal("second-stage gateway truncation replaced the retained complete universe")
	}
}

func TestR170PolyUSCompleteUniverseHasBoundedCorroboratedShrinkRecovery(t *testing.T) {
	now := time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)
	receipt := polyUSFullUniverseReceipt{Version: 1, LastGoodCount: 10000,
		LastGoodAt: now.Add(-time.Hour)}
	var accepted bool
	var reason string
	for i, at := range []time.Duration{0, 16 * time.Second, 31 * time.Second} {
		receipt, accepted, reason = evaluatePolyUSFullUniverseCandidate(
			receipt, 10000, 7000+i*5, now.Add(at))
	}
	if !accepted {
		t.Fatalf("three stable, bounded shrink crawls did not recover: %s", reason)
	}
	if receipt.LastGoodCount < 7000 || receipt.PendingConfirmations != 0 {
		t.Fatalf("recovered receipt=%+v", receipt)
	}
}
