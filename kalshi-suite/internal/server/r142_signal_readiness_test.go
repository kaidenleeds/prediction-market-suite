package server

import (
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
)

func TestR142SignalFamineUsesCurrentProducerForConditionalZeroOpportunityScan(t *testing.T) {
	now := time.Date(2026, 7, 13, 2, 0, 0, 0, time.UTC)
	oldRow := now.Add(-8 * time.Hour).Format(time.RFC3339Nano)
	receipts := map[string]signalProducerReceipt{
		"kalshi": {Completed: now.Add(-20 * time.Second)},
		"polyus": {Completed: now.Add(-10 * time.Second)},
	}
	producerOK, producerDetail := signalProducersClassify(now, 3*time.Minute,
		[]string{"kalshi", "polyus"}, receipts)
	ok, detail := signalFamineClassify(now, producerOK, producerDetail,
		oldRow, nil, "old cached query error", nil)
	if !ok {
		t.Fatalf("current zero-opportunity producer was called famine: %s", detail)
	}
	if !strings.Contains(detail, "zero opportunities is healthy") {
		t.Fatalf("quiet-scan reason missing: %s", detail)
	}
}

func TestR142SignalFamineStillFailsDeadOrMissingProducer(t *testing.T) {
	now := time.Date(2026, 7, 13, 2, 0, 0, 0, time.UTC)
	for name, receipt := range map[string]signalProducerReceipt{
		"missing": {},
		"stale":   {Completed: now.Add(-4 * time.Minute)},
		"failed":  {Completed: now.Add(-10 * time.Second), Attempts: 2, Errors: 1},
	} {
		t.Run(name, func(t *testing.T) {
			ok, detail := signalProducersClassify(now, 3*time.Minute, []string{"kalshi"},
				map[string]signalProducerReceipt{"kalshi": receipt})
			if ok {
				t.Fatalf("dead producer passed liveness: %s", detail)
			}
		})
	}
}

func TestR142SignalProducersRequireEveryExecutionVenue(t *testing.T) {
	now := time.Date(2026, 7, 13, 2, 0, 0, 0, time.UTC)
	receipts := map[string]signalProducerReceipt{
		"polyus": {Completed: now.Add(-10 * time.Second), Attempts: 0, Errors: 0},
	}
	if ok, detail := signalProducersClassify(now, 3*time.Minute,
		[]string{"kalshi", "polyus"}, receipts); ok || !strings.Contains(detail, "kalshi=warming") {
		t.Fatalf("one venue masked a missing producer: ok=%v detail=%s", ok, detail)
	}
}

func TestR142ReadyUsesCompletedProducerReceiptAndInsertOutcome(t *testing.T) {
	s := testServer(t)
	s.kal = kalshi.NewClient(kalshi.BaseDemo, nil, 1, 500*time.Millisecond)

	s.finishSignalProducer("kalshi", time.Now(), 0, 0)
	components, _ := readyPayload(t, s)
	if got := components["signal_producers"]; got == nil || got["ok"] != true {
		t.Fatalf("completed zero-candidate producer was not healthy: %v", got)
	}
	if got := components["signal_famine"]; got == nil || got["ok"] != true {
		t.Fatalf("famine duplicated an old-row false alarm: %v", got)
	}

	s.finishSignalProducer("kalshi", time.Now(), 2, 1)
	components, _ = readyPayload(t, s)
	if got := components["signal_producers"]; got == nil || got["ok"] != false ||
		!strings.Contains(got["detail"].(string), "errors=1") {
		t.Fatalf("failed insert scan was painted green: %v", got)
	}
}

func TestR142SignalFamineEnforcesOnlyConfiguredFamilyCadence(t *testing.T) {
	now := time.Date(2026, 7, 13, 2, 0, 0, 0, time.UTC)
	cadence := []signalFamilyCadence{{fam: "sharpline", bar: 6 * time.Hour}}
	currentProducer := now.Add(-15 * time.Second)
	producerOK, producerDetail := signalProducersClassify(now, 3*time.Minute, []string{"kalshi"},
		map[string]signalProducerReceipt{"kalshi": {Completed: currentProducer}})

	fresh := map[string]string{"sharpline": now.Add(-5 * time.Hour).Format(time.RFC3339Nano)}
	if ok, detail := signalFamineClassify(now, producerOK, producerDetail, "", fresh, "", cadence); !ok {
		t.Fatalf("fresh configured family failed: %s", detail)
	}

	stale := map[string]string{"sharpline": now.Add(-7 * time.Hour).Format(time.RFC3339Nano)}
	if ok, detail := signalFamineClassify(now, producerOK, producerDetail, "", stale, "", cadence); ok ||
		!strings.Contains(detail, "over-cadence: sharpline") {
		t.Fatalf("stale configured family passed: ok=%v detail=%s", ok, detail)
	}

	if ok, detail := signalFamineClassify(now, producerOK, producerDetail, "", nil, "", cadence); ok ||
		!strings.Contains(detail, "sharpline=missing") {
		t.Fatalf("missing configured family passed: ok=%v detail=%s", ok, detail)
	}
}
