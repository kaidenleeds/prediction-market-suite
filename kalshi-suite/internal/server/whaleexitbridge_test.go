package server

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func testWhaleExitBridgeJob(server *Server, condition string) whaleExitBridgeJob {
	return whaleExitBridgeJob{server: server, conditionID: condition, soldOutcome: "YES",
		title: "test", sourcePrice: 0.4, notional: 2500, traders: 2}
}

func TestWhaleExitBridgeDispatcherNeverBlocksProducerUnderBackpressure(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var runs atomic.Int32
	d := newWhaleExitBridgeDispatcher(1, func(context.Context, whaleExitBridgeJob) {
		if runs.Add(1) == 1 {
			close(started)
			<-release
		}
	}, nil)
	defer d.close()

	server := &Server{}
	if got := d.enqueue(testWhaleExitBridgeJob(server, "one")); got != whaleExitBridgeQueued {
		t.Fatalf("first enqueue=%v, want queued", got)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("worker never started first job")
	}
	if got := d.enqueue(testWhaleExitBridgeJob(server, "two")); got != whaleExitBridgeQueued {
		t.Fatalf("second enqueue=%v, want queued behind blocked worker", got)
	}
	before := time.Now()
	if got := d.enqueue(testWhaleExitBridgeJob(server, "three")); got != whaleExitBridgeQueueFull {
		t.Fatalf("third enqueue=%v, want queue-full drop", got)
	}
	if elapsed := time.Since(before); elapsed > 50*time.Millisecond {
		t.Fatalf("saturated enqueue blocked producer for %v", elapsed)
	}
	close(release)
}

func TestWhaleExitBridgeDispatcherDeduplicatesBeforeExpensiveWork(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var runs atomic.Int32
	d := newWhaleExitBridgeDispatcher(4, func(context.Context, whaleExitBridgeJob) {
		runs.Add(1)
		close(started)
		<-release
	}, nil)
	defer d.close()

	server := &Server{}
	job := testWhaleExitBridgeJob(server, "same-condition")
	if got := d.enqueue(job); got != whaleExitBridgeQueued {
		t.Fatalf("first enqueue=%v, want queued", got)
	}
	if got := d.enqueue(job); got != whaleExitBridgeDuplicate {
		t.Fatalf("duplicate enqueue=%v, want duplicate", got)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("worker never started")
	}
	close(release)
	deadline := time.Now().Add(time.Second)
	for runs.Load() != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := runs.Load(); got != 1 {
		t.Fatalf("expensive evaluator ran %d times, want one", got)
	}
}

func TestWhaleExitBridgeRejectionsAreBatchedByReasonAndVenue(t *testing.T) {
	var b whaleExitBridgeRejectBatcher
	server := &Server{}
	start := time.Unix(1_800_000_000, 0)
	if emit, count, _ := b.record(server, "no-certificate", "kalshi", start); !emit || count != 1 {
		t.Fatalf("first rejection emit/count=%v/%d, want true/1", emit, count)
	}
	for i := 1; i < whaleExitRejectBatchSize; i++ {
		if emit, _, _ := b.record(server, "no-certificate", "kalshi", start.Add(time.Duration(i)*time.Second)); emit {
			t.Fatalf("repeat %d emitted an individual rejection", i)
		}
	}
	if emit, count, since := b.record(server, "no-certificate", "kalshi",
		start.Add(whaleExitRejectBatchSize*time.Second)); !emit || count != whaleExitRejectBatchSize || since.IsZero() {
		t.Fatalf("batch emit/count/since=%v/%d/%v", emit, count, since)
	}
	if emit, count, _ := b.record(server, "no-certificate", "polyus",
		start.Add(whaleExitRejectBatchSize*time.Second)); !emit || count != 1 {
		t.Fatalf("venue-specific first rejection emit/count=%v/%d", emit, count)
	}
	if emit, count, _ := b.record(server, "no-certificate", "kalshi",
		start.Add(whaleExitRejectBatchSize*time.Second+whaleExitRejectBatchWindow+time.Second)); !emit || count != 1 {
		t.Fatalf("time-window summary emit/count=%v/%d, want true/1", emit, count)
	}
}

func TestWhaleExitHoldBridgeFadesTheExitInsteadOfBuyingTheComplement(t *testing.T) {
	// The wallet sold Padres (outcome token 1 / binary NO). The system's hypothesis is to BUY
	// Padres and hold, so a same-orientation certificate must also land on destination NO.
	sourceSide, ok := whaleExitSourceSide([]string{"Dodgers", "Padres"}, "Padres")
	if !ok || sourceSide != "NO" {
		t.Fatalf("source side=(%q,%v), want NO,true", sourceSide, ok)
	}
	cert := storage.RulePairCertificateSpec{SpecHash: "certificate", Orientation: "same"}
	buySold := whaleExitBridgeCandidate{Venue: "kalshi", Ticker: "KXGAME", Side: "NO"}
	if accepted, reason := whaleExitBridgeIdentityOK(sourceSide, buySold, cert, true, nil); !accepted {
		t.Fatalf("same sold exposure rejected: %s", reason)
	}
	buyComplement := whaleExitBridgeCandidate{Venue: "kalshi", Ticker: "KXGAME", Side: "YES"}
	if accepted, reason := whaleExitBridgeIdentityOK(sourceSide, buyComplement, cert, true, nil); accepted ||
		reason != "matched-side-conflicts-with-certificate-orientation" {
		t.Fatalf("complement exposure accepted=%v reason=%q", accepted, reason)
	}
}

func TestWhaleExitHoldBridgeHonorsInverseCertificateAndFailsClosed(t *testing.T) {
	cert := storage.RulePairCertificateSpec{SpecHash: "certificate", Orientation: "inverse"}
	if accepted, reason := whaleExitBridgeIdentityOK("YES",
		whaleExitBridgeCandidate{Venue: "polyus", Ticker: "slug", Side: "NO"}, cert, true, nil); !accepted {
		t.Fatalf("inverse certificate's exact destination side rejected: %s", reason)
	}
	if accepted, reason := whaleExitBridgeIdentityOK("YES",
		whaleExitBridgeCandidate{Venue: "polyus", Ticker: "slug", Side: "NO"}, cert, false, nil); accepted ||
		reason != "no-current-settlement-equivalence-certificate" {
		t.Fatalf("stale certificate accepted=%v reason=%q", accepted, reason)
	}
	if accepted, reason := whaleExitBridgeIdentityOK("YES",
		whaleExitBridgeCandidate{Venue: "polyus", Ticker: "slug", Side: "NO"}, cert, true,
		errors.New("storage unavailable")); accepted || reason != "rule-certificate-read-error" {
		t.Fatalf("certificate read error accepted=%v reason=%q", accepted, reason)
	}
}

func TestWhaleExitHoldBridgeRejectsAmbiguousSourceOutcome(t *testing.T) {
	for _, tc := range []struct {
		outcomes []string
		sold     string
	}{
		{[]string{"A", "B", "C"}, "B"},
		{[]string{"A", "B"}, "C"},
		{[]string{"A", "a"}, "A"},
	} {
		if side, ok := whaleExitSourceSide(tc.outcomes, tc.sold); ok || side != "" {
			t.Fatalf("ambiguous source %v/%q returned (%q,%v)", tc.outcomes, tc.sold, side, ok)
		}
	}
}

func TestWhaleExitHoldBridgeIsRegisteredAsExecutableOneSideSystem(t *testing.T) {
	if !oneSideFamilies[whaleExitHoldBridgeFamily] || !singleTriggerFamilies[whaleExitHoldBridgeFamily] {
		t.Fatal("whale-exit hold bridge missing from executable side/discovery registries")
	}
	sideClass, discovery, _ := systemSideClassification(whaleExitHoldBridgeFamily)
	if sideClass != sideClassOne || discovery != "single_trigger" {
		t.Fatalf("classification=%s/%s want one-side/single-trigger", sideClass, discovery)
	}
}
