package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/config"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestR133StopOnlyRatioUsesObservedGapFillFeeAndLatency(t *testing.T) {
	path := exitReplayPath{
		entry: 0.50, won: false, marks: []float64{0.50, 0.10, 0.00},
		slip: 0.01, resolveHours: 2,
	}
	feeCalls := 0
	fee := func(_ exitReplayPath, px float64) float64 {
		feeCalls++
		if math.Abs(px-0.09) > 1e-9 {
			t.Fatalf("fee price=%v; want observed 0.10 less 0.01 crossed spread", px)
		}
		return 0.02
	}
	net, held, exited := replayExitPolicy(path,
		exitResearchPolicy{mode: "sl-ratio", r: 0.50}, fee)
	if !exited || feeCalls != 1 {
		t.Fatalf("exited=%v feeCalls=%d; want one fee-bearing exit", exited, feeCalls)
	}
	// The 25c trigger gapped to a 10c observation. Honest execution is 9c, not the stale 25c
	// threshold: 0.09 - 0.50 - 0.02 fee = -0.43.
	if math.Abs(net-(-0.43)) > 1e-9 {
		t.Fatalf("net=%v; want -0.43 gap/slip/fee-inclusive", net)
	}
	if math.Abs(held-1) > 1e-9 { // index 1 of a 3-mark, 2h path
		t.Fatalf("held=%v; want 1h", held)
	}
}

func TestR133ZeroPriceExitNeverCallsFeeFailClosedSentinel(t *testing.T) {
	path := exitReplayPath{entry: 0.01, won: false, marks: []float64{0.01, 0}, slip: 0.03, resolveHours: 1}
	net, _, exited := replayExitPolicy(path, exitResearchPolicy{mode: "sl-ratio", r: 0.50},
		func(exitReplayPath, float64) float64 {
			t.Fatal("0c boundary must have zero fee without calling a fail-closed live fee helper")
			return 0
		})
	if !exited || math.Abs(net-(-0.01)) > 1e-9 {
		t.Fatalf("exited=%v net=%v; want a real 0c sale and -1c post-fill net", exited, net)
	}
}

func TestR133StopOnlyCanBeatRideWithoutForcingProfitTaking(t *testing.T) {
	paths := []exitReplayPath{
		{entry: 0.50, won: true, marks: []float64{0.50, 0.80, 1.00}, resolveHours: 2},
		{entry: 0.50, won: false, marks: []float64{0.50, 0.20, 0.00}, resolveHours: 2},
	}
	zeroFee := func(exitReplayPath, float64) float64 { return 0 }
	ride := summarizeExitPolicy(paths, exitResearchPolicy{mode: "ride"}, zeroFee)
	stopOnly := summarizeExitPolicy(paths, exitResearchPolicy{mode: "sl-ratio", r: 0.50}, zeroFee)
	if math.Abs(ride.net) > 1e-9 {
		t.Fatalf("ride net=%v; want 0", ride.net)
	}
	// Winner still settles +50c; loser gaps through the 25c stop and exits at 20c (-30c).
	if math.Abs(stopOnly.net-0.20) > 1e-9 || stopOnly.exits != 1 {
		t.Fatalf("stop-only net=%v exits=%d; want +0.20 and one exit", stopOnly.net, stopOnly.exits)
	}
}

func TestR133FreeRollCapitalRateDoesNotMasqueradeAsHigherEV(t *testing.T) {
	path := exitReplayPath{
		entry: 0.20, won: true, marks: []float64{0.20, 0.61, 1.00},
		resolveHours: 4,
	}
	zeroFee := func(exitReplayPath, float64) float64 { return 0 }
	ride := summarizeExitPolicy([]exitReplayPath{path}, exitResearchPolicy{mode: "ride"}, zeroFee)
	free := summarizeExitPolicy([]exitReplayPath{path},
		exitResearchPolicy{mode: "free-roll", r: 0.50}, zeroFee)
	if !(free.net < ride.net) {
		t.Fatalf("free-roll net=%v must remain below ride=%v on this winner", free.net, ride.net)
	}
	rideRate := ride.timedNet / ride.capitalDays
	freeRate := free.timedNet / free.capitalDays
	if !(freeRate > rideRate) {
		t.Fatalf("free-roll rate=%v should exceed ride=%v after principal returns halfway", freeRate, rideRate)
	}
	// This is why the rate selector is EV-constrained: fast cash return is reported, but cannot
	// promote an exit that gives away settlement profit.
}

func TestR133AddedExitModesStayResearchOnlyAndUseHoldout(t *testing.T) {
	// Newest half and older half each contain the same 30 winner/loser pairs, so the stop-only
	// improvement confirms out of sample. No cell may become live-eligible from this grid alone.
	paths := make([]exitReplayPath, 0, 120)
	newest := time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 60; i++ {
		ticker := fmt.Sprintf("KX-HOLDOUT-%03d", i)
		observed := newest.Add(-time.Duration(i) * time.Hour)
		paths = append(paths,
			exitReplayPath{platform: "kalshi", ticker: ticker, observed: observed, entry: 0.50, won: true, marks: []float64{0.50, 0.80, 1.00}, resolveHours: 2},
			exitReplayPath{platform: "kalshi", ticker: ticker, observed: observed, entry: 0.50, won: false, marks: []float64{0.50, 0.20, 0.00}, resolveHours: 2},
		)
	}
	out := buildExitResearch(paths, func(exitReplayPath, float64) float64 { return 0 })
	if !out.ResearchOnly || out.N != 120 || out.TimedN != 120 ||
		out.OlderClusters != 30 || out.NewerClusters != 30 || out.HoldoutExcludedRows != 0 {
		t.Fatalf("research envelope=%+v", out)
	}
	seen := map[string]bool{}
	confirmedStop := false
	for _, cell := range out.Grid {
		seen[cell.Mode] = true
		if cell.LiveEligible {
			t.Fatalf("research grid made %q live-eligible", cell.Label)
		}
		if cell.Mode == "sl-ratio" && cell.HoldoutPass {
			confirmedStop = true
		}
	}
	for _, mode := range []string{"ride", "sl-ratio", "tp-ratio", "free-roll", "break-even", "trail"} {
		if !seen[mode] {
			t.Fatalf("missing research mode %q", mode)
		}
	}
	if !confirmedStop {
		t.Fatal("synthetic stop-only edge did not pass the newer-half check")
	}
}

func TestR175ExitHoldoutNeverSplitsTickerProxyAcrossSelectionAndTest(t *testing.T) {
	base := time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)
	path := func(ticker string, observed time.Time) exitReplayPath {
		return exitReplayPath{platform: "kalshi", ticker: ticker, observed: observed, entry: .5, won: true,
			marks: []float64{.5, 1}, resolveHours: 2}
	}
	// Input order deliberately disagrees with observation chronology and repeats A. An id/insertion
	// split would leak or misorder A; the proxy partition must use authoritative observation time
	// and keep both A rows together.
	partition := partitionExitResearchHoldout([]exitReplayPath{
		path("A", base.Add(-4*time.Hour)),
		path("C", base.Add(-2*time.Hour)),
		path("A", base.Add(-time.Hour)),
		path("B", base),
		path("D", base.Add(-3*time.Hour)),
	})
	if partition.newerClusters != 2 || partition.olderClusters != 2 ||
		len(partition.newer) != 3 || len(partition.older) != 2 {
		t.Fatalf("unexpected event-proxy partition: %+v", partition)
	}
	if partition.newer[0].ticker != "B" || partition.newer[1].ticker != "A" ||
		partition.newer[2].ticker != "A" {
		t.Fatalf("proxy groups were not ordered by authoritative observation time: %+v", partition.newer)
	}
	seen := map[string]string{}
	for _, p := range partition.newer {
		seen[p.ticker] = "newer"
	}
	for _, p := range partition.older {
		if side := seen[p.ticker]; side != "" {
			t.Fatalf("ticker %q leaked from %s into older selection", p.ticker, side)
		}
	}

	unknownIdentity := path("", base)
	unknownClock := path("E", time.Time{})
	partition = partitionExitResearchHoldout([]exitReplayPath{
		path("A", base), path("B", base.Add(-time.Hour)), unknownIdentity, unknownClock,
	})
	if partition.excluded != 2 {
		t.Fatalf("unknown identity/clock was treated as independent: %+v", partition)
	}
}

func TestR175ExitReplayParsesAuthoritativeObservationClockFailClosed(t *testing.T) {
	want := time.Date(2026, 7, 25, 12, 34, 56, 123456789, time.UTC)
	base := storage.SignalRow{
		Platform: "kalshi", Ticker: "KX-CLOCK", Resolved: 1, Won: 1,
		FillPrice: .5, PostPath: ".5,1",
	}
	valid, invalid := base, base
	valid.TS = want.Format(time.RFC3339Nano)
	invalid.TS = "not-a-clock"
	paths := parseExitReplayPaths([]storage.SignalRow{valid, invalid})
	if len(paths) != 2 || !paths[0].observed.Equal(want) || !paths[1].observed.IsZero() {
		t.Fatalf("observation clocks were not parsed fail-closed: %+v", paths)
	}
}

func TestR175ExitHoldoutPassRequiresOlderAndNewerProxyEvidence(t *testing.T) {
	result := exitReplayResult{n: 60, net: 12}
	ride := exitReplayResult{n: 60, net: 0}
	policy := exitResearchPolicy{mode: "sl-ratio", label: "fixture"}
	if cell := researchCell(policy, result, result, result, ride, ride, ride, 2, 2); !cell.HoldoutPass {
		t.Fatalf("two-sided proxy evidence did not pass: %+v", cell)
	}
	if cell := researchCell(policy, result, result, result, ride, ride, ride, 1, 2); cell.HoldoutPass {
		t.Fatalf("one older proxy masqueraded as a holdout pass: %+v", cell)
	}
	weakOlder := exitReplayResult{n: 60, net: 0}
	if cell := researchCell(policy, result, weakOlder, result, ride, ride, ride, 2, 2); cell.HoldoutPass {
		t.Fatalf("newer-only edge masqueraded as a holdout pass: %+v", cell)
	}
}

func TestR133CapitalDayExcludesUntimedAndOverSixHourPaths(t *testing.T) {
	paths := []exitReplayPath{
		{entry: 0.50, won: true, marks: []float64{0.50, 1.00}, resolveHours: 2},
		{entry: 0.50, won: true, marks: []float64{0.50, 1.00}, resolveHours: 0},
		{entry: 0.50, won: true, marks: []float64{0.50, 1.00}, resolveHours: 12},
	}
	r := summarizeExitPolicy(paths, exitResearchPolicy{mode: "ride"},
		func(exitReplayPath, float64) float64 { return 0 })
	if r.n != 3 || r.timedN != 1 {
		t.Fatalf("n=%d timed=%d; want 3/1", r.n, r.timedN)
	}
}

// Opt-in evidence receipt for an operator snapshot. Normal CI skips it; a local audit can point
// R133_EXIT_DB_DIR at a COPY containing kalshi.db and retain the exact grid in `go test -v` output.
// Never point this at the running suite's live data directory.
func TestR133ExitModesSnapshot(t *testing.T) {
	dir := os.Getenv("R133_EXIT_DB_DIR")
	if dir == "" {
		t.Skip("set R133_EXIT_DB_DIR to a copied DB directory for the full evidence receipt")
	}
	st, err := storage.Open(dir)
	if err != nil {
		t.Fatalf("open copied store: %v", err)
	}
	defer st.Close()
	rows, err := st.ListSLTPEligibleSignals(context.Background(), 8000)
	if err != nil {
		t.Fatalf("eligible paths: %v", err)
	}
	s := &Server{store: st, log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	cfg := config.Default()
	cfg.DataDir = dir
	s.cfgP.Store(&cfg)
	out := s.exitResearchResult(rows)
	b, _ := json.Marshal(out)
	t.Logf("R133_EXIT_RECEIPT %s", b)
	sl := s.stopLabResult(context.Background())
	t.Logf("R133_STOPLAB_RECEIPT n=%d ride=%.6f ratio_best=%.6f ratio_r=%.2f best=%s best_net=%.6f",
		sl.N, sl.Ride, sl.RatioBest, sl.RatioR, sl.Best, sl.BestNet)
}
