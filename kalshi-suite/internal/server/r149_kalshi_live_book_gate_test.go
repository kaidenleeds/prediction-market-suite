package server

import (
	"strings"
	"testing"
)

func TestR149KalshiBoardCapacityShortfallWarnsButDoesNotBlockArm(t *testing.T) {
	ok, detail, warning := kalshiLiveBooksClassify(1000, 1000, 0, 733, "")
	if !ok {
		t.Fatalf("healthy bounded transport was blocked by board-wide capacity: %s", detail)
	}
	if !strings.Contains(warning, "short by 733") || !strings.Contains(warning, "exact current full book and depth") {
		t.Fatalf("capacity warning did not preserve the per-order fail-closed truth: %q", warning)
	}

	in := greenLiveAutoGoInput()
	in.KalshiBooksOK, in.KalshiBooksDetail, in.KalshiBooksWarning = ok, detail, warning
	got := evaluateLiveAutoGo(in)
	if !got.SafeToArm || got.State != "SAFE_TO_ARM" {
		t.Fatalf("board-wide capacity warning became a safety blocker: %+v", got)
	}
	if !strings.Contains(strings.Join(got.Warnings, " | "), "short by 733") {
		t.Fatalf("capacity shortfall disappeared instead of remaining visible: %+v", got.Warnings)
	}
}

func TestR149KalshiLiveBookGateStillRequiresProvableFreshTransport(t *testing.T) {
	tests := []struct {
		name                   string
		subs, fresh, shortfall int
		err                    string
	}{
		{name: "no subscriptions", subs: 0, fresh: 0},
		{name: "dark transport", subs: 1000, fresh: 0, shortfall: 733},
		{name: "command error", subs: 1000, fresh: 1000, shortfall: 733, err: "websocket command failed"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ok, detail, warning := kalshiLiveBooksClassify(tc.subs, tc.fresh, 0, tc.shortfall, tc.err)
			if ok || warning != "" {
				t.Fatalf("unhealthy transport appeared safe: ok=%t detail=%q warning=%q", ok, detail, warning)
			}
			in := greenLiveAutoGoInput()
			in.KalshiBooksOK, in.KalshiBooksDetail = ok, detail
			got := evaluateLiveAutoGo(in)
			if got.SafeToArm || !strings.Contains(strings.Join(got.Blockers, " "), "Kalshi executable books") {
				t.Fatalf("unhealthy exact-book transport did not block ARM: %+v", got)
			}
		})
	}
}

func TestR149PerOrderDepthGateRemainsFailClosed(t *testing.T) {
	// The readiness card may ignore a board-wide subscription-cap shortfall; the money path may not
	// ignore the candidate's own depth. This pure seam is used after liveMirrorExecutable refreshes
	// the exact full book and before submit.
	if liveMirrorFullDepthAllows(false, 0, 1) || liveMirrorFullDepthAllows(false, .99, 1) ||
		liveMirrorFullDepthAllows(false, 2, 3) {
		t.Fatal("taker order escaped without enough exact visible depth")
	}
	if !liveMirrorFullDepthAllows(false, 3, 3) {
		t.Fatal("exact sufficient taker depth was incorrectly refused")
	}
}

func TestR149KalshiTransportRecoveryRestoresRunningStateWithoutChangingAuto(t *testing.T) {
	in := greenLiveAutoGoInput()
	in.Armed, in.Auto = true, true
	in.KalshiBooksOK, in.KalshiBooksDetail, in.KalshiBooksWarning =
		kalshiLiveBooksClassify(1000, 0, 0, 733, "")
	cold := evaluateLiveAutoGo(in)
	if cold.SafeToArm || cold.State != "PAUSED" || !cold.Armed || !cold.Auto {
		t.Fatalf("transient transport loss must pause readiness without toggling operator state: %+v", cold)
	}

	in.KalshiBooksOK, in.KalshiBooksDetail, in.KalshiBooksWarning =
		kalshiLiveBooksClassify(1000, 1000, 0, 733, "")
	warm := evaluateLiveAutoGo(in)
	if !warm.SafeToArm || warm.State != "RUNNING" || !warm.Armed || !warm.Auto {
		t.Fatalf("healthy transport should resume readiness without an operator retoggle: %+v", warm)
	}
}
