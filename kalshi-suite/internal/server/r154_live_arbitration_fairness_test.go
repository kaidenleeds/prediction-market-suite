package server

import (
	"testing"
	"time"
)

func TestR154WeakerSameEventResultWaitsForPendingStrongerPreflight(t *testing.T) {
	weak := liveMirrorPreflightResult{index: 1, why: ""}
	done := []bool{false, true}
	priority := []int{0, 1}
	events := []string{"KXEVENT", "KXEVENT"}
	if !liveMirrorPendingStrongerSibling(weak, done, priority, events) {
		t.Fatal("completed weaker candidate ignored its pending stronger same-event sibling")
	}
	done[0] = true
	if liveMirrorPendingStrongerSibling(weak, done, priority, events) {
		t.Fatal("completed stronger sibling still blocked the weaker result")
	}
	done[0] = false
	events[0] = "OTHER-EVENT"
	if liveMirrorPendingStrongerSibling(weak, done, priority, events) {
		t.Fatal("unrelated pending candidate delayed the ready result")
	}
	if liveMirrorRankSiblingGrace != 25*time.Millisecond {
		t.Fatalf("bounded arbitration sibling timing changed: %s",
			liveMirrorRankSiblingGrace)
	}
}
