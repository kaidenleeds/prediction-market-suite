package server

import (
	"reflect"
	"testing"
)

func TestPrioritySettlementWindowRotatesAndWraps(t *testing.T) {
	keys := []string{"d", "b", "a", "c"}
	if got, want := prioritySettlementWindow(keys, "b", 3), []string{"c", "d", "a"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("window after b = %v, want %v", got, want)
	}
	if got, want := prioritySettlementWindow(keys, "z", 2), []string{"a", "b"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("wrapped window = %v, want %v", got, want)
	}
}

func TestPrioritySettlementWindowBoundsAndEmpty(t *testing.T) {
	if got := prioritySettlementWindow(nil, "", 4); got != nil {
		t.Fatalf("empty = %v, want nil", got)
	}
	if got, want := prioritySettlementWindow([]string{"b", "a"}, "", 99), []string{"a", "b"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("bounded = %v, want %v", got, want)
	}
}
