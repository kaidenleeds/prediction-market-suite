package server

import (
	"testing"
	"time"
)

func TestPolyUSMaintenanceJuly9Cutover(t *testing.T) {
	et, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		at   string
		want bool
	}{
		{"2026-07-09T01:54:59-04:00", false},
		{"2026-07-09T01:55:00-04:00", true},
		{"2026-07-09T02:00:00-04:00", true},
		{"2026-07-09T05:59:59-04:00", true},
		{"2026-07-09T06:00:00-04:00", false},
		{"2026-07-09T07:00:00-04:00", false}, // old 6-8 window must not survive
		{"2026-07-10T03:00:00-04:00", false},
	}
	for _, tc := range cases {
		at, err := time.Parse(time.RFC3339, tc.at)
		if err != nil {
			t.Fatal(err)
		}
		got, end := polyUSMaintenanceWindow(at.In(et))
		if got != tc.want {
			t.Errorf("%s got %v want %v (end=%s)", tc.at, got, tc.want, end)
		}
	}
}
