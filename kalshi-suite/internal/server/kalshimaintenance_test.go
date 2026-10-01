package server

import (
	"testing"
	"time"
)

func TestKalshiMaintenanceWindow(t *testing.T) {
	cases := []struct {
		at   string
		want bool
	}{
		{"2026-07-16T02:54:59-04:00", false},
		{"2026-07-16T02:55:00-04:00", true},
		{"2026-07-16T03:00:00-04:00", true},
		{"2026-07-16T04:59:59-04:00", true},
		{"2026-07-16T05:00:00-04:00", false},
		{"2026-07-17T03:00:00-04:00", false},
	}
	for _, tc := range cases {
		at, err := time.Parse(time.RFC3339, tc.at)
		if err != nil {
			t.Fatal(err)
		}
		got, _ := kalshiMaintenanceWindow(at)
		if got != tc.want {
			t.Errorf("%s got %v want %v", tc.at, got, tc.want)
		}
	}
}
