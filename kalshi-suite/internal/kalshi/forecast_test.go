package kalshi

import (
	"net/url"
	"reflect"
	"testing"
)

func TestR135ForecastPercentileWireUsesExplodedFormArray(t *testing.T) {
	path, err := forecastPercentileHistoryPath("KXHIGHNY", "KXHIGHNY-26JUL11", []int{0, 5000, 9999}, 100, 200, 1)
	if err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(path)
	if err != nil {
		t.Fatal(err)
	}
	if u.Path != "/series/KXHIGHNY/events/KXHIGHNY-26JUL11/forecast_percentile_history" {
		t.Fatalf("path=%q", u.Path)
	}
	q := u.Query()
	if got := q["percentiles"]; !reflect.DeepEqual(got, []string{"0", "5000", "9999"}) {
		t.Fatalf("percentiles=%v; want repeated explode=true form keys", got)
	}
	if q.Get("start_ts") != "60" || q.Get("end_ts") != "180" || q.Get("period_interval") != "1" {
		t.Fatalf("query=%v", q)
	}
}

func TestForecastPercentileWireAlignsEverySupportedPeriod(t *testing.T) {
	for _, tc := range []struct {
		period             int
		wantStart, wantEnd string
	}{
		{0, "172900", "173000"},
		{1, "172860", "172980"},
		{60, "172800", "172800"},
		{1440, "172800", "172800"},
	} {
		path, err := forecastPercentileHistoryPath("S", "E", []int{5000}, 172903, 173003, tc.period)
		if err != nil {
			t.Fatal(err)
		}
		q, _ := url.Parse(path)
		if q.Query().Get("start_ts") != tc.wantStart || q.Query().Get("end_ts") != tc.wantEnd {
			t.Errorf("period %d query=%v", tc.period, q.Query())
		}
	}
}

func TestR135ForecastPercentileWireRejectsUnknownShapes(t *testing.T) {
	if _, err := forecastPercentileHistoryPath("S", "E", make([]int, 11), 1, 2, 1); err == nil {
		t.Fatal("more than ten percentiles must fail before the wire")
	}
	if _, err := forecastPercentileHistoryPath("S", "E", []int{5000, 5000}, 1, 2, 1); err == nil {
		t.Fatal("duplicate percentiles must fail before the wire")
	}
	if _, err := forecastPercentileHistoryPath("S", "E", []int{5000}, 1, 2, 15); err == nil {
		t.Fatal("unsupported period interval must fail before the wire")
	}
}
