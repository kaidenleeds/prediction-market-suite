package server

import (
	"strings"
	"testing"
)

func TestR146SampleEvidenceBucketsStayAlignedAcrossSurfaces(t *testing.T) {
	cases := []struct {
		n     int
		key   string
		emoji string
	}{
		{0, "red", "🔴"}, {10, "red", "🔴"},
		{11, "orange", "🟠"}, {40, "orange", "🟠"},
		{41, "yellow", "🟡"}, {120, "yellow", "🟡"},
		{121, "green", "🟢"}, {500, "green", "🟢"},
		{501, "blue", "🔵"}, {1000, "blue", "🔵"},
		{1001, "purple", "🟣"}, {24836, "purple", "🟣"},
	}
	for _, tc := range cases {
		bucket := evidenceSampleBucket(tc.n)
		if bucket.Key != tc.key || bucket.Emoji != tc.emoji {
			t.Fatalf("n=%d central bucket=%+v want %s/%s", tc.n, bucket, tc.key, tc.emoji)
		}
		if got := briefSampleBucket(tc.n); got != tc.emoji {
			t.Fatalf("n=%d briefing bucket=%q want %q", tc.n, got, tc.emoji)
		}
		if key, emoji := plabSampleBucket(tc.n); key != tc.key || emoji != tc.emoji {
			t.Fatalf("n=%d combo bucket=%s/%s want %s/%s", tc.n, key, emoji, tc.key, tc.emoji)
		}
		if got := mlAccSampleBucket(tc.n); !strings.HasPrefix(got, tc.emoji+" ") {
			t.Fatalf("n=%d ML bucket=%q want prefix %q", tc.n, got, tc.emoji+" ")
		}
	}
}

func TestR146DashboardExplainsPurpleTopBucketWithoutBriefingLegend(t *testing.T) {
	for _, want := range []string{"🔴0–10", "🟠11–40", "🟡41–120", "🟢121–500", "🔵501–1,000", "🟣1,001+"} {
		if !strings.Contains(dashboardHTML, want) {
			t.Fatalf("dashboard sample legend missing %q", want)
		}
	}
	if !strings.Contains(dashboardHTML, "m<=1000?'🔵':'🟣'") {
		t.Fatal("dashboard JavaScript sample bucket thresholds drifted from Go")
	}
}
