package server

import "testing"

func TestForecastPersonaRouterCannotBypassUntouchedEvidence(t *testing.T) {
	e := forecastPersonaEvidence{ForecastSource: "book-model", ForecastVersion: "v1",
		EventDays: 30, EventClusters: 20, BrierLower: .01, CodeManifestHash: "code", DataManifestHash: "data"}
	if got, why := chooseForecastPersona("book-model", "v1", e); got != "" || why != "untouched_event_day_replication_missing" {
		t.Fatalf("unreplicated router selected %q: %s", got, why)
	}
	e.UntouchedReplication = true
	e.SphericalLower = .02
	if got, why := chooseForecastPersona("book-model", "v1", e); got != "spherical" || why != "" {
		t.Fatalf("replicated router got %q: %s", got, why)
	}
	if got, why := chooseForecastPersona("book-model", "v2", e); got != "" || why != "forecast_version_drift" {
		t.Fatalf("version drift selected %q: %s", got, why)
	}
}

func TestSphericalProperScoreVectorIsFiniteAndBounded(t *testing.T) {
	yes, no, qty, ok := properScoreVector("spherical", .65, .55)
	if !ok || qty <= 0 || qty > 2 || yes <= no {
		t.Fatalf("spherical vector yes=%v no=%v qty=%v ok=%v", yes, no, qty, ok)
	}
}
