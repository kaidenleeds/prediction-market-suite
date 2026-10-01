package server

import "strings"

// forecastPersonaEvidence is deliberately not populated from the current training report. It is a
// future immutable untouched-holdout certificate; without it the router must abstain.
type forecastPersonaEvidence struct {
	ForecastSource, ForecastVersion, CodeManifestHash, DataManifestHash string
	EventDays, EventClusters                                            int
	BrierLower, LogLower, SphericalLower                                float64
	UntouchedReplication                                                bool
}

func chooseForecastPersona(currentSource, currentVersion string, e forecastPersonaEvidence) (string, string) {
	if !e.UntouchedReplication || e.EventDays < 30 || e.EventClusters < 20 {
		return "", "untouched_event_day_replication_missing"
	}
	if strings.TrimSpace(e.CodeManifestHash) == "" || strings.TrimSpace(e.DataManifestHash) == "" {
		return "", "holdout_manifest_missing"
	}
	if e.ForecastSource != currentSource || e.ForecastVersion != currentVersion {
		return "", "forecast_version_drift"
	}
	best, lower := "", 0.0
	for transform, candidate := range map[string]float64{
		"brier": e.BrierLower, "log": e.LogLower, "spherical": e.SphericalLower,
	} {
		if candidate > lower {
			best, lower = transform, candidate
		}
	}
	if best == "" {
		return "", "no_transform_lower_bound_above_zero"
	}
	return best, ""
}
