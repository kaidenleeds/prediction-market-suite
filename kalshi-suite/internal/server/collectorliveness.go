package server

import (
	"net/http"
)

func (s *Server) handleCollectorLiveness(w http.ResponseWriter, r *http.Request) {
	report, err := s.store.CollectorLivenessReport(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, report)
}
