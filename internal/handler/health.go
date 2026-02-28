package handler

import "net/http"

// Health returns 200 OK with a status payload. Useful for load-balancer
// probes and container orchestration.
func Health() http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	}
}
