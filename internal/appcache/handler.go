package appcache

import (
	"encoding/json"
	"net/http"
)

// StatsHandler exposes cache metrics via GET /debug/cache/stats.
func StatsHandler(c *Cache) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(c.Stats())
	}
}
