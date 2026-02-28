// Package handler contains all HTTP and WebSocket handlers.
// Handlers are pure adapters: they decode HTTP, call a service, and encode
// the response. No business logic lives here.
package handler

import (
	"encoding/json"
	"log"
	"net/http"
)

// writeJSON encodes payload as JSON and writes it to w.
func writeJSON(w http.ResponseWriter, code int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		log.Printf("[handler] write json: %v", err)
	}
}

// writeError writes a JSON error response.
func writeError(w http.ResponseWriter, code int, message string) {
	writeJSON(w, code, map[string]string{"error": message})
}
