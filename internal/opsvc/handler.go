package opsvc

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
)

type Handler struct {
	store Store
}

func NewHandler(store Store) *Handler {
	return &Handler{store: store}
}

func (h *Handler) Store() Store {
	if h == nil {
		return nil
	}
	return h.store
}

func (h *Handler) Mount(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/v1/operations", h.List)
	mux.HandleFunc("GET /api/v1/operations/{id}", h.Get)
}

func (h *Handler) List(w http.ResponseWriter, r *http.Request) {
	filter := ListFilter{
		Limit:   parseLimit(r.URL.Query().Get("limit"), 20, 100),
		Type:    strings.TrimSpace(r.URL.Query().Get("type")),
		Status:  strings.TrimSpace(r.URL.Query().Get("status")),
		FleetID: strings.TrimSpace(r.URL.Query().Get("fleet_id")),
	}
	items, err := h.store.List(r.Context(), filter)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody("failed to load operations"))
		return
	}
	writeJSON(w, http.StatusOK, items)
}

func (h *Handler) Get(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSpace(r.PathValue("id"))
	if id == "" {
		writeJSON(w, http.StatusBadRequest, errBody("operation id required"))
		return
	}
	op, err := h.store.Get(r.Context(), id)
	if err != nil {
		writeJSON(w, http.StatusNotFound, errBody("operation not found"))
		return
	}
	writeJSON(w, http.StatusOK, op)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func errBody(msg string) map[string]string {
	return map[string]string{"error": msg}
}

func parseLimit(raw string, fallback, max int) int {
	if fallback <= 0 {
		fallback = 20
	}
	limit := fallback
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return limit
	}
	if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
		limit = parsed
	}
	if limit > max {
		return max
	}
	return limit
}
