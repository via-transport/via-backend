package requestsvc

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

	"via-backend/internal/fleetsvc"
)

type Handler struct {
	store      Store
	fleetStore fleetsvc.FleetStore
}

func NewHandler(store Store, fleetStore fleetsvc.FleetStore) *Handler {
	return &Handler{store: store, fleetStore: fleetStore}
}

func (h *Handler) Mount(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/v1/driver-requests", h.List)
	mux.HandleFunc("POST /api/v1/driver-requests", h.Create)
	mux.HandleFunc("POST /api/v1/driver-requests/{id}/approve", h.Approve)
	mux.HandleFunc("POST /api/v1/driver-requests/{id}/deny", h.Deny)
}

func (h *Handler) List(w http.ResponseWriter, r *http.Request) {
	fleetID := strings.TrimSpace(r.URL.Query().Get("fleet_id"))
	status := strings.TrimSpace(r.URL.Query().Get("status"))
	if status == "" {
		status = StatusPending
	}
	items, err := h.store.List(r.Context(), fleetID, status)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody(err.Error()))
		return
	}
	if items == nil {
		items = []DriverRequest{}
	}
	writeJSON(w, http.StatusOK, items)
}

func (h *Handler) Create(w http.ResponseWriter, r *http.Request) {
	var req DriverRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, errBody("invalid json"))
		return
	}
	req.UserID = strings.TrimSpace(req.UserID)
	req.FleetID = strings.TrimSpace(req.FleetID)
	req.FullName = strings.TrimSpace(req.FullName)
	req.Email = strings.TrimSpace(strings.ToLower(req.Email))
	req.Phone = strings.TrimSpace(req.Phone)
	req.Note = strings.TrimSpace(req.Note)
	if req.UserID == "" || req.FleetID == "" || req.FullName == "" || req.Email == "" {
		writeJSON(w, http.StatusBadRequest, errBody("user_id, fleet_id, full_name and email required"))
		return
	}

	if existing, err := h.store.FindPendingByUser(r.Context(), req.FleetID, req.UserID); err == nil && existing != nil {
		writeJSON(w, http.StatusOK, existing)
		return
	}

	if req.ID == "" {
		req.ID = uuid.New().String()
	}
	now := time.Now().UTC()
	req.Status = StatusPending
	req.CreatedAt = now
	req.UpdatedAt = now

	if err := h.store.Put(r.Context(), &req); err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody("create failed"))
		return
	}
	writeJSON(w, http.StatusCreated, req)
}

func (h *Handler) Approve(w http.ResponseWriter, r *http.Request) {
	reqID := strings.TrimSpace(r.PathValue("id"))
	req, err := h.store.Get(r.Context(), reqID)
	if err != nil {
		writeJSON(w, http.StatusNotFound, errBody("driver request not found"))
		return
	}

	now := time.Now().UTC()
	driver := &fleetsvc.Driver{
		ID:        req.UserID,
		Email:     req.Email,
		FullName:  req.FullName,
		Phone:     req.Phone,
		FleetID:   req.FleetID,
		IsActive:  true,
		CreatedAt: now,
		UpdatedAt: now,
	}

	if existing, err := h.fleetStore.GetDriver(r.Context(), req.FleetID, req.UserID); err == nil && existing != nil {
		driver = existing
		driver.Email = req.Email
		driver.FullName = req.FullName
		driver.Phone = req.Phone
		driver.IsActive = true
		driver.UpdatedAt = now
	}

	if err := h.fleetStore.PutDriver(r.Context(), driver); err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody("approve failed"))
		return
	}

	notice := &fleetsvc.DriverNotice{
		ID:        uuid.New().String(),
		Title:     "Access Approved",
		Message:   "Your driver access was approved. Await vehicle assignment from the owner.",
		DriverID:  req.UserID,
		FleetID:   req.FleetID,
		Priority:  "high",
		IsRead:    false,
		Timestamp: now,
	}
	_ = h.fleetStore.PutNotice(r.Context(), notice)

	req.Status = StatusApproved
	req.UpdatedAt = now
	if err := h.store.Put(r.Context(), req); err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody("approve failed"))
		return
	}

	writeJSON(w, http.StatusOK, req)
}

func (h *Handler) Deny(w http.ResponseWriter, r *http.Request) {
	reqID := strings.TrimSpace(r.PathValue("id"))
	req, err := h.store.Get(r.Context(), reqID)
	if err != nil {
		writeJSON(w, http.StatusNotFound, errBody("driver request not found"))
		return
	}
	req.Status = StatusDenied
	req.UpdatedAt = time.Now().UTC()
	if err := h.store.Put(r.Context(), req); err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody("deny failed"))
		return
	}
	writeJSON(w, http.StatusOK, req)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func errBody(msg string) map[string]string {
	return map[string]string{"error": msg}
}
