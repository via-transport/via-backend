package subsvc

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/google/uuid"

	"via-backend/internal/auth"
)

// Handler provides subscription REST endpoints.
type Handler struct {
	store *Store
}

// NewHandler creates a subscription handler.
func NewHandler(store *Store) *Handler {
	return &Handler{store: store}
}

// Mount registers subscription routes.
func (h *Handler) Mount(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/v1/subscriptions", h.List)
	mux.HandleFunc("POST /api/v1/subscriptions", h.Create)
	mux.HandleFunc("GET /api/v1/subscriptions/{id}", h.Get)
	mux.HandleFunc("PUT /api/v1/subscriptions/{id}", h.Update)
	mux.HandleFunc("DELETE /api/v1/subscriptions/{id}", h.Cancel)
	mux.HandleFunc("GET /api/v1/subscriptions/vehicle/{vehicleId}", h.ListForVehicle)
}

// List returns all subscriptions for a user.
func (h *Handler) List(w http.ResponseWriter, r *http.Request) {
	userID := userIDFromRequest(r)
	if userID == "" {
		writeJSON(w, http.StatusBadRequest, errBody("user_id required"))
		return
	}
	subs, err := h.store.ListForUser(r.Context(), userID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody(err.Error()))
		return
	}
	if subs == nil {
		subs = []Subscription{}
	}
	writeJSON(w, http.StatusOK, subs)
}

// Get returns a single subscription.
func (h *Handler) Get(w http.ResponseWriter, r *http.Request) {
	subID := r.PathValue("id")
	userID := userIDFromRequest(r)
	if userID == "" {
		writeJSON(w, http.StatusBadRequest, errBody("user_id required"))
		return
	}
	sub, err := h.store.Get(r.Context(), userID, subID)
	if err != nil {
		writeJSON(w, http.StatusNotFound, errBody("subscription not found"))
		return
	}
	writeJSON(w, http.StatusOK, sub)
}

// Create creates a new subscription.
func (h *Handler) Create(w http.ResponseWriter, r *http.Request) {
	var sub Subscription
	if err := json.NewDecoder(r.Body).Decode(&sub); err != nil {
		writeJSON(w, http.StatusBadRequest, errBody("invalid json"))
		return
	}
	// Auto-fill user_id from auth context if not provided in body.
	if sub.UserID == "" {
		sub.UserID = userIDFromRequest(r)
	}
	if sub.UserID == "" || sub.VehicleID == "" {
		writeJSON(w, http.StatusBadRequest, errBody("user_id and vehicle_id required"))
		return
	}
	if sub.ID == "" {
		sub.ID = uuid.New().String()
	}
	now := time.Now().UTC()
	sub.CreatedAt = now
	sub.UpdatedAt = now
	sub.Status = "active"
	if sub.Preferences == (SubPrefs{}) {
		sub.Preferences = SubPrefs{
			NotifyOnArrival: true,
			NotifyOnDelay:   true,
			NotifyOnEvent:   true,
		}
	}

	if err := h.store.Put(r.Context(), &sub); err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody("create failed"))
		return
	}
	writeJSON(w, http.StatusCreated, sub)
}

// Update modifies a subscription's preferences or status.
func (h *Handler) Update(w http.ResponseWriter, r *http.Request) {
	subID := r.PathValue("id")
	userID := userIDFromRequest(r)
	if userID == "" {
		writeJSON(w, http.StatusBadRequest, errBody("user_id required"))
		return
	}

	existing, err := h.store.Get(r.Context(), userID, subID)
	if err != nil {
		writeJSON(w, http.StatusNotFound, errBody("subscription not found"))
		return
	}

	var update Subscription
	if err := json.NewDecoder(r.Body).Decode(&update); err != nil {
		writeJSON(w, http.StatusBadRequest, errBody("invalid json"))
		return
	}
	if update.Status != "" {
		existing.Status = update.Status
	}
	// Only override preferences if provided (non-zero).
	if update.Preferences != (SubPrefs{}) {
		existing.Preferences = update.Preferences
	}
	existing.UpdatedAt = time.Now().UTC()

	if err := h.store.Put(r.Context(), existing); err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody("update failed"))
		return
	}
	writeJSON(w, http.StatusOK, existing)
}

// Cancel soft-deletes a subscription by setting status to "cancelled".
func (h *Handler) Cancel(w http.ResponseWriter, r *http.Request) {
	subID := r.PathValue("id")
	userID := userIDFromRequest(r)
	if userID == "" {
		writeJSON(w, http.StatusBadRequest, errBody("user_id required"))
		return
	}
	existing, err := h.store.Get(r.Context(), userID, subID)
	if err != nil {
		writeJSON(w, http.StatusNotFound, errBody("subscription not found"))
		return
	}
	existing.Status = "cancelled"
	existing.UpdatedAt = time.Now().UTC()
	if err := h.store.Put(r.Context(), existing); err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody("cancel failed"))
		return
	}
	writeJSON(w, http.StatusOK, existing)
}

// ListForVehicle returns all active subscribers for a vehicle.
func (h *Handler) ListForVehicle(w http.ResponseWriter, r *http.Request) {
	vehicleID := r.PathValue("vehicleId")
	subs, err := h.store.ListForVehicle(r.Context(), vehicleID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody(err.Error()))
		return
	}
	if subs == nil {
		subs = []Subscription{}
	}
	writeJSON(w, http.StatusOK, subs)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func userIDFromRequest(r *http.Request) string {
	if uid := r.URL.Query().Get("user_id"); uid != "" {
		return uid
	}
	if id := auth.IdentityFromContext(r.Context()); id.UserID != "" {
		return id.UserID
	}
	return ""
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func errBody(msg string) map[string]string {
	return map[string]string{"error": msg}
}
