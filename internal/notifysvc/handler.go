package notifysvc

import (
	"encoding/json"
	"log"
	"net/http"
	"sort"
	"time"

	"github.com/google/uuid"

	"via-backend/internal/auth"
	"via-backend/internal/messaging"
)

// Handler provides notification REST endpoints and WebSocket delivery.
type Handler struct {
	store  *Store
	broker *messaging.Broker
	hub    *Hub
}

// NewHandler creates a notification handler.
func NewHandler(store *Store, broker *messaging.Broker) *Handler {
	h := &Handler{
		store:  store,
		broker: broker,
		hub:    NewHub(),
	}
	go h.hub.Run()
	return h
}

// Mount registers notification routes on the mux.
func (h *Handler) Mount(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/v1/notifications", h.List)
	mux.HandleFunc("POST /api/v1/notifications", h.Create)
	mux.HandleFunc("PUT /api/v1/notifications/{id}/read", h.MarkRead)
	mux.HandleFunc("GET /api/v1/notifications/unread-count", h.UnreadCount)
	mux.HandleFunc("/ws/notifications", h.WSHandler)
}

// List returns notifications for the authenticated user.
func (h *Handler) List(w http.ResponseWriter, r *http.Request) {
	userID := r.URL.Query().Get("user_id")
	if userID == "" {
		userID = userIDFromRequest(r)
	}
	if userID == "" {
		writeJSON(w, http.StatusBadRequest, errBody("user_id required"))
		return
	}
	unreadOnly := r.URL.Query().Get("unread") == "true"
	notifs, err := h.store.ListForUser(r.Context(), userID, unreadOnly)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody(err.Error()))
		return
	}
	if notifs == nil {
		notifs = []Notification{}
	}
	sort.Slice(notifs, func(i, j int) bool {
		return notifs[i].CreatedAt.After(notifs[j].CreatedAt)
	})
	writeJSON(w, http.StatusOK, notifs)
}

// Create sends a notification to a user (also pushes via WebSocket).
func (h *Handler) Create(w http.ResponseWriter, r *http.Request) {
	var n Notification
	if err := json.NewDecoder(r.Body).Decode(&n); err != nil {
		writeJSON(w, http.StatusBadRequest, errBody("invalid json"))
		return
	}
	if n.UserID == "" || n.Title == "" {
		writeJSON(w, http.StatusBadRequest, errBody("user_id and title required"))
		return
	}
	if n.ID == "" {
		n.ID = uuid.New().String()
	}
	n.CreatedAt = time.Now().UTC()
	n.IsRead = false

	if err := h.store.Put(r.Context(), &n); err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody("store failed"))
		return
	}

	// Push via WebSocket hub.
	unread, _ := h.store.CountUnread(r.Context(), n.UserID)
	payload := NotificationPayload{
		Action:       "new",
		Notification: &n,
		UnreadCount:  unread,
	}
	data, _ := json.Marshal(payload)
	h.hub.SendToUser(n.UserID, data)

	// Also publish to NATS for cross-instance delivery.
	subject := "notify." + n.UserID
	if err := h.broker.Publish(subject, data); err != nil {
		log.Printf("[notify] NATS publish %s: %v", subject, err)
	}

	writeJSON(w, http.StatusCreated, n)
}

// MarkRead marks a notification as read.
func (h *Handler) MarkRead(w http.ResponseWriter, r *http.Request) {
	notifID := r.PathValue("id")
	userID := r.URL.Query().Get("user_id")
	if userID == "" {
		userID = userIDFromRequest(r)
	}
	if userID == "" {
		writeJSON(w, http.StatusBadRequest, errBody("user_id required"))
		return
	}

	n, err := h.store.Get(r.Context(), userID, notifID)
	if err != nil {
		writeJSON(w, http.StatusNotFound, errBody("notification not found"))
		return
	}
	n.IsRead = true
	n.ReadAt = time.Now().UTC()
	if err := h.store.Put(r.Context(), n); err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody("update failed"))
		return
	}

	unread, _ := h.store.CountUnread(r.Context(), userID)
	payload := NotificationPayload{
		Action:       "read",
		Notification: n,
		UnreadCount:  unread,
	}
	data, _ := json.Marshal(payload)
	h.hub.SendToUser(userID, data)

	writeJSON(w, http.StatusOK, n)
}

// UnreadCount returns the count of unread notifications.
func (h *Handler) UnreadCount(w http.ResponseWriter, r *http.Request) {
	userID := r.URL.Query().Get("user_id")
	if userID == "" {
		userID = userIDFromRequest(r)
	}
	if userID == "" {
		writeJSON(w, http.StatusBadRequest, errBody("user_id required"))
		return
	}
	count, err := h.store.CountUnread(r.Context(), userID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody(err.Error()))
		return
	}
	writeJSON(w, http.StatusOK, map[string]int{"unread_count": count})
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func userIDFromRequest(r *http.Request) string {
	// Try query param first, then auth identity from middleware context.
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
