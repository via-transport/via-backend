package authsvc

import (
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"golang.org/x/crypto/bcrypt"
)

// UserIDFunc extracts the user ID from a request context.
// This is injected to avoid an import cycle with the auth package.
type UserIDFunc func(r *http.Request) string

// Handler exposes auth HTTP endpoints.
type Handler struct {
	store        UserStore
	jwtCfg       JWTConfig
	userIDFromReq UserIDFunc
}

// NewHandler creates auth handlers.
func NewHandler(store UserStore, jwtCfg JWTConfig) *Handler {
	return &Handler{store: store, jwtCfg: jwtCfg}
}

// SetUserIDFunc sets the function used to extract user ID from request context.
// Must be called before serving requests.
func (h *Handler) SetUserIDFunc(fn UserIDFunc) {
	h.userIDFromReq = fn
}

// Mount registers all auth routes on the mux.
func (h *Handler) Mount(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/v1/auth/register", h.Register)
	mux.HandleFunc("POST /api/v1/auth/login", h.Login)
	mux.HandleFunc("POST /api/v1/auth/refresh", h.Refresh)
	mux.HandleFunc("GET /api/v1/auth/profile", h.GetProfile)
	mux.HandleFunc("PUT /api/v1/auth/profile", h.UpdateProfile)
	mux.HandleFunc("PUT /api/v1/auth/password", h.ChangePassword)
	mux.HandleFunc("POST /api/v1/auth/forgot-password", h.ForgotPassword)
}

// Register handles POST /api/v1/auth/register.
func (h *Handler) Register(w http.ResponseWriter, r *http.Request) {
	var req RegisterRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, errBody("invalid json"))
		return
	}

	req.Email = strings.TrimSpace(strings.ToLower(req.Email))
	req.DisplayName = strings.TrimSpace(req.DisplayName)
	if req.Email == "" || req.Password == "" {
		writeJSON(w, http.StatusBadRequest, errBody("email and password required"))
		return
	}
	if len(req.Password) < 6 {
		writeJSON(w, http.StatusBadRequest, errBody("password must be at least 6 characters"))
		return
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody("password hash failed"))
		return
	}

	role := req.Role
	if role == "" {
		role = "passenger"
	}
	// Only allow self-registration as passenger or driver.
	if role != "passenger" && role != "driver" {
		writeJSON(w, http.StatusBadRequest, errBody("role must be passenger or driver"))
		return
	}
	if req.DisplayName == "" {
		req.DisplayName = strings.Split(req.Email, "@")[0]
	}

	now := time.Now().UTC()
	user := &User{
		ID:           uuid.New().String(),
		Email:        req.Email,
		PasswordHash: string(hash),
		DisplayName:  req.DisplayName,
		Role:         role,
		FleetID:      req.FleetID,
		IsActive:     true,
		CreatedAt:    now,
		UpdatedAt:    now,
		LastLoginAt:  now,
	}

	if err := h.store.CreateUser(r.Context(), user); err != nil {
		if strings.Contains(err.Error(), "already registered") {
			writeJSON(w, http.StatusConflict, errBody("email already registered"))
			return
		}
		log.Printf("[auth] create user: %v", err)
		writeJSON(w, http.StatusInternalServerError, errBody("registration failed"))
		return
	}

	pair, err := GenerateTokenPair(h.jwtCfg, user)
	if err != nil {
		log.Printf("[auth] generate tokens: %v", err)
		writeJSON(w, http.StatusInternalServerError, errBody("token generation failed"))
		return
	}

	writeJSON(w, http.StatusCreated, pair)
}

// Login handles POST /api/v1/auth/login.
func (h *Handler) Login(w http.ResponseWriter, r *http.Request) {
	var req LoginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, errBody("invalid json"))
		return
	}

	req.Email = strings.TrimSpace(strings.ToLower(req.Email))
	if req.Email == "" || req.Password == "" {
		writeJSON(w, http.StatusBadRequest, errBody("email and password required"))
		return
	}

	user, err := h.store.GetUserByEmail(r.Context(), req.Email)
	if err != nil {
		writeJSON(w, http.StatusUnauthorized, errBody("invalid credentials"))
		return
	}

	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(req.Password)); err != nil {
		writeJSON(w, http.StatusUnauthorized, errBody("invalid credentials"))
		return
	}

	if !user.IsActive {
		writeJSON(w, http.StatusForbidden, errBody("account disabled"))
		return
	}

	// Update last login.
	user.LastLoginAt = time.Now().UTC()
	_ = h.store.UpdateUser(r.Context(), user)

	pair, err := GenerateTokenPair(h.jwtCfg, user)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody("token generation failed"))
		return
	}

	writeJSON(w, http.StatusOK, pair)
}

// Refresh handles POST /api/v1/auth/refresh.
func (h *Handler) Refresh(w http.ResponseWriter, r *http.Request) {
	var req RefreshRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, errBody("invalid json"))
		return
	}

	claims, err := ValidateToken(h.jwtCfg.Secret, req.RefreshToken)
	if err != nil {
		writeJSON(w, http.StatusUnauthorized, errBody("invalid refresh token"))
		return
	}
	if claims.TokenType != "refresh" {
		writeJSON(w, http.StatusUnauthorized, errBody("not a refresh token"))
		return
	}

	user, err := h.store.GetUser(r.Context(), claims.Sub)
	if err != nil {
		writeJSON(w, http.StatusUnauthorized, errBody("user not found"))
		return
	}

	pair, err := GenerateTokenPair(h.jwtCfg, user)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody("token generation failed"))
		return
	}

	writeJSON(w, http.StatusOK, pair)
}

// GetProfile handles GET /api/v1/auth/profile.
func (h *Handler) GetProfile(w http.ResponseWriter, r *http.Request) {
	userID := h.userIDFromContext(r)
	if userID == "" {
		writeJSON(w, http.StatusUnauthorized, errBody("authentication required"))
		return
	}

	user, err := h.store.GetUser(r.Context(), userID)
	if err != nil {
		writeJSON(w, http.StatusNotFound, errBody("user not found"))
		return
	}

	writeJSON(w, http.StatusOK, user.ToPublic())
}

// UpdateProfile handles PUT /api/v1/auth/profile.
func (h *Handler) UpdateProfile(w http.ResponseWriter, r *http.Request) {
	userID := h.userIDFromContext(r)
	if userID == "" {
		writeJSON(w, http.StatusUnauthorized, errBody("authentication required"))
		return
	}

	var req UpdateProfileRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, errBody("invalid json"))
		return
	}

	user, err := h.store.GetUser(r.Context(), userID)
	if err != nil {
		writeJSON(w, http.StatusNotFound, errBody("user not found"))
		return
	}

	if req.DisplayName != "" {
		user.DisplayName = req.DisplayName
	}
	if req.Phone != "" {
		user.Phone = req.Phone
	}
	if req.PhotoURL != "" {
		user.PhotoURL = req.PhotoURL
	}
	user.UpdatedAt = time.Now().UTC()

	if err := h.store.UpdateUser(r.Context(), user); err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody("update failed"))
		return
	}

	writeJSON(w, http.StatusOK, user.ToPublic())
}

// ChangePassword handles PUT /api/v1/auth/password.
func (h *Handler) ChangePassword(w http.ResponseWriter, r *http.Request) {
	userID := h.userIDFromContext(r)
	if userID == "" {
		writeJSON(w, http.StatusUnauthorized, errBody("authentication required"))
		return
	}

	var req ChangePasswordRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, errBody("invalid json"))
		return
	}

	if len(req.NewPassword) < 6 {
		writeJSON(w, http.StatusBadRequest, errBody("password must be at least 6 characters"))
		return
	}

	user, err := h.store.GetUser(r.Context(), userID)
	if err != nil {
		writeJSON(w, http.StatusNotFound, errBody("user not found"))
		return
	}

	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(req.OldPassword)); err != nil {
		writeJSON(w, http.StatusUnauthorized, errBody("current password incorrect"))
		return
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(req.NewPassword), bcrypt.DefaultCost)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody("password hash failed"))
		return
	}

	user.PasswordHash = string(hash)
	user.UpdatedAt = time.Now().UTC()

	if err := h.store.UpdateUser(r.Context(), user); err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody("update failed"))
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "password updated"})
}

// ForgotPassword handles POST /api/v1/auth/forgot-password.
// In a full implementation this would send a reset email. For now it
// acknowledges the request.
func (h *Handler) ForgotPassword(w http.ResponseWriter, r *http.Request) {
	var req ForgotPasswordRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, errBody("invalid json"))
		return
	}
	// Always return success to avoid email enumeration.
	writeJSON(w, http.StatusOK, map[string]string{
		"status":  "ok",
		"message": "if the email exists, a reset link will be sent",
	})
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// userIDFromContext extracts user ID from the auth identity in context.
func (h *Handler) userIDFromContext(r *http.Request) string {
	if h.userIDFromReq != nil {
		return h.userIDFromReq(r)
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
