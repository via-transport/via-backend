package authsvc

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"
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
	store            UserStore
	jwtCfg           JWTConfig
	userIDFromReq    UserIDFunc
	ownerProvisioner OwnerFleetProvisioner
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

// SetOwnerProvisioner sets the service used to link an owner account to its
// first fleet after registration.
func (h *Handler) SetOwnerProvisioner(provisioner OwnerFleetProvisioner) {
	h.ownerProvisioner = provisioner
}

// Mount registers all auth routes on the mux.
func (h *Handler) Mount(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/v1/auth/register", h.Register)
	mux.HandleFunc("POST /api/v1/auth/login", h.Login)
	mux.HandleFunc("POST /api/v1/auth/refresh", h.Refresh)
	mux.HandleFunc("POST /api/v1/auth/owner/setup-fleet", h.SetupOwnerFleet)
	mux.HandleFunc("GET /api/v1/auth/users", h.SearchUsers)
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

	req.FleetID = strings.TrimSpace(req.FleetID)
	role := strings.ToLower(strings.TrimSpace(req.Role))
	if role == "" {
		role = "passenger"
	}
	// Only allow self-registration as passenger, driver, or owner.
	if role != "passenger" && role != "driver" && role != "owner" {
		writeJSON(w, http.StatusBadRequest, errBody("role must be passenger, driver, or owner"))
		return
	}
	if role == "driver" {
		req.FleetID = ""
	}
	if role == "owner" {
		req.FleetID = ""
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

	createUser := func() error {
		return h.store.CreateUser(r.Context(), user)
	}

	if err := createUser(); err != nil {
		switch {
		case strings.Contains(err.Error(), "email already registered"):
			writeJSON(w, http.StatusConflict, errBody("email already registered"))
			return
		case strings.Contains(err.Error(), "fleet already registered"):
			writeJSON(w, http.StatusConflict, errBody("fleet already registered"))
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

// SetupOwnerFleet handles POST /api/v1/auth/owner/setup-fleet.
func (h *Handler) SetupOwnerFleet(w http.ResponseWriter, r *http.Request) {
	userID := h.userIDFromContext(r)
	if userID == "" {
		writeJSON(w, http.StatusUnauthorized, errBody("authentication required"))
		return
	}
	if h.ownerProvisioner == nil {
		writeJSON(w, http.StatusInternalServerError, errBody("owner fleet setup unavailable"))
		return
	}

	var req SetupOwnerFleetRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, errBody("invalid json"))
		return
	}
	req.FleetName = strings.TrimSpace(req.FleetName)
	if req.FleetName == "" {
		writeJSON(w, http.StatusBadRequest, errBody("fleet_name is required"))
		return
	}

	user, err := h.ownerProvisioner.SetupOwnerFleet(r.Context(), userID, req.FleetName)
	if err != nil {
		switch {
		case strings.Contains(err.Error(), "owner already linked"):
			writeJSON(w, http.StatusConflict, errBody("owner already linked to a fleet"))
			return
		case strings.Contains(err.Error(), "fleet already registered"):
			writeJSON(w, http.StatusConflict, errBody("fleet already registered"))
			return
		case strings.Contains(err.Error(), "only owners can create fleets"):
			writeJSON(w, http.StatusForbidden, errBody("only owners can create fleets"))
			return
		case strings.Contains(err.Error(), "user not found"):
			writeJSON(w, http.StatusNotFound, errBody("user not found"))
			return
		}
		log.Printf("[auth] setup owner fleet: %v", err)
		writeJSON(w, http.StatusInternalServerError, errBody("fleet setup failed"))
		return
	}

	pair, err := GenerateTokenPair(h.jwtCfg, user)
	if err != nil {
		log.Printf("[auth] generate tokens after fleet setup: %v", err)
		writeJSON(w, http.StatusInternalServerError, errBody("token generation failed"))
		return
	}

	writeJSON(w, http.StatusOK, pair)
}

// SearchUsers handles GET /api/v1/auth/users.
// This is used by the owner/admin app to look up already-registered drivers.
func (h *Handler) SearchUsers(w http.ResponseWriter, r *http.Request) {
	query := strings.TrimSpace(strings.ToLower(r.URL.Query().Get("q")))
	if len(query) < 2 {
		writeJSON(w, http.StatusOK, []UserPublic{})
		return
	}

	filterRole := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("role")))
	filterFleet := strings.TrimSpace(r.URL.Query().Get("fleet_id"))
	limit := 10
	if rawLimit := strings.TrimSpace(r.URL.Query().Get("limit")); rawLimit != "" {
		if parsed, err := strconv.Atoi(rawLimit); err == nil && parsed > 0 {
			if parsed > 50 {
				parsed = 50
			}
			limit = parsed
		}
	}

	users, err := h.store.ListUsers(r.Context(), filterRole, filterFleet)
	if err != nil {
		log.Printf("[auth] list users: %v", err)
		writeJSON(w, http.StatusInternalServerError, errBody("user lookup failed"))
		return
	}

	results := make([]UserPublic, 0, limit)
	for _, user := range users {
		if !matchesUserQuery(user, query) {
			continue
		}
		results = append(results, user.ToPublic())
		if len(results) >= limit {
			break
		}
	}

	writeJSON(w, http.StatusOK, results)
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

func matchesUserQuery(user User, query string) bool {
	if query == "" {
		return false
	}
	haystack := strings.ToLower(strings.Join([]string{
		user.ID,
		user.Email,
		user.DisplayName,
		user.Phone,
		user.FleetID,
	}, " "))
	return strings.Contains(haystack, query)
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
	if req.Workplace != "" {
		user.Workplace = req.Workplace
	}
	if req.Address != "" {
		user.Address = req.Address
	}
	if req.EmployeeNo != "" {
		user.EmployeeNo = req.EmployeeNo
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
