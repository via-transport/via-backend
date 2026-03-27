package authsvc

import (
	"encoding/json"
	"log"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"golang.org/x/crypto/bcrypt"
	"via-backend/internal/logx"
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
	googleVerifier   GoogleIDTokenVerifier
	googleAudiences  []string
}

// NewHandler creates auth handlers.
func NewHandler(store UserStore, jwtCfg JWTConfig) *Handler {
	return &Handler{
		store:          store,
		jwtCfg:         jwtCfg,
		googleVerifier: NewGoogleIDTokenVerifier(),
	}
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

// SetGoogleIDTokenVerifier overrides the Google token verifier.
func (h *Handler) SetGoogleIDTokenVerifier(verifier GoogleIDTokenVerifier) {
	h.googleVerifier = verifier
}

// SetGoogleAudiences sets the allowed Google OAuth client IDs for ID tokens.
func (h *Handler) SetGoogleAudiences(audiences []string) {
	h.googleAudiences = normalizeGoogleAudiences(audiences)
}

// Mount registers all auth routes on the mux.
func (h *Handler) Mount(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/v1/auth/register", h.Register)
	mux.HandleFunc("POST /api/v1/auth/login", h.Login)
	mux.HandleFunc("POST /api/v1/auth/google", h.GoogleAuth)
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
	logger := authLogger(r, "register")
	var req RegisterRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		logger.Warn("registration rejected", "reason", "invalid_json")
		writeJSON(w, http.StatusBadRequest, errBody("invalid json"))
		return
	}

	req.Email = strings.TrimSpace(strings.ToLower(req.Email))
	req.DisplayName = strings.TrimSpace(req.DisplayName)
	if req.Email == "" || req.Password == "" {
		logger.Warn("registration rejected", "reason", "missing_credentials")
		writeJSON(w, http.StatusBadRequest, errBody("email and password required"))
		return
	}
	if len(req.Password) < 6 {
		logger.Warn("registration rejected", "reason", "password_too_short", "email", logx.MaskEmail(req.Email))
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
		logger.Warn("registration rejected", "reason", "invalid_role", "role", role, "email", logx.MaskEmail(req.Email))
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
			logger.Warn("registration rejected", "reason", "email_exists", "email", logx.MaskEmail(req.Email))
			writeJSON(w, http.StatusConflict, errBody("email already registered"))
			return
		case strings.Contains(err.Error(), "fleet already registered"):
			logger.Warn("registration rejected", "reason", "fleet_exists", "fleet_id", req.FleetID)
			writeJSON(w, http.StatusConflict, errBody("fleet already registered"))
			return
		}
		log.Printf("[auth] create user: %v", err)
		logger.Error("registration failed", "email", logx.MaskEmail(req.Email), "error", err)
		writeJSON(w, http.StatusInternalServerError, errBody("registration failed"))
		return
	}

	pair, err := GenerateTokenPair(h.jwtCfg, user)
	if err != nil {
		log.Printf("[auth] generate tokens: %v", err)
		logger.Error("registration token generation failed", "email", logx.MaskEmail(req.Email), "error", err)
		writeJSON(w, http.StatusInternalServerError, errBody("token generation failed"))
		return
	}

	logger.Info("registration succeeded", "user_id", user.ID, "role", user.Role, "fleet_id", user.FleetID, "email", logx.MaskEmail(user.Email))
	writeJSON(w, http.StatusCreated, pair)
}

// SetupOwnerFleet handles POST /api/v1/auth/owner/setup-fleet.
func (h *Handler) SetupOwnerFleet(w http.ResponseWriter, r *http.Request) {
	logger := authLogger(r, "setup_owner_fleet")
	userID := h.userIDFromContext(r)
	if userID == "" {
		logger.Warn("owner fleet setup rejected", "reason", "missing_auth")
		writeJSON(w, http.StatusUnauthorized, errBody("authentication required"))
		return
	}
	if h.ownerProvisioner == nil {
		logger.Error("owner fleet setup unavailable", "user_id", userID)
		writeJSON(w, http.StatusInternalServerError, errBody("owner fleet setup unavailable"))
		return
	}

	var req SetupOwnerFleetRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		logger.Warn("owner fleet setup rejected", "reason", "invalid_json", "user_id", userID)
		writeJSON(w, http.StatusBadRequest, errBody("invalid json"))
		return
	}
	req.FleetName = strings.TrimSpace(req.FleetName)
	if req.FleetName == "" {
		logger.Warn("owner fleet setup rejected", "reason", "missing_fleet_name", "user_id", userID)
		writeJSON(w, http.StatusBadRequest, errBody("fleet_name is required"))
		return
	}

	user, err := h.ownerProvisioner.SetupOwnerFleet(r.Context(), userID, req.FleetName)
	if err != nil {
		switch {
		case strings.Contains(err.Error(), "owner already linked"):
			logger.Warn("owner fleet setup rejected", "reason", "owner_already_linked", "user_id", userID)
			writeJSON(w, http.StatusConflict, errBody("owner already linked to a fleet"))
			return
		case strings.Contains(err.Error(), "fleet already registered"):
			logger.Warn("owner fleet setup rejected", "reason", "fleet_exists", "user_id", userID, "fleet_name", req.FleetName)
			writeJSON(w, http.StatusConflict, errBody("fleet already registered"))
			return
		case strings.Contains(err.Error(), "only owners can create fleets"):
			logger.Warn("owner fleet setup rejected", "reason", "role_forbidden", "user_id", userID)
			writeJSON(w, http.StatusForbidden, errBody("only owners can create fleets"))
			return
		case strings.Contains(err.Error(), "user not found"):
			logger.Warn("owner fleet setup rejected", "reason", "user_not_found", "user_id", userID)
			writeJSON(w, http.StatusNotFound, errBody("user not found"))
			return
		}
		log.Printf("[auth] setup owner fleet: %v", err)
		logger.Error("owner fleet setup failed", "user_id", userID, "error", err)
		writeJSON(w, http.StatusInternalServerError, errBody("fleet setup failed"))
		return
	}

	pair, err := GenerateTokenPair(h.jwtCfg, user)
	if err != nil {
		log.Printf("[auth] generate tokens after fleet setup: %v", err)
		logger.Error("owner fleet setup token generation failed", "user_id", userID, "error", err)
		writeJSON(w, http.StatusInternalServerError, errBody("token generation failed"))
		return
	}

	logger.Info("owner fleet setup succeeded", "user_id", user.ID, "fleet_id", user.FleetID, "fleet_name", req.FleetName)
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
	logger := authLogger(r, "login")
	var req LoginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		logger.Warn("login rejected", "reason", "invalid_json")
		writeJSON(w, http.StatusBadRequest, errBody("invalid json"))
		return
	}

	req.Email = strings.TrimSpace(strings.ToLower(req.Email))
	if req.Email == "" || req.Password == "" {
		logger.Warn("login rejected", "reason", "missing_credentials")
		writeJSON(w, http.StatusBadRequest, errBody("email and password required"))
		return
	}

	user, err := h.store.GetUserByEmail(r.Context(), req.Email)
	if err != nil {
		logger.Warn("login rejected", "reason", "invalid_credentials", "email", logx.MaskEmail(req.Email))
		writeJSON(w, http.StatusUnauthorized, errBody("invalid credentials"))
		return
	}

	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(req.Password)); err != nil {
		logger.Warn("login rejected", "reason", "invalid_credentials", "email", logx.MaskEmail(req.Email))
		writeJSON(w, http.StatusUnauthorized, errBody("invalid credentials"))
		return
	}

	if !user.IsActive {
		logger.Warn("login rejected", "reason", "account_disabled", "user_id", user.ID, "email", logx.MaskEmail(user.Email))
		writeJSON(w, http.StatusForbidden, errBody("account disabled"))
		return
	}

	h.issueAuthPair(w, r, user)
}

// GoogleAuth handles POST /api/v1/auth/google.
func (h *Handler) GoogleAuth(w http.ResponseWriter, r *http.Request) {
	logger := authLogger(r, "google_auth")
	if h.googleVerifier == nil {
		logger.Error("google sign-in unavailable", "reason", "verifier_missing")
		writeJSON(w, http.StatusServiceUnavailable, errBody("google sign-in unavailable"))
		return
	}
	if len(h.googleAudiences) == 0 {
		logger.Error("google sign-in unavailable", "reason", "audiences_missing")
		writeJSON(w, http.StatusServiceUnavailable, errBody("google sign-in is not configured"))
		return
	}

	var req GoogleAuthRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		logger.Warn("google sign-in rejected", "reason", "invalid_json")
		writeJSON(w, http.StatusBadRequest, errBody("invalid json"))
		return
	}

	identity, err := h.googleVerifier.Verify(r.Context(), req.IDToken, h.googleAudiences)
	if err != nil {
		logger.Warn("google sign-in rejected", "reason", "invalid_google_token", "error", err)
		writeJSON(w, http.StatusUnauthorized, errBody("invalid google token"))
		return
	}
	if identity.Email == "" || !identity.EmailVerified {
		logger.Warn("google sign-in rejected", "reason", "email_not_verified")
		writeJSON(w, http.StatusUnauthorized, errBody("google account email must be verified"))
		return
	}
	if identity.Subject == "" {
		logger.Warn("google sign-in rejected", "reason", "subject_missing", "email", logx.MaskEmail(identity.Email))
		writeJSON(w, http.StatusUnauthorized, errBody("google account subject missing"))
		return
	}

	user, err := h.store.GetUserByEmail(r.Context(), identity.Email)
	switch {
	case err == nil:
		if user.GoogleSubject != "" && user.GoogleSubject != identity.Subject {
			logger.Warn("google sign-in rejected", "reason", "subject_mismatch", "email", logx.MaskEmail(identity.Email))
			writeJSON(w, http.StatusConflict, errBody("google account does not match the existing user"))
			return
		}
		if !user.IsActive {
			logger.Warn("google sign-in rejected", "reason", "account_disabled", "user_id", user.ID, "email", logx.MaskEmail(user.Email))
			writeJSON(w, http.StatusForbidden, errBody("account disabled"))
			return
		}

		user.GoogleSubject = identity.Subject
		if user.DisplayName == "" {
			user.DisplayName = firstNonEmpty(strings.TrimSpace(req.DisplayName), identity.DisplayName, strings.Split(identity.Email, "@")[0])
		}
		if user.PhotoURL == "" {
			user.PhotoURL = firstNonEmpty(strings.TrimSpace(req.PhotoURL), identity.Picture)
		}
		h.issueAuthPair(w, r, user)
		return

	case isNotFoundError(err):
		role := normalizeGoogleRegistrationRole(req.Role)
		if role == "" {
			logger.Warn("google sign-in rejected", "reason", "invalid_role", "role", req.Role, "email", logx.MaskEmail(identity.Email))
			writeJSON(w, http.StatusBadRequest, errBody("role must be passenger, driver, or owner"))
			return
		}

		now := time.Now().UTC()
		user = &User{
			ID:            uuid.New().String(),
			Email:         identity.Email,
			GoogleSubject: identity.Subject,
			DisplayName: firstNonEmpty(
				strings.TrimSpace(req.DisplayName),
				identity.DisplayName,
				strings.Split(identity.Email, "@")[0],
			),
			PhotoURL: firstNonEmpty(
				strings.TrimSpace(req.PhotoURL),
				identity.Picture,
			),
			Role:        role,
			IsActive:    true,
			CreatedAt:   now,
			UpdatedAt:   now,
			LastLoginAt: now,
		}

		if err := h.store.CreateUser(r.Context(), user); err != nil {
			switch {
			case strings.Contains(err.Error(), "email already registered"):
				logger.Warn("google sign-in rejected", "reason", "email_exists", "email", logx.MaskEmail(identity.Email))
				writeJSON(w, http.StatusConflict, errBody("email already registered"))
				return
			case strings.Contains(err.Error(), "google account already linked"):
				logger.Warn("google sign-in rejected", "reason", "google_subject_exists", "email", logx.MaskEmail(identity.Email))
				writeJSON(w, http.StatusConflict, errBody("google account is already linked to another user"))
				return
			case strings.Contains(err.Error(), "fleet already registered"):
				logger.Warn("google sign-in rejected", "reason", "fleet_exists")
				writeJSON(w, http.StatusConflict, errBody("fleet already registered"))
				return
			}
			log.Printf("[auth] create google user: %v", err)
			logger.Error("google sign-in failed", "email", logx.MaskEmail(identity.Email), "error", err)
			writeJSON(w, http.StatusInternalServerError, errBody("google sign-in failed"))
			return
		}

		pair, pairErr := GenerateTokenPair(h.jwtCfg, user)
		if pairErr != nil {
			logger.Error("google sign-in token generation failed", "email", logx.MaskEmail(identity.Email), "error", pairErr)
			writeJSON(w, http.StatusInternalServerError, errBody("token generation failed"))
			return
		}
		logger.Info("google sign-in succeeded", "user_id", user.ID, "role", user.Role, "fleet_id", user.FleetID, "email", logx.MaskEmail(user.Email))
		writeJSON(w, http.StatusOK, pair)
		return

	default:
		log.Printf("[auth] lookup google user by email: %v", err)
		logger.Error("google sign-in lookup failed", "email", logx.MaskEmail(identity.Email), "error", err)
		writeJSON(w, http.StatusInternalServerError, errBody("google sign-in failed"))
		return
	}
}

// Refresh handles POST /api/v1/auth/refresh.
func (h *Handler) Refresh(w http.ResponseWriter, r *http.Request) {
	logger := authLogger(r, "refresh")
	var req RefreshRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		logger.Warn("token refresh rejected", "reason", "invalid_json")
		writeJSON(w, http.StatusBadRequest, errBody("invalid json"))
		return
	}

	claims, err := ValidateToken(h.jwtCfg.Secret, req.RefreshToken)
	if err != nil {
		logger.Warn("token refresh rejected", "reason", "invalid_refresh_token", "error", err)
		writeJSON(w, http.StatusUnauthorized, errBody("invalid refresh token"))
		return
	}
	if claims.TokenType != "refresh" {
		logger.Warn("token refresh rejected", "reason", "wrong_token_type", "token_type", claims.TokenType)
		writeJSON(w, http.StatusUnauthorized, errBody("not a refresh token"))
		return
	}

	user, err := h.store.GetUser(r.Context(), claims.Sub)
	if err != nil {
		logger.Warn("token refresh rejected", "reason", "user_not_found", "user_id", claims.Sub)
		writeJSON(w, http.StatusUnauthorized, errBody("user not found"))
		return
	}

	pair, err := GenerateTokenPair(h.jwtCfg, user)
	if err != nil {
		logger.Error("token refresh failed", "user_id", user.ID, "error", err)
		writeJSON(w, http.StatusInternalServerError, errBody("token generation failed"))
		return
	}

	logger.Info("token refresh succeeded", "user_id", user.ID, "role", user.Role, "fleet_id", user.FleetID)
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
	logger := authLogger(r, "forgot_password")
	var req ForgotPasswordRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		logger.Warn("forgot password rejected", "reason", "invalid_json")
		writeJSON(w, http.StatusBadRequest, errBody("invalid json"))
		return
	}
	logger.Info("forgot password requested", "email", logx.MaskEmail(req.Email))
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

func (h *Handler) issueAuthPair(w http.ResponseWriter, r *http.Request, user *User) {
	logger := authLogger(r, "issue_auth_pair")
	now := time.Now().UTC()
	user.LastLoginAt = now
	if user.CreatedAt.IsZero() {
		user.CreatedAt = now
	}
	user.UpdatedAt = now
	if err := h.store.UpdateUser(r.Context(), user); err != nil {
		log.Printf("[auth] update user login timestamp: %v", err)
		logger.Error("failed to update login timestamp", "user_id", user.ID, "error", err)
		writeJSON(w, http.StatusInternalServerError, errBody("login failed"))
		return
	}

	pair, err := GenerateTokenPair(h.jwtCfg, user)
	if err != nil {
		logger.Error("failed to generate auth tokens", "user_id", user.ID, "error", err)
		writeJSON(w, http.StatusInternalServerError, errBody("token generation failed"))
		return
	}

	logger.Info("auth tokens issued", "user_id", user.ID, "role", user.Role, "fleet_id", user.FleetID, "email", logx.MaskEmail(user.Email))
	writeJSON(w, http.StatusOK, pair)
}

func authLogger(r *http.Request, action string) *slog.Logger {
	return logx.Logger(r.Context()).With(
		"component", "auth",
		"action", action,
		"path", r.URL.Path,
	)
}

func normalizeGoogleRegistrationRole(role string) string {
	role = strings.ToLower(strings.TrimSpace(role))
	if role == "" {
		return "owner"
	}
	switch role {
	case "passenger", "driver", "owner":
		return role
	default:
		return ""
	}
}

func isNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(strings.ToLower(err.Error()), "not found")
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			return value
		}
	}
	return ""
}
