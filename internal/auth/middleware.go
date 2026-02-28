package auth

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"log"
	"net/http"
	"strings"

	"via-backend/internal/authsvc"
	viasentry "via-backend/internal/sentry"
)

// contextKey is an unexported type to prevent collisions.
type contextKey struct{}

// identityKey is the context key for Identity.
var identityKey = contextKey{}

// IdentityFromContext retrieves the authenticated Identity from context.
// Returns a zero Identity if none is set (unauthenticated).
func IdentityFromContext(ctx context.Context) Identity {
	id, _ := ctx.Value(identityKey).(Identity)
	return id
}

// ContextWithIdentity stores an Identity in the context.
func ContextWithIdentity(ctx context.Context, id Identity) context.Context {
	return context.WithValue(ctx, identityKey, id)
}

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

// MiddlewareConfig configures the auth middleware.
type MiddlewareConfig struct {
	// Enabled controls whether auth is enforced. When false, a default
	// service identity is injected (useful for local dev).
	Enabled bool

	// JWTSecret is the HMAC-SHA256 secret for self-hosted JWT validation.
	JWTSecret string

	// APIKeys is a map of API-key → Identity for machine-to-machine auth.
	// Checked via the X-API-Key header.
	APIKeys map[string]Identity

	// SkipPaths are path prefixes that bypass authentication entirely
	// (e.g. /healthz, /debug/).
	SkipPaths []string
}

// ---------------------------------------------------------------------------
// Middleware
// ---------------------------------------------------------------------------

// Middleware extracts identity from the request and stores it in context.
//
// Authentication order:
//  1. X-API-Key header → look up APIKeys map
//  2. Authorization: Bearer <jwt> → validate HMAC-SHA256 JWT signature
//  3. If nothing matches and auth is enabled → 401
func Middleware(cfg MiddlewareConfig) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Skip exempt paths.
			for _, prefix := range cfg.SkipPaths {
				if strings.HasPrefix(r.URL.Path, prefix) {
					next.ServeHTTP(w, r)
					return
				}
			}

			// --- When auth is disabled (local dev) ---
			if !cfg.Enabled {
				id := Identity{
					UserID: "dev",
					Role:   RoleService,
				}
				ctx := ContextWithIdentity(r.Context(), id)
				next.ServeHTTP(w, r.WithContext(ctx))
				return
			}

			// --- 1. API key ---
			if apiKey := r.Header.Get("X-API-Key"); apiKey != "" {
				if id, ok := cfg.APIKeys[apiKey]; ok {
					ctx := ContextWithIdentity(r.Context(), id)
					viasentry.SetUser(r, id.UserID, id.Email, string(id.Role))
					next.ServeHTTP(w, r.WithContext(ctx))
					return
				}
				writeAuthError(w, http.StatusUnauthorized, "invalid api key")
				return
			}

			// --- 2. Bearer JWT ---
			authHeader := r.Header.Get("Authorization")
			if strings.HasPrefix(authHeader, "Bearer ") {
				token := strings.TrimPrefix(authHeader, "Bearer ")
				id, err := validateJWT(cfg.JWTSecret, token)
				if err != nil {
					log.Printf("[auth] jwt validate: %v", err)
					writeAuthError(w, http.StatusUnauthorized, "invalid token")
					return
				}
				ctx := ContextWithIdentity(r.Context(), id)
				viasentry.SetUser(r, id.UserID, id.Email, string(id.Role))
				next.ServeHTTP(w, r.WithContext(ctx))
				return
			}

			writeAuthError(w, http.StatusUnauthorized, "authentication required")
		})
	}
}

// ---------------------------------------------------------------------------
// JWT validation using self-hosted HMAC-SHA256 tokens (replaces Firebase)
// ---------------------------------------------------------------------------

// validateJWT verifies the token signature and extracts identity fields.
func validateJWT(secret, token string) (Identity, error) {
	if secret == "" {
		// Fallback: if no secret configured, decode claims without verification
		// (backwards compat / dev mode – same as the old Firebase path).
		return decodeJWTClaimsUnsafe(token)
	}

	claims, err := authsvc.ValidateToken(secret, token)
	if err != nil {
		return Identity{}, err
	}

	role := Role(claims.Role)
	if role == "" {
		role = RolePassenger
	}
	return Identity{
		UserID:    claims.Sub,
		Email:     claims.Email,
		Role:      role,
		FleetID:   claims.FleetID,
		VehicleID: claims.VehicleID,
	}, nil
}

// decodeJWTClaimsUnsafe decodes JWT payload without signature verification.
// Only used as a fallback when no JWT secret is configured (dev mode).
func decodeJWTClaimsUnsafe(token string) (Identity, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return Identity{}, errInvalidToken
	}

	// Decode the payload (middle part)
	payload := parts[1]
	// Add padding if needed
	switch len(payload) % 4 {
	case 2:
		payload += "=="
	case 3:
		payload += "="
	}

	decoded, err := base64.URLEncoding.DecodeString(payload)
	if err != nil {
		// Try RawURLEncoding
		decoded, err = base64.RawURLEncoding.DecodeString(parts[1])
		if err != nil {
			return Identity{}, errInvalidToken
		}
	}

	var claims struct {
		Sub       string `json:"sub"`
		Email     string `json:"email"`
		Role      string `json:"role"`
		FleetID   string `json:"fleet_id"`
		VehicleID string `json:"vehicle_id"`
	}
	if err := json.Unmarshal(decoded, &claims); err != nil {
		return Identity{}, errInvalidToken
	}

	role := Role(claims.Role)
	if role == "" {
		role = RolePassenger
	}
	return Identity{
		UserID:    claims.Sub,
		Email:     claims.Email,
		Role:      role,
		FleetID:   claims.FleetID,
		VehicleID: claims.VehicleID,
	}, nil
}

var errInvalidToken = &authError{message: "invalid token"}

type authError struct{ message string }

func (e *authError) Error() string { return e.message }

func writeAuthError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
