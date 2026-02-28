package authsvc

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// JWTConfig holds token generation parameters.
type JWTConfig struct {
	Secret     string
	AccessTTL  time.Duration
	RefreshTTL time.Duration
	Issuer     string
}

// jwtHeader is the fixed JOSE header.
var jwtHeader = base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))

// globalSecret is set once at startup for package-level token validation.
var globalSecret string

// SetGlobalSecret stores the JWT secret for ValidateTokenStatic.
// Must be called once at startup before any WebSocket auth.
func SetGlobalSecret(secret string) { globalSecret = secret }

// ValidateTokenStatic validates a token using the package-level secret.
// Used by WebSocket handlers that don't hold a JWTConfig reference.
func ValidateTokenStatic(token string) (Claims, error) {
	if globalSecret == "" {
		return Claims{}, errors.New("jwt secret not configured")
	}
	return ValidateToken(globalSecret, token)
}

// Claims is the JWT payload.
type Claims struct {
	Sub       string `json:"sub"`                  // user ID
	Email     string `json:"email"`
	Role      string `json:"role"`
	FleetID   string `json:"fleet_id,omitempty"`
	VehicleID string `json:"vehicle_id,omitempty"`
	Iss       string `json:"iss,omitempty"`
	Iat       int64  `json:"iat"`
	Exp       int64  `json:"exp"`
	TokenType string `json:"token_type"` // "access" | "refresh"
}

// GenerateTokenPair creates an access + refresh token pair.
func GenerateTokenPair(cfg JWTConfig, user *User) (TokenPair, error) {
	now := time.Now()
	accessExp := now.Add(cfg.AccessTTL)
	refreshExp := now.Add(cfg.RefreshTTL)

	accessToken, err := signJWT(cfg.Secret, Claims{
		Sub:       user.ID,
		Email:     user.Email,
		Role:      user.Role,
		FleetID:   user.FleetID,
		VehicleID: user.VehicleID,
		Iss:       cfg.Issuer,
		Iat:       now.Unix(),
		Exp:       accessExp.Unix(),
		TokenType: "access",
	})
	if err != nil {
		return TokenPair{}, fmt.Errorf("sign access token: %w", err)
	}

	refreshToken, err := signJWT(cfg.Secret, Claims{
		Sub:       user.ID,
		Email:     user.Email,
		Role:      user.Role,
		FleetID:   user.FleetID,
		Iss:       cfg.Issuer,
		Iat:       now.Unix(),
		Exp:       refreshExp.Unix(),
		TokenType: "refresh",
	})
	if err != nil {
		return TokenPair{}, fmt.Errorf("sign refresh token: %w", err)
	}

	return TokenPair{
		AccessToken:  accessToken,
		RefreshToken: refreshToken,
		ExpiresAt:    accessExp,
		User:         user.ToPublic(),
	}, nil
}

// ValidateToken verifies signature and expiry. Returns claims on success.
func ValidateToken(secret, token string) (Claims, error) {
	parts, err := splitJWT(token)
	if err != nil {
		return Claims{}, err
	}

	// Verify signature.
	sigInput := parts[0] + "." + parts[1]
	expectedSig := hmacSHA256(secret, sigInput)
	actualSig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil || !hmac.Equal(expectedSig, actualSig) {
		return Claims{}, errors.New("invalid token signature")
	}

	// Decode claims.
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return Claims{}, errors.New("invalid token payload")
	}

	var claims Claims
	if err := json.Unmarshal(payload, &claims); err != nil {
		return Claims{}, errors.New("invalid token claims")
	}

	// Check expiry.
	if time.Now().Unix() > claims.Exp {
		return Claims{}, errors.New("token expired")
	}

	return claims, nil
}

// signJWT creates a signed JWT string.
func signJWT(secret string, claims Claims) (string, error) {
	payload, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	encodedPayload := base64.RawURLEncoding.EncodeToString(payload)
	sigInput := jwtHeader + "." + encodedPayload
	sig := base64.RawURLEncoding.EncodeToString(hmacSHA256(secret, sigInput))
	return sigInput + "." + sig, nil
}

func hmacSHA256(secret, data string) []byte {
	h := hmac.New(sha256.New, []byte(secret))
	h.Write([]byte(data))
	return h.Sum(nil)
}

func splitJWT(token string) ([3]string, error) {
	var parts [3]string
	i, j := 0, 0
	for p := 0; p < 3; p++ {
		if p < 2 {
			for j = i; j < len(token) && token[j] != '.'; j++ {
			}
			if j >= len(token) && p < 2 {
				return parts, errors.New("invalid token format")
			}
			parts[p] = token[i:j]
			i = j + 1
		} else {
			parts[p] = token[i:]
		}
	}
	if parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return parts, errors.New("invalid token format")
	}
	return parts, nil
}
