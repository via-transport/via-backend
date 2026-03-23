package authsvc

import (
	"context"
	"errors"
	"fmt"
	"strings"

	googleidtoken "cloud.google.com/go/auth/credentials/idtoken"
)

type GoogleIdentity struct {
	Subject       string
	Email         string
	DisplayName   string
	Picture       string
	EmailVerified bool
	Audience      string
	Issuer        string
}

type GoogleIDTokenVerifier interface {
	Verify(ctx context.Context, idToken string, audiences []string) (*GoogleIdentity, error)
}

type liveGoogleIDTokenVerifier struct{}

func NewGoogleIDTokenVerifier() GoogleIDTokenVerifier {
	return liveGoogleIDTokenVerifier{}
}

func (liveGoogleIDTokenVerifier) Verify(ctx context.Context, idToken string, audiences []string) (*GoogleIdentity, error) {
	idToken = strings.TrimSpace(idToken)
	if idToken == "" {
		return nil, errors.New("google id token is required")
	}

	normalizedAudiences := normalizeGoogleAudiences(audiences)
	if len(normalizedAudiences) == 0 {
		return nil, errors.New("google auth client ids are not configured")
	}

	var lastErr error
	for _, audience := range normalizedAudiences {
		payload, err := googleidtoken.Validate(ctx, idToken, audience)
		if err != nil {
			lastErr = err
			continue
		}
		identity := googleIdentityFromPayload(payload)
		if !isGoogleIssuer(identity.Issuer) {
			return nil, fmt.Errorf("unexpected google token issuer %q", identity.Issuer)
		}
		return identity, nil
	}

	if lastErr == nil {
		lastErr = errors.New("google token validation failed")
	}
	return nil, fmt.Errorf("validate google id token: %w", lastErr)
}

func normalizeGoogleAudiences(audiences []string) []string {
	seen := make(map[string]struct{}, len(audiences))
	normalized := make([]string, 0, len(audiences))
	for _, audience := range audiences {
		audience = strings.TrimSpace(audience)
		if audience == "" {
			continue
		}
		if _, ok := seen[audience]; ok {
			continue
		}
		seen[audience] = struct{}{}
		normalized = append(normalized, audience)
	}
	return normalized
}

func googleIdentityFromPayload(payload *googleidtoken.Payload) *GoogleIdentity {
	claims := payload.Claims
	return &GoogleIdentity{
		Subject:       strings.TrimSpace(payload.Subject),
		Email:         strings.ToLower(strings.TrimSpace(stringClaim(claims, "email"))),
		DisplayName:   strings.TrimSpace(stringClaim(claims, "name")),
		Picture:       strings.TrimSpace(stringClaim(claims, "picture")),
		EmailVerified: boolClaim(claims, "email_verified"),
		Audience:      strings.TrimSpace(payload.Audience),
		Issuer:        strings.TrimSpace(payload.Issuer),
	}
}

func isGoogleIssuer(issuer string) bool {
	switch strings.TrimSpace(issuer) {
	case "accounts.google.com", "https://accounts.google.com":
		return true
	default:
		return false
	}
}

func stringClaim(claims map[string]interface{}, key string) string {
	if claims == nil {
		return ""
	}
	value, ok := claims[key]
	if !ok || value == nil {
		return ""
	}
	switch typed := value.(type) {
	case string:
		return typed
	default:
		return fmt.Sprint(typed)
	}
}

func boolClaim(claims map[string]interface{}, key string) bool {
	if claims == nil {
		return false
	}
	value, ok := claims[key]
	if !ok || value == nil {
		return false
	}
	switch typed := value.(type) {
	case bool:
		return typed
	case string:
		switch strings.ToLower(strings.TrimSpace(typed)) {
		case "1", "true", "yes":
			return true
		default:
			return false
		}
	default:
		return false
	}
}
