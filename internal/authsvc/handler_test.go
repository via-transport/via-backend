package authsvc

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type captureUserStore struct {
	created      *User
	updated      *User
	usersByID    map[string]*User
	usersByEmail map[string]*User
}

func newCaptureUserStore(users ...*User) *captureUserStore {
	store := &captureUserStore{
		usersByID:    map[string]*User{},
		usersByEmail: map[string]*User{},
	}
	for _, user := range users {
		copy := cloneUser(user)
		store.usersByID[copy.ID] = copy
		store.usersByEmail[normalizeEmail(copy.Email)] = copy
	}
	return store
}

func cloneUser(user *User) *User {
	if user == nil {
		return nil
	}
	copy := *user
	return &copy
}

func (s *captureUserStore) CreateUser(_ context.Context, user *User) error {
	if s.usersByID == nil {
		s.usersByID = map[string]*User{}
	}
	if s.usersByEmail == nil {
		s.usersByEmail = map[string]*User{}
	}
	copy := cloneUser(user)
	s.created = copy
	s.usersByID[copy.ID] = copy
	s.usersByEmail[normalizeEmail(copy.Email)] = copy
	return nil
}

func (s *captureUserStore) GetUser(_ context.Context, userID string) (*User, error) {
	user, ok := s.usersByID[userID]
	if !ok {
		return nil, errors.New("user not found")
	}
	return cloneUser(user), nil
}

func (s *captureUserStore) GetUserByEmail(_ context.Context, email string) (*User, error) {
	user, ok := s.usersByEmail[normalizeEmail(email)]
	if !ok {
		return nil, errors.New("user not found")
	}
	return cloneUser(user), nil
}

func (s *captureUserStore) UpdateUser(_ context.Context, user *User) error {
	if s.usersByID == nil {
		s.usersByID = map[string]*User{}
	}
	if s.usersByEmail == nil {
		s.usersByEmail = map[string]*User{}
	}
	copy := cloneUser(user)
	s.updated = copy
	s.usersByID[copy.ID] = copy
	s.usersByEmail[normalizeEmail(copy.Email)] = copy
	return nil
}

func (s *captureUserStore) ListUsers(_ context.Context, _, _ string) ([]User, error) {
	return nil, errors.New("not implemented")
}

type fakeGoogleIDTokenVerifier struct {
	identity      *GoogleIdentity
	err           error
	token         string
	audiences     []string
	verificationN int
}

func (v *fakeGoogleIDTokenVerifier) Verify(_ context.Context, idToken string, audiences []string) (*GoogleIdentity, error) {
	v.token = idToken
	v.audiences = append([]string(nil), audiences...)
	v.verificationN++
	if v.err != nil {
		return nil, v.err
	}
	return cloneGoogleIdentity(v.identity), nil
}

func cloneGoogleIdentity(identity *GoogleIdentity) *GoogleIdentity {
	if identity == nil {
		return nil
	}
	copy := *identity
	return &copy
}

type captureOwnerRegistrar struct {
	user      *User
	fleetName string
}

func (r *captureOwnerRegistrar) RegisterOwner(_ context.Context, user *User, fleetName string) error {
	copy := *user
	r.user = &copy
	r.fleetName = fleetName
	return nil
}

type captureOwnerProvisioner struct {
	userID    string
	fleetName string
	user      *User
}

func (p *captureOwnerProvisioner) SetupOwnerFleet(_ context.Context, userID, fleetName string) (*User, error) {
	p.userID = userID
	p.fleetName = fleetName
	if p.user == nil {
		return nil, errors.New("not configured")
	}
	copy := *p.user
	return &copy, nil
}

func TestRegisterDriverIgnoresFleetID(t *testing.T) {
	t.Parallel()

	store := &captureUserStore{}
	handler := NewHandler(store, JWTConfig{
		Secret:     "test-secret",
		AccessTTL:  time.Hour,
		RefreshTTL: 24 * time.Hour,
		Issuer:     "test-suite",
	})

	req := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/auth/register",
		strings.NewReader(`{
			"email":"driver@example.com",
			"password":"hunter2",
			"display_name":"Driver Example",
			"role":"driver",
			"fleet_id":"fleet-should-not-stick"
		}`),
	)
	rec := httptest.NewRecorder()

	handler.Register(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected status %d, got %d", http.StatusCreated, rec.Code)
	}
	if store.created == nil {
		t.Fatal("expected CreateUser to be called")
	}
	if store.created.Role != "driver" {
		t.Fatalf("expected driver role, got %q", store.created.Role)
	}
	if store.created.FleetID != "" {
		t.Fatalf("expected empty fleet id for self-registered driver, got %q", store.created.FleetID)
	}

	var pair TokenPair
	if err := json.NewDecoder(rec.Body).Decode(&pair); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if pair.User.FleetID != "" {
		t.Fatalf("expected empty fleet id in response, got %q", pair.User.FleetID)
	}
}

func TestRegisterOwnerCreatesFleetScopedOwner(t *testing.T) {
	t.Parallel()

	store := &captureUserStore{}
	handler := NewHandler(store, JWTConfig{
		Secret:     "test-secret",
		AccessTTL:  time.Hour,
		RefreshTTL: 24 * time.Hour,
		Issuer:     "test-suite",
	})

	req := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/auth/register",
		strings.NewReader(`{
			"email":"owner@example.com",
			"password":"hunter2",
			"display_name":"Owner Example",
			"role":"owner"
		}`),
	)
	rec := httptest.NewRecorder()

	handler.Register(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected status %d, got %d", http.StatusCreated, rec.Code)
	}
	if store.created == nil {
		t.Fatal("expected CreateUser to be called")
	}
	if store.created.Role != "owner" {
		t.Fatalf("expected owner role, got %q", store.created.Role)
	}
	if store.created.FleetID != "" {
		t.Fatalf("expected owner registration to remain fleetless, got %q", store.created.FleetID)
	}

	var pair TokenPair
	if err := json.NewDecoder(rec.Body).Decode(&pair); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if pair.User.Role != "owner" {
		t.Fatalf("expected owner role in response, got %q", pair.User.Role)
	}
	if pair.User.FleetID != "" {
		t.Fatalf("expected empty fleet id in response, got %q", pair.User.FleetID)
	}
}

func TestSetupOwnerFleetReturnsUpdatedTokens(t *testing.T) {
	t.Parallel()

	provisioner := &captureOwnerProvisioner{
		user: &User{
			ID:          "owner-1",
			Email:       "owner@example.com",
			DisplayName: "Owner Example",
			Role:        "owner",
			FleetID:     "acme-school-transit",
			IsActive:    true,
		},
	}
	handler := NewHandler(&captureUserStore{}, JWTConfig{
		Secret:     "test-secret",
		AccessTTL:  time.Hour,
		RefreshTTL: 24 * time.Hour,
		Issuer:     "test-suite",
	})
	handler.SetOwnerProvisioner(provisioner)
	handler.SetUserIDFunc(func(_ *http.Request) string {
		return "owner-1"
	})

	req := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/auth/owner/setup-fleet",
		strings.NewReader(`{"fleet_name":"Acme School Transit"}`),
	)
	rec := httptest.NewRecorder()

	handler.SetupOwnerFleet(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, rec.Code)
	}
	if provisioner.userID != "owner-1" {
		t.Fatalf("expected user id owner-1, got %q", provisioner.userID)
	}
	if provisioner.fleetName != "Acme School Transit" {
		t.Fatalf("expected fleet name to be forwarded, got %q", provisioner.fleetName)
	}

	var pair TokenPair
	if err := json.NewDecoder(rec.Body).Decode(&pair); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if pair.User.FleetID != "acme-school-transit" {
		t.Fatalf("expected updated fleet id, got %q", pair.User.FleetID)
	}
}

func TestGoogleAuthCreatesOwnerByDefault(t *testing.T) {
	t.Parallel()

	store := newCaptureUserStore()
	verifier := &fakeGoogleIDTokenVerifier{
		identity: &GoogleIdentity{
			Subject:       "google-sub-1",
			Email:         "owner@example.com",
			DisplayName:   "Owner Example",
			Picture:       "https://example.com/avatar.png",
			EmailVerified: true,
			Audience:      "via-admin-web-client",
			Issuer:        "https://accounts.google.com",
		},
	}
	handler := NewHandler(store, JWTConfig{
		Secret:     "test-secret",
		AccessTTL:  time.Hour,
		RefreshTTL: 24 * time.Hour,
		Issuer:     "test-suite",
	})
	handler.SetGoogleIDTokenVerifier(verifier)
	handler.SetGoogleAudiences([]string{"via-admin-web-client"})

	req := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/auth/google",
		strings.NewReader(`{"id_token":"google-id-token"}`),
	)
	rec := httptest.NewRecorder()

	handler.GoogleAuth(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, rec.Code)
	}
	if store.created == nil {
		t.Fatal("expected CreateUser to be called")
	}
	if store.created.Role != "owner" {
		t.Fatalf("expected owner role, got %q", store.created.Role)
	}
	if store.created.GoogleSubject != "google-sub-1" {
		t.Fatalf("expected google subject to be stored, got %q", store.created.GoogleSubject)
	}
	if store.created.PhotoURL != "https://example.com/avatar.png" {
		t.Fatalf("expected google photo to be stored, got %q", store.created.PhotoURL)
	}
	if verifier.token != "google-id-token" {
		t.Fatalf("expected verifier to receive token, got %q", verifier.token)
	}
	if len(verifier.audiences) != 1 || verifier.audiences[0] != "via-admin-web-client" {
		t.Fatalf("expected configured google audience to be forwarded, got %#v", verifier.audiences)
	}

	var pair TokenPair
	if err := json.NewDecoder(rec.Body).Decode(&pair); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if pair.User.Email != "owner@example.com" {
		t.Fatalf("expected response email owner@example.com, got %q", pair.User.Email)
	}
	if pair.User.Role != "owner" {
		t.Fatalf("expected owner role in response, got %q", pair.User.Role)
	}
}

func TestGoogleAuthLinksExistingUser(t *testing.T) {
	t.Parallel()

	existing := &User{
		ID:           "owner-1",
		Email:        "owner@example.com",
		DisplayName:  "Owner Example",
		Role:         "owner",
		IsActive:     true,
		CreatedAt:    time.Now().UTC().Add(-time.Hour),
		UpdatedAt:    time.Now().UTC().Add(-time.Hour),
		LastLoginAt:  time.Now().UTC().Add(-2 * time.Hour),
		PasswordHash: "bcrypt-hash-placeholder",
	}
	store := newCaptureUserStore(existing)
	verifier := &fakeGoogleIDTokenVerifier{
		identity: &GoogleIdentity{
			Subject:       "google-sub-linked",
			Email:         "owner@example.com",
			EmailVerified: true,
			Issuer:        "accounts.google.com",
		},
	}
	handler := NewHandler(store, JWTConfig{
		Secret:     "test-secret",
		AccessTTL:  time.Hour,
		RefreshTTL: 24 * time.Hour,
		Issuer:     "test-suite",
	})
	handler.SetGoogleIDTokenVerifier(verifier)
	handler.SetGoogleAudiences([]string{"via-admin-web-client"})

	req := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/auth/google",
		strings.NewReader(`{"id_token":"google-id-token","role":"owner"}`),
	)
	rec := httptest.NewRecorder()

	handler.GoogleAuth(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, rec.Code)
	}
	if store.created != nil {
		t.Fatal("did not expect a new user to be created")
	}
	if store.updated == nil {
		t.Fatal("expected UpdateUser to be called")
	}
	if store.updated.GoogleSubject != "google-sub-linked" {
		t.Fatalf("expected google subject to be linked, got %q", store.updated.GoogleSubject)
	}
	if !store.updated.LastLoginAt.After(existing.LastLoginAt) {
		t.Fatal("expected last login timestamp to be refreshed")
	}
}

func TestGoogleAuthRejectsMismatchedLinkedGoogleAccount(t *testing.T) {
	t.Parallel()

	existing := &User{
		ID:            "owner-1",
		Email:         "owner@example.com",
		GoogleSubject: "google-sub-old",
		DisplayName:   "Owner Example",
		Role:          "owner",
		IsActive:      true,
	}
	store := newCaptureUserStore(existing)
	verifier := &fakeGoogleIDTokenVerifier{
		identity: &GoogleIdentity{
			Subject:       "google-sub-new",
			Email:         "owner@example.com",
			EmailVerified: true,
			Issuer:        "accounts.google.com",
		},
	}
	handler := NewHandler(store, JWTConfig{
		Secret:     "test-secret",
		AccessTTL:  time.Hour,
		RefreshTTL: 24 * time.Hour,
		Issuer:     "test-suite",
	})
	handler.SetGoogleIDTokenVerifier(verifier)
	handler.SetGoogleAudiences([]string{"via-admin-web-client"})

	req := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/auth/google",
		strings.NewReader(`{"id_token":"google-id-token"}`),
	)
	rec := httptest.NewRecorder()

	handler.GoogleAuth(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("expected status %d, got %d", http.StatusConflict, rec.Code)
	}
	if store.updated != nil {
		t.Fatal("did not expect existing user to be updated")
	}
}
