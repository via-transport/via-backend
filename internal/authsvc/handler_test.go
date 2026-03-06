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
	created *User
}

func (s *captureUserStore) CreateUser(_ context.Context, user *User) error {
	copy := *user
	s.created = &copy
	return nil
}

func (s *captureUserStore) GetUser(_ context.Context, _ string) (*User, error) {
	return nil, errors.New("not implemented")
}

func (s *captureUserStore) GetUserByEmail(_ context.Context, _ string) (*User, error) {
	return nil, errors.New("not implemented")
}

func (s *captureUserStore) UpdateUser(_ context.Context, _ *User) error {
	return errors.New("not implemented")
}

func (s *captureUserStore) ListUsers(_ context.Context, _, _ string) ([]User, error) {
	return nil, errors.New("not implemented")
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
