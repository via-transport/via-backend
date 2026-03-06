package subsvc

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"via-backend/internal/authsvc"
	"via-backend/internal/opsvc"
)

type subsvcTestSubStore struct {
	items map[string]Subscription
}

func (s *subsvcTestSubStore) Put(_ context.Context, sub *Subscription) error {
	if s.items == nil {
		s.items = map[string]Subscription{}
	}
	copy := *sub
	s.items[sub.ID] = copy
	return nil
}

func (s *subsvcTestSubStore) Get(_ context.Context, userID, subID string) (*Subscription, error) {
	item, ok := s.items[subID]
	if !ok || item.UserID != userID {
		return nil, context.Canceled
	}
	copy := item
	return &copy, nil
}

func (s *subsvcTestSubStore) GetByID(_ context.Context, subID string) (*Subscription, error) {
	item, ok := s.items[subID]
	if !ok {
		return nil, context.Canceled
	}
	copy := item
	return &copy, nil
}

func (s *subsvcTestSubStore) ListForUser(_ context.Context, userID string) ([]Subscription, error) {
	var items []Subscription
	for _, item := range s.items {
		if strings.TrimSpace(item.UserID) == strings.TrimSpace(userID) {
			items = append(items, item)
		}
	}
	return items, nil
}

func (s *subsvcTestSubStore) ListForVehicle(_ context.Context, vehicleID string) ([]Subscription, error) {
	var items []Subscription
	for _, item := range s.items {
		if strings.TrimSpace(item.VehicleID) == strings.TrimSpace(vehicleID) {
			items = append(items, item)
		}
	}
	return items, nil
}

func (s *subsvcTestSubStore) ListByFleetStatus(_ context.Context, fleetID, status string) ([]Subscription, error) {
	var items []Subscription
	for _, item := range s.items {
		if fleetID != "" && strings.TrimSpace(item.FleetID) != strings.TrimSpace(fleetID) {
			continue
		}
		if status != "" && strings.TrimSpace(item.Status) != strings.TrimSpace(status) {
			continue
		}
		items = append(items, item)
	}
	return items, nil
}

func (s *subsvcTestSubStore) Delete(_ context.Context, _, subID string) error {
	delete(s.items, subID)
	return nil
}

type subsvcTestOpsStore struct {
	ops map[string]*opsvc.Operation
}

func (s *subsvcTestOpsStore) Put(_ context.Context, op *opsvc.Operation) error {
	if s.ops == nil {
		s.ops = map[string]*opsvc.Operation{}
	}
	copy := *op
	s.ops[op.ID] = &copy
	return nil
}

func (s *subsvcTestOpsStore) Get(_ context.Context, id string) (*opsvc.Operation, error) {
	op, ok := s.ops[id]
	if !ok {
		return nil, context.Canceled
	}
	copy := *op
	return &copy, nil
}

func (s *subsvcTestOpsStore) List(_ context.Context, _ opsvc.ListFilter) ([]opsvc.Operation, error) {
	var items []opsvc.Operation
	for _, op := range s.ops {
		items = append(items, *op)
	}
	return items, nil
}

func (s *subsvcTestOpsStore) FindByIdempotencyKey(_ context.Context, key string) (*opsvc.Operation, error) {
	for _, op := range s.ops {
		if op.IdempotencyKey == key {
			copy := *op
			return &copy, nil
		}
	}
	return nil, nil
}

type subsvcTestUserStore struct {
	users map[string]authsvc.User
}

func (s *subsvcTestUserStore) CreateUser(_ context.Context, user *authsvc.User) error {
	if s.users == nil {
		s.users = map[string]authsvc.User{}
	}
	s.users[user.ID] = *user
	return nil
}

func (s *subsvcTestUserStore) GetUser(_ context.Context, userID string) (*authsvc.User, error) {
	user, ok := s.users[userID]
	if !ok {
		return nil, context.Canceled
	}
	copy := user
	return &copy, nil
}

func (s *subsvcTestUserStore) GetUserByEmail(_ context.Context, email string) (*authsvc.User, error) {
	for _, user := range s.users {
		if strings.EqualFold(strings.TrimSpace(user.Email), strings.TrimSpace(email)) {
			copy := user
			return &copy, nil
		}
	}
	return nil, context.Canceled
}

func (s *subsvcTestUserStore) UpdateUser(_ context.Context, user *authsvc.User) error {
	if s.users == nil {
		s.users = map[string]authsvc.User{}
	}
	s.users[user.ID] = *user
	return nil
}

func (s *subsvcTestUserStore) ListUsers(_ context.Context, filterRole, filterFleet string) ([]authsvc.User, error) {
	var users []authsvc.User
	for _, user := range s.users {
		if filterRole != "" && strings.TrimSpace(user.Role) != strings.TrimSpace(filterRole) {
			continue
		}
		if filterFleet != "" && strings.TrimSpace(user.FleetID) != strings.TrimSpace(filterFleet) {
			continue
		}
		users = append(users, user)
	}
	return users, nil
}

func TestCreateJoinRequestRejectsMissingFleetID(t *testing.T) {
	handler := &Handler{
		store:    &subsvcTestSubStore{items: map[string]Subscription{}},
		opsStore: &subsvcTestOpsStore{ops: map[string]*opsvc.Operation{}},
	}

	payload := map[string]any{
		"user_id":    "passenger-1",
		"vehicle_id": "veh_001",
	}
	body, _ := json.Marshal(payload)

	req := httptest.NewRequest("POST", "/api/v1/join-requests", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	handler.CreateJoinRequest(rec, req)

	if rec.Code != 400 {
		t.Fatalf("expected HTTP 400 when fleet_id is missing, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "fleet_id required") {
		t.Fatalf("expected fleet_id validation error, got body=%s", rec.Body.String())
	}
}

func TestListJoinRequestsIncludesPassengerReviewData(t *testing.T) {
	joinedAt := time.Date(2026, 1, 4, 9, 0, 0, 0, time.UTC)
	subStore := &subsvcTestSubStore{
		items: map[string]Subscription{
			"join-1": {
				ID:        "join-1",
				UserID:    "passenger-1",
				VehicleID: "veh_001",
				FleetID:   "fleet-west",
				Status:    "pending",
				CreatedAt: joinedAt,
				UpdatedAt: joinedAt,
			},
		},
	}
	userStore := &subsvcTestUserStore{
		users: map[string]authsvc.User{
			"passenger-1": {
				ID:          "passenger-1",
				Email:       "passenger@example.com",
				DisplayName: "Ayesha Perera",
				Phone:       "+94770000000",
				Workplace:   "ACME Garments",
				Address:     "Kandy Road, Kurunegala",
				EmployeeNo:  "EMP-8899",
				CreatedAt:   joinedAt,
			},
		},
	}
	handler := &Handler{
		store:     subStore,
		userStore: userStore,
	}

	req := httptest.NewRequest("GET", "/api/v1/join-requests?fleet_id=fleet-west&status=pending", nil)
	rec := httptest.NewRecorder()
	handler.ListJoinRequests(rec, req)

	if rec.Code != 200 {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}
	var body []JoinRequest
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(body) != 1 {
		t.Fatalf("expected one join request, got %d", len(body))
	}
	item := body[0]
	if item.PassengerName != "Ayesha Perera" {
		t.Fatalf("expected passenger_name to be present, got %q", item.PassengerName)
	}
	if item.PassengerEmail != "passenger@example.com" {
		t.Fatalf("expected passenger_email to be present, got %q", item.PassengerEmail)
	}
	if item.PassengerWorkplace != "ACME Garments" {
		t.Fatalf("expected passenger_workplace to be present, got %q", item.PassengerWorkplace)
	}
	if item.PassengerAddress != "Kandy Road, Kurunegala" {
		t.Fatalf("expected passenger_address to be present, got %q", item.PassengerAddress)
	}
	if item.PassengerEmployeeNumber != "EMP-8899" {
		t.Fatalf("expected passenger_employee_number to be present, got %q", item.PassengerEmployeeNumber)
	}
	if item.PassengerJoinedAt == nil || !item.PassengerJoinedAt.Equal(joinedAt) {
		t.Fatalf("expected passenger_joined_at to equal %v, got %v", joinedAt, item.PassengerJoinedAt)
	}
}
