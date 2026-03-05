package fleetsvc

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"via-backend/internal/auth"
	"via-backend/internal/authsvc"
	"via-backend/internal/notifysvc"
	"via-backend/internal/opsvc"
)

type handlerTestStore struct {
	vehicles map[string]*Vehicle
	drivers  map[string]*Driver
	notices  map[string]*DriverNotice
}

func newHandlerTestStore() *handlerTestStore {
	return &handlerTestStore{
		vehicles: map[string]*Vehicle{},
		drivers:  map[string]*Driver{},
		notices:  map[string]*DriverNotice{},
	}
}

func (s *handlerTestStore) PutVehicle(_ context.Context, v *Vehicle) error {
	s.vehicles[s.vehicleKey(v.FleetID, v.ID)] = cloneTestVehicle(v)
	return nil
}

func (s *handlerTestStore) GetVehicle(_ context.Context, fleetID, vehicleID string) (*Vehicle, error) {
	v, ok := s.vehicles[s.vehicleKey(fleetID, vehicleID)]
	if !ok {
		return nil, fmt.Errorf("vehicle not found")
	}
	return cloneTestVehicle(v), nil
}

func (s *handlerTestStore) GetVehicleByID(_ context.Context, vehicleID string) (*Vehicle, error) {
	for _, v := range s.vehicles {
		if v.ID == vehicleID {
			return cloneTestVehicle(v), nil
		}
	}
	return nil, fmt.Errorf("vehicle not found")
}

func (s *handlerTestStore) DeleteVehicle(_ context.Context, fleetID, vehicleID string) error {
	delete(s.vehicles, s.vehicleKey(fleetID, vehicleID))
	return nil
}

func (s *handlerTestStore) ListVehicles(_ context.Context, fleetID string) ([]Vehicle, error) {
	items := make([]Vehicle, 0, len(s.vehicles))
	for _, v := range s.vehicles {
		if fleetID != "" && v.FleetID != fleetID {
			continue
		}
		items = append(items, *cloneTestVehicle(v))
	}
	return items, nil
}

func (s *handlerTestStore) ListVehiclesForDriver(_ context.Context, fleetID, driverID string) ([]Vehicle, error) {
	items := make([]Vehicle, 0, len(s.vehicles))
	for _, v := range s.vehicles {
		if v.FleetID != fleetID || v.DriverID != driverID {
			continue
		}
		items = append(items, *cloneTestVehicle(v))
	}
	return items, nil
}

func (s *handlerTestStore) PutDriver(_ context.Context, d *Driver) error {
	s.drivers[s.driverKey(d.FleetID, d.ID)] = cloneTestDriver(d)
	return nil
}

func (s *handlerTestStore) GetDriver(_ context.Context, fleetID, driverID string) (*Driver, error) {
	d, ok := s.drivers[s.driverKey(fleetID, driverID)]
	if !ok {
		return nil, fmt.Errorf("driver not found")
	}
	return cloneTestDriver(d), nil
}

func (s *handlerTestStore) DeleteDriver(_ context.Context, fleetID, driverID string) error {
	delete(s.drivers, s.driverKey(fleetID, driverID))
	return nil
}

func (s *handlerTestStore) ListDrivers(_ context.Context, fleetID string) ([]Driver, error) {
	items := make([]Driver, 0, len(s.drivers))
	for _, d := range s.drivers {
		if fleetID != "" && d.FleetID != fleetID {
			continue
		}
		items = append(items, *cloneTestDriver(d))
	}
	return items, nil
}

func (s *handlerTestStore) PutEvent(_ context.Context, _ *SpecialEvent) error {
	return nil
}

func (s *handlerTestStore) GetEvent(_ context.Context, _ string) (*SpecialEvent, error) {
	return nil, fmt.Errorf("event not found")
}

func (s *handlerTestStore) ListEvents(_ context.Context, _, _ string) ([]SpecialEvent, error) {
	return nil, nil
}

func (s *handlerTestStore) PutNotice(_ context.Context, n *DriverNotice) error {
	s.notices[n.ID] = cloneTestNotice(n)
	return nil
}

func (s *handlerTestStore) GetNotice(_ context.Context, noticeID string) (*DriverNotice, error) {
	n, ok := s.notices[noticeID]
	if !ok {
		return nil, fmt.Errorf("notice not found")
	}
	return cloneTestNotice(n), nil
}

func (s *handlerTestStore) ListNotices(_ context.Context, fleetID, vehicleID, driverID string) ([]DriverNotice, error) {
	items := make([]DriverNotice, 0, len(s.notices))
	for _, n := range s.notices {
		if fleetID != "" && n.FleetID != fleetID {
			continue
		}
		if vehicleID != "" && n.VehicleID != vehicleID {
			continue
		}
		if driverID != "" && n.DriverID != driverID {
			continue
		}
		items = append(items, *cloneTestNotice(n))
	}
	return items, nil
}

func (s *handlerTestStore) vehicleKey(fleetID, vehicleID string) string {
	return fleetID + "/" + vehicleID
}

func (s *handlerTestStore) driverKey(fleetID, driverID string) string {
	return fleetID + "/" + driverID
}

func cloneTestVehicle(v *Vehicle) *Vehicle {
	if v == nil {
		return nil
	}
	copy := *v
	if v.CurrentLocation != nil {
		location := *v.CurrentLocation
		copy.CurrentLocation = &location
	}
	return &copy
}

func cloneTestDriver(d *Driver) *Driver {
	if d == nil {
		return nil
	}
	copy := *d
	if d.AssignedVehicleIDs != nil {
		copy.AssignedVehicleIDs = append([]string(nil), d.AssignedVehicleIDs...)
	}
	return &copy
}

func cloneTestNotice(n *DriverNotice) *DriverNotice {
	if n == nil {
		return nil
	}
	copy := *n
	return &copy
}

type handlerTestOperationStore struct {
	operations map[string]*opsvc.Operation
}

type handlerTestNotifStore struct {
	items []*notifysvc.Notification
}

func (s *handlerTestNotifStore) Put(_ context.Context, n *notifysvc.Notification) error {
	if n == nil {
		return nil
	}
	copy := *n
	if n.Data != nil {
		copy.Data = make(map[string]string, len(n.Data))
		for key, value := range n.Data {
			copy.Data[key] = value
		}
	}
	s.items = append(s.items, &copy)
	return nil
}

func (s *handlerTestNotifStore) Get(_ context.Context, userID, notifID string) (*notifysvc.Notification, error) {
	for _, item := range s.items {
		if item.UserID == userID && item.ID == notifID {
			copy := *item
			return &copy, nil
		}
	}
	return nil, fmt.Errorf("notification not found")
}

func (s *handlerTestNotifStore) ListForUser(_ context.Context, userID string, unreadOnly bool) ([]notifysvc.Notification, error) {
	items := make([]notifysvc.Notification, 0, len(s.items))
	for _, item := range s.items {
		if item.UserID != userID {
			continue
		}
		if unreadOnly && item.IsRead {
			continue
		}
		copy := *item
		items = append(items, copy)
	}
	return items, nil
}

func (s *handlerTestNotifStore) ListForFleet(_ context.Context, fleetID string, unreadOnly bool) ([]notifysvc.Notification, error) {
	items := make([]notifysvc.Notification, 0, len(s.items))
	for _, item := range s.items {
		if item.FleetID != fleetID {
			continue
		}
		if unreadOnly && item.IsRead {
			continue
		}
		copy := *item
		items = append(items, copy)
	}
	return items, nil
}

func (s *handlerTestNotifStore) CountUnread(_ context.Context, userID string) (int, error) {
	count := 0
	for _, item := range s.items {
		if item.UserID == userID && !item.IsRead {
			count++
		}
	}
	return count, nil
}

func (s *handlerTestNotifStore) Delete(_ context.Context, userID, notifID string) error {
	filtered := s.items[:0]
	for _, item := range s.items {
		if item.UserID == userID && item.ID == notifID {
			continue
		}
		filtered = append(filtered, item)
	}
	s.items = filtered
	return nil
}

type handlerTestUserStore struct {
	byEmail map[string]*authsvc.User
}

func (s *handlerTestUserStore) CreateUser(_ context.Context, user *authsvc.User) error {
	if s.byEmail == nil {
		s.byEmail = map[string]*authsvc.User{}
	}
	if user != nil {
		copy := *user
		s.byEmail[strings.ToLower(strings.TrimSpace(user.Email))] = &copy
	}
	return nil
}

func (s *handlerTestUserStore) GetUser(_ context.Context, userID string) (*authsvc.User, error) {
	for _, user := range s.byEmail {
		if user != nil && user.ID == userID {
			copy := *user
			return &copy, nil
		}
	}
	return nil, fmt.Errorf("user not found")
}

func (s *handlerTestUserStore) GetUserByEmail(_ context.Context, email string) (*authsvc.User, error) {
	user, ok := s.byEmail[strings.ToLower(strings.TrimSpace(email))]
	if !ok || user == nil {
		return nil, fmt.Errorf("user not found")
	}
	copy := *user
	return &copy, nil
}

func (s *handlerTestUserStore) UpdateUser(_ context.Context, user *authsvc.User) error {
	if user == nil {
		return nil
	}
	if s.byEmail == nil {
		s.byEmail = map[string]*authsvc.User{}
	}
	copy := *user
	s.byEmail[strings.ToLower(strings.TrimSpace(user.Email))] = &copy
	return nil
}

func (s *handlerTestUserStore) ListUsers(_ context.Context, filterRole, filterFleet string) ([]authsvc.User, error) {
	items := make([]authsvc.User, 0, len(s.byEmail))
	for _, user := range s.byEmail {
		if user == nil {
			continue
		}
		if filterRole != "" && !strings.EqualFold(user.Role, filterRole) {
			continue
		}
		if filterFleet != "" && !strings.EqualFold(user.FleetID, filterFleet) {
			continue
		}
		items = append(items, *user)
	}
	return items, nil
}

func newHandlerTestOperationStore() *handlerTestOperationStore {
	return &handlerTestOperationStore{
		operations: map[string]*opsvc.Operation{},
	}
}

func (s *handlerTestOperationStore) Put(_ context.Context, op *opsvc.Operation) error {
	copy := *op
	s.operations[op.ID] = &copy
	return nil
}

func (s *handlerTestOperationStore) Get(_ context.Context, id string) (*opsvc.Operation, error) {
	op, ok := s.operations[id]
	if !ok {
		return nil, fmt.Errorf("operation not found")
	}
	copy := *op
	return &copy, nil
}

func (s *handlerTestOperationStore) List(_ context.Context, _ opsvc.ListFilter) ([]opsvc.Operation, error) {
	items := make([]opsvc.Operation, 0, len(s.operations))
	for _, op := range s.operations {
		items = append(items, *op)
	}
	return items, nil
}

func (s *handlerTestOperationStore) FindByIdempotencyKey(_ context.Context, key string) (*opsvc.Operation, error) {
	for _, op := range s.operations {
		if op.IdempotencyKey == key {
			copy := *op
			return &copy, nil
		}
	}
	return nil, fmt.Errorf("operation not found")
}

func TestCreateVehicleAllowsMissingRoute(t *testing.T) {
	t.Parallel()

	opsStore := newHandlerTestOperationStore()
	handler := &Handler{ops: opsStore}

	req := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/vehicles",
		strings.NewReader(`{
			"fleet_id":"fleet-1",
			"registration_number":"BUS-1001",
			"type":"bus",
			"service_type":"school_transport"
		}`),
	)
	rec := httptest.NewRecorder()

	handler.CreateVehicle(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d", http.StatusAccepted, rec.Code)
	}
	if len(opsStore.operations) != 1 {
		t.Fatalf("expected queued operation, got %d", len(opsStore.operations))
	}
	for _, op := range opsStore.operations {
		if op.FleetID != "fleet-1" {
			t.Fatalf("expected queued operation fleet to be fleet-1, got %q", op.FleetID)
		}
	}
}

func TestApplyCreateVehicleDefaultsNicknameFromRegistration(t *testing.T) {
	t.Parallel()

	store := newHandlerTestStore()
	handler := &Handler{store: store}

	vehicle, err := handler.applyCreateVehicle(context.Background(), Vehicle{
		FleetID:            "fleet-1",
		RegistrationNumber: "ND-4521",
		Type:               "bus",
	})
	if err != nil {
		t.Fatalf("applyCreateVehicle returned error: %v", err)
	}
	if vehicle.Nickname != "ND-4521" {
		t.Fatalf("expected nickname to default from registration, got %q", vehicle.Nickname)
	}

	stored, err := store.GetVehicle(context.Background(), "fleet-1", vehicle.ID)
	if err != nil {
		t.Fatalf("GetVehicle returned error: %v", err)
	}
	if stored.Nickname != "ND-4521" {
		t.Fatalf("expected stored nickname to default from registration, got %q", stored.Nickname)
	}
}

func TestListVehiclesRedactsRegistrationForUnapprovedDriver(t *testing.T) {
	t.Parallel()

	store := newHandlerTestStore()
	now := time.Now().UTC()
	store.vehicles[store.vehicleKey("fleet-1", "veh-1")] = &Vehicle{
		ID:                 "veh-1",
		FleetID:            "fleet-1",
		RegistrationNumber: "ND-4521",
		Nickname:           "Morning Shuttle",
		Type:               "bus",
		ServiceType:        "school_transport",
		IsActive:           true,
		Status:             "on_time",
		LastUpdated:        now,
		CreatedAt:          now,
	}

	handler := &Handler{store: store}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/vehicles?fleet_id=fleet-1", nil)
	req = req.WithContext(auth.ContextWithIdentity(req.Context(), auth.Identity{
		UserID: "driver-pending",
		Role:   auth.RoleDriver,
	}))
	rec := httptest.NewRecorder()

	handler.ListVehicles(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, rec.Code)
	}

	var vehicles []Vehicle
	if err := json.NewDecoder(rec.Body).Decode(&vehicles); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(vehicles) != 1 {
		t.Fatalf("expected 1 redacted vehicle, got %d", len(vehicles))
	}
	if vehicles[0].RegistrationNumber != "" {
		t.Fatalf("expected registration number to be redacted, got %q", vehicles[0].RegistrationNumber)
	}
	if vehicles[0].Nickname != "Morning Shuttle" {
		t.Fatalf("expected nickname to remain visible, got %q", vehicles[0].Nickname)
	}
}

func TestListVehiclesKeepsRegistrationForApprovedDriver(t *testing.T) {
	t.Parallel()

	store := newHandlerTestStore()
	now := time.Now().UTC()
	store.vehicles[store.vehicleKey("fleet-1", "veh-1")] = &Vehicle{
		ID:                 "veh-1",
		FleetID:            "fleet-1",
		RegistrationNumber: "ND-4521",
		Nickname:           "Morning Shuttle",
		Type:               "bus",
		ServiceType:        "school_transport",
		IsActive:           true,
		Status:             "on_time",
		LastUpdated:        now,
		CreatedAt:          now,
	}
	store.drivers[store.driverKey("fleet-1", "driver-approved")] = &Driver{
		ID:        "driver-approved",
		FleetID:   "fleet-1",
		FullName:  "Approved Driver",
		IsActive:  true,
		CreatedAt: now,
		UpdatedAt: now,
	}

	handler := &Handler{store: store}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/vehicles?fleet_id=fleet-1", nil)
	req = req.WithContext(auth.ContextWithIdentity(req.Context(), auth.Identity{
		UserID: "driver-approved",
		Role:   auth.RoleDriver,
	}))
	rec := httptest.NewRecorder()

	handler.ListVehicles(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, rec.Code)
	}

	var vehicles []Vehicle
	if err := json.NewDecoder(rec.Body).Decode(&vehicles); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(vehicles) != 1 {
		t.Fatalf("expected 1 vehicle, got %d", len(vehicles))
	}
	if vehicles[0].RegistrationNumber != "ND-4521" {
		t.Fatalf("expected registration number to remain visible, got %q", vehicles[0].RegistrationNumber)
	}
}

func TestListVehiclesRejectsEmptyDriverFilter(t *testing.T) {
	t.Parallel()

	store := newHandlerTestStore()
	now := time.Now().UTC()
	store.vehicles[store.vehicleKey("fleet-1", "veh-1")] = &Vehicle{
		ID:                 "veh-1",
		FleetID:            "fleet-1",
		RegistrationNumber: "ND-4521",
		Nickname:           "Morning Shuttle",
		Type:               "bus",
		ServiceType:        "school_transport",
		IsActive:           true,
		Status:             "on_time",
		LastUpdated:        now,
		CreatedAt:          now,
	}

	handler := &Handler{store: store}
	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v1/vehicles?fleet_id=fleet-1&driver_id=",
		nil,
	)
	rec := httptest.NewRecorder()

	handler.ListVehicles(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, rec.Code)
	}
}

func TestApplyAssignDriverReassignsExistingVehicle(t *testing.T) {
	t.Parallel()

	store := newHandlerTestStore()
	notifStore := &handlerTestNotifStore{}
	now := time.Now().UTC()
	store.drivers[store.driverKey("fleet-1", "driver-1")] = &Driver{
		ID:                 "driver-1",
		FleetID:            "fleet-1",
		FullName:           "Driver One",
		Phone:              "+94 77 111 1111",
		AssignedVehicleIDs: []string{"vehicle-old"},
		IsActive:           true,
		CreatedAt:          now,
		UpdatedAt:          now,
	}
	store.vehicles[store.vehicleKey("fleet-1", "vehicle-old")] = &Vehicle{
		ID:                 "vehicle-old",
		FleetID:            "fleet-1",
		RegistrationNumber: "OLD-1001",
		DriverID:           "driver-1",
		DriverName:         "Driver One",
		DriverPhone:        "+94 77 111 1111",
		LastUpdated:        now,
		CreatedAt:          now,
	}
	store.vehicles[store.vehicleKey("fleet-1", "vehicle-new")] = &Vehicle{
		ID:                 "vehicle-new",
		FleetID:            "fleet-1",
		RegistrationNumber: "NEW-2002",
		LastUpdated:        now,
		CreatedAt:          now,
	}

	handler := &Handler{store: store, notifyStore: notifStore}

	updatedVehicle, err := handler.applyAssignDriver(
		context.Background(),
		"vehicle-new",
		assignDriverPayload{
			FleetID:  "fleet-1",
			DriverID: "driver-1",
		},
	)
	if err != nil {
		t.Fatalf("applyAssignDriver returned error: %v", err)
	}

	if updatedVehicle.DriverID != "driver-1" {
		t.Fatalf("expected new vehicle to be assigned to driver-1, got %q", updatedVehicle.DriverID)
	}
	if updatedVehicle.DriverName != "Driver One" {
		t.Fatalf("expected driver name to be populated from driver profile, got %q", updatedVehicle.DriverName)
	}

	oldVehicle, err := store.GetVehicle(context.Background(), "fleet-1", "vehicle-old")
	if err != nil {
		t.Fatalf("GetVehicle old: %v", err)
	}
	if oldVehicle.DriverID != "" {
		t.Fatalf("expected previous vehicle to be cleared, got driver %q", oldVehicle.DriverID)
	}
	if oldVehicle.StatusMessage != "Driver moved to another vehicle" {
		t.Fatalf("expected previous vehicle status to explain reassignment, got %q", oldVehicle.StatusMessage)
	}

	driver, err := store.GetDriver(context.Background(), "fleet-1", "driver-1")
	if err != nil {
		t.Fatalf("GetDriver: %v", err)
	}
	if len(driver.AssignedVehicleIDs) != 1 || driver.AssignedVehicleIDs[0] != "vehicle-new" {
		t.Fatalf("expected driver to have only the new vehicle assignment, got %v", driver.AssignedVehicleIDs)
	}
	if len(notifStore.items) != 1 {
		t.Fatalf("expected 1 driver notification, got %d", len(notifStore.items))
	}
	if notifStore.items[0].UserID != "driver-1" ||
		notifStore.items[0].Data["event_type"] != "driver_vehicle_assignment_approved" {
		t.Fatalf("expected assignment notification, got %#v", notifStore.items[0])
	}
	notices, err := store.ListNotices(context.Background(), "fleet-1", "", "driver-1")
	if err != nil {
		t.Fatalf("ListNotices: %v", err)
	}
	if len(notices) != 1 || notices[0].Title != "Vehicle Assignment" {
		t.Fatalf("expected assignment notice, got %#v", notices)
	}
}

func TestApplyDeleteDriverClearsAssignedVehiclesBeforeRemovingDriver(t *testing.T) {
	t.Parallel()

	store := newHandlerTestStore()
	notifStore := &handlerTestNotifStore{}
	now := time.Now().UTC()
	store.drivers[store.driverKey("fleet-1", "driver-1")] = &Driver{
		ID:                 "driver-1",
		FleetID:            "fleet-1",
		FullName:           "Driver One",
		Phone:              "+94 77 111 1111",
		AssignedVehicleIDs: []string{"vehicle-1"},
		IsActive:           true,
		CreatedAt:          now,
		UpdatedAt:          now,
	}
	store.vehicles[store.vehicleKey("fleet-1", "vehicle-1")] = &Vehicle{
		ID:                 "vehicle-1",
		FleetID:            "fleet-1",
		RegistrationNumber: "ND-4521",
		Nickname:           "Morning Shuttle",
		DriverID:           "driver-1",
		DriverName:         "Driver One",
		DriverPhone:        "+94 77 111 1111",
		LastUpdated:        now,
		CreatedAt:          now,
	}

	handler := &Handler{store: store, notifyStore: notifStore}

	if err := handler.applyDeleteDriver(context.Background(), "driver-1", "fleet-1"); err != nil {
		t.Fatalf("applyDeleteDriver returned error: %v", err)
	}

	if _, err := store.GetDriver(context.Background(), "fleet-1", "driver-1"); err == nil {
		t.Fatalf("expected driver record to be removed")
	}

	vehicle, err := store.GetVehicle(context.Background(), "fleet-1", "vehicle-1")
	if err != nil {
		t.Fatalf("GetVehicle: %v", err)
	}
	if vehicle.DriverID != "" {
		t.Fatalf("expected vehicle to be detached from deleted driver, got %q", vehicle.DriverID)
	}
	if vehicle.StatusMessage != "Driver left fleet" {
		t.Fatalf("expected vehicle status message to reflect driver departure, got %q", vehicle.StatusMessage)
	}

	if len(notifStore.items) != 1 {
		t.Fatalf("expected 1 driver notification, got %d", len(notifStore.items))
	}
	if notifStore.items[0].UserID != "driver-1" ||
		notifStore.items[0].Data["event_type"] != "driver_access_removed" {
		t.Fatalf("expected fleet-leave notification, got %#v", notifStore.items[0])
	}
}

func TestApplyUnassignDriverNotifiesResolvedAuthUser(t *testing.T) {
	t.Parallel()

	store := newHandlerTestStore()
	notifStore := &handlerTestNotifStore{}
	userStore := &handlerTestUserStore{
		byEmail: map[string]*authsvc.User{
			"driver.one@via.local": {
				ID:      "auth-user-1",
				Email:   "driver.one@via.local",
				Role:    "driver",
				FleetID: "fleet-1",
			},
		},
	}
	now := time.Now().UTC()
	store.drivers[store.driverKey("fleet-1", "driver-record-1")] = &Driver{
		ID:                 "driver-record-1",
		Email:              "driver.one@via.local",
		FullName:           "Driver One",
		FleetID:            "fleet-1",
		AssignedVehicleIDs: []string{"vehicle-1"},
		IsActive:           true,
		CreatedAt:          now,
		UpdatedAt:          now,
	}
	store.vehicles[store.vehicleKey("fleet-1", "vehicle-1")] = &Vehicle{
		ID:                 "vehicle-1",
		FleetID:            "fleet-1",
		RegistrationNumber: "NC-9912",
		Nickname:           "Evening Shuttle",
		DriverID:           "driver-record-1",
		DriverName:         "Driver One",
		LastUpdated:        now,
		CreatedAt:          now,
	}

	handler := &Handler{
		store:       store,
		notifyStore: notifStore,
		userStore:   userStore,
	}

	updated, err := handler.applyUnassignDriver(context.Background(), "vehicle-1", unassignDriverPayload{
		FleetID: "fleet-1",
	})
	if err != nil {
		t.Fatalf("applyUnassignDriver returned error: %v", err)
	}
	if updated.DriverID != "" {
		t.Fatalf("expected vehicle driver to be cleared, got %q", updated.DriverID)
	}

	foundAuthUserNotification := false
	for _, item := range notifStore.items {
		if item.UserID == "auth-user-1" && item.Data["event_type"] == "driver_unassigned" {
			foundAuthUserNotification = true
			break
		}
	}
	if !foundAuthUserNotification {
		t.Fatalf("expected driver_unassigned notification for resolved auth user, got %#v", notifStore.items)
	}
}
