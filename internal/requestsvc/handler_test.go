package requestsvc

import (
	"bytes"
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
	"via-backend/internal/fleetsvc"
	"via-backend/internal/notifysvc"
	"via-backend/internal/opsvc"
)

type handlerTestRequestStore struct {
	lastPut *DriverRequest
	items   map[string]*DriverRequest
}

type handlerTestOpsStore struct {
	operations map[string]*opsvc.Operation
}

func (s *handlerTestOpsStore) Put(_ context.Context, op *opsvc.Operation) error {
	if op == nil {
		return nil
	}
	copy := *op
	if s.operations == nil {
		s.operations = map[string]*opsvc.Operation{}
	}
	s.operations[op.ID] = &copy
	return nil
}

func (s *handlerTestOpsStore) Get(_ context.Context, id string) (*opsvc.Operation, error) {
	if s.operations == nil {
		return nil, fmt.Errorf("operation not found")
	}
	op, ok := s.operations[id]
	if !ok {
		return nil, fmt.Errorf("operation not found")
	}
	copy := *op
	return &copy, nil
}

func (s *handlerTestOpsStore) List(_ context.Context, _ opsvc.ListFilter) ([]opsvc.Operation, error) {
	items := make([]opsvc.Operation, 0, len(s.operations))
	for _, op := range s.operations {
		items = append(items, *op)
	}
	return items, nil
}

func (s *handlerTestOpsStore) FindByIdempotencyKey(_ context.Context, key string) (*opsvc.Operation, error) {
	for _, op := range s.operations {
		if op.IdempotencyKey == key {
			copy := *op
			return &copy, nil
		}
	}
	return nil, fmt.Errorf("operation not found")
}

type handlerTestUserStore struct {
	users []authsvc.User
}

func (s *handlerTestUserStore) CreateUser(_ context.Context, _ *authsvc.User) error {
	return nil
}

func (s *handlerTestUserStore) GetUser(_ context.Context, userID string) (*authsvc.User, error) {
	for i := range s.users {
		if s.users[i].ID == userID {
			copy := s.users[i]
			return &copy, nil
		}
	}
	return nil, fmt.Errorf("user not found")
}

func (s *handlerTestUserStore) GetUserByEmail(_ context.Context, email string) (*authsvc.User, error) {
	for i := range s.users {
		if s.users[i].Email == email {
			copy := s.users[i]
			return &copy, nil
		}
	}
	return nil, fmt.Errorf("user not found")
}

func (s *handlerTestUserStore) UpdateUser(_ context.Context, _ *authsvc.User) error {
	return nil
}

func (s *handlerTestUserStore) ListUsers(_ context.Context, filterRole, filterFleet string) ([]authsvc.User, error) {
	items := make([]authsvc.User, 0, len(s.users))
	for _, user := range s.users {
		if filterRole != "" && user.Role != filterRole {
			continue
		}
		if filterFleet != "" && user.FleetID != filterFleet {
			continue
		}
		items = append(items, user)
	}
	return items, nil
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

func (s *handlerTestRequestStore) Put(_ context.Context, req *DriverRequest) error {
	if req == nil {
		s.lastPut = nil
		return nil
	}
	copy := *req
	s.lastPut = &copy
	if s.items == nil {
		s.items = map[string]*DriverRequest{}
	}
	s.items[copy.ID] = &copy
	return nil
}

func (s *handlerTestRequestStore) Get(_ context.Context, id string) (*DriverRequest, error) {
	if s.items != nil {
		if item, ok := s.items[id]; ok && item != nil {
			copy := *item
			return &copy, nil
		}
	}
	if s.lastPut != nil && s.lastPut.ID == id {
		copy := *s.lastPut
		return &copy, nil
	}
	return nil, fmt.Errorf("driver request not found")
}

func (s *handlerTestRequestStore) List(
	_ context.Context,
	fleetID,
	status,
	requestType string,
) ([]DriverRequest, error) {
	if s.items == nil {
		return nil, nil
	}
	items := make([]DriverRequest, 0, len(s.items))
	for _, item := range s.items {
		if item == nil {
			continue
		}
		if fleetID != "" && item.FleetID != fleetID {
			continue
		}
		if status != "" && item.Status != status {
			continue
		}
		if requestType != "" && item.RequestType != requestType {
			continue
		}
		copy := *item
		items = append(items, copy)
	}
	return items, nil
}

func (s *handlerTestRequestStore) FindPendingByUser(
	_ context.Context,
	fleetID,
	userID,
	requestType string,
) (*DriverRequest, error) {
	if s.items == nil {
		return nil, fmt.Errorf("driver request not found")
	}
	for _, item := range s.items {
		if item == nil {
			continue
		}
		if fleetID != "" && item.FleetID != fleetID {
			continue
		}
		if item.UserID != userID || item.Status != StatusPending {
			continue
		}
		if requestType != "" && item.RequestType != requestType {
			continue
		}
		copy := *item
		return &copy, nil
	}
	return nil, fmt.Errorf("driver request not found")
}

func (s *handlerTestRequestStore) RevokeApprovedVehicleAssignments(
	_ context.Context,
	fleetID,
	userID string,
) (int, error) {
	normalizedFleetID := strings.TrimSpace(fleetID)
	normalizedUserID := strings.TrimSpace(userID)
	if normalizedFleetID == "" || normalizedUserID == "" {
		return 0, nil
	}
	revokedCount := 0
	now := time.Now().UTC()
	for id, item := range s.items {
		if item == nil {
			continue
		}
		if item.FleetID != normalizedFleetID ||
			item.UserID != normalizedUserID ||
			item.RequestType != RequestTypeVehicleAssignment ||
			item.Status != StatusApproved {
			continue
		}
		copy := *item
		copy.Status = StatusRevoked
		copy.UpdatedAt = now
		s.items[id] = &copy
		s.lastPut = &copy
		revokedCount++
	}
	return revokedCount, nil
}

type handlerTestFleetStore struct {
	vehicles map[string]*fleetsvc.Vehicle
	drivers  map[string]*fleetsvc.Driver
	notices  []*fleetsvc.DriverNotice
}

func newRequestHandlerTestFleetStore() *handlerTestFleetStore {
	return &handlerTestFleetStore{
		vehicles: map[string]*fleetsvc.Vehicle{},
		drivers:  map[string]*fleetsvc.Driver{},
		notices:  []*fleetsvc.DriverNotice{},
	}
}

func (s *handlerTestFleetStore) PutVehicle(_ context.Context, v *fleetsvc.Vehicle) error {
	s.vehicles[s.vehicleKey(v.FleetID, v.ID)] = cloneRequestTestVehicle(v)
	return nil
}

func (s *handlerTestFleetStore) GetVehicle(_ context.Context, fleetID, vehicleID string) (*fleetsvc.Vehicle, error) {
	v, ok := s.vehicles[s.vehicleKey(fleetID, vehicleID)]
	if !ok {
		return nil, fmt.Errorf("vehicle not found")
	}
	return cloneRequestTestVehicle(v), nil
}

func (s *handlerTestFleetStore) GetVehicleByID(_ context.Context, vehicleID string) (*fleetsvc.Vehicle, error) {
	for _, v := range s.vehicles {
		if v.ID == vehicleID {
			return cloneRequestTestVehicle(v), nil
		}
	}
	return nil, fmt.Errorf("vehicle not found")
}

func (s *handlerTestFleetStore) DeleteVehicle(_ context.Context, fleetID, vehicleID string) error {
	delete(s.vehicles, s.vehicleKey(fleetID, vehicleID))
	return nil
}

func (s *handlerTestFleetStore) ListVehicles(_ context.Context, fleetID string) ([]fleetsvc.Vehicle, error) {
	items := make([]fleetsvc.Vehicle, 0, len(s.vehicles))
	for _, v := range s.vehicles {
		if fleetID != "" && v.FleetID != fleetID {
			continue
		}
		items = append(items, *cloneRequestTestVehicle(v))
	}
	return items, nil
}

func (s *handlerTestFleetStore) ListVehiclesForDriver(_ context.Context, fleetID, driverID string) ([]fleetsvc.Vehicle, error) {
	items := make([]fleetsvc.Vehicle, 0, len(s.vehicles))
	for _, v := range s.vehicles {
		if v.FleetID != fleetID || v.DriverID != driverID {
			continue
		}
		items = append(items, *cloneRequestTestVehicle(v))
	}
	return items, nil
}

func (s *handlerTestFleetStore) PutDriver(_ context.Context, d *fleetsvc.Driver) error {
	s.drivers[s.driverKey(d.FleetID, d.ID)] = cloneRequestTestDriver(d)
	return nil
}

func (s *handlerTestFleetStore) GetDriver(_ context.Context, fleetID, driverID string) (*fleetsvc.Driver, error) {
	d, ok := s.drivers[s.driverKey(fleetID, driverID)]
	if !ok {
		return nil, fmt.Errorf("driver not found")
	}
	return cloneRequestTestDriver(d), nil
}

func (s *handlerTestFleetStore) DeleteDriver(_ context.Context, fleetID, driverID string) error {
	delete(s.drivers, s.driverKey(fleetID, driverID))
	return nil
}

func (s *handlerTestFleetStore) ListDrivers(_ context.Context, fleetID string) ([]fleetsvc.Driver, error) {
	items := make([]fleetsvc.Driver, 0, len(s.drivers))
	for _, d := range s.drivers {
		if fleetID != "" && d.FleetID != fleetID {
			continue
		}
		items = append(items, *cloneRequestTestDriver(d))
	}
	return items, nil
}

func (s *handlerTestFleetStore) PutEvent(_ context.Context, _ *fleetsvc.SpecialEvent) error {
	return nil
}

func (s *handlerTestFleetStore) GetEvent(_ context.Context, _ string) (*fleetsvc.SpecialEvent, error) {
	return nil, fmt.Errorf("event not found")
}

func (s *handlerTestFleetStore) ListEvents(_ context.Context, _, _ string) ([]fleetsvc.SpecialEvent, error) {
	return nil, nil
}

func (s *handlerTestFleetStore) PutNotice(_ context.Context, n *fleetsvc.DriverNotice) error {
	if n == nil {
		return nil
	}
	copy := *n
	s.notices = append(s.notices, &copy)
	return nil
}

func (s *handlerTestFleetStore) GetNotice(_ context.Context, noticeID string) (*fleetsvc.DriverNotice, error) {
	for _, n := range s.notices {
		if n.ID == noticeID {
			copy := *n
			return &copy, nil
		}
	}
	return nil, fmt.Errorf("notice not found")
}

func (s *handlerTestFleetStore) ListNotices(_ context.Context, fleetID, vehicleID, driverID string) ([]fleetsvc.DriverNotice, error) {
	items := make([]fleetsvc.DriverNotice, 0, len(s.notices))
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
		items = append(items, *n)
	}
	return items, nil
}

func (s *handlerTestFleetStore) vehicleKey(fleetID, vehicleID string) string {
	return fleetID + "/" + vehicleID
}

func (s *handlerTestFleetStore) driverKey(fleetID, driverID string) string {
	return fleetID + "/" + driverID
}

func cloneRequestTestVehicle(v *fleetsvc.Vehicle) *fleetsvc.Vehicle {
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

func cloneRequestTestDriver(d *fleetsvc.Driver) *fleetsvc.Driver {
	if d == nil {
		return nil
	}
	copy := *d
	if d.AssignedVehicleIDs != nil {
		copy.AssignedVehicleIDs = append([]string(nil), d.AssignedVehicleIDs...)
	}
	return &copy
}

func TestApproveAccessRequestCreatesDriverWithoutVehicleAssignment(t *testing.T) {
	t.Parallel()

	requestStore := &handlerTestRequestStore{}
	fleetStore := newRequestHandlerTestFleetStore()
	notifStore := &handlerTestNotifStore{}
	now := time.Now().UTC()

	req := &DriverRequest{
		ID:          "access-1",
		UserID:      "driver-1",
		FleetID:     "fleet-1",
		RequestType: RequestTypeAccess,
		FullName:    "Driver One",
		Email:       "driver-1@example.com",
		Phone:       "+94 77 111 1111",
		Status:      StatusPending,
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	handler := &Handler{
		store:       requestStore,
		fleetStore:  fleetStore,
		notifyStore: notifStore,
	}

	if err := handler.approveRequest(context.Background(), req); err != nil {
		t.Fatalf("approveRequest returned error: %v", err)
	}

	if req.Status != StatusApproved {
		t.Fatalf("expected access request status to be %q, got %q", StatusApproved, req.Status)
	}
	if requestStore.lastPut == nil || requestStore.lastPut.Status != StatusApproved {
		t.Fatalf("expected approved request to be persisted, got %#v", requestStore.lastPut)
	}

	driver, err := fleetStore.GetDriver(context.Background(), "fleet-1", "driver-1")
	if err != nil {
		t.Fatalf("GetDriver: %v", err)
	}
	if len(driver.AssignedVehicleIDs) != 0 {
		t.Fatalf("expected no vehicle assignment after access approval, got %v", driver.AssignedVehicleIDs)
	}

	if len(fleetStore.notices) != 1 {
		t.Fatalf("expected 1 notice for access approval, got %d", len(fleetStore.notices))
	}
	if fleetStore.notices[0].Title != "Access Approved" {
		t.Fatalf("expected access approval notice, got %#v", fleetStore.notices[0])
	}
	if len(notifStore.items) != 1 {
		t.Fatalf("expected 1 driver app notification, got %d", len(notifStore.items))
	}
	if notifStore.items[0].UserID != "driver-1" ||
		notifStore.items[0].Data["event_type"] != "driver_access_approved" {
		t.Fatalf("expected driver access approval notification, got %#v", notifStore.items[0])
	}
}

func TestApproveAccessRequestClearsExistingVehicleAssignments(t *testing.T) {
	t.Parallel()

	requestStore := &handlerTestRequestStore{}
	fleetStore := newRequestHandlerTestFleetStore()
	notifStore := &handlerTestNotifStore{}
	now := time.Now().UTC()

	fleetStore.drivers[fleetStore.driverKey("fleet-1", "driver-1")] = &fleetsvc.Driver{
		ID:                 "driver-1",
		FleetID:            "fleet-1",
		Email:              "driver-1@example.com",
		FullName:           "Driver One",
		Phone:              "+94 77 111 1111",
		AssignedVehicleIDs: []string{"vehicle-old"},
		IsActive:           true,
		CreatedAt:          now,
		UpdatedAt:          now,
	}
	fleetStore.vehicles[fleetStore.vehicleKey("fleet-1", "vehicle-old")] = &fleetsvc.Vehicle{
		ID:                 "vehicle-old",
		FleetID:            "fleet-1",
		RegistrationNumber: "OLD-1001",
		DriverID:           "driver-1",
		DriverName:         "Driver One",
		DriverPhone:        "+94 77 111 1111",
		StatusMessage:      "Driver assigned",
		LastUpdated:        now,
		CreatedAt:          now,
	}

	req := &DriverRequest{
		ID:          "access-2",
		UserID:      "driver-1",
		FleetID:     "fleet-1",
		RequestType: RequestTypeAccess,
		FullName:    "Driver One",
		Email:       "driver-1@example.com",
		Phone:       "+94 77 111 1111",
		Status:      StatusPending,
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	handler := &Handler{
		store:       requestStore,
		fleetStore:  fleetStore,
		notifyStore: notifStore,
	}

	if err := handler.approveRequest(context.Background(), req); err != nil {
		t.Fatalf("approveRequest returned error: %v", err)
	}

	driver, err := fleetStore.GetDriver(context.Background(), "fleet-1", "driver-1")
	if err != nil {
		t.Fatalf("GetDriver: %v", err)
	}
	if len(driver.AssignedVehicleIDs) != 0 {
		t.Fatalf(
			"expected access approval to clear stale assigned vehicles, got %v",
			driver.AssignedVehicleIDs,
		)
	}

	vehicle, err := fleetStore.GetVehicle(context.Background(), "fleet-1", "vehicle-old")
	if err != nil {
		t.Fatalf("GetVehicle: %v", err)
	}
	if vehicle.DriverID != "" || vehicle.DriverName != "" || vehicle.DriverPhone != "" {
		t.Fatalf(
			"expected vehicle assignment to be cleared on access approval, got %#v",
			vehicle,
		)
	}
}

func TestApproveVehicleRequestAssignsRequestedVehicleAndClearsPreviousAssignment(t *testing.T) {
	t.Parallel()

	requestStore := &handlerTestRequestStore{}
	fleetStore := newRequestHandlerTestFleetStore()
	notifStore := &handlerTestNotifStore{}
	now := time.Now().UTC()

	fleetStore.drivers[fleetStore.driverKey("fleet-1", "driver-1")] = &fleetsvc.Driver{
		ID:                 "driver-1",
		FleetID:            "fleet-1",
		Email:              "driver-1@example.com",
		FullName:           "Driver One",
		Phone:              "+94 77 111 1111",
		AssignedVehicleIDs: []string{"vehicle-old"},
		IsActive:           true,
		CreatedAt:          now,
		UpdatedAt:          now,
	}
	fleetStore.vehicles[fleetStore.vehicleKey("fleet-1", "vehicle-old")] = &fleetsvc.Vehicle{
		ID:                 "vehicle-old",
		FleetID:            "fleet-1",
		RegistrationNumber: "OLD-1001",
		DriverID:           "driver-1",
		DriverName:         "Driver One",
		DriverPhone:        "+94 77 111 1111",
		LastUpdated:        now,
		CreatedAt:          now,
	}
	fleetStore.vehicles[fleetStore.vehicleKey("fleet-1", "vehicle-new")] = &fleetsvc.Vehicle{
		ID:                 "vehicle-new",
		FleetID:            "fleet-1",
		RegistrationNumber: "NEW-2002",
		CurrentRouteID:     "route-a",
		LastUpdated:        now,
		CreatedAt:          now,
	}

	req := &DriverRequest{
		ID:          "request-1",
		UserID:      "driver-1",
		FleetID:     "fleet-1",
		RequestType: RequestTypeVehicleAssignment,
		VehicleID:   "vehicle-new",
		FullName:    "Driver One",
		Email:       "driver-1@example.com",
		Phone:       "+94 77 111 1111",
		Status:      StatusPending,
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	handler := &Handler{
		store:       requestStore,
		fleetStore:  fleetStore,
		notifyStore: notifStore,
	}

	if err := handler.approveRequest(context.Background(), req); err != nil {
		t.Fatalf("approveRequest returned error: %v", err)
	}

	if req.Status != StatusApproved {
		t.Fatalf("expected request status to be %q, got %q", StatusApproved, req.Status)
	}
	if requestStore.lastPut == nil || requestStore.lastPut.Status != StatusApproved {
		t.Fatalf("expected approved request to be persisted, got %#v", requestStore.lastPut)
	}

	oldVehicle, err := fleetStore.GetVehicle(context.Background(), "fleet-1", "vehicle-old")
	if err != nil {
		t.Fatalf("GetVehicle old: %v", err)
	}
	if oldVehicle.DriverID != "" {
		t.Fatalf("expected previous vehicle to be cleared, got driver %q", oldVehicle.DriverID)
	}

	newVehicle, err := fleetStore.GetVehicle(context.Background(), "fleet-1", "vehicle-new")
	if err != nil {
		t.Fatalf("GetVehicle new: %v", err)
	}
	if newVehicle.DriverID != "driver-1" {
		t.Fatalf("expected requested vehicle to be assigned to driver-1, got %q", newVehicle.DriverID)
	}

	driver, err := fleetStore.GetDriver(context.Background(), "fleet-1", "driver-1")
	if err != nil {
		t.Fatalf("GetDriver: %v", err)
	}
	if len(driver.AssignedVehicleIDs) != 1 || driver.AssignedVehicleIDs[0] != "vehicle-new" {
		t.Fatalf("expected driver to retain only the requested vehicle, got %v", driver.AssignedVehicleIDs)
	}

	if len(fleetStore.notices) != 1 {
		t.Fatalf("expected 1 notice for vehicle assignment approval, got %d", len(fleetStore.notices))
	}
	if fleetStore.notices[0].Title != "Vehicle Assignment Approved" ||
		fleetStore.notices[0].VehicleID != "vehicle-new" {
		t.Fatalf("expected a vehicle assignment approval notice for vehicle-new, got %#v", fleetStore.notices)
	}
	if len(notifStore.items) != 1 {
		t.Fatalf("expected 1 driver app notification, got %d", len(notifStore.items))
	}
	if notifStore.items[0].UserID != "driver-1" ||
		notifStore.items[0].VehicleID != "vehicle-new" ||
		notifStore.items[0].Data["event_type"] != "driver_vehicle_assignment_approved" {
		t.Fatalf("expected vehicle assignment approval notification, got %#v", notifStore.items[0])
	}
}

func TestApproveVehicleRequestRequiresApprovedDriver(t *testing.T) {
	t.Parallel()

	requestStore := &handlerTestRequestStore{}
	fleetStore := newRequestHandlerTestFleetStore()
	now := time.Now().UTC()

	fleetStore.vehicles[fleetStore.vehicleKey("fleet-1", "vehicle-new")] = &fleetsvc.Vehicle{
		ID:                 "vehicle-new",
		FleetID:            "fleet-1",
		RegistrationNumber: "NEW-2002",
		LastUpdated:        now,
		CreatedAt:          now,
	}

	req := &DriverRequest{
		ID:          "request-2",
		UserID:      "driver-2",
		FleetID:     "fleet-1",
		RequestType: RequestTypeVehicleAssignment,
		VehicleID:   "vehicle-new",
		FullName:    "Driver Two",
		Email:       "driver-2@example.com",
		Status:      StatusPending,
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	handler := &Handler{
		store:      requestStore,
		fleetStore: fleetStore,
	}

	if err := handler.approveRequest(context.Background(), req); err == nil {
		t.Fatalf("expected vehicle approval to fail when the driver is not approved")
	}
	if requestStore.lastPut != nil {
		t.Fatalf("expected request not to be persisted on failure, got %#v", requestStore.lastPut)
	}
}

func TestNotifyOwnersOfPendingRequestCreatesOwnerNotifications(t *testing.T) {
	t.Parallel()

	notifStore := &handlerTestNotifStore{}
	userStore := &handlerTestUserStore{
		users: []authsvc.User{
			{ID: "owner-1", Role: "owner", FleetID: "fleet-1"},
			{ID: "driver-1", Role: "driver", FleetID: "fleet-1"},
			{ID: "owner-2", Role: "owner", FleetID: "fleet-2"},
		},
	}
	handler := &Handler{
		notifyStore: notifStore,
		userStore:   userStore,
	}

	handler.notifyOwnersOfPendingRequest(context.Background(), &DriverRequest{
		ID:          "request-1",
		UserID:      "driver-1",
		FleetID:     "fleet-1",
		RequestType: RequestTypeAccess,
		FullName:    "Driver One",
	})

	if len(notifStore.items) != 1 {
		t.Fatalf("expected 1 owner notification, got %d", len(notifStore.items))
	}
	if notifStore.items[0].UserID != "owner-1" ||
		notifStore.items[0].Type != "driver_request" ||
		notifStore.items[0].Data["event_type"] != "driver_access_request_created" {
		t.Fatalf("expected owner access-request notification, got %#v", notifStore.items[0])
	}
}

func TestListAllowsDriverToSeeOnlyOwnRequests(t *testing.T) {
	t.Parallel()

	requestStore := &handlerTestRequestStore{
		items: map[string]*DriverRequest{
			"request-1": {
				ID:          "request-1",
				UserID:      "driver-1",
				FleetID:     "fleet-1",
				RequestType: RequestTypeAccess,
				Status:      StatusPending,
			},
			"request-2": {
				ID:          "request-2",
				UserID:      "driver-2",
				FleetID:     "fleet-2",
				RequestType: RequestTypeAccess,
				Status:      StatusPending,
			},
		},
	}
	handler := &Handler{store: requestStore}
	mux := http.NewServeMux()
	handler.Mount(mux)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/driver-requests?status=pending", nil)
	req = req.WithContext(auth.ContextWithIdentity(req.Context(), auth.Identity{
		UserID: "driver-1",
		Role:   auth.RoleDriver,
	}))
	recorder := httptest.NewRecorder()

	mux.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", recorder.Code, recorder.Body.String())
	}

	var items []DriverRequest
	if err := json.Unmarshal(recorder.Body.Bytes(), &items); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if len(items) != 1 || items[0].ID != "request-1" {
		t.Fatalf("expected only driver-1 request, got %#v", items)
	}
}

func TestCreateRejectsDriverImpersonation(t *testing.T) {
	t.Parallel()

	handler := &Handler{}
	mux := http.NewServeMux()
	handler.Mount(mux)

	body := bytes.NewBufferString(`{
		"user_id":"driver-2",
		"fleet_id":"fleet-1",
		"request_type":"access",
		"full_name":"Driver Two",
		"email":"driver-2@example.com"
	}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/driver-requests", body)
	req = req.WithContext(auth.ContextWithIdentity(req.Context(), auth.Identity{
		UserID: "driver-1",
		Role:   auth.RoleDriver,
	}))
	recorder := httptest.NewRecorder()

	mux.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d: %s", recorder.Code, recorder.Body.String())
	}
}

func TestApproveRejectsDriverIdentity(t *testing.T) {
	t.Parallel()

	requestStore := &handlerTestRequestStore{
		items: map[string]*DriverRequest{
			"request-1": {
				ID:          "request-1",
				UserID:      "driver-1",
				FleetID:     "fleet-1",
				RequestType: RequestTypeVehicleAssignment,
				VehicleID:   "vehicle-1",
				Status:      StatusPending,
			},
		},
	}
	handler := &Handler{store: requestStore}
	mux := http.NewServeMux()
	handler.Mount(mux)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/driver-requests/request-1/approve", nil)
	req = req.WithContext(auth.ContextWithIdentity(req.Context(), auth.Identity{
		UserID:  "driver-1",
		Role:    auth.RoleDriver,
		FleetID: "fleet-1",
	}))
	recorder := httptest.NewRecorder()

	mux.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d: %s", recorder.Code, recorder.Body.String())
	}
}

func TestApproveRejectsDevServiceIdentity(t *testing.T) {
	t.Parallel()

	requestStore := &handlerTestRequestStore{
		items: map[string]*DriverRequest{
			"request-1": {
				ID:          "request-1",
				UserID:      "driver-1",
				FleetID:     "fleet-1",
				RequestType: RequestTypeVehicleAssignment,
				VehicleID:   "vehicle-1",
				Status:      StatusPending,
			},
		},
	}
	handler := &Handler{store: requestStore}
	mux := http.NewServeMux()
	handler.Mount(mux)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/driver-requests/request-1/approve", nil)
	req = req.WithContext(auth.ContextWithIdentity(req.Context(), auth.Identity{
		UserID:  "dev",
		Role:    auth.RoleService,
		FleetID: "fleet-1",
	}))
	recorder := httptest.NewRecorder()

	mux.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d: %s", recorder.Code, recorder.Body.String())
	}
}

func TestProcessCancelCommandMarksPendingRequestCanceled(t *testing.T) {
	t.Parallel()

	requestStore := &handlerTestRequestStore{
		lastPut: &DriverRequest{
			ID:          "request-1",
			UserID:      "driver-1",
			FleetID:     "fleet-1",
			RequestType: RequestTypeAccess,
			FullName:    "Driver One",
			Status:      StatusPending,
			CreatedAt:   time.Now().UTC(),
			UpdatedAt:   time.Now().UTC(),
		},
	}
	userStore := &handlerTestUserStore{
		users: []authsvc.User{
			{ID: "owner-1", Role: "owner", FleetID: "fleet-1"},
		},
	}
	notifStore := &handlerTestNotifStore{}
	opsStore := &handlerTestOpsStore{operations: map[string]*opsvc.Operation{}}
	handler := &Handler{
		store:       requestStore,
		opsStore:    opsStore,
		notifyStore: notifStore,
		userStore:   userStore,
	}

	payload, err := json.Marshal(driverRequestDecisionCommand{
		OperationID: "op-1",
		RequestID:   "request-1",
		FleetID:     "fleet-1",
	})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	handler.processCancelCommand(payload)

	if requestStore.lastPut == nil || requestStore.lastPut.Status != StatusCanceled {
		t.Fatalf("expected canceled request to be persisted, got %#v", requestStore.lastPut)
	}
	if len(notifStore.items) != 1 ||
		notifStore.items[0].Data["event_type"] != "driver_request_cancelled" {
		t.Fatalf("expected owner cancel notification, got %#v", notifStore.items)
	}
}

func TestProcessDriverAccessRevokedEventRevokesApprovedVehicleRequests(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	requestStore := &handlerTestRequestStore{
		items: map[string]*DriverRequest{
			"req-access": {
				ID:          "req-access",
				UserID:      "driver-1",
				FleetID:     "fleet-1",
				RequestType: RequestTypeAccess,
				Status:      StatusApproved,
				CreatedAt:   now,
				UpdatedAt:   now,
			},
			"req-vehicle-approved": {
				ID:          "req-vehicle-approved",
				UserID:      "driver-1",
				FleetID:     "fleet-1",
				RequestType: RequestTypeVehicleAssignment,
				VehicleID:   "veh-1",
				Status:      StatusApproved,
				CreatedAt:   now,
				UpdatedAt:   now,
			},
			"req-vehicle-other-driver": {
				ID:          "req-vehicle-other-driver",
				UserID:      "driver-2",
				FleetID:     "fleet-1",
				RequestType: RequestTypeVehicleAssignment,
				VehicleID:   "veh-2",
				Status:      StatusApproved,
				CreatedAt:   now,
				UpdatedAt:   now,
			},
		},
	}

	handler := &Handler{
		store: requestStore,
	}

	payload, err := json.Marshal(driverAccessRevokedEvent{
		FleetID:  "fleet-1",
		DriverID: "driver-1",
	})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	handler.processDriverAccessRevokedEvent(payload)

	approvedVehicle, err := requestStore.Get(context.Background(), "req-vehicle-approved")
	if err != nil {
		t.Fatalf("Get approved vehicle request: %v", err)
	}
	if approvedVehicle.Status != StatusRevoked {
		t.Fatalf("expected approved vehicle request to be revoked, got %q", approvedVehicle.Status)
	}

	accessRequest, err := requestStore.Get(context.Background(), "req-access")
	if err != nil {
		t.Fatalf("Get access request: %v", err)
	}
	if accessRequest.Status != StatusApproved {
		t.Fatalf("expected access request status to remain %q, got %q", StatusApproved, accessRequest.Status)
	}

	otherDriverRequest, err := requestStore.Get(context.Background(), "req-vehicle-other-driver")
	if err != nil {
		t.Fatalf("Get other driver request: %v", err)
	}
	if otherDriverRequest.Status != StatusApproved {
		t.Fatalf(
			"expected other driver request status to remain %q, got %q",
			StatusApproved,
			otherDriverRequest.Status,
		)
	}
}
