package fleetsvc

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"via-backend/internal/auth"
	"via-backend/internal/authsvc"
	"via-backend/internal/messaging"
	"via-backend/internal/notifysvc"
	"via-backend/internal/opsvc"
	"via-backend/internal/tenantsvc"
)

const (
	createVehicleCommandSubject         = "cmd.fleet.vehicle.create"
	updateVehicleCommandSubject         = "cmd.fleet.vehicle.update"
	deleteVehicleCommandSubject         = "cmd.fleet.vehicle.delete"
	updateVehicleStatusCommandSubject   = "cmd.fleet.vehicle.update_status"
	updateVehicleLocationCommandSubject = "cmd.fleet.vehicle.update_location"
	createDriverCommandSubject          = "cmd.fleet.driver.create"
	updateDriverCommandSubject          = "cmd.fleet.driver.update"
	deleteDriverCommandSubject          = "cmd.fleet.driver.delete"
	createEventCommandSubject           = "cmd.fleet.event.create"
	updateEventCommandSubject           = "cmd.fleet.event.update"
	createNoticeCommandSubject          = "cmd.fleet.notice.create"
	markNoticeReadCommandSubject        = "cmd.fleet.notice.mark_read"
	assignDriverCommandSubject          = "cmd.fleet.vehicle.assign_driver"
	unassignDriverCommandSubject        = "cmd.fleet.vehicle.unassign_driver"
	driverAccessRevokedEventSubject     = "evt.driver.access.revoked"

	createVehicleOperationType         = "vehicle.create"
	updateVehicleOperationType         = "vehicle.update"
	deleteVehicleOperationType         = "vehicle.delete"
	updateVehicleStatusOperationType   = "vehicle.update_status"
	updateVehicleLocationOperationType = "vehicle.update_location"
	createDriverOperationType          = "driver.create"
	updateDriverOperationType          = "driver.update"
	deleteDriverOperationType          = "driver.delete"
	createEventOperationType           = "event.create"
	updateEventOperationType           = "event.update"
	createNoticeOperationType          = "notice.create"
	markNoticeReadOperationType        = "notice.mark_read"
	assignDriverOperationType          = "vehicle.assign_driver"
	unassignDriverOperationType        = "vehicle.unassign_driver"
)

type createVehicleCommand struct {
	OperationID string  `json:"operation_id"`
	Vehicle     Vehicle `json:"vehicle"`
}

type updateVehicleCommand struct {
	OperationID string                     `json:"operation_id"`
	VehicleID   string                     `json:"vehicle_id"`
	FleetID     string                     `json:"fleet_id,omitempty"`
	Fields      map[string]json.RawMessage `json:"fields"`
}

type deleteVehicleCommand struct {
	OperationID string `json:"operation_id"`
	VehicleID   string `json:"vehicle_id"`
	FleetID     string `json:"fleet_id"`
}

type updateVehicleStatusPayload struct {
	FleetID       string `json:"fleet_id"`
	Status        string `json:"status"`
	StatusMessage string `json:"status_message"`
}

type updateVehicleStatusCommand struct {
	OperationID string                     `json:"operation_id"`
	VehicleID   string                     `json:"vehicle_id"`
	Payload     updateVehicleStatusPayload `json:"payload"`
}

type updateVehicleLocationPayload struct {
	FleetID  string          `json:"fleet_id"`
	Location VehicleLocation `json:"location"`
}

type updateVehicleLocationCommand struct {
	OperationID string                       `json:"operation_id"`
	VehicleID   string                       `json:"vehicle_id"`
	Payload     updateVehicleLocationPayload `json:"payload"`
}

type createDriverCommand struct {
	OperationID string `json:"operation_id"`
	Driver      Driver `json:"driver"`
}

type updateDriverCommand struct {
	OperationID string                     `json:"operation_id"`
	DriverID    string                     `json:"driver_id"`
	FleetID     string                     `json:"fleet_id,omitempty"`
	Fields      map[string]json.RawMessage `json:"fields"`
}

type deleteDriverCommand struct {
	OperationID string `json:"operation_id"`
	DriverID    string `json:"driver_id"`
	FleetID     string `json:"fleet_id"`
}

type createEventCommand struct {
	OperationID string       `json:"operation_id"`
	Event       SpecialEvent `json:"event"`
}

type updateEventCommand struct {
	OperationID string                     `json:"operation_id"`
	EventID     string                     `json:"event_id"`
	Fields      map[string]json.RawMessage `json:"fields"`
}

type createNoticeCommand struct {
	OperationID string       `json:"operation_id"`
	Notice      DriverNotice `json:"notice"`
}

type markNoticeReadCommand struct {
	OperationID string `json:"operation_id"`
	NoticeID    string `json:"notice_id"`
}

type assignDriverPayload struct {
	FleetID     string `json:"fleet_id"`
	DriverID    string `json:"driver_id"`
	DriverName  string `json:"driver_name"`
	DriverPhone string `json:"driver_phone"`
}

type assignDriverCommand struct {
	OperationID string              `json:"operation_id"`
	VehicleID   string              `json:"vehicle_id"`
	Payload     assignDriverPayload `json:"payload"`
}

type unassignDriverPayload struct {
	FleetID string `json:"fleet_id"`
}

type unassignDriverCommand struct {
	OperationID string                `json:"operation_id"`
	VehicleID   string                `json:"vehicle_id"`
	Payload     unassignDriverPayload `json:"payload"`
}

type driverAccessRevokedEvent struct {
	FleetID  string `json:"fleet_id"`
	DriverID string `json:"driver_id"`
}

// Handler exposes fleet CRUD endpoints.
type Handler struct {
	store       FleetStore
	broker      *messaging.Broker
	policy      *tenantsvc.Policy
	ops         opsvc.Store
	notifyStore notifysvc.NotifStore
	userStore   authsvc.UserStore
}

// NewHandler creates fleet handlers.
func NewHandler(
	store FleetStore,
	broker *messaging.Broker,
	policy *tenantsvc.Policy,
	opsStore opsvc.Store,
	notifyStore notifysvc.NotifStore,
	userStore authsvc.UserStore,
) *Handler {
	return &Handler{
		store:       store,
		broker:      broker,
		policy:      policy,
		ops:         opsStore,
		notifyStore: notifyStore,
		userStore:   userStore,
	}
}

// Mount registers all fleet routes on the mux.
func (h *Handler) Mount(mux *http.ServeMux) {
	// Vehicles
	mux.HandleFunc("GET /api/v1/vehicles", h.ListVehicles)
	mux.HandleFunc("POST /api/v1/vehicles", h.CreateVehicle)
	mux.HandleFunc("GET /api/v1/vehicles/{id}", h.GetVehicle)
	mux.HandleFunc("PUT /api/v1/vehicles/{id}", h.UpdateVehicle)
	mux.HandleFunc("DELETE /api/v1/vehicles/{id}", h.DeleteVehicle)
	mux.HandleFunc("PUT /api/v1/vehicles/{id}/status", h.UpdateVehicleStatus)
	mux.HandleFunc("PUT /api/v1/vehicles/{id}/location", h.UpdateVehicleLocation)
	mux.HandleFunc("PUT /api/v1/vehicles/{id}/assign", h.AssignDriver)
	mux.HandleFunc("PUT /api/v1/vehicles/{id}/unassign", h.UnassignDriver)

	// Drivers
	mux.HandleFunc("GET /api/v1/drivers", h.ListDrivers)
	mux.HandleFunc("POST /api/v1/drivers", h.CreateDriver)
	mux.HandleFunc("GET /api/v1/drivers/{id}", h.GetDriver)
	mux.HandleFunc("PUT /api/v1/drivers/{id}", h.UpdateDriver)
	mux.HandleFunc("DELETE /api/v1/drivers/{id}", h.DeleteDriver)

	// Events
	mux.HandleFunc("GET /api/v1/events", h.ListEvents)
	mux.HandleFunc("POST /api/v1/events", h.CreateEvent)
	mux.HandleFunc("PUT /api/v1/events/{id}", h.UpdateEvent)

	// Notices
	mux.HandleFunc("GET /api/v1/notices", h.ListNotices)
	mux.HandleFunc("POST /api/v1/notices", h.CreateNotice)
	mux.HandleFunc("PUT /api/v1/notices/{id}/read", h.MarkNoticeRead)
}

func (h *Handler) SubscribeCommands() error {
	if err := h.subscribe(createVehicleCommandSubject, h.processCreateVehicleCommand); err != nil {
		return err
	}
	if err := h.subscribe(updateVehicleCommandSubject, h.processUpdateVehicleCommand); err != nil {
		return err
	}
	if err := h.subscribe(deleteVehicleCommandSubject, h.processDeleteVehicleCommand); err != nil {
		return err
	}
	if err := h.subscribe(updateVehicleStatusCommandSubject, h.processUpdateVehicleStatusCommand); err != nil {
		return err
	}
	if err := h.subscribe(updateVehicleLocationCommandSubject, h.processUpdateVehicleLocationCommand); err != nil {
		return err
	}
	if err := h.subscribe(createDriverCommandSubject, h.processCreateDriverCommand); err != nil {
		return err
	}
	if err := h.subscribe(updateDriverCommandSubject, h.processUpdateDriverCommand); err != nil {
		return err
	}
	if err := h.subscribe(deleteDriverCommandSubject, h.processDeleteDriverCommand); err != nil {
		return err
	}
	if err := h.subscribe(createEventCommandSubject, h.processCreateEventCommand); err != nil {
		return err
	}
	if err := h.subscribe(updateEventCommandSubject, h.processUpdateEventCommand); err != nil {
		return err
	}
	if err := h.subscribe(createNoticeCommandSubject, h.processCreateNoticeCommand); err != nil {
		return err
	}
	if err := h.subscribe(markNoticeReadCommandSubject, h.processMarkNoticeReadCommand); err != nil {
		return err
	}
	if err := h.subscribe(assignDriverCommandSubject, h.processAssignDriverCommand); err != nil {
		return err
	}
	return h.subscribe(unassignDriverCommandSubject, h.processUnassignDriverCommand)
}

// ---------------------------------------------------------------------------
// Vehicle handlers
// ---------------------------------------------------------------------------

func (h *Handler) ListVehicles(w http.ResponseWriter, r *http.Request) {
	queryValues := r.URL.Query()
	fleetID := strings.TrimSpace(queryValues.Get("fleet_id"))
	driverIDValues, hasDriverFilter := queryValues["driver_id"]
	driverID := ""
	if hasDriverFilter && len(driverIDValues) > 0 {
		driverID = strings.TrimSpace(driverIDValues[0])
	}
	routeID := strings.TrimSpace(queryValues.Get("route_id"))
	if routeID == "" {
		routeID = strings.TrimSpace(queryValues.Get("routeId"))
	}
	query := strings.ToLower(strings.TrimSpace(queryValues.Get("query")))
	limit, offset := parsePagination(r)
	identity := auth.IdentityFromContext(r.Context())
	if identity.Role == auth.RoleDriver && fleetID == "" {
		writeJSON(w, http.StatusBadRequest, errBody("fleet_id required for driver vehicle lookup"))
		return
	}
	if hasDriverFilter && driverID == "" {
		writeJSON(w, http.StatusBadRequest, errBody("driver_id cannot be empty"))
		return
	}

	var vehicles []Vehicle
	var err error
	if hasDriverFilter {
		vehicles, err = h.store.ListVehiclesForDriver(r.Context(), fleetID, driverID)
	} else {
		vehicles, err = h.store.ListVehicles(r.Context(), fleetID)
	}
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody(err.Error()))
		return
	}
	if vehicles == nil {
		vehicles = []Vehicle{}
	}
	if routeID != "" {
		filtered := make([]Vehicle, 0, len(vehicles))
		for _, vehicle := range vehicles {
			if strings.EqualFold(strings.TrimSpace(vehicle.CurrentRouteID), routeID) {
				filtered = append(filtered, vehicle)
			}
		}
		vehicles = filtered
	}
	if query != "" {
		filtered := make([]Vehicle, 0, len(vehicles))
		for _, vehicle := range vehicles {
			if matchesVehicleQuery(vehicle, query) {
				filtered = append(filtered, vehicle)
			}
		}
		vehicles = filtered
	}
	sort.Slice(vehicles, func(i, j int) bool {
		return vehicles[i].LastUpdated.After(vehicles[j].LastUpdated)
	})
	if h.shouldRedactVehicleDetails(r.Context(), identity, fleetID) {
		vehicles = sanitizeVehiclesForUnapprovedDriver(vehicles)
	}
	vehicles = paginate(vehicles, limit, offset)
	writeJSON(w, http.StatusOK, vehicles)
}

func (h *Handler) GetVehicle(w http.ResponseWriter, r *http.Request) {
	vehicleID := r.PathValue("id")
	fleetID := strings.TrimSpace(r.URL.Query().Get("fleet_id"))
	identity := auth.IdentityFromContext(r.Context())
	if identity.Role == auth.RoleDriver && fleetID == "" {
		writeJSON(w, http.StatusBadRequest, errBody("fleet_id required for driver vehicle lookup"))
		return
	}
	if fleetID == "" {
		// Try to find in any fleet by iterating.
		v := h.findVehicleAnyFleet(r, vehicleID)
		if v == nil {
			writeJSON(w, http.StatusNotFound, errBody("vehicle not found"))
			return
		}
		writeJSON(w, http.StatusOK, v)
		return
	}
	v, err := h.store.GetVehicle(r.Context(), fleetID, vehicleID)
	if err != nil {
		writeJSON(w, http.StatusNotFound, errBody("vehicle not found"))
		return
	}
	if h.shouldRedactVehicleDetails(r.Context(), identity, fleetID) {
		if !v.IsActive || strings.TrimSpace(v.DriverID) != "" {
			writeJSON(w, http.StatusNotFound, errBody("vehicle not found"))
			return
		}
		redacted := sanitizeVehicleForUnapprovedDriver(*v)
		writeJSON(w, http.StatusOK, redacted)
		return
	}
	writeJSON(w, http.StatusOK, v)
}

func (h *Handler) CreateVehicle(w http.ResponseWriter, r *http.Request) {
	var v Vehicle
	if err := json.NewDecoder(r.Body).Decode(&v); err != nil {
		writeJSON(w, http.StatusBadRequest, errBody("invalid json"))
		return
	}
	v.FleetID = strings.TrimSpace(v.FleetID)
	v.RegistrationNumber = strings.TrimSpace(v.RegistrationNumber)
	v.Nickname = strings.TrimSpace(v.Nickname)
	v.CurrentRouteID = strings.TrimSpace(v.CurrentRouteID)
	if v.FleetID == "" || v.RegistrationNumber == "" {
		writeJSON(
			w,
			http.StatusBadRequest,
			errBody("fleet_id and registration_number required"),
		)
		return
	}
	cmd := &createVehicleCommand{Vehicle: v}
	if err := h.enqueueOperation(
		r.Context(),
		createVehicleOperationType,
		"Vehicle creation accepted for async processing.",
		createVehicleCommandSubject,
		r.Header.Get("Idempotency-Key"),
		cmd,
	); err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody("queue publish failed"))
		return
	}
	writeJSON(w, http.StatusAccepted, opsvc.CommandAccepted{
		OperationID: cmd.OperationID,
		Status:      opsvc.StatusQueued,
		Message:     "Vehicle creation queued.",
	})
}

func (h *Handler) UpdateVehicle(w http.ResponseWriter, r *http.Request) {
	vehicleID := r.PathValue("id")

	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errBody("invalid body"))
		return
	}

	var update Vehicle
	if err := json.Unmarshal(bodyBytes, &update); err != nil {
		writeJSON(w, http.StatusBadRequest, errBody("invalid json"))
		return
	}

	var rawFields map[string]json.RawMessage
	_ = json.Unmarshal(bodyBytes, &rawFields)

	fleetID := update.FleetID
	if fleetID == "" {
		fleetID = r.URL.Query().Get("fleet_id")
	}
	cmd := &updateVehicleCommand{
		VehicleID: vehicleID,
		FleetID:   fleetID,
		Fields:    rawFields,
	}
	if err := h.enqueueOperation(
		r.Context(),
		updateVehicleOperationType,
		"Vehicle update accepted for async processing.",
		updateVehicleCommandSubject,
		r.Header.Get("Idempotency-Key"),
		cmd,
	); err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody("queue publish failed"))
		return
	}
	writeJSON(w, http.StatusAccepted, opsvc.CommandAccepted{
		OperationID: cmd.OperationID,
		Status:      opsvc.StatusQueued,
		Message:     "Vehicle update queued.",
	})
}

func (h *Handler) DeleteVehicle(w http.ResponseWriter, r *http.Request) {
	vehicleID := r.PathValue("id")
	fleetID := r.URL.Query().Get("fleet_id")
	if fleetID == "" {
		writeJSON(w, http.StatusBadRequest, errBody("fleet_id required"))
		return
	}
	cmd := &deleteVehicleCommand{
		VehicleID: vehicleID,
		FleetID:   fleetID,
	}
	if err := h.enqueueOperation(
		r.Context(),
		deleteVehicleOperationType,
		"Vehicle deletion accepted for async processing.",
		deleteVehicleCommandSubject,
		r.Header.Get("Idempotency-Key"),
		cmd,
	); err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody("queue publish failed"))
		return
	}
	writeJSON(w, http.StatusAccepted, opsvc.CommandAccepted{
		OperationID: cmd.OperationID,
		Status:      opsvc.StatusQueued,
		Message:     "Vehicle deletion queued.",
	})
}

func (h *Handler) UpdateVehicleStatus(w http.ResponseWriter, r *http.Request) {
	vehicleID := r.PathValue("id")
	var body updateVehicleStatusPayload
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, errBody("invalid json"))
		return
	}
	cmd := &updateVehicleStatusCommand{
		VehicleID: vehicleID,
		Payload:   body,
	}
	if err := h.enqueueOperation(
		r.Context(),
		updateVehicleStatusOperationType,
		"Vehicle status update accepted for async processing.",
		updateVehicleStatusCommandSubject,
		r.Header.Get("Idempotency-Key"),
		cmd,
	); err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody("queue publish failed"))
		return
	}
	writeJSON(w, http.StatusAccepted, opsvc.CommandAccepted{
		OperationID: cmd.OperationID,
		Status:      opsvc.StatusQueued,
		Message:     "Vehicle status update queued.",
	})
}

func (h *Handler) UpdateVehicleLocation(w http.ResponseWriter, r *http.Request) {
	vehicleID := r.PathValue("id")
	var body updateVehicleLocationPayload
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, errBody("invalid json"))
		return
	}
	cmd := &updateVehicleLocationCommand{
		VehicleID: vehicleID,
		Payload:   body,
	}
	if err := h.enqueueOperation(
		r.Context(),
		updateVehicleLocationOperationType,
		"Vehicle location update accepted for async processing.",
		updateVehicleLocationCommandSubject,
		r.Header.Get("Idempotency-Key"),
		cmd,
	); err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody("queue publish failed"))
		return
	}
	writeJSON(w, http.StatusAccepted, opsvc.CommandAccepted{
		OperationID: cmd.OperationID,
		Status:      opsvc.StatusQueued,
		Message:     "Vehicle location update queued.",
	})
}

func (h *Handler) AssignDriver(w http.ResponseWriter, r *http.Request) {
	vehicleID := r.PathValue("id")
	var body assignDriverPayload
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, errBody("invalid json"))
		return
	}
	if strings.TrimSpace(body.FleetID) == "" || strings.TrimSpace(body.DriverID) == "" {
		writeJSON(w, http.StatusBadRequest, errBody("fleet_id and driver_id required"))
		return
	}
	cmd := &assignDriverCommand{
		VehicleID: vehicleID,
		Payload:   body,
	}
	if err := h.enqueueOperation(
		r.Context(),
		assignDriverOperationType,
		"Vehicle assignment accepted for async processing.",
		assignDriverCommandSubject,
		r.Header.Get("Idempotency-Key"),
		cmd,
	); err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody("queue publish failed"))
		return
	}
	writeJSON(w, http.StatusAccepted, opsvc.CommandAccepted{
		OperationID: cmd.OperationID,
		Status:      opsvc.StatusQueued,
		Message:     "Vehicle assignment queued.",
	})
}

func (h *Handler) UnassignDriver(w http.ResponseWriter, r *http.Request) {
	vehicleID := r.PathValue("id")
	var body unassignDriverPayload
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, errBody("invalid json"))
		return
	}
	if strings.TrimSpace(body.FleetID) == "" {
		writeJSON(w, http.StatusBadRequest, errBody("fleet_id required"))
		return
	}

	cmd := &unassignDriverCommand{
		VehicleID: vehicleID,
		Payload:   body,
	}
	if err := h.enqueueOperation(
		r.Context(),
		unassignDriverOperationType,
		"Vehicle unassignment accepted for async processing.",
		unassignDriverCommandSubject,
		r.Header.Get("Idempotency-Key"),
		cmd,
	); err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody("queue publish failed"))
		return
	}
	writeJSON(w, http.StatusAccepted, opsvc.CommandAccepted{
		OperationID: cmd.OperationID,
		Status:      opsvc.StatusQueued,
		Message:     "Vehicle unassignment queued.",
	})
}

// ---------------------------------------------------------------------------
// Driver handlers
// ---------------------------------------------------------------------------

func (h *Handler) ListDrivers(w http.ResponseWriter, r *http.Request) {
	fleetID := r.URL.Query().Get("fleet_id")
	limit, offset := parsePagination(r)
	drivers, err := h.store.ListDrivers(r.Context(), fleetID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody(err.Error()))
		return
	}
	if drivers == nil {
		drivers = []Driver{}
	}
	drivers = paginate(drivers, limit, offset)
	writeJSON(w, http.StatusOK, drivers)
}

func (h *Handler) GetDriver(w http.ResponseWriter, r *http.Request) {
	driverID := r.PathValue("id")
	fleetID := r.URL.Query().Get("fleet_id")

	if fleetID == "" {
		d := h.findDriverAnyFleet(r, driverID)
		if d == nil {
			writeJSON(w, http.StatusNotFound, errBody("driver not found"))
			return
		}
		writeJSON(w, http.StatusOK, d)
		return
	}

	d, err := h.store.GetDriver(r.Context(), fleetID, driverID)
	if err != nil {
		writeJSON(w, http.StatusNotFound, errBody("driver not found"))
		return
	}
	writeJSON(w, http.StatusOK, d)
}

func (h *Handler) CreateDriver(w http.ResponseWriter, r *http.Request) {
	var d Driver
	if err := json.NewDecoder(r.Body).Decode(&d); err != nil {
		writeJSON(w, http.StatusBadRequest, errBody("invalid json"))
		return
	}
	if d.FleetID == "" || d.FullName == "" {
		writeJSON(w, http.StatusBadRequest, errBody("fleet_id and full_name required"))
		return
	}
	cmd := &createDriverCommand{Driver: d}
	if err := h.enqueueOperation(
		r.Context(),
		createDriverOperationType,
		"Driver creation accepted for async processing.",
		createDriverCommandSubject,
		r.Header.Get("Idempotency-Key"),
		cmd,
	); err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody("queue publish failed"))
		return
	}
	writeJSON(w, http.StatusAccepted, opsvc.CommandAccepted{
		OperationID: cmd.OperationID,
		Status:      opsvc.StatusQueued,
		Message:     "Driver creation queued.",
	})
}

func (h *Handler) UpdateDriver(w http.ResponseWriter, r *http.Request) {
	driverID := r.PathValue("id")

	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errBody("invalid body"))
		return
	}

	var update Driver
	if err := json.Unmarshal(bodyBytes, &update); err != nil {
		writeJSON(w, http.StatusBadRequest, errBody("invalid json"))
		return
	}

	var rawFields map[string]json.RawMessage
	_ = json.Unmarshal(bodyBytes, &rawFields)

	fleetID := update.FleetID
	if fleetID == "" {
		fleetID = r.URL.Query().Get("fleet_id")
	}
	cmd := &updateDriverCommand{
		DriverID: driverID,
		FleetID:  fleetID,
		Fields:   rawFields,
	}
	if err := h.enqueueOperation(
		r.Context(),
		updateDriverOperationType,
		"Driver update accepted for async processing.",
		updateDriverCommandSubject,
		r.Header.Get("Idempotency-Key"),
		cmd,
	); err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody("queue publish failed"))
		return
	}
	writeJSON(w, http.StatusAccepted, opsvc.CommandAccepted{
		OperationID: cmd.OperationID,
		Status:      opsvc.StatusQueued,
		Message:     "Driver update queued.",
	})
}

func (h *Handler) DeleteDriver(w http.ResponseWriter, r *http.Request) {
	driverID := r.PathValue("id")
	fleetID := r.URL.Query().Get("fleet_id")
	if fleetID == "" {
		writeJSON(w, http.StatusBadRequest, errBody("fleet_id required"))
		return
	}
	cmd := &deleteDriverCommand{
		DriverID: driverID,
		FleetID:  fleetID,
	}
	if err := h.enqueueOperation(
		r.Context(),
		deleteDriverOperationType,
		"Driver deletion accepted for async processing.",
		deleteDriverCommandSubject,
		r.Header.Get("Idempotency-Key"),
		cmd,
	); err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody("queue publish failed"))
		return
	}
	writeJSON(w, http.StatusAccepted, opsvc.CommandAccepted{
		OperationID: cmd.OperationID,
		Status:      opsvc.StatusQueued,
		Message:     "Driver deletion queued.",
	})
}

// ---------------------------------------------------------------------------
// Event handlers
// ---------------------------------------------------------------------------

func (h *Handler) ListEvents(w http.ResponseWriter, r *http.Request) {
	fleetID := r.URL.Query().Get("fleet_id")
	vehicleID := r.URL.Query().Get("vehicle_id")
	limit, offset := parsePagination(r)
	events, err := h.store.ListEvents(r.Context(), fleetID, vehicleID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody(err.Error()))
		return
	}
	if events == nil {
		events = []SpecialEvent{}
	}
	sort.Slice(events, func(i, j int) bool {
		return events[i].Timestamp.After(events[j].Timestamp)
	})
	events = paginate(events, limit, offset)
	writeJSON(w, http.StatusOK, events)
}

func (h *Handler) CreateEvent(w http.ResponseWriter, r *http.Request) {
	var e SpecialEvent
	if err := json.NewDecoder(r.Body).Decode(&e); err != nil {
		writeJSON(w, http.StatusBadRequest, errBody("invalid json"))
		return
	}
	if e.FleetID == "" || e.VehicleID == "" || e.Type == "" {
		writeJSON(w, http.StatusBadRequest, errBody("fleet_id, vehicle_id, and type required"))
		return
	}
	if e.ID == "" {
		e.ID = uuid.NewString()
	}
	cmd := &createEventCommand{Event: e}
	if err := h.enqueueOperation(
		r.Context(),
		createEventOperationType,
		"Event creation accepted for async processing.",
		createEventCommandSubject,
		r.Header.Get("Idempotency-Key"),
		cmd,
	); err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody("queue publish failed"))
		return
	}
	writeJSON(w, http.StatusAccepted, opsvc.CommandAccepted{
		OperationID: cmd.OperationID,
		Status:      opsvc.StatusQueued,
		Message:     "Event creation queued.",
	})
}

func (h *Handler) UpdateEvent(w http.ResponseWriter, r *http.Request) {
	eventID := r.PathValue("id")
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errBody("invalid body"))
		return
	}
	var update SpecialEvent
	if err := json.Unmarshal(bodyBytes, &update); err != nil {
		writeJSON(w, http.StatusBadRequest, errBody("invalid json"))
		return
	}
	var rawFields map[string]json.RawMessage
	_ = json.Unmarshal(bodyBytes, &rawFields)
	cmd := &updateEventCommand{
		EventID: eventID,
		Fields:  rawFields,
	}
	if err := h.enqueueOperation(
		r.Context(),
		updateEventOperationType,
		"Event update accepted for async processing.",
		updateEventCommandSubject,
		r.Header.Get("Idempotency-Key"),
		cmd,
	); err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody("queue publish failed"))
		return
	}
	writeJSON(w, http.StatusAccepted, opsvc.CommandAccepted{
		OperationID: cmd.OperationID,
		Status:      opsvc.StatusQueued,
		Message:     "Event update queued.",
	})
}

// ---------------------------------------------------------------------------
// Notice handlers
// ---------------------------------------------------------------------------

func (h *Handler) ListNotices(w http.ResponseWriter, r *http.Request) {
	fleetID := r.URL.Query().Get("fleet_id")
	vehicleID := r.URL.Query().Get("vehicle_id")
	driverID := r.URL.Query().Get("driver_id")
	limit, offset := parsePagination(r)
	notices, err := h.store.ListNotices(r.Context(), fleetID, vehicleID, driverID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody(err.Error()))
		return
	}
	if notices == nil {
		notices = []DriverNotice{}
	}
	sort.Slice(notices, func(i, j int) bool {
		return notices[i].Timestamp.After(notices[j].Timestamp)
	})
	notices = paginate(notices, limit, offset)
	writeJSON(w, http.StatusOK, notices)
}

func (h *Handler) CreateNotice(w http.ResponseWriter, r *http.Request) {
	var n DriverNotice
	if err := json.NewDecoder(r.Body).Decode(&n); err != nil {
		writeJSON(w, http.StatusBadRequest, errBody("invalid json"))
		return
	}
	if n.FleetID == "" || n.Title == "" {
		writeJSON(w, http.StatusBadRequest, errBody("fleet_id and title required"))
		return
	}
	if n.ID == "" {
		n.ID = uuid.NewString()
	}
	cmd := &createNoticeCommand{Notice: n}
	if err := h.enqueueOperation(
		r.Context(),
		createNoticeOperationType,
		"Notice creation accepted for async processing.",
		createNoticeCommandSubject,
		r.Header.Get("Idempotency-Key"),
		cmd,
	); err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody("queue publish failed"))
		return
	}
	writeJSON(w, http.StatusAccepted, opsvc.CommandAccepted{
		OperationID: cmd.OperationID,
		Status:      opsvc.StatusQueued,
		Message:     "Notice creation queued.",
	})
}

func (h *Handler) MarkNoticeRead(w http.ResponseWriter, r *http.Request) {
	noticeID := r.PathValue("id")
	cmd := &markNoticeReadCommand{NoticeID: noticeID}
	if err := h.enqueueOperation(
		r.Context(),
		markNoticeReadOperationType,
		"Notice read update accepted for async processing.",
		markNoticeReadCommandSubject,
		r.Header.Get("Idempotency-Key"),
		cmd,
	); err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody("queue publish failed"))
		return
	}
	writeJSON(w, http.StatusAccepted, opsvc.CommandAccepted{
		OperationID: cmd.OperationID,
		Status:      opsvc.StatusQueued,
		Message:     "Notice read update queued.",
	})
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func (h *Handler) fanoutVehicleChange(
	fleetID, vehicleID, event string,
	messageParts ...string,
) {
	if h.broker == nil || h.broker.NC == nil {
		return
	}
	subject := "fleet." + fleetID + ".vehicle." + vehicleID + ".ops." + event
	payload := map[string]string{
		"fleet_id":   fleetID,
		"vehicle_id": vehicleID,
		"event":      event,
		"timestamp":  time.Now().UTC().Format(time.RFC3339),
	}
	if len(messageParts) > 0 {
		normalizedMessage := strings.TrimSpace(messageParts[0])
		if normalizedMessage != "" {
			payload["message"] = normalizedMessage
		}
	}
	body, _ := json.Marshal(payload)
	if err := h.broker.Publish(subject, body); err != nil {
		log.Printf("[fleet] fanout %s: %v", subject, err)
	}
}

func (h *Handler) publishDriverAccessRevokedEvent(fleetID, driverID string) {
	if h.broker == nil || h.broker.NC == nil {
		return
	}

	normalizedFleetID := strings.TrimSpace(fleetID)
	normalizedDriverID := strings.TrimSpace(driverID)
	if normalizedFleetID == "" || normalizedDriverID == "" {
		return
	}

	payload, err := json.Marshal(driverAccessRevokedEvent{
		FleetID:  normalizedFleetID,
		DriverID: normalizedDriverID,
	})
	if err != nil {
		return
	}
	if err := h.broker.Publish(driverAccessRevokedEventSubject, payload); err != nil {
		log.Printf("[fleet] publish %s: %v", driverAccessRevokedEventSubject, err)
	}
}

func (h *Handler) subscribe(subject string, process func([]byte)) error {
	ch := make(chan []byte, 64)
	if _, err := h.broker.Subscribe(subject, ch); err != nil {
		return err
	}
	go func() {
		for payload := range ch {
			process(payload)
		}
	}()
	return nil
}

func (h *Handler) processAssignDriverCommand(payload []byte) {
	var cmd assignDriverCommand
	if err := json.Unmarshal(payload, &cmd); err != nil {
		log.Printf("[fleet] decode assign command: %v", err)
		return
	}
	ctx := context.Background()
	op := h.loadOperation(ctx, cmd.OperationID, assignDriverOperationType)
	h.markProcessing(ctx, op)

	v, err := h.applyAssignDriver(ctx, cmd.VehicleID, cmd.Payload)
	if err != nil {
		h.markFailed(ctx, op, "Failed to assign vehicle driver.", err)
		return
	}

	h.markSucceeded(ctx, op, v.ID, "Vehicle assignment completed.")
}

func (h *Handler) processCreateVehicleCommand(payload []byte) {
	var cmd createVehicleCommand
	if err := json.Unmarshal(payload, &cmd); err != nil {
		log.Printf("[fleet] decode create vehicle command: %v", err)
		return
	}
	ctx := context.Background()
	op := h.loadOperation(ctx, cmd.OperationID, createVehicleOperationType)
	h.markProcessing(ctx, op)

	v, err := h.applyCreateVehicle(ctx, cmd.Vehicle)
	if err != nil {
		h.markFailed(ctx, op, "Failed to create vehicle.", err)
		return
	}

	h.markSucceeded(ctx, op, v.ID, "Vehicle creation completed.")
}

func (h *Handler) processUpdateVehicleCommand(payload []byte) {
	var cmd updateVehicleCommand
	if err := json.Unmarshal(payload, &cmd); err != nil {
		log.Printf("[fleet] decode update vehicle command: %v", err)
		return
	}
	ctx := context.Background()
	op := h.loadOperation(ctx, cmd.OperationID, updateVehicleOperationType)
	h.markProcessing(ctx, op)

	v, err := h.applyUpdateVehicle(ctx, cmd.VehicleID, cmd.FleetID, cmd.Fields)
	if err != nil {
		h.markFailed(ctx, op, "Failed to update vehicle.", err)
		return
	}

	h.markSucceeded(ctx, op, v.ID, "Vehicle update completed.")
}

func (h *Handler) processDeleteVehicleCommand(payload []byte) {
	var cmd deleteVehicleCommand
	if err := json.Unmarshal(payload, &cmd); err != nil {
		log.Printf("[fleet] decode delete vehicle command: %v", err)
		return
	}
	ctx := context.Background()
	op := h.loadOperation(ctx, cmd.OperationID, deleteVehicleOperationType)
	h.markProcessing(ctx, op)

	if err := h.applyDeleteVehicle(ctx, cmd.VehicleID, cmd.FleetID); err != nil {
		h.markFailed(ctx, op, "Failed to delete vehicle.", err)
		return
	}

	h.markSucceeded(ctx, op, cmd.VehicleID, "Vehicle deletion completed.")
}

func (h *Handler) processCreateDriverCommand(payload []byte) {
	var cmd createDriverCommand
	if err := json.Unmarshal(payload, &cmd); err != nil {
		log.Printf("[fleet] decode create driver command: %v", err)
		return
	}
	ctx := context.Background()
	op := h.loadOperation(ctx, cmd.OperationID, createDriverOperationType)
	h.markProcessing(ctx, op)

	d, err := h.applyCreateDriver(ctx, cmd.Driver)
	if err != nil {
		h.markFailed(ctx, op, "Failed to create driver.", err)
		return
	}

	h.markSucceeded(ctx, op, d.ID, "Driver creation completed.")
}

func (h *Handler) processUpdateDriverCommand(payload []byte) {
	var cmd updateDriverCommand
	if err := json.Unmarshal(payload, &cmd); err != nil {
		log.Printf("[fleet] decode update driver command: %v", err)
		return
	}
	ctx := context.Background()
	op := h.loadOperation(ctx, cmd.OperationID, updateDriverOperationType)
	h.markProcessing(ctx, op)

	d, err := h.applyUpdateDriver(ctx, cmd.DriverID, cmd.FleetID, cmd.Fields)
	if err != nil {
		h.markFailed(ctx, op, "Failed to update driver.", err)
		return
	}

	h.markSucceeded(ctx, op, d.ID, "Driver update completed.")
}

func (h *Handler) processDeleteDriverCommand(payload []byte) {
	var cmd deleteDriverCommand
	if err := json.Unmarshal(payload, &cmd); err != nil {
		log.Printf("[fleet] decode delete driver command: %v", err)
		return
	}
	ctx := context.Background()
	op := h.loadOperation(ctx, cmd.OperationID, deleteDriverOperationType)
	h.markProcessing(ctx, op)

	if err := h.applyDeleteDriver(ctx, cmd.DriverID, cmd.FleetID); err != nil {
		h.markFailed(ctx, op, "Failed to delete driver.", err)
		return
	}

	h.markSucceeded(ctx, op, cmd.DriverID, "Driver deletion completed.")
}

func (h *Handler) processUpdateVehicleStatusCommand(payload []byte) {
	var cmd updateVehicleStatusCommand
	if err := json.Unmarshal(payload, &cmd); err != nil {
		log.Printf("[fleet] decode update vehicle status command: %v", err)
		return
	}
	ctx := context.Background()
	op := h.loadOperation(ctx, cmd.OperationID, updateVehicleStatusOperationType)
	h.markProcessing(ctx, op)

	v, err := h.applyUpdateVehicleStatus(ctx, cmd.VehicleID, cmd.Payload)
	if err != nil {
		h.markFailed(ctx, op, "Failed to update vehicle status.", err)
		return
	}

	h.markSucceeded(ctx, op, v.ID, "Vehicle status update completed.")
}

func (h *Handler) processUpdateVehicleLocationCommand(payload []byte) {
	var cmd updateVehicleLocationCommand
	if err := json.Unmarshal(payload, &cmd); err != nil {
		log.Printf("[fleet] decode update vehicle location command: %v", err)
		return
	}
	ctx := context.Background()
	op := h.loadOperation(ctx, cmd.OperationID, updateVehicleLocationOperationType)
	h.markProcessing(ctx, op)

	v, err := h.applyUpdateVehicleLocation(ctx, cmd.VehicleID, cmd.Payload)
	if err != nil {
		h.markFailed(ctx, op, "Failed to update vehicle location.", err)
		return
	}

	h.markSucceeded(ctx, op, v.ID, "Vehicle location update completed.")
}

func (h *Handler) processCreateEventCommand(payload []byte) {
	var cmd createEventCommand
	if err := json.Unmarshal(payload, &cmd); err != nil {
		log.Printf("[fleet] decode create event command: %v", err)
		return
	}
	ctx := context.Background()
	op := h.loadOperation(ctx, cmd.OperationID, createEventOperationType)
	h.markProcessing(ctx, op)

	e, err := h.applyCreateEvent(ctx, cmd.Event)
	if err != nil {
		h.markFailed(ctx, op, "Failed to create event.", err)
		return
	}

	h.markSucceeded(ctx, op, e.ID, "Event creation completed.")
}

func (h *Handler) processUpdateEventCommand(payload []byte) {
	var cmd updateEventCommand
	if err := json.Unmarshal(payload, &cmd); err != nil {
		log.Printf("[fleet] decode update event command: %v", err)
		return
	}
	ctx := context.Background()
	op := h.loadOperation(ctx, cmd.OperationID, updateEventOperationType)
	h.markProcessing(ctx, op)

	e, err := h.applyUpdateEvent(ctx, cmd.EventID, cmd.Fields)
	if err != nil {
		h.markFailed(ctx, op, "Failed to update event.", err)
		return
	}

	h.markSucceeded(ctx, op, e.ID, "Event update completed.")
}

func (h *Handler) processCreateNoticeCommand(payload []byte) {
	var cmd createNoticeCommand
	if err := json.Unmarshal(payload, &cmd); err != nil {
		log.Printf("[fleet] decode create notice command: %v", err)
		return
	}
	ctx := context.Background()
	op := h.loadOperation(ctx, cmd.OperationID, createNoticeOperationType)
	h.markProcessing(ctx, op)

	n, err := h.applyCreateNotice(ctx, cmd.Notice)
	if err != nil {
		h.markFailed(ctx, op, "Failed to create notice.", err)
		return
	}

	h.markSucceeded(ctx, op, n.ID, "Notice creation completed.")
}

func (h *Handler) processMarkNoticeReadCommand(payload []byte) {
	var cmd markNoticeReadCommand
	if err := json.Unmarshal(payload, &cmd); err != nil {
		log.Printf("[fleet] decode mark notice read command: %v", err)
		return
	}
	ctx := context.Background()
	op := h.loadOperation(ctx, cmd.OperationID, markNoticeReadOperationType)
	h.markProcessing(ctx, op)

	n, err := h.applyMarkNoticeRead(ctx, cmd.NoticeID)
	if err != nil {
		h.markFailed(ctx, op, "Failed to mark notice as read.", err)
		return
	}

	h.markSucceeded(ctx, op, n.ID, "Notice read update completed.")
}

func (h *Handler) processUnassignDriverCommand(payload []byte) {
	var cmd unassignDriverCommand
	if err := json.Unmarshal(payload, &cmd); err != nil {
		log.Printf("[fleet] decode unassign command: %v", err)
		return
	}
	ctx := context.Background()
	op := h.loadOperation(ctx, cmd.OperationID, unassignDriverOperationType)
	h.markProcessing(ctx, op)

	v, err := h.applyUnassignDriver(ctx, cmd.VehicleID, cmd.Payload)
	if err != nil {
		h.markFailed(ctx, op, "Failed to unassign vehicle driver.", err)
		return
	}

	h.markSucceeded(ctx, op, v.ID, "Vehicle unassignment completed.")
}

func (h *Handler) applyCreateVehicle(ctx context.Context, v Vehicle) (*Vehicle, error) {
	if v.ID == "" {
		v.ID = uuid.NewString()
	}
	if strings.TrimSpace(v.Nickname) == "" {
		v.Nickname = strings.TrimSpace(v.RegistrationNumber)
	}
	now := time.Now().UTC()
	v.CreatedAt = now
	v.LastUpdated = now
	if v.Status == "" {
		v.Status = "idle"
	}
	if h.policy != nil {
		if creator, ok := h.store.(VehicleLimitEnforcer); ok {
			view, err := h.policy.EnsureRealtimeAllowed(ctx, v.FleetID)
			if err != nil {
				return nil, err
			}
			vehicleLimit := 0
			if view.EffectiveStatus == tenantsvc.StatusTrial {
				vehicleLimit = view.VehicleLimit
			}
			if err := creator.CreateVehicleIfWithinLimit(ctx, &v, vehicleLimit); err != nil {
				if err == ErrVehicleLimitReached {
					return nil, &tenantsvc.PolicyError{
						HTTPStatus: http.StatusForbidden,
						Code:       "vehicle_limit_reached",
						Message:    ErrVehicleLimitReached.Error(),
					}
				}
				return nil, fmt.Errorf("create failed")
			}
			return &v, nil
		}
		existing, err := h.store.ListVehicles(ctx, v.FleetID)
		if err != nil {
			return nil, fmt.Errorf("quota lookup failed")
		}
		if _, err := h.policy.CheckVehicleCreate(ctx, v.FleetID, len(existing)); err != nil {
			return nil, err
		}
	}

	if err := h.store.PutVehicle(ctx, &v); err != nil {
		return nil, fmt.Errorf("create failed")
	}
	return &v, nil
}

func (h *Handler) applyUpdateVehicle(
	ctx context.Context,
	vehicleID string,
	fleetID string,
	rawFields map[string]json.RawMessage,
) (*Vehicle, error) {
	existing, err := h.store.GetVehicle(ctx, fleetID, vehicleID)
	if err != nil {
		return nil, fmt.Errorf("vehicle not found")
	}

	previousStatus := existing.Status
	previousIsActive := existing.IsActive
	statusTransitionRequested := false

	if raw, ok := rawFields["registration_number"]; ok {
		var value string
		if err := json.Unmarshal(raw, &value); err == nil && strings.TrimSpace(value) != "" {
			existing.RegistrationNumber = value
		}
	}
	if raw, ok := rawFields["nickname"]; ok {
		var value *string
		if err := json.Unmarshal(raw, &value); err == nil {
			if value == nil {
				existing.Nickname = ""
			} else {
				existing.Nickname = strings.TrimSpace(*value)
			}
		}
	}
	if raw, ok := rawFields["type"]; ok {
		var value string
		if err := json.Unmarshal(raw, &value); err == nil && strings.TrimSpace(value) != "" {
			existing.Type = value
		}
	}
	if raw, ok := rawFields["service_type"]; ok {
		var value string
		if err := json.Unmarshal(raw, &value); err == nil && strings.TrimSpace(value) != "" {
			existing.ServiceType = value
		}
	}
	if raw, ok := rawFields["status"]; ok {
		var value string
		if err := json.Unmarshal(raw, &value); err == nil && strings.TrimSpace(value) != "" {
			existing.Status = value
		}
		statusTransitionRequested = true
	}
	if raw, ok := rawFields["status_message"]; ok {
		var value string
		if err := json.Unmarshal(raw, &value); err == nil {
			existing.StatusMessage = value
		}
	}
	if raw, ok := rawFields["capacity"]; ok {
		var value int
		if err := json.Unmarshal(raw, &value); err == nil && value > 0 {
			existing.Capacity = value
		}
	}
	if raw, ok := rawFields["current_route_id"]; ok {
		var value *string
		if err := json.Unmarshal(raw, &value); err == nil {
			if value == nil {
				existing.CurrentRouteID = ""
			} else {
				existing.CurrentRouteID = *value
			}
		}
	}
	if raw, ok := rawFields["driver_id"]; ok {
		var value *string
		if err := json.Unmarshal(raw, &value); err == nil {
			if value == nil {
				existing.DriverID = ""
			} else {
				existing.DriverID = *value
			}
		}
	}
	if raw, ok := rawFields["driver_name"]; ok {
		var value *string
		if err := json.Unmarshal(raw, &value); err == nil {
			if value == nil {
				existing.DriverName = ""
			} else {
				existing.DriverName = *value
			}
		}
	}
	if raw, ok := rawFields["driver_phone"]; ok {
		var value *string
		if err := json.Unmarshal(raw, &value); err == nil {
			if value == nil {
				existing.DriverPhone = ""
			} else {
				existing.DriverPhone = *value
			}
		}
	}
	if raw, ok := rawFields["is_active"]; ok {
		var value bool
		if err := json.Unmarshal(raw, &value); err == nil {
			existing.IsActive = value
		}
		statusTransitionRequested = true
	}

	existing.LastUpdated = time.Now().UTC()
	if err := h.store.PutVehicle(ctx, existing); err != nil {
		return nil, fmt.Errorf("update failed")
	}
	if statusTransitionRequested &&
		(previousStatus != existing.Status || previousIsActive != existing.IsActive) {
		h.fanoutVehicleChange(
			existing.FleetID,
			existing.ID,
			"vehicle_status_changed",
			buildVehicleStatusMessage(existing),
		)
	}
	return existing, nil
}

func (h *Handler) applyDeleteVehicle(ctx context.Context, vehicleID string, fleetID string) error {
	if err := h.store.DeleteVehicle(ctx, fleetID, vehicleID); err != nil {
		return fmt.Errorf("vehicle not found")
	}
	return nil
}

func (h *Handler) applyCreateDriver(ctx context.Context, d Driver) (*Driver, error) {
	if d.ID == "" {
		d.ID = uuid.NewString()
	}
	now := time.Now().UTC()
	d.CreatedAt = now
	d.UpdatedAt = now
	d.IsActive = true
	if h.policy != nil {
		existing, err := h.store.ListDrivers(ctx, d.FleetID)
		if err != nil {
			return nil, fmt.Errorf("quota lookup failed")
		}
		if _, err := h.policy.CheckDriverCreate(ctx, d.FleetID, len(existing)); err != nil {
			return nil, err
		}
	}

	if err := h.store.PutDriver(ctx, &d); err != nil {
		return nil, fmt.Errorf("create failed")
	}
	return &d, nil
}

func (h *Handler) applyUpdateDriver(
	ctx context.Context,
	driverID string,
	fleetID string,
	rawFields map[string]json.RawMessage,
) (*Driver, error) {
	if strings.TrimSpace(fleetID) == "" {
		return nil, fmt.Errorf("fleet_id required")
	}
	existing, err := h.store.GetDriver(ctx, fleetID, driverID)
	if err != nil {
		return nil, fmt.Errorf("driver not found")
	}

	if raw, ok := rawFields["full_name"]; ok {
		var value string
		if err := json.Unmarshal(raw, &value); err == nil && strings.TrimSpace(value) != "" {
			existing.FullName = value
		}
	}
	if raw, ok := rawFields["email"]; ok {
		var value string
		if err := json.Unmarshal(raw, &value); err == nil && strings.TrimSpace(value) != "" {
			existing.Email = value
		}
	}
	if raw, ok := rawFields["phone"]; ok {
		var value *string
		if err := json.Unmarshal(raw, &value); err == nil {
			if value == nil {
				existing.Phone = ""
			} else {
				existing.Phone = *value
			}
		}
	}
	if raw, ok := rawFields["is_active"]; ok {
		var value bool
		if err := json.Unmarshal(raw, &value); err == nil {
			existing.IsActive = value
		}
	}
	if raw, ok := rawFields["assigned_vehicle_ids"]; ok {
		var value []string
		if err := json.Unmarshal(raw, &value); err == nil {
			existing.AssignedVehicleIDs = value
		}
	}

	existing.UpdatedAt = time.Now().UTC()
	if err := h.store.PutDriver(ctx, existing); err != nil {
		return nil, fmt.Errorf("update failed")
	}
	return existing, nil
}

func (h *Handler) applyDeleteDriver(ctx context.Context, driverID string, fleetID string) error {
	now := time.Now().UTC()
	driverEmail := ""
	if existingDriver, derr := h.store.GetDriver(ctx, fleetID, driverID); derr == nil {
		driverEmail = existingDriver.Email
	}
	assignedVehicles, err := h.store.ListVehiclesForDriver(ctx, fleetID, driverID)
	if err == nil {
		for i := range assignedVehicles {
			assigned := assignedVehicles[i]
			unassignmentMessage := buildDriverUnassignmentMessage(&assigned)
			assigned.DriverID = ""
			assigned.DriverName = ""
			assigned.DriverPhone = ""
			assigned.StatusMessage = "Driver left fleet"
			assigned.LastUpdated = now
			if err := h.store.PutVehicle(ctx, &assigned); err == nil {
				h.fanoutVehicleChange(
					assigned.FleetID,
					assigned.ID,
					"driver_unassigned",
					unassignmentMessage,
				)
			}
		}
	}

	if err := h.store.DeleteDriver(ctx, fleetID, driverID); err != nil {
		return fmt.Errorf("driver not found")
	}

	h.pushDriverNotificationToRecipients(
		ctx,
		h.resolveDriverNotificationRecipients(ctx, driverID, driverEmail),
		&notifysvc.Notification{
			FleetID: strings.TrimSpace(fleetID),
			Type:    "driver_access_update",
			Title:   "Fleet Access Removed",
			Body: "You are no longer attached to this fleet. " +
				"Choose a new owner and request access before operating again.",
			Data: map[string]string{
				"event_type": "driver_access_removed",
			},
		},
	)
	h.publishDriverAccessRevokedEvent(fleetID, driverID)
	return nil
}

func (h *Handler) applyUpdateVehicleStatus(
	ctx context.Context,
	vehicleID string,
	body updateVehicleStatusPayload,
) (*Vehicle, error) {
	v, err := h.store.GetVehicle(ctx, body.FleetID, vehicleID)
	if err != nil {
		return nil, fmt.Errorf("vehicle not found")
	}
	v.Status = body.Status
	if body.StatusMessage != "" {
		v.StatusMessage = body.StatusMessage
	}
	v.LastUpdated = time.Now().UTC()

	if err := h.store.PutVehicle(ctx, v); err != nil {
		return nil, fmt.Errorf("update failed")
	}
	h.fanoutVehicleChange(
		v.FleetID,
		v.ID,
		"vehicle_status_changed",
		buildVehicleStatusMessage(v),
	)
	return v, nil
}

func (h *Handler) applyUpdateVehicleLocation(
	ctx context.Context,
	vehicleID string,
	body updateVehicleLocationPayload,
) (*Vehicle, error) {
	v, err := h.store.GetVehicle(ctx, body.FleetID, vehicleID)
	if err != nil {
		return nil, fmt.Errorf("vehicle not found")
	}
	// Live location is transported through NATS/JetStream and cached there.
	// Keep the legacy endpoint backward compatible without writing GPS churn
	// into the durable vehicle record.
	return v, nil
}

func (h *Handler) applyCreateEvent(ctx context.Context, e SpecialEvent) (*SpecialEvent, error) {
	if e.ID == "" {
		e.ID = uuid.NewString()
	}
	if e.Timestamp.IsZero() {
		e.Timestamp = time.Now().UTC()
	}
	if err := h.store.PutEvent(ctx, &e); err != nil {
		return nil, fmt.Errorf("create failed")
	}
	h.fanoutVehicleChange(e.FleetID, e.VehicleID, "event_"+e.Type)
	return &e, nil
}

func (h *Handler) applyUpdateEvent(
	ctx context.Context,
	eventID string,
	rawFields map[string]json.RawMessage,
) (*SpecialEvent, error) {
	existing, err := h.store.GetEvent(ctx, eventID)
	if err != nil {
		return nil, fmt.Errorf("event not found")
	}
	if raw, ok := rawFields["metadata"]; ok {
		var value map[string]interface{}
		if err := json.Unmarshal(raw, &value); err == nil {
			existing.Metadata = value
		}
	}
	if raw, ok := rawFields["message"]; ok {
		var value string
		if err := json.Unmarshal(raw, &value); err == nil && value != "" {
			existing.Message = value
		}
	}
	if err := h.store.PutEvent(ctx, existing); err != nil {
		return nil, fmt.Errorf("update failed")
	}
	return existing, nil
}

func (h *Handler) applyCreateNotice(ctx context.Context, n DriverNotice) (*DriverNotice, error) {
	if n.ID == "" {
		n.ID = uuid.NewString()
	}
	if n.Timestamp.IsZero() {
		n.Timestamp = time.Now().UTC()
	}
	if err := h.store.PutNotice(ctx, &n); err != nil {
		return nil, fmt.Errorf("create failed")
	}
	return &n, nil
}

func (h *Handler) applyMarkNoticeRead(ctx context.Context, noticeID string) (*DriverNotice, error) {
	n, err := h.store.GetNotice(ctx, noticeID)
	if err != nil {
		return nil, fmt.Errorf("notice not found")
	}
	n.IsRead = true
	n.ReadAt = time.Now().UTC()
	if err := h.store.PutNotice(ctx, n); err != nil {
		return nil, fmt.Errorf("update failed")
	}
	return n, nil
}

func (h *Handler) applyAssignDriver(
	ctx context.Context,
	vehicleID string,
	body assignDriverPayload,
) (*Vehicle, error) {
	now := time.Now().UTC()
	v, err := h.store.GetVehicle(ctx, body.FleetID, vehicleID)
	if err != nil {
		return nil, fmt.Errorf("vehicle not found")
	}
	previousDriverID := strings.TrimSpace(v.DriverID)
	if previousDriverID != "" && !strings.EqualFold(previousDriverID, body.DriverID) {
		if previousDriver, derr := h.store.GetDriver(ctx, body.FleetID, previousDriverID); derr == nil {
			filtered := make([]string, 0, len(previousDriver.AssignedVehicleIDs))
			for _, id := range previousDriver.AssignedVehicleIDs {
				if !strings.EqualFold(strings.TrimSpace(id), vehicleID) {
					filtered = append(filtered, id)
				}
			}
			previousDriver.AssignedVehicleIDs = filtered
			previousDriver.UpdatedAt = time.Now().UTC()
			_ = h.store.PutDriver(ctx, previousDriver)
		}
	}

	driverDetails, derr := h.store.GetDriver(ctx, body.FleetID, body.DriverID)
	if derr != nil {
		return nil, fmt.Errorf("driver not found")
	}
	if strings.TrimSpace(body.DriverName) == "" {
		body.DriverName = driverDetails.FullName
	}
	if strings.TrimSpace(body.DriverPhone) == "" {
		body.DriverPhone = driverDetails.Phone
	}

	currentAssignments, err := h.store.ListVehiclesForDriver(ctx, body.FleetID, body.DriverID)
	if err == nil {
		for i := range currentAssignments {
			assigned := currentAssignments[i]
			if strings.EqualFold(strings.TrimSpace(assigned.ID), vehicleID) {
				continue
			}
			unassignmentMessage := buildDriverUnassignmentMessage(&assigned)
			assigned.DriverID = ""
			assigned.DriverName = ""
			assigned.DriverPhone = ""
			assigned.StatusMessage = "Driver moved to another vehicle"
			assigned.LastUpdated = now
			if err := h.store.PutVehicle(ctx, &assigned); err == nil {
				h.fanoutVehicleChange(
					assigned.FleetID,
					assigned.ID,
					"driver_unassigned",
					unassignmentMessage,
				)
			}
		}
	}

	v.DriverID = body.DriverID
	v.DriverName = body.DriverName
	v.DriverPhone = body.DriverPhone
	if strings.TrimSpace(v.StatusMessage) == "" ||
		strings.EqualFold(strings.TrimSpace(v.StatusMessage), "driver unassigned by owner") {
		v.StatusMessage = "Driver assigned"
	}
	v.LastUpdated = now

	if err := h.store.PutVehicle(ctx, v); err != nil {
		return nil, fmt.Errorf("update failed")
	}

	driverDetails.AssignedVehicleIDs = []string{vehicleID}
	driverDetails.UpdatedAt = now
	_ = h.store.PutDriver(ctx, driverDetails)

	assignmentMessage := "You can now operate " + vehicleDisplayLabel(v) + ". Driver controls are enabled again."
	_ = h.store.PutNotice(ctx, &DriverNotice{
		ID:        uuid.NewString(),
		Title:     "Vehicle Assignment",
		Message:   assignmentMessage,
		VehicleID: v.ID,
		DriverID:  body.DriverID,
		FleetID:   v.FleetID,
		Priority:  "high",
		IsRead:    false,
		Timestamp: now,
	})
	h.pushDriverNotificationToRecipients(
		ctx,
		h.resolveDriverNotificationRecipients(ctx, body.DriverID, driverDetails.Email),
		&notifysvc.Notification{
			FleetID:   strings.TrimSpace(v.FleetID),
			VehicleID: strings.TrimSpace(v.ID),
			Type:      "driver_access_update",
			Title:     "Vehicle Assignment",
			Body:      assignmentMessage,
			Data: map[string]string{
				"event_type": "driver_vehicle_assignment_approved",
				"vehicle_id": strings.TrimSpace(v.ID),
			},
		},
	)

	h.fanoutVehicleChange(
		v.FleetID,
		v.ID,
		"driver_assigned",
		buildDriverAssignmentMessage(v),
	)
	return v, nil
}

func (h *Handler) applyUnassignDriver(
	ctx context.Context,
	vehicleID string,
	body unassignDriverPayload,
) (*Vehicle, error) {
	v, err := h.store.GetVehicle(ctx, body.FleetID, vehicleID)
	if err != nil {
		return nil, fmt.Errorf("vehicle not found")
	}

	previousDriverID := strings.TrimSpace(v.DriverID)
	previousDriverEmail := ""
	unassignmentMessage := buildDriverUnassignmentMessage(v)
	if previousDriverID != "" {
		if previousDriver, derr := h.store.GetDriver(ctx, body.FleetID, previousDriverID); derr == nil {
			previousDriverEmail = previousDriver.Email
			filtered := make([]string, 0, len(previousDriver.AssignedVehicleIDs))
			for _, id := range previousDriver.AssignedVehicleIDs {
				if !strings.EqualFold(strings.TrimSpace(id), vehicleID) {
					filtered = append(filtered, id)
				}
			}
			previousDriver.AssignedVehicleIDs = filtered
			previousDriver.UpdatedAt = time.Now().UTC()
			_ = h.store.PutDriver(ctx, previousDriver)
		}
	}

	v.DriverID = ""
	v.DriverName = ""
	v.DriverPhone = ""
	v.StatusMessage = "Driver unassigned by owner"
	v.LastUpdated = time.Now().UTC()

	if err := h.store.PutVehicle(ctx, v); err != nil {
		return nil, fmt.Errorf("update failed")
	}

	if previousDriverID != "" {
		_ = h.store.PutNotice(ctx, &DriverNotice{
			ID:        uuid.NewString(),
			Title:     "Vehicle Unassigned",
			Message:   "You have been unassigned from " + vehicleDisplayLabel(v) + ". Trip controls are locked until a new assignment is made.",
			VehicleID: v.ID,
			DriverID:  previousDriverID,
			FleetID:   v.FleetID,
			Priority:  "high",
			IsRead:    false,
			Timestamp: v.LastUpdated,
		})
		h.pushDriverNotificationToRecipients(
			ctx,
			h.resolveDriverNotificationRecipients(ctx, previousDriverID, previousDriverEmail),
			&notifysvc.Notification{
				FleetID:   strings.TrimSpace(v.FleetID),
				VehicleID: strings.TrimSpace(v.ID),
				Type:      "driver_access_update",
				Title:     "Vehicle Unassigned",
				Body:      "You have been unassigned from " + vehicleDisplayLabel(v) + ". Trip controls are locked until a new assignment is made.",
				Data: map[string]string{
					"event_type": "driver_unassigned",
					"vehicle_id": strings.TrimSpace(v.ID),
				},
			},
		)
	}

	h.fanoutVehicleChange(
		v.FleetID,
		v.ID,
		"driver_unassigned",
		unassignmentMessage,
	)
	return v, nil
}

func (h *Handler) resolveDriverNotificationRecipients(
	ctx context.Context,
	driverID string,
	driverEmail string,
) []string {
	seen := map[string]struct{}{}
	recipients := make([]string, 0, 2)
	addRecipient := func(id string) {
		normalized := strings.TrimSpace(id)
		if normalized == "" {
			return
		}
		if _, ok := seen[normalized]; ok {
			return
		}
		seen[normalized] = struct{}{}
		recipients = append(recipients, normalized)
	}

	addRecipient(driverID)

	if h.userStore != nil {
		normalizedEmail := strings.ToLower(strings.TrimSpace(driverEmail))
		if normalizedEmail != "" {
			if user, err := h.userStore.GetUserByEmail(ctx, normalizedEmail); err == nil && user != nil {
				addRecipient(user.ID)
			}
		}
	}

	return recipients
}

func (h *Handler) pushDriverNotificationToRecipients(
	ctx context.Context,
	recipients []string,
	template *notifysvc.Notification,
) {
	if template == nil {
		return
	}
	for _, recipient := range recipients {
		data := map[string]string{}
		for key, value := range template.Data {
			data[key] = value
		}
		h.pushDriverNotification(ctx, &notifysvc.Notification{
			UserID:    recipient,
			FleetID:   strings.TrimSpace(template.FleetID),
			VehicleID: strings.TrimSpace(template.VehicleID),
			Type:      template.Type,
			Title:     template.Title,
			Body:      template.Body,
			Data:      data,
		})
	}
}

func (h *Handler) pushDriverNotification(ctx context.Context, n *notifysvc.Notification) {
	if h.notifyStore == nil || n == nil {
		return
	}

	n.UserID = strings.TrimSpace(n.UserID)
	if n.UserID == "" {
		return
	}
	if strings.TrimSpace(n.ID) == "" {
		n.ID = uuid.NewString()
	}
	if n.CreatedAt.IsZero() {
		n.CreatedAt = time.Now().UTC()
	}
	n.IsRead = false

	if err := h.notifyStore.Put(ctx, n); err != nil {
		log.Printf("[fleet] store driver notification for %s: %v", n.UserID, err)
		return
	}

	unread, err := h.notifyStore.CountUnread(ctx, n.UserID)
	if err != nil {
		unread = 0
	}

	payload := notifysvc.NotificationPayload{
		Action:       "new",
		Notification: n,
		UnreadCount:  unread,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return
	}

	if h.broker != nil {
		subject := "notify." + n.UserID
		if err := h.broker.Publish(subject, data); err != nil {
			log.Printf("[fleet] publish driver notification %s: %v", subject, err)
		}
	}
}

func (h *Handler) enqueueOperation(
	ctx context.Context,
	opType string,
	message string,
	subject string,
	rawIdempotencyKey string,
	command any,
) error {
	idempotencyKey := normalizeIdempotencyKey(opType, rawIdempotencyKey)
	if idempotencyKey != "" {
		if existing, err := h.ops.FindByIdempotencyKey(ctx, idempotencyKey); err == nil && existing != nil {
			switch cmd := command.(type) {
			case *createVehicleCommand:
				cmd.OperationID = existing.ID
			case *updateVehicleCommand:
				cmd.OperationID = existing.ID
			case *deleteVehicleCommand:
				cmd.OperationID = existing.ID
			case *updateVehicleStatusCommand:
				cmd.OperationID = existing.ID
			case *updateVehicleLocationCommand:
				cmd.OperationID = existing.ID
			case *createDriverCommand:
				cmd.OperationID = existing.ID
			case *updateDriverCommand:
				cmd.OperationID = existing.ID
			case *deleteDriverCommand:
				cmd.OperationID = existing.ID
			case *createEventCommand:
				cmd.OperationID = existing.ID
			case *updateEventCommand:
				cmd.OperationID = existing.ID
			case *createNoticeCommand:
				cmd.OperationID = existing.ID
			case *markNoticeReadCommand:
				cmd.OperationID = existing.ID
			case *assignDriverCommand:
				cmd.OperationID = existing.ID
			case *unassignDriverCommand:
				cmd.OperationID = existing.ID
			}
			return nil
		}
	}

	now := time.Now().UTC()
	opID := uuid.NewString()
	op := &opsvc.Operation{
		ID:             opID,
		Type:           opType,
		FleetID:        fleetIDFromFleetCommand(command),
		IdempotencyKey: idempotencyKey,
		Status:         opsvc.StatusQueued,
		Message:        message,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	if err := h.ops.Put(ctx, op); err != nil {
		return err
	}
	_ = opsvc.PublishUpdate(h.broker, op)

	switch cmd := command.(type) {
	case *createVehicleCommand:
		cmd.OperationID = opID
	case *updateVehicleCommand:
		cmd.OperationID = opID
	case *deleteVehicleCommand:
		cmd.OperationID = opID
	case *updateVehicleStatusCommand:
		cmd.OperationID = opID
	case *updateVehicleLocationCommand:
		cmd.OperationID = opID
	case *createDriverCommand:
		cmd.OperationID = opID
	case *updateDriverCommand:
		cmd.OperationID = opID
	case *deleteDriverCommand:
		cmd.OperationID = opID
	case *createEventCommand:
		cmd.OperationID = opID
	case *updateEventCommand:
		cmd.OperationID = opID
	case *createNoticeCommand:
		cmd.OperationID = opID
	case *markNoticeReadCommand:
		cmd.OperationID = opID
	case *assignDriverCommand:
		cmd.OperationID = opID
	case *unassignDriverCommand:
		cmd.OperationID = opID
	}

	payload, err := json.Marshal(command)
	if err != nil {
		h.markFailed(ctx, op, "Failed to queue command.", err)
		return err
	}
	if h.broker != nil {
		if err := h.broker.Publish(subject, payload); err != nil {
			h.markFailed(ctx, op, "Failed to publish command.", err)
			return err
		}
	}
	return nil
}

func (h *Handler) loadOperation(ctx context.Context, operationID, opType string) *opsvc.Operation {
	op, err := h.ops.Get(ctx, operationID)
	if err == nil {
		return op
	}
	now := time.Now().UTC()
	return &opsvc.Operation{
		ID:        operationID,
		Type:      opType,
		Status:    opsvc.StatusQueued,
		CreatedAt: now,
		UpdatedAt: now,
	}
}

func fleetIDFromFleetCommand(command any) string {
	switch cmd := command.(type) {
	case *createVehicleCommand:
		return strings.TrimSpace(cmd.Vehicle.FleetID)
	case *updateVehicleCommand:
		return strings.TrimSpace(cmd.FleetID)
	case *deleteVehicleCommand:
		return strings.TrimSpace(cmd.FleetID)
	case *updateVehicleStatusCommand:
		return strings.TrimSpace(cmd.Payload.FleetID)
	case *updateVehicleLocationCommand:
		return strings.TrimSpace(cmd.Payload.FleetID)
	case *createDriverCommand:
		return strings.TrimSpace(cmd.Driver.FleetID)
	case *updateDriverCommand:
		return strings.TrimSpace(cmd.FleetID)
	case *deleteDriverCommand:
		return strings.TrimSpace(cmd.FleetID)
	case *createEventCommand:
		return strings.TrimSpace(cmd.Event.FleetID)
	case *updateEventCommand:
		return ""
	case *createNoticeCommand:
		return strings.TrimSpace(cmd.Notice.FleetID)
	case *markNoticeReadCommand:
		return ""
	case *assignDriverCommand:
		return strings.TrimSpace(cmd.Payload.FleetID)
	case *unassignDriverCommand:
		return strings.TrimSpace(cmd.Payload.FleetID)
	default:
		return ""
	}
}

func (h *Handler) markProcessing(ctx context.Context, op *opsvc.Operation) {
	op.Status = opsvc.StatusProcessing
	op.UpdatedAt = time.Now().UTC()
	if err := h.ops.Put(ctx, op); err != nil {
		log.Printf("[fleet] persist processing operation %s: %v", op.ID, err)
	}
	_ = opsvc.PublishUpdate(h.broker, op)
}

func (h *Handler) markSucceeded(ctx context.Context, op *opsvc.Operation, resourceID, message string) {
	op.Status = opsvc.StatusSucceeded
	op.ResourceID = resourceID
	op.Message = message
	op.ErrorMessage = ""
	op.UpdatedAt = time.Now().UTC()
	if err := h.ops.Put(ctx, op); err != nil {
		log.Printf("[fleet] persist success operation %s: %v", op.ID, err)
	}
	_ = opsvc.PublishUpdate(h.broker, op)
}

func (h *Handler) markFailed(ctx context.Context, op *opsvc.Operation, message string, cause error) {
	op.Status = opsvc.StatusFailed
	op.Message = message
	if cause != nil {
		op.ErrorMessage = cause.Error()
	}
	op.UpdatedAt = time.Now().UTC()
	if err := h.ops.Put(ctx, op); err != nil {
		log.Printf("[fleet] persist failed operation %s: %v", op.ID, err)
	}
	_ = opsvc.PublishUpdate(h.broker, op)
}

func normalizeIdempotencyKey(opType, raw string) string {
	normalized := strings.TrimSpace(raw)
	if normalized == "" {
		return ""
	}
	return opType + ":" + normalized
}

func buildVehicleStatusMessage(v *Vehicle) string {
	if v == nil {
		return "Vehicle status updated"
	}
	vehicleLabel := vehicleDisplayLabel(v)
	statusLabel := humanizeToken(v.Status)
	normalizedMessage := strings.TrimSpace(v.StatusMessage)
	if normalizedMessage == "" || strings.EqualFold(normalizedMessage, statusLabel) {
		return fmt.Sprintf("%s status updated to %s", vehicleLabel, statusLabel)
	}
	return fmt.Sprintf("%s status updated to %s: %s", vehicleLabel, statusLabel, normalizedMessage)
}

func buildDriverAssignmentMessage(v *Vehicle) string {
	if v == nil {
		return "Driver assigned"
	}
	driverLabel := strings.TrimSpace(v.DriverName)
	if driverLabel == "" {
		driverLabel = strings.TrimSpace(v.DriverID)
	}
	if driverLabel == "" {
		driverLabel = "Driver"
	}
	return fmt.Sprintf("%s assigned to %s", driverLabel, vehicleDisplayLabel(v))
}

func buildDriverUnassignmentMessage(v *Vehicle) string {
	if v == nil {
		return "Driver unassigned"
	}
	driverLabel := strings.TrimSpace(v.DriverName)
	if driverLabel == "" {
		driverLabel = strings.TrimSpace(v.DriverID)
	}
	if driverLabel == "" {
		driverLabel = "Driver"
	}
	return fmt.Sprintf("%s unassigned from %s", driverLabel, vehicleDisplayLabel(v))
}

func vehicleDisplayLabel(v *Vehicle) string {
	if v == nil {
		return "vehicle"
	}
	baseLabel := strings.TrimSpace(v.RegistrationNumber)
	if baseLabel == "" {
		baseLabel = strings.TrimSpace(v.ID)
	}
	if baseLabel == "" {
		baseLabel = "vehicle"
	}
	routeID := strings.TrimSpace(v.CurrentRouteID)
	if routeID == "" {
		return strings.ToUpper(baseLabel)
	}
	return fmt.Sprintf("%s (%s)", strings.ToUpper(baseLabel), routeID)
}

func humanizeToken(value string) string {
	normalized := strings.TrimSpace(value)
	if normalized == "" {
		return "updated"
	}
	normalized = strings.ReplaceAll(normalized, "_", " ")
	parts := strings.Fields(normalized)
	for i, part := range parts {
		if part == "" {
			continue
		}
		lower := strings.ToLower(part)
		parts[i] = strings.ToUpper(lower[:1]) + lower[1:]
	}
	return strings.Join(parts, " ")
}

func (h *Handler) findVehicleAnyFleet(r *http.Request, vehicleID string) *Vehicle {
	v, err := h.store.GetVehicleByID(r.Context(), vehicleID)
	if err != nil {
		return nil
	}
	return v
}

func (h *Handler) findDriverAnyFleet(r *http.Request, driverID string) *Driver {
	drivers, err := h.store.ListDrivers(r.Context(), "")
	if err != nil {
		return nil
	}
	for i := range drivers {
		if strings.EqualFold(drivers[i].ID, driverID) {
			driver := drivers[i]
			return &driver
		}
	}
	return nil
}

func contains(ss []string, s string) bool {
	for _, v := range ss {
		if strings.EqualFold(v, s) {
			return true
		}
	}
	return false
}

func matchesVehicleQuery(vehicle Vehicle, query string) bool {
	if query == "" {
		return true
	}
	haystack := strings.ToLower(strings.Join([]string{
		vehicle.ID,
		vehicle.RegistrationNumber,
		vehicle.Nickname,
		vehicle.CurrentRouteID,
		vehicle.DriverName,
		vehicle.DriverPhone,
		vehicle.Status,
		vehicle.StatusMessage,
		vehicle.Type,
		vehicle.ServiceType,
	}, " "))
	return strings.Contains(haystack, query)
}

func (h *Handler) shouldRedactVehicleDetails(
	ctx context.Context,
	identity auth.Identity,
	fleetID string,
) bool {
	if identity.Role != auth.RoleDriver {
		return false
	}
	if strings.TrimSpace(fleetID) == "" {
		return true
	}
	_, err := h.store.GetDriver(ctx, fleetID, strings.TrimSpace(identity.UserID))
	return err != nil
}

func sanitizeVehiclesForUnapprovedDriver(vehicles []Vehicle) []Vehicle {
	filtered := make([]Vehicle, 0, len(vehicles))
	for _, vehicle := range vehicles {
		if !vehicle.IsActive {
			continue
		}
		if strings.TrimSpace(vehicle.DriverID) != "" {
			continue
		}
		filtered = append(filtered, sanitizeVehicleForUnapprovedDriver(vehicle))
	}
	return filtered
}

func sanitizeVehicleForUnapprovedDriver(vehicle Vehicle) Vehicle {
	return Vehicle{
		ID:          vehicle.ID,
		Nickname:    vehicle.DiscoveryLabel(),
		Type:        vehicle.Type,
		ServiceType: vehicle.ServiceType,
		IsActive:    vehicle.IsActive,
		Status:      vehicle.Status,
		LastUpdated: vehicle.LastUpdated,
		CreatedAt:   vehicle.CreatedAt,
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func errBody(msg string) map[string]string {
	return map[string]string{"error": msg}
}

func writePolicyError(w http.ResponseWriter, err error) {
	if pe, ok := tenantsvc.AsPolicyError(err); ok {
		body := map[string]string{
			"error": pe.Message,
			"code":  pe.Code,
		}
		if pe.PublicMessage != "" {
			body["public_message"] = pe.PublicMessage
		}
		writeJSON(w, pe.HTTPStatus, body)
		return
	}
	writeJSON(w, http.StatusBadRequest, errBody(err.Error()))
}

// parsePagination extracts limit/offset from query params.
// Defaults: limit=100, offset=0. Max limit=500.
func parsePagination(r *http.Request) (limit, offset int) {
	limit = 100
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}
	if limit > 500 {
		limit = 500
	}
	if v := r.URL.Query().Get("offset"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			offset = n
		}
	}
	return
}

// paginate applies offset/limit to an already-sorted slice.
func paginate[T any](items []T, limit, offset int) []T {
	if offset >= len(items) {
		return items[:0]
	}
	items = items[offset:]
	if limit < len(items) {
		items = items[:limit]
	}
	return items
}
