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

	"via-backend/internal/messaging"
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

// Handler exposes fleet CRUD endpoints.
type Handler struct {
	store  FleetStore
	broker *messaging.Broker
	policy *tenantsvc.Policy
	ops    opsvc.Store
}

// NewHandler creates fleet handlers.
func NewHandler(
	store FleetStore,
	broker *messaging.Broker,
	policy *tenantsvc.Policy,
	opsStore opsvc.Store,
) *Handler {
	return &Handler{store: store, broker: broker, policy: policy, ops: opsStore}
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
	fleetID := r.URL.Query().Get("fleet_id")
	driverID := r.URL.Query().Get("driver_id")
	limit, offset := parsePagination(r)

	var vehicles []Vehicle
	var err error
	if driverID != "" {
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
	sort.Slice(vehicles, func(i, j int) bool {
		return vehicles[i].LastUpdated.After(vehicles[j].LastUpdated)
	})
	vehicles = paginate(vehicles, limit, offset)
	writeJSON(w, http.StatusOK, vehicles)
}

func (h *Handler) GetVehicle(w http.ResponseWriter, r *http.Request) {
	vehicleID := r.PathValue("id")
	fleetID := r.URL.Query().Get("fleet_id")
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
	writeJSON(w, http.StatusOK, v)
}

func (h *Handler) CreateVehicle(w http.ResponseWriter, r *http.Request) {
	var v Vehicle
	if err := json.NewDecoder(r.Body).Decode(&v); err != nil {
		writeJSON(w, http.StatusBadRequest, errBody("invalid json"))
		return
	}
	if v.FleetID == "" || v.RegistrationNumber == "" {
		writeJSON(w, http.StatusBadRequest, errBody("fleet_id and registration_number required"))
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
	now := time.Now().UTC()
	v.CreatedAt = now
	v.LastUpdated = now
	if v.Status == "" {
		v.Status = "idle"
	}
	if h.policy != nil {
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
	if err := h.store.DeleteDriver(ctx, fleetID, driverID); err != nil {
		return fmt.Errorf("driver not found")
	}
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
	v.CurrentLocation = &body.Location
	v.LastUpdated = time.Now().UTC()

	if err := h.store.PutVehicle(ctx, v); err != nil {
		return nil, fmt.Errorf("update failed")
	}
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

	v.DriverID = body.DriverID
	v.DriverName = body.DriverName
	v.DriverPhone = body.DriverPhone
	if strings.TrimSpace(v.StatusMessage) == "" ||
		strings.EqualFold(strings.TrimSpace(v.StatusMessage), "driver unassigned by owner") {
		v.StatusMessage = "Driver assigned"
	}
	v.LastUpdated = time.Now().UTC()

	if err := h.store.PutVehicle(ctx, v); err != nil {
		return nil, fmt.Errorf("update failed")
	}

	if !contains(driverDetails.AssignedVehicleIDs, vehicleID) {
		driverDetails.AssignedVehicleIDs = append(driverDetails.AssignedVehicleIDs, vehicleID)
		driverDetails.UpdatedAt = time.Now().UTC()
		_ = h.store.PutDriver(ctx, driverDetails)
	}

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
	unassignmentMessage := buildDriverUnassignmentMessage(v)
	if previousDriverID != "" {
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

	v.DriverID = ""
	v.DriverName = ""
	v.DriverPhone = ""
	v.StatusMessage = "Driver unassigned by owner"
	v.LastUpdated = time.Now().UTC()

	if err := h.store.PutVehicle(ctx, v); err != nil {
		return nil, fmt.Errorf("update failed")
	}

	h.fanoutVehicleChange(
		v.FleetID,
		v.ID,
		"driver_unassigned",
		unassignmentMessage,
	)
	return v, nil
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
	if err := h.broker.Publish(subject, payload); err != nil {
		h.markFailed(ctx, op, "Failed to publish command.", err)
		return err
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
