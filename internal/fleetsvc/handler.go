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
	assignDriverCommandSubject   = "cmd.fleet.vehicle.assign_driver"
	unassignDriverCommandSubject = "cmd.fleet.vehicle.unassign_driver"

	assignDriverOperationType   = "vehicle.assign_driver"
	unassignDriverOperationType = "vehicle.unassign_driver"
)

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
	if v.ID == "" {
		v.ID = uuid.New().String()
	}
	now := time.Now().UTC()
	v.CreatedAt = now
	v.LastUpdated = now
	if v.Status == "" {
		v.Status = "idle"
	}
	if h.policy != nil {
		existing, err := h.store.ListVehicles(r.Context(), v.FleetID)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, errBody("quota lookup failed"))
			return
		}
		if _, err := h.policy.CheckVehicleCreate(r.Context(), v.FleetID, len(existing)); err != nil {
			writePolicyError(w, err)
			return
		}
	}

	if err := h.store.PutVehicle(r.Context(), &v); err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody("create failed"))
		return
	}
	writeJSON(w, http.StatusCreated, v)
}

func (h *Handler) UpdateVehicle(w http.ResponseWriter, r *http.Request) {
	vehicleID := r.PathValue("id")

	// Read body once so we can both decode the struct and check raw keys.
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

	// Check which keys were explicitly sent.
	var rawFields map[string]json.RawMessage
	_ = json.Unmarshal(bodyBytes, &rawFields)

	fleetID := update.FleetID
	if fleetID == "" {
		fleetID = r.URL.Query().Get("fleet_id")
	}

	existing, err := h.store.GetVehicle(r.Context(), fleetID, vehicleID)
	if err != nil {
		writeJSON(w, http.StatusNotFound, errBody("vehicle not found"))
		return
	}

	// Merge fields.
	previousStatus := existing.Status
	previousIsActive := existing.IsActive
	statusTransitionRequested := false
	if update.RegistrationNumber != "" {
		existing.RegistrationNumber = update.RegistrationNumber
	}
	if update.Type != "" {
		existing.Type = update.Type
	}
	if update.ServiceType != "" {
		existing.ServiceType = update.ServiceType
	}
	if update.Status != "" {
		existing.Status = update.Status
	}
	if _, ok := rawFields["status_message"]; ok {
		existing.StatusMessage = update.StatusMessage
	}
	if _, ok := rawFields["status"]; ok {
		statusTransitionRequested = true
	}
	if update.Capacity > 0 {
		existing.Capacity = update.Capacity
	}
	if _, ok := rawFields["current_route_id"]; ok {
		existing.CurrentRouteID = update.CurrentRouteID
	}
	if _, ok := rawFields["driver_id"]; ok {
		existing.DriverID = update.DriverID
	}
	if _, ok := rawFields["driver_name"]; ok {
		existing.DriverName = update.DriverName
	}
	if _, ok := rawFields["driver_phone"]; ok {
		existing.DriverPhone = update.DriverPhone
	}
	// Only update IsActive if explicitly present in the request body.
	if _, ok := rawFields["is_active"]; ok {
		existing.IsActive = update.IsActive
		statusTransitionRequested = true
	}
	existing.LastUpdated = time.Now().UTC()

	if err := h.store.PutVehicle(r.Context(), existing); err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody("update failed"))
		return
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
	writeJSON(w, http.StatusOK, existing)
}

func (h *Handler) DeleteVehicle(w http.ResponseWriter, r *http.Request) {
	vehicleID := r.PathValue("id")
	fleetID := r.URL.Query().Get("fleet_id")
	if fleetID == "" {
		writeJSON(w, http.StatusBadRequest, errBody("fleet_id required"))
		return
	}
	if err := h.store.DeleteVehicle(r.Context(), fleetID, vehicleID); err != nil {
		writeJSON(w, http.StatusNotFound, errBody("vehicle not found"))
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "deleted"})
}

func (h *Handler) UpdateVehicleStatus(w http.ResponseWriter, r *http.Request) {
	vehicleID := r.PathValue("id")
	var body struct {
		FleetID       string `json:"fleet_id"`
		Status        string `json:"status"`
		StatusMessage string `json:"status_message"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, errBody("invalid json"))
		return
	}

	v, err := h.store.GetVehicle(r.Context(), body.FleetID, vehicleID)
	if err != nil {
		writeJSON(w, http.StatusNotFound, errBody("vehicle not found"))
		return
	}
	v.Status = body.Status
	if body.StatusMessage != "" {
		v.StatusMessage = body.StatusMessage
	}
	v.LastUpdated = time.Now().UTC()

	if err := h.store.PutVehicle(r.Context(), v); err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody("update failed"))
		return
	}
	h.fanoutVehicleChange(
		v.FleetID,
		v.ID,
		"vehicle_status_changed",
		buildVehicleStatusMessage(v),
	)
	writeJSON(w, http.StatusOK, v)
}

func (h *Handler) UpdateVehicleLocation(w http.ResponseWriter, r *http.Request) {
	vehicleID := r.PathValue("id")
	var body struct {
		FleetID  string          `json:"fleet_id"`
		Location VehicleLocation `json:"location"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, errBody("invalid json"))
		return
	}

	v, err := h.store.GetVehicle(r.Context(), body.FleetID, vehicleID)
	if err != nil {
		writeJSON(w, http.StatusNotFound, errBody("vehicle not found"))
		return
	}
	v.CurrentLocation = &body.Location
	v.LastUpdated = time.Now().UTC()

	if err := h.store.PutVehicle(r.Context(), v); err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody("update failed"))
		return
	}
	writeJSON(w, http.StatusOK, v)
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
	if d.ID == "" {
		d.ID = uuid.New().String()
	}
	now := time.Now().UTC()
	d.CreatedAt = now
	d.UpdatedAt = now
	d.IsActive = true
	if h.policy != nil {
		existing, err := h.store.ListDrivers(r.Context(), d.FleetID)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, errBody("quota lookup failed"))
			return
		}
		if _, err := h.policy.CheckDriverCreate(r.Context(), d.FleetID, len(existing)); err != nil {
			writePolicyError(w, err)
			return
		}
	}

	if err := h.store.PutDriver(r.Context(), &d); err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody("create failed"))
		return
	}
	writeJSON(w, http.StatusCreated, d)
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

	var existing *Driver
	if fleetID == "" {
		existing = h.findDriverAnyFleet(r, driverID)
		if existing == nil {
			writeJSON(w, http.StatusNotFound, errBody("driver not found"))
			return
		}
		fleetID = existing.FleetID
	} else {
		existing, err = h.store.GetDriver(r.Context(), fleetID, driverID)
		if err != nil {
			writeJSON(w, http.StatusNotFound, errBody("driver not found"))
			return
		}
	}

	if update.FullName != "" {
		existing.FullName = update.FullName
	}
	if update.Email != "" {
		existing.Email = update.Email
	}
	if update.Phone != "" {
		existing.Phone = update.Phone
	}
	// Only update IsActive if explicitly present in the request body.
	if _, ok := rawFields["is_active"]; ok {
		existing.IsActive = update.IsActive
	}
	if _, ok := rawFields["assigned_vehicle_ids"]; ok {
		existing.AssignedVehicleIDs = update.AssignedVehicleIDs
	}
	existing.UpdatedAt = time.Now().UTC()

	if err := h.store.PutDriver(r.Context(), existing); err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody("update failed"))
		return
	}
	writeJSON(w, http.StatusOK, existing)
}

func (h *Handler) DeleteDriver(w http.ResponseWriter, r *http.Request) {
	driverID := r.PathValue("id")
	fleetID := r.URL.Query().Get("fleet_id")
	if fleetID == "" {
		existing := h.findDriverAnyFleet(r, driverID)
		if existing == nil {
			writeJSON(w, http.StatusNotFound, errBody("driver not found"))
			return
		}
		fleetID = existing.FleetID
	}
	if err := h.store.DeleteDriver(r.Context(), fleetID, driverID); err != nil {
		writeJSON(w, http.StatusNotFound, errBody("driver not found"))
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "deleted"})
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
		e.ID = uuid.New().String()
	}
	if e.Timestamp.IsZero() {
		e.Timestamp = time.Now().UTC()
	}
	if err := h.store.PutEvent(r.Context(), &e); err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody("create failed"))
		return
	}
	h.fanoutVehicleChange(e.FleetID, e.VehicleID, "event_"+e.Type)
	writeJSON(w, http.StatusCreated, e)
}

func (h *Handler) UpdateEvent(w http.ResponseWriter, r *http.Request) {
	eventID := r.PathValue("id")
	existing, err := h.store.GetEvent(r.Context(), eventID)
	if err != nil {
		writeJSON(w, http.StatusNotFound, errBody("event not found"))
		return
	}
	var update SpecialEvent
	if err := json.NewDecoder(r.Body).Decode(&update); err != nil {
		writeJSON(w, http.StatusBadRequest, errBody("invalid json"))
		return
	}
	if update.Metadata != nil {
		existing.Metadata = update.Metadata
	}
	if update.Message != "" {
		existing.Message = update.Message
	}
	if err := h.store.PutEvent(r.Context(), existing); err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody("update failed"))
		return
	}
	writeJSON(w, http.StatusOK, existing)
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
		n.ID = uuid.New().String()
	}
	if n.Timestamp.IsZero() {
		n.Timestamp = time.Now().UTC()
	}
	if err := h.store.PutNotice(r.Context(), &n); err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody("create failed"))
		return
	}
	writeJSON(w, http.StatusCreated, n)
}

func (h *Handler) MarkNoticeRead(w http.ResponseWriter, r *http.Request) {
	noticeID := r.PathValue("id")
	n, err := h.store.GetNotice(r.Context(), noticeID)
	if err != nil {
		writeJSON(w, http.StatusNotFound, errBody("notice not found"))
		return
	}
	n.IsRead = true
	n.ReadAt = time.Now().UTC()
	if err := h.store.PutNotice(r.Context(), n); err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody("update failed"))
		return
	}
	writeJSON(w, http.StatusOK, n)
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
	command any,
) error {
	now := time.Now().UTC()
	opID := uuid.NewString()
	op := &opsvc.Operation{
		ID:        opID,
		Type:      opType,
		Status:    opsvc.StatusQueued,
		Message:   message,
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := h.ops.Put(ctx, op); err != nil {
		return err
	}

	switch cmd := command.(type) {
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
