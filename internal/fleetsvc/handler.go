package fleetsvc

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"via-backend/internal/messaging"
)

// Handler exposes fleet CRUD endpoints.
type Handler struct {
	store  FleetStore
	broker *messaging.Broker
}

// NewHandler creates fleet handlers.
func NewHandler(store FleetStore, broker *messaging.Broker) *Handler {
	return &Handler{store: store, broker: broker}
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

	if err := h.store.PutVehicle(r.Context(), &v); err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody("create failed"))
		return
	}
	h.fanoutVehicleChange(v.FleetID, v.ID, "vehicle_created")
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
	if update.StatusMessage != "" {
		existing.StatusMessage = update.StatusMessage
	}
	if update.Capacity > 0 {
		existing.Capacity = update.Capacity
	}
	// Only update IsActive if explicitly present in the request body.
	if _, ok := rawFields["is_active"]; ok {
		existing.IsActive = update.IsActive
	}
	existing.LastUpdated = time.Now().UTC()

	if err := h.store.PutVehicle(r.Context(), existing); err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody("update failed"))
		return
	}
	h.fanoutVehicleChange(existing.FleetID, existing.ID, "vehicle_updated")
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
	h.fanoutVehicleChange(fleetID, vehicleID, "vehicle_deleted")
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
	h.fanoutVehicleChange(v.FleetID, v.ID, "vehicle_status_changed")
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
	var body struct {
		FleetID     string `json:"fleet_id"`
		DriverID    string `json:"driver_id"`
		DriverName  string `json:"driver_name"`
		DriverPhone string `json:"driver_phone"`
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
	v.DriverID = body.DriverID
	v.DriverName = body.DriverName
	v.DriverPhone = body.DriverPhone
	v.LastUpdated = time.Now().UTC()

	if err := h.store.PutVehicle(r.Context(), v); err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody("update failed"))
		return
	}

	// Also update the driver's assigned vehicles.
	if body.DriverID != "" {
		d, derr := h.store.GetDriver(r.Context(), body.FleetID, body.DriverID)
		if derr == nil {
			if !contains(d.AssignedVehicleIDs, vehicleID) {
				d.AssignedVehicleIDs = append(d.AssignedVehicleIDs, vehicleID)
				d.UpdatedAt = time.Now().UTC()
				_ = h.store.PutDriver(r.Context(), d)
			}
		}
	}

	h.fanoutVehicleChange(v.FleetID, v.ID, "driver_assigned")
	writeJSON(w, http.StatusOK, v)
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

func (h *Handler) fanoutVehicleChange(fleetID, vehicleID, event string) {
	subject := "fleet." + fleetID + ".vehicle." + vehicleID + ".ops." + event
	body, _ := json.Marshal(map[string]string{
		"fleet_id":   fleetID,
		"vehicle_id": vehicleID,
		"event":      event,
		"timestamp":  time.Now().UTC().Format(time.RFC3339),
	})
	if err := h.broker.Publish(subject, body); err != nil {
		log.Printf("[fleet] fanout %s: %v", subject, err)
	}
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
