package requestsvc

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

	"via-backend/internal/auth"
	"via-backend/internal/authsvc"
	"via-backend/internal/fleetsvc"
	"via-backend/internal/messaging"
	"via-backend/internal/notifysvc"
	"via-backend/internal/opsvc"
)

const (
	driverRequestCreateCommandSubject  = "cmd.access.driver_request.create"
	driverRequestApproveCommandSubject = "cmd.access.driver_request.approve"
	driverRequestDenyCommandSubject    = "cmd.access.driver_request.deny"
	driverRequestCancelCommandSubject  = "cmd.access.driver_request.cancel"
	driverAccessRevokedEventSubject    = "evt.driver.access.revoked"

	driverRequestCreateOperationType  = "driver_request.create"
	driverRequestApproveOperationType = "driver_request.approve"
	driverRequestDenyOperationType    = "driver_request.deny"
	driverRequestCancelOperationType  = "driver_request.cancel"
)

type createDriverRequestCommand struct {
	OperationID string        `json:"operation_id"`
	Request     DriverRequest `json:"request"`
}

type driverRequestDecisionCommand struct {
	OperationID string `json:"operation_id"`
	RequestID   string `json:"request_id"`
	FleetID     string `json:"fleet_id,omitempty"`
}

type driverAccessRevokedEvent struct {
	FleetID  string `json:"fleet_id"`
	DriverID string `json:"driver_id"`
}

type Handler struct {
	store       Store
	fleetStore  fleetsvc.FleetStore
	broker      *messaging.Broker
	opsStore    opsvc.Store
	notifyStore notifysvc.NotifStore
	userStore   authsvc.UserStore
}

func NewHandler(
	store Store,
	fleetStore fleetsvc.FleetStore,
	broker *messaging.Broker,
	opsStore opsvc.Store,
	notifyStore notifysvc.NotifStore,
	userStore authsvc.UserStore,
) *Handler {
	return &Handler{
		store:       store,
		fleetStore:  fleetStore,
		broker:      broker,
		opsStore:    opsStore,
		notifyStore: notifyStore,
		userStore:   userStore,
	}
}

func (h *Handler) Mount(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/v1/driver-requests", h.List)
	mux.HandleFunc("POST /api/v1/driver-requests", h.Create)
	mux.HandleFunc("POST /api/v1/driver-requests/{id}/approve", h.Approve)
	mux.HandleFunc("POST /api/v1/driver-requests/{id}/deny", h.Deny)
	mux.HandleFunc("POST /api/v1/driver-requests/{id}/cancel", h.Cancel)
}

func (h *Handler) SubscribeCommands() error {
	if err := h.subscribe(driverRequestCreateCommandSubject, h.processCreateCommand); err != nil {
		return err
	}
	if err := h.subscribe(driverRequestApproveCommandSubject, h.processApproveCommand); err != nil {
		return err
	}
	if err := h.subscribe(driverRequestDenyCommandSubject, h.processDenyCommand); err != nil {
		return err
	}
	if err := h.subscribe(driverRequestCancelCommandSubject, h.processCancelCommand); err != nil {
		return err
	}
	return h.subscribe(driverAccessRevokedEventSubject, h.processDriverAccessRevokedEvent)
}

func (h *Handler) List(w http.ResponseWriter, r *http.Request) {
	identity := auth.IdentityFromContext(r.Context())
	fleetID := strings.TrimSpace(r.URL.Query().Get("fleet_id"))
	status := strings.TrimSpace(r.URL.Query().Get("status"))
	requestType := strings.TrimSpace(r.URL.Query().Get("request_type"))
	userID := strings.TrimSpace(r.URL.Query().Get("user_id"))
	if status == "" {
		status = StatusPending
	}
	switch identity.Role {
	case auth.RoleOwner, auth.RoleAdmin:
		if fleetID == "" {
			fleetID = strings.TrimSpace(identity.FleetID)
		}
		if fleetID == "" {
			writeJSON(w, http.StatusBadRequest, errBody("fleet_id is required"))
			return
		}
		if !canManageFleetDriverRequests(identity, fleetID) {
			writeJSON(w, http.StatusForbidden, errBody("forbidden"))
			return
		}
	case auth.RoleDriver:
		if identity.UserID == "" {
			writeJSON(w, http.StatusUnauthorized, errBody("authentication required"))
			return
		}
		if userID == "" {
			userID = identity.UserID
		}
		if userID != identity.UserID {
			writeJSON(w, http.StatusForbidden, errBody("forbidden"))
			return
		}
		if fleetID != "" && identity.FleetID != "" && fleetID != identity.FleetID {
			writeJSON(w, http.StatusForbidden, errBody("forbidden"))
			return
		}
	default:
		writeJSON(w, http.StatusForbidden, errBody("forbidden"))
		return
	}
	items, err := h.store.List(r.Context(), fleetID, status, requestType)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody(err.Error()))
		return
	}
	if items == nil {
		items = []DriverRequest{}
	}
	if userID != "" {
		filtered := make([]DriverRequest, 0, len(items))
		for _, item := range items {
			if strings.TrimSpace(item.UserID) != userID {
				continue
			}
			filtered = append(filtered, item)
		}
		items = filtered
	}
	writeJSON(w, http.StatusOK, items)
}

func (h *Handler) Create(w http.ResponseWriter, r *http.Request) {
	var req DriverRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, errBody("invalid json"))
		return
	}
	req.UserID = strings.TrimSpace(req.UserID)
	req.FleetID = strings.TrimSpace(req.FleetID)
	req.RequestType = strings.TrimSpace(strings.ToLower(req.RequestType))
	req.VehicleID = strings.TrimSpace(req.VehicleID)
	req.FullName = strings.TrimSpace(req.FullName)
	req.Email = strings.TrimSpace(strings.ToLower(req.Email))
	req.Phone = strings.TrimSpace(req.Phone)
	req.Note = strings.TrimSpace(req.Note)
	if req.UserID == "" || req.FleetID == "" || req.FullName == "" || req.Email == "" {
		writeJSON(w, http.StatusBadRequest, errBody("user_id, fleet_id, full_name and email required"))
		return
	}
	requestType, err := normalizeDriverRequestType(req.RequestType, req.VehicleID)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errBody(err.Error()))
		return
	}
	req.RequestType = requestType
	if req.RequestType == RequestTypeAccess {
		req.VehicleID = ""
	}
	identity := auth.IdentityFromContext(r.Context())
	switch identity.Role {
	case auth.RoleOwner, auth.RoleAdmin:
		if !canManageFleetDriverRequests(identity, req.FleetID) {
			writeJSON(w, http.StatusForbidden, errBody("forbidden"))
			return
		}
	case auth.RoleDriver:
		if identity.UserID == "" || identity.UserID != req.UserID {
			writeJSON(w, http.StatusForbidden, errBody("forbidden"))
			return
		}
		if identity.FleetID != "" && req.FleetID != identity.FleetID {
			writeJSON(w, http.StatusForbidden, errBody("forbidden"))
			return
		}
	default:
		writeJSON(w, http.StatusForbidden, errBody("forbidden"))
		return
	}
	if req.RequestType == RequestTypeVehicleAssignment {
		if _, err := h.fleetStore.GetDriver(r.Context(), req.FleetID, req.UserID); err != nil {
			writeJSON(w, http.StatusConflict, errBody("driver access must be approved before requesting a vehicle"))
			return
		}
	}
	if req.ID == "" {
		req.ID = uuid.NewString()
	}

	cmd := createDriverRequestCommand{
		Request: req,
	}
	if err := h.enqueueCommand(
		r.Context(),
		driverRequestCreateOperationType,
		"Driver access request accepted for async processing.",
		driverRequestCreateCommandSubject,
		r.Header.Get("Idempotency-Key"),
		&cmd,
	); err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody("queue publish failed"))
		return
	}

	writeJSON(w, http.StatusAccepted, opsvc.CommandAccepted{
		OperationID: cmd.OperationID,
		Status:      opsvc.StatusQueued,
		Message:     "Driver access request queued.",
	})
}

func (h *Handler) Approve(w http.ResponseWriter, r *http.Request) {
	reqID := strings.TrimSpace(r.PathValue("id"))
	if reqID == "" {
		writeJSON(w, http.StatusBadRequest, errBody("driver request id required"))
		return
	}

	existing, err := h.store.Get(r.Context(), reqID)
	if err != nil || existing == nil {
		writeJSON(w, http.StatusNotFound, errBody("driver request not found"))
		return
	}
	identity := auth.IdentityFromContext(r.Context())
	if !canManageFleetDriverRequests(identity, strings.TrimSpace(existing.FleetID)) {
		writeJSON(w, http.StatusForbidden, errBody("forbidden"))
		return
	}
	cmd := driverRequestDecisionCommand{
		RequestID: reqID,
		FleetID:   strings.TrimSpace(existing.FleetID),
	}
	if err := h.enqueueCommand(
		r.Context(),
		driverRequestApproveOperationType,
		"Driver access approval accepted for async processing.",
		driverRequestApproveCommandSubject,
		r.Header.Get("Idempotency-Key"),
		&cmd,
	); err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody("queue publish failed"))
		return
	}

	writeJSON(w, http.StatusAccepted, opsvc.CommandAccepted{
		OperationID: cmd.OperationID,
		Status:      opsvc.StatusQueued,
		Message:     "Driver access approval queued.",
	})
}

func (h *Handler) Deny(w http.ResponseWriter, r *http.Request) {
	reqID := strings.TrimSpace(r.PathValue("id"))
	if reqID == "" {
		writeJSON(w, http.StatusBadRequest, errBody("driver request id required"))
		return
	}

	existing, err := h.store.Get(r.Context(), reqID)
	if err != nil || existing == nil {
		writeJSON(w, http.StatusNotFound, errBody("driver request not found"))
		return
	}
	identity := auth.IdentityFromContext(r.Context())
	if !canManageFleetDriverRequests(identity, strings.TrimSpace(existing.FleetID)) {
		writeJSON(w, http.StatusForbidden, errBody("forbidden"))
		return
	}
	cmd := driverRequestDecisionCommand{
		RequestID: reqID,
		FleetID:   strings.TrimSpace(existing.FleetID),
	}
	if err := h.enqueueCommand(
		r.Context(),
		driverRequestDenyOperationType,
		"Driver access denial accepted for async processing.",
		driverRequestDenyCommandSubject,
		r.Header.Get("Idempotency-Key"),
		&cmd,
	); err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody("queue publish failed"))
		return
	}

	writeJSON(w, http.StatusAccepted, opsvc.CommandAccepted{
		OperationID: cmd.OperationID,
		Status:      opsvc.StatusQueued,
		Message:     "Driver access denial queued.",
	})
}

func (h *Handler) Cancel(w http.ResponseWriter, r *http.Request) {
	reqID := strings.TrimSpace(r.PathValue("id"))
	if reqID == "" {
		writeJSON(w, http.StatusBadRequest, errBody("driver request id required"))
		return
	}

	existing, err := h.store.Get(r.Context(), reqID)
	if err != nil || existing == nil {
		writeJSON(w, http.StatusNotFound, errBody("driver request not found"))
		return
	}
	identity := auth.IdentityFromContext(r.Context())
	isOwner := canManageFleetDriverRequests(identity, strings.TrimSpace(existing.FleetID))
	isSelfDriver := identity.Role == auth.RoleDriver && strings.TrimSpace(identity.UserID) == strings.TrimSpace(existing.UserID)
	if !isOwner && !isSelfDriver {
		writeJSON(w, http.StatusForbidden, errBody("forbidden"))
		return
	}
	cmd := driverRequestDecisionCommand{
		RequestID: reqID,
		FleetID:   strings.TrimSpace(existing.FleetID),
	}
	if err := h.enqueueCommand(
		r.Context(),
		driverRequestCancelOperationType,
		"Driver request cancellation accepted for async processing.",
		driverRequestCancelCommandSubject,
		r.Header.Get("Idempotency-Key"),
		&cmd,
	); err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody("queue publish failed"))
		return
	}

	writeJSON(w, http.StatusAccepted, opsvc.CommandAccepted{
		OperationID: cmd.OperationID,
		Status:      opsvc.StatusQueued,
		Message:     "Driver request cancellation queued.",
	})
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

func (h *Handler) processCreateCommand(payload []byte) {
	var cmd createDriverRequestCommand
	if err := json.Unmarshal(payload, &cmd); err != nil {
		log.Printf("[driver-requests] decode create command: %v", err)
		return
	}
	ctx := context.Background()
	op := h.loadOperation(ctx, cmd.OperationID, driverRequestCreateOperationType)
	h.markProcessing(ctx, op)

	now := time.Now().UTC()
	if existing, err := h.store.FindPendingByUser(ctx, cmd.Request.FleetID, cmd.Request.UserID, cmd.Request.RequestType); err == nil && existing != nil {
		existing.RequestType = cmd.Request.RequestType
		existing.VehicleID = cmd.Request.VehicleID
		existing.FullName = cmd.Request.FullName
		existing.Email = cmd.Request.Email
		existing.Phone = cmd.Request.Phone
		existing.Note = cmd.Request.Note
		existing.UpdatedAt = now
		if err := h.store.Put(ctx, existing); err != nil {
			h.markFailed(ctx, op, "Failed to update pending driver access request.", err)
			return
		}
		h.markSucceeded(ctx, op, existing.ID, "Driver access request is already pending. Request details were updated.")
		return
	}

	cmd.Request.Status = StatusPending
	if cmd.Request.CreatedAt.IsZero() {
		cmd.Request.CreatedAt = now
	}
	cmd.Request.UpdatedAt = now

	if err := h.store.Put(ctx, &cmd.Request); err != nil {
		h.markFailed(ctx, op, "Failed to create driver access request.", err)
		return
	}

	h.notifyOwnersOfPendingRequest(ctx, &cmd.Request)
	h.markSucceeded(ctx, op, cmd.Request.ID, "Driver access request queued successfully.")
}

func (h *Handler) processApproveCommand(payload []byte) {
	var cmd driverRequestDecisionCommand
	if err := json.Unmarshal(payload, &cmd); err != nil {
		log.Printf("[driver-requests] decode approve command: %v", err)
		return
	}
	ctx := context.Background()
	op := h.loadOperation(ctx, cmd.OperationID, driverRequestApproveOperationType)
	h.markProcessing(ctx, op)

	req, err := h.store.Get(ctx, cmd.RequestID)
	if err != nil {
		h.markFailed(ctx, op, "Driver access request not found.", err)
		return
	}

	if err := h.approveRequest(ctx, req); err != nil {
		h.markFailed(ctx, op, "Failed to approve driver access request.", err)
		return
	}

	h.markSucceeded(ctx, op, req.ID, "Driver access request approved.")
}

func (h *Handler) processDenyCommand(payload []byte) {
	var cmd driverRequestDecisionCommand
	if err := json.Unmarshal(payload, &cmd); err != nil {
		log.Printf("[driver-requests] decode deny command: %v", err)
		return
	}
	ctx := context.Background()
	op := h.loadOperation(ctx, cmd.OperationID, driverRequestDenyOperationType)
	h.markProcessing(ctx, op)

	req, err := h.store.Get(ctx, cmd.RequestID)
	if err != nil {
		h.markFailed(ctx, op, "Driver access request not found.", err)
		return
	}
	if req.Status != StatusDenied {
		req.Status = StatusDenied
		req.UpdatedAt = time.Now().UTC()
		if err := h.store.Put(ctx, req); err != nil {
			h.markFailed(ctx, op, "Failed to deny driver access request.", err)
			return
		}
	}

	h.markSucceeded(ctx, op, req.ID, "Driver access request denied.")
}

func (h *Handler) processCancelCommand(payload []byte) {
	var cmd driverRequestDecisionCommand
	if err := json.Unmarshal(payload, &cmd); err != nil {
		log.Printf("[driver-requests] decode cancel command: %v", err)
		return
	}
	ctx := context.Background()
	op := h.loadOperation(ctx, cmd.OperationID, driverRequestCancelOperationType)
	h.markProcessing(ctx, op)

	req, err := h.store.Get(ctx, cmd.RequestID)
	if err != nil {
		h.markFailed(ctx, op, "Driver access request not found.", err)
		return
	}
	if req.Status != StatusPending {
		h.markFailed(ctx, op, "Only pending driver requests can be canceled.", fmt.Errorf("request is %s", req.Status))
		return
	}

	req.Status = StatusCanceled
	req.UpdatedAt = time.Now().UTC()
	if err := h.store.Put(ctx, req); err != nil {
		h.markFailed(ctx, op, "Failed to cancel driver access request.", err)
		return
	}

	h.notifyOwnersOfCanceledRequest(ctx, req)
	h.markSucceeded(ctx, op, req.ID, "Driver access request canceled.")
}

func (h *Handler) processDriverAccessRevokedEvent(payload []byte) {
	var event driverAccessRevokedEvent
	if err := json.Unmarshal(payload, &event); err != nil {
		log.Printf("[driver-requests] decode driver access revoked event: %v", err)
		return
	}

	event.FleetID = strings.TrimSpace(event.FleetID)
	event.DriverID = strings.TrimSpace(event.DriverID)
	if event.FleetID == "" || event.DriverID == "" {
		return
	}

	type approvedVehicleAssignmentRevoker interface {
		RevokeApprovedVehicleAssignments(ctx context.Context, fleetID, userID string) (int, error)
	}

	revoker, ok := h.store.(approvedVehicleAssignmentRevoker)
	if !ok {
		return
	}

	revokedCount, err := revoker.RevokeApprovedVehicleAssignments(context.Background(), event.FleetID, event.DriverID)
	if err != nil {
		log.Printf(
			"[driver-requests] revoke approved vehicle assignments for driver %s in fleet %s: %v",
			event.DriverID,
			event.FleetID,
			err,
		)
		return
	}
	if revokedCount > 0 {
		log.Printf(
			"[driver-requests] revoked %d approved vehicle assignments for driver %s in fleet %s",
			revokedCount,
			event.DriverID,
			event.FleetID,
		)
	}
}

func (h *Handler) approveRequest(ctx context.Context, req *DriverRequest) error {
	req.RequestType = defaultDriverRequestType(req.RequestType, req.VehicleID)
	switch req.RequestType {
	case RequestTypeAccess:
		return h.approveAccessRequest(ctx, req)
	case RequestTypeVehicleAssignment:
		return h.approveVehicleAssignmentRequest(ctx, req)
	default:
		return fmt.Errorf("unsupported driver request type")
	}
}

func (h *Handler) approveAccessRequest(ctx context.Context, req *DriverRequest) error {
	now := time.Now().UTC()
	resetVehicleAssignments := req.Status != StatusApproved
	driver := &fleetsvc.Driver{
		ID:       req.UserID,
		Email:    req.Email,
		FullName: req.FullName,
		Phone:    req.Phone,
		FleetID:  req.FleetID,
		// Access approval must not imply a vehicle approval.
		AssignedVehicleIDs: []string{},
		IsActive:           true,
		CreatedAt:          now,
		UpdatedAt:          now,
	}

	if existing, err := h.fleetStore.GetDriver(ctx, req.FleetID, req.UserID); err == nil && existing != nil {
		driver = existing
		driver.Email = req.Email
		driver.FullName = req.FullName
		driver.Phone = req.Phone
		driver.IsActive = true
		driver.UpdatedAt = now
	}

	if resetVehicleAssignments {
		if err := h.clearVehicleAssignmentsForDriver(ctx, req.FleetID, req.UserID, now); err != nil {
			return err
		}
		driver.AssignedVehicleIDs = []string{}
	}

	if err := h.fleetStore.PutDriver(ctx, driver); err != nil {
		return err
	}

	req.Status = StatusApproved
	req.UpdatedAt = now
	if err := h.store.Put(ctx, req); err != nil {
		return err
	}

	notice := &fleetsvc.DriverNotice{
		ID:        uuid.NewString(),
		Title:     "Access Approved",
		Message:   "Your driver access was approved. You can now choose a vehicle and send it to the owner for approval.",
		DriverID:  req.UserID,
		FleetID:   req.FleetID,
		Priority:  "high",
		IsRead:    false,
		Timestamp: now,
	}
	_ = h.fleetStore.PutNotice(ctx, notice)
	h.notifyDriverApproval(
		ctx,
		req,
		"Access Approved",
		"Your driver access was approved. You can now choose a vehicle and send it to the owner for approval.",
		"driver_access_approved",
		"",
	)
	return nil
}

func (h *Handler) clearVehicleAssignmentsForDriver(
	ctx context.Context,
	fleetID string,
	driverID string,
	now time.Time,
) error {
	assignedVehicles, err := h.fleetStore.ListVehiclesForDriver(
		ctx,
		strings.TrimSpace(fleetID),
		strings.TrimSpace(driverID),
	)
	if err != nil {
		return err
	}
	for i := range assignedVehicles {
		vehicle := assignedVehicles[i]
		vehicle.DriverID = ""
		vehicle.DriverName = ""
		vehicle.DriverPhone = ""
		vehicle.StatusMessage = "Driver assignment pending approval"
		vehicle.LastUpdated = now
		if err := h.fleetStore.PutVehicle(ctx, &vehicle); err != nil {
			return err
		}
	}
	return nil
}

func (h *Handler) approveVehicleAssignmentRequest(ctx context.Context, req *DriverRequest) error {
	now := time.Now().UTC()
	if strings.TrimSpace(req.VehicleID) == "" {
		return fmt.Errorf("vehicle request missing vehicle_id")
	}

	driver, err := h.fleetStore.GetDriver(ctx, req.FleetID, req.UserID)
	if err != nil || driver == nil {
		return fmt.Errorf("driver access must be approved before a vehicle can be assigned")
	}
	driver.Email = req.Email
	driver.FullName = req.FullName
	driver.Phone = req.Phone
	driver.IsActive = true
	driver.UpdatedAt = now
	if err := h.fleetStore.PutDriver(ctx, driver); err != nil {
		return err
	}

	assignedVehicle, err := h.assignRequestedVehicle(ctx, req, driver, now)
	if err != nil {
		return err
	}

	req.Status = StatusApproved
	req.UpdatedAt = now
	if err := h.store.Put(ctx, req); err != nil {
		return err
	}

	notice := &fleetsvc.DriverNotice{
		ID:        uuid.NewString(),
		Title:     "Vehicle Assignment Approved",
		Message:   "Your vehicle assignment was approved. You were assigned to " + driverRequestVehicleLabel(assignedVehicle) + ".",
		VehicleID: assignedVehicle.ID,
		DriverID:  req.UserID,
		FleetID:   req.FleetID,
		Priority:  "high",
		IsRead:    false,
		Timestamp: now,
	}
	_ = h.fleetStore.PutNotice(ctx, notice)
	h.notifyDriverApproval(
		ctx,
		req,
		"Vehicle Assignment Approved",
		"Your vehicle assignment was approved. You were assigned to "+driverRequestVehicleLabel(assignedVehicle)+".",
		"driver_vehicle_assignment_approved",
		assignedVehicle.ID,
	)
	return nil
}

func (h *Handler) notifyOwnersOfPendingRequest(ctx context.Context, req *DriverRequest) {
	if h.notifyStore == nil || h.userStore == nil || req == nil {
		return
	}

	owners, err := h.userStore.ListUsers(ctx, "owner", strings.TrimSpace(req.FleetID))
	if err != nil {
		log.Printf("[driver-requests] load owners for notifications: %v", err)
		return
	}

	vehicleLabel := ""
	if strings.TrimSpace(req.VehicleID) != "" {
		if vehicle, lookupErr := h.fleetStore.GetVehicle(ctx, req.FleetID, req.VehicleID); lookupErr == nil && vehicle != nil {
			vehicleLabel = driverRequestVehicleLabel(vehicle)
		}
	}

	title := "Driver Access Request"
	body := strings.TrimSpace(req.FullName) + " requested access to your fleet."
	eventType := "driver_access_request_created"
	if req.RequestType == RequestTypeVehicleAssignment {
		title = "Vehicle Approval Request"
		if vehicleLabel == "" {
			body = strings.TrimSpace(req.FullName) + " requested vehicle assignment and is waiting for your approval."
		} else {
			body = strings.TrimSpace(req.FullName) + " requested assignment to " + vehicleLabel + "."
		}
		eventType = "driver_vehicle_assignment_request_created"
	}

	for i := range owners {
		ownerID := strings.TrimSpace(owners[i].ID)
		if ownerID == "" {
			continue
		}
		h.pushNotification(ctx, &notifysvc.Notification{
			UserID:    ownerID,
			FleetID:   strings.TrimSpace(req.FleetID),
			VehicleID: strings.TrimSpace(req.VehicleID),
			Type:      "driver_request",
			Title:     title,
			Body:      body,
			Data: map[string]string{
				"event_type":   eventType,
				"request_id":   strings.TrimSpace(req.ID),
				"request_type": strings.TrimSpace(req.RequestType),
				"driver_id":    strings.TrimSpace(req.UserID),
				"vehicle_id":   strings.TrimSpace(req.VehicleID),
			},
		})
	}
}

func (h *Handler) notifyOwnersOfCanceledRequest(ctx context.Context, req *DriverRequest) {
	if h.notifyStore == nil || h.userStore == nil || req == nil {
		return
	}

	owners, err := h.userStore.ListUsers(ctx, "owner", strings.TrimSpace(req.FleetID))
	if err != nil {
		log.Printf("[driver-requests] load owners for cancel notifications: %v", err)
		return
	}

	title := "Driver Request Canceled"
	body := strings.TrimSpace(req.FullName) + " canceled the pending request."
	eventType := "driver_request_cancelled"
	if req.RequestType == RequestTypeVehicleAssignment {
		title = "Vehicle Request Canceled"
		body = strings.TrimSpace(req.FullName) + " canceled the pending vehicle assignment request."
		eventType = "driver_vehicle_assignment_request_cancelled"
	}

	for i := range owners {
		ownerID := strings.TrimSpace(owners[i].ID)
		if ownerID == "" {
			continue
		}
		h.pushNotification(ctx, &notifysvc.Notification{
			UserID:    ownerID,
			FleetID:   strings.TrimSpace(req.FleetID),
			VehicleID: strings.TrimSpace(req.VehicleID),
			Type:      "driver_request_update",
			Title:     title,
			Body:      body,
			Data: map[string]string{
				"event_type":   eventType,
				"request_id":   strings.TrimSpace(req.ID),
				"request_type": strings.TrimSpace(req.RequestType),
				"driver_id":    strings.TrimSpace(req.UserID),
				"vehicle_id":   strings.TrimSpace(req.VehicleID),
			},
		})
	}
}

func (h *Handler) notifyDriverApproval(
	ctx context.Context,
	req *DriverRequest,
	title string,
	body string,
	eventType string,
	vehicleID string,
) {
	if req == nil {
		return
	}

	h.pushNotification(ctx, &notifysvc.Notification{
		UserID:    strings.TrimSpace(req.UserID),
		FleetID:   strings.TrimSpace(req.FleetID),
		VehicleID: strings.TrimSpace(vehicleID),
		Type:      "driver_access_update",
		Title:     title,
		Body:      body,
		Data: map[string]string{
			"event_type":   strings.TrimSpace(eventType),
			"request_id":   strings.TrimSpace(req.ID),
			"request_type": strings.TrimSpace(req.RequestType),
			"vehicle_id":   strings.TrimSpace(vehicleID),
		},
	})
}

func (h *Handler) pushNotification(ctx context.Context, n *notifysvc.Notification) {
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
		log.Printf("[driver-requests] store notification for %s: %v", n.UserID, err)
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
			log.Printf("[driver-requests] publish notification %s: %v", subject, err)
		}
	}
}

func (h *Handler) assignRequestedVehicle(
	ctx context.Context,
	req *DriverRequest,
	driver *fleetsvc.Driver,
	now time.Time,
) (*fleetsvc.Vehicle, error) {
	requestedVehicleID := strings.TrimSpace(req.VehicleID)
	if requestedVehicleID == "" {
		return nil, fmt.Errorf("vehicle request missing vehicle_id")
	}

	vehicle, err := h.fleetStore.GetVehicle(ctx, req.FleetID, requestedVehicleID)
	if err != nil || vehicle == nil {
		if err != nil {
			log.Printf("[driver-requests] requested vehicle lookup failed for request %s: %v", req.ID, err)
		}
		return nil, fmt.Errorf("requested vehicle is no longer available")
	}

	existingDriverID := strings.TrimSpace(vehicle.DriverID)
	if existingDriverID != "" && !strings.EqualFold(existingDriverID, req.UserID) {
		return nil, fmt.Errorf("requested vehicle is already assigned")
	}

	currentAssignments, err := h.fleetStore.ListVehiclesForDriver(ctx, req.FleetID, req.UserID)
	if err == nil {
		for i := range currentAssignments {
			assigned := currentAssignments[i]
			if strings.EqualFold(strings.TrimSpace(assigned.ID), vehicle.ID) {
				continue
			}
			assigned.DriverID = ""
			assigned.DriverName = ""
			assigned.DriverPhone = ""
			assigned.StatusMessage = "Driver moved to another vehicle"
			assigned.LastUpdated = now
			if err := h.fleetStore.PutVehicle(ctx, &assigned); err != nil {
				log.Printf("[driver-requests] clear prior vehicle assignment failed for request %s: %v", req.ID, err)
			}
		}
	}

	vehicle.DriverID = req.UserID
	vehicle.DriverName = driver.FullName
	vehicle.DriverPhone = driver.Phone
	if strings.TrimSpace(vehicle.StatusMessage) == "" ||
		strings.EqualFold(strings.TrimSpace(vehicle.StatusMessage), "driver unassigned by owner") {
		vehicle.StatusMessage = "Driver assigned"
	}
	vehicle.LastUpdated = now

	if err := h.fleetStore.PutVehicle(ctx, vehicle); err != nil {
		log.Printf("[driver-requests] requested vehicle assignment failed for request %s: %v", req.ID, err)
		return nil, fmt.Errorf("requested vehicle could not be assigned yet")
	}

	driver.AssignedVehicleIDs = []string{vehicle.ID}
	driver.UpdatedAt = now
	if err := h.fleetStore.PutDriver(ctx, driver); err != nil {
		log.Printf("[driver-requests] sync driver assignment failed for request %s: %v", req.ID, err)
	}

	return vehicle, nil
}

func driverRequestVehicleLabel(v *fleetsvc.Vehicle) string {
	if v == nil {
		return "your vehicle"
	}

	label := strings.TrimSpace(v.Nickname)
	if label == "" {
		label = strings.TrimSpace(v.RegistrationNumber)
	}
	if label == "" {
		label = strings.TrimSpace(v.ID)
	}
	routeID := strings.TrimSpace(v.CurrentRouteID)
	if routeID == "" {
		return label
	}
	return label + " (" + routeID + ")"
}

func normalizeDriverRequestType(rawRequestType, vehicleID string) (string, error) {
	switch strings.TrimSpace(strings.ToLower(rawRequestType)) {
	case "":
		return defaultDriverRequestType("", vehicleID), nil
	case RequestTypeAccess:
		if strings.TrimSpace(vehicleID) != "" {
			return "", fmt.Errorf("vehicle_id is not allowed for access requests")
		}
		return RequestTypeAccess, nil
	case RequestTypeVehicleAssignment:
		if strings.TrimSpace(vehicleID) == "" {
			return "", fmt.Errorf("vehicle_id is required for vehicle assignment requests")
		}
		return RequestTypeVehicleAssignment, nil
	default:
		return "", fmt.Errorf("unsupported request_type")
	}
}

func defaultDriverRequestType(rawRequestType, vehicleID string) string {
	normalized := strings.TrimSpace(strings.ToLower(rawRequestType))
	if normalized == RequestTypeAccess || normalized == RequestTypeVehicleAssignment {
		return normalized
	}
	if strings.TrimSpace(vehicleID) != "" {
		return RequestTypeVehicleAssignment
	}
	return RequestTypeAccess
}

func (h *Handler) enqueueCommand(
	ctx context.Context,
	opType string,
	message string,
	subject string,
	rawIdempotencyKey string,
	command any,
) error {
	idempotencyKey := normalizeIdempotencyKey(opType, rawIdempotencyKey)
	if idempotencyKey != "" {
		if existing, err := h.opsStore.FindByIdempotencyKey(ctx, idempotencyKey); err == nil && existing != nil {
			switch cmd := command.(type) {
			case *createDriverRequestCommand:
				cmd.OperationID = existing.ID
			case *driverRequestDecisionCommand:
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
		FleetID:        fleetIDFromRequestCommand(command),
		IdempotencyKey: idempotencyKey,
		Status:         opsvc.StatusQueued,
		Message:        message,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	if err := h.opsStore.Put(ctx, op); err != nil {
		return err
	}
	_ = opsvc.PublishUpdate(h.broker, op)

	switch cmd := command.(type) {
	case *createDriverRequestCommand:
		cmd.OperationID = opID
	case *driverRequestDecisionCommand:
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
	op, err := h.opsStore.Get(ctx, operationID)
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

func fleetIDFromRequestCommand(command any) string {
	switch cmd := command.(type) {
	case *createDriverRequestCommand:
		return strings.TrimSpace(cmd.Request.FleetID)
	case *driverRequestDecisionCommand:
		return strings.TrimSpace(cmd.FleetID)
	default:
		return ""
	}
}

func (h *Handler) markProcessing(ctx context.Context, op *opsvc.Operation) {
	op.Status = opsvc.StatusProcessing
	op.UpdatedAt = time.Now().UTC()
	if err := h.opsStore.Put(ctx, op); err != nil {
		log.Printf("[driver-requests] persist processing operation %s: %v", op.ID, err)
	}
	_ = opsvc.PublishUpdate(h.broker, op)
}

func (h *Handler) markSucceeded(ctx context.Context, op *opsvc.Operation, resourceID, message string) {
	op.Status = opsvc.StatusSucceeded
	op.ResourceID = resourceID
	op.Message = message
	op.ErrorMessage = ""
	op.UpdatedAt = time.Now().UTC()
	if err := h.opsStore.Put(ctx, op); err != nil {
		log.Printf("[driver-requests] persist success operation %s: %v", op.ID, err)
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
	if err := h.opsStore.Put(ctx, op); err != nil {
		log.Printf("[driver-requests] persist failed operation %s: %v", op.ID, err)
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

func canManageFleetDriverRequests(identity auth.Identity, fleetID string) bool {
	normalizedFleetID := strings.TrimSpace(fleetID)
	if normalizedFleetID == "" {
		return false
	}
	if identity.Role != auth.RoleOwner && identity.Role != auth.RoleAdmin {
		return false
	}
	return strings.TrimSpace(identity.FleetID) == normalizedFleetID
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func errBody(msg string) map[string]string {
	return map[string]string{"error": msg}
}
