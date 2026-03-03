package requestsvc

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

	"via-backend/internal/fleetsvc"
	"via-backend/internal/messaging"
	"via-backend/internal/opsvc"
)

const (
	driverRequestCreateCommandSubject  = "cmd.access.driver_request.create"
	driverRequestApproveCommandSubject = "cmd.access.driver_request.approve"
	driverRequestDenyCommandSubject    = "cmd.access.driver_request.deny"

	driverRequestCreateOperationType  = "driver_request.create"
	driverRequestApproveOperationType = "driver_request.approve"
	driverRequestDenyOperationType    = "driver_request.deny"
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

type Handler struct {
	store      Store
	fleetStore fleetsvc.FleetStore
	broker     *messaging.Broker
	opsStore   opsvc.Store
}

func NewHandler(
	store Store,
	fleetStore fleetsvc.FleetStore,
	broker *messaging.Broker,
	opsStore opsvc.Store,
) *Handler {
	return &Handler{
		store:      store,
		fleetStore: fleetStore,
		broker:     broker,
		opsStore:   opsStore,
	}
}

func (h *Handler) Mount(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/v1/driver-requests", h.List)
	mux.HandleFunc("POST /api/v1/driver-requests", h.Create)
	mux.HandleFunc("POST /api/v1/driver-requests/{id}/approve", h.Approve)
	mux.HandleFunc("POST /api/v1/driver-requests/{id}/deny", h.Deny)
}

func (h *Handler) SubscribeCommands() error {
	if err := h.subscribe(driverRequestCreateCommandSubject, h.processCreateCommand); err != nil {
		return err
	}
	if err := h.subscribe(driverRequestApproveCommandSubject, h.processApproveCommand); err != nil {
		return err
	}
	return h.subscribe(driverRequestDenyCommandSubject, h.processDenyCommand)
}

func (h *Handler) List(w http.ResponseWriter, r *http.Request) {
	fleetID := strings.TrimSpace(r.URL.Query().Get("fleet_id"))
	status := strings.TrimSpace(r.URL.Query().Get("status"))
	if status == "" {
		status = StatusPending
	}
	items, err := h.store.List(r.Context(), fleetID, status)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody(err.Error()))
		return
	}
	if items == nil {
		items = []DriverRequest{}
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
	req.FullName = strings.TrimSpace(req.FullName)
	req.Email = strings.TrimSpace(strings.ToLower(req.Email))
	req.Phone = strings.TrimSpace(req.Phone)
	req.Note = strings.TrimSpace(req.Note)
	if req.UserID == "" || req.FleetID == "" || req.FullName == "" || req.Email == "" {
		writeJSON(w, http.StatusBadRequest, errBody("user_id, fleet_id, full_name and email required"))
		return
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

	cmd := driverRequestDecisionCommand{RequestID: reqID}
	if existing, err := h.store.Get(r.Context(), reqID); err == nil && existing != nil {
		cmd.FleetID = strings.TrimSpace(existing.FleetID)
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

	cmd := driverRequestDecisionCommand{RequestID: reqID}
	if existing, err := h.store.Get(r.Context(), reqID); err == nil && existing != nil {
		cmd.FleetID = strings.TrimSpace(existing.FleetID)
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

	if existing, err := h.store.FindPendingByUser(ctx, cmd.Request.FleetID, cmd.Request.UserID); err == nil && existing != nil {
		h.markSucceeded(ctx, op, existing.ID, "Driver access request is already pending.")
		return
	}

	now := time.Now().UTC()
	cmd.Request.Status = StatusPending
	if cmd.Request.CreatedAt.IsZero() {
		cmd.Request.CreatedAt = now
	}
	cmd.Request.UpdatedAt = now

	if err := h.store.Put(ctx, &cmd.Request); err != nil {
		h.markFailed(ctx, op, "Failed to create driver access request.", err)
		return
	}

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

func (h *Handler) approveRequest(ctx context.Context, req *DriverRequest) error {
	now := time.Now().UTC()
	driver := &fleetsvc.Driver{
		ID:        req.UserID,
		Email:     req.Email,
		FullName:  req.FullName,
		Phone:     req.Phone,
		FleetID:   req.FleetID,
		IsActive:  true,
		CreatedAt: now,
		UpdatedAt: now,
	}

	if existing, err := h.fleetStore.GetDriver(ctx, req.FleetID, req.UserID); err == nil && existing != nil {
		driver = existing
		driver.Email = req.Email
		driver.FullName = req.FullName
		driver.Phone = req.Phone
		driver.IsActive = true
		driver.UpdatedAt = now
	}

	if err := h.fleetStore.PutDriver(ctx, driver); err != nil {
		return err
	}

	notice := &fleetsvc.DriverNotice{
		ID:        uuid.NewString(),
		Title:     "Access Approved",
		Message:   "Your driver access was approved. Await vehicle assignment from the owner.",
		DriverID:  req.UserID,
		FleetID:   req.FleetID,
		Priority:  "high",
		IsRead:    false,
		Timestamp: now,
	}
	_ = h.fleetStore.PutNotice(ctx, notice)

	req.Status = StatusApproved
	req.UpdatedAt = now
	return h.store.Put(ctx, req)
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

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func errBody(msg string) map[string]string {
	return map[string]string{"error": msg}
}
