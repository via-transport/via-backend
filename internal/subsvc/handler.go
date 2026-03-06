package subsvc

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

	"via-backend/internal/auth"
	"via-backend/internal/authsvc"
	"via-backend/internal/messaging"
	"via-backend/internal/opsvc"
	"via-backend/internal/tenantsvc"
)

const (
	joinRequestCreateCommandSubject  = "cmd.access.join_request.create"
	joinRequestApproveCommandSubject = "cmd.access.join_request.approve"
	joinRequestDenyCommandSubject    = "cmd.access.join_request.deny"

	joinRequestCreateOperationType  = "join_request.create"
	joinRequestApproveOperationType = "join_request.approve"
	joinRequestDenyOperationType    = "join_request.deny"
)

type createJoinRequestCommand struct {
	OperationID  string       `json:"operation_id"`
	Subscription Subscription `json:"subscription"`
}

type joinRequestDecisionCommand struct {
	OperationID string `json:"operation_id"`
	RequestID   string `json:"request_id"`
	FleetID     string `json:"fleet_id,omitempty"`
}

// Handler provides subscription REST endpoints.
type Handler struct {
	store     SubStore
	policy    *tenantsvc.Policy
	broker    *messaging.Broker
	opsStore  opsvc.Store
	userStore authsvc.UserStore
}

// NewHandler creates a subscription handler.
func NewHandler(
	store SubStore,
	policy *tenantsvc.Policy,
	broker *messaging.Broker,
	opsStore opsvc.Store,
	userStore authsvc.UserStore,
) *Handler {
	return &Handler{
		store:     store,
		policy:    policy,
		broker:    broker,
		opsStore:  opsStore,
		userStore: userStore,
	}
}

// Mount registers subscription routes.
func (h *Handler) Mount(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/v1/subscriptions", h.List)
	mux.HandleFunc("POST /api/v1/subscriptions", h.Create)
	mux.HandleFunc("GET /api/v1/subscriptions/{id}", h.Get)
	mux.HandleFunc("PUT /api/v1/subscriptions/{id}", h.Update)
	mux.HandleFunc("DELETE /api/v1/subscriptions/{id}", h.Cancel)
	mux.HandleFunc("GET /api/v1/subscriptions/vehicle/{vehicleId}", h.ListForVehicle)
	mux.HandleFunc("GET /api/v1/subscriptions/fleet/{fleetId}", h.ListForFleet)
	mux.HandleFunc("POST /api/v1/subscriptions/{id}/revoke", h.Revoke)
	mux.HandleFunc("POST /api/v1/join-requests", h.CreateJoinRequest)
	mux.HandleFunc("GET /api/v1/join-requests", h.ListJoinRequests)
	mux.HandleFunc("POST /api/v1/join-requests/{id}/approve", h.ApproveJoinRequest)
	mux.HandleFunc("POST /api/v1/join-requests/{id}/deny", h.DenyJoinRequest)
}

func (h *Handler) SubscribeCommands() error {
	if err := h.subscribe(joinRequestCreateCommandSubject, h.processCreateJoinRequest); err != nil {
		return err
	}
	if err := h.subscribe(joinRequestApproveCommandSubject, h.processApproveJoinRequest); err != nil {
		return err
	}
	return h.subscribe(joinRequestDenyCommandSubject, h.processDenyJoinRequest)
}

// List returns all subscriptions for a user.
func (h *Handler) List(w http.ResponseWriter, r *http.Request) {
	userID := userIDFromRequest(r)
	if userID == "" {
		writeJSON(w, http.StatusBadRequest, errBody("user_id required"))
		return
	}
	subs, err := h.store.ListForUser(r.Context(), userID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody(err.Error()))
		return
	}
	if subs == nil {
		subs = []Subscription{}
	}
	writeJSON(w, http.StatusOK, subs)
}

// Get returns a single subscription.
func (h *Handler) Get(w http.ResponseWriter, r *http.Request) {
	subID := r.PathValue("id")
	userID := userIDFromRequest(r)
	if userID == "" {
		writeJSON(w, http.StatusBadRequest, errBody("user_id required"))
		return
	}
	sub, err := h.store.Get(r.Context(), userID, subID)
	if err != nil {
		writeJSON(w, http.StatusNotFound, errBody("subscription not found"))
		return
	}
	writeJSON(w, http.StatusOK, sub)
}

// Create creates a new subscription.
func (h *Handler) Create(w http.ResponseWriter, r *http.Request) {
	var sub Subscription
	if err := json.NewDecoder(r.Body).Decode(&sub); err != nil {
		writeJSON(w, http.StatusBadRequest, errBody("invalid json"))
		return
	}
	// Auto-fill user_id from auth context if not provided in body.
	if sub.UserID == "" {
		sub.UserID = userIDFromRequest(r)
	}
	if sub.UserID == "" || sub.VehicleID == "" {
		writeJSON(w, http.StatusBadRequest, errBody("user_id and vehicle_id required"))
		return
	}
	if sub.ID == "" {
		sub.ID = uuid.NewString()
	}
	if sub.FleetID == "" {
		sub.FleetID = strings.TrimSpace(r.URL.Query().Get("fleet_id"))
	}
	now := time.Now().UTC()
	sub.CreatedAt = now
	sub.UpdatedAt = now
	sub.Status = "active"
	if sub.Preferences == (SubPrefs{}) {
		sub.Preferences = SubPrefs{
			NotifyOnArrival: true,
			NotifyOnDelay:   true,
			NotifyOnEvent:   true,
		}
	}

	if h.policy != nil && sub.FleetID != "" {
		active, err := h.store.ListByFleetStatus(r.Context(), sub.FleetID, "active")
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, errBody("quota lookup failed"))
			return
		}
		if _, err := h.policy.CheckPassengerActivate(r.Context(), sub.FleetID, len(active)); err != nil {
			writePolicyError(w, err)
			return
		}
	}

	if err := h.store.Put(r.Context(), &sub); err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody("create failed"))
		return
	}
	writeJSON(w, http.StatusCreated, sub)
}

// CreateJoinRequest creates a pending passenger access request.
func (h *Handler) CreateJoinRequest(w http.ResponseWriter, r *http.Request) {
	var sub Subscription
	if err := json.NewDecoder(r.Body).Decode(&sub); err != nil {
		writeJSON(w, http.StatusBadRequest, errBody("invalid json"))
		return
	}
	if sub.UserID == "" {
		sub.UserID = userIDFromRequest(r)
	}
	sub.UserID = strings.TrimSpace(sub.UserID)
	sub.VehicleID = strings.TrimSpace(sub.VehicleID)
	if sub.UserID == "" || sub.VehicleID == "" {
		writeJSON(w, http.StatusBadRequest, errBody("user_id and vehicle_id required"))
		return
	}
	if sub.ID == "" {
		sub.ID = uuid.NewString()
	}
	if sub.FleetID == "" {
		sub.FleetID = strings.TrimSpace(r.URL.Query().Get("fleet_id"))
	}
	sub.FleetID = strings.TrimSpace(sub.FleetID)
	if sub.FleetID == "" {
		writeJSON(w, http.StatusBadRequest, errBody("fleet_id required"))
		return
	}
	if sub.Preferences == (SubPrefs{}) {
		sub.Preferences = SubPrefs{
			NotifyOnArrival: true,
			NotifyOnDelay:   true,
			NotifyOnEvent:   true,
		}
	}

	cmd := createJoinRequestCommand{
		Subscription: sub,
	}
	if err := h.enqueueCommand(
		r.Context(),
		joinRequestCreateOperationType,
		"Passenger join request accepted for async processing.",
		joinRequestCreateCommandSubject,
		r.Header.Get("Idempotency-Key"),
		&cmd,
	); err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody("queue publish failed"))
		return
	}

	writeJSON(w, http.StatusAccepted, opsvc.CommandAccepted{
		OperationID: cmd.OperationID,
		Status:      opsvc.StatusQueued,
		Message:     "Passenger join request queued.",
	})
}

func (h *Handler) ListJoinRequests(w http.ResponseWriter, r *http.Request) {
	fleetID := strings.TrimSpace(r.URL.Query().Get("fleet_id"))
	status := strings.TrimSpace(r.URL.Query().Get("status"))
	if status == "" {
		status = "pending"
	}
	items, err := h.store.ListByFleetStatus(r.Context(), fleetID, status)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody(err.Error()))
		return
	}
	if items == nil {
		items = []Subscription{}
	}
	views := make([]JoinRequest, 0, len(items))
	for i := range items {
		views = append(views, h.buildJoinRequestView(r.Context(), &items[i]))
	}
	writeJSON(w, http.StatusOK, views)
}

func (h *Handler) ApproveJoinRequest(w http.ResponseWriter, r *http.Request) {
	subID := strings.TrimSpace(r.PathValue("id"))
	if subID == "" {
		writeJSON(w, http.StatusBadRequest, errBody("join request id required"))
		return
	}

	cmd := joinRequestDecisionCommand{RequestID: subID}
	if existing, err := h.store.GetByID(r.Context(), subID); err == nil && existing != nil {
		cmd.FleetID = strings.TrimSpace(existing.FleetID)
	}
	if err := h.enqueueCommand(
		r.Context(),
		joinRequestApproveOperationType,
		"Passenger join request approval accepted for async processing.",
		joinRequestApproveCommandSubject,
		r.Header.Get("Idempotency-Key"),
		&cmd,
	); err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody("queue publish failed"))
		return
	}

	writeJSON(w, http.StatusAccepted, opsvc.CommandAccepted{
		OperationID: cmd.OperationID,
		Status:      opsvc.StatusQueued,
		Message:     "Passenger join request approval queued.",
	})
}

func (h *Handler) DenyJoinRequest(w http.ResponseWriter, r *http.Request) {
	subID := strings.TrimSpace(r.PathValue("id"))
	if subID == "" {
		writeJSON(w, http.StatusBadRequest, errBody("join request id required"))
		return
	}

	cmd := joinRequestDecisionCommand{RequestID: subID}
	if existing, err := h.store.GetByID(r.Context(), subID); err == nil && existing != nil {
		cmd.FleetID = strings.TrimSpace(existing.FleetID)
	}
	if err := h.enqueueCommand(
		r.Context(),
		joinRequestDenyOperationType,
		"Passenger join request denial accepted for async processing.",
		joinRequestDenyCommandSubject,
		r.Header.Get("Idempotency-Key"),
		&cmd,
	); err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody("queue publish failed"))
		return
	}

	writeJSON(w, http.StatusAccepted, opsvc.CommandAccepted{
		OperationID: cmd.OperationID,
		Status:      opsvc.StatusQueued,
		Message:     "Passenger join request denial queued.",
	})
}

// Update modifies a subscription's preferences or status.
func (h *Handler) Update(w http.ResponseWriter, r *http.Request) {
	subID := r.PathValue("id")
	userID := userIDFromRequest(r)
	if userID == "" {
		writeJSON(w, http.StatusBadRequest, errBody("user_id required"))
		return
	}

	existing, err := h.store.Get(r.Context(), userID, subID)
	if err != nil {
		writeJSON(w, http.StatusNotFound, errBody("subscription not found"))
		return
	}

	var update Subscription
	if err := json.NewDecoder(r.Body).Decode(&update); err != nil {
		writeJSON(w, http.StatusBadRequest, errBody("invalid json"))
		return
	}
	if update.Status != "" {
		existing.Status = update.Status
	}
	// Only override preferences if provided (non-zero).
	if update.Preferences != (SubPrefs{}) {
		existing.Preferences = update.Preferences
	}
	existing.UpdatedAt = time.Now().UTC()

	if err := h.store.Put(r.Context(), existing); err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody("update failed"))
		return
	}
	writeJSON(w, http.StatusOK, existing)
}

// Cancel soft-deletes a subscription by setting status to "cancelled".
func (h *Handler) Cancel(w http.ResponseWriter, r *http.Request) {
	subID := r.PathValue("id")
	userID := userIDFromRequest(r)
	if userID == "" {
		writeJSON(w, http.StatusBadRequest, errBody("user_id required"))
		return
	}
	existing, err := h.store.Get(r.Context(), userID, subID)
	if err != nil {
		writeJSON(w, http.StatusNotFound, errBody("subscription not found"))
		return
	}
	existing.Status = "cancelled"
	existing.UpdatedAt = time.Now().UTC()
	if err := h.store.Put(r.Context(), existing); err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody("cancel failed"))
		return
	}
	writeJSON(w, http.StatusOK, existing)
}

// ListForVehicle returns all active subscribers for a vehicle, enriched with passenger info.
func (h *Handler) ListForVehicle(w http.ResponseWriter, r *http.Request) {
	vehicleID := r.PathValue("vehicleId")
	subs, err := h.store.ListForVehicle(r.Context(), vehicleID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody(err.Error()))
		return
	}
	if subs == nil {
		subs = []Subscription{}
	}

	// Enrich with passenger info
	type enrichedSub struct {
		Subscription
		PassengerName  string `json:"passenger_name,omitempty"`
		PassengerEmail string `json:"passenger_email,omitempty"`
	}
	enriched := make([]enrichedSub, 0, len(subs))
	for _, sub := range subs {
		es := enrichedSub{Subscription: sub}
		if h.userStore != nil && sub.UserID != "" {
			if u, err := h.userStore.GetUser(r.Context(), sub.UserID); err == nil && u != nil {
				es.PassengerName = strings.TrimSpace(u.DisplayName)
				es.PassengerEmail = strings.TrimSpace(u.Email)
			}
		}
		enriched = append(enriched, es)
	}
	writeJSON(w, http.StatusOK, enriched)
}

// ListForFleet returns all subscriptions for a fleet, optionally filtered by status.
// Query params: ?status=active (defaults to "active" if omitted).
func (h *Handler) ListForFleet(w http.ResponseWriter, r *http.Request) {
	fleetID := r.PathValue("fleetId")
	if fleetID == "" {
		writeJSON(w, http.StatusBadRequest, errBody("fleet_id required"))
		return
	}
	status := strings.TrimSpace(r.URL.Query().Get("status"))
	if status == "" {
		status = "active"
	}
	subs, err := h.store.ListByFleetStatus(r.Context(), fleetID, status)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody(err.Error()))
		return
	}
	if subs == nil {
		subs = []Subscription{}
	}

	// Enrich with passenger info
	type enrichedSub struct {
		Subscription
		PassengerName  string `json:"passenger_name,omitempty"`
		PassengerEmail string `json:"passenger_email,omitempty"`
	}
	enriched := make([]enrichedSub, 0, len(subs))
	for _, sub := range subs {
		es := enrichedSub{Subscription: sub}
		if h.userStore != nil && sub.UserID != "" {
			if u, err := h.userStore.GetUser(r.Context(), sub.UserID); err == nil && u != nil {
				es.PassengerName = strings.TrimSpace(u.DisplayName)
				es.PassengerEmail = strings.TrimSpace(u.Email)
			}
		}
		enriched = append(enriched, es)
	}
	writeJSON(w, http.StatusOK, enriched)
}

// Revoke allows an admin/owner to cancel an active subscription.
// Does not require the subscriber's user_id — looks up by subscription ID only.
func (h *Handler) Revoke(w http.ResponseWriter, r *http.Request) {
	subID := r.PathValue("id")
	if subID == "" {
		writeJSON(w, http.StatusBadRequest, errBody("subscription id required"))
		return
	}
	existing, err := h.store.GetByID(r.Context(), subID)
	if err != nil {
		writeJSON(w, http.StatusNotFound, errBody("subscription not found"))
		return
	}
	if existing.Status == "cancelled" || existing.Status == "revoked" {
		writeJSON(w, http.StatusOK, existing)
		return
	}
	existing.Status = "revoked"
	existing.UpdatedAt = time.Now().UTC()
	if err := h.store.Put(r.Context(), existing); err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody("revoke failed"))
		return
	}
	log.Printf("[subsvc] subscription %s revoked by admin (fleet=%s, vehicle=%s, user=%s)",
		subID, existing.FleetID, existing.VehicleID, existing.UserID)
	writeJSON(w, http.StatusOK, existing)
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

func (h *Handler) processCreateJoinRequest(payload []byte) {
	var cmd createJoinRequestCommand
	if err := json.Unmarshal(payload, &cmd); err != nil {
		log.Printf("[join-requests] decode create command: %v", err)
		return
	}
	ctx := context.Background()
	op := h.loadOperation(ctx, cmd.OperationID, joinRequestCreateOperationType)
	h.markProcessing(ctx, op)

	existing, err := h.findExistingJoinRequest(ctx, cmd.Subscription.UserID, cmd.Subscription.VehicleID, cmd.Subscription.FleetID)
	if err != nil {
		h.markFailed(ctx, op, "Failed to check existing passenger access request.", err)
		return
	}
	if existing != nil {
		message := "Passenger access request is already pending."
		if existing.Status == "active" {
			message = "Passenger access is already active."
		}
		h.markSucceeded(ctx, op, existing.ID, message)
		return
	}

	now := time.Now().UTC()
	cmd.Subscription.Status = "pending"
	if cmd.Subscription.CreatedAt.IsZero() {
		cmd.Subscription.CreatedAt = now
	}
	cmd.Subscription.UpdatedAt = now

	if err := h.store.Put(ctx, &cmd.Subscription); err != nil {
		h.markFailed(ctx, op, "Failed to create passenger access request.", err)
		return
	}

	h.markSucceeded(ctx, op, cmd.Subscription.ID, "Passenger access request queued successfully.")
}

func (h *Handler) processApproveJoinRequest(payload []byte) {
	var cmd joinRequestDecisionCommand
	if err := json.Unmarshal(payload, &cmd); err != nil {
		log.Printf("[join-requests] decode approve command: %v", err)
		return
	}
	ctx := context.Background()
	op := h.loadOperation(ctx, cmd.OperationID, joinRequestApproveOperationType)
	h.markProcessing(ctx, op)

	existing, err := h.store.GetByID(ctx, cmd.RequestID)
	if err != nil {
		h.markFailed(ctx, op, "Passenger join request not found.", err)
		return
	}
	active, err := h.store.ListByFleetStatus(ctx, existing.FleetID, "active")
	if err != nil {
		h.markFailed(ctx, op, "Failed to check passenger quota.", err)
		return
	}
	if h.policy != nil {
		if _, err := h.policy.CheckPassengerActivate(ctx, existing.FleetID, len(active)); err != nil {
			h.markFailed(ctx, op, "Passenger activation blocked by tenant policy.", err)
			return
		}
	}
	existing.Status = "active"
	existing.UpdatedAt = time.Now().UTC()
	if err := h.store.Put(ctx, existing); err != nil {
		h.markFailed(ctx, op, "Failed to approve passenger join request.", err)
		return
	}

	h.markSucceeded(ctx, op, existing.ID, "Passenger join request approved.")
}

func (h *Handler) processDenyJoinRequest(payload []byte) {
	var cmd joinRequestDecisionCommand
	if err := json.Unmarshal(payload, &cmd); err != nil {
		log.Printf("[join-requests] decode deny command: %v", err)
		return
	}
	ctx := context.Background()
	op := h.loadOperation(ctx, cmd.OperationID, joinRequestDenyOperationType)
	h.markProcessing(ctx, op)

	existing, err := h.store.GetByID(ctx, cmd.RequestID)
	if err != nil {
		h.markFailed(ctx, op, "Passenger join request not found.", err)
		return
	}
	if existing.Status != "denied" {
		existing.Status = "denied"
		existing.UpdatedAt = time.Now().UTC()
		if err := h.store.Put(ctx, existing); err != nil {
			h.markFailed(ctx, op, "Failed to deny passenger join request.", err)
			return
		}
	}

	h.markSucceeded(ctx, op, existing.ID, "Passenger join request denied.")
}

func (h *Handler) findExistingJoinRequest(
	ctx context.Context,
	userID string,
	vehicleID string,
	fleetID string,
) (*Subscription, error) {
	items, err := h.store.ListForUser(ctx, userID)
	if err != nil {
		return nil, err
	}
	for i := range items {
		item := items[i]
		if item.VehicleID != vehicleID {
			continue
		}
		if fleetID != "" && item.FleetID != "" && item.FleetID != fleetID {
			continue
		}
		if item.Status == "pending" || item.Status == "active" {
			copy := item
			return &copy, nil
		}
	}
	return nil, nil
}

func (h *Handler) buildJoinRequestView(ctx context.Context, item *Subscription) JoinRequest {
	view := JoinRequest{
		ID:          item.ID,
		UserID:      item.UserID,
		VehicleID:   item.VehicleID,
		FleetID:     item.FleetID,
		Status:      item.Status,
		Preferences: item.Preferences,
		CreatedAt:   item.CreatedAt,
		UpdatedAt:   item.UpdatedAt,
		ExpiresAt:   item.ExpiresAt,
	}
	if h.userStore == nil {
		return view
	}

	userID := strings.TrimSpace(item.UserID)
	if userID == "" {
		return view
	}
	user, err := h.userStore.GetUser(ctx, userID)
	if err != nil || user == nil {
		return view
	}

	view.PassengerName = strings.TrimSpace(user.DisplayName)
	view.PassengerEmail = strings.TrimSpace(user.Email)
	view.PassengerPhone = strings.TrimSpace(user.Phone)
	view.PassengerWorkplace = strings.TrimSpace(user.Workplace)
	view.PassengerAddress = strings.TrimSpace(user.Address)
	view.PassengerEmployeeNumber = strings.TrimSpace(user.EmployeeNo)
	if !user.CreatedAt.IsZero() {
		joinedAt := user.CreatedAt
		view.PassengerJoinedAt = &joinedAt
	}
	return view
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
			case *createJoinRequestCommand:
				cmd.OperationID = existing.ID
			case *joinRequestDecisionCommand:
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
		FleetID:        fleetIDFromJoinCommand(command),
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
	case *createJoinRequestCommand:
		cmd.OperationID = opID
	case *joinRequestDecisionCommand:
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

func fleetIDFromJoinCommand(command any) string {
	switch cmd := command.(type) {
	case *createJoinRequestCommand:
		return strings.TrimSpace(cmd.Subscription.FleetID)
	case *joinRequestDecisionCommand:
		return strings.TrimSpace(cmd.FleetID)
	default:
		return ""
	}
}

func (h *Handler) markProcessing(ctx context.Context, op *opsvc.Operation) {
	op.Status = opsvc.StatusProcessing
	op.UpdatedAt = time.Now().UTC()
	if err := h.opsStore.Put(ctx, op); err != nil {
		log.Printf("[join-requests] persist processing operation %s: %v", op.ID, err)
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
		log.Printf("[join-requests] persist success operation %s: %v", op.ID, err)
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
		log.Printf("[join-requests] persist failed operation %s: %v", op.ID, err)
	}
	_ = opsvc.PublishUpdate(h.broker, op)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func userIDFromRequest(r *http.Request) string {
	if uid := r.URL.Query().Get("user_id"); uid != "" {
		return uid
	}
	if id := auth.IdentityFromContext(r.Context()); id.UserID != "" {
		return id.UserID
	}
	return ""
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

func writePolicyError(w http.ResponseWriter, err error) {
	if pe, ok := tenantsvc.AsPolicyError(err); ok {
		body := map[string]string{
			"error": pe.Message,
			"code":  pe.Code,
		}
		writeJSON(w, pe.HTTPStatus, body)
		return
	}
	writeJSON(w, http.StatusForbidden, errBody(err.Error()))
}
