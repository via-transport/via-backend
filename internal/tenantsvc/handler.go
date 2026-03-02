package tenantsvc

import (
	"encoding/json"
	"net/http"
	"strings"
)

type Handler struct {
	store  Store
	policy *Policy
}

func NewHandler(store Store, policy *Policy) *Handler {
	return &Handler{store: store, policy: policy}
}

func (h *Handler) Mount(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/v1/tenants", h.CreateTenant)
	mux.HandleFunc("GET /api/v1/tenants/{id}", h.GetTenant)
	mux.HandleFunc("GET /api/v1/billing/plan", h.GetPlan)
	mux.HandleFunc("POST /api/v1/billing/start-trial", h.StartTrial)
	mux.HandleFunc("POST /api/v1/billing/status", h.UpdateStatus)
	mux.HandleFunc("GET /api/v1/service-status", h.GetServiceStatus)
}

func (h *Handler) CreateTenant(w http.ResponseWriter, r *http.Request) {
	var req CreateTenantRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, errBody("invalid json"))
		return
	}
	req.ID = strings.TrimSpace(req.ID)
	req.Name = strings.TrimSpace(req.Name)
	if req.ID == "" || req.Name == "" {
		writeJSON(w, http.StatusBadRequest, errBody("id and name required"))
		return
	}

	now := nowUTC()
	tenant := DefaultTrialTenant(req.ID, req.Name, now)
	view, err := h.policy.UpsertTenant(r.Context(), tenant)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody(err.Error()))
		return
	}
	writeJSON(w, http.StatusCreated, view)
}

func (h *Handler) GetTenant(w http.ResponseWriter, r *http.Request) {
	tenantID := strings.TrimSpace(r.PathValue("id"))
	if tenantID == "" {
		writeJSON(w, http.StatusBadRequest, errBody("tenant id required"))
		return
	}
	tenant, err := h.store.Get(r.Context(), tenantID)
	if err != nil {
		writeJSON(w, http.StatusNotFound, errBody("tenant not found"))
		return
	}
	writeJSON(w, http.StatusOK, tenant)
}

func (h *Handler) GetPlan(w http.ResponseWriter, r *http.Request) {
	tenantID := strings.TrimSpace(r.URL.Query().Get("fleet_id"))
	view, err := h.policy.GetOrCreatePlan(r.Context(), tenantID)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errBody(err.Error()))
		return
	}
	writeJSON(w, http.StatusOK, view)
}

func (h *Handler) StartTrial(w http.ResponseWriter, r *http.Request) {
	var req StartTrialRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, errBody("invalid json"))
		return
	}
	req.FleetID = strings.TrimSpace(req.FleetID)
	req.Name = strings.TrimSpace(req.Name)
	if req.FleetID == "" {
		writeJSON(w, http.StatusBadRequest, errBody("fleet_id required"))
		return
	}
	tenant := DefaultTrialTenant(req.FleetID, req.Name, nowUTC())
	view, err := h.policy.UpsertTenant(r.Context(), tenant)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody(err.Error()))
		return
	}
	writeJSON(w, http.StatusOK, view)
}

func (h *Handler) UpdateStatus(w http.ResponseWriter, r *http.Request) {
	var req UpdateBillingStatusRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, errBody("invalid json"))
		return
	}
	req.FleetID = strings.TrimSpace(req.FleetID)
	if req.FleetID == "" {
		writeJSON(w, http.StatusBadRequest, errBody("fleet_id required"))
		return
	}

	tenant, err := h.policy.ensureTenant(r.Context(), req.FleetID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody(err.Error()))
		return
	}
	if req.PlanType != "" {
		tenant.PlanType = strings.ToUpper(strings.TrimSpace(req.PlanType))
	}
	if req.SubscriptionStatus != "" {
		tenant.SubscriptionStatus = strings.ToUpper(strings.TrimSpace(req.SubscriptionStatus))
	}
	if tenant.SubscriptionStatus == StatusActive && tenant.PlanType == "" {
		tenant.PlanType = PlanBasic
	}
	view, err := h.policy.UpsertTenant(r.Context(), tenant)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody(err.Error()))
		return
	}
	writeJSON(w, http.StatusOK, view)
}

func (h *Handler) GetServiceStatus(w http.ResponseWriter, r *http.Request) {
	tenantID := strings.TrimSpace(r.URL.Query().Get("fleet_id"))
	view, err := h.policy.GetOrCreatePlan(r.Context(), tenantID)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errBody(err.Error()))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"fleet_id":          view.TenantID,
		"effective_status":  view.EffectiveStatus,
		"realtime_enabled":  view.RealtimeEnabled,
		"public_message":    view.PublicMessage,
		"location_interval": view.LocationPublishIntervalS,
	})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func errBody(msg string) map[string]string {
	return map[string]string{"error": msg}
}
