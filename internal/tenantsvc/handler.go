package tenantsvc

import (
	"encoding/json"
	"net/http"
	"sort"
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
	mux.HandleFunc("GET /api/v1/public/tenants", h.ListPublicTenants)
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

func (h *Handler) ListPublicTenants(w http.ResponseWriter, r *http.Request) {
	query := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("q")))
	if len(query) < 2 {
		writeJSON(w, http.StatusOK, []map[string]string{})
		return
	}

	tenants, err := h.store.List(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody("tenant lookup failed"))
		return
	}

	matches := make([]Tenant, 0, len(tenants))
	for _, tenant := range tenants {
		if !matchesTenantQuery(tenant, query) {
			continue
		}
		matches = append(matches, tenant)
	}
	sort.Slice(matches, func(i, j int) bool {
		left := strings.ToLower(strings.TrimSpace(matches[i].Name))
		right := strings.ToLower(strings.TrimSpace(matches[j].Name))
		if left == right {
			return matches[i].ID < matches[j].ID
		}
		return left < right
	})
	if len(matches) > 10 {
		matches = matches[:10]
	}

	results := make([]map[string]string, 0, len(matches))
	for _, tenant := range matches {
		results = append(results, map[string]string{
			"id":   tenant.ID,
			"name": tenant.Name,
		})
	}

	writeJSON(w, http.StatusOK, results)
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

func matchesTenantQuery(tenant Tenant, query string) bool {
	if query == "" {
		return false
	}
	haystack := strings.ToLower(strings.Join([]string{
		tenant.ID,
		tenant.Name,
	}, " "))
	return strings.Contains(haystack, query)
}
