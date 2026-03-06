package handler

import (
	"encoding/json"
	"net/http"

	"via-backend/internal/auth"
	"via-backend/internal/fleetsvc"
	"via-backend/internal/model"
	"via-backend/internal/service"
	"via-backend/internal/tenantsvc"
)

// TripStart handles POST /v1/trip/start.
func TripStart(
	svc *service.EventService,
	policy *tenantsvc.Policy,
	assignments fleetsvc.FleetStore,
) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}

		var p model.RealtimeEvent
		if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
			writeError(w, http.StatusBadRequest, "invalid json payload")
			return
		}
		if identity := auth.IdentityFromContext(r.Context()); identity.Role == auth.RoleDriver {
			p.DriverID = identity.UserID
		}
		if err := authorizeDriverRealtimePublish(
			r.Context(),
			assignments,
			p.FleetID,
			p.VehicleID,
		); err != nil {
			writeError(w, http.StatusForbidden, err.Error())
			return
		}
		if policy != nil {
			if _, err := policy.CheckEventPublish(r.Context(), p.FleetID, p.DriverID); err != nil {
				if writePolicyError(w, err) {
					return
				}
				writeError(w, http.StatusBadRequest, err.Error())
				return
			}
		}

		res, err := svc.PublishTripStart(p)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}

		writeJSON(w, http.StatusAccepted, model.PublishAck{
			Status:  "published",
			Subject: res.Subject,
		})
	}
}

// EventPublish handles POST /v1/events/publish.
func EventPublish(
	svc *service.EventService,
	policy *tenantsvc.Policy,
	assignments fleetsvc.FleetStore,
) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}

		var p model.RealtimeEvent
		if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
			writeError(w, http.StatusBadRequest, "invalid json payload")
			return
		}
		if identity := auth.IdentityFromContext(r.Context()); identity.Role == auth.RoleDriver {
			p.DriverID = identity.UserID
		}
		if err := authorizeDriverRealtimePublish(
			r.Context(),
			assignments,
			p.FleetID,
			p.VehicleID,
		); err != nil {
			writeError(w, http.StatusForbidden, err.Error())
			return
		}
		if policy != nil {
			if _, err := policy.CheckEventPublish(r.Context(), p.FleetID, p.DriverID); err != nil {
				if writePolicyError(w, err) {
					return
				}
				writeError(w, http.StatusBadRequest, err.Error())
				return
			}
		}

		res, err := svc.Publish(p)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}

		writeJSON(w, http.StatusAccepted, model.PublishAck{
			Status:  "published",
			Subject: res.Subject,
		})
	}
}
