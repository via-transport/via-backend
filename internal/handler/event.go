package handler

import (
	"encoding/json"
	"net/http"

	"via-backend/internal/model"
	"via-backend/internal/service"
)

// TripStart handles POST /v1/trip/start.
func TripStart(svc *service.EventService) http.HandlerFunc {
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
func EventPublish(svc *service.EventService) http.HandlerFunc {
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
