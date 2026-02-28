package handler

import (
	"encoding/json"
	"net/http"

	"via-backend/internal/model"
	"via-backend/internal/service"
)

// GPSIngest handles POST /v1/gps/update.
func GPSIngest(svc *service.GPSService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}

		var p model.GPSUpdate
		if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
			writeError(w, http.StatusBadRequest, "invalid json payload")
			return
		}

		res, err := svc.Ingest(r.Context(), p)
		if err != nil {
			// Distinguish validation errors from infra errors.
			if isValidationErr(err) {
				writeError(w, http.StatusBadRequest, err.Error())
			} else {
				writeError(w, http.StatusBadGateway, err.Error())
			}
			return
		}

		writeJSON(w, http.StatusAccepted, model.PublishAck{
			Status:  "published",
			Subject: res.Subject,
		})
	}
}

// isValidationErr is a simple heuristic – service layer returns plain errors
// with known messages for validation failures.
func isValidationErr(err error) bool {
	msg := err.Error()
	return msg == "fleet_id and vehicle_id are required"
}
