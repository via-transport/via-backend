package requestsvc

import "time"

const (
	StatusPending  = "pending"
	StatusApproved = "approved"
	StatusDenied   = "denied"
	StatusCanceled = "canceled"
	StatusRevoked  = "revoked"

	RequestTypeAccess            = "access"
	RequestTypeVehicleAssignment = "vehicle_assignment"
)

type DriverRequest struct {
	ID          string    `json:"id"`
	UserID      string    `json:"user_id"`
	FleetID     string    `json:"fleet_id"`
	RequestType string    `json:"request_type,omitempty"`
	VehicleID   string    `json:"vehicle_id,omitempty"`
	FullName    string    `json:"full_name"`
	Email       string    `json:"email"`
	Phone       string    `json:"phone,omitempty"`
	Note        string    `json:"note,omitempty"`
	Status      string    `json:"status"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}
