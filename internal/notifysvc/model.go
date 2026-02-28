package notifysvc

import "time"

// Notification represents a push notification stored and delivered via WebSocket.
type Notification struct {
	ID        string            `json:"id"`
	UserID    string            `json:"user_id"`
	FleetID   string            `json:"fleet_id,omitempty"`
	VehicleID string            `json:"vehicle_id,omitempty"`
	Type      string            `json:"type"`       // e.g. "vehicle_arriving", "event", "announcement", "subscription_update"
	Title     string            `json:"title"`
	Body      string            `json:"body"`
	Data      map[string]string `json:"data,omitempty"`
	IsRead    bool              `json:"is_read"`
	CreatedAt time.Time         `json:"created_at"`
	ReadAt    time.Time         `json:"read_at,omitempty"`
}

// NotificationPayload is the JSON envelope pushed over WebSocket.
type NotificationPayload struct {
	Action       string        `json:"action"` // "new", "read", "bootstrap"
	Notification *Notification `json:"notification,omitempty"`
	Items        []Notification `json:"items,omitempty"`   // used for "bootstrap"
	UnreadCount  int           `json:"unread_count"`
}
