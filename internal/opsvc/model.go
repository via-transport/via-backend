package opsvc

import "time"

const (
	StatusQueued     = "queued"
	StatusProcessing = "processing"
	StatusSucceeded  = "succeeded"
	StatusFailed     = "failed"
)

type Operation struct {
	ID             string    `json:"id"`
	Type           string    `json:"type"`
	FleetID        string    `json:"fleet_id,omitempty"`
	IdempotencyKey string    `json:"idempotency_key,omitempty"`
	Status         string    `json:"status"`
	ResourceID     string    `json:"resource_id,omitempty"`
	Message        string    `json:"message,omitempty"`
	ErrorMessage   string    `json:"error_message,omitempty"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

type ListFilter struct {
	Limit   int
	Type    string
	Status  string
	FleetID string
}

type CommandAccepted struct {
	OperationID string `json:"operation_id"`
	Status      string `json:"status"`
	Message     string `json:"message"`
}
