// Package authsvc provides self-hosted JWT-based authentication.
// It replaces Firebase Auth with bcrypt password hashing, HMAC-SHA256
// tokens, and NATS KV user storage.
package authsvc

import "time"

// User is the persistent user record stored in NATS KV.
type User struct {
	ID           string    `json:"id"`
	Email        string    `json:"email"`
	PasswordHash string    `json:"password_hash,omitempty"` // bcrypt hash – never sent to clients
	DisplayName  string    `json:"display_name"`
	Phone        string    `json:"phone,omitempty"`
	PhotoURL     string    `json:"photo_url,omitempty"`
	Role         string    `json:"role"`     // owner | admin | driver | passenger
	FleetID      string    `json:"fleet_id"` // tenant scope
	VehicleID    string    `json:"vehicle_id,omitempty"`
	IsActive     bool      `json:"is_active"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
	LastLoginAt  time.Time `json:"last_login_at,omitempty"`
}

// UserPublic is the public-facing user profile (no password hash).
type UserPublic struct {
	ID          string    `json:"id"`
	Email       string    `json:"email"`
	DisplayName string    `json:"display_name"`
	Phone       string    `json:"phone,omitempty"`
	PhotoURL    string    `json:"photo_url,omitempty"`
	Role        string    `json:"role"`
	FleetID     string    `json:"fleet_id"`
	VehicleID   string    `json:"vehicle_id,omitempty"`
	IsActive    bool      `json:"is_active"`
	CreatedAt   time.Time `json:"created_at"`
}

// ToPublic strips sensitive fields.
func (u *User) ToPublic() UserPublic {
	return UserPublic{
		ID:          u.ID,
		Email:       u.Email,
		DisplayName: u.DisplayName,
		Phone:       u.Phone,
		PhotoURL:    u.PhotoURL,
		Role:        u.Role,
		FleetID:     u.FleetID,
		VehicleID:   u.VehicleID,
		IsActive:    u.IsActive,
		CreatedAt:   u.CreatedAt,
	}
}

// TokenPair is returned on login/register/refresh.
type TokenPair struct {
	AccessToken  string     `json:"access_token"`
	RefreshToken string     `json:"refresh_token"`
	ExpiresAt    time.Time  `json:"expires_at"`
	User         UserPublic `json:"user"`
}

// ---------------------------------------------------------------------------
// Request / Response DTOs
// ---------------------------------------------------------------------------

type LoginRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

type RegisterRequest struct {
	Email       string `json:"email"`
	Password    string `json:"password"`
	DisplayName string `json:"display_name"`
	Role        string `json:"role,omitempty"`  // defaults to "passenger"
	FleetID     string `json:"fleet_id,omitempty"`
}

type RefreshRequest struct {
	RefreshToken string `json:"refresh_token"`
}

type UpdateProfileRequest struct {
	DisplayName string `json:"display_name,omitempty"`
	Phone       string `json:"phone,omitempty"`
	PhotoURL    string `json:"photo_url,omitempty"`
}

type ChangePasswordRequest struct {
	OldPassword string `json:"old_password"`
	NewPassword string `json:"new_password"`
}

type ForgotPasswordRequest struct {
	Email string `json:"email"`
}
