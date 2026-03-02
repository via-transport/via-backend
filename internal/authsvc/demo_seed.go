package authsvc

import (
	"context"
	"fmt"
	"strings"
	"time"

	"golang.org/x/crypto/bcrypt"
)

const (
	demoPassengerID          = "psg_demo_001"
	demoPassengerEmail       = "demo.passenger@via.local"
	demoPassengerPassword    = "Demo@123456"
	demoPassengerDisplayName = "Ayesha Perera"
	demoPassengerPhone       = "+94775551188"

	demoDriverID          = "drv_001"
	demoDriverEmail       = "demo.driver@via.local"
	demoDriverPassword    = "Demo@123456"
	demoDriverDisplayName = "Kamal Perera Prime"
	demoDriverPhone       = "+94770001122"
)

// EnsureDemoPassenger provisions a stable passenger account for local demo and
// QA flows. It uses the real auth store so the mobile app can log in with a
// valid JWT instead of relying on a local fake session.
func EnsureDemoPassenger(
	ctx context.Context,
	store UserStore,
	fleetID string,
) error {
	now := time.Now().UTC()

	hash, err := bcrypt.GenerateFromPassword(
		[]byte(demoPassengerPassword),
		bcrypt.DefaultCost,
	)
	if err != nil {
		return fmt.Errorf("hash demo passenger password: %w", err)
	}

	user, err := store.GetUserByEmail(ctx, demoPassengerEmail)
	if err != nil {
		user = &User{
			ID:           demoPassengerID,
			Email:        demoPassengerEmail,
			PasswordHash: string(hash),
			DisplayName:  demoPassengerDisplayName,
			Phone:        demoPassengerPhone,
			Role:         "passenger",
			FleetID:      fleetID,
			IsActive:     true,
			CreatedAt:    now,
			UpdatedAt:    now,
			LastLoginAt:  now,
		}
		if err := store.CreateUser(ctx, user); err != nil {
			return fmt.Errorf("create demo passenger: %w", err)
		}
		return nil
	}

	changed := false
	if strings.TrimSpace(user.DisplayName) != demoPassengerDisplayName {
		user.DisplayName = demoPassengerDisplayName
		changed = true
	}
	if strings.TrimSpace(user.Phone) != demoPassengerPhone {
		user.Phone = demoPassengerPhone
		changed = true
	}
	if user.Role != "passenger" {
		user.Role = "passenger"
		changed = true
	}
	if fleetID != "" && user.FleetID != fleetID {
		user.FleetID = fleetID
		changed = true
	}
	if !user.IsActive {
		user.IsActive = true
		changed = true
	}
	if bcrypt.CompareHashAndPassword(
		[]byte(user.PasswordHash),
		[]byte(demoPassengerPassword),
	) != nil {
		user.PasswordHash = string(hash)
		changed = true
	}

	if !changed {
		return nil
	}

	if user.CreatedAt.IsZero() {
		user.CreatedAt = now
	}
	user.UpdatedAt = now
	if user.LastLoginAt.IsZero() {
		user.LastLoginAt = now
	}
	if err := store.UpdateUser(ctx, user); err != nil {
		return fmt.Errorf("update demo passenger: %w", err)
	}
	return nil
}

func EnsureDemoDriver(
	ctx context.Context,
	store UserStore,
	fleetID string,
) error {
	now := time.Now().UTC()

	hash, err := bcrypt.GenerateFromPassword(
		[]byte(demoDriverPassword),
		bcrypt.DefaultCost,
	)
	if err != nil {
		return fmt.Errorf("hash demo driver password: %w", err)
	}

	user, err := store.GetUserByEmail(ctx, demoDriverEmail)
	if err != nil {
		user = &User{
			ID:           demoDriverID,
			Email:        demoDriverEmail,
			PasswordHash: string(hash),
			DisplayName:  demoDriverDisplayName,
			Phone:        demoDriverPhone,
			Role:         "driver",
			FleetID:      fleetID,
			IsActive:     true,
			CreatedAt:    now,
			UpdatedAt:    now,
			LastLoginAt:  now,
		}
		if err := store.CreateUser(ctx, user); err != nil {
			return fmt.Errorf("create demo driver: %w", err)
		}
		return nil
	}

	changed := false
	if strings.TrimSpace(user.DisplayName) != demoDriverDisplayName {
		user.DisplayName = demoDriverDisplayName
		changed = true
	}
	if strings.TrimSpace(user.Phone) != demoDriverPhone {
		user.Phone = demoDriverPhone
		changed = true
	}
	if user.Role != "driver" {
		user.Role = "driver"
		changed = true
	}
	if fleetID != "" && user.FleetID != fleetID {
		user.FleetID = fleetID
		changed = true
	}
	if !user.IsActive {
		user.IsActive = true
		changed = true
	}
	if bcrypt.CompareHashAndPassword(
		[]byte(user.PasswordHash),
		[]byte(demoDriverPassword),
	) != nil {
		user.PasswordHash = string(hash)
		changed = true
	}

	if !changed {
		return nil
	}

	if user.CreatedAt.IsZero() {
		user.CreatedAt = now
	}
	user.UpdatedAt = now
	if user.LastLoginAt.IsZero() {
		user.LastLoginAt = now
	}
	if err := store.UpdateUser(ctx, user); err != nil {
		return fmt.Errorf("update demo driver: %w", err)
	}
	return nil
}
