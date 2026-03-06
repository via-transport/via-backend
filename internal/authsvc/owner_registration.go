package authsvc

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"time"

	"via-backend/internal/tenantsvc"
)

type OwnerAccountRegistrar interface {
	RegisterOwner(ctx context.Context, user *User, fleetName string) error
}

type OwnerFleetProvisioner interface {
	SetupOwnerFleet(ctx context.Context, userID, fleetName string) (*User, error)
}

type ownerTransactionalRegistrar interface {
	CreateOwnerWithTenant(ctx context.Context, user *User, tenant *tenantsvc.Tenant) error
}

type ownerFleetTransactionalStore interface {
	SetupOwnerFleet(ctx context.Context, userID string, tenant *tenantsvc.Tenant) (*User, error)
}

type ownerAccountRegistrar struct {
	users   UserStore
	tenants tenantsvc.Store
}

func NewOwnerAccountRegistrar(users UserStore, tenants tenantsvc.Store) *ownerAccountRegistrar {
	if users == nil || tenants == nil {
		return nil
	}
	return &ownerAccountRegistrar{
		users:   users,
		tenants: tenants,
	}
}

func (r *ownerAccountRegistrar) RegisterOwner(ctx context.Context, user *User, fleetName string) error {
	if r == nil {
		return errors.New("owner registration unavailable")
	}

	now := user.CreatedAt
	if now.IsZero() {
		now = time.Now().UTC()
	}
	tenant := tenantsvc.DefaultTrialTenant(user.FleetID, strings.TrimSpace(fleetName), now)

	if txRegistrar, ok := r.users.(ownerTransactionalRegistrar); ok {
		return txRegistrar.CreateOwnerWithTenant(ctx, user, tenant)
	}

	if _, err := r.users.GetUserByEmail(ctx, user.Email); err == nil {
		return errors.New("email already registered")
	}
	if _, err := r.tenants.Get(ctx, tenant.ID); err == nil {
		return errors.New("fleet already registered")
	}
	if err := r.tenants.Put(ctx, tenant); err != nil {
		return err
	}
	return r.users.CreateUser(ctx, user)
}

func (r *ownerAccountRegistrar) SetupOwnerFleet(ctx context.Context, userID, fleetName string) (*User, error) {
	if r == nil {
		return nil, errors.New("owner fleet setup unavailable")
	}

	fleetName = strings.TrimSpace(fleetName)
	if fleetName == "" {
		return nil, errors.New("fleet_name is required")
	}

	user, err := r.users.GetUser(ctx, userID)
	if err != nil {
		return nil, err
	}
	if user.Role != "owner" {
		return nil, errors.New("only owners can create fleets")
	}
	if strings.TrimSpace(user.FleetID) != "" {
		return nil, errors.New("owner already linked to a fleet")
	}

	now := time.Now().UTC()
	tenant := tenantsvc.DefaultTrialTenant(deriveFleetID(fleetName), fleetName, now)
	if txStore, ok := r.users.(ownerFleetTransactionalStore); ok {
		return txStore.SetupOwnerFleet(ctx, userID, tenant)
	}

	if _, err := r.tenants.Get(ctx, tenant.ID); err == nil {
		return nil, errors.New("fleet already registered")
	}
	if err := r.tenants.Put(ctx, tenant); err != nil {
		return nil, err
	}

	user.FleetID = tenant.ID
	user.UpdatedAt = now
	if err := r.users.UpdateUser(ctx, user); err != nil {
		return nil, err
	}
	return user, nil
}

var fleetSlugSanitizer = regexp.MustCompile(`[^a-z0-9]+`)

func deriveFleetID(fleetName string) string {
	normalized := strings.ToLower(strings.TrimSpace(fleetName))
	normalized = fleetSlugSanitizer.ReplaceAllString(normalized, "-")
	normalized = strings.Trim(normalized, "-")
	if normalized == "" {
		return "fleet"
	}
	return normalized
}
