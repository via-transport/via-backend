// Package auth provides fine-grained role-based access control (RBAC).
//
// Architecture:
//
//	JWT (Firebase/custom) → auth.Middleware extracts claims → Identity on context
//	→ handler reads Identity → calls auth.Can(identity, action, resource)
//
// Roles: owner, admin, driver, passenger, service (machine-to-machine).
// Each role has a fixed set of permissions. Fleet-scoped resources are checked
// against Identity.FleetID so owners cannot access other tenants' data.
package auth

import "fmt"

// ---------------------------------------------------------------------------
// Roles
// ---------------------------------------------------------------------------

// Role is a string enum for user roles.
type Role string

const (
	RoleOwner     Role = "owner"
	RoleAdmin     Role = "admin"
	RoleDriver    Role = "driver"
	RolePassenger Role = "passenger"
	RoleService   Role = "service" // machine-to-machine (internal)
)

// ---------------------------------------------------------------------------
// Actions & Resources
// ---------------------------------------------------------------------------

// Action represents an operation in the system.
type Action string

const (
	ActionRead    Action = "read"
	ActionWrite   Action = "write"
	ActionDelete  Action = "delete"
	ActionPublish Action = "publish"
)

// Resource represents a domain object.
type Resource string

const (
	ResourceGPS       Resource = "gps"
	ResourceTrip      Resource = "trip"
	ResourceEvent     Resource = "event"
	ResourceFleet     Resource = "fleet"
	ResourceVehicle   Resource = "vehicle"
	ResourceDriver    Resource = "driver"
	ResourceRoute     Resource = "route"
	ResourceAnalytics Resource = "analytics"
	ResourceAdmin     Resource = "admin"
)

// ---------------------------------------------------------------------------
// Permission table
// ---------------------------------------------------------------------------

// permission is a single allowed action on a resource.
type permission struct {
	Action   Action
	Resource Resource
}

// rolePermissions defines what each role can do. This is the single source
// of truth for access control policy.
var rolePermissions = map[Role][]permission{
	RoleOwner: {
		{ActionRead, ResourceGPS},
		{ActionWrite, ResourceGPS},
		{ActionRead, ResourceTrip},
		{ActionWrite, ResourceTrip},
		{ActionRead, ResourceEvent},
		{ActionWrite, ResourceEvent},
		{ActionPublish, ResourceEvent},
		{ActionRead, ResourceFleet},
		{ActionWrite, ResourceFleet},
		{ActionDelete, ResourceFleet},
		{ActionRead, ResourceVehicle},
		{ActionWrite, ResourceVehicle},
		{ActionDelete, ResourceVehicle},
		{ActionRead, ResourceDriver},
		{ActionWrite, ResourceDriver},
		{ActionDelete, ResourceDriver},
		{ActionRead, ResourceRoute},
		{ActionWrite, ResourceRoute},
		{ActionDelete, ResourceRoute},
		{ActionRead, ResourceAnalytics},
		{ActionRead, ResourceAdmin},
		{ActionWrite, ResourceAdmin},
	},
	RoleAdmin: {
		{ActionRead, ResourceGPS},
		{ActionWrite, ResourceGPS},
		{ActionRead, ResourceTrip},
		{ActionWrite, ResourceTrip},
		{ActionRead, ResourceEvent},
		{ActionWrite, ResourceEvent},
		{ActionPublish, ResourceEvent},
		{ActionRead, ResourceFleet},
		{ActionRead, ResourceVehicle},
		{ActionWrite, ResourceVehicle},
		{ActionRead, ResourceDriver},
		{ActionWrite, ResourceDriver},
		{ActionRead, ResourceRoute},
		{ActionWrite, ResourceRoute},
		{ActionRead, ResourceAnalytics},
		{ActionRead, ResourceAdmin},
	},
	RoleDriver: {
		{ActionRead, ResourceGPS},
		{ActionWrite, ResourceGPS},
		{ActionPublish, ResourceGPS},
		{ActionRead, ResourceTrip},
		{ActionWrite, ResourceTrip},
		{ActionPublish, ResourceEvent},
		{ActionRead, ResourceRoute},
		{ActionRead, ResourceVehicle},
	},
	RolePassenger: {
		{ActionRead, ResourceGPS},
		{ActionRead, ResourceTrip},
		{ActionRead, ResourceRoute},
		{ActionRead, ResourceVehicle},
		{ActionRead, ResourceEvent},
	},
	RoleService: {
		// Internal services have full access.
		{ActionRead, ResourceGPS},
		{ActionWrite, ResourceGPS},
		{ActionPublish, ResourceGPS},
		{ActionRead, ResourceTrip},
		{ActionWrite, ResourceTrip},
		{ActionRead, ResourceEvent},
		{ActionWrite, ResourceEvent},
		{ActionPublish, ResourceEvent},
		{ActionRead, ResourceFleet},
		{ActionWrite, ResourceFleet},
		{ActionRead, ResourceVehicle},
		{ActionWrite, ResourceVehicle},
		{ActionRead, ResourceDriver},
		{ActionWrite, ResourceDriver},
		{ActionRead, ResourceRoute},
		{ActionWrite, ResourceRoute},
		{ActionRead, ResourceAnalytics},
		{ActionRead, ResourceAdmin},
		{ActionWrite, ResourceAdmin},
	},
}

// ---------------------------------------------------------------------------
// Identity
// ---------------------------------------------------------------------------

// Identity represents an authenticated principal. It is attached to the
// request context by the auth middleware.
type Identity struct {
	UserID    string `json:"user_id"`
	Email     string `json:"email,omitempty"`
	Role      Role   `json:"role"`
	FleetID   string `json:"fleet_id,omitempty"` // tenant scope
	VehicleID string `json:"vehicle_id,omitempty"`
}

// ---------------------------------------------------------------------------
// Policy evaluation
// ---------------------------------------------------------------------------

// Can returns true if the identity's role grants the given action on the
// given resource.
func Can(id Identity, action Action, resource Resource) bool {
	perms, ok := rolePermissions[id.Role]
	if !ok {
		return false
	}
	for _, p := range perms {
		if p.Action == action && p.Resource == resource {
			return true
		}
	}
	return false
}

// MustCan is like Can but returns a descriptive error instead of a bool.
func MustCan(id Identity, action Action, resource Resource) error {
	if !Can(id, action, resource) {
		return fmt.Errorf("forbidden: role %q cannot %s %s", id.Role, action, resource)
	}
	return nil
}

// CanAccessFleet checks both permission and tenant isolation.
// Returns true only if the identity has the given permission AND the
// identity's fleet matches the target fleet (or the identity is a service).
func CanAccessFleet(id Identity, action Action, resource Resource, targetFleetID string) bool {
	if !Can(id, action, resource) {
		return false
	}
	// Service role bypasses tenant isolation (for internal microservices).
	if id.Role == RoleService {
		return true
	}
	// Tenant isolation: identity's fleet must match.
	return id.FleetID == targetFleetID
}
