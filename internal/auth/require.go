package auth

import (
	"net/http"
)

// Require returns a middleware that enforces a specific permission before
// allowing the request through. It reads Identity from context (set by
// auth.Middleware) and checks the RBAC policy.
//
// Usage:
//
//	mux.Handle("/v1/gps/update",
//	    auth.Require(auth.ActionWrite, auth.ResourceGPS)(handler))
func Require(action Action, resource Resource) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			id := IdentityFromContext(r.Context())
			if err := MustCan(id, action, resource); err != nil {
				writeAuthError(w, http.StatusForbidden, err.Error())
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// RequireFleet returns middleware that enforces both permission and tenant
// isolation. The fleetExtractor function reads the target fleet_id from the
// request (e.g. from a query param, path param, or JSON body).
func RequireFleet(action Action, resource Resource, fleetExtractor func(*http.Request) string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			id := IdentityFromContext(r.Context())
			targetFleet := fleetExtractor(r)

			if !CanAccessFleet(id, action, resource, targetFleet) {
				writeAuthError(w, http.StatusForbidden, "forbidden: insufficient fleet access")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// FleetFromQuery returns a fleet extractor that reads fleet_id from the URL query.
func FleetFromQuery(param string) func(*http.Request) string {
	return func(r *http.Request) string {
		return r.URL.Query().Get(param)
	}
}
