package tenantsvc

import "context"

func EnsureDemoTenant(ctx context.Context, store Store, tenantID string) error {
	if tenantID == "" {
		return nil
	}
	if _, err := store.Get(ctx, tenantID); err == nil {
		return nil
	}
	return store.Put(ctx, DefaultTrialTenant(tenantID, "Via Demo Fleet", nowUTC()))
}
