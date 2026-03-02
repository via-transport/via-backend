package tenantsvc

import "context"

type Store interface {
	Put(ctx context.Context, tenant *Tenant) error
	Get(ctx context.Context, tenantID string) (*Tenant, error)
	List(ctx context.Context) ([]Tenant, error)
}
