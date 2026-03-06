package requestsvc

import "context"

type Store interface {
	Put(ctx context.Context, req *DriverRequest) error
	Get(ctx context.Context, id string) (*DriverRequest, error)
	List(ctx context.Context, fleetID, status, requestType string) ([]DriverRequest, error)
	FindPendingByUser(ctx context.Context, fleetID, userID, requestType string) (*DriverRequest, error)
}
