package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"via-backend/internal/authsvc"
	"via-backend/internal/config"
	"via-backend/internal/database"
	"via-backend/internal/fleetsvc"
	"via-backend/internal/messaging"
	"via-backend/internal/notifysvc"
	"via-backend/internal/opsvc"
	"via-backend/internal/requestsvc"
	"via-backend/internal/subsvc"
	"via-backend/internal/tenantsvc"
)

type bucketSummary struct {
	Name     string
	Migrated int
	Failed   int
	Skipped  bool
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	cfg := config.Load()
	broker, err := messaging.NewBroker(cfg)
	if err != nil {
		log.Fatalf("[migrate] nats connect: %v", err)
	}
	defer broker.Close()

	pgCfg := database.ConfigFromEnv()
	pgPool, err := database.Connect(ctx, pgCfg)
	if err != nil {
		log.Fatalf("[migrate] postgres connect: %v", err)
	}
	defer pgPool.Close()

	if err := database.Migrate(ctx, pgPool); err != nil {
		log.Fatalf("[migrate] postgres migration: %v", err)
	}

	authTarget := authsvc.NewPGStore(pgPool)
	fleetTarget := fleetsvc.NewPGStore(pgPool)
	notifyTarget := notifysvc.NewPGStore(pgPool)
	subTarget := subsvc.NewPGStore(pgPool)
	tenantTarget := tenantsvc.NewPGStore(pgPool)
	requestTarget := requestsvc.NewPGStore(pgPool)
	opsTarget := opsvc.NewPGStore(pgPool)

	summaries := []bucketSummary{
		migrateUsers(ctx, broker.JS, authTarget),
		migrateBucket(ctx, broker.JS, "VIA_VEHICLES", func(item *fleetsvc.Vehicle) error {
			return fleetTarget.PutVehicle(ctx, item)
		}),
		migrateBucket(ctx, broker.JS, "VIA_DRIVERS", func(item *fleetsvc.Driver) error {
			return fleetTarget.PutDriver(ctx, item)
		}),
		migrateBucket(ctx, broker.JS, "VIA_EVENTS", func(item *fleetsvc.SpecialEvent) error {
			return fleetTarget.PutEvent(ctx, item)
		}),
		migrateBucket(ctx, broker.JS, "VIA_NOTICES", func(item *fleetsvc.DriverNotice) error {
			return fleetTarget.PutNotice(ctx, item)
		}),
		migrateBucket(ctx, broker.JS, "VIA_NOTIFICATIONS", func(item *notifysvc.Notification) error {
			return notifyTarget.Put(ctx, item)
		}),
		migrateBucket(ctx, broker.JS, "VIA_SUBSCRIPTIONS", func(item *subsvc.Subscription) error {
			return subTarget.Put(ctx, item)
		}),
		migrateBucket(ctx, broker.JS, "VIA_TENANTS", func(item *tenantsvc.Tenant) error {
			return tenantTarget.Put(ctx, item)
		}),
		migrateBucket(ctx, broker.JS, "VIA_DRIVER_REQUESTS", func(item *requestsvc.DriverRequest) error {
			return requestTarget.Put(ctx, item)
		}),
		migrateOperations(ctx, broker.JS, opsTarget),
	}

	totalMigrated := 0
	totalFailed := 0
	for _, summary := range summaries {
		if summary.Skipped {
			log.Printf("[migrate] %-20s skipped (bucket missing)", summary.Name)
			continue
		}
		log.Printf(
			"[migrate] %-20s migrated=%d failed=%d",
			summary.Name,
			summary.Migrated,
			summary.Failed,
		)
		totalMigrated += summary.Migrated
		totalFailed += summary.Failed
	}

	if totalFailed > 0 {
		log.Fatalf("[migrate] completed with failures: migrated=%d failed=%d", totalMigrated, totalFailed)
	}

	log.Printf("[migrate] completed successfully: migrated=%d", totalMigrated)
}

func migrateUsers(
	ctx context.Context,
	js jetstream.JetStream,
	target *authsvc.PGStore,
) bucketSummary {
	return migrateBucket(ctx, js, "VIA_USERS", func(item *authsvc.User) error {
		if _, err := target.GetUser(ctx, item.ID); err == nil {
			return target.UpdateUser(ctx, item)
		}
		if err := target.CreateUser(ctx, item); err != nil {
			return err
		}
		return nil
	})
}

func migrateOperations(
	ctx context.Context,
	js jetstream.JetStream,
	target *opsvc.PGStore,
) bucketSummary {
	return migrateBucket(ctx, js, "VIA_OPERATIONS", func(item *opsvc.Operation) error {
		return target.Put(ctx, item)
	})
}

func migrateBucket[T any](
	ctx context.Context,
	js jetstream.JetStream,
	bucket string,
	store func(*T) error,
) bucketSummary {
	summary := bucketSummary{Name: bucket}

	kv, err := openBucket(ctx, js, bucket)
	if err != nil {
		if errors.Is(err, errBucketMissing) {
			summary.Skipped = true
			return summary
		}
		log.Fatalf("[migrate] open %s: %v", bucket, err)
	}

	keys, err := kv.Keys(ctx)
	if err != nil {
		if isNoKeysError(err) {
			return summary
		}
		log.Fatalf("[migrate] list keys %s: %v", bucket, err)
	}

	for _, key := range keys {
		entry, err := kv.Get(ctx, key)
		if err != nil {
			summary.Failed++
			log.Printf("[migrate] read %s/%s: %v", bucket, key, err)
			continue
		}

		var item T
		if err := json.Unmarshal(entry.Value(), &item); err != nil {
			summary.Failed++
			log.Printf("[migrate] decode %s/%s: %v", bucket, key, err)
			continue
		}

		if err := store(&item); err != nil {
			summary.Failed++
			log.Printf("[migrate] import %s/%s: %v", bucket, key, err)
			continue
		}

		summary.Migrated++
	}

	return summary
}

var errBucketMissing = errors.New("bucket missing")

func openBucket(
	ctx context.Context,
	js jetstream.JetStream,
	bucket string,
) (jetstream.KeyValue, error) {
	kv, err := js.KeyValue(ctx, bucket)
	if err != nil {
		if isMissingBucketError(err) {
			return nil, errBucketMissing
		}
		return nil, err
	}
	return kv, nil
}

func isMissingBucketError(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "bucket") && strings.Contains(message, "not found") ||
		strings.Contains(message, "stream not found")
}

func isNoKeysError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(strings.ToLower(err.Error()), "no keys found")
}
