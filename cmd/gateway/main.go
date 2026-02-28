// Command gateway is the entrypoint for the Via realtime gateway.
// It wires configuration, messaging, services, and the HTTP server
// with clean dependency injection – no global state, no init().
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"via-backend/internal/appcache"
	"via-backend/internal/auth"
	"via-backend/internal/authsvc"
	"via-backend/internal/cache"
	"via-backend/internal/config"
	"via-backend/internal/database"
	"via-backend/internal/fleetsvc"
	"via-backend/internal/messaging"
	"via-backend/internal/notifysvc"
	viasentry "via-backend/internal/sentry"
	"via-backend/internal/server"
	"via-backend/internal/service"
	"via-backend/internal/subsvc"
	"via-backend/internal/tracing"
)

func main() {
	// 1. Configuration (immutable, from env)
	cfg := config.Load()

	// 2. Distributed tracing (OpenTelemetry)
	tracingProvider, err := tracing.Init(tracing.Config{
		Enabled:     cfg.TracingEnabled,
		ServiceName: cfg.NATSName,
		Environment: cfg.Environment,
		Endpoint:    cfg.TracingEndpoint,
		Insecure:    cfg.TracingInsecure,
		SampleRate:  cfg.TracingSampleRate,
	})
	if err != nil {
		log.Fatalf("[main] tracing: %v", err)
	}
	defer tracingProvider.Shutdown(context.Background())

	// 3. Sentry (error tracking & performance)
	if err := viasentry.Init(viasentry.Config{
		Enabled:          cfg.SentryEnabled,
		DSN:              cfg.SentryDSN,
		Environment:      cfg.Environment,
		Release:          cfg.Release,
		SampleRate:       1.0,
		TracesSampleRate: cfg.SentryTracesSampleRate,
		Debug:            cfg.SentryDebug,
	}); err != nil {
		log.Fatalf("[main] sentry: %v", err)
	}
	defer viasentry.Flush(2 * time.Second)

	// 4. Messaging (NATS + JetStream)
	broker, err := messaging.NewBroker(cfg)
	if err != nil {
		log.Fatalf("[main] %v", err)
	}
	defer broker.Close()

	kv, err := broker.ProvisionStreams(cfg)
	if err != nil {
		log.Fatalf("[main] %v", err)
	}

	// 4b. Build service stores (NATS KV or PostgreSQL).
	// NOTE: caches are created before stores since NATS branch needs appCache.

	// 5. Caches
	gpsCache := cache.New(cfg.GPSBootstrapMaxAge)

	appCache := appcache.New(
		appcache.WithMaxItems(cfg.CacheMaxItems),
		appcache.WithDefaultTTL(cfg.CacheDefaultTTL),
	)
	stopCleanup := appCache.StartCleanup(cfg.CacheCleanupInterval)
	defer stopCleanup()

	var authStore authsvc.UserStore
	var fleetStore fleetsvc.FleetStore
	var notifyStore notifysvc.NotifStore
	var subStore subsvc.SubStore

	if cfg.StoreBackend == "nats" {
		log.Println("[main] store backend: NATS KV")
		usersKV, err := broker.ProvisionKV("VIA_USERS", "User accounts")
		if err != nil {
			log.Fatalf("[main] %v", err)
		}
		emailsKV, err := broker.ProvisionKV("VIA_USER_EMAILS", "User email index")
		if err != nil {
			log.Fatalf("[main] %v", err)
		}
		vehiclesKV, err := broker.ProvisionKV("VIA_VEHICLES", "Fleet vehicles")
		if err != nil {
			log.Fatalf("[main] %v", err)
		}
		driversKV, err := broker.ProvisionKV("VIA_DRIVERS", "Fleet drivers")
		if err != nil {
			log.Fatalf("[main] %v", err)
		}
		eventsKV, err := broker.ProvisionKV("VIA_EVENTS", "Special events")
		if err != nil {
			log.Fatalf("[main] %v", err)
		}
		noticesKV, err := broker.ProvisionKV("VIA_NOTICES", "Driver notices")
		if err != nil {
			log.Fatalf("[main] %v", err)
		}
		notificationsKV, err := broker.ProvisionKV("VIA_NOTIFICATIONS", "User notifications")
		if err != nil {
			log.Fatalf("[main] %v", err)
		}
		subscriptionsKV, err := broker.ProvisionKV("VIA_SUBSCRIPTIONS", "Vehicle subscriptions")
		if err != nil {
			log.Fatalf("[main] %v", err)
		}
		authStore = authsvc.NewStore(usersKV, emailsKV)
		fleetStore = fleetsvc.NewStore(vehiclesKV, driversKV, eventsKV, noticesKV, appCache)
		notifyStore = notifysvc.NewStore(notificationsKV)
		subStore = subsvc.NewStore(subscriptionsKV)
	} else {
		log.Println("[main] store backend: PostgreSQL")
		pgCfg := database.ConfigFromEnv()
		pgPool, err := database.Connect(context.Background(), pgCfg)
		if err != nil {
			log.Fatalf("[main] postgres: %v", err)
		}
		defer pgPool.Close()
		if err := database.Migrate(context.Background(), pgPool); err != nil {
			log.Fatalf("[main] postgres migration: %v", err)
		}
		authStore = authsvc.NewPGStore(pgPool)
		fleetStore = fleetsvc.NewPGStore(pgPool)
		notifyStore = notifysvc.NewPGStore(pgPool)
		subStore = subsvc.NewPGStore(pgPool)
	}

	// 6. Services
	gpsSvc := service.NewGPSService(broker, gpsCache, kv, cfg)
	eventSvc := service.NewEventService(broker)

	// Seed GPS cache from KV (restart-safe recovery)
	seedCtx, seedCancel := context.WithTimeout(context.Background(), 10*time.Second)
	gpsSvc.SeedCache(seedCtx)
	seedCancel()

	// 6b. Microservice handlers
	jwtCfg := authsvc.JWTConfig{
		Secret:     cfg.JWTSecret,
		AccessTTL:  cfg.JWTAccessTTL,
		RefreshTTL: cfg.JWTRefreshTTL,
		Issuer:     cfg.JWTIssuer,
	}
	authsvc.SetGlobalSecret(cfg.JWTSecret) // for WebSocket auth

	authHandler := authsvc.NewHandler(authStore, jwtCfg)
	authHandler.SetUserIDFunc(func(r *http.Request) string {
		return auth.IdentityFromContext(r.Context()).UserID
	})

	fleetHandler := fleetsvc.NewHandler(fleetStore, broker)

	notifyHandler := notifysvc.NewHandler(notifyStore, broker)
	notifyHandler.SubscribeNATS(broker)        // cross-instance notification delivery
	notifyHandler.SubscribeFleetEvents(broker, subStore) // event → notification pipeline

	subHandler := subsvc.NewHandler(subStore)

	// 7. Auth / RBAC
	authCfg := auth.MiddlewareConfig{
		Enabled:   cfg.AuthEnabled,
		JWTSecret: cfg.JWTSecret,
		APIKeys:   parseAPIKeys(cfg.AuthAPIKeys),
		SkipPaths: []string{"/healthz", "/debug/", "/api/v1/auth/login", "/api/v1/auth/register", "/api/v1/auth/refresh", "/api/v1/auth/forgot-password"},
	}

	// 8. HTTP server
	srv := server.New(cfg, gpsSvc, eventSvc, broker, gpsCache, appCache, authCfg,
		authHandler, fleetHandler, notifyHandler, subHandler)

	go func() {
		if err := srv.Start(); err != nil {
			log.Fatalf("[main] server: %v", err)
		}
	}()

	// 9. Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	sig := <-quit
	log.Printf("[main] received %s, shutting down…", sig)

	srv.Shutdown(10 * time.Second)
	log.Println("[main] goodbye")
}

// parseAPIKeys parses AUTH_API_KEYS env var.
// Format: "key1:role1:fleet1,key2:role2:fleet2"
func parseAPIKeys(raw string) map[string]auth.Identity {
	keys := make(map[string]auth.Identity)
	if raw == "" {
		return keys
	}
	for _, entry := range strings.Split(raw, ",") {
		parts := strings.SplitN(strings.TrimSpace(entry), ":", 3)
		if len(parts) < 2 {
			continue
		}
		id := auth.Identity{
			UserID: "apikey-" + parts[0][:min(8, len(parts[0]))],
			Role:   auth.Role(parts[1]),
		}
		if len(parts) == 3 {
			id.FleetID = parts[2]
		}
		keys[parts[0]] = id
	}
	return keys
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
