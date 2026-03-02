// Package server wires the HTTP router, middleware stack, and graceful
// shutdown. It does not know about specific business logic – it receives
// ready-made handlers.
package server

import (
	"context"
	"errors"
	"log"
	"net/http"
	"time"

	"via-backend/internal/appcache"
	"via-backend/internal/auth"
	"via-backend/internal/authsvc"
	"via-backend/internal/cache"
	"via-backend/internal/config"
	"via-backend/internal/fleetsvc"
	"via-backend/internal/handler"
	"via-backend/internal/messaging"
	"via-backend/internal/middleware"
	"via-backend/internal/notifysvc"
	"via-backend/internal/requestsvc"
	viasentry "via-backend/internal/sentry"
	"via-backend/internal/service"
	"via-backend/internal/subsvc"
	"via-backend/internal/tenantsvc"
	"via-backend/internal/tracing"
)

// Server is the top-level HTTP server with all dependencies wired.
type Server struct {
	httpServer *http.Server
}

// New builds and returns a ready-to-start Server.
func New(
	cfg config.Config,
	gpsSvc *service.GPSService,
	eventSvc *service.EventService,
	broker *messaging.Broker,
	gpsCache *cache.GPSCache,
	appCache *appcache.Cache,
	authCfg auth.MiddlewareConfig,
	authHandler *authsvc.Handler,
	tenantHandler *tenantsvc.Handler,
	tenantPolicy *tenantsvc.Policy,
	fleetHandler *fleetsvc.Handler,
	notifyHandler *notifysvc.Handler,
	requestHandler *requestsvc.Handler,
	subHandler *subsvc.Handler,
) *Server {
	mux := http.NewServeMux()

	// --- Public / health routes (no auth required) ---
	mux.HandleFunc("/healthz", handler.Health())
	mux.HandleFunc("/debug/cache/stats", appcache.StatsHandler(appCache))

	// --- Legacy API routes (GPS, trip, events, websocket) ---
	mux.HandleFunc("/v1/gps/update", handler.GPSIngest(gpsSvc, tenantPolicy))
	mux.HandleFunc("/v1/trip/start", handler.TripStart(eventSvc, tenantPolicy))
	mux.HandleFunc("/v1/events/publish", handler.EventPublish(eventSvc, tenantPolicy))
	mux.HandleFunc("/ws", handler.WSFanout(broker, gpsCache, cfg, tenantPolicy))
	mux.HandleFunc("GET /api/v1/vehicles/{id}/stream", handler.WSFanout(broker, gpsCache, cfg, tenantPolicy))

	// --- Microservice routes ---
	authHandler.Mount(mux)
	tenantHandler.Mount(mux)
	fleetHandler.Mount(mux)
	notifyHandler.Mount(mux)
	requestHandler.Mount(mux)
	subHandler.Mount(mux)

	// Middleware stack (outermost → innermost):
	//   Recovery → Sentry → Tracing → Gzip → CORS → Logging → Auth → router
	stack := middleware.Chain(mux,
		middleware.Recovery,
		viasentry.HTTPMiddleware(),
		tracing.HTTPMiddleware(cfg.NATSName),
		middleware.Gzip,
		middleware.CORS,
		middleware.Logging,
		auth.Middleware(authCfg),
	)

	return &Server{
		httpServer: &http.Server{
			Addr:              cfg.ListenAddr,
			Handler:           stack,
			ReadHeaderTimeout: cfg.ReadHeaderTimeout,
			ReadTimeout:       30 * time.Second,
			WriteTimeout:      60 * time.Second,
			IdleTimeout:       120 * time.Second,
			MaxHeaderBytes:    1 << 16, // 64 KB
		},
	}
}

// Start begins listening. It blocks until the server is shut down.
func (s *Server) Start() error {
	log.Printf("[server] listening on %s", s.httpServer.Addr)
	err := s.httpServer.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// Shutdown gracefully drains connections within the given timeout.
func (s *Server) Shutdown(timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := s.httpServer.Shutdown(ctx); err != nil {
		log.Printf("[server] shutdown warning: %v", err)
	}
}
