package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"via-backend/internal/messaging"
	"via-backend/internal/model"
)

type eventSnapshotStore interface {
	Put(ctx context.Context, key string, value []byte) (uint64, error)
}

// EventService handles trip and operational event publishing.
type EventService struct {
	broker   *messaging.Broker
	snapshot eventSnapshotStore
}

// NewEventService creates an EventService.
func NewEventService(
	broker *messaging.Broker,
	snapshot eventSnapshotStore,
) *EventService {
	return &EventService{
		broker:   broker,
		snapshot: snapshot,
	}
}

// EventResult is the return value of Publish.
type EventResult struct {
	Subject string
}

// Publish validates, normalises, and publishes a realtime event.
func (s *EventService) Publish(p model.RealtimeEvent) (EventResult, error) {
	p.FleetID = strings.TrimSpace(p.FleetID)
	p.VehicleID = strings.TrimSpace(p.VehicleID)
	p.DriverID = strings.TrimSpace(p.DriverID)
	p.RouteID = strings.TrimSpace(p.RouteID)
	p.Topic = strings.ToLower(strings.TrimSpace(p.Topic))
	p.Message = strings.TrimSpace(p.Message)
	p.Event = strings.TrimSpace(p.Event)

	if p.FleetID == "" {
		return EventResult{}, fmt.Errorf("fleet_id is required")
	}

	p.Topic = messaging.NormalizeTopic(p.Topic, p.Event)

	if p.VehicleID == "" && p.Topic != model.TopicOps {
		return EventResult{}, fmt.Errorf("vehicle_id is required")
	}
	if p.Event == "" {
		return EventResult{}, fmt.Errorf("event is required")
	}
	if p.Timestamp.IsZero() {
		p.Timestamp = time.Now().UTC()
	}

	subject := messaging.EventSubject(p.Topic, p.FleetID, p.VehicleID, p.Event)
	body, err := json.Marshal(p)
	if err != nil {
		return EventResult{}, fmt.Errorf("marshal event: %w", err)
	}
	if err := s.broker.Publish(subject, body); err != nil {
		return EventResult{}, fmt.Errorf("publish event: %w", err)
	}
	s.storeLastPublishedEvent(body, p)

	return EventResult{Subject: subject}, nil
}

// PublishTripStart is a convenience wrapper that defaults the event to
// "trip_started" and topic to "trip".
func (s *EventService) PublishTripStart(p model.RealtimeEvent) (EventResult, error) {
	p.Topic = model.TopicTrip
	if strings.TrimSpace(p.Event) == "" {
		p.Event = "trip_started"
	}
	return s.Publish(p)
}

func (s *EventService) storeLastPublishedEvent(body []byte, p model.RealtimeEvent) {
	if s.snapshot == nil {
		return
	}

	key := strings.TrimSpace(p.FleetID)
	vehicleID := strings.TrimSpace(p.VehicleID)
	if key == "" {
		return
	}
	if vehicleID != "" {
		key += "_" + vehicleID
	} else {
		key += "_fleet"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := s.snapshot.Put(ctx, key, body); err != nil {
		log.Printf("[events] event snapshot warning: %v", err)
	}
}
