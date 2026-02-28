package messaging

import (
	"fmt"
	"strings"

	"via-backend/internal/model"
)

// ---------------------------------------------------------------------------
// GPS subjects
// ---------------------------------------------------------------------------

// GPSRawSubject returns the durable-stream subject for a single vehicle.
func GPSRawSubject(fleetID, vehicleID string) string {
	return fmt.Sprintf("fleet.%s.vehicle.%s.gps.raw", fleetID, vehicleID)
}

// GPSLiveSubject returns the fanout subject for filtered live points.
func GPSLiveSubject(fleetID, vehicleID string) string {
	return fmt.Sprintf("fleet.%s.vehicle.%s.gps.live", fleetID, vehicleID)
}

// GPSLiveWildcard returns the wildcard subscription for live GPS.
func GPSLiveWildcard(fleetID, vehicleID string) string {
	if vehicleID == "" {
		return fmt.Sprintf("fleet.%s.vehicle.*.gps.live", fleetID)
	}
	return GPSLiveSubject(fleetID, vehicleID)
}

// ---------------------------------------------------------------------------
// Trip subjects
// ---------------------------------------------------------------------------

func TripSubject(fleetID, vehicleID, event string) string {
	return fmt.Sprintf("fleet.%s.vehicle.%s.trip.%s", fleetID, vehicleID, event)
}

func TripWildcard(fleetID, vehicleID string) string {
	if vehicleID == "" {
		return fmt.Sprintf("fleet.%s.vehicle.*.trip.*", fleetID)
	}
	return fmt.Sprintf("fleet.%s.vehicle.%s.trip.*", fleetID, vehicleID)
}

// ---------------------------------------------------------------------------
// Ops subjects
// ---------------------------------------------------------------------------

func OpsSubject(fleetID, vehicleID, event string) string {
	if vehicleID == "" {
		return fmt.Sprintf("fleet.%s.ops.%s", fleetID, event)
	}
	return fmt.Sprintf("fleet.%s.vehicle.%s.ops.%s", fleetID, vehicleID, event)
}

func OpsWildcards(fleetID, vehicleID string) []string {
	if vehicleID == "" {
		return []string{
			fmt.Sprintf("fleet.%s.vehicle.*.ops.*", fleetID),
			fmt.Sprintf("fleet.%s.ops.*", fleetID),
		}
	}
	return []string{
		fmt.Sprintf("fleet.%s.vehicle.%s.ops.*", fleetID, vehicleID),
	}
}

// ---------------------------------------------------------------------------
// Composite helpers
// ---------------------------------------------------------------------------

// EventSubject resolves a concrete NATS subject for a realtime event.
func EventSubject(topic, fleetID, vehicleID, event string) string {
	switch topic {
	case model.TopicTrip:
		return TripSubject(fleetID, vehicleID, event)
	case model.TopicOps:
		return OpsSubject(fleetID, vehicleID, event)
	default:
		return TripSubject(fleetID, vehicleID, event)
	}
}

// SubjectsForTopic returns the list of NATS subjects a WebSocket client
// should subscribe to for a given topic.
func SubjectsForTopic(topic, fleetID, vehicleID string) ([]string, error) {
	switch topic {
	case model.TopicGPS:
		return []string{GPSLiveWildcard(fleetID, vehicleID)}, nil
	case model.TopicTrip:
		return []string{TripWildcard(fleetID, vehicleID)}, nil
	case model.TopicOps:
		return OpsWildcards(fleetID, vehicleID), nil
	case model.TopicEvents:
		s := []string{TripWildcard(fleetID, vehicleID)}
		s = append(s, OpsWildcards(fleetID, vehicleID)...)
		return s, nil
	default:
		return nil, fmt.Errorf("unsupported topic: %s", topic)
	}
}

// NormalizeTopic maps a free-form topic+event pair to one of the canonical
// topic constants.
func NormalizeTopic(requestedTopic, event string) string {
	switch requestedTopic {
	case model.TopicTrip, model.TopicOps:
		return requestedTopic
	}
	switch strings.ToLower(strings.TrimSpace(event)) {
	case "trip_started", "trip_completed":
		return model.TopicTrip
	default:
		return model.TopicOps
	}
}
