# VIA Backend (Go + NATS)

This service is the fleet control plane and realtime gateway for tenant-scoped fleet tracking.

## What it does
- Receives GPS updates over HTTP (`POST /v1/gps/update`)
- Receives trip events over HTTP (`POST /v1/trip/start`)
- Receives generic realtime events over HTTP (`POST /v1/events/publish`)
- Exposes tenant plan and service-status endpoints (`/api/v1/billing/*`, `/api/v1/service-status`)
- Exposes passenger join-request approval flow (`/api/v1/join-requests`)
- Publishes each update to NATS subject:
  - `fleet.<fleet_id>.vehicle.<vehicle_id>.gps`
  - `fleet.<fleet_id>.vehicle.<vehicle_id>.trip.<event>`
  - `fleet.<fleet_id>.vehicle.<vehicle_id>.ops.<event>`
  - `fleet.<fleet_id>.ops.<event>` for fleet-wide operational events
- Streams updates to owner/driver/passenger clients over WebSocket (`GET /ws`)
  - Fleet-wide: subscribe with `fleet_id`
  - Single vehicle: subscribe with `fleet_id` + `vehicle_id`
  - Topic selection: `topic=gps` (default), `topic=trip`, `topic=ops`, or `topic=events`
- Also exposes a spec-aligned vehicle stream route:
  - `GET /api/v1/vehicles/{id}/stream?fleet_id=<tenant>`
- Enforces backend-side quotas:
  - location publish minimum interval (default `3s`)
  - event publish hourly cap (default `30/hour`)
  - trial tenant limits for vehicles, drivers, and active passenger approvals
- Enforces tenant lifecycle:
  - `TRIAL -> GRACE -> SUSPENDED`
  - suspended tenants are blocked from realtime publish/stream paths

## Run
1. Start Firebase emulators (for local app sync without internet access):
```bash
cd via-backend
firebase emulators:start --only firestore,auth
```

2. Start NATS:
```bash
docker run -d --name via-nats -p 4222:4222 -p 8222:8222 nats:2.10 -js
```

3. Start backend:
```bash
cd via-backend
go mod tidy
go run ./cmd/server
```

## Environment
- `LISTEN_ADDR` default `:9090`
- `NATS_URL` default `nats://127.0.0.1:4222`

## API examples

Publish GPS update:
```bash
curl -X POST http://localhost:9090/v1/gps/update \
  -H "Content-Type: application/json" \
  -d '{
    "fleet_id":"school-west",
    "vehicle_id":"veh_001",
    "driver_id":"drv_001",
    "route_id":"route_10",
    "latitude":6.9271,
    "longitude":79.8612,
    "speed_kph":42.4,
    "heading":188.0
  }'
```

WebSocket subscribe:
```text
ws://localhost:9090/ws?fleet_id=school-west
ws://localhost:9090/ws?fleet_id=school-west&vehicle_id=veh_001
ws://localhost:9090/ws?fleet_id=school-west&topic=trip
ws://localhost:9090/ws?fleet_id=school-west&vehicle_id=veh_001&topic=trip
ws://localhost:9090/ws?fleet_id=school-west&topic=ops
ws://localhost:9090/ws?fleet_id=school-west&topic=events
ws://localhost:9090/api/v1/vehicles/veh_001/stream?fleet_id=school-west
```

Publish trip-start event:
```bash
curl -X POST http://localhost:9090/v1/trip/start \
  -H "Content-Type: application/json" \
  -d '{
    "fleet_id":"school-west",
    "vehicle_id":"veh_001",
    "driver_id":"drv_001",
    "route_id":"route_10",
    "event":"trip_started",
    "message":"Driver started trip"
  }'
```

Publish operational event:
```bash
curl -X POST http://localhost:9090/v1/events/publish \
  -H "Content-Type: application/json" \
  -d '{
    "fleet_id":"school-west",
    "vehicle_id":"veh_001",
    "driver_id":"drv_001",
    "topic":"ops",
    "event":"driver_assigned",
    "message":"Vehicle assigned to Nimal"
  }'
```

Create a tenant trial:
```bash
curl -X POST http://localhost:9090/api/v1/billing/start-trial \
  -H "Content-Type: application/json" \
  -d '{
    "fleet_id":"school-west",
    "name":"School West Transport"
  }'
```

Read billing plan / service state:
```bash
curl "http://localhost:9090/api/v1/billing/plan?fleet_id=school-west"
curl "http://localhost:9090/api/v1/service-status?fleet_id=school-west"
```

Passenger join request:
```bash
curl -X POST http://localhost:9090/api/v1/join-requests \
  -H "Content-Type: application/json" \
  -d '{
    "user_id":"psg_001",
    "fleet_id":"school-west",
    "vehicle_id":"veh_001"
  }'
```

Owner approval:
```bash
curl "http://localhost:9090/api/v1/join-requests?fleet_id=school-west&status=pending"
curl -X POST http://localhost:9090/api/v1/join-requests/<join_request_id>/approve
```
