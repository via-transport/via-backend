# VIA Backend (Go + NATS)

This service is a lightweight realtime gateway (GPS + fleet events) for fleet tracking.

## What it does
- Receives GPS updates over HTTP (`POST /v1/gps/update`)
- Receives trip events over HTTP (`POST /v1/trip/start`)
- Receives generic realtime events over HTTP (`POST /v1/events/publish`)
- Publishes each update to NATS subject:
  - `fleet.<fleet_id>.vehicle.<vehicle_id>.gps`
  - `fleet.<fleet_id>.vehicle.<vehicle_id>.trip.<event>`
  - `fleet.<fleet_id>.vehicle.<vehicle_id>.ops.<event>`
  - `fleet.<fleet_id>.ops.<event>` for fleet-wide operational events
- Streams updates to owner/driver/passenger clients over WebSocket (`GET /ws`)
  - Fleet-wide: subscribe with `fleet_id`
  - Single vehicle: subscribe with `fleet_id` + `vehicle_id`
  - Topic selection: `topic=gps` (default), `topic=trip`, `topic=ops`, or `topic=events`

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
