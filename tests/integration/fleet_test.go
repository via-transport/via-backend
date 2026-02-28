//go:build integration

package integration

import (
	"fmt"
	"testing"
)

// ---------------------------------------------------------------------------
// Vehicle CRUD
// ---------------------------------------------------------------------------

func TestFleet_Vehicle_Create(t *testing.T) {
	c := newClient()

	status, data, err := c.post("/api/v1/vehicles", map[string]interface{}{
		"fleet_id":            fleetID(),
		"registration_number": fmt.Sprintf("TEST-%d", uniqueSuffix()),
		"type":                "van",
		"capacity":            12,
		"make":                "Toyota",
		"model":               "HiAce",
		"year":                2024,
		"color":               "white",
		"status":              "active",
	})
	if err != nil {
		t.Fatalf("create vehicle failed: %v", err)
	}
	assertStatus(t, status, 201)
	assertFieldNotEmpty(t, data, "id")
	assertField(t, data, "fleet_id", fleetID())
	assertField(t, data, "type", "van")
	assertField(t, data, "capacity", 12)
	assertField(t, data, "status", "active")
}

func TestFleet_Vehicle_Create_MissingFleetID(t *testing.T) {
	c := newClient()

	status, data, err := c.post("/api/v1/vehicles", map[string]interface{}{
		"registration_number": "NO-FLEET-001",
		"type":                "bus",
	})
	if err != nil {
		t.Fatalf("create vehicle failed: %v", err)
	}
	if status == 201 {
		t.Fatalf("creating vehicle without fleet_id should fail")
	}
	assertError(t, data, "fleet_id")
}

func TestFleet_Vehicle_GetByID(t *testing.T) {
	c := newClient()

	// Create
	_, created, _ := c.post("/api/v1/vehicles", map[string]interface{}{
		"fleet_id":            fleetID(),
		"registration_number": fmt.Sprintf("GET-%d", uniqueSuffix()),
		"type":                "bus",
		"capacity":            50,
	})
	id := created["id"].(string)

	// Get
	status, data, err := c.get(fmt.Sprintf("/api/v1/vehicles/%s?fleet_id=%s", id, fleetID()))
	if err != nil {
		t.Fatalf("get vehicle failed: %v", err)
	}
	assertStatus(t, status, 200)
	assertField(t, data, "id", id)
	assertField(t, data, "type", "bus")
	assertField(t, data, "capacity", 50)
}

func TestFleet_Vehicle_List(t *testing.T) {
	c := newClient()
	regNum := fmt.Sprintf("LIST-%d", uniqueSuffix())

	// Create a vehicle
	c.post("/api/v1/vehicles", map[string]interface{}{
		"fleet_id":            fleetID(),
		"registration_number": regNum,
		"type":                "bus",
	})

	// List
	status, items, err := c.getList(fmt.Sprintf("/api/v1/vehicles?fleet_id=%s", fleetID()))
	if err != nil {
		t.Fatalf("list vehicles failed: %v", err)
	}
	assertStatus(t, status, 200)

	if len(items) == 0 {
		t.Fatalf("expected at least 1 vehicle, got 0")
	}

	// Find our vehicle
	found := false
	for _, v := range items {
		if v["registration_number"] == regNum {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("created vehicle %s not found in list", regNum)
	}
}

func TestFleet_Vehicle_PartialUpdate_PreservesIsActive(t *testing.T) {
	c := newClient()

	// Create
	_, created, _ := c.post("/api/v1/vehicles", map[string]interface{}{
		"fleet_id":            fleetID(),
		"registration_number": fmt.Sprintf("ACTIVE-%d", uniqueSuffix()),
		"type":                "van",
	})
	id := created["id"].(string)

	// Explicitly set is_active = true
	status, activated, err := c.put(fmt.Sprintf("/api/v1/vehicles/%s", id), map[string]interface{}{
		"fleet_id":  fleetID(),
		"is_active": true,
	})
	if err != nil {
		t.Fatalf("activate vehicle failed: %v", err)
	}
	assertStatus(t, status, 200)
	assertField(t, activated, "is_active", true)

	// Partial update (only color) – is_active must stay true
	status, updated, err := c.put(fmt.Sprintf("/api/v1/vehicles/%s", id), map[string]interface{}{
		"fleet_id": fleetID(),
		"color":    "blue",
	})
	if err != nil {
		t.Fatalf("partial update failed: %v", err)
	}
	assertStatus(t, status, 200)
	assertField(t, updated, "is_active", true) // Must NOT be zeroed!

	// Verify with GET
	status, fetched, err := c.get(fmt.Sprintf("/api/v1/vehicles/%s?fleet_id=%s", id, fleetID()))
	if err != nil {
		t.Fatalf("get vehicle failed: %v", err)
	}
	assertStatus(t, status, 200)
	assertField(t, fetched, "is_active", true)
}

func TestFleet_Vehicle_ExplicitDeactivate(t *testing.T) {
	c := newClient()

	// Create and activate
	_, created, _ := c.post("/api/v1/vehicles", map[string]interface{}{
		"fleet_id":            fleetID(),
		"registration_number": fmt.Sprintf("DEACT-%d", uniqueSuffix()),
		"type":                "van",
	})
	id := created["id"].(string)

	c.put(fmt.Sprintf("/api/v1/vehicles/%s", id), map[string]interface{}{
		"fleet_id":  fleetID(),
		"is_active": true,
	})

	// Explicitly set is_active = false
	status, deactivated, err := c.put(fmt.Sprintf("/api/v1/vehicles/%s", id), map[string]interface{}{
		"fleet_id":  fleetID(),
		"is_active": false,
	})
	if err != nil {
		t.Fatalf("deactivate vehicle failed: %v", err)
	}
	assertStatus(t, status, 200)
	assertField(t, deactivated, "is_active", false)
}

func TestFleet_Vehicle_Delete(t *testing.T) {
	c := newClient()

	// Create
	_, created, _ := c.post("/api/v1/vehicles", map[string]interface{}{
		"fleet_id":            fleetID(),
		"registration_number": fmt.Sprintf("DEL-%d", uniqueSuffix()),
		"type":                "van",
	})
	id := created["id"].(string)

	// Delete
	status, _, err := c.delete(fmt.Sprintf("/api/v1/vehicles/%s?fleet_id=%s", id, fleetID()))
	if err != nil {
		t.Fatalf("delete vehicle failed: %v", err)
	}
	assertStatus(t, status, 200)

	// Verify gone
	status, _, _ = c.get(fmt.Sprintf("/api/v1/vehicles/%s?fleet_id=%s", id, fleetID()))
	if status == 200 {
		t.Fatalf("deleted vehicle should not be retrievable")
	}
}

func TestFleet_Vehicle_UpdateLocation(t *testing.T) {
	c := newClient()

	// Create
	_, created, _ := c.post("/api/v1/vehicles", map[string]interface{}{
		"fleet_id":            fleetID(),
		"registration_number": fmt.Sprintf("LOC-%d", uniqueSuffix()),
		"type":                "bus",
	})
	id := created["id"].(string)

	// Update location
	status, _, err := c.put(fmt.Sprintf("/api/v1/vehicles/%s/location", id), map[string]interface{}{
		"fleet_id": fleetID(),
		"location": map[string]interface{}{
			"latitude":  6.9271,
			"longitude": 79.8612,
			"speed":     45.5,
			"heading":   180.0,
		},
	})
	if err != nil {
		t.Fatalf("update location failed: %v", err)
	}
	assertStatus(t, status, 200)

	// Verify location is set
	status, data, err := c.get(fmt.Sprintf("/api/v1/vehicles/%s?fleet_id=%s", id, fleetID()))
	if err != nil {
		t.Fatalf("get vehicle failed: %v", err)
	}
	assertStatus(t, status, 200)

	loc, ok := data["current_location"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected current_location object, got: %v", data["current_location"])
	}
	if lat, _ := loc["latitude"].(float64); lat < 6.92 || lat > 6.93 {
		t.Fatalf("expected latitude ~6.9271, got %f", lat)
	}
}

// ---------------------------------------------------------------------------
// Driver CRUD
// ---------------------------------------------------------------------------

func TestFleet_Driver_Create(t *testing.T) {
	c := newClient()

	status, data, err := c.post("/api/v1/drivers", map[string]interface{}{
		"fleet_id":  fleetID(),
		"full_name": "Test Driver",
		"email":     uniqueEmail("drv"),
		"phone":     "+94771112233",
	})
	if err != nil {
		t.Fatalf("create driver failed: %v", err)
	}
	assertStatus(t, status, 201)
	assertFieldNotEmpty(t, data, "id")
	assertField(t, data, "full_name", "Test Driver")
	assertField(t, data, "fleet_id", fleetID())
}

func TestFleet_Driver_PartialUpdate_PreservesIsActive(t *testing.T) {
	c := newClient()

	// Create
	_, created, _ := c.post("/api/v1/drivers", map[string]interface{}{
		"fleet_id":  fleetID(),
		"full_name": "Active Driver",
		"email":     uniqueEmail("drv_active"),
	})
	id := created["id"].(string)

	// Explicitly activate
	c.put(fmt.Sprintf("/api/v1/drivers/%s", id), map[string]interface{}{
		"fleet_id":  fleetID(),
		"is_active": true,
	})

	// Partial update name only
	status, updated, err := c.put(fmt.Sprintf("/api/v1/drivers/%s", id), map[string]interface{}{
		"fleet_id":  fleetID(),
		"full_name": "Renamed Driver",
	})
	if err != nil {
		t.Fatalf("partial update failed: %v", err)
	}
	assertStatus(t, status, 200)
	assertField(t, updated, "full_name", "Renamed Driver")
	assertField(t, updated, "is_active", true) // Must NOT be zeroed!
}

func TestFleet_Driver_Delete(t *testing.T) {
	c := newClient()

	// Create
	_, created, _ := c.post("/api/v1/drivers", map[string]interface{}{
		"fleet_id":  fleetID(),
		"full_name": "Delete Me",
		"email":     uniqueEmail("drv_del"),
	})
	id := created["id"].(string)

	// Delete
	status, _, err := c.delete(fmt.Sprintf("/api/v1/drivers/%s?fleet_id=%s", id, fleetID()))
	if err != nil {
		t.Fatalf("delete driver failed: %v", err)
	}
	assertStatus(t, status, 200)
}

// ---------------------------------------------------------------------------
// Events
// ---------------------------------------------------------------------------

func TestFleet_Event_CreateAndList(t *testing.T) {
	c := newClient()

	// Create a vehicle for the event
	_, vehicle, _ := c.post("/api/v1/vehicles", map[string]interface{}{
		"fleet_id":            fleetID(),
		"registration_number": fmt.Sprintf("EVT-%d", uniqueSuffix()),
		"type":                "bus",
	})
	vehicleID := vehicle["id"].(string)

	// Create event
	status, event, err := c.post("/api/v1/events", map[string]interface{}{
		"fleet_id":   fleetID(),
		"vehicle_id": vehicleID,
		"type":       "delay",
		"message":    "Traffic jam on Galle Road",
	})
	if err != nil {
		t.Fatalf("create event failed: %v", err)
	}
	assertStatus(t, status, 201)
	assertFieldNotEmpty(t, event, "id")
	assertField(t, event, "type", "delay")
	assertField(t, event, "vehicle_id", vehicleID)

	// List events
	status, items, err := c.getList(fmt.Sprintf("/api/v1/events?fleet_id=%s&vehicle_id=%s", fleetID(), vehicleID))
	if err != nil {
		t.Fatalf("list events failed: %v", err)
	}
	assertStatus(t, status, 200)
	if len(items) == 0 {
		t.Fatalf("expected at least 1 event")
	}
}

// ---------------------------------------------------------------------------
// Notices
// ---------------------------------------------------------------------------

func TestFleet_Notice_CreateAndList(t *testing.T) {
	c := newClient()

	// Create notice
	status, notice, err := c.post("/api/v1/notices", map[string]interface{}{
		"fleet_id": fleetID(),
		"title":    "Road closure ahead",
		"message":  "Colombo Fort area closed until 5pm",
		"priority": "high",
	})
	if err != nil {
		t.Fatalf("create notice failed: %v", err)
	}
	assertStatus(t, status, 201)
	assertFieldNotEmpty(t, notice, "id")
	assertField(t, notice, "title", "Road closure ahead")

	// List notices
	status, items, err := c.getList(fmt.Sprintf("/api/v1/notices?fleet_id=%s", fleetID()))
	if err != nil {
		t.Fatalf("list notices failed: %v", err)
	}
	assertStatus(t, status, 200)
	if len(items) == 0 {
		t.Fatalf("expected at least 1 notice")
	}
}

