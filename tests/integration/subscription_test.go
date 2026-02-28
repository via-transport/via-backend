//go:build integration

package integration

import (
	"fmt"
	"testing"
)

// ---------------------------------------------------------------------------
// Subscription: Create
// ---------------------------------------------------------------------------

func TestSub_Create(t *testing.T) {
	c := newClient()

	// Create a vehicle to subscribe to
	_, vehicle, _ := c.post("/api/v1/vehicles", map[string]interface{}{
		"fleet_id":            fleetID(),
		"registration_number": fmt.Sprintf("SUB-%d", uniqueSuffix()),
		"type":                "bus",
	})
	vehicleID := vehicle["id"].(string)

	// Register a passenger
	_, regData, _ := c.post("/api/v1/auth/register", map[string]interface{}{
		"email":    uniqueEmail("sub_user"),
		"password": "SubPass1!",
		"role":     "passenger",
	})
	userID := regData["user"].(map[string]interface{})["id"].(string)

	// Create subscription
	status, sub, err := c.post("/api/v1/subscriptions", map[string]interface{}{
		"user_id":    userID,
		"vehicle_id": vehicleID,
		"fleet_id":   fleetID(),
		"preferences": map[string]interface{}{
			"notify_on_arrival": true,
			"notify_on_delay":   true,
			"notify_on_event":   false,
		},
	})
	if err != nil {
		t.Fatalf("create subscription failed: %v", err)
	}
	assertStatus(t, status, 201)
	assertFieldNotEmpty(t, sub, "id")
	assertField(t, sub, "user_id", userID)
	assertField(t, sub, "vehicle_id", vehicleID)
	assertField(t, sub, "status", "active")

	// Verify preferences are nested correctly
	prefs, ok := sub["preferences"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected 'preferences' object, got: %v", sub["preferences"])
	}
	if prefs["notify_on_arrival"] != true {
		t.Fatalf("expected notify_on_arrival=true, got %v", prefs["notify_on_arrival"])
	}
	if prefs["notify_on_event"] != false {
		t.Fatalf("expected notify_on_event=false, got %v", prefs["notify_on_event"])
	}
}

// ---------------------------------------------------------------------------
// Subscription: Get
// ---------------------------------------------------------------------------

func TestSub_Get(t *testing.T) {
	c := newClient()
	sub := createTestSubscription(t, c)
	userID := sub["user_id"].(string)

	status, data, err := c.get(fmt.Sprintf("/api/v1/subscriptions/%s?user_id=%s", sub["id"].(string), userID))
	if err != nil {
		t.Fatalf("get subscription failed: %v", err)
	}
	assertStatus(t, status, 200)
	assertField(t, data, "id", sub["id"].(string))
	assertField(t, data, "status", "active")
}

// ---------------------------------------------------------------------------
// Subscription: List
// ---------------------------------------------------------------------------

func TestSub_List(t *testing.T) {
	c := newClient()
	sub := createTestSubscription(t, c)
	userID := sub["user_id"].(string)

	status, items, err := c.getList(fmt.Sprintf("/api/v1/subscriptions?user_id=%s", userID))
	if err != nil {
		t.Fatalf("list subscriptions failed: %v", err)
	}
	assertStatus(t, status, 200)
	if len(items) == 0 {
		t.Fatalf("expected at least 1 subscription")
	}
}

// ---------------------------------------------------------------------------
// Subscription: Update (preferences)
// ---------------------------------------------------------------------------

func TestSub_Update_Preferences(t *testing.T) {
	c := newClient()
	sub := createTestSubscription(t, c)
	subID := sub["id"].(string)
	userID := sub["user_id"].(string)

	// Update preferences only
	status, updated, err := c.put(fmt.Sprintf("/api/v1/subscriptions/%s?user_id=%s", subID, userID), map[string]interface{}{
		"preferences": map[string]interface{}{
			"notify_on_arrival": false,
			"notify_on_delay":   false,
			"notify_on_event":   true,
		},
	})
	if err != nil {
		t.Fatalf("update subscription failed: %v", err)
	}
	assertStatus(t, status, 200)

	prefs, ok := updated["preferences"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected preferences object, got: %v", updated["preferences"])
	}
	if prefs["notify_on_arrival"] != false {
		t.Fatalf("expected notify_on_arrival=false after update")
	}
	if prefs["notify_on_event"] != true {
		t.Fatalf("expected notify_on_event=true after update")
	}
}

func TestSub_Update_PreservesPreferences(t *testing.T) {
	c := newClient()
	sub := createTestSubscription(t, c)
	subID := sub["id"].(string)
	userID := sub["user_id"].(string)

	// Update status only (no preferences) – preferences should be preserved
	status, updated, err := c.put(fmt.Sprintf("/api/v1/subscriptions/%s?user_id=%s", subID, userID), map[string]interface{}{
		"status": "paused",
	})
	if err != nil {
		t.Fatalf("update subscription failed: %v", err)
	}
	assertStatus(t, status, 200)
	assertField(t, updated, "status", "paused")

	// Preferences should still be there
	prefs, ok := updated["preferences"].(map[string]interface{})
	if !ok {
		t.Fatalf("preferences should be preserved on partial update, got: %v", updated["preferences"])
	}
	if prefs["notify_on_arrival"] != true {
		t.Fatalf("preferences zeroed on partial update: notify_on_arrival=%v", prefs["notify_on_arrival"])
	}
}

// ---------------------------------------------------------------------------
// Subscription: Cancel (soft-delete)
// ---------------------------------------------------------------------------

func TestSub_Cancel_SoftDelete(t *testing.T) {
	c := newClient()
	sub := createTestSubscription(t, c)
	subID := sub["id"].(string)
	userID := sub["user_id"].(string)

	// Cancel (DELETE)
	status, cancelled, err := c.delete(fmt.Sprintf("/api/v1/subscriptions/%s?user_id=%s", subID, userID))
	if err != nil {
		t.Fatalf("cancel subscription failed: %v", err)
	}
	assertStatus(t, status, 200)
	assertField(t, cancelled, "status", "cancelled")

	// Should still be retrievable (soft-deleted, not purged)
	status, fetched, err := c.get(fmt.Sprintf("/api/v1/subscriptions/%s?user_id=%s", subID, userID))
	if err != nil {
		t.Fatalf("get cancelled subscription failed: %v", err)
	}
	assertStatus(t, status, 200)
	assertField(t, fetched, "status", "cancelled")
}

// ---------------------------------------------------------------------------
// Subscription: List for Vehicle
// ---------------------------------------------------------------------------

func TestSub_ListForVehicle(t *testing.T) {
	c := newClient()
	sub := createTestSubscription(t, c)
	vehicleID := sub["vehicle_id"].(string)

	status, items, err := c.getList(fmt.Sprintf("/api/v1/subscriptions/vehicle/%s", vehicleID))
	if err != nil {
		t.Fatalf("list subs for vehicle failed: %v", err)
	}
	assertStatus(t, status, 200)
	if len(items) == 0 {
		t.Fatalf("expected at least 1 subscription for vehicle %s", vehicleID)
	}
}

// ---------------------------------------------------------------------------
// Helper: create a test subscription
// ---------------------------------------------------------------------------

func createTestSubscription(t *testing.T, c *apiClient) map[string]interface{} {
	t.Helper()

	// Vehicle
	_, vehicle, _ := c.post("/api/v1/vehicles", map[string]interface{}{
		"fleet_id":            fleetID(),
		"registration_number": fmt.Sprintf("TSUB-%d", uniqueSuffix()),
		"type":                "van",
	})
	vehicleID := vehicle["id"].(string)

	// User
	_, regData, _ := c.post("/api/v1/auth/register", map[string]interface{}{
		"email":    uniqueEmail("tsub"),
		"password": "TestSub1!",
		"role":     "passenger",
	})
	userID := regData["user"].(map[string]interface{})["id"].(string)

	// Subscription
	status, sub, err := c.post("/api/v1/subscriptions", map[string]interface{}{
		"user_id":    userID,
		"vehicle_id": vehicleID,
		"fleet_id":   fleetID(),
		"preferences": map[string]interface{}{
			"notify_on_arrival": true,
			"notify_on_delay":   true,
			"notify_on_event":   false,
		},
	})
	if err != nil {
		t.Fatalf("create test subscription failed: %v", err)
	}
	if status != 201 {
		t.Fatalf("create test subscription got status %d: %v", status, sub)
	}
	return sub
}
