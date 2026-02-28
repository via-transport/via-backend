//go:build integration

package integration

import (
	"fmt"
	"testing"
)

// TestJourney_FullPassengerFlow exercises the complete happy-path lifecycle:
//
//	Register passenger → Login → Profile → Create Vehicle →
//	Subscribe → Receive Notification → Mark Read → Partial Update →
//	Cancel Subscription
//
// This mirrors the manual 12-step test performed via curl.
func TestJourney_FullPassengerFlow(t *testing.T) {
	c := newClient()
	email := uniqueEmail("journey")
	password := "Journey2025!"

	// ── Step 1: Register ──────────────────────────────────────────────
	t.Log("Step 1: Register passenger")
	status, reg, err := c.post("/api/v1/auth/register", map[string]interface{}{
		"email":        email,
		"password":     password,
		"role":         "passenger",
		"display_name": "Journey Tester",
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	assertStatus(t, status, 201)
	accessToken := reg["access_token"].(string)
	refreshToken := reg["refresh_token"].(string)
	user := reg["user"].(map[string]interface{})
	userID := user["id"].(string)
	assertField(t, user, "email", email)
	assertField(t, user, "role", "passenger")

	if accessToken == "" || refreshToken == "" {
		t.Fatal("expected non-empty tokens")
	}

	// ── Step 2: Login with same credentials ───────────────────────────
	t.Log("Step 2: Login")
	status, login, err := c.post("/api/v1/auth/login", map[string]interface{}{
		"email":    email,
		"password": password,
	})
	if err != nil {
		t.Fatalf("login: %v", err)
	}
	assertStatus(t, status, 200)
	accessToken = login["access_token"].(string)
	authed := c.withToken(accessToken)

	// ── Step 3: Get profile ───────────────────────────────────────────
	t.Log("Step 3: Profile")
	status, profile, err := authed.get("/api/v1/auth/profile")
	if err != nil {
		t.Fatalf("profile: %v", err)
	}
	assertStatus(t, status, 200)
	assertField(t, profile, "email", email)
	assertField(t, profile, "display_name", "Journey Tester")

	// ── Step 4: Create vehicle ────────────────────────────────────────
	t.Log("Step 4: Create vehicle")
	regNum := fmt.Sprintf("JRN-%d", uniqueSuffix())
	status, vehicle, err := c.post("/api/v1/vehicles", map[string]interface{}{
		"fleet_id":            fleetID(),
		"registration_number": regNum,
		"type":                "van",
		"capacity":            14,
		"color":               "blue",
		"is_active":           true,
	})
	if err != nil {
		t.Fatalf("create vehicle: %v", err)
	}
	assertStatus(t, status, 201)
	vehicleID := vehicle["id"].(string)
	assertField(t, vehicle, "is_active", true)

	// ── Step 5: Subscribe to vehicle ──────────────────────────────────
	t.Log("Step 5: Subscribe")
	status, sub, err := c.post("/api/v1/subscriptions", map[string]interface{}{
		"user_id":    userID,
		"vehicle_id": vehicleID,
		"fleet_id":   fleetID(),
		"role":       "parent",
		"preferences": map[string]interface{}{
			"push_enabled":  true,
			"sms_enabled":   false,
			"email_enabled": true,
		},
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	assertStatus(t, status, 201)
	subID := sub["id"].(string)
	assertFieldExists(t, sub, "preferences")

	// Verify preferences nested correctly
	prefs, ok := sub["preferences"].(map[string]interface{})
	if !ok {
		t.Fatal("preferences is not an object")
	}
	if prefs["notify_on_arrival"] != true {
		t.Fatal("notify_on_arrival should be true")
	}

	// ── Step 6: Create notification ───────────────────────────────────
	t.Log("Step 6: Create notification")
	status, notif, err := c.post("/api/v1/notifications", map[string]interface{}{
		"user_id":    userID,
		"vehicle_id": vehicleID,
		"fleet_id":   fleetID(),
		"type":       "arrival",
		"title":      "Bus Arriving Soon",
		"body":       "Van " + regNum + " is 5 minutes away",
	})
	if err != nil {
		t.Fatalf("create notification: %v", err)
	}
	assertStatus(t, status, 201)
	notifID := notif["id"].(string)
	assertField(t, notif, "is_read", false)

	// ── Step 7: List notifications ────────────────────────────────────
	t.Log("Step 7: List notifications")
	status, items, err := authed.getList("/api/v1/notifications")
	if err != nil {
		t.Fatalf("list notifications: %v", err)
	}
	assertStatus(t, status, 200)
	if len(items) < 1 {
		t.Fatal("expected at least 1 notification")
	}

	// ── Step 8: Unread count ──────────────────────────────────────────
	t.Log("Step 8: Unread count")
	status, countData, err := authed.get("/api/v1/notifications/unread-count")
	if err != nil {
		t.Fatalf("unread count: %v", err)
	}
	assertStatus(t, status, 200)
	assertField(t, countData, "unread_count", 1)

	// ── Step 9: Mark notification as read ─────────────────────────────
	t.Log("Step 9: Mark read")
	status, marked, err := authed.put(fmt.Sprintf("/api/v1/notifications/%s/read", notifID), nil)
	if err != nil {
		t.Fatalf("mark read: %v", err)
	}
	assertStatus(t, status, 200)
	assertField(t, marked, "is_read", true)
	assertFieldExists(t, marked, "read_at")

	// ── Step 10: Verify unread count is 0 ─────────────────────────────
	t.Log("Step 10: Verify unread = 0")
	status, countData, err = authed.get("/api/v1/notifications/unread-count")
	if err != nil {
		t.Fatalf("unread count after read: %v", err)
	}
	assertStatus(t, status, 200)
	assertField(t, countData, "unread_count", 0)

	// ── Step 11: Partial vehicle update (preserves is_active) ─────────
	t.Log("Step 11: Partial update vehicle")
	status, updated, err := c.put(fmt.Sprintf("/api/v1/vehicles/%s", vehicleID), map[string]interface{}{
		"fleet_id":       fleetID(),
		"status_message": "running on time",
	})
	if err != nil {
		t.Fatalf("partial update: %v", err)
	}
	assertStatus(t, status, 200)
	assertField(t, updated, "status_message", "running on time")
	assertField(t, updated, "is_active", true) // Must stay true

	// ── Step 12: Cancel subscription (soft delete) ────────────────────
	t.Log("Step 12: Cancel subscription")
	status, _, err = authed.delete(fmt.Sprintf("/api/v1/subscriptions/%s?user_id=%s", subID, userID))
	if err != nil {
		t.Fatalf("cancel sub: %v", err)
	}
	assertStatus(t, status, 200)

	// Verify subscription still exists but cancelled
	status, cancelled, err := c.get(fmt.Sprintf("/api/v1/subscriptions/%s?user_id=%s", subID, userID))
	if err != nil {
		t.Fatalf("get cancelled sub: %v", err)
	}
	assertStatus(t, status, 200)
	assertField(t, cancelled, "status", "cancelled")

	t.Log("✅ Full passenger journey completed successfully")
}

// TestJourney_DriverFlow covers driver-specific operations:
//
//	Register driver → Create vehicle → Create driver record →
//	Update driver location → Create event → Create notice
func TestJourney_DriverFlow(t *testing.T) {
	c := newClient()
	email := uniqueEmail("driver_journey")
	password := "DriverJourney1!"

	// Register as driver
	t.Log("Register driver")
	status, reg, err := c.post("/api/v1/auth/register", map[string]interface{}{
		"email":        email,
		"password":     password,
		"role":         "driver",
		"display_name": "Driver Journey",
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	assertStatus(t, status, 201)
	_ = reg["user"].(map[string]interface{})["id"].(string)
	token := reg["access_token"].(string)
	authed := c.withToken(token)

	// Create vehicle
	t.Log("Create vehicle")
	regNum := fmt.Sprintf("DRV-J-%d", uniqueSuffix())
	status, vehicle, err := c.post("/api/v1/vehicles", map[string]interface{}{
		"fleet_id":            fleetID(),
		"registration_number": regNum,
		"type":                "bus",
		"is_active":           true,
	})
	if err != nil {
		t.Fatalf("create vehicle: %v", err)
	}
	assertStatus(t, status, 201)
	vehicleID := vehicle["id"].(string)

	// Create driver record
	t.Log("Create driver record")
	status, driver, err := c.post("/api/v1/drivers", map[string]interface{}{
		"fleet_id":  fleetID(),
		"full_name": "Driver Journey",
		"email":     email,
		"phone":     "+94771234567",
		"is_active": true,
	})
	if err != nil {
		t.Fatalf("create driver: %v", err)
	}
	assertStatus(t, status, 201)
	driverID := driver["id"].(string)
	_ = driverID

	// Update vehicle location
	t.Log("Update vehicle location")
	status, loc, err := c.put(fmt.Sprintf("/api/v1/vehicles/%s/location", vehicleID), map[string]interface{}{
		"fleet_id": fleetID(),
		"location": map[string]interface{}{
			"latitude":  6.9271,
			"longitude": 79.8612,
			"speed":     45.0,
			"heading":   180.0,
		},
	})
	if err != nil {
		t.Fatalf("update location: %v", err)
	}
	assertStatus(t, status, 200)
	// Response is full vehicle; check current_location
	locData, ok := loc["current_location"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected current_location in response, got: %v", loc)
	}
	lat, _ := locData["latitude"].(float64)
	if lat < 6.0 || lat > 7.0 {
		t.Fatalf("unexpected latitude: %f", lat)
	}

	// Create event
	t.Log("Create event")
	status, event, err := c.post("/api/v1/events", map[string]interface{}{
		"type":       "trip_start",
		"vehicle_id": vehicleID,
		"fleet_id":   fleetID(),
		"message":    "Morning run started",
	})
	if err != nil {
		t.Fatalf("create event: %v", err)
	}
	assertStatus(t, status, 201)
	assertField(t, event, "type", "trip_start")

	// Create notice
	t.Log("Create notice")
	status, notice, err := c.post("/api/v1/notices", map[string]interface{}{
		"fleet_id": fleetID(),
		"title":    "Route Change",
		"message":  "Route modified for today",
	})
	if err != nil {
		t.Fatalf("create notice: %v", err)
	}
	assertStatus(t, status, 201)
	assertFieldNotEmpty(t, notice, "id")

	// Verify profile
	t.Log("Verify driver profile")
	status, profile, err := authed.get("/api/v1/auth/profile")
	if err != nil {
		t.Fatalf("profile: %v", err)
	}
	assertStatus(t, status, 200)
	assertField(t, profile, "role", "driver")

	t.Log("✅ Full driver journey completed successfully")
}
