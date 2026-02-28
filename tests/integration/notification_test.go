//go:build integration

package integration

import (
	"fmt"
	"testing"
)

// ---------------------------------------------------------------------------
// Notification: Create
// ---------------------------------------------------------------------------

func TestNotif_Create(t *testing.T) {
	c := newClient()

	// Register a user to receive notifications
	_, regData, _ := c.post("/api/v1/auth/register", map[string]interface{}{
		"email":    uniqueEmail("notif"),
		"password": "NotifPass1!",
		"role":     "passenger",
	})
	userID := regData["user"].(map[string]interface{})["id"].(string)

	// Create notification
	status, notif, err := c.post("/api/v1/notifications", map[string]interface{}{
		"user_id":  userID,
		"fleet_id": fleetID(),
		"type":     "arrival",
		"title":    "Bus Arriving",
		"body":     "Your bus is 3 minutes away",
	})
	if err != nil {
		t.Fatalf("create notification failed: %v", err)
	}
	assertStatus(t, status, 201)
	assertFieldNotEmpty(t, notif, "id")
	assertField(t, notif, "user_id", userID)
	assertField(t, notif, "title", "Bus Arriving")
	assertField(t, notif, "body", "Your bus is 3 minutes away")
	assertField(t, notif, "type", "arrival")
	assertField(t, notif, "is_read", false)
	assertFieldExists(t, notif, "created_at")
}

// ---------------------------------------------------------------------------
// Notification: List (requires JWT so user_id is extracted)
// ---------------------------------------------------------------------------

func TestNotif_List_WithToken(t *testing.T) {
	c := newClient()

	// Register
	email := uniqueEmail("notif_list")
	_, regData, _ := c.post("/api/v1/auth/register", map[string]interface{}{
		"email":    email,
		"password": "NotifList1!",
		"role":     "passenger",
	})
	userID := regData["user"].(map[string]interface{})["id"].(string)
	token := regData["access_token"].(string)

	// Create 2 notifications for this user
	c.post("/api/v1/notifications", map[string]interface{}{
		"user_id":  userID,
		"fleet_id": fleetID(),
		"type":     "event",
		"title":    "Notification 1",
		"body":     "First notification",
	})
	c.post("/api/v1/notifications", map[string]interface{}{
		"user_id":  userID,
		"fleet_id": fleetID(),
		"type":     "event",
		"title":    "Notification 2",
		"body":     "Second notification",
	})

	// List with JWT
	authed := c.withToken(token)
	status, items, err := authed.getList("/api/v1/notifications")
	if err != nil {
		t.Fatalf("list notifications failed: %v", err)
	}
	assertStatus(t, status, 200)
	if len(items) < 2 {
		t.Fatalf("expected at least 2 notifications, got %d", len(items))
	}

	// All should belong to this user
	for _, item := range items {
		if item["user_id"] != userID {
			t.Fatalf("notification user_id mismatch: expected %s, got %s", userID, item["user_id"])
		}
	}
}

// ---------------------------------------------------------------------------
// Notification: Unread Count
// ---------------------------------------------------------------------------

func TestNotif_UnreadCount(t *testing.T) {
	c := newClient()

	// Register
	_, regData, _ := c.post("/api/v1/auth/register", map[string]interface{}{
		"email":    uniqueEmail("unread"),
		"password": "Unread1!",
		"role":     "passenger",
	})
	userID := regData["user"].(map[string]interface{})["id"].(string)
	token := regData["access_token"].(string)
	authed := c.withToken(token)

	// Initially 0
	status, data, err := authed.get("/api/v1/notifications/unread-count")
	if err != nil {
		t.Fatalf("unread count failed: %v", err)
	}
	assertStatus(t, status, 200)
	assertField(t, data, "unread_count", 0)

	// Create 3 notifications
	for i := 0; i < 3; i++ {
		c.post("/api/v1/notifications", map[string]interface{}{
			"user_id":  userID,
			"fleet_id": fleetID(),
			"type":     "event",
			"title":    fmt.Sprintf("Notif %d", i+1),
			"body":     "Test body",
		})
	}

	// Should be 3
	status, data, err = authed.get("/api/v1/notifications/unread-count")
	if err != nil {
		t.Fatalf("unread count failed: %v", err)
	}
	assertStatus(t, status, 200)
	assertField(t, data, "unread_count", 3)
}

// ---------------------------------------------------------------------------
// Notification: Mark as Read
// ---------------------------------------------------------------------------

func TestNotif_MarkRead(t *testing.T) {
	c := newClient()

	// Register
	_, regData, _ := c.post("/api/v1/auth/register", map[string]interface{}{
		"email":    uniqueEmail("markread"),
		"password": "MarkRead1!",
		"role":     "passenger",
	})
	userID := regData["user"].(map[string]interface{})["id"].(string)
	token := regData["access_token"].(string)
	authed := c.withToken(token)

	// Create notification
	_, notif, _ := c.post("/api/v1/notifications", map[string]interface{}{
		"user_id":  userID,
		"fleet_id": fleetID(),
		"type":     "arrival",
		"title":    "Read Me",
		"body":     "Please read this",
	})
	notifID := notif["id"].(string)

	// Verify unread
	assertField(t, notif, "is_read", false)

	// Mark as read
	status, marked, err := authed.put(fmt.Sprintf("/api/v1/notifications/%s/read", notifID), nil)
	if err != nil {
		t.Fatalf("mark read failed: %v", err)
	}
	assertStatus(t, status, 200)
	assertField(t, marked, "is_read", true)
	assertFieldExists(t, marked, "read_at")

	// Unread count should be 0
	status, countData, err := authed.get("/api/v1/notifications/unread-count")
	if err != nil {
		t.Fatalf("unread count failed: %v", err)
	}
	assertStatus(t, status, 200)
	assertField(t, countData, "unread_count", 0)
}

// ---------------------------------------------------------------------------
// Notification: With vehicle_id
// ---------------------------------------------------------------------------

func TestNotif_WithVehicleID(t *testing.T) {
	c := newClient()

	// Setup
	_, regData, _ := c.post("/api/v1/auth/register", map[string]interface{}{
		"email":    uniqueEmail("notif_vid"),
		"password": "VehicleNotif1!",
		"role":     "passenger",
	})
	userID := regData["user"].(map[string]interface{})["id"].(string)

	_, vehicle, _ := c.post("/api/v1/vehicles", map[string]interface{}{
		"fleet_id":            fleetID(),
		"registration_number": fmt.Sprintf("NOTIF-V-%d", uniqueSuffix()),
		"type":                "bus",
	})
	vehicleID := vehicle["id"].(string)

	// Create notification with vehicle_id
	status, notif, err := c.post("/api/v1/notifications", map[string]interface{}{
		"user_id":    userID,
		"vehicle_id": vehicleID,
		"fleet_id":   fleetID(),
		"type":       "arrival",
		"title":      "Vehicle Notification",
		"body":       "Linked to a specific vehicle",
	})
	if err != nil {
		t.Fatalf("create notification failed: %v", err)
	}
	assertStatus(t, status, 201)
	assertField(t, notif, "vehicle_id", vehicleID)
}
