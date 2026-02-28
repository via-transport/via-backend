//go:build integration

package integration

import (
	"testing"
)

// ---------------------------------------------------------------------------
// Auth: Registration
// ---------------------------------------------------------------------------

func TestAuth_Register_Success(t *testing.T) {
	c := newClient()
	email := uniqueEmail("register")

	status, data, err := c.post("/api/v1/auth/register", map[string]interface{}{
		"email":        email,
		"password":     "SecurePass1!",
		"display_name": "Test User",
		"role":         "passenger",
		"phone":        "+94771234567",
	})
	if err != nil {
		t.Fatalf("register failed: %v", err)
	}
	assertStatus(t, status, 201)

	// Token pair
	assertFieldNotEmpty(t, data, "access_token")
	assertFieldNotEmpty(t, data, "refresh_token")
	assertFieldExists(t, data, "expires_at")

	// Nested user object
	user, ok := data["user"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected 'user' object in response, got: %v", data)
	}
	assertFieldNotEmpty(t, user, "id")
	assertField(t, user, "email", email)
	assertField(t, user, "display_name", "Test User")
	assertField(t, user, "role", "passenger")
	assertField(t, user, "is_active", true)
	assertFieldExists(t, user, "created_at")
}

func TestAuth_Register_Driver(t *testing.T) {
	c := newClient()
	email := uniqueEmail("driver_reg")

	status, data, err := c.post("/api/v1/auth/register", map[string]interface{}{
		"email":        email,
		"password":     "SecurePass1!",
		"display_name": "Driver User",
		"role":         "driver",
	})
	if err != nil {
		t.Fatalf("register driver failed: %v", err)
	}
	assertStatus(t, status, 201)

	user := data["user"].(map[string]interface{})
	assertField(t, user, "role", "driver")
}

func TestAuth_Register_DuplicateEmail(t *testing.T) {
	c := newClient()
	email := uniqueEmail("dup")

	// First registration
	status, _, err := c.post("/api/v1/auth/register", map[string]interface{}{
		"email":    email,
		"password": "SecurePass1!",
		"role":     "passenger",
	})
	if err != nil {
		t.Fatalf("first register failed: %v", err)
	}
	assertStatus(t, status, 201)

	// Duplicate
	status, data, err := c.post("/api/v1/auth/register", map[string]interface{}{
		"email":    email,
		"password": "AnotherPass1!",
		"role":     "passenger",
	})
	if err != nil {
		t.Fatalf("second register failed: %v", err)
	}
	if status == 201 {
		t.Fatalf("expected duplicate email to be rejected, got 201")
	}
	assertError(t, data, "already registered")
}

func TestAuth_Register_RoleEscalation_Blocked(t *testing.T) {
	c := newClient()
	email := uniqueEmail("escalation")

	// Try to register as owner
	status, data, err := c.post("/api/v1/auth/register", map[string]interface{}{
		"email":    email,
		"password": "SecurePass1!",
		"role":     "owner",
	})
	if err != nil {
		t.Fatalf("register failed: %v", err)
	}
	if status == 201 {
		t.Fatalf("registering as 'owner' should be blocked, got 201")
	}
	assertError(t, data, "role must be passenger or driver")

	// Try admin
	status, data, err = c.post("/api/v1/auth/register", map[string]interface{}{
		"email":    uniqueEmail("admin_esc"),
		"password": "SecurePass1!",
		"role":     "admin",
	})
	if err != nil {
		t.Fatalf("register failed: %v", err)
	}
	if status == 201 {
		t.Fatalf("registering as 'admin' should be blocked, got 201")
	}
	assertError(t, data, "role must be passenger or driver")

	// Try service
	status, data, err = c.post("/api/v1/auth/register", map[string]interface{}{
		"email":    uniqueEmail("svc_esc"),
		"password": "SecurePass1!",
		"role":     "service",
	})
	if err != nil {
		t.Fatalf("register failed: %v", err)
	}
	if status == 201 {
		t.Fatalf("registering as 'service' should be blocked, got 201")
	}
}

func TestAuth_Register_DefaultRole(t *testing.T) {
	c := newClient()
	email := uniqueEmail("defaultrole")

	// Register with no role – should default to passenger
	status, data, err := c.post("/api/v1/auth/register", map[string]interface{}{
		"email":    email,
		"password": "SecurePass1!",
	})
	if err != nil {
		t.Fatalf("register failed: %v", err)
	}
	assertStatus(t, status, 201)

	user := data["user"].(map[string]interface{})
	assertField(t, user, "role", "passenger")
}

// ---------------------------------------------------------------------------
// Auth: Login
// ---------------------------------------------------------------------------

func TestAuth_Login_Success(t *testing.T) {
	c := newClient()
	email := uniqueEmail("login")
	password := "LoginPass1!"

	// Register first
	status, _, err := c.post("/api/v1/auth/register", map[string]interface{}{
		"email":        email,
		"password":     password,
		"display_name": "Login Tester",
		"role":         "passenger",
	})
	if err != nil {
		t.Fatalf("register failed: %v", err)
	}
	assertStatus(t, status, 201)

	// Login
	status, data, err := c.post("/api/v1/auth/login", map[string]interface{}{
		"email":    email,
		"password": password,
	})
	if err != nil {
		t.Fatalf("login failed: %v", err)
	}
	assertStatus(t, status, 200)
	assertFieldNotEmpty(t, data, "access_token")
	assertFieldNotEmpty(t, data, "refresh_token")

	user := data["user"].(map[string]interface{})
	assertField(t, user, "email", email)
	assertField(t, user, "display_name", "Login Tester")
}

func TestAuth_Login_WrongPassword(t *testing.T) {
	c := newClient()
	email := uniqueEmail("wrongpw")

	// Register
	c.post("/api/v1/auth/register", map[string]interface{}{
		"email":    email,
		"password": "CorrectPass1!",
		"role":     "passenger",
	})

	// Wrong password
	status, data, err := c.post("/api/v1/auth/login", map[string]interface{}{
		"email":    email,
		"password": "WrongPass1!",
	})
	if err != nil {
		t.Fatalf("login failed: %v", err)
	}
	if status == 200 {
		t.Fatalf("login with wrong password should fail")
	}
	assertError(t, data, "invalid")
}

func TestAuth_Login_NonexistentUser(t *testing.T) {
	c := newClient()

	status, _, err := c.post("/api/v1/auth/login", map[string]interface{}{
		"email":    "nobody_exists@test.via.lk",
		"password": "Whatever1!",
	})
	if err != nil {
		t.Fatalf("login failed: %v", err)
	}
	if status == 200 {
		t.Fatalf("login for non-existent user should fail")
	}
}

// ---------------------------------------------------------------------------
// Auth: Token Refresh
// ---------------------------------------------------------------------------

func TestAuth_Refresh_Success(t *testing.T) {
	c := newClient()
	email := uniqueEmail("refresh")

	// Register to get tokens
	status, regData, err := c.post("/api/v1/auth/register", map[string]interface{}{
		"email":    email,
		"password": "RefreshPass1!",
		"role":     "passenger",
	})
	if err != nil {
		t.Fatalf("register failed: %v", err)
	}
	assertStatus(t, status, 201)

	refreshToken := regData["refresh_token"].(string)

	// Call refresh
	status, data, err := c.post("/api/v1/auth/refresh", map[string]interface{}{
		"refresh_token": refreshToken,
	})
	if err != nil {
		t.Fatalf("refresh failed: %v", err)
	}
	assertStatus(t, status, 200)
	assertFieldNotEmpty(t, data, "access_token")
	assertFieldNotEmpty(t, data, "refresh_token")

	// New access token should differ from original
	newAccess := data["access_token"].(string)
	oldAccess := regData["access_token"].(string)
	if newAccess == oldAccess {
		t.Log("warning: refreshed token is identical (may be OK if within same second)")
	}
}

func TestAuth_Refresh_InvalidToken(t *testing.T) {
	c := newClient()

	status, _, err := c.post("/api/v1/auth/refresh", map[string]interface{}{
		"refresh_token": "not.a.valid.token",
	})
	if err != nil {
		t.Fatalf("refresh failed: %v", err)
	}
	if status == 200 {
		t.Fatalf("refresh with invalid token should fail")
	}
}

// ---------------------------------------------------------------------------
// Auth: Profile (requires JWT parsing in dev mode)
// ---------------------------------------------------------------------------

func TestAuth_Profile_WithToken(t *testing.T) {
	c := newClient()
	email := uniqueEmail("profile")

	// Register
	status, regData, err := c.post("/api/v1/auth/register", map[string]interface{}{
		"email":        email,
		"password":     "ProfilePass1!",
		"display_name": "Profile User",
		"role":         "passenger",
		"phone":        "+94771112233",
	})
	if err != nil {
		t.Fatalf("register failed: %v", err)
	}
	assertStatus(t, status, 201)

	token := regData["access_token"].(string)
	authed := c.withToken(token)

	// Get profile
	status, profile, err := authed.get("/api/v1/auth/profile")
	if err != nil {
		t.Fatalf("get profile failed: %v", err)
	}
	assertStatus(t, status, 200)
	assertField(t, profile, "email", email)
	assertField(t, profile, "display_name", "Profile User")
	assertField(t, profile, "role", "passenger")
	assertField(t, profile, "is_active", true)
}

func TestAuth_Profile_Update(t *testing.T) {
	c := newClient()
	email := uniqueEmail("profile_upd")

	// Register
	_, regData, _ := c.post("/api/v1/auth/register", map[string]interface{}{
		"email":        email,
		"password":     "UpdatePass1!",
		"display_name": "Before Update",
		"role":         "passenger",
	})

	token := regData["access_token"].(string)
	authed := c.withToken(token)

	// Update profile
	status, updated, err := authed.put("/api/v1/auth/profile", map[string]interface{}{
		"display_name": "After Update",
		"phone":        "+94779999999",
	})
	if err != nil {
		t.Fatalf("update profile failed: %v", err)
	}
	assertStatus(t, status, 200)
	assertField(t, updated, "display_name", "After Update")

	// Verify persistence
	status, profile, err := authed.get("/api/v1/auth/profile")
	if err != nil {
		t.Fatalf("get profile failed: %v", err)
	}
	assertStatus(t, status, 200)
	assertField(t, profile, "display_name", "After Update")
}

// ---------------------------------------------------------------------------
// Auth: Password Change
// ---------------------------------------------------------------------------

func TestAuth_ChangePassword(t *testing.T) {
	c := newClient()
	email := uniqueEmail("chpw")
	oldPw := "OldPassword1!"
	newPw := "NewPassword1!"

	// Register
	_, regData, _ := c.post("/api/v1/auth/register", map[string]interface{}{
		"email":    email,
		"password": oldPw,
		"role":     "passenger",
	})

	token := regData["access_token"].(string)
	authed := c.withToken(token)

	// Change password
	status, _, err := authed.put("/api/v1/auth/password", map[string]interface{}{
		"old_password": oldPw,
		"new_password": newPw,
	})
	if err != nil {
		t.Fatalf("change password failed: %v", err)
	}
	assertStatus(t, status, 200)

	// Old password should no longer work
	status, _, _ = c.post("/api/v1/auth/login", map[string]interface{}{
		"email":    email,
		"password": oldPw,
	})
	if status == 200 {
		t.Fatalf("old password should no longer work after change")
	}

	// New password should work
	status, _, err = c.post("/api/v1/auth/login", map[string]interface{}{
		"email":    email,
		"password": newPw,
	})
	if err != nil {
		t.Fatalf("login with new password failed: %v", err)
	}
	assertStatus(t, status, 200)
}
