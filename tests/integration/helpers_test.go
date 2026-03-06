// Package integration contains end-to-end integration tests for the Via backend.
//
// Prerequisites:
//   - Backend running at VIA_TEST_BASE_URL (default http://localhost:9090)
//   - NATS running and connected to backend
//   - AUTH_ENABLED=false on the backend (or provide valid credentials)
//
// Run:
//
//	go test -v -tags integration ./tests/integration/...
//	go test -v -tags integration -run TestAuth ./tests/integration/...
//
// Environment:
//
//	VIA_TEST_BASE_URL  – backend base URL (default http://localhost:9090)
//	VIA_TEST_FLEET_ID  – fleet ID to use (default school-west)

//go:build integration

package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Unique suffix (shared across all test files)
// ---------------------------------------------------------------------------

var _suffixCounter int64

func uniqueSuffix() int64 {
	return atomic.AddInt64(&_suffixCounter, 1) + time.Now().UnixNano()%100000
}

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

func baseURL() string {
	if u := os.Getenv("VIA_TEST_BASE_URL"); u != "" {
		return strings.TrimRight(u, "/")
	}
	return "http://localhost:9090"
}

func fleetID() string {
	if f := os.Getenv("VIA_TEST_FLEET_ID"); f != "" {
		return f
	}
	return "school-west"
}

// uniqueEmail returns a unique email for each test run.
func uniqueEmail(prefix string) string {
	return fmt.Sprintf("%s_%d@test.via.lk", prefix, time.Now().UnixNano())
}

// ---------------------------------------------------------------------------
// HTTP helpers
// ---------------------------------------------------------------------------

// apiClient is a thin wrapper around http.Client with JSON helpers.
type apiClient struct {
	base   string
	token  string // Bearer token (optional)
	client *http.Client
}

func newClient() *apiClient {
	return &apiClient{
		base: baseURL(),
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (c *apiClient) withToken(token string) *apiClient {
	return &apiClient{
		base:   c.base,
		token:  token,
		client: c.client,
	}
}

// do sends an HTTP request and decodes the JSON response.
func (c *apiClient) do(method, path string, body interface{}) (int, map[string]interface{}, error) {
	var bodyReader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return 0, nil, fmt.Errorf("marshal body: %w", err)
		}
		bodyReader = bytes.NewReader(b)
	}

	url := c.base + path
	req, err := http.NewRequest(method, url, bodyReader)
	if err != nil {
		return 0, nil, fmt.Errorf("new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return 0, nil, fmt.Errorf("do request %s %s: %w", method, path, err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, nil, fmt.Errorf("read body: %w", err)
	}

	if len(respBody) == 0 {
		return resp.StatusCode, nil, nil
	}

	var result map[string]interface{}
	if err := json.Unmarshal(respBody, &result); err != nil {
		// Might be an array – wrap it
		var arr []interface{}
		if err2 := json.Unmarshal(respBody, &arr); err2 == nil {
			return resp.StatusCode, map[string]interface{}{"_items": arr}, nil
		}
		return resp.StatusCode, nil, fmt.Errorf("decode response: %w (body: %s)", err, string(respBody))
	}
	return resp.StatusCode, result, nil
}

// doList sends a request expecting a JSON array response.
func (c *apiClient) doList(method, path string, body interface{}) (int, []map[string]interface{}, error) {
	var bodyReader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return 0, nil, fmt.Errorf("marshal body: %w", err)
		}
		bodyReader = bytes.NewReader(b)
	}

	url := c.base + path
	req, err := http.NewRequest(method, url, bodyReader)
	if err != nil {
		return 0, nil, fmt.Errorf("new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return 0, nil, fmt.Errorf("do request %s %s: %w", method, path, err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, nil, fmt.Errorf("read body: %w", err)
	}

	var items []map[string]interface{}
	if err := json.Unmarshal(respBody, &items); err != nil {
		return resp.StatusCode, nil, fmt.Errorf("decode list: %w (body: %s)", err, string(respBody))
	}
	return resp.StatusCode, items, nil
}

// Convenience methods

func (c *apiClient) get(path string) (int, map[string]interface{}, error) {
	return c.do("GET", path, nil)
}

func (c *apiClient) post(path string, body interface{}) (int, map[string]interface{}, error) {
	return c.do("POST", path, body)
}

func (c *apiClient) put(path string, body interface{}) (int, map[string]interface{}, error) {
	return c.do("PUT", path, body)
}

func (c *apiClient) delete(path string) (int, map[string]interface{}, error) {
	return c.do("DELETE", path, nil)
}

func (c *apiClient) getList(path string) (int, []map[string]interface{}, error) {
	return c.doList("GET", path, nil)
}

func (c *apiClient) waitForOperation(
	operationID string,
	timeout time.Duration,
) (map[string]interface{}, error) {
	deadline := time.Now().Add(timeout)
	var last map[string]interface{}

	for time.Now().Before(deadline) {
		status, data, err := c.get(fmt.Sprintf("/api/v1/operations/%s", operationID))
		if err != nil {
			return nil, fmt.Errorf("get operation %s: %w", operationID, err)
		}
		if status != http.StatusOK {
			return nil, fmt.Errorf("expected operation status 200, got %d", status)
		}
		last = data

		switch strings.TrimSpace(fmt.Sprintf("%v", data["status"])) {
		case "succeeded":
			return data, nil
		case "failed":
			return data, fmt.Errorf(
				"operation failed: %v",
				data["error_message"],
			)
		}

		time.Sleep(200 * time.Millisecond)
	}

	if last != nil {
		return last, fmt.Errorf(
			"operation %s did not complete before timeout; last status=%v",
			operationID,
			last["status"],
		)
	}
	return nil, fmt.Errorf("operation %s not found before timeout", operationID)
}

// ---------------------------------------------------------------------------
// Assertion helpers
// ---------------------------------------------------------------------------

func assertStatus(t *testing.T, got, want int) {
	t.Helper()
	if got != want {
		t.Fatalf("expected status %d, got %d", want, got)
	}
}

func assertField(t *testing.T, data map[string]interface{}, key string, want interface{}) {
	t.Helper()
	got, ok := data[key]
	if !ok {
		t.Fatalf("missing key %q in response: %v", key, data)
	}
	// JSON numbers come back as float64
	switch w := want.(type) {
	case int:
		if g, ok := got.(float64); ok {
			if int(g) != w {
				t.Fatalf("%s: expected %v, got %v", key, want, got)
			}
			return
		}
	case bool:
		if g, ok := got.(bool); ok {
			if g != w {
				t.Fatalf("%s: expected %v, got %v", key, want, got)
			}
			return
		}
	}
	gotStr := fmt.Sprintf("%v", got)
	wantStr := fmt.Sprintf("%v", want)
	if gotStr != wantStr {
		t.Fatalf("%s: expected %q, got %q", key, wantStr, gotStr)
	}
}

func assertFieldExists(t *testing.T, data map[string]interface{}, key string) {
	t.Helper()
	if _, ok := data[key]; !ok {
		t.Fatalf("expected key %q in response, got: %v", key, data)
	}
}

func assertFieldNotEmpty(t *testing.T, data map[string]interface{}, key string) {
	t.Helper()
	v, ok := data[key]
	if !ok {
		t.Fatalf("expected key %q in response, got: %v", key, data)
	}
	s, _ := v.(string)
	if s == "" {
		t.Fatalf("expected non-empty %q, got empty string", key)
	}
}

func assertError(t *testing.T, data map[string]interface{}, substr string) {
	t.Helper()
	errMsg, ok := data["error"].(string)
	if !ok {
		t.Fatalf("expected error field in response, got: %v", data)
	}
	if !strings.Contains(strings.ToLower(errMsg), strings.ToLower(substr)) {
		t.Fatalf("expected error containing %q, got: %q", substr, errMsg)
	}
}

// ---------------------------------------------------------------------------
// Setup check
// ---------------------------------------------------------------------------

func TestHealthcheck(t *testing.T) {
	c := newClient()
	status, data, err := c.get("/healthz")
	if err != nil {
		t.Fatalf("healthcheck failed: %v", err)
	}
	assertStatus(t, status, 200)
	assertField(t, data, "status", "ok")
}
