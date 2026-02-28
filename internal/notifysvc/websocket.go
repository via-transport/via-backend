package notifysvc

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/nats-io/nats.go"

	"via-backend/internal/auth"
	"via-backend/internal/authsvc"
	"via-backend/internal/messaging"
)

var upgrader = websocket.Upgrader{
	CheckOrigin:  func(r *http.Request) bool { return true },
	ReadBufferSize:  4096,
	WriteBufferSize: 8192,
}

// Hub manages WebSocket connections for notification delivery.
type Hub struct {
	mu      sync.RWMutex
	clients map[string]map[*wsClient]struct{} // userID → set of clients
}

type wsClient struct {
	userID string
	conn   *websocket.Conn
	send   chan []byte
}

// NewHub creates a new notification hub.
func NewHub() *Hub {
	return &Hub{
		clients: make(map[string]map[*wsClient]struct{}),
	}
}

// Run processes outgoing messages (not strictly needed since we use SendToUser directly,
// but keeps the hub goroutine alive for future fan-out patterns).
func (h *Hub) Run() {
	// Keep-alive: periodically clean up dead connections.
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		h.mu.Lock()
		for uid, cs := range h.clients {
			if len(cs) == 0 {
				delete(h.clients, uid)
			}
		}
		h.mu.Unlock()
	}
}

// Register adds a client to the hub.
func (h *Hub) Register(c *wsClient) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.clients[c.userID] == nil {
		h.clients[c.userID] = make(map[*wsClient]struct{})
	}
	h.clients[c.userID][c] = struct{}{}
	log.Printf("[notify-ws] client registered for user %s (total: %d)", c.userID, len(h.clients[c.userID]))
}

// Unregister removes a client from the hub.
func (h *Hub) Unregister(c *wsClient) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if cs, ok := h.clients[c.userID]; ok {
		delete(cs, c)
		if len(cs) == 0 {
			delete(h.clients, c.userID)
		}
	}
	close(c.send)
}

// SendToUser pushes a message to all connections of a user.
func (h *Hub) SendToUser(userID string, msg []byte) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	cs, ok := h.clients[userID]
	if !ok {
		return
	}
	for c := range cs {
		select {
		case c.send <- msg:
		default:
			// Client too slow, drop message.
		}
	}
}

// WSHandler upgrades HTTP to WebSocket for notification streaming.
// Authenticate via ?token=<jwt> query param or Authorization header.
func (h *Handler) WSHandler(w http.ResponseWriter, r *http.Request) {
	// Authenticate.
	userID, err := h.authenticateWS(r)
	if err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("[notify-ws] upgrade error: %v", err)
		return
	}

	client := &wsClient{
		userID: userID,
		conn:   conn,
		send:   make(chan []byte, 64),
	}
	h.hub.Register(client)

	// Send bootstrap: all unread notifications.
	go h.sendBootstrap(client)

	// Writer goroutine.
	go h.writePump(client)

	// Reader goroutine (handles pings, close).
	h.readPump(client)
}

// authenticateWS extracts user ID from JWT token in query param or header.
// Falls back to the auth middleware identity (for dev mode with AUTH_ENABLED=false).
func (h *Handler) authenticateWS(r *http.Request) (string, error) {
	tokenStr := r.URL.Query().Get("token")
	if tokenStr == "" {
		authHeader := r.Header.Get("Authorization")
		if len(authHeader) > 7 && authHeader[:7] == "Bearer " {
			tokenStr = authHeader[7:]
		}
	}
	if tokenStr != "" {
		claims, err := authsvc.ValidateTokenStatic(tokenStr)
		if err != nil {
			return "", err
		}
		return claims.Sub, nil
	}

	// Fallback: use identity from auth middleware context (dev mode).
	if id := auth.IdentityFromContext(r.Context()); id.UserID != "" {
		return id.UserID, nil
	}
	return "", http.ErrNoCookie
}

func (h *Handler) sendBootstrap(c *wsClient) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	notifs, err := h.store.ListForUser(ctx, c.userID, false)
	if err != nil {
		log.Printf("[notify-ws] bootstrap error for %s: %v", c.userID, err)
		return
	}
	unread := 0
	for _, n := range notifs {
		if !n.IsRead {
			unread++
		}
	}
	payload := NotificationPayload{
		Action:      "bootstrap",
		Items:       notifs,
		UnreadCount: unread,
	}
	data, _ := json.Marshal(payload)
	select {
	case c.send <- data:
	default:
	}
}

func (h *Handler) writePump(c *wsClient) {
	ticker := time.NewTicker(30 * time.Second)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()
	for {
		select {
		case msg, ok := <-c.send:
			if !ok {
				return
			}
			_ = c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}
		case <-ticker.C:
			_ = c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func (h *Handler) readPump(c *wsClient) {
	defer func() {
		h.hub.Unregister(c)
		c.conn.Close()
	}()
	c.conn.SetReadLimit(4096)
	c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})
	for {
		_, _, err := c.conn.ReadMessage()
		if err != nil {
			break
		}
	}
}

// SubscribeNATS subscribes to cross-instance notification subjects.
// This ensures that if a notification is published on another gateway instance,
// connected WebSocket clients on this instance still receive it.
func (h *Handler) SubscribeNATS(broker *messaging.Broker) {
	if broker.NC == nil {
		return
	}
	nc := broker.NC
	_, err := nc.Subscribe("notify.*", func(msg *nats.Msg) {
		// Extract user ID from subject: notify.<user_id>
		parts := splitSubject(msg.Subject)
		if len(parts) < 2 {
			return
		}
		userID := parts[1]
		h.hub.SendToUser(userID, msg.Data)
	})
	if err != nil {
		log.Printf("[notify-ws] NATS subscribe error: %v", err)
	}
}

func splitSubject(s string) []string {
	var parts []string
	start := 0
	for i, c := range s {
		if c == '.' {
			parts = append(parts, s[start:i])
			start = i + 1
		}
	}
	parts = append(parts, s[start:])
	return parts
}
