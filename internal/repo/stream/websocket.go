package stream

import (
	"sync"
	"time"

	"github.com/TekkenSteve/GoAgent/internal/entity"
	"github.com/fasthttp/websocket"
)

const (
	// wsWriteWait is the maximum time to wait for a write to complete.
	wsWriteWait = 10 * time.Second
)

// wsClient represents a single WebSocket connection associated with a session.
// Writes are serialized via writeMu to satisfy the websocket.Conn contract
// (at most one concurrent writer).
type wsClient struct {
	conn    *websocket.Conn
	writeMu sync.Mutex
}

func newWSClient(conn *websocket.Conn) *wsClient {
	return &wsClient{conn: conn}
}

// writeMessage sends a message on the WebSocket connection with a timeout.
func (c *wsClient) writeMessage(messageType int, data []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if err := c.conn.SetWriteDeadline(time.Now().Add(wsWriteWait)); err != nil {
		return err
	}

	return c.conn.WriteMessage(messageType, data)
}

// WebSocketHub manages WebSocket connections grouped by session.
// It provides concurrent-safe registration, deregistration, and
// broadcasting to all clients within a session.
type WebSocketHub struct {
	mu       sync.RWMutex
	sessions map[string]map[*wsClient]struct{}
	codec    entity.EventCodec
}

// NewWebSocketHub creates an empty WebSocketHub.
func NewWebSocketHub() *WebSocketHub {
	return NewWebSocketHubWithCodec(entity.NewEventCodec(nil))
}

// NewWebSocketHubWithCodec creates an empty WebSocketHub with an explicit event codec.
func NewWebSocketHubWithCodec(codec entity.EventCodec) *WebSocketHub {
	return &WebSocketHub{
		sessions: make(map[string]map[*wsClient]struct{}),
		codec:    codec,
	}
}

// Add registers a new WebSocket connection under sessionID.
// Returns a close function that deregisters the connection from the hub.
func (h *WebSocketHub) Add(sessionID string, conn *websocket.Conn) func() {
	client := newWSClient(conn)

	h.mu.Lock()
	if h.sessions[sessionID] == nil {
		h.sessions[sessionID] = make(map[*wsClient]struct{})
	}

	h.sessions[sessionID][client] = struct{}{}
	h.mu.Unlock()

	return func() { h.remove(sessionID, client) }
}

// Broadcast marshals an event and sends it to every connection in a session.
// Errors on individual connections are silently dropped — a single slow or
// broken client must not block other clients or the event producer.
func (h *WebSocketHub) Broadcast(sessionID string, event entity.StreamEvent) {
	data, err := h.codec.MarshalEvent(event)
	if err != nil {
		return
	}

	h.BroadcastBytes(sessionID, websocket.TextMessage, data)
}

// BroadcastBytes sends raw bytes to every connection in a session.
func (h *WebSocketHub) BroadcastBytes(sessionID string, messageType int, data []byte) {
	h.mu.RLock()
	clients := h.sessions[sessionID]
	h.mu.RUnlock()

	for cl := range clients {
		if err := cl.writeMessage(messageType, data); err != nil {
			_ = err
		}
	}
}

// ClientCount returns the number of connected clients for a session.
func (h *WebSocketHub) ClientCount(sessionID string) int {
	h.mu.RLock()
	defer h.mu.RUnlock()

	return len(h.sessions[sessionID])
}

// remove deregisters a client from its session. If the session has no
// remaining clients the session entry is cleaned up.
func (h *WebSocketHub) remove(sessionID string, client *wsClient) {
	h.mu.Lock()
	defer h.mu.Unlock()

	clients := h.sessions[sessionID]
	if clients == nil {
		return
	}

	delete(clients, client)

	if len(clients) == 0 {
		delete(h.sessions, sessionID)
	}
}
