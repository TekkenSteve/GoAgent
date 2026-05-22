package v1

import (
	"context"
	"encoding/json"
	"time"

	"github.com/TekkenSteve/GoAgent/agentfw/stream"
	"github.com/TekkenSteve/GoAgent/entity"
	"github.com/fasthttp/websocket"
	"github.com/gofiber/fiber/v2"
	"github.com/valyala/fasthttp"
)

const (
	wsWriteWait      = 10 * time.Second
	wsPongWait       = 60 * time.Second
	wsMaxMessageSize = 4096
)

// ws handles GET /agent/ws.
// It upgrades the HTTP connection to WebSocket and streams agent execution
// events to the client. Unlike SSE, WebSocket supports bidirectional
// communication — client commands (cancel, pause, resume) are read from the
// same connection and forwarded via Temporal SignalWorkflow.
//
// Query parameters are identical to the SSE /agent/stream endpoint.
func (r *V1) ws(ctx *fiber.Ctx) error {
	params, err := parseWSQueryParams(ctx)
	if err != nil {
		return err
	}

	// Stream context: canceled when the client disconnects or sends cancel.
	streamCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req := entity.StreamRequest{
		RunID:        params.runID,
		SystemPrompt: params.systemPrompt,
		Message:      params.message,
		Config: entity.LLMConfig{
			Model: params.model,
		},
	}

	upgrader := websocket.FastHTTPUpgrader{
		CheckOrigin: func(_ *fasthttp.RequestCtx) bool { return true },
	}

	return upgrader.Upgrade(ctx.Context(), func(conn *websocket.Conn) {
		r.handleWSConnection(streamCtx, cancel, conn, params.sessionID, params.lastSequence, params.runID, &req)
	})
}

// wsQueryParams holds the validated WebSocket query parameters.
type wsQueryParams struct {
	message      string
	model        string
	runID        string
	systemPrompt string
	lastSequence int64
	sessionID    string
}

// parseWSQueryParams extracts and validates WebSocket query parameters.
func parseWSQueryParams(ctx *fiber.Ctx) (*wsQueryParams, error) {
	message := ctx.Query("message")
	if message == "" {
		return nil, errorResponse(ctx, fiber.StatusBadRequest, "missing message parameter")
	}

	model := ctx.Query("model")
	if model == "" {
		return nil, errorResponse(ctx, fiber.StatusBadRequest, "missing model parameter")
	}

	runID := ctx.Query("run_id")
	if runID == "" {
		return nil, errorResponse(ctx, fiber.StatusBadRequest, "missing run_id parameter")
	}

	systemPrompt := ctx.Query("system_prompt", "")
	lastEventID := ctx.Query("last_event_id", "0")

	return &wsQueryParams{
		message:      message,
		model:        model,
		runID:        runID,
		systemPrompt: systemPrompt,
		sessionID:    runID,
		lastSequence: parseLastSequence(lastEventID),
	}, nil
}

// handleWSConnection manages the WebSocket connection lifecycle.
func (r *V1) handleWSConnection(streamCtx context.Context, cancel func(), conn *websocket.Conn, sessionID string, lastSequence int64, runID string, req *entity.StreamRequest) {
	defer conn.Close()

	r.l.Info("agent ws - client connected - session=%s", sessionID)

	// Register with hub so the connection is visible for monitoring.
	removeFn := r.wsHub.Add(sessionID, conn)
	defer removeFn()

	// Subscribe to EventStore BEFORE starting execution so no events are missed.
	sub, err := r.subscriber.Subscribe(streamCtx, sessionID, lastSequence)
	if err != nil {
		r.l.Error("agent ws - subscribe: %v", err)

		return
	}
	defer sub.Close()

	// EventStore writer: execution writes events — store — subscription — WS.
	writer := &eventStoreWriter{
		store:     r.eventStore,
		sessionID: sessionID,
		runID:     runID,
	}

	// Start execution in background. doneCh signals when it exits.
	doneCh := make(chan struct{})
	go r.startStreamExecution(streamCtx, req, writer, doneCh)

	// --- connection-level setup ---
	if err := setupWSConn(conn); err != nil {
		return
	}

	// Send ack so the client knows the stream is established.
	if err := writeWSAck(conn, runID); err != nil {
		return
	}

	// --- command pump: client -> Temporal SignalWorkflow ---
	// Runs in a separate goroutine so commands are not blocked by event pumping.
	go r.runWSCommandPump(conn, sessionID, cancel)

	// --- event pump: EventStore subscription -> WebSocket ---
	r.runWSEventPump(streamCtx, conn, sub, doneCh)
}

// setupWSConn configures the WebSocket connection-level options (pong handler, read deadline).
func setupWSConn(conn *websocket.Conn) error {
	if err := conn.SetReadDeadline(time.Now().Add(wsPongWait)); err != nil {
		return err
	}

	conn.SetPongHandler(func(string) error {
		if rd := conn.SetReadDeadline(time.Now().Add(wsPongWait)); rd != nil {
			_ = rd
		}

		return nil
	})

	return nil
}

// writeWSAck sends an ack message to confirm the WebSocket stream is established.
func writeWSAck(conn *websocket.Conn, runID string) error {
	ack, err := json.Marshal(map[string]string{"event_type": "ack", "run_id": runID})
	if err != nil {
		return err
	}

	return writeWSMessage(conn, websocket.TextMessage, ack)
}

// runWSCommandPump reads incoming WebSocket messages and forwards them
// as Temporal signals (cancel, pause, resume). It exits when the connection
// closes or a cancel command is received.
func (r *V1) runWSCommandPump(conn *websocket.Conn, sessionID string, cancel func()) {
	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			return
		}

		if r.handleWSClientMessage(msg, sessionID, cancel) {
			return
		}
	}
}

// handleWSClientMessage parses a WebSocket message and acts on recognized commands.
// Returns true if the caller should stop processing (e.g., on cancel).
func (r *V1) handleWSClientMessage(msg []byte, sessionID string, cancel func()) bool {
	event, err := entity.UnmarshalEvent(msg)
	if err != nil || event == nil {
		return false
	}

	userCmd, ok := event.(*entity.UserCommandEvent)
	if !ok {
		return false
	}

	switch userCmd.Command {
	case "cancel":
		r.executeWSCancel(sessionID, cancel)

		return true
	case "pause", "resume":
		r.executeWSSignal(sessionID, userCmd.Command)

		return false
	}

	return false
}

// executeWSCancel cancels the workflow (Temporal) and the local context.
func (r *V1) executeWSCancel(sessionID string, cancel func()) {
	if r.cancelWorkflow != nil {
		workflowID := "agentfw-stream-" + sessionID
		if cwErr := r.cancelWorkflow(context.Background(), workflowID); cwErr != nil {
			_ = cwErr
		}
	}

	cancel()
}

// executeWSSignal sends a Temporal signal for pause/resume commands.
func (r *V1) executeWSSignal(sessionID, command string) {
	if r.signalWorkflow != nil {
		workflowID := "agentfw-stream-" + sessionID
		if swErr := r.signalWorkflow(context.Background(), workflowID, "agent-command", command); swErr != nil {
			_ = swErr
		}
	}
}

// runWSEventPump reads events from the subscription channel and writes them
// to the WebSocket connection until a terminal condition.
func (r *V1) runWSEventPump(streamCtx context.Context, conn *websocket.Conn, sub *stream.Subscription, doneCh <-chan struct{}) {
	for {
		select {
		case stored, ok := <-sub.C:
			if !ok {
				return
			}

			if r.writeWSEvent(conn, stored) {
				return
			}

		case <-doneCh:
			// Execution completed (or failed) without a terminal event.
			return

		case <-streamCtx.Done():
			return
		}
	}
}

// writeWSEvent converts and writes a stored event to the WebSocket connection.
// Returns true if the event is terminal or a write error occurred.
func (r *V1) writeWSEvent(conn *websocket.Conn, stored stream.StoredEvent) bool {
	payload, err := r.gateway.Convert(stored.Event)
	if err != nil {
		return false
	}

	if err := writeWSMessage(conn, websocket.TextMessage, payload); err != nil {
		return true
	}

	// Terminal events end the stream.
	switch stored.Event.(type) {
	case *entity.AgentRunFinishEvent, *entity.AgentErrorEvent:
		return true
	}

	return false
}

// writeWSMessage is a small helper to write a WebSocket message with a deadline.
func writeWSMessage(conn *websocket.Conn, messageType int, data []byte) error {
	if err := conn.SetWriteDeadline(time.Now().Add(wsWriteWait)); err != nil {
		return err
	}

	return conn.WriteMessage(messageType, data)
}
