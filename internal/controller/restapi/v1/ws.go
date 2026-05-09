package v1

import (
	"context"
	"encoding/json"
	"time"

	"github.com/TekkenSteve/GoAgent/internal/entity"
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
// same connection and forwarded to the use-case commandCh.
//
// Query parameters are identical to the SSE /agent/stream endpoint.
func (r *V1) ws(ctx *fiber.Ctx) error {
	message := ctx.Query("message")
	if message == "" {
		return errorResponse(ctx, 400, "missing message parameter")
	}
	model := ctx.Query("model")
	if model == "" {
		return errorResponse(ctx, 400, "missing model parameter")
	}
	runID := ctx.Query("run_id")
	if runID == "" {
		return errorResponse(ctx, 400, "missing run_id parameter")
	}
	systemPrompt := ctx.Query("system_prompt", "")
	lastEventID := ctx.Query("last_event_id", "0")

	// In non-Temporal mode, runID doubles as the sessionID.
	sessionID := runID
	lastSequence := parseLastSequence(lastEventID)

	// Stream context: cancelled when the client disconnects or sends cancel.
	streamCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req := entity.StreamRequest{
		RunID:        runID,
		SystemPrompt: systemPrompt,
		Message:      message,
		Config: entity.LLMConfig{
			Model: model,
		},
	}

	upgrader := websocket.FastHTTPUpgrader{
		CheckOrigin: func(_ *fasthttp.RequestCtx) bool { return true },
	}

	//nolint:contextcheck // the upgrader handler runs on a hijacked connection
	return upgrader.Upgrade(ctx.Context(), func(conn *websocket.Conn) {
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

		// EventStore writer: execution writes events → store → subscription → WS.
		writer := &eventStoreWriter{
			store:     r.eventStore,
			sessionID: sessionID,
			runID:     runID,
		}

		// Start execution in background. doneCh signals when it exits.
		doneCh := make(chan struct{})
		go func() {
			defer close(doneCh)
			if err := r.s.ExecuteStream(streamCtx, req, writer); err != nil {
				_ = writer.WriteEvent(streamCtx, entity.NewAgentErrorEvent("EXECUTION_ERROR", err.Error()))
			}
		}()

		// --- connection-level setup ---

		// Pong handler resets the read deadline to keep the connection alive.
		_ = conn.SetReadDeadline(time.Now().Add(wsPongWait))
		conn.SetPongHandler(func(string) error {
			_ = conn.SetReadDeadline(time.Now().Add(wsPongWait))
			return nil
		})

		// Send ack so the client knows the stream is established.
		ack, _ := json.Marshal(map[string]string{"event_type": "ack", "run_id": runID})
		_ = writeWSMessage(conn, websocket.TextMessage, ack)

		// --- command pump: client → commandCh / CancelWorkflow ---
		// Runs in a separate goroutine so commands are not blocked by event pumping.
		cmdErr := make(chan error, 1)
		go func() {
			defer close(cmdErr)
			for {
				_, msg, err := conn.ReadMessage()
				if err != nil {
					return
				}

				// Parse the client message as a StreamEvent (expect UserCommandEvent
				// or UserFeedbackEvent). Invalid messages are silently dropped.
				event, err := entity.UnmarshalEvent(msg)
				if err != nil || event == nil {
					continue
				}

				// Cancel: only supported with Temporal.
				if userCmd, ok := event.(*entity.UserCommandEvent); ok && userCmd.Command == "cancel" {
					if r.cancelWorkflow != nil {
						workflowID := "agentfw-stream-" + sessionID
						_ = r.cancelWorkflow(context.Background(), workflowID)
					}
					cancel()
					return
				}

				// Pause/resume: Temporal Signal only.
				if userCmd, ok := event.(*entity.UserCommandEvent); ok && (userCmd.Command == "pause" || userCmd.Command == "resume") {
					if r.signalWorkflow != nil {
						workflowID := "agentfw-stream-" + sessionID
						_ = r.signalWorkflow(context.Background(), workflowID, "agent-command", userCmd.Command)
					}
				}
			}
		}()

		// --- event pump: EventStore subscription → WebSocket ---
		for {
			select {
			case stored, ok := <-sub.C:
				if !ok {
					return
				}

				payload, err := r.gateway.Convert(stored.Event)
				if err != nil {
					continue
				}

				if err := writeWSMessage(conn, websocket.TextMessage, payload); err != nil {
					return
				}

				// Terminal events end the stream.
				switch stored.Event.(type) {
				case *entity.AgentRunFinishEvent, *entity.AgentErrorEvent:
					return
				}

			case <-doneCh:
				// Execution completed (or failed) without a terminal event.
				return

			case <-streamCtx.Done():
				return
			}
		}
	})
}

// writeWSMessage is a small helper to write a WebSocket message with a deadline.
func writeWSMessage(conn *websocket.Conn, messageType int, data []byte) error {
	if err := conn.SetWriteDeadline(time.Now().Add(wsWriteWait)); err != nil {
		return err
	}
	return conn.WriteMessage(messageType, data)
}
