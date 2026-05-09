package v1

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/TekkenSteve/GoAgent/internal/agentfw/stream"
	"github.com/TekkenSteve/GoAgent/internal/entity"
	"github.com/TekkenSteve/GoAgent/pkg/sse"
	"github.com/gofiber/fiber/v2"
)

const (
	pingInterval      = 30 * time.Second
	deadWorkerTimeout = 2 * time.Minute
)

// stream handles GET /agent/stream.
// It starts agent execution, writes events to the EventStore,
// and streams them back to the client via Server-Sent Events.
func (r *V1) stream(ctx *fiber.Ctx) error {
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

	// Parse last event sequence. Supports both plain integer and "seq-0" formats.
	lastSequence := parseLastSequence(lastEventID)

	// Create cancel context to stop the stream when client disconnects
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

	// Subscribe to the EventStore BEFORE starting execution.
	// lastSequence controls catch-up: 0 means live-only, >0 replays from after that seq.
	sub, err := r.subscriber.Subscribe(streamCtx, sessionID, lastSequence)
	if err != nil {
		return errorResponse(ctx, 500, "failed to subscribe to event stream")
	}
	defer sub.Close()

	// Create event writer that writes to the EventStore.
	writer := &eventStoreWriter{
		store:     r.eventStore,
		sessionID: sessionID,
		runID:     runID,
	}

	// Hijack the connection for SSE (fasthttp does not support response flushing)
	ctx.Context().Hijack(func(conn net.Conn) {
		defer conn.Close()
		defer cancel()

		bw := bufio.NewWriter(conn)

		// Write HTTP response headers manually
		bw.WriteString("HTTP/1.1 200 OK\r\n")
		bw.WriteString("Content-Type: text/event-stream\r\n")
		bw.WriteString("Cache-Control: no-cache\r\n")
		bw.WriteString("Connection: keep-alive\r\n")
		bw.WriteString("Access-Control-Allow-Origin: *\r\n")
		bw.WriteString("\r\n")
		bw.Flush()

		// Start execution in background — writes events to EventStore.
		// doneCh signals that the execution goroutine has exited.
		doneCh := make(chan struct{})
		go func() {
			defer close(doneCh)
			if err := r.s.ExecuteStream(streamCtx, req, writer); err != nil {
				// Validation/config error — write as event
				writer.WriteEvent(streamCtx, entity.NewAgentErrorEvent("EXECUTION_ERROR", err.Error()))
			}
		}()

		// Write ack event to confirm stream is established
		ackData, _ := json.Marshal(map[string]string{"type": "ack", "run_id": runID})
		sse.WriteEvent(bw, sse.Event{Data: string(ackData)})
		bw.Flush()

		// Ping ticker keeps the connection alive.
		pingTicker := time.NewTicker(pingInterval)
		defer pingTicker.Stop()

		// Track last activity for dead worker detection
		lastActivity := time.Now()

		for {
			select {
			case stored, ok := <-sub.C:
				if !ok {
					return
				}

				lastActivity = time.Now()

				// Serialize event via gateway (SSEGateway = direct JSON)
				payload, err := r.gateway.Convert(stored.Event)
				if err != nil {
					continue
				}

				sse.WriteEvent(bw, sse.Event{Data: string(payload)})
				if flushErr := bw.Flush(); flushErr != nil {
					return
				}

				// Check for terminal event via type assertion
				switch stored.Event.(type) {
				case *entity.AgentRunFinishEvent, *entity.AgentErrorEvent:
					return
				}

			case <-pingTicker.C:
				// Send ping to keep connection alive
				sse.WriteEvent(bw, sse.Event{Data: `{"type":"ping"}`})
				if flushErr := bw.Flush(); flushErr != nil {
					return
				}

				// Dead worker detection: if no events received for too long,
				// the execution goroutine may have crashed silently.
				if time.Since(lastActivity) > deadWorkerTimeout {
					select {
					case <-doneCh:
						// Execution exited without terminal event — write error
						sse.WriteEvent(bw, sse.Event{Data: `{"type":"error","error":"execution terminated unexpectedly"}`})
						bw.Flush()
						return
					default:
						// Still running, continue waiting
					}
				}

			case <-streamCtx.Done():
				return
			}
		}
	})

	return nil
}

// eventStoreWriter implements usecase.StreamEventWriter by appending to EventStore.
type eventStoreWriter struct {
	store     stream.EventStore
	sessionID string
	runID     string
}

func (w *eventStoreWriter) WriteEvent(ctx context.Context, event entity.StreamEvent) error {
	_, err := w.store.Append(ctx, w.sessionID, w.runID, event)
	return err
}

// parseLastSequence converts the last_event_id query parameter to a sequence number.
// Supports formats: "" / "0" / "$" → 0 (start from beginning),
// "123" → 123, "123-0" → 123 (Redis Stream ID format).
func parseLastSequence(s string) int64 {
	s = strings.TrimSpace(s)
	if s == "" || s == "0" || s == "$" {
		return 0
	}
	// Try plain integer first
	if seq, err := strconv.ParseInt(s, 10, 64); err == nil {
		return seq
	}
	// Try "seq-0" format (Redis Stream ID)
	parts := strings.SplitN(s, "-", 2)
	if len(parts) > 0 {
		if seq, err := strconv.ParseInt(parts[0], 10, 64); err == nil {
			return seq
		}
	}
	return 0
}
