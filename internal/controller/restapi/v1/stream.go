package v1

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/TekkenSteve/GoAgent/agentfw/stream"
	"github.com/TekkenSteve/GoAgent/entity"
	"github.com/TekkenSteve/GoAgent/pkg/sse"
	"github.com/gofiber/fiber/v2"
)

const splitNMaxParts = 2

const (
	pingInterval      = 30 * time.Second
	deadWorkerTimeout = 2 * time.Minute
)

// stream handles GET /agent/stream.
// It starts agent execution, writes events to the EventStore,
// and streams them back to the client via Server-Sent Events.
func (r *V1) stream(ctx *fiber.Ctx) error {
	params, err := parseStreamQueryParams(ctx)
	if err != nil {
		return err
	}

	// In non-Temporal mode, runID doubles as the sessionID.
	sessionID := params.runID

	// Create cancel context to stop the stream when client disconnects
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

	// Subscribe to the EventStore BEFORE starting execution.
	// lastSequence controls catch-up: 0 means live-only, >0 replays from after that seq.
	sub, err := r.subscriber.Subscribe(streamCtx, sessionID, params.lastSequence)
	if err != nil {
		return errorResponse(ctx, fiber.StatusInternalServerError, "failed to subscribe to event stream")
	}
	defer sub.Close()

	// Create event writer that writes to the EventStore.
	writer := &eventStoreWriter{
		store:     r.eventStore,
		sessionID: sessionID,
		runID:     params.runID,
	}

	// Hijack the connection for SSE (fasthttp does not support response flushing)
	r.hijackForSSE(streamCtx, ctx, cancel, &req, writer, sub)

	return nil
}

// parseStreamQueryParams extracts and validates SSE stream query parameters.
type streamQueryParams struct {
	message      string
	model        string
	runID        string
	systemPrompt string
	lastSequence int64
}

func parseStreamQueryParams(ctx *fiber.Ctx) (*streamQueryParams, error) {
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

	return &streamQueryParams{
		message:      message,
		model:        model,
		runID:        runID,
		systemPrompt: systemPrompt,
		lastSequence: parseLastSequence(lastEventID),
	}, nil
}

// hijackForSSE hijacks the connection for SSE streaming.
func (r *V1) hijackForSSE(streamCtx context.Context, ctx *fiber.Ctx, cancel func(), req *entity.StreamRequest, writer *eventStoreWriter, sub *stream.Subscription) {
	ctx.Context().Hijack(func(conn net.Conn) {
		defer conn.Close()
		defer cancel()

		bw := bufio.NewWriter(conn)

		if err := writeSSEHeaders(bw); err != nil {
			return
		}

		// Start execution in background — writes events to EventStore.
		// doneCh signals that the execution goroutine has exited.
		doneCh := make(chan struct{})

		go r.startStreamExecution(streamCtx, req, writer, doneCh)

		if err := writeAckEvent(bw, req.RunID); err != nil {
			return
		}

		// Ping ticker keeps the connection alive.
		pingTicker := time.NewTicker(pingInterval)
		r.runSSEEventLoop(streamCtx, bw, sub, pingTicker, doneCh)
	})
}

// writeSSEHeaders writes HTTP response headers for a Server-Sent Events stream.
func writeSSEHeaders(bw *bufio.Writer) error {
	headers := []string{
		"HTTP/1.1 200 OK\r\n",
		"Content-Type: text/event-stream\r\n",
		"Cache-Control: no-cache\r\n",
		"Connection: keep-alive\r\n",
		"Access-Control-Allow-Origin: *\r\n",
		"\r\n",
	}
	for _, h := range headers {
		if _, err := bw.WriteString(h); err != nil {
			return err
		}
	}

	return bw.Flush()
}

// startStreamExecution runs the agent execution in the background.
// It writes events to the EventStore and signals completion via doneCh.
func (r *V1) startStreamExecution(ctx context.Context, req *entity.StreamRequest, writer *eventStoreWriter, doneCh chan struct{}) {
	defer close(doneCh)

	if err := r.s.ExecuteStream(ctx, req, writer); err != nil {
		if we := writer.WriteEvent(ctx, entity.NewAgentErrorEvent("EXECUTION_ERROR", err.Error())); we != nil {
			_ = we
		}
	}
}

// writeAckEvent writes the ack event to confirm the stream is established.
func writeAckEvent(bw *bufio.Writer, runID string) error {
	ackData, err := json.Marshal(map[string]string{"type": "ack", "run_id": runID})
	if err != nil {
		return err
	}

	if err := sse.WriteEvent(bw, sse.Event{Data: string(ackData)}); err != nil {
		return err
	}

	return bw.Flush()
}

// handleStreamSubEvent processes a single event from the subscription channel.
// Returns true if the event is terminal (stream should end).
func (r *V1) handleStreamSubEvent(bw *bufio.Writer, stored stream.StoredEvent) (bool, error) {
	payload, err := r.gateway.Convert(stored.Event)
	if err != nil {
		return false, nil
	}

	if err := sse.WriteEvent(bw, sse.Event{Data: string(payload)}); err != nil {
		return false, err
	}

	if err := bw.Flush(); err != nil {
		return false, err
	}

	switch stored.Event.(type) {
	case *entity.AgentRunFinishEvent, *entity.AgentErrorEvent:
		return true, nil
	}

	return false, nil
}

// checkDeadWorker checks if the execution goroutine has crashed silently.
// Returns true if the connection should be closed.
func checkDeadWorker(lastActivity time.Time, doneCh chan struct{}, bw *bufio.Writer) bool {
	if time.Since(lastActivity) <= deadWorkerTimeout {
		return false
	}

	select {
	case <-doneCh:
		if wErr := sse.WriteEvent(bw, sse.Event{Data: `{"type":"error","error":"execution terminated unexpectedly"}`}); wErr != nil {
			_ = wErr
		}

		if fErr := bw.Flush(); fErr != nil {
			_ = fErr
		}

		return true
	default:
		return false
	}
}

// runSSEEventLoop runs the main event loop, reading from the subscription
// channel and writing SSE events to the client until a terminal condition.
func (r *V1) runSSEEventLoop(streamCtx context.Context, bw *bufio.Writer, sub *stream.Subscription, pingTicker *time.Ticker, doneCh chan struct{}) {
	defer pingTicker.Stop()

	lastActivity := time.Now()

	for {
		select {
		case stored, ok := <-sub.C:
			if !ok {
				return
			}

			lastActivity = time.Now()

			terminal, err := r.handleStreamSubEvent(bw, stored)
			if err != nil || terminal {
				return
			}

		case <-pingTicker.C:
			if r.handleSSEPingTick(bw, lastActivity, doneCh) {
				return
			}

		case <-streamCtx.Done():
			return
		}
	}
}

// handleSSEPingTick sends a ping event and checks for dead worker.
// Returns true if the event loop should stop.
func (r *V1) handleSSEPingTick(bw *bufio.Writer, lastActivity time.Time, doneCh chan struct{}) bool {
	if err := sse.WriteEvent(bw, sse.Event{Data: `{"type":"ping"}`}); err != nil {
		return true
	}

	if err := bw.Flush(); err != nil {
		return true
	}

	return checkDeadWorker(lastActivity, doneCh, bw)
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
	parts := strings.SplitN(s, "-", splitNMaxParts)
	if len(parts) > 0 {
		if seq, err := strconv.ParseInt(parts[0], 10, 64); err == nil {
			return seq
		}
	}

	return 0
}
