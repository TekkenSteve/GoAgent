package v1

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"time"

	"github.com/TekkenSteve/GoAgent/internal/entity"
	"github.com/TekkenSteve/GoAgent/pkg/redis"
	"github.com/TekkenSteve/GoAgent/pkg/sse"
	"github.com/gofiber/fiber/v2"
)

const (
	eventStreamMaxLen  = 1000
	eventStreamTTL     = 1 * time.Hour
	pingInterval       = 30 * time.Second
	deadWorkerTimeout  = 2 * time.Minute
)

// stream handles GET /agent/stream.
// It starts agent execution, writes events to a Redis Stream,
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
	if lastEventID == "" {
		lastEventID = "0"
	}

	streamKey := "event:run:" + runID

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

	// Subscribe to the Redis Stream BEFORE starting execution.
	// XREAD from lastEventID means catch-up on existing events, then live stream.
	// This eliminates the race between execution writes and subscriber reads.
	sub := r.rdb.Hub().Subscribe(streamKey, lastEventID)
	defer sub.Close()

	// Create event writer that writes to the same Redis Stream
	writer := &runEventStreamWriter{
		rdb:       r.rdb,
		streamKey: streamKey,
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

		// Start execution in background — writes events to Redis
		// doneCh signals that the execution goroutine has exited
		doneCh := make(chan struct{})
		go func() {
			defer close(doneCh)
			if err := r.s.ExecuteStream(streamCtx, req, writer); err != nil {
				// Validation/config error — write as event
				writer.WriteEvent(streamCtx, entity.StreamEvent{
					Type:  entity.StreamEventError,
					Error: err.Error(),
				})
			}
		}()

		// Write ack event to confirm stream is established
		ackData, _ := json.Marshal(map[string]string{"type": "ack", "run_id": runID})
		sse.WriteEvent(bw, sse.Event{Data: string(ackData)})
		bw.Flush()

		// Ping ticker keeps the connection alive.
		// If the stream is idle, we send a ping every 30s so
		// reverse proxies and browsers don't drop the connection.
		pingTicker := time.NewTicker(pingInterval)
		defer pingTicker.Stop()

		// Track last activity for dead worker detection
		lastActivity := time.Now()

		// Stream events from Redis via StreamHub
		for {
			select {
			case entry, ok := <-sub.C:
				if !ok {
					return
				}
				payload := entry.Values["payload"]
				if payload == "" {
					continue
				}

				lastActivity = time.Now()

				sse.WriteEvent(bw, sse.Event{Data: payload})
				if flushErr := bw.Flush(); flushErr != nil {
					return
				}

				// Check for terminal event
				var evt entity.StreamEvent
				if json.Unmarshal([]byte(payload), &evt) == nil {
					if evt.Type == entity.StreamEventError || evt.Type == entity.StreamEventDone {
						return
					}
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
					// Check if execution goroutine is still running
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

// runEventStreamWriter implements usecase.StreamEventWriter by writing to a Redis Stream.
type runEventStreamWriter struct {
	rdb       *redis.Redis
	streamKey string
}

func (w *runEventStreamWriter) WriteEvent(ctx context.Context, event entity.StreamEvent) error {
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	_, err = w.rdb.StreamAdd(ctx, w.streamKey, map[string]any{"payload": string(data)}, eventStreamMaxLen)
	if err != nil {
		return err
	}
	// Best-effort TTL so finished run streams expire
	w.rdb.Expire(ctx, w.streamKey, eventStreamTTL)
	return nil
}
