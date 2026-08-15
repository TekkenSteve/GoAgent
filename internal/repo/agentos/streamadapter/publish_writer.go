package streamadapter

import (
	"context"
	"strconv"

	"github.com/TekkenSteve/GoAgent/agentos/stream"
	"github.com/TekkenSteve/GoAgent/internal/entity"
	"github.com/TekkenSteve/GoAgent/pkg/logger"
)

// PublishWriter adapts the runtime's StreamEventWriter contract to the data
// plane: it maps each entity event to AG-UI and publishes it through the
// engine-neutral stream.Publisher, synthesizing message open/close around the
// raw token deltas.
//
// Runtime events carry no thread/run identity of their own — the Event Store
// stamps SessionID/RunID at persist time ([event_builder.go] NewBase). The
// writer captures the run's scope from the activity that constructs it and
// stamps it on every mapped and synthesized event, so the AG-UI timeline is
// complete even though the entity event was a bare delta.
//
// Publishing is fail-open by design — a bus hiccup must never break the run.
// Mapping or publish errors are logged and swallowed, mirroring the billing
// warn idiom in the activities. The authoritative projection (Postgres) and
// the Redis event store are unaffected: this writer is an additive mirror on
// the data plane, not a gate.
type PublishWriter struct {
	pub    stream.Publisher
	handle *stream.Handle
	logger logger.Interface

	// threadID / runID are the run's identity on the data plane (the frontend
	// session and the run), captured at construction because the entity events
	// this writer maps do not carry them.
	threadID string
	runID    string

	// openText / openReason track the in-progress message ids so token deltas
	// (which carry no message identity in the runtime model) can be wrapped in
	// synthesized START/END pairs. "" means no message is open.
	openText   string
	openReason string
	msgSeq     int
}

// NewPublishWriter creates a writer for one run's timeline. The logger may be
// nil; publishing failures are then dropped silently.
func NewPublishWriter(pub stream.Publisher, handle *stream.Handle, threadID, runID string, l logger.Interface) *PublishWriter {
	return &PublishWriter{
		pub:      pub,
		handle:   handle,
		logger:   l,
		threadID: threadID,
		runID:    runID,
	}
}

// WriteEvent maps one runtime event to AG-UI and publishes it. Content deltas
// open their message on first delivery; any non-content event closes whichever
// messages are still open. The method always returns nil: the run never
// depends on the data plane being up.
func (w *PublishWriter) WriteEvent(ctx context.Context, event entity.StreamEvent) error {
	ev, err := MapEvent(event)
	if err != nil {
		w.warnf("streamadapter: %v (dropped)", err)

		return nil
	}

	ev.SetThread(w.runID, w.threadID)

	// The two content kinds get message synthesis; every other AG-UI type (the
	// milestone and custom events) just closes whatever is open. A typed switch
	// would have to enumerate the whole vocabulary, so exhaustive is opted out
	// for this deliberately-partial dispatch.
	//exhaustive:ignore
	switch ev.Type {
	case stream.EventTextMessageContent:
		if w.openText == "" {
			w.openText = w.nextMessageID()
			w.publish(ctx, stream.NewEvent(stream.EventTextMessageStart).
				SetThread(w.runID, w.threadID).SetMessage(w.openText))
		}

		ev.MessageID = w.openText
	case stream.EventReasoningMessageContent:
		if w.openReason == "" {
			w.openReason = w.nextMessageID()
			w.publish(ctx, stream.NewEvent(stream.EventReasoningMessageStart).
				SetThread(w.runID, w.threadID).SetMessage(w.openReason))
		}

		ev.MessageID = w.openReason
	default:
		w.closeOpenMessages(ctx)
	}

	w.publish(ctx, ev)

	return nil
}

// Flush closes whichever messages are still open. The enclosing activity calls
// it when its stream ends, so a text-only round (no following non-content
// event to close the message) still terminates cleanly on the timeline and its
// TEXT_MESSAGE_END milestone reaches the projection.
func (w *PublishWriter) Flush(ctx context.Context) {
	w.closeOpenMessages(ctx)
}

// closeOpenMessages closes whichever text/reasoning messages are open before a
// non-content event lands, so the timeline never ends a message mid-stream.
func (w *PublishWriter) closeOpenMessages(ctx context.Context) {
	if w.openText != "" {
		w.publish(ctx, stream.NewEvent(stream.EventTextMessageEnd).
			SetThread(w.runID, w.threadID).SetMessage(w.openText))
		w.openText = ""
	}

	if w.openReason != "" {
		w.publish(ctx, stream.NewEvent(stream.EventReasoningMessageEnd).
			SetThread(w.runID, w.threadID).SetMessage(w.openReason))
		w.openReason = ""
	}
}

// nextMessageID synthesizes a stable message id for an opened message.
func (w *PublishWriter) nextMessageID() string {
	w.msgSeq++

	return "m-" + strconv.Itoa(w.msgSeq)
}

// publish sends one event to the bus, fail-open on transport errors.
func (w *PublishWriter) publish(ctx context.Context, ev *stream.Event) {
	if err := w.pub.Publish(ctx, w.handle, ev); err != nil {
		w.warnf("streamadapter: publish %s: %v (fail-open)", ev.Type, err)
	}
}

func (w *PublishWriter) warnf(format string, args ...any) {
	if w.logger != nil {
		w.logger.Warn(format, args...)
	}
}
