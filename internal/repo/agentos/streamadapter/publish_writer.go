package streamadapter

import (
	"context"
	"strconv"
	"strings"

	"github.com/TekkenSteve/GoAgent/agentos/stream"
	"github.com/TekkenSteve/GoAgent/internal/entity"
	"github.com/TekkenSteve/GoAgent/pkg/logger"
)

// PublishWriter adapts the runtime's StreamEventWriter contract to the data
// plane: it maps each entity event to AG-UI, makes every milestone durable
// before anything is announced, and publishes through the engine-neutral
// stream.Publisher, synthesizing message open/close around the raw token
// deltas.
//
// Runtime events carry no thread/run identity of their own — the Event Store
// stamps SessionID/RunID at persist time ([event_builder.go] NewBase). The
// writer captures the run's scope from the activity that constructs it and
// stamps it on every mapped and synthesized event, so the AG-UI timeline is
// complete even though the entity event was a bare delta.
//
// Two channels, two failure policies. Milestones are facts: when a recorder
// is attached, each one is appended (with its fact-log publication intent)
// before the bus publish, and a persist failure is returned so the enclosing
// activity fails and Temporal retries it — the fact path never depended on
// the live channel. The bus publish stays fail-open by design: a bus hiccup
// must never break the run, and the milestone is already durable.
type PublishWriter struct {
	pub    stream.Publisher
	handle *stream.Handle
	logger logger.Interface
	facts  *MilestoneRecorder

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

	// textBuf accumulates the open text message's deltas so its closing
	// TEXT_MESSAGE_END fact can carry the completed message — the timeline is
	// the durable record of what was said, while the deltas themselves never
	// become durable. Reset with every message.
	textBuf strings.Builder
}

// NewPublishWriter creates a writer for one run's timeline. The logger may be
// nil; bus-publish failures are then dropped silently.
func NewPublishWriter(pub stream.Publisher, handle *stream.Handle, threadID, runID string, l logger.Interface) *PublishWriter {
	return &PublishWriter{
		pub:      pub,
		handle:   handle,
		logger:   l,
		threadID: threadID,
		runID:    runID,
	}
}

// WithFacts attaches the milestone recorder that makes each milestone durable
// before it is announced on the bus. A writer without one publishes only:
// deployments without a run-event store keep the pure mirror behavior.
func (w *PublishWriter) WithFacts(recorder *MilestoneRecorder) *PublishWriter {
	w.facts = recorder

	return w
}

// WriteEvent maps one runtime event to AG-UI, makes any milestone durable,
// and publishes it to the bus. Content deltas open their message on first
// delivery; any non-content event closes whichever messages are still open.
//
// A returned error means a milestone could not be made durable — the caller
// must fail the run so the platform retries the fact. Bus-publish failures
// stay internal (fail-open): the milestone is already durable and the bus is
// only the live mirror.
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
			if err := w.emit(ctx, stream.NewEvent(stream.EventTextMessageStart).
				SetThread(w.runID, w.threadID).SetMessage(w.openText)); err != nil {
				return err
			}
		}

		ev.MessageID = w.openText
		w.accumulateText(ev)
	case stream.EventReasoningMessageContent:
		if w.openReason == "" {
			w.openReason = w.nextMessageID()
			if err := w.emit(ctx, stream.NewEvent(stream.EventReasoningMessageStart).
				SetThread(w.runID, w.threadID).SetMessage(w.openReason)); err != nil {
				return err
			}
		}

		ev.MessageID = w.openReason
	default:
		if err := w.closeOpenMessages(ctx); err != nil {
			return err
		}
	}

	return w.emit(ctx, ev)
}

// Flush closes whichever messages are still open. The enclosing activity calls
// it when its stream ends, so a text-only round (no following non-content
// event to close the message) still terminates cleanly on the timeline and its
// TEXT_MESSAGE_END fact becomes durable. The error semantics match WriteEvent.
func (w *PublishWriter) Flush(ctx context.Context) error {
	return w.closeOpenMessages(ctx)
}

// closeOpenMessages closes whichever text/reasoning messages are open before a
// non-content event lands, so the timeline never ends a message mid-stream.
//
// The TEXT_MESSAGE_END fact and its bus copy differ on purpose: the fact
// carries the accumulated message text (symmetric with TOOL_CALL_RESULT
// carrying the full result), because the durable timeline must answer "what
// was said", not just "a message completed". The bus copy stays byte-free —
// the live channel already carried the deltas, and re-sending the full text
// would double its payload for nothing.
func (w *PublishWriter) closeOpenMessages(ctx context.Context) error {
	if w.openText != "" {
		content := w.textBuf.String()
		w.textBuf.Reset()

		if err := w.recordMilestone(ctx, stream.NewEvent(stream.EventTextMessageEnd).
			SetThread(w.runID, w.threadID).
			SetMessage(w.openText).
			Set(stream.FieldContent, content)); err != nil {
			return err
		}

		w.publishFailOpen(ctx, stream.NewEvent(stream.EventTextMessageEnd).
			SetThread(w.runID, w.threadID).
			SetMessage(w.openText))

		w.openText = ""
	}

	if w.openReason != "" {
		if err := w.emit(ctx, stream.NewEvent(stream.EventReasoningMessageEnd).
			SetThread(w.runID, w.threadID).SetMessage(w.openReason)); err != nil {
			return err
		}

		w.openReason = ""
	}

	return nil
}

// accumulateText collects one text delta into the open message's buffer.
func (w *PublishWriter) accumulateText(ev *stream.Event) {
	if delta, ok := ev.Payload[stream.FieldDelta].(string); ok {
		w.textBuf.WriteString(delta)
	}
}

// nextMessageID synthesizes a stable message id for an opened message.
func (w *PublishWriter) nextMessageID() string {
	w.msgSeq++

	return "m-" + strconv.Itoa(w.msgSeq)
}

// emit makes a milestone durable, then announces it on the bus. A returned
// error means the fact could not be persisted — the caller must fail the run
// so the platform retries it. Bus-publish failures stay internal (fail-open):
// the fact is already durable and the bus is only the live mirror.
func (w *PublishWriter) emit(ctx context.Context, ev *stream.Event) error {
	if err := w.recordMilestone(ctx, ev); err != nil {
		return err
	}

	w.publishFailOpen(ctx, ev)

	return nil
}

// recordMilestone appends one milestone fact before it is announced. Without
// a recorder (deployments without a run-event store) the writer keeps the pure
// mirror behavior; non-milestone events are transport-only and pass through.
func (w *PublishWriter) recordMilestone(ctx context.Context, ev *stream.Event) error {
	if w.facts == nil || !stream.IsMilestone(ev.Type) {
		return nil
	}

	if err := w.facts.Record(ctx, w.handle, ev); err != nil {
		return err
	}

	return nil
}

// publishFailOpen sends one event to the bus. Transport errors are logged and
// dropped: a bus hiccup must never break the run.
func (w *PublishWriter) publishFailOpen(ctx context.Context, ev *stream.Event) {
	if err := w.pub.Publish(ctx, w.handle, ev); err != nil {
		w.warnf("streamadapter: publish %s: %v (fail-open)", ev.Type, err)
	}
}

func (w *PublishWriter) warnf(format string, args ...any) {
	if w.logger != nil {
		w.logger.Warn(format, args...)
	}
}
