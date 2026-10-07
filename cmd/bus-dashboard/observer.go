package main

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	"github.com/TekkenSteve/GoAgent/agentos/stream"
	"github.com/TekkenSteve/GoAgent/internal/repo/agentos/streamadapter"
	"github.com/TekkenSteve/GoAgent/internal/repo/persistent"
)

// Live lifecycle states surfaced in the dashboard.
const (
	stateOpen     = "open"
	stateRunning  = "running"
	stateFinished = "finished"
	stateError    = "error"
	stateCanceled = "canceled"
)

// Projection verification and UI pacing constants.
const (
	projectionPollInterval = 100 * time.Millisecond
	projectionCheckTimeout = 5 * time.Second
	waitPollInterval       = 10 * time.Millisecond
	maxViolationsPerRun    = 5
)

// Observer subscribes to every run's data-plane channel and reduces the stream
// into a live view while asserting the data-plane invariants as events land.
// It is the read-side twin of the generator: what a backend publishes, the
// observer verifies a frontend would receive in order, closed, and without
// leaks. With a run-event repo wired it additionally verifies that the durable
// projection converges with what the bus delivered.
type Observer struct {
	ctx       context.Context
	sub       stream.Subscriber
	repo      *persistent.AgentOSRunEventRepo // optional writer→PG projection check
	transport string
	bcast     *broadcaster
	startedAt time.Time

	mu   sync.Mutex
	runs map[string]*runView
	subs map[string]*stream.Subscription
}

// NewObserver wires the read side. A nil repo skips the projection check.
func NewObserver(ctx context.Context, sub stream.Subscriber, repo *persistent.AgentOSRunEventRepo, transport string) *Observer {
	return &Observer{
		ctx:       ctx,
		sub:       sub,
		repo:      repo,
		transport: transport,
		bcast:     newBroadcaster(),
		startedAt: time.Now(),
		runs:      make(map[string]*runView),
		subs:      make(map[string]*stream.Subscription),
	}
}

// runView is the observer's reduction of one run's channel. The boolean
// fields are per-assertion pass flags; violations collects the messages.
type runView struct {
	runID        string
	threadID     string
	tenant       string
	pathName     string
	state        string
	wantTerminal stream.EventType
	terminal     stream.EventType

	eventCount int
	tokenCount int
	lastSeq    int64
	lastEvent  string
	startedAt  time.Time
	terminalAt time.Time

	started  bool
	done     bool
	openMsgs int

	seqOK      bool
	msgOK      bool
	terminalOK bool
	projOK     *bool

	violationCount int
	violations     []string
}

// TrackRun subscribes to the run's channel before the generator publishes
// anything, so the full timeline is captured live. The subscription is
// drained by a dedicated goroutine until the run ends or the observer stops.
func (o *Observer) TrackRun(ctx context.Context, out outcome) error {
	handle := streamadapter.HandleForRun(out.tenant, out.runID)

	sub, err := o.sub.Subscribe(ctx, handle, stream.LiveOnly)
	if err != nil {
		return fmt.Errorf("subscribe %s: %w", handle.Channel, err)
	}

	o.registerRun(out)

	o.mu.Lock()
	o.subs[out.runID] = sub
	o.mu.Unlock()

	o.changed()

	go o.drain(ctx, out.runID, sub)

	return nil
}

// registerRun creates and stores a run's view under the observer lock.
func (o *Observer) registerRun(out outcome) {
	rv := &runView{
		runID:        out.runID,
		threadID:     out.threadID,
		tenant:       out.tenant,
		pathName:     out.path.runPathName(),
		state:        stateOpen,
		wantTerminal: terminalFor(out.terminal),
		startedAt:    time.Now(),
		seqOK:        true,
		msgOK:        true,
		terminalOK:   true,
	}

	o.mu.Lock()
	o.runs[out.runID] = rv
	o.mu.Unlock()
}

// drain forwards a run's channel into the assertion pipeline until the
// subscription is closed or the observer's context ends.
func (o *Observer) drain(ctx context.Context, runID string, sub *stream.Subscription) {
	defer sub.Close()

	for {
		select {
		case <-ctx.Done():
			return
		case stored, ok := <-sub.C:
			if !ok {
				return
			}

			o.observe(runID, &stored)
		}
	}
}

// observe reduces one stored event into its run's view, running the three
// always-on assertions: sequence monotonicity, terminal closure, message
// pairing.
func (o *Observer) observe(runID string, stored *stream.StoredEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()

	rv := o.runs[runID]
	if rv == nil {
		return
	}

	ev := &stored.Event
	rv.eventCount++

	// Sequence monotonicity: the bus must assign strictly increasing sequence
	// numbers per channel, so a replaying or out-of-order stream is a leak.
	if stored.Sequence <= rv.lastSeq {
		rv.seqOK = false
		o.violate(rv, fmt.Sprintf("sequence %d is not after %d", stored.Sequence, rv.lastSeq))
	}

	rv.lastSeq = stored.Sequence

	// Terminal closure: nothing may arrive after a terminal milestone.
	if rv.done {
		rv.terminalOK = false
		o.violate(rv, fmt.Sprintf("event %s arrived after terminal %s", ev.Type, rv.terminal))

		return
	}

	o.observeLifecycle(rv, ev)
	o.observeMessage(rv, ev)

	rv.lastEvent = string(ev.Type)

	o.changed()
}

// lifecycleKind classifies the AG-UI lifecycle events the observer tracks, so
// the assertion switch below is exhaustive over a small enum instead of the
// whole wire vocabulary.
type lifecycleKind uint8

const (
	lifecycleNone lifecycleKind = iota
	lifecycleStart
	lifecycleFinished
	lifecycleError
	lifecycleCanceled
)

func classifyLifecycle(typ stream.EventType) lifecycleKind {
	if typ == stream.EventRunStarted {
		return lifecycleStart
	}

	if typ == stream.EventRunFinished {
		return lifecycleFinished
	}

	if typ == stream.EventRunError {
		return lifecycleError
	}

	if typ == stream.EventRunCanceled {
		return lifecycleCanceled
	}

	return lifecycleNone
}

// observeLifecycle tracks the RUN_STARTED → terminal transition.
func (o *Observer) observeLifecycle(rv *runView, ev *stream.Event) {
	switch classifyLifecycle(ev.Type) {
	case lifecycleStart:
		if rv.started {
			rv.terminalOK = false
			o.violate(rv, "duplicate RUN_STARTED")
		}

		rv.started = true
		rv.state = stateRunning
	case lifecycleFinished:
		o.closeRun(rv, stream.EventRunFinished, stateFinished)
	case lifecycleError:
		o.closeRun(rv, stream.EventRunError, stateError)
	case lifecycleCanceled:
		o.closeRun(rv, stream.EventRunCanceled, stateCanceled)
	case lifecycleNone:
	}
}

// messageKind classifies the AG-UI content events the message-pairing
// assertion tracks.
type messageKind uint8

const (
	messageOther messageKind = iota
	messageStart
	messageContent
	messageEnd
)

func classifyMessage(typ stream.EventType) messageKind {
	if typ == stream.EventTextMessageStart || typ == stream.EventReasoningMessageStart {
		return messageStart
	}

	if typ == stream.EventTextMessageContent || typ == stream.EventReasoningMessageContent {
		return messageContent
	}

	if typ == stream.EventTextMessageEnd || typ == stream.EventReasoningMessageEnd {
		return messageEnd
	}

	return messageOther
}

// observeMessage checks that content deltas ride an open message and every
// message start is paired with an end.
func (o *Observer) observeMessage(rv *runView, ev *stream.Event) {
	switch classifyMessage(ev.Type) {
	case messageStart:
		rv.openMsgs++
	case messageContent:
		if rv.openMsgs == 0 {
			rv.msgOK = false
			o.violate(rv, string(ev.Type)+" content with no open message")

			return
		}

		rv.tokenCount += tokenCountFor(ev)
	case messageEnd:
		if rv.openMsgs == 0 {
			rv.msgOK = false
			o.violate(rv, string(ev.Type)+" with no open message")

			return
		}

		rv.openMsgs--
	case messageOther:
	}
}

// closeRun records a terminal milestone and checks the run's closure
// invariants: exactly one terminal, matching the intended outcome, with every
// message closed before the timeline ends.
func (o *Observer) closeRun(rv *runView, terminal stream.EventType, state string) {
	rv.done = true
	rv.terminal = terminal
	rv.state = state
	rv.terminalAt = time.Now()

	if !rv.started {
		rv.terminalOK = false
		o.violate(rv, string(terminal)+" arrived before RUN_STARTED")
	}

	if terminal != rv.wantTerminal {
		rv.terminalOK = false
		o.violate(rv, fmt.Sprintf("terminal %s does not match expected %s", terminal, rv.wantTerminal))
	}

	if rv.openMsgs != 0 {
		rv.msgOK = false
		o.violate(rv, fmt.Sprintf("%d message(s) left open at terminal", rv.openMsgs))
	}

	if o.repo != nil {
		o.checkProjection(rv.runID, terminal)
	}
}

// checkProjection verifies asynchronously that the terminal milestone reached
// Postgres, so the control plane's durable view converges with the live
// stream. The writer makes each milestone durable before it publishes, so a
// converged row is the stronger fact-first claim: the fact exists, and the
// bus merely mirrors it.
func (o *Observer) checkProjection(runID string, observed stream.EventType) {
	want, ok := terminalCoreType(observed)
	if !ok {
		return
	}

	ctx := o.ctx

	go func() {
		deadline := time.Now().Add(projectionCheckTimeout)

		for {
			events, err := o.repo.ListRunEvents(ctx, runID, 0, projectionPollLimit)
			if err == nil && projectionHasTerminal(events, want) {
				o.markProjected(runID, true)

				return
			}

			if time.Now().After(deadline) {
				o.markProjected(runID, false)

				return
			}

			time.Sleep(projectionPollInterval)
		}
	}()
}

// markProjected records the writer→PG projection verdict for a run.
func (o *Observer) markProjected(runID string, ok bool) {
	o.mu.Lock()
	defer o.mu.Unlock()

	rv := o.runs[runID]
	if rv == nil {
		return
	}

	rv.projOK = &ok

	if !ok {
		o.violate(rv, "projected milestone did not converge with the bus within timeout")
	}

	o.changed()
}

// violate appends a violation message, capping the detail list while keeping
// the running count exact for the stats panel.
func (o *Observer) violate(rv *runView, msg string) {
	rv.violationCount++

	if len(rv.violations) < maxViolationsPerRun {
		rv.violations = append(rv.violations, msg)
	}
}

// changed wakes every SSE subscriber that a state snapshot is ready.
func (o *Observer) changed() {
	o.bcast.notify()
}

// waitTerminal blocks until the run reaches a terminal milestone or the
// timeout elapses. It exists for tests and the CLI's self-check.
func (o *Observer) waitTerminal(runID string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)

	for {
		o.mu.Lock()
		rv := o.runs[runID]
		done := rv != nil && rv.done
		o.mu.Unlock()

		if done {
			return true
		}

		if time.Now().After(deadline) {
			return false
		}

		time.Sleep(waitPollInterval)
	}
}

// runSnapshot is the JSON wire shape of one run's live view.
type runSnapshot struct {
	RunID       string    `json:"runId"`
	ThreadID    string    `json:"threadId"`
	Tenant      string    `json:"tenant"`
	Path        string    `json:"path"`
	State       string    `json:"state"`
	EventCount  int       `json:"eventCount"`
	TokenCount  int       `json:"tokenCount"`
	LastSeq     int64     `json:"lastSeq"`
	LastEvent   string    `json:"lastEvent"`
	StartedAt   time.Time `json:"startedAt"`
	TerminalAt  time.Time `json:"terminalAt"`
	SeqOK       bool      `json:"seqOk"`
	MsgOK       bool      `json:"msgOk"`
	TerminalOK  bool      `json:"terminalOk"`
	ProjectedOK *bool     `json:"projectedOk,omitempty"`
	Violations  []string  `json:"violations,omitempty"`
}

func (rv *runView) snapshot() runSnapshot {
	return runSnapshot{
		RunID:       rv.runID,
		ThreadID:    rv.threadID,
		Tenant:      rv.tenant,
		Path:        rv.pathName,
		State:       rv.state,
		EventCount:  rv.eventCount,
		TokenCount:  rv.tokenCount,
		LastSeq:     rv.lastSeq,
		LastEvent:   rv.lastEvent,
		StartedAt:   rv.startedAt,
		TerminalAt:  rv.terminalAt,
		SeqOK:       rv.seqOK,
		MsgOK:       rv.msgOK,
		TerminalOK:  rv.terminalOK,
		ProjectedOK: rv.projOK,
		Violations:  rv.violations,
	}
}

// statsSnapshot aggregates the whole bus view.
type statsSnapshot struct {
	Total      int `json:"total"`
	Terminated int `json:"terminated"`
	Violations int `json:"violations"`
	Events     int `json:"events"`
	Tokens     int `json:"tokens"`
}

// dashboardState is the JSON document the dashboard renders.
type dashboardState struct {
	Transport         string        `json:"transport"`
	ProjectionEnabled bool          `json:"projectionEnabled"`
	UptimeSeconds     int64         `json:"uptimeSeconds"`
	Runs              []runSnapshot `json:"runs"`
	Stats             statsSnapshot `json:"stats"`
}

// snapshot builds the current dashboard state under the observer lock.
func (o *Observer) snapshot() dashboardState {
	o.mu.Lock()
	defer o.mu.Unlock()

	state := dashboardState{
		Transport:         o.transport,
		ProjectionEnabled: o.repo != nil,
		UptimeSeconds:     int64(time.Since(o.startedAt).Seconds()),
		Runs:              make([]runSnapshot, 0, len(o.runs)),
	}

	for _, rv := range o.runs {
		state.Runs = append(state.Runs, rv.snapshot())
		state.Stats.Events += rv.eventCount
		state.Stats.Tokens += rv.tokenCount
		state.Stats.Violations += rv.violationCount

		if rv.done {
			state.Stats.Terminated++
		}
	}

	sort.Slice(state.Runs, func(i, j int) bool {
		return state.Runs[i].RunID < state.Runs[j].RunID
	})

	state.Stats.Total = len(state.Runs)

	return state
}

// terminalFor maps the generator's intended terminal to the AG-UI wire type
// the observer expects on the channel.
func terminalFor(k terminalKind) stream.EventType {
	switch k {
	case terminalFinished:
		return stream.EventRunFinished
	case terminalError:
		return stream.EventRunError
	case terminalCanceled:
		return stream.EventRunCanceled
	default:
		return stream.EventRunFinished
	}
}

// terminalCoreType maps an observed AG-UI terminal back to the durable event
// type the milestone recorder persists, so the projection check can compare
// like with like.
func terminalCoreType(typ stream.EventType) (agentoscore.EventType, bool) {
	if typ == stream.EventRunFinished {
		return agentoscore.EventRunCompleted, true
	}

	if typ == stream.EventRunError {
		return agentoscore.EventRunFailed, true
	}

	if typ == stream.EventRunCanceled {
		return agentoscore.EventRunCanceled, true
	}

	return "", false
}

// projectionHasTerminal reports whether a projected run-event list reached the
// expected terminal milestone.
func projectionHasTerminal(events []agentoscore.Event, want agentoscore.EventType) bool {
	for i := range events {
		if events[i].EventType == want {
			return true
		}
	}

	return false
}

// tokenCountFor estimates the token delta an AG-UI content event carries, so
// the dashboard can show how many tokens the bus streamed per run.
func tokenCountFor(ev *stream.Event) int {
	delta, ok := ev.Payload[stream.FieldDelta].(string)
	if !ok {
		return 0
	}

	return len(strings.Fields(delta))
}

// broadcaster notifies a set of SSE subscribers that state changed. A slow
// subscriber's notification is coalesced, never blocking the observer.
type broadcaster struct {
	mu   sync.Mutex
	subs map[chan struct{}]struct{}
}

func newBroadcaster() *broadcaster {
	return &broadcaster{subs: make(map[chan struct{}]struct{})}
}

func (b *broadcaster) subscribe() chan struct{} {
	ch := make(chan struct{}, 1)

	b.mu.Lock()
	b.subs[ch] = struct{}{}
	b.mu.Unlock()

	return ch
}

func (b *broadcaster) unsubscribe(ch chan struct{}) {
	b.mu.Lock()
	delete(b.subs, ch)
	b.mu.Unlock()
}

func (b *broadcaster) notify() {
	b.mu.Lock()
	defer b.mu.Unlock()

	for ch := range b.subs {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// projectionPollLimit caps how many projected events a consistency check
// reads back per poll.
const projectionPollLimit = 32
