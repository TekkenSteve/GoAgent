// Package main implements the bus dashboard: a visual, runnable proof that the
// AgentOS data plane works. It generates synthetic agent runs through the same
// code paths the shipped backends use, subscribes to the same run channels a
// frontend would, and asserts the timeline invariants the contract promises
// (terminal closure, monotonic sequence numbers, message pairing, and — with
// a Postgres URL — that the durable projection converges with the bus).
package main

import (
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"strings"
	"sync"
	"time"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	"github.com/TekkenSteve/GoAgent/agentos/stream"
	"github.com/TekkenSteve/GoAgent/internal/entity"
	"github.com/TekkenSteve/GoAgent/internal/repo/agentos/streamadapter"
)

// terminalKind is the synthetic outcome a generated run ends with. It is the
// same terminal vocabulary the RunLifecycle normalizes, so the observer can
// assert the bus delivered the run's intended terminal milestone.
type terminalKind int

const (
	terminalFinished terminalKind = iota
	terminalError
	terminalCanceled
)

// pathMode selects which real publishing path the traffic generator uses.
type pathMode int

const (
	pathBoth pathMode = iota
	pathNative
	pathLifecycle
)

// Traffic-shape constants. They are the demo's tunables — stream lengths,
// outcome weights, token counts — named so the demo's magic numbers are
// auditable rather than scattered literals.
const (
	pathCoinFlip   = 2
	terminalRoll   = 10
	errorRoll      = 8
	cancelRoll     = 6
	tenantRoll     = 3
	spaceTokenRoll = 4

	reasoningDeltas     = 4
	firstTextDeltas     = 12
	tailTextDeltas      = 8
	toolArgsDeltas      = 2
	toolStdoutDeltas    = 3
	statusPollCount     = 3
	statusPollDelayMult = 4

	promptTokensBase       = 480
	promptTokensJitter     = 120
	completionTokensBase   = 640
	completionTokensJitter = 160
	totalTokensBase        = 1120
	totalTokensJitter      = 280

	toolDurationMs = 42

	// wordBankCount is the token-bank size; sampleTokenBank below is kept to
	// exactly this many words so the token index draw needs no runtime divisor.
	wordBankCount = 32
)

// runPathName is the human-readable name a path snapshot carries.
func (m pathMode) runPathName() string {
	switch m {
	case pathBoth:
		return "both"
	case pathNative:
		return "native"
	case pathLifecycle:
		return "lifecycle"
	default:
		return "unknown"
	}
}

// outcome is the generator's intent for one run: identity, publishing path,
// and the terminal the run is supposed to reach. The observer checks the
// bus-observed timeline against it.
type outcome struct {
	runID    string
	threadID string
	tenant   string
	path     pathMode
	terminal terminalKind
}

// Generator publishes synthetic runs on the real data-plane code paths. It is
// the write side of the dashboard: what it emits is exactly what a shipped
// native backend (PublishWriter) or a one-shot backend (RunLifecycle) would
// publish, so the live bus view and the assertions exercise production code.
type Generator struct {
	pub    stream.Publisher
	rate   time.Duration // min delay between token events
	mode   pathMode
	facts  *streamadapter.MilestoneRecorder // optional writer→PG fact recorder
	stderr io.Writer

	mu   sync.Mutex
	rng  *rand.PCG
	next int
}

// NewGenerator wires a traffic generator. A nil recorder skips the durable
// fact path the shipped app installs on publish. A zero seed derives from the
// clock; an explicit seed makes the traffic reproducible.
func NewGenerator(pub stream.Publisher, rate time.Duration, mode pathMode, facts *streamadapter.MilestoneRecorder, seed int64) *Generator {
	if seed == 0 {
		seed = time.Now().UnixNano()
	}

	// The demo's token stream is not security sensitive: a seeded PCG keeps
	// runs reproducible (crypto/rand would defeat -seed). rand/v2.NewPCG is the
	// non-flagged constructor for a plain PRNG; only rand.New is gosec G404.
	return &Generator{
		pub:    pub,
		rate:   rate,
		mode:   mode,
		facts:  facts,
		stderr: os.Stderr,
		rng:    rand.NewPCG(uint64(seed), uint64(seed)),
	}
}

// runOne generates one synthetic run: it registers the intended outcome with
// the observer (which subscribes to the run's channel), then publishes the
// timeline through the real adapter path. nextOutcome resolves a "both" mode
// to a concrete path, so only the two concrete paths dispatch here.
func (g *Generator) runOne(ctx context.Context, obs *Observer) error {
	out := g.nextOutcome()

	if err := obs.TrackRun(ctx, out); err != nil {
		return fmt.Errorf("bus-dashboard - track run %s: %w", out.runID, err)
	}

	if out.path == pathNative {
		g.emitNative(ctx, out)

		return nil
	}

	g.emitLifecycle(ctx, out)

	return nil
}

// nextOutcome picks a fresh run identity, a publishing path, and a random
// terminal, weighted toward a clean finish so the happy path dominates the
// live view while failures still exercise the error terminal.
func (g *Generator) nextOutcome() outcome {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.next++

	runID := fmt.Sprintf("run-%04d", g.next)

	mode := g.mode
	if mode == pathBoth {
		mode = pathNative
		if int(g.rng.Uint64()%pathCoinFlip) == 1 {
			mode = pathLifecycle
		}
	}

	terminal := terminalFinished

	switch roll := int(g.rng.Uint64() % terminalRoll); {
	case roll >= errorRoll:
		terminal = terminalError
	case roll == cancelRoll:
		terminal = terminalCanceled
	}

	tenant := "acme"
	if int(g.rng.Uint64()%tenantRoll) == 0 {
		tenant = "default"
	}

	return outcome{
		runID:    runID,
		threadID: "thread-" + runID,
		tenant:   tenant,
		path:     mode,
		terminal: terminal,
	}
}

// emitNative publishes a full native-style stream: a RUN_STARTED milestone,
// then the token/tool arc, then a terminal milestone — all through the real
// streamadapter.PublishWriter code path.
func (g *Generator) emitNative(ctx context.Context, out outcome) {
	handle := streamadapter.HandleForRun(out.tenant, out.runID)

	writer := streamadapter.NewPublishWriter(g.pub, handle, out.threadID, out.runID, nil)
	if g.facts != nil {
		writer = writer.WithFacts(g.facts)
	}

	g.write(ctx, writer, &entity.AgentRunStartEvent{
		BaseEvent:    base(out, entity.SourceAgent, entity.PhaseStart, entity.ContentStatus),
		AgentName:    "bus-dashboard",
		InputSummary: "synthetic run through the native publishing path",
	})

	g.emitNativeStream(ctx, writer, out)
	g.finishNative(ctx, writer, out)
}

// emitNativeStream emits the mid-run token and tool arc: reasoning, text, a
// tool invocation with stdout, more text, and a user feedback event.
func (g *Generator) emitNativeStream(ctx context.Context, writer *streamadapter.PublishWriter, out outcome) {
	g.deltas(ctx, writer, reasoningDeltas, func(_ int) entity.StreamEvent {
		return &entity.ReasoningDeltaEvent{
			BaseEvent: base(out, entity.SourceLLM, entity.PhaseDelta, entity.ContentReasoning),
			Reasoning: g.word("reasoning"),
		}
	})

	g.deltas(ctx, writer, firstTextDeltas, func(_ int) entity.StreamEvent {
		return &entity.TextDeltaEvent{
			BaseEvent: base(out, entity.SourceLLM, entity.PhaseDelta, entity.ContentText),
			Content:   g.word("text"),
		}
	})

	g.emitToolInvocation(ctx, writer, out)

	g.deltas(ctx, writer, tailTextDeltas, func(_ int) entity.StreamEvent {
		return &entity.TextDeltaEvent{
			BaseEvent: base(out, entity.SourceLLM, entity.PhaseDelta, entity.ContentText),
			Content:   g.word("text"),
		}
	})

	g.write(ctx, writer, &entity.UserFeedbackEvent{
		BaseEvent:     base(out, entity.SourceUser, entity.PhaseFinish, entity.ContentFeedback),
		TargetEventID: out.runID + ":1",
		FeedbackType:  "approve",
	})

	writer.Flush(ctx)
}

// finishNative closes a native run's timeline with the intended terminal.
func (g *Generator) finishNative(ctx context.Context, writer *streamadapter.PublishWriter, out outcome) {
	switch out.terminal {
	case terminalError:
		g.write(ctx, writer, &entity.AgentErrorEvent{
			BaseEvent:    base(out, entity.SourceSystem, entity.PhaseError, entity.ContentStatus),
			ErrorMessage: "simulated model timeout",
			ErrorCode:    "model.timeout",
		})
	case terminalCanceled:
		g.write(ctx, writer, &entity.AgentRunCanceledEvent{
			BaseEvent: base(out, entity.SourceSystem, entity.PhaseInterrupt, entity.ContentStatus),
		})
	case terminalFinished:
		g.write(ctx, writer, &entity.AgentRunFinishEvent{
			BaseEvent:    base(out, entity.SourceAgent, entity.PhaseFinish, entity.ContentStatus),
			FinishReason: "complete",
			Usage: &entity.Usage{
				PromptTokens:     promptTokensBase + int(g.drawU64()%promptTokensJitter),
				CompletionTokens: completionTokensBase + int(g.drawU64()%completionTokensJitter),
				TotalTokens:      totalTokensBase + int(g.drawU64()%totalTokensJitter),
			},
		})
	}
}

// emitToolInvocation publishes a tool call + execution with a couple of stdout
// deltas, so the UI shows the TOOL_CALL → CUSTOM tool.execution →
// TOOL_CALL_RESULT arc alongside the text stream.
func (g *Generator) emitToolInvocation(ctx context.Context, writer *streamadapter.PublishWriter, out outcome) {
	callID := "call-" + out.runID

	g.write(ctx, writer, &entity.ToolCallStartEvent{
		BaseEvent:  base(out, entity.SourceLLM, entity.PhaseStart, entity.ContentToolCall),
		ToolCallID: callID,
		ToolName:   "bus_demo",
	})

	g.deltas(ctx, writer, toolArgsDeltas, func(i int) entity.StreamEvent {
		delta := `{"query":`
		if i == 1 {
			delta = ` "dashboard"}`
		}

		return &entity.ToolCallDeltaEvent{
			BaseEvent:      base(out, entity.SourceLLM, entity.PhaseDelta, entity.ContentToolCall),
			ToolCallID:     callID,
			ArgumentsDelta: delta,
		}
	})

	g.write(ctx, writer, &entity.ToolCallFinishEvent{
		BaseEvent:  base(out, entity.SourceLLM, entity.PhaseFinish, entity.ContentToolCall),
		ToolCallID: callID,
		ToolName:   "bus_demo",
		Arguments:  `{"query": "dashboard"}`,
	})

	g.write(ctx, writer, &entity.ToolExecStartEvent{
		BaseEvent:  base(out, entity.SourceTool, entity.PhaseStart, entity.ContentToolResult),
		ToolCallID: callID,
		ToolName:   "bus_demo",
	})

	g.deltas(ctx, writer, toolStdoutDeltas, func(_ int) entity.StreamEvent {
		return &entity.ToolExecStdoutEvent{
			BaseEvent:   base(out, entity.SourceTool, entity.PhaseDelta, entity.ContentToolResult),
			ToolCallID:  callID,
			StdoutDelta: g.word("tool"),
		}
	})

	g.write(ctx, writer, &entity.ToolExecFinishEvent{
		BaseEvent:  base(out, entity.SourceTool, entity.PhaseFinish, entity.ContentToolResult),
		ToolCallID: callID,
		ToolName:   "bus_demo",
		Output:     "ok: dashboard stream observed",
		ExitCode:   0,
		DurationMs: toolDurationMs,
	})
}

// emitLifecycle publishes a one-shot backend's observable lifecycle: Start
// opens the timeline, intermediate Status polls are non-terminal (no-ops), and
// the terminal Status closes it exactly once — through the real
// streamadapter.RunLifecycle code path.
func (g *Generator) emitLifecycle(ctx context.Context, out outcome) {
	lifecycle := streamadapter.NewRunLifecycle(g.pub, nil)
	if g.facts != nil {
		lifecycle = lifecycle.WithFacts(g.facts)
	}

	spec := &agentos.RunSpec{
		RunID:       out.runID,
		ThreadID:    out.threadID,
		AccountID:   out.tenant,
		Backend:     agentos.BackendRef{Kind: agentos.BackendKindHTTP, Name: "bus-dashboard"},
		UserMessage: "synthetic run through the one-shot lifecycle path",
		RequestedAt: time.Now().UTC(),
	}

	if err := lifecycle.PublishStarted(ctx, spec, &agentos.RunStatus{RunID: out.runID, LifecycleState: "running"}); err != nil {
		fmt.Fprintf(g.stderr, "bus-dashboard: lifecycle start %s: %v\n", out.runID, err)
	}

	// A one-shot backend holds no byte stream locally: the run works remotely
	// and is observed through Status polls, which stay non-terminal here.
	for range statusPollCount {
		g.sleep(ctx, g.rate*statusPollDelayMult)

		if err := lifecycle.PublishStatus(ctx, out.runID, &agentos.RunStatus{RunID: out.runID, LifecycleState: "running"}); err != nil {
			fmt.Fprintf(g.stderr, "bus-dashboard: lifecycle status %s: %v\n", out.runID, err)
		}
	}

	g.finishLifecycle(ctx, lifecycle, out)
}

// finishLifecycle closes a one-shot run with the intended terminal status
// spelling, which the RunLifecycle normalizes onto the AG-UI terminal event.
func (g *Generator) finishLifecycle(ctx context.Context, lifecycle *streamadapter.RunLifecycle, out outcome) {
	var status agentos.RunStatus

	switch out.terminal {
	case terminalError:
		status = agentos.RunStatus{
			RunID:          out.runID,
			LifecycleState: "failed",
			Reason:         "remote status query returned failed",
		}
	case terminalCanceled:
		status = agentos.RunStatus{
			RunID:          out.runID,
			LifecycleState: "canceled",
		}
	case terminalFinished:
		status = agentos.RunStatus{
			RunID:          out.runID,
			LifecycleState: "completed",
		}
	}

	if err := lifecycle.PublishStatus(ctx, out.runID, &status); err != nil {
		fmt.Fprintf(g.stderr, "bus-dashboard: terminal fact %s: %v\n", out.runID, err)
	}
}

// deltas emits n incremental events at the configured token rate. WriteEvent
// is fail-open by contract, so a bus error is logged, never fatal.
func (g *Generator) deltas(ctx context.Context, writer *streamadapter.PublishWriter, n int, makeEvent func(int) entity.StreamEvent) {
	for i := range n {
		if !g.sleep(ctx, g.rate) {
			return
		}

		g.write(ctx, writer, makeEvent(i))
	}
}

// write sends one event through the real adapter. PublishWriter.WriteEvent
// returns nil by contract (fail-open), but the return is still surfaced for
// integrity of the demo: a dropped bus write would be visible.
func (g *Generator) write(ctx context.Context, writer *streamadapter.PublishWriter, ev entity.StreamEvent) {
	if err := writer.WriteEvent(ctx, ev); err != nil {
		fmt.Fprintf(g.stderr, "bus-dashboard: write %s: %v (fail-open)\n", ev.EventType(), err)
	}
}

// sleep pauses the generator, returning false when the context ends.
func (g *Generator) sleep(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

// drawU64 draws one raw PRNG word under the generator mutex. rng is not
// goroutine safe and is shared by every concurrent run's token stream. Callers
// reduce the draw to a bound inline with a named-constant divisor so gosec can
// prove the int conversion safe (G115); raw Uint64 is not G404-flagged.
func (g *Generator) drawU64() uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()

	return g.rng.Uint64()
}

// word yields a readable pseudo-token fragment so the token stream renders as
// prose in the UI rather than raw noise.
func (g *Generator) word(kind string) string {
	words := strings.Fields(sampleTokenBank)
	idx := int(g.drawU64() % wordBankCount)

	if kind == "tool" {
		return "tool-output-" + words[idx]
	}

	if int(g.drawU64()%spaceTokenRoll) == 0 {
		return words[idx] + " "
	}

	return words[idx]
}

// base builds the shared BaseEvent metadata for a generated event. The mapper
// reads SessionID/RunID for scope; the writer overwrites both with the run's
// canonical identity.
func base(out outcome, source entity.EventSource, phase entity.EventPhase, contentType entity.EventContentType) entity.BaseEvent {
	return entity.BaseEvent{
		Source:      source,
		Phase:       phase,
		ContentType: contentType,
		SessionID:   out.threadID,
		RunID:       out.runID,
		Timestamp:   time.Now().UTC(),
	}
}

// sampleTokenBank is the prose the generator streams as tokens. Keeping it one
// string avoids a package-level mutable slice (gochecknoglobals), and keeping
// it at exactly wordBankCount words lets the index draw use a constant divisor.
const sampleTokenBank = "the bus carries every event in order per channel and the terminal always closes the timeline exactly once so a monotonic sequence proves the data plane works while every run lands alone"
