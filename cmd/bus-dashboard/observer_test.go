package main

import (
	"strings"
	"testing"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos/stream"
	"github.com/TekkenSteve/GoAgent/internal/repo/stream/memstream"
	"github.com/stretchr/testify/require"
)

// TestObserverLifecycleRunIsClean locks the one-shot backend path: a run
// published through the real RunLifecycle adapter closes exactly once with
// the intended terminal.
func TestObserverLifecycleRunIsClean(t *testing.T) {
	t.Parallel()

	bus := memstream.New()
	obs := NewObserver(t.Context(), bus, nil, "memstream")

	gen := NewGenerator(bus, time.Millisecond, pathLifecycle, nil, 2)
	out := gen.nextOutcome()
	out.terminal = terminalFinished

	require.NoError(t, obs.TrackRun(t.Context(), out))
	gen.emitLifecycle(t.Context(), out)

	require.True(t, obs.waitTerminal(out.runID, 3*time.Second))

	snap := runSnapshotOf(t, obs, out.runID)
	require.Equal(t, "finished", snap.State)
	require.True(t, snap.SeqOK)
	require.True(t, snap.TerminalOK)
	require.Empty(t, snap.Violations)
}

// TestObserverTerminalKindsAreClean locks the error and cancellation paths
// alongside the happy path: each terminal entity event maps to its intended
// AG-UI terminal milestone and the observer accepts it with no violations.
func TestObserverTerminalKindsAreClean(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		seed     int64
		terminal terminalKind
		state    string
	}{
		{name: "finished", seed: 1, terminal: terminalFinished, state: stateFinished},
		{name: "error", seed: 3, terminal: terminalError, state: stateError},
		{name: "canceled", seed: 4, terminal: terminalCanceled, state: stateCanceled},
	}

	for i := range cases {
		tc := cases[i]

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			bus := memstream.New()
			obs := NewObserver(t.Context(), bus, nil, "memstream")

			gen := NewGenerator(bus, time.Millisecond, pathNative, nil, tc.seed)
			out := gen.nextOutcome()
			out.terminal = tc.terminal

			require.NoError(t, obs.TrackRun(t.Context(), out))
			gen.emitNative(t.Context(), out)

			require.True(t, obs.waitTerminal(out.runID, 3*time.Second))

			snap := runSnapshotOf(t, obs, out.runID)
			require.Equal(t, tc.state, snap.State)
			require.True(t, snap.SeqOK)
			require.True(t, snap.MsgOK)
			require.True(t, snap.TerminalOK)
			require.Empty(t, snap.Violations)
			require.Greater(t, snap.EventCount, 0)
			require.Greater(t, snap.TokenCount, 0)
		})
	}
}

// The violation tests feed synthetic StoredEvents straight into the observer,
// so each data-plane invariant can be broken deterministically without a bus.

func TestObserverFlagsEventAfterTerminal(t *testing.T) {
	t.Parallel()

	obs := NewObserver(t.Context(), nil, nil, "memstream")
	obs.registerRun(cleanOutcome("run-1"))

	obs.observe("run-1", stored(stream.EventRunStarted, 1))
	obs.observe("run-1", stored(stream.EventRunFinished, 2))
	obs.observe("run-1", stored(stream.EventTextMessageContent, 3))

	rv := obs.runView("run-1")
	require.False(t, rv.terminalOK)
	require.Contains(t, rv.violations, "event TEXT_MESSAGE_CONTENT arrived after terminal RUN_FINISHED")
}

func TestObserverFlagsTerminalBeforeStart(t *testing.T) {
	t.Parallel()

	obs := NewObserver(t.Context(), nil, nil, "memstream")
	obs.registerRun(cleanOutcome("run-2"))

	obs.observe("run-2", stored(stream.EventRunFinished, 1))

	rv := obs.runView("run-2")
	require.False(t, rv.terminalOK)
	require.Contains(t, rv.violations, "RUN_FINISHED arrived before RUN_STARTED")
}

func TestObserverFlagsWrongTerminal(t *testing.T) {
	t.Parallel()

	obs := NewObserver(t.Context(), nil, nil, "memstream")
	obs.registerRun(cleanOutcome("run-3")) // expects RUN_FINISHED

	obs.observe("run-3", stored(stream.EventRunStarted, 1))
	obs.observe("run-3", stored(stream.EventRunError, 2))

	rv := obs.runView("run-3")
	require.False(t, rv.terminalOK)
	require.Contains(t, rv.violations, "terminal RUN_ERROR does not match expected RUN_FINISHED")
}

func TestObserverFlagsContentWithoutOpenMessage(t *testing.T) {
	t.Parallel()

	obs := NewObserver(t.Context(), nil, nil, "memstream")
	obs.registerRun(cleanOutcome("run-4"))

	obs.observe("run-4", stored(stream.EventRunStarted, 1))
	obs.observe("run-4", stored(stream.EventTextMessageContent, 2))

	rv := obs.runView("run-4")
	require.False(t, rv.msgOK)
	require.Contains(t, rv.violations, "TEXT_MESSAGE_CONTENT content with no open message")
}

func TestObserverFlagsMessageLeftOpenAtTerminal(t *testing.T) {
	t.Parallel()

	obs := NewObserver(t.Context(), nil, nil, "memstream")
	obs.registerRun(cleanOutcome("run-5"))

	obs.observe("run-5", stored(stream.EventRunStarted, 1))
	obs.observe("run-5", stored(stream.EventTextMessageStart, 2))
	obs.observe("run-5", stored(stream.EventRunFinished, 3))

	rv := obs.runView("run-5")
	require.False(t, rv.msgOK)
	require.Contains(t, rv.violations, "1 message(s) left open at terminal")
}

func TestObserverFlagsNonMonotonicSequence(t *testing.T) {
	t.Parallel()

	obs := NewObserver(t.Context(), nil, nil, "memstream")
	obs.registerRun(cleanOutcome("run-6"))

	obs.observe("run-6", stored(stream.EventRunStarted, 5))
	obs.observe("run-6", stored(stream.EventRunFinished, 3))

	rv := obs.runView("run-6")
	require.False(t, rv.seqOK)
	require.Contains(t, rv.violations, "sequence 3 is not after 5")
}

// TestObserverMessagePairingPasses locks the happy message arc: START →
// CONTENT → END pairs cleanly and the token counter reflects the deltas.
func TestObserverMessagePairingPasses(t *testing.T) {
	t.Parallel()

	obs := NewObserver(t.Context(), nil, nil, "memstream")
	obs.registerRun(cleanOutcome("run-7"))

	obs.observe("run-7", stored(stream.EventRunStarted, 1))
	obs.observe("run-7", stored(stream.EventTextMessageStart, 2))
	obs.observe("run-7", content(3, "hello world"))
	obs.observe("run-7", stored(stream.EventTextMessageEnd, 4))
	obs.observe("run-7", stored(stream.EventRunFinished, 5))

	rv := obs.runView("run-7")
	require.True(t, rv.seqOK)
	require.True(t, rv.msgOK)
	require.True(t, rv.terminalOK)
	require.Empty(t, rv.violations)
	require.Equal(t, 2, rv.tokenCount)
}

// TestSampleTokenBankHasExactWordCount locks the invariant that word() relies
// on: the token bank must hold exactly wordBankCount words, or the constant
// divisor would index out of range.
func TestSampleTokenBankHasExactWordCount(t *testing.T) {
	t.Parallel()

	if got := len(strings.Fields(sampleTokenBank)); got != wordBankCount {
		t.Fatalf("sampleTokenBank has %d words, want %d", got, wordBankCount)
	}
}

// cleanOutcome builds a run whose expected terminal is a clean finish.
func cleanOutcome(runID string) outcome {
	return outcome{
		runID:    runID,
		threadID: "thread-" + runID,
		tenant:   "acme",
		path:     pathNative,
		terminal: terminalFinished,
	}
}

// stored builds a bare stored event with a monotonic-friendly payload map.
func stored(typ stream.EventType, seq int64) *stream.StoredEvent {
	return &stream.StoredEvent{
		Event:    *stream.NewEvent(typ),
		Sequence: seq,
		StoredAt: time.Now(),
	}
}

// content builds a text content event carrying a token delta.
func content(seq int64, delta string) *stream.StoredEvent {
	st := stored(stream.EventTextMessageContent, seq)
	st.Event.Set(stream.FieldDelta, delta)

	return st
}

// runView returns a registered run's live view (tests hold no concurrency).
func (o *Observer) runView(runID string) *runView {
	o.mu.Lock()
	defer o.mu.Unlock()

	return o.runs[runID]
}

// runSnapshotOf returns the JSON snapshot for one run.
func runSnapshotOf(t *testing.T, obs *Observer, runID string) runSnapshot {
	t.Helper()

	snap := obs.snapshot()
	for i := range snap.Runs {
		if snap.Runs[i].RunID == runID {
			return snap.Runs[i]
		}
	}

	t.Fatalf("run %s not observed", runID)

	return runSnapshot{}
}
