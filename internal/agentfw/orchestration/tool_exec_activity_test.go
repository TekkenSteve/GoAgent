package orchestration

import (
	"context"
	"errors"
	"testing"

	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	agentosstream "github.com/TekkenSteve/GoAgent/agentos/stream"
	"github.com/TekkenSteve/GoAgent/internal/entity"
	"github.com/TekkenSteve/GoAgent/internal/repo"
	"github.com/TekkenSteve/GoAgent/internal/repo/agentos/streamadapter"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agent"
	"go.temporal.io/sdk/temporal"
)

// A tool executes exactly once per activity attempt, and what happens after
// that attempt decides whether the platform may retry:
//
//   - a tool failure is the tool's answer — it becomes a failed tool message
//     for the model, never an activity error, because retrying would re-run a
//     side effect;
//   - a failure to record the finish fact happens after the tool ran, so it
//     fails the run visibly instead of silently duplicating the call;
//   - a failure to record the start fact happens before the tool ran, and a
//     retry is exactly what should happen.

var (
	errTestToolBroke  = errors.New("tool broke")
	errTestStoreBroke = errors.New("event store broke")
)

// countingFailingExecutor fails every call and counts them.
type countingFailingExecutor struct{ calls int }

func (e *countingFailingExecutor) Execute(_ context.Context, _ *entity.ToolRequest) (entity.ToolResult, error) {
	e.calls++

	return entity.ToolResult{}, errTestToolBroke
}

// breakingEventStore fails its n-th append and every one after it. Tool
// activities project exactly one durable fact — the finish milestone — so
// failOn=1 is the store that breaks while recording the tool's outcome.
type breakingEventStore struct {
	failOn  int
	appends int
}

func (s *breakingEventStore) LastRunEventSequence(_ context.Context, _ string) (int64, error) {
	return 0, nil
}

func (s *breakingEventStore) AppendRunEvent(_ context.Context, _ *agentoscore.Event) error {
	s.appends++
	if s.appends >= s.failOn {
		return errTestStoreBroke
	}

	return nil
}

// silentPublisher accepts every publication; the bus is fail-open by design.
type silentPublisher struct{}

func (silentPublisher) Publish(_ context.Context, _ *agentosstream.Handle, _ *agentosstream.Event) error {
	return nil
}

func newToolExecActivities(t *testing.T, executor repo.ToolExecutor) *AgentActivities {
	t.Helper()

	return NewAgentActivities(agent.New(nil, executor, nil, nil, nil), nil)
}

func TestToolExecActivityToolFailureIsTheToolsAnswer(t *testing.T) {
	t.Parallel()

	executor := &countingFailingExecutor{}
	activities := newToolExecActivities(t, executor)

	output, err := activities.ToolExecActivity(t.Context(), &ToolInput{
		AccountID:  "acct-1",
		ProjectID:  "proj-1",
		RunID:      "run-1",
		ToolCallID: "call-1",
		ToolName:   "send_email",
		Args:       `{"to":"a@example.com"}`,
	})
	if err != nil {
		t.Fatalf("a tool failure must not fail the activity (the platform would retry and re-run it): %v", err)
	}

	if !output.IsError {
		t.Fatalf("expected an error result, got %+v", output)
	}

	if executor.calls != 1 {
		t.Fatalf("tool executed %d times for one call", executor.calls)
	}

	if output.ExitCode == 0 {
		t.Errorf("a failed tool reports a non-zero exit code, got %d", output.ExitCode)
	}
}

func TestToolExecStreamActivityToolFailureIsTheToolsAnswer(t *testing.T) {
	t.Parallel()

	executor := &countingFailingExecutor{}
	activities := newToolExecActivities(t, executor)

	output, err := activities.ToolExecStreamActivity(t.Context(), &ToolInput{
		AccountID:  "acct-1",
		ProjectID:  "proj-1",
		RunID:      "run-1",
		ToolCallID: "call-1",
		ToolName:   "send_email",
		Args:       `{"to":"a@example.com"}`,
	})
	if err != nil {
		t.Fatalf("a tool failure must not fail the activity: %v", err)
	}

	if !output.IsError || executor.calls != 1 {
		t.Fatalf("expected one failed attempt as the answer, got calls=%d output=%+v", executor.calls, output)
	}
}

// The finish fact is written after the tool has executed. If that write
// fails, the platform must not retry the activity — the retry would re-run
// the tool — so the error is marked non-retryable and the run fails visibly.
func TestToolExecActivityFinishFactFailureIsNotRetryable(t *testing.T) {
	t.Parallel()

	executor := &countingFailingExecutor{}
	activities := newToolExecActivities(t, executor).
		WithStreamPublisher(silentPublisher{})

	// The finish milestone cannot be recorded.
	store := &breakingEventStore{failOn: 1}

	recorder, err := streamadapter.NewMilestoneRecorder(store, nil)
	if err != nil {
		t.Fatal(err)
	}

	activities.WithMilestoneRecorder(recorder)

	output, err := activities.ToolExecActivity(t.Context(), &ToolInput{
		AccountID:  "acct-1",
		ProjectID:  "proj-1",
		RunID:      "run-1",
		ToolCallID: "call-1",
		ToolName:   "send_email",
	})
	if err == nil {
		t.Fatalf("a lost finish fact must fail the activity, got output %+v", output)
	}

	var appErr *temporal.ApplicationError
	if !errors.As(err, &appErr) {
		t.Fatalf("expected a Temporal application error, got %T: %v", err, err)
	}

	if !appErr.NonRetryable() {
		t.Fatal("retrying after the tool executed would duplicate its side effect")
	}

	if executor.calls != 1 {
		t.Fatalf("tool executed %d times for one call", executor.calls)
	}
}

// The classification itself: post-side-effect failures carry their cause.
func TestNonRetryableAfterSideEffectCarriesCause(t *testing.T) {
	t.Parallel()

	cause := errTestStoreBroke

	err := nonRetryableAfterSideEffect(cause)

	var appErr *temporal.ApplicationError
	if !errors.As(err, &appErr) {
		t.Fatalf("expected an application error, got %T", err)
	}

	if !appErr.NonRetryable() {
		t.Fatal("must be non-retryable")
	}

	if !errors.Is(err, cause) {
		t.Fatal("the cause is lost")
	}
}

// tenantCapturingExecutor records the tenant each tool call arrives with and
// then behaves like a tool that works.
type tenantCapturingExecutor struct{ seen []*entity.ToolRequest }

func (e *tenantCapturingExecutor) Execute(_ context.Context, req *entity.ToolRequest) (entity.ToolResult, error) {
	e.seen = append(e.seen, req)

	// Fail like a tool that has no tenant to work with would: the assertion is
	// about what the pipeline was handed, not about the tool's answer.
	return entity.ToolResult{}, errTestToolBroke
}

// A run's identity is the run's, and every step it takes inherits it — a tool
// call included.
//
// The tool pipeline authorizes a call as the run: it builds the principal from
// the request's account and the tenant from its project. A tool call that
// arrives without them cannot be authorized, so a request that carries them
// only as far as the activity has not carried them at all.
func TestToolExecActivityCarriesTheRunsTenant(t *testing.T) {
	t.Parallel()

	executor := &tenantCapturingExecutor{}
	activities := newToolExecActivities(t, executor)

	_, err := activities.ToolExecActivity(t.Context(), &ToolInput{
		AccountID:  "acct-1",
		ProjectID:  "proj-1",
		RunID:      "run-1",
		ToolCallID: "call-1",
		ToolName:   "web_search",
		Args:       `{"query":"tenant"}`,
	})
	if err != nil {
		t.Fatalf("tool exec: %v", err)
	}

	if len(executor.seen) != 1 {
		t.Fatalf("tool executed %d times, want once", len(executor.seen))
	}

	if got := executor.seen[0].AccountID; got != "acct-1" {
		t.Fatalf("tool request account = %q, want the run's account %q", got, "acct-1")
	}

	if got := executor.seen[0].ProjectID; got != "proj-1" {
		t.Fatalf("tool request project = %q, want the run's project %q", got, "proj-1")
	}
}

// A run without a tenant cannot authorize anything, and a tool call that could
// not be authorized never reached a tool. The run fails visibly instead of
// telling the model that a tool failed.
func TestToolExecActivityRefusesARunWithoutATenant(t *testing.T) {
	t.Parallel()

	executor := &tenantCapturingExecutor{}
	activities := newToolExecActivities(t, executor)

	_, err := activities.ToolExecActivity(t.Context(), &ToolInput{
		AccountID:  "acct-1",
		RunID:      "run-1",
		ToolCallID: "call-1",
		ToolName:   "web_search",
		Args:       `{"query":"tenant"}`,
	})
	if err == nil {
		t.Fatal("a tool call without a tenant must fail the activity")
	}

	if len(executor.seen) != 0 {
		t.Fatalf("a tool ran without a tenant: %+v", executor.seen)
	}

	var appErr *temporal.ApplicationError
	if !errors.As(err, &appErr) || !appErr.NonRetryable() {
		t.Fatalf("a missing tenant is deterministic, so the retry must be refused: %v", err)
	}

	if !errors.Is(err, agent.ErrInvalidRunIdentity) {
		t.Fatalf("the failure must name the missing identity: %v", err)
	}
}
