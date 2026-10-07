package temporal

import (
	"context"
	"errors"
	"testing"
	"time"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	"github.com/TekkenSteve/GoAgent/internal/agentfw/orchestration"
	"github.com/TekkenSteve/GoAgent/internal/entity"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosruntime/agentosruntimetest"
	"github.com/stretchr/testify/require"
)

const AgentOSConformanceRun = "agentos-conformance-run"

func TestTemporalNativeBackendConformance(t *testing.T) {
	t.Parallel()

	probe := &agentosruntimetest.SubscriberProbe{}
	executor := &fakeNativeExecutor{
		status: entity.RunStatus{
			RunID:          AgentOSConformanceRun,
			LifecycleState: "running",
			UpdatedAt:      time.Date(2026, 6, 16, 12, 1, 0, 0, time.UTC),
		},
	}
	backend := newTemporalNativeBackend(executor, probe, nil)

	agentosruntimetest.RunBackendConformance(t, &agentosruntimetest.BackendConformanceCase{
		Name:            "native",
		Backend:         backend,
		Ref:             agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative},
		StatusState:     "running",
		SubscriberProbe: probe,
	})

	if executor.start == nil ||
		executor.start.RunID != AgentOSConformanceRun ||
		executor.start.UserMessage != Hello ||
		executor.userMessageRunID != AgentOSConformanceRun ||
		executor.canceledRunID != AgentOSConformanceRun {
		t.Fatalf("native backend calls were not routed through the shared contract: %#v", executor)
	}
}

func TestTemporalNativeBackendSignalUserMessage(t *testing.T) {
	t.Parallel()

	executor := &fakeNativeExecutor{}
	backend := newTemporalNativeBackend(executor, nil, nil)
	sentAt := time.Date(2026, 6, 16, 12, 0, 0, 0, time.UTC)

	signal := agentoscore.Signal{
		Type:           agentoscore.SignalUserMessage,
		IdempotencyKey: "idem-1",
		SentAt:         sentAt,
		Payload: map[string]any{
			"message_id": "msg-1",
			"content":    "continue",
			"context": map[string]any{
				"source": "test",
			},
			"attachments": []any{
				map[string]any{"file_id": "file-1"},
			},
		},
	}

	err := backend.Signal(context.Background(), "run-1", &signal)
	if err != nil {
		t.Fatalf("Signal: %v", err)
	}

	if executor.userMessageRunID != Run1 {
		t.Fatalf("run id = %q", executor.userMessageRunID)
	}

	if executor.userMessage.MessageID != "msg-1" ||
		executor.userMessage.IdempotencyKey != "idem-1" ||
		executor.userMessage.Content != "continue" ||
		executor.userMessage.Context["source"] != "test" ||
		executor.userMessage.Attachments[0]["file_id"] != "file-1" ||
		executor.userMessage.ReceivedAtUnix != sentAt.Unix() {
		t.Fatalf("unexpected user message: %#v", executor.userMessage)
	}
}

func TestTemporalNativeBackendRejectsEmptyUserMessageContent(t *testing.T) {
	t.Parallel()

	backend := newTemporalNativeBackend(&fakeNativeExecutor{}, nil, nil)

	signal := agentoscore.Signal{
		Type:    agentoscore.SignalUserMessage,
		Payload: map[string]any{},
	}

	err := backend.Signal(context.Background(), "run-1", &signal)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestTemporalNativeBackendCapabilitiesIncludeUserMessage(t *testing.T) {
	t.Parallel()

	backend := newTemporalNativeBackend(&fakeNativeExecutor{}, nil, nil)

	capabilities := backend.Capabilities()
	if !capabilities.SupportsSignalUserMessage {
		t.Fatalf("SupportsSignalUserMessage = false")
	}
}

func TestTemporalNativeBackendPublishesLifecycleMilestones(t *testing.T) {
	t.Parallel()

	probe := &agentosruntimetest.LifecycleProbe{}
	executor := &fakeNativeExecutor{}
	backend := newTemporalNativeBackend(executor, nil, probe)

	spec := &agentos.RunSpec{
		RunID:       Run1,
		AccountID:   "acct-1",
		UserMessage: "hello",
	}

	status, err := backend.Start(context.Background(), spec)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	if probe.LastStartedSpec().RunID != Run1 {
		t.Fatalf("PublishStarted spec run id = %q", probe.LastStartedSpec().RunID)
	}

	if probe.LastStartedStatus().RunID != status.RunID {
		t.Fatalf("PublishStarted status run id = %q", probe.LastStartedStatus().RunID)
	}

	polled, err := backend.Status(context.Background(), Run1)
	if err != nil {
		t.Fatalf("Status: %v", err)
	}

	if len(probe.Statuses()) != 1 || probe.Statuses()[0].RunID != polled.RunID {
		t.Fatalf("PublishStatus calls = %#v", probe.Statuses())
	}
}

var errNativeLifecycleProbe = errors.New("native lifecycle probe: milestone persist failed")

type failingLifecycleProbe struct{}

func (failingLifecycleProbe) PublishStarted(context.Context, *agentos.RunSpec, *agentos.RunStatus) error {
	return errNativeLifecycleProbe
}

func (failingLifecycleProbe) PublishStatus(context.Context, string, *agentos.RunStatus) error {
	return errNativeLifecycleProbe
}

func TestTemporalNativeBackendFailsHardOnMilestonePersist(t *testing.T) {
	t.Parallel()

	backend := newTemporalNativeBackend(&fakeNativeExecutor{}, nil, failingLifecycleProbe{})

	spec := &agentos.RunSpec{RunID: Run1, AccountID: "acct-1", UserMessage: "hello"}

	if _, err := backend.Start(context.Background(), spec); !errors.Is(err, errNativeLifecycleProbe) {
		t.Fatalf("Start error = %v", err)
	}

	if _, err := backend.Status(context.Background(), Run1); !errors.Is(err, errNativeLifecycleProbe) {
		t.Fatalf("Status error = %v", err)
	}
}

type fakeNativeExecutor struct {
	start            *entity.ExecuteRequest
	status           entity.RunStatus
	userMessageRunID string
	userMessage      orchestration.UserMessageSignal
	pausedRunID      string
	resumedRunID     string
	canceledRunID    string
	stepMutation     *entity.StepMutation
	externalEvent    map[string]any
}

func (e *fakeNativeExecutor) SignalStepModify(_ context.Context, _ string, mutation *entity.StepMutation) error {
	e.stepMutation = mutation

	return nil
}

func (e *fakeNativeExecutor) SignalExternalEvent(_ context.Context, _ string, event map[string]any) error {
	e.externalEvent = event

	return nil
}

func (e *fakeNativeExecutor) StartExecution(_ context.Context, req *entity.ExecuteRequest) (entity.RunStatus, error) {
	e.start = req

	return entity.RunStatus{
		RunID:          req.RunID,
		LifecycleState: "created",
		UpdatedAt:      time.Date(2026, 6, 16, 12, 0, 0, 0, time.UTC),
	}, nil
}

func (e *fakeNativeExecutor) GetStatus(context.Context, string) (entity.RunStatus, error) {
	if e.status.RunID == "" {
		e.status = entity.RunStatus{
			RunID:          "run-1",
			LifecycleState: "running",
			UpdatedAt:      time.Date(2026, 6, 16, 12, 1, 0, 0, time.UTC),
		}
	}

	return e.status, nil
}

func (e *fakeNativeExecutor) Pause(_ context.Context, runID string) error {
	e.pausedRunID = runID

	return nil
}

func (e *fakeNativeExecutor) Resume(_ context.Context, runID string) error {
	e.resumedRunID = runID

	return nil
}

func (e *fakeNativeExecutor) Cancel(_ context.Context, runID string) error {
	e.canceledRunID = runID

	return nil
}

func (e *fakeNativeExecutor) SignalUserMessage(_ context.Context, runID string, message *orchestration.UserMessageSignal) error {
	e.userMessageRunID = runID
	e.userMessage = *message

	return nil
}

// The step-queue mode is driven from outside: a queue is changed while it runs,
// and a waiting step is woken by an event. Both arrive as control-plane signals,
// so both have to reach the executor that owns the workflow.
func TestTemporalNativeBackendSignalStepModify(t *testing.T) {
	t.Parallel()

	executor := &fakeNativeExecutor{}
	backend := newTemporalNativeBackend(executor, nil, nil)

	err := backend.Signal(t.Context(), AgentOSConformanceRun, &agentoscore.Signal{
		Type: agentoscore.SignalStepModify,
		Payload: map[string]any{
			"append_after": "research",
			"insert_steps": []any{
				map[string]any{"id": "follow-up", "type": "agent", "input": map[string]any{"message": "dig"}},
			},
		},
	})
	require.NoError(t, err)

	require.NotNil(t, executor.stepMutation)
	require.Equal(t, "research", executor.stepMutation.AppendAfter)
	require.Len(t, executor.stepMutation.InsertSteps, 1)
	require.Equal(t, "follow-up", executor.stepMutation.InsertSteps[0].ID)
}

// A mutation that changes nothing is refused: applying it would report success
// and leave the caller's mistake to surface as a run that did not do what they
// asked.
func TestTemporalNativeBackendRefusesAnEmptyStepMutation(t *testing.T) {
	t.Parallel()

	executor := &fakeNativeExecutor{}
	backend := newTemporalNativeBackend(executor, nil, nil)

	err := backend.Signal(t.Context(), AgentOSConformanceRun, &agentoscore.Signal{
		Type:    agentoscore.SignalStepModify,
		Payload: map[string]any{},
	})

	require.ErrorIs(t, err, agentoscore.ErrInvalidSignal)
	require.Nil(t, executor.stepMutation)
}

func TestTemporalNativeBackendSignalExternalEvent(t *testing.T) {
	t.Parallel()

	executor := &fakeNativeExecutor{}
	backend := newTemporalNativeBackend(executor, nil, nil)

	err := backend.Signal(t.Context(), AgentOSConformanceRun, &agentoscore.Signal{
		Type:    agentoscore.SignalExternalEvent,
		Payload: map[string]any{"ticket": "TCK-1", "approved": true},
	})
	require.NoError(t, err)

	require.Equal(t, "TCK-1", executor.externalEvent["ticket"])
	require.Equal(t, true, executor.externalEvent["approved"])
}
