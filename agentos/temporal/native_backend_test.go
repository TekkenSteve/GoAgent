package temporal

import (
	"context"
	"testing"
	"time"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	"github.com/TekkenSteve/GoAgent/internal/agentfw/orchestration"
	"github.com/TekkenSteve/GoAgent/internal/entity"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosruntime/agentosruntimetest"
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
	backend := newTemporalNativeBackend(executor, probe)

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
	backend := newTemporalNativeBackend(executor, nil)
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

	backend := newTemporalNativeBackend(&fakeNativeExecutor{}, nil)

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

	backend := newTemporalNativeBackend(&fakeNativeExecutor{}, nil)

	capabilities := backend.Capabilities()
	if !capabilities.SupportsSignalUserMessage {
		t.Fatalf("SupportsSignalUserMessage = false")
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
