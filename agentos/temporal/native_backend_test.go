package temporal

import (
	"context"
	"testing"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos"
	"github.com/TekkenSteve/GoAgent/internal/agentfw/orchestration"
	"github.com/TekkenSteve/GoAgent/internal/entity"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosruntime/agentosruntimetest"
)

func TestTemporalNativeBackendConformance(t *testing.T) {
	probe := &agentosruntimetest.SubscriberProbe{}
	executor := &fakeNativeExecutor{
		status: entity.RunStatus{
			RunID:          "agentos-conformance-run",
			LifecycleState: "running",
			UpdatedAt:      time.Date(2026, 6, 16, 12, 1, 0, 0, time.UTC),
		},
	}
	backend := newTemporalNativeBackend(executor, probe)

	agentosruntimetest.RunBackendConformance(t, agentosruntimetest.BackendConformanceCase{
		Name:            "native",
		Backend:         backend,
		Ref:             agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative},
		StatusState:     "running",
		SubscriberProbe: probe,
	})

	if executor.start == nil ||
		executor.start.RunID != "agentos-conformance-run" ||
		executor.start.UserMessage != "hello" ||
		executor.userMessageRunID != "agentos-conformance-run" ||
		executor.canceledRunID != "agentos-conformance-run" {
		t.Fatalf("native backend calls were not routed through the shared contract: %#v", executor)
	}
}

func TestTemporalNativeBackendSignalUserMessage(t *testing.T) {
	executor := &fakeNativeExecutor{}
	backend := newTemporalNativeBackend(executor, nil)
	sentAt := time.Date(2026, 6, 16, 12, 0, 0, 0, time.UTC)

	err := backend.Signal(context.Background(), "run-1", agentos.Signal{
		Type:           agentos.SignalUserMessage,
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
	})
	if err != nil {
		t.Fatalf("Signal: %v", err)
	}

	if executor.userMessageRunID != "run-1" {
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
	backend := newTemporalNativeBackend(&fakeNativeExecutor{}, nil)

	err := backend.Signal(context.Background(), "run-1", agentos.Signal{
		Type:    agentos.SignalUserMessage,
		Payload: map[string]any{},
	})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestTemporalNativeBackendCapabilitiesIncludeUserMessage(t *testing.T) {
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

func (e *fakeNativeExecutor) SignalUserMessage(_ context.Context, runID string, message orchestration.UserMessageSignal) error {
	e.userMessageRunID = runID
	e.userMessage = message

	return nil
}
