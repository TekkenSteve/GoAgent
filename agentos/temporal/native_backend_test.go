package temporal

import (
	"context"
	"testing"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos"
	"github.com/TekkenSteve/GoAgent/internal/agentfw/orchestration"
	"github.com/TekkenSteve/GoAgent/internal/entity"
)

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
	userMessageRunID string
	userMessage      orchestration.UserMessageSignal
}

func (e *fakeNativeExecutor) StartExecution(context.Context, *entity.ExecuteRequest) (entity.RunStatus, error) {
	return entity.RunStatus{}, nil
}

func (e *fakeNativeExecutor) GetStatus(context.Context, string) (entity.RunStatus, error) {
	return entity.RunStatus{}, nil
}

func (e *fakeNativeExecutor) Pause(context.Context, string) error {
	return nil
}

func (e *fakeNativeExecutor) Resume(context.Context, string) error {
	return nil
}

func (e *fakeNativeExecutor) Cancel(context.Context, string) error {
	return nil
}

func (e *fakeNativeExecutor) SignalUserMessage(_ context.Context, runID string, message orchestration.UserMessageSignal) error {
	e.userMessageRunID = runID
	e.userMessage = message

	return nil
}
