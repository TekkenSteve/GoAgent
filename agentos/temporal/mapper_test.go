package temporal

import (
	"testing"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos"
	"github.com/TekkenSteve/GoAgent/internal/entity"
)

func TestExecutionRequestFromRunSpec(t *testing.T) {
	requestedAt := time.Date(2026, 6, 14, 12, 0, 0, 0, time.UTC)

	req, err := executionRequestFromRunSpec(agentos.RunSpec{
		RunID:          "run-1",
		ThreadID:       "thread-1",
		AccountID:      "acct-1",
		ProjectID:      "proj-1",
		AgentID:        "agent-1",
		ModelRef:       "model-1",
		SystemPrompt:   "system",
		UserMessage:    "hello",
		IdempotencyKey: "idem-1",
		RequestedAt:    requestedAt,
		Backend:        agentos.DefaultBackendRef(),
	})
	if err != nil {
		t.Fatalf("executionRequestFromRunSpec: %v", err)
	}

	if req.RunID != "run-1" ||
		req.ThreadID != "thread-1" ||
		req.AccountID != "acct-1" ||
		req.ProjectID != "proj-1" ||
		req.AgentID != "agent-1" ||
		req.ModelRef != "model-1" ||
		req.SystemPrompt != "system" ||
		req.UserMessage != "hello" ||
		req.IdempotencyKey != "idem-1" ||
		!req.RequestedAt.Equal(requestedAt) {
		t.Fatalf("unexpected request mapping: %#v", req)
	}
}

func TestRunStatusFromEntity(t *testing.T) {
	updatedAt := time.Date(2026, 6, 14, 12, 30, 0, 0, time.UTC)

	got := runStatusFromEntity(entity.RunStatus{
		RunID:          "run-1",
		LifecycleState: "running",
		Step:           3,
		Reason:         "waiting",
		UpdatedAt:      updatedAt,
	})

	want := agentos.RunStatus{
		RunID:          "run-1",
		LifecycleState: "running",
		Step:           3,
		Reason:         "waiting",
		UpdatedAt:      updatedAt,
	}
	if got != want {
		t.Fatalf("status mismatch: got %#v want %#v", got, want)
	}
}

func TestControlOperationToEntity(t *testing.T) {
	tests := map[agentos.ControlOperation]entity.ControlOperation{
		agentos.ControlPause:  entity.ControlPause,
		agentos.ControlResume: entity.ControlResume,
		agentos.ControlCancel: entity.ControlCancel,
	}

	for input, want := range tests {
		got, err := controlOperationToEntity(input)
		if err != nil {
			t.Fatalf("controlOperationToEntity(%q): %v", input, err)
		}
		if got != want {
			t.Fatalf("controlOperationToEntity(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestTemporalExternalConfig(t *testing.T) {
	got := temporalExternalConfig(ExternalBackendConfig{
		Name:         "langgraph-main",
		TaskQueue:    "langgraph-queue",
		WorkflowType: "langgraph.agent.v1",
		QueryType:    "agentos_status",
		Signals: ExternalSignalNames{
			Pause:  "pause",
			Resume: "resume",
			Cancel: "cancel",
			Defaults: map[agentos.SignalType]string{
				agentos.SignalUserMessage: "user_input",
			},
		},
	})

	if got.Name != "langgraph-main" ||
		got.TaskQueue != "langgraph-queue" ||
		got.WorkflowType != "langgraph.agent.v1" ||
		got.QueryType != "agentos_status" ||
		got.Signals.Pause != "pause" ||
		got.Signals.Resume != "resume" ||
		got.Signals.Cancel != "cancel" ||
		got.Signals.Defaults[agentos.SignalUserMessage] != "user_input" {
		t.Fatalf("unexpected temporal external config: %#v", got)
	}
}
