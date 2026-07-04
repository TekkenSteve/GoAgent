package temporal

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos"
	agentfwconfig "github.com/TekkenSteve/GoAgent/internal/agentfw/config"
	"github.com/TekkenSteve/GoAgent/internal/entity"
)

const (
	Run1    = "run-1"
	Thread1 = "thread-1"
	Acct1   = "acct-1"
	Proj1   = "proj-1"
	Hello   = "hello"
	Idem1   = "idem-1"
)

func TestExecutionRequestFromRunSpec(t *testing.T) {
	t.Parallel()

	requestedAt := time.Date(2026, 6, 14, 12, 0, 0, 0, time.UTC)

	spec := agentos.RunSpec{
		RunID:          Run1,
		ThreadID:       Thread1,
		AccountID:      Acct1,
		ProjectID:      Proj1,
		AgentID:        "agent-1",
		ModelRef:       "model-1",
		SystemPrompt:   "system",
		UserMessage:    Hello,
		IdempotencyKey: Idem1,
		RequestedAt:    requestedAt,
		Backend:        agentos.BackendRef{Kind: agentos.BackendKindNative, Name: agentos.BackendNameGoAgentNative},
	}

	req, err := executionRequestFromRunSpec(&spec)
	if err != nil {
		t.Fatalf("executionRequestFromRunSpec: %v", err)
	}

	assertExecutionRequestMapped(t, req, requestedAt)
}

func assertExecutionRequestMapped(t *testing.T, req *entity.ExecuteRequest, requestedAt time.Time) {
	t.Helper()

	assertEqual(t, req.RunID, Run1, "run id")
	assertEqual(t, req.ThreadID, Thread1, "thread id")
	assertEqual(t, req.AccountID, Acct1, "account id")
	assertEqual(t, req.ProjectID, Proj1, "project id")
	assertEqual(t, req.AgentID, "agent-1", "agent id")
	assertEqual(t, req.ModelRef, "model-1", "model ref")
	assertEqual(t, req.SystemPrompt, "system", "system prompt")
	assertEqual(t, req.UserMessage, Hello, "user message")
	assertEqual(t, req.IdempotencyKey, Idem1, "idempotency key")

	if !req.RequestedAt.Equal(requestedAt) {
		t.Fatalf("requested at = %v, want %v", req.RequestedAt, requestedAt)
	}
}

func assertEqual[T comparable](t *testing.T, got, want T, name string) {
	t.Helper()

	if got != want {
		t.Fatalf("%s = %v, want %v", name, got, want)
	}
}

func TestRunStatusFromEntity(t *testing.T) {
	t.Parallel()

	updatedAt := time.Date(2026, 6, 14, 12, 30, 0, 0, time.UTC)

	status := entity.RunStatus{
		RunID:          Run1,
		LifecycleState: "running",
		Step:           3,
		Reason:         "waiting",
		UpdatedAt:      updatedAt,
	}
	got := runStatusFromEntity(&status)

	want := agentos.RunStatus{
		RunID:          Run1,
		LifecycleState: "running",
		Progress:       &agentos.RunProgress{Current: 3},
		Reason:         "waiting",
		UpdatedAt:      updatedAt,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("status mismatch: got %#v want %#v", got, want)
	}
}

func TestControlOperationToEntity(t *testing.T) {
	t.Parallel()

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

func TestTemporalConfigRequiresExplicitTaskQueues(t *testing.T) {
	t.Parallel()

	got := temporalConfig(&RuntimeConfig{})
	if err := got.TaskQueues.Validate(); !errors.Is(err, agentfwconfig.ErrTemporalTaskQueuesInvalid) {
		t.Fatalf("Temporal task queue validation error = %v, want %v", err, agentfwconfig.ErrTemporalTaskQueuesInvalid)
	}
}

func TestTemporalExternalConfig(t *testing.T) {
	t.Parallel()

	cfg := ExternalBackendConfig{
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
	}
	got := temporalExternalConfig(&cfg)

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

func TestGRPCBackendConfig(t *testing.T) {
	t.Parallel()

	cfg := GRPCBackendConfig{
		Name:      "opencode",
		Target:    "opencode-runtime:9090",
		Authority: "agentos.example",
		Insecure:  true,
		Service:   "agentos.v1.AgentBackend",
		Methods: GRPCMethodNames{
			Start:   "StartRun",
			Signal:  "SignalRun",
			Control: "ControlRun",
			Status:  "StatusRun",
		},
	}
	got := grpcBackendConfig(&cfg)

	if got.Name != "opencode" ||
		got.Target != "opencode-runtime:9090" ||
		got.Authority != "agentos.example" ||
		!got.Insecure ||
		got.Service != "agentos.v1.AgentBackend" ||
		got.Methods.Start != "StartRun" ||
		got.Methods.Signal != "SignalRun" ||
		got.Methods.Control != "ControlRun" ||
		got.Methods.Status != "StatusRun" {
		t.Fatalf("unexpected grpc backend config: %#v", got)
	}
}
