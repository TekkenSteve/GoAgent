package temporalexternal

import (
	"context"
	"errors"
	"testing"
	"time"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosruntime/agentosruntimetest"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
)

const (
	agentosExternalRun1 = "agentos-external-run-1"
	Run1                = "run-1"
)

func TestBackendConformance(t *testing.T) {
	t.Parallel()

	probe := &agentosruntimetest.SubscriberProbe{}
	temporalClient := &fakeTemporalClient{
		runID: "temporal-run-1",
		queryValue: &encodedStatus{status: agentos.RunStatus{
			RunID:          "agentos-conformance-run",
			LifecycleState: "running",
			UpdatedAt:      time.Date(2026, 6, 16, 12, 1, 0, 0, time.UTC),
		}},
	}

	config := Config{
		Name:         "langgraph-conformance",
		TaskQueue:    "langgraph-queue",
		WorkflowType: "langgraph.agent.v1",
		QueryType:    "agentos_status",
		Signals: SignalNames{
			Cancel: "cancel",
			Defaults: map[agentoscore.SignalType]string{
				agentoscore.SignalUserMessage: "user_input",
			},
		},
	}

	backend, err := NewBackend(temporalClient, probe, &config)
	if err != nil {
		t.Fatalf("NewBackend: %v", err)
	}

	backend.now = func() time.Time { return time.Date(2026, 6, 16, 12, 0, 0, 0, time.UTC) }

	agentosruntimetest.RunBackendConformance(t, &agentosruntimetest.BackendConformanceCase{
		Name:            "temporal_external",
		Backend:         backend,
		Ref:             backend.config.Ref(),
		StatusState:     "running",
		SubscriberProbe: probe,
	})
}

func TestBackendStartExecutesConfiguredWorkflow(t *testing.T) {
	t.Parallel()

	temporalClient := &fakeTemporalClient{runID: "temporal-run-1"}
	backend := newTestBackend(t, temporalClient, &Config{
		Name:         "langgraph-main",
		TaskQueue:    "langgraph-queue",
		WorkflowType: "langgraph.agent.v1",
		QueryType:    "agentos_status",
	})
	backend.now = func() time.Time { return time.Date(2026, 6, 16, 9, 0, 0, 0, time.UTC) }

	spec := agentos.RunSpec{
		RunID:       "run-1",
		ThreadID:    "thread-1",
		UserMessage: "hello",
		Backend:     backend.config.Ref(),
		Input: map[string]any{
			"topic": "agentos",
		},
	}

	status, err := backend.Start(context.Background(), &spec)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	requireTemporalStartWorkflow(t, temporalClient)
	requireTemporalStartStatus(t, &status)
}

func TestNewBackendRejectsNilConfig(t *testing.T) {
	t.Parallel()

	_, err := NewBackend(&fakeTemporalClient{}, nil, nil)
	if !errors.Is(err, agentoscore.ErrInvalidBackendRef) {
		t.Fatalf("NewBackend nil config error = %v, want ErrInvalidBackendRef", err)
	}
}

func TestBackendRejectsNilRunInputs(t *testing.T) {
	t.Parallel()

	backend := newTestBackend(t, &fakeTemporalClient{}, &Config{
		Name:         "langgraph-main",
		TaskQueue:    "langgraph-queue",
		WorkflowType: "langgraph.agent.v1",
		QueryType:    "agentos_status",
	})

	if _, err := backend.Start(context.Background(), nil); !errors.Is(err, agentoscore.ErrInvalidRunSpec) {
		t.Fatalf("Start nil error = %v, want ErrInvalidRunSpec", err)
	}

	if err := backend.Signal(context.Background(), Run1, nil); !errors.Is(err, agentoscore.ErrInvalidSignal) {
		t.Fatalf("Signal nil error = %v, want ErrInvalidSignal", err)
	}
}

func requireTemporalStartWorkflow(t *testing.T, temporalClient *fakeTemporalClient) {
	t.Helper()

	if temporalClient.startOptions.ID != agentosExternalRun1 {
		t.Fatalf("workflow id = %q", temporalClient.startOptions.ID)
	}

	if temporalClient.startOptions.TaskQueue != "langgraph-queue" {
		t.Fatalf("task queue = %q", temporalClient.startOptions.TaskQueue)
	}

	if temporalClient.workflow != "langgraph.agent.v1" {
		t.Fatalf("workflow type = %#v", temporalClient.workflow)
	}

	input, ok := temporalClient.startArgs[0].(StartInput)
	if !ok {
		t.Fatalf("start arg type = %T", temporalClient.startArgs[0])
	}

	if input.RunID != Run1 || input.ThreadID != "thread-1" || input.Input["topic"] != "agentos" {
		t.Fatalf("unexpected input: %#v", input)
	}
}

func requireTemporalStartStatus(t *testing.T, status *agentos.RunStatus) {
	t.Helper()

	if status.RunID != "run-1" || status.LifecycleState != "created" || status.Reason != "temporal-run-1" {
		t.Fatalf("unexpected status: %#v", status)
	}
}

func TestBackendSignalUsesConfiguredSignalName(t *testing.T) {
	t.Parallel()

	temporalClient := &fakeTemporalClient{}
	backend := newTestBackend(t, temporalClient, &Config{
		Name:         "langgraph-main",
		TaskQueue:    "langgraph-queue",
		WorkflowType: "langgraph.agent.v1",
		QueryType:    "agentos_status",
		Signals: SignalNames{
			Defaults: map[agentoscore.SignalType]string{
				agentoscore.SignalUserMessage: "user_input",
			},
		},
	})

	signal := agentoscore.Signal{
		Type:           agentoscore.SignalUserMessage,
		IdempotencyKey: "idem-1",
		Payload: map[string]any{
			"text": "continue",
		},
	}

	err := backend.Signal(context.Background(), "run-1", &signal)
	if err != nil {
		t.Fatalf("Signal: %v", err)
	}

	if temporalClient.signalWorkflowID != agentosExternalRun1 || temporalClient.signalName != "user_input" {
		t.Fatalf("unexpected signal route: workflow=%q signal=%q", temporalClient.signalWorkflowID, temporalClient.signalName)
	}

	input, ok := temporalClient.signalArg.(SignalInput)
	if !ok {
		t.Fatalf("signal arg type = %T", temporalClient.signalArg)
	}

	if input.Type != agentoscore.SignalUserMessage || input.Payload["text"] != "continue" {
		t.Fatalf("unexpected signal input: %#v", input)
	}
}

func TestBackendControlCancelRequiresConfiguredSignal(t *testing.T) {
	t.Parallel()

	temporalClient := &fakeTemporalClient{}
	backend := newTestBackend(t, temporalClient, &Config{
		Name:         "python-agent",
		TaskQueue:    "python-queue",
		WorkflowType: "python.agent.v1",
		QueryType:    "agentos_status",
	})

	control := agentoscore.ControlRequest{Operation: agentoscore.ControlCancel}

	err := backend.Control(context.Background(), "run-1", &control)
	if !errors.Is(err, agentoscore.ErrInvalidControlOperation) {
		t.Fatalf("Control cancel error = %v, want ErrInvalidControlOperation", err)
	}
}

func TestBackendControlCancelUsesConfiguredSignal(t *testing.T) {
	t.Parallel()

	temporalClient := &fakeTemporalClient{}
	backend := newTestBackend(t, temporalClient, &Config{
		Name:         "python-agent",
		TaskQueue:    "python-queue",
		WorkflowType: "python.agent.v1",
		QueryType:    "agentos_status",
		Signals: SignalNames{
			Cancel: "agentos_cancel",
		},
	})

	control := agentoscore.ControlRequest{Operation: agentoscore.ControlCancel}

	err := backend.Control(context.Background(), "run-1", &control)
	if err != nil {
		t.Fatalf("Control cancel: %v", err)
	}

	if temporalClient.signalWorkflowID != agentosExternalRun1 || temporalClient.signalName != "agentos_cancel" {
		t.Fatalf("unexpected cancel signal route: workflow=%q signal=%q", temporalClient.signalWorkflowID, temporalClient.signalName)
	}
}

func TestBackendStatusUsesQueryWhenConfigured(t *testing.T) {
	t.Parallel()

	temporalClient := &fakeTemporalClient{
		queryValue: &encodedStatus{status: agentos.RunStatus{
			RunID:          "run-1",
			LifecycleState: "paused",
			Progress:       &agentoscore.RunProgress{Current: 7, Total: 9, Label: "checkpoint"},
		}},
	}
	backend := newTestBackend(t, temporalClient, &Config{
		Name:         "langgraph-main",
		TaskQueue:    "langgraph-queue",
		WorkflowType: "langgraph.agent.v1",
		QueryType:    "agentos_status",
	})

	status, err := backend.Status(context.Background(), "run-1")
	if err != nil {
		t.Fatalf("Status: %v", err)
	}

	if temporalClient.queryType != "agentos_status" || status.LifecycleState != "paused" || status.Progress == nil || status.Progress.Current != 7 {
		t.Fatalf("unexpected status/query: status=%#v query=%q", status, temporalClient.queryType)
	}
}

func TestBackendRequiresStatusQuery(t *testing.T) {
	t.Parallel()

	config := Config{
		Name:         "python-agent",
		TaskQueue:    "python-queue",
		WorkflowType: "python.agent.v1",
	}

	_, err := NewBackend(&fakeTemporalClient{}, nil, &config)
	if !errors.Is(err, agentoscore.ErrInvalidBackendRef) {
		t.Fatalf("NewBackend error = %v, want ErrInvalidBackendRef", err)
	}
}

func newTestBackend(t *testing.T, temporalClient *fakeTemporalClient, config *Config) *Backend {
	t.Helper()

	backend, err := NewBackend(temporalClient, nil, config)
	if err != nil {
		t.Fatalf("NewBackend: %v", err)
	}

	backend.now = func() time.Time { return time.Date(2026, 6, 16, 10, 0, 0, 0, time.UTC) }

	return backend
}

type fakeTemporalClient struct {
	runID            string
	startOptions     client.StartWorkflowOptions
	workflow         any
	startArgs        []any
	signalWorkflowID string
	signalName       string
	signalArg        any
	queryType        string
	queryValue       converter.EncodedValue
}

func (c *fakeTemporalClient) ExecuteWorkflow(_ context.Context, options *client.StartWorkflowOptions, workflow any, args ...any) (client.WorkflowRun, error) {
	c.startOptions = *options
	c.workflow = workflow
	c.startArgs = args

	return fakeWorkflowRun{id: options.ID, runID: c.runID}, nil
}

func (c *fakeTemporalClient) SignalWorkflow(_ context.Context, workflowID, _, signalName string, arg any) error {
	c.signalWorkflowID = workflowID
	c.signalName = signalName
	c.signalArg = arg

	return nil
}

func (c *fakeTemporalClient) QueryWorkflow(_ context.Context, _, _, queryType string, _ ...any) (converter.EncodedValue, error) {
	c.queryType = queryType

	return c.queryValue, nil
}

type fakeWorkflowRun struct {
	id    string
	runID string
}

func (r fakeWorkflowRun) GetID() string {
	return r.id
}

func (r fakeWorkflowRun) GetRunID() string {
	return r.runID
}

func (r fakeWorkflowRun) Get(context.Context, any) error {
	return nil
}

func (r fakeWorkflowRun) GetWithOptions(context.Context, any, client.WorkflowRunGetOptions) error {
	return nil
}

type encodedStatus struct {
	status agentos.RunStatus
}

func (e *encodedStatus) HasValue() bool {
	return true
}

func (e *encodedStatus) Get(valuePtr any) error {
	status, ok := valuePtr.(*agentos.RunStatus)
	if !ok {
		return nil
	}

	*status = e.status

	return nil
}
