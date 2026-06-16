package temporalexternal

import (
	"context"
	"testing"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosruntime/agentosruntimetest"
	enumspb "go.temporal.io/api/enums/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	workflowservicepb "go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
)

func TestBackendConformance(t *testing.T) {
	probe := &agentosruntimetest.SubscriberProbe{}
	temporalClient := &fakeTemporalClient{
		runID: "temporal-run-1",
		queryValue: encodedStatus{status: agentos.RunStatus{
			RunID:          "agentos-conformance-run",
			LifecycleState: "running",
			UpdatedAt:      time.Date(2026, 6, 16, 12, 1, 0, 0, time.UTC),
		}},
	}
	backend, err := NewBackend(temporalClient, probe, Config{
		Name:         "langgraph-conformance",
		TaskQueue:    "langgraph-queue",
		WorkflowType: "langgraph.agent.v1",
		QueryType:    "agentos_status",
		Signals: SignalNames{
			Cancel: "cancel",
			Defaults: map[agentos.SignalType]string{
				agentos.SignalUserMessage: "user_input",
			},
		},
	})
	if err != nil {
		t.Fatalf("NewBackend: %v", err)
	}
	backend.now = func() time.Time { return time.Date(2026, 6, 16, 12, 0, 0, 0, time.UTC) }

	agentosruntimetest.RunBackendConformance(t, agentosruntimetest.BackendConformanceCase{
		Name:            "temporal_external",
		Backend:         backend,
		Ref:             backend.config.Ref(),
		StatusState:     "running",
		SubscriberProbe: probe,
	})
}

func TestBackendStartExecutesConfiguredWorkflow(t *testing.T) {
	temporalClient := &fakeTemporalClient{runID: "temporal-run-1"}
	backend := newTestBackend(t, temporalClient, Config{
		Name:         "langgraph-main",
		TaskQueue:    "langgraph-queue",
		WorkflowType: "langgraph.agent.v1",
	})
	backend.now = func() time.Time { return time.Date(2026, 6, 16, 9, 0, 0, 0, time.UTC) }

	status, err := backend.Start(context.Background(), agentos.RunSpec{
		RunID:       "run-1",
		ThreadID:    "thread-1",
		UserMessage: "hello",
		Backend:     backend.config.Ref(),
		Input: map[string]any{
			"topic": "agentos",
		},
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	if temporalClient.startOptions.ID != "agentos-external-run-1" {
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
	if input.RunID != "run-1" || input.ThreadID != "thread-1" || input.Input["topic"] != "agentos" {
		t.Fatalf("unexpected input: %#v", input)
	}
	if status.RunID != "run-1" || status.LifecycleState != "created" || status.Reason != "temporal-run-1" {
		t.Fatalf("unexpected status: %#v", status)
	}
}

func TestBackendSignalUsesConfiguredSignalName(t *testing.T) {
	temporalClient := &fakeTemporalClient{}
	backend := newTestBackend(t, temporalClient, Config{
		Name:         "langgraph-main",
		TaskQueue:    "langgraph-queue",
		WorkflowType: "langgraph.agent.v1",
		Signals: SignalNames{
			Defaults: map[agentos.SignalType]string{
				agentos.SignalUserMessage: "user_input",
			},
		},
	})

	err := backend.Signal(context.Background(), "run-1", agentos.Signal{
		Type:           agentos.SignalUserMessage,
		IdempotencyKey: "idem-1",
		Payload: map[string]any{
			"text": "continue",
		},
	})
	if err != nil {
		t.Fatalf("Signal: %v", err)
	}

	if temporalClient.signalWorkflowID != "agentos-external-run-1" || temporalClient.signalName != "user_input" {
		t.Fatalf("unexpected signal route: workflow=%q signal=%q", temporalClient.signalWorkflowID, temporalClient.signalName)
	}
	input, ok := temporalClient.signalArg.(SignalInput)
	if !ok {
		t.Fatalf("signal arg type = %T", temporalClient.signalArg)
	}
	if input.Type != agentos.SignalUserMessage || input.Payload["text"] != "continue" {
		t.Fatalf("unexpected signal input: %#v", input)
	}
}

func TestBackendControlCancelFallsBackToTemporalCancel(t *testing.T) {
	temporalClient := &fakeTemporalClient{}
	backend := newTestBackend(t, temporalClient, Config{
		Name:         "python-agent",
		TaskQueue:    "python-queue",
		WorkflowType: "python.agent.v1",
	})

	if err := backend.Control(context.Background(), "run-1", agentos.ControlCancel); err != nil {
		t.Fatalf("Control cancel: %v", err)
	}

	if temporalClient.cancelWorkflowID != "agentos-external-run-1" {
		t.Fatalf("cancel workflow id = %q", temporalClient.cancelWorkflowID)
	}
}

func TestBackendStatusUsesQueryWhenConfigured(t *testing.T) {
	temporalClient := &fakeTemporalClient{
		queryValue: encodedStatus{status: agentos.RunStatus{
			RunID:          "run-1",
			LifecycleState: "paused",
			Step:           7,
		}},
	}
	backend := newTestBackend(t, temporalClient, Config{
		Name:         "langgraph-main",
		TaskQueue:    "langgraph-queue",
		WorkflowType: "langgraph.agent.v1",
		QueryType:    "agentos_status",
	})

	status, err := backend.Status(context.Background(), "run-1")
	if err != nil {
		t.Fatalf("Status: %v", err)
	}

	if temporalClient.queryType != "agentos_status" || status.LifecycleState != "paused" || status.Step != 7 {
		t.Fatalf("unexpected status/query: status=%#v query=%q", status, temporalClient.queryType)
	}
}

func TestBackendStatusFallsBackToDescribe(t *testing.T) {
	temporalClient := &fakeTemporalClient{
		describeResponse: &workflowservicepb.DescribeWorkflowExecutionResponse{
			WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
				Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			},
		},
	}
	backend := newTestBackend(t, temporalClient, Config{
		Name:         "python-agent",
		TaskQueue:    "python-queue",
		WorkflowType: "python.agent.v1",
	})

	status, err := backend.Status(context.Background(), "run-1")
	if err != nil {
		t.Fatalf("Status: %v", err)
	}

	if status.LifecycleState != "completed" {
		t.Fatalf("lifecycle = %q", status.LifecycleState)
	}
}

func newTestBackend(t *testing.T, temporalClient *fakeTemporalClient, config Config) *Backend {
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
	workflow         interface{}
	startArgs        []interface{}
	signalWorkflowID string
	signalName       string
	signalArg        interface{}
	cancelWorkflowID string
	queryType        string
	queryValue       converter.EncodedValue
	describeResponse *workflowservicepb.DescribeWorkflowExecutionResponse
}

func (c *fakeTemporalClient) ExecuteWorkflow(_ context.Context, options client.StartWorkflowOptions, workflow interface{}, args ...interface{}) (client.WorkflowRun, error) {
	c.startOptions = options
	c.workflow = workflow
	c.startArgs = args

	return fakeWorkflowRun{id: options.ID, runID: c.runID}, nil
}

func (c *fakeTemporalClient) SignalWorkflow(_ context.Context, workflowID string, _ string, signalName string, arg interface{}) error {
	c.signalWorkflowID = workflowID
	c.signalName = signalName
	c.signalArg = arg

	return nil
}

func (c *fakeTemporalClient) CancelWorkflow(_ context.Context, workflowID string, _ string) error {
	c.cancelWorkflowID = workflowID

	return nil
}

func (c *fakeTemporalClient) QueryWorkflow(_ context.Context, _ string, _ string, queryType string, _ ...interface{}) (converter.EncodedValue, error) {
	c.queryType = queryType

	return c.queryValue, nil
}

func (c *fakeTemporalClient) DescribeWorkflowExecution(context.Context, string, string) (*workflowservicepb.DescribeWorkflowExecutionResponse, error) {
	if c.describeResponse == nil {
		return &workflowservicepb.DescribeWorkflowExecutionResponse{}, nil
	}

	return c.describeResponse, nil
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

func (r fakeWorkflowRun) Get(context.Context, interface{}) error {
	return nil
}

func (r fakeWorkflowRun) GetWithOptions(context.Context, interface{}, client.WorkflowRunGetOptions) error {
	return nil
}

type encodedStatus struct {
	status agentos.RunStatus
}

func (e encodedStatus) HasValue() bool {
	return true
}

func (e encodedStatus) Get(valuePtr interface{}) error {
	status, ok := valuePtr.(*agentos.RunStatus)
	if !ok {
		return nil
	}
	*status = e.status

	return nil
}
