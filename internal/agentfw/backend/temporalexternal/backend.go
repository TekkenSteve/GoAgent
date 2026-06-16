package temporalexternal

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos"
	agentfwbackend "github.com/TekkenSteve/GoAgent/internal/agentfw/backend"
	enumspb "go.temporal.io/api/enums/v1"
	workflowservicepb "go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
)

// TemporalClient is the subset of Temporal client.Client used by the external backend.
type TemporalClient interface {
	ExecuteWorkflow(ctx context.Context, options client.StartWorkflowOptions, workflow interface{}, args ...interface{}) (client.WorkflowRun, error)
	SignalWorkflow(ctx context.Context, workflowID string, runID string, signalName string, arg interface{}) error
	CancelWorkflow(ctx context.Context, workflowID string, runID string) error
	QueryWorkflow(ctx context.Context, workflowID string, runID string, queryType string, args ...interface{}) (converter.EncodedValue, error)
	DescribeWorkflowExecution(ctx context.Context, workflowID string, runID string) (*workflowservicepb.DescribeWorkflowExecutionResponse, error)
}

// Backend starts and controls external Temporal workflows that implement AgentOS protocol.
type Backend struct {
	client     TemporalClient
	subscriber agentfwbackend.EventSubscriber
	config     Config
	now        func() time.Time
}

// NewBackend creates a temporal_external backend.
func NewBackend(temporalClient TemporalClient, subscriber agentfwbackend.EventSubscriber, config Config) (*Backend, error) {
	if temporalClient == nil {
		return nil, errors.New("temporal external backend: nil temporal client")
	}
	if err := config.validate(); err != nil {
		return nil, err
	}

	return &Backend{
		client:     temporalClient,
		subscriber: subscriber,
		config:     config,
		now:        func() time.Time { return time.Now().UTC() },
	}, nil
}

// Start starts an external Temporal workflow.
func (b *Backend) Start(ctx context.Context, spec agentos.RunSpec) (agentos.RunStatus, error) {
	if spec.RunID == "" {
		return agentos.RunStatus{}, fmt.Errorf("%w: run id is required", agentos.ErrInvalidRunSpec)
	}
	if spec.Backend != b.config.Ref() {
		return agentos.RunStatus{}, fmt.Errorf("%w: run backend %s/%s does not match temporal external backend %s/%s",
			agentos.ErrInvalidBackendRef,
			spec.Backend.Kind,
			spec.Backend.Name,
			b.config.Ref().Kind,
			b.config.Ref().Name,
		)
	}

	run, err := b.client.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        workflowID(spec.RunID),
		TaskQueue: b.config.TaskQueue,
	}, b.config.WorkflowType, startInputFromSpec(spec))
	if err != nil {
		return agentos.RunStatus{}, fmt.Errorf("temporal external backend - start workflow: %w", err)
	}

	return agentos.RunStatus{
		RunID:          spec.RunID,
		LifecycleState: "created",
		Reason:         run.GetRunID(),
		UpdatedAt:      b.now(),
	}, nil
}

// Signal translates an AgentOS signal into a Temporal workflow signal.
func (b *Backend) Signal(ctx context.Context, runID string, signal agentos.Signal) error {
	if signal.Type == "" {
		return fmt.Errorf("%w: type is required", agentos.ErrInvalidSignal)
	}

	signalName, err := b.signalName(signal.Type)
	if err != nil {
		return err
	}

	if err := b.client.SignalWorkflow(ctx, workflowID(runID), "", signalName, signalInputFromSignal(signal)); err != nil {
		return fmt.Errorf("temporal external backend - signal workflow: %w", err)
	}

	return nil
}

// Control translates lifecycle operations to external workflow signal/cancel operations.
func (b *Backend) Control(ctx context.Context, runID string, op agentos.ControlOperation) error {
	switch op {
	case agentos.ControlPause:
		return b.controlBySignal(ctx, runID, agentos.SignalControlPause)
	case agentos.ControlResume:
		return b.controlBySignal(ctx, runID, agentos.SignalControlResume)
	case agentos.ControlCancel:
		if b.config.Signals.Cancel != "" {
			return b.controlBySignal(ctx, runID, agentos.SignalControlCancel)
		}
		if err := b.client.CancelWorkflow(ctx, workflowID(runID), ""); err != nil {
			return fmt.Errorf("temporal external backend - cancel workflow: %w", err)
		}

		return nil
	default:
		return fmt.Errorf("%w: %s", agentos.ErrInvalidControlOperation, op)
	}
}

// Status returns external workflow status from optional query or Temporal describe.
func (b *Backend) Status(ctx context.Context, runID string) (agentos.RunStatus, error) {
	if b.config.QueryType != "" {
		status, err := b.queryStatus(ctx, runID)
		if err == nil {
			return status, nil
		}
	}

	desc, err := b.client.DescribeWorkflowExecution(ctx, workflowID(runID), "")
	if err != nil {
		return agentos.RunStatus{}, fmt.Errorf("temporal external backend - describe workflow: %w", err)
	}

	return runStatusFromDescription(runID, desc, b.now()), nil
}

// Subscribe returns the shared AgentOS event stream for the run.
func (b *Backend) Subscribe(ctx context.Context, scope agentos.StreamScope) (agentos.Subscription, error) {
	if b.subscriber == nil {
		return nil, errors.New("temporal external backend: event subscriber is not configured")
	}

	return b.subscriber.SubscribeAgentOS(ctx, scope)
}

// Capabilities reports the baseline temporal_external backend features.
func (b *Backend) Capabilities() agentfwbackend.BackendCapabilities {
	return agentfwbackend.BackendCapabilities{
		SupportsSignal:    true,
		SupportsPause:     b.config.Signals.Pause != "",
		SupportsResume:    b.config.Signals.Resume != "",
		SupportsCancel:    true,
		SupportsStreaming: b.subscriber != nil,
	}
}

func (b *Backend) controlBySignal(ctx context.Context, runID string, signalType agentos.SignalType) error {
	if _, err := b.signalName(signalType); err != nil {
		return fmt.Errorf("%w: control %s is not mapped for backend %s", agentos.ErrInvalidControlOperation, signalType, b.config.Name)
	}

	return b.Signal(ctx, runID, agentos.Signal{Type: signalType})
}

func (b *Backend) signalName(signalType agentos.SignalType) (string, error) {
	switch signalType {
	case agentos.SignalControlPause:
		if b.config.Signals.Pause != "" {
			return b.config.Signals.Pause, nil
		}
	case agentos.SignalControlResume:
		if b.config.Signals.Resume != "" {
			return b.config.Signals.Resume, nil
		}
	case agentos.SignalControlCancel:
		if b.config.Signals.Cancel != "" {
			return b.config.Signals.Cancel, nil
		}
	}

	if b.config.Signals.Defaults != nil {
		if name := b.config.Signals.Defaults[signalType]; name != "" {
			return name, nil
		}
	}

	return "", fmt.Errorf("%w: signal %s is not mapped for backend %s", agentos.ErrInvalidSignal, signalType, b.config.Name)
}

func (b *Backend) queryStatus(ctx context.Context, runID string) (agentos.RunStatus, error) {
	value, err := b.client.QueryWorkflow(ctx, workflowID(runID), "", b.config.QueryType)
	if err != nil {
		return agentos.RunStatus{}, fmt.Errorf("temporal external backend - query workflow: %w", err)
	}

	var status agentos.RunStatus
	if err := value.Get(&status); err != nil {
		return agentos.RunStatus{}, fmt.Errorf("temporal external backend - decode status query: %w", err)
	}
	if status.RunID == "" {
		status.RunID = runID
	}
	if status.UpdatedAt.IsZero() {
		status.UpdatedAt = b.now()
	}

	return status, nil
}

func workflowID(runID string) string {
	return "agentos-external-" + runID
}

func runStatusFromDescription(runID string, desc *workflowservicepb.DescribeWorkflowExecutionResponse, now time.Time) agentos.RunStatus {
	state := "unknown"
	if info := desc.GetWorkflowExecutionInfo(); info != nil {
		state = lifecycleFromTemporalStatus(info.GetStatus())
	}

	return agentos.RunStatus{
		RunID:          runID,
		LifecycleState: state,
		UpdatedAt:      now,
	}
}

func lifecycleFromTemporalStatus(status enumspb.WorkflowExecutionStatus) string {
	switch status {
	case enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING:
		return "running"
	case enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED:
		return "completed"
	case enumspb.WORKFLOW_EXECUTION_STATUS_FAILED:
		return "failed"
	case enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED:
		return "canceled"
	case enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED:
		return "canceled"
	case enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT:
		return "failed"
	default:
		return "unknown"
	}
}
