package temporalexternal

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/TekkenSteve/GoAgent/agentos"
	agentosruntime "github.com/TekkenSteve/GoAgent/internal/usecase/agentosruntime"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
)

var (
	errTemporalExternalNilClient    = errors.New("temporal external backend: nil temporal client")
	errTemporalExternalNoSubscriber = errors.New("temporal external backend: event subscriber is not configured")
)

// TemporalClient is the subset of Temporal client.Client used by the external backend.
type TemporalClient interface {
	ExecuteWorkflow(ctx context.Context, options *client.StartWorkflowOptions, workflow any, args ...any) (client.WorkflowRun, error)
	SignalWorkflow(ctx context.Context, workflowID, runID, signalName string, arg any) error
	QueryWorkflow(ctx context.Context, workflowID, runID, queryType string, args ...any) (converter.EncodedValue, error)
}

type temporalSDKClient struct {
	client.Client
}

// NewTemporalClient adapts Temporal's SDK client to the pointer-oriented port
// used by this backend.
func NewTemporalClient(c client.Client) TemporalClient {
	if c == nil {
		return nil
	}

	return temporalSDKClient{Client: c}
}

func (c temporalSDKClient) ExecuteWorkflow(ctx context.Context, options *client.StartWorkflowOptions, workflow any, args ...any) (client.WorkflowRun, error) {
	return c.Client.ExecuteWorkflow(ctx, *options, workflow, args...)
}

// Backend starts and controls external Temporal workflows that implement AgentOS protocol.
type Backend struct {
	client     TemporalClient
	subscriber agentosruntime.EventSubscriber
	config     Config
	now        func() time.Time
}

// NewBackend creates a temporal_external backend.
func NewBackend(temporalClient TemporalClient, subscriber agentosruntime.EventSubscriber, config *Config) (*Backend, error) {
	if temporalClient == nil {
		return nil, errTemporalExternalNilClient
	}

	if err := config.validate(); err != nil {
		return nil, err
	}

	return &Backend{
		client:     temporalClient,
		subscriber: subscriber,
		config:     *config,
		now:        func() time.Time { return time.Now().UTC() },
	}, nil
}

// Start starts an external Temporal workflow.
func (b *Backend) Start(ctx context.Context, spec *agentos.RunSpec) (agentos.RunStatus, error) {
	if spec == nil {
		return agentos.RunStatus{}, fmt.Errorf("%w: run spec is required", agentos.ErrInvalidRunSpec)
	}

	if spec.RunID == "" {
		return agentos.RunStatus{}, fmt.Errorf("%w: run id is required", agentos.ErrInvalidRunSpec)
	}

	if spec.Backend != b.config.Ref() {
		return agentos.RunStatus{}, fmt.Errorf(
			"%w: run backend %s/%s does not match temporal external backend %s/%s",
			agentos.ErrInvalidBackendRef,
			spec.Backend.Kind,
			spec.Backend.Name,
			b.config.Ref().Kind,
			b.config.Ref().Name,
		)
	}

	options := client.StartWorkflowOptions{
		ID:        workflowID(spec.RunID),
		TaskQueue: b.config.TaskQueue,
	}

	run, err := b.client.ExecuteWorkflow(ctx, &options, b.config.WorkflowType, startInputFromSpec(spec))
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
func (b *Backend) Signal(ctx context.Context, runID string, signal *agentos.Signal) error {
	if runID == "" {
		return fmt.Errorf("%w: run id is required", agentos.ErrInvalidRunSpec)
	}

	if signal == nil {
		return fmt.Errorf("%w: signal is required", agentos.ErrInvalidSignal)
	}

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
func (b *Backend) Control(ctx context.Context, runID string, control *agentos.ControlRequest) error {
	if err := agentos.ValidateControlRequest(control); err != nil {
		return err
	}

	switch control.Operation {
	case agentos.ControlPause:
		return b.controlBySignal(ctx, runID, agentos.SignalControlPause, control)
	case agentos.ControlResume:
		return b.controlBySignal(ctx, runID, agentos.SignalControlResume, control)
	case agentos.ControlCancel:
		return b.controlBySignal(ctx, runID, agentos.SignalControlCancel, control)
	default:
		return fmt.Errorf("%w: %s", agentos.ErrInvalidControlOperation, control.Operation)
	}
}

// Status returns external workflow status from the backend-owned AgentOS query.
func (b *Backend) Status(ctx context.Context, runID string) (agentos.RunStatus, error) {
	return b.queryStatus(ctx, runID)
}

// Subscribe returns the shared AgentOS event stream for the run.
func (b *Backend) Subscribe(ctx context.Context, scope agentos.StreamScope) (agentos.Subscription, error) {
	if b.subscriber == nil {
		return nil, errTemporalExternalNoSubscriber
	}

	return b.subscriber.SubscribeAgentOS(ctx, scope)
}

// Capabilities reports the baseline temporal_external backend features.
func (b *Backend) Capabilities() agentosruntime.BackendCapabilities {
	return agentosruntime.BackendCapabilities{
		SupportsSignal:            true,
		SupportsSignalUserMessage: b.config.Signals.Defaults[agentos.SignalUserMessage] != "",
		SupportsPause:             b.config.Signals.Pause != "",
		SupportsResume:            b.config.Signals.Resume != "",
		SupportsCancel:            b.config.Signals.Cancel != "",
		SupportsStreaming:         b.subscriber != nil,
	}
}

func (b *Backend) controlBySignal(ctx context.Context, runID string, signalType agentos.SignalType, control *agentos.ControlRequest) error {
	if _, err := b.signalName(signalType); err != nil {
		return fmt.Errorf("%w: control %s is not mapped for backend %s", agentos.ErrInvalidControlOperation, signalType, b.config.Name)
	}

	signal := agentos.Signal{
		Type:           signalType,
		IdempotencyKey: control.IdempotencyKey,
		SentAt:         control.RequestedAt,
	}

	return b.Signal(ctx, runID, &signal)
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
	case agentos.SignalPlanNodeRetry, agentos.SignalPlanApprove, agentos.SignalPlanReject,
		agentos.SignalUserMessage, agentos.SignalUserApproval, agentos.SignalUserReject,
		agentos.SignalToolResult, agentos.SignalHumanFeedback, agentos.SignalConfigPatch,
		agentos.SignalMemoryPatch:
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
