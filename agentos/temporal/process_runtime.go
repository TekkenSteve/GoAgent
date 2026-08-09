package temporal

import (
	"context"
	"errors"
	"fmt"
	"time"

	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
	agentosproc "github.com/TekkenSteve/GoAgent/agentos/process"
	"github.com/TekkenSteve/GoAgent/internal/agentfw/orchestration"
	"github.com/TekkenSteve/GoAgent/internal/pkg/postgres"
	temporalrepo "github.com/TekkenSteve/GoAgent/internal/repo/persistent"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosprocess"
	"go.temporal.io/sdk/client"
	sdktemporal "go.temporal.io/sdk/temporal"
)

type processRuntime struct {
	temporalClient planTemporalClient
	closeTemporal  bool
	postgres       *postgres.Postgres
	processRuntime *agentosprocess.Runtime
	taskQueue      string
	taskQueues     *TaskQueues
}

var (
	errProcessRuntimeConfigRequired              = errors.New("agentos temporal process runtime: config is required")
	errProcessRuntimeNilTemporalClient           = errors.New("agentos temporal process runtime: nil temporal client")
	errProcessRuntimeTemporalClientNotConfigured = errors.New("agentos temporal process runtime: temporal client is not configured")
	errProcessRuntimeNotConfigured               = errors.New("agentos temporal process runtime: runtime is not configured")
)

// NewProcessRuntime creates the default Temporal implementation of agentosproc.Runtime.
func NewProcessRuntime(ctx context.Context, cfg *RuntimeConfig) (agentosproc.Runtime, error) {
	if err := validateProcessRuntimeConfig(cfg); err != nil {
		return nil, err
	}

	fwTemporal := temporalConfig(cfg)

	c, err := dialTemporalClient(&fwTemporal)
	if err != nil {
		return nil, fmt.Errorf("agentos temporal process runtime client: %w", err)
	}

	rt, err := newProcessRuntimeWithClient(ctx, cfg, newPlanTemporalClient(c), true)
	if err != nil {
		c.Close()

		return nil, err
	}

	return rt, nil
}

// NewProcessRuntimeWithClient adapts an existing Temporal client to agentosproc.Runtime.
func NewProcessRuntimeWithClient(ctx context.Context, cfg *RuntimeConfig, c client.Client) (agentosproc.Runtime, error) {
	if cfg == nil {
		return nil, errProcessRuntimeConfigRequired
	}

	if c == nil {
		return nil, errProcessRuntimeNilTemporalClient
	}

	return newProcessRuntimeWithClient(ctx, cfg, newPlanTemporalClient(c), false)
}

func newProcessRuntimeWithClient(_ context.Context, cfg *RuntimeConfig, c planTemporalClient, closeTemporal bool) (*processRuntime, error) {
	if err := validateProcessRuntimeConfig(cfg); err != nil {
		return nil, err
	}

	pg, err := newRuntimePostgres(cfg)
	if err != nil {
		return nil, fmt.Errorf("agentos temporal process runtime postgres: %w", err)
	}

	store := temporalrepo.NewAgentOSProcessRepo(pg)

	processUsecase, err := agentosprocess.NewRuntime(store, store)
	if err != nil {
		pg.Close()

		return nil, err
	}

	fwTemporal := temporalConfig(cfg)

	return newProcessRuntimeWithStores(c, closeTemporal, pg, processUsecase, fwTemporal.TaskQueues.ProcessControl, &cfg.TemporalTaskQueues), nil
}

func newProcessRuntimeWithStores(
	c planTemporalClient,
	closeTemporal bool,
	pg *postgres.Postgres,
	processUsecase *agentosprocess.Runtime,
	taskQueue string,
	taskQueues *TaskQueues,
) *processRuntime {
	return &processRuntime{
		temporalClient: c,
		closeTemporal:  closeTemporal,
		postgres:       pg,
		processRuntime: processUsecase,
		taskQueue:      taskQueue,
		taskQueues:     taskQueues,
	}
}

func (r *processRuntime) StartProcess(ctx context.Context, spec *agentosproc.Spec) (agentosproc.Status, error) {
	if r == nil || r.processRuntime == nil {
		return agentosproc.Status{}, errProcessRuntimeNotConfigured
	}

	prepared := cloneProcessSpec(spec)

	status, err := r.processRuntime.StartProcess(ctx, &prepared)
	if err != nil {
		return agentosproc.Status{}, err
	}

	if processWorkflowTerminal(status.LifecycleState) {
		return status, nil
	}

	return status, r.executeProcessWorkflow(ctx, &prepared)
}

func (r *processRuntime) StatusProcess(ctx context.Context, ref agentosproc.Ref) (agentosproc.Status, error) {
	if r == nil || r.processRuntime == nil {
		return agentosproc.Status{}, errProcessRuntimeNotConfigured
	}

	return r.processRuntime.StatusProcess(ctx, ref)
}

func (r *processRuntime) DescribeProcess(ctx context.Context, ref agentosproc.Ref) (agentosproc.Description, error) {
	if r == nil || r.processRuntime == nil {
		return agentosproc.Description{}, errProcessRuntimeNotConfigured
	}

	return r.processRuntime.DescribeProcess(ctx, ref)
}

func (r *processRuntime) ListProcesses(ctx context.Context, scope *agentosproc.Scope) ([]agentosproc.Status, error) {
	if r == nil || r.processRuntime == nil {
		return nil, errProcessRuntimeNotConfigured
	}

	return r.processRuntime.ListProcesses(ctx, scope)
}

func (r *processRuntime) SignalProcess(ctx context.Context, ref agentosproc.Ref, signal *agentoscore.Signal) error {
	if r == nil || r.processRuntime == nil {
		return errProcessRuntimeNotConfigured
	}

	if err := r.authorizeProcess(ctx, ref); err != nil {
		return err
	}

	prepared := cloneProcessSignal(signal)

	return r.signalProcessWorkflow(ctx, ref, ProcessSignalName, &prepared)
}

func (r *processRuntime) ControlProcess(ctx context.Context, ref agentosproc.Ref, control *agentoscore.ControlRequest) error {
	if r == nil || r.processRuntime == nil {
		return errProcessRuntimeNotConfigured
	}

	if err := r.authorizeProcess(ctx, ref); err != nil {
		return err
	}

	prepared := cloneProcessControlRequest(control)

	return r.signalProcessWorkflow(ctx, ref, ProcessControlSignalName, &prepared)
}

func (r *processRuntime) SubscribeProcess(ctx context.Context, scope *agentosproc.StreamScope) (agentoscore.Subscription, error) {
	if r == nil || r.processRuntime == nil {
		return nil, errProcessRuntimeNotConfigured
	}

	return r.processRuntime.SubscribeProcess(ctx, scope)
}

func (r *processRuntime) ListProcessEvents(ctx context.Context, scope *agentosproc.EventScope) ([]agentosproc.Event, error) {
	if r == nil || r.processRuntime == nil {
		return nil, errProcessRuntimeNotConfigured
	}

	return r.processRuntime.ListProcessEvents(ctx, scope)
}

func (r *processRuntime) Close() error {
	if r == nil {
		return nil
	}

	if r.closeTemporal && r.temporalClient != nil {
		r.temporalClient.Close()
	}

	if r.postgres != nil {
		r.postgres.Close()
	}

	return nil
}

func (r *processRuntime) authorizeProcess(ctx context.Context, ref agentosproc.Ref) error {
	_, err := r.processRuntime.StatusProcess(ctx, ref)

	return err
}

func (r *processRuntime) executeProcessWorkflow(ctx context.Context, spec *agentosproc.Spec) error {
	if r.temporalClient == nil {
		return errProcessRuntimeTemporalClientNotConfigured
	}

	if err := r.taskQueues.Validate(); err != nil {
		return err
	}

	options := client.StartWorkflowOptions{
		ID:               processWorkflowID(spec.ProcessID),
		TaskQueue:        r.taskQueue,
		SearchAttributes: orchestration.SearchAttributesForRun(spec.ProcessID, "running"),
	}

	_, err := r.temporalClient.ExecuteWorkflow(ctx, &options, ProcessWorkflowName, &processWorkflowInput{
		Spec:            *spec,
		WorkflowVersion: currentProcessWorkflowVersion,
		TaskQueues: processTaskQueues{
			ProcessActivity: r.taskQueues.ProcessActivity,
		},
	})
	if err != nil && !sdktemporal.IsWorkflowExecutionAlreadyStartedError(err) {
		return fmt.Errorf("agentos temporal process runtime - start process workflow: %w", err)
	}

	return nil
}

func (r *processRuntime) signalProcessWorkflow(ctx context.Context, ref agentosproc.Ref, signalName string, arg any) error {
	if r.temporalClient == nil {
		return errProcessRuntimeTemporalClientNotConfigured
	}

	if err := r.temporalClient.SignalWorkflow(ctx, processWorkflowID(ref.ProcessID), "", signalName, arg); err != nil {
		return fmt.Errorf("agentos temporal process runtime - signal process workflow: %w", err)
	}

	return nil
}

func validateProcessRuntimeConfig(cfg *RuntimeConfig) error {
	if cfg == nil {
		return errProcessRuntimeConfigRequired
	}

	if cfg.PostgresURL == "" {
		return ErrRuntimePostgresURLRequired
	}

	return cfg.TemporalTaskQueues.Validate()
}

func cloneProcessSpec(spec *agentosproc.Spec) agentosproc.Spec {
	if spec == nil {
		return agentosproc.Spec{}
	}

	clone := *spec
	clone.Inputs = cloneAnyMap(spec.Inputs)
	clone.Metadata = cloneStringMap(spec.Metadata)

	clone.Timers = append([]agentosproc.TimerSpec(nil), spec.Timers...)
	for i := range clone.Timers {
		clone.Timers[i].Payload = cloneAnyMap(clone.Timers[i].Payload)
	}

	return clone
}

func cloneProcessSignal(signal *agentoscore.Signal) agentoscore.Signal {
	if signal == nil {
		return agentoscore.Signal{}
	}

	clone := *signal

	clone.Payload = cloneAnyMap(signal.Payload)
	if clone.SentAt.IsZero() {
		clone.SentAt = time.Now().UTC()
	}

	return clone
}

func cloneProcessControlRequest(control *agentoscore.ControlRequest) agentoscore.ControlRequest {
	if control == nil {
		return agentoscore.ControlRequest{}
	}

	clone := *control

	clone.Metadata = cloneStringMap(control.Metadata)
	if clone.RequestedAt.IsZero() {
		clone.RequestedAt = time.Now().UTC()
	}

	return clone
}

var _ agentosproc.Runtime = (*processRuntime)(nil)
