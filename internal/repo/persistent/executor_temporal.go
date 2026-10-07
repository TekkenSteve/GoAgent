package persistent

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/TekkenSteve/GoAgent/internal/agentfw/agent"
	"github.com/TekkenSteve/GoAgent/internal/agentfw/config"
	"github.com/TekkenSteve/GoAgent/internal/agentfw/orchestration"
	"github.com/TekkenSteve/GoAgent/internal/agentfw/team"
	"github.com/TekkenSteve/GoAgent/internal/entity"
	"go.temporal.io/sdk/client"
)

var errExecutorTemporalConfigRequired = errors.New("ExecutorTemporal - config is required")

// ExecutorTemporal runs agent and orchestration workflows on a Temporal cluster.
type ExecutorTemporal struct {
	client client.Client
	opts   options
}

type options struct {
	workflowName       string
	workflowIDPrefix   string
	workflowTaskQueues *orchestration.WorkflowTaskQueues
	delegation         orchestration.DelegateBudget
	runTimeout         time.Duration
}

// NewExecutorTemporal creates an ExecutorTemporal bound to the given Temporal client and configuration.
func NewExecutorTemporal(c client.Client, cfg *config.Temporal) (*ExecutorTemporal, error) {
	if cfg == nil {
		return nil, errExecutorTemporalConfigRequired
	}

	if err := cfg.TaskQueues.Validate(); err != nil {
		return nil, err
	}

	taskQueues := orchestration.WorkflowTaskQueues{
		NativeControl: cfg.TaskQueues.NativeControl,
		NativeLLM:     cfg.TaskQueues.NativeLLM,
		NativeTool:    cfg.TaskQueues.NativeTool,
		Stream:        cfg.TaskQueues.Stream,
	}

	return &ExecutorTemporal{
		client: c,
		opts: options{
			workflowName:       orchestration.AgentWorkflowName,
			workflowIDPrefix:   "agentfw-run-",
			workflowTaskQueues: &taskQueues,
			// The root starts the tree's allowance; every child inherits it.
			delegation: orchestration.DelegateBudget{
				MaxDepth:    cfg.MaxDelegateDepth,
				TokenBudget: cfg.DelegateTokenBudget,
			},
			runTimeout: cfg.RunWorkflowTimeout,
		},
	}, nil
}

// StartExecution starts the native run for the given request.
//
// The native backend has two execution modes and the request selects one: a
// step queue or a team runs the step-queue interpreter, which starts one child
// agent run per agent step; anything else runs the single-agent loop. Both are
// this backend, so both use the same workflow id — status, control and signals
// then address a run the same way whichever mode it is in.
func (r *ExecutorTemporal) StartExecution(ctx context.Context, req *entity.ExecuteRequest) (entity.RunStatus, error) {
	if req.HasStepQueue() {
		return r.startStepQueue(ctx, req)
	}

	workflowID := r.opts.workflowIDPrefix + req.RunID

	input := orchestration.AgentWorkflowInput{
		RunID:                 req.RunID,
		AccountID:             req.AccountID,
		ProjectID:             req.ProjectID,
		SystemPrompt:          req.SystemPrompt,
		Message:               req.UserMessage,
		Config:                entity.LLMConfig{Model: req.ModelRef},
		MCPServerConfigs:      req.MCPServerConfigs,
		AwaitUserInput:        req.AwaitUserInput,
		AwaitUserInputTimeout: time.Duration(req.AwaitUserInputTimeoutSeconds) * time.Second,
		TaskQueues:            *r.opts.workflowTaskQueues,
		Delegate:              r.opts.delegation,
	}

	opts := client.StartWorkflowOptions{
		ID:                       workflowID,
		TaskQueue:                r.opts.workflowTaskQueues.NativeControl,
		WorkflowExecutionTimeout: r.opts.runTimeout,
		TypedSearchAttributes:    orchestration.SearchAttributesForRun(req.RunID, "running"),
	}

	_, err := r.client.ExecuteWorkflow(ctx, opts, r.opts.workflowName, &input)
	if err != nil {
		return entity.RunStatus{}, fmt.Errorf("ExecutorTemporal - StartExecution - r.client.ExecuteWorkflow: %w", err)
	}

	return entity.RunStatus{
		RunID:          req.RunID,
		LifecycleState: string(entity.LifecycleCreated),
		Step:           0,
		UpdatedAt:      time.Now(),
	}, nil
}

// startStepQueue starts the step-queue interpreter for a run whose request
// carries a queue or a team.
func (r *ExecutorTemporal) startStepQueue(ctx context.Context, req *entity.ExecuteRequest) (entity.RunStatus, error) {
	steps := req.Steps

	if req.TeamSpec != nil && len(steps) == 0 {
		// The team is expanded here, before the workflow starts: the queue the
		// workflow executes is a value in its input, which is what keeps the
		// expansion deterministic and out of workflow code.
		expanded, err := team.Expand(req.TeamSpec, agent.NewRegistry())
		if err != nil {
			return entity.RunStatus{}, fmt.Errorf("ExecutorTemporal - startStepQueue - team.Expand: %w", err)
		}

		steps = expanded
	}

	workflowID := r.opts.workflowIDPrefix + req.RunID

	opts := client.StartWorkflowOptions{
		ID:                       workflowID,
		TaskQueue:                r.opts.workflowTaskQueues.NativeControl,
		WorkflowExecutionTimeout: r.opts.runTimeout,
		TypedSearchAttributes:    orchestration.SearchAttributesForRun(req.RunID, "running"),
	}

	_, err := r.client.ExecuteWorkflow(ctx, opts, orchestration.OrchestrationWorkflowName, &orchestration.WorkflowInput{
		Input: entity.OrchestrationInput{
			RunID:          req.RunID,
			AccountID:      req.AccountID,
			ProjectID:      req.ProjectID,
			Steps:          steps,
			SystemPrompt:   req.SystemPrompt,
			Message:        req.UserMessage,
			MaxDepth:       req.MaxDepth,
			ContinuePolicy: req.ContinuePolicy,
		},
		TaskQueues: *r.opts.workflowTaskQueues,
	})
	if err != nil {
		return entity.RunStatus{}, fmt.Errorf("ExecutorTemporal - startStepQueue - r.client.ExecuteWorkflow: %w", err)
	}

	return entity.RunStatus{
		RunID:          req.RunID,
		LifecycleState: string(entity.LifecycleCreated),
		Step:           0,
		UpdatedAt:      time.Now(),
	}, nil
}

// SignalStepModify changes a running step queue.
func (r *ExecutorTemporal) SignalStepModify(ctx context.Context, runID string, mutation *entity.StepMutation) error {
	workflowID := r.opts.workflowIDPrefix + runID

	if err := r.client.SignalWorkflow(ctx, workflowID, "", orchestration.StepModifySignal, mutation); err != nil {
		return fmt.Errorf("ExecutorTemporal - SignalStepModify - r.client.SignalWorkflow: %w", err)
	}

	return nil
}

// SignalExternalEvent delivers an outside event to a waiting step.
func (r *ExecutorTemporal) SignalExternalEvent(ctx context.Context, runID string, event map[string]any) error {
	workflowID := r.opts.workflowIDPrefix + runID

	if err := r.client.SignalWorkflow(ctx, workflowID, "", orchestration.ExternalEventSignal, event); err != nil {
		return fmt.Errorf("ExecutorTemporal - SignalExternalEvent - r.client.SignalWorkflow: %w", err)
	}

	return nil
}

// GetStatus queries the agent workflow's current run status.
func (r *ExecutorTemporal) GetStatus(ctx context.Context, runID string) (entity.RunStatus, error) {
	workflowID := r.opts.workflowIDPrefix + runID

	resp, err := r.client.QueryWorkflow(ctx, workflowID, "", orchestration.QueryRunStatus)
	if err != nil {
		return entity.RunStatus{}, fmt.Errorf("ExecutorTemporal - GetStatus - r.client.QueryWorkflow: %w", err)
	}

	// Deserialize into orchestration.RunStatus first (no JSON tags → matches Temporal's
	// serialized field names exactly), then convert to the API-facing entity type.
	var orchStatus orchestration.RunStatus
	if err := resp.Get(&orchStatus); err != nil {
		return entity.RunStatus{}, fmt.Errorf("ExecutorTemporal - GetStatus - resp.Get: %w", err)
	}

	return entity.RunStatus{
		RunID:          orchStatus.RunID,
		LifecycleState: orchStatus.LifecycleState,
		Step:           orchStatus.Step,
		Reason:         orchStatus.Reason,
		UpdatedAt:      orchStatus.UpdatedAt,
	}, nil
}

// StartOrchestration starts the orchestration workflow for the given input.
func (r *ExecutorTemporal) StartOrchestration(ctx context.Context, input *entity.OrchestrationInput) (entity.RunStatus, error) {
	workflowID := "orch-" + r.opts.workflowIDPrefix + input.RunID

	opts := client.StartWorkflowOptions{
		ID:                    workflowID,
		TaskQueue:             r.opts.workflowTaskQueues.NativeControl,
		TypedSearchAttributes: orchestration.SearchAttributesForRun(input.RunID, "running"),
	}

	_, err := r.client.ExecuteWorkflow(ctx, opts, orchestration.OrchestrationWorkflowName, &orchestration.WorkflowInput{
		Input:      *input,
		TaskQueues: *r.opts.workflowTaskQueues,
	})
	if err != nil {
		return entity.RunStatus{}, fmt.Errorf("ExecutorTemporal - StartOrchestration - r.client.ExecuteWorkflow: %w", err)
	}

	return entity.RunStatus{
		RunID:          input.RunID,
		LifecycleState: string(entity.LifecycleCreated),
		Step:           0,
		UpdatedAt:      time.Now(),
	}, nil
}

// GetOrchestrationStatus queries the orchestration workflow's current status.
func (r *ExecutorTemporal) GetOrchestrationStatus(ctx context.Context, runID string) (entity.RunStatus, error) {
	workflowID := "orch-" + r.opts.workflowIDPrefix + runID

	resp, err := r.client.QueryWorkflow(ctx, workflowID, "", "query-run-status")
	if err != nil {
		return entity.RunStatus{}, fmt.Errorf("ExecutorTemporal - GetOrchestrationStatus - r.client.QueryWorkflow: %w", err)
	}

	var orchStatus orchestration.Status
	if err := resp.Get(&orchStatus); err != nil {
		return entity.RunStatus{}, fmt.Errorf("ExecutorTemporal - GetOrchestrationStatus - resp.Get: %w", err)
	}

	return entity.RunStatus{
		RunID:          orchStatus.RunID,
		LifecycleState: orchStatus.State,
		Step:           orchStatus.Round,
		UpdatedAt:      time.Now(),
	}, nil
}

// Pause signals the agent workflow to pause.
func (r *ExecutorTemporal) Pause(ctx context.Context, runID string) error {
	workflowID := r.opts.workflowIDPrefix + runID

	if err := r.client.SignalWorkflow(ctx, workflowID, "", orchestration.AgentCommandSignal, "pause"); err != nil {
		return fmt.Errorf("ExecutorTemporal - Pause - r.client.SignalWorkflow: %w", err)
	}

	return nil
}

// Resume signals the agent workflow to resume.
func (r *ExecutorTemporal) Resume(ctx context.Context, runID string) error {
	workflowID := r.opts.workflowIDPrefix + runID

	if err := r.client.SignalWorkflow(ctx, workflowID, "", orchestration.AgentCommandSignal, "resume"); err != nil {
		return fmt.Errorf("ExecutorTemporal - Resume - r.client.SignalWorkflow: %w", err)
	}

	return nil
}

// Cancel signals the agent workflow to cancel.
func (r *ExecutorTemporal) Cancel(ctx context.Context, runID string) error {
	workflowID := r.opts.workflowIDPrefix + runID

	if err := r.client.SignalWorkflow(ctx, workflowID, "", orchestration.AgentCommandSignal, "cancel"); err != nil {
		return fmt.Errorf("ExecutorTemporal - Cancel - r.client.SignalWorkflow: %w", err)
	}

	return nil
}

// SignalUserMessage delivers a user message signal to the running agent workflow.
func (r *ExecutorTemporal) SignalUserMessage(ctx context.Context, runID string, message *orchestration.UserMessageSignal) error {
	workflowID := r.opts.workflowIDPrefix + runID

	if err := r.client.SignalWorkflow(ctx, workflowID, "", orchestration.AgentMessageSignal, message); err != nil {
		return fmt.Errorf("ExecutorTemporal - SignalUserMessage - r.client.SignalWorkflow: %w", err)
	}

	return nil
}
