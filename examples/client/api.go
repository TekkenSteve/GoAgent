package client

import (
	"context"
	"errors"
	"fmt"
	"net/http"
)

var (
	// errOrchestrationNeedsOneQueue reports a request that did not say what to
	// run.
	errOrchestrationNeedsOneQueue = errors.New("a team_spec or steps is required")
	// errOrchestrationQueueAmbiguous reports a request that said it twice.
	errOrchestrationQueueAmbiguous = errors.New("send a team_spec or steps, not both")
)

// StartRun starts an AgentOS run.
// POST /v1/agentos/runs.
func (c *Client) StartRun(ctx context.Context, req *AgentOSRunRequest) (*RunStatus, error) {
	if req.AccountID == "" {
		req.AccountID = c.accountID
	}

	if req.ProjectID == "" {
		req.ProjectID = c.projectID
	}

	if req.Backend.Kind == "" && req.Backend.Name == "" {
		req.Backend = BackendRef{Kind: "native", Name: "goagent-native"}
	}

	var status RunStatus
	if err := c.Do(ctx, http.MethodPost, "/v1/agentos/runs", &req, &status); err != nil {
		return nil, fmt.Errorf("StartRun: %w", err)
	}

	return &status, nil
}

// GetStatus queries the current state of an AgentOS run.
// GET /v1/agentos/runs/{runID}/status.
func (c *Client) GetStatus(ctx context.Context, runID string) (*RunStatus, error) {
	var status RunStatus
	if err := c.Do(ctx, http.MethodGet, "/v1/agentos/runs/"+runID+"/status", nil, &status); err != nil {
		return nil, fmt.Errorf("GetStatus: %w", err)
	}

	return &status, nil
}

// SignalRun sends a business signal to an AgentOS run.
// POST /v1/agentos/runs/{runID}/signals.
func (c *Client) SignalRun(ctx context.Context, runID string, req AgentOSSignalRequest) error {
	if err := c.Do(ctx, http.MethodPost, "/v1/agentos/runs/"+runID+"/signals", &req, nil); err != nil {
		return fmt.Errorf("SignalRun: %w", err)
	}

	return nil
}

// ControlRun sends a lifecycle operation to an AgentOS run.
// POST /v1/agentos/runs/{runID}/control.
func (c *Client) ControlRun(ctx context.Context, runID string, req AgentOSControlRequest) error {
	if err := c.Do(ctx, http.MethodPost, "/v1/agentos/runs/"+runID+"/control", &req, nil); err != nil {
		return fmt.Errorf("ControlRun: %w", err)
	}

	return nil
}

// ExecuteOrchestration starts the native backend's step-queue mode: a team it
// expands, or a queue the caller authored.
//
// It is an AgentOS run like any other — POST /v1/agentos/runs with the native
// payload — so the run has an identity, appears in the run index, publishes its
// lifecycle, and is addressed by the same status, signal and control routes.
func (c *Client) ExecuteOrchestration(ctx context.Context, req *OrchestrationRequest) (*RunStatus, error) {
	if req.TeamSpec == nil && req.Steps == nil {
		return nil, fmt.Errorf("ExecuteOrchestration: %w", errOrchestrationNeedsOneQueue)
	}

	if req.TeamSpec != nil && req.Steps != nil {
		return nil, fmt.Errorf("ExecuteOrchestration: %w", errOrchestrationQueueAmbiguous)
	}

	projectID := req.ProjectID
	if projectID == "" {
		projectID = c.projectID
	}

	native := map[string]any{}
	if req.TeamSpec != nil {
		native["team_spec"] = req.TeamSpec
	}

	if req.Steps != nil {
		native["steps"] = req.Steps
	}

	run := &AgentOSRunRequest{
		RunID:     req.RunID,
		ProjectID: projectID,
		Backend:   BackendRef{Kind: "native", Name: "goagent-native"},
		Input:     map[string]any{"native": native},
	}

	return c.StartRun(ctx, run)
}

// GetOrchestrationStatus queries the current state of a run started by
// ExecuteOrchestration. It is the control plane's run status.
func (c *Client) GetOrchestrationStatus(ctx context.Context, runID string) (*RunStatus, error) {
	return c.GetStatus(ctx, runID)
}
