package client

import (
	"context"
	"fmt"
	"net/http"
)

// StartRun starts an AgentOS run.
// POST /v1/agentos/runs.
func (c *Client) StartRun(ctx context.Context, req AgentOSRunRequest) (*RunStatus, error) {
	if req.AccountID == "" {
		req.AccountID = c.accountID
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

// ExecuteOrchestration starts an orchestration workflow from a TeamSpec or step queue.
// POST /v1/orchestration/execute.
func (c *Client) ExecuteOrchestration(ctx context.Context, req OrchestrationRequest) (*RunStatus, error) {
	if req.AccountID == "" {
		req.AccountID = c.accountID
	}

	var status RunStatus
	if err := c.Do(ctx, http.MethodPost, "/v1/orchestration/execute", &req, &status); err != nil {
		return nil, fmt.Errorf("ExecuteOrchestration: %w", err)
	}

	return &status, nil
}

// GetOrchestrationStatus queries the current state of an orchestration workflow.
// GET /v1/orchestration/status/{runID}.
func (c *Client) GetOrchestrationStatus(ctx context.Context, runID string) (*RunStatus, error) {
	var status RunStatus
	if err := c.Do(ctx, http.MethodGet, "/v1/orchestration/status/"+runID, nil, &status); err != nil {
		return nil, fmt.Errorf("GetOrchestrationStatus: %w", err)
	}

	return &status, nil
}
