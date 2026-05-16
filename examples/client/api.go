package client

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

// ExecuteAgent starts a ReAct agent run.
// POST /v1/agent/execute
func (c *Client) ExecuteAgent(ctx context.Context, req ExecuteRequest) (*RunStatus, error) {
	if req.AccountID == "" {
		req.AccountID = c.accountID
	}
	var status RunStatus
	if err := c.Do(ctx, http.MethodPost, "/v1/agent/execute", &req, &status); err != nil {
		return nil, fmt.Errorf("ExecuteAgent: %w", err)
	}
	return &status, nil
}

// GetStatus queries the current state of an agent run.
// GET /v1/agent/status/{runID}
func (c *Client) GetStatus(ctx context.Context, runID string) (*RunStatus, error) {
	var status RunStatus
	if err := c.Do(ctx, http.MethodGet, "/v1/agent/status/"+runID, nil, &status); err != nil {
		return nil, fmt.Errorf("GetStatus: %w", err)
	}
	return &status, nil
}

// ListMessages fetches conversation messages for a completed run.
// GET /v1/agent/{runID}/messages
func (c *Client) ListMessages(ctx context.Context, runID string) ([]json.RawMessage, error) {
	var resp MessagesResponse
	if err := c.Do(ctx, http.MethodGet, "/v1/agent/"+runID+"/messages", nil, &resp); err != nil {
		return nil, fmt.Errorf("ListMessages: %w", err)
	}
	return resp.Data, nil
}

// ExecuteOrchestration starts an orchestration workflow from a TeamSpec or step queue.
// POST /v1/orchestration/execute
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
// GET /v1/orchestration/status/{runID}
func (c *Client) GetOrchestrationStatus(ctx context.Context, runID string) (*RunStatus, error) {
	var status RunStatus
	if err := c.Do(ctx, http.MethodGet, "/v1/orchestration/status/"+runID, nil, &status); err != nil {
		return nil, fmt.Errorf("GetOrchestrationStatus: %w", err)
	}
	return &status, nil
}
