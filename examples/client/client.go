// Package client provides a typed Go SDK for the GoAgent REST API.
//
// Usage:
//
//	c := client.New("http://localhost:8080", "e2e-test-account")
//	status, err := c.ExecuteAgent(ctx, client.ExecuteRequest{
//	    RunID: "my-run", UserMessage: "Hello",
//	})
//	status, err = c.WaitForCompletion(ctx, "my-run", 2*time.Second, 4*time.Minute)
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

const (
	LifecycleStateCompleted = "completed"
	LifecycleStateFailed    = "failed"
	LifecycleStateCanceled  = "canceled"
)

var ErrWaitTimeout = errors.New("wait timeout")

const defaultHTTPTimeout = 30 * time.Second

// Client is a lightweight HTTP client for the GoAgent REST API.
// Stateless — safe for concurrent use.
type Client struct {
	baseURL   string
	accountID string
	http      *http.Client
}

// New creates a Client targeting the given base URL.
// accountID is the default account sent with every request.
func New(baseURL, accountID string) *Client {
	return &Client{
		baseURL:   strings.TrimRight(baseURL, "/"),
		accountID: accountID,
		http:      &http.Client{Timeout: defaultHTTPTimeout},
	}
}

// Do sends a JSON request and decodes the response into result (if non-nil).
// path is a URL path like "/v1/agent/execute".
func (c *Client) Do(ctx context.Context, method, path string, body, result any) error {
	var bodyReader io.Reader

	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("marshal request: %w", err)
		}

		bodyReader = bytes.NewReader(data)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, bodyReader)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("http request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return &APIError{Code: resp.StatusCode, Body: fmt.Sprintf("failed to read body: %v", readErr)}
		}

		return &APIError{Code: resp.StatusCode, Body: string(respBody)}
	}

	if result != nil {
		if err := json.NewDecoder(resp.Body).Decode(result); err != nil {
			return fmt.Errorf("decode response: %w", err)
		}
	}

	return nil
}

// APIError represents a non-2xx HTTP response.
type APIError struct {
	Code int
	Body string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("API error %d: %s", e.Code, e.Body)
}

func (c *Client) pollStatus(ctx context.Context, runID, pathPrefix, desc string, interval, timeout time.Duration) (*RunStatus, error) {
	deadline := time.Now().Add(timeout)

	for {
		var status RunStatus

		err := c.Do(ctx, http.MethodGet, pathPrefix+runID, nil, &status)
		if err != nil {
			fmt.Fprintf(os.Stderr, "  poll error: %v\n", err)
		} else {
			fmt.Fprintf(os.Stdout, "  lifecycle_state=%s step=%d\n", status.LifecycleState, status.Step)

			if status.LifecycleState == LifecycleStateCompleted ||
				status.LifecycleState == LifecycleStateFailed ||
				status.LifecycleState == LifecycleStateCanceled {
				return &status, nil
			}
		}

		if time.Now().After(deadline) {
			return nil, fmt.Errorf("%w: %s %s after %v", ErrWaitTimeout, desc, runID, timeout)
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(interval):
		}
	}
}

// WaitForOrchestrationCompletion polls GET /v1/orchestration/status/{runID} until a
// terminal state (completed, failed, canceled) is reached.
func (c *Client) WaitForOrchestrationCompletion(ctx context.Context, runID string, interval, timeout time.Duration) (*RunStatus, error) {
	return c.pollStatus(ctx, runID, "/v1/orchestration/status/", "orchestration run", interval, timeout)
}

// WaitForCompletion polls GET /v1/agent/status/{runID} until a terminal state
// (completed, failed, canceled) is reached, then returns the final status.
func (c *Client) WaitForCompletion(ctx context.Context, runID string, interval, timeout time.Duration) (*RunStatus, error) {
	return c.pollStatus(ctx, runID, "/v1/agent/status/", "run", interval, timeout)
}
