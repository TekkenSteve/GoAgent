// Package client provides a typed Go SDK for the GoAgent REST API.
//
// Usage:
//
//	c := client.New("http://localhost:8080", "e2e-test-account")
//	status, err := c.StartRun(ctx, client.AgentOSRunRequest{
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
	// LifecycleStateCompleted is the lifecycle state of a run that finished successfully.
	LifecycleStateCompleted = "completed"
	// LifecycleStateFailed is the lifecycle state of a run that failed.
	LifecycleStateFailed = "failed"
	// LifecycleStateCanceled is the lifecycle state of a run canceled before completion.
	LifecycleStateCanceled = "canceled"
	// LifecycleStateWaitingInput is the lifecycle state of a run waiting for business input.
	LifecycleStateWaitingInput = "waiting_input"
)

// ErrWaitTimeout is returned when polling for a run status exceeds the caller-provided timeout.
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
// path is a URL path like "/v1/agentos/runs".
func (c *Client) Do(ctx context.Context, method, path string, body, result any) (err error) {
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

	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}()

	if !is2xx(resp.StatusCode) {
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

func is2xx(code int) bool {
	return code >= 200 && code < 300
}

func writePollError(err error) error {
	if _, werr := fmt.Fprintf(os.Stderr, "  poll error: %v\n", err); werr != nil {
		return fmt.Errorf("write poll error: %w", werr)
	}

	return nil
}

func pollOnce(status *RunStatus, err error) (stable bool, werr error) {
	if err != nil {
		if werr := writePollError(err); werr != nil {
			return false, werr
		}

		return false, nil
	}

	if werr := writePollStatus(status); werr != nil {
		return false, werr
	}

	return isStableRunState(status.LifecycleState), nil
}

func writePollStatus(status *RunStatus) error {
	if _, werr := fmt.Fprintf(os.Stdout, "  lifecycle_state=%s step=%d\n", status.LifecycleState, status.Step); werr != nil {
		return fmt.Errorf("write poll status: %w", werr)
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

		stable, werr := pollOnce(&status, err)
		if werr != nil {
			return nil, werr
		}

		if stable {
			return &status, nil
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

// WaitForCompletion polls GET /v1/agentos/runs/{runID}/status until the run reaches
// a stable state: completed, failed, canceled, or waiting_input.
func (c *Client) WaitForCompletion(ctx context.Context, runID string, interval, timeout time.Duration) (*RunStatus, error) {
	deadline := time.Now().Add(timeout)

	for {
		status, err := c.GetStatus(ctx, runID)

		stable, werr := pollOnce(status, err)
		if werr != nil {
			return nil, werr
		}

		if stable {
			return status, nil
		}

		if time.Now().After(deadline) {
			return nil, fmt.Errorf("%w: run %s after %v", ErrWaitTimeout, runID, timeout)
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(interval):
		}
	}
}

func isStableRunState(state string) bool {
	return state == LifecycleStateCompleted ||
		state == LifecycleStateFailed ||
		state == LifecycleStateCanceled ||
		state == LifecycleStateWaitingInput
}
