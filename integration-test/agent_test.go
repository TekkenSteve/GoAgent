package integration_test

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/TekkenSteve/GoAgent/internal/entity"
	"github.com/goccy/go-json"
)

const waitingInput = "waiting_input"

// runStatus is the JSON response from /v1/agentos/runs endpoints.
type runStatus struct {
	RunID          string `json:"run_id"`
	LifecycleState string `json:"lifecycle_state"`
	Progress       struct {
		Current int `json:"current"`
	} `json:"progress"`
}

// executeAgentRun creates an agent run and returns the run status. The account
// the run belongs to is the token's subject (see integration_test.go), not a
// request field.
func executeAgentRun(t *testing.T, runID string) runStatus {
	t.Helper()

	body := agentOSStartBody(runID, "Hello, this is a test message")

	ctx, cancel := context.WithTimeout(t.Context(), requestTimeout)
	defer cancel()

	resp, err := doWebRequestWithTimeout(ctx, http.MethodPost, basePathV1()+"/agentos/runs", bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("executeAgentRun: request failed: %v", err)
	}

	t.Cleanup(func() {
		if err := resp.Body.Close(); err != nil {
			t.Errorf("close response body: %v", err)
		}
	})

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("executeAgentRun: expected 202, got %d", resp.StatusCode)
	}

	var status runStatus
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		t.Fatalf("executeAgentRun: decode failed: %v", err)
	}

	return status
}

// waitForRunCompletion polls the run status until it reaches a terminal state.
func waitForRunCompletion(t *testing.T, runID string) runStatus {
	t.Helper()

	url := basePathV1() + "/agentos/runs/" + runID + "/status"

	for range 30 {
		ctx, cancel := context.WithTimeout(t.Context(), requestTimeout)
		resp, err := doWebRequestWithTimeout(ctx, http.MethodGet, url, nil)

		cancel()

		if err != nil {
			t.Fatalf("waitForRunCompletion: request failed: %v", err)
		}

		var status runStatus
		if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
			if closeErr := resp.Body.Close(); closeErr != nil {
				t.Errorf("close response body: %v", closeErr)
			}

			t.Fatalf("waitForRunCompletion: decode failed: %v", err)
		}

		if err := resp.Body.Close(); err != nil {
			t.Errorf("close response body: %v", err)
		}

		if status.LifecycleState == waitingInput ||
			status.LifecycleState == string(entity.LifecycleCompleted) ||
			status.LifecycleState == string(entity.LifecycleFailed) ||
			status.LifecycleState == string(entity.LifecycleCanceled) {
			return status
		}

		time.Sleep(time.Second)
	}

	t.Fatalf("waitForRunCompletion: timed out waiting for run %s", runID)

	return runStatus{}
}

// HTTP POST: /v1/agentos/runs.
func TestHTTPAgentOSStartV1(t *testing.T) {
	t.Parallel()

	runID := fmt.Sprintf("e2e-exec-%d", time.Now().UnixNano())

	tests := []struct {
		description string
		runID       string
		message     string
		expected    int
	}{
		{
			description: "success",
			runID:       runID,
			message:     "Hello, this is a test message",
			expected:    http.StatusAccepted,
		},
		{
			description: "empty run_id",
			runID:       "",
			message:     "Hello",
			expected:    http.StatusBadRequest,
		},
		{
			description: "empty message",
			runID:       runID + "_nomsg",
			message:     "",
			expected:    http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			t.Parallel()
			testExecuteAgentRequest(t, tt.runID, tt.message, tt.expected)
		})
	}
}

// testExecuteAgentRequest sends an AgentOS start request and asserts the
// response status code and (for successful requests) the run status body.
func testExecuteAgentRequest(t *testing.T, runID, message string, expectedStatus int) {
	t.Helper()

	body := agentOSStartBody(runID, message)

	ctx, cancel := context.WithTimeout(t.Context(), requestTimeout)
	defer cancel()

	resp, err := doWebRequestWithTimeout(ctx, http.MethodPost, basePathV1()+"/agentos/runs", bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("Failed to send request: %v", err)
	}

	t.Cleanup(func() {
		if err := resp.Body.Close(); err != nil {
			t.Errorf("close response body: %v", err)
		}
	})

	if resp.StatusCode != expectedStatus {
		t.Errorf("Expected status %d, got %d", expectedStatus, resp.StatusCode)
	}

	if expectedStatus == http.StatusAccepted {
		var status runStatus
		if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
			t.Fatalf("Failed to decode response: %v", err)
		}

		if status.RunID == "" {
			t.Error("Expected non-empty run_id")
		}
	}
}

// HTTP GET: /v1/agentos/runs/{run_id}/status.
func TestHTTPAgentOSStatusV1(t *testing.T) {
	t.Parallel()

	runID := fmt.Sprintf("e2e-status-%d", time.Now().UnixNano())

	status := executeAgentRun(t, runID)
	if status.RunID != runID {
		t.Fatalf("Expected run_id %q, got %q", runID, status.RunID)
	}

	status = waitForRunCompletion(t, runID)

	if status.LifecycleState != waitingInput {
		t.Errorf("Expected lifecycle_state waiting_input, got %q", status.LifecycleState)
	}

	if status.Progress.Current == 0 {
		t.Error("Expected non-zero step count")
	}

	signalAgentOSUserMessage(t, runID, "continue")
	controlAgentOSRun(t, runID, "cancel")
}

// agentOSStartBody builds a start request. The tenant is deliberately absent:
// the credential supplies the account, and a request DTO that carried one would
// be a self-reported identity (guarded by the request package's tenant test).
func agentOSStartBody(runID, message string) string {
	return fmt.Sprintf(`{
		"run_id": "%s",
		"project_id": "e2e-test-project",
		"idempotency_key": "%s-start",
		"user_message": "%s",
		"backend": {
			"kind": "native",
			"name": "goagent-native"
		}
	}`, runID, runID, message)
}

func signalAgentOSUserMessage(t *testing.T, runID, content string) {
	t.Helper()

	body := fmt.Sprintf(`{
		"type": "user.message",
		"idempotency_key": "%s-message",
		"payload": {
			"content": "%s"
		}
	}`, runID, content)

	ctx, cancel := context.WithTimeout(t.Context(), requestTimeout)
	defer cancel()

	resp, err := doWebRequestWithTimeout(ctx, http.MethodPost, basePathV1()+"/agentos/runs/"+runID+"/signals", bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("signalAgentOSUserMessage: request failed: %v", err)
	}

	t.Cleanup(func() {
		if err := resp.Body.Close(); err != nil {
			t.Errorf("close response body: %v", err)
		}
	})

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("signalAgentOSUserMessage: expected 202, got %d", resp.StatusCode)
	}
}

func controlAgentOSRun(t *testing.T, runID, operation string) {
	t.Helper()

	body := fmt.Sprintf(`{"operation": %q}`, operation)

	ctx, cancel := context.WithTimeout(t.Context(), requestTimeout)
	defer cancel()

	resp, err := doWebRequestWithTimeout(ctx, http.MethodPost, basePathV1()+"/agentos/runs/"+runID+"/control", bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("controlAgentOSRun: request failed: %v", err)
	}

	t.Cleanup(func() {
		if err := resp.Body.Close(); err != nil {
			t.Errorf("close response body: %v", err)
		}
	})

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("controlAgentOSRun: expected 202, got %d", resp.StatusCode)
	}
}
