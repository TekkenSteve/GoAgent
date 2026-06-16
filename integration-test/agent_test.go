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

// runStatus is the JSON response from /v1/agentos/runs endpoints.
type runStatus struct {
	RunID          string `json:"run_id"`
	LifecycleState string `json:"lifecycle_state"`
	Step           int    `json:"step,omitempty"`
}

// executeAgentRun creates an agent run and returns the run status.
func executeAgentRun(t *testing.T, runID, accountID string) runStatus {
	t.Helper()

	body := agentOSStartBody(runID, accountID, "Hello, this is a test message")

	ctx, cancel := context.WithTimeout(t.Context(), requestTimeout)
	defer cancel()

	resp, err := doWebRequestWithTimeout(ctx, http.MethodPost, basePathV1()+"/agentos/runs", bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("executeAgentRun: request failed: %v", err)
	}
	defer resp.Body.Close()

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
			resp.Body.Close()
			t.Fatalf("waitForRunCompletion: decode failed: %v", err)
		}

		resp.Body.Close()

		if status.LifecycleState == "waiting_input" ||
			status.LifecycleState == string(entity.LifecycleCompleted) ||
			status.LifecycleState == string(entity.LifecycleFailed) ||
			status.LifecycleState == string(entity.LifecycleCancelled) {
			return status
		}

		time.Sleep(time.Second)
	}

	t.Fatalf("waitForRunCompletion: timed out waiting for run %s", runID)

	return runStatus{}
}

// HTTP POST: /v1/agentos/runs.
func TestHTTPAgentOSStartV1(t *testing.T) {
	runID := fmt.Sprintf("e2e-exec-%d", time.Now().UnixNano())

	tests := []struct {
		description string
		runID       string
		accountID   string
		message     string
		expected    int
	}{
		{
			description: "success",
			runID:       runID,
			accountID:   "e2e-test-account",
			message:     "Hello, this is a test message",
			expected:    http.StatusAccepted,
		},
		{
			description: "empty run_id",
			runID:       "",
			accountID:   "e2e-test-account",
			message:     "Hello",
			expected:    http.StatusAccepted,
		},
		{
			description: "empty account_id",
			runID:       runID + "_noaccount",
			accountID:   "",
			message:     "Hello",
			expected:    http.StatusBadRequest,
		},
		{
			description: "empty message",
			runID:       runID + "_nomsg",
			accountID:   "e2e-test-account",
			message:     "",
			expected:    http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			testExecuteAgentRequest(t, tt.runID, tt.accountID, tt.message, tt.expected)
		})
	}
}

// testExecuteAgentRequest sends an AgentOS start request and asserts the
// response status code and (for successful requests) the run status body.
func testExecuteAgentRequest(t *testing.T, runID, accountID, message string, expectedStatus int) {
	t.Helper()

	body := agentOSStartBody(runID, accountID, message)

	ctx, cancel := context.WithTimeout(t.Context(), requestTimeout)
	defer cancel()

	resp, err := doWebRequestWithTimeout(ctx, http.MethodPost, basePathV1()+"/agentos/runs", bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("Failed to send request: %v", err)
	}
	defer resp.Body.Close()

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
	runID := fmt.Sprintf("e2e-status-%d", time.Now().UnixNano())

	status := executeAgentRun(t, runID, "e2e-test-account")
	if status.RunID != runID {
		t.Fatalf("Expected run_id %q, got %q", runID, status.RunID)
	}

	status = waitForRunCompletion(t, runID)

	if status.LifecycleState != "waiting_input" {
		t.Errorf("Expected lifecycle_state waiting_input, got %q", status.LifecycleState)
	}

	if status.Step == 0 {
		t.Error("Expected non-zero step count")
	}

	signalAgentOSUserMessage(t, runID, "continue")
	controlAgentOSRun(t, runID, "cancel")
}

func agentOSStartBody(runID, accountID, message string) string {
	return fmt.Sprintf(`{
		"run_id": "%s",
		"account_id": "%s",
		"user_message": "%s",
		"backend": {
			"kind": "temporal_native",
			"name": "goagent-native"
		}
	}`, runID, accountID, message)
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
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("signalAgentOSUserMessage: expected 202, got %d", resp.StatusCode)
	}
}

func controlAgentOSRun(t *testing.T, runID, operation string) {
	t.Helper()

	body := fmt.Sprintf(`{"operation": "%s"}`, operation)

	ctx, cancel := context.WithTimeout(t.Context(), requestTimeout)
	defer cancel()

	resp, err := doWebRequestWithTimeout(ctx, http.MethodPost, basePathV1()+"/agentos/runs/"+runID+"/control", bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("controlAgentOSRun: request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("controlAgentOSRun: expected 202, got %d", resp.StatusCode)
	}
}
