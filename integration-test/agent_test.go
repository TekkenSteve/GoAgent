package integration_test

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/goccy/go-json"
)

// runStatus is the JSON response from /v1/agent/status and /v1/agent/execute.
type runStatus struct {
	RunID          string `json:"run_id"`
	LifecycleState string `json:"lifecycle_state"`
	Step           int    `json:"step,omitempty"`
}

// executeAgentRun creates an agent run and returns the run status.
func executeAgentRun(t *testing.T, runID, accountID string) runStatus {
	t.Helper()

	body := fmt.Sprintf(`{
		"run_id": "%s",
		"account_id": "%s",
		"user_message": "Hello, this is a test message"
	}`, runID, accountID)

	ctx, cancel := context.WithTimeout(t.Context(), requestTimeout)
	defer cancel()

	resp, err := doWebRequestWithTimeout(ctx, http.MethodPost, basePathV1+"/agent/execute", bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("executeAgentRun: request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("executeAgentRun: expected 200, got %d", resp.StatusCode)
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

	url := basePathV1 + "/agent/status/" + runID
	for i := 0; i < 30; i++ {
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

		if status.LifecycleState == "completed" ||
			status.LifecycleState == "failed" ||
			status.LifecycleState == "cancelled" {
			return status
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("waitForRunCompletion: timed out waiting for run %s", runID)
	return runStatus{}
}

// HTTP POST: /v1/agent/execute.
func TestHTTPAgentExecuteV1(t *testing.T) {
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
			expected:    http.StatusOK,
		},
		{
			description: "empty run_id",
			runID:       "",
			accountID:   "e2e-test-account",
			message:     "Hello",
			expected:    http.StatusBadRequest,
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
			body := fmt.Sprintf(`{
				"run_id": "%s",
				"account_id": "%s",
				"user_message": "%s"
			}`, tt.runID, tt.accountID, tt.message)

			ctx, cancel := context.WithTimeout(t.Context(), requestTimeout)
			defer cancel()

			resp, err := doWebRequestWithTimeout(ctx, http.MethodPost, basePathV1+"/agent/execute", bytes.NewBufferString(body))
			if err != nil {
				t.Fatalf("Failed to send request: %v", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != tt.expected {
				t.Errorf("Expected status %d, got %d", tt.expected, resp.StatusCode)
			}

			if tt.expected == http.StatusOK {
				var status runStatus
				if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
					t.Fatalf("Failed to decode response: %v", err)
				}
				if status.RunID == "" {
					t.Error("Expected non-empty run_id")
				}
			}
		})
	}
}

// HTTP GET: /v1/agent/status/{run_id}.
func TestHTTPAgentStatusV1(t *testing.T) {
	runID := fmt.Sprintf("e2e-status-%d", time.Now().UnixNano())

	status := executeAgentRun(t, runID, "e2e-test-account")
	if status.RunID != runID {
		t.Fatalf("Expected run_id %q, got %q", runID, status.RunID)
	}

	status = waitForRunCompletion(t, runID)

	if status.LifecycleState != "completed" {
		t.Errorf("Expected lifecycle_state completed, got %q", status.LifecycleState)
	}
	if status.Step == 0 {
		t.Error("Expected non-zero step count")
	}
}

// HTTP GET: /v1/agent/{run_id}/messages.
//
// Note: the step-level workflow path (used by /v1/agent/execute) does not persist
// individual messages to the WarmStateRepo — message persistence is tied to the
// streaming workflow path. This test validates the endpoint exists, returns 200,
// and has the correct response structure, even though Data will be empty.
func TestHTTPAgentMessagesV1(t *testing.T) {
	runID := fmt.Sprintf("e2e-msg-%d", time.Now().UnixNano())

	executeAgentRun(t, runID, "e2e-test-account")
	waitForRunCompletion(t, runID)

	url := basePathV1 + "/agent/" + runID + "/messages"
	ctx, cancel := context.WithTimeout(t.Context(), requestTimeout)
	defer cancel()

	resp, err := doWebRequestWithTimeout(ctx, http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("Failed to send request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected status 200, got %d", resp.StatusCode)
	}

	var result struct {
		Data   []json.RawMessage `json:"data"`
		Limit  uint64            `json:"limit"`
		Offset uint64            `json:"offset"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	// Data may be empty for step-level executes — the endpoint contract is valid
	// as long as the response parses correctly.
	_ = result.Data
}
