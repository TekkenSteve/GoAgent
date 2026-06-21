package v1

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/TekkenSteve/GoAgent/internal/controller/restapi/v1/request"
)

func TestNativeOrchestrationInputDecodesRawNativePayload(t *testing.T) {
	input, err := nativeOrchestrationInput(request.Orchestrate{
		RunID: "run-1",
		TeamSpec: json.RawMessage(`{
			"id": "team-1",
			"name": "Research",
			"agents": [{"id": "agent-1", "name": "Researcher", "model_ref": "gpt-4.1-mini"}],
			"steps": [{"id": "step-template-1", "type": "agent", "agent_ref": "agent-1", "input": {"topic": "agentos"}}]
		}`),
		Steps: json.RawMessage(`[
			{"id": "step-1", "type": "agent", "name": "Research", "agent_id": "agent-1", "input": {"topic": "agentos"}, "status": "pending"}
		]`),
		ContinuePolicy: json.RawMessage(`{"max_depth": 7, "max_rounds": 11}`),
	})
	if err != nil {
		t.Fatalf("nativeOrchestrationInput: %v", err)
	}
	if input.TeamSpec == nil || input.TeamSpec.ID != "team-1" || len(input.TeamSpec.Steps) != 1 {
		t.Fatalf("team spec = %#v", input.TeamSpec)
	}
	if len(input.Steps) != 1 || input.Steps[0].ID != "step-1" || input.Steps[0].AgentID != "agent-1" {
		t.Fatalf("steps = %#v", input.Steps)
	}
	if input.ContinuePolicy.MaxDepth != 7 || input.ContinuePolicy.MaxRounds != 11 {
		t.Fatalf("continue policy = %#v", input.ContinuePolicy)
	}
}

func TestNativeOrchestrationInputRejectsInvalidRawPayload(t *testing.T) {
	_, err := nativeOrchestrationInput(request.Orchestrate{
		RunID: "run-1",
		Steps: json.RawMessage(`{"id":"not-an-array"}`),
	})
	if err == nil {
		t.Fatal("nativeOrchestrationInput succeeded, want invalid steps error")
	}
	var typeErr *json.UnmarshalTypeError
	if !errors.As(err, &typeErr) {
		t.Fatalf("error = %v, want json.UnmarshalTypeError", err)
	}
}
