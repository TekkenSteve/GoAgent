package config

import "testing"

func TestTemporalExternalBackends(t *testing.T) {
	backends, err := AgentFW{
		TemporalExternalBackendsJSON: `[
			{
				"name":"langgraph-main",
				"task_queue":"langgraph-agent-queue",
				"workflow_type":"langgraph.agent.v1",
				"query_type":"agentos_status",
				"signals":{
					"pause":"pause",
					"resume":"resume",
					"cancel":"cancel",
					"defaults":{
						"user.message":"user_input",
						"tool.result":"tool_result"
					}
				}
			}
		]`,
	}.TemporalExternalBackends()
	if err != nil {
		t.Fatalf("TemporalExternalBackends: %v", err)
	}

	if len(backends) != 1 {
		t.Fatalf("backend count = %d", len(backends))
	}

	got := backends[0]
	if got.Name != "langgraph-main" ||
		got.TaskQueue != "langgraph-agent-queue" ||
		got.WorkflowType != "langgraph.agent.v1" ||
		got.QueryType != "agentos_status" ||
		got.Signals.Pause != "pause" ||
		got.Signals.Resume != "resume" ||
		got.Signals.Cancel != "cancel" ||
		got.Signals.Defaults["user.message"] != "user_input" ||
		got.Signals.Defaults["tool.result"] != "tool_result" {
		t.Fatalf("unexpected backend config: %#v", got)
	}
}

func TestHTTPBackends(t *testing.T) {
	backends, err := AgentFW{
		HTTPBackendsJSON: `[
			{
				"name":"claude-code",
				"endpoint":"http://claude-code-runtime:8080",
				"headers":{"Authorization":"Bearer token"}
			}
		]`,
	}.HTTPBackends()
	if err != nil {
		t.Fatalf("HTTPBackends: %v", err)
	}

	if len(backends) != 1 {
		t.Fatalf("backend count = %d", len(backends))
	}

	got := backends[0]
	if got.Name != "claude-code" ||
		got.Endpoint != "http://claude-code-runtime:8080" ||
		got.Headers["Authorization"] != "Bearer token" {
		t.Fatalf("unexpected backend config: %#v", got)
	}
}
