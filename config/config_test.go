package config

import (
	"testing"

	"github.com/TekkenSteve/GoAgent/agentos"
)

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

func TestGRPCBackends(t *testing.T) {
	backends, err := AgentFW{
		GRPCBackendsJSON: `[
			{
				"name":"opencode",
				"target":"opencode-runtime:9090",
				"insecure":true,
				"service":"agentos.v1.AgentBackend",
				"methods":{
					"start":"StartRun",
					"signal":"SignalRun",
					"control":"ControlRun",
					"status":"StatusRun"
				}
			}
		]`,
	}.GRPCBackends()
	if err != nil {
		t.Fatalf("GRPCBackends: %v", err)
	}

	if len(backends) != 1 {
		t.Fatalf("backend count = %d", len(backends))
	}

	got := backends[0]
	if got.Name != "opencode" ||
		got.Target != "opencode-runtime:9090" ||
		!got.Insecure ||
		got.Service != "agentos.v1.AgentBackend" ||
		got.Methods.Start != "StartRun" ||
		got.Methods.Signal != "SignalRun" ||
		got.Methods.Control != "ControlRun" ||
		got.Methods.Status != "StatusRun" {
		t.Fatalf("unexpected backend config: %#v", got)
	}
}

func TestBackendSelectionRules(t *testing.T) {
	rules, err := AgentFW{
		BackendSelectionRulesJSON: `[
			{
				"name":"research-report",
				"backend":{"kind":"temporal_external","name":"research-agent-workflow"},
				"input":{"task_type":"research_report"},
				"metadata":{"domain":"research"}
			}
		]`,
	}.BackendSelectionRules()
	if err != nil {
		t.Fatalf("BackendSelectionRules: %v", err)
	}

	if len(rules) != 1 {
		t.Fatalf("rule count = %d", len(rules))
	}

	got := rules[0]
	if got.Name != "research-report" ||
		got.Backend.Kind != agentos.BackendKindTemporalExternal ||
		got.Backend.Name != "research-agent-workflow" ||
		got.Input["task_type"] != "research_report" ||
		got.Metadata["domain"] != "research" {
		t.Fatalf("unexpected rule config: %#v", got)
	}
}

func TestAgentOSCapabilities(t *testing.T) {
	capabilities, err := AgentOS{
		CapabilitiesJSON: `[
			{
				"backend":{"kind":"http","name":"research-http"},
				"name":"summarize",
				"description":"Summarize source material",
				"input_schema":{"type":"object","required":["topic"]},
				"output_schema":{"type":"object","required":["summary"]},
				"signals":["user.message"],
				"controls":["cancel"]
			}
		]`,
	}.Capabilities()
	if err != nil {
		t.Fatalf("Capabilities: %v", err)
	}
	if len(capabilities) != 1 {
		t.Fatalf("capability count = %d", len(capabilities))
	}

	got := capabilities[0]
	if got.Backend.Kind != agentos.BackendKindHTTP ||
		got.Backend.Name != "research-http" ||
		got.Name != "summarize" ||
		got.Signals[0] != agentos.SignalUserMessage ||
		got.Controls[0] != agentos.ControlCancel {
		t.Fatalf("unexpected capability config: %#v", got)
	}
}
