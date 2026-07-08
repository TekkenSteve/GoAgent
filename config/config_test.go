package config

import (
	"testing"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
)

func TestAgentOSArtifactStoreConfig(t *testing.T) {
	t.Parallel()

	cfg := AgentOS{
		ArtifactStoreBackend:          "s3",
		ArtifactStoreS3Bucket:         "agentos-artifacts",
		ArtifactStoreS3Region:         "us-east-1",
		ArtifactStoreS3Endpoint:       "http://minio:9000",
		ArtifactStoreS3AccessKeyID:    "access",
		ArtifactStoreS3SecretKey:      "secret",
		ArtifactStoreS3SessionToken:   "session",
		ArtifactStoreS3ForcePathStyle: true,
	}

	got := cfg.ArtifactStoreConfig()
	if got.Backend != "s3" ||
		got.S3.Bucket != "agentos-artifacts" ||
		got.S3.Region != "us-east-1" ||
		got.S3.Endpoint != "http://minio:9000" ||
		got.S3.AccessKeyID != "access" ||
		got.S3.SecretAccessKey != "secret" ||
		got.S3.SessionToken != "session" ||
		!got.S3.ForcePathStyle {
		t.Fatalf("unexpected artifact store config: %#v", got)
	}
}

func TestTemporalExternalBackends(t *testing.T) {
	t.Parallel()

	cfg := AgentFW{
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
	}

	backends, err := cfg.TemporalExternalBackends()
	if err != nil {
		t.Fatalf("TemporalExternalBackends: %v", err)
	}

	if len(backends) != 1 {
		t.Fatalf("backend count = %d", len(backends))
	}

	assertTemporalExternalBackend(t, &backends[0])
}

func assertTemporalExternalBackend(t *testing.T, got *TemporalExternalBackend) {
	t.Helper()

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
	t.Parallel()

	cfg := AgentFW{
		HTTPBackendsJSON: `[
			{
				"name":"claude-code",
				"endpoint":"http://claude-code-runtime:8080",
				"headers":{"Authorization":"Bearer token"}
			}
		]`,
	}

	backends, err := cfg.HTTPBackends()
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
	t.Parallel()

	cfg := AgentFW{
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
	}

	backends, err := cfg.GRPCBackends()
	if err != nil {
		t.Fatalf("GRPCBackends: %v", err)
	}

	if len(backends) != 1 {
		t.Fatalf("backend count = %d", len(backends))
	}

	assertGRPCBackend(t, &backends[0])
}

func assertGRPCBackend(t *testing.T, got *GRPCBackend) {
	t.Helper()

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
	t.Parallel()

	cfg := AgentFW{
		BackendSelectionRulesJSON: `[
			{
				"name":"research-report",
				"backend":{"kind":"temporal_external","name":"research-agent-workflow"},
				"input":{"task_type":"research_report"},
				"metadata":{"domain":"research"}
			}
		]`,
	}

	rules, err := cfg.BackendSelectionRules()
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
	t.Parallel()

	cfg := AgentOS{
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
	}

	capabilities, err := cfg.Capabilities()
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
		got.Signals[0] != agentoscore.SignalUserMessage ||
		got.Controls[0] != agentoscore.ControlCancel {
		t.Fatalf("unexpected capability config: %#v", got)
	}
}

func TestAgentOSArtifactSchemas(t *testing.T) {
	t.Parallel()

	cfg := AgentOS{
		ArtifactSchemasJSON: `[
			{
				"ref":"schema:summary",
				"description":"Summary artifact",
				"schema":{"type":"object","required":["summary"]}
			}
		]`,
	}

	schemas, err := cfg.ArtifactSchemas()
	if err != nil {
		t.Fatalf("ArtifactSchemas: %v", err)
	}

	if len(schemas) != 1 {
		t.Fatalf("schema count = %d", len(schemas))
	}

	got := schemas[0]
	if got.Ref != "schema:summary" || got.Description != "Summary artifact" || len(got.Schema) == 0 {
		t.Fatalf("unexpected schema config: %#v", got)
	}
}
