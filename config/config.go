// Package config loads and validates the GoAgent application configuration.
package config

import (
	"encoding/json"
	"errors"
	"fmt"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	"github.com/caarlos0/env/v11"
)

type (
	// Config -.
	Config struct {
		App              App
		HTTP             HTTP
		Log              Log
		PG               PG
		Redis            Redis
		Auth             Auth
		MCP              MCP
		AgentOS          AgentOS
		AgentFW          AgentFW
		StreamCentrifugo StreamCentrifugo
		Metrics          Metrics
		Swagger          Swagger
		Tracing          Tracing
	}

	// App -.
	App struct {
		Name    string `env:"APP_NAME,required"`
		Version string `env:"APP_VERSION,required"`
	}

	// HTTP -.
	HTTP struct {
		Port           string `env:"HTTP_PORT,required"`
		UsePreforkMode bool   `env:"HTTP_USE_PREFORK_MODE" envDefault:"false"`
	}

	// Log -.
	Log struct {
		Level string `env:"LOG_LEVEL,required"`
	}

	// PG -.
	PG struct {
		PoolMax int    `env:"PG_POOL_MAX,required"`
		URL     string `env:"PG_URL,required"`
	}

	// Redis -.
	Redis struct {
		URL string `env:"REDIS_URL,required"`
	}

	// Auth -.
	//
	// Authentication is mandatory: the loader refuses to start without exactly
	// one key source and a token binding (issuer and/or audience). A service
	// that can be deployed anonymously is worse than one that refuses to boot.
	Auth struct {
		// JWKSURL is the issuer's JWK Set endpoint (identity provider or
		// authenticating gateway).
		JWKSURL string `env:"AUTH_JWKS_URL"`
		// HMACSecret is a shared HS256 secret for local development and
		// integration tests. Never the production posture.
		HMACSecret string `env:"AUTH_HMAC_SECRET"`
		// Issuer must match the token's iss claim when set.
		Issuer string `env:"AUTH_ISSUER"`
		// Audience must be present in the token's aud claim when set.
		Audience string `env:"AUTH_AUDIENCE"`
		// LeewaySeconds absorbs clock skew between issuer and this service.
		LeewaySeconds int `env:"AUTH_LEEWAY_SECONDS" envDefault:"0"`
		// AccountClaim names the claim carrying the tenant boundary.
		AccountClaim string `env:"AUTH_ACCOUNT_CLAIM" envDefault:"sub"`
		// ActorClaim names the claim identifying the acting party.
		ActorClaim string `env:"AUTH_ACTOR_CLAIM" envDefault:"act"`
		// PublicPaths lists exact paths reachable without a credential
		// (probes, documentation). Everything else requires authentication.
		PublicPaths []string `env:"AUTH_PUBLIC_PATHS" envSeparator:","`
	}

	// AgentOS -.
	AgentOS struct {
		ArtifactStoreBackend                    string `env:"AGENTOS_ARTIFACT_STORE_BACKEND,required"`
		ArtifactStoreLocalRoot                  string `env:"AGENTOS_ARTIFACT_STORE_LOCAL_ROOT"`
		ArtifactStoreS3Bucket                   string `env:"AGENTOS_ARTIFACT_STORE_S3_BUCKET"`
		ArtifactStoreS3Region                   string `env:"AGENTOS_ARTIFACT_STORE_S3_REGION"`
		ArtifactStoreS3Endpoint                 string `env:"AGENTOS_ARTIFACT_STORE_S3_ENDPOINT"`
		ArtifactStoreS3AccessKeyID              string `env:"AGENTOS_ARTIFACT_STORE_S3_ACCESS_KEY_ID"`
		ArtifactStoreS3SecretKey                string `env:"AGENTOS_ARTIFACT_STORE_S3_SECRET_ACCESS_KEY"`
		ArtifactStoreS3SessionToken             string `env:"AGENTOS_ARTIFACT_STORE_S3_SESSION_TOKEN"`
		ArtifactStoreS3ForcePathStyle           bool   `env:"AGENTOS_ARTIFACT_STORE_S3_FORCE_PATH_STYLE" envDefault:"false"`
		CapabilitiesJSON                        string `env:"AGENTOS_CAPABILITIES_JSON" envDefault:"[]"`
		ArtifactSchemasJSON                     string `env:"AGENTOS_ARTIFACT_SCHEMAS_JSON" envDefault:"[]"`
		PlanCommandRecoveryEnabled              bool   `env:"AGENTOS_PLAN_COMMAND_RECOVERY_ENABLED" envDefault:"true"`
		PlanCommandRecoveryIntervalSeconds      int    `env:"AGENTOS_PLAN_COMMAND_RECOVERY_INTERVAL_SECONDS" envDefault:"30"`
		PlanCommandRecoveryLimit                int    `env:"AGENTOS_PLAN_COMMAND_RECOVERY_LIMIT" envDefault:"100"`
		PlanCommandRecoveryImmediateOnWorkerRun bool   `env:"AGENTOS_PLAN_COMMAND_RECOVERY_IMMEDIATE_ON_WORKER_RUN" envDefault:"true"`
		PlanMetricsExporterEnabled              bool   `env:"AGENTOS_PLAN_METRICS_EXPORTER_ENABLED" envDefault:"true"`
		PlanMetricsExporterID                   string `env:"AGENTOS_PLAN_METRICS_EXPORTER_ID" envDefault:"agentos-plan-metrics"`
		PlanMetricsExporterIntervalSeconds      int    `env:"AGENTOS_PLAN_METRICS_EXPORTER_INTERVAL_SECONDS" envDefault:"30"`
		PlanMetricsExporterPlanLimit            int    `env:"AGENTOS_PLAN_METRICS_EXPORTER_PLAN_LIMIT" envDefault:"100"`
		PlanMetricsExporterBatchSize            int    `env:"AGENTOS_PLAN_METRICS_EXPORTER_BATCH_SIZE" envDefault:"1000"`
		PlanMetricsExporterImmediateOnWorkerRun bool   `env:"AGENTOS_PLAN_METRICS_EXPORTER_IMMEDIATE_ON_WORKER_RUN" envDefault:"true"`
	}

	// MCP -.
	//
	// The MCP transport policy is the boundary between request-shaped
	// server configs and this host: nothing launches or dials unless the
	// operator declared it here.
	MCP struct {
		// AllowedCommands lists the exact executables a stdio server
		// config may launch. Empty runs nothing — that is the posture.
		AllowedCommands []string `env:"MCP_ALLOWED_COMMANDS" envSeparator:","`
		// InheritedEnvKeys lists host environment keys a subprocess
		// receives. PATH and HOME by default; never widen it to a key
		// that carries a secret.
		InheritedEnvKeys []string `env:"MCP_INHERITED_ENV" envSeparator:"," envDefault:"PATH,HOME"`
		// ConfigEnvKeys lists environment keys a server config may set
		// itself. None by default; a config can never override an
		// inherited key such as PATH.
		ConfigEnvKeys []string `env:"MCP_CONFIG_ENV" envSeparator:","`
		// AllowedHTTPHosts lists hosts a network transport may reach
		// regardless of address class — for MCP servers on the internal
		// network. Everything else must resolve publicly.
		AllowedHTTPHosts []string `env:"MCP_ALLOWED_HTTP_HOSTS" envSeparator:","`
	}

	// AgentFW -.
	AgentFW struct {
		Enabled bool `env:"AGENTFW_ENABLED" envDefault:"false"`

		TemporalAddress   string `env:"AGENTFW_TEMPORAL_ADDRESS" envDefault:"127.0.0.1:7233"`
		TemporalNamespace string `env:"AGENTFW_TEMPORAL_NAMESPACE" envDefault:"default"`
		// TemporalNexusEndpoint is the Nexus endpoint callers reference to reach
		// the AgentOS run service. Must match the endpoint created by
		// scripts/temporal/create-namespace.sh.
		TemporalNexusEndpoint string `env:"AGENTFW_TEMPORAL_NEXUS_ENDPOINT" envDefault:"agentos"`
		// NexusPeersJSON maps a peer town to the Nexus endpoint that reaches it
		// ({"townb":"agentos-townb"}). Endpoint names are namespace-scoped, so a
		// deployment that issues operations to more than one town needs the map;
		// a plan node names its peer, and a node that names none uses
		// TemporalNexusEndpoint.
		NexusPeersJSON string `env:"AGENTFW_NEXUS_PEERS_JSON" envDefault:"{}"`
		// Workload-specific Temporal task queues.
		TemporalPlanControlTaskQueue     string `env:"AGENTFW_TEMPORAL_PLAN_CONTROL_TASK_QUEUE" envDefault:"agentos-plan-control"`
		TemporalPlanActivityTaskQueue    string `env:"AGENTFW_TEMPORAL_PLAN_ACTIVITY_TASK_QUEUE" envDefault:"agentos-plan-activity"`
		TemporalProcessControlTaskQueue  string `env:"AGENTFW_TEMPORAL_PROCESS_CONTROL_TASK_QUEUE" envDefault:"agentos-process-control"`
		TemporalProcessActivityTaskQueue string `env:"AGENTFW_TEMPORAL_PROCESS_ACTIVITY_TASK_QUEUE" envDefault:"agentos-process-activity"`
		TemporalNativeControlTaskQueue   string `env:"AGENTFW_TEMPORAL_NATIVE_CONTROL_TASK_QUEUE" envDefault:"agentfw-native-control"`
		TemporalNativeLLMTaskQueue       string `env:"AGENTFW_TEMPORAL_NATIVE_LLM_TASK_QUEUE" envDefault:"agentfw-native-llm"`
		TemporalNativeToolTaskQueue      string `env:"AGENTFW_TEMPORAL_NATIVE_TOOL_TASK_QUEUE" envDefault:"agentfw-native-tool"`
		TemporalStreamTaskQueue          string `env:"AGENTFW_TEMPORAL_STREAM_TASK_QUEUE" envDefault:"agentfw-stream"`
		TemporalNexusTaskQueue           string `env:"AGENTFW_TEMPORAL_NEXUS_TASK_QUEUE" envDefault:"agentos-nexus"`
		// Event backbone (NATS JetStream). An empty URL leaves it disabled,
		// so a deployment without a fact log stays supported.
		NatsURL           string `env:"AGENTFW_NATS_URL" envDefault:""`
		NatsSubjectPrefix string `env:"AGENTFW_NATS_SUBJECT_PREFIX" envDefault:"agentos"`
		// NatsShards is how many shards each domain is split into. It decides
		// which consumer owns an entity's records, so changing it while
		// writers run moves entities and breaks their ordering.
		NatsShards uint32 `env:"AGENTFW_NATS_SHARDS" envDefault:"16"`
		// Cross-town ingress: the bridge that turns friend-cell facts into
		// Temporal work. An empty route table leaves it off, so a town with
		// nothing to react to runs without a consumer.
		//
		// IngressRoutesJSON is a JSON array of routes. Which fact types this
		// town reacts to is deployment policy, so it lives here rather than in
		// code:
		//   [{"domain":"run.timeline","type":"run.requested",
		//     "workflow_type":"agentfw.agent-workflow.v1","signal_name":"..."}]
		// The domain decides which log domain the bridge consumes, so it is
		// required and must be a domain the fact log declares.
		IngressRoutesJSON string `env:"AGENTFW_INGRESS_ROUTES_JSON" envDefault:"[]"`
		// IngressTaskQueue is where the workflows a routed fact starts or
		// signals live. Required once a route exists: a route into a queue
		// nobody polls would swallow cross-town work silently.
		IngressTaskQueue string `env:"AGENTFW_INGRESS_TASK_QUEUE" envDefault:""`
		// IngressConsumerPrefix names the durable consumers the bridge owns.
		// The domain and the shard group are appended, so two processes can
		// never share one consumer by accident — that would round-robin a
		// shard across processes and break its per-entity ordering.
		IngressConsumerPrefix string `env:"AGENTFW_INGRESS_CONSUMER_PREFIX" envDefault:"agentos-ingress"`
		// IngressShards restricts the shards this process consumes, written as
		// a list of numbers and ranges ("0-7,12"). Empty owns every shard,
		// which is always correct; a subset is how the ingress scales out.
		IngressShards string `env:"AGENTFW_INGRESS_SHARDS" envDefault:""`
		// TemporalExternalBackendsJSON is a JSON array of temporal_external backend configs.
		TemporalExternalBackendsJSON string `env:"AGENTFW_TEMPORAL_EXTERNAL_BACKENDS_JSON" envDefault:"[]"`
		// HTTPBackendsJSON is a JSON array of HTTP backend configs.
		HTTPBackendsJSON string `env:"AGENTFW_HTTP_BACKENDS_JSON" envDefault:"[]"`
		// GRPCBackendsJSON is a JSON array of gRPC backend configs.
		GRPCBackendsJSON string `env:"AGENTFW_GRPC_BACKENDS_JSON" envDefault:"[]"`
		// DSHBackendsJSON is a JSON array of DeepSeek Harness SDK backend configs.
		DSHBackendsJSON string `env:"AGENTFW_DSH_BACKENDS_JSON" envDefault:"[]"`
		// BackendSelectionRulesJSON is an ordered JSON array of backend selection rules.
		BackendSelectionRulesJSON string `env:"AGENTFW_BACKEND_SELECTION_RULES_JSON" envDefault:"[]"`

		MaxConcurrentWorkflowTaskPollers int `env:"AGENTFW_MAX_CONCURRENT_WORKFLOW_TASK_POLLERS" envDefault:"2"`
		MaxConcurrentActivityTaskPollers int `env:"AGENTFW_MAX_CONCURRENT_ACTIVITY_TASK_POLLERS" envDefault:"2"`
		MaxConcurrentActivityExecution   int `env:"AGENTFW_MAX_CONCURRENT_ACTIVITY_EXECUTION" envDefault:"100"`

		// Delegation bounds. Every delegation level re-injects the delegate
		// tool, so without a limit a prompt that talks an agent into
		// delegating to itself builds an unbounded tree of paid LLM calls.
		MaxDelegateDepth int `env:"AGENTFW_MAX_DELEGATE_DEPTH" envDefault:"3"`
		// DelegateTokenBudget bounds what a run's whole delegation tree may
		// spend. Zero means no token bound.
		DelegateTokenBudget int64 `env:"AGENTFW_DELEGATE_TOKEN_BUDGET" envDefault:"0"`
		// RunWorkflowTimeoutSeconds bounds one root workflow execution. It
		// must comfortably exceed the await-user-input window, or a run
		// waiting for a person would be killed instead of answered.
		RunWorkflowTimeoutSeconds       int64 `env:"AGENTFW_RUN_WORKFLOW_TIMEOUT_SECONDS" envDefault:"172800"`
		ContinueAsNewStepThreshold      int32 `env:"AGENTFW_CONTINUE_AS_NEW_STEP_THRESHOLD" envDefault:"80"`
		ContinueAsNewHistoryThreshold   int   `env:"AGENTFW_CONTINUE_AS_NEW_HISTORY_THRESHOLD" envDefault:"10000"`
		ContinueAsNewStateSizeThreshold int   `env:"AGENTFW_CONTINUE_AS_NEW_STATE_SIZE_THRESHOLD_BYTES" envDefault:"524288"`
		ContinueAsNewWallClockSeconds   int   `env:"AGENTFW_CONTINUE_AS_NEW_WALL_CLOCK_THRESHOLD_SECONDS" envDefault:"3000"`
		ContinueAsNewMaxContinuations   int32 `env:"AGENTFW_CONTINUE_AS_NEW_MAX_CONTINUATIONS" envDefault:"1000"`

		// LLM configuration — providers and scenarios in YAML (AGENTFW_LLM_CONFIG_PATH).
		LLMConfigPath string `env:"AGENTFW_LLM_CONFIG_PATH" envDefault:""`
	}

	// StreamCentrifugo configures the external Centrifugo data-plane bus. When
	// both env vars are empty the app assembles the in-process memstream bus
	// (single-node dev default); setting CENTRIFUGO_BASE_URL switches the live
	// transport to Centrifugo (production multi-process).
	StreamCentrifugo struct {
		BaseURL string `env:"CENTRIFUGO_BASE_URL" envDefault:""`
		APIKey  string `env:"CENTRIFUGO_API_KEY" envDefault:""`
	}

	// Metrics -.
	Metrics struct {
		Enabled bool `env:"METRICS_ENABLED" envDefault:"true"`
	}

	// Swagger -.
	Swagger struct {
		Enabled bool `env:"SWAGGER_ENABLED" envDefault:"false"`
	}

	// Tracing -.
	Tracing struct {
		Enabled      bool    `env:"TRACING_ENABLED" envDefault:"false"`
		OTLPEndpoint string  `env:"TRACING_OTLP_ENDPOINT" envDefault:"localhost:4317"`
		OTLPInsecure bool    `env:"TRACING_OTLP_INSECURE" envDefault:"true"`
		SampleRate   float64 `env:"TRACING_SAMPLE_RATE" envDefault:"0.1"`
	}
)

// TemporalExternalBackend configures an external Temporal workflow backend.
type TemporalExternalBackend struct {
	Name         string              `json:"name"`
	TaskQueue    string              `json:"task_queue"`
	WorkflowType string              `json:"workflow_type"`
	QueryType    string              `json:"query_type,omitempty"`
	Signals      ExternalSignalNames `json:"signals"`
}

// ExternalSignalNames maps AgentOS signals/control operations to external workflow signal names.
type ExternalSignalNames struct {
	Pause    string            `json:"pause,omitempty"`
	Resume   string            `json:"resume,omitempty"`
	Cancel   string            `json:"cancel,omitempty"`
	Defaults map[string]string `json:"defaults,omitempty"`
}

// HTTPBackend configures a remote HTTP agent backend.
type HTTPBackend struct {
	Name     string            `json:"name"`
	Endpoint string            `json:"endpoint"`
	Headers  map[string]string `json:"headers,omitempty"`
}

// DSHBackend configures a DeepSeek Harness SDK backend: the subprocess to
// spawn and the session profile to run it under. Command defaults to "dsh" and
// Profile to "sdk" (the SDK protocol profile).
type DSHBackend struct {
	Name       string   `json:"name"`
	Command    string   `json:"command,omitempty"`
	Profile    string   `json:"profile,omitempty"`
	Args       []string `json:"args,omitempty"`
	Env        []string `json:"env,omitempty"`
	WorkingDir string   `json:"working_dir,omitempty"`
}

// GRPCBackend configures a remote gRPC agent backend.
type GRPCBackend struct {
	Name      string          `json:"name"`
	Target    string          `json:"target"`
	Authority string          `json:"authority,omitempty"`
	Insecure  bool            `json:"insecure,omitempty"`
	Service   string          `json:"service,omitempty"`
	Methods   GRPCMethodNames `json:"methods"`
}

// GRPCMethodNames maps AgentOS operations to external gRPC unary method names.
type GRPCMethodNames struct {
	Start   string `json:"start,omitempty"`
	Signal  string `json:"signal,omitempty"`
	Control string `json:"control,omitempty"`
	Status  string `json:"status,omitempty"`
}

// BackendSelectionRule maps generic run attributes to a backend.
type BackendSelectionRule struct {
	Name     string             `json:"name,omitempty"`
	Backend  agentos.BackendRef `json:"backend"`
	AgentID  string             `json:"agent_id,omitempty"`
	Metadata map[string]string  `json:"metadata,omitempty"`
	Input    map[string]any     `json:"input,omitempty"`
}

// ArtifactStoreConfig is the app-level artifact blob configuration. The app
// maps it to the selected AgentOS runtime implementation at wiring time.
type ArtifactStoreConfig struct {
	Backend string
	Local   LocalArtifactStoreConfig
	S3      S3ArtifactStoreConfig
}

// LocalArtifactStoreConfig holds artifact blob storage settings for the local filesystem backend.
type LocalArtifactStoreConfig struct {
	Root string
}

// S3ArtifactStoreConfig holds artifact blob storage settings for the S3 backend.
type S3ArtifactStoreConfig struct {
	Bucket          string
	Region          string
	Endpoint        string
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
	ForcePathStyle  bool
}

// NexusPeers parses the peer-town → Nexus endpoint map. Both halves are
// required: a peer without an endpoint could never be reached, and an endpoint
// without a peer would never be selected.
func (c *AgentFW) NexusPeers() (map[string]string, error) {
	peers := map[string]string{}
	if err := json.Unmarshal([]byte(c.NexusPeersJSON), &peers); err != nil {
		return nil, fmt.Errorf("parse AGENTFW_NEXUS_PEERS_JSON: %w", err)
	}

	for peer, endpoint := range peers {
		if peer == "" || endpoint == "" {
			return nil, fmt.Errorf("parse AGENTFW_NEXUS_PEERS_JSON: %w: %q", errNexusPeerIncomplete, peer)
		}
	}

	return peers, nil
}

// errNexusPeerIncomplete reports a peer entry missing one of its halves.
var errNexusPeerIncomplete = errors.New("peer and endpoint must both be set")

// TemporalExternalBackends parses configured temporal_external backends.
func (c *AgentFW) TemporalExternalBackends() ([]TemporalExternalBackend, error) {
	var backends []TemporalExternalBackend
	if err := json.Unmarshal([]byte(c.TemporalExternalBackendsJSON), &backends); err != nil {
		return nil, fmt.Errorf("parse AGENTFW_TEMPORAL_EXTERNAL_BACKENDS_JSON: %w", err)
	}

	return backends, nil
}

// HTTPBackends parses configured HTTP agent backends.
func (c *AgentFW) HTTPBackends() ([]HTTPBackend, error) {
	var backends []HTTPBackend
	if err := json.Unmarshal([]byte(c.HTTPBackendsJSON), &backends); err != nil {
		return nil, fmt.Errorf("parse AGENTFW_HTTP_BACKENDS_JSON: %w", err)
	}

	return backends, nil
}

// GRPCBackends parses configured gRPC agent backends.
func (c *AgentFW) GRPCBackends() ([]GRPCBackend, error) {
	var backends []GRPCBackend
	if err := json.Unmarshal([]byte(c.GRPCBackendsJSON), &backends); err != nil {
		return nil, fmt.Errorf("parse AGENTFW_GRPC_BACKENDS_JSON: %w", err)
	}

	return backends, nil
}

// DSHBackends parses configured DeepSeek Harness SDK backends.
func (c *AgentFW) DSHBackends() ([]DSHBackend, error) {
	var backends []DSHBackend
	if err := json.Unmarshal([]byte(c.DSHBackendsJSON), &backends); err != nil {
		return nil, fmt.Errorf("parse AGENTFW_DSH_BACKENDS_JSON: %w", err)
	}

	return backends, nil
}

// BackendSelectionRules parses ordered backend selection rules.
func (c *AgentFW) BackendSelectionRules() ([]BackendSelectionRule, error) {
	var rules []BackendSelectionRule
	if err := json.Unmarshal([]byte(c.BackendSelectionRulesJSON), &rules); err != nil {
		return nil, fmt.Errorf("parse AGENTFW_BACKEND_SELECTION_RULES_JSON: %w", err)
	}

	return rules, nil
}

// Capabilities parses configured AgentOS backend capabilities.
func (c *AgentOS) Capabilities() ([]agentos.Capability, error) {
	var capabilities []agentos.Capability
	if err := json.Unmarshal([]byte(c.CapabilitiesJSON), &capabilities); err != nil {
		return nil, fmt.Errorf("parse AGENTOS_CAPABILITIES_JSON: %w", err)
	}

	return capabilities, nil
}

// ArtifactSchemas parses configured AgentOS artifact schema declarations.
func (c *AgentOS) ArtifactSchemas() ([]agentos.ArtifactSchema, error) {
	var schemas []agentos.ArtifactSchema
	if err := json.Unmarshal([]byte(c.ArtifactSchemasJSON), &schemas); err != nil {
		return nil, fmt.Errorf("parse AGENTOS_ARTIFACT_SCHEMAS_JSON: %w", err)
	}

	return schemas, nil
}

// ArtifactStoreConfig maps the AgentOS artifact store env settings into an ArtifactStoreConfig.
func (c *AgentOS) ArtifactStoreConfig() ArtifactStoreConfig {
	return ArtifactStoreConfig{
		Backend: c.ArtifactStoreBackend,
		Local: LocalArtifactStoreConfig{
			Root: c.ArtifactStoreLocalRoot,
		},
		S3: S3ArtifactStoreConfig{
			Bucket:          c.ArtifactStoreS3Bucket,
			Region:          c.ArtifactStoreS3Region,
			Endpoint:        c.ArtifactStoreS3Endpoint,
			AccessKeyID:     c.ArtifactStoreS3AccessKeyID,
			SecretAccessKey: c.ArtifactStoreS3SecretKey,
			SessionToken:    c.ArtifactStoreS3SessionToken,
			ForcePathStyle:  c.ArtifactStoreS3ForcePathStyle,
		},
	}
}

// NewConfig returns app config.
func NewConfig() (*Config, error) {
	cfg := &Config{}
	if err := env.Parse(cfg); err != nil {
		return nil, fmt.Errorf("config error: %w", err)
	}

	if err := cfg.Auth.Validate(); err != nil {
		return nil, fmt.Errorf("config error: %w", err)
	}

	return cfg, nil
}
