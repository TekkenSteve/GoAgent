package config

import (
	"encoding/json"
	"fmt"

	"github.com/TekkenSteve/GoAgent/agentos"
	"github.com/caarlos0/env/v11"
)

type (
	// Config -.
	Config struct {
		App     App
		HTTP    HTTP
		Log     Log
		PG      PG
		Redis   Redis
		AgentOS AgentOS
		AgentFW AgentFW
		Metrics Metrics
		Swagger Swagger
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

	// AgentOS -.
	AgentOS struct {
		ArtifactStoreRoot string `env:"AGENTOS_ARTIFACT_STORE_ROOT,required"`
		CapabilitiesJSON  string `env:"AGENTOS_CAPABILITIES_JSON" envDefault:"[]"`
	}

	// AgentFW -.
	AgentFW struct {
		Enabled bool `env:"AGENTFW_ENABLED" envDefault:"false"`

		TemporalAddress   string `env:"AGENTFW_TEMPORAL_ADDRESS" envDefault:"127.0.0.1:7233"`
		TemporalNamespace string `env:"AGENTFW_TEMPORAL_NAMESPACE" envDefault:"default"`
		TemporalTaskQueue string `env:"AGENTFW_TEMPORAL_TASK_QUEUE" envDefault:"agent-framework"`
		// TemporalExternalBackendsJSON is a JSON array of temporal_external backend configs.
		TemporalExternalBackendsJSON string `env:"AGENTFW_TEMPORAL_EXTERNAL_BACKENDS_JSON" envDefault:"[]"`
		// HTTPBackendsJSON is a JSON array of HTTP backend configs.
		HTTPBackendsJSON string `env:"AGENTFW_HTTP_BACKENDS_JSON" envDefault:"[]"`
		// GRPCBackendsJSON is a JSON array of gRPC backend configs.
		GRPCBackendsJSON string `env:"AGENTFW_GRPC_BACKENDS_JSON" envDefault:"[]"`
		// BackendSelectionRulesJSON is an ordered JSON array of backend selection rules.
		BackendSelectionRulesJSON string `env:"AGENTFW_BACKEND_SELECTION_RULES_JSON" envDefault:"[]"`

		MaxConcurrentWorkflowTaskPollers int   `env:"AGENTFW_MAX_CONCURRENT_WORKFLOW_TASK_POLLERS" envDefault:"2"`
		MaxConcurrentActivityTaskPollers int   `env:"AGENTFW_MAX_CONCURRENT_ACTIVITY_TASK_POLLERS" envDefault:"2"`
		MaxConcurrentActivityExecution   int   `env:"AGENTFW_MAX_CONCURRENT_ACTIVITY_EXECUTION" envDefault:"100"`
		ContinueAsNewStepThreshold       int32 `env:"AGENTFW_CONTINUE_AS_NEW_STEP_THRESHOLD" envDefault:"80"`
		ContinueAsNewHistoryThreshold    int   `env:"AGENTFW_CONTINUE_AS_NEW_HISTORY_THRESHOLD" envDefault:"10000"`
		ContinueAsNewStateSizeThreshold  int   `env:"AGENTFW_CONTINUE_AS_NEW_STATE_SIZE_THRESHOLD_BYTES" envDefault:"524288"`
		ContinueAsNewWallClockSeconds    int   `env:"AGENTFW_CONTINUE_AS_NEW_WALL_CLOCK_THRESHOLD_SECONDS" envDefault:"3000"`
		ContinueAsNewMaxContinuations    int32 `env:"AGENTFW_CONTINUE_AS_NEW_MAX_CONTINUATIONS" envDefault:"1000"`

		// LLM configuration — providers and scenarios in YAML (AGENTFW_LLM_CONFIG_PATH).
		LLMConfigPath string `env:"AGENTFW_LLM_CONFIG_PATH" envDefault:""`
	}

	// Metrics -.
	Metrics struct {
		Enabled bool `env:"METRICS_ENABLED" envDefault:"true"`
	}

	// Swagger -.
	Swagger struct {
		Enabled bool `env:"SWAGGER_ENABLED" envDefault:"false"`
	}
)

// TemporalExternalBackend configures an external Temporal workflow backend.
type TemporalExternalBackend struct {
	Name         string              `json:"name"`
	TaskQueue    string              `json:"task_queue"`
	WorkflowType string              `json:"workflow_type"`
	QueryType    string              `json:"query_type,omitempty"`
	Signals      ExternalSignalNames `json:"signals,omitempty"`
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

// GRPCBackend configures a remote gRPC agent backend.
type GRPCBackend struct {
	Name      string          `json:"name"`
	Target    string          `json:"target"`
	Authority string          `json:"authority,omitempty"`
	Insecure  bool            `json:"insecure,omitempty"`
	Service   string          `json:"service,omitempty"`
	Methods   GRPCMethodNames `json:"methods,omitempty"`
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

// TemporalExternalBackends parses configured temporal_external backends.
func (c AgentFW) TemporalExternalBackends() ([]TemporalExternalBackend, error) {
	var backends []TemporalExternalBackend
	if err := json.Unmarshal([]byte(c.TemporalExternalBackendsJSON), &backends); err != nil {
		return nil, fmt.Errorf("parse AGENTFW_TEMPORAL_EXTERNAL_BACKENDS_JSON: %w", err)
	}

	return backends, nil
}

// HTTPBackends parses configured HTTP agent backends.
func (c AgentFW) HTTPBackends() ([]HTTPBackend, error) {
	var backends []HTTPBackend
	if err := json.Unmarshal([]byte(c.HTTPBackendsJSON), &backends); err != nil {
		return nil, fmt.Errorf("parse AGENTFW_HTTP_BACKENDS_JSON: %w", err)
	}

	return backends, nil
}

// GRPCBackends parses configured gRPC agent backends.
func (c AgentFW) GRPCBackends() ([]GRPCBackend, error) {
	var backends []GRPCBackend
	if err := json.Unmarshal([]byte(c.GRPCBackendsJSON), &backends); err != nil {
		return nil, fmt.Errorf("parse AGENTFW_GRPC_BACKENDS_JSON: %w", err)
	}

	return backends, nil
}

// BackendSelectionRules parses ordered backend selection rules.
func (c AgentFW) BackendSelectionRules() ([]BackendSelectionRule, error) {
	var rules []BackendSelectionRule
	if err := json.Unmarshal([]byte(c.BackendSelectionRulesJSON), &rules); err != nil {
		return nil, fmt.Errorf("parse AGENTFW_BACKEND_SELECTION_RULES_JSON: %w", err)
	}

	return rules, nil
}

// Capabilities parses configured AgentOS backend capabilities.
func (c AgentOS) Capabilities() ([]agentos.Capability, error) {
	var capabilities []agentos.Capability
	if err := json.Unmarshal([]byte(c.CapabilitiesJSON), &capabilities); err != nil {
		return nil, fmt.Errorf("parse AGENTOS_CAPABILITIES_JSON: %w", err)
	}

	return capabilities, nil
}

// NewConfig returns app config.
func NewConfig() (*Config, error) {
	cfg := &Config{}
	if err := env.Parse(cfg); err != nil {
		return nil, fmt.Errorf("config error: %w", err)
	}

	return cfg, nil
}
