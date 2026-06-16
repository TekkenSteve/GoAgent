package config

import (
	"encoding/json"
	"fmt"

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

// NewConfig returns app config.
func NewConfig() (*Config, error) {
	cfg := &Config{}
	if err := env.Parse(cfg); err != nil {
		return nil, fmt.Errorf("config error: %w", err)
	}

	return cfg, nil
}
