package config

import (
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
		GRPC    GRPC
		RMQ     RMQ
		NATS    NATS
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

	// GRPC -.
	GRPC struct {
		Port string `env:"GRPC_PORT,required"`
	}

	// RMQ -.
	RMQ struct {
		ServerExchange string `env:"RMQ_RPC_SERVER,required"`
		ClientExchange string `env:"RMQ_RPC_CLIENT,required"`
		URL            string `env:"RMQ_URL,required"`
	}

	// NATS -.
	NATS struct {
		ServerExchange string `env:"NATS_RPC_SERVER,required"`
		URL            string `env:"NATS_URL,required"`
	}

	// Redis -.
	Redis struct {
		URL string `env:"REDIS_URL,required"`
	}

	// AgentFW -.
	AgentFW struct {
		Enabled bool `env:"AGENTFW_ENABLED" envDefault:"false"`
		// RolloutMode: disabled|shadow|canary|enabled.
		RolloutMode string `env:"AGENTFW_ROLLOUT_MODE" envDefault:"disabled"`
		// RolloutPercent applies in canary mode.
		RolloutPercent int `env:"AGENTFW_ROLLOUT_PERCENT" envDefault:"0"`
		// RolloutAllowlist is a comma-separated account id list always routed to Temporal in canary mode.
		RolloutAllowlist string `env:"AGENTFW_ROLLOUT_ALLOWLIST" envDefault:""`
		// RollbackForceLegacy forces old execution path regardless of rollout mode.
		RollbackForceLegacy bool `env:"AGENTFW_ROLLBACK_FORCE_LEGACY" envDefault:"false"`
		// RolloutHashSalt stabilizes account/run hashing for canary percentages.
		RolloutHashSalt string `env:"AGENTFW_ROLLOUT_HASH_SALT" envDefault:"agentfw-v1"`

		TemporalAddress   string `env:"AGENTFW_TEMPORAL_ADDRESS" envDefault:"127.0.0.1:7233"`
		TemporalNamespace string `env:"AGENTFW_TEMPORAL_NAMESPACE" envDefault:"default"`
		TemporalTaskQueue string `env:"AGENTFW_TEMPORAL_TASK_QUEUE" envDefault:"agent-framework"`

		MaxConcurrentWorkflowTaskPollers int   `env:"AGENTFW_MAX_CONCURRENT_WORKFLOW_TASK_POLLERS" envDefault:"2"`
		MaxConcurrentActivityTaskPollers int   `env:"AGENTFW_MAX_CONCURRENT_ACTIVITY_TASK_POLLERS" envDefault:"2"`
		MaxConcurrentActivityExecution   int   `env:"AGENTFW_MAX_CONCURRENT_ACTIVITY_EXECUTION" envDefault:"100"`
		ContinueAsNewStepThreshold       int32 `env:"AGENTFW_CONTINUE_AS_NEW_STEP_THRESHOLD" envDefault:"80"`
		ContinueAsNewHistoryThreshold    int   `env:"AGENTFW_CONTINUE_AS_NEW_HISTORY_THRESHOLD" envDefault:"10000"`
		ContinueAsNewStateSizeThreshold  int   `env:"AGENTFW_CONTINUE_AS_NEW_STATE_SIZE_THRESHOLD_BYTES" envDefault:"524288"`
		ContinueAsNewWallClockSeconds    int   `env:"AGENTFW_CONTINUE_AS_NEW_WALL_CLOCK_THRESHOLD_SECONDS" envDefault:"3000"`
		ContinueAsNewMaxContinuations    int32 `env:"AGENTFW_CONTINUE_AS_NEW_MAX_CONTINUATIONS" envDefault:"1000"`

		// LLM configuration for agent execution.
		LLMProvider  string  `env:"AGENTFW_LLM_PROVIDER" envDefault:"openai"`
		LLMModel     string  `env:"AGENTFW_LLM_MODEL" envDefault:"gpt-4.1-mini"`
		LLMBaseURL   string  `env:"AGENTFW_LLM_BASE_URL" envDefault:"https://api.openai.com/v1"`
		LLMAPIKey    string  `env:"AGENTFW_LLM_API_KEY" envDefault:""`
		LLMMaxTokens int     `env:"AGENTFW_LLM_MAX_TOKENS" envDefault:"4096"`
		LLMTemp      float64 `env:"AGENTFW_LLM_TEMPERATURE" envDefault:"0"`
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

// NewConfig returns app config.
func NewConfig() (*Config, error) {
	cfg := &Config{}
	if err := env.Parse(cfg); err != nil {
		return nil, fmt.Errorf("config error: %w", err)
	}

	return cfg, nil
}
