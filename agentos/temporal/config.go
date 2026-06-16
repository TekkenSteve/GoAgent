package temporal

import "github.com/TekkenSteve/GoAgent/agentos"

// RuntimeConfig configures the default Temporal/Redis runtime implementation.
type RuntimeConfig struct {
	TemporalAddress          string
	TemporalNamespace        string
	TemporalTaskQueue        string
	RedisURL                 string
	TemporalExternalBackends []ExternalBackendConfig
}

// ExternalBackendConfig configures a temporal_external AgentOS backend.
type ExternalBackendConfig struct {
	Name         string
	TaskQueue    string
	WorkflowType string
	QueryType    string
	Signals      ExternalSignalNames
}

// ExternalSignalNames maps AgentOS signals and controls to external workflow signal names.
type ExternalSignalNames struct {
	Pause    string
	Resume   string
	Cancel   string
	Defaults map[agentos.SignalType]string
}

// WorkerConfig configures registration of GoAgent workflows and activities into
// a Temporal worker.
type WorkerConfig struct {
	TemporalAddress   string
	TemporalNamespace string
	TemporalTaskQueue string
	PostgresURL       string
	PostgresPoolMax   int
	RedisURL          string
	LLMConfigPath     string
	LogLevel          string

	RegisterEnvTools      bool
	EnsureDefaultTemplate bool
}
