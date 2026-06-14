package temporal

// RuntimeConfig configures the default Temporal/Redis runtime implementation.
type RuntimeConfig struct {
	TemporalAddress   string
	TemporalNamespace string
	TemporalTaskQueue string
	RedisURL          string
}

// WorkerConfig configures registration of GoAgent workflows and activities into
// a Temporal worker.
type WorkerConfig struct{}
