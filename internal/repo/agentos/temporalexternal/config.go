package temporalexternal

import (
	"fmt"

	"github.com/TekkenSteve/GoAgent/agentos"
)

// Config describes one external Temporal workflow backend.
type Config struct {
	Name         string
	TaskQueue    string
	WorkflowType string
	QueryType    string
	Signals      SignalNames
}

// SignalNames maps AgentOS signals and controls to workflow signal names.
type SignalNames struct {
	Pause    string
	Resume   string
	Cancel   string
	Defaults map[agentos.SignalType]string
}

// Ref returns the public AgentOS backend reference for this backend.
func (c *Config) Ref() agentos.BackendRef {
	if c == nil {
		return agentos.BackendRef{}
	}

	return agentos.BackendRef{
		Kind: agentos.BackendKindTemporalExternal,
		Name: c.Name,
	}
}

func (c *Config) validate() error {
	if c == nil {
		return fmt.Errorf("%w: temporal external backend config is required", agentos.ErrInvalidBackendRef)
	}

	if c.Name == "" {
		return fmt.Errorf("%w: temporal external backend name is required", agentos.ErrInvalidBackendRef)
	}

	if c.TaskQueue == "" {
		return fmt.Errorf("%w: temporal external task queue is required", agentos.ErrInvalidBackendRef)
	}

	if c.WorkflowType == "" {
		return fmt.Errorf("%w: temporal external workflow type is required", agentos.ErrInvalidBackendRef)
	}

	if c.QueryType == "" {
		return fmt.Errorf("%w: temporal external status query type is required", agentos.ErrInvalidBackendRef)
	}

	return nil
}
