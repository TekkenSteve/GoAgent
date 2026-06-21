package grpcbackend

import (
	"fmt"
	"strings"

	"github.com/TekkenSteve/GoAgent/agentos"
)

const defaultService = "agentos.v1.AgentBackend"

// Config describes one gRPC agent backend.
type Config struct {
	Name      string
	Target    string
	Authority string
	Insecure  bool
	Service   string
	Methods   MethodNames
}

// MethodNames maps AgentOS operations to gRPC unary method names.
type MethodNames struct {
	Start   string
	Signal  string
	Control string
	Status  string
}

// Ref returns the public AgentOS backend reference.
func (c Config) Ref() agentos.BackendRef {
	return agentos.BackendRef{
		Kind: agentos.BackendKindGRPC,
		Name: c.Name,
	}
}

func (c *Config) normalize() error {
	if c.Name == "" {
		return fmt.Errorf("%w: grpc backend name is required", agentos.ErrInvalidBackendRef)
	}
	if c.Target == "" {
		return fmt.Errorf("%w: grpc backend target is required", agentos.ErrInvalidBackendRef)
	}
	if c.Service == "" {
		c.Service = defaultService
	}
	if c.Methods.Start == "" {
		c.Methods.Start = "StartRun"
	}
	if c.Methods.Signal == "" {
		c.Methods.Signal = "SignalRun"
	}
	if c.Methods.Control == "" {
		c.Methods.Control = "ControlRun"
	}
	if c.Methods.Status == "" {
		c.Methods.Status = "StatusRun"
	}

	return nil
}

func (c Config) method(name string) string {
	return "/" + strings.Trim(c.Service, "/") + "/" + strings.Trim(name, "/")
}
