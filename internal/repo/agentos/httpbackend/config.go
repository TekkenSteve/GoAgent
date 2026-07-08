package httpbackend

import (
	"fmt"
	"net/url"
	"strings"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
)

// Config describes one HTTP agent backend.
type Config struct {
	Name     string
	Endpoint string
	Headers  map[string]string
}

// Ref returns the public AgentOS backend reference.
func (c Config) Ref() agentos.BackendRef {
	return agentos.BackendRef{
		Kind: agentos.BackendKindHTTP,
		Name: c.Name,
	}
}

func (c Config) validate() error {
	if c.Name == "" {
		return fmt.Errorf("%w: http backend name is required", agentoscore.ErrInvalidBackendRef)
	}

	if c.Endpoint == "" {
		return fmt.Errorf("%w: http backend endpoint is required", agentoscore.ErrInvalidBackendRef)
	}

	parsed, err := url.Parse(c.Endpoint)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return fmt.Errorf("%w: invalid http backend endpoint %q", agentoscore.ErrInvalidBackendRef, c.Endpoint)
	}

	return nil
}

func (c Config) endpoint(path string) string {
	return strings.TrimRight(c.Endpoint, "/") + path
}
