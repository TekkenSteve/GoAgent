package httpbackend

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/TekkenSteve/GoAgent/agentos"
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
		return fmt.Errorf("%w: http backend name is required", agentos.ErrInvalidBackendRef)
	}

	if c.Endpoint == "" {
		return fmt.Errorf("%w: http backend endpoint is required", agentos.ErrInvalidBackendRef)
	}

	parsed, err := url.Parse(c.Endpoint)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return fmt.Errorf("%w: invalid http backend endpoint %q", agentos.ErrInvalidBackendRef, c.Endpoint)
	}

	return nil
}

func (c Config) endpoint(path string) string {
	return strings.TrimRight(c.Endpoint, "/") + path
}
