// Package dshbackend adapts DeepSeek Harness (dsh) into an AgentOS streaming
// backend. The adapter spawns the dsh SDK subprocess over stdio, drives it with
// newline-delimited JSON-RPC 2.0, maps the session/event stream onto entity
// events, and publishes them to the message bus through the same PublishWriter
// path a native streaming backend uses. dsh's stream never reaches a frontend
// directly: it lands on the bus and is consumed through a run's channel.
package dshbackend

import (
	"fmt"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
)

// Config describes one dsh backend: the SDK subprocess to spawn and the
// session profile to run it under. Command defaults to "dsh" and Profile to
// "sdk"; tests override Command with the test binary's own executable path.
type Config struct {
	Name       string
	Command    string
	Profile    string
	Args       []string
	Env        []string
	WorkingDir string
}

const (
	// defaultCommand is the dsh CLI name resolved from $PATH when Config.Command
	// is empty.
	defaultCommand = "dsh"

	// defaultProfile is the dsh session profile that serves the SDK protocol.
	defaultProfile = "sdk"
)

// Ref returns the public AgentOS backend reference.
func (c *Config) Ref() agentos.BackendRef {
	return agentos.BackendRef{
		Kind: agentos.BackendKindDSH,
		Name: c.Name,
	}
}

// withDefaults fills omitted Command and Profile fields with their defaults.
func (c *Config) withDefaults() {
	if c.Command == "" {
		c.Command = defaultCommand
	}

	if c.Profile == "" {
		c.Profile = defaultProfile
	}
}

// validate verifies the config is spawnable.
func (c *Config) validate() error {
	if c.Name == "" {
		return fmt.Errorf("%w: dsh backend name is required", agentoscore.ErrInvalidBackendRef)
	}

	if c.Command == "" {
		return fmt.Errorf("%w: dsh backend command is required", agentoscore.ErrInvalidBackendRef)
	}

	return nil
}
