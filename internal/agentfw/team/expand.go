// Package team provides TeamSpec expansion and template resolution.
package team

import (
	"errors"
	"fmt"

	"github.com/TekkenSteve/GoAgent/internal/agentfw/agent"
	"github.com/TekkenSteve/GoAgent/internal/entity"
)

var (
	// ErrNilTeamSpec is returned when a nil TeamSpec is provided to Expand.
	ErrNilTeamSpec = errors.New("team expand: nil team spec")
	// ErrMaxDepthExceeded is returned when sub-team recursion exceeds the limit.
	ErrMaxDepthExceeded = errors.New("team expand: max recursion depth exceeded")
	// ErrEmptyAgentID is returned when an agent spec has an empty ID.
	ErrEmptyAgentID = errors.New("team expand: agent with empty id")
	// ErrUnknownAgentRef is returned when a step references an unknown agent.
	ErrUnknownAgentRef = errors.New("team expand: step references unknown agent")
)

const defaultMaxDepth = 5

// ExpandOption controls expansion behavior.
type ExpandOption func(*expandConfig)

type expandConfig struct {
	maxDepth int
}

// WithMaxDepth limits recursive sub-team expansion depth. Default 5.
func WithMaxDepth(d int) ExpandOption {
	return func(c *expandConfig) {
		c.maxDepth = d
	}
}

// Expand flattens a TeamSpec into a []Step by recursively resolving
// SubTeams and replacing AgentRef entries with concrete agent lookups.
func Expand(spec *entity.TeamSpec, registry *agent.Registry, opts ...ExpandOption) ([]entity.Step, error) {
	cfg := &expandConfig{maxDepth: defaultMaxDepth}
	for _, opt := range opts {
		opt(cfg)
	}

	if spec == nil {
		return nil, ErrNilTeamSpec
	}

	var steps []entity.Step
	if err := expandRecursive(spec, registry, &steps, 0, cfg.maxDepth); err != nil {
		return nil, err
	}

	return steps, nil
}

func expandRecursive(spec *entity.TeamSpec, registry *agent.Registry, steps *[]entity.Step, depth, maxDepth int) error {
	if depth > maxDepth {
		return fmt.Errorf("%w: depth %d for team %q", ErrMaxDepthExceeded, depth, spec.ID)
	}

	agentIDs := make(map[string]bool, len(spec.Agents))
	for i := range spec.Agents {
		a := &spec.Agents[i]
		if a.ID == "" {
			return fmt.Errorf("%w in team %q", ErrEmptyAgentID, spec.ID)
		}

		agentIDs[a.ID] = true
		if err := ensureAgentRegistered(registry, a); err != nil {
			return err
		}
	}

	for i := range spec.SubTeams {
		if err := expandRecursive(&spec.SubTeams[i], registry, steps, depth+1, maxDepth); err != nil {
			return err
		}
	}

	for i := range spec.Steps {
		step, err := resolveTemplate(&spec.Steps[i], agentIDs, spec.ID)
		if err != nil {
			return err
		}

		*steps = append(*steps, step)
	}

	return nil
}

// ensureAgentRegistered checks if an agent is registered and auto-registers if needed.
func ensureAgentRegistered(registry *agent.Registry, a *entity.AgentSpec) error {
	if _, ok := registry.Get(a.ID); ok {
		return nil
	}

	if err := registry.RegisterFromSpec(a, nil); err != nil {
		return fmt.Errorf("register agent %q: %w", a.ID, err)
	}

	return nil
}

func resolveTemplate(tmpl *entity.StepTemplate, knownAgents map[string]bool, teamID string) (entity.Step, error) {
	step := entity.Step{
		ID:        tmpl.ID,
		Type:      tmpl.Type,
		Name:      tmpl.ID,
		Input:     tmpl.Input,
		DependsOn: tmpl.DependsOn,
		WaitFor:   tmpl.WaitFor,
		OnResult:  tmpl.OnResult,
		Status:    entity.StepPending,
	}

	switch tmpl.Type {
	case entity.StepAgent:
		step.Tool = ""

		if tmpl.AgentRef != "" {
			if !knownAgents[tmpl.AgentRef] {
				return entity.Step{}, fmt.Errorf("%w %q in team %q", ErrUnknownAgentRef, tmpl.AgentRef, teamID)
			}

			step.AgentID = tmpl.AgentRef
		}

	case entity.StepTool:
		step.Tool = tmpl.Tool

	case entity.StepWait:
		step.WaitFor = tmpl.WaitFor

	case entity.StepSplit, entity.StepJoin, entity.StepEval:
	}

	return step, nil
}
