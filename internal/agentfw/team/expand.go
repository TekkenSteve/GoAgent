// Package team provides TeamSpec expansion and template resolution.
package team

import (
	"fmt"

	"github.com/TekkenSteve/GoAgent/internal/agentfw/agent"
	"github.com/TekkenSteve/GoAgent/internal/entity"
)

// ExpandOption controls expansion behaviour.
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
func Expand(spec *entity.TeamSpec, registry *agent.AgentRegistry, opts ...ExpandOption) ([]entity.Step, error) {
	cfg := &expandConfig{maxDepth: 5}
	for _, opt := range opts {
		opt(cfg)
	}

	if spec == nil {
		return nil, fmt.Errorf("team expand: nil team spec")
	}

	var steps []entity.Step
	if err := expandRecursive(spec, registry, &steps, 0, cfg.maxDepth); err != nil {
		return nil, err
	}
	return steps, nil
}

func expandRecursive(spec *entity.TeamSpec, registry *agent.AgentRegistry, steps *[]entity.Step, depth, maxDepth int) error {
	if depth > maxDepth {
		return fmt.Errorf("team expand: max recursion depth %d exceeded for team %q", maxDepth, spec.ID)
	}

	// Validate agents reference
	agentIDs := make(map[string]bool, len(spec.Agents))
	for _, a := range spec.Agents {
		if a.ID == "" {
			return fmt.Errorf("team expand: agent with empty id in team %q", spec.ID)
		}
		agentIDs[a.ID] = true
		// Ensure agent is registered
		if _, ok := registry.Get(a.ID); !ok {
			// Auto-register if not already in registry
			if err := registry.RegisterFromSpec(a, nil); err != nil {
				return fmt.Errorf("team expand: register agent %q: %w", a.ID, err)
			}
		}
	}

	// Expand sub-teams recursively first
	for _, sub := range spec.SubTeams {
		if err := expandRecursive(&sub, registry, steps, depth+1, maxDepth); err != nil {
			return err
		}
	}

	// Convert StepTemplates to Steps, resolving agent references
	for _, tmpl := range spec.Steps {
		step, err := resolveTemplate(tmpl, agentIDs, spec.ID)
		if err != nil {
			return err
		}
		*steps = append(*steps, step)
	}

	return nil
}

func resolveTemplate(tmpl entity.StepTemplate, agentIDs map[string]bool, teamID string) (entity.Step, error) {
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
			if !agentIDs[tmpl.AgentRef] {
				return entity.Step{}, fmt.Errorf(
					"team %q: step %q references unknown agent %q",
					teamID, tmpl.ID, tmpl.AgentRef,
				)
			}
			step.AgentID = tmpl.AgentRef
		}
	case entity.StepTool:
		step.Tool = tmpl.Tool
	case entity.StepWait:
		step.WaitFor = tmpl.WaitFor
	case entity.StepSplit, entity.StepJoin, entity.StepEval:
		// no additional resolution needed
	}

	return step, nil
}
