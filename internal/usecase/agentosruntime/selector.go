package agentosruntime

import (
	"context"
	"fmt"
	"reflect"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	agentoscore "github.com/TekkenSteve/GoAgent/agentos/core"
)

// BackendSelectionRule maps generic RunSpec attributes to a backend.
type BackendSelectionRule struct {
	Name     string
	Backend  agentos.BackendRef
	AgentID  string
	Metadata map[string]string
	Input    map[string]any
}

// RuleBackendSelector resolves a backend from ordered exact-match rules.
type RuleBackendSelector struct {
	rules []BackendSelectionRule
}

// NewRuleBackendSelector creates a deterministic selector from ordered rules.
func NewRuleBackendSelector(rules []BackendSelectionRule) (*RuleBackendSelector, error) {
	if len(rules) == 0 {
		return nil, fmt.Errorf("%w: backend selection rules are required", agentoscore.ErrInvalidBackendRef)
	}

	copied := make([]BackendSelectionRule, 0, len(rules))
	for _, rule := range rules {
		if err := validateBackendRef(rule.Backend); err != nil {
			return nil, err
		}

		if rule.AgentID == "" && len(rule.Metadata) == 0 && len(rule.Input) == 0 {
			return nil, fmt.Errorf("%w: backend selection rule %q has no match criteria", agentoscore.ErrInvalidBackendRef, rule.Name)
		}

		copied = append(copied, rule)
	}

	return &RuleBackendSelector{rules: copied}, nil
}

// Select returns the backend from the first rule matching the RunSpec.
func (s *RuleBackendSelector) Select(_ context.Context, spec *agentos.RunSpec) (agentos.BackendRef, error) {
	for index := range s.rules {
		if ruleMatches(&s.rules[index], spec) {
			return s.rules[index].Backend, nil
		}
	}

	return agentos.BackendRef{}, fmt.Errorf("%w: no backend selection rule matched run %q", agentoscore.ErrInvalidBackendRef, spec.RunID)
}

func ruleMatches(rule *BackendSelectionRule, spec *agentos.RunSpec) bool {
	if rule.AgentID != "" && rule.AgentID != spec.AgentID {
		return false
	}

	for key, want := range rule.Metadata {
		if spec.Metadata[key] != want {
			return false
		}
	}

	for key, want := range rule.Input {
		if !reflect.DeepEqual(spec.Input[key], want) {
			return false
		}
	}

	return true
}
