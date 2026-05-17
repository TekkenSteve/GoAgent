package loader

import (
	"encoding/json"
	"fmt"

	"github.com/TekkenSteve/GoAgent/internal/entity"
	"gopkg.in/yaml.v3"
)

// Spec is the YAML/JSON schema for defining a workflow in a config file.
// It mirrors TeamSpec but uses simplified agent references.
type Spec struct {
	Version      string   `json:"version" yaml:"version"`
	Name         string   `json:"name" yaml:"name"`
	Description  string   `json:"description,omitempty" yaml:"description,omitempty"`
	SystemPrompt string   `json:"system_prompt,omitempty" yaml:"system_prompt,omitempty"`
	DefaultModel string   `json:"default_model,omitempty" yaml:"default_model,omitempty"`
	Agents       []Agent  `json:"agents" yaml:"agents"`
	Steps        []Step   `json:"steps" yaml:"steps"`
	SubTeams     []Spec   `json:"sub_teams,omitempty" yaml:"sub_teams,omitempty"`
	Tags         []string `json:"tags,omitempty" yaml:"tags,omitempty"`
}

// Agent defines an agent in a config file.
type Agent struct {
	ID           string   `json:"id" yaml:"id"`
	Name         string   `json:"name" yaml:"name"`
	Model        string   `json:"model" yaml:"model"`
	SystemPrompt string   `json:"system_prompt,omitempty" yaml:"system_prompt,omitempty"`
	Tools        []string `json:"tools,omitempty" yaml:"tools,omitempty"`
}

// Step defines a step in a config file.
type Step struct {
	ID        string         `json:"id" yaml:"id"`
	Type      string         `json:"type" yaml:"type"`
	AgentRef  string         `json:"agent_ref,omitempty" yaml:"agent_ref,omitempty"`
	Tool      string         `json:"tool,omitempty" yaml:"tool,omitempty"`
	Input     map[string]any `json:"input,omitempty" yaml:"input,omitempty"`
	DependsOn []string       `json:"depends_on,omitempty" yaml:"depends_on,omitempty"`
}

// ParseYAML parses a YAML byte slice into a TeamSpec.
func ParseYAML(data []byte) (*entity.TeamSpec, error) {
	var spec Spec
	if err := yaml.Unmarshal(data, &spec); err != nil {
		return nil, fmt.Errorf("loader - ParseYAML - unmarshal: %w", err)
	}

	return convertSpec(&spec)
}

// ParseJSON parses a JSON byte slice into a TeamSpec.
func ParseJSON(data []byte) (*entity.TeamSpec, error) {
	var spec Spec
	if err := json.Unmarshal(data, &spec); err != nil {
		return nil, fmt.Errorf("loader - ParseJSON - unmarshal: %w", err)
	}

	return convertSpec(&spec)
}

func convertSpec(spec *Spec) (*entity.TeamSpec, error) {
	agents := make([]entity.AgentSpec, len(spec.Agents))
	for i, a := range spec.Agents {
		id := a.ID
		if id == "" {
			id = a.Name
		}

		tools := make([]entity.ToolBinding, len(a.Tools))
		for j, t := range a.Tools {
			tools[j] = entity.ToolBinding{Name: t}
		}

		agents[i] = entity.AgentSpec{
			ID:           id,
			Name:         a.Name,
			SystemPrompt: a.SystemPrompt,
			ModelRef:     a.Model,
			Tools:        tools,
		}
	}

	steps := make([]entity.StepTemplate, len(spec.Steps))
	for i, s := range spec.Steps {
		stepType := resolveStepType(&s)

		steps[i] = entity.StepTemplate{
			ID:        s.ID,
			Type:      stepType,
			AgentRef:  s.AgentRef,
			Tool:      s.Tool,
			Input:     s.Input,
			DependsOn: s.DependsOn,
		}
	}

	// Recursively convert sub-teams
	var subTeams []entity.TeamSpec

	for i := range spec.SubTeams {
		sub := spec.SubTeams[i]

		converted, err := convertSpec(&sub)
		if err != nil {
			return nil, err
		}

		subTeams = append(subTeams, *converted)
	}

	return &entity.TeamSpec{
		ID:       spec.Name,
		Name:     spec.Name,
		Agents:   agents,
		SubTeams: subTeams,
		Steps:    steps,
	}, nil
}

func resolveStepType(s *Step) entity.StepType {
	stepType := entity.StepType(s.Type)
	if stepType != "" {
		return stepType
	}

	switch {
	case s.AgentRef != "":
		return entity.StepAgent
	case s.Tool != "":
		return entity.StepTool
	default:
		return entity.StepAgent
	}
}
