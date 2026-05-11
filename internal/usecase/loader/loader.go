package loader

import (
	"encoding/json"
	"fmt"

	"github.com/TekkenSteve/GoAgent/internal/entity"
	"gopkg.in/yaml.v3"
)

// LoaderSpec is the YAML/JSON schema for defining a workflow in a config file.
// It mirrors TeamSpec but uses simplified agent references.
type LoaderSpec struct {
	Version      string           `json:"version" yaml:"version"`
	Name         string           `json:"name" yaml:"name"`
	Description  string           `json:"description,omitempty" yaml:"description,omitempty"`
	SystemPrompt string           `json:"system_prompt,omitempty" yaml:"system_prompt,omitempty"`
	DefaultModel string           `json:"default_model,omitempty" yaml:"default_model,omitempty"`
	Agents       []LoaderAgent    `json:"agents" yaml:"agents"`
	Steps        []LoaderStep     `json:"steps" yaml:"steps"`
	SubTeams     []LoaderSpec     `json:"sub_teams,omitempty" yaml:"sub_teams,omitempty"`
	Tags         []string         `json:"tags,omitempty" yaml:"tags,omitempty"`
}

// LoaderAgent defines an agent in a config file.
type LoaderAgent struct {
	ID           string   `json:"id" yaml:"id"`
	Name         string   `json:"name" yaml:"name"`
	Model        string   `json:"model" yaml:"model"`
	SystemPrompt string   `json:"system_prompt,omitempty" yaml:"system_prompt,omitempty"`
	Tools        []string `json:"tools,omitempty" yaml:"tools,omitempty"`
}

// LoaderStep defines a step in a config file.
type LoaderStep struct {
	ID        string            `json:"id" yaml:"id"`
	Type      string            `json:"type" yaml:"type"`
	AgentRef  string            `json:"agent_ref,omitempty" yaml:"agent_ref,omitempty"`
	Tool      string            `json:"tool,omitempty" yaml:"tool,omitempty"`
	Input     map[string]any    `json:"input,omitempty" yaml:"input,omitempty"`
	DependsOn []string          `json:"depends_on,omitempty" yaml:"depends_on,omitempty"`
}

// ParseYAML parses a YAML byte slice into a TeamSpec.
func ParseYAML(data []byte) (*entity.TeamSpec, error) {
	var spec LoaderSpec
	if err := yaml.Unmarshal(data, &spec); err != nil {
		return nil, fmt.Errorf("Loader - ParseYAML - unmarshal: %w", err)
	}
	return convertSpec(&spec)
}

// ParseJSON parses a JSON byte slice into a TeamSpec.
func ParseJSON(data []byte) (*entity.TeamSpec, error) {
	var spec LoaderSpec
	if err := json.Unmarshal(data, &spec); err != nil {
		return nil, fmt.Errorf("Loader - ParseJSON - unmarshal: %w", err)
	}
	return convertSpec(&spec)
}

func convertSpec(spec *LoaderSpec) (*entity.TeamSpec, error) {
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
		stepType := entity.StepType(s.Type)
		if stepType == "" {
			if s.AgentRef != "" {
				stepType = entity.StepAgent
			} else if s.Tool != "" {
				stepType = entity.StepTool
			} else {
				stepType = entity.StepAgent
			}
		}

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
	for _, sub := range spec.SubTeams {
		converted, err := convertSpec(&sub)
		if err != nil {
			return nil, err
		}
		subTeams = append(subTeams, *converted)
	}

	return &entity.TeamSpec{
		ID:           spec.Name,
		Name:         spec.Name,
		Agents:       agents,
		SubTeams:     subTeams,
		Steps:        steps,
	}, nil
}
