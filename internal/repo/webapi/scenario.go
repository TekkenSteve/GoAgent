package webapi

import (
	"fmt"

	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
	"github.com/maximhq/bifrost/core/schemas"
)

// CandidateConfig is a single model candidate within a scenario.
type CandidateConfig struct {
	Provider    string  `yaml:"provider"`
	Model       string  `yaml:"model"`
	Temperature float64 `yaml:"temperature"`
	MaxTokens   int     `yaml:"max_tokens"`
}

// ScenarioConfig defines a named usage scenario with ordered candidates.
// The first candidate with a configured API key is used as primary;
// remaining candidates become automatic fallbacks via Bifrost SDK.
type ScenarioConfig struct {
	Description string            `yaml:"description"`
	Candidates  []CandidateConfig `yaml:"candidates"`
}

// ProviderConfig is a single provider entry in the YAML config.
type ProviderConfig struct {
	Name    string `yaml:"name"`
	APIKey  string `yaml:"api_key"`
	BaseURL string `yaml:"base_url"`
}

// LLMConfigFile is the root structure of the LLM YAML configuration.
type LLMConfigFile struct {
	Providers []ProviderConfig          `yaml:"providers"`
	Scenarios map[string]ScenarioConfig `yaml:"scenarios"`
}

// LoadLLMConfigFile reads and parses a YAML LLM configuration file via koanf.
func LoadLLMConfigFile(path string) (*LLMConfigFile, error) {
	k := koanf.New(".")
	fp := file.Provider(path)
	if err := k.Load(fp, yaml.Parser()); err != nil {
		return nil, fmt.Errorf("koanf load: %w", err)
	}

	var cfg LLMConfigFile
	if err := k.UnmarshalWithConf("", &cfg, koanf.UnmarshalConf{Tag: "yaml"}); err != nil {
		return nil, fmt.Errorf("koanf unmarshal: %w", err)
	}

	if len(cfg.Providers) == 0 {
		return nil, fmt.Errorf("LLM config has no providers")
	}

	return &cfg, nil
}

// ToProviderEntries converts the YAML provider configs to ProviderEntry slice.
func (f *LLMConfigFile) ToProviderEntries() []ProviderEntry {
	entries := make([]ProviderEntry, 0, len(f.Providers))
	for _, p := range f.Providers {
		entries = append(entries, ProviderEntry{
			Provider: schemas.ModelProvider(p.Name),
			APIKey:   p.APIKey,
			BaseURL:  p.BaseURL,
		})
	}
	return entries
}

// DefaultScenarioName returns the name of a scenario to use as default.
// Returns the "default" key if present, otherwise the first scenario.
func (f *LLMConfigFile) DefaultScenarioName() string {
	if _, ok := f.Scenarios["default"]; ok {
		return "default"
	}
	for name := range f.Scenarios {
		return name
	}
	return ""
}

// LLMProvidersResult holds the parsed LLM configuration for wiring into app.
type LLMProvidersResult struct {
	Providers       []ProviderEntry
	Scenarios       map[string]ScenarioConfig
	DefaultScenario string
}

// LoadLLMProviders reads and parses the LLM YAML config at path.
// Returns nil, nil when path is empty.
func LoadLLMProviders(path string) (*LLMProvidersResult, error) {
	if path == "" {
		return nil, nil
	}
	cfg, err := LoadLLMConfigFile(path)
	if err != nil {
		return nil, fmt.Errorf("load LLM config: %w", err)
	}
	return &LLMProvidersResult{
		Providers:       cfg.ToProviderEntries(),
		Scenarios:       cfg.Scenarios,
		DefaultScenario: cfg.DefaultScenarioName(),
	}, nil
}
