package agentosplan

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"

	"github.com/TekkenSteve/GoAgent/agentos"
)

// CapabilitySelectionTrace is the durable debug view for a selected backend
// capability without embedding potentially large JSON schemas in event history.
type CapabilitySelectionTrace struct {
	Backend         agentos.BackendRef         `json:"backend"`
	Capability      string                     `json:"capability"`
	Signals         []agentos.SignalType       `json:"signals,omitempty"`
	Controls        []agentos.ControlOperation `json:"controls,omitempty"`
	HasInputSchema  bool                       `json:"has_input_schema,omitempty"`
	HasOutputSchema bool                       `json:"has_output_schema,omitempty"`
}

// InputResolutionTrace describes how a node input was resolved without storing
// full input payloads in workflow history or durable events.
type InputResolutionTrace struct {
	InputDigest  string              `json:"input_digest,omitempty"`
	InputKeys    []string            `json:"input_keys,omitempty"`
	MappingCount int                 `json:"mapping_count,omitempty"`
	Mappings     []InputMappingTrace `json:"mappings,omitempty"`
}

// InputMappingTrace is the redacted debug form of one mapping rule.
type InputMappingTrace struct {
	Target         string `json:"target"`
	SourceNodeID   string `json:"source_node_id,omitempty"`
	SourceArtifact string `json:"source_artifact,omitempty"`
	SourcePath     string `json:"source_path,omitempty"`
	Expression     string `json:"expression,omitempty"`
	Required       bool   `json:"required,omitempty"`
}

// NewCapabilitySelectionTrace creates the event-safe trace projection for a
// capability selected during plan validation.
func NewCapabilitySelectionTrace(capability agentos.Capability) CapabilitySelectionTrace {
	return CapabilitySelectionTrace{
		Backend:         capability.Backend,
		Capability:      capability.Name,
		Signals:         append([]agentos.SignalType(nil), capability.Signals...),
		Controls:        append([]agentos.ControlOperation(nil), capability.Controls...),
		HasInputSchema:  len(capability.InputSchema) > 0,
		HasOutputSchema: len(capability.OutputSchema) > 0,
	}
}

// NewInputResolutionTrace creates a redacted trace for resolved input mapping.
func NewInputResolutionTrace(resolved map[string]any, nodeMappings []agentos.InputMapping, edges []agentos.PlanEdgeSpec) InputResolutionTrace {
	mappings := make([]agentos.InputMapping, 0, len(nodeMappings))
	mappings = append(mappings, nodeMappings...)
	for _, edge := range edges {
		mappings = append(mappings, edge.InputMapping...)
	}

	data, err := json.Marshal(resolved)
	digest := ""
	if err == nil {
		sum := sha256.Sum256(data)
		digest = hex.EncodeToString(sum[:])
	}

	keys := make([]string, 0, len(resolved))
	for key := range resolved {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	traceMappings := make([]InputMappingTrace, 0, len(mappings))
	for _, mapping := range mappings {
		traceMappings = append(traceMappings, InputMappingTrace{
			Target:         mapping.Target,
			SourceNodeID:   mapping.SourceNodeID,
			SourceArtifact: mapping.SourceArtifact,
			SourcePath:     mapping.SourcePath,
			Expression:     mapping.Expression,
			Required:       mapping.Required,
		})
	}

	return InputResolutionTrace{
		InputDigest:  digest,
		InputKeys:    keys,
		MappingCount: len(traceMappings),
		Mappings:     traceMappings,
	}
}
