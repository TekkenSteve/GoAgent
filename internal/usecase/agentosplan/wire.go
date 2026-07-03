package agentosplan

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"

	"github.com/TekkenSteve/GoAgent/agentos"
	"gopkg.in/yaml.v3"
)

// DecodeWireJSON decodes public RunPlan authoring JSON without accepting
// unknown struct fields. Free-form maps such as RunSpec.Input remain open.
func DecodeWireJSON[T any](data []byte) (T, error) {
	var value T

	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&value); err != nil {
		return value, fmt.Errorf("%w: decode json: %w", agentos.ErrInvalidRunPlan, err)
	}

	var trailing struct{}
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return value, fmt.Errorf("%w: decode json: multiple documents", agentos.ErrInvalidRunPlan)
		}

		return value, fmt.Errorf("%w: decode json trailing data: %w", agentos.ErrInvalidRunPlan, err)
	}

	return value, nil
}

// DecodeWireYAML decodes public RunPlan authoring YAML using the same strict
// field contract as JSON.
func DecodeWireYAML[T any](data []byte) (T, error) {
	var zero T

	jsonData, err := yamlWireToJSON(data)
	if err != nil {
		return zero, fmt.Errorf("%w: decode yaml: %w", agentos.ErrInvalidRunPlan, err)
	}

	return DecodeWireJSON[T](jsonData)
}

func yamlWireToJSON(data []byte) ([]byte, error) {
	var document yaml.Node
	if err := yaml.Unmarshal(data, &document); err != nil {
		return nil, err
	}

	value, err := yamlWireNodeValue(&document)
	if err != nil {
		return nil, err
	}

	return json.Marshal(value)
}

var (
	errWireDocumentRoot         = errors.New("document must contain exactly one root node")
	errWireAliasNotSupported    = errors.New("aliases are not supported")
	errWireUnsupportedNodeKind  = errors.New("unsupported yaml node kind")
	errWireMappingOddChildren   = errors.New("mapping node has an odd number of children")
	errWireMappingKeyScalar     = errors.New("mapping key must be scalar")
	errWireMappingDuplicateKey  = errors.New("duplicate mapping key")
	errWireUnsupportedScalarTag = errors.New("unsupported scalar tag")
)

func yamlWireNodeValue(node *yaml.Node) (any, error) {
	if node == nil {
		return nil, nil
	}

	switch node.Kind {
	case yaml.DocumentNode:
		if len(node.Content) == 0 {
			return nil, nil
		}

		if len(node.Content) != 1 {
			return nil, errWireDocumentRoot
		}

		return yamlWireNodeValue(node.Content[0])
	case yaml.MappingNode:
		return yamlWireMappingValue(node)
	case yaml.SequenceNode:
		return yamlWireSequenceValue(node)
	case yaml.ScalarNode:
		return yamlWireScalarValue(node)
	case yaml.AliasNode:
		return nil, errWireAliasNotSupported
	default:
		return nil, fmt.Errorf("%w: %d", errWireUnsupportedNodeKind, node.Kind)
	}
}

func yamlWireSequenceValue(node *yaml.Node) ([]any, error) {
	items := make([]any, 0, len(node.Content))

	for _, item := range node.Content {
		value, err := yamlWireNodeValue(item)
		if err != nil {
			return nil, err
		}

		items = append(items, value)
	}

	return items, nil
}

func yamlWireMappingValue(node *yaml.Node) (map[string]any, error) {
	const yamlMappingPairWidth = 2

	if len(node.Content)%2 != 0 {
		return nil, errWireMappingOddChildren
	}

	result := make(map[string]any, len(node.Content)/yamlMappingPairWidth)
	for i := 0; i < len(node.Content); i += yamlMappingPairWidth {
		keyNode := node.Content[i]
		if keyNode.Kind != yaml.ScalarNode {
			return nil, errWireMappingKeyScalar
		}

		key := keyNode.Value
		if _, exists := result[key]; exists {
			return nil, fmt.Errorf("%w: %q", errWireMappingDuplicateKey, key)
		}

		value, err := yamlWireNodeValue(node.Content[i+1])
		if err != nil {
			return nil, err
		}

		result[key] = value
	}

	return result, nil
}

func yamlWireScalarValue(node *yaml.Node) (any, error) {
	switch node.Tag {
	case "!!null":
		return nil, nil
	case "!!bool":
		value, err := strconv.ParseBool(node.Value)
		if err != nil {
			return nil, err
		}

		return value, nil
	case "!!int":
		var value int64
		if err := node.Decode(&value); err != nil {
			return nil, err
		}

		return value, nil
	case "!!float":
		var value float64
		if err := node.Decode(&value); err != nil {
			return nil, err
		}

		return value, nil
	case "!!str", "!!timestamp":
		return node.Value, nil
	default:
		return nil, fmt.Errorf("%w: %q", errWireUnsupportedScalarTag, node.Tag)
	}
}
