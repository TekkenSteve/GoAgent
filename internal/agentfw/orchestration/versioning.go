package orchestration

import "slices"

import "fmt"

// VersionPins are versions pinned at run start.
type VersionPins struct {
	WorkflowVersion    int
	ToolSchemaVersion  string
	EventSchemaVersion string
}

// VersionCompatibility defines allowed versions for migration windows.
type VersionCompatibility struct {
	WorkflowVersions []int
	ToolSchemas      []string
	EventSchemas     []string
}

// ValidateVersionPins enforces explicit run-version compatibility rules.
func ValidateVersionPins(pins VersionPins, compatibility VersionCompatibility) error {
	if !containsInt(compatibility.WorkflowVersions, pins.WorkflowVersion) {
		return fmt.Errorf("unsupported workflow version: %d", pins.WorkflowVersion)
	}
	if !containsStr(compatibility.ToolSchemas, pins.ToolSchemaVersion) {
		return fmt.Errorf("unsupported tool schema version: %s", pins.ToolSchemaVersion)
	}
	if !containsStr(compatibility.EventSchemas, pins.EventSchemaVersion) {
		return fmt.Errorf("unsupported event schema version: %s", pins.EventSchemaVersion)
	}
	return nil
}

func containsInt(values []int, target int) bool {
	return slices.Contains(values, target)
}

func containsStr(values []string, target string) bool {
	return slices.Contains(values, target)
}
