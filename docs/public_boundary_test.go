package docs

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestSwaggerDoesNotExposeInternalEntityDefinitions(t *testing.T) {
	data, err := os.ReadFile("swagger.json")
	if err != nil {
		t.Fatalf("ReadFile swagger.json: %v", err)
	}

	var doc map[string]any
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatalf("decode swagger.json: %v", err)
	}

	definitions, ok := doc["definitions"].(map[string]any)
	if !ok {
		t.Fatal("swagger.json has no definitions object")
	}
	for name := range definitions {
		if strings.HasPrefix(name, "entity.") {
			t.Fatalf("public swagger must not expose internal entity definition %q", name)
		}
	}
	assertNoInternalEntityRef(t, doc)
}

func assertNoInternalEntityRef(t *testing.T, value any) {
	t.Helper()

	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			if key == "$ref" {
				ref, ok := child.(string)
				if ok && strings.HasPrefix(ref, "#/definitions/entity.") {
					t.Fatalf("public swagger must not reference internal entity schema %q", ref)
				}
			}
			assertNoInternalEntityRef(t, child)
		}
	case []any:
		for _, child := range typed {
			assertNoInternalEntityRef(t, child)
		}
	}
}
