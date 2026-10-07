package core

import (
	"encoding/json"
	"reflect"
	"strings"

	"github.com/google/jsonschema-go/jsonschema"
)

// JSONSchemaFor builds a JSON schema for T, applying schema:"optional" tags to relax required fields.
func JSONSchemaFor[T any]() ([]byte, error) {
	schema, err := jsonschema.For[T](&jsonschema.ForOptions{
		TypeSchemas: map[reflect.Type]*jsonschema.Schema{
			reflect.TypeFor[json.RawMessage](): {
				Type: "object",
			},
		},
	})
	if err != nil {
		return nil, err
	}

	applySchemaOptionalTags(schema, reflect.TypeFor[T]())

	return json.MarshalIndent(schema, "", "  ")
}

func applySchemaOptionalTags(schema *jsonschema.Schema, typ reflect.Type) {
	if schema == nil {
		return
	}

	typ = indirectSchemaType(typ)
	if typ.Kind() != reflect.Struct {
		return
	}

	for field := range typ.Fields() {
		if field.PkgPath != "" {
			continue
		}

		name, ok := schemaJSONFieldName(&field)
		if !ok {
			continue
		}

		if field.Tag.Get("schema") == "optional" {
			schema.Required = removeSchemaRequiredField(schema.Required, name)
		}

		applySchemaOptionalTags(schemaForFieldType(schema.Properties[name], field.Type), field.Type)
	}
}

func schemaForFieldType(schema *jsonschema.Schema, typ reflect.Type) *jsonschema.Schema {
	for schema != nil && (typ.Kind() == reflect.Pointer || typ.Kind() == reflect.Slice || typ.Kind() == reflect.Array) {
		if typ.Kind() == reflect.Pointer {
			typ = typ.Elem()

			continue
		}

		schema = schema.Items
		typ = typ.Elem()
	}

	return schema
}

func indirectSchemaType(typ reflect.Type) reflect.Type {
	for typ.Kind() == reflect.Pointer || typ.Kind() == reflect.Slice || typ.Kind() == reflect.Array {
		typ = typ.Elem()
	}

	return typ
}

func schemaJSONFieldName(field *reflect.StructField) (string, bool) {
	if field == nil {
		return "", false
	}

	tag := field.Tag.Get("json")
	if tag == "-" {
		return "", false
	}

	name, _, _ := strings.Cut(tag, ",")
	if name == "" {
		name = field.Name
	}

	return name, true
}

func removeSchemaRequiredField(required []string, field string) []string {
	if len(required) == 0 {
		return required
	}

	out := required[:0]
	for _, name := range required {
		if name != field {
			out = append(out, name)
		}
	}

	return out
}
