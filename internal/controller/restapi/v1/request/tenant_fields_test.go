package request_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestNoRequestCarriesATenantAccount pins the invariant the control plane
// depends on: an account is never a request field, because an account from a
// request is not an identity. The credential supplies it and the handlers bind
// it to the project the request names.
//
// It reads the package source rather than its types so a new DTO cannot slip
// past it — the same reason agentos/core guards its import boundary from
// source. A field that legitimately needs naming would have to say why here.
func TestNoRequestCarriesATenantAccount(t *testing.T) {
	t.Parallel()

	offenders := accountTaggedFields(t)

	if len(offenders) > 0 {
		t.Fatalf("request DTOs must not carry a tenant account, found:\n%s", strings.Join(offenders, "\n"))
	}
}

// accountTaggedFields collects every struct tag in the package that binds an
// account, with the file it came from.
func accountTaggedFields(t *testing.T) []string {
	t.Helper()

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package directory: %v", err)
	}

	var offenders []string

	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}

		offenders = append(offenders, fileAccountTags(t, name)...)
	}

	return offenders
}

func fileAccountTags(t *testing.T, name string) []string {
	t.Helper()

	file, err := parser.ParseFile(token.NewFileSet(), filepath.Join(".", name), nil, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse %s: %v", name, err)
	}

	var offenders []string

	ast.Inspect(file, func(node ast.Node) bool {
		field, ok := node.(*ast.Field)
		if !ok || field.Tag == nil {
			return true
		}

		if strings.Contains(strings.ToLower(field.Tag.Value), "account_id") {
			offenders = append(offenders, name+": "+field.Tag.Value)
		}

		return true
	})

	return offenders
}
