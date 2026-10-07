package v1

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestNoEndpointDocumentsATenantAccount pins the documented half of the
// invariant request/tenant_fields_test.go pins in the code: an account is never
// a request parameter, so no handler may advertise one. A published parameter
// the server ignores is worse than no parameter — it teaches callers a contract
// that does not exist, and every generated client then requires it.
//
// It reads the package source, for the same reason the request-package guard
// does: an annotation that is not a type cannot be caught by the compiler.
func TestNoEndpointDocumentsATenantAccount(t *testing.T) {
	t.Parallel()

	offenders := tenantAccountParams(t)

	if len(offenders) > 0 {
		t.Fatalf("endpoints must not document an account request parameter, found:\n%s", strings.Join(offenders, "\n"))
	}
}

// tenantAccountParams collects every swagger @Param annotation naming an
// account, with the file and function it documents.
func tenantAccountParams(t *testing.T) []string {
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

		offenders = append(offenders, fileTenantAccountParams(t, name)...)
	}

	return offenders
}

func fileTenantAccountParams(t *testing.T, name string) []string {
	t.Helper()

	file, err := parser.ParseFile(token.NewFileSet(), filepath.Join(".", name), nil, parser.ParseComments)
	if err != nil {
		t.Fatalf("parse %s: %v", name, err)
	}

	var offenders []string

	ast.Inspect(file, func(node ast.Node) bool {
		fn, ok := node.(*ast.FuncDecl)
		if !ok || fn.Doc == nil {
			return true
		}

		for _, comment := range fn.Doc.List {
			fields := strings.Fields(comment.Text)

			// An annotation reads "@Param <name> <in> ..."; only the first two
			// tokens matter, and they are structure rather than prose.
			for i, field := range fields {
				if field == "@Param" && i+1 < len(fields) && fields[i+1] == "account_id" {
					offenders = append(offenders, name+": "+fn.Name.Name+": "+comment.Text)
				}
			}
		}

		return true
	})

	return offenders
}
