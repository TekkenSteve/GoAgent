package agentos

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
)

const modulePath = "github.com/TekkenSteve/GoAgent"

var forbiddenPublicImportSegments = []string{
	"agentfw",
	"entity",
	"internal",
	"repo",
	"usecase",
}

func TestPublicExamplesAndDocsRespectAgentOSImportBoundary(t *testing.T) {
	repoRoot := testRepoRoot(t)
	for _, dir := range []string{"examples", "docs"} {
		root := filepath.Join(repoRoot, dir)
		if _, err := os.Stat(root); err != nil {
			if os.IsNotExist(err) {
				continue
			}
			t.Fatalf("stat %s: %v", root, err)
		}
		if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() || filepath.Ext(path) != ".go" {
				return nil
			}
			assertPublicGoFileImports(t, path)

			return nil
		}); err != nil {
			t.Fatalf("walk %s: %v", root, err)
		}
	}
}

func testRepoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime caller unavailable")
	}

	return filepath.Dir(filepath.Dir(file))
}

func assertPublicGoFileImports(t *testing.T, path string) {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	for _, spec := range file.Imports {
		importPath := strings.Trim(spec.Path.Value, `"`)
		if segment, ok := forbiddenAgentOSImportSegment(importPath); ok {
			t.Fatalf("%s imports private GoAgent package segment %q via %s", path, segment, importPath)
		}
	}
}

func forbiddenAgentOSImportSegment(importPath string) (string, bool) {
	if importPath == modulePath {
		return "", false
	}
	rest, ok := strings.CutPrefix(importPath, modulePath+"/")
	if !ok {
		return "", false
	}
	segment, _, _ := strings.Cut(rest, "/")
	if slices.Contains(forbiddenPublicImportSegments, segment) {
		return segment, true
	}

	return "", false
}
