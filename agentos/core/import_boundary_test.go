package core

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

const (
	goFileExt  = ".go"
	modulePath = "github.com/TekkenSteve/GoAgent"
)

func TestPublicExamplesAndDocsRespectAgentOSImportBoundary(t *testing.T) {
	t.Parallel()

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

			if entry.IsDir() || filepath.Ext(path) != goFileExt {
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

	return filepath.Dir(filepath.Dir(filepath.Dir(file)))
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
	if isForbiddenPublicImportSegment(segment) {
		return segment, true
	}

	return "", false
}

func isForbiddenPublicImportSegment(segment string) bool {
	return slices.Contains([]string{
		"agentfw",
		"entity",
		"internal",
		"repo",
		"usecase",
	}, segment)
}

func TestAgentOSPublicPackagesRespectLayering(t *testing.T) {
	t.Parallel()

	repoRoot := testRepoRoot(t)
	rules := []packageImportRule{
		{
			dir: "agentos/core",
			forbiddenPrefixes: []string{
				modulePath + "/agentos/control",
				modulePath + "/agentos/process",
				modulePath + "/agentos/platform",
				modulePath + "/agentos/temporal",
				modulePath + "/internal/",
			},
		},
		{
			dir: "agentos/stream",
			forbiddenPrefixes: []string{
				modulePath + "/agentos/control",
				modulePath + "/agentos/process",
				modulePath + "/agentos/platform",
				modulePath + "/agentos/temporal",
				modulePath + "/internal/",
			},
		},
		{
			dir: "agentos/control",
			forbiddenPrefixes: []string{
				modulePath + "/agentos/process",
				modulePath + "/agentos/platform",
				modulePath + "/agentos/temporal",
				modulePath + "/internal/",
			},
		},
		{
			dir: "agentos/process",
			forbiddenPrefixes: []string{
				modulePath + "/agentos/control",
				modulePath + "/agentos/platform",
				modulePath + "/agentos/temporal",
				modulePath + "/internal/",
			},
		},
	}

	for _, rule := range rules {
		t.Run(rule.dir, func(t *testing.T) {
			t.Parallel()

			assertPackageDoesNotImport(t, filepath.Join(repoRoot, rule.dir), rule.forbiddenPrefixes)
		})
	}
}

func TestBackendAdaptersDoNotImportProcessLayer(t *testing.T) {
	t.Parallel()

	repoRoot := testRepoRoot(t)
	for _, dir := range []string{
		"internal/repo/agentos/httpbackend",
		"internal/repo/agentos/grpcbackend",
		"internal/repo/agentos/temporalexternal",
	} {
		t.Run(dir, func(t *testing.T) {
			t.Parallel()

			assertPackageDoesNotImport(t, filepath.Join(repoRoot, dir), []string{
				modulePath + "/agentos/process",
			})
		})
	}
}

type packageImportRule struct {
	dir               string
	forbiddenPrefixes []string
}

func assertPackageDoesNotImport(t *testing.T, root string, forbiddenPrefixes []string) {
	t.Helper()

	if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if skip, err := skipImportCheck(entry, path, err); skip || err != nil {
			return err
		}

		assertGoFileDoesNotImport(t, path, forbiddenPrefixes)

		return nil
	}); err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
}

func skipImportCheck(entry os.DirEntry, path string, err error) (bool, error) {
	if err != nil {
		return false, err
	}

	if entry.IsDir() || filepath.Ext(path) != goFileExt {
		return true, nil
	}

	return false, nil
}

func assertGoFileDoesNotImport(t *testing.T, path string, forbiddenPrefixes []string) {
	t.Helper()

	file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}

	for _, spec := range file.Imports {
		importPath := strings.Trim(spec.Path.Value, `"`)
		if hasForbiddenPrefix(importPath, forbiddenPrefixes) {
			t.Fatalf("%s imports forbidden package %s", path, importPath)
		}
	}
}

func hasForbiddenPrefix(importPath string, forbiddenPrefixes []string) bool {
	for _, prefix := range forbiddenPrefixes {
		if strings.HasPrefix(importPath, prefix) {
			return true
		}
	}

	return false
}
