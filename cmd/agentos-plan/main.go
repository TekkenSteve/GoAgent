package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"

	"github.com/TekkenSteve/GoAgent/agentos"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosplan"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosplan/serverlessworkflow"
	"sigs.k8s.io/yaml"
)

type compiledPlanOutput struct {
	Spec  agentos.RunPlanSpec `json:"spec"`
	Order []string            `json:"order"`
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout io.Writer, stderr io.Writer) int {
	if len(args) == 0 {
		printUsage(stderr)

		return 2
	}

	switch args[0] {
	case "schema":
		return runSchema(args[1:], stdout, stderr)
	case "validate":
		return runValidate(args[1:], stdout, stderr)
	case "compile":
		return runCompile(args[1:], stdout, stderr)
	case "validate-delta":
		return runValidateDelta(args[1:], stdout, stderr)
	case "compile-delta":
		return runCompileDelta(args[1:], stdout, stderr)
	case "export-serverless":
		return runExportServerless(args[1:], stdout, stderr)
	case "import-serverless":
		return runImportServerless(args[1:], stdout, stderr)
	default:
		_, _ = fmt.Fprintf(stderr, "unknown command %q\n", args[0])
		printUsage(stderr)

		return 2
	}
}

func runSchema(args []string, stdout io.Writer, stderr io.Writer) int {
	fs := flag.NewFlagSet("schema", flag.ContinueOnError)
	fs.SetOutput(stderr)
	outPath := fs.String("out", "", "write schema to path instead of stdout")
	kind := fs.String("kind", string(agentos.PlanSchemaKindRunPlan), "schema kind: run-plan, plan-delta, capability-catalog, or artifact-schema-catalog")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	schema, err := agentos.PlanJSONSchema(agentos.PlanSchemaKind(*kind))
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "generate schema: %v\n", err)

		return 1
	}
	if err := writeOutput(*outPath, schema, stdout); err != nil {
		_, _ = fmt.Fprintf(stderr, "write schema: %v\n", err)

		return 1
	}

	return 0
}

func runValidate(args []string, stdout io.Writer, stderr io.Writer) int {
	_, err := compilePlanCommand(args, stderr)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "validate run plan: %v\n", err)

		return 1
	}
	_, _ = fmt.Fprintln(stdout, "valid")

	return 0
}

func runValidateDelta(args []string, stdout io.Writer, stderr io.Writer) int {
	_, err := compilePlanDeltaCommand(args, stderr)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "validate plan delta: %v\n", err)

		return 1
	}
	_, _ = fmt.Fprintln(stdout, "valid")

	return 0
}

func runCompile(args []string, stdout io.Writer, stderr io.Writer) int {
	fs := newCompileFlagSet("compile", stderr)
	opts, err := parseCompileFlags(fs, args)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "compile run plan: %v\n", err)

		return 2
	}
	plan, err := compilePlan(opts)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "compile run plan: %v\n", err)

		return 1
	}

	payload, err := json.MarshalIndent(compiledPlanOutput{Spec: plan.Spec, Order: plan.Order}, "", "  ")
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "encode compiled plan: %v\n", err)

		return 1
	}
	payload = append(payload, '\n')
	if err := writeOutput(opts.outPath, payload, stdout); err != nil {
		_, _ = fmt.Fprintf(stderr, "write compiled plan: %v\n", err)

		return 1
	}

	return 0
}

func runCompileDelta(args []string, stdout io.Writer, stderr io.Writer) int {
	fs := newDeltaFlagSet("compile-delta", stderr)
	opts, err := parseDeltaFlags(fs, args)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "compile plan delta: %v\n", err)

		return 2
	}
	plan, err := compilePlanDelta(opts)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "compile plan delta: %v\n", err)

		return 1
	}

	payload, err := json.MarshalIndent(compiledPlanOutput{Spec: plan.Spec, Order: plan.Order}, "", "  ")
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "encode compiled plan delta: %v\n", err)

		return 1
	}
	payload = append(payload, '\n')
	if err := writeOutput(opts.outPath, payload, stdout); err != nil {
		_, _ = fmt.Fprintf(stderr, "write compiled plan delta: %v\n", err)

		return 1
	}

	return 0
}

func runExportServerless(args []string, stdout io.Writer, stderr io.Writer) int {
	fs := newServerlessExportFlagSet("export-serverless", stderr)
	opts, err := parseServerlessExportFlags(fs, args)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "export serverless workflow: %v\n", err)

		return 2
	}
	workflow, err := exportServerless(opts)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "export serverless workflow: %v\n", err)

		return 1
	}
	payload, err := encodeServerlessWorkflow(opts.outputFormat, workflow)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "encode serverless workflow: %v\n", err)

		return 1
	}
	if err := writeOutput(opts.outPath, payload, stdout); err != nil {
		_, _ = fmt.Fprintf(stderr, "write serverless workflow: %v\n", err)

		return 1
	}

	return 0
}

func runImportServerless(args []string, stdout io.Writer, stderr io.Writer) int {
	fs := newServerlessImportFlagSet("import-serverless", stderr)
	opts, err := parseServerlessImportFlags(fs, args)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "import serverless workflow: %v\n", err)

		return 2
	}
	plan, err := importServerless(opts)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "import serverless workflow: %v\n", err)

		return 1
	}
	payload, err := encodeWire(opts.outputFormat, compiledPlanOutput{Spec: plan.Spec, Order: plan.Order})
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "encode imported run plan: %v\n", err)

		return 1
	}
	if err := writeOutput(opts.outPath, payload, stdout); err != nil {
		_, _ = fmt.Fprintf(stderr, "write imported run plan: %v\n", err)

		return 1
	}

	return 0
}

type compileOptions struct {
	filePath              string
	format                string
	capabilitiesPath      string
	capabilitiesFormat    string
	artifactSchemasPath   string
	artifactSchemasFormat string
	outPath               string
}

type deltaOptions struct {
	basePath              string
	baseFormat            string
	filePath              string
	format                string
	capabilitiesPath      string
	capabilitiesFormat    string
	artifactSchemasPath   string
	artifactSchemasFormat string
	expansionCount        int
	outPath               string
}

type serverlessExportOptions struct {
	compileOptions
	outputFormat string
}

type serverlessImportOptions struct {
	filePath              string
	format                string
	capabilitiesPath      string
	capabilitiesFormat    string
	artifactSchemasPath   string
	artifactSchemasFormat string
	outputFormat          string
	outPath               string
}

func compilePlanCommand(args []string, stderr io.Writer) (agentosplan.ExecutablePlan, error) {
	fs := newCompileFlagSet("validate", stderr)
	opts, err := parseCompileFlags(fs, args)
	if err != nil {
		return agentosplan.ExecutablePlan{}, err
	}

	return compilePlan(opts)
}

func compilePlanDeltaCommand(args []string, stderr io.Writer) (agentosplan.ExecutablePlan, error) {
	fs := newDeltaFlagSet("validate-delta", stderr)
	opts, err := parseDeltaFlags(fs, args)
	if err != nil {
		return agentosplan.ExecutablePlan{}, err
	}

	return compilePlanDelta(opts)
}

func newCompileFlagSet(name string, stderr io.Writer) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.String("file", "", "RunPlan JSON/YAML file")
	fs.String("format", "", "RunPlan format: json or yaml")
	fs.String("capabilities", "", "capability catalog JSON/YAML file")
	fs.String("capabilities-format", "", "capability catalog format: json or yaml")
	fs.String("artifact-schemas", "", "artifact schema catalog JSON/YAML file")
	fs.String("artifact-schemas-format", "", "artifact schema catalog format: json or yaml")
	fs.String("out", "", "write compiled JSON to path instead of stdout")

	return fs
}

func newDeltaFlagSet(name string, stderr io.Writer) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.String("base", "", "base RunPlan JSON/YAML file")
	fs.String("base-format", "", "base RunPlan format: json or yaml")
	fs.String("file", "", "PlanDelta JSON/YAML file")
	fs.String("format", "", "PlanDelta format: json or yaml")
	fs.String("capabilities", "", "capability catalog JSON/YAML file")
	fs.String("capabilities-format", "", "capability catalog format: json or yaml")
	fs.String("artifact-schemas", "", "artifact schema catalog JSON/YAML file")
	fs.String("artifact-schemas-format", "", "artifact schema catalog format: json or yaml")
	fs.Int("expansion-count", 0, "already-applied expansion count")
	fs.String("out", "", "write compiled JSON to path instead of stdout")

	return fs
}

func newServerlessExportFlagSet(name string, stderr io.Writer) *flag.FlagSet {
	fs := newCompileFlagSet(name, stderr)
	fs.String("out-format", "", "Serverless Workflow output format: json or yaml")

	return fs
}

func newServerlessImportFlagSet(name string, stderr io.Writer) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.String("file", "", "Serverless Workflow JSON/YAML file")
	fs.String("format", "", "Serverless Workflow format: json or yaml")
	fs.String("capabilities", "", "capability catalog JSON/YAML file")
	fs.String("capabilities-format", "", "capability catalog format: json or yaml")
	fs.String("artifact-schemas", "", "artifact schema catalog JSON/YAML file")
	fs.String("artifact-schemas-format", "", "artifact schema catalog format: json or yaml")
	fs.String("out-format", "", "RunPlan output format: json or yaml")
	fs.String("out", "", "write imported RunPlan to path instead of stdout")

	return fs
}

func parseCompileFlags(fs *flag.FlagSet, args []string) (compileOptions, error) {
	if err := fs.Parse(args); err != nil {
		return compileOptions{}, err
	}
	opts := compileOptions{
		filePath:              fs.Lookup("file").Value.String(),
		format:                fs.Lookup("format").Value.String(),
		capabilitiesPath:      fs.Lookup("capabilities").Value.String(),
		capabilitiesFormat:    fs.Lookup("capabilities-format").Value.String(),
		artifactSchemasPath:   fs.Lookup("artifact-schemas").Value.String(),
		artifactSchemasFormat: fs.Lookup("artifact-schemas-format").Value.String(),
		outPath:               fs.Lookup("out").Value.String(),
	}
	if opts.filePath == "" {
		return compileOptions{}, errors.New("file is required")
	}
	if err := validateWireFormat(opts.format); err != nil {
		return compileOptions{}, err
	}
	if opts.capabilitiesPath != "" {
		if err := validateWireFormat(opts.capabilitiesFormat); err != nil {
			return compileOptions{}, fmt.Errorf("capabilities format: %w", err)
		}
	} else if opts.capabilitiesFormat != "" {
		return compileOptions{}, errors.New("capabilities-format requires capabilities")
	}
	if opts.artifactSchemasPath != "" {
		if err := validateWireFormat(opts.artifactSchemasFormat); err != nil {
			return compileOptions{}, fmt.Errorf("artifact-schemas format: %w", err)
		}
	} else if opts.artifactSchemasFormat != "" {
		return compileOptions{}, errors.New("artifact-schemas-format requires artifact-schemas")
	}

	return opts, nil
}

func parseDeltaFlags(fs *flag.FlagSet, args []string) (deltaOptions, error) {
	if err := fs.Parse(args); err != nil {
		return deltaOptions{}, err
	}
	expansionCount, err := strconv.Atoi(fs.Lookup("expansion-count").Value.String())
	if err != nil {
		return deltaOptions{}, fmt.Errorf("expansion-count: %w", err)
	}
	opts := deltaOptions{
		basePath:              fs.Lookup("base").Value.String(),
		baseFormat:            fs.Lookup("base-format").Value.String(),
		filePath:              fs.Lookup("file").Value.String(),
		format:                fs.Lookup("format").Value.String(),
		capabilitiesPath:      fs.Lookup("capabilities").Value.String(),
		capabilitiesFormat:    fs.Lookup("capabilities-format").Value.String(),
		artifactSchemasPath:   fs.Lookup("artifact-schemas").Value.String(),
		artifactSchemasFormat: fs.Lookup("artifact-schemas-format").Value.String(),
		expansionCount:        expansionCount,
		outPath:               fs.Lookup("out").Value.String(),
	}
	if opts.basePath == "" {
		return deltaOptions{}, errors.New("base is required")
	}
	if opts.filePath == "" {
		return deltaOptions{}, errors.New("file is required")
	}
	if err := validateWireFormat(opts.baseFormat); err != nil {
		return deltaOptions{}, fmt.Errorf("base format: %w", err)
	}
	if err := validateWireFormat(opts.format); err != nil {
		return deltaOptions{}, err
	}
	if opts.capabilitiesPath != "" {
		if err := validateWireFormat(opts.capabilitiesFormat); err != nil {
			return deltaOptions{}, fmt.Errorf("capabilities format: %w", err)
		}
	} else if opts.capabilitiesFormat != "" {
		return deltaOptions{}, errors.New("capabilities-format requires capabilities")
	}
	if opts.artifactSchemasPath != "" {
		if err := validateWireFormat(opts.artifactSchemasFormat); err != nil {
			return deltaOptions{}, fmt.Errorf("artifact-schemas format: %w", err)
		}
	} else if opts.artifactSchemasFormat != "" {
		return deltaOptions{}, errors.New("artifact-schemas-format requires artifact-schemas")
	}
	if opts.expansionCount < 0 {
		return deltaOptions{}, errors.New("expansion-count cannot be negative")
	}

	return opts, nil
}

func parseServerlessExportFlags(fs *flag.FlagSet, args []string) (serverlessExportOptions, error) {
	compileOpts, err := parseCompileFlags(fs, args)
	if err != nil {
		return serverlessExportOptions{}, err
	}
	outputFormat := fs.Lookup("out-format").Value.String()
	if err := validateWireFormat(outputFormat); err != nil {
		return serverlessExportOptions{}, fmt.Errorf("out-format: %w", err)
	}

	return serverlessExportOptions{
		compileOptions: compileOpts,
		outputFormat:   outputFormat,
	}, nil
}

func parseServerlessImportFlags(fs *flag.FlagSet, args []string) (serverlessImportOptions, error) {
	if err := fs.Parse(args); err != nil {
		return serverlessImportOptions{}, err
	}
	opts := serverlessImportOptions{
		filePath:              fs.Lookup("file").Value.String(),
		format:                fs.Lookup("format").Value.String(),
		capabilitiesPath:      fs.Lookup("capabilities").Value.String(),
		capabilitiesFormat:    fs.Lookup("capabilities-format").Value.String(),
		artifactSchemasPath:   fs.Lookup("artifact-schemas").Value.String(),
		artifactSchemasFormat: fs.Lookup("artifact-schemas-format").Value.String(),
		outputFormat:          fs.Lookup("out-format").Value.String(),
		outPath:               fs.Lookup("out").Value.String(),
	}
	if opts.filePath == "" {
		return serverlessImportOptions{}, errors.New("file is required")
	}
	if err := validateWireFormat(opts.format); err != nil {
		return serverlessImportOptions{}, err
	}
	if err := validateWireFormat(opts.outputFormat); err != nil {
		return serverlessImportOptions{}, fmt.Errorf("out-format: %w", err)
	}
	if opts.capabilitiesPath != "" {
		if err := validateWireFormat(opts.capabilitiesFormat); err != nil {
			return serverlessImportOptions{}, fmt.Errorf("capabilities format: %w", err)
		}
	} else if opts.capabilitiesFormat != "" {
		return serverlessImportOptions{}, errors.New("capabilities-format requires capabilities")
	}
	if opts.artifactSchemasPath != "" {
		if err := validateWireFormat(opts.artifactSchemasFormat); err != nil {
			return serverlessImportOptions{}, fmt.Errorf("artifact-schemas format: %w", err)
		}
	} else if opts.artifactSchemasFormat != "" {
		return serverlessImportOptions{}, errors.New("artifact-schemas-format requires artifact-schemas")
	}

	return opts, nil
}

func compilePlan(opts compileOptions) (agentosplan.ExecutablePlan, error) {
	plan, _, err := compilePlanWithValidator(opts)

	return plan, err
}

func compilePlanWithValidator(opts compileOptions) (agentosplan.ExecutablePlan, agentosplan.Validator, error) {
	data, err := os.ReadFile(opts.filePath)
	if err != nil {
		return agentosplan.ExecutablePlan{}, agentosplan.Validator{}, err
	}
	validator, err := newRunPlanValidator(opts.capabilitiesPath, opts.capabilitiesFormat, opts.artifactSchemasPath, opts.artifactSchemasFormat)
	if err != nil {
		return agentosplan.ExecutablePlan{}, agentosplan.Validator{}, err
	}

	runPlanCompiler := agentosplan.RunPlanCompiler{
		Validator: validator,
	}
	var plan agentosplan.ExecutablePlan
	switch opts.format {
	case "json":
		plan, err = runPlanCompiler.CompileJSON(context.Background(), data)
	case "yaml":
		plan, err = runPlanCompiler.CompileYAML(context.Background(), data)
	default:
		err = fmt.Errorf("unsupported format %q", opts.format)
	}
	if err != nil {
		return agentosplan.ExecutablePlan{}, agentosplan.Validator{}, err
	}

	return plan, validator, nil
}

func compilePlanDelta(opts deltaOptions) (agentosplan.ExecutablePlan, error) {
	basePlan, err := compilePlan(compileOptions{
		filePath:              opts.basePath,
		format:                opts.baseFormat,
		capabilitiesPath:      opts.capabilitiesPath,
		capabilitiesFormat:    opts.capabilitiesFormat,
		artifactSchemasPath:   opts.artifactSchemasPath,
		artifactSchemasFormat: opts.artifactSchemasFormat,
	})
	if err != nil {
		return agentosplan.ExecutablePlan{}, fmt.Errorf("base plan: %w", err)
	}
	data, err := os.ReadFile(opts.filePath)
	if err != nil {
		return agentosplan.ExecutablePlan{}, err
	}
	validator, err := newRunPlanValidator(opts.capabilitiesPath, opts.capabilitiesFormat, opts.artifactSchemasPath, opts.artifactSchemasFormat)
	if err != nil {
		return agentosplan.ExecutablePlan{}, err
	}
	runPlanCompiler := agentosplan.RunPlanCompiler{
		Validator: validator,
	}
	var plan agentosplan.ExecutablePlan
	switch opts.format {
	case "json":
		_, plan, err = runPlanCompiler.CompileDeltaJSON(context.Background(), basePlan.Spec, data, int32(opts.expansionCount))
	case "yaml":
		_, plan, err = runPlanCompiler.CompileDeltaYAML(context.Background(), basePlan.Spec, data, int32(opts.expansionCount))
	default:
		err = fmt.Errorf("unsupported format %q", opts.format)
	}
	if err != nil {
		return agentosplan.ExecutablePlan{}, err
	}

	return plan, nil
}

func exportServerless(opts serverlessExportOptions) (serverlessworkflow.Workflow, error) {
	plan, validator, err := compilePlanWithValidator(opts.compileOptions)
	if err != nil {
		return serverlessworkflow.Workflow{}, err
	}
	adapter := serverlessworkflow.Adapter{Validator: validator}

	return adapter.Export(context.Background(), plan.Spec)
}

func importServerless(opts serverlessImportOptions) (agentosplan.ExecutablePlan, error) {
	data, err := os.ReadFile(opts.filePath)
	if err != nil {
		return agentosplan.ExecutablePlan{}, err
	}
	workflow, err := decodeServerlessWorkflow(opts.format, data)
	if err != nil {
		return agentosplan.ExecutablePlan{}, err
	}
	validator, err := newRunPlanValidator(opts.capabilitiesPath, opts.capabilitiesFormat, opts.artifactSchemasPath, opts.artifactSchemasFormat)
	if err != nil {
		return agentosplan.ExecutablePlan{}, err
	}
	adapter := serverlessworkflow.Adapter{Validator: validator}
	spec, err := adapter.Import(context.Background(), workflow)
	if err != nil {
		return agentosplan.ExecutablePlan{}, err
	}

	return validator.Validate(context.Background(), spec)
}

func newRunPlanValidator(capabilitiesPath string, capabilitiesFormat string, artifactSchemasPath string, artifactSchemasFormat string) (agentosplan.Validator, error) {
	compiler, err := agentosplan.NewCELCompiler()
	if err != nil {
		return agentosplan.Validator{}, err
	}
	catalog, err := loadCapabilityCatalog(capabilitiesPath, capabilitiesFormat)
	if err != nil {
		return agentosplan.Validator{}, err
	}
	artifactSchemas, err := loadArtifactSchemaCatalog(artifactSchemasPath, artifactSchemasFormat)
	if err != nil {
		return agentosplan.Validator{}, err
	}

	return agentosplan.Validator{
		Expressions:     compiler,
		Capabilities:    catalog,
		ArtifactSchemas: artifactSchemas,
	}, nil
}

func loadCapabilityCatalog(path string, format string) (agentosplan.CapabilityCatalog, error) {
	if path == "" {
		return nil, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var catalogFile agentos.CapabilityCatalogSpec
	if err := decodeWire(format, data, &catalogFile); err != nil {
		return nil, err
	}

	return agentosplan.NewStaticCapabilityCatalog(catalogFile.Capabilities)
}

func loadArtifactSchemaCatalog(path string, format string) (agentosplan.ArtifactSchemaCatalog, error) {
	if path == "" {
		return nil, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var catalogFile agentos.ArtifactSchemaCatalogSpec
	if err := decodeWire(format, data, &catalogFile); err != nil {
		return nil, err
	}

	return agentosplan.NewStaticArtifactSchemaCatalog(catalogFile.ArtifactSchemas)
}

func encodeServerlessWorkflow(format string, workflow serverlessworkflow.Workflow) ([]byte, error) {
	switch format {
	case "json":
		payload, err := serverlessworkflow.MarshalJSON(workflow)
		if err != nil {
			return nil, err
		}

		return append(payload, '\n'), nil
	case "yaml":
		return serverlessworkflow.MarshalYAML(workflow)
	default:
		return nil, fmt.Errorf("unsupported format %q", format)
	}
}

func decodeServerlessWorkflow(format string, data []byte) (serverlessworkflow.Workflow, error) {
	switch format {
	case "json":
		return serverlessworkflow.UnmarshalJSON(data)
	case "yaml":
		return serverlessworkflow.UnmarshalYAML(data)
	default:
		return serverlessworkflow.Workflow{}, fmt.Errorf("unsupported format %q", format)
	}
}

func encodeWire(format string, value any) ([]byte, error) {
	switch format {
	case "json":
		payload, err := json.MarshalIndent(value, "", "  ")
		if err != nil {
			return nil, err
		}

		return append(payload, '\n'), nil
	case "yaml":
		payload, err := json.MarshalIndent(value, "", "  ")
		if err != nil {
			return nil, err
		}

		return yaml.JSONToYAML(payload)
	default:
		return nil, fmt.Errorf("unsupported format %q", format)
	}
}

func decodeWire(format string, data []byte, target any) error {
	switch format {
	case "json":
		if err := json.Unmarshal(data, target); err != nil {
			return fmt.Errorf("decode json: %w", err)
		}

		return nil
	case "yaml":
		jsonData, err := yaml.YAMLToJSON(data)
		if err != nil {
			return fmt.Errorf("decode yaml: %w", err)
		}
		if err := json.Unmarshal(jsonData, target); err != nil {
			return fmt.Errorf("decode yaml json: %w", err)
		}

		return nil
	default:
		return fmt.Errorf("unsupported format %q", format)
	}
}

func validateWireFormat(format string) error {
	switch format {
	case "json", "yaml":
		return nil
	default:
		return fmt.Errorf("format must be json or yaml, got %q", format)
	}
}

func writeOutput(path string, data []byte, stdout io.Writer) error {
	if path == "" {
		_, err := stdout.Write(data)

		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}

	return os.WriteFile(path, data, 0o644)
}

func printUsage(w io.Writer) {
	_, _ = fmt.Fprintln(w, "usage: agentos-plan <schema|validate|compile|validate-delta|compile-delta|export-serverless|import-serverless> [flags]")
}
