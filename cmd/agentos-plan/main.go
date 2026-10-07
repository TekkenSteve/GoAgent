// Package main is the agentos-plan command-line tool.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"

	agentos "github.com/TekkenSteve/GoAgent/agentos/control"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosplan"
	"github.com/TekkenSteve/GoAgent/internal/usecase/agentosplan/serverlessworkflow"
	"sigs.k8s.io/yaml"
)

var (
	errFileRequired                           = errors.New("file is required")
	errCapabilitiesFormatRequiresCapabilities = errors.New("capabilities-format requires capabilities")
	errArtifactSchemasFormatRequiresSchemas   = errors.New("artifact-schemas-format requires artifact-schemas")
	errBaseRequired                           = errors.New("base is required")
	errExpansionCountCannotBeNegative         = errors.New("expansion-count cannot be negative")
	errInvalidFormat                          = errors.New("unsupported format")
	errInvalidDecodeTarget                    = errors.New("unsupported decode target")
	errInvalidFilePath                        = errors.New("invalid file path")
)

const (
	exitCodeSuccess = 0
	exitCodeError   = 1
	exitCodeUsage   = 2

	JSON = "json"
	YAML = "yaml"

	dirPerm  os.FileMode = 0o755
	filePerm os.FileMode = 0o600
)

type compiledPlanOutput struct {
	Spec  agentos.RunPlanSpec `json:"spec"`
	Order []string            `json:"order"`
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		return runUsage(stderr)
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
		return runUnknownCommand(args[0], stderr)
	}
}

func runUsage(stderr io.Writer) int {
	if err := printUsage(stderr); err != nil {
		return exitCodeError
	}

	return exitCodeUsage
}

func runUnknownCommand(command string, stderr io.Writer) int {
	if _, werr := fmt.Fprintf(stderr, "unknown command %q\n", command); werr != nil {
		return exitCodeError
	}

	if err := printUsage(stderr); err != nil {
		return exitCodeError
	}

	return exitCodeUsage
}

func runSchema(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("schema", flag.ContinueOnError)
	fs.SetOutput(stderr)
	outPath := fs.String("out", "", "write schema to path instead of stdout")

	kind := fs.String("kind", string(agentos.PlanSchemaKindRunPlan), "schema kind: run-plan, plan-delta, capability-catalog, or artifact-schema-catalog")
	if err := fs.Parse(args); err != nil {
		return exitCodeUsage
	}

	schema, err := agentos.PlanJSONSchema(agentos.PlanSchemaKind(*kind))
	if err != nil {
		if _, werr := fmt.Fprintf(stderr, "generate schema: %v\n", err); werr != nil {
			return exitCodeError
		}

		return exitCodeError
	}

	if err := writeOutput(*outPath, schema, stdout); err != nil {
		if _, werr := fmt.Fprintf(stderr, "write schema: %v\n", err); werr != nil {
			return exitCodeError
		}

		return exitCodeError
	}

	return exitCodeSuccess
}

func runValidate(args []string, stdout, stderr io.Writer) int {
	_, err := compilePlanCommand(args, stderr)
	if err != nil {
		if _, werr := fmt.Fprintf(stderr, "validate run plan: %v\n", err); werr != nil {
			return exitCodeError
		}

		return exitCodeError
	}

	if _, werr := fmt.Fprintln(stdout, "valid"); werr != nil {
		return exitCodeError
	}

	return exitCodeSuccess
}

func runValidateDelta(args []string, stdout, stderr io.Writer) int {
	_, err := compilePlanDeltaCommand(args, stderr)
	if err != nil {
		if _, werr := fmt.Fprintf(stderr, "validate plan delta: %v\n", err); werr != nil {
			return exitCodeError
		}

		return exitCodeError
	}

	if _, werr := fmt.Fprintln(stdout, "valid"); werr != nil {
		return exitCodeError
	}

	return exitCodeSuccess
}

func runCompile(args []string, stdout, stderr io.Writer) int {
	fs := newCompileFlagSet("compile", stderr)

	opts, err := parseCompileFlags(fs, args)
	if err != nil {
		if _, werr := fmt.Fprintf(stderr, "compile run plan: %v\n", err); werr != nil {
			return exitCodeError
		}

		return exitCodeUsage
	}

	plan, err := compilePlan(&opts)
	if err != nil {
		if _, werr := fmt.Fprintf(stderr, "compile run plan: %v\n", err); werr != nil {
			return exitCodeError
		}

		return exitCodeError
	}

	return emitCompiledPlan(&plan, opts.outPath, stdout, stderr)
}

func runCompileDelta(args []string, stdout, stderr io.Writer) int {
	fs := newDeltaFlagSet("compile-delta", stderr)

	opts, err := parseDeltaFlags(fs, args)
	if err != nil {
		if _, werr := fmt.Fprintf(stderr, "compile plan delta: %v\n", err); werr != nil {
			return exitCodeError
		}

		return exitCodeUsage
	}

	plan, err := compilePlanDelta(&opts)
	if err != nil {
		if _, werr := fmt.Fprintf(stderr, "compile plan delta: %v\n", err); werr != nil {
			return exitCodeError
		}

		return exitCodeError
	}

	return emitCompiledPlan(&plan, opts.outPath, stdout, stderr)
}

func emitCompiledPlan(plan *agentosplan.ExecutablePlan, outPath string, stdout, stderr io.Writer) int {
	payload, err := json.MarshalIndent(compiledPlanOutput{Spec: plan.Spec, Order: plan.Order}, "", "  ")
	if err != nil {
		if _, werr := fmt.Fprintf(stderr, "encode compiled plan: %v\n", err); werr != nil {
			return exitCodeError
		}

		return exitCodeError
	}

	payload = append(payload, '\n')
	if err := writeOutput(outPath, payload, stdout); err != nil {
		if _, werr := fmt.Fprintf(stderr, "write compiled plan: %v\n", err); werr != nil {
			return exitCodeError
		}

		return exitCodeError
	}

	return exitCodeSuccess
}

func runExportServerless(args []string, stdout, stderr io.Writer) int {
	fs := newServerlessExportFlagSet("export-serverless", stderr)

	opts, err := parseServerlessExportFlags(fs, args)
	if err != nil {
		if _, werr := fmt.Fprintf(stderr, "export serverless workflow: %v\n", err); werr != nil {
			return exitCodeError
		}

		return exitCodeUsage
	}

	workflow, err := exportServerless(&opts)
	if err != nil {
		if _, werr := fmt.Fprintf(stderr, "export serverless workflow: %v\n", err); werr != nil {
			return exitCodeError
		}

		return exitCodeError
	}

	payload, err := encodeServerlessWorkflow(opts.outputFormat, &workflow)
	if err != nil {
		if _, werr := fmt.Fprintf(stderr, "encode serverless workflow: %v\n", err); werr != nil {
			return exitCodeError
		}

		return exitCodeError
	}

	if err := writeOutput(opts.outPath, payload, stdout); err != nil {
		if _, werr := fmt.Fprintf(stderr, "write serverless workflow: %v\n", err); werr != nil {
			return exitCodeError
		}

		return exitCodeError
	}

	return exitCodeSuccess
}

func runImportServerless(args []string, stdout, stderr io.Writer) int {
	fs := newServerlessImportFlagSet("import-serverless", stderr)

	opts, err := parseServerlessImportFlags(fs, args)
	if err != nil {
		if _, werr := fmt.Fprintf(stderr, "import serverless workflow: %v\n", err); werr != nil {
			return exitCodeError
		}

		return exitCodeUsage
	}

	plan, err := importServerless(&opts)
	if err != nil {
		if _, werr := fmt.Fprintf(stderr, "import serverless workflow: %v\n", err); werr != nil {
			return exitCodeError
		}

		return exitCodeError
	}

	payload, err := encodeWire(opts.outputFormat, compiledPlanOutput{Spec: plan.Spec, Order: plan.Order})
	if err != nil {
		if _, werr := fmt.Fprintf(stderr, "encode imported run plan: %v\n", err); werr != nil {
			return exitCodeError
		}

		return exitCodeError
	}

	if err := writeOutput(opts.outPath, payload, stdout); err != nil {
		if _, werr := fmt.Fprintf(stderr, "write imported run plan: %v\n", err); werr != nil {
			return exitCodeError
		}

		return exitCodeError
	}

	return exitCodeSuccess
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

	return compilePlan(&opts)
}

func compilePlanDeltaCommand(args []string, stderr io.Writer) (agentosplan.ExecutablePlan, error) {
	fs := newDeltaFlagSet("validate-delta", stderr)

	opts, err := parseDeltaFlags(fs, args)
	if err != nil {
		return agentosplan.ExecutablePlan{}, err
	}

	return compilePlanDelta(&opts)
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
	if err := validatePlanInputFlags(opts.filePath, opts.format); err != nil {
		return compileOptions{}, err
	}

	if err := validateCatalogFormatFlags(
		opts.capabilitiesPath,
		opts.capabilitiesFormat,
		opts.artifactSchemasPath,
		opts.artifactSchemasFormat,
	); err != nil {
		return compileOptions{}, err
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
	if err := validateDeltaInputFlags(opts.basePath, opts.baseFormat, opts.filePath, opts.format); err != nil {
		return deltaOptions{}, err
	}

	if err := validateCatalogFormatFlags(
		opts.capabilitiesPath,
		opts.capabilitiesFormat,
		opts.artifactSchemasPath,
		opts.artifactSchemasFormat,
	); err != nil {
		return deltaOptions{}, err
	}

	if opts.expansionCount < 0 {
		return deltaOptions{}, errExpansionCountCannotBeNegative
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
	if err := validatePlanInputFlags(opts.filePath, opts.format); err != nil {
		return serverlessImportOptions{}, err
	}

	if err := validateWireFormat(opts.outputFormat); err != nil {
		return serverlessImportOptions{}, fmt.Errorf("out-format: %w", err)
	}

	if err := validateCatalogFormatFlags(
		opts.capabilitiesPath,
		opts.capabilitiesFormat,
		opts.artifactSchemasPath,
		opts.artifactSchemasFormat,
	); err != nil {
		return serverlessImportOptions{}, err
	}

	return opts, nil
}

func validatePlanInputFlags(filePath, format string) error {
	if filePath == "" {
		return errFileRequired
	}

	return validateWireFormat(format)
}

func validateDeltaInputFlags(basePath, baseFormat, filePath, format string) error {
	if basePath == "" {
		return errBaseRequired
	}

	if err := validatePlanInputFlags(filePath, format); err != nil {
		return err
	}

	if err := validateWireFormat(baseFormat); err != nil {
		return fmt.Errorf("base format: %w", err)
	}

	return nil
}

func validateCatalogFormatFlags(capabilitiesPath, capabilitiesFormat, artifactSchemasPath, artifactSchemasFormat string) error {
	if err := validateOptionalFormatFlag(
		capabilitiesPath,
		capabilitiesFormat,
		"capabilities format",
		errCapabilitiesFormatRequiresCapabilities,
	); err != nil {
		return err
	}

	return validateOptionalFormatFlag(
		artifactSchemasPath,
		artifactSchemasFormat,
		"artifact-schemas format",
		errArtifactSchemasFormatRequiresSchemas,
	)
}

func validateOptionalFormatFlag(path, format, label string, missingPathErr error) error {
	if path == "" {
		if format != "" {
			return missingPathErr
		}

		return nil
	}

	if err := validateWireFormat(format); err != nil {
		return fmt.Errorf("%s: %w", label, err)
	}

	return nil
}

func compilePlan(opts *compileOptions) (agentosplan.ExecutablePlan, error) {
	plan, _, err := compilePlanWithValidator(opts)

	return plan, err
}

func compilePlanWithValidator(opts *compileOptions) (agentosplan.ExecutablePlan, agentosplan.Validator, error) {
	data, err := readInputFile(opts.filePath)
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
	case JSON:
		plan, err = runPlanCompiler.CompileJSON(context.Background(), data)
	case YAML:
		plan, err = runPlanCompiler.CompileYAML(context.Background(), data)
	default:
		err = fmt.Errorf("%w: %q", errInvalidFormat, opts.format)
	}

	if err != nil {
		return agentosplan.ExecutablePlan{}, agentosplan.Validator{}, err
	}

	return plan, validator, nil
}

func compilePlanDelta(opts *deltaOptions) (agentosplan.ExecutablePlan, error) {
	basePlan, err := compilePlan(&compileOptions{
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

	data, err := readInputFile(opts.filePath)
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

	expansionCount32, err := checkedInt32("expansion count", opts.expansionCount)
	if err != nil {
		return agentosplan.ExecutablePlan{}, err
	}

	switch opts.format {
	case JSON:
		_, plan, err = runPlanCompiler.CompileDeltaJSON(context.Background(), &basePlan.Spec, data, expansionCount32)
	case YAML:
		_, plan, err = runPlanCompiler.CompileDeltaYAML(context.Background(), &basePlan.Spec, data, expansionCount32)
	default:
		err = fmt.Errorf("%w: %q", errInvalidFormat, opts.format)
	}

	if err != nil {
		return agentosplan.ExecutablePlan{}, err
	}

	return plan, nil
}

func exportServerless(opts *serverlessExportOptions) (serverlessworkflow.Workflow, error) {
	plan, validator, err := compilePlanWithValidator(&opts.compileOptions)
	if err != nil {
		return serverlessworkflow.Workflow{}, err
	}

	adapter := serverlessworkflow.Adapter{Validator: validator}

	return adapter.Export(context.Background(), &plan.Spec)
}

func importServerless(opts *serverlessImportOptions) (agentosplan.ExecutablePlan, error) {
	data, err := readInputFile(opts.filePath)
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

	spec, err := adapter.Import(context.Background(), &workflow)
	if err != nil {
		return agentosplan.ExecutablePlan{}, err
	}

	return validator.Validate(context.Background(), &spec)
}

func newRunPlanValidator(capabilitiesPath, capabilitiesFormat, artifactSchemasPath, artifactSchemasFormat string) (agentosplan.Validator, error) {
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

func loadCapabilityCatalog(path, format string) (agentosplan.CapabilityCatalog, error) {
	if path == "" {
		return nil, nil
	}

	data, err := readInputFile(path)
	if err != nil {
		return nil, err
	}

	var catalogFile agentos.CapabilityCatalogSpec
	if err := decodeWire(format, data, &catalogFile); err != nil {
		return nil, err
	}

	return agentosplan.NewStaticCapabilityCatalog(catalogFile.Capabilities)
}

func loadArtifactSchemaCatalog(path, format string) (agentosplan.ArtifactSchemaCatalog, error) {
	if path == "" {
		return nil, nil
	}

	data, err := readInputFile(path)
	if err != nil {
		return nil, err
	}

	var catalogFile agentos.ArtifactSchemaCatalogSpec
	if err := decodeWire(format, data, &catalogFile); err != nil {
		return nil, err
	}

	return agentosplan.NewStaticArtifactSchemaCatalog(catalogFile.ArtifactSchemas)
}

func encodeServerlessWorkflow(format string, workflow *serverlessworkflow.Workflow) ([]byte, error) {
	switch format {
	case JSON:
		payload, err := serverlessworkflow.MarshalJSON(workflow)
		if err != nil {
			return nil, err
		}

		return append(payload, '\n'), nil
	case YAML:
		return serverlessworkflow.MarshalYAML(workflow)
	default:
		return nil, fmt.Errorf("%w: %q", errInvalidFormat, format)
	}
}

func decodeServerlessWorkflow(format string, data []byte) (serverlessworkflow.Workflow, error) {
	switch format {
	case JSON:
		return serverlessworkflow.UnmarshalJSON(data)
	case YAML:
		return serverlessworkflow.UnmarshalYAML(data)
	default:
		return serverlessworkflow.Workflow{}, fmt.Errorf("%w: %q", errInvalidFormat, format)
	}
}

func encodeWire(format string, value any) ([]byte, error) {
	switch format {
	case JSON:
		payload, err := json.MarshalIndent(value, "", "  ")
		if err != nil {
			return nil, err
		}

		return append(payload, '\n'), nil
	case YAML:
		payload, err := json.MarshalIndent(value, "", "  ")
		if err != nil {
			return nil, err
		}

		return yaml.JSONToYAML(payload)
	default:
		return nil, fmt.Errorf("%w: %q", errInvalidFormat, format)
	}
}

func decodeWire(format string, data []byte, target any) error {
	switch format {
	case JSON:
		return decodeWireJSONTarget(data, target)
	case YAML:
		return decodeWireYAMLTarget(data, target)
	default:
		return fmt.Errorf("%w: %q", errInvalidFormat, format)
	}
}

func decodeWireJSONTarget(data []byte, target any) error {
	switch value := target.(type) {
	case *agentos.CapabilityCatalogSpec:
		decoded, err := agentosplan.DecodeWireJSON[agentos.CapabilityCatalogSpec](data)
		if err != nil {
			return err
		}

		*value = decoded

		return nil
	case *agentos.ArtifactSchemaCatalogSpec:
		decoded, err := agentosplan.DecodeWireJSON[agentos.ArtifactSchemaCatalogSpec](data)
		if err != nil {
			return err
		}

		*value = decoded

		return nil
	default:
		return fmt.Errorf("%w: %T", errInvalidDecodeTarget, target)
	}
}

func decodeWireYAMLTarget(data []byte, target any) error {
	switch value := target.(type) {
	case *agentos.CapabilityCatalogSpec:
		decoded, err := agentosplan.DecodeWireYAML[agentos.CapabilityCatalogSpec](data)
		if err != nil {
			return err
		}

		*value = decoded

		return nil
	case *agentos.ArtifactSchemaCatalogSpec:
		decoded, err := agentosplan.DecodeWireYAML[agentos.ArtifactSchemaCatalogSpec](data)
		if err != nil {
			return err
		}

		*value = decoded

		return nil
	default:
		return fmt.Errorf("%w: %T", errInvalidDecodeTarget, target)
	}
}

func validateWireFormat(format string) error {
	switch format {
	case JSON, YAML:
		return nil
	default:
		return fmt.Errorf("%w: format must be json or yaml, got %q", errInvalidFormat, format)
	}
}

func writeOutput(path string, data []byte, stdout io.Writer) error {
	if path == "" {
		_, err := stdout.Write(data)

		return err
	}

	target, err := parseFileTarget(path)
	if err != nil {
		return err
	}

	return writeRootedFile(target, data)
}

func readInputFile(path string) (data []byte, err error) {
	target, err := parseFileTarget(path)
	if err != nil {
		return nil, err
	}

	root, err := os.OpenRoot(target.dir)
	if err != nil {
		return nil, err
	}

	defer func() {
		if closeErr := root.Close(); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}()

	file, err := root.Open(target.name)
	if err != nil {
		return nil, err
	}

	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}()

	return io.ReadAll(file)
}

type fileTarget struct {
	dir  string
	name string
}

func parseFileTarget(path string) (fileTarget, error) {
	cleanPath := filepath.Clean(path)
	name := filepath.Base(cleanPath)

	if name == "." || name == string(filepath.Separator) {
		return fileTarget{}, fmt.Errorf("%w: %q", errInvalidFilePath, path)
	}

	return fileTarget{
		dir:  filepath.Dir(cleanPath),
		name: name,
	}, nil
}

func writeRootedFile(target fileTarget, data []byte) (err error) {
	if err := os.MkdirAll(target.dir, dirPerm); err != nil {
		return err
	}

	root, err := os.OpenRoot(target.dir)
	if err != nil {
		return err
	}

	defer func() {
		if closeErr := root.Close(); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}()

	file, err := root.OpenFile(target.name, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, filePerm)
	if err != nil {
		return err
	}

	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}()

	_, err = file.Write(data)

	return err
}

func checkedInt32(name string, value int) (int32, error) {
	if value < 0 {
		return 0, fmt.Errorf("%w: %s %d", errExpansionCountCannotBeNegative, name, value)
	}

	if value > math.MaxInt32 {
		return 0, fmt.Errorf("%w: %s %d exceeds max int32", errExpansionCountCannotBeNegative, name, value)
	}

	return int32(value), nil
}

func printUsage(w io.Writer) error {
	if _, werr := fmt.Fprintln(w, "usage: agentos-plan <schema|validate|compile|validate-delta|compile-delta|export-serverless|import-serverless> [flags]"); werr != nil {
		return werr
	}

	return nil
}
