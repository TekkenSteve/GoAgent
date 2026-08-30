# GoAgent

GoAgent is the reference implementation of **AgentOS**: an **Agent Control Plane** plus a **Durable Process Platform** built on Temporal.

It has two public surfaces:

- **Agent Control Plane** — starts, controls, observes, and composes backend-owned agent runs across native GoAgent, LangGraph, OpenCode-style runtimes, HTTP backends, gRPC backends, and external Temporal workflows.
- **Durable Process Platform** — models long-lived intelligent work as generic resources, processes, ledgers, governed actions, batches, and projections. Domain systems such as AiSOC should build on these primitives without adding their business nouns to AgentOS core.

The native GoAgent agent framework is one backend implementation. Temporal is the durable process kernel. `agentos/temporal` is the default adapter that wires AgentOS ports to Temporal, Postgres, Redis, and artifact storage.

## Features

- **Agent Control Plane** — backend-owned run lifecycle, signals, controls, durable RunPlan orchestration, backend capabilities, and event ingest
- **Durable Process Platform** — generic resource/process runtime, ledger, governed action, batch/workset, and projection interfaces
- **Temporal Kernel Adapter** — explicit Temporal task queue isolation, durable workflows, timers, signals, retries, cancellation, and recovery
- **Native Agent Backend** — ReAct loop agent runtime with LLM integration, tool execution, team composition, and MCP server support
- **External Backend Adapters** — HTTP, gRPC, and `temporal_external` backends for runtimes owned outside GoAgent
- **Multi-Agent Teams** — Hierarchical team composition with recursive sub-team expansion
- **Human-in-the-Loop** — Workflow pause/resume/cancel and signal-based waiting steps
- **Streaming** — Server-Sent Events (SSE) and WebSocket support for real-time agent output
- **Account & Billing** — Credit-based usage tracking with plan assignment
- **Clean Architecture** — Dependency inversion, interface-based isolation, testability
- **Observability** — Structured logging (zerolog), Prometheus metrics, OpenTelemetry tracing
- **Database Migrations** — golang-migrate for PostgreSQL schema management

## Technology Stack

[![Web Framework](https://img.shields.io/badge/Fiber-Web%20Framework-blue)](https://github.com/gofiber/fiber)
[![Workflow Engine](https://img.shields.io/badge/Temporal-Workflow%20Engine-blue)](https://temporal.io/)
[![API Documentation](https://img.shields.io/badge/Swagger-API%20Documentation-blue)](https://github.com/swaggo/swag)
[![Validation](https://img.shields.io/badge/Validator-Data%20Integrity-blue)](https://github.com/go-playground/validator)
[![JSON Handling](https://img.shields.io/badge/Go--JSON-Fast%20Serialization-blue)](https://github.com/goccy/go-json)
[![Query Builder](https://img.shields.io/badge/Squirrel-SQL%20Query%20Builder-blue)](https://github.com/Masterminds/squirrel)
[![Database Migrations](https://img.shields.io/badge/Migrations-Seamless%20Schema%20Updates-blue)](https://github.com/golang-migrate/migrate)
[![Logging](https://img.shields.io/badge/ZeroLog-Structured%20Logging-blue)](https://github.com/rs/zerolog)
[![Metrics](https://img.shields.io/badge/Prometheus-Metrics%20Integration-blue)](https://github.com/ansrivas/fiberprometheus)
[![Testing](https://img.shields.io/badge/Testify-Testing%20Framework-blue)](https://github.com/stretchr/testify)

## Quick Start

### Prerequisites

- Go 1.26+
- Docker & Docker Compose
- Temporal Server (via Docker)

### Local Development

```sh
# Start dependency services (Postgres, Redis, Temporal)
make compose-up

# Run the application (includes database migration)
make run
```

### Integration Tests

```sh
# Start full test environment with mock LLM
make compose-up-integration-test
```

### Full Docker Stack

```sh
make compose-up-all
```

## Service Endpoints

- **REST API**:
  - `http://127.0.0.1:8080/healthz` — Health check
  - `http://127.0.0.1:8080/metrics` — Prometheus metrics
  - `http://127.0.0.1:8080/swagger` — API documentation
- **AgentOS API** (v1):
  - `POST /v1/agentos/runs` — Start a run on a selected backend
  - `GET /v1/agentos/runs/{run_id}/status` — Poll run status
  - `POST /v1/agentos/runs/{run_id}/signals` — Send business input such as `user.message`
  - `POST /v1/agentos/runs/{run_id}/control` — Send pause, resume, or cancel
  - `POST /v1/agentos/runs/{run_id}/events` — Ingest backend events
  - `GET /v1/agentos/plans/schemas/{kind}` — Read RunPlan authoring JSON Schema
  - `GET /v1/agentos/plans/author` — Render the RunPlanSpec authoring console
  - `POST /v1/agentos/plans` — Start a durable cross-backend RunPlan
  - `GET /v1/agentos/plans/{plan_id}/status` — Poll aggregate plan status
  - `GET /v1/agentos/plans/{plan_id}/description` — Read public topology and status
  - `GET /v1/agentos/plans/{plan_id}/console` — Render the RunPlan operator console
  - `POST /v1/agentos/plans/{plan_id}/signals` — Send plan signals such as retry, approve, or reject
  - `POST /v1/agentos/plans/{plan_id}/control` — Send pause, resume, or cancel to a RunPlan
  - `GET /v1/agentos/plans/{plan_id}/events` — Stream RunPlan events as SSE
  - `GET /v1/agentos/plans/{plan_id}/events/history` — Query durable RunPlan event history
  - `GET /v1/agentos/plans/{plan_id}/debug/traces` — Query typed debug traces
  - `GET /v1/agentos/plans/{plan_id}/audits` — Query durable plan audit records
  - `GET /v1/agentos/plans/{plan_id}/artifacts` — Query plan artifact refs
  - `GET /v1/agentos/plans/{plan_id}/artifacts/{artifact_id}` — Read one plan artifact document
- **Templates API**:
  - `POST /v1/templates/import` — Import workflow template from YAML
  - `GET /v1/templates/` — List templates
  - `GET /v1/templates/{template_id}` — Get template details
  - `DELETE /v1/templates/{template_id}` — Delete template
- **PostgreSQL**: `postgres://user:myAwEsOm3pa55@w0rd@127.0.0.1:5432/db`

## Project Structure

GoAgent is structured around a small public **AgentOS boundary** plus adapters and an application shell. Implementation packages live under `internal/` and are not public contracts.

```text
Applications / reference distributions
  -> agentos/platform        # optional facade when an app wants both planes
      -> agentos/control     # Agent Control Plane
      -> agentos/process     # Durable Process Platform
          -> agentos/core    # shared OS primitives

Default implementation
  -> agentos/temporal        # Temporal/Postgres/Redis/artifact adapter
      -> agentos/control
      -> agentos/process
      -> agentos/core

Internal application
  -> internal/controller     # REST transport
  -> internal/usecase        # application use cases
  -> internal/repo           # persistence/backend adapters
  -> internal/agentfw        # native GoAgent backend implementation
```

The separation is intentional:

- `agentos/control` never imports `agentos/process`; agent runs and plans do not know business process semantics.
- `agentos/process` never imports `agentos/control`; durable processes can exist without agent execution.
- `agentos/platform` is the composition facade for applications that need both.
- `agentos/temporal` implements public ports. It must not define AiSOC, DevOps, CodeAgent, or other business domain models.
- `internal/agentfw` is a native backend, not the public architecture of every backend.

### Public Library Packages

| Package | Layer | Description |
|---------|-------|-------------|
| `agentos/core/` | Shared primitives | Signals, controls, events, artifacts, messages, tools, subscriptions, and public errors |
| `agentos/control/` | Agent Control Plane | Runtime, PlanRuntime, RunSpec, RunPlanSpec, PlanNodeSpec, capabilities, backend refs, plan schemas |
| `agentos/process/` | Durable Process Platform | ResourceRef, process runtime, ledger runtime, governed action runtime, batch runtime, projection runtime |
| `agentos/platform/` | Composition facade | One runtime interface that embeds the control and process interfaces |
| `agentos/temporal/` | Default adapter | Temporal/Postgres/Redis/artifact-store implementation and worker registration kit |
| `config/` | Outer | Application configuration (env-based) |
| `pkg/` | Generic utilities | Infrastructure wrappers that are not GoAgent implementation contracts |

### Application Shell

- `internal/app/` — Dependency injection and application bootstrap
- `internal/agentfw/` — Agent workflow/runtime implementation
- `internal/entity/`, `internal/usecase/`, `internal/repo/`, `internal/state/` — Internal domain and infrastructure implementation
- `internal/controller/` — Transport layer (REST AgentOS control plane)
- `cmd/app/` — Entry point

### Other Directories

- `docs/` — Swagger docs
- `examples/` — Runnable pattern examples
- `integration-test/` — Integration tests (requires Docker)
- `migrations/` — PostgreSQL migrations

### Configuration Management

Follows the [12-Factor App](https://12factor.net/) principles. All configuration is managed through environment variables.

Configuration file: [config/config.go](config/config.go)  
Example configuration: [.env.example](.env.example)

### Observability

OpenTelemetry tracing exports spans to an OTLP gRPC collector. Toggled by `TRACING_ENABLED` (default `false`); also manages `TRACING_OTLP_ENDPOINT`, `TRACING_OTLP_INSECURE`, and `TRACING_SAMPLE_RATE`. See [pkg/tracing](pkg/tracing).

## Agent Control Plane

The Agent Control Plane coordinates backend-owned agent runs. A backend may be the native GoAgent backend, a LangGraph service, an OpenCode-style runtime, an HTTP service, a gRPC service, or an external Temporal workflow.

The public model is intentionally coarse-grained:

- `control.RunSpec` starts one backend-owned run.
- `control.RunStatus` is the public lifecycle view of that run.
- `control.RunPlanSpec` composes backend-owned runs.
- `control.PlanNodeSpec` is a run-level orchestration node. It is not a native GoAgent step, not a Temporal activity, not a LangGraph graph node, not an OpenCode step, and not a tool call.
- `control.CapabilityRunBatch` represents one backend-owned batch run. AgentOS validates coarse limits and observes progress; it does not expand batch items into thousands of plan nodes.

Backend-specific graph, loop, step, tool, and record-level execution details stay inside the owning backend or data plane. They can be reported to AgentOS as events, artifacts, ledger records, or projections.

## Durable Process Platform

The Durable Process Platform provides generic building blocks for long-lived intelligent work:

- `process.ResourceRef` identifies domain resources such as cases, tickets, orders, incidents, alerts, pull requests, or changes without making those nouns part of AgentOS core.
- `process.Runtime` owns durable process lifecycle.
- `process.LedgerRuntime` records decisions, evidence refs, action refs, prompt/response refs, artifact refs, actors, timestamps, and rationale.
- `process.GovernedActionRuntime` models dry-run, risk evaluation, approval, execution, cancellation, and compensation.
- `process.BatchRuntime` models worksets and bounded batch progress.
- `process.ProjectionRuntime` serves REST, MCP, UI, and operator read models from durable projections instead of high-frequency Temporal Workflow Query calls.

Temporal stores deterministic process control, timers, signals, retries, and compact references. Large prompts, responses, evidence blobs, search indexes, lake data, graph data, and artifact payloads stay in external stores.

## Native Agent Backend

The native GoAgent backend is one implementation behind the control plane:

1. **Agent Runtime** — ReAct loop with LLM provider abstraction
2. **Tool System** — Tool definitions with JSON Schema, executor abstraction, MCP server integration
3. **Team System** — Hierarchical team composition inside the native backend
4. **Temporal Worker Kit** — Workflow/activity registration for durable native execution

Native step queues, team expansion, LLM calls, tool calls, and backend-internal graph logic are implementation details. They are not the model that external backends must copy.

### REST Examples

The `examples/http/` directory contains AgentOS REST examples:

| Example | File | What It Shows |
|---------|------|---------------|
| [RunPlan](examples/http/runplan/) | `examples/http/runplan/main.go` | Start a durable AgentOS RunPlan over REST using public `agentos/control` types |

### Library Embedding Examples (Mode 2 — AgentOS Runtime)

The `examples/embed/` directory shows how to embed GoAgent through the public AgentOS boundary:

| Example | File | What It Shows |
|---------|------|---------------|
| [ReAct](examples/embed/react/) | `examples/embed/react/main.go` | Start a generic run with `control.Runtime` |
| [Conversation](examples/embed/conversation/) | `examples/embed/conversation/main.go` | Start a conversational run through `agentos/temporal` |
| [Tools](examples/embed/tools/) | `examples/embed/tools/main.go` | Start a tool-capable prompt through the runtime boundary |
| [RunPlan](examples/embed/plan/) | `examples/embed/plan/main.go` | Start a durable cross-backend plan with `control.PlanRuntime` |

### Type-Only Usage (Mode 3)

The `examples/types/` directory shows importing only `agentos/core` and `agentos/control` for shared public type definitions.

### Cross-Backend Plans (AgentOS RunPlan)

AgentOS supports durable cross-backend orchestration through `control.PlanRuntime`.

`RunPlan` is the public control-plane model for coordinating backend-owned child runs. A `PlanNodeSpec` is a full `control.RunSpec` plus backend, capability, input, output, condition, and policy contracts. It is not a native GoAgent step, not a Temporal activity, and not a LangGraph node.

Native GoAgent `entity.Step` remains an internal detail of the GoAgent native backend. Backend-specific step, graph, loop, and tool execution details should be emitted through events or artifacts, not promoted into the public AgentOS API.

Capabilities are coarse-grained backend contracts. `control.CapabilityRun` starts one backend-owned run. `control.CapabilityRunBatch` starts one backend-owned batch run and validates bounded batch input through capability limits; AgentOS does not expand batch items into plan nodes.

Plan runtime query APIs are durable: status comes from the plan index, event history comes from the plan event store, audits come from the audit store, and artifact payloads come from the artifact store. SSE is only the live streaming transport layered on top of the durable event history.

The durable process layer exposes `process.ProjectionRuntime` for REST, MCP, UI, and operator read models. Projection reads come from durable process, ledger, governed action, and workset stores, not from high-frequency Temporal Workflow Query calls.

`control.PlanJSONSchema` and `GET /v1/agentos/plans/schemas/{kind}` expose the
public authoring schemas for editors and CI. `cmd/agentos-plan` is the RunPlan
DSL/compiler tool. It validates JSON/YAML
`RunPlanSpec`, generates JSON Schema, validates bounded `PlanDelta` expansion,
and imports/exports Serverless Workflow as an edge interoperability format. The
typed `control.RunPlanSpec` remains the source of truth.

```bash
go run ./cmd/agentos-plan schema --kind run-plan --out docs/schemas/run_plan.schema.json
go run ./cmd/agentos-plan schema --kind plan-delta --out docs/schemas/plan_delta.schema.json
go run ./cmd/agentos-plan schema --kind capability-catalog --out docs/schemas/capability_catalog.schema.json
go run ./cmd/agentos-plan schema --kind artifact-schema-catalog --out docs/schemas/artifact_schema_catalog.schema.json
go run ./cmd/agentos-plan validate --file plan.yaml --format yaml --capabilities capabilities.yaml --capabilities-format yaml --artifact-schemas artifact-schemas.yaml --artifact-schemas-format yaml
go run ./cmd/agentos-plan export-serverless --file plan.yaml --format yaml --capabilities capabilities.yaml --capabilities-format yaml --artifact-schemas artifact-schemas.yaml --artifact-schemas-format yaml --out-format yaml --out workflow.yaml
go run ./cmd/agentos-plan import-serverless --file workflow.yaml --format yaml --capabilities capabilities.yaml --capabilities-format yaml --artifact-schemas artifact-schemas.yaml --artifact-schemas-format yaml --out-format json
```

External Go projects should import only:

```go
import (
    "github.com/TekkenSteve/GoAgent/agentos/control"
    "github.com/TekkenSteve/GoAgent/agentos/core"
    "github.com/TekkenSteve/GoAgent/agentos/process"
    "github.com/TekkenSteve/GoAgent/agentos/platform"
    agentostemporal "github.com/TekkenSteve/GoAgent/agentos/temporal"
)
```

Do not import implementation packages such as `internal/entity`, `internal/repo`, `internal/usecase`, or old root-level implementation packages. Public examples and docs are guarded by tests to keep that boundary intact.

## Usage Modes

GoAgent can be consumed in three ways, from simple to deeply integrated:

### Mode 1 — Standalone Server (REST API)

Run GoAgent as a standalone service. Your application talks to it through the AgentOS REST control plane.

```go
import (
    "bytes"
    "encoding/json"
    "net/http"

    agentos "github.com/TekkenSteve/GoAgent/agentos/control"
)

body, _ := json.Marshal(agentos.RunSpec{
    RunID: "run-1",
    AccountID: "acct-1",
    ProjectID: "proj-1",
    UserMessage: "What is 2+2?",
    IdempotencyKey: "run-1-start",
    Backend: agentos.BackendRef{
        Kind: agentos.BackendKindNative,
        Name: agentos.BackendNameGoAgentNative,
    },
})
req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "http://localhost:8080/v1/agentos/runs", bytes.NewReader(body))
req.Header.Set("Content-Type", "application/json")
resp, _ := http.DefaultClient.Do(req)
defer resp.Body.Close()
```

### Mode 2 — Go Library Embedding

Import the stable AgentOS boundary into a Go application. Use `agentos/temporal` for the default Temporal/Postgres/Redis implementation.

```go
import (
    agentos "github.com/TekkenSteve/GoAgent/agentos/control"
    agentostemporal "github.com/TekkenSteve/GoAgent/agentos/temporal"
)

rt, _ := agentostemporal.NewRuntime(ctx, agentostemporal.RuntimeConfig{
    TemporalAddress: "127.0.0.1:7233",
    TemporalNamespace: "default",
    TemporalTaskQueues: agentostemporal.DefaultTaskQueues(),
    PostgresURL: "postgres://goagent:goagent@127.0.0.1:5432/goagent?sslmode=disable",
    RedisURL: "redis://127.0.0.1:6379/0",
})
status, _ := rt.Start(ctx, agentos.RunSpec{
    RunID: "run-1",
    AccountID: "acct-1",
    ProjectID: "proj-1",
    ModelRef: "gpt-4.1-mini",
    UserMessage: "What is 2+2?",
    IdempotencyKey: "run-1-start",
})
```

### Mode 3 — Public Types Only

Import only the public AgentOS packages needed by the service. Use `agentos/core` for shared messages/tools/events and `agentos/control` for run/plan contracts.

```go
import (
    "github.com/TekkenSteve/GoAgent/agentos/core"
    "github.com/TekkenSteve/GoAgent/agentos/control"
)

type MyService struct {
    messages []core.Message
    tools    []core.ToolDef
    plans    []control.RunPlanSpec
}
```

## Architecture Design

### Clean Architecture Principles

This project follows the [go-clean-template](https://github.com/evrone/go-clean-template) architecture pattern:

1. **Public ports** (`agentos/core`, `agentos/control`, `agentos/process`, `agentos/platform`) define stable application-facing contracts.
2. **Adapters** (`agentos/temporal`, `internal/repo/*`, `internal/controller/*`) implement ports for Temporal, storage, backend runtimes, and transports.
3. **Use cases** coordinate application behavior through interfaces instead of concrete infrastructure.
4. **Dependency direction** stays explicit: domain contracts do not import adapters, and public packages do not import `internal`.
5. **Testability** comes from small interfaces, deterministic workflow inputs, and boundary tests.

### Dependency Flow

```text
┌────────────────────────────────────────────────────────────┐
│ Applications / reference distributions                     │
│  - AiSOC, CodeAgent, DevOps, CustomerOps                   │
│  - import agentos/platform or specific public packages      │
├────────────────────────────────────────────────────────────┤
│ Public AgentOS ports                                        │
│  agentos/core                                               │
│  agentos/control        Agent Control Plane                 │
│  agentos/process        Durable Process Platform            │
│  agentos/platform       Composition facade                  │
├────────────────────────────────────────────────────────────┤
│ Public default adapter                                      │
│  agentos/temporal       Temporal/Postgres/Redis/artifacts   │
├────────────────────────────────────────────────────────────┤
│ Application shell                                           │
│  internal/controller    REST transport                      │
│  internal/usecase       application services                │
│  internal/repo          persistence and backend adapters     │
│  internal/agentfw       native GoAgent backend              │
│  internal/app           dependency injection                │
└────────────────────────────────────────────────────────────┘
```

- **Public ports** are the supported application-facing contracts.
- **Temporal adapter** is the default implementation of those contracts, not the owner of business vocabulary.
- **Application shell** wires the service, HTTP API, persistence, worker registration, and native backend.
- **Reference distributions** should live in `examples/` or downstream repositories. They use AgentOS primitives but do not change AgentOS core types.

### Dependency Injection

Dependencies are injected through constructors, maintaining the independence and testability of business logic:

```go
type UseCase struct {
    repo Repository  // Interface dependency
}

func New(r Repository) *UseCase {
    return &UseCase{repo: r}
}
```

### API Versioning

Supports a simple versioning strategy, with versions distinguished by directory structure:

- REST API: `internal/controller/restapi/v1`, `v2`...

## Development Guide

### Database Migrations

```sh
# Run migrations
go run -tags migrate ./cmd/app

# Or use make
make run
```

### Code Generation

```sh
# Generate Swagger documentation
make swag-v1

# Generate Mocks
make mock
```

### Code Quality

```sh
# Run linter
make linter-golangci

# Format code
make format

# Run unit tests
make test
```

## CI Checks (run locally before push)

```sh
make linter-golangci    # golangci-lint
make linter-hadolint    # Dockerfile lint
make linter-dotenv       # .env lint
make check-workflow-determinism  # Temporal workflow determinism
make test               # Unit tests
```

## References

- [Clean Architecture](https://blog.cleancoder.com/uncle-bob/2012/08/13/the-clean-architecture.html) — Robert Martin
- [The Twelve-Factor App](https://12factor.net/)
- [Temporal Workflow Platform](https://temporal.io/)
- [Go Project Layout](https://github.com/golang-standards/project-layout)

## License

MIT License — See [LICENSE](LICENSE) file for details
