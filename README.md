# GoAgent

A Go microservices framework built on Clean Architecture principles, integrating an Agent runtime and Temporal-based orchestration engine.

## Features

- **Agent Framework** — ReAct loop agent runtime with LLM integration, tool execution, and MCP server support
- **Orchestration Engine** — Temporal-based workflow orchestration with multi-step, parallel, conditional, and dynamic execution patterns
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
  - `POST /v1/agentos/plans` — Start a durable cross-backend RunPlan
  - `GET /v1/agentos/plans/{plan_id}/status` — Poll aggregate plan status
  - `POST /v1/agentos/plans/{plan_id}/signals` — Send plan signals such as retry, approve, or reject
  - `POST /v1/agentos/plans/{plan_id}/control` — Send pause, resume, or cancel to a RunPlan
  - `GET /v1/agentos/plans/{plan_id}/events` — Stream RunPlan events as SSE
  - `GET /v1/agentos/plans/{plan_id}/events/history` — Query durable RunPlan event history
  - `GET /v1/agentos/plans/{plan_id}/audits` — Query durable plan audit records
  - `GET /v1/agentos/plans/{plan_id}/artifacts` — Query plan artifact refs
  - `GET /v1/agentos/plans/{plan_id}/artifacts/{artifact_id}` — Read one plan artifact document
- **Orchestration API**:
  - `POST /v1/orchestration/execute` — Start multi-step orchestration workflow
  - `GET /v1/orchestration/status/{run_id}` — Poll orchestration status
- **Templates API**:
  - `POST /v1/templates/import` — Import workflow template from YAML
  - `GET /v1/templates/` — List templates
  - `GET /v1/templates/{template_id}` — Get template details
  - `DELETE /v1/templates/{template_id}` — Delete template
- **Triggers API**:
  - `POST /v1/triggers/events` — Fire trigger event webhook
- **PostgreSQL**: `postgres://user:myAwEsOm3pa55@w0rd@127.0.0.1:5432/db`

## Project Structure

GoAgent is structured around a small public **AgentOS SDK boundary** plus an application shell. Implementation packages live under `internal/` and are not public contracts.

### Public Library Packages

| Package | Layer | Description |
|---------|-------|-------------|
| `agentos/` | Public SDK | Stable runtime and plan interfaces, run specs, RunPlan specs, statuses, events, artifacts, capabilities, messages, and tool definitions |
| `agentos/temporal/` | Public implementation | Default Temporal/Redis runtime, PlanRuntime, and worker registration kit |
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

## Agent Framework

### Architecture

The agent framework consists of:

1. **Agent Runtime** — ReAct loop: `think → act → observe → repeat`, with LLM provider abstraction
2. **Tool System** — Tool definitions with JSON Schema, executor abstraction, MCP server integration
3. **Team System** — Hierarchical team composition with recursive expansion into flat step queues
4. **Orchestration Engine** — Temporal workflow that executes steps with dependency resolution, parallel fan-out, dynamic mutation, and human-in-the-loop signals

### Step Types

| Type | Purpose |
|------|---------|
| `agent` | Execute an agent with a prompt |
| `tool` | Execute a tool directly |
| `wait` | Wait for a Temporal signal (HITL) or timeout |
| `split` | Fan-out into parallel sub-steps |
| `join` | Fan-in to gather parallel results |
| `eval` | Conditional evaluation with dynamic step mutation |

### Orchestration Patterns (Mode 1 — HTTP Client)

The `examples/http/` directory contains runnable demonstrations using the HTTP client SDK:

| Pattern | File | Key Concepts |
|---------|------|-------------|
| [ReAct](examples/http/react/) | Single agent + tool loop | `AgentOSRunRequest`, polling |
| [Pipeline](examples/http/pipeline/) | Sequential processing stages | `depends_on` chain |
| [DAG](examples/http/dag/) | Directed acyclic graph | Multi-dependency resolution |
| [Research](examples/http/research/) | Parallel exploration + synthesis | `split`/`join`, `wait` (HITL) |
| [Supervisor-Worker](examples/http/supervisor-worker/) | Decompose + parallel workers | `split`/`join`, supervisor agent |
| [Router](examples/http/router/) | Conditional branching | `eval` + `OnResult` mutation |
| [Reflexion](examples/http/reflexion/) | Self-critique quality loop | `eval` + dynamic refinement |
| [Plan-and-Execute](examples/http/plan-and-execute/) | Plan → parallel execute → evaluate | `split`/`join` + `eval` mutation |
| [Exploratory](examples/http/exploratory/) | Self-modifying step queue | `eval` + `append_after` mutation |
| [ToT / LATS](examples/http/tot-lats/) | Multiple reasoning paths | Parallel exploration + best-path eval |
| [Scientific](examples/http/scientific/) | Hypothesis → HITL → experiment | `wait` signal, timeout handling |
| [Team](examples/http/team/) | Multi-agent hierarchy | `TeamSpec` + `SubTeams` |
| [Hierarchical](examples/http/hierarchical/) | Executive → departments | Nested `TeamSpec` with expansion |

### Library Embedding Examples (Mode 2 — AgentOS Runtime)

The `examples/embed/` directory shows how to embed GoAgent through the public AgentOS boundary:

| Example | File | What It Shows |
|---------|------|---------------|
| [ReAct](examples/embed/react/) | `examples/embed/react/main.go` | Start a generic run with `agentos.Runtime` |
| [Conversation](examples/embed/conversation/) | `examples/embed/conversation/main.go` | Start a conversational run through `agentos/temporal` |
| [Tools](examples/embed/tools/) | `examples/embed/tools/main.go` | Start a tool-capable prompt through the runtime boundary |
| [RunPlan](examples/embed/plan/) | `examples/embed/plan/main.go` | Start a durable cross-backend plan with `agentos.PlanRuntime` |

### Type-Only Usage (Mode 3)

The `examples/types/` directory shows importing only `agentos/` for shared public type definitions.

### Cross-Backend Plans (AgentOS RunPlan)

AgentOS supports durable cross-backend orchestration through `agentos.PlanRuntime`.

`RunPlan` is the public control-plane model for coordinating backend-owned child runs. A `PlanNodeSpec` is a full `agentos.RunSpec` plus backend, capability, input, output, condition, and policy contracts. It is not a native GoAgent step, not a Temporal activity, and not a LangGraph node.

Native GoAgent `entity.Step` remains an internal detail of the GoAgent native backend. Backend-specific step, graph, loop, and tool execution details should be emitted through events or artifacts, not promoted into the public AgentOS API.

Plan runtime query APIs are durable: status comes from the plan index, event history comes from the plan event store, audits come from the audit store, and artifact payloads come from the artifact store. SSE is only the live streaming transport layered on top of the durable event history.

`cmd/agentos-plan` is the RunPlan DSL/compiler tool. It validates JSON/YAML
`RunPlanSpec`, generates JSON Schema, validates bounded `PlanDelta` expansion,
and imports/exports Serverless Workflow as an edge interoperability format. The
typed `agentos.RunPlanSpec` remains the source of truth.

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
    "github.com/TekkenSteve/GoAgent/agentos"
    agentostemporal "github.com/TekkenSteve/GoAgent/agentos/temporal"
)
```

Do not import implementation packages such as `internal/entity`, `internal/repo`, `internal/usecase`, or old root-level implementation packages. Public examples and docs are guarded by tests to keep that boundary intact.

## Three Usage Modes

GoAgent can be consumed in three ways, from simple to deeply integrated:

### Mode 1 — Standalone Server (REST API)

Run GoAgent as a standalone service. Your application talks to it through the AgentOS REST control plane.

```go
import "github.com/TekkenSteve/GoAgent/examples/client"

c := client.New("http://localhost:8080", "my-account")
status, _ := c.StartRun(ctx, client.AgentOSRunRequest{
    RunID: "run-1",
    UserMessage: "What is 2+2?",
    Backend: client.BackendRef{Kind: "native", Name: "goagent-native"},
})
```

### Mode 2 — Library Embedding

Import the stable AgentOS runtime boundary into your Go application. Use `agentos/temporal` for the default Temporal/Redis implementation.

```go
import (
    "github.com/TekkenSteve/GoAgent/agentos"
    agentostemporal "github.com/TekkenSteve/GoAgent/agentos/temporal"
)

rt, _ := agentostemporal.NewRuntime(ctx, agentostemporal.RuntimeConfig{
    TemporalAddress: "127.0.0.1:7233",
    TemporalNamespace: "default",
    TemporalTaskQueue: "agent-framework",
    RedisURL: "redis://127.0.0.1:6379/0",
})
status, _ := rt.Start(ctx, agentos.RunSpec{
    RunID: "run-1",
    AccountID: "acct-1",
    ModelRef: "gpt-4.1-mini",
    UserMessage: "What is 2+2?",
})
```

### Mode 3 — Type-Only

Import only `agentos/` to share public AgentOS type definitions across microservices.

```go
import "github.com/TekkenSteve/GoAgent/agentos"

type MyService struct {
    messages []agentos.Message
    tools    []agentos.ToolDef
}
```

## Architecture Design

### Clean Architecture Principles

This project follows the [go-clean-template](https://github.com/evrone/go-clean-template) architecture pattern:

1. **Public SDK boundary** (`agentos/`) — stable external contract for embedded callers
2. **Internal ports** (`internal/usecase/contracts.go`, `internal/repo/contracts.go`) — implementation contracts hidden from downstream projects
3. **Dependency direction**: outer layers import inner layers, never the reverse
4. **Testability**: interface isolation enables easy unit testing with mocks

### Dependency Flow

```
┌──────────────────────────────────────────────┐
│  agentos/                                      │  Public SDK
│  agentos/temporal/                             │  Default implementation
├──────────────────────────────────────────────┤
│  internal/entity/   │  internal/state/       │  Inner Layer
│  ──────────┼──────────                        │  (zero external deps,
│  internal/usecase/contracts.go               │   stdlib only)
│  internal/repo/contracts.go                  │
├──────────────────────────────────────────────┤
│  internal/usecase/agent/  ...                │  Inner Layer
│  (imports internal/repo for output ports)     │  (business logic)
├──────────────────────────────────────────────┤
│  internal/repo/persistent/  ...              │  Outer Layer
│  internal/controller/  internal/app/         │  (infrastructure,
│  internal/agentfw/orchestration/              │   imports inner)
└──────────────────────────────────────────────┘
```

- **Public layer** (`agentos/`, `agentos/temporal/`) is the only supported embedded import contract
- **Inner layer** (`internal/entity/`, `internal/state/`, `internal/usecase/`, `internal/repo/contracts.go`) is implementation-only
- **Outer layer** (`internal/repo/*/`, `internal/controller/`, `internal/app/`, `pkg/`) implements interfaces defined by the inner layer

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
