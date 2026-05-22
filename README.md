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
- **Multiple Server Types** — REST API, gRPC, AMQP RPC, NATS RPC
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
# Start dependency services (Postgres, RabbitMQ, NATS, Temporal)
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
- **Agent API** (v1):
  - `POST /v1/agent/execute` — Execute a single agent run (ReAct loop)
  - `GET /v1/agent/status/{run_id}` — Poll agent run status
  - `GET /v1/agent/{run_id}/messages` — List conversation messages
  - `GET /v1/agent/{run_id}/tools` — List tool execution results
  - `GET /v1/agent/stream` — SSE stream of agent output
  - `GET /v1/agent/ws` — WebSocket for real-time agent communication
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
- **gRPC**: `tcp://127.0.0.1:8081`
- **AMQP RPC**: `amqp://guest:guest@127.0.0.1:5672/`
- **NATS RPC**: `nats://guest:guest@127.0.0.1:4222/`
- **PostgreSQL**: `postgres://user:myAwEsOm3pa55@w0rd@127.0.0.1:5432/db`

## Project Structure

GoAgent is structured as a **library + application shell** following the [go-clean-template](https://github.com/evrone/go-clean-template) pattern. Public packages at the root level are importable by external projects; `internal/` contains only the application shell.

### Public Library Packages

| Package | Layer | Description |
|---------|-------|-------------|
| `entity/` | Inner | Domain primitives — zero dependencies, stdlib only |
| `usecase/` | Inner | Business logic + **input port** interfaces (called by controllers) |
| `repo/` | Inner/Outer | **Output port** interfaces (called by use cases) + infrastructure adapters |
| `state/` | Inner | State management abstractions — hot/warm/cold layering |
| `agentfw/` | Both | Agent runtime — `agent/`, `team/`, `tool/`, `stream/` (inner); `orchestration/`, `runtime/`, `config/` (outer) |
| `config/` | Outer | Application configuration (env-based) |
| `pkg/` | Outer | Infrastructure wrappers — Postgres, Redis, HTTP server, etc. |

### Application Shell

- `internal/app/` — Dependency injection and application bootstrap
- `internal/controller/` — Transport layer (REST, gRPC, AMQP RPC, NATS RPC)
- `cmd/app/` — Entry point

### Other Directories

- `docs/` — Swagger docs and Proto files
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

### Orchestration Patterns

The `examples/` directory contains runnable demonstrations of common agent patterns:

| Pattern | File | Key Concepts |
|---------|------|-------------|
| [ReAct](examples/react/) | Single agent + tool loop | `ExecuteRequest`, polling |
| [Pipeline](examples/pipeline/) | Sequential processing stages | `depends_on` chain |
| [DAG](examples/dag/) | Directed acyclic graph | Multi-dependency resolution |
| [Research](examples/research/) | Parallel exploration + synthesis | `split`/`join`, `wait` (HITL) |
| [Supervisor-Worker](examples/supervisor-worker/) | Decompose + parallel workers | `split`/`join`, supervisor agent |
| [Router](examples/router/) | Conditional branching | `eval` + `OnResult` mutation |
| [Reflexion](examples/reflexion/) | Self-critique quality loop | `eval` + dynamic refinement |
| [Plan-and-Execute](examples/plan-and-execute/) | Plan → parallel execute → evaluate | `split`/`join` + `eval` mutation |
| [Exploratory](examples/exploratory/) | Self-modifying step queue | `eval` + `append_after` mutation |
| [ToT / LATS](examples/tot-lats/) | Multiple reasoning paths | Parallel exploration + best-path eval |
| [Scientific](examples/scientific/) | Hypothesis → HITL → experiment | `wait` signal, timeout handling |
| [Team](examples/team/) | Multi-agent hierarchy | `TeamSpec` + `SubTeams` |
| [Hierarchical](examples/hierarchical/) | Executive → departments | Nested `TeamSpec` with expansion |

Each example uses the [examples/client](examples/client/) SDK to interact with the REST API.

## Three Usage Modes

GoAgent can be consumed in three ways, from simple to deeply integrated:

### Mode 1 — Standalone Server (REST API)

Run GoAgent as a standalone service. Your application talks to it via HTTP/gRPC.

```go
import "github.com/TekkenSteve/GoAgent/examples/client"

c := client.New("http://localhost:8080", "my-account")
status, _ := c.ExecuteAgent(ctx, client.ExecuteRequest{
    RunID: "run-1", UserMessage: "What is 2+2?",
})
```

### Mode 2 — Library Embedding

Import GoAgent packages directly into your Go application. Bring your own infrastructure adapters or use the built-in ones.

```go
import (
    "github.com/TekkenSteve/GoAgent/usecase/agent"
    "github.com/TekkenSteve/GoAgent/repo/webapi"      // LLM provider
    "github.com/TekkenSteve/GoAgent/repo/pipeline"    // Redis WAL
    "github.com/TekkenSteve/GoAgent/repo/compressor"  // Context compression
    "github.com/TekkenSteve/GoAgent/repo/toolkit"     // Built-in tools
)

llm := webapi.NewBifrostProvider(cfg)
tools := toolkit.NewRegistry(llm)
wal := pipeline.NewRedisWAL(appender, rdb)

agentUC := agent.New(llm, tools, wal, compressor, tools, nil)
result, _ := agentUC.ExecuteStep(ctx, &agent.StepRequest{
    RunID:   "run-1",
    Message: "What is 2+2?",
    Config:  entity.LLMConfig{Model: "claude-sonnet-4-20250514"},
})
```

### Mode 3 — Type-Only

Import only `entity/` to share domain type definitions across microservices.

```go
import "github.com/TekkenSteve/GoAgent/entity"

type MyService struct {
    messages []entity.Message
    tools    []entity.ToolDef
}
```

## Architecture Design

### Clean Architecture Principles

This project follows the [go-clean-template](https://github.com/evrone/go-clean-template) architecture pattern:

1. **Input ports** (`usecase/contracts.go`) — interfaces implemented by business logic, called by controllers
2. **Output ports** (`repo/contracts.go`) — interfaces called by business logic, implemented by infrastructure adapters
3. **Dependency direction**: outer layers import inner layers, never the reverse
4. **Testability**: interface isolation enables easy unit testing with mocks

### Dependency Flow

```
┌──────────────────────────────────────────────┐
│  entity/   │  state/                         │  Inner Layer
│  ──────────┼──────────                        │  (zero external deps,
│  usecase/contracts.go  (input ports)         │   stdlib only)
│  repo/contracts.go     (output ports)        │
├──────────────────────────────────────────────┤
│  usecase/agent/  usecase/template/  ...     │  Inner Layer
│  (imports repo/ for output ports)            │  (business logic)
├──────────────────────────────────────────────┤
│  repo/persistent/  repo/webapi/  ...         │  Outer Layer
│  internal/controller/  internal/app/         │  (infrastructure,
│  agentfw/orchestration/                       │   imports inner)
└──────────────────────────────────────────────┘
```

- **Inner layer** (`entity/`, `state/`, `usecase/`, `repo/contracts.go`) depends only on Go stdlib
- **Outer layer** (`repo/*/`, `internal/`, `pkg/`) implements interfaces defined by the inner layer
- `usecase/agent/` imports `repo/` for output port interfaces — the same pattern as go-clean-template's `usecase/translation/` importing `repo/`

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
- gRPC: `internal/controller/grpc/v1`, `v2`...
- RPC: `internal/controller/amqp_rpc/v1`, `v2`...

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

# Generate gRPC code
make proto-v1

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
