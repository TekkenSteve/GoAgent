# GoAgent

> **GoAgent** is an open-source **AgentOS**: the agent execution layer for products with AI inside. Product teams build the product — the domain, the UX, the loop; GoAgent runs its agentic half: durable runs and plans, tool and MCP execution, streaming, artifacts, audit, billing, and multi-tenancy behind one control plane.

[![Web Framework](https://img.shields.io/badge/Fiber-Web%20Framework-blue)](https://github.com/gofiber/fiber)
[![Workflow Engine](https://img.shields.io/badge/Temporal-Workflow%20Engine-blue)](https://temporal.io/)
[![Event Backbone](https://img.shields.io/badge/NATS%20JetStream-Event%20Backbone-blue)](https://nats.io/)
[![SQL Compiler](https://img.shields.io/badge/sqlc-Type--Safe%20SQL-blue)](https://sqlc.dev/)
[![Database Migrations](https://img.shields.io/badge/golang--migrate-Schema%20Updates-blue)](https://github.com/golang-migrate/migrate)
[![Logging](https://img.shields.io/badge/ZeroLog-Structured%20Logging-blue)](https://github.com/rs/zerolog)
[![Metrics](https://img.shields.io/badge/Prometheus-Metrics%20Integration-blue)](https://github.com/ansrivas/fiberprometheus)

## The problem

The model is one layer of the product, not the product. Model upgrades improve that layer for free — and leave everything around it untouched. A product whose core loop includes long-running AI work still has to answer, in engineering: what happens when the process dies mid-run, how a three-day conversation survives restarts, who approved that tool call, what exactly did the agent do to my data, and what did it cost.

Every AI product team hand-rolls the same unglamorous machinery to answer those questions — run lifecycle, crash-safe task queues, tool dispatch, progress streaming, artifact storage, audit trail, usage metering. None of that machinery knows anything about the product. A flashcard studio and a security-operations platform need the *same* half; only their nouns differ.

Five distributed-systems problems, each amplified by non-deterministic reasoning:

| Production problem | What agents add | What the system must own |
|---|---|---|
| Long-running work | conversations, approvals, watch cycles span days | durable waits, timers, crash recovery |
| External side effects | the model *will* call your payment and notification tools | idempotency, retry, compensation, approval gates |
| Non-deterministic decisions | one goal, many reasoning paths | policy gates, budgets, recorded rationale |
| Many actors | teams, agents, and humans at once | contracts, isolation, authorization |
| Constant change | prompts, tool schemas, and policies drift | versioned schemas, controlled migration |

GoAgent is that machinery, productized. The LLM keeps the uncertain cognitive steps; a durable workflow owns the certain lifecycle and governance around them.

## What GoAgent is

```mermaid
graph TB
    subgraph PRODUCTS["Your products — any domain"]
        direction LR
        P1["a flashcard studio<br/>(conversation-driven crafting)"]
        P2["a security-ops platform"]
        P3["a dev tool · a game · anything<br/>with long-running AI work"]
    end

    subgraph AGENTOS["GoAgent / AgentOS"]
        CP["Agent Control Plane<br/>runs · plans · signals · capabilities"]
        PP["Durable Process Platform<br/>resources · ledger · governed actions · worksets"]
        NX["Nexus API<br/>durable cross-service operations"]
    end

    subgraph STATE["Durability & facts"]
        T["Temporal<br/>state · timers · retries · recovery"]
        N["NATS JetStream<br/>transactional outbox → projections · mirrors"]
        P[("Postgres<br/>ledger · hash-chained audit · projections")]
    end

    subgraph BACKENDS["Agent backends"]
        NA["native GoAgent ReAct<br/>+ MCP tools"]
        LG["LangGraph · OpenCode-style<br/>runtimes"]
        EXT["HTTP · gRPC ·<br/>external Temporal"]
    end

    PRODUCTS -->|"engine-neutral contracts<br/>agentos/core · control · process"| AGENTOS
    CP --> BACKENDS
    PP --> STATE
    NX --> T
```

Three decisions carry the design:

- **Your nouns stay yours.** Domain work arrives as generic `ResourceRef`s — a card pack and a security case are the same primitive to AgentOS. It never learns your domain model, and your product never learns its internals: the public contracts (`agentos/core`, `agentos/control`, `agentos/process`) import no execution engine at all, locked by `make check-import-boundary`.
- **The control plane owns orchestration; backends own execution.** GoAgent starts, signals, controls, and observes *backend-owned* runs and composes them into durable cross-backend plans. A backend is the native ReAct loop, a LangGraph service, an OpenCode-style runtime, or any HTTP/gRPC/Temporal service — the graph, steps, and tools inside it stay there.
- **Big payloads never enter workflow history.** Prompts, tool I/O, and evidence live in external stores and are referenced by claim-check; the workflow keeps state, commands, and references. Committed facts flow from a transactional outbox onto JetStream to projections, mirrors, and analytics; REST, MCP, and consoles read Postgres read-models, never high-frequency workflow queries.

## In practice

[Kardcraft](https://github.com/TekkenSteve/Kardcraft) — a conversation-driven flashcard crafting product for spaced repetition — runs on this design. Its LangGraph card-production graphs are an AgentOS backend; its Go task orchestrator is a downstream consumer of `agentos.PlanRuntime`. One measure of the boundary: the orchestrator's AgentOS adapter is, by its own contract, *the only Kardcraft package that knows AgentOS types*.

## The guarantees

The audit checklist a platform reviewer actually runs down:

- **Durable** — runs and plans survive crashes, deployments, and multi-day waits; `run.status` over Nexus is a bounded-staleness snapshot (≈5s), never a blocking backend call.
- **Backend-agnostic** — native, LangGraph, OpenCode-style, HTTP, gRPC, and external Temporal runs sit behind one contract; batch items stay bounded by capability limits instead of exploding into plan nodes.
- **Governed** — risky work goes through dry-run, risk evaluation, approval, execution, cancellation, and compensation (`GovernedActionRuntime`).
- **Auditable** — decisions, evidence refs, actors, and rationale land in a ledger; the audit log is hash-chained and tamper-evident.
- **Billable** — credit accounts and a usage ledger meter every run.
- **Multi-tenant** — account/project scoping enforced end to end, including row-level tenant keys in the schema.
- **Engine-neutral** — the public contracts have zero Temporal imports; Temporal lives entirely in the `agentos/temporal` adapter and could be exchanged without touching a consumer.
- **Interop edge** — `cmd/agentos-plan` validates plans, emits JSON Schema authoring contracts, and imports/exports [Serverless Workflow](https://serverlessworkflow.io/) as an exchange format.

## Where it sits

Every layer of the agent stack is having its moment. They solve different problems:

| Kind | Examples | Unit of service | What they optimize |
|---|---|---|---|
| Companion agents | OpenClaw (🦞), Hermes, Muse, Cue, Grokbot | one person's attention | personality, personal context, chat channels |
| Autonomous task products | Manus | one task deliverable | end-to-end completion in a vendor sandbox |
| Coding agent harnesses | DeepSeek Harness, Claude Code, Codex | one developer's session | the agent loop: tools, sandbox, review |
| Agent frameworks | LangGraph, CrewAI | your application's code | building one agent app |
| **GoAgent (AgentOS)** | **this repo** | **one product's agent workload** | durability, governance, audit, billing, tenancy — as a platform the product owns |

**Is GoAgent a harness?** No. A harness is the cockpit for *one* agent loop — it drives tools, sandbox, and review for one developer at a terminal. GoAgent is air-traffic control for *many* backend-owned runs — lifecycle, governance, and audit for the whole workload. They compose rather than compete: a harness-built agent (or any HTTP/gRPC/Temporal service) plugs in as one backend behind the control plane, which is exactly how LangGraph and OpenCode-style runtimes already sit inside it. And GoAgent is not a framework you code against in-process — it is infrastructure your product *uses*, over REST, Go embedding, or type-only contracts.

## Quick start

Requires Go 1.26+, Docker, and Docker Compose.

```sh
# Once: generate development credentials (.env). The stack refuses to
# start without them, and the repository ships none.
make dev-secrets

# Start dependencies: Postgres, Redis, NATS JetStream, Centrifugo, Temporal
make compose-up

# Run the application (builds with the migrate tag and applies migrations)
make run
```

- REST API: `http://127.0.0.1:8080` — [`/healthz`](http://127.0.0.1:8080/healthz), [`/metrics`](http://127.0.0.1:8080/metrics), [`/swagger`](http://127.0.0.1:8080/swagger)
- Full stack in Docker: `make compose-up-all`
- Integration tests (mock LLM, container network): `make compose-up-integration-test`

## API surface

Versioned REST under `/v1`; full reference at [`/swagger`](http://127.0.0.1:8080/swagger). The shape:

| Area | Representative endpoints |
|---|---|
| Runs | `POST /v1/agentos/runs` · `GET /runs/{id}/status` · `POST /runs/{id}/signals` · `POST /runs/{id}/control` |
| Durable plans | `POST /v1/agentos/plans` · `GET /plans/{id}/status` · `GET /plans/{id}/events` (SSE) · `GET /plans/{id}/audits` · `GET /plans/{id}/artifacts/{id}` |
| Authoring | `GET /v1/agentos/plans/schemas/{kind}` · `GET /v1/agentos/plans/author` · `cmd/agentos-plan` (validate / schema / Serverless Workflow in/out) |
| Templates | `POST /v1/templates/import` · `GET /v1/templates/` |

## Project structure

A small public **AgentOS boundary** plus adapters and an application shell. Implementation packages under `internal/` are not public contracts.

```text
Public ports (engine-neutral, zero Temporal imports)
  agentos/core       # signals, controls, events, artifacts, tools, errors
  agentos/control    # run + plan contracts, capabilities, backend refs
  agentos/process    # resources, ledger, governed actions, batches, projections
  agentos/platform   # composition facade when an app wants both planes

Default adapter
  agentos/temporal   # Temporal + Postgres + Redis + artifact store
  agentos/nexusapi   # versioned Nexus service contract

Application shell (not public)
  internal/controller   # REST transport
  internal/usecase      # application use cases
  internal/repo         # persistence (sqlc-first) and backend adapters
  internal/agentfw      # the native GoAgent backend — one backend, not the architecture
  internal/app, cmd/    # wiring and entry points
```

Two rules keep the boundary honest: `agentos/control` never imports `agentos/process` (agent runs don't know business semantics), and `agentos/process` never imports `agentos/control` (durable processes exist without agent execution). SQL lives in `internal/repo/persistent/queries/*.sql` and is compiled to type-safe bindings by `make sqlc` — no ORM, no runtime SQL assembly.

## Usage modes

1. **Standalone server** — run GoAgent as a service; your product talks to it over the REST control plane.
2. **Go library embedding** — import the AgentOS boundary, boot it with the default adapter:

```go
import (
    agentos "github.com/TekkenSteve/GoAgent/agentos/control"
    agentostemporal "github.com/TekkenSteve/GoAgent/agentos/temporal"
)

rt, _ := agentostemporal.NewRuntime(ctx, agentostemporal.RuntimeConfig{
    TemporalAddress:    "127.0.0.1:7233",
    TemporalNamespace:  "default",
    TemporalTaskQueues: agentostemporal.DefaultTaskQueues(),
    PostgresURL:        "postgres://goagent:goagent@127.0.0.1:5432/goagent?sslmode=disable",
    RedisURL:           "redis://127.0.0.1:6379/0",
})
status, _ := rt.Start(ctx, agentos.RunSpec{ /* … */ })
```

3. **Public types only** — import `agentos/core` and `agentos/control` for shared contracts.

Runnable examples for every mode live in [`examples/`](examples/) (REST, embed, types).

## Development

```sh
make sqlc                # regenerate type-safe SQL bindings (required after query/schema changes)
make swag-v1             # regenerate Swagger docs
make mock                # regenerate gomock mocks
make test                # unit tests (-race)
make compose-up-integration-test   # integration suite inside the container network
make linter-golangci     # lint
make check-import-boundary        # enforce engine-neutral public contracts
make check-workflow-determinism   # Temporal workflow determinism
```

Configuration is 12-factor, environment-only — see [config/config.go](config/config.go) and [.env.example](.env.example). Migrations are golang-migrate pairs in [`migrations/`](migrations/); the app applies them at startup when built with the `migrate` tag. Tracing is OpenTelemetry (`TRACING_ENABLED`), metrics Prometheus, logging zerolog.

## Outlook

- **The kernel is a detail.** Public contracts never mention the execution engine; Temporal lives in one adapter. A different durable kernel could replace it without touching a single consumer.
- **Promises over polling.** The Nexus surface grows toward durable cross-service operations that hold across crashes for seconds or days, exactly-once, instead of ad-hoc status polling.
- **Facts flow one way.** The transactional-outbox → JetStream backbone becomes the canonical fact feed: projections, mirrors, analytics, and replay subscribe; UI deltas stream separately over Centrifugo and never enter the streams.
- **MCP as the tool lingua franca.** The native backend already speaks MCP; capability and artifact-schema catalogs are versioned JSON Schema — the substrate an agent marketplace needs before it needs agents.
- **Agents as utilities.** The endgame is agent work you can run like electricity: metered by the ledger, tamper-evident by the audit chain, fenced by governed actions — boring in exactly the way infrastructure should be.

## References

- [Temporal](https://temporal.io/) · [NATS JetStream](https://nats.io/) · [Centrifugo](https://centrifugal.dev/) · [sqlc](https://sqlc.dev/)
- [Clean Architecture](https://blog.cleancoder.com/uncle-bob/2012/08/13/the-clean-architecture.html) · [The Twelve-Factor App](https://12factor.net/)

## License

MIT License — see [LICENSE](LICENSE).
