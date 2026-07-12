# GoAgent

GoAgent is the reference implementation of **AgentOS**: an **Agent Control Plane** plus a **Durable Process Platform** built on Temporal.

It has two public surfaces:

- **Agent Control Plane** — starts, controls, observes, and composes backend-owned agent runs across native GoAgent, LangGraph, OpenCode-style runtimes, HTTP backends, gRPC backends, and external Temporal workflows.
- **Durable Process Platform** — models long-lived intelligent work as generic resources, processes, ledgers, governed actions, batches, and projections. Domain systems such as AiSOC should build on these primitives without adding their business nouns to AgentOS core.

The native GoAgent agent framework is one backend implementation. Temporal is the durable process kernel. `agentos/temporal` is the default adapter that wires AgentOS ports to Temporal, Postgres, Redis, and artifact storage.

## Возможности

- **Agent Control Plane** — backend-owned run lifecycle, signals, controls, durable RunPlan orchestration, backend capabilities, and event ingest
- **Durable Process Platform** — generic resource/process runtime, ledger, governed action, batch/workset, and projection interfaces
- **Temporal Kernel Adapter** — explicit Temporal task queue isolation, durable workflows, timers, signals, retries, cancellation, and recovery
- **Native Agent Backend** — ReAct loop agent runtime with LLM integration, tool execution, team composition, and MCP server support
- **External Backend Adapters** — HTTP, gRPC, and `temporal_external` backends for runtimes owned outside GoAgent
- **Многоагентные команды** — Иерархическая композиция команд с рекурсивным расширением подкоманд
- **Человек в цикле** — Пауза/возобновление/отмена рабочих процессов и шаги ожидания сигналов
- **Потоковая передача** — SSE и WebSocket поддержка для вывода агента в реальном времени
- **Учет и биллинг** — Отслеживание использования на основе кредитов с назначением тарифных планов
- **Чистая архитектура** — Инверсия зависимостей, изоляция на основе интерфейсов, тестируемость
- **Наблюдаемость** — Структурированное логирование (zerolog), метрики Prometheus, трассировка OpenTelemetry
- **Миграции БД** — golang-migrate для управления схемой PostgreSQL

## Технологический стек

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

## Быстрый старт

### Требования

- Go 1.26+
- Docker & Docker Compose
- Temporal Server (через Docker)

### Локальная разработка

```sh
# Запуск зависимых сервисов (Postgres, Redis, Temporal)
make compose-up

# Запуск приложения (с миграциями базы данных)
make run
```

### Интеграционные тесты

```sh
# Запуск полного тестового окружения с mock LLM
make compose-up-integration-test
```

### Полный Docker стек

```sh
make compose-up-all
```

## Эндпоинты сервисов

- **REST API**:
  - `http://127.0.0.1:8080/healthz` — проверка здоровья
  - `http://127.0.0.1:8080/metrics` — метрики Prometheus
  - `http://127.0.0.1:8080/swagger` — документация API
- **AgentOS API (v1)**:
  - `POST /v1/agentos/runs` — запуск run на выбранном backend
  - `GET /v1/agentos/runs/{run_id}/status` — проверка статуса run
  - `POST /v1/agentos/runs/{run_id}/signals` — отправка бизнес-сигналов, например `user.message`
  - `POST /v1/agentos/runs/{run_id}/control` — pause, resume или cancel
  - `POST /v1/agentos/runs/{run_id}/events` — прием событий backend
  - `GET /v1/agentos/plans/schemas/{kind}` — JSON Schema для авторинга RunPlan
  - `GET /v1/agentos/plans/author` — консоль авторинга RunPlanSpec
  - `POST /v1/agentos/plans` — запуск durable RunPlan для нескольких backend
  - `GET /v1/agentos/plans/{plan_id}/status` — проверка агрегированного статуса plan
  - `GET /v1/agentos/plans/{plan_id}/description` — публичная топология и статус
  - `GET /v1/agentos/plans/{plan_id}/console` — операторская консоль RunPlan
  - `POST /v1/agentos/plans/{plan_id}/signals` — отправка plan-сигналов retry, approve или reject
  - `POST /v1/agentos/plans/{plan_id}/control` — pause, resume или cancel для RunPlan
  - `GET /v1/agentos/plans/{plan_id}/events` — поток событий RunPlan через SSE
  - `GET /v1/agentos/plans/{plan_id}/events/history` — durable история событий RunPlan
  - `GET /v1/agentos/plans/{plan_id}/debug/traces` — typed debug traces
  - `GET /v1/agentos/plans/{plan_id}/audits` — durable audit records
  - `GET /v1/agentos/plans/{plan_id}/artifacts` — plan artifact refs
  - `GET /v1/agentos/plans/{plan_id}/artifacts/{artifact_id}` — документ одного plan artifact
- **Шаблоны API**:
  - `POST /v1/templates/import` — импорт шаблона из YAML
  - `GET /v1/templates/` — список шаблонов
  - `GET /v1/templates/{template_id}` — детали шаблона
  - `DELETE /v1/templates/{template_id}` — удаление шаблона
- **PostgreSQL**: `postgres://user:myAwEsOm3pa55@w0rd@127.0.0.1:5432/db`

## Структура проекта

GoAgent организован вокруг небольшой публичной границы **AgentOS**, adapters и оболочки приложения. Реализация находится в `internal/` и не является публичным контрактом.

```text
Applications / reference distributions
  -> agentos/platform        # facade when an app wants both planes
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

Границы слоев являются частью архитектуры:

- `agentos/control` не импортирует `agentos/process`; agent runs и plans не знают семантику бизнес-процессов.
- `agentos/process` не импортирует `agentos/control`; durable processes могут существовать без agent execution.
- `agentos/platform` является composition facade для приложений, которым нужны оба слоя.
- `agentos/temporal` реализует public ports и не определяет доменные модели AiSOC, DevOps, CodeAgent или других приложений.
- `internal/agentfw` является native backend, а не публичной архитектурой для всех backend.

### Публичные пакеты библиотеки

| Пакет | Слой | Описание |
|---------|-------|-------------|
| `agentos/core/` | Shared primitives | Signals, controls, events, artifacts, messages, tools, subscriptions, and public errors |
| `agentos/control/` | Agent Control Plane | Runtime, PlanRuntime, RunSpec, RunPlanSpec, PlanNodeSpec, capabilities, backend refs, plan schemas |
| `agentos/process/` | Durable Process Platform | ResourceRef, process runtime, ledger runtime, governed action runtime, batch runtime, projection runtime |
| `agentos/platform/` | Composition facade | One runtime interface that embeds the control and process interfaces |
| `agentos/temporal/` | Default adapter | Temporal/Postgres/Redis/artifact-store implementation and worker registration kit |
| `config/` | Внешний | Конфигурация приложения (на основе env) |
| `pkg/` | Общие утилиты | Инфраструктурные обертки, которые не являются контрактами реализации GoAgent |

### Оболочка приложения

- `internal/app/` — внедрение зависимостей и инициализация приложения
- `internal/agentfw/` — реализация agent workflow/runtime
- `internal/entity/`, `internal/usecase/`, `internal/repo/`, `internal/state/` — внутренняя доменная и инфраструктурная реализация
- `internal/controller/` — транспортный слой (REST AgentOS control plane)
- `cmd/app/` — точка входа

### Прочие директории

- `docs/` — Swagger документация
- `examples/` — исполняемые примеры паттернов
- `integration-test/` — интеграционные тесты (требуется Docker)
- `migrations/` — миграции PostgreSQL

### Управление конфигурацией

Следуя принципам [12-Factor App](https://12factor.net/), вся конфигурация управляется через переменные окружения.

Файл конфигурации: [config/config.go](config/config.go)  
Пример конфигурации: [.env.example](.env.example)

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

Директория `examples/http/` содержит REST-примеры AgentOS:

| Пример | Файл | Что демонстрирует |
|---------|------|---------------|
| [RunPlan](examples/http/runplan/) | `examples/http/runplan/main.go` | Запуск durable AgentOS RunPlan через REST с публичными типами `agentos/control` |

### Примеры встраивания библиотеки (Режим 2 — AgentOS Runtime)

Директория `examples/embed/` показывает, как встраивать GoAgent через публичную границу AgentOS:

| Пример | Файл | Что демонстрирует |
|---------|------|---------------|
| [ReAct](examples/embed/react/) | `examples/embed/react/main.go` | Запуск generic run через `control.Runtime` |
| [Conversation](examples/embed/conversation/) | `examples/embed/conversation/main.go` | Запуск диалогового run через `agentos/temporal` |
| [Tools](examples/embed/tools/) | `examples/embed/tools/main.go` | Запуск tool-capable prompt через runtime boundary |
| [RunPlan](examples/embed/plan/) | `examples/embed/plan/main.go` | Запуск durable cross-backend plan через `control.PlanRuntime` |

### Только типы (Режим 3)

Директория `examples/types/` показывает импорт только `agentos/core` и `agentos/control` для общих публичных типов.

## Три режима использования

GoAgent поддерживает три способа интеграции, от простого к глубокому:

### Режим 1 — Автономный сервер (REST API)

Запустите GoAgent как самостоятельный сервис. Приложение взаимодействует с ним через AgentOS REST control plane.

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
    UserMessage: "Сколько будет 2+2?",
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

### Режим 2 — Go library embedding

Импортируйте стабильную границу AgentOS runtime в ваше Go-приложение. Реализация по умолчанию на Temporal/Postgres/Redis находится в `agentos/temporal`.

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
    UserMessage: "Сколько будет 2+2?",
    IdempotencyKey: "run-1-start",
})
```

### Режим 3 — Только типы

Импортируйте только нужные публичные пакеты AgentOS. Используйте `agentos/core` для messages/tools/events и `agentos/control` для run/plan contracts.

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

## Архитектурный дизайн

### Принципы Clean Architecture

Проект следует архитектурному паттерну [go-clean-template](https://github.com/evrone/go-clean-template):

1. **Public ports** (`agentos/core`, `agentos/control`, `agentos/process`, `agentos/platform`) define stable application-facing contracts.
2. **Adapters** (`agentos/temporal`, `internal/repo/*`, `internal/controller/*`) implement ports for Temporal, storage, backend runtimes, and transports.
3. **Use cases** coordinate application behavior through interfaces instead of concrete infrastructure.
4. **Dependency direction** stays explicit: domain contracts do not import adapters, and public packages do not import `internal`.
5. **Testability** comes from small interfaces, deterministic workflow inputs, and boundary tests.

### Поток зависимостей

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

### Внедрение зависимостей

Зависимости внедряются через конструкторы, сохраняя независимость и тестируемость бизнес-логики:

```go
type UseCase struct {
    repo Repository  // зависимость через интерфейс
}

func New(r Repository) *UseCase {
    return &UseCase{repo: r}
}
```

### Версионирование API

Поддерживается простая стратегия версионирования, версии различаются структурой директорий:

- REST API: `internal/controller/restapi/v1`, `v2`...

## Руководство разработчика

### Миграции базы данных

```sh
# Запуск миграций
go run -tags migrate ./cmd/app

# Или через make
make run
```

### Генерация кода

```sh
# Генерация документации Swagger
make swag-v1

# Генерация Mock
make mock
```

### Проверка кода

```sh
# Запуск линтера
make linter-golangci

# Форматирование кода
make format

# Запуск юнит-тестов
make test
```

## CI проверки (запустите локально перед push)

```sh
make linter-golangci               # golangci-lint
make linter-hadolint               # проверка Dockerfile
make linter-dotenv                  # проверка .env
make check-workflow-determinism    # проверка детерминизма Temporal
make test                          # юнит-тесты
```

## Справочные материалы

- [Clean Architecture](https://blog.cleancoder.com/uncle-bob/2012/08/13/the-clean-architecture.html) — Robert Martin
- [The Twelve-Factor App](https://12factor.net/)
- [Temporal Workflow Platform](https://temporal.io/)
- [Go Project Layout](https://github.com/golang-standards/project-layout)

## Лицензия

MIT License — см. файл [LICENSE](LICENSE)
