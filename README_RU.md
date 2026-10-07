# GoAgent

GoAgent — эталонная реализация **AgentOS**: **Agent Control Plane** плюс **Durable Process Platform** на базе Temporal.

У неё две публичные поверхности:

- **Agent Control Plane** — запускает, контролирует, наблюдает и композирует backend-owned agent runs: native GoAgent, LangGraph, runtime в стиле OpenCode, HTTP backend, gRPC backend и внешние Temporal workflow.
- **Durable Process Platform** — моделирует долгоживущую интеллектуальную работу как generic resources, processes, ledgers, governed actions, batches и projections. Доменные системы вроде AiSOC должны строиться на этих примитивах, не добавляя свои бизнес-существительные в ядро AgentOS.

Native GoAgent agent framework — лишь одна из реализаций backend. Temporal — durable process kernel. `agentos/temporal` — адаптер по умолчанию, связывающий порты AgentOS с Temporal, Postgres, Redis и artifact storage.

## Возможности

- **Agent Control Plane** — жизненный цикл backend-owned runs, сигналы, управление, durable оркестрация RunPlan, возможности backend и прием событий
- **Durable Process Platform** — generic runtime ресурсов и процессов, ledger, governed action, batch/workset и projection интерфейсы
- **Temporal Kernel Adapter** — явная изоляция Temporal task queue, durable workflow, timer, signal, retry, cancel и восстановление
- **Nexus Service Surface** — версионированный Nexus service (`agentos/nexusapi`) предоставляет запуск run, signal, control и status между Namespaces, поэтому вызывающий код зависит от контракта, а не от внутренностей; `run.status` — снимок с ограниченной задержкой (≈5s), так как синхронные handler'ы никогда не обращаются к backend напрямую
- **Native Agent Backend** — ReAct-loop runtime агента с интеграцией LLM, выполнением инструментов, композицией команд и поддержкой MCP server
- **External Backend Adapters** — HTTP, gRPC и `temporal_external` backend для runtime, которыми владеет не GoAgent
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
[![SQL Compiler](https://img.shields.io/badge/sqlc-Type--Safe%20SQL-blue)](https://sqlc.dev/)
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
# Запуск зависимых сервисов (Postgres, Redis, NATS JetStream, Centrifugo, Temporal)
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
- **PostgreSQL**: `postgres://user@127.0.0.1:5432/db`

## Структура проекта

GoAgent организован вокруг небольшой публичной границы **AgentOS**, adapters и оболочки приложения. Реализация находится в `internal/` и не является публичным контрактом.

```text
Applications / reference distributions
  -> agentos/platform        # facade when an app wants both planes
      -> agentos/control     # Agent Control Plane
      -> agentos/process     # Durable Process Platform
          -> agentos/core    # shared OS primitives

Default implementation
  -> agentos/nexusapi        # Nexus service contract: names and operation I/O types
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
- `internal/repo/persistent/queries/` — SQL-запросы, по файлу на домен
- `internal/repo/persistent/sqlcgen/` — сгенерированные типобезопасные привязки (`make sqlc`)

### Управление конфигурацией

Следуя принципам [12-Factor App](https://12factor.net/), вся конфигурация управляется через переменные окружения.

Файл конфигурации: [config/config.go](config/config.go)  
Пример конфигурации: [.env.example](.env.example)

### Наблюдаемость

OpenTelemetry tracing экспортирует спаны в OTLP gRPC коллектор. Включается переменной `TRACING_ENABLED` (по умолчанию `false`); дополнительно управляется через `TRACING_OTLP_ENDPOINT`, `TRACING_OTLP_INSECURE` и `TRACING_SAMPLE_RATE`. См. [pkg/tracing](pkg/tracing).

## Agent Control Plane

Agent Control Plane координирует backend-owned agent runs. Backend может быть native GoAgent backend, сервисом LangGraph, runtime в стиле OpenCode, HTTP-сервисом, gRPC-сервисом или внешним Temporal workflow.

Публичная модель намеренно крупнозернистая:

- `control.RunSpec` запускает один backend-owned run.
- `control.RunStatus` — публичное представление жизненного цикла этого run.
- `control.RunPlanSpec` композирует backend-owned runs.
- `control.PlanNodeSpec` — узел оркестрации уровня run. Это не шаг native GoAgent, не Temporal activity, не узел графа LangGraph, не шаг OpenCode и не вызов инструмента.
- `control.CapabilityRunBatch` представляет один backend-owned batch run. AgentOS проверяет грубые лимиты и наблюдает прогресс; он не разворачивает элементы batch в тысячи узлов plan.

Детали выполнения, специфичные для backend — графы, циклы, шаги, инструменты и записи, — остаются внутри владеющего backend или data plane. Они могут передаваться в AgentOS как события, artifacts, ledger records или projections.

## Durable Process Platform

Durable Process Platform даёт generic строительные блоки для долгоживущей интеллектуальной работы:

- `process.ResourceRef` идентифицирует доменные ресурсы — cases, tickets, orders, incidents, alerts, pull requests или changes — не делая эти существительные частью ядра AgentOS.
- `process.Runtime` владеет жизненным циклом durable процесса.
- `process.LedgerRuntime` записывает решения, ссылки на доказательства, ссылки на действия, ссылки на prompt/response, ссылки на artifacts, акторов, временные метки и обоснование.
- `process.GovernedActionRuntime` моделирует dry-run, оценку риска, согласование, выполнение, отмену и компенсацию.
- `process.BatchRuntime` моделирует worksets и ограниченный прогресс batch.
- `process.ProjectionRuntime` отдает read-модели для REST, MCP, UI и операторов из durable projections вместо частых Temporal Workflow Query.

Temporal хранит детерминированное управление процессами, timer'ы, сигналы, retry и компактные ссылки. Большие prompts, responses, blobs доказательств, поисковые индексы, lake-данные, графовые данные и payload'ы artifacts остаются во внешних хранилищах.

## Native Agent Backend

Native GoAgent backend — одна из реализаций за control plane:

1. **Agent Runtime** — ReAct-loop с абстракцией LLM provider
2. **Tool System** — определения инструментов с JSON Schema, абстракция executor, интеграция MCP server
3. **Team System** — иерархическая композиция команд внутри native backend
4. **Temporal Worker Kit** — регистрация workflow/activity для durable native исполнения

Очереди native шагов, расширение команд, вызовы LLM, вызовы инструментов и внутренняя графовая логика backend — детали реализации. Это не модель, которую обязаны копировать внешние backend.

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

### Кросс-бэкенд планы (AgentOS RunPlan)

AgentOS поддерживает durable кросс-бэкендную оркестрацию через `control.PlanRuntime`.

`RunPlan` — публичная модель control plane для координации backend-owned дочерних runs. `PlanNodeSpec` — это полный `control.RunSpec` плюс контракты backend, capability, peer (город, в котором выполняется его run), input, output, condition и policy. Это не шаг native GoAgent, не Temporal activity и не узел LangGraph.

Native GoAgent `entity.Step` остается внутренней деталью native backend GoAgent. Детали выполнения step, graph, loop и tool, специфичные для backend, должны передаваться через события или artifacts, а не подниматься в публичный API AgentOS.

Capabilities — крупнозернистые контракты backend. `control.CapabilityRun` запускает один backend-owned run. `control.CapabilityRunBatch` запускает один backend-owned batch run и валидирует ограниченный batch input через лимиты capability; AgentOS не разворачивает элементы batch в узлы plan.

Query API plan runtime являются durable: status берется из plan index, история событий — из plan event store, аудиты — из audit store, payload'ы artifacts — из artifact store. SSE — лишь живой streaming-транспорт поверх durable истории событий.

Durable process layer предоставляет `process.ProjectionRuntime` для read-моделей REST, MCP, UI и операторов. Чтения проекций идут из durable хранилищ process, ledger, governed action и workset, а не из частых Temporal Workflow Query.

`control.PlanJSONSchema` и `GET /v1/agentos/plans/schemas/{kind}` предоставляют публичные схемы авторинга для редакторов и CI. `cmd/agentos-plan` — инструмент RunPlan DSL/компилятора: валидирует JSON/YAML `RunPlanSpec`, генерирует JSON Schema, валидирует ограниченное расширение `PlanDelta` и импортирует/экспортирует Serverless Workflow как краевой формат интероперабельности. Типизированный `control.RunPlanSpec` остается источником истины.

```bash
go run ./cmd/agentos-plan schema --kind run-plan --out docs/schemas/run_plan.schema.json
go run ./cmd/agentos-plan schema --kind plan-delta --out docs/schemas/plan_delta.schema.json
go run ./cmd/agentos-plan schema --kind capability-catalog --out docs/schemas/capability_catalog.schema.json
go run ./cmd/agentos-plan schema --kind artifact-schema-catalog --out docs/schemas/artifact_schema_catalog.schema.json
go run ./cmd/agentos-plan validate --file plan.yaml --format yaml --capabilities capabilities.yaml --capabilities-format yaml --artifact-schemas artifact-schemas.yaml --artifact-schemas-format yaml
go run ./cmd/agentos-plan export-serverless --file plan.yaml --format yaml --capabilities capabilities.yaml --capabilities-format yaml --artifact-schemas artifact-schemas.yaml --artifact-schemas-format yaml --out-format yaml --out workflow.yaml
go run ./cmd/agentos-plan import-serverless --file workflow.yaml --format yaml --capabilities capabilities.yaml --capabilities-format yaml --artifact-schemas artifact-schemas.yaml --artifact-schemas-format yaml --out-format json
```

Внешние Go-проекты должны импортировать только:

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

1. **Public ports** (`agentos/core`, `agentos/control`, `agentos/process`, `agentos/platform`) определяют стабильные контракты для приложений.
2. **Adapters** (`agentos/temporal`, `internal/repo/*`, `internal/controller/*`) реализуют порты для Temporal, хранилищ, backend runtime и транспортов.
3. **Use cases** координируют поведение приложения через интерфейсы, а не через конкретную инфраструктуру.
4. **Направление зависимостей** остается явным: доменные контракты не импортируют adapters, а публичные пакеты не импортируют `internal`.
5. **Тестируемость** достигается небольшими интерфейсами, детерминированными входами workflow и boundary-тестами.

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

- **Public ports** — поддерживаемые контракты для приложений.
- **Temporal adapter** — реализация этих контрактов по умолчанию, а не владелец бизнес-лексики.
- **Application shell** связывает сервис, HTTP API, персистентность, регистрацию worker и native backend.
- **Reference distributions** должны жить в `examples/` или в downstream-репозиториях. Они используют примитивы AgentOS, но не меняют типы ядра AgentOS.

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

# Генерация типобезопасных SQL-привязок из queries/*.sql
make sqlc
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
