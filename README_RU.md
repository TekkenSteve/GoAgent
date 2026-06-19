# GoAgent

Фреймворк для микросервисов на Go, построенный на принципах Clean Architecture с интегрированной средой выполнения агентов и оркестрацией на базе Temporal.

## Возможности

- **Agent Framework** — Среда выполнения ReAct цикла с интеграцией LLM, выполнением инструментов и поддержкой MCP серверов
- **Оркестрация** — Управление рабочими процессами на базе Temporal с поддержкой многошаговых, параллельных, условных и динамических сценариев
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
  - `POST /v1/agentos/plans` — запуск durable RunPlan для нескольких backend
  - `GET /v1/agentos/plans/{plan_id}/status` — проверка агрегированного статуса plan
  - `POST /v1/agentos/plans/{plan_id}/signals` — отправка plan-сигналов retry, approve или reject
  - `POST /v1/agentos/plans/{plan_id}/control` — pause, resume или cancel для RunPlan
  - `GET /v1/agentos/plans/{plan_id}/events` — поток событий RunPlan через SSE
- **Оркестрация API**:
  - `POST /v1/orchestration/execute` — запуск многошагового рабочего процесса
  - `GET /v1/orchestration/status/{run_id}` — проверка статуса оркестрации
- **Шаблоны API**:
  - `POST /v1/templates/import` — импорт шаблона из YAML
  - `GET /v1/templates/` — список шаблонов
  - `GET /v1/templates/{template_id}` — детали шаблона
  - `DELETE /v1/templates/{template_id}` — удаление шаблона
- **Триггеры API**:
  - `POST /v1/triggers/events` — запуск события вебхука
- **PostgreSQL**: `postgres://user:myAwEsOm3pa55@w0rd@127.0.0.1:5432/db`

## Структура проекта

GoAgent организован вокруг небольшой публичной границы **AgentOS SDK** и оболочки приложения. Реализация находится в `internal/` и не является публичным контрактом.

### Публичные пакеты библиотеки

| Пакет | Слой | Описание |
|---------|-------|-------------|
| `agentos/` | Public SDK | Стабильный runtime-интерфейс, спецификации запусков, статусы, события, сообщения и определения инструментов |
| `agentos/temporal/` | Публичная реализация | Реализация по умолчанию на Temporal/Redis и kit для регистрации worker |
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

## Agent Framework

### Архитектура

Фреймворк состоит из:

1. **Среда выполнения агентов** — ReAct цикл: `думай → действуй → наблюдай → повторяй` с абстракцией LLM провайдера
2. **Система инструментов** — Определения инструментов с JSON Schema, абстракция исполнителя, интеграция MCP серверов
3. **Система команд** — Иерархическая композиция команд с рекурсивным расширением в плоскую очередь шагов
4. **Движок оркестрации** — Temporal рабочий процесс, выполняющий шаги с разрешением зависимостей, параллельным разветвлением, динамическими мутациями и сигналами

### Типы шагов

| Тип | Назначение |
|------|-----------|
| `agent` | Выполнение агента с промптом |
| `tool` | Прямое выполнение инструмента |
| `wait` | Ожидание Temporal сигнала (HITL) или таймаута |
| `split` | Разветвление на параллельные подшаги |
| `join` | Сбор параллельных результатов |
| `eval` | Условная оценка с динамической мутацией шагов |

### Паттерны оркестрации (Режим 1 — HTTP клиент)

Директория `examples/http/` содержит исполняемые демонстрации с использованием HTTP клиентского SDK:

| Паттерн | Файл | Ключевые концепции |
|---------|------|-------------|
| [ReAct](examples/http/react/) | Один агент + цикл инструментов | `AgentOSRunRequest`, опрос |
| [Pipeline](examples/http/pipeline/) | Последовательные этапы обработки | Цепочка `depends_on` |
| [DAG](examples/http/dag/) | Направленный ациклический граф | Разрешение множественных зависимостей |
| [Research](examples/http/research/) | Параллельное исследование + синтез | `split`/`join`, `wait` (HITL) |
| [Supervisor-Worker](examples/http/supervisor-worker/) | Декомпозиция + параллельные исполнители | `split`/`join`, агент-супервизор |
| [Router](examples/http/router/) | Условное ветвление | `eval` + мутация `OnResult` |
| [Reflexion](examples/http/reflexion/) | Цикл самокритики и улучшения | `eval` + динамическое уточнение |
| [Plan-and-Execute](examples/http/plan-and-execute/) | План → параллельное выполнение → оценка | `split`/`join` + мутация `eval` |
| [Exploratory](examples/http/exploratory/) | Самоизменяющаяся очередь шагов | `eval` + мутация `append_after` |
| [ToT / LATS](examples/http/tot-lats/) | Множественные пути рассуждения | Параллельное исследование + выбор лучшего пути |
| [Scientific](examples/http/scientific/) | Гипотеза → HITL → эксперимент | Сигнал `wait`, обработка таймаута |
| [Team](examples/http/team/) | Многоагентная иерархия | `TeamSpec` + `SubTeams` |
| [Hierarchical](examples/http/hierarchical/) | Руководитель → отделы | Вложенный `TeamSpec` с расширением |

### Примеры встраивания библиотеки (Режим 2 — AgentOS Runtime)

Директория `examples/embed/` показывает, как встраивать GoAgent через публичную границу AgentOS:

| Пример | Файл | Что демонстрирует |
|---------|------|---------------|
| [ReAct](examples/embed/react/) | `examples/embed/react/main.go` | Запуск generic run через `agentos.Runtime` |
| [Conversation](examples/embed/conversation/) | `examples/embed/conversation/main.go` | Запуск диалогового run через `agentos/temporal` |
| [Tools](examples/embed/tools/) | `examples/embed/tools/main.go` | Запуск tool-capable prompt через runtime boundary |

### Только типы (Режим 3)

Директория `examples/types/` показывает импорт только `agentos/` для общих публичных типов.

## Три режима использования

GoAgent поддерживает три способа интеграции, от простого к глубокому:

### Режим 1 — Автономный сервер (REST API)

Запустите GoAgent как самостоятельный сервис. Приложение взаимодействует с ним через AgentOS REST control plane.

```go
import "github.com/TekkenSteve/GoAgent/examples/client"

c := client.New("http://localhost:8080", "my-account")
status, _ := c.StartRun(ctx, client.AgentOSRunRequest{
    RunID: "run-1",
    UserMessage: "Сколько будет 2+2?",
    Backend: client.BackendRef{Kind: "native", Name: "goagent-native"},
})
```

### Режим 2 — Встраивание библиотеки

Импортируйте стабильную границу AgentOS runtime в ваше Go-приложение. Реализация по умолчанию на Temporal/Redis находится в `agentos/temporal`.

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
    UserMessage: "Сколько будет 2+2?",
})
```

### Режим 3 — Только типы

Импортируйте только `agentos/` для обмена публичными типами AgentOS между микросервисами.

```go
import "github.com/TekkenSteve/GoAgent/agentos"

type MyService struct {
    messages []agentos.Message
    tools    []agentos.ToolDef
}
```

## Архитектурный дизайн

### Принципы Clean Architecture

Проект следует архитектурному паттерну [go-clean-template](https://github.com/evrone/go-clean-template):

1. **Публичная SDK-граница** (`agentos/`) — стабильный контракт для внешних embedded callers
2. **Внутренние порты** (`internal/usecase/contracts.go`, `internal/repo/contracts.go`) — контракты реализации, скрытые от downstream-проектов
3. **Направление зависимостей**: внешние слои импортируют внутренние, никогда наоборот
4. **Тестируемость**: изоляция через интерфейсы упрощает юнит-тестирование с моками

### Поток зависимостей

```
┌──────────────────────────────────────────────┐
│  agentos/                                      │  Public SDK
│  agentos/temporal/                             │  Реализация по умолчанию
├──────────────────────────────────────────────┤
│  internal/entity/ │  internal/state/         │  Внутренний слой
│  ──────────┼──────────                        │  (без внешних зависимостей,
│  internal/usecase/contracts.go                │   только stdlib)
│  internal/repo/contracts.go                   │
├──────────────────────────────────────────────┤
│  internal/usecase/agent/  ...                │  Внутренний слой
│  (импортирует internal/repo для портов)       │  (бизнес-логика)
├──────────────────────────────────────────────┤
│  internal/repo/persistent/  ...              │  Внешний слой
│  internal/controller/  internal/app/         │  (инфраструктура,
│  internal/agentfw/orchestration/              │   импортирует внутренний)
└──────────────────────────────────────────────┘
```

- **Публичный слой** (`agentos/`, `agentos/temporal/`) — единственный поддерживаемый embedded import contract
- **Внутренний слой** (`internal/entity/`, `internal/state/`, `internal/usecase/`, `internal/repo/contracts.go`) является деталью реализации
- **Внешний слой** (`internal/repo/*/`, `internal/controller/`, `internal/app/`, `pkg/`) реализует интерфейсы внутреннего слоя

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
