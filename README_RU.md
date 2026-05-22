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
- **Несколько типов серверов** — REST API, gRPC, AMQP RPC, NATS RPC
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
# Запуск зависимых сервисов (Postgres, RabbitMQ, NATS, Temporal)
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
- **Agent API (v1)**:
  - `POST /v1/agent/execute` — запуск одного агента (ReAct цикл)
  - `GET /v1/agent/status/{run_id}` — проверка статуса агента
  - `GET /v1/agent/{run_id}/messages` — просмотр сообщений
  - `GET /v1/agent/{run_id}/tools` — просмотр результатов инструментов
  - `GET /v1/agent/stream` — SSE поток вывода агента
  - `GET /v1/agent/ws` — WebSocket для связи в реальном времени
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
- **gRPC**: `tcp://127.0.0.1:8081`
- **AMQP RPC**: `amqp://guest:guest@127.0.0.1:5672/`
- **NATS RPC**: `nats://guest:guest@127.0.0.1:4222/`
- **PostgreSQL**: `postgres://user:myAwEsOm3pa55@w0rd@127.0.0.1:5432/db`

## Структура проекта

GoAgent использует структуру **библиотека + оболочка приложения** по паттерну [go-clean-template](https://github.com/evrone/go-clean-template). Публичные пакеты на верхнем уровне доступны для импорта внешними проектами; `internal/` содержит только оболочку приложения.

### Публичные пакеты библиотеки

| Пакет | Слой | Описание |
|---------|-------|-------------|
| `entity/` | Внутренний | Доменные примитивы — без зависимостей, только stdlib |
| `usecase/` | Внутренний | Бизнес-логика + интерфейсы **входных портов** (вызываются контроллерами) |
| `repo/` | Внутр./Внеш. | Интерфейсы **выходных портов** (вызываются usecase) + адаптеры инфраструктуры |
| `state/` | Внутренний | Абстракции управления состоянием — hot/warm/cold слои |
| `agentfw/` | Оба | Среда выполнения агентов — `agent/`, `team/`, `tool/`, `stream/` (внутренний); `orchestration/`, `runtime/`, `config/` (внешний) |
| `config/` | Внешний | Конфигурация приложения (на основе env) |
| `pkg/` | Внешний | Инфраструктурные обертки — Postgres, Redis, HTTP сервер и др. |

### Оболочка приложения

- `internal/app/` — внедрение зависимостей и инициализация приложения
- `internal/controller/` — транспортный слой (REST, gRPC, AMQP RPC, NATS RPC)
- `cmd/app/` — точка входа

### Прочие директории

- `docs/` — Swagger документация и Proto файлы
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
| [ReAct](examples/http/react/) | Один агент + цикл инструментов | `ExecuteRequest`, опрос |
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

### Примеры встраивания библиотеки (Режим 2 — Прямой импорт)

Директория `examples/embed/` показывает, как импортировать пакеты GoAgent напрямую:

| Пример | Файл | Что демонстрирует |
|---------|------|---------------|
| [ReAct](examples/embed/react/) | `examples/embed/react/main.go` | `agent.New()`, `ExecuteStep`, mock LLM |
| [Conversation](examples/embed/conversation/) | `examples/embed/conversation/main.go` | Многошаговый диалог с накоплением истории |
| [Tools](examples/embed/tools/) | `examples/embed/tools/main.go` | Вызов инструментов с `repo.ToolExecutor` |

### Только типы (Режим 3)

Директория `examples/types/` показывает импорт только `entity/` для общих определений типов.

## Три режима использования

GoAgent поддерживает три способа интеграции, от простого к глубокому:

### Режим 1 — Автономный сервер (REST API)

Запустите GoAgent как самостоятельный сервис. Приложение взаимодействует с ним через HTTP/gRPC.

```go
import "github.com/TekkenSteve/GoAgent/examples/client"

c := client.New("http://localhost:8080", "my-account")
status, _ := c.ExecuteAgent(ctx, client.ExecuteRequest{
    RunID: "run-1", UserMessage: "Сколько будет 2+2?",
})
```

### Режим 2 — Встраивание библиотеки

Импортируйте пакеты GoAgent напрямую в ваше Go-приложение. Используйте встроенные адаптеры или создайте свои.

```go
import (
    "github.com/TekkenSteve/GoAgent/usecase/agent"
    "github.com/TekkenSteve/GoAgent/repo/webapi"      // LLM провайдер
    "github.com/TekkenSteve/GoAgent/repo/pipeline"    // Redis WAL
    "github.com/TekkenSteve/GoAgent/repo/compressor"  // Сжатие контекста
    "github.com/TekkenSteve/GoAgent/repo/toolkit"     // Встроенные инструменты
)

llm := webapi.NewBifrostProvider(cfg)
tools := toolkit.NewRegistry(llm)
wal := pipeline.NewRedisWAL(appender, rdb)

agentUC := agent.New(llm, tools, wal, compressor, tools, nil)
result, _ := agentUC.ExecuteStep(ctx, &agent.StepRequest{
    RunID:   "run-1",
    Message: "Сколько будет 2+2?",
    Config:  entity.LLMConfig{Model: "claude-sonnet-4-20250514"},
})
```

### Режим 3 — Только типы

Импортируйте только `entity/` для обмена доменными типами между микросервисами.

```go
import "github.com/TekkenSteve/GoAgent/entity"

type MyService struct {
    messages []entity.Message
    tools    []entity.ToolDef
}
```

## Архитектурный дизайн

### Принципы Clean Architecture

Проект следует архитектурному паттерну [go-clean-template](https://github.com/evrone/go-clean-template):

1. **Входные порты** (`usecase/contracts.go`) — интерфейсы, реализуемые бизнес-логикой, вызываются контроллерами
2. **Выходные порты** (`repo/contracts.go`) — интерфейсы, вызываемые бизнес-логикой, реализуемые адаптерами
3. **Направление зависимостей**: внешние слои импортируют внутренние, никогда наоборот
4. **Тестируемость**: изоляция через интерфейсы упрощает юнит-тестирование с моками

### Поток зависимостей

```
┌──────────────────────────────────────────────┐
│  entity/   │  state/                         │  Внутренний слой
│  ──────────┼──────────                        │  (без внешних зависимостей,
│  usecase/contracts.go  (входные порты)       │   только stdlib)
│  repo/contracts.go     (выходные порты)      │
├──────────────────────────────────────────────┤
│  usecase/agent/  usecase/template/  ...     │  Внутренний слой
│  (импортирует repo/ для выходных портов)     │  (бизнес-логика)
├──────────────────────────────────────────────┤
│  repo/persistent/  repo/webapi/  ...         │  Внешний слой
│  internal/controller/  internal/app/         │  (инфраструктура,
│  agentfw/orchestration/                       │   импортирует внутренний)
└──────────────────────────────────────────────┘
```

- **Внутренний слой** (`entity/`, `state/`, `usecase/`, `repo/contracts.go`) зависит только от stdlib
- **Внешний слой** (`repo/*/`, `internal/`, `pkg/`) реализует интерфейсы внутреннего слоя
- `usecase/agent/` импортирует `repo/` для интерфейсов выходных портов — по аналогии с `usecase/translation/` → `repo/` в go-clean-template

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
- gRPC: `internal/controller/grpc/v1`, `v2`...
- RPC: `internal/controller/amqp_rpc/v1`, `v2`...

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

# Генерация кода gRPC
make proto-v1

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
