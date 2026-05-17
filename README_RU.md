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

### Основные директории

- `cmd/app/` — точка входа приложения
- `config/` — управление конфигурацией (на основе переменных окружения)
- `internal/` — приватный код приложения
  - `app/` — инициализация приложения и внедрение зависимостей
  - `controller/` — слой обработчиков сервера (REST, gRPC, RPC)
  - `usecase/` — слой бизнес-логики
  - `entity/` — бизнес-сущности (Step, AgentSpec, TeamSpec и др.)
  - `repo/` — слой доступа к данным (Temporal, PostgreSQL)
  - `agentfw/` — реализация Agent Framework (среда выполнения, команды, стриминг)
- `pkg/` — переиспользуемые публичные пакеты
- `docs/` — документация API и Proto файлы
- `examples/` — исполняемые примеры паттернов (клиентский SDK)
- `integration-test/` — интеграционные тесты
- `scripts/` — проверки детерминизма, нагрузочные тесты
- `migrations/` — файлы миграций PostgreSQL

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

## Архитектурный дизайн

### Принципы Clean Architecture

Проект следует принципам Clean Architecture Роберта Мартина (Uncle Bob):

1. **Инверсия зависимостей**: зависимости направлены от внешнего слоя к внутреннему
2. **Независимость бизнес-логики**: ядро бизнес-логики не зависит от внешних фреймворков и инструментов
3. **Тестируемость**: изоляция через интерфейсы облегчает юнит-тестирование
4. **Разделение ответственности**: четкое разделение по слоям

### Слоистая архитектура

```
┌─────────────────────────────────────┐
│   Controller (HTTP/gRPC/RPC)        │  Внешний слой: адаптеры интерфейсов
├─────────────────────────────────────┤
│   Use Case (Business Logic)         │  Внутренний слой: бизнес-логика
├─────────────────────────────────────┤
│   Repository / WebAPI               │  Внешний слой: доступ к данным
├─────────────────────────────────────┤
│   Database / External Services      │  Внешний слой: инфраструктура
└─────────────────────────────────────┘
```

**Внутренний слой (бизнес-логика)**:
- Использует только стандартную библиотеку Go
- Не зависит от реализации внешнего слоя
- Взаимодействует с внешним слоем через интерфейсы

**Внешний слой (инфраструктура)**:
- Реализует интерфейсы, определенные внутренним слоем
- Обрабатывает конкретные технические реализации
- Компоненты взаимодействуют через слой бизнес-логики

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

## Справочные материалы

- [Clean Architecture](https://blog.cleancoder.com/uncle-bob/2012/08/13/the-clean-architecture.html) — Robert Martin
- [The Twelve-Factor App](https://12factor.net/)
- [Temporal Workflow Platform](https://temporal.io/)
- [Go Project Layout](https://github.com/golang-standards/project-layout)

## Лицензия

MIT License — см. файл [LICENSE](LICENSE)
