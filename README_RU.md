# Agent Framework

Фреймворк для микросервисов на Go, построенный на принципах Clean Architecture с интегрированной средой выполнения и оркестрацией агентов.

## Возможности

- Следование принципам Clean Architecture
- Поддержка нескольких типов серверов (REST API, gRPC, AMQP RPC, NATS RPC)
- Встроенный фреймворк агентов и среда выполнения
- Полная поддержка наблюдаемости (логирование, метрики, трассировка)
- Управление миграциями базы данных
- Внедрение зависимостей

## Технологический стек

[![Web Framework](https://img.shields.io/badge/Fiber-Web%20Framework-blue)](https://github.com/gofiber/fiber)
[![API Documentation](https://img.shields.io/badge/Swagger-API%20Documentation-blue)](https://github.com/swaggo/swag)
[![Validation](https://img.shields.io/badge/Validator-Data%20Integrity-blue)](https://github.com/go-playground/validator)
[![JSON Handling](https://img.shields.io/badge/Go--JSON-Fast%20Serialization-blue)](https://github.com/goccy/go-json)
[![Query Builder](https://img.shields.io/badge/Squirrel-SQL%20Query%20Builder-blue)](https://github.com/Masterminds/squirrel)
[![Database Migrations](https://img.shields.io/badge/Migrations-Seamless%20Schema%20Updates-blue)](https://github.com/golang-migrate/migrate)
[![Logging](https://img.shields.io/badge/ZeroLog-Structured%20Logging-blue)](https://github.com/rs/zerolog)
[![Metrics](https://img.shields.io/badge/Prometheus-Metrics%20Integration-blue)](https://github.com/ansrivas/fiberprometheus)
[![Testing](https://img.shields.io/badge/Testify-Testing%20Framework-blue)](https://github.com/stretchr/testify)
[![Mocking](https://img.shields.io/badge/Mock-Mocking%20Library-blue)](https://go.uber.org/mock)

## Быстрый старт

### Локальная разработка

```sh
# Запуск зависимых сервисов (Postgres, RabbitMQ, NATS)
make compose-up

# Запуск приложения (с миграциями базы данных)
make run
```

### Интеграционные тесты

```sh
# Запуск полного тестового окружения
make compose-up-integration-test
```

### Полный Docker стек

```sh
make compose-up-all 
```

### Эндпоинты сервисов

- REST API:
  - http://127.0.0.1:8080/healthz - проверка здоровья
  - http://127.0.0.1:8080/metrics - метрики Prometheus
  - http://127.0.0.1:8080/swagger - документация API
- gRPC:
  - `tcp://127.0.0.1:8081`
- AMQP RPC:
  - URL: `amqp://guest:guest@127.0.0.1:5672/`
- NATS RPC:
  - URL: `nats://guest:guest@127.0.0.1:4222/`
- PostgreSQL:
  - `postgres://user:myAwEsOm3pa55@w0rd@127.0.0.1:5432/db`

## Структура проекта

### Основные директории

- `cmd/app/` - точка входа приложения
- `config/` - управление конфигурацией (на основе переменных окружения)
- `internal/` - приватный код приложения
  - `app/` - инициализация приложения и внедрение зависимостей
  - `controller/` - слой обработчиков сервера (REST, gRPC, RPC)
  - `usecase/` - слой бизнес-логики
  - `entity/` - бизнес-сущности
  - `repo/` - слой доступа к данным
  - `agentfw/` - реализация фреймворка агентов
- `pkg/` - переиспользуемые публичные пакеты
- `docs/` - документация API и Proto файлы
- `integration-test/` - интеграционные тесты

### Управление конфигурацией

Следуя принципам [12-Factor App](https://12factor.net/), вся конфигурация управляется через переменные окружения.

Файл конфигурации: [config/config.go](config/config.go)  
Пример конфигурации: [.env.example](.env.example)

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

Такой дизайн обеспечивает:
- Независимое тестирование бизнес-логики
- Легкую замену реализаций
- Простую генерацию Mock объектов

### Управление версиями API

Поддержка простой стратегии версионирования через структуру директорий:

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
make swag

# Генерация кода gRPC
make proto

# Генерация Mock
make mock
```

### Проверка кода

```sh
# Запуск линтера
make lint

# Форматирование кода
make fmt
```

## Справочные материалы

- [Clean Architecture](https://blog.cleancoder.com/uncle-bob/2012/08/13/the-clean-architecture.html) - Robert Martin
- [The Twelve-Factor App](https://12factor.net/)
- [Go Project Layout](https://github.com/golang-standards/project-layout)

## Лицензия

MIT License - см. файл [LICENSE](LICENSE)
