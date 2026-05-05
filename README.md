# Agent Framework

A Go microservices framework built on Clean Architecture principles, integrating Agent runtime and orchestration capabilities.

## Features

- Follows Clean Architecture design principles
- Supports multiple server types (REST API, gRPC, AMQP RPC, NATS RPC)
- Built-in Agent framework and runtime
- Complete observability support (logging, metrics, tracing)
- Database migration management
- Dependency injection design

## Technology Stack

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

## Quick Start

### Local Development

```sh
# Start dependency services (Postgres, RabbitMQ, NATS)
make compose-up

# Run the application (includes database migration)
make run
```

### Integration Tests

```sh
# Start full test environment
make compose-up-integration-test
```

### Full Docker Stack

```sh
make compose-up-all
```

### Service Endpoints

- REST API:
  - http://127.0.0.1:8080/healthz - Health check
  - http://127.0.0.1:8080/metrics - Prometheus metrics
  - http://127.0.0.1:8080/swagger - API documentation
- gRPC:
  - `tcp://127.0.0.1:8081`
- AMQP RPC:
  - URL: `amqp://guest:guest@127.0.0.1:5672/`
- NATS RPC:
  - URL: `nats://guest:guest@127.0.0.1:4222/`
- PostgreSQL:
  - `postgres://user:myAwEsOm3pa55@w0rd@127.0.0.1:5432/db`

## Project Structure

### Core Directories

- `cmd/app/` - Application entry point
- `config/` - Configuration management (environment variable based)
- `internal/` - Private application code
  - `app/` - Application initialization and dependency injection
  - `controller/` - Server handling layer (REST, gRPC, RPC)
  - `usecase/` - Business logic layer
  - `entity/` - Business entities
  - `repo/` - Data access layer
  - `agentfw/` - Agent framework implementation
- `pkg/` - Reusable public packages
- `docs/` - API documentation and Proto files
- `integration-test/` - Integration tests

### Configuration Management

Follows the [12-Factor App](https://12factor.net/) principles. All configuration is managed through environment variables.

Configuration file: [config/config.go](config/config.go)  
Example configuration: [.env.example](.env.example)

## Architecture Design

### Clean Architecture Principles

This project follows Robert Martin (Uncle Bob)'s Clean Architecture principles:

1. **Dependency Inversion**: Dependencies flow from outer layers to inner layers
2. **Independent Business Logic**: Core business logic does not depend on external frameworks and tools
3. **Testability**: Interface isolation enables easy unit testing
4. **Separation of Concerns**: Clear layer separation

### Layered Architecture

```
┌─────────────────────────────────────┐
│   Controller (HTTP/gRPC/RPC)        │  Outer Layer: Interface Adapters
├─────────────────────────────────────┤
│   Use Case (Business Logic)         │  Inner Layer: Business Logic
├─────────────────────────────────────┤
│   Repository / WebAPI               │  Outer Layer: Data Access
├─────────────────────────────────────┤
│   Database / External Services      │  Outer Layer: Infrastructure
└─────────────────────────────────────┘
```

**Inner Layer (Business Logic)**:
- Uses only Go standard library
- Does not depend on outer layer implementations
- Interacts with outer layers through interfaces

**Outer Layer (Infrastructure)**:
- Implements interfaces defined by the inner layer
- Handles specific technical implementations
- Components communicate through the business logic layer

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

This design enables:
- Independent testing of business logic
- Easy replacement of implementations
- Convenient generation of Mock objects

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
make swag

# Generate gRPC code
make proto

# Generate Mocks
make mock
```

### Code Quality

```sh
# Run linter
make lint

# Format code
make fmt
```

## References

- [Clean Architecture](https://blog.cleancoder.com/uncle-bob/2012/08/13/the-clean-architecture.html) - Robert Martin
- [The Twelve-Factor App](https://12factor.net/)
- [Go Project Layout](https://github.com/golang-standards/project-layout)

## License

MIT License - See [LICENSE](LICENSE) file for details
