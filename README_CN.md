# Agent Framework

基于 Clean Architecture 原则构建的 Go 微服务框架，集成了 Agent 运行时和编排能力。

## 特性

- 遵循 Clean Architecture 设计原则
- 支持多种服务器类型（REST API、gRPC、AMQP RPC、NATS RPC）
- 内置 Agent 框架和运行时
- 完整的可观测性支持（日志、指标、追踪）
- 数据库迁移管理
- 依赖注入设计

## 技术栈

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

## 快速开始

### 本地开发

```sh
# 启动依赖服务 (Postgres, RabbitMQ, NATS)
make compose-up

# 运行应用（包含数据库迁移）
make run
```

### 集成测试

```sh
# 启动完整测试环境
make compose-up-integration-test
```

### 完整 Docker 栈

```sh
make compose-up-all 
```

### 服务端点

- REST API:
  - http://127.0.0.1:8080/healthz - 健康检查
  - http://127.0.0.1:8080/metrics - Prometheus 指标
  - http://127.0.0.1:8080/swagger - API 文档
- gRPC:
  - `tcp://127.0.0.1:8081`
- AMQP RPC:
  - URL: `amqp://guest:guest@127.0.0.1:5672/`
- NATS RPC:
  - URL: `nats://guest:guest@127.0.0.1:4222/`
- PostgreSQL:
  - `postgres://user:myAwEsOm3pa55@w0rd@127.0.0.1:5432/db`

## 项目结构

### 核心目录

- `cmd/app/` - 应用入口点
- `config/` - 配置管理（基于环境变量）
- `internal/` - 私有应用代码
  - `app/` - 应用初始化和依赖注入
  - `controller/` - 服务器处理层（REST、gRPC、RPC）
  - `usecase/` - 业务逻辑层
  - `entity/` - 业务实体
  - `repo/` - 数据访问层
  - `agentfw/` - Agent 框架实现
- `pkg/` - 可复用的公共包
- `docs/` - API 文档和 Proto 文件
- `integration-test/` - 集成测试

### 配置管理

遵循 [12-Factor App](https://12factor.net/) 原则，所有配置通过环境变量管理。

配置文件：[config/config.go](config/config.go)  
示例配置：[.env.example](.env.example)

## 架构设计

### Clean Architecture 原则

本项目遵循 Robert Martin (Uncle Bob) 的 Clean Architecture 原则：

1. **依赖倒置**：依赖方向从外层指向内层
2. **业务逻辑独立**：核心业务逻辑不依赖外部框架和工具
3. **可测试性**：通过接口隔离，便于单元测试
4. **关注点分离**：清晰的层次划分

### 分层架构

```
┌─────────────────────────────────────┐
│   Controller (HTTP/gRPC/RPC)        │  外层：接口适配
├─────────────────────────────────────┤
│   Use Case (Business Logic)         │  内层：业务逻辑
├─────────────────────────────────────┤
│   Repository / WebAPI               │  外层：数据访问
├─────────────────────────────────────┤
│   Database / External Services      │  外层：基础设施
└─────────────────────────────────────┘
```

**内层（业务逻辑）**：
- 只使用 Go 标准库
- 不依赖外层实现
- 通过接口与外层交互

**外层（基础设施）**：
- 实现内层定义的接口
- 处理具体的技术实现
- 组件间通过业务逻辑层通信

### 依赖注入

通过构造函数注入依赖，保持业务逻辑的独立性和可测试性：

```go
type UseCase struct {
    repo Repository  // 接口依赖
}

func New(r Repository) *UseCase {
    return &UseCase{repo: r}
}
```

这种设计使得：
- 业务逻辑可独立测试
- 实现可轻松替换
- 便于生成 Mock 对象

### API 版本管理

支持简单的版本管理策略，通过目录结构区分版本：

- REST API: `internal/controller/restapi/v1`, `v2`...
- gRPC: `internal/controller/grpc/v1`, `v2`...
- RPC: `internal/controller/amqp_rpc/v1`, `v2`...

## 开发指南

### 数据库迁移

```sh
# 运行迁移
go run -tags migrate ./cmd/app

# 或使用 make
make run
```

### 生成代码

```sh
# 生成 Swagger 文档
make swag

# 生成 gRPC 代码
make proto

# 生成 Mock
make mock
```

### 代码检查

```sh
# 运行 linter
make lint

# 格式化代码
make fmt
```

## 参考资料

- [Clean Architecture](https://blog.cleancoder.com/uncle-bob/2012/08/13/the-clean-architecture.html) - Robert Martin
- [The Twelve-Factor App](https://12factor.net/)
- [Go Project Layout](https://github.com/golang-standards/project-layout)

## 许可证

MIT License - 详见 [LICENSE](LICENSE) 文件
