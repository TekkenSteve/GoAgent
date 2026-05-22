# GoAgent

基于 Clean Architecture 原则构建的 Go 微服务框架，集成了 Agent 运行时和基于 Temporal 的编排引擎。

## 特性

- **Agent 框架** — 支持 LLM 集成的 ReAct 循环运行时、工具执行和 MCP 服务器
- **编排引擎** — 基于 Temporal 的工作流编排，支持多步骤、并行、条件和动态执行模式
- **多 Agent 团队** — 具有递归子团队扩展的分层团队组合
- **人在回路中** — 工作流暂停/恢复/取消和基于信号的等待步骤
- **流式输出** — 支持 SSE 和 WebSocket 的实时 Agent 输出
- **账户与计费** — 基于信用额度的使用跟踪和套餐分配
- **整洁架构** — 依赖反转、基于接口的隔离、可测试性
- **多服务器类型** — REST API、gRPC、AMQP RPC、NATS RPC
- **可观测性** — 结构化日志（zerolog）、Prometheus 指标、OpenTelemetry 追踪
- **数据库迁移** — 使用 golang-migrate 管理 PostgreSQL 架构

## 技术栈

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

## 快速开始

### 前提条件

- Go 1.26+
- Docker & Docker Compose
- Temporal Server（通过 Docker）

### 本地开发

```sh
# 启动依赖服务 (Postgres, RabbitMQ, NATS, Temporal)
make compose-up

# 运行应用（包含数据库迁移）
make run
```

### 集成测试

```sh
# 启动完整测试环境（含 mock LLM）
make compose-up-integration-test
```

### 完整 Docker 栈

```sh
make compose-up-all
```

## 服务端点

- **REST API**:
  - `http://127.0.0.1:8080/healthz` — 健康检查
  - `http://127.0.0.1:8080/metrics` — Prometheus 指标
  - `http://127.0.0.1:8080/swagger` — API 文档
- **Agent API (v1)**:
  - `POST /v1/agent/execute` — 执行单 Agent 运行（ReAct 循环）
  - `GET /v1/agent/status/{run_id}` — 轮询 Agent 运行状态
  - `GET /v1/agent/{run_id}/messages` — 查看对话消息
  - `GET /v1/agent/{run_id}/tools` — 查看工具执行结果
  - `GET /v1/agent/stream` — SSE 流式 Agent 输出
  - `GET /v1/agent/ws` — WebSocket 实时 Agent 通信
- **编排 API**:
  - `POST /v1/orchestration/execute` — 启动多步骤编排工作流
  - `GET /v1/orchestration/status/{run_id}` — 轮询编排状态
- **模板 API**:
  - `POST /v1/templates/import` — 从 YAML 导入工作流模板
  - `GET /v1/templates/` — 模板列表
  - `GET /v1/templates/{template_id}` — 模板详情
  - `DELETE /v1/templates/{template_id}` — 删除模板
- **触发器 API**:
  - `POST /v1/triggers/events` — 触发事件 webhook
- **gRPC**: `tcp://127.0.0.1:8081`
- **AMQP RPC**: `amqp://guest:guest@127.0.0.1:5672/`
- **NATS RPC**: `nats://guest:guest@127.0.0.1:4222/`
- **PostgreSQL**: `postgres://user:myAwEsOm3pa55@w0rd@127.0.0.1:5432/db`

## 项目结构

GoAgent 采用 **库 + 应用壳** 结构，遵循 [go-clean-template](https://github.com/evrone/go-clean-template) 模式。根级公共包可被外部项目导入；`internal/` 仅包含应用壳。

### 公共库包

| 包 | 层 | 说明 |
|---------|-------|-------------|
| `entity/` | 内层 | 领域原语——零依赖，仅标准库 |
| `usecase/` | 内层 | 业务逻辑 + **输入端口**接口（由 controller 调用） |
| `repo/` | 内层/外层 | **输出端口**接口（由 usecase 调用）+ 基础设施适配器 |
| `state/` | 内层 | 状态管理抽象——热/温/冷分层 |
| `agentfw/` | 双层 | Agent 运行时——`agent/`、`team/`、`tool/`、`stream/`（内层）；`orchestration/`、`runtime/`、`config/`（外层） |
| `config/` | 外层 | 应用配置（基于环境变量） |
| `pkg/` | 外层 | 基础设施包装——Postgres、Redis、HTTP 服务器等 |

### 应用壳

- `internal/app/` — 依赖注入与应用引导
- `internal/controller/` — 传输层（REST、gRPC、AMQP RPC、NATS RPC）
- `cmd/app/` — 入口点

### 其他目录

- `docs/` — Swagger 文档和 Proto 文件
- `examples/` — 可运行的模式示例
- `integration-test/` — 集成测试（需要 Docker）
- `migrations/` — PostgreSQL 迁移文件

### 配置管理

遵循 [12-Factor App](https://12factor.net/) 原则，所有配置通过环境变量管理。

配置文件：[config/config.go](config/config.go)  
示例配置：[.env.example](.env.example)

## Agent 框架

### 架构

Agent 框架由以下部分组成：

1. **Agent 运行时** — ReAct 循环：`思考 → 行动 → 观察 → 重复`，带有 LLM 提供者抽象
2. **工具系统** — 基于 JSON Schema 的工具定义、执行器抽象、MCP 服务器集成
3. **团队系统** — 分层团队组合，支持递归扩展为扁平步骤队列
4. **编排引擎** — Temporal 工作流，执行依赖解析、并行扇出、动态变更和人在回路中信号

### 步骤类型

| 类型 | 用途 |
|------|------|
| `agent` | 使用提示执行 Agent |
| `tool` | 直接执行工具 |
| `wait` | 等待 Temporal 信号（HITL）或超时 |
| `split` | 扇出到并行子步骤 |
| `join` | 扇入收集并行结果 |
| `eval` | 条件评估，支持动态步骤变更 |

### 编排模式

`examples/` 目录包含常见 Agent 模式的可运行演示：

| 模式 | 文件 | 关键概念 |
|---------|------|---------|
| [ReAct](examples/react/) | 单 Agent + 工具循环 | `ExecuteRequest`、轮询 |
| [Pipeline](examples/pipeline/) | 顺序处理阶段 | `depends_on` 链 |
| [DAG](examples/dag/) | 有向无环图 | 多依赖解析 |
| [Research](examples/research/) | 并行探索 + 综合 | `split`/`join`、`wait`（HITL） |
| [Supervisor-Worker](examples/supervisor-worker/) | 分解 + 并行工作 | `split`/`join`、监督 Agent |
| [Router](examples/router/) | 条件分支 | `eval` + `OnResult` 变更 |
| [Reflexion](examples/reflexion/) | 自我批判质量循环 | `eval` + 动态优化 |
| [Plan-and-Execute](examples/plan-and-execute/) | 计划 → 并行执行 → 评估 | `split`/`join` + `eval` 变更 |
| [Exploratory](examples/exploratory/) | 自我修改步骤队列 | `eval` + `append_after` 变更 |
| [ToT / LATS](examples/tot-lats/) | 多推理路径 | 并行探索 + 最优路径评估 |
| [Scientific](examples/scientific/) | 假设 → HITL → 实验 | `wait` 信号、超时处理 |
| [Team](examples/team/) | 多 Agent 层级 | `TeamSpec` + `SubTeams` |
| [Hierarchical](examples/hierarchical/) | 高管 → 部门 | 嵌套 `TeamSpec` + 扩展 |

每个示例都使用 [examples/client](examples/client/) SDK 与 REST API 交互。

## 三种使用方式

GoAgent 支持三种集成方式，从简单到深度集成：

### 方式一 — 独立服务（REST API）

将 GoAgent 作为独立服务运行，应用通过 HTTP/gRPC 与其交互。

```go
import "github.com/TekkenSteve/GoAgent/examples/client"

c := client.New("http://localhost:8080", "my-account")
status, _ := c.ExecuteAgent(ctx, client.ExecuteRequest{
    RunID: "run-1", UserMessage: "1+1 等于几？",
})
```

### 方式二 — 库嵌入

直接将 GoAgent 包导入你的 Go 应用。可自行提供基础设施适配器或使用内置实现。

```go
import (
    "github.com/TekkenSteve/GoAgent/usecase/agent"
    "github.com/TekkenSteve/GoAgent/repo/webapi"      // LLM 提供者
    "github.com/TekkenSteve/GoAgent/repo/pipeline"    // Redis WAL
    "github.com/TekkenSteve/GoAgent/repo/compressor"  // 上下文压缩
    "github.com/TekkenSteve/GoAgent/repo/toolkit"     // 内置工具
)

llm := webapi.NewBifrostProvider(cfg)
tools := toolkit.NewRegistry(llm)
wal := pipeline.NewRedisWAL(appender, rdb)

agentUC := agent.New(llm, tools, wal, compressor, tools, nil)
result, _ := agentUC.ExecuteStep(ctx, &agent.StepRequest{
    RunID:   "run-1",
    Message: "1+1 等于几？",
    Config:  entity.LLMConfig{Model: "claude-sonnet-4-20250514"},
})
```

### 方式三 — 仅使用类型

仅导入 `entity/` 跨微服务共享领域类型定义。

```go
import "github.com/TekkenSteve/GoAgent/entity"

type MyService struct {
    messages []entity.Message
    tools    []entity.ToolDef
}
```

## 架构设计

### Clean Architecture 原则

本项目遵循 [go-clean-template](https://github.com/evrone/go-clean-template) 架构模式：

1. **输入端口**（`usecase/contracts.go`）——业务逻辑实现的接口，由 controller 调用
2. **输出端口**（`repo/contracts.go`）——业务逻辑调用的接口，由基础设施适配器实现
3. **依赖方向**：外层导入内层，绝不反向
4. **可测试性**：接口隔离，便于使用 mock 进行单元测试

### 依赖流向

```
┌──────────────────────────────────────────────┐
│  entity/   │  state/                         │  内层
│  ──────────┼──────────                        │  （零外部依赖，
│  usecase/contracts.go  （输入端口）           │   仅标准库）
│  repo/contracts.go     （输出端口）           │
├──────────────────────────────────────────────┤
│  usecase/agent/  usecase/template/  ...     │  内层
│  （导入 repo/ 获取输出端口）                  │  （业务逻辑）
├──────────────────────────────────────────────┤
│  repo/persistent/  repo/webapi/  ...         │  外层
│  internal/controller/  internal/app/         │  （基础设施，
│  agentfw/orchestration/                       │   导入内层）
└──────────────────────────────────────────────┘
```

- **内层**（`entity/`、`state/`、`usecase/`、`repo/contracts.go`）仅依赖 Go 标准库
- **外层**（`repo/*/`、`internal/`、`pkg/`）实现内层定义的接口
- `usecase/agent/` 导入 `repo/` 获取输出端口接口——与 go-clean-template 中 `usecase/translation/` 导入 `repo/` 的模式一致

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
make swag-v1

# 生成 gRPC 代码
make proto-v1

# 生成 Mock
make mock
```

### 代码检查

```sh
# 运行 linter
make linter-golangci

# 格式化代码
make format

# 运行单元测试
make test
```

## CI 检查（推送前本地运行）

```sh
make linter-golangci               # golangci-lint
make linter-hadolint               # Dockerfile 检查
make linter-dotenv                  # .env 检查
make check-workflow-determinism    # Temporal 工作流确定性检查
make test                          # 单元测试
```

## 参考资料

- [Clean Architecture](https://blog.cleancoder.com/uncle-bob/2012/08/13/the-clean-architecture.html) — Robert Martin
- [The Twelve-Factor App](https://12factor.net/)
- [Temporal Workflow Platform](https://temporal.io/)
- [Go Project Layout](https://github.com/golang-standards/project-layout)

## 许可证

MIT License — 详见 [LICENSE](LICENSE) 文件
