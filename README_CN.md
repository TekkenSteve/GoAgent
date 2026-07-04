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
# 启动依赖服务 (Postgres, Redis, Temporal)
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
- **AgentOS API (v1)**:
  - `POST /v1/agentos/runs` — 在指定 backend 上启动 run
  - `GET /v1/agentos/runs/{run_id}/status` — 轮询 run 状态
  - `POST /v1/agentos/runs/{run_id}/signals` — 发送 `user.message` 等业务输入
  - `POST /v1/agentos/runs/{run_id}/control` — 发送 pause、resume、cancel
  - `POST /v1/agentos/runs/{run_id}/events` — 接收 backend 事件回写
  - `GET /v1/agentos/plans/schemas/{kind}` — 读取 RunPlan 编写用 JSON Schema
  - `POST /v1/agentos/plans` — 启动跨 backend 的持久 RunPlan
  - `GET /v1/agentos/plans/{plan_id}/status` — 轮询 plan 聚合状态
  - `POST /v1/agentos/plans/{plan_id}/signals` — 发送 retry、approve、reject 等 plan 信号
  - `POST /v1/agentos/plans/{plan_id}/control` — 向 RunPlan 发送 pause、resume、cancel
  - `GET /v1/agentos/plans/{plan_id}/events` — 通过 SSE 订阅 RunPlan 事件
- **模板 API**:
  - `POST /v1/templates/import` — 从 YAML 导入工作流模板
  - `GET /v1/templates/` — 模板列表
  - `GET /v1/templates/{template_id}` — 模板详情
  - `DELETE /v1/templates/{template_id}` — 删除模板
- **触发器 API**:
  - `POST /v1/triggers/events` — 触发事件 webhook
- **PostgreSQL**: `postgres://user:myAwEsOm3pa55@w0rd@127.0.0.1:5432/db`

## 项目结构

GoAgent 围绕小而稳定的 **AgentOS SDK 边界** 和应用壳组织。实现包位于 `internal/`，不是外部项目的公共契约。

### 公共库包

| 包 | 层 | 说明 |
|---------|-------|-------------|
| `agentos/core/` | 公共 SDK 核心 | 共享 signal、control、event、artifact、message、tool、subscription 和公共错误 |
| `agentos/control/` | Agent 控制面 | RunRuntime、PlanRuntime、run spec、RunPlan spec、capability、backend ref、plan schema |
| `agentos/process/` | Durable process 平台 | ResourceRef、Runtime、LedgerRuntime、GovernedActionRuntime、BatchRuntime、ProjectionRuntime、workset |
| `agentos/platform/` | 公共门面 | 组合 control 与 process 的应用侧 platform runtime 接口 |
| `agentos/temporal/` | 公共适配器 | 默认 Temporal/Postgres/Redis 实现和 worker 注册工具 |
| `config/` | 外层 | 应用配置（基于环境变量） |
| `pkg/` | 通用工具 | 不作为 GoAgent 实现契约的基础设施包装 |

### 应用壳

- `internal/app/` — 依赖注入与应用引导
- `internal/agentfw/` — Agent 工作流和运行时实现
- `internal/entity/`、`internal/usecase/`、`internal/repo/`、`internal/state/` — 内部领域和基础设施实现
- `internal/controller/` — 传输层（REST AgentOS 控制面）
- `cmd/app/` — 入口点

### 其他目录

- `docs/` — Swagger 文档
- `examples/` — 可运行的模式示例
- `integration-test/` — 集成测试（需要 Docker）
- `migrations/` — PostgreSQL 迁移文件

### 配置管理

遵循 [12-Factor App](https://12factor.net/) 原则，所有配置通过环境变量管理。

配置文件：[config/config.go](config/config.go)  
示例配置：[.env.example](.env.example)

## Agent 框架

### 架构

GoAgent native backend 由以下部分组成：

1. **Agent 运行时** — ReAct 循环：`思考 → 行动 → 观察 → 重复`，带有 LLM 提供者抽象
2. **工具系统** — 基于 JSON Schema 的工具定义、执行器抽象、MCP 服务器集成
3. **团队系统** — native backend 内部的分层团队组合
4. **Temporal Worker Kit** — 为 durable native execution 注册 workflow/activity

native step 队列、team expansion 和 backend 内部 graph 逻辑都是实现细节。外部控制面调用方应该使用 AgentOS `RunSpec` 和 `RunPlanSpec` 描述跨框架编排，而不是 native step payload。

`RunPlan` 用来协调 backend-owned child runs。`PlanNodeSpec` 是一个完整的 `control.RunSpec` 加 backend、capability、input、output、condition、policy 契约；它不是 native GoAgent step、Temporal activity，也不是 LangGraph node。

Capability 是粗粒度 backend 契约。`control.CapabilityRun` 启动一个 backend-owned run；`control.CapabilityRunBatch` 启动一个 backend-owned batch run，并通过 capability limits 校验有界 batch input。AgentOS 不会把 batch item 展开成大量 plan node。

Durable process 层提供 `process.ProjectionRuntime` 给 REST、MCP、UI 和运维读模型使用。Projection 从 durable process、ledger、governed action、workset store 读取，不依赖高频 Temporal Workflow Query。

### REST 示例

`examples/http/` 目录包含 AgentOS REST 示例：

| 示例 | 文件 | 展示内容 |
|---------|------|---------------|
| [RunPlan](examples/http/runplan/) | `examples/http/runplan/main.go` | 使用公共 `agentos/control` 类型通过 REST 启动 durable AgentOS RunPlan |

### 库嵌入示例（方式二 — AgentOS Runtime）

`examples/embed/` 目录展示如何通过公共 AgentOS 边界嵌入 GoAgent：

| 示例 | 文件 | 展示内容 |
|---------|------|---------------|
| [ReAct](examples/embed/react/) | `examples/embed/react/main.go` | 使用 `control.Runtime` 启动通用运行 |
| [对话](examples/embed/conversation/) | `examples/embed/conversation/main.go` | 通过 `agentos/temporal` 启动对话运行 |
| [工具](examples/embed/tools/) | `examples/embed/tools/main.go` | 通过运行时边界启动可使用工具的提示 |
| [RunPlan](examples/embed/plan/) | `examples/embed/plan/main.go` | 使用 `control.PlanRuntime` 启动 durable 跨 backend plan |

### 仅使用类型（方式三）

`examples/types/` 目录展示仅导入 `agentos/core` 和 `agentos/control` 来共享公共类型定义。

## 三种使用方式

GoAgent 支持三种集成方式，从简单到深度集成：

### 方式一 — 独立服务（REST API）

将 GoAgent 作为独立服务运行，应用通过 AgentOS REST 控制面与其交互。

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
    UserMessage: "1+1 等于几？",
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

### 方式二 — 库嵌入

将稳定的 AgentOS 控制面边界导入你的 Go 应用。默认 Temporal/Postgres/Redis 实现位于 `agentos/temporal`。

```go
import (
    agentos "github.com/TekkenSteve/GoAgent/agentos/control"
    agentostemporal "github.com/TekkenSteve/GoAgent/agentos/temporal"
)

rt, _ := agentostemporal.NewRuntime(ctx, agentostemporal.RuntimeConfig{
    TemporalAddress: "127.0.0.1:7233",
    TemporalNamespace: "default",
    TemporalTaskQueue: "agent-framework",
    PostgresURL: "postgres://goagent:goagent@127.0.0.1:5432/goagent?sslmode=disable",
    RedisURL: "redis://127.0.0.1:6379/0",
})
status, _ := rt.Start(ctx, agentos.RunSpec{
    RunID: "run-1",
    AccountID: "acct-1",
    ProjectID: "proj-1",
    ModelRef: "gpt-4.1-mini",
    UserMessage: "1+1 等于几？",
    IdempotencyKey: "run-1-start",
})
```

### 方式三 — 仅使用类型

仅导入服务需要的公共 AgentOS 包。`agentos/core` 放共享 message/tool/event，`agentos/control` 放 run/plan 契约。

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

## 架构设计

### Clean Architecture 原则

本项目遵循 [go-clean-template](https://github.com/evrone/go-clean-template) 架构模式：

1. **公共 SDK 边界**（`agentos/core`、`agentos/control`、`agentos/process`、`agentos/platform`）——面向外部嵌入调用方的稳定契约
2. **内部端口**（`internal/usecase/contracts.go`、`internal/repo/contracts.go`）——对下游项目隐藏的实现契约
3. **依赖方向**：外层导入内层，绝不反向
4. **可测试性**：接口隔离，便于使用 mock 进行单元测试

### 依赖流向

```
┌──────────────────────────────────────────────┐
│  agentos/core/      agentos/control/          │  公共 SDK
│  agentos/process/   agentos/platform/         │
│  agentos/temporal/                            │  默认适配器
├──────────────────────────────────────────────┤
│  internal/entity/ │  internal/state/         │  内层
│  ──────────┼──────────                        │  （零外部依赖，
│  internal/usecase/contracts.go                │   仅标准库）
│  internal/repo/contracts.go                   │
├──────────────────────────────────────────────┤
│  internal/usecase/agent/  ...                │  内层
│  （导入 internal/repo 获取输出端口）          │  （业务逻辑）
├──────────────────────────────────────────────┤
│  internal/repo/persistent/  ...              │  外层
│  internal/controller/  internal/app/         │  （基础设施，
│  internal/agentfw/orchestration/              │   导入内层）
└──────────────────────────────────────────────┘
```

- **公共层**（`agentos/core`、`agentos/control`、`agentos/process`、`agentos/platform`、`agentos/temporal`）是唯一支持的嵌入式导入契约
- **内层**（`internal/entity/`、`internal/state/`、`internal/usecase/`、`internal/repo/contracts.go`）仅作为实现细节
- **外层**（`internal/repo/*/`、`internal/controller/`、`internal/app/`、`pkg/`）实现内层定义的接口

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
