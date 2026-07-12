# GoAgent

GoAgent 是 **AgentOS** 的参考实现：一个基于 Temporal 的 **Agent Control Plane** 与 **Durable Process Platform**。

它有两个公共使用面：

- **Agent Control Plane** — 启动、控制、观察和组合 backend-owned agent runs，可编排 native GoAgent、LangGraph、OpenCode 风格 runtime、HTTP backend、gRPC backend 和外部 Temporal workflow。
- **Durable Process Platform** — 用通用 resource、process、ledger、governed action、batch、projection 描述长生命周期智能工作。AiSOC 这类领域系统应该构建在这些通用原语上，而不是把自己的业务名词写进 AgentOS core。

GoAgent native agent framework 只是一个 backend 实现。Temporal 是 durable process kernel。`agentos/temporal` 是默认 adapter，负责把 AgentOS ports 接到 Temporal、Postgres、Redis 和 artifact storage。

## 特性

- **Agent Control Plane** — backend-owned run 生命周期、signal、control、durable RunPlan 编排、backend capability 和 event ingest
- **Durable Process Platform** — 通用 resource/process runtime、ledger、governed action、batch/workset 和 projection 接口
- **Temporal Kernel Adapter** — 显式 Temporal task queue 隔离、durable workflow、timer、signal、retry、cancel 和故障恢复
- **Native Agent Backend** — 支持 LLM 集成的 ReAct 循环运行时、工具执行、team composition 和 MCP 服务器
- **外部 Backend Adapter** — HTTP、gRPC 和 `temporal_external` backend，用于接入 GoAgent 外部拥有的 runtime
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
  - `GET /v1/agentos/plans/author` — 渲染 RunPlanSpec 编写控制台
  - `POST /v1/agentos/plans` — 启动跨 backend 的持久 RunPlan
  - `GET /v1/agentos/plans/{plan_id}/status` — 轮询 plan 聚合状态
  - `GET /v1/agentos/plans/{plan_id}/description` — 读取公开拓扑和状态
  - `GET /v1/agentos/plans/{plan_id}/console` — 渲染 RunPlan 运维控制台
  - `POST /v1/agentos/plans/{plan_id}/signals` — 发送 retry、approve、reject 等 plan 信号
  - `POST /v1/agentos/plans/{plan_id}/control` — 向 RunPlan 发送 pause、resume、cancel
  - `GET /v1/agentos/plans/{plan_id}/events` — 通过 SSE 订阅 RunPlan 事件
  - `GET /v1/agentos/plans/{plan_id}/events/history` — 查询持久 RunPlan 事件历史
  - `GET /v1/agentos/plans/{plan_id}/debug/traces` — 查询类型化 debug traces
  - `GET /v1/agentos/plans/{plan_id}/audits` — 查询持久 plan audit records
  - `GET /v1/agentos/plans/{plan_id}/artifacts` — 查询 plan artifact refs
  - `GET /v1/agentos/plans/{plan_id}/artifacts/{artifact_id}` — 读取单个 plan artifact 文档
- **模板 API**:
  - `POST /v1/templates/import` — 从 YAML 导入工作流模板
  - `GET /v1/templates/` — 模板列表
  - `GET /v1/templates/{template_id}` — 模板详情
  - `DELETE /v1/templates/{template_id}` — 删除模板
- **PostgreSQL**: `postgres://user:myAwEsOm3pa55@w0rd@127.0.0.1:5432/db`

## 项目结构

GoAgent 围绕小而稳定的 **AgentOS 边界**、adapter 和应用壳组织。实现包位于 `internal/`，不是外部项目的公共契约。

```text
Applications / reference distributions
  -> agentos/platform        # 应用需要同时使用两个平面时的组合门面
      -> agentos/control     # Agent Control Plane
      -> agentos/process     # Durable Process Platform
          -> agentos/core    # 共享 OS 原语

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

这个隔离是架构边界，不是目录装饰：

- `agentos/control` 不 import `agentos/process`；agent run 和 plan 不知道业务 process 语义。
- `agentos/process` 不 import `agentos/control`；durable process 可以独立于 agent execution 存在。
- `agentos/platform` 是需要同时使用两个平面的应用侧组合门面。
- `agentos/temporal` 只实现 public ports，不定义 AiSOC、DevOps、CodeAgent 等业务模型。
- `internal/agentfw` 是 native backend，不是所有 backend 都必须复制的公共架构。

### 公共库包

| 包 | 层 | 说明 |
|---------|-------|-------------|
| `agentos/core/` | 共享原语 | signal、control、event、artifact、message、tool、subscription 和公共错误 |
| `agentos/control/` | Agent Control Plane | Runtime、PlanRuntime、RunSpec、RunPlanSpec、PlanNodeSpec、capability、backend ref、plan schema |
| `agentos/process/` | Durable Process Platform | ResourceRef、process runtime、ledger runtime、governed action runtime、batch runtime、projection runtime |
| `agentos/platform/` | 组合门面 | 嵌入 control 与 process interfaces 的统一 runtime 接口 |
| `agentos/temporal/` | 默认 adapter | Temporal/Postgres/Redis/artifact-store 实现和 worker 注册工具 |
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

## Agent Control Plane

Agent Control Plane 协调 backend-owned agent runs。backend 可以是 native GoAgent backend、LangGraph 服务、OpenCode 风格 runtime、HTTP 服务、gRPC 服务，或外部 Temporal workflow。

公共模型刻意保持粗粒度：

- `control.RunSpec` 启动一个 backend-owned run。
- `control.RunStatus` 是这个 run 的公共生命周期视图。
- `control.RunPlanSpec` 组合多个 backend-owned runs。
- `control.PlanNodeSpec` 是 run 级编排节点。它不是 native GoAgent step、Temporal activity、LangGraph graph node、OpenCode step，也不是 tool call。
- `control.CapabilityRunBatch` 表示一个 backend-owned batch run。AgentOS 校验粗粒度 limit 并观察进度，不会把 batch items 展开成成千上万个 plan nodes。

backend 内部的 graph、loop、step、tool、record 级执行细节留在拥有它的 backend 或 data plane 里。它们可以作为 event、artifact、ledger record 或 projection 回写给 AgentOS。

## Durable Process Platform

Durable Process Platform 为长生命周期智能工作提供通用构件：

- `process.ResourceRef` 标识 case、ticket、order、incident、alert、pull request、change 等领域资源，但这些业务名词不会进入 AgentOS core。
- `process.Runtime` 管理 durable process 生命周期。
- `process.LedgerRuntime` 记录 decision、evidence ref、action ref、prompt/response ref、artifact ref、actor、timestamp 和 rationale。
- `process.GovernedActionRuntime` 描述 dry-run、risk evaluation、approval、execution、cancel 和 compensation。
- `process.BatchRuntime` 描述 workset 和有界 batch progress。
- `process.ProjectionRuntime` 从 durable projection 为 REST、MCP、UI 和 operator read model 服务，不依赖高频 Temporal Workflow Query。

Temporal 存储确定性的 process control、timer、signal、retry 和紧凑引用。大型 prompt、response、evidence blob、search index、lake data、graph data 和 artifact payload 留在外部存储。

## Native Agent Backend

GoAgent native backend 是 control plane 后面的一个实现：

1. **Agent 运行时** — 带 LLM provider 抽象的 ReAct 循环
2. **工具系统** — 基于 JSON Schema 的工具定义、执行器抽象、MCP 服务器集成
3. **团队系统** — native backend 内部的分层团队组合
4. **Temporal Worker Kit** — 为 durable native execution 注册 workflow/activity

native step 队列、team expansion、LLM call、tool call 和 backend 内部 graph 逻辑都是实现细节。它们不是外部 backend 必须复制的模型。

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

### 方式二 — Go 库嵌入

将稳定的 AgentOS 边界导入你的 Go 应用。默认 Temporal/Postgres/Redis 实现位于 `agentos/temporal`。

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

1. **公共 ports**（`agentos/core`、`agentos/control`、`agentos/process`、`agentos/platform`）定义稳定的应用侧契约。
2. **Adapters**（`agentos/temporal`、`internal/repo/*`、`internal/controller/*`）为 Temporal、存储、backend runtime 和 transport 实现 ports。
3. **Use cases** 通过接口协调应用行为，而不是依赖具体基础设施。
4. **依赖方向**保持明确：领域契约不 import adapter，公共包不 import `internal`。
5. **可测试性**来自小接口、确定性的 workflow input 和边界测试。

### 依赖流向

```text
┌────────────────────────────────────────────────────────────┐
│ Applications / reference distributions                     │
│  - AiSOC, CodeAgent, DevOps, CustomerOps                   │
│  - import agentos/platform 或具体 public packages           │
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

- **Public ports** 是支持应用直接依赖的契约。
- **Temporal adapter** 是这些契约的默认实现，不拥有业务词汇。
- **Application shell** 负责组装 service、HTTP API、persistence、worker registration 和 native backend。
- **Reference distributions** 应放在 `examples/` 或下游仓库；它们使用 AgentOS primitives，但不改变 AgentOS core types。

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
