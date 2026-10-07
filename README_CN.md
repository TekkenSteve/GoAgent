# GoAgent

> **GoAgent** 是一个开源 **AgentOS**：为内嵌 AI 的产品提供 Agent 执行层。产品团队只做产品——领域、交互、闭环；GoAgent 运行它的 Agent 那一半：耐久 run 与 plan、工具与 MCP 执行、流式输出、artifacts、审计、计费、多租户，全部收在一个控制平面后面。

[![Web Framework](https://img.shields.io/badge/Fiber-Web%20Framework-blue)](https://github.com/gofiber/fiber)
[![Workflow Engine](https://img.shields.io/badge/Temporal-Workflow%20Engine-blue)](https://temporal.io/)
[![Event Backbone](https://img.shields.io/badge/NATS%20JetStream-Event%20Backbone-blue)](https://nats.io/)
[![SQL Compiler](https://img.shields.io/badge/sqlc-Type--Safe%20SQL-blue)](https://sqlc.dev/)
[![Database Migrations](https://img.shields.io/badge/golang--migrate-Schema%20Updates-blue)](https://github.com/golang-migrate/migrate)
[![Logging](https://img.shields.io/badge/ZeroLog-Structured%20Logging-blue)](https://github.com/rs/zerolog)
[![Metrics](https://img.shields.io/badge/Prometheus-Metrics%20Integration-blue)](https://github.com/ansrivas/fiberprometheus)

## 问题

模型是产品的一层，不是产品本身。模型升级让这一层免费变强——而它周围的一切原封不动。一个核心环节包含长时 AI 工作的产品，仍然必须用工程回答：进程跑到一半挂了怎么办、跨三天的对话怎么活过重启、那次工具调用是谁批准的、Agent 究竟对我的数据做了什么、花了多少钱。

每个 AI 产品团队都在手搓同一套不性感的机器来回答这些问题——run 生命周期、抗宕机的任务队列、工具分发、进度流式、artifact 存储、审计轨迹、用量计量。这套机器对产品一无所知：一个抽认卡工坊和一个安全运营平台需要*相同*的那一半，不同的只是各自的名词。

五个分布式系统问题，每一个都被不确定性推理放大：

| 生产问题 | Agent 带来的放大 | 系统必须承担的 |
|---|---|---|
| 长时运行 | 对话、审批、观察周期跨天跨月 | 耐久等待、定时器、宕机恢复 |
| 外部副作用 | 模型*一定会*调你的付款和通知工具 | 幂等、重试、补偿、审批门 |
| 不确定决策 | 同一目标，多条推理路径 | 策略门、预算、决策留痕 |
| 多主体协作 | 团队、Agent、人同时在场 | 契约、隔离、授权 |
| 持续演进 | prompt、工具 schema、策略漂移 | 版本化 schema、可控迁移 |

GoAgent 就是这套机器的产品化。LLM 负责不确定的认知步骤；耐久工作流负责围绕它的确定的生命周期与治理。

## GoAgent 是什么

```mermaid
graph TB
    subgraph PRODUCTS["你的产品 —— 任何领域"]
        direction LR
        P1["抽认卡工坊<br/>（对话驱动的卡片制作）"]
        P2["安全运营平台"]
        P3["开发工具 · 游戏 · 任何<br/>有长时 AI 工作的东西"]
    end

    subgraph AGENTOS["GoAgent / AgentOS"]
        CP["Agent 控制平面<br/>runs · plans · signals · capabilities"]
        PP["耐久流程平台<br/>resources · ledger · governed actions · worksets"]
        NX["Nexus API<br/>跨服务的耐久操作"]
    end

    subgraph STATE["耐久性与事实"]
        T["Temporal<br/>状态 · 定时器 · 重试 · 恢复"]
        N["NATS JetStream<br/>事务性 outbox → 投影 · 镜像"]
        P[("Postgres<br/>账本 · 哈希链审计 · 投影")]
    end

    subgraph BACKENDS["Agent 后端"]
        NA["原生 GoAgent ReAct<br/>+ MCP 工具"]
        LG["LangGraph · OpenCode 式<br/>运行时"]
        EXT["HTTP · gRPC ·<br/>外部 Temporal"]
    end

    PRODUCTS -->|"引擎中立契约<br/>agentos/core · control · process"| AGENTOS
    CP --> BACKENDS
    PP --> STATE
    NX --> T
```

三个决定撑起整个设计：

- **你的名词归你。** 领域工作以泛型 `ResourceRef` 进入——对 AgentOS 来说，一个卡包和一个安全事件是同一种原语。它永远不学你的领域模型，你的产品也永远不碰它的内部：公共契约（`agentos/core`、`agentos/control`、`agentos/process`）不 import 任何执行引擎，由 `make check-import-boundary` 锁死。
- **控制平面管编排，后端管执行。** GoAgent 启动、发信号、控制和观察*后端持有*的 run，并把它们编排成跨后端的耐久 plan。后端可以是原生 ReAct 循环、一个 LangGraph 服务、一个 OpenCode 式运行时，或任何 HTTP/gRPC/Temporal 服务——它内部的图、步骤、工具都留在它那里。
- **大载荷永不进入工作流历史。** prompt、工具 I/O、证据存在外部存储，以 claim-check 引用进入；工作流只保留状态、命令和引用。已提交的事实从事务性 outbox 流向 JetStream，供给投影、镜像和分析；REST、MCP、控制台读的是 Postgres 读模型，从不打高频 workflow query。

## 实际案例

[Kardcraft](https://github.com/TekkenSteve/Kardcraft)——一个面向间隔重复的对话式抽认卡制作产品——运行在这个设计上：它的 LangGraph 卡片生产图是一个 AgentOS 后端；它的 Go 任务编排器是 `agentos.PlanRuntime` 的下游消费者。边界有多干净，一句话可以度量：编排器的 AgentOS 适配器按其自身契约，是*唯一知道 AgentOS 类型的 Kardcraft 包*。

## 保证清单

平台评审者真正会逐条核对的东西：

- **耐久** —— run 和 plan 扛得住宕机、发版和跨天等待；Nexus 上的 `run.status` 是有界陈旧度快照（≈5s），从不阻塞调用后端。
- **后端无关** —— 原生、LangGraph、OpenCode 式、HTTP、gRPC、外部 Temporal 的 run 在同一契约后面；批量条目由 capability 上界约束，不会爆成几千个 plan 节点。
- **受治理** —— 高危工作走 dry-run、风险评估、审批、执行、取消、补偿（`GovernedActionRuntime`）。
- **可审计** —— 决策、证据引用、执行者、理由进入账本；审计日志哈希链化、防篡改。
- **可计费** —— credit 账户与使用台账对每个 run 计量。
- **多租户** —— account/project 维度端到端强制，包括 schema 里的行级租户键。
- **引擎中立** —— 公共契约零 Temporal import；Temporal 只存在于 `agentos/temporal` 适配器里，可以整体更换而不动任何消费者。
- **互操作边缘** —— `cmd/agentos-plan` 校验 plan、产出 JSON Schema 创作契约，并以 [Serverless Workflow](https://serverlessworkflow.io/) 作为交换格式导入导出。

## 定位

Agent 技术栈的每一层都在风口上。它们解决的是不同的问题：

| 类别 | 例子 | 服务单位 | 优化目标 |
|---|---|---|---|
| 陪伴型 Agent | OpenClaw（🦞 龙虾）、Hermes（爱马仕）、Muse、Cue、Grokbot | 一个人的注意力 | 人格、个人上下文、聊天渠道 |
| 自主任务产品 | Manus | 一个任务交付物 | 厂商沙箱里的端到端完成 |
| 编码 Agent harness | DeepSeek Harness、Claude Code、Codex | 一个开发者的会话 | Agent 循环：工具、沙箱、评审 |
| Agent 框架 | LangGraph、CrewAI | 你这个应用的代码 | 构建一个 Agent 应用 |
| **GoAgent（AgentOS）** | **本仓库** | **一个产品的 Agent 工作负载** | 耐久、治理、审计、计费、多租户——作为产品自己拥有的平台 |

**GoAgent 算 harness 吗？** 不算。harness 是*单个* Agent 循环的驾驶舱——为一个终端前的开发者驱动工具、沙箱和评审。GoAgent 是*众多*后端持有 run 的空管系统——面向整个工作负载的生命周期、治理与审计。二者是组合而非竞争：harness 构建的 Agent（或任何 HTTP/gRPC/Temporal 服务）作为一个后端接入控制平面——LangGraph 和 OpenCode 式运行时现在就是这么接进来的。GoAgent 也不是你在进程内编码的框架——它是你的产品*使用*的基础设施，通过 REST、Go 内嵌或纯类型契约接入。

## 快速开始

需要 Go 1.26+、Docker 和 Docker Compose。

```sh
# 一次性：生成开发凭据（.env）。没有它栈拒绝启动，仓库也不自带任何凭据。
make dev-secrets

# 启动依赖：Postgres、Redis、NATS JetStream、Centrifugo、Temporal
make compose-up

# 运行应用（以 migrate 标签构建并应用迁移）
make run
```

- REST API：`http://127.0.0.1:8080` —— [`/healthz`](http://127.0.0.1:8080/healthz)、[`/metrics`](http://127.0.0.1:8080/metrics)、[`/swagger`](http://127.0.0.1:8080/swagger)
- Docker 全栈：`make compose-up-all`
- 集成测试（mock LLM，容器网络内）：`make compose-up-integration-test`

## API 一览

REST 按 `/v1` 版本化；完整参考见 [`/swagger`](http://127.0.0.1:8080/swagger)。形状如下：

| 领域 | 代表端点 |
|---|---|
| Runs | `POST /v1/agentos/runs` · `GET /runs/{id}/status` · `POST /runs/{id}/signals` · `POST /runs/{id}/control` |
| 耐久 Plans | `POST /v1/agentos/plans` · `GET /plans/{id}/status` · `GET /plans/{id}/events`（SSE）· `GET /plans/{id}/audits` · `GET /plans/{id}/artifacts/{id}` |
| 创作工具 | `GET /v1/agentos/plans/schemas/{kind}` · `GET /v1/agentos/plans/author` · `cmd/agentos-plan`（validate / schema / Serverless Workflow 导入导出） |
| 模板 | `POST /v1/templates/import` · `GET /v1/templates/` |

## 项目结构

一个小的公共 **AgentOS 边界** + 适配器 + 应用外壳。`internal/` 下的实现包不是公共契约。

```text
公共端口（引擎中立，零 Temporal import）
  agentos/core       # signals、controls、events、artifacts、tools、errors
  agentos/control    # run 与 plan 契约、capabilities、backend refs
  agentos/process    # resources、ledger、governed actions、batches、projections
  agentos/platform   # 需要两个平面时的组合门面

默认适配器
  agentos/temporal   # Temporal + Postgres + Redis + artifact store
  agentos/nexusapi   # 版本化的 Nexus 服务契约

应用外壳（非公共）
  internal/controller   # REST 传输
  internal/usecase      # 应用用例
  internal/repo         # 持久化（sqlc 优先）与后端适配器
  internal/agentfw      # 原生 GoAgent 后端——一个后端，不是架构本身
  internal/app, cmd/    # 装配与入口
```

两条规则保持边界诚实：`agentos/control` 永不 import `agentos/process`（Agent run 不懂业务语义）；`agentos/process` 永不 import `agentos/control`（耐久流程离开 Agent 执行也存在）。SQL 放在 `internal/repo/persistent/queries/*.sql`，由 `make sqlc` 编译成类型安全的绑定——没有 ORM，没有运行时拼 SQL。

## 使用方式

1. **独立服务器** —— 把 GoAgent 当服务跑，你的产品通过 REST 控制面对接。
2. **Go 库内嵌** —— import AgentOS 边界，用默认适配器启动：

```go
import (
    agentos "github.com/TekkenSteve/GoAgent/agentos/control"
    agentostemporal "github.com/TekkenSteve/GoAgent/agentos/temporal"
)

rt, _ := agentostemporal.NewRuntime(ctx, agentostemporal.RuntimeConfig{
    TemporalAddress:    "127.0.0.1:7233",
    TemporalNamespace:  "default",
    TemporalTaskQueues: agentostemporal.DefaultTaskQueues(),
    PostgresURL:        "postgres://goagent:goagent@127.0.0.1:5432/goagent?sslmode=disable",
    RedisURL:           "redis://127.0.0.1:6379/0",
})
status, _ := rt.Start(ctx, agentos.RunSpec{ /* … */ })
```

3. **只用公共类型** —— import `agentos/core` 和 `agentos/control` 获取共享契约。

每种模式都有可运行示例，在 [`examples/`](examples/)（REST、embed、types）。

## 开发

```sh
make sqlc                # 重新生成类型安全的 SQL 绑定（query/schema 变更后必跑）
make swag-v1             # 重新生成 Swagger 文档
make mock                # 重新生成 gomock mocks
make test                # 单元测试（-race）
make compose-up-integration-test   # 容器网络内的集成套件
make linter-golangci     # lint
make check-import-boundary        # 强制公共契约引擎中立
make check-workflow-determinism   # Temporal workflow 确定性
```

配置遵循 12-factor，只用环境变量——见 [config/config.go](config/config.go) 和 [.env.example](.env.example)。迁移是 [`migrations/`](migrations/) 里的 golang-migrate 成对文件；应用以 `migrate` 标签构建时在启动时应用。链路追踪 OpenTelemetry（`TRACING_ENABLED`），指标 Prometheus，日志 zerolog。

## 展望

- **内核只是细节。** 公共契约从不提及执行引擎；Temporal 活在一个适配器里。换一个耐久内核，不用动任何消费者。
- **承诺取代轮询。** Nexus 面向耐久跨服务操作生长：在崩溃中保持数秒或数天、精确一次，而不是各自造状态轮询。
- **事实单向流动。** 事务性 outbox → JetStream 骨干成为唯一正道的事实源：投影、镜像、分析、重放都来订阅；UI 增量单独走 Centrifugo，永不进入 stream。
- **MCP 作为工具通用语。** 原生后端已说 MCP；capability 与 artifact-schema 目录是版本化的 JSON Schema——这是 Agent 市场在拥有 Agent 之前先需要的地基。
- **Agent 即公用事业。** 终局是像用电一样使用 Agent 工作：账本计量、审计链防篡改、governed action 圈定边界——以基础设施应有的方式无聊。

## 参考

- [Temporal](https://temporal.io/) · [NATS JetStream](https://nats.io/) · [Centrifugo](https://centrifugal.dev/) · [sqlc](https://sqlc.dev/)
- [Clean Architecture](https://blog.cleancoder.com/uncle-bob/2012/08/13/the-clean-architecture.html) · [The Twelve-Factor App](https://12factor.net/)

## 许可证

MIT License —— 见 [LICENSE](LICENSE)。
