# AgentOS 流式数据面：Centrifugo + AG-UI + 投影消费

> 定位：本项目的流式传输层设计。它回答一个悬而未决的问题——**AI 的流式返回（thinking / 过程 / 逐字 token）该不该经过控制平面？**
>
> 结论先行：**不该。** token 流是数据面字节，不是控制面状态。控制面只发牌（频道 + 令牌）、投影（里程碑 + 引用）、治理（审批 / 账本 / 预算）。字节走一条独立、可横向扩容的数据面总线，前端与后端各连总线一端，GoAgent 不在字节路径上。
>
> 设计不发明新轮子：总线用 Centrifugo（成熟、开源、Redis broker 复用现有 Redis），词汇用 AG-UI（成熟、多端实现的 agent↔UI 事件协议），投影消费采用 LangGraph「一条流两个消费者」与 AgentScope「持久化事件才是真相」的成熟原则。所有细节均对照成熟参考，见文末。

## 1. 为什么是「Centrifugo + AG-UI + 投影消费」

用户上传文件 → 直接进 MinIO，通道里只走引用（claim-check）。AI 流式返回是**反过来**的 MinIO：字节是持续生成的。成熟业界对此的一致答案不是「控制面逐字中继」，而是三层：

- **数据面总线**：一个可横向扩容的实时扇出服务，前端连它、后端推它，频道即会话，令牌控权限，历史窗口兜重连。这就是 Centrifugo（或同族 Mercure）在做的。
- **归一化事件词汇**：所有 backend 往总线推同一套事件，前端 SDK 直接认。这就是 AG-UI。
- **投影消费**：控制面以投影者身份消费同一条频道，落里程碑 / 引用 / 用量到耐久库；逐字字节在总线上是瞬态。这就是 LangGraph「驾驶舱 vs 塔台」、AgentScope「SSE 只是即时体验，落库事件才是真相」、Temporal「chunk 走 side-channel，history 只留结果」。

| 要解决的事 | 采纳的成熟方案 |
|---|---|
| 数据面总线 + 会话频道 + JWT 授权 + 重连 | **Centrifugo**（Redis broker，复用现有 Redis） |
| 跨 backend 统一事件词汇 | **AG-UI**（CopilotKit / AG2 / Atmosphere 多端实现） |
| 控制面不碰字节、只投影 | AgentScope「控制面不跑 Turn」+ LangGraph「一条流两个消费者」 |
| 逐字不落控制面耐久库 | 字节落总线历史窗口；耐久层只存里程碑 + 引用 |

## 2. 目标拓扑

```
┌─────────────┐          ┌─────────────────────────────────────────────┐
│  Frontend     │  WS/SSE │  Centrifugo（数据面总线）                      │
│  (浏览器/IDE) │◄────────►│  · $agentos:run:{tenant}:{run} 会话频道        │
│  订 sub JWT  │   sub   │  · history+recovery（offset 重放断线窗口）       │
└──────┬───────┘          │  · Redis broker（横向扩容）                    │
       │                  └──────▲───────────────────┬───────────────────┘
       │ POST /runs        pub   │                   │ 投影（消费同一频道）
       ▼                server  API                 ▼
┌─────────────┐         ┌───────┴───────────────────▼───────────────────┐
│ GoAgent      │───────►│  任意 backend：native / LangGraph / OpenCode    │
│ 控制面        │  Stream │  HTTP / gRPC / temporal_external               │
│ · 发牌:频道+令牌│ handle │  只做一件事：Publish(AG-UI 事件) 到会话频道       │
│ · 投影:里程碑/ │         └───────────────────────────────────────────────┘
│   引用/usage→PG│
│ · 生命周期/审批│
│ · 账本/审计    │
└─────────────┘
```

数据流（一次对话）：

1. 前端 `POST /agentos/runs` → GoAgent 建 run，签发 **Handle**（频道名 + 词汇 + 分批提示）给 backend，同时给前端签发 **订阅 JWT**。
2. backend 拿到 Handle，把内部事件映射成 AG-UI，经 Centrifugo **server API**（`X-API-Key`）`publish` 到频道——**不经过 GoAgent 进程**。
3. 前端用订阅 JWT 订频道；Centrifugo 按频道精确投递；断线由 history+recovery 补。
4. GoAgent 以**投影者**身份消费同一频道，过滤出里程碑事件，写入 Postgres（权威事件），并驱动审批 / 账本 / 生命周期 / search attribute。逐字不落库。

## 3. 频道模型

遵循 Centrifugo 最佳实践：命名空间 + `<resource>:<id>` 层级，私有频道用 `$` 前缀。

| 频道 | 语义 |
|---|---|
| `$agentos:run:{tenant}:{run_id}` | 一次 run 的完整时间线（token / 工具 / 里程碑） |
| `$agentos:plan:{tenant}:{plan_id}` | 一个 plan 跨多个 child run 的汇聚时间线 |
| `$agentos:thread:{tenant}:{thread_id}` | 会话线程级（可选，thread 跨 run） |

命名空间配置（history 是总线上 token 级回放的窗口，瞬态、有界）：

```json
{
  "namespaces": {
    "agentos": {
      "history_size": 5000,
      "history_ttl": "24h",
      "force_recovery": true,
      "force_positioning": true,
      "presence": false,
      "join_leave": false
    }
  }
}
```

超出历史窗口的旧内容：控制面投影 + blob 快照（claim-check 引用）负责——复用 `SnapshotHistoryActivity` 的机制。

## 4. 令牌模型

遵循 Centrifugo 双 JWT + 服务端发布的最佳实践：

- **连接令牌**（前端连 Centrifugo）：`sub`=用户、`exp`。GoAgent 在 run 启动 / 登录时签发。
- **订阅令牌**（前端订私有频道）：`channel`=具体频道、`sub` 与连接令牌一致、`exp` 短命。**scope 到具体频道**——这就是「精准找到前端指定会话 + 谁都不能越权」的实现。
- **发布**：backend 不拿客户端令牌，走 **server API**（HTTP/gRPC `POST /api/publish` + `X-API-Key`）——与 Centrifugo GPT 流式示例一致。

**Handle**（GoAgent 在 run 启动时交给 backend 的唯一契约）：

```go
type Handle struct {
    Channel    string // "$agentos:run:{tenant}:{run_id}"
    Vocabulary string // "ag-ui/v1"
    BatchMs    int    // 分批提示；0 = DefaultBatchMs
}
```

服务端 API 凭据（Centrifugo 地址 + API key）不在 Handle 上——那是总线 adapter 的配置，backend 只认频道。

## 5. 事件词汇：AG-UI（+ 最小扩展）

总线上的线格式采纳 AG-UI 事件类型：

- 文本：`TEXT_MESSAGE_START / TEXT_MESSAGE_CONTENT(delta) / TEXT_MESSAGE_END`
- 推理：`REASONING_MESSAGE_START / REASONING_MESSAGE_CONTENT / REASONING_MESSAGE_END`
- 工具：`TOOL_CALL_START / TOOL_CALL_ARGS(delta) / TOOL_CALL_RESULT / TOOL_CALL_END / TOOL_CALL_ERROR`
- 生命周期：`RUN_STARTED / RUN_FINISHED / RUN_ERROR`、`STEP_STARTED / STEP_FINISHED`
- 状态：`STATE_SNAPSHOT / STATE_DELTA(JSON Patch)`、`MESSAGES_SNAPSHOT`

控制面自有事件（审批门、账本、plan 状态迁移）以 `CUSTOM`（`name: "agentos.*"`）发布到频道，让前端在**同一条时间线**看到治理事件；其权威副本仍落 Postgres。

相对 dsh 的 SessionEventMap，AG-UI 缺三样，按需用最小扩展补（**不撑大协议**，扩在 adapter 层或 CUSTOM）：

1. **turn/step 层级**：需要「第几轮第几次模型调用」时，在 `STEP_*` 上带 `turn`/`step` 编号。
2. **工具表现层 meta**：`TOOL_CALL_RESULT` 上带工具自有的 JSON `meta`（如 fs diff 卡片）。
3. **chunk 关联**：token 级回放若硬性要求，由**总线历史**承担（Centrifugo offset），不由控制面耐久承担。

### 表达能力与扩展策略

表达力不靠「把协议撑大」，靠**分层 + 命名空间扩展**：

- **协议层只放通用词**：文本 / 推理 / 工具 / 生命周期 / 状态——所有 backend 都表达得了的。
- **领域词走 `CUSTOM` + 命名空间**：控制面自有事件用 `name: "agentos.*"`（审批门、账本、plan 状态）；backend 专属语义用 `name: "<backend>.*"`。前端对未知 `CUSTOM` 可安全忽略或降级渲染，不破坏兼容。
- **丰富模型留在 backend 内部**：dsh 的 SessionEventMap（turn/step、工具 meta、chunk 关联）这类丰富模型是 backend 自己的源模型，adapter 投影成 AG-UI 上总线——总线保持薄，backend 保持强。
- **投影者与前端看到同一条时间线**：控制面治理事件以 `CUSTOM` 发布进频道，前端在同一条流里看到「模型说了什么 + 系统批准了什么」，无需第二条连接。

## 6. 引擎中立契约：`agentos/stream`

新包，零 Temporal 依赖，符合 `scripts/check_import_boundary.sh` 锁死的边界：

```go
// Handle 由控制面在 run 启动时签发
type Handle struct {
    Channel    string // "$agentos:run:{tenant}:{run_id}"
    Vocabulary string // "ag-ui/v1"
    BatchMs    int    // 分批提示；0 = DefaultBatchMs
}

// Publisher 由总线 adapter 实现（生产 = Centrifugo，dev/test = memstream）；backend 只依赖它
type Publisher interface {
    Publish(ctx context.Context, handle *Handle, ev *Event) error
}

// Subscriber 是读侧：前端网关与投影者都经它消费；after 是重连游标
type Subscriber interface {
    Subscribe(ctx context.Context, handle *Handle, after int64) (*Subscription, error)
}

// Projector 是控制面的读角色：NewProjector 把任意 Subscriber 提升为投影者，
// ProjectToCore 把里程碑归约成 agentos/core.Event，字节增量直接过滤
type Projector interface {
    Consume(ctx context.Context, handle *Handle, after int64) (*Subscription, error)
}
```

P1 已落地：内存实现 `internal/repo/stream/memstream`（ordered replay + LiveOnly 游标 + 慢消费者丢弃）服务测试 / dev；`internal/repo/stream/streamconformance` 的 `RunStreamConformance` 锁定「发布→订阅→投影」行为，任何总线通过即视为接入完成。

`agentos/temporal` 是默认 adapter（native 的 Centrifugo 接入 + 投影者）。后端家族各一个薄适配器做「内部事件 → AG-UI」映射。

## 7. backend 接入契约（3 步，不用调试）

任何新 backend 接入时拿到 Handle，做且只做：

1. **映射**：把自家输出映射成 AG-UI（文本 / 推理 / 工具 / 生命周期 / 状态）。
2. **发布**：热路径上按 `BatchMs` 分批 `Publish` chunk；里程碑处发生命周期事件。
3. **上报用量**：`RUN_FINISHED` 上带 `usage`（供投影者计费，不逐字）。

不需要知道 GoAgent 的鉴权、持久化、SSE、重连——总线全包了。这正对应 AgentScope「Managed 与 BYO 共用舰队契约，框架差异收敛在适配器」。

## 8. 投影消费与耐久

- **权威真相**：Postgres 里的投影事件（plan / run 里程碑、审批、账本、usage）带会话单调序号。前端需要权威历史时走 `GET /agentos/{scope}/events?after={seq}`（复用现有 `after_sequence`）。
- **即时体验**：token 增量只在 Centrifugo 历史窗口（瞬态），不落 Postgres。对应用户断言：AgentScope「SSE 可推送 event_delta 获得即时体验，但最终以落库事件为准」。
- **重连**：Centrifugo recovery 补窗口内；窗口外走 Postgres 投影 + blob 快照。
- **审计**：模型原始输出不进控制面历史（愿景文档既定原则）；引用 + 里程碑 + usage 汇总进账本。

## 9. 与现有代码的关系

现状澄清：**当前运行时传输就是 Redis Streams**——`RedisSubscriber` / `redis.StreamHub` / `RedisPlanEventStream` 经 XREAD 扇出，`LLMStreamActivity` 把逐字事件 `EventStore.Append` 进 `agent:events:{sessionID}`。`WebSocketHub` 全仓无引用，是死代码。换句话说：**本项目已经在手搓一个"数据面总线"。** Centrifugo 就是这条线的成熟版——同样以 Redis 为 broker，但补上连接层（WS/SSE）、JWT 授权、recovery / 历史、横向扩容。「不重复造轮子」在这里的意思是：别再造一个 Centrifugo。

| 现状 | 去向 |
|---|---|
| `internal/agentfw/stream/interfaces.go` EventStore / Subscriber | 保留接口，作为投影 / 权威历史的抽象；不再承载逐字 |
| `internal/repo/stream/redis` RedisSubscriber / RedisPlanEventStream（手搓 Redis Stream 扇出） | **由 Centrifugo（Redis broker）取代**——成熟版数据面总线 |
| `internal/repo/stream/websocket.go` WebSocketHub | 死代码，删除（不在运行时路径上） |
| `StreamAgentWorkflow.LLMStreamActivity` 逐字 Append EventStore（[activity.go:373](internal/agentfw/orchestration/activity.go#L373)） | 改为分批 Publish AG-UI 到频道；workflow history 只留里程碑 + 引用 |
| plan 事件入 Postgres（[roadmap:20](docs/architecture-task-roadmap.md#L20)） | 保持；run 级里程碑同样投影入 PG |
| `agentos/core` StreamScope / Subscription | 保持引擎中立契约 |

**灵活性：传输可换。** 契约只依赖 `Publisher` / `Subscriber` / `Projector` 接口，Centrifugo 是默认实现；NATS JetStream / Mercure 随时可作替换实现，后端与前端无感。测试 / dev 用接口后的内存实现（`memstream`），生产用 Centrifugo。

## 10. 分阶段落地

**统一契约下，接入顺序无关紧要。** 契约用一致性测试锁定——仿照现有 `agentosruntimetest/conformance.go` 的 `BackendConformanceCase`，写一套「任何 backend 通过即视为接入完成」的 conformance suite。谁先接入没有差别，先用一个假 backend 验证契约，再用任何真实 backend 验收即可。

- **P1 契约 + 一致性测试**：`agentos/stream` 包（`Handle` + AG-UI 事件类型 + `Publisher`/`Subscriber`/`Projector`）+ 内存实现 `memstream`（测试 / dev）+ `streamconformance` suite 锁定「发布→订阅→投影」行为。Centrifugo adapter 留待 P2。
- **P2 任一真实 backend 接入**：native 或某个外部 backend 跑通契约（通常先 native 便于调试，但非必须）。
- **P3 其余 backend 铺开**：HTTP / gRPC / temporal_external 各一薄适配器 + 一个参考实现。
- **P4 重连与收尾**：Centrifugo recovery + PG `after_sequence` 双通道重连；删除死代码（WebSocketHub）与旧 Redis 扇出。

## 11. 成熟参考

- [Centrifugo for AI apps](https://centrifugal.dev/docs/getting-started/ai_apps)——总线定位与「直连流在生产中的痛点」
- [Streaming AI responses with Centrifugo](https://centrifugal.dev/blog/2025/06/17/streaming-ai-gpt-responses-with-centrifugo)——临时频道 + server API 发布 + done 标志
- [Centrifugo channels & namespaces](https://centrifugal.dev/docs/server/channels)、[Private channels](https://centrifugal.dev/docs/3/server/private_channels)、[Channel JWT auth](https://centrifugal.dev/docs/4/server/channel_token_auth)——命名空间 / 历史 / recovery / 双令牌
- [centrifuge-go README](https://github.com/WallEnd/centrifuge-go/blob/e52577f3e2cd56683754d88e2c12137c8cff629c/README.md)——Go 客户端订阅 / history / recovery
- [AgentScope Service 技术解读](https://java.agentscope.io/v2/zh/blogs/agentscope-service-release-tech.html)——控制面不跑 Turn、持久化事件才是真相、SSE 仅即时体验
- [LangGraph streaming](https://docs.langchain.com/oss/javascript/langgraph/streaming) / [LangGraph event streaming（build your own projection）](https://docs.langchain.com/oss/javascript/langgraph/event-streaming)——一条流两个消费者
- [Temporal + Google ADK（Workflow Streams）](https://docs.temporal.io/develop/typescript/integrations/strands-agents)——chunk 走 side-channel，history 只留结果
- [AG-UI protocol](https://github.com/ag-ui-protocol/ag-ui)——事件词汇与生命周期
