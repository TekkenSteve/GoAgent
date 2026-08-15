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
│  (浏览器/IDE) │◄────────►│  · agentos:run:{tenant}:{run} 会话频道        │
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

遵循 Centrifugo 最佳实践：命名空间 + `<resource>:<id>` 层级。频道不带 `$` 私有前缀——服务端签发的频道由 `agentos` 命名空间承载 history 与访问控制（v5 里 `$` 频道强制要求订阅令牌，与本仓无令牌的单机默认传输不兼容）；生产收紧仍走 §4 的订阅令牌 / subscribe proxy，不改频道名。

| 频道 | 语义 |
|---|---|
| `agentos:run:{tenant}:{run_id}` | 一次 run 的完整时间线（token / 工具 / 里程碑） |
| `agentos:plan:{tenant}:{plan_id}` | 一个 plan 跨多个 child run 的汇聚时间线 |
| `agentos:thread:{tenant}:{thread_id}` | 会话线程级（可选，thread 跨 run） |

命名空间配置（history 是总线上 token 级回放的窗口，瞬态、有界）。v5 的 `namespaces` 是**数组**；dev 默认就是仓库里的 `configs/centrifugo/config.json`——匿名连 + 匿名订 + 匿名读 history，单机即用。生产调大 history 窗口、去掉匿名 allow、加订阅令牌：

```json
{
  "namespaces": [
    {
      "name": "agentos",
      "history_size": 100,
      "history_ttl": "300s",
      "allow_subscribe_for_client": true,
      "allow_subscribe_for_anonymous": true,
      "allow_history_for_client": true,
      "allow_history_for_anonymous": true
    }
  ]
}
```

超出历史窗口的旧内容：控制面投影 + blob 快照（claim-check 引用）负责——复用 `SnapshotHistoryActivity` 的机制。

## 4. 令牌模型

遵循 Centrifugo 双 JWT + 服务端发布的最佳实践。**当前 shipped 传输是无令牌匿名（服务端签发频道，§3 的 dev 命名空间允许匿名订 + 读 history）**；本节的 JWT 模型是前端接入时的生产收紧路径，频道名不变：

- **连接令牌**（前端连 Centrifugo）：`sub`=用户、`exp`。GoAgent 在 run 启动 / 登录时签发。
- **订阅令牌**（前端订私有频道）：`channel`=具体频道、`sub` 与连接令牌一致、`exp` 短命。**scope 到具体频道**——这就是「精准找到前端指定会话 + 谁都不能越权」的实现。
- **发布**：backend 不拿客户端令牌，走 **server API**（HTTP/gRPC `POST /api/publish` + `X-API-Key`）——与 Centrifugo GPT 流式示例一致。

**Handle**（GoAgent 在 run 启动时交给 backend 的唯一契约）：

```go
type Handle struct {
    Channel    string // "agentos:run:{tenant}:{run_id}"
    Vocabulary string // "ag-ui/v1"
    BatchMs    int    // 分批提示；0 = DefaultBatchMs
}
```

服务端 API 凭据（Centrifugo 地址 + API key）不在 Handle 上——那是总线 adapter 的配置，backend 只认频道。

## 5. 事件词汇：AG-UI（+ 最小扩展）

总线上的线格式采纳 AG-UI 事件类型（`agentos/stream/event.go` 的 `EventType`，词表与 AG-UI wire 逐字对齐）：

- 文本：`TEXT_MESSAGE_START / TEXT_MESSAGE_CONTENT(delta) / TEXT_MESSAGE_END`
- 推理：`REASONING_START / REASONING_MESSAGE_START / REASONING_MESSAGE_CONTENT / REASONING_MESSAGE_END`
- 工具：`TOOL_CALL_START / TOOL_CALL_ARGS(delta) / TOOL_CALL_RESULT / TOOL_CALL_END / TOOL_CALL_ERROR`
- 生命周期：`RUN_STARTED / RUN_FINISHED / RUN_ERROR / RUN_CANCELLED`、`STEP_STARTED / STEP_FINISHED`
- 状态：`STATE_SNAPSHOT / STATE_DELTA(JSON Patch)`、`MESSAGES_SNAPSHOT`、`ACTIVITY_SNAPSHOT / ACTIVITY_DELTA`
- 扩展：`RAW`（透传任意字节）与 `CUSTOM`（命名空间事件，见下）

Payload 字段名与 AG-UI 线格式逐字对齐（`agentos/stream` 的 `Field*` 常量）：

| 字段 | 承载 | 出现于 |
|---|---|---|
| `delta` | 文本 / 推理 / 工具参数增量 | `*_CONTENT` |
| `result` | 工具完成的返回结果 | `TOOL_CALL_RESULT` |
| `meta` | 工具自有的展示载荷（opaque JSON） | `TOOL_CALL_RESULT` |
| `error` | 结构化错误 | `TOOL_CALL_ERROR` / `RUN_ERROR` |
| `isError` | 工具结果错误标志（AG-UI） | `TOOL_CALL_RESULT` |
| `callId` | 工具调用关联 id | `TOOL_CALL_*` |
| `turn` / `step` | 1-based 轮次 / 步内编号 | `STEP_*` |
| `usage` | token 用量，`RUN_FINISHED` 上报 | `RUN_FINISHED` |
| `state` / `patch` | 全量状态 / RFC 6902 JSON Patch | `STATE_*` |
| `name` | `CUSTOM` 事件名（命名空间，如 `agentos.plan.event`） | `CUSTOM` |

控制面自有事件（审批门、账本、plan 状态迁移）以 `CUSTOM`（`name: "agentos.*"`）发布到频道，让前端在**同一条时间线**看到治理事件；其权威副本仍落 Postgres。plan 事件就是这样上线的：`planstream` 把 `PlanEvent` 序列化进 `CUSTOM agentos.plan.event` 的 `data` 载荷。

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
    Channel    string // "agentos:run:{tenant}:{run_id}"
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

**游标语义**（`Subscribe` 的 `after` 参数）：

- `after=0`：从历史窗口起点全量回放。
- `after=N`：只回放 `Sequence > N` 的补集——重连游标，断线窗口由总线 history 补。
- `LiveOnly`（`-1`）：跳过回放，只收订阅建立后新发布的事件（plan live tail 用它，耐久回放仍归 PG）。

投递的是 `StoredEvent{Event, Sequence, StoredAt}`——`Sequence` 是总线为每频道分配的单调序号，即重连与去重的游标；`Subscription.Close()` 释放传输资源（从总线扇出移除）。

P1 已落地：内存实现 `internal/repo/stream/memstream`（ordered replay + LiveOnly 游标 + 慢消费者丢弃）服务测试 / dev；`internal/repo/stream/streamconformance` 的 `RunStreamConformance` 锁定「发布→订阅→投影」行为，任何总线通过即视为接入完成。

`agentos/temporal` 是默认 adapter（native 的 Centrifugo 接入 + 投影者）。后端家族各一个薄适配器做「内部事件 → AG-UI」映射：native 走 `PublishWriter + MapEvent` 逐字流；HTTP / gRPC / temporal_external 是 one-shot（字节流在远端、经 ingest 端点回流），共享 `streamadapter.RunLifecycle` 参考实现做「可观测生命周期 → AG-UI 里程碑」映射（详见 §7 与 §12 P3）。

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

现状澄清：**运行时传输就是统一总线。** `agentos/stream` 契约（`Publisher` / `Subscriber` / `Projector`）由 Centrifugo（默认传输，以 Redis 为 broker）实现；未配置 Centrifugo 时降级为进程内 `memstream`（单机兜底，装配时打 WARN）。activities 把逐字事件**纯发布**到 AG-UI 频道；run 订阅、plan SSE、run 里程碑投影全部从总线读取，PG 是权威历史。旧 Redis 扇出（`RedisEventStore` / `RedisSequencer` / `RedisSubscriber` / `RedisPlanEventStream` / `redis.StreamHub`）与死代码 `WebSocketHub` 已删除。「不重复造轮子」的含义没变：别再造一个 Centrifugo。

| 组件 | 现状 |
|---|---|
| `internal/agentfw/stream` EventStore / Subscriber / Sequencer / StatelessGateway | **已删除**；`agentos/stream` 契约 + `agentos/core` + PG repo 是唯一抽象 |
| 手搓 Redis Stream 扇出（RedisEventStore / RedisSequencer / RedisSubscriber / RedisPlanEventStream / `redis.StreamHub`） | **已删除**，由总线取代 |
| `WebSocketHub` | 已删除（死代码，不在运行时路径上） |
| `LLMStreamActivity` 逐字事件（[activity.go](internal/agentfw/orchestration/activity.go)） | 纯 Publish AG-UI 到频道；workflow history 只留里程碑 + 引用 |
| run 里程碑投影 | `runprojection` 消费总线 → 持久化 PG（shipped app 接线） |
| plan 事件 | 双写 PG + 总线 plan 频道（`planstream`）推 SSE |
| `agentos/conversation` | example-only，退化为**纯 PG 轮询**订阅（无 live 传输） |
| Redis 本体 | 保留：Centrifugo 以 Redis 为 broker；WAL / DLQ（pipeline）+ `EventDedupeStore` 仍用 Redis |

**灵活性：传输可换。** 契约只依赖 `Publisher` / `Subscriber` / `Projector` 接口，Centrifugo 是默认实现，memstream 是未配置时的单机兜底；NATS JetStream / Mercure 随时可作替换实现，后端与前端无感。docker-compose 默认起 Centrifugo（app 直连）；本地 `go run` 未配 `CENTRIFUGO_BASE_URL` 时自动降级 `memstream`。

## 10. 运行装配与部署（shipped 现状）

### 装配点

shipped app（`cmd/app → app.Run`）在 `initInfrastructure` 里经 [`agentos/temporal.NewStreamingDataPlane`](agentos/temporal/dataplane.go) 装配总线——返回一个 `StreamingDataPlane{Publisher, Subscriber, Projector}`：

- **`StreamCentrifugo.BaseURL` 非空**（默认路径）：`centrifugo.NewPublisher` + `centrifugo.NewSubscriber`，读写真实 Centrifugo。
- **为空**：降级到进程内 `memstream.New()`，Publisher 与 Subscriber 是**同一个 bus 实例**（单进程内必须共享，否则 run 订阅 / plan SSE 看不到 activities 的发布），装配时打一条 WARN。
- **Projector 总是创建**：`runprojection.RunEventProjector` 消费 Subscriber → 落 PG（`agentos_run_events`）。activities 纯发布，run 订阅 / plan SSE / 里程碑投影都从总线读，PG 是权威历史。

库入口 `agentos/temporal/worker_builder.go` 的 `configureStreamingProjection` 走同一装配（Temporal worker 独立进程时用它）。

### 配置与部署

| 项 | 值 | 说明 |
|---|---|---|
| env `CENTRIFUGO_BASE_URL` | `http://centrifugo:8000`（compose） | 空 = memstream 兜底 |
| env `CENTRIFUGO_API_KEY` | `dev-api-key` | server API 发布凭据 |
| 服务端配置 | [configs/centrifugo/config.json](configs/centrifugo/config.json) | v5，Redis broker，`agentos` 命名空间 |
| docker-compose | [docker-compose.yml](docker-compose.yml) `centrifugo` 服务 | v5 镜像，8000：client WS + server API + admin 面板（`/`，admin 密码 `password`） |

发布走 **server API**：`POST {base}/api/publish`，同时带 `X-API-Key` 与 `Authorization: apikey <key>`（v5/v6 都认），事件 JSON 直接是 AG-UI 线格式。

订阅走**匿名 websocket**（`centrifuge-go`）：`ws://{base}/connection/websocket` 匿名连接（服务端 `allow_anonymous_connect_without_token`）→ 订 `agentos:run:*` 频道（`Positioned` + `Recoverable`）→ **显式 `History(since after)` 重放窗口，再桥接 live**（该客户端版本无法在订阅时播种恢复游标，重放是显式做的）。慢消费者丢弃，与 memstream 扇出同策略。

### 真机验证

P4 收尾时对真实 Centrifugo **v5.4.9** 做了端到端验证（非 httptest 假服务）：匿名连 → 订 `agentos:run:default:smoketest` → history 重放（seq 1, 2）→ live 投递（seq 3）全链路通过，memory 与 Redis 引擎各跑一遍。这同时暴露并修掉两处与真机语义不符的假设：**`$` 前缀频道在 v5 里是私有频道**（订阅强要令牌，匿名订直接 `PermissionDenied`），且 **`ns:rest` 频道必须配 `ns` 命名空间**否则发布返回 `102 unknown channel`——所以频道定型为 `agentos:run:*`（去 `$`）+ 服务端配 `agentos` 命名空间，详见 §3。

## 11. 已知限制与后续工作

- **前端订阅令牌（§4 JWT 模型）未做**：当前传输无令牌匿名，靠 `agentos` 命名空间的匿名 allow 开门。前端接入时收敛为连接令牌 + 频道级订阅令牌（`channel` claim），关掉 `allow_*_for_anonymous`——频道名不变。
- **`agentos:thread:*` 会话线程频道未铸造**：§3 表里的可选项，thread 跨 run 的汇聚线需要时才做。
- **plan SSE 直连前端**：`SubscribePlan` 已走总线 plan 频道（`agentos:plan:*`），HTTP SSE 端点仍在；前端直连总线的 claim-check 故事留后续。
- **多实例部署**：docker-compose 单实例 Centrifugo + Redis broker（config 已配 `engine: redis`）；横向扩容是加实例共用 Redis，无代码改动。
- **`agentos/conversation`（example）无 live 传输**：纯 PG 轮询；其事件非 AG-UI，不接总线契约。
- **投影的时序保证**：总线挂则里程碑延迟（投影由总线驱动，bus 抖动不失败 run）；重连靠 history 窗口 + PG 兜底。

## 12. 分阶段落地

**统一契约下，接入顺序无关紧要。** 契约用一致性测试锁定——`agentosruntimetest` 的 `RunBackendConformance` 锁定 `AgentBackend` 契约，新增 `LifecycleProbe` 锁定「Start → `PublishStarted`、Status → `PublishStatus`」的生命周期接线，`RunLifecycle` 单测锁适配器行为，HTTP 端到端 bus 测试锁「backend → 适配器 → 总线」全链路（§12 P3）。谁先接入没有差别，先用一个假 backend 验证契约，再用任何真实 backend 验收即可。

- **P1 契约 + 一致性测试** ✅：`agentos/stream` 包（`Handle` + AG-UI 事件类型 + `Publisher`/`Subscriber`/`Projector`）+ 内存实现 `memstream`（测试 / dev）+ `streamconformance` suite 锁定「发布→订阅→投影」行为。
- **P2 任一真实 backend 接入** ✅（native）：native 已跑通契约；其余 backend 未铺开。
- **P3 其余 backend 铺开** ✅：HTTP / gRPC / temporal_external 各一薄适配器 + 一个参考实现。薄适配器即共享的 [`streamadapter.RunLifecycle`](internal/repo/agentos/streamadapter/run_lifecycle.go)——外部 backend 不拥有本地字节流，控制面可观测的「自家输出」就是 run 生命周期：`Start` 成功 → `RUN_STARTED`，终态 `Status` → `RUN_FINISHED` / `RUN_ERROR` / `RUN_CANCELLED`（终态拼写归一：completed / succeeded → finished，failed → error，canceled / cancelled → cancelled）。三个 backend 经 `agentosruntime.LifecyclePublisher` 端口接入（Start / Status 各发布一次）；per-run scope 终态发布即删（重复终态去重）、nil 发布器降级 no-op、`WithEnsure` 钩子让投影者在首发布时挂载（run 里程碑落 PG 与 native 一致）。用量不上里程碑——远程 token 流走既有 ingest 端点。验收：`RunLifecycle` 单测（真实 memstream）+ 三 backend 接线测试（`LifecycleProbe`）+ HTTP 端到端 bus 验收（`Start` → `RUN_STARTED`，终态 `Status` → `RUN_FINISHED` 落到 run 频道）。
- **P4 重连与收尾**：Centrifugo recovery + PG `after_sequence` 双通道重连；删除死代码（WebSocketHub）与旧 Redis 扇出。✅ **完成**：总线（Centrifugo 默认传输 / 未配置时 memstream 单机兜底）已是 shipped app 的默认 live 传输——activities 纯发布，run 订阅、plan SSE、run 里程碑投影都从总线读；PG 是权威历史（run 里程碑投影 + plan 事件双写）。docker-compose 已内置 Centrifugo 服务（`configs/centrifugo/config.json`，app 默认指向 `CENTRIFUGO_BASE_URL`）。旧 Redis 扇出四件套与 `WebSocketHub` 已删除，`agentos/conversation` 退化为纯 PG 轮询；Redis 本体保留（Centrifugo broker / WAL / DLQ / 事件去重）。

## 13. 成熟参考

- [Centrifugo for AI apps](https://centrifugal.dev/docs/getting-started/ai_apps)——总线定位与「直连流在生产中的痛点」
- [Streaming AI responses with Centrifugo](https://centrifugal.dev/blog/2025/06/17/streaming-ai-gpt-responses-with-centrifugo)——临时频道 + server API 发布 + done 标志
- [Centrifugo channels & namespaces](https://centrifugal.dev/docs/server/channels)、[Private channels](https://centrifugal.dev/docs/3/server/private_channels)、[Channel JWT auth](https://centrifugal.dev/docs/4/server/channel_token_auth)——命名空间 / 历史 / recovery / 双令牌
- [centrifuge-go README](https://github.com/WallEnd/centrifuge-go/blob/e52577f3e2cd56683754d88e2c12137c8cff629c/README.md)——Go 客户端订阅 / history / recovery
- [AgentScope Service 技术解读](https://java.agentscope.io/v2/zh/blogs/agentscope-service-release-tech.html)——控制面不跑 Turn、持久化事件才是真相、SSE 仅即时体验
- [LangGraph streaming](https://docs.langchain.com/oss/javascript/langgraph/streaming) / [LangGraph event streaming（build your own projection）](https://docs.langchain.com/oss/javascript/langgraph/event-streaming)——一条流两个消费者
- [Temporal + Google ADK（Workflow Streams）](https://docs.temporal.io/develop/typescript/integrations/strands-agents)——chunk 走 side-channel，history 只留结果
- [AG-UI protocol](https://github.com/ag-ui-protocol/ag-ui)——事件词汇与生命周期
