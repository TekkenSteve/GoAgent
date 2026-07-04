# AgentOS Durable Process Platform

## Positioning

AgentOS treats Temporal as a durable process kernel. Temporal owns long-running process execution, timers, signals, retries, cancellation, versioned workflow evolution, and recovery after worker failure.

AgentOS is the generic intelligent work OS layer above that kernel. It provides control-plane primitives for backend-owned runs, process state, audit trails, governed actions, batch work, and durable projections. Domain systems build on those primitives.

AiSOC is a reference distribution, not a core domain model. Concepts such as alerts, cases, incidents, detection rules, investigations, and hunts belong in examples or domain packages that use AgentOS public interfaces. They must not become required AgentOS core types.

## Package Layers

AgentOS keeps the agent control plane and durable process platform separate at the package boundary:

```text
application distributions
  -> agentos/platform
      -> agentos/process
      -> agentos/control
          -> agentos/core

agentos/temporal
  -> agentos/process
  -> agentos/control
  -> agentos/core
```

- `agentos/core` contains only shared OS primitives: signals, controls, events, artifacts, messages, tools, subscriptions, and public errors.
- `agentos/control` owns agent execution contracts: runs, plans, capabilities, backend refs, plan schemas, and backend-owned child run orchestration.
- `agentos/process` owns durable process contracts: resources, processes, ledgers, governed actions, worksets, and batch progress.
- `agentos/platform` is a facade for applications that intentionally compose both layers.
- `agentos/temporal` is an adapter for the public ports. It does not define application domain models.

The dependency direction is enforced by tests. `control` must not import `process`; `process` must not import `control`; neither may import `temporal` or `internal`. Backend adapters may use `control` and `core`, but must not import the process layer.

## Boundaries

AgentOS core uses generic OS primitives:

- `RunPlan`: coarse-grained orchestration of backend-owned runs.
- `ResourceRef`: a stable reference to domain resources such as cases, tickets, orders, incidents, or code changes.
- `Spec`: durable lifecycle logic for a long-running business object.
- `LedgerRuntime`: append-only decision, evidence, action, and artifact records.
- `GovernedAction`: dry-run, risk evaluation, approval, execution, cancellation, and compensation.
- `Workset` / `BatchRuntime`: coarse-grained batch work with chunking, throttling, retry, and progress projection.
- `ProjectionRuntime`: query-optimized state for REST, MCP, UI, and operators.

AgentOS core must not model backend internals as control-plane nodes. Do not turn each LangGraph node, OpenCode step, tool call, LLM call, or data record into an AgentOS `PlanNode`. A `PlanNode` represents a backend-owned run or batch run. The backend runtime owns its internal graph, tools, state transitions, and low-level execution details.

Temporal must not become the high-volume data plane. Large prompts, responses, evidence blobs, tool I/O payloads, artifacts, graph data, search indexes, and lake data stay in external storage. Temporal history stores compact commands, references, durable process state, and deterministic control flow.

## Platform Phases

### Phase 1: Temporal Kernel Hygiene

Use explicit task queues for workload isolation:

- `agentos-plan-control` for RunPlan workflow control, signals, and audit decisions.
- `agentos-plan-activity` for plan persistence, artifact publishing, and event projection.
- `agentfw-native-control` for native backend workflow control.
- `agentfw-native-llm` for native LLM activities.
- `agentfw-native-tool` for native tool activities.
- `agentfw-stream` for streaming workflows and activities.
- `agentfw-trigger` for trigger and schedule workflows.

Workflow inputs must persist routing decisions needed for deterministic child workflow and activity scheduling. Missing or duplicate queues should fail at validation time.

State reads for UI and API should use durable projections. High-frequency status reads should not depend on Workflow Query calls.

### Phase 2: Generic Resource And Process Runtime

Introduce a generic resource/process layer. Business systems register resource kinds such as `case`, `incident`, `ticket`, `order`, or `change`, while AgentOS core only stores generic identity, ownership, lifecycle, policy, and references.

A long-lived business object maps to a coarse-grained process workflow. Signals represent external events, human input, approvals, and domain callbacks. Timers represent SLA, waiting, escalation, and scheduled follow-up.

### Phase 3: Ledger And Projection

Add an append-only ledger for decision records, evidence references, prompt and response references, tool I/O references, artifact references, rationale, actor, timestamp, and policy context.

The ledger stores references to large payloads instead of embedding them in Temporal history. Projection rebuilds process, resource, and ledger read models for REST, MCP, UI, and operational dashboards.

### Phase 4: Governed Action And Batch

Add governed action primitives for dry-run, risk evaluation, approval gates, execution, cancellation, and compensation references. Actions should be idempotent and auditable.

Add workset and batch primitives for large workloads. AgentOS controls chunking, limits, retries, and progress projection. A batch should be represented as one coarse-grained process or backend-owned run, not one workflow per record.

### Phase 5: Reference Distributions

Build `examples/aisoc` as a reference distribution that uses only generic AgentOS primitives:

- alert/resource ingestion maps to `ResourceRef`.
- case handling maps to `Spec`.
- investigation maps to `RunPlan`.
- analyst and agent decisions map to ledger records.
- response actions map to governed actions.
- hunt and enrichment workloads map to worksets and batch runs.

The same platform shape should support future CodeAgent, DevOps, CustomerOps, and other intelligent work distributions without adding their domain nouns to AgentOS core.

## AiSOC-On-AgentOS Shape

An AiSOC distribution should map its domain objects onto generic AgentOS primitives:

```text
AiSOC Alert/Case/Rule/Hunt
  -> agentos/process ResourceRef and Spec
      -> agentos/control RunPlanSpec for investigation runs
          -> LangGraph/OpenCode/native backend-owned run
      -> LedgerRuntime for evidence, rationale, artifact refs
      -> GovernedActionRuntime for containment/remediation
      -> BatchRuntime for hunts and backfills
```

LangGraph graph nodes, OpenCode steps, tool calls, LLM calls, and individual lake records remain inside their owning backend or data plane. AgentOS coordinates durable process state and references, not backend internals.

## Design Rules

- Prefer coarse-grained workflows over per-record or per-tool-call workflows.
- Keep backend runtime internals backend-owned.
- Put durable process control in Temporal and high-volume data in specialized stores.
- Validate routing, schema, capability, and ownership at the boundary.
- Treat workflow history as a scarce durable log, not an event warehouse.
- Version workflow control-flow changes before production rollout.
- Keep public AgentOS interfaces generic and domain-neutral.
