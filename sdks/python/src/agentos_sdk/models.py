from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


MODEL_CONFIG = ConfigDict(extra="forbid", populate_by_name=True)

BackendKind = Literal["native", "temporal_external", "http", "grpc"]
ControlOperation = Literal["pause", "resume", "cancel"]


class AgentOSModel(BaseModel):
    model_config = MODEL_CONFIG


class BackendRef(AgentOSModel):
    kind: BackendKind
    name: str


class ArtifactRef(AgentOSModel):
    artifact_id: str
    name: str = ""
    kind: str = ""
    media_type: str = ""
    uri: str = ""
    digest: str = ""
    metadata: dict[str, str] = Field(default_factory=dict)


class Artifact(AgentOSModel):
    ref: ArtifactRef
    payload: Any = None
    created_at: datetime | None = None


class RunProgress(AgentOSModel):
    current: int = 0
    total: int = 0
    message: str = ""


class PlanBudgetUsage(AgentOSModel):
    spent_cents: int = 0


class RunStatus(AgentOSModel):
    run_id: str
    lifecycle_state: str
    progress: RunProgress | None = None
    artifacts: list[ArtifactRef] = Field(default_factory=list)
    budget_usage: PlanBudgetUsage | None = None
    reason: str = ""
    updated_at: datetime | None = None


class RunStart(AgentOSModel):
    run_id: str
    account_id: str
    backend: BackendRef
    thread_id: str = ""
    project_id: str = ""
    agent_id: str = ""
    model_ref: str = ""
    system_prompt: str = ""
    user_message: str = ""
    idempotency_key: str = ""
    requested_at: datetime | None = None
    metadata: dict[str, str] = Field(default_factory=dict)
    input: dict[str, Any] = Field(default_factory=dict)


class Signal(AgentOSModel):
    type: str
    idempotency_key: str = ""
    actor_id: str = ""
    payload: dict[str, Any] = Field(default_factory=dict)
    sent_at: datetime | None = None


class ControlRequest(AgentOSModel):
    operation: ControlOperation
    idempotency_key: str = ""
    requested_at: datetime | None = None
    actor_id: str = ""
    metadata: dict[str, str] = Field(default_factory=dict)


class AgentOSEvent(AgentOSModel):
    event_id: str
    event_type: str
    source: str
    run_id: str = ""
    thread_id: str = ""
    sequence: int = 0
    timestamp: datetime | None = None
    trace_id: str = ""
    tags: dict[str, str] = Field(default_factory=dict)
    payload: dict[str, Any] = Field(default_factory=dict)


class AgentOSEventIngestResult(AgentOSModel):
    run_id: str
    event_id: str
    sequence: int
    duplicate: bool


class InputMapping(AgentOSModel):
    target: str
    source_node_id: str = ""
    source_artifact: str = ""
    source_path: str = ""
    expression: str = ""
    required: bool = False


class ArtifactSpec(AgentOSModel):
    name: str
    kind: str
    media_type: str = ""
    schema_ref: str = ""
    required: bool = False


class NodePolicy(AgentOSModel):
    max_attempts: int = 0
    timeout_seconds: int = 0
    join: str = ""


class RunPlanNodeSpec(AgentOSModel):
    node_id: str
    run: RunStart
    capability: str = ""
    inputs: list[InputMapping] = Field(default_factory=list)
    outputs: list[ArtifactSpec] = Field(default_factory=list)
    conditions: list[str] = Field(default_factory=list)
    policy: NodePolicy | None = None


class RunPlanEdgeSpec(AgentOSModel):
    edge_id: str
    from_: str = Field(alias="from")
    to: str
    on: str = ""
    condition: str = ""
    input_mapping: list[InputMapping] = Field(default_factory=list)


class PlanPolicy(AgentOSModel):
    max_nodes: int = 0
    max_depth: int = 0
    max_expansions: int = 0
    max_iterations: int = 0
    max_history_events: int = 0
    continue_as_new_events: int = 0
    max_parallel_nodes: int = 0
    budget_cents: int = 0
    timeout_seconds: int = 0


class RunPlanSpec(AgentOSModel):
    plan_id: str
    account_id: str
    project_id: str
    nodes: list[RunPlanNodeSpec]
    thread_id: str = ""
    idempotency_key: str = ""
    requested_at: datetime | None = None
    inputs: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, str] = Field(default_factory=dict)
    edges: list[RunPlanEdgeSpec] = Field(default_factory=list)
    policy: PlanPolicy | None = None


class PlanRef(AgentOSModel):
    plan_id: str
    account_id: str
    project_id: str


class PlanNodeStatus(AgentOSModel):
    node_id: str
    backend: BackendRef
    lifecycle_state: str
    run_id: str = ""
    attempts: int = 0
    budget_usage: PlanBudgetUsage | None = None
    reason: str = ""
    artifacts: list[ArtifactRef] = Field(default_factory=list)
    started_at: datetime | None = None
    completed_at: datetime | None = None
    updated_at: datetime | None = None


class RunPlanStatus(AgentOSModel):
    plan_id: str
    lifecycle_state: str
    nodes: list[PlanNodeStatus] = Field(default_factory=list)
    active_run_ids: list[str] = Field(default_factory=list)
    artifacts: list[ArtifactRef] = Field(default_factory=list)
    reason: str = ""
    budget_usage: PlanBudgetUsage | None = None
    metadata: dict[str, str] = Field(default_factory=dict)
    started_at: datetime | None = None
    updated_at: datetime | None = None


class PlanTopologyNode(AgentOSModel):
    node_id: str
    backend: BackendRef
    status: PlanNodeStatus
    run_id: str = ""
    capability: str = ""
    conditions: list[str] = Field(default_factory=list)
    inputs: list[InputMapping] = Field(default_factory=list)
    outputs: list[ArtifactSpec] = Field(default_factory=list)
    policy: NodePolicy | None = None


class PlanTopologyEdge(AgentOSModel):
    from_: str = Field(alias="from")
    to: str
    edge_id: str = ""
    on: str = ""
    condition: str = ""
    input_mapping: list[InputMapping] = Field(default_factory=list)


class PlanTopology(AgentOSModel):
    nodes: list[PlanTopologyNode]
    edges: list[PlanTopologyEdge] = Field(default_factory=list)
    order: list[str] = Field(default_factory=list)


class RunPlanDescription(AgentOSModel):
    plan_id: str
    status: RunPlanStatus
    topology: PlanTopology
    thread_id: str = ""
    account_id: str = ""
    project_id: str = ""
    policy: PlanPolicy | None = None
    metadata: dict[str, str] = Field(default_factory=dict)
    updated_at: datetime | None = None


class PlanSignal(Signal):
    account_id: str
    project_id: str


class PlanControl(ControlRequest):
    account_id: str
    project_id: str


class PlanEventScope(AgentOSModel):
    account_id: str
    project_id: str
    node_id: str = ""
    run_id: str = ""
    after_sequence: int = 0
    limit: int = 0


class PlanAuditScope(AgentOSModel):
    account_id: str
    project_id: str
    node_id: str = ""
    run_id: str = ""
    action: str = ""
    limit: int = 0


class PlanArtifactScope(AgentOSModel):
    account_id: str
    project_id: str
    node_id: str = ""
    run_id: str = ""
    limit: int = 0


class PlanEvent(AgentOSModel):
    event_id: str
    event_type: str
    source: str = ""
    sequence: int = 0
    run_id: str = ""
    plan_id: str = ""
    node_id: str = ""
    timestamp: datetime | None = None
    payload: dict[str, Any] = Field(default_factory=dict)


class PlanDebugTrace(AgentOSModel):
    trace_id: str = ""
    plan_id: str = ""
    node_id: str = ""
    run_id: str = ""
    sequence: int = 0
    event_type: str = ""
    message: str = ""
    timestamp: datetime | None = None
    payload: dict[str, Any] = Field(default_factory=dict)


class PlanAuditRecord(AgentOSModel):
    audit_id: str = ""
    plan_id: str
    account_id: str
    project_id: str
    node_id: str = ""
    run_id: str = ""
    action: str = ""
    actor_id: str = ""
    idempotency_key: str = ""
    created_at: datetime | None = None
    metadata: dict[str, str] = Field(default_factory=dict)


class ResourceRef(AgentOSModel):
    kind: str
    resource_id: str
    account_id: str
    project_id: str


class ProcessRef(AgentOSModel):
    process_id: str
    account_id: str
    project_id: str


class ProcessPolicy(AgentOSModel):
    timeout_seconds: int = 0
    max_history_events: int = 0
    continue_as_new_events: int = 0


class ProcessTimerSpec(AgentOSModel):
    timer_id: str
    fire_at: datetime | None = None
    after_seconds: int = 0
    signal: str = ""
    payload: dict[str, Any] = Field(default_factory=dict)


class ProcessSpec(AgentOSModel):
    process_id: str
    kind: str
    account_id: str
    project_id: str
    idempotency_key: str
    resource: ResourceRef
    requested_at: datetime | None = None
    inputs: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, str] = Field(default_factory=dict)
    policy: ProcessPolicy | None = None
    timers: list[ProcessTimerSpec] = Field(default_factory=list)


class ProcessStatus(AgentOSModel):
    process_id: str
    kind: str = ""
    account_id: str = ""
    project_id: str = ""
    resource: ResourceRef | None = None
    lifecycle_state: str
    reason: str = ""
    progress: RunProgress | None = None
    metadata: dict[str, str] = Field(default_factory=dict)
    started_at: datetime | None = None
    updated_at: datetime | None = None


class ProcessDescription(AgentOSModel):
    process_id: str
    kind: str
    account_id: str
    project_id: str
    resource: ResourceRef
    status: ProcessStatus
    policy: ProcessPolicy | None = None
    timers: list[ProcessTimerSpec] = Field(default_factory=list)
    metadata: dict[str, str] = Field(default_factory=dict)
    updated_at: datetime | None = None


class ProcessScope(AgentOSModel):
    account_id: str
    project_id: str
    resource_kind: str = ""
    resource_id: str = ""
    kind: str = ""
    lifecycle_state: str = ""
    limit: int = 0


class ActorRef(AgentOSModel):
    kind: str = ""
    actor_id: str = ""


class LedgerDataRef(AgentOSModel):
    kind: str
    uri: str = ""
    artifact_id: str = ""
    media_type: str = ""
    digest: str = ""
    metadata: dict[str, str] = Field(default_factory=dict)


class LedgerEntrySpec(AgentOSModel):
    entry_id: str
    idempotency_key: str
    account_id: str
    project_id: str
    process_id: str = ""
    resource: ResourceRef | None = None
    kind: str
    actor: ActorRef | None = None
    occurred_at: datetime | None = None
    summary: str = ""
    rationale: str = ""
    data_refs: list[LedgerDataRef] = Field(default_factory=list)
    artifact_refs: list[ArtifactRef] = Field(default_factory=list)
    metadata: dict[str, str] = Field(default_factory=dict)


class LedgerEntry(LedgerEntrySpec):
    sequence: int
    created_at: datetime | None = None


class LedgerScope(AgentOSModel):
    account_id: str
    project_id: str
    process_id: str = ""
    resource_kind: str = ""
    resource_id: str = ""
    kind: str = ""
    after_sequence: int = 0
    limit: int = 0


class ActionRef(AgentOSModel):
    action_id: str
    account_id: str
    project_id: str


class ActionRiskAssessment(AgentOSModel):
    level: str = ""
    reason: str = ""
    evidence_refs: list[LedgerDataRef] = Field(default_factory=list)
    assessed_at: datetime | None = None


class GovernedActionSpec(AgentOSModel):
    action_id: str
    idempotency_key: str
    account_id: str
    project_id: str
    process_id: str = ""
    resource: ResourceRef | None = None
    kind: str
    intent: str = ""
    requested_by: ActorRef | None = None
    requested_at: datetime | None = None
    dry_run_required: bool = False
    approval_required: bool = False
    risk: ActionRiskAssessment | None = None
    input_refs: list[LedgerDataRef] = Field(default_factory=list)
    compensation_ref: LedgerDataRef | None = None
    metadata: dict[str, str] = Field(default_factory=dict)


class GovernedActionStatus(AgentOSModel):
    action_id: str
    account_id: str
    project_id: str
    process_id: str = ""
    resource: ResourceRef | None = None
    kind: str = ""
    lifecycle_state: str
    dry_run_state: str = ""
    approval_state: str = ""
    execution_state: str = ""
    risk: ActionRiskAssessment | None = None
    reason: str = ""
    updated_at: datetime | None = None


class ActionScope(AgentOSModel):
    account_id: str
    project_id: str
    process_id: str = ""
    resource_kind: str = ""
    resource_id: str = ""
    kind: str = ""
    lifecycle_state: str = ""
    limit: int = 0


class WorksetRef(AgentOSModel):
    workset_id: str
    account_id: str
    project_id: str


class WorksetItemsRef(AgentOSModel):
    kind: str
    uri: str = ""
    artifact_id: str = ""
    media_type: str = ""
    digest: str = ""
    count: int = 0
    metadata: dict[str, str] = Field(default_factory=dict)


class WorksetChunkSpec(AgentOSModel):
    chunk_id: str
    items_ref: WorksetItemsRef
    item_count: int = 0
    concurrency: int = 0


class WorksetPolicy(AgentOSModel):
    max_items: int = 0
    max_chunk_size: int = 0
    max_chunks: int = 0
    max_concurrency: int = 0


class WorksetSpec(AgentOSModel):
    workset_id: str
    idempotency_key: str
    account_id: str
    project_id: str
    process_id: str = ""
    resource: ResourceRef | None = None
    kind: str
    requested_by: ActorRef | None = None
    requested_at: datetime | None = None
    items_ref: WorksetItemsRef
    chunks: list[WorksetChunkSpec] = Field(default_factory=list)
    policy: WorksetPolicy | None = None
    metadata: dict[str, str] = Field(default_factory=dict)


class WorksetProgress(AgentOSModel):
    total_items: int = 0
    completed_items: int = 0
    failed_items: int = 0
    total_chunks: int = 0
    completed_chunks: int = 0
    failed_chunks: int = 0


class WorksetStatus(AgentOSModel):
    workset_id: str
    account_id: str
    project_id: str
    process_id: str = ""
    resource: ResourceRef | None = None
    kind: str = ""
    lifecycle_state: str
    progress: WorksetProgress | None = None
    reason: str = ""
    metadata: dict[str, str] = Field(default_factory=dict)
    updated_at: datetime | None = None


class WorksetScope(AgentOSModel):
    account_id: str
    project_id: str
    process_id: str = ""
    resource_kind: str = ""
    resource_id: str = ""
    kind: str = ""
    lifecycle_state: str = ""
    limit: int = 0


class ResourceProjectionScope(AgentOSModel):
    account_id: str
    project_id: str
    resource_kind: str
    resource_id: str = ""
    lifecycle_state: str = ""
    limit: int = 0


class ResourceProjection(AgentOSModel):
    resource: ResourceRef
    processes: list[ProcessStatus] = Field(default_factory=list)
    ledger: list[LedgerEntry] = Field(default_factory=list)
    actions: list[GovernedActionStatus] = Field(default_factory=list)
    worksets: list[WorksetStatus] = Field(default_factory=list)
    artifacts: list[ArtifactRef] = Field(default_factory=list)
    updated_at: datetime | None = None
    metadata: dict[str, str] = Field(default_factory=dict)


class ResourceProjectionSummary(AgentOSModel):
    resource: ResourceRef
    latest_process_id: str = ""
    latest_lifecycle_state: str = ""
    process_count: int = 0
    action_count: int = 0
    workset_count: int = 0
    ledger_count: int = 0
    updated_at: datetime | None = None
