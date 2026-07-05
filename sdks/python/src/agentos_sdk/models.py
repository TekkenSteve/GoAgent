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

