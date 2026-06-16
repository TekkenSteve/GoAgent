from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any
from uuid import uuid4

import httpx
from temporalio import activity, workflow

with workflow.unsafe.imports_passed_through():
    from graph import run_graph


WORKFLOW_TYPE = "langgraph.agent.v1"
STATUS_QUERY = "agentos_status"


@dataclass
class BackendRef:
    kind: str
    name: str


@dataclass
class StartInput:
    run_id: str
    thread_id: str | None = None
    account_id: str | None = None
    project_id: str | None = None
    agent_id: str | None = None
    model_ref: str | None = None
    system_prompt: str | None = None
    user_message: str | None = None
    idempotency_key: str | None = None
    requested_at: str | None = None
    metadata: dict[str, str] | None = None
    backend: BackendRef | None = None
    input: dict[str, Any] = field(default_factory=dict)


@dataclass
class SignalInput:
    type: str
    idempotency_key: str | None = None
    payload: dict[str, Any] = field(default_factory=dict)
    sent_at: str | None = None


@dataclass
class RunStatus:
    run_id: str
    lifecycle_state: str
    step: int = 0
    reason: str = ""
    updated_at: str = ""


@dataclass
class EventInput:
    run_id: str
    thread_id: str
    event_type: str
    source: str
    payload: dict[str, Any]
    event_id: str = ""


@dataclass
class GraphTurnInput:
    run_id: str
    thread_id: str
    messages: list[dict[str, Any]]
    input: dict[str, Any]


@dataclass
class GraphTurnOutput:
    content: str
    messages: list[dict[str, Any]]


@dataclass(frozen=True)
class WorkflowConfig:
    goagent_base_url: str
    event_source: str


class AgentOSActivities:
    def __init__(self, config: WorkflowConfig) -> None:
        self._config = config

    @activity.defn
    async def emit_event(self, event: EventInput) -> None:
        event_id = event.event_id or f"evt-{uuid4()}"
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.post(
                f"{self._config.goagent_base_url}/v1/agentos/runs/{event.run_id}/events",
                json={
                    "event_id": event_id,
                    "run_id": event.run_id,
                    "thread_id": event.thread_id,
                    "event_type": event.event_type,
                    "source": event.source,
                    "payload": event.payload,
                },
            )
            response.raise_for_status()

    @activity.defn
    async def run_langgraph_turn(self, turn: GraphTurnInput) -> GraphTurnOutput:
        result = await run_graph(turn.messages, turn.input)
        return GraphTurnOutput(
            content=result["content"],
            messages=result["messages"],
        )


@workflow.defn(name=WORKFLOW_TYPE)
class LangGraphAgentWorkflow:
    def __init__(self) -> None:
        self._status = RunStatus(run_id="", lifecycle_state="created")
        self._paused = False
        self._cancelled = False
        self._messages: list[dict[str, Any]] = []
        self._pending_user_messages: list[SignalInput] = []
        self._input: StartInput | None = None

    @workflow.run
    async def run(self, start: StartInput) -> RunStatus:
        self._input = start
        thread_id = start.thread_id or start.run_id
        self._status = RunStatus(
            run_id=start.run_id,
            lifecycle_state="running",
            updated_at=workflow.now().isoformat(),
        )
        self._messages = self._initial_messages(start)

        await self._emit("run.started", {"backend": "langgraph"})

        while not self._cancelled:
            await workflow.wait_condition(lambda: not self._paused or self._cancelled)
            if self._cancelled:
                break

            self._status.step += 1
            await self._emit("agent.step.started", {"step": self._status.step})
            result = await workflow.execute_activity_method(
                AgentOSActivities.run_langgraph_turn,
                GraphTurnInput(
                    run_id=start.run_id,
                    thread_id=thread_id,
                    messages=self._messages,
                    input=start.input,
                ),
                start_to_close_timeout=timedelta(minutes=5),
            )
            self._messages = result.messages
            await self._emit(
                "agent.message.completed",
                {"role": "assistant", "content": result.content},
            )
            await self._emit("agent.step.completed", {"step": self._status.step})

            if not self._pending_user_messages:
                self._status.lifecycle_state = "completed"
                self._status.updated_at = workflow.now().isoformat()
                await self._emit("run.completed", {"step": self._status.step})
                return self._status

            next_message = self._pending_user_messages.pop(0)
            self._messages.append(
                {"role": "user", "content": str(next_message.payload.get("content", ""))}
            )

        self._status.lifecycle_state = "cancelled"
        self._status.updated_at = workflow.now().isoformat()
        await self._emit("run.cancelled", {"reason": self._status.reason})
        return self._status

    @workflow.signal(name="user_input")
    async def user_input(self, signal: SignalInput) -> None:
        if self._status.lifecycle_state == "completed":
            self._status.lifecycle_state = "running"
        self._pending_user_messages.append(signal)

    @workflow.signal(name="pause")
    async def pause(self, _: SignalInput | None = None) -> None:
        self._paused = True
        self._status.lifecycle_state = "paused"
        self._status.updated_at = workflow.now().isoformat()
        await self._emit("run.paused", {})

    @workflow.signal(name="resume")
    async def resume(self, _: SignalInput | None = None) -> None:
        self._paused = False
        self._status.lifecycle_state = "running"
        self._status.updated_at = workflow.now().isoformat()
        await self._emit("run.resumed", {})

    @workflow.signal(name="cancel")
    async def cancel(self, signal: SignalInput | None = None) -> None:
        self._cancelled = True
        self._paused = False
        if signal is not None:
            self._status.reason = signal.payload.get("reason", "")

    @workflow.query(name=STATUS_QUERY)
    def agentos_status(self) -> RunStatus:
        return self._status

    async def _emit(self, event_type: str, payload: dict[str, Any]) -> None:
        if self._input is None:
            return
        await workflow.execute_activity_method(
            AgentOSActivities.emit_event,
            EventInput(
                run_id=self._input.run_id,
                thread_id=self._input.thread_id or self._input.run_id,
                event_type=event_type,
                source="langgraph",
                payload=payload,
            ),
            start_to_close_timeout=timedelta(seconds=15),
        )

    def _initial_messages(self, start: StartInput) -> list[dict[str, Any]]:
        messages = list(start.input.get("messages", []))
        if start.system_prompt:
            messages.insert(0, {"role": "system", "content": start.system_prompt})
        if start.user_message:
            messages.append({"role": "user", "content": start.user_message})
        return messages
