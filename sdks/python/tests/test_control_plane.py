from __future__ import annotations

import json

import httpx
import pytest

from agentos_sdk import AgentOSAPIError, AgentOSClient, BackendRef, RunStart, Signal
from agentos_sdk.models import (
    AgentOSEvent,
    ControlRequest,
    PlanEventScope,
    PlanRef,
    RunPlanNodeSpec,
    RunPlanSpec,
)


def json_response(status_code: int, payload: object) -> httpx.Response:
    return httpx.Response(
        status_code,
        headers={"content-type": "application/json"},
        content=json.dumps(payload).encode(),
    )


@pytest.mark.asyncio
async def test_runs_start_posts_agentos_contract() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.url.path == "/v1/agentos/runs"
        assert json.loads(request.content) == {
            "run_id": "run-1",
            "account_id": "acct-1",
            "backend": {"kind": "temporal_external", "name": "langgraph-main"},
            "input": {"messages": [{"role": "user", "content": "investigate"}]},
        }
        return json_response(202, {"run_id": "run-1", "lifecycle_state": "created"})

    async with AgentOSClient("http://agentos.local", transport=httpx.MockTransport(handler)) as client:
        status = await client.runs.start(
            RunStart(
                run_id="run-1",
                account_id="acct-1",
                backend=BackendRef(kind="temporal_external", name="langgraph-main"),
                input={"messages": [{"role": "user", "content": "investigate"}]},
            )
        )

    assert len(requests) == 1
    assert status.run_id == "run-1"
    assert status.lifecycle_state == "created"


@pytest.mark.asyncio
async def test_runs_signal_control_and_event_paths() -> None:
    paths: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        paths.append(request.url.path)
        if request.url.path.endswith("/events"):
            return json_response(
                202,
                {"run_id": "run-1", "event_id": "evt-1", "sequence": 7, "duplicate": False},
            )
        return httpx.Response(202)

    async with AgentOSClient("http://agentos.local", transport=httpx.MockTransport(handler)) as client:
        await client.runs.signal("run-1", Signal(type="user.message", payload={"content": "go"}))
        await client.runs.control("run-1", ControlRequest(operation="cancel"))
        result = await client.runs.emit_event(
            "run-1",
            AgentOSEvent(event_id="evt-1", event_type="run.started", source="worker"),
        )

    assert paths == [
        "/v1/agentos/runs/run-1/signals",
        "/v1/agentos/runs/run-1/control",
        "/v1/agentos/runs/run-1/events",
    ]
    assert result.sequence == 7


@pytest.mark.asyncio
async def test_plans_start_status_events_and_schema() -> None:
    paths: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        paths.append(request.url.path)
        match request.url.path:
            case "/v1/agentos/plans":
                return json_response(202, {"plan_id": "plan-1", "lifecycle_state": "running"})
            case "/v1/agentos/plans/plan-1/status":
                assert dict(request.url.params) == {"account_id": "acct-1", "project_id": "proj-1"}
                return json_response(200, {"plan_id": "plan-1", "lifecycle_state": "running"})
            case "/v1/agentos/plans/plan-1/events/history":
                assert dict(request.url.params) == {
                    "account_id": "acct-1",
                    "project_id": "proj-1",
                    "limit": "10",
                }
                return json_response(200, [{"event_id": "evt-1", "event_type": "run.completed"}])
            case "/v1/agentos/plans/schemas/run-plan":
                return json_response(200, {"type": "object"})
        raise AssertionError(request.url.path)

    async with AgentOSClient("http://agentos.local", transport=httpx.MockTransport(handler)) as client:
        status = await client.plans.start(
            RunPlanSpec(
                plan_id="plan-1",
                account_id="acct-1",
                project_id="proj-1",
                nodes=[
                    RunPlanNodeSpec(
                        node_id="investigate",
                        run=RunStart(
                            run_id="run-1",
                            account_id="acct-1",
                            project_id="proj-1",
                            backend=BackendRef(kind="temporal_external", name="langgraph-main"),
                        ),
                    )
                ],
            )
        )
        ref = PlanRef(plan_id="plan-1", account_id="acct-1", project_id="proj-1")
        current = await client.plans.status(ref)
        events = await client.plans.events(
            "plan-1",
            PlanEventScope(account_id="acct-1", project_id="proj-1", limit=10),
        )
        schema = await client.plans.schema("run-plan")

    assert status.plan_id == "plan-1"
    assert current.lifecycle_state == "running"
    assert events[0].event_id == "evt-1"
    assert schema == {"type": "object"}
    assert paths == [
        "/v1/agentos/plans",
        "/v1/agentos/plans/plan-1/status",
        "/v1/agentos/plans/plan-1/events/history",
        "/v1/agentos/plans/schemas/run-plan",
    ]


@pytest.mark.asyncio
async def test_error_raises_api_error() -> None:
    def handler(_: httpx.Request) -> httpx.Response:
        return json_response(400, {"message": "bad run"})

    async with AgentOSClient("http://agentos.local", transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(AgentOSAPIError) as error:
            await client.runs.status("run-1")

    assert error.value.status_code == 400
    assert error.value.detail == "bad run"

