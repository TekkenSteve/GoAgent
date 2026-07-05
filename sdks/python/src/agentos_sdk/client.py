from __future__ import annotations

from typing import Any, TypeVar

import httpx
from pydantic import BaseModel, TypeAdapter

from .models import (
    AgentOSEvent,
    AgentOSEventIngestResult,
    Artifact,
    ArtifactRef,
    ControlRequest,
    PlanArtifactScope,
    PlanAuditRecord,
    PlanAuditScope,
    PlanControl,
    PlanDebugTrace,
    PlanEvent,
    PlanEventScope,
    PlanRef,
    PlanSignal,
    RunPlanDescription,
    RunPlanSpec,
    RunPlanStatus,
    RunStart,
    RunStatus,
    Signal,
)

T = TypeVar("T")


class AgentOSAPIError(Exception):
    def __init__(self, status_code: int, detail: str) -> None:
        self.status_code = status_code
        self.detail = detail
        super().__init__(f"AgentOS API {status_code}: {detail}")


class _Transport:
    def __init__(self, http: httpx.AsyncClient) -> None:
        self._http = http

    async def get(self, path: str, model: type[T] | Any = None, **params: Any) -> T | Any:
        response = await self._http.get(path, params=self._query(params))
        return self._decode(response, model)

    async def post(self, path: str, body: BaseModel | dict[str, Any] | None = None, model: type[T] | Any = None) -> T | Any:
        payload = self._body(body)
        response = await self._http.post(path, json=payload)
        return self._decode(response, model)

    @staticmethod
    def _body(body: BaseModel | dict[str, Any] | None) -> dict[str, Any]:
        if body is None:
            return {}
        if isinstance(body, BaseModel):
            return body.model_dump(
                mode="json",
                by_alias=True,
                exclude_defaults=True,
                exclude_none=True,
            )
        return body

    @staticmethod
    def _query(params: dict[str, Any]) -> dict[str, Any]:
        return {key: value for key, value in params.items() if value not in (None, "", 0)}

    @staticmethod
    def _decode(response: httpx.Response, model: type[T] | Any = None) -> T | Any:
        if not response.is_success:
            detail = response.text
            try:
                parsed = response.json()
                if isinstance(parsed, dict):
                    detail = str(parsed.get("message") or parsed.get("detail") or detail)
            except ValueError:
                pass
            raise AgentOSAPIError(response.status_code, detail)
        if response.status_code == 204 or not response.content:
            return None
        data = response.json()
        if model is None:
            return data
        return TypeAdapter(model).validate_python(data)


class RunsClient:
    def __init__(self, transport: _Transport) -> None:
        self._transport = transport

    async def start(self, spec: RunStart) -> RunStatus:
        return await self._transport.post("/v1/agentos/runs", spec, RunStatus)

    async def status(self, run_id: str) -> RunStatus:
        return await self._transport.get(f"/v1/agentos/runs/{run_id}/status", RunStatus)

    async def signal(self, run_id: str, signal: Signal) -> None:
        await self._transport.post(f"/v1/agentos/runs/{run_id}/signals", signal)

    async def control(self, run_id: str, control: ControlRequest) -> None:
        await self._transport.post(f"/v1/agentos/runs/{run_id}/control", control)

    async def emit_event(self, run_id: str, event: AgentOSEvent) -> AgentOSEventIngestResult:
        return await self._transport.post(
            f"/v1/agentos/runs/{run_id}/events",
            event,
            AgentOSEventIngestResult,
        )


class PlansClient:
    def __init__(self, transport: _Transport) -> None:
        self._transport = transport

    async def start(self, spec: RunPlanSpec) -> RunPlanStatus:
        return await self._transport.post("/v1/agentos/plans", spec, RunPlanStatus)

    async def status(self, ref: PlanRef) -> RunPlanStatus:
        return await self._transport.get(
            f"/v1/agentos/plans/{ref.plan_id}/status",
            RunPlanStatus,
            account_id=ref.account_id,
            project_id=ref.project_id,
        )

    async def describe(self, ref: PlanRef) -> RunPlanDescription:
        return await self._transport.get(
            f"/v1/agentos/plans/{ref.plan_id}/description",
            RunPlanDescription,
            account_id=ref.account_id,
            project_id=ref.project_id,
        )

    async def signal(self, plan_id: str, signal: PlanSignal) -> None:
        await self._transport.post(f"/v1/agentos/plans/{plan_id}/signals", signal)

    async def control(self, plan_id: str, control: PlanControl) -> None:
        await self._transport.post(f"/v1/agentos/plans/{plan_id}/control", control)

    async def events(self, plan_id: str, scope: PlanEventScope) -> list[PlanEvent]:
        return await self._transport.get(
            f"/v1/agentos/plans/{plan_id}/events/history",
            list[PlanEvent],
            **scope.model_dump(mode="json"),
        )

    async def debug_traces(self, plan_id: str, scope: PlanEventScope) -> list[PlanDebugTrace]:
        return await self._transport.get(
            f"/v1/agentos/plans/{plan_id}/debug/traces",
            list[PlanDebugTrace],
            **scope.model_dump(mode="json"),
        )

    async def audits(self, plan_id: str, scope: PlanAuditScope) -> list[PlanAuditRecord]:
        return await self._transport.get(
            f"/v1/agentos/plans/{plan_id}/audits",
            list[PlanAuditRecord],
            **scope.model_dump(mode="json"),
        )

    async def artifacts(self, plan_id: str, scope: PlanArtifactScope) -> list[ArtifactRef]:
        return await self._transport.get(
            f"/v1/agentos/plans/{plan_id}/artifacts",
            list[ArtifactRef],
            **scope.model_dump(mode="json"),
        )

    async def artifact(self, ref: PlanRef, artifact_id: str) -> Artifact:
        return await self._transport.get(
            f"/v1/agentos/plans/{ref.plan_id}/artifacts/{artifact_id}",
            Artifact,
            account_id=ref.account_id,
            project_id=ref.project_id,
        )

    async def schema(self, kind: str) -> dict[str, Any]:
        return await self._transport.get(f"/v1/agentos/plans/schemas/{kind}")


class AgentOSClient:
    def __init__(
        self,
        base_url: str,
        token: str = "",
        timeout: float = 30.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        headers = {"Accept": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        self._http = httpx.AsyncClient(
            base_url=base_url.rstrip("/"),
            headers=headers,
            timeout=timeout,
            transport=transport,
        )
        self._transport = _Transport(self._http)
        self.runs = RunsClient(self._transport)
        self.plans = PlansClient(self._transport)

    async def __aenter__(self) -> AgentOSClient:
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await self._http.aclose()
