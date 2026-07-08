from __future__ import annotations

import json

import httpx
import pytest

from agentos_sdk import (
    ActionApprovalDecision,
    ActionCancelRequest,
    ActionDryRunResult,
    ActionExecutionResult,
    ActionRef,
    ActionScope,
    AgentOSClient,
    GovernedActionSpec,
    LedgerEntrySpec,
    LedgerScope,
    ProcessRef,
    ProcessScope,
    ProcessSpec,
    ResourceProjectionScope,
    ResourceRef,
    WorksetItemsRef,
    WorksetRef,
    WorksetScope,
    WorksetSpec,
)


def json_response(status_code: int, payload: object) -> httpx.Response:
    return httpx.Response(
        status_code,
        headers={"content-type": "application/json"},
        content=json.dumps(payload).encode(),
    )


@pytest.mark.asyncio
async def test_process_client_posts_and_queries_tenant_scope() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        match request.url.path:
            case "/v1/agentos/processes":
                if request.method == "POST":
                    assert json.loads(request.content)["idempotency_key"] == "process-start-1"
                    return json_response(
                        202,
                        {
                            "process_id": "process-1",
                            "kind": "aisoc.investigation",
                            "lifecycle_state": "running",
                        },
                    )
                assert dict(request.url.params) == {
                    "account_id": "acct-1",
                    "project_id": "proj-1",
                    "resource_kind": "alert",
                    "kind": "aisoc.investigation",
                    "limit": "5",
                }
                return json_response(200, [{"process_id": "process-1", "lifecycle_state": "running"}])
            case "/v1/agentos/processes/process-1/status":
                assert dict(request.url.params) == {"account_id": "acct-1", "project_id": "proj-1"}
                return json_response(200, {"process_id": "process-1", "lifecycle_state": "running"})
            case "/v1/agentos/processes/process-1":
                assert dict(request.url.params) == {"account_id": "acct-1", "project_id": "proj-1"}
                return json_response(
                    200,
                    {
                        "process_id": "process-1",
                        "kind": "aisoc.investigation",
                        "account_id": "acct-1",
                        "project_id": "proj-1",
                        "resource": resource_payload(),
                        "status": {"process_id": "process-1", "lifecycle_state": "running"},
                    },
                )
        raise AssertionError(request.url.path)

    resource = ResourceRef(
        kind="alert",
        resource_id="alert-1",
        account_id="acct-1",
        project_id="proj-1",
    )
    async with AgentOSClient("http://agentos.local", transport=httpx.MockTransport(handler)) as client:
        started = await client.processes.start(
            ProcessSpec(
                process_id="process-1",
                kind="aisoc.investigation",
                account_id="acct-1",
                project_id="proj-1",
                idempotency_key="process-start-1",
                resource=resource,
            )
        )
        listed = await client.processes.list(
            ProcessScope(
                account_id="acct-1",
                project_id="proj-1",
                resource_kind="alert",
                kind="aisoc.investigation",
                limit=5,
            )
        )
        ref = ProcessRef(process_id="process-1", account_id="acct-1", project_id="proj-1")
        status = await client.processes.status(ref)
        description = await client.processes.describe(ref)

    assert started.lifecycle_state == "running"
    assert listed[0].process_id == "process-1"
    assert status.process_id == "process-1"
    assert description.resource.resource_id == "alert-1"
    assert [request.url.path for request in requests] == [
        "/v1/agentos/processes",
        "/v1/agentos/processes",
        "/v1/agentos/processes/process-1/status",
        "/v1/agentos/processes/process-1",
    ]


@pytest.mark.asyncio
async def test_ledger_action_workset_clients_preserve_scopes_and_keys() -> None:
    seen: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.method, request.url.path))
        match request.url.path:
            case "/v1/agentos/ledger":
                if request.method == "POST":
                    assert json.loads(request.content)["idempotency_key"] == "ledger-append-1"
                    return json_response(201, ledger_payload())
                assert dict(request.url.params) == {
                    "account_id": "acct-1",
                    "project_id": "proj-1",
                    "process_id": "process-1",
                    "kind": "evidence",
                    "after_sequence": "6",
                    "limit": "10",
                }
                return json_response(200, [ledger_payload()])
            case "/v1/agentos/actions":
                if request.method == "POST":
                    assert json.loads(request.content)["idempotency_key"] == "action-request-1"
                    return json_response(
                        202,
                        {"action_id": "action-1", "account_id": "acct-1", "project_id": "proj-1", "lifecycle_state": "waiting_approval"},
                    )
                assert dict(request.url.params) == {
                    "account_id": "acct-1",
                    "project_id": "proj-1",
                    "kind": "isolate_host",
                    "lifecycle_state": "waiting_approval",
                }
                return json_response(200, [{"action_id": "action-1", "account_id": "acct-1", "project_id": "proj-1", "lifecycle_state": "waiting_approval"}])
            case "/v1/agentos/actions/action-1":
                assert dict(request.url.params) == {"account_id": "acct-1", "project_id": "proj-1"}
                return json_response(200, {"action_id": "action-1", "account_id": "acct-1", "project_id": "proj-1", "lifecycle_state": "waiting_approval"})
            case "/v1/agentos/actions/action-1/dry-run":
                assert dict(request.url.params) == {"account_id": "acct-1", "project_id": "proj-1"}
                assert json.loads(request.content)["idempotency_key"] == "action-dry-run-1"
                return json_response(200, {"action_id": "action-1", "account_id": "acct-1", "project_id": "proj-1", "lifecycle_state": "waiting_approval"})
            case "/v1/agentos/actions/action-1/approval":
                assert dict(request.url.params) == {"account_id": "acct-1", "project_id": "proj-1"}
                assert json.loads(request.content)["idempotency_key"] == "action-approval-1"
                return json_response(200, {"action_id": "action-1", "account_id": "acct-1", "project_id": "proj-1", "lifecycle_state": "ready"})
            case "/v1/agentos/actions/action-1/execution":
                assert dict(request.url.params) == {"account_id": "acct-1", "project_id": "proj-1"}
                assert json.loads(request.content)["idempotency_key"] == "action-execution-1"
                return json_response(200, {"action_id": "action-1", "account_id": "acct-1", "project_id": "proj-1", "lifecycle_state": "executed"})
            case "/v1/agentos/actions/action-1/cancel":
                assert dict(request.url.params) == {"account_id": "acct-1", "project_id": "proj-1"}
                assert json.loads(request.content)["idempotency_key"] == "action-cancel-1"
                return json_response(200, {"action_id": "action-1", "account_id": "acct-1", "project_id": "proj-1", "lifecycle_state": "canceled"})
            case "/v1/agentos/worksets":
                if request.method == "POST":
                    assert json.loads(request.content)["idempotency_key"] == "workset-start-1"
                    return json_response(202, {"workset_id": "workset-1", "account_id": "acct-1", "project_id": "proj-1", "lifecycle_state": "running"})
                assert dict(request.url.params) == {
                    "account_id": "acct-1",
                    "project_id": "proj-1",
                    "kind": "hunt.backfill",
                    "limit": "2",
                }
                return json_response(200, [{"workset_id": "workset-1", "account_id": "acct-1", "project_id": "proj-1", "lifecycle_state": "running"}])
            case "/v1/agentos/worksets/workset-1":
                assert dict(request.url.params) == {"account_id": "acct-1", "project_id": "proj-1"}
                return json_response(200, {"workset_id": "workset-1", "account_id": "acct-1", "project_id": "proj-1", "lifecycle_state": "running"})
        raise AssertionError(request.url.path)

    async with AgentOSClient("http://agentos.local", transport=httpx.MockTransport(handler)) as client:
        entry = await client.ledger.append(
            LedgerEntrySpec(
                entry_id="entry-1",
                idempotency_key="ledger-append-1",
                account_id="acct-1",
                project_id="proj-1",
                process_id="process-1",
                kind="evidence",
            )
        )
        entries = await client.ledger.list(
            LedgerScope(
                account_id="acct-1",
                project_id="proj-1",
                process_id="process-1",
                kind="evidence",
                after_sequence=6,
                limit=10,
            )
        )
        action = await client.actions.request(
            GovernedActionSpec(
                action_id="action-1",
                idempotency_key="action-request-1",
                account_id="acct-1",
                project_id="proj-1",
                process_id="process-1",
                kind="isolate_host",
            )
        )
        actions = await client.actions.list(
            ActionScope(
                account_id="acct-1",
                project_id="proj-1",
                kind="isolate_host",
                lifecycle_state="waiting_approval",
            )
        )
        action_status = await client.actions.status(
            ActionRef(action_id="action-1", account_id="acct-1", project_id="proj-1")
        )
        action_ref = ActionRef(action_id="action-1", account_id="acct-1", project_id="proj-1")
        dry_run_status = await client.actions.record_dry_run(
            action_ref,
            ActionDryRunResult(idempotency_key="action-dry-run-1", succeeded=True),
        )
        approval_status = await client.actions.resolve_approval(
            action_ref,
            ActionApprovalDecision(idempotency_key="action-approval-1", approved=True),
        )
        execution_status = await client.actions.complete(
            action_ref,
            ActionExecutionResult(idempotency_key="action-execution-1", succeeded=True),
        )
        cancel_status = await client.actions.cancel(
            action_ref,
            ActionCancelRequest(idempotency_key="action-cancel-1", reason="operator canceled"),
        )
        workset = await client.worksets.start(
            WorksetSpec(
                workset_id="workset-1",
                idempotency_key="workset-start-1",
                account_id="acct-1",
                project_id="proj-1",
                process_id="process-1",
                kind="hunt.backfill",
                items_ref=WorksetItemsRef(kind="object", uri="s3://worksets/1.jsonl"),
            )
        )
        worksets = await client.worksets.list(
            WorksetScope(account_id="acct-1", project_id="proj-1", kind="hunt.backfill", limit=2)
        )
        workset_status = await client.worksets.status(
            WorksetRef(workset_id="workset-1", account_id="acct-1", project_id="proj-1")
        )

    assert entry.sequence == 7
    assert entries[0].entry_id == "entry-1"
    assert action.action_id == actions[0].action_id == action_status.action_id
    assert dry_run_status.action_id == approval_status.action_id == execution_status.action_id == cancel_status.action_id
    assert workset.workset_id == worksets[0].workset_id == workset_status.workset_id
    assert seen == [
        ("POST", "/v1/agentos/ledger"),
        ("GET", "/v1/agentos/ledger"),
        ("POST", "/v1/agentos/actions"),
        ("GET", "/v1/agentos/actions"),
        ("GET", "/v1/agentos/actions/action-1"),
        ("POST", "/v1/agentos/actions/action-1/dry-run"),
        ("POST", "/v1/agentos/actions/action-1/approval"),
        ("POST", "/v1/agentos/actions/action-1/execution"),
        ("POST", "/v1/agentos/actions/action-1/cancel"),
        ("POST", "/v1/agentos/worksets"),
        ("GET", "/v1/agentos/worksets"),
        ("GET", "/v1/agentos/worksets/workset-1"),
    ]


@pytest.mark.asyncio
async def test_resources_client_gets_projection_and_lists_summaries() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v1/agentos/resources"
        params = dict(request.url.params)
        if "resource_id" in params:
            assert params == {
                "account_id": "acct-1",
                "project_id": "proj-1",
                "resource_kind": "alert",
                "resource_id": "alert-1",
                "limit": "3",
            }
            return json_response(200, {"resource": resource_payload()})
        assert params == {
            "account_id": "acct-1",
            "project_id": "proj-1",
            "resource_kind": "alert",
            "lifecycle_state": "running",
            "limit": "20",
        }
        return json_response(
            200,
            [{"resource": resource_payload(), "latest_process_id": "process-1", "process_count": 1}],
        )

    async with AgentOSClient("http://agentos.local", transport=httpx.MockTransport(handler)) as client:
        projection = await client.resources.get(
            ResourceProjectionScope(
                account_id="acct-1",
                project_id="proj-1",
                resource_kind="alert",
                resource_id="alert-1",
                limit=3,
            )
        )
        summaries = await client.resources.list(
            ResourceProjectionScope(
                account_id="acct-1",
                project_id="proj-1",
                resource_kind="alert",
                lifecycle_state="running",
                limit=20,
            )
        )

    assert projection.resource.resource_id == "alert-1"
    assert summaries[0].latest_process_id == "process-1"


def resource_payload() -> dict[str, str]:
    return {
        "kind": "alert",
        "resource_id": "alert-1",
        "account_id": "acct-1",
        "project_id": "proj-1",
    }


def ledger_payload() -> dict[str, object]:
    return {
        "entry_id": "entry-1",
        "idempotency_key": "ledger-append-1",
        "account_id": "acct-1",
        "project_id": "proj-1",
        "process_id": "process-1",
        "kind": "evidence",
        "sequence": 7,
    }
