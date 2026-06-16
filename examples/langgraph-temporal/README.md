# LangGraph over Temporal Backend

This example provides a Python LangGraph worker that implements the AgentOS
protocol expected from an external Temporal backend.

GoAgent does not import LangGraph. LangGraph is a `temporal_external` backend:

```text
GoAgent agentos.Runtime
  -> temporal_external backend
  -> Temporal StartWorkflow / SignalWorkflow / CancelWorkflow
  -> worker-python LangGraph workflow
  -> POST /v1/agentos/runs/{run_id}/events
  -> GoAgent EventStore / Subscribe / SSE
```

The example worker lives in `worker-python/` and registers Temporal workflow
type `langgraph.agent.v1`.

## GoAgent Configuration

Set `AGENTFW_TEMPORAL_EXTERNAL_BACKENDS_JSON` to register the LangGraph backend:

```json
[
  {
    "name": "langgraph-main",
    "task_queue": "langgraph-agent-queue",
    "workflow_type": "langgraph.agent.v1",
    "query_type": "agentos_status",
    "signals": {
      "pause": "pause",
      "resume": "resume",
      "cancel": "cancel",
      "defaults": {
        "user.message": "user_input",
        "user.approval": "approval",
        "user.reject": "reject",
        "tool.result": "tool_result",
        "human.feedback": "feedback",
        "config.patch": "config_patch",
        "memory.patch": "memory_patch"
      }
    }
  }
]
```

## Run The Worker

Install dependencies and start the worker:

```sh
cd examples/langgraph-temporal/worker-python
python -m venv .venv
. .venv/bin/activate
pip install -e .

export TEMPORAL_ADDRESS=127.0.0.1:7233
export TEMPORAL_NAMESPACE=default
export LANGGRAPH_TASK_QUEUE=langgraph-agent-queue
export GOAGENT_BASE_URL=http://127.0.0.1:8080

goagent-langgraph-worker
```

The worker implements:

```text
Workflow type: langgraph.agent.v1
Task queue:    langgraph-agent-queue
Query:         agentos_status
Signals:
  user_input
  pause
  resume
  cancel
```

The sample graph is intentionally small. Replace `worker-python/graph.py` with
your real LangGraph graph; keep `workflow.py` as the AgentOS adapter.

Start a run with:

```go
spec := agentos.RunSpec{
    RunID: "run-123",
    ThreadID: "thread-123",
    Backend: agentos.BackendRef{
        Kind: agentos.BackendKindTemporalExternal,
        Name: "langgraph-main",
    },
    Input: map[string]any{
        "messages": []map[string]string{
            {"role": "user", "content": "Plan the migration."},
        },
    },
}
```

GoAgent starts Temporal workflow `langgraph.agent.v1` on task queue
`langgraph-agent-queue` with this input envelope:

```json
{
  "run_id": "run-123",
  "thread_id": "thread-123",
  "backend": {
    "kind": "temporal_external",
    "name": "langgraph-main"
  },
  "input": {
    "messages": [
      {
        "role": "user",
        "content": "Plan the migration."
      }
    ]
  }
}
```

## Signal Contract

GoAgent sends mapped Temporal signals with this payload shape:

```json
{
  "type": "user.message",
  "idempotency_key": "msg-1",
  "payload": {
    "content": "continue"
  },
  "sent_at": "2026-06-16T10:00:00Z"
}
```

## Event Ingest Contract

LangGraph writes normalized events back to GoAgent:

```http
POST /v1/agentos/runs/run-123/events
Content-Type: application/json
```

```json
{
  "event_id": "evt-001",
  "run_id": "run-123",
  "thread_id": "thread-123",
  "event_type": "agent.message.delta",
  "source": "langgraph",
  "timestamp": "2026-06-16T10:00:00Z",
  "payload": {
    "message_id": "msg-1",
    "delta": "hello"
  }
}
```

GoAgent returns the authoritative stream sequence:

```json
{
  "run_id": "run-123",
  "event_id": "evt-001",
  "sequence": 12,
  "duplicate": false
}
```

External sequence numbers are not authoritative. GoAgent assigns sequence on
append. Duplicate `event_id` values for the same run are ignored.

## Supported Standard Events

External backends must emit one of the standard AgentOS event types:

```text
run.started
run.completed
run.failed
run.cancelled
run.paused
run.resumed
agent.step.started
agent.step.completed
agent.step.failed
agent.message.delta
agent.message.completed
tool.call.started
tool.call.delta
tool.call.completed
tool.call.failed
approval.requested
approval.resolved
usage.reported
checkpoint.created
```
