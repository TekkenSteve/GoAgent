# HTTP AgentOS Backend

HTTP backends let GoAgent control agent runtimes that are not Temporal workers,
for example Claude Code, Codex, opencode, or a custom Python service wrapped by
a small HTTP server.

GoAgent treats the remote service as an AgentOS backend:

```text
GoAgent agentos.Runtime
  -> http backend
  -> POST /runs
  -> POST /runs/{run_id}/signals
  -> POST /runs/{run_id}/control
  -> GET  /runs/{run_id}/status
  -> remote runtime POSTs events back to GoAgent ingest
```

## GoAgent Configuration

Register HTTP backends with `AGENTFW_HTTP_BACKENDS_JSON`:

```json
[
  {
    "name": "claude-code",
    "endpoint": "http://claude-code-runtime:8080",
    "headers": {
      "Authorization": "Bearer ${AGENT_RUNTIME_TOKEN}"
    }
  }
]
```

Start a run with:

```go
spec := agentos.RunSpec{
    RunID: "run-123",
    ThreadID: "thread-123",
    Backend: agentos.BackendRef{
        Kind: agentos.BackendKindHTTP,
        Name: "claude-code",
    },
    Input: map[string]any{
        "prompt": "Modify the repository according to the task.",
        "workspace": "/workspace/project",
    },
}
```

## Remote Runtime Contract

### Start

```http
POST /runs
Content-Type: application/json
```

The request body is the AgentOS run envelope:

```json
{
  "run_id": "run-123",
  "thread_id": "thread-123",
  "backend": {"kind": "http", "name": "claude-code"},
  "input": {
    "prompt": "Modify the repository according to the task.",
    "workspace": "/workspace/project"
  }
}
```

Return:

```json
{
  "run_id": "run-123",
  "lifecycle_state": "created",
  "updated_at": "2026-06-16T10:00:00Z"
}
```

### Signal

```http
POST /runs/run-123/signals
Content-Type: application/json
```

```json
{
  "type": "user.message",
  "idempotency_key": "msg-1",
  "payload": {
    "content": "continue with the migration"
  },
  "sent_at": "2026-06-16T10:01:00Z"
}
```

### Control

```http
POST /runs/run-123/control
Content-Type: application/json
```

```json
{
  "operation": "cancel"
}
```

### Status

```http
GET /runs/run-123/status
```

```json
{
  "run_id": "run-123",
  "lifecycle_state": "running",
  "step": 3,
  "updated_at": "2026-06-16T10:02:00Z"
}
```

## Event Ingest

The remote runtime should write events back to GoAgent:

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
  "source": "claude-code",
  "timestamp": "2026-06-16T10:02:00Z",
  "payload": {
    "delta": "updated the files"
  }
}
```
