# agentos-sdk

Typed async Python SDK for AgentOS.

```python
from agentos_sdk import AgentOSClient, BackendRef, RunStart

async with AgentOSClient(base_url="http://127.0.0.1:8080") as client:
    status = await client.runs.start(
        RunStart(
            run_id="run-1",
            account_id="acct-1",
            project_id="proj-1",
            backend=BackendRef(kind="temporal_external", name="langgraph-main"),
            input={"messages": [{"role": "user", "content": "Investigate this alert"}]},
        )
    )
```

The SDK intentionally mirrors AgentOS public JSON contracts. It does not accept
alias field names or compatibility fallbacks.

