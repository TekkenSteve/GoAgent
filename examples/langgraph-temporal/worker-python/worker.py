import asyncio
import os

from temporalio.client import Client
from temporalio.worker import Worker

from workflow import (
    AgentOSActivities,
    LangGraphAgentWorkflow,
    WorkflowConfig,
)


def env(name: str, default: str) -> str:
    return os.environ.get(name, default)


async def run() -> None:
    temporal_address = env("TEMPORAL_ADDRESS", "127.0.0.1:7233")
    temporal_namespace = env("TEMPORAL_NAMESPACE", "default")
    task_queue = env("LANGGRAPH_TASK_QUEUE", "langgraph-agent-queue")

    client = await Client.connect(temporal_address, namespace=temporal_namespace)
    activities = AgentOSActivities(
        WorkflowConfig(
            goagent_base_url=env("GOAGENT_BASE_URL", "http://127.0.0.1:8080"),
            event_source=env("AGENTOS_EVENT_SOURCE", "langgraph"),
        )
    )

    worker = Worker(
        client,
        task_queue=task_queue,
        workflows=[LangGraphAgentWorkflow],
        activities=[
            activities.emit_event,
            activities.run_langgraph_turn,
        ],
    )
    await worker.run()


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
