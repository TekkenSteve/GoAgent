from __future__ import annotations

from typing import Any, TypedDict

from langgraph.graph import END, StateGraph


class AgentState(TypedDict):
    messages: list[dict[str, Any]]
    input: dict[str, Any]


async def assistant_node(state: AgentState) -> AgentState:
    messages = list(state["messages"])
    latest_user = next(
        (message for message in reversed(messages) if message.get("role") == "user"),
        {"content": ""},
    )
    content = f"LangGraph received: {latest_user.get('content', '')}"
    messages.append({"role": "assistant", "content": content})
    return {"messages": messages, "input": state["input"]}


def build_graph():
    graph = StateGraph(AgentState)
    graph.add_node("assistant", assistant_node)
    graph.set_entry_point("assistant")
    graph.add_edge("assistant", END)
    return graph.compile()


async def run_graph(
    messages: list[dict[str, Any]],
    input_payload: dict[str, Any],
) -> dict[str, Any]:
    app = build_graph()
    state = await app.ainvoke({"messages": messages, "input": input_payload})
    assistant = next(
        (message for message in reversed(state["messages"]) if message.get("role") == "assistant"),
        {"content": ""},
    )
    return {
        "content": str(assistant.get("content", "")),
        "messages": state["messages"],
    }
