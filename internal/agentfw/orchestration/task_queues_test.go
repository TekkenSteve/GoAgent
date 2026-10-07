package orchestration

func testWorkflowTaskQueues() WorkflowTaskQueues {
	return WorkflowTaskQueues{
		NativeControl: "test-native-control",
		NativeLLM:     "test-native-llm",
		NativeTool:    "test-native-tool",
		Stream:        "test-stream",
	}
}
