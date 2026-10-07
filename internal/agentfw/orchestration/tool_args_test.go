package orchestration

import (
	"encoding/json"
	"strings"
	"testing"
)

// The arguments a model emits must reach the tool unchanged. They travel as the
// JSON string the model produced, because decoding into a map and re-marshaling
// rewrites number literals — an id beyond float64 precision comes back wrong,
// and a tool keyed on it addresses someone else's record.

func TestToolInputToolCallCarriesArgumentsVerbatim(t *testing.T) {
	t.Parallel()

	// 9007199254740993 is the first integer float64 cannot represent.
	args := `{"user_id":9007199254740993,"q":"weather in 上海"}`

	input := ToolInput{
		AccountID:  "acct-1",
		RunID:      "run-1",
		ToolCallID: "call-1",
		ToolName:   "search",
		Args:       args,
	}

	call := input.toolCall()

	if call.Function.Arguments != args {
		t.Fatalf("arguments were altered on the way to the tool:\n sent: %s\ngot:  %s", args, call.Function.Arguments)
	}

	if call.ID != input.ToolCallID || call.Function.Name != input.ToolName {
		t.Fatalf("tool call identity lost: got id=%q name=%q", call.ID, call.Function.Name)
	}

	if call.Type != "function" {
		t.Fatalf("expected a function call, got %q", call.Type)
	}
}

func TestToolInputToolCallEmptyArgumentsStayEmpty(t *testing.T) {
	t.Parallel()

	input := ToolInput{ToolCallID: "call-1", ToolName: "no_args_tool", Args: ""}

	if got := input.toolCall().Function.Arguments; got != "" {
		t.Fatalf("absent arguments must stay absent, got %q", got)
	}
}

func TestMarshalToolArgsDeterministicAndSorted(t *testing.T) {
	t.Parallel()

	args := map[string]any{"z_last": 1, "a_first": "two", "nested": map[string]any{"k": true}}

	got, err := marshalToolArgs(args)
	if err != nil {
		t.Fatal(err)
	}

	again, err := marshalToolArgs(args)
	if err != nil {
		t.Fatal(err)
	}

	if got != again {
		t.Fatalf("marshaled arguments are not deterministic:\nfirst:  %s\nsecond: %s", got, again)
	}

	// Key order must not depend on map iteration, or workflow replay breaks.
	if !strings.HasPrefix(got, `{"a_first"`) {
		t.Fatalf("expected sorted keys, got %s", got)
	}

	var round map[string]any
	if err := json.Unmarshal([]byte(got), &round); err != nil {
		t.Fatalf("marshaled arguments are not valid JSON: %v", err)
	}
}

func TestMarshalToolArgsNilIsNoArguments(t *testing.T) {
	t.Parallel()

	got, err := marshalToolArgs(nil)
	if err != nil {
		t.Fatal(err)
	}

	if got != "" {
		t.Fatalf("a step without input has no arguments, got %q", got)
	}
}

func TestMarshalToolArgsEmptyObjectIsAnEmptyObject(t *testing.T) {
	t.Parallel()

	got, err := marshalToolArgs(map[string]any{})
	if err != nil {
		t.Fatal(err)
	}

	if got != "{}" {
		t.Fatalf("an explicitly empty input is an empty object, got %q", got)
	}
}

func TestMarshalToolArgsRejectsUnserializableInput(t *testing.T) {
	t.Parallel()

	// Channels cannot cross a tool boundary; failing loudly here is better
	// than a tool executing with silently dropped arguments.
	if _, err := marshalToolArgs(map[string]any{"bad": make(chan int)}); err == nil {
		t.Fatal("expected an error for unserializable step input")
	}
}

func TestMarshalToolArgsRoundTripsAuthoredNumbers(t *testing.T) {
	t.Parallel()

	// Authored ints stay int64 through the one serialization; only a
	// decode-and-remarshal round-trip would corrupt them.
	const big = int64(9007199254740993)

	got, err := marshalToolArgs(map[string]any{"user_id": big})
	if err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(got, `"user_id":9007199254740993`) {
		t.Fatalf("authored int64 altered: %s", got)
	}
}
