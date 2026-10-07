package orchestration

import (
	"testing"

	"github.com/TekkenSteve/GoAgent/internal/entity"
	"github.com/stretchr/testify/require"
)

// The native backend's payload is its own, and it lives under its own key
// inside the control plane's generic Input. These tests state what it accepts
// and, more importantly, what it refuses: a payload it does not understand must
// not be quietly ignored, because the run would then execute something other
// than what the caller wrote.

func nativeInput(payload map[string]any) map[string]any {
	return map[string]any{RunInputKey: payload}
}

func TestDecodeStepQueueInputAcceptsAStepQueue(t *testing.T) {
	t.Parallel()

	decoded, err := DecodeStepQueueInput(nativeInput(map[string]any{
		RunInputSteps: []any{
			map[string]any{"id": "research", "type": "agent", "input": map[string]any{"message": "go"}},
			map[string]any{"id": "check", "type": "eval", "depends_on": []any{"research"}},
		},
		RunInputMaxDepth:       float64(2),
		RunInputContinuePolicy: map[string]any{"max_rounds": float64(10)},
	}))

	require.NoError(t, err)
	require.Len(t, decoded.Steps, 2)
	require.Equal(t, "research", decoded.Steps[0].ID)
	require.Equal(t, []string{"research"}, decoded.Steps[1].DependsOn)
	require.Equal(t, 2, decoded.MaxDepth)
	require.Equal(t, 10, decoded.ContinuePolicy.MaxRounds)
	require.Nil(t, decoded.TeamSpec)
}

func TestDecodeStepQueueInputAcceptsATeam(t *testing.T) {
	t.Parallel()

	decoded, err := DecodeStepQueueInput(nativeInput(map[string]any{
		RunInputTeamSpec: map[string]any{
			"id":   "team-1",
			"name": "Team",
			"agents": []any{
				map[string]any{"id": "researcher", "name": "Researcher", "model_ref": "gpt-4.1-mini"},
			},
			"steps": []any{
				map[string]any{"id": "research", "type": "agent", "agent_ref": "researcher"},
			},
		},
	}))

	require.NoError(t, err)
	require.NotNil(t, decoded.TeamSpec)
	require.Equal(t, "team-1", decoded.TeamSpec.ID)
	require.Empty(t, decoded.Steps, "a team is expanded by the backend, not by the caller")
}

// No payload is not a step queue: it is a caller asking for the default
// single-agent loop. A payload under another key is another layer's business,
// not a mistake to report.
func TestDecodeStepQueueInputTreatsAnAbsentPayloadAsNoStepQueue(t *testing.T) {
	t.Parallel()

	cases := map[string]map[string]any{
		"nil":              nil,
		"empty":            {},
		"another key":      {"purpose": "conformance"},
		"nil native value": {RunInputKey: nil},
	}

	for name, input := range cases {
		decoded, err := DecodeStepQueueInput(input)
		require.NoError(t, err, name)
		require.Nil(t, decoded, name)
	}
}

func TestDecodeStepQueueInputRefusesWhatItCannotRun(t *testing.T) {
	t.Parallel()

	cases := map[string]map[string]any{
		"unknown key": nativeInput(map[string]any{
			"stepps": []any{map[string]any{"id": "x", "type": "agent"}},
		}),
		"both ways at once": nativeInput(map[string]any{
			RunInputSteps:    []any{map[string]any{"id": "x", "type": "agent"}},
			RunInputTeamSpec: map[string]any{"id": "team-1"},
		}),
		"empty queue": nativeInput(map[string]any{
			RunInputSteps: []any{},
		}),
		"steps of the wrong shape": nativeInput(map[string]any{
			RunInputSteps: "research",
		}),
		"negative depth": nativeInput(map[string]any{
			RunInputSteps:    []any{map[string]any{"id": "x", "type": "agent"}},
			RunInputMaxDepth: float64(-1),
		}),
		"fractional depth": nativeInput(map[string]any{
			RunInputSteps:    []any{map[string]any{"id": "x", "type": "agent"}},
			RunInputMaxDepth: 1.5,
		}),
		"payload of the wrong shape": {RunInputKey: "steps"},
	}

	for name, input := range cases {
		decoded, err := DecodeStepQueueInput(input)

		require.Error(t, err, name)
		require.ErrorIs(t, err, ErrInvalidRunInput, name)
		require.Nil(t, decoded, name)
	}
}

// The refusal names the accepted set, so the caller can fix the payload without
// reading this file.
func TestDecodeStepQueueInputNamesTheAcceptedKeys(t *testing.T) {
	t.Parallel()

	_, err := DecodeStepQueueInput(nativeInput(map[string]any{"stepps": []any{}}))

	require.Error(t, err)
	require.Contains(t, err.Error(), "stepps")

	for _, key := range RunInputKeys() {
		require.Contains(t, err.Error(), key)
	}
}

// A team is expanded by the backend, so a spec that only names a team is a run
// the backend can start.
func TestStepQueueInputExpandsIntoAQueue(t *testing.T) {
	t.Parallel()

	decoded, err := DecodeStepQueueInput(nativeInput(map[string]any{
		RunInputTeamSpec: map[string]any{
			"id":     "team-1",
			"agents": []any{map[string]any{"id": "worker", "name": "Worker"}},
			"steps":  []any{map[string]any{"id": "work", "type": string(entity.StepAgent), "agent_ref": "worker"}},
		},
	}))
	require.NoError(t, err)

	request := &entity.ExecuteRequest{TeamSpec: decoded.TeamSpec}

	require.True(t, request.HasStepQueue(), "a team selects the step-queue mode")
	require.False(t, (&entity.ExecuteRequest{}).HasStepQueue())
	require.True(t, (&entity.ExecuteRequest{Steps: []entity.Step{{ID: "x"}}}).HasStepQueue())
}
