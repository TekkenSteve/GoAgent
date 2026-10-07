package orchestration

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"slices"
	"sort"
	"strings"

	"github.com/TekkenSteve/GoAgent/internal/entity"
)

// RunInputKey names the native backend's own payload inside control.RunSpec.Input.
//
// Input belongs to the control plane and is carried opaquely: a caller may put
// its own metadata there, and another backend may look for its own key. So the
// native backend reads exactly this key and leaves the rest alone — one key per
// backend, and no backend guessing at another's keys.
const RunInputKey = "native"

// The keys the native backend reads inside its own payload.
//
// They live here, next to the executor that consumes them, so the accepted set
// and the code that acts on it cannot drift apart. Anything else under
// RunInputKey is refused: this payload is entirely the backend's, so a
// misspelled key is a mistake, not another layer's business.
const (
	RunInputSteps          = "steps"
	RunInputTeamSpec       = "team_spec"
	RunInputMaxDepth       = "max_depth"
	RunInputContinuePolicy = "continue_policy"
)

// RunInputKeys is the accepted set. A payload with anything else is refused:
// silently ignoring a misspelled key would run a queue the caller did not
// author, which is worse than refusing to start.
func RunInputKeys() []string {
	return []string{RunInputSteps, RunInputTeamSpec, RunInputMaxDepth, RunInputContinuePolicy}
}

// StepQueueInput is a decoded step-queue request: the queue to execute, or the
// team the backend expands into one.
type StepQueueInput struct {
	Steps          []entity.Step
	TeamSpec       *entity.TeamSpec
	MaxDepth       int
	ContinuePolicy entity.ContinuePolicy
}

// DecodeStepQueueInput reads the native backend's payload from a run spec.
//
// No payload is not an error: it means the caller asked for the backend's
// default single-agent loop. A payload selects the step-queue execution mode,
// and one that cannot be read is an error rather than a fallback, because
// falling back would run something other than what was asked for.
func DecodeStepQueueInput(input map[string]any) (*StepQueueInput, error) {
	raw, ok := input[RunInputKey]
	if !ok || raw == nil {
		return nil, nil
	}

	payload, ok := raw.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%w: %s must be an object", ErrInvalidRunInput, RunInputKey)
	}

	if err := rejectUnknownRunInputKeys(payload); err != nil {
		return nil, err
	}

	decoded, err := decodeStepQueuePayload(payload)
	if err != nil {
		return nil, err
	}

	if err := validateQueueChoice(decoded); err != nil {
		return nil, err
	}

	return decoded, nil
}

// decodeStepQueuePayload reads each key the payload may carry. Every key is
// optional here; validateQueueChoice has already established that the payload
// says what to run.
func decodeStepQueuePayload(payload map[string]any) (*StepQueueInput, error) {
	steps, err := decodeOptional[[]entity.Step](payload, RunInputSteps)
	if err != nil {
		return nil, err
	}

	teamSpec, err := decodeOptional[entity.TeamSpec](payload, RunInputTeamSpec)
	if err != nil {
		return nil, err
	}

	maxDepth, err := decodeOptional[float64](payload, RunInputMaxDepth)
	if err != nil {
		return nil, err
	}

	if maxDepth < 0 || maxDepth != math.Trunc(maxDepth) {
		return nil, fmt.Errorf("%w: %s must be a whole number and not negative", ErrInvalidRunInput, RunInputMaxDepth)
	}

	policy, err := decodeOptional[entity.ContinuePolicy](payload, RunInputContinuePolicy)
	if err != nil {
		return nil, err
	}

	decoded := &StepQueueInput{Steps: steps, MaxDepth: int(maxDepth), ContinuePolicy: policy}

	if !reflect.ValueOf(teamSpec).IsZero() {
		decoded.TeamSpec = &teamSpec
	}

	return decoded, nil
}

// validateQueueChoice refuses a payload that says which queue to run twice, or
// not at all: the mode is selected by the payload, so an ambiguous one has no
// right answer.
func validateQueueChoice(decoded *StepQueueInput) error {
	switch {
	case len(decoded.Steps) > 0 && decoded.TeamSpec != nil:
		return fmt.Errorf(
			"%w: %s and %s are two ways to say the same thing; send one",
			ErrInvalidRunInput, RunInputSteps, RunInputTeamSpec,
		)
	case len(decoded.Steps) == 0 && decoded.TeamSpec == nil:
		return fmt.Errorf("%w: %s must not be empty when it is sent", ErrInvalidRunInput, RunInputSteps)
	}

	return nil
}

// decodeOptional reads one payload value, treating an absent or null key as the
// zero value: absent is not the same as invalid.
func decodeOptional[T any](payload map[string]any, key string) (T, error) {
	var zero T

	raw, ok := payload[key]
	if !ok || raw == nil {
		return zero, nil
	}

	return decodeRunInputValue[T](key, raw)
}

func rejectUnknownRunInputKeys(input map[string]any) error {
	var unknown []string

	for key := range input {
		if !slices.Contains(RunInputKeys(), key) {
			unknown = append(unknown, key)
		}
	}

	if len(unknown) == 0 {
		return nil
	}

	sort.Strings(unknown)

	return fmt.Errorf(
		"%w: unknown native run input %s; %s reads %s",
		ErrInvalidRunInput, strings.Join(unknown, ", "), RunInputKey, strings.Join(RunInputKeys(), ", "),
	)
}

// decodeRunInputValue re-reads one payload value as a typed value.
//
// The payload arrives from JSON, so its values are already generic: going back
// through the encoder is what gives the strongly typed model the same treatment
// every other request body gets, including its validation.
func decodeRunInputValue[T any](key string, raw any) (T, error) {
	var decoded T

	encoded, err := json.Marshal(raw)
	if err != nil {
		return decoded, fmt.Errorf("%w: %s cannot be read: %w", ErrInvalidRunInput, key, err)
	}

	if err := json.Unmarshal(encoded, &decoded); err != nil {
		return decoded, fmt.Errorf("%w: %s cannot be read: %w", ErrInvalidRunInput, key, err)
	}

	return decoded, nil
}
