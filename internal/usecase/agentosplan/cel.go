package agentosplan

import (
	"context"
	"fmt"

	"github.com/TekkenSteve/GoAgent/agentos"
	"github.com/google/cel-go/cel"
)

// CELCompiler compiles deterministic CEL expressions for RunPlan conditions.
type CELCompiler struct {
	env *cel.Env
}

// NewCELCompiler creates a CEL compiler with the fixed AgentOS plan variables.
func NewCELCompiler() (*CELCompiler, error) {
	env, err := cel.NewEnv(
		cel.Variable("plan", cel.DynType),
		cel.Variable("node", cel.DynType),
		cel.Variable("inputs", cel.DynType),
		cel.Variable("outputs", cel.DynType),
		cel.Variable("artifacts", cel.DynType),
		cel.Variable("metadata", cel.DynType),
		cel.Variable("status", cel.DynType),
	)
	if err != nil {
		return nil, fmt.Errorf("agentos plan cel: %w", err)
	}

	return &CELCompiler{env: env}, nil
}

// Compile validates one expression.
func (c *CELCompiler) Compile(expression string) (Expression, error) {
	if expression == "" {
		return alwaysTrueExpression{}, nil
	}
	ast, issues := c.env.Compile(expression)
	if issues != nil && issues.Err() != nil {
		return nil, fmt.Errorf("%w: %s", agentos.ErrInvalidExpression, issues.Err())
	}
	program, err := c.env.Program(ast)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", agentos.ErrInvalidExpression, err)
	}

	return celExpression{program: program}, nil
}

type celExpression struct {
	program cel.Program
}

// Evaluate returns a boolean expression result.
func (e celExpression) Evaluate(_ context.Context, vars map[string]any) (bool, error) {
	value, _, err := e.program.Eval(vars)
	if err != nil {
		return false, fmt.Errorf("%w: %s", agentos.ErrInvalidExpression, err)
	}
	result, ok := value.Value().(bool)
	if !ok {
		return false, fmt.Errorf("%w: expression result must be bool", agentos.ErrInvalidExpression)
	}

	return result, nil
}

type alwaysTrueExpression struct{}

func (alwaysTrueExpression) Evaluate(context.Context, map[string]any) (bool, error) {
	return true, nil
}
