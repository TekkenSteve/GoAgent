package entity

// TeamSpec defines a group of agents and their orchestration steps.
// Teams can be recursively nested via SubTeams.
type TeamSpec struct {
	ID       string         `json:"id"`
	Name     string         `json:"name"`
	Agents   []AgentSpec    `json:"agents"`
	SubTeams []TeamSpec     `json:"sub_teams,omitempty"`
	Steps    []StepTemplate `json:"steps"`
}

// AgentSpec defines a single agent's identity and capabilities.
// Sources: code definition, config file, runtime creation tool.
type AgentSpec struct {
	ID           string        `json:"id"`
	Name         string        `json:"name"`
	Version      string        `json:"version"`
	SystemPrompt string        `json:"system_prompt"`
	ModelRef     string        `json:"model_ref"`
	Config       LLMConfig     `json:"config"`
	Tools        []ToolBinding `json:"tools"`
}

// ToolBinding binds a tool to an agent with optional overrides.
type ToolBinding struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"` // override default
	Required    bool   `json:"required,omitempty"`    // fail if unavailable
}

// StepTemplate is a step definition inside a TeamSpec before flattening.
// AgentRef references an AgentSpec in the same team by ID.
type StepTemplate struct {
	ID        string         `json:"id"`
	Type      StepType       `json:"type"`
	AgentRef  string         `json:"agent_ref,omitempty"`  // TeamSpec.Agents[i].ID
	Tool      string         `json:"tool,omitempty"`
	Input     map[string]any `json:"input"`
	DependsOn []string       `json:"depends_on,omitempty"`
	WaitFor   *WaitCondition `json:"wait_for,omitempty"`
	OnResult  *StepMutation  `json:"on_result,omitempty"`
}

// OrchestrationInput is the input for OrchestrationWorkflow.
type OrchestrationInput struct {
	RunID          string          `json:"run_id"`
	TeamSpec       *TeamSpec       `json:"team_spec,omitempty"`       // from team definition
	Steps          []Step          `json:"steps,omitempty"`           // direct step array
	SystemPrompt   string          `json:"system_prompt,omitempty"`
	Message        string          `json:"message,omitempty"`
	MaxDepth       int             `json:"max_depth,omitempty"`       // max recursion depth
	ContinuePolicy ContinuePolicy  `json:"continue_policy,omitempty"`
}

// OrchestrationResult is the output of OrchestrationWorkflow.
type OrchestrationResult struct {
	RunID     string         `json:"run_id"`
	Steps     []Step         `json:"steps"`      // final step queue with statuses
	Summary   map[string]any `json:"summary"`
}

// ContinuePolicy limits orchestration depth and duration.
type ContinuePolicy struct {
	MaxDepth   int `json:"max_depth"`   // max step queue length
	MaxRounds  int `json:"max_rounds"`  // max workflow loop iterations
}
