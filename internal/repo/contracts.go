// Package repo provides infrastructure adapters that implement usecase output ports.
//
// Deprecated: Import usecase directly for port interfaces. This package exists only
// as a migration shim and will be removed once all consumers have been updated.
package repo

import "github.com/TekkenSteve/GoAgent/usecase"

type (
	WarmStateRepo       = usecase.WarmStateRepo
	ColdStateRepo       = usecase.ColdStateRepo
	WorkflowStateRepo   = usecase.WorkflowStateRepo
	AgentRepo           = usecase.AgentRepo
	LLMProvider         = usecase.LLMProvider
	LLMStreamProvider   = usecase.LLMStreamProvider
	ToolExecutor        = usecase.ToolExecutor
	WALAppender         = usecase.WALAppender
	ContextCompressor   = usecase.ContextCompressor
	ExecutorRepo        = usecase.ExecutorRepo
	WorkflowTemplateRepo = usecase.WorkflowTemplateRepo
	TriggerRepo         = usecase.TriggerRepo
	TriggerScheduler    = usecase.TriggerScheduler
	CreditManager       = usecase.CreditManager
	CostCalculator      = usecase.CostCalculator
	UsageRecordRepo     = usecase.UsageRecordRepo
)
