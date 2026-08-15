package core

import "errors"

var (
	// ErrInvalidRunSpec reports a malformed run spec.
	ErrInvalidRunSpec = errors.New("agentos: invalid run spec")
	// ErrInvalidBackendRef reports an invalid backend reference.
	ErrInvalidBackendRef = errors.New("agentos: invalid backend ref")
	// ErrBackendNotFound reports that a backend was not found.
	ErrBackendNotFound = errors.New("agentos: backend not found")
	// ErrRunRouteNotFound reports that a run route was not found.
	ErrRunRouteNotFound = errors.New("agentos: run route not found")
	// ErrInvalidSignal reports a malformed signal.
	ErrInvalidSignal = errors.New("agentos: invalid signal")
	// ErrInvalidControlOperation reports an invalid control operation.
	ErrInvalidControlOperation = errors.New("agentos: invalid control operation")
	// ErrInvalidStreamScope reports an invalid stream scope.
	ErrInvalidStreamScope = errors.New("agentos: invalid stream scope")
	// ErrInvalidRunPlan reports a malformed run plan.
	ErrInvalidRunPlan = errors.New("agentos: invalid run plan")
	// ErrInvalidPlanScope reports an invalid plan scope.
	ErrInvalidPlanScope = errors.New("agentos: invalid plan scope")
	// ErrPlanRouteNotFound reports that a plan route was not found.
	ErrPlanRouteNotFound = errors.New("agentos: plan route not found")
	// ErrInvalidResourceRef reports an invalid resource reference.
	ErrInvalidResourceRef = errors.New("agentos: invalid resource ref")
	// ErrInvalidProcess reports a malformed process.
	ErrInvalidProcess = errors.New("agentos: invalid process")
	// ErrInvalidProcessScope reports an invalid process scope.
	ErrInvalidProcessScope = errors.New("agentos: invalid process scope")
	// ErrProcessRouteNotFound reports that a process route was not found.
	ErrProcessRouteNotFound = errors.New("agentos: process route not found")
	// ErrCapabilityNotFound reports that a capability was not found.
	ErrCapabilityNotFound = errors.New("agentos: capability not found")
	// ErrArtifactNotFound reports that an artifact was not found.
	ErrArtifactNotFound = errors.New("agentos: artifact not found")
	// ErrInvalidArtifact reports a malformed artifact.
	ErrInvalidArtifact = errors.New("agentos: invalid artifact")
	// ErrInvalidExpression reports an invalid expression.
	ErrInvalidExpression = errors.New("agentos: invalid expression")
	// ErrInvalidPlanEvent reports a malformed plan event.
	ErrInvalidPlanEvent = errors.New("agentos: invalid plan event")
	// ErrInvalidRunEvent reports a malformed run event.
	ErrInvalidRunEvent = errors.New("agentos: invalid run event")
	// ErrInvalidLedgerEntry reports a malformed ledger entry.
	ErrInvalidLedgerEntry = errors.New("agentos: invalid ledger entry")
	// ErrInvalidLedgerScope reports an invalid ledger scope.
	ErrInvalidLedgerScope = errors.New("agentos: invalid ledger scope")
	// ErrInvalidGovernedAction reports a malformed governed action.
	ErrInvalidGovernedAction = errors.New("agentos: invalid governed action")
	// ErrInvalidGovernedActionScope reports an invalid governed action scope.
	ErrInvalidGovernedActionScope = errors.New("agentos: invalid governed action scope")
	// ErrInvalidWorkset reports a malformed workset.
	ErrInvalidWorkset = errors.New("agentos: invalid workset")
	// ErrInvalidWorksetScope reports an invalid workset scope.
	ErrInvalidWorksetScope = errors.New("agentos: invalid workset scope")
	// ErrInvalidTrigger reports a malformed trigger.
	ErrInvalidTrigger = errors.New("agentos: invalid trigger")
	// ErrInvalidTriggerScope reports an invalid trigger scope.
	ErrInvalidTriggerScope = errors.New("agentos: invalid trigger scope")
)
