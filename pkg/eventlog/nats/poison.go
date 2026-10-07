package nats

// PoisonPolicy decides what a consumer does with a record it has failed often
// enough to spend the log's delivery budget.
//
// It is a transport-level policy rather than part of the eventlog port: the
// port promises at-least-once delivery, and what to do with a record that never
// succeeds is a property of the transport's delivery budget. A consumer picks
// the policy that matches why its failures happen.
type PoisonPolicy int

const (
	// PoisonRetry keeps the record for another attempt. It is the zero value,
	// and the right choice when the failures belong to the sink: a projection
	// that cannot reach Postgres will succeed once Postgres answers. Parking
	// such a record would drop a fact from the durable timeline.
	PoisonRetry PoisonPolicy = iota
	// PoisonDeadLetter moves the record to its domain's dead-letter stream and
	// acknowledges it, so one unprocessable record cannot hold a shard's
	// progress. It is the right choice when the failures belong to the record
	// or to the consumer's contract with an external system (an ingress bridge
	// whose Temporal call is rejected deterministically): the record still
	// exists in the log and in the dead-letter stream, so parking loses nothing.
	PoisonDeadLetter
)

// Headers a parked record carries, so an operator can see where it came from
// and what it cost without decoding the dead-letter stream's ordering.
const (
	// HeaderPoisonSource is the subject the record was parked from.
	HeaderPoisonSource = "AgentOS-Poison-Source"
	// HeaderPoisonDeliveries is how many deliveries were spent on it.
	HeaderPoisonDeliveries = "AgentOS-Poison-Deliveries"
)
