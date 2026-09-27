package model

import "time"

const (
	EventCreateStart         = "instance.create.start"
	EventCreateEnd           = "instance.create.end"
	EventCreateError         = "instance.create.error"
	EventBuildInstancesError = "compute_task.build_instances.error"
	EventInstanceUpdate      = "instance.update"
)

// EventTypes returns the five decodable event types in catalogue order.
func EventTypes() []string {
	return []string{EventCreateStart, EventCreateEnd, EventCreateError, EventBuildInstancesError, EventInstanceUpdate}
}

// Event is one decoded notification that survived the filter.
type Event struct {
	MessageID   string
	EventType   string
	Priority    string
	PublisherID string
	Timestamp   time.Time
	TimestampOK bool // false → fell back to receive time

	Instance    Instance
	Fault       *Fault       // create.error fault, or build_instances.error reason
	StateUpdate *StateUpdate // instance.update only
}

// Instance is the subset of a Nova instance payload the notifier uses.
type Instance struct {
	UUID, Name, ProjectID, UserID, RequestID, Host, Node, AZ string
	State, TaskState, PowerState                             string
	Flavor                                                   Flavor
	ImageUUID                                                string
	IPs                                                      []IP
	CreatedAt, LaunchedAt                                    time.Time
}

// Flavor sizes. DiskGB comes from Nova's root_gb.
type Flavor struct {
	Name                    string
	VCPUs, MemoryMB, DiskGB int
}

// IP is one entry of the instance's ip_addresses.
type IP struct{ Address, Label, MAC, DeviceName string }

// Fault explains a failure.
type Fault struct{ Exception, Message, Function, Module, Traceback string }

// StateUpdate is instance.update's state_update.
type StateUpdate struct{ OldState, State, OldTaskState, NewTaskState string }

// OutcomeKind is what happened to a build.
type OutcomeKind int

const (
	OutcomeSuccess OutcomeKind = iota
	OutcomeFailure
	OutcomeTimeout
)

func (k OutcomeKind) String() string {
	switch k {
	case OutcomeSuccess:
		return "success"
	case OutcomeFailure:
		return "failure"
	case OutcomeTimeout:
		return "timeout"
	}
	return "unknown"
}

// FailureStage is where a failed build stopped.
type FailureStage int

const (
	StageNone FailureStage = iota
	StageScheduling
	StageCompute
	StageUnknown // fallback outcome from instance.update building→error (ADR-0009)
)

func (s FailureStage) String() string {
	switch s {
	case StageNone:
		return "none"
	case StageScheduling:
		return "scheduling"
	case StageCompute:
		return "compute"
	case StageUnknown:
		return "unknown"
	}
	return "invalid"
}

// Outcome is the notifier's decision about one build.
type Outcome struct {
	Kind     OutcomeKind
	Stage    FailureStage
	Event    Event
	Duration time.Duration // 0 if .start not seen
	Fallback bool          // from instance.update building→error; held for FALLBACK_GRACE
}

// Disposition is how a delivery is settled (docs/ARCHITECTURE.md §5).
type Disposition int

const (
	Ack Disposition = iota
	Requeue
	DeadLetter
	Deferred // settled later through an Acker
)

func (d Disposition) String() string {
	switch d {
	case Ack:
		return "ack"
	case Requeue:
		return "requeue"
	case DeadLetter:
		return "dead_letter"
	case Deferred:
		return "deferred"
	}
	return "invalid"
}

// Acker settles one delivery later (used by Deferred). Implemented by listener,
// consumed by pipeline/holdback/ratelimit — lives here so neither imports the other.
type Acker interface {
	Ack() error
	Nack(requeue bool) error
}
