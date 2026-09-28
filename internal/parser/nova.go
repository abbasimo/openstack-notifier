package parser

import "encoding/json"

// object is the {"nova_object.name", ".namespace", ".version", ".data"} wrapper Nova puts around
// every payload object. Only the data half is decoded (docs/DOMAIN.md §3).
type object[T any] struct {
	Data T `json:"nova_object.data"`
}

// notification is the tier-1 view of oslo.message: everything needed to filter, plus the payload
// left as raw bytes so the expensive decode only happens for events that survive (DD6).
type notification struct {
	MessageID        string          `json:"message_id"`
	EventType        string          `json:"event_type"`
	Priority         string          `json:"priority"`
	PublisherID      string          `json:"publisher_id"`
	Timestamp        string          `json:"timestamp"`
	ContextRequestID string          `json:"_context_request_id"`
	Payload          json.RawMessage `json:"payload"`
}

// stateUpdate mirrors InstanceStateUpdatePayload.
type stateUpdate struct {
	OldState     string `json:"old_state"`
	State        string `json:"state"`
	OldTaskState string `json:"old_task_state"`
	NewTaskState string `json:"new_task_state"`
}

// updateProbe is the tier-2 view of InstanceUpdatePayload: only the state transition.
type updateProbe struct {
	StateUpdate object[stateUpdate] `json:"state_update"`
}

// flavorPayload mirrors FlavorPayload. Disk comes from root_gb, not disk_gb.
type flavorPayload struct {
	Name     string `json:"name"`
	VCPUs    int    `json:"vcpus"`
	MemoryMB int    `json:"memory_mb"`
	RootGB   int    `json:"root_gb"`
}

// ipPayload mirrors IpPayload.
type ipPayload struct {
	Address    string `json:"address"`
	Label      string `json:"label"`
	MAC        string `json:"mac"`
	DeviceName string `json:"device_name"`
}

// exceptionPayload mirrors ExceptionPayload, used for both `fault` and `reason`.
type exceptionPayload struct {
	Exception string `json:"exception"`
	Message   string `json:"exception_message"`
	Function  string `json:"function_name"`
	Module    string `json:"module_name"`
	Traceback string `json:"traceback"`
}

// instancePayload covers InstanceCreatePayload and InstanceUpdatePayload: the union of the fields
// we map, with every optional object a pointer so "absent" is distinguishable from "empty".
type instancePayload struct {
	UUID        string                    `json:"uuid"`
	DisplayName string                    `json:"display_name"`
	TenantID    string                    `json:"tenant_id"`
	UserID      string                    `json:"user_id"`
	RequestID   string                    `json:"request_id"`
	Host        string                    `json:"host"`
	Node        string                    `json:"node"`
	AZ          string                    `json:"availability_zone"`
	State       string                    `json:"state"`
	TaskState   string                    `json:"task_state"`
	PowerState  string                    `json:"power_state"`
	ImageUUID   string                    `json:"image_uuid"`
	CreatedAt   string                    `json:"created_at"`
	LaunchedAt  string                    `json:"launched_at"`
	Flavor      *object[flavorPayload]    `json:"flavor"`
	IPs         []object[ipPayload]       `json:"ip_addresses"`
	Fault       *object[exceptionPayload] `json:"fault"`
	StateUpdate *object[stateUpdate]      `json:"state_update"`
}

// requestSpec is the part of RequestSpecPayload that survives into the email. ComputeTaskPayload
// carries no display name, host, node, IPs or timestamps (docs/DOMAIN.md §3).
type requestSpec struct {
	ProjectID string                 `json:"project_id"`
	UserID    string                 `json:"user_id"`
	AZ        string                 `json:"availability_zone"`
	Flavor    *object[flavorPayload] `json:"flavor"`
	Image     *object[imageMeta]     `json:"image"`
}

type imageMeta struct {
	ID string `json:"id"`
}

// computeTaskPayload mirrors ComputeTaskPayload 1.0.
type computeTaskPayload struct {
	InstanceUUID string                    `json:"instance_uuid"`
	State        string                    `json:"state"`
	RequestSpec  *object[requestSpec]      `json:"request_spec"`
	Reason       *object[exceptionPayload] `json:"reason"`
}
