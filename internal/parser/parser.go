package parser

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/abbasimo/openstack-notifier/internal/model"
)

// Errors returned by Parse. Each one means the delivery is dead-lettered (ARCHITECTURE §5 row 1);
// they are sentinels so the pipeline can tell them apart in logs and tests.
var (
	ErrTooLarge   = errors.New("body exceeds MAX_BODY_BYTES")
	ErrEnvelope   = errors.New("invalid oslo envelope")
	ErrMalformed  = errors.New("malformed notification")
	ErrNoInstance = errors.New("notification has no instance uuid")
)

// oslo envelope constants.
const (
	osloVersion = "2.0"
	// buildingState and errorState are the tier-2 transition we keep (docs/DOMAIN.md §2).
	buildingState = "building"
	errorState    = "error"
)

// Result is one parsed body.
type Result struct {
	Event   model.Event
	Keep    bool     // false → filtered out; not an error, the delivery is acked
	Missing []string // payload paths that were absent (tolerant parse, Plan.md 4.5)
}

// Parser is safe for concurrent use: it is immutable after New.
type Parser struct {
	maxBodyBytes int
	allowed      map[string]bool
}

// New returns a parser accepting bodies up to maxBodyBytes and event types in eventTypes.
// An event type with no decoder (i.e. not in model.EventTypes) is a configuration error.
func New(maxBodyBytes int, eventTypes []string) (*Parser, error) {
	if maxBodyBytes <= 0 {
		return nil, fmt.Errorf("parser: maxBodyBytes must be positive, got %d", maxBodyBytes)
	}
	known := model.EventTypes()
	allowed := make(map[string]bool, len(eventTypes))
	for _, t := range eventTypes {
		if !slices.Contains(known, t) {
			return nil, fmt.Errorf("parser: no decoder for event type %q", t)
		}
		allowed[t] = true
	}
	if len(allowed) == 0 {
		return nil, errors.New("parser: no event types enabled")
	}
	return &Parser{maxBodyBytes: maxBodyBytes, allowed: allowed}, nil
}

// Parse decodes one AMQP body. received is the time the delivery arrived; it becomes the event
// timestamp when the envelope carries none we understand.
//
// Order: size guard → envelope → tier-1 allow-list → tier-2 (instance.update) → full decode.
func (p *Parser) Parse(body []byte, received time.Time) (Result, error) {
	if len(body) > p.maxBodyBytes {
		return Result{}, fmt.Errorf("%w: %d bytes", ErrTooLarge, len(body))
	}

	inner, err := unwrap(body)
	if err != nil {
		return Result{}, err
	}

	var n notification
	if err := json.Unmarshal(inner, &n); err != nil {
		return Result{}, fmt.Errorf("%w: decoding notification: %v", ErrMalformed, err)
	}
	if n.EventType == "" {
		return Result{}, fmt.Errorf("%w: no event_type", ErrMalformed)
	}

	ev := model.Event{
		MessageID:   n.MessageID,
		EventType:   n.EventType,
		Priority:    n.Priority,
		PublisherID: n.PublisherID,
	}
	ev.Timestamp, ev.TimestampOK = EnvelopeTime(n.Timestamp)
	if !ev.TimestampOK {
		ev.Timestamp = received
	}

	// Tier 1: the allow-list. Everything below this line costs a full payload decode.
	if !p.allowed[n.EventType] {
		return Result{Event: ev}, nil
	}

	// Tier 2: instance.update is the loudest event type by far; keep only building→error.
	if n.EventType == model.EventInstanceUpdate {
		keep, su, err := buildingToError(n.Payload)
		if err != nil {
			return Result{}, err
		}
		if !keep {
			return Result{Event: ev}, nil
		}
		ev.StateUpdate = su
	}

	missing, err := decodePayload(&ev, n)
	if err != nil {
		return Result{}, err
	}
	if ev.Instance.UUID == "" {
		return Result{}, fmt.Errorf("%w: event_type %s", ErrNoInstance, n.EventType)
	}
	return Result{Event: ev, Keep: true, Missing: missing}, nil
}

// envelope is the oslo wire wrapper. EventType is only read to recognise a bare (un-enveloped)
// notification, which some drivers and test tools publish.
type envelope struct {
	Version   string `json:"oslo.version"`
	Message   string `json:"oslo.message"`
	EventType string `json:"event_type"`
}

// unwrap returns the notification JSON inside the oslo envelope, or the body itself when it is a
// bare notification.
func unwrap(body []byte) ([]byte, error) {
	var env envelope
	if err := json.Unmarshal(body, &env); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrEnvelope, err)
	}
	switch {
	case env.Message != "":
		if env.Version != osloVersion {
			return nil, fmt.Errorf("%w: oslo.version %q, want %q", ErrEnvelope, env.Version, osloVersion)
		}
		return []byte(env.Message), nil
	case env.EventType != "":
		return body, nil // bare notification
	default:
		return nil, fmt.Errorf("%w: no oslo.message and no event_type", ErrEnvelope)
	}
}

// buildingToError is the tier-2 filter: decode only state_update and report whether this update
// marks a build turning into ERROR.
func buildingToError(payload json.RawMessage) (bool, *model.StateUpdate, error) {
	if !hasPayload(payload) {
		return false, nil, fmt.Errorf("%w: instance.update without payload", ErrMalformed)
	}
	var probe object[updateProbe]
	if err := json.Unmarshal(payload, &probe); err != nil {
		return false, nil, fmt.Errorf("%w: decoding state_update: %v", ErrMalformed, err)
	}
	su := probe.Data.StateUpdate.Data
	if su.OldState != buildingState || su.State != errorState {
		return false, nil, nil
	}
	return true, &model.StateUpdate{
		OldState:     su.OldState,
		State:        su.State,
		OldTaskState: su.OldTaskState,
		NewTaskState: su.NewTaskState,
	}, nil
}

// hasPayload reports whether the notification carries a payload at all. A literal JSON null is
// treated as absent: decoding it would silently yield an empty instance.
func hasPayload(raw json.RawMessage) bool {
	return len(raw) > 0 && string(raw) != "null"
}

// decodePayload fills ev.Instance and ev.Fault from the payload and reports the paths that were
// absent. A missing or renamed field degrades to a zero value — never an error, never a panic:
// an email without a flavor beats no email (Plan.md 4.5).
func decodePayload(ev *model.Event, n notification) ([]string, error) {
	if !hasPayload(n.Payload) {
		return nil, fmt.Errorf("%w: no payload", ErrMalformed)
	}
	if n.EventType == model.EventBuildInstancesError {
		return decodeComputeTask(ev, n)
	}
	return decodeInstance(ev, n)
}

func decodeInstance(ev *model.Event, n notification) ([]string, error) {
	var p object[instancePayload]
	if err := json.Unmarshal(n.Payload, &p); err != nil {
		return nil, fmt.Errorf("%w: decoding %s payload: %v", ErrMalformed, n.EventType, err)
	}
	d := p.Data

	ev.Instance = model.Instance{
		UUID:       d.UUID,
		Name:       d.DisplayName,
		ProjectID:  d.TenantID,
		UserID:     d.UserID,
		RequestID:  firstNonEmpty(d.RequestID, n.ContextRequestID),
		Host:       d.Host,
		Node:       d.Node,
		AZ:         d.AZ,
		State:      d.State,
		TaskState:  d.TaskState,
		PowerState: d.PowerState,
		ImageUUID:  d.ImageUUID,
		CreatedAt:  NovaTime(d.CreatedAt),
		LaunchedAt: NovaTime(d.LaunchedAt),
	}
	if d.Flavor != nil {
		ev.Instance.Flavor = flavor(d.Flavor.Data)
	}
	for _, ip := range d.IPs {
		ev.Instance.IPs = append(ev.Instance.IPs, model.IP{
			Address:    ip.Data.Address,
			Label:      ip.Data.Label,
			MAC:        ip.Data.MAC,
			DeviceName: ip.Data.DeviceName,
		})
	}
	if d.Fault != nil {
		ev.Fault = fault(d.Fault.Data)
	}
	// instance.update sets StateUpdate in the tier-2 pass; a bare payload may still carry one.
	if ev.StateUpdate == nil && d.StateUpdate != nil {
		su := d.StateUpdate.Data
		ev.StateUpdate = &model.StateUpdate{
			OldState: su.OldState, State: su.State,
			OldTaskState: su.OldTaskState, NewTaskState: su.NewTaskState,
		}
	}
	return missingPaths(n.EventType, ev), nil
}

func decodeComputeTask(ev *model.Event, n notification) ([]string, error) {
	var p object[computeTaskPayload]
	if err := json.Unmarshal(n.Payload, &p); err != nil {
		return nil, fmt.Errorf("%w: decoding %s payload: %v", ErrMalformed, n.EventType, err)
	}
	d := p.Data

	ev.Instance = model.Instance{
		UUID:      d.InstanceUUID,
		State:     d.State,
		RequestID: n.ContextRequestID,
	}
	if d.RequestSpec != nil {
		rs := d.RequestSpec.Data
		ev.Instance.ProjectID = rs.ProjectID
		ev.Instance.UserID = rs.UserID
		ev.Instance.AZ = rs.AZ
		if rs.Flavor != nil {
			ev.Instance.Flavor = flavor(rs.Flavor.Data)
		}
		if rs.Image != nil {
			ev.Instance.ImageUUID = rs.Image.Data.ID
		}
	}
	if d.Reason != nil {
		ev.Fault = fault(d.Reason.Data)
	}
	return missingPaths(n.EventType, ev), nil
}

func flavor(f flavorPayload) model.Flavor {
	return model.Flavor{Name: f.Name, VCPUs: f.VCPUs, MemoryMB: f.MemoryMB, DiskGB: f.RootGB}
}

func fault(e exceptionPayload) *model.Fault {
	return &model.Fault{
		Exception: e.Exception,
		Message:   e.Message,
		Function:  e.Function,
		Module:    e.Module,
		Traceback: e.Traceback,
	}
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

// expected lists, per event type, the payload paths whose absence is worth reporting: the fields
// that event's email would otherwise show empty. Fields Nova legitimately leaves null at that
// point of the lifecycle (host on create.start, IPs before networking) are not listed.
var expected = map[string][]string{
	model.EventCreateStart:         {"uuid", "display_name", "tenant_id", "flavor"},
	model.EventCreateEnd:           {"uuid", "display_name", "tenant_id", "user_id", "flavor", "image_uuid", "host", "created_at", "launched_at"},
	model.EventCreateError:         {"uuid", "display_name", "tenant_id", "flavor", "fault"},
	model.EventInstanceUpdate:      {"uuid", "display_name", "tenant_id", "state_update"},
	model.EventBuildInstancesError: {"instance_uuid", "reason", "request_spec.project_id", "request_spec.flavor"},
}

// missingPaths reports which expected paths ended up as zero values.
func missingPaths(eventType string, ev *model.Event) []string {
	var missing []string
	for _, path := range expected[eventType] {
		if !present(path, ev) {
			missing = append(missing, path)
		}
	}
	return missing
}

func present(path string, ev *model.Event) bool {
	i := ev.Instance
	switch path {
	case "uuid", "instance_uuid":
		return i.UUID != ""
	case "display_name":
		return i.Name != ""
	case "tenant_id", "request_spec.project_id":
		return i.ProjectID != ""
	case "user_id":
		return i.UserID != ""
	case "flavor", "request_spec.flavor":
		return i.Flavor != model.Flavor{}
	case "image_uuid":
		return i.ImageUUID != ""
	case "host":
		return i.Host != ""
	case "created_at":
		return !i.CreatedAt.IsZero()
	case "launched_at":
		return !i.LaunchedAt.IsZero()
	case "fault", "reason":
		return ev.Fault != nil
	case "state_update":
		return ev.StateUpdate != nil
	}
	return true
}
