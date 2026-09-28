package render

import (
	"fmt"
	"time"

	"github.com/abbasimo/openstack-notifier/internal/model"
)

// timeFormat is what the emails show. UTC with an explicit zone, because the admin's clock and
// Nova's are rarely the same one.
const timeFormat = "2006-01-02 15:04:05 MST"

// unknown is what an absent value renders as. Every field must read sensibly when empty — Nova
// legitimately omits half of them depending on where the build stopped.
const unknown = "unknown"

// outcomeView is the flattened, sanitised, already-formatted data the templates see. Templates
// stay free of logic: no time formatting, no truncation, no conditionals beyond "is it set".
type outcomeView struct {
	Kind     string
	Stage    string
	Fallback bool

	Name      string
	UUID      string
	ProjectID string
	UserID    string
	RequestID string
	Host      string
	Node      string
	AZ        string

	State      string
	TaskState  string
	PowerState string

	FlavorName string
	VCPUs      int
	MemoryMB   int
	DiskGB     int
	HasFlavor  bool

	ImageUUID string
	IPs       []ipView

	CreatedAt  string
	LaunchedAt string
	EventAt    string
	Duration   string

	EventType   string
	PublisherID string
	MessageID   string

	Fault *faultView
}

type ipView struct {
	Address    string
	Label      string
	MAC        string
	DeviceName string
}

type faultView struct {
	Exception string
	Message   string
	Function  string
	Module    string
	Traceback string
	Truncated bool
}

type digestView struct {
	Total        int
	FailureCount int
	Generated    string
	Failures     []outcomeView
	Timeouts     []outcomeView
	Successes    []outcomeView
}

// view flattens one Outcome, truncating every tenant-influenced string to the configured budget.
func (r *Renderer) view(o model.Outcome) outcomeView {
	i := o.Event.Instance
	text := func(s string) string { return Truncate(s, r.limits.TextBytes) }

	v := outcomeView{
		Kind:     o.Kind.String(),
		Stage:    o.Stage.String(),
		Fallback: o.Fallback,

		Name:      text(StripCRLF(i.Name)),
		UUID:      StripCRLF(i.UUID),
		ProjectID: text(i.ProjectID),
		UserID:    text(i.UserID),
		RequestID: text(i.RequestID),
		Host:      text(StripCRLF(i.Host)),
		Node:      text(i.Node),
		AZ:        text(i.AZ),

		State:      text(i.State),
		TaskState:  text(i.TaskState),
		PowerState: text(i.PowerState),

		FlavorName: text(i.Flavor.Name),
		VCPUs:      i.Flavor.VCPUs,
		MemoryMB:   i.Flavor.MemoryMB,
		DiskGB:     i.Flavor.DiskGB,
		HasFlavor:  i.Flavor != model.Flavor{},

		ImageUUID: text(i.ImageUUID),

		CreatedAt:  formatTime(i.CreatedAt),
		LaunchedAt: formatTime(i.LaunchedAt),
		EventAt:    formatTime(o.Event.Timestamp),
		Duration:   formatDuration(o.Duration),

		EventType:   o.Event.EventType,
		PublisherID: text(o.Event.PublisherID),
		MessageID:   text(o.Event.MessageID),
	}
	for _, ip := range i.IPs {
		v.IPs = append(v.IPs, ipView{
			Address:    text(ip.Address),
			Label:      text(ip.Label),
			MAC:        text(ip.MAC),
			DeviceName: text(ip.DeviceName),
		})
	}
	if o.Event.Fault != nil {
		f := o.Event.Fault
		fv := &faultView{
			Exception: text(f.Exception),
			Message:   text(f.Message),
			Function:  text(f.Function),
			Module:    text(f.Module),
		}
		// Tracebacks are the one field measured in kilobytes; TRACEBACK_MAX_BYTES=0 drops them.
		if r.limits.TracebackBytes > 0 {
			fv.Traceback = Truncate(f.Traceback, r.limits.TracebackBytes)
			fv.Truncated = len(f.Traceback) > len(fv.Traceback)
		}
		v.Fault = fv
	}
	return v
}

func formatTime(t time.Time) string {
	if t.IsZero() {
		return unknown
	}
	return t.UTC().Format(timeFormat)
}

// formatDuration renders a build duration the way an operator reads it, or "unknown" when the
// `.start` event never reached this replica.
func formatDuration(d time.Duration) string {
	switch {
	case d <= 0:
		return unknown
	case d < time.Minute:
		return fmt.Sprintf("%.1fs", d.Seconds())
	case d < time.Hour:
		return fmt.Sprintf("%dm %ds", int(d.Minutes()), int(d.Seconds())%60)
	default:
		return fmt.Sprintf("%dh %dm", int(d.Hours()), int(d.Minutes())%60)
	}
}
