package obs

import (
	"io"
	"log/slog"
)

// Standard attribute keys. Use these instead of literals so log queries stay stable.
const (
	KeyInstanceUUID = "instance_uuid"
	KeyMessageID    = "message_id"
	KeyEventType    = "event_type"
	KeyOutcome      = "outcome"
	KeyDisposition  = "disposition"
	KeyError        = "error"
)

// NewLogger returns a JSON logger writing to w at the given minimum level.
func NewLogger(w io.Writer, level slog.Level) *slog.Logger {
	return slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level}))
}

// Err is the standard way to attach an error to a log record.
func Err(err error) slog.Attr {
	if err == nil {
		return slog.Attr{}
	}
	return slog.String(KeyError, err.Error())
}
