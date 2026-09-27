package config

import (
	"fmt"
	"io"
	"log/slog"
)

const redacted = "[REDACTED]"


type Secret string

func (s Secret) Reveal() string { return string(s) }

func (s Secret) String() string {
	if s == "" {
		return ""
	}
	return redacted
}

func (s Secret) Format(f fmt.State, _ rune) { _, _ = io.WriteString(f, s.String()) }

func (s Secret) LogValue() slog.Value { return slog.StringValue(s.String()) }

func (s Secret) MarshalText() ([]byte, error) { return []byte(s.String()), nil }
