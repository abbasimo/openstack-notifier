package parser

import "time"

// envelopeLayouts are the forms oslo.messaging's `timestamp` takes. Python's str(datetime) drops
// the fraction when microseconds are 0, and Go treats a trailing ".999999" as optional, so the
// first layout covers both `2026-09-17 10:00:00` and `2026-09-17 10:00:00.104233`.
var envelopeLayouts = []string{
	"2006-01-02 15:04:05.999999",
	"2006-01-02T15:04:05.999999",
	time.RFC3339Nano,
}

// novaLayouts are the forms Nova uses inside payloads (RFC 3339 in the samples), plus the
// envelope forms so a payload that carries an envelope-style timestamp still parses.
var novaLayouts = []string{
	time.RFC3339Nano,
	"2006-01-02T15:04:05.999999",
	"2006-01-02 15:04:05.999999",
}

// EnvelopeTime parses an oslo envelope timestamp. It has no zone and is UTC by convention.
// ok is false when the string is empty or in none of the known forms; the caller then falls back
// to the receive time and sets Event.TimestampOK = false.
func EnvelopeTime(s string) (t time.Time, ok bool) {
	return parseAny(s, envelopeLayouts)
}

// NovaTime parses a payload timestamp (created_at, launched_at, …). A missing, null or
// unparseable value yields the zero time, which every template renders as "unknown".
func NovaTime(s string) time.Time {
	t, _ := parseAny(s, novaLayouts)
	return t
}

func parseAny(s string, layouts []string) (time.Time, bool) {
	if s == "" {
		return time.Time{}, false
	}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), true
		}
	}
	return time.Time{}, false
}
