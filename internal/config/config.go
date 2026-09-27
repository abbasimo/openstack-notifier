// Package config loads and validates the notifier's configuration: environment variables over an
// optional YAML file (ADR-0011), over the defaults. Both sources carry the same variables.
// The normative reference is docs/CONFIG.md; TestDefaultsMatchDocs keeps the two in sync.
package config

import (
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/mail"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/abbasimo/openstack-notifier/internal/model"
)

// Config is the complete runtime configuration. Build it with Load.
type Config struct {
	// AMQP
	AMQPURL            Secret
	AMQPCAFile         string
	AMQPExchange       string
	AMQPBindingKeys    []string
	AMQPQueue          string
	AMQPQueueType      string
	AMQPQueueTTL       time.Duration
	AMQPQueueMaxLength int
	AMQPDeliveryLimit  int
	AMQPDLQTTL         time.Duration
	AMQPDLQMaxLength   int
	AMQPPrefetch       int
	AMQPHeartbeat      time.Duration
	AMQPDialTimeout    time.Duration
	Workers            int
	MaxBodyBytes       int
	EventTypes         []string

	// State & correlation
	Store               string
	StoreMaxEntries     int
	BuildTimeoutEnabled bool
	BuildTimeout        time.Duration
	FallbackGrace       time.Duration
	FallbackMax         int

	// Notification
	Notifier              string
	MockEMLDir            string
	MailFrom              mail.Address
	MailTo                []mail.Address
	SMTPAddr              string
	SMTPTLS               string
	SMTPCAFile            string
	SMTPUsername          Secret
	SMTPPassword          Secret
	SMTPTimeout           time.Duration
	NotifyRetryMaxElapsed time.Duration
	MessageDeadline       time.Duration
	RatePerMin            int
	RateBurst             int
	DigestInterval        time.Duration
	DigestMax             int
	TracebackMaxBytes     int
	TextMaxBytes          int
	TemplatesDir          string

	// Operations
	ConfigFile      string
	HTTPAddr        string
	LogLevel        string
	LogRawPayloads  bool
	ShutdownTimeout time.Duration
}

// RabbitMQ's default consumer_timeout; MESSAGE_DEADLINE should stay below it.
const brokerConsumerTimeout = 30 * time.Minute

// templateFiles must match the files embedded by internal/render (Phase 6).
var templateFiles = []string{
	"success.html.tmpl", "success.txt.tmpl", "failure.html.tmpl", "failure.txt.tmpl",
	"timeout.html.tmpl", "timeout.txt.tmpl", "digest.html.tmpl", "digest.txt.tmpl",
}

type field struct {
	name string
	def  string // "" = no default
	ptr  any
}

// fields is the single table of variable names, defaults and destinations.
func (c *Config) fields() []field {
	return []field{
		{"AMQP_URL", "", &c.AMQPURL},
		{"AMQP_CA_FILE", "", &c.AMQPCAFile},
		{"AMQP_EXCHANGE", "nova", &c.AMQPExchange},
		{"AMQP_BINDING_KEYS", "versioned_notifications.info,versioned_notifications.error", &c.AMQPBindingKeys},
		{"AMQP_QUEUE", "vm_notifier", &c.AMQPQueue},
		{"AMQP_QUEUE_TYPE", "classic", &c.AMQPQueueType},
		{"AMQP_QUEUE_TTL", "24h", &c.AMQPQueueTTL},
		{"AMQP_QUEUE_MAX_LENGTH", "100000", &c.AMQPQueueMaxLength},
		{"AMQP_DELIVERY_LIMIT", "1000", &c.AMQPDeliveryLimit},
		{"AMQP_DLQ_TTL", "168h", &c.AMQPDLQTTL},
		{"AMQP_DLQ_MAX_LENGTH", "10000", &c.AMQPDLQMaxLength},
		{"AMQP_PREFETCH", "64", &c.AMQPPrefetch},
		{"AMQP_HEARTBEAT", "10s", &c.AMQPHeartbeat},
		{"AMQP_DIAL_TIMEOUT", "10s", &c.AMQPDialTimeout},
		{"WORKERS", "16", &c.Workers},
		{"MAX_BODY_BYTES", "1048576", &c.MaxBodyBytes},
		{"EVENT_TYPES", strings.Join(model.EventTypes(), ","), &c.EventTypes},

		{"STORE", "memory", &c.Store},
		{"STORE_MAX_ENTRIES", "100000", &c.StoreMaxEntries},
		{"BUILD_TIMEOUT_ENABLED", "false", &c.BuildTimeoutEnabled},
		{"BUILD_TIMEOUT", "15m", &c.BuildTimeout},
		{"FALLBACK_GRACE", "60s", &c.FallbackGrace},
		{"FALLBACK_MAX", "8", &c.FallbackMax},

		{"NOTIFIER", "smtp", &c.Notifier},
		{"MOCK_EML_DIR", "", &c.MockEMLDir},
		{"MAIL_FROM", "", &c.MailFrom},
		{"MAIL_TO", "", &c.MailTo},
		{"SMTP_ADDR", "", &c.SMTPAddr},
		{"SMTP_TLS", "starttls", &c.SMTPTLS},
		{"SMTP_CA_FILE", "", &c.SMTPCAFile},
		{"SMTP_USERNAME", "", &c.SMTPUsername},
		{"SMTP_PASSWORD", "", &c.SMTPPassword},
		{"SMTP_TIMEOUT", "15s", &c.SMTPTimeout},
		{"NOTIFY_RETRY_MAX_ELAPSED", "10m", &c.NotifyRetryMaxElapsed},
		{"MESSAGE_DEADLINE", "20m", &c.MessageDeadline},
		{"RATE_PER_MIN", "30", &c.RatePerMin},
		{"RATE_BURST", "10", &c.RateBurst},
		{"DIGEST_INTERVAL", "60s", &c.DigestInterval},
		{"DIGEST_MAX", "40", &c.DigestMax},
		{"TRACEBACK_MAX_BYTES", "8192", &c.TracebackMaxBytes},
		{"TEXT_MAX_BYTES", "1024", &c.TextMaxBytes},
		{"TEMPLATES_DIR", "", &c.TemplatesDir},

		{"CONFIG_FILE", "", &c.ConfigFile},
		{"HTTP_ADDR", "127.0.0.1:9090", &c.HTTPAddr},
		{"LOG_LEVEL", "info", &c.LogLevel},
		{"LOG_RAW_PAYLOADS", "false", &c.LogRawPayloads},
		{"SHUTDOWN_TIMEOUT", "30s", &c.ShutdownTimeout},
	}
}

// Default returns the documented default of a variable, or "" when it has none.
func Default(name string) string {
	var c Config
	for _, f := range c.fields() {
		if f.name == name {
			return f.def
		}
	}
	return ""
}

// LoadFile is Load over Sources: the environment overrides the YAML file named by CONFIG_FILE,
// which overrides the defaults. It is what the service uses; Load takes a single source.
func LoadFile(lookup func(string) (string, bool)) (Config, error) {
	src, err := Sources(lookup)
	if err != nil {
		// A file we cannot read or parse leaves every value in doubt: report that alone.
		return Config{}, err
	}
	return Load(src)
}

// Load reads every variable through lookup (os.LookupEnv in production), applies defaults and
// validates. The returned error joins every problem found, one per line. An empty value is the
// same as an unset one.
func Load(lookup func(string) (string, bool)) (Config, error) {
	var c Config
	var errs []error
	failed := map[string]bool{}
	for _, f := range c.fields() {
		raw, err := rawValue(lookup, f)
		if err != nil {
			errs = append(errs, err)
		}
		if raw == "" {
			raw = f.def
		}
		if raw == "" {
			continue
		}
		if err := parse(f, raw); err != nil {
			errs = append(errs, err)
			failed[f.name] = true
			if f.def != "" {
				_ = parse(f, f.def) // keep cross-field checks meaningful
			}
		}
	}
	for _, p := range c.problems() {
		if !failed[p.name] || p.msg != "required" {
			errs = append(errs, p)
		}
	}
	return c, errors.Join(errs...)
}

type problem struct{ name, msg string }

func (p problem) Error() string { return p.name + ": " + p.msg }

func rawValue(lookup func(string) (string, bool), f field) (string, error) {
	v, _ := lookup(f.name)
	if _, secret := f.ptr.(*Secret); !secret {
		return v, nil
	}
	path, _ := lookup(f.name + "_FILE")
	switch {
	case path == "":
		return v, nil
	case v != "":
		return "", fmt.Errorf("%s: set either %s or %s_FILE, not both", f.name, f.name, f.name)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("%s_FILE: cannot read %q: %w", f.name, path, errors.Unwrap(err))
	}
	return strings.TrimRight(string(b), "\r\n"), nil
}

// parse converts raw into the field's type. Errors never include Secret values.
func parse(f field, raw string) error {
	var err error
	switch p := f.ptr.(type) {
	case *string:
		*p = raw
	case *Secret:
		*p = Secret(raw)
	case *int:
		*p, err = strconv.Atoi(raw)
	case *bool:
		*p, err = strconv.ParseBool(raw)
	case *time.Duration:
		*p, err = time.ParseDuration(raw)
	case *[]string:
		*p = nil
		for item := range strings.SplitSeq(raw, ",") {
			item = strings.TrimSpace(item)
			if item == "" {
				return fmt.Errorf("%s: empty item in list %q", f.name, raw)
			}
			*p = append(*p, item)
		}
	case *mail.Address:
		if strings.ContainsAny(raw, "\r\n") {
			return fmt.Errorf("%s: must not contain CR or LF", f.name)
		}
		var a *mail.Address
		if a, err = mail.ParseAddress(raw); err == nil {
			*p = *a
		}
	case *[]mail.Address:
		if strings.ContainsAny(raw, "\r\n") {
			return fmt.Errorf("%s: must not contain CR or LF", f.name)
		}
		var list []*mail.Address
		if list, err = mail.ParseAddressList(raw); err == nil {
			*p = nil
			for _, a := range list {
				*p = append(*p, *a)
			}
		}
	default:
		panic("config: unsupported field type for " + f.name)
	}
	if err != nil {
		return fmt.Errorf("%s: invalid %s %q", f.name, kind(f.ptr), raw)
	}
	return nil
}

func kind(ptr any) string {
	switch ptr.(type) {
	case *int:
		return "integer"
	case *bool:
		return "boolean"
	case *time.Duration:
		return "duration"
	case *mail.Address:
		return "email address"
	case *[]mail.Address:
		return "email address list"
	}
	return "value"
}

// Validate checks single-field ranges, cross-field rules and referenced files. It returns every
// problem joined, one per line, each prefixed with the variable name.
func (c Config) Validate() error {
	var errs []error
	for _, p := range c.problems() {
		errs = append(errs, p)
	}
	return errors.Join(errs...)
}

func (c Config) problems() []problem {
	var errs []problem
	fail := func(name, format string, args ...any) {
		errs = append(errs, problem{name, fmt.Sprintf(format, args...)})
	}
	atLeast := func(name string, got, min int) {
		if got < min {
			fail(name, "must be ≥ %d, got %d", min, got)
		}
	}
	positive := func(name string, d time.Duration) {
		if d <= 0 {
			fail(name, "must be > 0, got %s", d)
		}
	}
	oneOf := func(name, got string, allowed ...string) {
		for _, a := range allowed {
			if got == a {
				return
			}
		}
		fail(name, "must be one of %s, got %q", strings.Join(allowed, "|"), got)
	}

	// AMQP
	if u := c.AMQPURL.Reveal(); u == "" {
		fail("AMQP_URL", "required")
	} else if p, err := url.Parse(u); err != nil || (p.Scheme != "amqp" && p.Scheme != "amqps") || p.Host == "" {
		// The value is secret: never echo it or the parser's error.
		fail("AMQP_URL", "must be amqp://… or amqps://… with a host")
	} else if c.AMQPCAFile != "" && p.Scheme != "amqps" {
		fail("AMQP_CA_FILE", "requires an amqps:// AMQP_URL")
	}
	if c.AMQPCAFile != "" {
		if err := checkPEM(c.AMQPCAFile); err != nil {
			fail("AMQP_CA_FILE", "%v", err)
		}
	}
	if c.AMQPExchange == "" {
		fail("AMQP_EXCHANGE", "must not be empty")
	}
	if len(c.AMQPBindingKeys) == 0 {
		fail("AMQP_BINDING_KEYS", "needs at least one key")
	}
	if n := len(c.AMQPQueue); n < 1 || n > 240 {
		fail("AMQP_QUEUE", "must be 1–240 bytes, got %d", n)
	}
	oneOf("AMQP_QUEUE_TYPE", c.AMQPQueueType, "classic", "quorum")
	positive("AMQP_QUEUE_TTL", c.AMQPQueueTTL)
	atLeast("AMQP_QUEUE_MAX_LENGTH", c.AMQPQueueMaxLength, 1)
	atLeast("AMQP_DELIVERY_LIMIT", c.AMQPDeliveryLimit, 1)
	positive("AMQP_DLQ_TTL", c.AMQPDLQTTL)
	atLeast("AMQP_DLQ_MAX_LENGTH", c.AMQPDLQMaxLength, 1)
	if c.AMQPPrefetch < 1 || c.AMQPPrefetch > 65535 {
		fail("AMQP_PREFETCH", "must be 1–65535, got %d", c.AMQPPrefetch)
	}
	if c.AMQPHeartbeat < time.Second {
		fail("AMQP_HEARTBEAT", "must be ≥ 1s, got %s", c.AMQPHeartbeat)
	}
	positive("AMQP_DIAL_TIMEOUT", c.AMQPDialTimeout)
	atLeast("WORKERS", c.Workers, 1)
	if c.MaxBodyBytes < 1024 || c.MaxBodyBytes > 16<<20 {
		fail("MAX_BODY_BYTES", "must be 1024–16777216, got %d", c.MaxBodyBytes)
	}
	known := model.EventTypes()
	terminal := false
	for _, et := range c.EventTypes {
		oneOf("EVENT_TYPES", et, known...)
		terminal = terminal || (et != model.EventCreateStart)
	}
	if !terminal {
		fail("EVENT_TYPES", "must include %s or a failure event type", model.EventCreateEnd)
	}

	// State & correlation
	oneOf("STORE", c.Store, "memory")
	atLeast("STORE_MAX_ENTRIES", c.StoreMaxEntries, 1000)
	if c.BuildTimeout < time.Minute {
		fail("BUILD_TIMEOUT", "must be ≥ 1m, got %s", c.BuildTimeout)
	}
	if c.FallbackGrace < 0 {
		fail("FALLBACK_GRACE", "must be ≥ 0, got %s", c.FallbackGrace)
	}
	atLeast("FALLBACK_MAX", c.FallbackMax, 0)

	// Notification
	oneOf("NOTIFIER", c.Notifier, "smtp", "mock")
	if c.MockEMLDir != "" {
		if err := checkWritableDir(c.MockEMLDir); err != nil {
			fail("MOCK_EML_DIR", "%v", err)
		}
	}
	if c.MailFrom.Address == "" {
		fail("MAIL_FROM", "required")
	}
	if len(c.MailTo) == 0 {
		fail("MAIL_TO", "required")
	}
	switch {
	case c.SMTPAddr == "" && c.Notifier == "smtp":
		fail("SMTP_ADDR", "required when NOTIFIER=smtp")
	case c.SMTPAddr != "":
		if _, err := splitHostPort(c.SMTPAddr, 1); err != nil {
			fail("SMTP_ADDR", "%v", err)
		}
	}
	oneOf("SMTP_TLS", c.SMTPTLS, "none", "starttls", "implicit")
	if c.SMTPCAFile != "" {
		if c.SMTPTLS == "none" {
			fail("SMTP_CA_FILE", "not allowed with SMTP_TLS=none")
		}
		if err := checkPEM(c.SMTPCAFile); err != nil {
			fail("SMTP_CA_FILE", "%v", err)
		}
	}
	if (c.SMTPUsername == "") != (c.SMTPPassword == "") {
		fail("SMTP_USERNAME", "set together with SMTP_PASSWORD or not at all")
	}
	if c.SMTPPassword != "" && c.SMTPTLS == "none" {
		if host, _ := splitHostPort(c.SMTPAddr, 1); !isLoopback(host) {
			fail("SMTP_PASSWORD", "refusing to send credentials without TLS to a non-loopback SMTP_ADDR")
		}
	}
	positive("SMTP_TIMEOUT", c.SMTPTimeout)
	if c.NotifyRetryMaxElapsed <= c.SMTPTimeout {
		fail("NOTIFY_RETRY_MAX_ELAPSED", "must be > SMTP_TIMEOUT (%s), got %s", c.SMTPTimeout, c.NotifyRetryMaxElapsed)
	}
	positive("MESSAGE_DEADLINE", c.MessageDeadline)
	atLeast("RATE_PER_MIN", c.RatePerMin, 1)
	atLeast("RATE_BURST", c.RateBurst, 1)
	if c.DigestInterval < 5*time.Second {
		fail("DIGEST_INTERVAL", "must be ≥ 5s, got %s", c.DigestInterval)
	}
	atLeast("DIGEST_MAX", c.DigestMax, 1)
	atLeast("TRACEBACK_MAX_BYTES", c.TracebackMaxBytes, 0)
	atLeast("TEXT_MAX_BYTES", c.TextMaxBytes, 64)
	if c.TemplatesDir != "" {
		for _, name := range templateFiles {
			if fi, err := os.Stat(filepath.Join(c.TemplatesDir, name)); err != nil || !fi.Mode().IsRegular() {
				fail("TEMPLATES_DIR", "missing template file %s", name)
			}
		}
	}

	// Operations
	if _, err := splitHostPort(c.HTTPAddr, 0); err != nil {
		fail("HTTP_ADDR", "%v", err)
	}
	oneOf("LOG_LEVEL", c.LogLevel, "debug", "info", "warn", "error")
	if c.ShutdownTimeout <= c.SMTPTimeout {
		fail("SHUTDOWN_TIMEOUT", "must be > SMTP_TIMEOUT (%s), got %s", c.SMTPTimeout, c.ShutdownTimeout)
	}

	// Cross-field budgets (docs/ARCHITECTURE.md §8)
	if sum := c.Workers + c.DigestMax + c.FallbackMax; sum > c.AMQPPrefetch {
		fail("AMQP_PREFETCH", "WORKERS + DIGEST_MAX + FALLBACK_MAX (%d) must be ≤ AMQP_PREFETCH (%d)", sum, c.AMQPPrefetch)
	}
	if held := c.FallbackGrace + c.DigestInterval + c.NotifyRetryMaxElapsed; held >= c.MessageDeadline {
		fail("MESSAGE_DEADLINE", "FALLBACK_GRACE + DIGEST_INTERVAL + NOTIFY_RETRY_MAX_ELAPSED (%s) must be < MESSAGE_DEADLINE (%s)", held, c.MessageDeadline)
	}
	return errs
}

// Warnings lists non-fatal configuration risks (docs/CONFIG.md "warn").
func (c Config) Warnings() []string {
	var w []string
	errorsBound := false
	for _, k := range c.AMQPBindingKeys {
		last := k[strings.LastIndex(k, ".")+1:]
		errorsBound = errorsBound || last == "error" || last == "*" || last == "#"
	}
	if !errorsBound {
		w = append(w, "AMQP_BINDING_KEYS: no key binds *.error routing keys; build failures will be missed")
	}
	if c.BuildTimeoutEnabled && c.Store == "memory" {
		w = append(w, "BUILD_TIMEOUT_ENABLED: with STORE=memory this is only safe with a single replica (ADR-0004)")
	}
	if c.MessageDeadline >= brokerConsumerTimeout {
		w = append(w, fmt.Sprintf("MESSAGE_DEADLINE: %s is not below RabbitMQ's default consumer_timeout (30m); the broker may close the channel", c.MessageDeadline))
	}
	if host, err := splitHostPort(c.HTTPAddr, 0); err == nil && !isLoopback(host) {
		w = append(w, "HTTP_ADDR: not a loopback address; /metrics is reachable from the network")
	}
	if c.LogRawPayloads {
		w = append(w, "LOG_RAW_PAYLOADS: enabled; debug logs will contain tenant data")
	}
	return w
}

// SlogLevel returns LOG_LEVEL as a slog.Level (info for an invalid value).
func (c Config) SlogLevel() slog.Level {
	var l slog.Level
	if err := l.UnmarshalText([]byte(c.LogLevel)); err != nil {
		return slog.LevelInfo
	}
	return l
}

// LogValue renders every variable with secrets redacted.
func (c Config) LogValue() slog.Value {
	fs := c.fields()
	attrs := make([]slog.Attr, 0, len(fs))
	for _, f := range fs {
		attrs = append(attrs, slog.String(f.name, display(f)))
	}
	return slog.GroupValue(attrs...)
}

// String renders every variable as NAME=value with secrets redacted.
func (c Config) String() string {
	fs := c.fields()
	parts := make([]string, 0, len(fs))
	for _, f := range fs {
		parts = append(parts, f.name+"="+display(f))
	}
	return strings.Join(parts, " ")
}

func display(f field) string {
	switch p := f.ptr.(type) {
	case *Secret:
		if f.name == "AMQP_URL" && *p != "" {
			// Only a well-formed URL is shown (password removed); anything else may hide the
			// secret in an opaque part or path.
			if u, err := url.Parse(p.Reveal()); err == nil && u.Opaque == "" && u.Host != "" &&
				(u.Scheme == "amqp" || u.Scheme == "amqps") {
				return u.Redacted()
			}
		}
		return p.String()
	case *[]string:
		return strings.Join(*p, ",")
	case *mail.Address:
		if p.Address == "" {
			return ""
		}
		return p.String()
	case *[]mail.Address:
		s := make([]string, len(*p))
		for i, a := range *p {
			s[i] = a.String()
		}
		return strings.Join(s, ", ")
	case *string:
		return *p
	case *int:
		return strconv.Itoa(*p)
	case *bool:
		return strconv.FormatBool(*p)
	case *time.Duration:
		return p.String()
	}
	panic("config: unsupported field type for " + f.name)
}

// splitHostPort validates a host:port address. minPort is 1 for real endpoints; HTTP_ADDR also
// allows port 0, which asks the kernel for a free port (used by tests and some sandboxes).
func splitHostPort(addr string, minPort int) (string, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "", fmt.Errorf("must be host:port, got %q", addr)
	}
	if n, err := strconv.Atoi(port); err != nil || n < minPort || n > 65535 {
		return "", fmt.Errorf("port must be %d–65535, got %q", minPort, port)
	}
	return host, nil
}

func isLoopback(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func checkPEM(path string) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("cannot read %q: %w", path, errors.Unwrap(err))
	}
	if !x509.NewCertPool().AppendCertsFromPEM(b) {
		return fmt.Errorf("%q contains no PEM certificate", path)
	}
	return nil
}

func checkWritableDir(dir string) error {
	f, err := os.CreateTemp(dir, ".write-check-*")
	if err != nil {
		return fmt.Errorf("%q is not a writable directory", dir)
	}
	f.Close()
	return os.Remove(f.Name())
}
