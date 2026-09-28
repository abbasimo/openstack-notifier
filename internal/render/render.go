// Package render turns an Outcome into the email that reaches the admin.
//
// Contract: docs/ARCHITECTURE.md §4.8. Anatomy and override guide: docs/NOTIFICATIONS.md.
//
// Two rules govern everything here, because a tenant chooses the instance's display name and Nova
// copies tenant-influenced text into fault messages:
//   - HTML is rendered with html/template, never text/template, so escaping is automatic.
//   - Every value that reaches a header is CR/LF-stripped, and every free-text field is truncated
//     to a byte budget without splitting a UTF-8 rune.
package render

import (
	"bytes"
	"embed"
	"fmt"
	htmltemplate "html/template"
	"io/fs"
	"os"
	"strings"
	texttemplate "text/template"
	"time"
	"unicode/utf8"

	"github.com/abbasimo/openstack-notifier/internal/model"
	"github.com/abbasimo/openstack-notifier/internal/notify"
)

//go:embed templates/*.tmpl
var builtinTemplates embed.FS

// Template file names. config.templateFiles must list exactly these (a startup check).
var templateNames = []string{
	"success.html.tmpl", "success.txt.tmpl",
	"failure.html.tmpl", "failure.txt.tmpl",
	"timeout.html.tmpl", "timeout.txt.tmpl",
	"digest.html.tmpl", "digest.txt.tmpl",
}

// Limits are the byte budgets for untrusted text.
type Limits struct {
	TracebackBytes int // TRACEBACK_MAX_BYTES; 0 omits tracebacks entirely
	TextBytes      int // TEXT_MAX_BYTES, per free-text field
}

// Renderer holds the parsed templates. Parsing happens once, at startup: a broken template must
// fail the process, not the first failure email.
type Renderer struct {
	limits Limits
	now    func() time.Time
	html   map[string]*htmltemplate.Template
	text   map[string]*texttemplate.Template
}

// New parses every template. With templatesDir set, files are read from there instead of the
// embedded copies; all eight must be present. now is the pipeline's clock (nil means time.Now);
// the only thing it dates is the digest header.
func New(templatesDir string, l Limits, now func() time.Time) (*Renderer, error) {
	var source fs.FS = builtinTemplates
	prefix := "templates/"
	if templatesDir != "" {
		source, prefix = os.DirFS(templatesDir), ""
	}

	if now == nil {
		now = time.Now
	}
	r := &Renderer{
		limits: l,
		now:    now,
		html:   make(map[string]*htmltemplate.Template, 4),
		text:   make(map[string]*texttemplate.Template, 4),
	}
	for _, name := range templateNames {
		content, err := fs.ReadFile(source, prefix+name)
		if err != nil {
			return nil, fmt.Errorf("reading template %s: %w", name, err)
		}
		if strings.HasSuffix(name, ".html.tmpl") {
			t, err := htmltemplate.New(name).Parse(string(content))
			if err != nil {
				return nil, fmt.Errorf("parsing template %s: %w", name, err)
			}
			r.html[name] = t
			continue
		}
		t, err := texttemplate.New(name).Parse(string(content))
		if err != nil {
			return nil, fmt.Errorf("parsing template %s: %w", name, err)
		}
		r.text[name] = t
	}
	return r, nil
}

// Outcome renders one build outcome.
func (r *Renderer) Outcome(o model.Outcome) (notify.Message, error) {
	view := r.view(o)
	base := templateBase(o.Kind)

	msg := notify.Message{
		Subject: subject(o, view),
		Headers: map[string]string{
			"X-OpenStack-Instance-UUID": StripCRLF(o.Event.Instance.UUID),
			"X-OpenStack-Outcome":       o.Kind.String(),
		},
	}
	if id := o.Event.Instance.RequestID; id != "" {
		msg.Headers["X-OpenStack-Request-ID"] = StripCRLF(id)
	}

	var err error
	if msg.TextBody, err = r.execText(base+".txt.tmpl", view); err != nil {
		return notify.Message{}, err
	}
	if msg.HTMLBody, err = r.execHTML(base+".html.tmpl", view); err != nil {
		return notify.Message{}, err
	}
	return msg, nil
}

// Digest renders several outcomes as one email. Failures come first: a digest exists because
// something bulk happened, and the failures are what the admin must act on.
func (r *Renderer) Digest(outcomes []model.Outcome) (notify.Message, error) {
	if len(outcomes) == 0 {
		return notify.Message{}, fmt.Errorf("render: empty digest")
	}
	view := digestView{Generated: r.now().UTC().Format(timeFormat)}
	for _, o := range outcomes {
		v := r.view(o)
		switch o.Kind {
		case model.OutcomeSuccess:
			view.Successes = append(view.Successes, v)
		case model.OutcomeTimeout:
			view.Timeouts = append(view.Timeouts, v)
		default:
			view.Failures = append(view.Failures, v)
		}
	}
	view.Total = len(outcomes)
	view.FailureCount = len(view.Failures) + len(view.Timeouts)

	msg := notify.Message{
		Subject: fmt.Sprintf("[OpenStack] %d VM build notifications (%d failed)", view.Total, view.FailureCount),
		Headers: map[string]string{"X-OpenStack-Outcome": "digest"},
	}
	var err error
	if msg.TextBody, err = r.execText("digest.txt.tmpl", view); err != nil {
		return notify.Message{}, err
	}
	if msg.HTMLBody, err = r.execHTML("digest.html.tmpl", view); err != nil {
		return notify.Message{}, err
	}
	return msg, nil
}

// templateBase picks the template family for an outcome kind.
func templateBase(kind model.OutcomeKind) string {
	switch kind {
	case model.OutcomeSuccess:
		return "success"
	case model.OutcomeTimeout:
		return "timeout"
	default:
		return "failure"
	}
}

// subject builds the subject line (Plan.md 6.2 + ARCHITECTURE §4.8). The instance may have no
// name at all — scheduling failures carry none — so every variant degrades to the UUID.
func subject(o model.Outcome, v outcomeView) string {
	switch {
	case o.Kind == model.OutcomeSuccess:
		return fmt.Sprintf("[OpenStack] VM %s created successfully (%s)", quoted(v.Name), v.UUID)
	case o.Kind == model.OutcomeTimeout:
		return fmt.Sprintf("[OpenStack] VM %s build timed out (%s)", quoted(v.Name), v.UUID)
	case o.Fallback:
		return fmt.Sprintf("[OpenStack] VM %s entered ERROR during build", nameOrUUID(v))
	case o.Stage == model.StageScheduling:
		if v.Name == "" {
			return fmt.Sprintf("[OpenStack] VM %s FAILED to schedule", v.UUID)
		}
		return fmt.Sprintf("[OpenStack] VM %s FAILED to schedule (%s)", quoted(v.Name), v.UUID)
	case v.Host != "":
		return fmt.Sprintf("[OpenStack] VM %s FAILED to build on %s (%s)", quoted(v.Name), v.Host, v.UUID)
	default:
		return fmt.Sprintf("[OpenStack] VM %s FAILED to build (%s)", quoted(v.Name), v.UUID)
	}
}

func quoted(name string) string {
	if name == "" {
		return `"(unnamed)"`
	}
	return `"` + name + `"`
}

func nameOrUUID(v outcomeView) string {
	if v.Name != "" {
		return `"` + v.Name + `"`
	}
	return v.UUID
}

func (r *Renderer) execText(name string, data any) (string, error) {
	t, ok := r.text[name]
	if !ok {
		return "", fmt.Errorf("render: no template %s", name)
	}
	// Into a buffer: a template that fails halfway must not produce half an email.
	var b bytes.Buffer
	if err := t.Execute(&b, data); err != nil {
		return "", fmt.Errorf("rendering %s: %w", name, err)
	}
	return b.String(), nil
}

func (r *Renderer) execHTML(name string, data any) (string, error) {
	t, ok := r.html[name]
	if !ok {
		return "", fmt.Errorf("render: no template %s", name)
	}
	var b bytes.Buffer
	if err := t.Execute(&b, data); err != nil {
		return "", fmt.Errorf("rendering %s: %w", name, err)
	}
	return b.String(), nil
}

// StripCRLF removes carriage returns and line feeds. Any value that reaches a mail header goes
// through it: a `\r\nBcc:` in a display name would otherwise be a header-injection primitive.
func StripCRLF(s string) string {
	if !strings.ContainsAny(s, "\r\n") {
		return s
	}
	return strings.Join(strings.FieldsFunc(s, func(r rune) bool { return r == '\r' || r == '\n' }), " ")
}

// Truncate shortens s to at most maxBytes bytes without splitting a rune, appending an ellipsis
// when it cut something. maxBytes ≤ 0 returns "".
func Truncate(s string, maxBytes int) string {
	if maxBytes <= 0 {
		return ""
	}
	if len(s) <= maxBytes {
		return s
	}
	const ellipsis = "…"
	cut := maxBytes
	if maxBytes > len(ellipsis) {
		cut = maxBytes - len(ellipsis)
	}
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	// A rune may start at cut but still be cut short; drop it if so.
	for cut > 0 {
		if r, size := utf8.DecodeLastRuneInString(s[:cut]); r == utf8.RuneError && size <= 1 {
			cut--
			continue
		}
		break
	}
	if cut <= 0 {
		return ""
	}
	return s[:cut] + ellipsis
}
