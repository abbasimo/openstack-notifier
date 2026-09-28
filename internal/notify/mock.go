package notify

import (
	"context"
	"fmt"
	"log/slog"
	"net/mail"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Mock is the no-infrastructure notifier: it keeps the last messages in memory, logs a one-line
// summary of each, and can drop `.eml` files for inspection. `NOTIFIER=mock` is what makes
// `make run` and the load tests work without any SMTP server at all.
//
// The "real SMTP, fake delivery" path is Mailpit in compose; this is one step simpler.
type Mock struct {
	name   string
	keep   int
	emlDir string
	log    *slog.Logger
	now    func() time.Time
	from   mail.Address
	to     []mail.Address

	mu       sync.Mutex
	messages []Message // ring buffer of the last `keep`
	sent     int
	failWith error // set by FailWith, for tests
}

// NewMock returns a Mock keeping the last `keep` messages. When emlDir is not empty, each message
// is also written there as an .eml file.
func NewMock(keep int, emlDir string, log *slog.Logger) *Mock {
	return &Mock{
		name:   "mock",
		keep:   keep,
		emlDir: emlDir,
		log:    log,
		now:    time.Now,
		from:   mail.Address{Name: "VM Notifier", Address: "notifier@localhost"},
		to:     []mail.Address{{Address: "admin@localhost"}},
	}
}

func (m *Mock) Name() string { return m.name }

// Notify records the message. It never touches the network.
func (m *Mock) Notify(_ context.Context, msg Message) error {
	m.mu.Lock()
	if err := m.failWith; err != nil {
		m.mu.Unlock()
		return err
	}
	m.messages = append(m.messages, msg)
	if m.keep > 0 && len(m.messages) > m.keep {
		m.messages = m.messages[len(m.messages)-m.keep:]
	}
	m.sent++
	seq := m.sent
	m.mu.Unlock()

	m.log.Info("mock notification", "subject", msg.Subject, "seq", seq,
		slog.Any("headers", msg.Headers))

	if m.emlDir != "" {
		if err := m.writeEML(msg, seq); err != nil {
			// Losing the file copy must not look like a failed send, or the delivery is requeued
			// and the admin gets the same email again.
			m.log.Warn("cannot write the .eml copy", "error", err.Error())
		}
	}
	return nil
}

func (m *Mock) writeEML(msg Message, seq int) error {
	body, err := build(msg, m.from, m.to, m.now(), "localhost")
	if err != nil {
		return err
	}
	name := filepath.Join(m.emlDir, fmt.Sprintf("%s-%04d.eml", m.now().UTC().Format("20060102-150405"), seq))
	return os.WriteFile(name, body, 0o600)
}

// Messages returns the messages kept, oldest first.
func (m *Mock) Messages() []Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]Message(nil), m.messages...)
}

// Count returns how many messages were sent in total, including ones the ring buffer dropped.
func (m *Mock) Count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.sent
}

// FailWith makes the next sends fail with err (nil restores success). Tests use it to exercise the
// retry and dedup paths; production never calls it.
func (m *Mock) FailWith(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failWith = err
}

// Reset clears the recorded messages.
func (m *Mock) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.messages, m.sent = nil, 0
}
