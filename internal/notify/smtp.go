package notify

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/mail"
	"net/smtp"
	"net/textproto"
	"os"
	"strings"
	"time"
)

// TLS modes for SMTPConfig.TLSMode (docs/CONFIG.md → SMTP_TLS).
const (
	TLSNone     = "none"     // plaintext; only sane for a relay on localhost
	TLSStartTLS = "starttls" // port 587: STARTTLS is *required*, not opportunistic
	TLSImplicit = "implicit" // port 465: TLS from the first byte
)

// SMTPConfig is everything the SMTP notifier needs. From and To are pre-parsed by config.
type SMTPConfig struct {
	Addr     string
	TLSMode  string
	CAFile   string
	Username string
	Password string
	From     mail.Address
	To       []mail.Address
	Timeout  time.Duration // per attempt: dial plus the whole session

	// Now is the clock used for the Date header only; nil means time.Now. Deadlines always use
	// the real clock — a test or display clock must never be able to expire a live connection.
	Now func() time.Time
}

type smtpNotifier struct {
	cfg    SMTPConfig
	host   string
	tls    *tls.Config
	domain string // From's domain, used for Message-ID
}

// NewSMTP validates the configuration and returns a Notifier. It does not connect: a broker that
// starts before the relay must still come up, and /readyz reflects AMQP, not SMTP.
func NewSMTP(cfg SMTPConfig) (Notifier, error) {
	host, _, err := net.SplitHostPort(cfg.Addr)
	if err != nil {
		return nil, fmt.Errorf("SMTP address %q: %w", cfg.Addr, err)
	}
	if len(cfg.To) == 0 {
		return nil, errors.New("smtp: no recipients")
	}
	switch cfg.TLSMode {
	case TLSNone, TLSStartTLS, TLSImplicit:
	default:
		return nil, fmt.Errorf("smtp: unknown TLS mode %q", cfg.TLSMode)
	}
	// Credentials in the clear are refused here as well as in config: this is the code that would
	// actually put them on the wire.
	if cfg.Username != "" && cfg.TLSMode == TLSNone && !isLoopback(host) {
		return nil, fmt.Errorf("smtp: refusing to send credentials to %s without TLS", cfg.Addr)
	}
	n := &smtpNotifier{cfg: cfg, host: host, domain: domainOf(cfg.From)}
	if cfg.TLSMode != TLSNone {
		if n.tls, err = tlsConfig(host, cfg.CAFile); err != nil {
			return nil, err
		}
	}
	if n.cfg.Now == nil {
		n.cfg.Now = time.Now
	}
	return n, nil
}

func (s *smtpNotifier) Name() string { return "smtp" }

// Notify sends one message on its own connection. No pooling: at tens of emails a minute the
// connection setup is irrelevant next to the clarity of never reusing a session that may have
// been half-closed by an idle timeout.
func (s *smtpNotifier) Notify(ctx context.Context, msg Message) error {
	body, err := build(msg, s.cfg.From, s.cfg.To, s.cfg.Now(), s.domain)
	if err != nil {
		// A message we cannot even assemble will not assemble on a retry either.
		return fmt.Errorf("%w: building the message: %w", ErrPermanent, err)
	}

	conn, err := s.dial(ctx)
	if err != nil {
		return err // transient: the relay may be restarting
	}
	defer conn.Close()

	// One deadline for the whole session, so a relay that accepts the connection and then stalls
	// cannot hold a worker past NOTIFY_RETRY_MAX_ELAPSED.
	deadline := time.Now().Add(s.cfg.Timeout)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}
	if err := conn.SetDeadline(deadline); err != nil {
		return fmt.Errorf("setting the connection deadline: %w", err)
	}

	client, err := smtp.NewClient(conn, s.host)
	if err != nil {
		return classifySMTP("greeting", err)
	}
	defer client.Close()

	if s.cfg.TLSMode == TLSStartTLS {
		if ok, _ := client.Extension("STARTTLS"); !ok {
			// Required, not opportunistic: silently continuing in plaintext is how credentials
			// and tenant data end up on the wire.
			return fmt.Errorf("%w: %s does not offer STARTTLS", ErrPermanent, s.cfg.Addr)
		}
		if err := client.StartTLS(s.tls); err != nil {
			return classifySMTP("starttls", err)
		}
	}
	if s.cfg.Username != "" {
		auth := smtp.PlainAuth("", s.cfg.Username, s.cfg.Password, s.host)
		if err := client.Auth(auth); err != nil {
			// Bad credentials do not become good on a retry.
			return fmt.Errorf("%w: authenticating: %w", ErrPermanent, err)
		}
	}

	if err := client.Mail(s.cfg.From.Address); err != nil {
		return classifySMTP("MAIL FROM", err)
	}
	for _, to := range s.cfg.To {
		if err := client.Rcpt(to.Address); err != nil {
			return classifySMTP("RCPT TO", err)
		}
	}
	w, err := client.Data()
	if err != nil {
		return classifySMTP("DATA", err)
	}
	if _, err := w.Write(body); err != nil {
		return fmt.Errorf("writing the message: %w", err)
	}
	if err := w.Close(); err != nil {
		return classifySMTP("end of data", err)
	}
	if err := client.Quit(); err != nil {
		// The message was accepted at end-of-data; a failed QUIT is cosmetic and must not cause
		// a retry, which would send it twice.
		return nil
	}
	return nil
}

func (s *smtpNotifier) dial(ctx context.Context) (net.Conn, error) {
	d := &net.Dialer{Timeout: s.cfg.Timeout}
	if s.cfg.TLSMode == TLSImplicit {
		conn, err := (&tls.Dialer{NetDialer: d, Config: s.tls}).DialContext(ctx, "tcp", s.cfg.Addr)
		if err != nil {
			return nil, fmt.Errorf("connecting to %s over TLS: %w", s.cfg.Addr, err)
		}
		return conn, nil
	}
	conn, err := d.DialContext(ctx, "tcp", s.cfg.Addr)
	if err != nil {
		return nil, fmt.Errorf("connecting to %s: %w", s.cfg.Addr, err)
	}
	return conn, nil
}

// classifySMTP decides whether an error is worth retrying. 5xx is a refusal — the relay has made
// up its mind — so it goes to the DLQ instead of looping. Everything else (4xx, timeouts, resets)
// is transient.
func classifySMTP(stage string, err error) error {
	var protoErr *textproto.Error
	if errors.As(err, &protoErr) && protoErr.Code >= 500 {
		return fmt.Errorf("%w: %s: %w", ErrPermanent, stage, err)
	}
	return fmt.Errorf("%s: %w", stage, err)
}

func tlsConfig(host, caFile string) (*tls.Config, error) {
	cfg := &tls.Config{ServerName: host, MinVersion: tls.VersionTLS12}
	if caFile == "" {
		return cfg, nil // the system roots; never InsecureSkipVerify
	}
	pem, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("reading SMTP_CA_FILE: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("SMTP_CA_FILE %s contains no certificate", caFile)
	}
	cfg.RootCAs = pool
	return cfg, nil
}

func isLoopback(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func domainOf(addr mail.Address) string {
	if _, domain, ok := strings.Cut(addr.Address, "@"); ok && domain != "" {
		return domain
	}
	return "localhost"
}
