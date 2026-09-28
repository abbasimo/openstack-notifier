package notify

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"mime"
	"mime/multipart"
	"mime/quotedprintable"
	"net/mail"
	"slices"
	"strings"
	"time"
)

// crlf is the line ending required by RFC 5322 on the wire.
const crlf = "\r\n"

// build renders one Message as an RFC 5322 message with a multipart/alternative body.
//
// Everything that reaches a header goes through sanitizeHeader first. The display name in a
// subject is tenant-controlled, and a bare CR or LF in a header value is a header-injection
// primitive — this is the last place to stop it, so it is stopped here as well as in the renderer.
func build(msg Message, from mail.Address, to []mail.Address, now time.Time, domain string) ([]byte, error) {
	var body bytes.Buffer
	mp := multipart.NewWriter(&body)
	if err := writePart(mp, "text/plain; charset=utf-8", msg.TextBody); err != nil {
		return nil, fmt.Errorf("writing the text part: %w", err)
	}
	if msg.HTMLBody != "" {
		if err := writePart(mp, "text/html; charset=utf-8", msg.HTMLBody); err != nil {
			return nil, fmt.Errorf("writing the html part: %w", err)
		}
	}
	if err := mp.Close(); err != nil {
		return nil, fmt.Errorf("closing the multipart body: %w", err)
	}

	recipients := make([]string, len(to))
	for i, addr := range to {
		recipients[i] = addr.String()
	}

	var b strings.Builder
	header := func(name, value string) {
		b.WriteString(name)
		b.WriteString(": ")
		b.WriteString(sanitizeHeader(value))
		b.WriteString(crlf)
	}
	header("From", from.String())
	header("To", strings.Join(recipients, ", "))
	header("Subject", encodeHeader(msg.Subject))
	header("Date", now.Format(time.RFC1123Z))
	header("Message-ID", messageID(domain))
	header("MIME-Version", "1.0")
	// Tells mail systems this is machine-generated, so vacation responders stay quiet.
	header("Auto-Submitted", "auto-generated")
	for _, name := range sortedKeys(msg.Headers) {
		header(name, msg.Headers[name])
	}
	header("Content-Type", "multipart/alternative; boundary="+mp.Boundary())
	b.WriteString(crlf)
	b.Write(body.Bytes())

	return []byte(b.String()), nil
}

func writePart(mp *multipart.Writer, contentType, content string) error {
	part, err := mp.CreatePart(map[string][]string{
		"Content-Type":              {contentType},
		"Content-Transfer-Encoding": {"quoted-printable"},
	})
	if err != nil {
		return err
	}
	qp := quotedprintable.NewWriter(part)
	if _, err := qp.Write([]byte(content)); err != nil {
		return err
	}
	return qp.Close()
}

// sanitizeHeader removes everything that could start a new header line. Folding whitespace is
// collapsed to a single space rather than preserved: no header we emit needs continuation lines.
func sanitizeHeader(v string) string {
	if !strings.ContainsAny(v, "\r\n") {
		return v
	}
	return strings.Join(strings.FieldsFunc(v, func(r rune) bool { return r == '\r' || r == '\n' }), " ")
}

// encodeHeader Q-encodes a value when it is not pure ASCII, so `سرور-۱` survives as a subject
// instead of arriving as mojibake or being dropped by a strict relay.
func encodeHeader(v string) string {
	v = sanitizeHeader(v)
	for i := range len(v) {
		if v[i] > 127 {
			return mime.QEncoding.Encode("utf-8", v)
		}
	}
	return v
}

// messageID returns a globally unique id in the From domain. Nova's hostname is deliberately not
// used: it would leak an internal name into every email.
func messageID(domain string) string {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		// crypto/rand does not fail in practice; a time-based id still beats no Message-ID.
		return fmt.Sprintf("<%d@%s>", time.Now().UnixNano(), domain)
	}
	return "<" + hex.EncodeToString(buf[:]) + "@" + domain + ">"
}

// sortedKeys keeps header order stable, which is what makes golden .eml tests possible.
func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}
