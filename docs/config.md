# Configuration

There are no flags. Every setting below is one variable that can come from **the environment** or
from a **YAML file**, in that order of precedence (ADR-0011):

```text
environment variable  →  CONFIG_FILE (YAML)  →  default
```

`config.LoadFile` reports **every** problem at once (one log record, exit code 2). Warnings are
logged at startup and do not stop it. An **empty value is the same as unset** — the next source, or
the default, applies.

- **Secret** (🔒): never logged (`Config.LogValue()`/`String()` redact it; `AMQP_URL` via
  `url.URL.Redacted()`). Each secret also accepts `<NAME>_FILE` (path; trailing newline trimmed).
  Setting both `<NAME>` and `<NAME>_FILE` **in the same source** is an error; across sources the
  pair is resolved from the first source that sets either, so `AMQP_URL` in the environment
  overrides `amqp.url_file` in the file rather than colliding with it.
- Types: *duration* = Go `time.ParseDuration` (`90s`, `15m`, `24h`); *bool* = `strconv.ParseBool`;
  *list* = comma-separated, items trimmed, empty items rejected (a YAML sequence in the file).
- Supersedes Plan.md §2.3. Additions: `AMQP_CA_FILE`, `AMQP_DIAL_TIMEOUT`, `EVENT_TYPES`,
  `FALLBACK_GRACE`, `FALLBACK_MAX` (ADR-0009), `SMTP_CA_FILE`, `CONFIG_FILE` (ADR-0011).

## The file

`CONFIG_FILE=/etc/vm-notifier/config.yaml` names it; unset, the environment is the only source.
`deploy/config.sample.yaml` is a complete, commented copy of every setting at its default (a test
keeps it that way). The file adds **no settings of its own** — it is a second way to write the ones
in the tables below:

- A key is the variable name, lower-cased. `_` may be written as nesting, so `amqp_queue_ttl: 48h`,
  `amqp: {queue_ttl: 48h}` and `amqp: {queue: {ttl: 48h}}` are the same setting. `-` is read as `_`.
- A scalar is taken **exactly as written** (`24h`, `16`, `false`) and reaches the same parser an
  environment variable would, so YAML's own typing cannot reshape a value.
- A list may be a YAML sequence (`mail: {to: [a@x, b@x]}`) or the comma-separated string. An item
  containing a comma is rejected — write the whole list as one string instead.
- An empty value (`templates_dir:`) is unset, not `""` as a value.
- Unknown keys, a key repeated under two spellings, a non-mapping top level and `config_file`
  itself are errors, each reported with its line: `config.yaml:12: unknown setting "amqp.urls"`.
  A file that cannot be read or parsed is reported **on its own**, before any value is validated:
  with the file in doubt, "MAIL_TO: required" would be a guess.
- Anchors and aliases work; multiple YAML documents in one file do not.
- A secret written literally in the file is in clear text on disk. Prefer a `*_file` key or the
  environment; if you do it anyway, `chmod 0600` the file.

Deployment recipes (systemd, Compose, Kubernetes, and what `.env` does and does not do) are in
[DEPLOYMENT.md](DEPLOYMENT.md#0-configuration-in-practice).

## AMQP

| Variable | Default | Req. | 🔒 | Validation |
|---|---|---|---|---|
| `AMQP_URL` | — | yes | 🔒 | scheme `amqp` or `amqps`; host non-empty |
| `AMQP_CA_FILE` | empty | | | if set: readable PEM with ≥1 cert; requires `amqps` |
| `AMQP_EXCHANGE` | `nova` | | | non-empty; passive-declared only (ADR-0006) |
| `AMQP_BINDING_KEYS` | `versioned_notifications.info,versioned_notifications.error` | | | list, ≥1 item; **warn** unless some key ends in `error`, `*` or `#` (failures would be missed) |
| `AMQP_QUEUE` | `vm_notifier` | | | 1–240 bytes (DLX/DLQ names append `.dlx`/`.dlq`; AMQP limit 255) |
| `AMQP_QUEUE_TYPE` | `classic` | | | `classic` \| `quorum` |
| `AMQP_QUEUE_TTL` | `24h` | | | duration > 0 → `x-message-ttl` |
| `AMQP_QUEUE_MAX_LENGTH` | `100000` | | | int > 0 → `x-max-length` (overflow `drop-head`) |
| `AMQP_DELIVERY_LIMIT` | `1000` | | | int > 0; used only with `quorum` → `x-delivery-limit` |
| `AMQP_DLQ_TTL` | `168h` | | | duration > 0 |
| `AMQP_DLQ_MAX_LENGTH` | `10000` | | | int > 0 |
| `AMQP_PREFETCH` | `64` | | | 1–65535 |
| `AMQP_HEARTBEAT` | `10s` | | | ≥ 1s |
| `AMQP_DIAL_TIMEOUT` | `10s` | | | > 0 |
| `WORKERS` | `16` | | | ≥ 1 |
| `MAX_BODY_BYTES` | `1048576` | | | 1024 – 16777216 |
| `EVENT_TYPES` | `instance.create.start,instance.create.end,instance.create.error,compute_task.build_instances.error,instance.update` | | | list; each must be one of these five (the parser has no decoder for others); must include `instance.create.end` or a failure type |

Changing `AMQP_QUEUE_*` / `AMQP_DLQ_*` / `AMQP_DELIVERY_LIMIT` after the queue exists makes the broker
reject the declaration (406); delete or migrate the queue (see MESSAGING.md, Phase 3).

## State & correlation

| Variable | Default | Req. | 🔒 | Validation |
|---|---|---|---|---|
| `STORE` | `memory` | | | `memory` (`redis` only after Phase 10.1) |
| `STORE_MAX_ENTRIES` | `100000` | | | ≥ 1000 |
| `BUILD_TIMEOUT_ENABLED` | `false` | | | bool; **warn** when `true` with `STORE=memory`: "only safe with a single replica" (ADR-0004) |
| `BUILD_TIMEOUT` | `15m` | | | ≥ 1m |
| `FALLBACK_GRACE` | `60s` | | | ≥ 0; `0` disables the hold (ADR-0009) |
| `FALLBACK_MAX` | `8` | | | ≥ 0 |

## Notification

| Variable | Default | Req. | 🔒 | Validation |
|---|---|---|---|---|
| `NOTIFIER` | `smtp` | | | `smtp` \| `mock` |
| `MOCK_EML_DIR` | empty | | | if set: existing writable directory |
| `MAIL_FROM` | — | yes | | `net/mail.ParseAddress`; no CR/LF |
| `MAIL_TO` | — | yes | | `net/mail.ParseAddressList`, ≥1 address; no CR/LF |
| `SMTP_ADDR` | — | if smtp | | `host:port`, numeric port 1–65535 |
| `SMTP_TLS` | `starttls` | | | `none` \| `starttls` \| `implicit` |
| `SMTP_CA_FILE` | empty | | | if set: readable PEM; not with `none` |
| `SMTP_USERNAME` | empty | | 🔒 | set together with `SMTP_PASSWORD` or not at all |
| `SMTP_PASSWORD` | empty | | 🔒 | with `SMTP_TLS=none`: error unless `SMTP_ADDR` host is loopback |
| `SMTP_TIMEOUT` | `15s` | | | > 0, per attempt (dial + session) |
| `NOTIFY_RETRY_MAX_ELAPSED` | `10m` | | | > `SMTP_TIMEOUT` |
| `MESSAGE_DEADLINE` | `20m` | | | > 0; **warn** if ≥ `30m` (RabbitMQ default `consumer_timeout`) |
| `RATE_PER_MIN` | `30` | | | ≥ 1 |
| `RATE_BURST` | `10` | | | ≥ 1 |
| `DIGEST_INTERVAL` | `60s` | | | ≥ 5s |
| `DIGEST_MAX` | `40` | | | ≥ 1 |
| `TRACEBACK_MAX_BYTES` | `8192` | | | ≥ 0 (`0` omits tracebacks) |
| `TEXT_MAX_BYTES` | `1024` | | | ≥ 64 (per text field, e.g. display name, exception message) |
| `TEMPLATES_DIR` | empty | | | if set: directory containing all eight template files |

## Operations

| Variable | Default | Req. | 🔒 | Validation |
|---|---|---|---|---|
| `CONFIG_FILE` | empty | | | path to the YAML file; **read from the environment only** (the file cannot name itself). If set: must be readable and parse |
| `HTTP_ADDR` | `127.0.0.1:9090` | | | `host:port`, port 0–65535 (`0` = pick a free port); **warn** if host is not loopback (`/metrics` exposure) |
| `LOG_LEVEL` | `info` | | | `debug` \| `info` \| `warn` \| `error` |
| `LOG_RAW_PAYLOADS` | `false` | | | bool; effective only at `debug`; **warn** when enabled (payloads contain tenant data) |
| `SHUTDOWN_TIMEOUT` | `30s` | | | > `SMTP_TIMEOUT` (final digest flush is one send) |

## Cross-field rules

| Rule | Why |
|---|---|
| `WORKERS + DIGEST_MAX + FALLBACK_MAX ≤ AMQP_PREFETCH` | Held deliveries occupy prefetch slots; otherwise workers starve (ARCHITECTURE §8) |
| `FALLBACK_GRACE + DIGEST_INTERVAL + NOTIFY_RETRY_MAX_ELAPSED < MESSAGE_DEADLINE` | Longest time a delivery can stay unacked must fit the per-message deadline |
| `SMTP_TIMEOUT < NOTIFY_RETRY_MAX_ELAPSED` | At least one retry |
| `SMTP_USERNAME`/`SMTP_PASSWORD` both or neither; auth needs TLS unless loopback | No credentials in clear text |
| `SMTP_*` required only when `NOTIFIER=smtp` | Mock needs no network |
| `AMQP_CA_FILE` only with `amqps://`; `SMTP_CA_FILE` not with `SMTP_TLS=none` | Misconfiguration signal |
| `SHUTDOWN_TIMEOUT > SMTP_TIMEOUT` | Final digest flush can complete |

## Implementation (Phase 2; file support ADR-0011)

`internal/config` is the only place that reads the environment or a configuration file.

```go
func LoadFile(lookup func(string) (string, bool)) (Config, error) // what the service calls: env over CONFIG_FILE
func Load(lookup func(string) (string, bool)) (Config, error)     // parse + validate one source; errors.Join of every problem
func Sources(lookup func(string) (string, bool)) (func(string) (string, bool), error) // the layered lookup
func FromYAML(name string, data []byte) (Source, error)           // file → Source, every problem at once
func Layered(sources ...Source) func(string) (string, bool)       // first non-empty wins; secret pairs stay together
func (c Config) Validate() error                                  // ranges, cross-field rules, referenced files
func (c Config) Warnings() []string                               // non-fatal risks, logged at startup
func (c Config) SlogLevel() slog.Level
func Default(name string) string                                  // documented default of one variable
type Secret string                                                // Reveal() is the only way to read it
type Source struct{ Name string; Lookup func(string) (string, bool) }
```

- One table in `config.go` holds every variable's name, default and destination; `Load`, `LogValue`,
  `String` and the file's own known-key set all iterate it, and `TestDefaultsMatchDocs` fails if
  this document and that table disagree on a name or a default. Add a variable in both places —
  the file needs no change, and `TestSampleConfigIsComplete` will ask for the sample's line.
- `Load` takes one lookup, so the file is invisible to it: a value that came from the file is
  validated, reported and logged under its variable name, like any other.
- Secrets are typed `Secret`: every formatting path (`%v`, `%#v`, `slog`, JSON, text) prints
  `[REDACTED]`, so a secret cannot leak through a new log line by accident. `AMQP_URL` is shown as
  `url.URL.Redacted()` when it parses, `[REDACTED]` otherwise.
- Error messages are `VARIABLE: problem` and never contain a secret value.

## Minimal examples

```bash
# demo, no network
AMQP_URL=amqp://guest:guest@localhost:5672/ NOTIFIER=mock MAIL_FROM=notifier@example.org MAIL_TO=admin@example.org

# Kolla all-in-one, local relay with STARTTLS + auth, secrets from files
AMQP_URL_FILE=/run/secrets/amqp_url
MAIL_FROM=vm-notifier@cloud.example.org MAIL_TO=admin@example.org
SMTP_ADDR=smtp.example.org:587 SMTP_USERNAME_FILE=/run/secrets/smtp_user SMTP_PASSWORD_FILE=/run/secrets/smtp_pass
```

The same second example as a file (`CONFIG_FILE=/etc/vm-notifier/config.yaml`), with the two
credentials left in the environment:

```yaml
amqp:
  url_file: /run/secrets/amqp_url
mail:
  from: vm-notifier@cloud.example.org
  to: [admin@example.org, ops@example.org]
smtp:
  addr: smtp.example.org:587
  tls: starttls
log_level: info
```
