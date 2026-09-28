## 1. Context

```text
 ┌──────────── Kolla all-in-one ─────────────┐
 │ nova-compute ─┐                            │
 │ nova-conductor┼─▶ RabbitMQ  exchange "nova"│ (topic, owned by Nova)
 │ nova-api ─────┘      │ versioned_notifications.info / .error
 │                      ├─▶ versioned_notifications.{info,error}  (Nova-declared; capped by policy, never consumed by us)
 │                      └─▶ vm_notifier ──DLX──▶ vm_notifier.dlx ─▶ vm_notifier.dlq
 └──────────────────────────│─────────────────┘
                            ▼ competing consumers (1..N replicas)
                   ┌─────────────────┐   SMTP (none/STARTTLS/implicit TLS)
                   │  vm-notifier    │ ─────────────────────────────────▶ relay ─▶ admin
                   │ :9090 health    │
                   └─────────────────┘
```

## 2. Pipeline

```text
 delivery ─▶ [1] listener worker (WORKERS goroutines, prefetch AMQP_PREFETCH, manual ack)
               │ Handler(ctx with MESSAGE_DEADLINE, body, acker) ─▶ Disposition ─▶ Ack / Nack
               ▼
            [2] parser.Parse: size guard → envelope → tier-1 allow-list → tier-2 (instance.update b→error) → full decode
               │ err ▶ DeadLetter          filtered ▶ Ack
               ▼
            [3] correlator.Observe: event → Outcome (or none)            none ▶ Ack
               │
               ├─ Outcome.Fallback ─▶ [3b] holdback.Offer ── accepted ▶ Deferred ──(FALLBACK_GRACE)──┐
               ▼                                                                                   │
  ┌───────── tail(ctx, outcome, acker) ◀─────────────────────────────────────────────────────────┘
  │         [4] dedup.Check                                              duplicate ▶ Ack
  │         [5] ratelimit.Bucket.Allow ─ no ─▶ digest.Add ── accepted ▶ Deferred ─(DIGEST_INTERVAL/full)─▶ FlushDigest
  │         [6] render.Outcome                                           err ▶ Release, DeadLetter
  │         [7] notifier.Notify (Retry-wrapped: in-worker backoff until NOTIFY_RETRY_MAX_ELAPSED)
  │               ok ▶ Commit, Ack   permanent ▶ Release, DeadLetter   transient/ctx done ▶ Release, Requeue
  └──────────
 background: holdback.Run · digest.Run · correlator timeout ticker (D6) · store sweeper · HTTP server
```
