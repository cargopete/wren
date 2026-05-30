# wren roadmap

Target: **feature parity with `bunnyhop`** (the Rust messaging crate in
`platform-backend`), expressed *idiomatically in Gleam*.

We match bunnyhop's **capabilities**, not its Rust-specific machinery. Gleam has
no typeclasses, proc-macros, or variadic generics, so the axum-style extractor
system, blanket trait impls, and `#[derive(Message)]` become explicit Gleam
constructs: codec records, a kind→handler router, and handlers that take a
context value. Where OTP does a job better than hand-rolled Rust (supervision,
reconnection), we lean on OTP.

Legend: ✅ done · ⚠️ partial / stubbed · ❌ not started

---

## Milestone 0 — Foundations ✅

The vertical slice that already exists.

- [x] Typed `Connection` / `Channel` over the `amqp_client` Erlang FFI
- [x] `connect`, `open_channel`, `close_channel`, `close_connection`
- [x] `declare_queue` (durable), `publish`, `get` (polling one-off)
- [x] Supervised consumer as an OTP actor (`start_consumer` / `stop`)
- [x] `Confirmation` (`Ack` / `Reject` / `Retry` / `DeadLetter`) — settlement dispatch
- [x] `docker-compose.yml` local broker; `test` CI workflow

**Known gaps inherited into the roadmap:** test suite is still the scaffold's
`hello_world`; `Retry` is a naive requeue; `DeadLetter` is identical to `Reject`;
publish can't set headers; there's no router or codec.

---

## Milestone 1 — Test harness + producer options ✅

Stop flying blind, and bring `publish` up to bunnyhop's producer surface.

- [x] Real integration tests against the docker-compose broker
  (publish → consume → assert settlement; queue declare idempotency; `get`; purge)
- [x] CI: stand up RabbitMQ as a service container so `gleam test` exercises the broker
- [x] `PublishOptions` record: `exchange`, `routing_key`, `priority`,
  `expiration`, `mandatory` — mirrors `RabbitMqProduceOptions`
- [x] `publish_with_options` + header support (`List(#(String, String))` → AMQP `FieldTable`)
- [x] Set `content_type` on published messages
- [x] Replace the demo `main` with an example module (`wren/demo`) so the library stays clean
- [x] Bonus: `purge_queue` for deterministic tests

_Parity target:_ `producer.rs`, `producer.rs::RabbitMqProduceOptions`.

---

## Milestone 2 — Typed messages + codec ✅

The foundation for routing. Replaces bunnyhop's `Message`/`FromMessage`/`ToMessage`
derives and `Codec` trait with explicit Gleam values.

- [x] `Codec(a)` abstraction (`wren/codec`): `encode`/`decode` with `CodecError`
- [x] `codec.json` built on `gleam_json` (parity with bunnyhop's `Json`); `codec.string` identity codec
- [x] Kind convention: `kind_header` constant, `with_kind` option sugar, `message_kind` reader
- [x] Typed publish/decode helpers: `publish_encoded`, `decode_message`
- [x] Round-trip tests (pure) + typed publish→consume→decode end-to-end

_Parity target:_ `consumer/codec.rs`, `message.rs`, `fathom_messaging` derives.

_Note:_ payloads stay `String` (UTF-8 JSON) for now; a `BitArray` path can come
later if a binary codec is needed.

---

## Milestone 3 — Router-style consumer ✅

The headline ergonomic feature. bunnyhop's `Router` + `MessageConsumer` builder,
minus the extractor magic.

- [x] `Router` mapping `kind` → typed handler (decode body via codec, return `Confirmation`)
- [x] Fallback handler for unrouted kinds (defaults to `Reject` + warn, like bunnyhop)
- [x] Handler context: `handle_with` hands the decoded value *and* the raw `Message`
  (headers, routing key, payload) — the idiomatic stand-in for `ProcessContext`
- [x] Builder API: `router() |> handle(kind, codec, handler) |> fallback(...) |> start_router(channel, queue)`
- [x] Decode failures are rejected + logged without crashing the consumer
- [x] Wired into the existing supervised actor (same restart/re-subscribe guarantees)

_Parity target:_ `consumer/router.rs`, `consumer/builder.rs`, `consumer/handler.rs`,
`consumer/context.rs`.

---

## Milestone 4 — Topology management ✅

Declare the world properly. bunnyhop's `QueueManager`.

- [x] `declare_exchange` with `ExchangeType` (`Direct`/`Fanout`/`Topic`/`Headers`)
- [x] `bind_queue` / `unbind_queue` (queue ↔ exchange ↔ routing key)
- [x] `delete_queue` / `delete_exchange`
- [x] Queue/exchange **arguments** (`x-*`) via a typed `Arg` (`IntArg`/`StringArg`/`BoolArg`);
  `QueueOptions` / `ExchangeOptions` for durable/exclusive/auto-delete/internal
- [x] `declare_queue` now delegates to `declare_queue_with` (single declare path)

_Parity target:_ `implementations/amqprs/queue_manager.rs`, `config.rs` (Queue/Exchange/Binding).

_Deferred:_ `if_unused` / `if_empty` delete flags (add when a use case needs them).

---

## Milestone 5 — Retry policy + metadata ✅

The brains of retrying. bunnyhop's `retry.rs`. New module `wren/retry` (pure).

- [x] `RetryStrategy`: `ExponentialBackoff(initial, max, multiplier)` and `FixedInterval(d)`
- [x] `RetryPolicy { strategy, max_attempts }`; `calculate_delay(attempt)` (capped); `retry_intervals()`
- [x] `RetryMetadata`: attempt count, first-death/last-retry timestamps, original error/reason, original routing key
- [x] Header (de)serialisation: `x-retry-count`, `x-max-retries`, `x-first-death`,
  `x-last-retry`, `x-original-error`, `x-retry-reason`, `x-original-routing-key`
- [x] `record_failure` + exhaustion detection (`is_exhausted`)

_Parity target:_ `retry.rs`, `consumer/builder.rs` (retry-header plumbing).

_Deferred to M6:_ timestamp generation (needs a clock) happens when wiring into
the live retry flow.

---

## Milestone 6 — Retry infrastructure + real DLX ✅

Where wren finally earns its keep over raw `amqp_client`. Replaces the `Retry`
requeue stub and the `DeadLetter == Reject` stub with genuine topology.

- [x] `RetryInfrastructure` from a `RetryPolicy`: retry exchange, DLX, DLQ,
  per-attempt delay queues with `x-message-ttl`
- [x] Exponential → N delay queues (per-attempt TTL); fixed → single retry queue
- [x] Delay queues dead-letter back to the main queue on TTL expiry
- [x] `setup_retry` builds the whole topology idempotently
- [x] Re-wired consumer `Retry`/`DeadLetter` settlement to republish through the
  retry infrastructure (delay queue / DLQ) and ack — FFI placeholders retired
- [x] Retry headers refreshed + timestamped each hop (`now_timestamp` FFI)
- [x] `start_consumer_with_retry` / `start_router_with_retry`; no-infra `Retry` warns + rejects

_Parity target:_ `retry_infrastructure.rs`, `consumer/auto_retry.rs`,
`consumer/builder.rs::handle_retry`/`handle_dlq`, `config.rs::DeadLetterConfig`.

---

## Milestone 7 — Connection recovery + QoS 🎯 (next)

bunnyhop hand-rolls reconnection with backoff+jitter; we let OTP do the heavy
lifting and add the QoS knobs.

- [ ] `basic.qos` prefetch (`prefetch_count`, `prefetch_size`, global flag)
- [ ] Supervise the **connection**, not just the channel; reconnect + re-open
  channels + re-subscribe on connection loss
- [ ] Backoff strategy for reconnection attempts (supervisor restart intensity / sleep)
- [ ] Optional concurrency: process N deliveries concurrently (`max_concurrent_messages`)
- [ ] Health check + basic connection stats

_Parity target:_ `implementations/amqprs/{connection,recovery,message_bus,consumer}.rs`.

---

## Milestone 8 — Client config + ergonomics ❌

The friendly front door. bunnyhop's `RabbitMqClient` + hierarchical config.

- [ ] Richer `Config`: heartbeat, connection timeout, vhost, TLS toggle
- [ ] `config_from_env` (`RABBITMQ_*`) with validation
- [ ] A `Client` entry point with factory helpers (producer, consumer, queue manager)
- [ ] One-call `with_auto_retry`-style setup wiring infra + producer + consumer

_Parity target:_ `implementations/amqprs/client.rs`, `config.rs`.

---

## Milestone 9 — Docs, examples, 1.0 polish ❌

- [ ] Module docs + `gleam docs` pass
- [ ] Worked examples: simple consumer, router, retry/DLX, recovery
- [ ] README rewrite (away from "🐣 early days")
- [ ] `CHANGELOG.md`
- [ ] Publish to Hex

---

## Explicit departures from bunnyhop

These are deliberate — capability parity without fighting Gleam:

- **No extractors / extensions / DI.** Handlers receive an explicit context
  record. Dependencies are captured in the handler closure instead of injected
  by type.
- **No derive macros.** `kind` + codec are supplied explicitly per message type
  rather than generated from a `#[derive(Message)]`.
- **Recovery via OTP supervision** rather than a hand-rolled `RecoverableChannel`
  / backoff loop, where the supervisor tree expresses the same intent.
- **`amqp_client` (Erlang) FFI** rather than `amqprs` — same protocol, native to the BEAM.
