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

## Milestone 7 — Connection recovery + QoS ✅

bunnyhop hand-rolls reconnection with backoff+jitter; we let OTP do the heavy
lifting and add the QoS knobs.

- [x] `basic.qos` prefetch via `qos` / `qos_with` (`prefetch_count`, `prefetch_size`, global)
- [x] Recoverable consumer owns its connection, **monitors** it, and reconnects +
  re-opens channel + re-subscribes on connection loss (`start_recoverable_consumer` / `_router`)
- [x] Capped exponential backoff for reconnection attempts (`with_backoff`)
- [x] `on_connect` hook (re-declare topology / metrics / observability)
- [x] Health check (`is_open`)

_Parity target:_ `implementations/amqprs/{connection,recovery,message_bus,consumer}.rs`.

_Deferred:_ concurrent delivery processing (`max_concurrent_messages`) and richer
connection stats — add when a workload needs them.

---

## Milestone 8 — Client config + ergonomics ✅

The friendly front door. bunnyhop's `RabbitMqClient` + hierarchical config.

- [x] Richer `Config`: `virtual_host`, `heartbeat_seconds`, `connection_timeout_ms`
- [x] `config_from_env` (`RABBITMQ_*`) + `config_from_lookup` (any source), with default fallback
- [x] A `Client` front door (`start_client` / `client_channel` / `close_client`)
- [x] One-call retry wiring already provided by `start_consumer_with_retry` / `start_recoverable_consumer`

_Parity target:_ `implementations/amqprs/client.rs`, `config.rs`.

_Deferred:_ TLS toggle (needs `ssl_options` wiring) — add when a deployment needs it.

---

## Milestone 9 — Docs, examples, 1.0 polish ✅ (bar publish)

- [x] Module docs + clean `gleam docs build`
- [x] Worked examples: `wren/examples/{router,retry,recovery}` (+ `wren/demo`)
- [x] README rewrite (away from "🐣 early days")
- [x] `CHANGELOG.md`
- [x] Release prep: version `0.1.0`, `internal_modules` hides examples from docs
- [ ] `gleam publish` to Hex — awaiting the nod (irreversible)

---

# v0.2 — closing the bunnyhop gaps

v0.1 reaches **capability parity on the core** of bunnyhop. These are the
features bunnyhop has that v0.1 does **not** — genuine gaps, not stylistic
departures. v0.2 closes them.

## Milestone 10 — Publisher confirms + persistence ✅

Without confirms, a publish can vanish silently on a broker hiccup — the biggest
reliability gap for an at-least-once system.

- [x] `enable_confirms` — put a channel into publisher-confirm mode (`confirm.select`)
- [x] `publish_confirmed` — publish and wait for the broker ack (returns a `Confirm` verdict; errors on nack/timeout)
- [x] `with_persistence` — persistent delivery mode (`delivery_mode = 2`) on `PublishOptions`

_Parity target:_ `producer.rs::new_with_confirms`, `config.rs` (ProducerConfig persistence/confirms).

## Milestone 11 — Concurrent delivery processing ✅

- [x] Opt-in concurrent handling — `start_consumer_concurrent` and
  `with_concurrency` on recoverable consumers; each delivery runs in its own process
- [x] Bounded by the broker's prefetch (QoS set to the concurrency level)
- [x] Per-delivery settlement preserved (each worker acks/settles its own tag)

_Parity target:_ `config.rs` (`process_concurrently`, `max_concurrent_messages`).

## Milestone 12 — Connection pooling ✅

One connection per consumer/client scales poorly under many consumers.

- [x] A `Pool` actor owning N connections; `pool_channel` hands out channels round-robin
- [x] `start_pool` / `pool_size` / `close_pool`

_Parity target:_ `implementations/amqprs/connection.rs` (`ConnectionManager`, `PoolConfig`).

_Deferred:_ idle-timeout / max-lifetime channel reaping (needs a reaper) — the
pool holds connections until `close_pool`. Add if a workload needs churn control.

## Milestone 13 — Connection stats + deeper health ✅

- [x] `health_check` that actually exercises the channel (round-trips a throwaway declare), not just liveness
- [x] `pool_stats` — connection count + channels handed out

_Parity target:_ `connection.rs::ConnectionStats`, `client.rs::health_check`.

_Deferred:_ built-in reconnection counter on the recoverable consumer — the
`on_connect` hook already lets callers count reconnects; a built-in stat would
mean exposing the actor's subject.

## Milestone 14 — Kind-based producer ✅

- [x] `KindRouting` — a `kind → (exchange, routing key)` map; `publish_for_kind` /
  `publish_encoded_for_kind` apply it and stamp the `kind` header
- [x] Explicit exchange wins; routing key defaults to the kind (matching bunnyhop)

_Parity target:_ `producer.rs::KindBasedProducer`.

## Milestone 15 — TLS connections ✅

- [x] `Tls` type (`NoTls` / `Tls` with verify + CA/cert/key paths) on `Config`,
  wired to `amqp_params_network` `ssl_options`

_Parity target:_ `Cargo.toml` `tls` feature, `config.rs` connection settings.

_Note:_ the dev broker is plaintext, so the successful-handshake path isn't
integration-tested here; the test confirms TLS is wired by asserting a handshake
against the plaintext port fails (as it must). A TLS broker would round it out.

## Milestone 16 — Topology refinements ✅

- [x] `delete_queue_with` (`if_unused` / `if_empty`); `delete_exchange_with` (`if_unused`)
- [x] `bind_queue_with` — binding arguments (e.g. `x-match` for headers exchanges)

_Parity target:_ `queue_manager.rs` delete options, binding arguments.

## Milestone 17 — Consumer subscribe options ✅

The last cluster of `basic.consume` knobs from bunnyhop's `ConsumerConfig`.

- [x] `ConsumeOptions` — `auto_ack`, `exclusive`, `no_local`, `consumer_tag`, subscription `arguments`
- [x] `start_consumer_with_options`; `with_consume_options` on recoverable consumers
- [x] `auto_ack` skips settlement (the broker already acked on delivery)

_Parity target:_ `config.rs::ConsumerConfig`.

_Note:_ the producer `immediate` flag is intentionally omitted — it was removed
from RabbitMQ years ago; bunnyhop carries a dead field.

## Milestone 18 — AMQP message properties ✅

Found in a deeper audit: bunnyhop's `produce_with_properties` exposes the full
`BasicProperties`; wren only had a subset.

- [x] `Property` (`correlation_id`, `reply_to`, `message_id`, `type`, `user_id`,
  `app_id`, `content_encoding`, `timestamp`) on `PublishOptions` + `with_*` builders
- [x] `Message` now also carries received `correlation_id` / `reply_to` /
  `redelivered`, so request/reply (RPC) works end to end

_Parity target:_ `producer.rs::produce_with_properties`, `IncomingMessage`.

## Milestone 19 — Batch / multi-target producer ✅

- [x] `Target`, `publish_batch` (per-message failure capture via `BatchResult`),
  `publish_to_targets` (one message → many), `publish_batch_with_retry`

_Parity target:_ `producer.rs::{MultiQueueProducer, TargetProducer}`.

---

## Milestone 21 — Raw byte payloads ✅ (the last real gap)

- [x] Payloads are `BitArray` throughout (`Message.payload`, `publish`,
  `publish_with_options`, `get`, batch, codecs) — arbitrary binary now works
- [x] Ergonomic text conveniences: `publish_text`, `message_text`, `codec.string` / `codec.bytes`

_Parity target:_ `producer.rs::produce_raw`.

## Milestone 22 — Completeness: passive declare + validation ✅

- [x] `declare_queue_passive` — check a queue exists without creating it
- [x] `validate_config` — non-empty host, valid port (after the lenient `config_from_env`)

## Known remaining gaps (post-audit)

All niche or intentionally deferred — none is a missing capability.

- **Polling-loop consumer** (`start_consuming_with_polling`) — wren has push consumers + `get`.
- **Pool idle/stale-channel reaping** — deferred (see M12).
- **`next_publish_seqno`** — confirm sequence numbers (niche).
- **`key()` / `make_span`** — a partition-key abstraction and Rust `tracing`
  spans; not RabbitMQ-native / not applicable on the BEAM.

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
