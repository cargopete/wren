# Changelog

All notable changes to wren are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/), and the project aims to follow
[Semantic Versioning](https://semver.org/).

## [0.2.2] — 2026-05-30

### Added

- **AMQP message properties** — `Property` (`correlation_id`, `reply_to`,
  `message_id`, `type`, `user_id`, `app_id`, `content_encoding`, `timestamp`) on
  `PublishOptions` with `with_*` builders. `Message` now also exposes received
  `correlation_id` / `reply_to` / `redelivered`, enabling request/reply (RPC).
- **Batch / multi-target publishing** — `Target`, `publish_batch` (with
  per-message `BatchResult` failures), `publish_to_targets`, and
  `publish_batch_with_retry`.

## [0.2.1] — 2026-05-30

### Added

- **Consumer subscribe options** — `ConsumeOptions` (`auto_ack`, `exclusive`,
  `no_local`, `consumer_tag`, subscription `arguments`) via
  `start_consumer_with_options` and `with_consume_options` on recoverable
  consumers. `auto_ack` skips settlement.

## [0.2.0] — 2026-05-30

Closes the remaining capability gaps with the `bunnyhop` crate.

### Added

- **Publisher confirms** — `enable_confirms` puts a channel into confirm mode;
  `publish_confirmed` publishes and waits for the broker's ack (`Confirm`
  verdict), erroring on nack or timeout.
- **Persistent delivery** — `with_persistence` on `PublishOptions` (delivery
  mode 2) so messages survive a broker restart on a durable queue.
- **Concurrent processing** — `start_consumer_concurrent` and `with_concurrency`
  (recoverable) run up to N deliveries at once, each in its own process, bounded
  by the broker's prefetch.
- **Connection pool** — `start_pool` opens N connections; `pool_channel` hands
  out channels round-robin across them; `pool_size` / `pool_stats` / `close_pool`.
- **Active health check** — `health_check` round-trips a throwaway declare to
  confirm a channel is responsive, not merely alive.
- **Kind-based producer** — `KindRouting` maps message kinds to
  `#(exchange, routing key)`; `publish_for_kind` / `publish_encoded_for_kind`
  apply it and stamp the `kind` header.
- **TLS** — a `Tls` setting on `Config` (verify + CA/cert/key paths) wired
  through to the connection's `ssl_options`.
- **Topology refinements** — `delete_queue_with` (`if_unused` / `if_empty`),
  `delete_exchange_with` (`if_unused`), and `bind_queue_with` for binding
  arguments (e.g. `x-match` on headers exchanges).

## [0.1.0] — 2026-05-30

The first cut: a complete, type-safe AMQP messaging core on the BEAM.

### Added

- **Connections & channels** — typed `connect` / `open_channel` over the Erlang
  `amqp_client`, with `close_*` and an `is_open` health check.
- **Config** — `Config` with host, port, credentials, virtual host, heartbeat,
  and connection timeout; `default_config`, `config_from_env` (`RABBITMQ_*`), and
  `config_from_lookup` for any source.
- **Client** — a `start_client` / `client_channel` / `close_client` front door
  bundling a connection and channel.
- **Producers** — `publish` plus `publish_with_options` (exchange, routing key,
  headers, priority, expiration, `mandatory`, content type) and typed
  `publish_encoded`.
- **Codecs** — a `Codec(a)` abstraction with a `json` codec (on `gleam_json`) and
  a `string` codec; `decode_message` and the `kind` header convention.
- **Consumers** — a supervised, actor-based `start_consumer`; a router
  (`router` / `handle` / `handle_with` / `fallback`) dispatching by message kind
  to typed handlers; settlement via `Ack` / `Reject` / `Retry` / `DeadLetter`.
- **Topology** — `declare_exchange` (direct/fanout/topic/headers), `declare_queue`
  / `declare_queue_with`, `bind_queue` / `unbind_queue`, deletes, and typed `x-*`
  arguments.
- **Retry & dead-lettering** — a pure `wren/retry` module (backoff strategies,
  policy, metadata header round-tripping) plus broker-side `RetryInfrastructure`:
  delay queues with TTL that dead-letter back to the main queue, a dead-letter
  exchange, and a DLQ. Wired into `start_consumer_with_retry` /
  `start_router_with_retry`.
- **Recovery & QoS** — `qos` / `qos_with` prefetch, and a self-healing
  `start_recoverable_consumer` / `start_recoverable_router` that owns its
  connection, monitors it, and reconnects with capped exponential backoff, with
  an `on_connect` hook.
- **Examples** — runnable `wren/examples/{router,retry,recovery}` and `wren/demo`.

[0.2.2]: https://github.com/cargopete/wren/releases/tag/v0.2.2
[0.2.1]: https://github.com/cargopete/wren/releases/tag/v0.2.1
[0.2.0]: https://github.com/cargopete/wren/releases/tag/v0.2.0
[0.1.0]: https://github.com/cargopete/wren/releases/tag/v0.1.0
