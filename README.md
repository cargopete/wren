# wren

An ergonomic **RabbitMQ / AMQP** messaging library for [Gleam](https://gleam.run),
built on the BEAM and OTP.

> Small, quick, and busy — a wren is a tiny bird that does a great deal. This library
> aims to do the same for your messages: a friendly, type-safe layer over AMQP that
> lets OTP handle the hard parts (supervision, reconnection, fault tolerance) so you
> don't have to.

## Status

**The core is in place** and exercised by an integration suite against a real
broker. Pre-1.0, so the API may still shift, but the feature set is whole:

- ✅ Typed connections & channels over the Erlang `amqp_client`
- ✅ A supervised, actor-based consumer (OTP restarts and re-subscribes on crash)
- ✅ Producer options — exchange, routing key, headers, priority, expiration, `mandatory`
- ✅ A `Codec` abstraction with a JSON codec, plus typed `publish_encoded` / `decode_message`
- ✅ Router-style consumer — dispatch by message `kind` to typed handlers, with a fallback
- ✅ Topology management — exchanges, bindings, deletes, and `x-*` queue/exchange arguments
- ✅ Retry policy & metadata — backoff strategies, header round-tripping (`wren/retry`)
- ✅ Retry/dead-letter infrastructure — delay queues with TTL, dead-letter exchange, DLQ
- ✅ Connection recovery — self-healing consumer that reconnects with backoff, plus QoS prefetch
- ✅ Client config & ergonomics — `config_from_env`, vhost/heartbeat/timeout, a `Client` front door

And, since v0.2:

- ✅ Publisher confirms (`publish_confirmed`) + persistent delivery (`with_persistence`)
- ✅ Concurrent delivery processing, bounded by prefetch (`start_consumer_concurrent`)
- ✅ Connection pool (`start_pool`) with round-robin channel handout and `pool_stats`
- ✅ Active `health_check`, kind-based producer (`publish_for_kind`), TLS, and topology guards
- ✅ Consumer subscribe options — auto-ack, exclusive, no-local, consumer tag, subscription arguments
- ✅ Full AMQP message properties (`correlation_id`/`reply_to` for RPC, …) and batch / multi-target publishing

wren covers the **entire core and the vast majority** of the production `bunnyhop`
crate's surface. A few bunnyhop features are expressed differently by deliberate,
idiomatic-Gleam choice — no axum-style extractors, dependency injection, or derive
macros; handlers take an explicit `Message` and codecs are values. A handful of
genuinely minor gaps remain (raw-byte payloads, passive declare, a polling
consumer); see the roadmap's "Known remaining gaps" for the honest list.

See [`ROADMAP.md`](./ROADMAP.md) for how each piece maps to a production AMQP
client, and [`CHANGELOG.md`](./CHANGELOG.md) for what landed when.

## Design

- A typed, router-style API for consuming messages by kind.
- Producers with sensible defaults and explicit routing.
- Retry and dead-letter handling driven by message headers and broker topology.
- Connection recovery that leans on OTP supervision rather than hand-rolled loops.

## A quick taste

Publish a typed message with a `kind`, then consume and decode it:

```gleam
import wren
import wren/codec

// A codec is just an encoder + decoder; `codec.json` builds one from gleam_json.
let order_codec = codec.json(encode_order, order_decoder)

// Publish, tagging the message with its kind for routing.
let options =
  wren.publish_options()
  |> wren.route("orders")
  |> wren.with_kind("order.created")
let assert Ok(_) = wren.publish_encoded(channel, order, order_codec, options)

// Consume under supervision, routing by kind to typed handlers.
let router =
  wren.router()
  |> wren.handle("order.created", order_codec, fn(order: Order) {
    handle(order)
    wren.Ack
  })
  |> wren.fallback(fn(_message) { wren.Reject })

let assert Ok(_) = wren.start_router(channel, "orders", router)
```

Handlers receive the already-decoded value; a malformed payload is rejected and
logged before it ever reaches your code. Need the headers or routing key too?
Reach for `handle_with`, which also hands you the raw `Message`.

### Retries & dead-lettering

Give the consumer a retry policy and wren builds the delay-queue + dead-letter
topology for you. A handler returning `wren.Retry` redelivers the message after
a backoff (via a TTL'd delay queue); once attempts are exhausted — or on
`wren.DeadLetter` — it goes to the dead-letter queue.

```gleam
import wren/retry

let infra =
  wren.retry_infrastructure(
    "orders",
    retry.RetryPolicy(
      strategy: retry.ExponentialBackoff(
        initial_ms: 1000,
        max_ms: 60_000,
        multiplier: 2.0,
      ),
      max_attempts: 5,
    ),
  )

// Declares the topology and starts consuming, routing by kind.
let assert Ok(_) = wren.start_router_with_retry(channel, router, infra)
```

## Examples

Runnable, self-contained programs live under [`src/wren/examples`](./src/wren/examples):

```sh
gleam run -m wren/examples/router     # dispatch typed messages by kind
gleam run -m wren/examples/retry      # fail once, then succeed via a delay queue
gleam run -m wren/examples/recovery   # a self-healing consumer
gleam run -m wren/demo                # the original supervised-consumer demo
```

## Development

Everything talks to a real broker. Bring one up first:

```sh
docker compose up -d            # start a local RabbitMQ (wren / wren)
gleam test                      # run the integration + unit suites
gleam run -m wren/examples/router
docker compose down             # stop the broker
```

## Licence

Released under the [MIT](./LICENSE) licence.
