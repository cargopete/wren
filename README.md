# wren

An ergonomic **RabbitMQ / AMQP** messaging library for [Gleam](https://gleam.run),
built on the BEAM and OTP.

> Small, quick, and busy — a wren is a tiny bird that does a great deal. This library
> aims to do the same for your messages: a friendly, type-safe layer over AMQP that
> lets OTP handle the hard parts (supervision, reconnection, fault tolerance) so you
> don't have to.

## Status

🐣 **Early days, but airborne.** The API will still change. So far:

- ✅ Typed connections & channels over the Erlang `amqp_client`
- ✅ A supervised, actor-based consumer (OTP restarts and re-subscribes on crash)
- ✅ Producer options — exchange, routing key, headers, priority, expiration, `mandatory`
- ✅ A `Codec` abstraction with a JSON codec, plus typed `publish_encoded` / `decode_message`
- ✅ Router-style consumer — dispatch by message `kind` to typed handlers, with a fallback
- ✅ Topology management — exchanges, bindings, deletes, and `x-*` queue/exchange arguments
- ✅ Retry policy & metadata — backoff strategies, header round-tripping (`wren/retry`)
- ✅ Retry/dead-letter infrastructure — delay queues with TTL, dead-letter exchange, DLQ
- 🚧 Connection recovery, client config & ergonomics

See [`ROADMAP.md`](./ROADMAP.md) for the path to feature parity with a
production AMQP client and the milestones along the way.

## Goals

- A typed, router-style API for consuming messages by kind.
- Producers with sensible defaults and explicit routing.
- Retry and dead-letter handling driven by message headers.
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

## Development

The tests and demo talk to a real broker. Bring one up first:

```sh
docker compose up -d            # start a local RabbitMQ (wren / wren)
gleam test                      # run the integration tests
gleam run -m wren/demo          # run the supervised-consumer demo
docker compose down             # stop the broker
```

## Licence

Released under the [MIT](./LICENSE) licence.
