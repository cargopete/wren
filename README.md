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
- 🚧 Router-style consumer, retry/dead-letter infrastructure, connection recovery

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

// Consume under supervision; decode the payload back into a value.
let handler = fn(message: wren.Message) {
  case wren.decode_message(message, order_codec) {
    Ok(order) -> handle(order)
    Error(_) -> wren.Reject
  }
}
let assert Ok(_) = wren.start_consumer(channel, "orders", handler)
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
