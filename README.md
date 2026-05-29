# wren

An ergonomic **RabbitMQ / AMQP** messaging library for [Gleam](https://gleam.run),
built on the BEAM and OTP.

> Small, quick, and busy — a wren is a tiny bird that does a great deal. This library
> aims to do the same for your messages: a friendly, type-safe layer over AMQP that
> lets OTP handle the hard parts (supervision, reconnection, fault tolerance) so you
> don't have to.

## Status

🐣 **Early days.** The project is just hatching. Expect the API to change.

## Goals

- A typed, router-style API for consuming messages by kind.
- Producers with sensible defaults and explicit routing.
- Retry and dead-letter handling driven by message headers.
- Connection recovery that leans on OTP supervision rather than hand-rolled loops.

See [`ROADMAP.md`](./ROADMAP.md) for the path to feature parity with a
production AMQP client and the milestones along the way.

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
