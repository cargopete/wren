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

## Development

```sh
gleam run   # run the project
gleam test  # run the tests
```

## Licence

Released under the [MIT](./LICENSE) licence.
