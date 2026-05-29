//// wren — an ergonomic RabbitMQ / AMQP client for Gleam.
////
//// Typed connections and channels, plus a subscription-based consumer that
//// dispatches each delivery to a handler and settles it according to the
//// `Confirmation` the handler returns.
////
//// The consumer currently runs as a plain spawned process; the next step is
//// to place it under an OTP supervisor so reconnection is the runtime's job.

import gleam/io
import gleam/result

// ===========================================================================
// Types
// ===========================================================================

/// An open connection to a RabbitMQ broker. Opaque: created via `connect`.
pub type Connection

/// A channel multiplexed over a `Connection`. Opaque: created via `open_channel`.
pub type Channel

/// A running subscription. Opaque: created via `consume`, ended via `stop`.
pub type Consumer

/// A delivered message handed to a consumer's handler.
pub type Message {
  Message(
    payload: String,
    routing_key: String,
    headers: List(#(String, String)),
  )
}

/// Anything that can go wrong talking to the broker.
pub type WrenError {
  /// Failed to establish the underlying AMQP connection.
  ConnectionFailed(reason: String)
  /// A channel-level operation (declare, publish, consume, …) failed.
  ChannelFailed(reason: String)
}

/// How a consumer wishes a delivered message to be settled with the broker.
pub type Confirmation {
  /// Processed successfully — remove from the queue.
  Ack
  /// Permanent failure — discard without redelivery or dead-lettering.
  Reject
  /// Transient failure — redeliver for another attempt.
  Retry
  /// Unprocessable — route to the dead-letter exchange, if configured.
  DeadLetter
}

/// Connection settings. Build via `default_config` and override as needed.
pub type Config {
  Config(host: String, port: Int, username: String, password: String)
}

/// Sensible localhost defaults (the classic `guest`/`guest`).
pub fn default_config() -> Config {
  Config(host: "localhost", port: 5672, username: "guest", password: "guest")
}

// ===========================================================================
// Connection & channel lifecycle
// ===========================================================================

/// Open a connection to the broker.
pub fn connect(config: Config) -> Result(Connection, WrenError) {
  ffi_connect(config.host, config.port, config.username, config.password)
  |> result.map_error(ConnectionFailed)
}

/// Open a channel over an existing connection.
pub fn open_channel(connection: Connection) -> Result(Channel, WrenError) {
  ffi_open_channel(connection)
  |> result.map_error(ChannelFailed)
}

/// Declare a durable queue (idempotent).
pub fn declare_queue(channel: Channel, name: String) -> Result(Nil, WrenError) {
  ffi_declare_queue(channel, name)
  |> result.map_error(ChannelFailed)
}

/// Publish a message to an exchange with a routing key.
/// Use `""` as the exchange to publish straight to a queue by name.
pub fn publish(
  channel: Channel,
  exchange exchange: String,
  routing_key routing_key: String,
  payload payload: String,
) -> Result(Nil, WrenError) {
  ffi_publish(channel, exchange, routing_key, payload)
  |> result.map_error(ChannelFailed)
}

/// Fetch a single message from a queue (polls briefly). A primitive for
/// one-off fetches; prefer `consume` for ongoing work.
pub fn get(channel: Channel, queue: String) -> Result(String, WrenError) {
  ffi_get(channel, queue)
  |> result.map_error(ChannelFailed)
}

// ===========================================================================
// Consuming
// ===========================================================================

/// Subscribe to a queue. Each delivery is passed to `handler`, and settled
/// with the broker according to the `Confirmation` it returns.
pub fn consume(
  channel: Channel,
  queue: String,
  handler: fn(Message) -> Confirmation,
) -> Result(Consumer, WrenError) {
  ffi_consume(channel, queue, handler)
  |> result.map_error(ChannelFailed)
}

/// Stop a running consumer.
pub fn stop(consumer: Consumer) -> Nil {
  ffi_stop(consumer)
}

/// Close a channel. Safe to call even if already closed.
pub fn close_channel(channel: Channel) -> Nil {
  ffi_close_channel(channel)
}

/// Close a connection (and implicitly its channels).
pub fn close_connection(connection: Connection) -> Nil {
  ffi_close_connection(connection)
}

// ===========================================================================
// Demo — a live subscription that handles real deliveries.
// ===========================================================================

pub fn main() -> Nil {
  let config = Config(..default_config(), username: "wren", password: "wren")
  let outcome = {
    use connection <- result.try(connect(config))
    use channel <- result.try(open_channel(connection))
    use _ <- result.try(declare_queue(channel, "wren_demo"))

    let handler = fn(message: Message) -> Confirmation {
      io.println(
        "📨 received on '" <> message.routing_key <> "': " <> message.payload,
      )
      Ack
    }
    use consumer <- result.try(consume(channel, "wren_demo", handler))

    use _ <- result.try(publish(
      channel,
      exchange: "",
      routing_key: "wren_demo",
      payload: "first message",
    ))
    use _ <- result.try(publish(
      channel,
      exchange: "",
      routing_key: "wren_demo",
      payload: "second message",
    ))

    // Give the consumer a moment to process before we tear down.
    sleep(500)
    stop(consumer)
    close_channel(channel)
    close_connection(connection)
    Ok(Nil)
  }

  case outcome {
    Ok(_) -> io.println("✅ consumer demo complete")
    Error(error) -> io.println("❌ " <> describe(error))
  }
}

fn describe(error: WrenError) -> String {
  case error {
    ConnectionFailed(reason) -> "connection failed: " <> reason
    ChannelFailed(reason) -> "channel failed: " <> reason
  }
}

// ===========================================================================
// FFI bindings into src/wren_ffi.erl (Erlang `amqp_client`).
// ===========================================================================

@external(erlang, "wren_ffi", "connect")
fn ffi_connect(
  host: String,
  port: Int,
  username: String,
  password: String,
) -> Result(Connection, String)

@external(erlang, "wren_ffi", "open_channel")
fn ffi_open_channel(connection: Connection) -> Result(Channel, String)

@external(erlang, "wren_ffi", "declare_queue")
fn ffi_declare_queue(channel: Channel, name: String) -> Result(Nil, String)

@external(erlang, "wren_ffi", "publish")
fn ffi_publish(
  channel: Channel,
  exchange: String,
  routing_key: String,
  payload: String,
) -> Result(Nil, String)

@external(erlang, "wren_ffi", "get")
fn ffi_get(channel: Channel, queue: String) -> Result(String, String)

@external(erlang, "wren_ffi", "consume")
fn ffi_consume(
  channel: Channel,
  queue: String,
  handler: fn(Message) -> Confirmation,
) -> Result(Consumer, String)

@external(erlang, "wren_ffi", "stop")
fn ffi_stop(consumer: Consumer) -> Nil

@external(erlang, "wren_ffi", "sleep")
fn sleep(milliseconds: Int) -> Nil

@external(erlang, "wren_ffi", "close_channel")
fn ffi_close_channel(channel: Channel) -> Nil

@external(erlang, "wren_ffi", "close_connection")
fn ffi_close_connection(connection: Connection) -> Nil
