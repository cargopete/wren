//// wren — an ergonomic RabbitMQ / AMQP client for Gleam.
////
//// Typed connections and channels, plus a supervised, actor-based consumer.
//// Each delivery is dispatched to a handler and settled according to the
//// `Confirmation` the handler returns. The consumer runs as an OTP actor under
//// a supervisor, so a crash means the runtime restarts it and re-subscribes —
//// no hand-rolled reconnection loops.

import gleam/dynamic.{type Dynamic}
import gleam/erlang/process.{type Pid}
import gleam/io
import gleam/otp/actor
import gleam/otp/static_supervisor
import gleam/otp/supervision
import gleam/result

// ===========================================================================
// Types
// ===========================================================================

/// An open connection to a RabbitMQ broker. Opaque: created via `connect`.
pub type Connection

/// A channel multiplexed over a `Connection`. Opaque: created via `open_channel`.
pub type Channel

/// A running, supervised subscription. Created via `start_consumer`.
pub opaque type Consumer {
  Consumer(supervisor: Pid)
}

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
/// one-off fetches; prefer `start_consumer` for ongoing work.
pub fn get(channel: Channel, queue: String) -> Result(String, WrenError) {
  ffi_get(channel, queue)
  |> result.map_error(ChannelFailed)
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
// Consumer — a supervised OTP actor
// ===========================================================================

/// Internal actor state.
type State {
  State(channel: Channel, handler: fn(Message) -> Confirmation)
}

/// Internal actor messages, decoded from the raw AMQP mailbox.
type Event {
  Delivery(
    tag: Int,
    payload: String,
    routing_key: String,
    headers: List(#(String, String)),
  )
  Cancelled
  Ignored
}

/// Start a supervised consumer on `queue`. Each delivery is passed to
/// `handler`, then settled with the broker per the returned `Confirmation`.
///
/// The consumer runs under a one-for-one supervisor: if it crashes (or the
/// broker cancels it) the runtime restarts it and re-subscribes.
pub fn start_consumer(
  channel: Channel,
  queue: String,
  handler: fn(Message) -> Confirmation,
) -> Result(Consumer, WrenError) {
  let builder = consumer_builder(channel, queue, handler)
  let child = supervision.worker(fn() { actor.start(builder) })

  static_supervisor.new(static_supervisor.OneForOne)
  |> static_supervisor.add(child)
  |> static_supervisor.start
  |> result.map(fn(started) { Consumer(started.pid) })
  |> result.replace_error(ChannelFailed("failed to start consumer supervisor"))
}

/// Stop a running consumer and its supervisor.
///
/// We unlink first so that tearing down the consumer doesn't send an exit
/// signal back to the caller that started it.
pub fn stop(consumer: Consumer) -> Nil {
  process.unlink(consumer.supervisor)
  process.kill(consumer.supervisor)
}

fn consumer_builder(
  channel: Channel,
  queue: String,
  handler: fn(Message) -> Confirmation,
) {
  actor.new_with_initialiser(5000, fn(subject) {
    // init runs in the actor's own process, so `self` is the consumer pid.
    case ffi_subscribe(channel, queue, process.self()) {
      Ok(_) -> {
        let selector =
          process.new_selector()
          |> process.select_other(decode_event)
        actor.initialised(State(channel:, handler:))
        |> actor.selecting(selector)
        |> actor.returning(subject)
        |> Ok
      }
      Error(reason) -> Error(reason)
    }
  })
  |> actor.on_message(handle_event)
}

fn handle_event(state: State, event: Event) -> actor.Next(State, Event) {
  case event {
    Delivery(tag, payload, routing_key, headers) -> {
      let message = Message(payload:, routing_key:, headers:)
      let confirmation = state.handler(message)
      ffi_settle(state.channel, tag, confirmation)
      actor.continue(state)
    }
    // Broker cancelled us: stop so the supervisor restarts and re-subscribes.
    Cancelled -> actor.stop()
    Ignored -> actor.continue(state)
  }
}

// ===========================================================================
// Demo — a supervised consumer handling real deliveries.
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
    use consumer <- result.try(start_consumer(channel, "wren_demo", handler))

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

    // Give the supervised consumer a moment to process before tearing down.
    sleep(500)
    stop(consumer)
    close_channel(channel)
    close_connection(connection)
    Ok(Nil)
  }

  case outcome {
    Ok(_) -> io.println("✅ supervised consumer demo complete")
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

@external(erlang, "wren_ffi", "subscribe")
fn ffi_subscribe(
  channel: Channel,
  queue: String,
  pid: Pid,
) -> Result(Nil, String)

@external(erlang, "wren_ffi", "settle")
fn ffi_settle(channel: Channel, tag: Int, confirmation: Confirmation) -> Nil

@external(erlang, "wren_ffi", "decode_event")
fn decode_event(message: Dynamic) -> Event

@external(erlang, "wren_ffi", "sleep")
fn sleep(milliseconds: Int) -> Nil

@external(erlang, "wren_ffi", "close_channel")
fn ffi_close_channel(channel: Channel) -> Nil

@external(erlang, "wren_ffi", "close_connection")
fn ffi_close_connection(connection: Connection) -> Nil
