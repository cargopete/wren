//// wren — an ergonomic RabbitMQ / AMQP client for Gleam.
////
//// Typed connections and channels, plus a supervised, actor-based consumer.
//// Each delivery is dispatched to a handler and settled according to the
//// `Confirmation` the handler returns. The consumer runs as an OTP actor under
//// a supervisor, so a crash means the runtime restarts it and re-subscribes —
//// no hand-rolled reconnection loops.

import gleam/dynamic.{type Dynamic}
import gleam/erlang/process.{type Pid}
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/otp/actor
import gleam/otp/static_supervisor
import gleam/otp/supervision
import gleam/result
import wren/codec.{type Codec}

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

/// The header carrying a message's kind — the discriminator a router dispatches
/// on. Matches the convention used by the Rust `bunnyhop` crate.
pub const kind_header = "kind"

/// Read the `kind` header off a delivered message, if present.
pub fn message_kind(message: Message) -> Result(String, Nil) {
  list.key_find(message.headers, kind_header)
}

/// Anything that can go wrong talking to the broker.
pub type WrenError {
  /// Failed to establish the underlying AMQP connection.
  ConnectionFailed(reason: String)
  /// A channel-level operation (declare, publish, consume, …) failed.
  ChannelFailed(reason: String)
  /// A value could not be serialised before publishing.
  EncodingFailed(reason: String)
  /// A payload could not be deserialised into the expected type.
  DecodingFailed(reason: String)
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

/// Remove all ready messages from a queue, returning nothing.
pub fn purge_queue(channel: Channel, name: String) -> Result(Nil, WrenError) {
  ffi_purge_queue(channel, name)
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

// ===========================================================================
// Publishing with options
// ===========================================================================

/// Options controlling how a message is published. Build with `publish_options`
/// and refine with the `to_*` / `with_*` helpers below.
///
/// Mirrors the producer surface of the Rust `bunnyhop` crate: routing,
/// headers, priority, per-message expiration, and the `mandatory` flag.
pub type PublishOptions {
  PublishOptions(
    /// Exchange to publish to. `""` is the default exchange (route by queue name).
    exchange: String,
    /// Routing key (or queue name when using the default exchange).
    routing_key: String,
    /// Arbitrary string headers, carried as an AMQP `longstr` field table.
    headers: List(#(String, String)),
    /// Message priority (0–255 on a priority queue).
    priority: Option(Int),
    /// Per-message TTL in milliseconds before the broker discards it.
    expiration: Option(Int),
    /// Ask the broker to return the message if it can't be routed to a queue.
    mandatory: Bool,
    /// MIME content type, e.g. `"application/json"`.
    content_type: Option(String),
  )
}

/// A blank set of publish options targeting the default exchange. Refine it
/// with the builder helpers, e.g.
/// `publish_options() |> route("orders") |> with_priority(5)`.
pub fn publish_options() -> PublishOptions {
  PublishOptions(
    exchange: "",
    routing_key: "",
    headers: [],
    priority: None,
    expiration: None,
    mandatory: False,
    content_type: None,
  )
}

/// Set the target exchange.
pub fn to_exchange(
  options: PublishOptions,
  exchange: String,
) -> PublishOptions {
  PublishOptions(..options, exchange:)
}

/// Set the routing key (or queue name, on the default exchange).
pub fn route(options: PublishOptions, routing_key: String) -> PublishOptions {
  PublishOptions(..options, routing_key:)
}

/// Append a single header.
pub fn with_header(
  options: PublishOptions,
  key: String,
  value: String,
) -> PublishOptions {
  PublishOptions(..options, headers: [#(key, value), ..options.headers])
}

/// Replace all headers at once.
pub fn with_headers(
  options: PublishOptions,
  headers: List(#(String, String)),
) -> PublishOptions {
  PublishOptions(..options, headers:)
}

/// Set the message priority.
pub fn with_priority(options: PublishOptions, priority: Int) -> PublishOptions {
  PublishOptions(..options, priority: Some(priority))
}

/// Set a per-message expiration (TTL) in milliseconds.
pub fn with_expiration(options: PublishOptions, millis: Int) -> PublishOptions {
  PublishOptions(..options, expiration: Some(millis))
}

/// Mark the publish as mandatory (broker returns unroutable messages).
pub fn mandatory(options: PublishOptions) -> PublishOptions {
  PublishOptions(..options, mandatory: True)
}

/// Set the MIME content type.
pub fn with_content_type(
  options: PublishOptions,
  content_type: String,
) -> PublishOptions {
  PublishOptions(..options, content_type: Some(content_type))
}

/// Set the message `kind` header — the discriminator a consumer's router uses
/// to pick a handler. Sugar over `with_header(options, kind_header, kind)`.
pub fn with_kind(options: PublishOptions, kind: String) -> PublishOptions {
  with_header(options, kind_header, kind)
}

/// Publish a message with the full set of `PublishOptions`.
pub fn publish_with_options(
  channel: Channel,
  payload: String,
  options: PublishOptions,
) -> Result(Nil, WrenError) {
  ffi_publish_full(
    channel,
    options.exchange,
    options.routing_key,
    payload,
    options.headers,
    options.priority,
    options.expiration,
    options.mandatory,
    options.content_type,
  )
  |> result.map_error(ChannelFailed)
}

/// Encode a typed `value` with `codec` and publish it with the given options.
///
/// Pair with `with_kind` so consumers can route on the message kind:
/// `publish_options() |> route("orders") |> with_kind("order.created")`.
pub fn publish_encoded(
  channel: Channel,
  value: a,
  codec: Codec(a),
  options: PublishOptions,
) -> Result(Nil, WrenError) {
  case codec.encode(value) {
    Ok(payload) -> publish_with_options(channel, payload, options)
    Error(error) -> Error(EncodingFailed(codec_error_reason(error)))
  }
}

/// Decode a delivered message's payload into a typed value with `codec`.
pub fn decode_message(
  message: Message,
  codec: Codec(a),
) -> Result(a, WrenError) {
  codec.decode(message.payload)
  |> result.map_error(fn(error) { DecodingFailed(codec_error_reason(error)) })
}

fn codec_error_reason(error: codec.CodecError) -> String {
  case error {
    codec.EncodeError(reason) -> reason
    codec.DecodeError(reason) -> reason
  }
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

@external(erlang, "wren_ffi", "publish_full")
fn ffi_publish_full(
  channel: Channel,
  exchange: String,
  routing_key: String,
  payload: String,
  headers: List(#(String, String)),
  priority: Option(Int),
  expiration: Option(Int),
  mandatory: Bool,
  content_type: Option(String),
) -> Result(Nil, String)

@external(erlang, "wren_ffi", "purge_queue")
fn ffi_purge_queue(channel: Channel, name: String) -> Result(Nil, String)

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

@external(erlang, "wren_ffi", "close_channel")
fn ffi_close_channel(channel: Channel) -> Nil

@external(erlang, "wren_ffi", "close_connection")
fn ffi_close_connection(connection: Connection) -> Nil
