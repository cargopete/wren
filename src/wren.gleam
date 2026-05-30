//// wren — an ergonomic RabbitMQ / AMQP client for Gleam.
////
//// Typed connections and channels, plus a supervised, actor-based consumer.
//// Each delivery is dispatched to a handler and settled according to the
//// `Confirmation` the handler returns. The consumer runs as an OTP actor under
//// a supervisor, so a crash means the runtime restarts it and re-subscribes —
//// no hand-rolled reconnection loops.

import gleam/dict.{type Dict}
import gleam/dynamic.{type Dynamic}
import gleam/erlang/process.{type Pid}
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/otp/actor
import gleam/otp/static_supervisor
import gleam/otp/supervision
import gleam/result
import wren/codec.{type Codec}
import wren/retry.{
  type RetryMetadata, type RetryPolicy, ExponentialBackoff, FixedInterval,
  RetryMetadata,
}

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

/// The broker's verdict when waiting on a publisher confirm.
pub type Confirm {
  /// All messages since the last wait were acknowledged.
  Confirmed
  /// At least one message was negatively acknowledged.
  Nacked
  /// The wait expired before a verdict arrived.
  TimedOut
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

/// The routing behaviour of an exchange.
pub type ExchangeType {
  /// Exact routing-key match.
  Direct
  /// Broadcast to every bound queue.
  Fanout
  /// Wildcard routing-key match (`*` / `#`).
  Topic
  /// Match on message headers rather than routing key.
  Headers
}

/// A typed value for an AMQP argument (the `x-*` settings on queues, exchanges
/// and bindings — e.g. `x-message-ttl`, `x-dead-letter-exchange`).
pub type Arg {
  IntArg(Int)
  StringArg(String)
  BoolArg(Bool)
}

/// Settings for declaring a queue. Build with `queue_options`.
pub type QueueOptions {
  QueueOptions(
    durable: Bool,
    exclusive: Bool,
    auto_delete: Bool,
    arguments: List(#(String, Arg)),
  )
}

/// Durable, non-exclusive, non-auto-delete queue with no extra arguments —
/// the sensible default. Refine the record fields as needed.
pub fn queue_options() -> QueueOptions {
  QueueOptions(
    durable: True,
    exclusive: False,
    auto_delete: False,
    arguments: [],
  )
}

/// Settings for declaring an exchange. Build with `exchange_options`.
pub type ExchangeOptions {
  ExchangeOptions(
    durable: Bool,
    auto_delete: Bool,
    internal: Bool,
    arguments: List(#(String, Arg)),
  )
}

/// Durable, non-auto-delete, non-internal exchange with no extra arguments.
pub fn exchange_options() -> ExchangeOptions {
  ExchangeOptions(
    durable: True,
    auto_delete: False,
    internal: False,
    arguments: [],
  )
}

/// Connection settings. Build via `default_config` and override as needed.
pub type Config {
  Config(
    host: String,
    port: Int,
    username: String,
    password: String,
    /// The AMQP virtual host (`"/"` is the broker default).
    virtual_host: String,
    /// Heartbeat interval in seconds (`0` disables heartbeats).
    heartbeat_seconds: Int,
    /// How long to wait for the TCP connection to establish, in milliseconds.
    connection_timeout_ms: Int,
  )
}

/// Sensible localhost defaults (the classic `guest`/`guest`, vhost `/`).
pub fn default_config() -> Config {
  Config(
    host: "localhost",
    port: 5672,
    username: "guest",
    password: "guest",
    virtual_host: "/",
    heartbeat_seconds: 60,
    connection_timeout_ms: 10_000,
  )
}

/// Build a `Config` from the environment, reading the `RABBITMQ_*` variables.
/// Anything unset (or an unparseable number) falls back to `default_config`.
///
/// Recognised: `RABBITMQ_HOST`, `RABBITMQ_PORT`, `RABBITMQ_USERNAME` (or
/// `RABBITMQ_USER`), `RABBITMQ_PASSWORD` (or `RABBITMQ_PASS`), `RABBITMQ_VHOST`,
/// `RABBITMQ_HEARTBEAT`, `RABBITMQ_CONNECTION_TIMEOUT`.
pub fn config_from_env() -> Config {
  config_from_lookup(ffi_getenv)
}

/// Build a `Config` from an arbitrary lookup function (env, a map, a config
/// file…). Keys are the same `RABBITMQ_*` names as `config_from_env`; missing or
/// invalid values fall back to `default_config`.
pub fn config_from_lookup(lookup: fn(String) -> Result(String, Nil)) -> Config {
  let defaults = default_config()
  Config(
    host: first_of(lookup, ["RABBITMQ_HOST"], defaults.host),
    port: int_or(lookup("RABBITMQ_PORT"), defaults.port),
    username: first_of(
      lookup,
      ["RABBITMQ_USERNAME", "RABBITMQ_USER"],
      defaults.username,
    ),
    password: first_of(
      lookup,
      ["RABBITMQ_PASSWORD", "RABBITMQ_PASS"],
      defaults.password,
    ),
    virtual_host: first_of(lookup, ["RABBITMQ_VHOST"], defaults.virtual_host),
    heartbeat_seconds: int_or(
      lookup("RABBITMQ_HEARTBEAT"),
      defaults.heartbeat_seconds,
    ),
    connection_timeout_ms: int_or(
      lookup("RABBITMQ_CONNECTION_TIMEOUT"),
      defaults.connection_timeout_ms,
    ),
  )
}

fn first_of(
  lookup: fn(String) -> Result(String, Nil),
  keys: List(String),
  default: String,
) -> String {
  case keys {
    [] -> default
    [key, ..rest] ->
      case lookup(key) {
        Ok(value) -> value
        Error(_) -> first_of(lookup, rest, default)
      }
  }
}

fn int_or(value: Result(String, Nil), default: Int) -> Int {
  case value {
    Ok(raw) -> result.unwrap(int.parse(raw), default)
    Error(_) -> default
  }
}

// ===========================================================================
// Connection & channel lifecycle
// ===========================================================================

/// Open a connection to the broker.
pub fn connect(config: Config) -> Result(Connection, WrenError) {
  ffi_connect(
    config.host,
    config.port,
    config.username,
    config.password,
    config.virtual_host,
    config.heartbeat_seconds,
    config.connection_timeout_ms,
  )
  |> result.map_error(ConnectionFailed)
}

/// Open a channel over an existing connection.
pub fn open_channel(connection: Connection) -> Result(Channel, WrenError) {
  ffi_open_channel(connection)
  |> result.map_error(ChannelFailed)
}

/// Is the connection still alive? A cheap health check.
pub fn is_open(connection: Connection) -> Bool {
  ffi_is_connection_open(connection)
}

/// Set channel prefetch: the number of unacknowledged messages the broker will
/// deliver before waiting for acks. Apply this before starting a consumer to
/// bound in-flight work.
pub fn qos(channel: Channel, prefetch_count: Int) -> Result(Nil, WrenError) {
  qos_with(channel, prefetch_count, 0, False)
}

/// Set channel prefetch with full control over `prefetch_size` (octets, `0` for
/// no limit) and whether the setting is `global` (channel-wide vs per-consumer).
pub fn qos_with(
  channel: Channel,
  prefetch_count: Int,
  prefetch_size: Int,
  global: Bool,
) -> Result(Nil, WrenError) {
  ffi_set_qos(channel, prefetch_count, prefetch_size, global)
  |> result.map_error(ChannelFailed)
}

/// Declare a durable queue with default options (idempotent).
pub fn declare_queue(channel: Channel, name: String) -> Result(Nil, WrenError) {
  declare_queue_with(channel, name, queue_options())
}

/// Declare a queue with explicit options, including AMQP `x-*` arguments.
pub fn declare_queue_with(
  channel: Channel,
  name: String,
  options: QueueOptions,
) -> Result(Nil, WrenError) {
  ffi_declare_queue_full(
    channel,
    name,
    options.durable,
    options.exclusive,
    options.auto_delete,
    options.arguments,
  )
  |> result.map_error(ChannelFailed)
}

/// Declare an exchange of the given type (idempotent).
pub fn declare_exchange(
  channel: Channel,
  name: String,
  exchange_type: ExchangeType,
  options: ExchangeOptions,
) -> Result(Nil, WrenError) {
  ffi_declare_exchange(
    channel,
    name,
    exchange_type_name(exchange_type),
    options.durable,
    options.auto_delete,
    options.internal,
    options.arguments,
  )
  |> result.map_error(ChannelFailed)
}

/// Bind a queue to an exchange with a routing key.
pub fn bind_queue(
  channel: Channel,
  queue queue: String,
  exchange exchange: String,
  routing_key routing_key: String,
) -> Result(Nil, WrenError) {
  ffi_bind_queue(channel, queue, exchange, routing_key)
  |> result.map_error(ChannelFailed)
}

/// Remove a binding between a queue and an exchange.
pub fn unbind_queue(
  channel: Channel,
  queue queue: String,
  exchange exchange: String,
  routing_key routing_key: String,
) -> Result(Nil, WrenError) {
  ffi_unbind_queue(channel, queue, exchange, routing_key)
  |> result.map_error(ChannelFailed)
}

/// Delete a queue (and any messages still in it).
pub fn delete_queue(channel: Channel, name: String) -> Result(Nil, WrenError) {
  ffi_delete_queue(channel, name)
  |> result.map_error(ChannelFailed)
}

/// Delete an exchange.
pub fn delete_exchange(
  channel: Channel,
  name: String,
) -> Result(Nil, WrenError) {
  ffi_delete_exchange(channel, name)
  |> result.map_error(ChannelFailed)
}

fn exchange_type_name(exchange_type: ExchangeType) -> String {
  case exchange_type {
    Direct -> "direct"
    Fanout -> "fanout"
    Topic -> "topic"
    Headers -> "headers"
  }
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
    /// Persist the message (delivery mode 2) so it survives a broker restart on
    /// a durable queue.
    persistent: Bool,
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
    persistent: False,
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

/// Mark the message persistent (delivery mode 2) so it survives a broker
/// restart on a durable queue.
pub fn with_persistence(options: PublishOptions) -> PublishOptions {
  PublishOptions(..options, persistent: True)
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
    options.persistent,
  )
  |> result.map_error(ChannelFailed)
}

/// Put a channel into publisher-confirm mode. Call once per channel before
/// using `publish_confirmed`.
pub fn enable_confirms(channel: Channel) -> Result(Nil, WrenError) {
  ffi_enable_confirms(channel)
  |> result.map_error(ChannelFailed)
}

/// Publish and wait (up to `timeout_ms`) for the broker to confirm the message.
/// Requires `enable_confirms` to have been called on the channel. Returns an
/// error if the broker nacks the message or the wait times out.
pub fn publish_confirmed(
  channel: Channel,
  payload: String,
  options: PublishOptions,
  timeout_ms: Int,
) -> Result(Nil, WrenError) {
  use _ <- result.try(publish_with_options(channel, payload, options))
  case ffi_wait_for_confirms(channel, timeout_ms) {
    Ok(Confirmed) -> Ok(Nil)
    Ok(Nacked) -> Error(ChannelFailed("publish was nacked by the broker"))
    Ok(TimedOut) -> Error(ChannelFailed("publish confirmation timed out"))
    Error(reason) -> Error(ChannelFailed(reason))
  }
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
// Client — a friendly front door bundling a connection and a channel
// ===========================================================================

/// A ready-to-use connection paired with an open channel. Saves the
/// connect-then-open-channel dance for the common case; reach for the
/// `client_*` accessors to use the wider API.
pub opaque type Client {
  Client(connection: Connection, channel: Channel, config: Config)
}

/// Connect and open a channel in one step.
pub fn start_client(config: Config) -> Result(Client, WrenError) {
  use connection <- result.try(connect(config))
  use channel <- result.try(open_channel(connection))
  Ok(Client(connection:, channel:, config:))
}

/// The client's open channel — pass it to `publish`, `declare_queue`, etc.
pub fn client_channel(client: Client) -> Channel {
  client.channel
}

/// The client's underlying connection (e.g. for `is_open` or a second channel).
pub fn client_connection(client: Client) -> Connection {
  client.connection
}

/// The config the client was opened with.
pub fn client_config(client: Client) -> Config {
  client.config
}

/// Close the client's channel and connection.
pub fn close_client(client: Client) -> Nil {
  close_channel(client.channel)
  close_connection(client.connection)
}

// ===========================================================================
// Retry infrastructure — delay queues + dead-letter exchange
// ===========================================================================

/// The broker topology that powers retries and dead-lettering for one main
/// queue. Build with `retry_infrastructure`, declare it with `setup_retry`,
/// and hand it to `start_consumer_with_retry` / `start_router_with_retry`.
///
/// Mirrors bunnyhop's `RetryInfrastructure` / `InfrastructureLayout`: when a
/// handler asks to `Retry`, the message is republished into a delay queue (a
/// queue with a TTL and no consumer) that dead-letters back to the main queue
/// when the TTL expires. Exhausted and `DeadLetter` messages go to the DLQ.
pub type RetryInfrastructure {
  RetryInfrastructure(
    main_queue: String,
    retry_exchange: String,
    dlx_exchange: String,
    dlq: String,
    policy: RetryPolicy,
  )
}

const dlq_routing_key = "dlq"

/// Derive the retry topology for `main_queue` from a `RetryPolicy`. Names are
/// derived from the main queue: `<q>.retry`, `<q>.dlx`, `<q>.dlq`.
pub fn retry_infrastructure(
  main_queue: String,
  policy: RetryPolicy,
) -> RetryInfrastructure {
  RetryInfrastructure(
    main_queue:,
    retry_exchange: main_queue <> ".retry",
    dlx_exchange: main_queue <> ".dlx",
    dlq: main_queue <> ".dlq",
    policy:,
  )
}

/// Declare the whole retry topology, idempotently: the retry exchange, the DLX,
/// the main queue, the DLQ (bound to the DLX), and one delay queue per retry
/// slot (each with its TTL and a dead-letter route back to the main queue).
pub fn setup_retry(
  channel: Channel,
  infra: RetryInfrastructure,
) -> Result(Nil, WrenError) {
  use _ <- result.try(declare_exchange(
    channel,
    infra.retry_exchange,
    Direct,
    exchange_options(),
  ))
  use _ <- result.try(declare_exchange(
    channel,
    infra.dlx_exchange,
    Direct,
    exchange_options(),
  ))
  use _ <- result.try(declare_queue(channel, infra.main_queue))
  use _ <- result.try(declare_queue(channel, infra.dlq))
  use _ <- result.try(bind_queue(
    channel,
    queue: infra.dlq,
    exchange: infra.dlx_exchange,
    routing_key: dlq_routing_key,
  ))

  list.try_map(retry_slots(infra), fn(slot) {
    let #(queue, routing_key, ttl) = slot
    let options =
      QueueOptions(..queue_options(), arguments: [
        #("x-message-ttl", IntArg(ttl)),
        #("x-dead-letter-exchange", StringArg("")),
        #("x-dead-letter-routing-key", StringArg(infra.main_queue)),
      ])
    use _ <- result.try(declare_queue_with(channel, queue, options))
    bind_queue(
      channel,
      queue: queue,
      exchange: infra.retry_exchange,
      routing_key: routing_key,
    )
  })
  |> result.replace(Nil)
}

/// The delay queues to declare: `#(queue_name, routing_key, ttl_ms)`. One per
/// attempt for exponential backoff (each with its own TTL), or a single queue
/// for a fixed interval.
fn retry_slots(infra: RetryInfrastructure) -> List(#(String, String, Int)) {
  case infra.policy.strategy {
    FixedInterval(interval_ms) -> [
      #(infra.main_queue <> ".retry", "retry", int.max(interval_ms, 0)),
    ]
    ExponentialBackoff(..) ->
      int_range(1, infra.policy.max_attempts)
      |> list.map(fn(attempt) {
        #(
          infra.main_queue <> ".retry." <> int.to_string(attempt),
          "attempt." <> int.to_string(attempt),
          retry.calculate_delay(infra.policy, attempt),
        )
      })
  }
}

/// The routing key that lands a message in the delay queue for `attempt`.
fn retry_routing_key_for_attempt(
  infra: RetryInfrastructure,
  attempt: Int,
) -> String {
  case infra.policy.strategy {
    FixedInterval(_) -> "retry"
    ExponentialBackoff(..) -> {
      let capped = int.min(int.max(attempt, 1), infra.policy.max_attempts)
      "attempt." <> int.to_string(capped)
    }
  }
}

fn int_range(from: Int, to: Int) -> List(Int) {
  case from > to {
    True -> []
    False -> [from, ..int_range(from + 1, to)]
  }
}

// ===========================================================================
// Consumer — a supervised OTP actor
// ===========================================================================

/// Internal actor state.
type State {
  State(
    channel: Channel,
    handler: fn(Message) -> Confirmation,
    retry: Option(RetryInfrastructure),
    concurrency: Int,
  )
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
  /// A monitored connection died (only seen by recoverable consumers).
  ConnectionDown
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
  start_consumer_internal(channel, queue, handler, None, 1)
}

/// Like `start_consumer`, but processes up to `max_concurrent` deliveries at
/// once. Each delivery runs in its own process; the broker's prefetch is set to
/// `max_concurrent`, so that many messages are in flight (and settled
/// independently) at a time.
pub fn start_consumer_concurrent(
  channel: Channel,
  queue: String,
  handler: fn(Message) -> Confirmation,
  max_concurrent: Int,
) -> Result(Consumer, WrenError) {
  start_consumer_internal(
    channel,
    queue,
    handler,
    None,
    int.max(max_concurrent, 1),
  )
}

/// Start a supervised consumer backed by retry infrastructure. The topology is
/// declared first (via `setup_retry`), then the consumer subscribes to the
/// infrastructure's main queue. Handlers returning `Retry` are redelivered
/// through the delay queues; `DeadLetter` (and exhausted retries) go to the DLQ.
pub fn start_consumer_with_retry(
  channel: Channel,
  handler: fn(Message) -> Confirmation,
  infra: RetryInfrastructure,
) -> Result(Consumer, WrenError) {
  use _ <- result.try(setup_retry(channel, infra))
  start_consumer_internal(channel, infra.main_queue, handler, Some(infra), 1)
}

fn start_consumer_internal(
  channel: Channel,
  queue: String,
  handler: fn(Message) -> Confirmation,
  retry: Option(RetryInfrastructure),
  concurrency: Int,
) -> Result(Consumer, WrenError) {
  start_supervised(consumer_builder(channel, queue, handler, retry, concurrency))
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
  retry: Option(RetryInfrastructure),
  concurrency: Int,
) {
  actor.new_with_initialiser(5000, fn(subject) {
    // init runs in the actor's own process, so `self` is the consumer pid.
    // Bound concurrent processing with the broker's prefetch.
    case concurrency > 1 {
      True -> {
        let _ = qos(channel, concurrency)
        Nil
      }
      False -> Nil
    }
    case ffi_subscribe(channel, queue, process.self()) {
      Ok(_) -> {
        let selector =
          process.new_selector()
          |> process.select_other(decode_event)
        actor.initialised(State(channel:, handler:, retry:, concurrency:))
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
      process_delivery(
        state.channel,
        state.retry,
        state.handler,
        message,
        tag,
        state.concurrency,
      )
      actor.continue(state)
    }
    // Broker cancelled us: stop so the supervisor restarts and re-subscribes.
    Cancelled -> actor.stop()
    // A plain consumer doesn't monitor a connection, so this shouldn't arrive.
    ConnectionDown -> actor.continue(state)
    Ignored -> actor.continue(state)
  }
}

/// Run the handler and settle the delivery. With concurrency, each delivery
/// runs in its own (unlinked) process so handlers proceed in parallel and a
/// crashing handler can't take the consumer down; the broker's prefetch bounds
/// how many are in flight.
fn process_delivery(
  channel: Channel,
  retry: Option(RetryInfrastructure),
  handler: fn(Message) -> Confirmation,
  message: Message,
  tag: Int,
  concurrency: Int,
) -> Nil {
  case concurrency > 1 {
    True -> {
      let _ =
        process.spawn_unlinked(fn() {
          settle(channel, retry, message, tag, handler(message))
        })
      Nil
    }
    False -> settle(channel, retry, message, tag, handler(message))
  }
}

/// Settle a delivery according to the handler's `Confirmation`. `Ack`/`Reject`
/// settle directly with the broker; `Retry`/`DeadLetter` route through the
/// retry infrastructure when one is configured.
fn settle(
  channel: Channel,
  retry_infra: Option(RetryInfrastructure),
  message: Message,
  tag: Int,
  confirmation: Confirmation,
) -> Nil {
  case confirmation {
    Ack -> ffi_settle(channel, tag, Ack)
    Reject -> ffi_settle(channel, tag, Reject)
    Retry -> settle_retry(channel, retry_infra, message, tag)
    DeadLetter -> settle_dead_letter(channel, retry_infra, message, tag)
  }
}

fn settle_retry(
  channel: Channel,
  retry_infra: Option(RetryInfrastructure),
  message: Message,
  tag: Int,
) -> Nil {
  case retry_infra {
    None -> {
      log_warning(
        "wren: Retry requested but no retry infrastructure configured; rejecting",
      )
      ffi_settle(channel, tag, Reject)
    }
    Some(infra) -> {
      let metadata =
        retry.from_headers(message.headers, infra.policy.max_attempts)
        |> retry.record_failure("handler returned Retry")
        |> stamp
      // Exhausted retries fall through to the DLQ; otherwise into a delay queue.
      let #(exchange, routing_key) = case retry.is_exhausted(metadata) {
        True -> #(infra.dlx_exchange, dlq_routing_key)
        False -> #(
          infra.retry_exchange,
          retry_routing_key_for_attempt(infra, metadata.attempt),
        )
      }
      reroute(channel, message, tag, exchange, routing_key, metadata)
    }
  }
}

fn settle_dead_letter(
  channel: Channel,
  retry_infra: Option(RetryInfrastructure),
  message: Message,
  tag: Int,
) -> Nil {
  case retry_infra {
    // No DLQ to route to — reject without requeue (the broker's own DLX, if any).
    None -> ffi_settle(channel, tag, DeadLetter)
    Some(infra) -> {
      let metadata =
        retry.from_headers(message.headers, infra.policy.max_attempts)
        |> retry.record_failure("handler returned DeadLetter")
        |> stamp
      reroute(
        channel,
        message,
        tag,
        infra.dlx_exchange,
        dlq_routing_key,
        metadata,
      )
    }
  }
}

/// Republish `message` (with refreshed retry headers) to `exchange`/`routing_key`,
/// then ack the original. If the republish fails, reject so we don't lose track.
fn reroute(
  channel: Channel,
  message: Message,
  tag: Int,
  exchange: String,
  routing_key: String,
  metadata: RetryMetadata,
) -> Nil {
  let options =
    publish_options()
    |> to_exchange(exchange)
    |> route(routing_key)
    |> with_headers(merge_retry_headers(message.headers, metadata))

  case publish_with_options(channel, message.payload, options) {
    Ok(_) -> ffi_settle(channel, tag, Ack)
    Error(_) -> {
      log_warning("wren: failed to route message to '" <> exchange <> "'")
      ffi_settle(channel, tag, Reject)
    }
  }
}

/// Overlay refreshed retry headers onto the message's existing headers,
/// replacing any stale retry headers of the same name.
fn merge_retry_headers(
  original: List(#(String, String)),
  metadata: RetryMetadata,
) -> List(#(String, String)) {
  let refreshed = retry.to_headers(metadata)
  let refreshed_keys = list.map(refreshed, fn(header) { header.0 })
  let kept =
    list.filter(original, fn(header) {
      !list.contains(refreshed_keys, header.0)
    })
  list.append(kept, refreshed)
}

/// Timestamp a failure: always set `last_retry`; set `first_death` only once.
fn stamp(metadata: RetryMetadata) -> RetryMetadata {
  let now = now_timestamp()
  let first_death = case metadata.first_death {
    Some(_) -> metadata.first_death
    None -> Some(now)
  }
  RetryMetadata(..metadata, last_retry: Some(now), first_death:)
}

// ===========================================================================
// Router — dispatch deliveries by kind to typed handlers
// ===========================================================================

/// Routes deliveries to handlers by their `kind` header. Build with `router`,
/// register typed handlers with `handle` / `handle_with`, set a catch-all with
/// `fallback`, then run it under supervision with `start_router`.
///
/// This is wren's idiomatic take on bunnyhop's `Router` + `MessageConsumer`:
/// each typed handler is erased to `fn(Message) -> Confirmation` by closing over
/// its codec, so handlers for different message types live in one table.
pub opaque type Router {
  Router(
    handlers: Dict(String, fn(Message) -> Confirmation),
    fallback: fn(Message) -> Confirmation,
  )
}

/// A new router whose default fallback rejects unrouted messages with a warning.
pub fn router() -> Router {
  Router(handlers: dict.new(), fallback: warn_and_reject)
}

/// Register a handler for messages of `kind`. The payload is decoded with
/// `codec`; on a decode failure the message is rejected (and a warning logged),
/// so the handler only ever sees well-formed values.
pub fn handle(
  router: Router,
  kind: String,
  codec: Codec(a),
  handler: fn(a) -> Confirmation,
) -> Router {
  handle_with(router, kind, codec, fn(value, _message) { handler(value) })
}

/// Like `handle`, but the handler also receives the raw `Message` — its
/// headers, routing key, and undecoded payload — as context.
pub fn handle_with(
  router: Router,
  kind: String,
  codec: Codec(a),
  handler: fn(a, Message) -> Confirmation,
) -> Router {
  let erased = fn(message: Message) -> Confirmation {
    case codec.decode(message.payload) {
      Ok(value) -> handler(value, message)
      Error(error) -> {
        log_warning(
          "wren: dropping '"
          <> kind
          <> "' — payload failed to decode: "
          <> codec_error_reason(error),
        )
        Reject
      }
    }
  }
  Router(..router, handlers: dict.insert(router.handlers, kind, erased))
}

/// Set the fallback handler invoked for messages whose `kind` has no registered
/// handler (or that carry no `kind` header at all).
pub fn fallback(
  router: Router,
  handler: fn(Message) -> Confirmation,
) -> Router {
  Router(..router, fallback: handler)
}

/// Start a supervised consumer on `queue` that dispatches each delivery through
/// `router`. Same supervision guarantees as `start_consumer`.
pub fn start_router(
  channel: Channel,
  queue: String,
  router: Router,
) -> Result(Consumer, WrenError) {
  start_consumer(channel, queue, fn(message) { dispatch(router, message) })
}

/// Start a router-backed consumer with retry infrastructure: the topology is
/// declared, then deliveries to the main queue are routed by kind. Handlers
/// returning `Retry`/`DeadLetter` flow through the delay queues and DLQ.
pub fn start_router_with_retry(
  channel: Channel,
  router: Router,
  infra: RetryInfrastructure,
) -> Result(Consumer, WrenError) {
  start_consumer_with_retry(
    channel,
    fn(message) { dispatch(router, message) },
    infra,
  )
}

fn dispatch(router: Router, message: Message) -> Confirmation {
  let handler = case message_kind(message) {
    Ok(kind) ->
      dict.get(router.handlers, kind)
      |> result.unwrap(router.fallback)
    Error(_) -> router.fallback
  }
  handler(message)
}

fn warn_and_reject(message: Message) -> Confirmation {
  let kind = result.unwrap(message_kind(message), "<none>")
  log_warning("wren: no handler for kind '" <> kind <> "', rejecting")
  Reject
}

// ===========================================================================
// Recoverable consumer — owns its connection and self-heals
// ===========================================================================

/// Tuning for a recoverable consumer. Build with `recoverable_options` and
/// refine with the `with_*` / `on_connect` helpers.
pub type RecoverableOptions {
  RecoverableOptions(
    prefetch: Option(Int),
    retry: Option(RetryInfrastructure),
    on_connect: fn(Connection) -> Nil,
    initial_backoff_ms: Int,
    max_backoff_ms: Int,
    max_concurrent: Int,
  )
}

/// Default recoverable options: no prefetch, no retry, serial processing, a
/// no-op connect hook, and reconnection backoff from 500ms up to 30s.
pub fn recoverable_options() -> RecoverableOptions {
  RecoverableOptions(
    prefetch: None,
    retry: None,
    on_connect: fn(_connection) { Nil },
    initial_backoff_ms: 500,
    max_backoff_ms: 30_000,
    max_concurrent: 1,
  )
}

/// Process up to `max_concurrent` deliveries at once (each in its own process).
/// Sets the channel prefetch to match, bounding in-flight work.
pub fn with_concurrency(
  options: RecoverableOptions,
  max_concurrent: Int,
) -> RecoverableOptions {
  RecoverableOptions(..options, max_concurrent: int.max(max_concurrent, 1))
}

/// Apply a channel prefetch each time the consumer (re)connects.
pub fn with_prefetch(
  options: RecoverableOptions,
  prefetch_count: Int,
) -> RecoverableOptions {
  RecoverableOptions(..options, prefetch: Some(prefetch_count))
}

/// Back the consumer with retry infrastructure. The consumer subscribes to the
/// infrastructure's main queue, and the topology is (re)declared on each connect.
pub fn with_retry_infrastructure(
  options: RecoverableOptions,
  infra: RetryInfrastructure,
) -> RecoverableOptions {
  RecoverableOptions(..options, retry: Some(infra))
}

/// Register a hook run every time the consumer (re)establishes its connection —
/// handy for re-declaring topology, emitting metrics, or logging.
pub fn on_connect(
  options: RecoverableOptions,
  hook: fn(Connection) -> Nil,
) -> RecoverableOptions {
  RecoverableOptions(..options, on_connect: hook)
}

/// Tune the reconnection backoff bounds (milliseconds).
pub fn with_backoff(
  options: RecoverableOptions,
  initial_ms: Int,
  max_ms: Int,
) -> RecoverableOptions {
  RecoverableOptions(
    ..options,
    initial_backoff_ms: initial_ms,
    max_backoff_ms: max_ms,
  )
}

type RecoverableState {
  RecoverableState(
    config: Config,
    queue: String,
    handler: fn(Message) -> Confirmation,
    options: RecoverableOptions,
    connection: Connection,
    channel: Channel,
  )
}

/// Start a self-healing consumer that owns its own connection. It monitors the
/// connection and, if it drops, reconnects with capped exponential backoff,
/// re-opening the channel and re-subscribing — OTP doing the resilience work
/// instead of a hand-rolled loop.
///
/// When `options` carries retry infrastructure, the consumer subscribes to the
/// infrastructure's main queue (and `queue` is ignored).
pub fn start_recoverable_consumer(
  config: Config,
  queue: String,
  handler: fn(Message) -> Confirmation,
  options: RecoverableOptions,
) -> Result(Consumer, WrenError) {
  start_supervised(recoverable_builder(config, queue, handler, options))
}

/// A recoverable consumer that dispatches deliveries through a `Router`.
pub fn start_recoverable_router(
  config: Config,
  queue: String,
  router: Router,
  options: RecoverableOptions,
) -> Result(Consumer, WrenError) {
  start_recoverable_consumer(
    config,
    queue,
    fn(message) { dispatch(router, message) },
    options,
  )
}

fn recoverable_builder(
  config: Config,
  queue: String,
  handler: fn(Message) -> Confirmation,
  options: RecoverableOptions,
) {
  actor.new_with_initialiser(10_000, fn(subject) {
    case establish(config, queue, options) {
      Ok(#(connection, channel)) -> {
        let selector =
          process.new_selector()
          |> process.select_other(decode_event)
        actor.initialised(RecoverableState(
          config:,
          queue:,
          handler:,
          options:,
          connection:,
          channel:,
        ))
        |> actor.selecting(selector)
        |> actor.returning(subject)
        |> Ok
      }
      Error(error) -> Error(error_reason(error))
    }
  })
  |> actor.on_message(handle_recoverable_event)
}

/// Connect, configure the channel, (re)declare retry topology, subscribe, and
/// monitor the connection so its death wakes the actor. Runs in the actor's own
/// process, so `process.self()` is the consumer pid.
fn establish(
  config: Config,
  queue: String,
  options: RecoverableOptions,
) -> Result(#(Connection, Channel), WrenError) {
  use connection <- result.try(connect(config))
  use channel <- result.try(open_channel(connection))
  // An explicit prefetch wins; otherwise concurrency implies a matching prefetch.
  let prefetch = case options.prefetch {
    Some(count) -> Some(count)
    None ->
      case options.max_concurrent > 1 {
        True -> Some(options.max_concurrent)
        False -> None
      }
  }
  use _ <- result.try(case prefetch {
    Some(count) -> qos(channel, count)
    None -> Ok(Nil)
  })
  use _ <- result.try(case options.retry {
    Some(infra) -> setup_retry(channel, infra)
    None -> Ok(Nil)
  })
  let subscribe_queue = case options.retry {
    Some(infra) -> infra.main_queue
    None -> queue
  }
  use _ <- result.try(
    ffi_subscribe(channel, subscribe_queue, process.self())
    |> result.map_error(ChannelFailed),
  )
  let _ = process.monitor(connection_pid(connection))
  options.on_connect(connection)
  Ok(#(connection, channel))
}

fn handle_recoverable_event(
  state: RecoverableState,
  event: Event,
) -> actor.Next(RecoverableState, Event) {
  case event {
    Delivery(tag, payload, routing_key, headers) -> {
      let message = Message(payload:, routing_key:, headers:)
      process_delivery(
        state.channel,
        state.options.retry,
        state.handler,
        message,
        tag,
        state.options.max_concurrent,
      )
      actor.continue(state)
    }
    // The connection died — reconnect (with backoff) and carry on.
    ConnectionDown -> {
      log_warning("wren: connection lost; reconnecting…")
      let #(connection, channel) = reconnect(state)
      actor.continue(RecoverableState(..state, connection:, channel:))
    }
    // Broker cancelled the subscription — try to re-subscribe, else reconnect.
    Cancelled -> {
      let subscribe_queue = case state.options.retry {
        Some(infra) -> infra.main_queue
        None -> state.queue
      }
      case ffi_subscribe(state.channel, subscribe_queue, process.self()) {
        Ok(_) -> actor.continue(state)
        Error(_) -> {
          let #(connection, channel) = reconnect(state)
          actor.continue(RecoverableState(..state, connection:, channel:))
        }
      }
    }
    Ignored -> actor.continue(state)
  }
}

/// Tear down the old connection (best-effort) and reconnect with backoff.
fn reconnect(state: RecoverableState) -> #(Connection, Channel) {
  close_connection(state.connection)
  reconnect_loop(state, state.options.initial_backoff_ms)
}

fn reconnect_loop(
  state: RecoverableState,
  backoff_ms: Int,
) -> #(Connection, Channel) {
  process.sleep(backoff_ms)
  case establish(state.config, state.queue, state.options) {
    Ok(pair) -> pair
    Error(_) -> {
      let next = int.min(backoff_ms * 2, state.options.max_backoff_ms)
      log_warning(
        "wren: reconnect failed; retrying in " <> int.to_string(next) <> "ms",
      )
      reconnect_loop(state, next)
    }
  }
}

fn start_supervised(
  builder: actor.Builder(state, Event, whatever),
) -> Result(Consumer, WrenError) {
  let child = supervision.worker(fn() { actor.start(builder) })

  static_supervisor.new(static_supervisor.OneForOne)
  |> static_supervisor.add(child)
  |> static_supervisor.start
  |> result.map(fn(started) { Consumer(started.pid) })
  |> result.replace_error(ChannelFailed("failed to start consumer supervisor"))
}

fn error_reason(error: WrenError) -> String {
  case error {
    ConnectionFailed(reason) -> reason
    ChannelFailed(reason) -> reason
    EncodingFailed(reason) -> reason
    DecodingFailed(reason) -> reason
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
  virtual_host: String,
  heartbeat_seconds: Int,
  connection_timeout_ms: Int,
) -> Result(Connection, String)

@external(erlang, "wren_ffi", "getenv")
fn ffi_getenv(name: String) -> Result(String, Nil)

@external(erlang, "wren_ffi", "open_channel")
fn ffi_open_channel(connection: Connection) -> Result(Channel, String)

@external(erlang, "wren_ffi", "declare_queue_full")
fn ffi_declare_queue_full(
  channel: Channel,
  name: String,
  durable: Bool,
  exclusive: Bool,
  auto_delete: Bool,
  arguments: List(#(String, Arg)),
) -> Result(Nil, String)

@external(erlang, "wren_ffi", "declare_exchange")
fn ffi_declare_exchange(
  channel: Channel,
  name: String,
  exchange_type: String,
  durable: Bool,
  auto_delete: Bool,
  internal: Bool,
  arguments: List(#(String, Arg)),
) -> Result(Nil, String)

@external(erlang, "wren_ffi", "bind_queue")
fn ffi_bind_queue(
  channel: Channel,
  queue: String,
  exchange: String,
  routing_key: String,
) -> Result(Nil, String)

@external(erlang, "wren_ffi", "unbind_queue")
fn ffi_unbind_queue(
  channel: Channel,
  queue: String,
  exchange: String,
  routing_key: String,
) -> Result(Nil, String)

@external(erlang, "wren_ffi", "delete_queue")
fn ffi_delete_queue(channel: Channel, name: String) -> Result(Nil, String)

@external(erlang, "wren_ffi", "delete_exchange")
fn ffi_delete_exchange(channel: Channel, name: String) -> Result(Nil, String)

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
  persistent: Bool,
) -> Result(Nil, String)

@external(erlang, "wren_ffi", "enable_confirms")
fn ffi_enable_confirms(channel: Channel) -> Result(Nil, String)

@external(erlang, "wren_ffi", "wait_for_confirms")
fn ffi_wait_for_confirms(
  channel: Channel,
  timeout_ms: Int,
) -> Result(Confirm, String)

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

@external(erlang, "wren_ffi", "set_qos")
fn ffi_set_qos(
  channel: Channel,
  prefetch_count: Int,
  prefetch_size: Int,
  global: Bool,
) -> Result(Nil, String)

@external(erlang, "wren_ffi", "connection_pid")
fn connection_pid(connection: Connection) -> Pid

@external(erlang, "wren_ffi", "is_connection_open")
fn ffi_is_connection_open(connection: Connection) -> Bool

@external(erlang, "wren_ffi", "settle")
fn ffi_settle(channel: Channel, tag: Int, confirmation: Confirmation) -> Nil

@external(erlang, "wren_ffi", "decode_event")
fn decode_event(message: Dynamic) -> Event

@external(erlang, "wren_ffi", "log_warning")
fn log_warning(message: String) -> Nil

@external(erlang, "wren_ffi", "now_timestamp")
fn now_timestamp() -> String

@external(erlang, "wren_ffi", "close_channel")
fn ffi_close_channel(channel: Channel) -> Nil

@external(erlang, "wren_ffi", "close_connection")
fn ffi_close_connection(connection: Connection) -> Nil
