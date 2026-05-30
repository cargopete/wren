//// Integration tests for wren. These talk to a real RabbitMQ broker — bring
//// one up with `docker compose up -d` (or rely on the CI service container).
//// Credentials match the project's `docker-compose.yml` (`wren` / `wren`).

import fixtures.{Order}
import gleam/erlang/process
import gleam/list
import gleam/result
import gleeunit
import wren

pub fn main() -> Nil {
  gleeunit.main()
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn test_config() -> wren.Config {
  wren.Config(..wren.default_config(), username: "wren", password: "wren")
}

/// Open a connection + channel against the test broker, or crash the test.
fn open() -> #(wren.Connection, wren.Channel) {
  let assert Ok(connection) = wren.connect(test_config())
  let assert Ok(channel) = wren.open_channel(connection)
  #(connection, channel)
}

/// Declare `queue` fresh and drained, ready for a test.
fn fresh_queue(channel: wren.Channel, queue: String) -> Nil {
  let assert Ok(_) = wren.declare_queue(channel, queue)
  let assert Ok(_) = wren.purge_queue(channel, queue)
  Nil
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

pub fn connect_and_open_channel_test() {
  let #(connection, channel) = open()
  wren.close_channel(channel)
  wren.close_connection(connection)
}

pub fn declare_queue_is_idempotent_test() {
  let #(connection, channel) = open()
  let assert Ok(_) = wren.declare_queue(channel, "wren_test_idem")
  let assert Ok(_) = wren.declare_queue(channel, "wren_test_idem")
  wren.close_connection(connection)
}

// ---------------------------------------------------------------------------
// Publish / get round trips
// ---------------------------------------------------------------------------

pub fn publish_and_get_round_trip_test() {
  let #(connection, channel) = open()
  fresh_queue(channel, "wren_test_get")

  let assert Ok(_) =
    wren.publish(
      channel,
      exchange: "",
      routing_key: "wren_test_get",
      payload: "hello wren",
    )

  let assert Ok(payload) = wren.get(channel, "wren_test_get")
  assert payload == "hello wren"

  wren.close_connection(connection)
}

pub fn purge_clears_pending_messages_test() {
  let #(connection, channel) = open()
  fresh_queue(channel, "wren_test_purge")

  let assert Ok(_) =
    wren.publish(
      channel,
      exchange: "",
      routing_key: "wren_test_purge",
      payload: "doomed",
    )
  let assert Ok(_) = wren.purge_queue(channel, "wren_test_purge")

  // Nothing should remain to fetch.
  assert result.is_error(wren.get(channel, "wren_test_purge"))

  wren.close_connection(connection)
}

pub fn publish_with_options_uses_default_exchange_test() {
  let #(connection, channel) = open()
  fresh_queue(channel, "wren_test_opts")

  let options =
    wren.publish_options()
    |> wren.route("wren_test_opts")
    |> wren.with_priority(3)
    |> wren.with_content_type("text/plain")

  let assert Ok(_) =
    wren.publish_with_options(channel, "optioned payload", options)

  let assert Ok(payload) = wren.get(channel, "wren_test_opts")
  assert payload == "optioned payload"

  wren.close_connection(connection)
}

// ---------------------------------------------------------------------------
// Consumer
// ---------------------------------------------------------------------------

pub fn supervised_consumer_receives_delivery_test() {
  let #(connection, channel) = open()
  fresh_queue(channel, "wren_test_consume")

  // The handler forwards each delivery back to the test process so we can
  // assert on what actually arrived over the wire.
  let inbox = process.new_subject()
  let handler = fn(message: wren.Message) -> wren.Confirmation {
    process.send(inbox, message)
    wren.Ack
  }

  let assert Ok(consumer) =
    wren.start_consumer(channel, "wren_test_consume", handler)

  let options =
    wren.publish_options()
    |> wren.route("wren_test_consume")
    |> wren.with_header("kind", "greeting")
    |> wren.with_header("trace-id", "abc-123")

  let assert Ok(_) = wren.publish_with_options(channel, "ahoy", options)

  let assert Ok(received) = process.receive(from: inbox, within: 5000)
  assert received.payload == "ahoy"
  assert received.routing_key == "wren_test_consume"
  assert list.key_find(received.headers, "kind") == Ok("greeting")
  assert list.key_find(received.headers, "trace-id") == Ok("abc-123")

  wren.stop(consumer)
  wren.close_connection(connection)
}

pub fn publish_encoded_typed_round_trip_test() {
  let #(connection, channel) = open()
  fresh_queue(channel, "wren_test_typed")

  let inbox = process.new_subject()
  let handler = fn(message: wren.Message) -> wren.Confirmation {
    process.send(inbox, message)
    wren.Ack
  }
  let assert Ok(consumer) =
    wren.start_consumer(channel, "wren_test_typed", handler)

  let order = Order(id: "o-99", qty: 7)
  let options =
    wren.publish_options()
    |> wren.route("wren_test_typed")
    |> wren.with_kind("order.created")
  let assert Ok(_) =
    wren.publish_encoded(channel, order, fixtures.order_codec(), options)

  let assert Ok(received) = process.receive(from: inbox, within: 5000)
  // The kind header drives routing; the codec turns the payload back into a type.
  assert wren.message_kind(received) == Ok("order.created")
  let assert Ok(decoded) = wren.decode_message(received, fixtures.order_codec())
  assert decoded == order

  wren.stop(consumer)
  wren.close_connection(connection)
}
