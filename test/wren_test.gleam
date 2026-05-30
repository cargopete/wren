//// Integration tests for wren. These talk to a real RabbitMQ broker — bring
//// one up with `docker compose up -d` (or rely on the CI service container).
//// Credentials match the project's `docker-compose.yml` (`wren` / `wren`).

import fixtures.{type Order, type Shipment, Order, Shipment}
import gleam/erlang/process
import gleam/list
import gleam/option
import gleam/result
import gleeunit
import wren
import wren/retry

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
    wren.publish(channel, exchange: "", routing_key: "wren_test_get", payload: <<
      "hello wren",
    >>)

  let assert Ok(payload) = wren.get(channel, "wren_test_get")
  assert payload == <<"hello wren">>

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
      payload: <<"doomed">>,
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
    wren.publish_with_options(channel, <<"optioned payload">>, options)

  let assert Ok(payload) = wren.get(channel, "wren_test_opts")
  assert payload == <<"optioned payload">>

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

  let assert Ok(_) = wren.publish_with_options(channel, <<"ahoy">>, options)

  let assert Ok(received) = process.receive(from: inbox, within: 5000)
  assert received.payload == <<"ahoy">>
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

// ---------------------------------------------------------------------------
// Topology — exchanges, bindings, arguments, deletes
// ---------------------------------------------------------------------------

pub fn exchange_binding_routes_to_queue_test() {
  let #(connection, channel) = open()
  let assert Ok(_) =
    wren.declare_exchange(
      channel,
      "wren_test_ex",
      wren.Topic,
      wren.exchange_options(),
    )
  fresh_queue(channel, "wren_test_bound")
  let assert Ok(_) =
    wren.bind_queue(
      channel,
      queue: "wren_test_bound",
      exchange: "wren_test_ex",
      routing_key: "orders.*",
    )

  // `orders.created` matches the `orders.*` binding.
  let assert Ok(_) =
    wren.publish(
      channel,
      exchange: "wren_test_ex",
      routing_key: "orders.created",
      payload: <<"via topic">>,
    )
  let assert Ok(payload) = wren.get(channel, "wren_test_bound")
  assert payload == <<"via topic">>

  let assert Ok(_) = wren.delete_exchange(channel, "wren_test_ex")
  wren.close_connection(connection)
}

pub fn unbind_stops_routing_test() {
  let #(connection, channel) = open()
  let assert Ok(_) =
    wren.declare_exchange(
      channel,
      "wren_test_unbind_ex",
      wren.Direct,
      wren.exchange_options(),
    )
  fresh_queue(channel, "wren_test_unbind_q")
  let assert Ok(_) =
    wren.bind_queue(
      channel,
      queue: "wren_test_unbind_q",
      exchange: "wren_test_unbind_ex",
      routing_key: "k",
    )
  let assert Ok(_) =
    wren.unbind_queue(
      channel,
      queue: "wren_test_unbind_q",
      exchange: "wren_test_unbind_ex",
      routing_key: "k",
    )

  // With the binding gone, the message has nowhere to land.
  let assert Ok(_) =
    wren.publish(
      channel,
      exchange: "wren_test_unbind_ex",
      routing_key: "k",
      payload: <<"orphan">>,
    )
  assert result.is_error(wren.get(channel, "wren_test_unbind_q"))

  let assert Ok(_) = wren.delete_exchange(channel, "wren_test_unbind_ex")
  wren.close_connection(connection)
}

pub fn queue_message_ttl_argument_takes_effect_test() {
  let #(connection, channel) = open()
  // An x-message-ttl of 100ms means messages self-destruct if not consumed.
  let options =
    wren.QueueOptions(..wren.queue_options(), arguments: [
      #("x-message-ttl", wren.IntArg(100)),
    ])
  let assert Ok(_) = wren.declare_queue_with(channel, "wren_test_ttl", options)
  let assert Ok(_) = wren.purge_queue(channel, "wren_test_ttl")

  let assert Ok(_) =
    wren.publish(channel, exchange: "", routing_key: "wren_test_ttl", payload: <<
      "ephemeral",
    >>)
  // Outlive the TTL, then confirm the broker discarded it.
  process.sleep(400)
  assert result.is_error(wren.get(channel, "wren_test_ttl"))

  wren.close_connection(connection)
}

pub fn delete_queue_and_exchange_test() {
  let #(connection, channel) = open()

  let assert Ok(_) = wren.declare_queue(channel, "wren_test_del_q")
  let assert Ok(_) = wren.delete_queue(channel, "wren_test_del_q")
  // Re-declaring cleanly proves the delete landed.
  let assert Ok(_) = wren.declare_queue(channel, "wren_test_del_q")
  let assert Ok(_) = wren.delete_queue(channel, "wren_test_del_q")

  let assert Ok(_) =
    wren.declare_exchange(
      channel,
      "wren_test_del_ex",
      wren.Fanout,
      wren.exchange_options(),
    )
  let assert Ok(_) = wren.delete_exchange(channel, "wren_test_del_ex")

  wren.close_connection(connection)
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

pub fn router_dispatches_by_kind_test() {
  let #(connection, channel) = open()
  fresh_queue(channel, "wren_test_router")

  let orders = process.new_subject()
  let shipments = process.new_subject()
  let unrouted = process.new_subject()

  let router =
    wren.router()
    |> wren.handle("order.created", fixtures.order_codec(), fn(order: Order) {
      process.send(orders, order)
      wren.Ack
    })
    |> wren.handle(
      "shipment.dispatched",
      fixtures.shipment_codec(),
      fn(shipment: Shipment) {
        process.send(shipments, shipment)
        wren.Ack
      },
    )
    |> wren.fallback(fn(message: wren.Message) {
      process.send(unrouted, message)
      wren.Reject
    })

  let assert Ok(consumer) =
    wren.start_router(channel, "wren_test_router", router)

  let order = Order(id: "o-1", qty: 2)
  let shipment = Shipment(tracking: "TRK-1")
  let assert Ok(_) =
    wren.publish_encoded(
      channel,
      order,
      fixtures.order_codec(),
      wren.publish_options()
        |> wren.route("wren_test_router")
        |> wren.with_kind("order.created"),
    )
  let assert Ok(_) =
    wren.publish_encoded(
      channel,
      shipment,
      fixtures.shipment_codec(),
      wren.publish_options()
        |> wren.route("wren_test_router")
        |> wren.with_kind("shipment.dispatched"),
    )
  let assert Ok(_) =
    wren.publish_with_options(
      channel,
      <<"{}">>,
      wren.publish_options()
        |> wren.route("wren_test_router")
        |> wren.with_kind("mystery.kind"),
    )

  // Each kind reaches its own typed handler; the stranger hits the fallback.
  let assert Ok(received_order) = process.receive(from: orders, within: 5000)
  assert received_order == order
  let assert Ok(received_shipment) =
    process.receive(from: shipments, within: 5000)
  assert received_shipment == shipment
  let assert Ok(stranger) = process.receive(from: unrouted, within: 5000)
  assert wren.message_kind(stranger) == Ok("mystery.kind")

  wren.stop(consumer)
  wren.close_connection(connection)
}

pub fn router_rejects_undecodable_without_crashing_test() {
  let #(connection, channel) = open()
  fresh_queue(channel, "wren_test_router_bad")

  let orders = process.new_subject()
  let router =
    wren.router()
    |> wren.handle("order.created", fixtures.order_codec(), fn(order: Order) {
      process.send(orders, order)
      wren.Ack
    })

  let assert Ok(consumer) =
    wren.start_router(channel, "wren_test_router_bad", router)

  // A malformed payload for a known kind must be rejected, not crash the
  // consumer — so the *next*, valid message still gets through.
  let bad =
    wren.publish_options()
    |> wren.route("wren_test_router_bad")
    |> wren.with_kind("order.created")
  let assert Ok(_) = wren.publish_with_options(channel, <<"not json">>, bad)

  let good = Order(id: "o-2", qty: 9)
  let assert Ok(_) =
    wren.publish_encoded(channel, good, fixtures.order_codec(), bad)

  // The handler only ever sees the well-formed value.
  let assert Ok(received) = process.receive(from: orders, within: 5000)
  assert received == good

  wren.stop(consumer)
  wren.close_connection(connection)
}

pub fn router_handle_with_exposes_context_test() {
  let #(connection, channel) = open()
  fresh_queue(channel, "wren_test_router_ctx")

  let inbox = process.new_subject()
  let router =
    wren.router()
    |> wren.handle_with(
      "order.created",
      fixtures.order_codec(),
      fn(order: Order, message: wren.Message) {
        // Forward the decoded value alongside its delivery context.
        process.send(inbox, #(order, message.routing_key, message.headers))
        wren.Ack
      },
    )

  let assert Ok(consumer) =
    wren.start_router(channel, "wren_test_router_ctx", router)

  let order = Order(id: "o-3", qty: 1)
  let assert Ok(_) =
    wren.publish_encoded(
      channel,
      order,
      fixtures.order_codec(),
      wren.publish_options()
        |> wren.route("wren_test_router_ctx")
        |> wren.with_kind("order.created")
        |> wren.with_header("trace-id", "ctx-42"),
    )

  let assert Ok(#(decoded, routing_key, headers)) =
    process.receive(from: inbox, within: 5000)
  assert decoded == order
  assert routing_key == "wren_test_router_ctx"
  assert list.key_find(headers, "trace-id") == Ok("ctx-42")

  wren.stop(consumer)
  wren.close_connection(connection)
}

// ---------------------------------------------------------------------------
// Retry infrastructure + dead-letter
// ---------------------------------------------------------------------------

fn fixed_infra(
  name: String,
  interval: Int,
  max: Int,
) -> wren.RetryInfrastructure {
  wren.retry_infrastructure(
    name,
    retry.RetryPolicy(
      strategy: retry.FixedInterval(interval),
      max_attempts: max,
    ),
  )
}

/// Tear down a fixed-interval retry topology so tests don't leave litter behind.
fn teardown_fixed(
  channel: wren.Channel,
  infra: wren.RetryInfrastructure,
) -> Nil {
  // The main queue is purged, not deleted: the consumer we just stopped still
  // has a subscription on the shared channel, and deleting its queue would have
  // the broker fire a `basic.cancel` at the dead subscriber.
  let _ = wren.purge_queue(channel, infra.main_queue)
  let _ = wren.delete_queue(channel, infra.dlq)
  let _ = wren.delete_queue(channel, infra.main_queue <> ".retry")
  let _ = wren.delete_exchange(channel, infra.retry_exchange)
  let _ = wren.delete_exchange(channel, infra.dlx_exchange)
  Nil
}

pub fn retry_round_trips_through_delay_queue_test() {
  let #(connection, channel) = open()
  let infra = fixed_infra("wren_test_retry", 300, 5)
  let assert Ok(_) = wren.setup_retry(channel, infra)
  let assert Ok(_) = wren.purge_queue(channel, infra.main_queue)
  let assert Ok(_) = wren.purge_queue(channel, infra.main_queue <> ".retry")

  let inbox = process.new_subject()
  // Fail the first time (no retry header yet), succeed once redelivered.
  let handler = fn(message: wren.Message) -> wren.Confirmation {
    process.send(inbox, message)
    case retry.from_headers(message.headers, 5).attempt {
      0 -> wren.Retry
      _ -> wren.Ack
    }
  }
  let assert Ok(consumer) =
    wren.start_consumer_with_retry(channel, handler, infra)

  let assert Ok(_) =
    wren.publish(channel, exchange: "", routing_key: infra.main_queue, payload: <<
      "retry me",
    >>)

  // First delivery: no retry count yet.
  let assert Ok(first) = process.receive(from: inbox, within: 5000)
  assert list.key_find(first.headers, "x-retry-count") == Error(Nil)
  // Second delivery arrives after the TTL, now stamped as attempt 1.
  let assert Ok(second) = process.receive(from: inbox, within: 8000)
  assert list.key_find(second.headers, "x-retry-count") == Ok("1")
  assert second.payload == <<"retry me">>

  wren.stop(consumer)
  teardown_fixed(channel, infra)
  wren.close_connection(connection)
}

pub fn exhausted_retry_routes_to_dlq_test() {
  let #(connection, channel) = open()
  // max_attempts 1: the very first Retry is already exhausted -> DLQ.
  let infra = fixed_infra("wren_test_exhaust", 200, 1)
  let assert Ok(_) = wren.setup_retry(channel, infra)
  let assert Ok(_) = wren.purge_queue(channel, infra.main_queue)
  let assert Ok(_) = wren.purge_queue(channel, infra.dlq)

  let handler = fn(_message: wren.Message) -> wren.Confirmation { wren.Retry }
  let assert Ok(consumer) =
    wren.start_consumer_with_retry(channel, handler, infra)

  let assert Ok(_) =
    wren.publish(channel, exchange: "", routing_key: infra.main_queue, payload: <<
      "doomed",
    >>)

  // It should land in the DLQ rather than bounce around forever.
  let assert Ok(payload) = wren.get(channel, infra.dlq)
  assert payload == <<"doomed">>

  wren.stop(consumer)
  teardown_fixed(channel, infra)
  wren.close_connection(connection)
}

pub fn dead_letter_confirmation_routes_to_dlq_test() {
  let #(connection, channel) = open()
  let infra = fixed_infra("wren_test_dl", 200, 5)
  let assert Ok(_) = wren.setup_retry(channel, infra)
  let assert Ok(_) = wren.purge_queue(channel, infra.main_queue)
  let assert Ok(_) = wren.purge_queue(channel, infra.dlq)

  let handler = fn(_message: wren.Message) -> wren.Confirmation {
    wren.DeadLetter
  }
  let assert Ok(consumer) =
    wren.start_consumer_with_retry(channel, handler, infra)

  let assert Ok(_) =
    wren.publish(channel, exchange: "", routing_key: infra.main_queue, payload: <<
      "straight to jail",
    >>)

  let assert Ok(payload) = wren.get(channel, infra.dlq)
  assert payload == <<"straight to jail">>

  wren.stop(consumer)
  teardown_fixed(channel, infra)
  wren.close_connection(connection)
}

pub fn setup_retry_is_idempotent_for_exponential_test() {
  let #(connection, channel) = open()
  let infra =
    wren.retry_infrastructure(
      "wren_test_expo",
      retry.RetryPolicy(
        strategy: retry.ExponentialBackoff(
          initial_ms: 100,
          max_ms: 1000,
          multiplier: 2.0,
        ),
        max_attempts: 3,
      ),
    )

  // Declaring the exponential topology twice must be a no-op the second time.
  let assert Ok(_) = wren.setup_retry(channel, infra)
  let assert Ok(_) = wren.setup_retry(channel, infra)

  let _ = wren.delete_queue(channel, infra.main_queue)
  let _ = wren.delete_queue(channel, infra.dlq)
  let _ = wren.delete_queue(channel, "wren_test_expo.retry.1")
  let _ = wren.delete_queue(channel, "wren_test_expo.retry.2")
  let _ = wren.delete_queue(channel, "wren_test_expo.retry.3")
  let _ = wren.delete_exchange(channel, infra.retry_exchange)
  let _ = wren.delete_exchange(channel, infra.dlx_exchange)
  wren.close_connection(connection)
}

// ---------------------------------------------------------------------------
// QoS, health, and connection recovery
// ---------------------------------------------------------------------------

pub fn qos_sets_prefetch_test() {
  let #(connection, channel) = open()
  let assert Ok(_) = wren.qos(channel, 10)
  let assert Ok(_) = wren.qos_with(channel, 5, 0, True)
  wren.close_connection(connection)
}

pub fn is_open_reflects_connection_state_test() {
  let assert Ok(connection) = wren.connect(test_config())
  assert wren.is_open(connection) == True

  wren.close_connection(connection)
  // Closing is asynchronous; give the connection process a moment to exit.
  process.sleep(300)
  assert wren.is_open(connection) == False
}

pub fn recoverable_consumer_receives_delivery_test() {
  let #(connection, channel) = open()
  fresh_queue(channel, "wren_test_recover")

  let inbox = process.new_subject()
  let handler = fn(message: wren.Message) -> wren.Confirmation {
    process.send(inbox, message)
    wren.Ack
  }
  let assert Ok(consumer) =
    wren.start_recoverable_consumer(
      test_config(),
      "wren_test_recover",
      handler,
      wren.recoverable_options(),
    )

  let assert Ok(_) =
    wren.publish(
      channel,
      exchange: "",
      routing_key: "wren_test_recover",
      payload: <<"alive">>,
    )
  let assert Ok(received) = process.receive(from: inbox, within: 5000)
  assert received.payload == <<"alive">>

  wren.stop(consumer)
  wren.close_connection(connection)
}

pub fn recoverable_consumer_heals_after_connection_drop_test() {
  let #(connection, channel) = open()
  fresh_queue(channel, "wren_test_recover_drop")

  // `on_connect` reports every (re)connection, so the test can drop the first
  // one and watch the consumer re-establish a second.
  let connects = process.new_subject()
  let inbox = process.new_subject()
  let options =
    wren.recoverable_options()
    |> wren.on_connect(fn(conn) { process.send(connects, conn) })
    |> wren.with_backoff(200, 2000)
  let handler = fn(message: wren.Message) -> wren.Confirmation {
    process.send(inbox, message)
    wren.Ack
  }
  let assert Ok(consumer) =
    wren.start_recoverable_consumer(
      test_config(),
      "wren_test_recover_drop",
      handler,
      options,
    )

  // First connection up; simulate a drop by closing it from the outside.
  let assert Ok(first_connection) =
    process.receive(from: connects, within: 5000)
  wren.close_connection(first_connection)

  // The consumer should reconnect on its own, reporting a fresh connection.
  let assert Ok(_second) = process.receive(from: connects, within: 8000)

  // And it should be consuming again on the re-established subscription.
  let assert Ok(_) =
    wren.publish(
      channel,
      exchange: "",
      routing_key: "wren_test_recover_drop",
      payload: <<"after-reconnect">>,
    )
  let assert Ok(received) = process.receive(from: inbox, within: 5000)
  assert received.payload == <<"after-reconnect">>

  wren.stop(consumer)
  wren.close_connection(connection)
}

// ---------------------------------------------------------------------------
// Client front door
// ---------------------------------------------------------------------------

pub fn client_opens_channel_and_publishes_test() {
  let assert Ok(client) = wren.start_client(test_config())
  let channel = wren.client_channel(client)
  fresh_queue(channel, "wren_test_client")

  let assert Ok(_) =
    wren.publish(
      channel,
      exchange: "",
      routing_key: "wren_test_client",
      payload: <<"via client">>,
    )
  let assert Ok(payload) = wren.get(channel, "wren_test_client")
  assert payload == <<"via client">>
  assert wren.is_open(wren.client_connection(client)) == True

  wren.close_client(client)
}

// ---------------------------------------------------------------------------
// Publisher confirms + persistence
// ---------------------------------------------------------------------------

pub fn publish_confirmed_succeeds_test() {
  let #(connection, channel) = open()
  fresh_queue(channel, "wren_test_confirm")
  let assert Ok(_) = wren.enable_confirms(channel)

  let options =
    wren.publish_options()
    |> wren.route("wren_test_confirm")
    |> wren.with_persistence()
  let assert Ok(_) =
    wren.publish_confirmed(channel, <<"confirmed payload">>, options, 5000)

  let assert Ok(payload) = wren.get(channel, "wren_test_confirm")
  assert payload == <<"confirmed payload">>

  wren.close_connection(connection)
}

pub fn publish_confirmed_without_enabling_is_an_error_test() {
  // Each `open()` is a fresh connection, so a poisoned channel here can't
  // disturb the other tests.
  let #(connection, channel) = open()
  fresh_queue(channel, "wren_test_confirm_off")

  let options =
    wren.publish_options()
    |> wren.route("wren_test_confirm_off")
  // Waiting for confirms on a non-confirm channel is an error, not a hang.
  assert result.is_error(wren.publish_confirmed(channel, <<"x">>, options, 1000))

  wren.close_connection(connection)
}

// ---------------------------------------------------------------------------
// Concurrent delivery processing
// ---------------------------------------------------------------------------

pub fn concurrent_consumer_runs_each_delivery_in_its_own_process_test() {
  let #(connection, channel) = open()
  fresh_queue(channel, "wren_test_concurrent")

  // Each handler reports the process it runs in.
  let pids = process.new_subject()
  let handler = fn(_message: wren.Message) -> wren.Confirmation {
    process.send(pids, process.self())
    wren.Ack
  }
  let assert Ok(consumer) =
    wren.start_consumer_concurrent(channel, "wren_test_concurrent", handler, 5)

  let publish = fn(body) {
    wren.publish_text(
      channel,
      exchange: "",
      routing_key: "wren_test_concurrent",
      text: body,
    )
  }
  let assert Ok(_) = publish("1")
  let assert Ok(_) = publish("2")
  let assert Ok(_) = publish("3")

  // Concurrent processing spawns a fresh process per delivery, so the three
  // pids are all distinct.
  let assert Ok(p1) = process.receive(from: pids, within: 5000)
  let assert Ok(p2) = process.receive(from: pids, within: 5000)
  let assert Ok(p3) = process.receive(from: pids, within: 5000)
  assert p1 != p2
  assert p2 != p3
  assert p1 != p3

  wren.stop(consumer)
  wren.close_connection(connection)
}

pub fn serial_consumer_runs_deliveries_in_one_process_test() {
  let #(connection, channel) = open()
  fresh_queue(channel, "wren_test_serial")

  let pids = process.new_subject()
  let handler = fn(_message: wren.Message) -> wren.Confirmation {
    process.send(pids, process.self())
    wren.Ack
  }
  // A plain consumer handles deliveries inline in the actor — one process.
  let assert Ok(consumer) =
    wren.start_consumer(channel, "wren_test_serial", handler)

  let publish = fn(body) {
    wren.publish_text(
      channel,
      exchange: "",
      routing_key: "wren_test_serial",
      text: body,
    )
  }
  let assert Ok(_) = publish("1")
  let assert Ok(_) = publish("2")

  let assert Ok(p1) = process.receive(from: pids, within: 5000)
  let assert Ok(p2) = process.receive(from: pids, within: 5000)
  assert p1 == p2

  wren.stop(consumer)
  wren.close_connection(connection)
}

// ---------------------------------------------------------------------------
// Connection pool
// ---------------------------------------------------------------------------

pub fn connection_pool_hands_out_working_channels_test() {
  let assert Ok(pool) = wren.start_pool(test_config(), 2)
  assert wren.pool_size(pool) == 2

  // A channel from the pool behaves like any other.
  let assert Ok(channel) = wren.pool_channel(pool)
  let assert Ok(_) = wren.declare_queue(channel, "wren_test_pool")
  let assert Ok(_) = wren.purge_queue(channel, "wren_test_pool")
  let assert Ok(_) =
    wren.publish(channel, exchange: "", routing_key: "wren_test_pool", payload: <<
      "pooled",
    >>)
  let assert Ok(payload) = wren.get(channel, "wren_test_pool")
  assert payload == <<"pooled">>
  wren.close_channel(channel)

  // A second checkout (round-robined to the other connection) works too.
  let assert Ok(channel2) = wren.pool_channel(pool)
  let assert Ok(_) = wren.declare_queue(channel2, "wren_test_pool")
  wren.close_channel(channel2)

  wren.close_pool(pool)
}

// ---------------------------------------------------------------------------
// Health check + pool stats
// ---------------------------------------------------------------------------

pub fn health_check_succeeds_on_a_live_channel_test() {
  let #(connection, channel) = open()
  let assert Ok(_) = wren.health_check(channel)
  wren.close_connection(connection)
}

pub fn pool_stats_counts_channels_handed_out_test() {
  let assert Ok(pool) = wren.start_pool(test_config(), 2)

  let before = wren.pool_stats(pool)
  assert before.connections == 2
  assert before.channels_handed_out == 0

  let assert Ok(c1) = wren.pool_channel(pool)
  let assert Ok(c2) = wren.pool_channel(pool)
  let assert Ok(c3) = wren.pool_channel(pool)

  let after = wren.pool_stats(pool)
  assert after.channels_handed_out == 3

  wren.close_channel(c1)
  wren.close_channel(c2)
  wren.close_channel(c3)
  wren.close_pool(pool)
}

// ---------------------------------------------------------------------------
// Kind-based producer
// ---------------------------------------------------------------------------

pub fn publish_for_kind_routes_via_mapped_exchange_test() {
  let #(connection, channel) = open()
  let assert Ok(_) =
    wren.declare_exchange(
      channel,
      "wren_test_kind_ex",
      wren.Topic,
      wren.exchange_options(),
    )
  fresh_queue(channel, "wren_test_kind_q")
  let assert Ok(_) =
    wren.bind_queue(
      channel,
      queue: "wren_test_kind_q",
      exchange: "wren_test_kind_ex",
      routing_key: "order.created",
    )

  // Map the kind to the exchange; the routing key defaults to the kind.
  let routing =
    wren.kind_routing()
    |> wren.route_kind("order.created", "wren_test_kind_ex")

  let assert Ok(_) =
    wren.publish_for_kind(
      channel,
      routing,
      "order.created",
      <<"routed by kind">>,
      wren.publish_options(),
    )
  let assert Ok(payload) = wren.get(channel, "wren_test_kind_q")
  assert payload == <<"routed by kind">>

  let assert Ok(_) = wren.delete_exchange(channel, "wren_test_kind_ex")
  wren.close_connection(connection)
}

pub fn publish_for_kind_falls_back_to_default_exchange_test() {
  let #(connection, channel) = open()
  fresh_queue(channel, "wren_test_kind_fallback")

  // No mapping for this kind -> default exchange, routing key = kind = queue.
  let routing = wren.kind_routing()
  let assert Ok(_) =
    wren.publish_for_kind(
      channel,
      routing,
      "wren_test_kind_fallback",
      <<"unmapped">>,
      wren.publish_options(),
    )
  let assert Ok(payload) = wren.get(channel, "wren_test_kind_fallback")
  assert payload == <<"unmapped">>

  wren.close_connection(connection)
}

// ---------------------------------------------------------------------------
// TLS
// ---------------------------------------------------------------------------

pub fn tls_handshake_against_plaintext_port_fails_test() {
  // The dev broker speaks plaintext on 5672, so a TLS handshake there must
  // error (not hang) — proof the TLS path is wired through to amqp_client.
  let config =
    wren.Config(
      ..test_config(),
      connection_timeout_ms: 3000,
      tls: wren.Tls(
        verify: False,
        cacert_file: option.None,
        cert_file: option.None,
        key_file: option.None,
      ),
    )
  assert result.is_error(wren.connect(config))
}

// ---------------------------------------------------------------------------
// Topology refinements — delete guards + binding arguments
// ---------------------------------------------------------------------------

pub fn delete_queue_if_empty_guard_test() {
  // Happy path: an empty queue deletes under if_empty.
  let #(conn, channel) = open()
  fresh_queue(channel, "wren_test_if_empty_ok")
  let assert Ok(_) =
    wren.delete_queue_with(
      channel,
      "wren_test_if_empty_ok",
      if_unused: False,
      if_empty: True,
    )
  wren.close_connection(conn)

  // Guard path: a non-empty queue refuses. A failed precondition closes the
  // channel, so this runs on its own connection.
  let #(conn2, channel2) = open()
  fresh_queue(channel2, "wren_test_if_empty_no")
  let assert Ok(_) =
    wren.publish(
      channel2,
      exchange: "",
      routing_key: "wren_test_if_empty_no",
      payload: <<"blocker">>,
    )
  assert result.is_error(wren.delete_queue_with(
    channel2,
    "wren_test_if_empty_no",
    if_unused: False,
    if_empty: True,
  ))
  wren.close_connection(conn2)

  // Tidy up the leftover queue on a fresh channel.
  let #(conn3, channel3) = open()
  let _ = wren.delete_queue(channel3, "wren_test_if_empty_no")
  wren.close_connection(conn3)
}

pub fn headers_exchange_binding_arguments_route_test() {
  let #(connection, channel) = open()
  let assert Ok(_) =
    wren.declare_exchange(
      channel,
      "wren_test_headers_ex",
      wren.Headers,
      wren.exchange_options(),
    )
  fresh_queue(channel, "wren_test_headers_q")
  // Match all of: department = sales. The matcher lives in the binding args.
  let assert Ok(_) =
    wren.bind_queue_with(
      channel,
      queue: "wren_test_headers_q",
      exchange: "wren_test_headers_ex",
      routing_key: "",
      arguments: [
        #("x-match", wren.StringArg("all")),
        #("department", wren.StringArg("sales")),
      ],
    )

  let options =
    wren.publish_options()
    |> wren.to_exchange("wren_test_headers_ex")
    |> wren.with_header("department", "sales")
  let assert Ok(_) =
    wren.publish_with_options(channel, <<"for sales">>, options)

  let assert Ok(payload) = wren.get(channel, "wren_test_headers_q")
  assert payload == <<"for sales">>

  let assert Ok(_) = wren.delete_exchange(channel, "wren_test_headers_ex")
  wren.close_connection(connection)
}

// ---------------------------------------------------------------------------
// Consumer subscribe options
// ---------------------------------------------------------------------------

pub fn auto_ack_consumer_receives_delivery_test() {
  let #(connection, channel) = open()
  fresh_queue(channel, "wren_test_autoack")

  let inbox = process.new_subject()
  let handler = fn(message: wren.Message) -> wren.Confirmation {
    process.send(inbox, message)
    wren.Ack
  }
  // With auto-ack the broker acks on delivery; settlement is skipped.
  let options = wren.consume_options() |> wren.with_auto_ack()
  let assert Ok(consumer) =
    wren.start_consumer_with_options(
      channel,
      "wren_test_autoack",
      handler,
      options,
    )

  let assert Ok(_) =
    wren.publish(
      channel,
      exchange: "",
      routing_key: "wren_test_autoack",
      payload: <<"auto">>,
    )
  let assert Ok(received) = process.receive(from: inbox, within: 5000)
  assert received.payload == <<"auto">>

  wren.stop(consumer)
  wren.close_connection(connection)
}

pub fn consumer_with_tag_and_arguments_subscribes_test() {
  let #(connection, channel) = open()
  fresh_queue(channel, "wren_test_consopts")

  let inbox = process.new_subject()
  let handler = fn(message: wren.Message) -> wren.Confirmation {
    process.send(inbox, message)
    wren.Ack
  }
  // A custom tag, consumer-priority argument, and no_local all pass through.
  let options =
    wren.consume_options()
    |> wren.with_consumer_tag("wren-tag")
    |> wren.with_consume_arguments([#("x-priority", wren.IntArg(5))])
    |> wren.with_no_local()
  let assert Ok(consumer) =
    wren.start_consumer_with_options(
      channel,
      "wren_test_consopts",
      handler,
      options,
    )

  let assert Ok(_) =
    wren.publish(
      channel,
      exchange: "",
      routing_key: "wren_test_consopts",
      payload: <<"tagged">>,
    )
  let assert Ok(received) = process.receive(from: inbox, within: 5000)
  assert received.payload == <<"tagged">>

  wren.stop(consumer)
  wren.close_connection(connection)
}

// ---------------------------------------------------------------------------
// Message properties (M18) + batch publishing (M19)
// ---------------------------------------------------------------------------

pub fn message_properties_round_trip_test() {
  let #(connection, channel) = open()
  fresh_queue(channel, "wren_test_props")

  let inbox = process.new_subject()
  let handler = fn(message: wren.Message) -> wren.Confirmation {
    process.send(inbox, message)
    wren.Ack
  }
  let assert Ok(consumer) =
    wren.start_consumer(channel, "wren_test_props", handler)

  // The pair that makes RPC work: correlation id + reply-to.
  let options =
    wren.publish_options()
    |> wren.route("wren_test_props")
    |> wren.with_correlation_id("corr-1")
    |> wren.with_reply_to("reply-queue")
  let assert Ok(_) = wren.publish_with_options(channel, <<"ping">>, options)

  let assert Ok(received) = process.receive(from: inbox, within: 5000)
  assert received.correlation_id == option.Some("corr-1")
  assert received.reply_to == option.Some("reply-queue")

  wren.stop(consumer)
  wren.close_connection(connection)
}

pub fn publish_batch_delivers_to_each_target_test() {
  let #(connection, channel) = open()
  fresh_queue(channel, "wren_test_batch_a")
  fresh_queue(channel, "wren_test_batch_b")

  let messages = [
    #(wren.queue_target("wren_test_batch_a"), <<"to-a">>),
    #(wren.queue_target("wren_test_batch_b"), <<"to-b">>),
  ]
  let result = wren.publish_batch(channel, messages, wren.publish_options())
  assert result.published == 2
  assert result.failures == []

  let assert Ok(a) = wren.get(channel, "wren_test_batch_a")
  assert a == <<"to-a">>
  let assert Ok(b) = wren.get(channel, "wren_test_batch_b")
  assert b == <<"to-b">>

  wren.close_connection(connection)
}

pub fn publish_to_targets_fans_out_one_message_test() {
  let #(connection, channel) = open()
  fresh_queue(channel, "wren_test_fan_a")
  fresh_queue(channel, "wren_test_fan_b")

  let targets = [
    wren.queue_target("wren_test_fan_a"),
    wren.queue_target("wren_test_fan_b"),
  ]
  let result =
    wren.publish_to_targets(
      channel,
      <<"broadcast">>,
      targets,
      wren.publish_options(),
    )
  assert result.published == 2

  let assert Ok(a) = wren.get(channel, "wren_test_fan_a")
  assert a == <<"broadcast">>
  let assert Ok(b) = wren.get(channel, "wren_test_fan_b")
  assert b == <<"broadcast">>

  wren.close_connection(connection)
}

pub fn publish_batch_with_retry_succeeds_test() {
  let #(connection, channel) = open()
  fresh_queue(channel, "wren_test_batch_retry")

  let messages = [
    #(wren.queue_target("wren_test_batch_retry"), <<"m1">>),
    #(wren.queue_target("wren_test_batch_retry"), <<"m2">>),
  ]
  let result =
    wren.publish_batch_with_retry(channel, messages, wren.publish_options(), 3)
  assert result.published == 2
  assert result.failures == []

  wren.close_connection(connection)
}

// ---------------------------------------------------------------------------
// Raw byte payloads (M21)
// ---------------------------------------------------------------------------

pub fn raw_byte_payload_round_trips_test() {
  let #(connection, channel) = open()
  fresh_queue(channel, "wren_test_bytes")

  let inbox = process.new_subject()
  let handler = fn(message: wren.Message) -> wren.Confirmation {
    process.send(inbox, message)
    wren.Ack
  }
  let assert Ok(consumer) =
    wren.start_consumer(channel, "wren_test_bytes", handler)

  // Arbitrary non-UTF-8 bytes — the whole point of byte payloads.
  let payload = <<0, 159, 146, 150, 255>>
  let assert Ok(_) =
    wren.publish(
      channel,
      exchange: "",
      routing_key: "wren_test_bytes",
      payload: payload,
    )

  let assert Ok(received) = process.receive(from: inbox, within: 5000)
  assert received.payload == payload
  // It isn't valid UTF-8, so the text view correctly refuses it.
  assert result.is_error(wren.message_text(received))

  wren.stop(consumer)
  wren.close_connection(connection)
}

// ---------------------------------------------------------------------------
// Passive declare + config validation (M22)
// ---------------------------------------------------------------------------

pub fn passive_declare_detects_existence_test() {
  let #(connection, channel) = open()
  fresh_queue(channel, "wren_test_passive")
  // It exists, so passive declare succeeds.
  let assert Ok(_) = wren.declare_queue_passive(channel, "wren_test_passive")
  wren.close_connection(connection)

  // A missing queue fails (and closes the channel, hence a fresh connection).
  let #(conn2, channel2) = open()
  assert result.is_error(wren.declare_queue_passive(
    channel2,
    "wren_test_does_not_exist_42",
  ))
  wren.close_connection(conn2)
}

pub fn validate_config_test() {
  assert wren.validate_config(test_config()) == Ok(Nil)
  assert result.is_error(wren.validate_config(
    wren.Config(..test_config(), host: ""),
  ))
  assert result.is_error(wren.validate_config(
    wren.Config(..test_config(), port: 0),
  ))
}
