//// Router example — dispatch typed messages by kind.
////
////   docker compose up -d
////   gleam run -m wren/examples/router

import gleam/dynamic/decode
import gleam/erlang/process
import gleam/int
import gleam/io
import gleam/json
import gleam/result
import wren
import wren/codec

type Order {
  Order(id: String, total: Int)
}

fn order_codec() -> codec.Codec(Order) {
  codec.json(
    fn(order: Order) {
      json.object([
        #("id", json.string(order.id)),
        #("total", json.int(order.total)),
      ])
    },
    {
      use id <- decode.field("id", decode.string)
      use total <- decode.field("total", decode.int)
      decode.success(Order(id:, total:))
    },
  )
}

pub fn main() -> Nil {
  let config =
    wren.Config(..wren.default_config(), username: "wren", password: "wren")

  let outcome = {
    use client <- result.try(wren.start_client(config))
    let channel = wren.client_channel(client)
    use _ <- result.try(wren.declare_queue(channel, "orders"))
    use _ <- result.try(wren.purge_queue(channel, "orders"))

    let router =
      wren.router()
      |> wren.handle("order.created", order_codec(), fn(order: Order) {
        io.println(
          "🧾 order " <> order.id <> " for " <> int.to_string(order.total),
        )
        wren.Ack
      })
      |> wren.fallback(fn(message: wren.Message) {
        io.println(
          "🤷 unrouted: "
          <> result.unwrap(wren.message_text(message), "<binary>"),
        )
        wren.Reject
      })
    use consumer <- result.try(wren.start_router(channel, "orders", router))

    use _ <- result.try(wren.publish_encoded(
      channel,
      Order(id: "A-1", total: 4200),
      order_codec(),
      wren.publish_options()
        |> wren.route("orders")
        |> wren.with_kind("order.created"),
    ))

    process.sleep(500)
    wren.stop(consumer)
    wren.close_client(client)
    Ok(Nil)
  }

  case outcome {
    Ok(_) -> io.println("✅ router example complete")
    Error(_) -> io.println("❌ router example failed (is the broker up?)")
  }
}
