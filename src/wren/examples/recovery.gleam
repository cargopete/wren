//// Recovery example — a self-healing consumer that owns its connection.
////
////   docker compose up -d
////   gleam run -m wren/examples/recovery

import gleam/erlang/process
import gleam/io
import gleam/result
import wren

pub fn main() -> Nil {
  let config =
    wren.Config(..wren.default_config(), username: "wren", password: "wren")

  let outcome = {
    use client <- result.try(wren.start_client(config))
    let channel = wren.client_channel(client)
    use _ <- result.try(wren.declare_queue(channel, "events"))
    use _ <- result.try(wren.purge_queue(channel, "events"))

    // `on_connect` fires on the first connect and on every reconnection — a
    // good place to re-declare topology or emit a metric.
    let options =
      wren.recoverable_options()
      |> wren.on_connect(fn(_connection) { io.println("🔌 (re)connected") })

    let handler = fn(message: wren.Message) -> wren.Confirmation {
      io.println("📥 " <> result.unwrap(wren.message_text(message), "<binary>"))
      wren.Ack
    }
    use consumer <- result.try(wren.start_recoverable_consumer(
      config,
      "events",
      handler,
      options,
    ))

    use _ <- result.try(wren.publish_text(
      channel,
      exchange: "",
      routing_key: "events",
      text: "hello from a resilient consumer",
    ))

    process.sleep(500)
    wren.stop(consumer)
    wren.close_client(client)
    Ok(Nil)
  }

  case outcome {
    Ok(_) -> io.println("✅ recovery example complete")
    Error(_) -> io.println("❌ recovery example failed (is the broker up?)")
  }
}
