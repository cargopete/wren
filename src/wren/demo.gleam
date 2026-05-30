//// A runnable demo of a supervised wren consumer handling real deliveries.
////
////   docker compose up -d
////   gleam run -m wren/demo

import gleam/erlang/process
import gleam/io
import gleam/result
import wren.{type WrenError}

pub fn main() -> Nil {
  let config =
    wren.Config(..wren.default_config(), username: "wren", password: "wren")

  let outcome = {
    use connection <- result.try(wren.connect(config))
    use channel <- result.try(wren.open_channel(connection))
    use _ <- result.try(wren.declare_queue(channel, "wren_demo"))

    let handler = fn(message: wren.Message) -> wren.Confirmation {
      let text = result.unwrap(wren.message_text(message), "<binary>")
      io.println("📨 received on '" <> message.routing_key <> "': " <> text)
      wren.Ack
    }
    use consumer <- result.try(wren.start_consumer(
      channel,
      "wren_demo",
      handler,
    ))

    use _ <- result.try(wren.publish_text(
      channel,
      exchange: "",
      routing_key: "wren_demo",
      text: "first message",
    ))
    use _ <- result.try(wren.publish_text(
      channel,
      exchange: "",
      routing_key: "wren_demo",
      text: "second message",
    ))

    // Give the supervised consumer a moment to process before tearing down.
    process.sleep(500)
    wren.stop(consumer)
    wren.close_channel(channel)
    wren.close_connection(connection)
    Ok(Nil)
  }

  case outcome {
    Ok(_) -> io.println("✅ supervised consumer demo complete")
    Error(error) -> io.println("❌ " <> describe(error))
  }
}

fn describe(error: WrenError) -> String {
  case error {
    wren.ConnectionFailed(reason) -> "connection failed: " <> reason
    wren.ChannelFailed(reason) -> "channel failed: " <> reason
    wren.EncodingFailed(reason) -> "encoding failed: " <> reason
    wren.DecodingFailed(reason) -> "decoding failed: " <> reason
  }
}
