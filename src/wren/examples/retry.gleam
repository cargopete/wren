//// Retry example — a handler that fails once, then succeeds via a delay queue.
////
////   docker compose up -d
////   gleam run -m wren/examples/retry

import gleam/erlang/process
import gleam/int
import gleam/io
import gleam/result
import wren
import wren/retry

pub fn main() -> Nil {
  let config =
    wren.Config(..wren.default_config(), username: "wren", password: "wren")

  // Fixed 500ms backoff, up to 3 attempts.
  let policy =
    retry.RetryPolicy(strategy: retry.FixedInterval(500), max_attempts: 3)
  let infra = wren.retry_infrastructure("jobs", policy)

  let outcome = {
    use client <- result.try(wren.start_client(config))
    let channel = wren.client_channel(client)

    // The retry count rides along in the headers, so the handler can see which
    // attempt it's on and behave accordingly.
    let handler = fn(message: wren.Message) -> wren.Confirmation {
      case retry.from_headers(message.headers, 3).attempt {
        0 -> {
          io.println("💥 first try failed — sending to the retry queue")
          wren.Retry
        }
        attempt -> {
          io.println("✅ succeeded on retry " <> int.to_string(attempt))
          wren.Ack
        }
      }
    }
    use consumer <- result.try(wren.start_consumer_with_retry(
      channel,
      handler,
      infra,
    ))

    use _ <- result.try(wren.publish_text(
      channel,
      exchange: "",
      routing_key: infra.main_queue,
      text: "do the thing",
    ))

    // Long enough for the failure, the delay, and the successful retry.
    process.sleep(2000)
    wren.stop(consumer)
    wren.close_client(client)
    Ok(Nil)
  }

  case outcome {
    Ok(_) -> io.println("✅ retry example complete")
    Error(_) -> io.println("❌ retry example failed (is the broker up?)")
  }
}
