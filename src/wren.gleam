import gleam/io

pub fn main() -> Nil {
  case spike() {
    Ok(payload) -> io.println("✅ wren received: " <> payload)
    Error(reason) -> io.println("❌ wren spike failed: " <> reason)
  }
}

/// FFI into the Erlang `amqp_client`: connect → declare → publish → consume.
@external(erlang, "wren_ffi", "spike")
fn spike() -> Result(String, String)
