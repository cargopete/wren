//// Pure (broker-free) tests for the codec abstraction.

import fixtures.{Order}
import gleam/result
import wren/codec

pub fn json_codec_round_trips_test() {
  let order_codec = fixtures.order_codec()
  let assert Ok(payload) = codec.encode(order_codec, Order(id: "a1", qty: 3))
  let assert Ok(decoded) = codec.decode(order_codec, payload)
  assert decoded == Order(id: "a1", qty: 3)
}

pub fn json_codec_rejects_garbage_test() {
  let order_codec = fixtures.order_codec()
  assert result.is_error(codec.decode(order_codec, "not valid json"))
}

pub fn json_codec_rejects_wrong_shape_test() {
  let order_codec = fixtures.order_codec()
  // Right JSON, wrong fields.
  assert result.is_error(codec.decode(order_codec, "{\"id\":\"a1\"}"))
}

pub fn string_codec_round_trips_test() {
  let string_codec = codec.string()
  let assert Ok(payload) = codec.encode(string_codec, "hello")
  assert payload == "hello"
  let assert Ok(decoded) = codec.decode(string_codec, "hello")
  assert decoded == "hello"
}
