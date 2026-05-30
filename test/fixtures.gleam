//// Shared test fixtures: a sample domain type and its JSON codec.

import gleam/dynamic/decode
import gleam/json
import wren/codec

pub type Order {
  Order(id: String, qty: Int)
}

pub fn order_codec() -> codec.Codec(Order) {
  codec.json(
    fn(order: Order) {
      json.object([
        #("id", json.string(order.id)),
        #("qty", json.int(order.qty)),
      ])
    },
    {
      use id <- decode.field("id", decode.string)
      use qty <- decode.field("qty", decode.int)
      decode.success(Order(id:, qty:))
    },
  )
}
