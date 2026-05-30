//// Codecs turn typed values into wire payloads and back again.
////
//// A `Codec(a)` is just a pair of functions, so you can supply any
//// serialisation you like. `json` builds one from a `gleam_json` encoder and a
//// `gleam/dynamic/decode` decoder; `string` is the identity codec for raw text.
////
//// This is wren's idiomatic stand-in for bunnyhop's `Codec` trait — explicit
//// values rather than typeclass machinery.

import gleam/dynamic/decode.{type Decoder}
import gleam/json.{type Json}
import gleam/result
import gleam/string

/// Something went wrong encoding to, or decoding from, the wire.
pub type CodecError {
  /// A value could not be serialised.
  EncodeError(reason: String)
  /// A payload could not be deserialised into the expected type.
  DecodeError(reason: String)
}

/// A reversible mapping between a typed value `a` and its string payload.
pub type Codec(a) {
  Codec(
    encode: fn(a) -> Result(String, CodecError),
    decode: fn(String) -> Result(a, CodecError),
  )
}

/// Encode a value with the given codec.
pub fn encode(codec: Codec(a), value: a) -> Result(String, CodecError) {
  codec.encode(value)
}

/// Decode a payload with the given codec.
pub fn decode(codec: Codec(a), payload: String) -> Result(a, CodecError) {
  codec.decode(payload)
}

/// A JSON codec built from a `gleam_json` encoder and a decoder.
///
/// ```gleam
/// let order_codec =
///   codec.json(
///     fn(o: Order) { json.object([#("id", json.string(o.id))]) },
///     {
///       use id <- decode.field("id", decode.string)
///       decode.success(Order(id:))
///     },
///   )
/// ```
pub fn json(to_json: fn(a) -> Json, decoder: Decoder(a)) -> Codec(a) {
  Codec(
    encode: fn(value) { Ok(json.to_string(to_json(value))) },
    decode: fn(payload) {
      json.parse(from: payload, using: decoder)
      |> result.map_error(fn(error) { DecodeError(string.inspect(error)) })
    },
  )
}

/// The identity codec: payloads are passed through as raw strings.
pub fn string() -> Codec(String) {
  Codec(encode: fn(value) { Ok(value) }, decode: fn(payload) { Ok(payload) })
}
