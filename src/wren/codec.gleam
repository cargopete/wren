//// Codecs turn typed values into wire payloads (bytes) and back again.
////
//// A `Codec(a)` is just a pair of functions, so you can supply any
//// serialisation you like. `json` builds one from a `gleam_json` encoder and a
//// `gleam/dynamic/decode` decoder; `string` is the UTF-8 text codec, and
//// `bytes` is the identity codec for raw binary.
////
//// This is wren's idiomatic stand-in for bunnyhop's `Codec` trait — explicit
//// values rather than typeclass machinery.

import gleam/bit_array
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

/// A reversible mapping between a typed value `a` and its byte payload.
pub type Codec(a) {
  Codec(
    encode: fn(a) -> Result(BitArray, CodecError),
    decode: fn(BitArray) -> Result(a, CodecError),
  )
}

/// Encode a value with the given codec.
pub fn encode(codec: Codec(a), value: a) -> Result(BitArray, CodecError) {
  codec.encode(value)
}

/// Decode a payload with the given codec.
pub fn decode(codec: Codec(a), payload: BitArray) -> Result(a, CodecError) {
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
    encode: fn(value) {
      Ok(bit_array.from_string(json.to_string(to_json(value))))
    },
    decode: fn(payload) {
      case bit_array.to_string(payload) {
        Ok(text) ->
          json.parse(from: text, using: decoder)
          |> result.map_error(fn(error) { DecodeError(string.inspect(error)) })
        Error(_) -> Error(DecodeError("payload is not valid UTF-8"))
      }
    },
  )
}

/// A UTF-8 text codec: values are `String`s, decoding fails on invalid UTF-8.
pub fn string() -> Codec(String) {
  Codec(
    encode: fn(value) { Ok(bit_array.from_string(value)) },
    decode: fn(payload) {
      bit_array.to_string(payload)
      |> result.map_error(fn(_) { DecodeError("payload is not valid UTF-8") })
    },
  )
}

/// The identity codec: payloads are passed through as raw bytes.
pub fn bytes() -> Codec(BitArray) {
  Codec(encode: fn(value) { Ok(value) }, decode: fn(payload) { Ok(payload) })
}
