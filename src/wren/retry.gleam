//// Retry policy and per-message retry metadata.
////
//// A `RetryPolicy` decides *how long* to wait before the next attempt and *how
//// many* attempts to allow. `RetryMetadata` is the running state carried on a
//// message's headers as it bounces through the retry machinery — wren's
//// idiomatic take on bunnyhop's `retry.rs`.
////
//// This module is pure: no broker, no clock. M6 wires it into real delay-queue
//// topology and timestamping.

import gleam/float
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result

// ===========================================================================
// Strategy & policy
// ===========================================================================

/// How the delay before each retry is computed.
pub type RetryStrategy {
  /// `initial_ms * multiplier^(attempt - 1)`, capped at `max_ms`.
  ExponentialBackoff(initial_ms: Int, max_ms: Int, multiplier: Float)
  /// The same `interval_ms` before every attempt.
  FixedInterval(interval_ms: Int)
}

/// A strategy plus a ceiling on how many attempts to make.
pub type RetryPolicy {
  RetryPolicy(strategy: RetryStrategy, max_attempts: Int)
}

/// A common starting point: exponential backoff from 1s, doubling, capped at
/// 1 minute, over 5 attempts.
pub fn default_policy() -> RetryPolicy {
  RetryPolicy(
    strategy: ExponentialBackoff(
      initial_ms: 1000,
      max_ms: 60_000,
      multiplier: 2.0,
    ),
    max_attempts: 5,
  )
}

/// The delay in milliseconds before `attempt` (1-based: attempt 1 is the first
/// retry). Never negative; exponential delays are capped at `max_ms`.
pub fn calculate_delay(policy: RetryPolicy, attempt: Int) -> Int {
  let attempt = int.max(attempt, 1)
  case policy.strategy {
    FixedInterval(interval_ms) -> int.max(interval_ms, 0)
    ExponentialBackoff(initial_ms, max_ms, multiplier) -> {
      let factor =
        float.power(multiplier, int.to_float(attempt - 1))
        |> result.unwrap(1.0)
      let delay = float.round(int.to_float(initial_ms) *. factor)
      delay |> int.min(max_ms) |> int.max(0)
    }
  }
}

/// The full schedule of delays, one per allowed attempt.
pub fn retry_intervals(policy: RetryPolicy) -> List(Int) {
  range(1, policy.max_attempts)
  |> list.map(fn(attempt) { calculate_delay(policy, attempt) })
}

fn range(from: Int, to: Int) -> List(Int) {
  case from > to {
    True -> []
    False -> [from, ..range(from + 1, to)]
  }
}

// ===========================================================================
// Per-message metadata
// ===========================================================================

/// Header carrying the current retry count.
pub const retry_count_header = "x-retry-count"

/// Header carrying the maximum number of attempts.
pub const max_retries_header = "x-max-retries"

/// Header carrying the timestamp of the first failure.
pub const first_death_header = "x-first-death"

/// Header carrying the timestamp of the most recent retry.
pub const last_retry_header = "x-last-retry"

/// Header carrying the original error message.
pub const original_error_header = "x-original-error"

/// Header carrying a human-readable reason for the retry.
pub const retry_reason_header = "x-retry-reason"

/// Header preserving the message's original routing key.
pub const original_routing_key_header = "x-original-routing-key"

/// The retry state carried on a message's headers.
pub type RetryMetadata {
  RetryMetadata(
    attempt: Int,
    max_attempts: Int,
    first_death: Option(String),
    last_retry: Option(String),
    original_error: Option(String),
    reason: Option(String),
    original_routing_key: Option(String),
  )
}

/// Fresh metadata for a message that has not yet failed.
pub fn new_metadata(max_attempts: Int) -> RetryMetadata {
  RetryMetadata(
    attempt: 0,
    max_attempts:,
    first_death: None,
    last_retry: None,
    original_error: None,
    reason: None,
    original_routing_key: None,
  )
}

/// Read retry metadata from message headers. When `x-max-retries` is absent,
/// `default_max` is used (the consumer's configured policy maximum).
pub fn from_headers(
  headers: List(#(String, String)),
  default_max: Int,
) -> RetryMetadata {
  RetryMetadata(
    attempt: header_int(headers, retry_count_header, 0),
    max_attempts: header_int(headers, max_retries_header, default_max),
    first_death: header_opt(headers, first_death_header),
    last_retry: header_opt(headers, last_retry_header),
    original_error: header_opt(headers, original_error_header),
    reason: header_opt(headers, retry_reason_header),
    original_routing_key: header_opt(headers, original_routing_key_header),
  )
}

/// Serialise metadata back into headers. Always emits the count and maximum;
/// optional fields are emitted only when present.
pub fn to_headers(metadata: RetryMetadata) -> List(#(String, String)) {
  [
    #(retry_count_header, int.to_string(metadata.attempt)),
    #(max_retries_header, int.to_string(metadata.max_attempts)),
  ]
  |> push_opt(first_death_header, metadata.first_death)
  |> push_opt(last_retry_header, metadata.last_retry)
  |> push_opt(original_error_header, metadata.original_error)
  |> push_opt(retry_reason_header, metadata.reason)
  |> push_opt(original_routing_key_header, metadata.original_routing_key)
}

/// Record another failure: bump the attempt count and note the reason.
pub fn record_failure(
  metadata: RetryMetadata,
  reason: String,
) -> RetryMetadata {
  RetryMetadata(..metadata, attempt: metadata.attempt + 1, reason: Some(reason))
}

/// Has this message used up its allowed attempts?
pub fn is_exhausted(metadata: RetryMetadata) -> Bool {
  metadata.attempt >= metadata.max_attempts
}

// ===========================================================================
// Internal helpers
// ===========================================================================

fn header_int(
  headers: List(#(String, String)),
  key: String,
  default: Int,
) -> Int {
  case list.key_find(headers, key) {
    Ok(value) -> int.parse(value) |> result.unwrap(default)
    Error(_) -> default
  }
}

fn header_opt(headers: List(#(String, String)), key: String) -> Option(String) {
  case list.key_find(headers, key) {
    Ok(value) -> Some(value)
    Error(_) -> None
  }
}

fn push_opt(
  headers: List(#(String, String)),
  key: String,
  value: Option(String),
) -> List(#(String, String)) {
  case value {
    Some(value) -> [#(key, value), ..headers]
    None -> headers
  }
}
