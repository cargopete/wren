//// Pure (broker-free) tests for retry policy and metadata.

import gleam/list
import gleam/option.{None, Some}
import wren/retry

// ---------------------------------------------------------------------------
// Delay maths
// ---------------------------------------------------------------------------

pub fn exponential_backoff_grows_test() {
  let policy =
    retry.RetryPolicy(
      strategy: retry.ExponentialBackoff(
        initial_ms: 1000,
        max_ms: 60_000,
        multiplier: 2.0,
      ),
      max_attempts: 5,
    )
  // 1000 * 2^0, 2^1, 2^2 ...
  assert retry.calculate_delay(policy, 1) == 1000
  assert retry.calculate_delay(policy, 2) == 2000
  assert retry.calculate_delay(policy, 3) == 4000
  assert retry.calculate_delay(policy, 4) == 8000
}

pub fn exponential_backoff_is_capped_test() {
  let policy =
    retry.RetryPolicy(
      strategy: retry.ExponentialBackoff(
        initial_ms: 1000,
        max_ms: 5000,
        multiplier: 2.0,
      ),
      max_attempts: 10,
    )
  // 1000, 2000, 4000, then capped at 5000.
  assert retry.calculate_delay(policy, 4) == 5000
  assert retry.calculate_delay(policy, 9) == 5000
}

pub fn fixed_interval_is_constant_test() {
  let policy =
    retry.RetryPolicy(strategy: retry.FixedInterval(3000), max_attempts: 4)
  assert retry.calculate_delay(policy, 1) == 3000
  assert retry.calculate_delay(policy, 9) == 3000
}

pub fn retry_intervals_has_one_entry_per_attempt_test() {
  let policy =
    retry.RetryPolicy(strategy: retry.FixedInterval(500), max_attempts: 3)
  assert retry.retry_intervals(policy) == [500, 500, 500]
}

// ---------------------------------------------------------------------------
// Metadata
// ---------------------------------------------------------------------------

pub fn record_failure_increments_and_notes_reason_test() {
  let metadata = retry.new_metadata(3)
  assert metadata.attempt == 0

  let metadata = retry.record_failure(metadata, "boom")
  assert metadata.attempt == 1
  assert metadata.reason == Some("boom")
}

pub fn is_exhausted_at_the_boundary_test() {
  let metadata = retry.RetryMetadata(..retry.new_metadata(3), attempt: 2)
  assert retry.is_exhausted(metadata) == False

  let metadata = retry.record_failure(metadata, "again")
  // attempt is now 3, equal to max_attempts.
  assert retry.is_exhausted(metadata) == True
}

pub fn headers_round_trip_test() {
  let metadata =
    retry.RetryMetadata(
      attempt: 2,
      max_attempts: 5,
      first_death: Some("2026-05-30T10:00:00Z"),
      last_retry: Some("2026-05-30T10:05:00Z"),
      original_error: Some("timeout"),
      reason: Some("handler returned Retry"),
      original_routing_key: Some("orders.created"),
    )

  let restored = retry.from_headers(retry.to_headers(metadata), 99)
  assert restored == metadata
}

pub fn from_headers_uses_default_max_when_absent_test() {
  // Only a count header present; max should fall back to the policy default.
  let headers = [#(retry.retry_count_header, "1")]
  let metadata = retry.from_headers(headers, 7)
  assert metadata.attempt == 1
  assert metadata.max_attempts == 7
}

pub fn to_headers_omits_absent_optionals_test() {
  let headers = retry.to_headers(retry.new_metadata(3))
  // Count and max are always present; the optional timestamps are not.
  assert list.key_find(headers, retry.retry_count_header) == Ok("0")
  assert list.key_find(headers, retry.max_retries_header) == Ok("3")
  assert list.key_find(headers, retry.first_death_header) == Error(Nil)
}

pub fn new_metadata_starts_clean_test() {
  let metadata = retry.new_metadata(4)
  assert metadata.attempt == 0
  assert metadata.max_attempts == 4
  assert metadata.first_death == None
  assert metadata.original_routing_key == None
}
