//// Pure (broker-free) tests for environment-driven configuration.

import gleam/list
import wren

fn lookup_from(
  pairs: List(#(String, String)),
) -> fn(String) -> Result(String, Nil) {
  fn(key) { list.key_find(pairs, key) }
}

pub fn config_from_lookup_reads_all_fields_test() {
  let env =
    lookup_from([
      #("RABBITMQ_HOST", "broker.example"),
      #("RABBITMQ_PORT", "5673"),
      #("RABBITMQ_USERNAME", "alice"),
      #("RABBITMQ_PASSWORD", "secret"),
      #("RABBITMQ_VHOST", "/app"),
      #("RABBITMQ_HEARTBEAT", "30"),
      #("RABBITMQ_CONNECTION_TIMEOUT", "5000"),
    ])
  let config = wren.config_from_lookup(env)
  assert config.host == "broker.example"
  assert config.port == 5673
  assert config.username == "alice"
  assert config.password == "secret"
  assert config.virtual_host == "/app"
  assert config.heartbeat_seconds == 30
  assert config.connection_timeout_ms == 5000
}

pub fn config_from_lookup_falls_back_to_defaults_test() {
  // Nothing in the environment — every field should be the default.
  let config = wren.config_from_lookup(fn(_key) { Error(Nil) })
  assert config == wren.default_config()
}

pub fn config_from_lookup_ignores_unparseable_numbers_test() {
  let env = lookup_from([#("RABBITMQ_PORT", "not-a-number")])
  let config = wren.config_from_lookup(env)
  assert config.port == wren.default_config().port
}

pub fn config_from_lookup_accepts_user_and_pass_aliases_test() {
  let env =
    lookup_from([#("RABBITMQ_USER", "bob"), #("RABBITMQ_PASS", "hunter2")])
  let config = wren.config_from_lookup(env)
  assert config.username == "bob"
  assert config.password == "hunter2"
}
