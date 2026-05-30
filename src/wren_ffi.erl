-module(wren_ffi).
-include_lib("amqp_client/include/amqp_client.hrl").
-export([
    connect/8,
    getenv/1,
    open_channel/1,
    declare_queue_full/6,
    declare_exchange/7,
    bind_queue_full/5,
    unbind_queue/4,
    delete_queue_full/4,
    delete_exchange_full/3,
    purge_queue/2,
    publish/4,
    publish_full/10,
    enable_confirms/1,
    wait_for_confirms/2,
    get/2,
    subscribe/3,
    set_qos/4,
    settle/3,
    decode_event/1,
    connection_pid/1,
    is_connection_open/1,
    health_check/1,
    log_warning/1,
    now_timestamp/0,
    close_channel/1,
    close_connection/1
]).

%% Each function returns a value shaped for Gleam:
%%   {ok, X} | {error, Binary}  -> Result(X, String)
%% Opaque connection/channel pids are passed back and forth as-is.

connect(Host, Port, User, Pass, VHost, Heartbeat, Timeout, Tls) ->
    Params = #amqp_params_network{
        host = binary_to_list(Host),
        port = Port,
        username = User,
        password = Pass,
        virtual_host = VHost,
        heartbeat = Heartbeat,
        connection_timeout = Timeout,
        ssl_options = build_ssl(Tls)
    },
    case amqp_connection:start(Params) of
        {ok, Connection} -> {ok, Connection};
        {error, Reason} -> {error, fmt(Reason)}
    end.

%% `ssl_options = none` means plaintext; a list enables TLS. The Gleam `Tls`
%% value arrives as `no_tls` | `{tls, Verify, CaCert, Cert, Key}`.
build_ssl(no_tls) ->
    none;
build_ssl({tls, Verify, CaCert, Cert, Key}) ->
    [{verify, verify_mode(Verify)}]
        ++ maybe_file(cacertfile, CaCert)
        ++ maybe_file(certfile, Cert)
        ++ maybe_file(keyfile, Key).

verify_mode(true) -> verify_peer;
verify_mode(false) -> verify_none.

maybe_file(_Key, none) -> [];
maybe_file(Key, {some, Path}) -> [{Key, binary_to_list(Path)}].

%% Read an environment variable as `{ok, Binary} | {error, nil}`.
getenv(Name) ->
    case os:getenv(binary_to_list(Name)) of
        false -> {error, nil};
        Value -> {ok, list_to_binary(Value)}
    end.

open_channel(Connection) ->
    case amqp_connection:open_channel(Connection) of
        {ok, Channel} -> {ok, Channel};
        closing -> {error, <<"connection is closing">>};
        {error, Reason} -> {error, fmt(Reason)}
    end.

declare_queue_full(Channel, Queue, Durable, Exclusive, AutoDelete, Arguments) ->
    Declare = #'queue.declare'{
        queue = Queue,
        durable = Durable,
        exclusive = Exclusive,
        auto_delete = AutoDelete,
        arguments = to_amqp_args(Arguments)
    },
    try amqp_channel:call(Channel, Declare) of
        #'queue.declare_ok'{} -> {ok, nil};
        Other -> {error, fmt(Other)}
    catch
        Class:Reason -> {error, fmt({Class, Reason})}
    end.

declare_exchange(Channel, Exchange, Type, Durable, AutoDelete, Internal, Arguments) ->
    Declare = #'exchange.declare'{
        exchange = Exchange,
        type = Type,
        durable = Durable,
        auto_delete = AutoDelete,
        internal = Internal,
        arguments = to_amqp_args(Arguments)
    },
    try amqp_channel:call(Channel, Declare) of
        #'exchange.declare_ok'{} -> {ok, nil};
        Other -> {error, fmt(Other)}
    catch
        Class:Reason -> {error, fmt({Class, Reason})}
    end.

bind_queue_full(Channel, Queue, Exchange, RoutingKey, Arguments) ->
    Bind = #'queue.bind'{
        queue = Queue,
        exchange = Exchange,
        routing_key = RoutingKey,
        arguments = to_amqp_args(Arguments)
    },
    try amqp_channel:call(Channel, Bind) of
        #'queue.bind_ok'{} -> {ok, nil};
        Other -> {error, fmt(Other)}
    catch
        Class:Reason -> {error, fmt({Class, Reason})}
    end.

unbind_queue(Channel, Queue, Exchange, RoutingKey) ->
    Unbind = #'queue.unbind'{queue = Queue, exchange = Exchange, routing_key = RoutingKey},
    try amqp_channel:call(Channel, Unbind) of
        #'queue.unbind_ok'{} -> {ok, nil};
        Other -> {error, fmt(Other)}
    catch
        Class:Reason -> {error, fmt({Class, Reason})}
    end.

delete_queue_full(Channel, Queue, IfUnused, IfEmpty) ->
    Delete = #'queue.delete'{queue = Queue, if_unused = IfUnused, if_empty = IfEmpty},
    try amqp_channel:call(Channel, Delete) of
        #'queue.delete_ok'{} -> {ok, nil};
        Other -> {error, fmt(Other)}
    catch
        Class:Reason -> {error, fmt({Class, Reason})}
    end.

delete_exchange_full(Channel, Exchange, IfUnused) ->
    Delete = #'exchange.delete'{exchange = Exchange, if_unused = IfUnused},
    try amqp_channel:call(Channel, Delete) of
        #'exchange.delete_ok'{} -> {ok, nil};
        Other -> {error, fmt(Other)}
    catch
        Class:Reason -> {error, fmt({Class, Reason})}
    end.

%% Convert Gleam `Arg` values into AMQP `{Name, Type, Value}` field-table tuples.
to_amqp_args(Arguments) ->
    [to_amqp_arg(Key, Arg) || {Key, Arg} <- Arguments].

to_amqp_arg(Key, {int_arg, Value}) -> {Key, long, Value};
to_amqp_arg(Key, {string_arg, Value}) -> {Key, longstr, Value};
to_amqp_arg(Key, {bool_arg, Value}) -> {Key, bool, Value}.

%% Remove all ready messages from a queue. Handy for deterministic tests.
purge_queue(Channel, Queue) ->
    try amqp_channel:call(Channel, #'queue.purge'{queue = Queue}) of
        #'queue.purge_ok'{} -> {ok, nil};
        Other -> {error, fmt(Other)}
    catch
        Class:Reason -> {error, fmt({Class, Reason})}
    end.

publish(Channel, Exchange, RoutingKey, Payload) ->
    Publish = #'basic.publish'{exchange = Exchange, routing_key = RoutingKey},
    try amqp_channel:cast(Channel, Publish, #amqp_msg{payload = Payload}) of
        ok -> {ok, nil};
        Other -> {error, fmt(Other)}
    catch
        Class:Reason -> {error, fmt({Class, Reason})}
    end.

%% Publish with the full set of producer options. Optional fields arrive from
%% Gleam as `none` | `{some, Value}`; bools as `true` | `false`; headers as a
%% list of `{Key, Value}` binaries.
publish_full(Channel, Exchange, RoutingKey, Payload, Headers, Priority, Expiration, Mandatory, ContentType, Persistent) ->
    Publish = #'basic.publish'{
        exchange = Exchange,
        routing_key = RoutingKey,
        mandatory = Mandatory
    },
    Props =
        lists:foldl(
            fun(Apply, Acc) -> Apply(Acc) end,
            #'P_basic'{headers = build_headers(Headers)},
            [
                with_content_type(ContentType),
                with_priority(Priority),
                with_expiration(Expiration),
                with_persistence(Persistent)
            ]
        ),
    try amqp_channel:cast(Channel, Publish, #amqp_msg{props = Props, payload = Payload}) of
        ok -> {ok, nil};
        Other -> {error, fmt(Other)}
    catch
        Class:Reason -> {error, fmt({Class, Reason})}
    end.

%% Put a channel into publisher-confirm mode.
enable_confirms(Channel) ->
    try amqp_channel:call(Channel, #'confirm.select'{}) of
        #'confirm.select_ok'{} -> {ok, nil};
        Other -> {error, fmt(Other)}
    catch
        Class:Reason -> {error, fmt({Class, Reason})}
    end.

%% Wait (up to TimeoutMs) for the broker to confirm all messages published since
%% the last call: `confirmed` (all ack'd), `nacked`, or `timed_out`.
wait_for_confirms(Channel, TimeoutMs) ->
    try amqp_channel:wait_for_confirms(Channel, {TimeoutMs, millisecond}) of
        true -> {ok, confirmed};
        false -> {ok, nacked};
        timeout -> {ok, timed_out}
    catch
        Class:Reason -> {error, fmt({Class, Reason})}
    end.

%% An empty header list means "no headers table" (undefined), not an empty one.
build_headers([]) ->
    undefined;
build_headers(Headers) when is_list(Headers) ->
    [{Key, longstr, Value} || {Key, Value} <- Headers].

with_content_type(none) -> fun(Props) -> Props end;
with_content_type({some, ContentType}) -> fun(Props) -> Props#'P_basic'{content_type = ContentType} end.

with_priority(none) -> fun(Props) -> Props end;
with_priority({some, Priority}) -> fun(Props) -> Props#'P_basic'{priority = Priority} end.

%% AMQP carries per-message expiration as a shortstr of milliseconds.
with_expiration(none) -> fun(Props) -> Props end;
with_expiration({some, Millis}) -> fun(Props) -> Props#'P_basic'{expiration = integer_to_binary(Millis)} end.

%% delivery_mode 2 = persistent (survives a broker restart on a durable queue);
%% leaving it unset means transient.
with_persistence(false) -> fun(Props) -> Props end;
with_persistence(true) -> fun(Props) -> Props#'P_basic'{delivery_mode = 2} end.

%% Blocking-ish poll over basic.get; a primitive kept for one-off fetches.
get(Channel, Queue) ->
    get(Channel, Queue, 20).

get(_Channel, _Queue, 0) ->
    {error, <<"no message available">>};
get(Channel, Queue, Retries) ->
    case amqp_channel:call(Channel, #'basic.get'{queue = Queue, no_ack = true}) of
        {#'basic.get_ok'{}, #amqp_msg{payload = Payload}} ->
            {ok, Payload};
        #'basic.get_empty'{} ->
            timer:sleep(100),
            get(Channel, Queue, Retries - 1)
    end.

%% Register `Pid` (a Gleam actor) as the consumer for `Queue`. Deliveries then
%% arrive in that process's mailbox as raw AMQP records, decoded by decode_event/1.
subscribe(Channel, Queue, Pid) ->
    Consume = #'basic.consume'{queue = Queue},
    try amqp_channel:subscribe(Channel, Consume, Pid) of
        #'basic.consume_ok'{} -> {ok, nil};
        Other -> {error, fmt(Other)}
    catch
        Class:Reason -> {error, fmt({Class, Reason})}
    end.

%% Set channel prefetch (QoS): how many unacked messages the broker will hand
%% out before waiting for acks.
set_qos(Channel, PrefetchCount, PrefetchSize, Global) ->
    Qos = #'basic.qos'{
        prefetch_count = PrefetchCount,
        prefetch_size = PrefetchSize,
        global = Global
    },
    try amqp_channel:call(Channel, Qos) of
        #'basic.qos_ok'{} -> {ok, nil};
        Other -> {error, fmt(Other)}
    catch
        Class:Reason -> {error, fmt({Class, Reason})}
    end.

%% A connection is represented as a pid; expose it so the consumer can monitor it.
connection_pid(Connection) ->
    Connection.

is_connection_open(Connection) ->
    is_process_alive(Connection).

%% Active health probe: declare a throwaway exclusive, auto-delete, server-named
%% queue. This round-trips to the broker, proving the channel is responsive (not
%% merely that the process is alive). The queue vanishes with the channel.
health_check(Channel) ->
    Declare = #'queue.declare'{queue = <<>>, exclusive = true, auto_delete = true},
    try amqp_channel:call(Channel, Declare) of
        #'queue.declare_ok'{} -> {ok, nil};
        Other -> {error, fmt(Other)}
    catch
        Class:Reason -> {error, fmt({Class, Reason})}
    end.

%% Settle a delivery according to a Gleam `Confirmation` (passed as an atom).
settle(Channel, Tag, ack) ->
    amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = Tag}),
    nil;
settle(Channel, Tag, reject) ->
    amqp_channel:cast(Channel, #'basic.reject'{delivery_tag = Tag, requeue = false}),
    nil;
settle(Channel, Tag, retry) ->
    %% Placeholder: requeue for immediate redelivery. A real retry policy
    %% (delay queues, attempt counting) comes later.
    amqp_channel:cast(Channel, #'basic.nack'{delivery_tag = Tag, requeue = true}),
    nil;
settle(Channel, Tag, dead_letter) ->
    %% Reject without requeue -> routed to the queue's DLX, if configured.
    amqp_channel:cast(Channel, #'basic.reject'{delivery_tag = Tag, requeue = false}),
    nil.

%% Convert a raw AMQP mailbox message into a Gleam `Event`:
%%   {delivery, Tag, Payload, RoutingKey, Headers} | cancelled | ignored
decode_event(
    {#'basic.deliver'{delivery_tag = Tag, routing_key = RoutingKey},
     #amqp_msg{payload = Payload, props = Props}}
) ->
    {delivery, Tag, Payload, RoutingKey, extract_headers(Props)};
decode_event(#'basic.cancel'{}) ->
    cancelled;
%% A monitored connection going down arrives as a standard `DOWN` message.
decode_event({'DOWN', _Ref, process, _Pid, _Reason}) ->
    connection_down;
decode_event(_Other) ->
    ignored.

%% Extract string-valued headers as a list of {Key, Value} binaries.
extract_headers(#'P_basic'{headers = undefined}) ->
    [];
extract_headers(#'P_basic'{headers = Headers}) when is_list(Headers) ->
    lists:filtermap(
        fun
            ({Key, longstr, Value}) -> {true, {Key, Value}};
            ({Key, binary, Value}) -> {true, {Key, Value}};
            (_) -> false
        end,
        Headers
    );
extract_headers(_) ->
    [].

%% Emit a warning through the standard OTP logger.
log_warning(Message) ->
    logger:warning(Message),
    nil.

%% Current time as an RFC 3339 / ISO 8601 UTC string.
now_timestamp() ->
    list_to_binary(calendar:system_time_to_rfc3339(erlang:system_time(second), [{offset, "Z"}])).

close_channel(Channel) ->
    try amqp_channel:close(Channel) catch _:_ -> ok end,
    nil.

close_connection(Connection) ->
    try amqp_connection:close(Connection) catch _:_ -> ok end,
    nil.

fmt(Term) ->
    list_to_binary(io_lib:format("~p", [Term])).
