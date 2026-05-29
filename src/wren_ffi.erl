-module(wren_ffi).
-include_lib("amqp_client/include/amqp_client.hrl").
-export([
    connect/4,
    open_channel/1,
    declare_queue/2,
    publish/4,
    get/2,
    subscribe/3,
    settle/3,
    decode_event/1,
    sleep/1,
    close_channel/1,
    close_connection/1
]).

%% Each function returns a value shaped for Gleam:
%%   {ok, X} | {error, Binary}  -> Result(X, String)
%% Opaque connection/channel pids are passed back and forth as-is.

connect(Host, Port, User, Pass) ->
    Params = #amqp_params_network{
        host = binary_to_list(Host),
        port = Port,
        username = User,
        password = Pass
    },
    case amqp_connection:start(Params) of
        {ok, Connection} -> {ok, Connection};
        {error, Reason} -> {error, fmt(Reason)}
    end.

open_channel(Connection) ->
    case amqp_connection:open_channel(Connection) of
        {ok, Channel} -> {ok, Channel};
        closing -> {error, <<"connection is closing">>};
        {error, Reason} -> {error, fmt(Reason)}
    end.

declare_queue(Channel, Queue) ->
    %% RabbitMQ 4.x forbids transient queues by default, so declare durable.
    Declare = #'queue.declare'{queue = Queue, durable = true},
    try amqp_channel:call(Channel, Declare) of
        #'queue.declare_ok'{} -> {ok, nil};
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

sleep(Milliseconds) ->
    timer:sleep(Milliseconds),
    nil.

close_channel(Channel) ->
    try amqp_channel:close(Channel) catch _:_ -> ok end,
    nil.

close_connection(Connection) ->
    try amqp_connection:close(Connection) catch _:_ -> ok end,
    nil.

fmt(Term) ->
    list_to_binary(io_lib:format("~p", [Term])).
