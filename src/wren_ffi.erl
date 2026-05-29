-module(wren_ffi).
-include_lib("amqp_client/include/amqp_client.hrl").
-export([
    connect/4,
    open_channel/1,
    declare_queue/2,
    publish/4,
    get/2,
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

%% Blocking-ish poll over basic.get; a placeholder until the real
%% subscription-based consumer lands.
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

close_channel(Channel) ->
    try amqp_channel:close(Channel) catch _:_ -> ok end,
    nil.

close_connection(Connection) ->
    try amqp_connection:close(Connection) catch _:_ -> ok end,
    nil.

fmt(Term) ->
    list_to_binary(io_lib:format("~p", [Term])).
