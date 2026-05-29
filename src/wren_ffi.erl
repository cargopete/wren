-module(wren_ffi).
-include_lib("amqp_client/include/amqp_client.hrl").
-export([spike/0]).

%% Minimal end-to-end spike: connect, declare a queue, publish a message,
%% then poll basic.get until it comes back. Returns {ok, Payload} | {error, Reason},
%% which Gleam reads directly as Result(String, String).
spike() ->
    Params = #amqp_params_network{
        host = "localhost",
        port = 5672,
        username = <<"wren">>,
        password = <<"wren">>
    },
    case amqp_connection:start(Params) of
        {ok, Connection} ->
            try
                {ok, Channel} = amqp_connection:open_channel(Connection),
                Queue = <<"wren_spike">>,
                #'queue.declare_ok'{} =
                    amqp_channel:call(Channel, #'queue.declare'{queue = Queue, durable = true}),
                Payload = <<"hello from wren"/utf8>>,
                Publish = #'basic.publish'{exchange = <<"">>, routing_key = Queue},
                ok = amqp_channel:cast(Channel, Publish, #amqp_msg{payload = Payload}),
                Result = get_message(Channel, Queue, 20),
                amqp_channel:close(Channel),
                amqp_connection:close(Connection),
                Result
            catch
                Class:Reason ->
                    catch amqp_connection:close(Connection),
                    {error, list_to_binary(io_lib:format("~p:~p", [Class, Reason]))}
            end;
        {error, Reason} ->
            {error, list_to_binary(io_lib:format("connect failed: ~p", [Reason]))}
    end.

get_message(_Channel, _Queue, 0) ->
    {error, <<"no message after retries">>};
get_message(Channel, Queue, Retries) ->
    case amqp_channel:call(Channel, #'basic.get'{queue = Queue, no_ack = true}) of
        {#'basic.get_ok'{}, #amqp_msg{payload = Payload}} ->
            {ok, Payload};
        #'basic.get_empty'{} ->
            timer:sleep(100),
            get_message(Channel, Queue, Retries - 1)
    end.
