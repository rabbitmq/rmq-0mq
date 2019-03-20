-module(r0mq_pub_service).

%% A pub service.
%%
%% See https://wiki.github.com/rabbitmq/rmq-0mq/pubsub

%% Callbacks
-export([init/3, create_socket/0,
        start_listening/3]).

-include_lib("amqp_client/include/amqp_client.hrl").

-record(params, {queue}).

%% -- Callbacks --

create_socket() ->
    {ok, Out} = zmq:socket(pub, [{active, false}]),
    Out.

init(Options, Connection, ConsumeChannel) ->
    Exchange = case r0mq_util:get_option(name, Options) of
                   missing -> throw({?MODULE, no_name_supplied, Options});
                   Name    -> Name
               end,
    case r0mq_util:ensure_exchange(Exchange, <<"fanout">>, Connection) of
        {error, _, _Spec} ->
            throw({cannot_declare_exchange, Exchange});
        {ok, Exchange} ->
            {ok, Queue} = r0mq_util:create_bind_private_queue(
                            Exchange, <<"">>, ConsumeChannel),
            {ok, #params{queue = Queue}}
    end.

start_listening(Channel, Sock, Params = #params{queue = Queue}) ->
    %% We use prefetch here for flow control
    amqp_channel:call(Channel, #'basic.qos'{prefetch_count = 100}),
    Consume = #'basic.consume'{ queue = Queue,
                                no_ack = false,
                                exclusive = true },
    _Pid = spawn_link(
             fun() ->
                     amqp_channel:subscribe(Channel, Consume, self()),
                     receive
                         #'basic.consume_ok'{} -> ok
                     end,
                     loop(Channel, Sock, Params)
             end),
    {ok, Params}.

loop(Channel, Sock, Params) ->
    receive
        {#'basic.deliver'{delivery_tag = Tag},
         #amqp_msg{ payload = Payload}} ->
            ok = zmq:send(Sock, Payload),
            amqp_channel:cast(Channel,
                              #'basic.ack'{delivery_tag = Tag,
                                           multiple = false})
    end,
    loop(Channel, Sock, Params).
