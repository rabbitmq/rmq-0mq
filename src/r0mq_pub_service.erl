-module(r0mq_pub_service).

%% A pub service.
%%
%% See http://wiki.github.com/rabbitmq/rmq-0mq/pubsub

%% Callbacks
-export([init/3, create_socket/0,
        start_listening/3]).

-include_lib("amqp_client/include/amqp_client.hrl").

-record(state, {queue}).

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
            {ok, #state{queue = Queue}}
    end.

start_listening(Channel, Sock, State = #state{queue = Queue}) ->
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
                     loop(Channel, Sock, State)
             end),
    {ok, State}.

loop(Channel, Sock, State) ->
    receive
        {#'basic.deliver'{delivery_tag = Tag}, Msg} ->
            {ok, State1} = send_message(Msg, Sock, State),
            amqp_channel:cast(Channel,
                              #'basic.ack'{delivery_tag = Tag, multiple = false}),
            loop(Channel, Sock, State1)
    end.

send_message(#amqp_msg{payload = Payload}, Sock, State) ->
    case zmq:send(Sock, Payload) of
        ok -> {ok, State}
    end.
