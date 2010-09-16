-module(r0mq_pub_service).

%% A pub service.
%%
%% See http://wiki.github.com/rabbitmq/rmq-0mq/pubsub

%% Callbacks
-export([init/3, create_socket/0,
        start_listening/2, zmq_message/3, amqp_message/5]).

-include_lib("amqp_client/include/amqp_client.hrl").

-record(state, {queue}).

%% -- Callbacks --

create_socket() ->
    {ok, Out} = zmq:socket(pub, [{active, true}]),
    Out.

init(Options, Connection, ConsumeChannel) ->
    Exchange = case r0mq_util:get_option(name, Options) of
                   missing -> throw({?MODULE, no_name_supplied, Options});
                   Name    -> Name
               end,
    case r0mq_util:ensure_exchange(Exchange, <<"fanout">>, Connection) of
        {error, _, _Spec} ->
            %io:format("Error declaring exchange ~p~n", [Spec]),
            throw({cannot_declare_exchange, Exchange});
        {ok, Exchange} ->
            %io:format("Exchange OK ~p~n", [Exchange]),
            {ok, Queue} = r0mq_util:create_bind_private_queue(
                            Exchange, <<"">>, ConsumeChannel),
            %io:format("Using queue ~p~n", [Queue]),
            {ok, #state{queue = Queue}}
    end.

start_listening(Channel, State = #state{queue = Queue}) ->
    Consume = #'basic.consume'{ queue = Queue,
                                no_ack = true,
                                exclusive = true },
    amqp_channel:subscribe(Channel, Consume, self()),
    {ok, State}.

zmq_message(Data, _Channel, _State) ->
    throw({?MODULE, unexpected_zmq_message, Data}).

amqp_message(_Env, #amqp_msg{ payload = Payload }, Sock, _Channel, State) ->
    zmq:send(Sock, Payload),
    {ok, State}.
