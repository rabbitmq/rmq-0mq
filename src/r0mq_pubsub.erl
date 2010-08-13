-module(r0mq_pubsub).

%% A pubsub service.
%%
%% See http://wiki.github.com/rabbitmq/rmq-0mq/pubsub

%% Callbacks
-export([init/3, create_in_socket/0, create_out_socket/0,
        start_listening/2, zmq_message/3, amqp_message/5]).

-include_lib("amqp_client/include/amqp_client.hrl").

-record(state, {exchange, queue}).

%% -- Callbacks --

create_in_socket() ->
    {ok, In} = zmq:socket(sub, [{active, true}, {subscribe, <<"">>}]),
    In.

create_out_socket() ->
    {ok, Out} = zmq:socket(pub, [{active, true}]),
    Out.

init(Options, Connection, ConsumeChannel) ->
    Exchange = case r0mq_util:get_option(exchange, Options) of
                   missing -> r0mq_util:private_name();
                   Name    -> Name
               end,
    case r0mq_util:ensure_exchange(Exchange, <<"fanout">>, Connection) of
        {error, _, Spec} ->
            %io:format("Error declaring exchange ~p~n", [Spec]),
            throw({cannot_declare_exchange, Exchange});
        ok ->
            %io:format("Exchange OK ~p~n", [Exchange]),
            Queue = r0mq_util:create_bind_private_queue(
                      Exchange, <<"">>, ConsumeChannel),
            %io:format("Using queue ~p~n", [Queue]),
            {ok, #state{exchange = Exchange,
                         queue = Queue }}
    end.

start_listening(Channel, State = #state{queue = Queue}) ->
    Consume = #'basic.consume'{ queue = Queue,
                                no_ack = true,
                                exclusive = true },
    amqp_channel:subscribe(Channel, Consume, self()),
    {ok, State}.

zmq_message(Data, Channel, State = #state{ exchange = Exchange }) ->
    Msg = #amqp_msg{payload = Data},
    Pub = #'basic.publish'{ exchange = Exchange },
    amqp_channel:cast(Channel, Pub, Msg),
    {ok, State}.

amqp_message(_Env, #amqp_msg{ payload = Payload }, Sock, _Channel, State) ->
    zmq:send(Sock, Payload),
    {ok, State}.
