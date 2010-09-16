-module(r0mq_sub_service).

%% A sub service.
%%
%% See http://wiki.github.com/rabbitmq/rmq-0mq/pubsub

%% Callbacks
-export([init/3, create_socket/0,
        start_listening/2, zmq_message/3, amqp_message/5]).

-include_lib("amqp_client/include/amqp_client.hrl").

-record(state, {exchange}).

%% -- Callbacks --

create_socket() ->
    {ok, In} = zmq:socket(sub, [{active, true}, {subscribe, <<"">>}]),
    In.

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
            {ok, #state{exchange = Exchange}}
    end.

start_listening(_Channel, State) ->
    {ok, State}.

zmq_message(Data, Channel, State = #state{ exchange = Exchange }) ->
    Msg = #amqp_msg{payload = Data},
    Pub = #'basic.publish'{ exchange = Exchange },
    amqp_channel:cast(Channel, Pub, Msg),
    {ok, State}.

amqp_message(_Env, Msg, _Sock, _Channel, State) ->
    throw({?MODULE, unexpected_amqp_message, Msg}).
