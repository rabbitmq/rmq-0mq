-module(r0mq_push).

%% A pipeline push (downstream) service.
%%
%% See http://wiki.github.com/rabbitmq/rmq-0mq/pipeline

%% Callbacks
%-export([init/1, terminate/2, code_change/3,
%         handle_call/3, handle_cast/2, handle_info/2]).


%% Callbacks
-export([init/3, create_socket/0,
        start_listening/2, zmq_message/3, amqp_message/5]).

-include_lib("amqp_client/include/amqp_client.hrl").

-record(state, {queue}).

%% -- Callbacks --

create_socket() ->
    {ok, Out} = zmq:socket(downstream, [{active, true}]),
    Out.

init(Options, Connection, ConsumeChannel) ->
    case r0mq_util:get_option(name, Options) of
        missing ->
            throw({?MODULE, no_queue_given});
        QName when is_binary(QName) ->
            r0mq_util:ensure_shared_queue(
              QName, <<"">>, queue, Connection),
            {ok, #state{queue = QName}}
    end.

start_listening(_Channel, State) ->
    {ok, State}.

zmq_message(Data, Channel, State = #state{queue = Queue }) ->
    Msg = #amqp_msg{payload = Data},
    Pub = #'basic.publish'{ exchange = <<"">>,
                            routing_key = Queue },
    amqp_channel:cast(Channel, Pub, Msg),
    {ok, State}.

amqp_message(_Env, Msg, _Sock, _Channel, State) ->
    throw({?MODULE, unexpected_amqp_message, Msg}).
