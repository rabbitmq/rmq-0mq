-module(r0mq_pull).

%% A pull (upstream) pipeline service.
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
    {ok, In} = zmq:socket(downstream, [{active, true}]),
    In.

init(Options, Connection, ConsumeChannel) ->
    case r0mq_util:get_option(name, Options) of
        missing ->
            throw({?MODULE, no_queue_given});
        QName when is_binary(QName) ->
            r0mq_util:ensure_shared_queue(
              QName, <<"">>, queue, Connection),
            {ok, #state{queue = QName }}
    end.

start_listening(Channel, State = #state{queue = Queue}) ->
    Consume = #'basic.consume'{ queue = Queue,
                                no_ack = true,
                                exclusive = true },
    amqp_channel:subscribe(Channel, Consume, self()),
    {ok, State}.

zmq_message(Data, Channel, State) ->
    throw({?MODULE, unexpected_zmq_message, Data}).

amqp_message(_Env, #amqp_msg{ payload = Payload }, Sock, _Channel, State) ->
    zmq:send(Sock, Payload),
    {ok, State}.
