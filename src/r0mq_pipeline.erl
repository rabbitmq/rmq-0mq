-module(r0mq_pipeline).

%% A gen server implementing a pipeline service. This maintains two
%% 0MQ sockets, one for incoming (upstream) connections and one for
%% outgoing downstream connections.
%%
%% See http://wiki.github.com/rabbitmq/rmq-0mq/pipeline

%-behaviour(gen_server).

%% Callbacks
%-export([init/1, terminate/2, code_change/3,
%         handle_call/3, handle_cast/2, handle_info/2]).


%% Callbacks
-export([init/3, create_in_socket/0, create_out_socket/0,
        start_listening/2, zmq_message/3, amqp_message/5]).

-include_lib("amqp_client/include/amqp_client.hrl").

-record(state, {exchange, queue}).

%% -- Callbacks --

create_in_socket() ->
    {ok, In} = zmq:socket(upstream, [{active, true}]),
    In.

create_out_socket() ->
    {ok, Out} = zmq:socket(downstream, [{active, true}]),
    Out.

init(Options, Connection, ConsumeChannel) ->
    Exchange = case r0mq_util:get_option(exchange, Options) of
                   missing -> r0mq_util:private_name();
                   Name    -> Name
               end,
    Queue = r0mq_util:get_option(queue, Options),
    %% We don't actually care about the type, but we supply one
    %% in case it gets created.
    r0mq_util:ensure_exchange(Exchange, <<"fanout">>, Connection),
    {ok, QueueName} =
        case Queue of
            missing ->
                r0mq_util:create_bind_private_queue(
                  Exchange, queue, ConsumeChannel);
            QName when is_binary(QName) ->
                r0mq_util:ensure_shared_queue(
                  QName, Exchange, queue, Connection)
        end,
    {ok, #state{ exchange = Exchange,
                 queue = QueueName }}.

start_listening(Channel, State = #state{queue = Queue}) ->
    Consume = #'basic.consume'{ queue = Queue,
                                no_ack = true,
                                exclusive = true },
    amqp_channel:subscribe(Channel, Consume, self()),
    {ok, State}.

zmq_message(Data, Channel, State = #state{ exchange = Exchange,
                                           queue = Queue }) ->
    Msg = #amqp_msg{payload = Data},
    Pub = #'basic.publish'{ exchange = Exchange,
                            routing_key = Queue },
    amqp_channel:cast(Channel, Pub, Msg),
    {ok, State}.

amqp_message(_Env, #amqp_msg{ payload = Payload }, Sock, _Channel, State) ->
    zmq:send(Sock, Payload),
    {ok, State}.
