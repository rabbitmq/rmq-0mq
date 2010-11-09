-module(r0mq_pull_service).

%% A pipeline pull (upstream) service.
%%
%% See http://wiki.github.com/rabbitmq/rmq-0mq/pipeline

%% Callbacks
-export([init/3, create_socket/0, start_listening/3]).

-include_lib("amqp_client/include/amqp_client.hrl").

-record(state, {queue}).

%% -- Callbacks --

create_socket() ->
    {ok, Out} = zmq:socket(upstream, [{active, false}]),
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

start_listening(Channel, Sock, State) ->
    _Pid = spawn_link(fun () ->
                              loop(Channel, Sock, State)
                      end),
    {ok, State}.

loop(Channel, Sock, State) ->
    {ok, Msg} = zmq:recv(Sock),
    {ok, State1} = publish_message(Msg, Channel, State),
    loop(Channel, Sock, State1).

publish_message(Data, Channel, State = #state{queue = Queue }) ->
    Msg = #amqp_msg{payload = Data},
    Pub = #'basic.publish'{ exchange = <<"">>,
                            routing_key = Queue },
    amqp_channel:cast(Channel, Pub, Msg),
    {ok, State}.
