-module(r0mq_pull_service).

%% A pipeline pull (upstream) service.
%%
%% See http://wiki.github.com/rabbitmq/rmq-0mq/pipeline

%% Callbacks
-export([init/3, create_socket/0, start_listening/3]).

-include_lib("amqp_client/include/amqp_client.hrl").

-record(params, {queue}).

%% -- Callbacks --

create_socket() ->
    {ok, Out} = zmq:socket(upstream, [{active, false}]),
    Out.

init(Options, Connection, _ConsumeChannel) ->
    case r0mq_util:get_option(name, Options) of
        missing ->
            throw({?MODULE, no_queue_given});
        QName when is_binary(QName) ->
            r0mq_util:ensure_shared_queue(
              QName, <<"">>, queue, Connection),
            {ok, #params{queue = QName}}
    end.

start_listening(Channel, Sock, Params) ->
    _Pid = spawn_link(fun () ->
                              loop(Channel, Sock, Params)
                      end),
    {ok, Params}.

loop(Channel, Sock, Params) ->
    {ok, Msg} = zmq:recv(Sock),
    ok = publish_message(Msg, Channel, Params),
    loop(Channel, Sock, Params).

publish_message(Data, Channel, #params{queue = Queue}) ->
    Msg = #amqp_msg{payload = Data},
    Pub = #'basic.publish'{ exchange = <<"">>,
                            routing_key = Queue },
    amqp_channel:cast(Channel, Pub, Msg),
    ok.
