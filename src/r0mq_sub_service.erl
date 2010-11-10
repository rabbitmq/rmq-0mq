-module(r0mq_sub_service).

%% A sub service.
%%
%% See http://wiki.github.com/rabbitmq/rmq-0mq/pubsub

%% Callbacks
-export([init/3, create_socket/0, start_listening/3]).

-include_lib("amqp_client/include/amqp_client.hrl").

-record(params, {exchange}).

%% -- Callbacks --

create_socket() ->
    {ok, In} = zmq:socket(sub, [{active, false}, {subscribe, <<"">>}]),
    In.

init(Options, Connection, ConsumeChannel) ->
    Exchange = case r0mq_util:get_option(name, Options) of
                   missing -> throw({?MODULE, no_name_supplied, Options});
                   Name    -> Name
               end,
    case r0mq_util:ensure_exchange(Exchange, <<"fanout">>, Connection) of
        {error, _, _Spec} ->
            throw({cannot_declare_exchange, Exchange});
        {ok, Exchange} ->
            {ok, #params{exchange = Exchange}}
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

publish_message(Data, Channel, Params = #params{exchange = Exchange }) ->
    Msg = #amqp_msg{payload = Data},
    Pub = #'basic.publish'{ exchange = Exchange },
    amqp_channel:cast(Channel, Pub, Msg),
    ok.
