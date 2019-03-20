-module(r0mq_push_service).

%% A pipeline push (downstream) service.
%%
%% See https://wiki.github.com/rabbitmq/rmq-0mq/pipeline

%% Callbacks
-export([init/3, create_socket/0, start_listening/3]).

-include_lib("amqp_client/include/amqp_client.hrl").

-record(params, {queue}).

%% -- Callbacks --

create_socket() ->
    {ok, In} = zmq:socket(downstream, [{active, false}]),
    In.

init(Options, Connection, _ConsumeChannel) ->
    case r0mq_util:get_option(name, Options) of
        missing ->
            throw({?MODULE, no_queue_given});
        QName when is_binary(QName) ->
            r0mq_util:ensure_shared_queue(
              QName, <<"">>, queue, Connection),
            {ok, #params{queue = QName}}
    end.

start_listening(Channel, Sock, Params = #params{queue = Queue}) ->
    %% We use acking and basic.qos as a HWM for our process mailbox, so we don't
    %% pile things up in the (unbounded) process mailbox.
    amqp_channel:call(Channel, #'basic.qos'{prefetch_count = 100}),
    Consume = #'basic.consume'{ queue = Queue,
                                no_ack = false,
                                exclusive = false },
    _Pid = spawn_link(
             fun () ->
                     amqp_channel:subscribe(Channel, Consume, self()),
                     receive
                         #'basic.consume_ok'{} -> ok
                     end,
                     loop(Channel, Sock, Params)
             end),
    {ok, Params}.

loop(Channel, Sock, Params) ->
    receive
        {#'basic.deliver'{delivery_tag = Tag}, #amqp_msg{ payload = Msg} } ->
            ok = zmq:send(Sock, Msg),
            amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = Tag,
                                                    multiple = false})
    end,
    loop(Channel, Sock, Params).
