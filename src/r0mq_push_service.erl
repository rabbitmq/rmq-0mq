-module(r0mq_push_service).

%% A pipeline push (downstream) service.
%%
%% See http://wiki.github.com/rabbitmq/rmq-0mq/pipeline

%% Callbacks
%-export([init/1, terminate/2, code_change/3,
%         handle_call/3, handle_cast/2, handle_info/2]).


%% Callbacks
-export([init/3, create_socket/0, start_listening/3]).

-include_lib("amqp_client/include/amqp_client.hrl").

-record(state, {queue}).

%% -- Callbacks --

create_socket() ->
    {ok, In} = zmq:socket(downstream, [{active, false}]),
    In.

init(Options, Connection, ConsumeChannel) ->
    case r0mq_util:get_option(name, Options) of
        missing ->
            throw({?MODULE, no_queue_given});
        QName when is_binary(QName) ->
            r0mq_util:ensure_shared_queue(
              QName, <<"">>, queue, Connection),
            {ok, #state{queue = QName}}
    end.

start_listening(Channel, Sock, State = #state{queue = Queue}) ->
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
                     loop(Channel, Sock, State)
             end),
    {ok, State}.

loop(Channel, Sock, State) ->
    receive
        {#'basic.deliver'{delivery_tag = Tag}, Msg} ->
            {ok, State1} = send_message(Msg, Sock, State),
            amqp_channel:cast(Channel,
                              #'basic.ack'{delivery_tag = Tag, multiple = false}),
            loop(Channel, Sock, State1)
    end.

send_message(#amqp_msg{ payload = Payload }, Sock, State) ->
    case zmq:send(Sock, Payload) of
        ok -> {ok, State}
    end.
