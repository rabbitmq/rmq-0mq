-module(r0mq_pubsub).

%% A gen server implementing a pubsub service. This maintains two 0MQ
%% sockets, one for incoming sub connections and one for outgoing pub
%% connections.
%%
%% See http://wiki.github.com/rabbitmq/rmq-0mq/pubsub

-behaviour(gen_server).

%% Callbacks
-export([init/1, terminate/2, code_change/3,
         handle_call/3, handle_cast/2, handle_info/2]).


%% Interface
-export([start_link/1]).

-record(state, {connection,
                channel,
                exchange,
                queue,
                in_sock,
                out_sock}).

-include_lib("amqp_client/include/amqp_client.hrl").

%% -- Interface --

start_link(Spec) ->
    gen_server:start_link(?MODULE, [Spec], []).

%% -- Internal --

%% We want either the exchange name (a binary) or
%% 'missing'.
get_option(Key, Options) ->
    case lists:keyfind(Key, 1, Options) of
        false              -> missing; 
        {Key, OptionValue} -> OptionValue
    end.

%% There's no server-supplied names for exchanges, so we make our own
%% here.
private_name() ->
    rabbit_guid:guid().

%% Make sure an exchange is present.  We take a connection as a
%% parameter, because we'll use a throwaway channel.
%% For the minute, we presume that exchanges will be durable.
ensure_exchange(Name, Type, Conn) ->
    Channel = amqp_connection:open_channel(Conn),
    ExchangeDecl = #'exchange.declare'{exchange = Name,
                                       type = Type,
                                       durable = true},
    Result = case amqp_channel:call(Channel, ExchangeDecl) of
                 #'exchange.declare_ok'{} ->
                     {ok, Name};
                 _ ->
                     {error, wrong_type, ExchangeDecl}
             end,
    amqp_channel:close(Channel).

create_in_socket([]) ->
    no_socket;
create_in_socket(Specs) when is_list(Specs) ->
    {ok, In} = zmq:socket(sub, [{active, true}, {subscribe, <<"">>}]),
    bindings_and_connections(In, Specs),
    In.

create_out_socket([]) ->
    no_socket;
create_out_socket(Specs) when is_list(Specs) ->
    {ok, Out} = zmq:socket(pub, [{active, true}]),
    bindings_and_connections(Out, Specs),
    Out.

bindings_and_connections(Sock, Specs) ->
    lists:foreach(fun (Spec) ->
                          bind_or_connect(Sock, Spec)
                  end, Specs).

bind_or_connect(Sock, {bind, Address}) ->
    zmq:bind(Sock, Address);
bind_or_connect(Sock, {connect, Address}) ->
    zmq:connect(Sock, Address).

create_bind_private_queue(Exchange, BindingKey, Channel) ->
    QueueDecl = #'queue.declare'{ exclusive = true },
    #'queue.declare_ok'{ queue = Queue } =
        amqp_channel:call(Channel, QueueDecl),
    Bind = #'queue.bind'{ exchange = Exchange,
                          queue = Queue,
                          routing_key = BindingKey },
    #'queue.bind_ok'{} = amqp_channel:call(Channel, Bind),
    Queue.

%% -- Callbacks --

init([{pubsub, InSpec, OutSpec, Options}]) ->
    Exchange = case get_option(exchange, Options) of
                   missing -> private_name();
                   Name    -> Name
               end,
    Connection = amqp_connection:start_direct_link(),
    case ensure_exchange(Exchange, <<"fanout">>, Connection) of
        {error, _, Spec} ->
            io:format("Error declaring exchange ~p~n", [Spec]),
            throw({cannot_declare_exchange, Spec});
        ok ->
            io:format("Exchange OK ~p~n", [Exchange]),
            InSock = create_in_socket(InSpec),
            OutSock = create_out_socket(OutSpec),
            Channel = amqp_connection:open_channel(Connection),
            Queue = create_bind_private_queue(Exchange, <<"">>, Channel),
            io:format("Using queue ~p~n", [Queue]),
            gen_server:cast(self(), init),
            {ok, #state{ connection = Connection,
                         channel = Channel,
                         exchange = Exchange,
                         queue = Queue,
                         in_sock = InSock,
                         out_sock = OutSock }}
    end.

handle_cast(init, State = #state{ channel = Channel,
                                  queue = Queue }) ->
    Consume = #'basic.consume'{ queue = Queue,
                                no_ack = true,
                                exclusive = true }, 
    amqp_channel:subscribe(Channel, Consume, self()),
    {noreply, State}.

handle_call(Request, From, State) ->
    io:format("Unexpected request ~n", [Request]),
    {reply, ok, State}.

handle_info(#'basic.consume_ok'{}, State) ->
    io:format("(consume ok recvd)~n", []),
    {noreply, State};
handle_info({zmq, _FD, Data}, State = #state{ channel = Channel,
                                             exchange = Exchange }) ->
    io:format("ZeroMQ message recvd: ~p~n", [Data]),
    Msg = #amqp_msg{payload = Data},
    Pub = #'basic.publish'{ exchange = Exchange }, 
    amqp_channel:cast(Channel, Pub, Msg),
    {noreply, State};
handle_info({#'basic.deliver'{}, #amqp_msg{ payload = Payload }},
            State = #state{ out_sock = OutSock }) ->
    io:format("AMQP message recvd: ~p~n", [Payload]),
    zmq:send(OutSock, Payload),
    {noreply, State}.

terminate(_Reason,
          State = #state{ connection = Connection,
                          channel = Channel,
                          in_sock = In,
                          out_sock = Out }) ->
    amqp_channel:close(Channel),
    amqp_connection:close(Connection),
    zmq:close(In),
    zmq:close(Out),
    ok.

code_change(_Old, State, _Extra) ->
    State.
