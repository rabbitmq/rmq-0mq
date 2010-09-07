-module(r0mq_service).

%% A gen_server that manages a 0MQ socket and an AMQP connection,
%% and responds to incoming messages on each.

-behaviour(gen_server).

-include_lib("amqp_client/include/amqp_client.hrl").

-record(state, {connection,
                channel,
                service_module,
                service_state,
                in_sock,
                out_sock}).

%% interface
-export([start_link/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

%% Procedures for controlling a listener
%-export([connect/2, bind/2]).

%% -- Interface --

start_link(ServiceArgs) ->
    %% TODO: options?
    gen_server:start_link(?MODULE, [ServiceArgs], []).

%% Connect the listener to an address
%connect(Listener, Address) ->
%    gen_server:cast(Listener, {connect, Address}).

%% Bind the listener to an interface
%bind(Listener, Address) ->
%    gen_server:cast(Listener, {bind, Address}).

%% -- Callbacks --

init([{Module, InSpec, OutSpec, Options}]) ->
    Connection = amqp_connection:start_direct_link(),
    Channel = amqp_connection:open_channel(Connection),
    {ok, ServiceState} = Module:init(Options, Connection, Channel),
    InSock = create_socket(Module, create_in_socket, InSpec),
    OutSock = create_socket(Module, create_out_socket, OutSpec),
    gen_server:cast(self(), start_listening),
    {ok, #state{ connection = Connection,
                 channel = Channel,
                 service_module = Module,
                 service_state = ServiceState,
                 in_sock = InSock,
                 out_sock = OutSock }}.

%% -- Callbacks --

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(start_listening, State = #state { service_state = ServiceState,
                                              service_module = Module,
                                              channel = Channel }) ->
    {ok, ServiceState1} = Module:start_listening(Channel, ServiceState),
    {noreply, State#state { service_state = ServiceState1 } }.

%% This will get sent when a service subscribes us to a queue.
handle_info(#'basic.consume_ok'{}, State) ->
    io:format("(consume ok recvd)~n", []),
    {noreply, State};
handle_info({zmq, FD, Data}, State = #state{
                               service_module = Module,
                               service_state = ServiceState,
                               in_sock = InFD,
                               out_sock = OutFD,
                               channel = Channel}) ->
    InOrOut = case FD of
                  InFD  -> in;
                  OutFD -> out
              end,
    io:format("ZeroMQ message recvd: ~p ~p~n", [InOrOut, Data]),
    {ok, ServiceState1} = Module:zmq_message(Data, InOrOut, Channel, ServiceState),
    {noreply, State#state {service_state = ServiceState1}};

handle_info({Env = #'basic.deliver'{}, Msg},
            State = #state{ service_module = Module,
                            service_state = ServiceState,
                            in_sock = In,
                            out_sock = Out,
                            channel = Channel}) ->
    io:format("AMQP message recvd: ~p~n", [Msg]),
    {ok, ServiceState1} = Module:amqp_message(Env, Msg, In, Out, Channel, ServiceState),
    {noreply, State#state{service_state = ServiceState1}}.

%% TODO termination protocol for service module
terminate(_Reason,
          #state{ connection = Connection,
                  channel = Channel,
                  in_sock = In,
                  out_sock = Out }) ->
    amqp_channel:close(Channel),
    amqp_connection:close(Connection),
    close_socket(In),
    close_socket(Out),
    ok.

code_change(_, State, _) ->
    State.

%% -- Internal --

create_socket(_Module, _Function, []) ->
    no_socket;
create_socket(Module, Function, Specs) ->
    {_Port, S} = Module:Function(),
    bindings_and_connections(S, Specs),
    S.

bindings_and_connections(Sock, Specs) ->
    lists:foreach(fun (Spec) ->
                          ok = bind_or_connect(Sock, Spec)
                  end, Specs).

bind_or_connect(Sock, {bind, Address}) ->
    zmq:bind(Sock, Address);
bind_or_connect(Sock, {connect, Address}) ->
    zmq:connect(Sock, Address).

close_socket(no_socket) ->
    ok;
close_socket(Sock) ->
    zmq:close(Sock).
