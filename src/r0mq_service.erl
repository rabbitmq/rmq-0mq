-module(r0mq_service).

%% A gen_server that manages a 0MQ socket and an AMQP connection,
%% and responds to incoming messages on each.

-behaviour(gen_server).

-include_lib("amqp_client/include/amqp_client.hrl").

-record(state, {connection,
                channel,
                service_module,
                service_params,
                sock}).

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

init([{Module, SockSpec, Options}]) ->
    {ok, Connection} = amqp_connection:start(#amqp_params_direct{}),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    {ok, ServiceParams} = Module:init(Options, Connection, Channel),
    Sock = create_socket(Module, create_socket, SockSpec),
    gen_server:cast(self(), start_listening),
    rabbit_log:info(
      "0MQ ~p service starting~n" ++
      "  listening on: ~p~n" ++
      "  options:~n" ++
      "    ~p",
      [Module, SockSpec, Options]),
    {ok, #state{ connection = Connection,
                 channel = Channel,
                 service_module = Module,
                 service_params = ServiceParams,
                 sock = Sock}}.

%% -- Callbacks --

%% FIXME throw an error for unexpected call and info
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(start_listening, State = #state { service_params = Params,
                                              service_module = Module,
                                              sock = Sock,
                                              channel = Channel }) ->
    {ok, Params1} = Module:start_listening(Channel, Sock, Params),
    {noreply, State#state { service_params = Params1 } }.

handle_info(_Msg, State) ->
    {noreply, State}.

%% TODO termination protocol for service module
terminate(_Reason,
          #state{ connection = Connection,
                  channel = Channel,
                  sock = Sock }) ->
    amqp_channel:close(Channel),
    amqp_connection:close(Connection),
    close_socket(Sock),
    ok.

code_change(_, State, _) ->
    State.

%% -- Internal --

create_socket(Module, Function, Specs) ->
    Sock = Module:Function(),
    bindings_and_connections(Sock, Specs),
    Sock.

bindings_and_connections(Sock, Specs) ->
    lists:foreach(fun (Spec) ->
                          ok = bind_or_connect(Sock, Spec)
                  end, Specs).

bind_or_connect({_, FD}, {bind, Address}) ->
    zmq:bind(FD, Address);
bind_or_connect({_, FD}, {connect, Address}) ->
    zmq:connect(FD, Address).

close_socket({_, FD}) ->
    zmq:close(FD).
