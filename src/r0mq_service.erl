-module(r0mq_service).

%% A gen_server that manages a 0MQ socket and an AMQP connection,
%% and responds to incoming messages on each.

-behaviour(gen_server).

-record(state, {placeholder}).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

%% Procedures for controlling a listener
-export([connect/2, bind/2]).

%% -- Interface --

start_link(ServiceArgs) ->
    %% TODO: options?
    gen_server:start_link(?MODULE, [ServiceArgs], []).

%% Connect the listener to an address
connect(Listener, Address) ->
    gen_server:cast(Listener, {connect, Address}).

%% Bind the listener to an interface
bind(Listener, Address) ->
    gen_server:cast(Listener, {bind, Address}).

%% -- Callbacks --

%% TODO everything ..

init([ServiceArgs]) ->
    {ok, #state{}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(_Message, State) ->
    {noreply, State}.

terminate(_, _) ->
    ok.

code_change(_, State, _) ->
    State.
