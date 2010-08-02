-module(r0mq_sup).

-behaviour(supervisor).

%% Supervisor callback
-export([init/1]).

%% Interface
-export([start_link/1]).

%% -- Interface --

start_link(ServerDefs) ->
    supervisor:start_link(?MODULE, [ServerDefs]).

%% -- Callback --

init([ServerDefs]) ->
    {ok, {{one_for_one, 10, 10, []}}}. % TODO none for now.
