-module(r0mq).

-behaviour(application).
 
%% Just the application; responsible for invoking the supervisor, on
%% start.

%% Application callbacks
-export([start/2, stop/1]).

%% -- Callbacks --

start(normal, []) ->
    %% TODO: Validate the configuration and pass it along.
    Specs = case application:get_env(services) of
                {ok, Services} -> Services;
                undefined      -> []
            end,
    r0mq_sup:start_link(Specs).

stop(_State) ->
    ok.
