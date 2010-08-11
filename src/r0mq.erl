-module(r0mq).

%% Just the application; responsible for invoking the supervisor, on
%% start, and validating the configuration.

%% Application callbacks
-export([start/2, stop/1]).

%% -- Callbacks --

start(Type, Args) ->
    %% TODO: Validate the configuration and pass it along.
    Specs = case application:get_env(services) of
                {ok, Services} -> Services;
                undefined      -> []
            end,
    r0mq_sup:start_link(Specs).

stop(State) ->
    ok.
