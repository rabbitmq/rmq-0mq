-module(r0mq_sup).

-behaviour(supervisor).

%% Supervisor callback
-export([init/1]).

%% Interface
-export([start_link/1]).

%% -- Interface --

start_link(ServerDefs) ->
    zmq:start_link(), % FIXME put in hierarchy
    case child_specs(ServerDefs) of
        {ok, ChildSpecs} ->
            supervisor:start_link(?MODULE, [ChildSpecs]);
        {errors, Errors} ->
            error_logger:error_msg("Errors starting 0MQ services: ~p", Errors),
            exit({invalid_config, ServerDefs})
    end.

%% -- Callback --

init([ChildSpecs]) ->
    {ok, {{one_for_one, 10, 10}, ChildSpecs}}.

%% -- Internal --

child_specs([]) ->
    {ok, []};
child_specs(Services) ->
    case lists:partition(fun ({ok, Spec}) -> true;
                             ({error, Error}) -> false
                         end,
                         lists:map(fun child_spec/1, Services)) of
        {OKs, []} ->
            {ok, lists:map(fun (T) -> element(2, T) end, OKs)};
        {_, Errors} ->
            {errors, lists:map(fun (T) -> element(2, T) end, Errors)}
    end.

child_spec(S = {pubsub, Ins, Outs, Options}) ->
    {ok,
     {erlang:md5(term_to_binary(S)),{r0mq_service, start_link,
                                     [{r0mq_pubsub, Ins, Outs, Options}]},
      transient,10,worker,[r0mq_service]}};
child_spec(S = {pipeline, Ins, Outs, Options}) ->
    {ok,
     {erlang:md5(term_to_binary(S)),{r0mq_service, start_link,
                                     [{r0mq_pipeline, Ins, Outs, Options}]},
      transient,10,worker,[r0mq_service]}};
child_spec(S) ->
    {error, {invalid_service, S}}.
