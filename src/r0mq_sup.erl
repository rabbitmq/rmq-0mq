-module(r0mq_sup).

-behaviour(supervisor).

%% Supervisor callback
-export([init/1]).

%% Interface
-export([start_link/1]).

%% -- Interface --

start_link(ServerDefs) ->
    {ok, Pid} = zmq:start_link(), % FIXME put in hierarchy
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

child_spec({Rendezvous, Type, Address}) ->
    child_spec({Rendezvous, Type, Address, []});
child_spec(S = {Rendezvous, Type, Address, Options}) ->
    case module_for_type(Type) of
        no_such_type ->
            {error, {no_such_type, Type}, S};
        Module ->
            case full_address(Address) of
                {error, invalid_addresses, Invalids} ->
                    {error, {invalid_addresses, Invalids}, S};
                Address1 ->
                    {ok,
                     {erlang:md5(term_to_binary(S)),
                      {r0mq_service, start_link,
                       [{Module, Address1,
                         [ {name, Rendezvous} | Options]}]},
                      transient,10,worker,[r0mq_service]}}
            end
    end.

module_for_type(pub)  -> r0mq_pub_service;
module_for_type(sub)  -> r0mq_sub_service;
module_for_type(req)  -> r0mq_req_service;
module_for_type(rep)  -> r0mq_rep_service;
module_for_type(push) -> r0mq_push_service;
module_for_type(pull) -> r0mq_pull_service;
module_for_type(_)    -> no_such_type.

full_address(Address) when is_list(Address) ->
    case io_lib:char_list(Address) of
        true ->
            [{bind, Address}];
        _    ->
            lists:foldl(fun (Addr, {error, Acc}) ->
                                case full_address1(Addr) of
                                    {error, invalid_address, _} ->
                                        {error, [Addr | Acc]};
                                    _ ->
                                        {error, Acc}
                                end;
                            (Addr, {ok, Acc}) ->
                                case full_address1(Addr) of
                                    {error, invalid_address, _} ->
                                        {error, [Addr]};
                                    Full ->
                                        {ok, [Full, Acc]}
                                end
                        end, Address)
    end;
full_address(Address) ->
    full_address([Address]).

full_address1(A = {bind, Addr}) when is_list(Addr) ->
    A;
full_address1(A = {connect, Addr}) when is_list(Addr) ->
    A;
full_address1(Addr) when is_list(Addr) ->
    {bind, Addr};
full_address1(Addr) ->
    {error, invalid_address, Addr}.
