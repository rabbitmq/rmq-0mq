-module(r0mq_util).

-export([get_option/2, private_name/0]).

-export([ensure_exchange/3,
         ensure_shared_queue/4,
         create_bind_private_queue/3]).

-include_lib("amqp_client/include/amqp_client.hrl").

%% There's no server-supplied names for exchanges, so we make our own
%% here.
private_name() ->
    rabbit_guid:gen().

%% We want either the exchange name (a binary) or
%% 'missing'.
get_option(Key, Options) ->
    case lists:keyfind(Key, 1, Options) of
        false              -> missing;
        {Key, OptionValue} -> OptionValue
    end.

%% Make sure an exchange is present.  We take a connection as a
%% parameter, because we'll use a throwaway channel.
%% For the minute, we presume that exchanges will be durable.
ensure_exchange(Name, Type, Conn) ->
    {ok, Channel} = amqp_connection:open_channel(Conn),
    ExchangeDecl = #'exchange.declare'{exchange = Name,
                                       type = Type,
                                       durable = true},
    Result = case amqp_channel:call(Channel, ExchangeDecl) of
                 #'exchange.declare_ok'{} ->
                     {ok, Name};
                 _ ->
                     {error, wrong_type, ExchangeDecl}
             end,
    amqp_channel:close(Channel),
    Result.

%% Make sure a queue is present. We use a sacrificial channel.
%% Presumes that the queue is durable.
ensure_shared_queue(Queue, Exchange, BindingKey, Conn) ->
    QueueDecl = #'queue.declare'{ queue = Queue,
                                  durable = true },
    {ok, Channel} = amqp_connection:open_channel(Conn),
    #'queue.declare_ok'{ queue = Queue } =
        amqp_channel:call(Channel, QueueDecl),
    bind_queue(Queue, Exchange, BindingKey, Channel),
    amqp_channel:close(Channel),
    {ok, Queue}.

create_bind_private_queue(Exchange, BindingKey, Channel) ->
    QueueDecl = #'queue.declare'{ exclusive = true },
    #'queue.declare_ok'{ queue = Queue } =
        amqp_channel:call(Channel, QueueDecl),
    bind_queue(Queue, Exchange, BindingKey, Channel),
    {ok, Queue}.

bind_queue(_, <<"">>, _, _) ->
    ok;
bind_queue(Queue, Exchange, BindingKey, Channel) ->
    Bind = #'queue.bind'{ exchange = Exchange,
                          queue = Queue,
                          routing_key = case BindingKey of
                                           queue -> Queue;
                                           Key   -> Key
                                        end},
    #'queue.bind_ok'{} = amqp_channel:call(Channel, Bind).
