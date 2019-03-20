-module(r0mq_rep_service).

%% A reply service.
%%
%% See https://wiki.github.com/rabbitmq/rmq-0mq/reqrep

%% Callbacks
-export([init/3, create_socket/0, start_listening/3]).

-include_lib("amqp_client/include/amqp_client.hrl").

-record(params, {req_queue, % used as routing key for requests
                 req_exchange, % exchange to which to send requests
                 rep_queue % (private) queue from which to collect replies
                }).

%% -- Callbacks --

%% We use xrep, because we want to be asynchronous, and to
%% strip the routing information from the messages (and apply it when
%% sending back).  This means we have to recv and send multipart
%% messages.  For receiving, we have a little state machine.  For
%% sending, we just do it all in one go.

create_socket() ->
    {ok, In} = zmq:socket(xrep, [{active, false}]),
    In.

init(Options, _Connection, ConsumeChannel) ->
    %% We MUST have a request queue name to use as a routing key;
    %% there's no point in constructing a private queue, because
    %% no-one will be listening to it.
    ReqQueueName = case r0mq_util:get_option(name, Options) of
                       missing -> throw({?MODULE,
                                         no_request_queue_supplied,
                                         Options});
                       Name    -> Name
                   end,
    %% Just use default for now; this is the most common usage
    ReqExchange = <<"">>,
    RepExchange = <<"">>,
    {ok, RepQueueName} = r0mq_util:create_bind_private_queue(
                           RepExchange, queue, ConsumeChannel),
    {ok, #params{ req_queue = ReqQueueName,
                  req_exchange = ReqExchange,
                  rep_queue = RepQueueName}}.

start_listening(Channel, Sock, Params = #params{rep_queue = RepQueue}) ->
    ConsumeRep = #'basic.consume'{ queue = RepQueue,
                                   no_ack = false,
                                   exclusive = true },
    _Pid = spawn_link(fun() ->
                              #'basic.consume_ok'{} =
                                  amqp_channel:subscribe(Channel, ConsumeRep, self()),
                              receive
                                  #'basic.consume_ok'{} -> ok
                              end,
                              response_loop(Channel, Sock, Params)
                      end),
    _Pid2 = spawn_link(fun() ->
                               request_loop(Channel, Sock, Params, [], path)
                       end),
    {ok, Params}.

response_loop(Channel, Sock, Params) ->
    receive
        {#'basic.deliver'{ delivery_tag = Tag },
         #amqp_msg{ payload = Payload,
                    props = Props }} ->
            #'P_basic'{correlation_id = CorrelationId} = Props,
            Path = decode_path(CorrelationId),
            lists:foreach(fun (PathElement) ->
                                  zmq:send(Sock, PathElement, [sndmore])
                          end, Path),
            zmq:send(Sock, <<>>, [sndmore]),
            zmq:send(Sock, Payload),
            amqp_channel:cast(Channel, #'basic.ack'{ delivery_tag = Tag,
                                                     multiple = false })
    end,
    response_loop(Channel, Sock, Params).

request_loop(Channel, Sock, Params = #params{ req_queue = Queue,
                                              rep_queue = ReplyQueue,
                                              req_exchange = Exchange },
             Path, payload) ->
    {ok, Data} = zmq:recv(Sock),
    CorrelationId = encode_path(Path),
    Msg = #amqp_msg{payload = Data,
                    props = #'P_basic'{
                      reply_to = ReplyQueue,
                      correlation_id = CorrelationId}},
    Pub = #'basic.publish'{ exchange = Exchange,
                            routing_key = Queue },
    amqp_channel:cast(Channel, Pub, Msg),
    request_loop(Channel, Sock, Params, [], path);
request_loop(Channel, Sock, Params, Path, path) ->
    {ok, Msg} = zmq:recv(Sock),
    case Msg of
        <<>> ->
            request_loop(Channel, Sock, Params, Path, payload);
        PathElem ->
            request_loop(Channel, Sock, Params, [ PathElem | Path ], path)
    end.

%% FIXME only deal with one for the minute
encode_path([Id]) ->
    base64:encode(Id).

decode_path(undefined) ->
    [];
decode_path(CorrId) ->
    [base64:decode(CorrId)].
