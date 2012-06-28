-module(r0mq_req_service).

%% A request service.
%%
%% See http://wiki.github.com/rabbitmq/rmq-0mq/reqrep

%% Callbacks
-export([init/3, create_socket/0, start_listening/3]).

-include_lib("amqp_client/include/amqp_client.hrl").

-record(params, {req_exchange, % (probably default) exchange through which we get requests
                 req_queue, % (probably shared) queue from which we retrieve requests
                 rep_exchange % (probably default) exchange to send replies
               }).

%% -- Callbacks --

%% We use xreq, because we want to be asynchronous, and to
%% strip the routing information from the messages (and apply it when
%% sending back).  This means we have to recv and send multipart
%% messages.  For receiving, we have a little state machine.  For
%% sending, we just do it all in one go.

create_socket() ->
    {ok, Out} = zmq:socket(xreq, [{active, false}]),
    Out.

init(Options, Connection, _ConsumeChannel) ->
    %% We MUST have a request queue name;
    %% there's no point in constructing a private queue, because
    %% no-one will be sending to it.
    ReqQueueName = case r0mq_util:get_option(name, Options) of
                       missing -> throw({?MODULE,
                                         no_request_queue_supplied,
                                         Options});
                       Name    -> Name
                   end,
    ReqExchange = <<"">>,
    RepExchange = <<"">>,
    r0mq_util:ensure_shared_queue(ReqQueueName, ReqExchange, queue, Connection),
    {ok, #params{ req_exchange = ReqExchange,
                  rep_exchange = RepExchange,
                  req_queue = ReqQueueName}}.

start_listening(Channel, Sock, Params = #params{req_queue = ReqQueue}) ->
    ConsumeReq = #'basic.consume'{ queue = ReqQueue,
                                   no_ack = false,
                                   exclusive = false },
    %% We are listening for two things:
    %% Firstly, deliveries from our request queue, which are forwarded to
    %% the outgoing port; second is incoming responses, which are forwarded
    %% to the (decoded) reply-to queue.
    _Pid = spawn_link(fun() ->
                              amqp_channel:subscribe(Channel, ConsumeReq, self()),
                              receive
                                  #'basic.consume_ok'{} -> ok
                              end,
                              request_loop(Channel, Sock, Params)
                      end),
    _Pid2 = spawn_link(fun() ->
                               response_loop(Channel, Sock, Params, [], path)
                       end),
    {ok, Params}.

request_loop(Channel, Sock, Params) ->
    receive
        {#'basic.deliver'{ delivery_tag = Tag },
         #amqp_msg{ payload = Payload, props = Props }} ->
            #'P_basic'{correlation_id = CorrelationId,
                       reply_to = ReplyTo } = Props,
            case CorrelationId of
                undefined -> no_send;
                Id -> zmq:send(Sock, Id, [sndmore])
            end,
	    % if there is no ReplyTo field in the amqp message don't sent it
            if 
	       ReplyTo =/= undefined ->
                  zmq:send(Sock, ReplyTo, [sndmore]);
	       true -> ok
	    end,
            zmq:send(Sock, <<>>, [sndmore]),
            zmq:send(Sock, Payload),
            amqp_channel:cast(Channel, #'basic.ack'{ delivery_tag = Tag,
                                                     multiple = false })
    end,
    request_loop(Channel, Sock, Params).

response_loop(Channel, Sock, Params = #params{rep_exchange = Exchange},
              Path, payload) ->
    {ok, Data} = zmq:recv(Sock),
    [ ReplyTo | Rest ] = Path,
    CorrelationId = case Rest of
                        []   -> undefined;
                        [Id] -> Id
                    end,
    Msg = #amqp_msg{payload = Data,
                    props = #'P_basic'{
                      reply_to = ReplyTo,
                      correlation_id = CorrelationId}},
    Pub = #'basic.publish'{ exchange = Exchange,
                            routing_key = ReplyTo },
    amqp_channel:cast(Channel, Pub, Msg),
    response_loop(Channel, Sock, Params, [], path);
response_loop(Channel, Sock, Params, Path, path) ->
    {ok, Msg} = zmq:recv(Sock),
    case Msg of
        <<>> ->
            response_loop(Channel, Sock, Params, Path, payload);
        PathElem ->
            response_loop(Channel, Sock, Params, [ PathElem | Path ], path)
    end.
