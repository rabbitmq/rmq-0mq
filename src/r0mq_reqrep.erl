-module(r0mq_reqrep).

%% The request/response service. The "incoming" socket accepts
%% requests and sends responses (i.e., it is connected to by req
%% sockets); the "outgoing" socket sends requests and accepts
%% responses.
%%
%% See http://wiki.github.com/rabbitmq/rmq-0mq/reqrep


%% Callbacks
-export([init/3, create_in_socket/0, create_out_socket/0,
        start_listening/2, zmq_message/4, amqp_message/6]).

-include_lib("amqp_client/include/amqp_client.hrl").

-record(state, {req_exchange, % (probably default) exchange to which we send requests
                req_queue, % (probably shared) queue from which we retrieve requests
                rep_exchange, % (probably default) exchange to send replies
                rep_queue, % (private) queue from which to collect replies
                incoming_state = path, % state of multipart request message
                incoming_path = [],
                outgoing_state = path, % state of multipart reply message
                outgoing_path = [],
                request_tag = none, % consumer tag for request queue
                reply_tag = none % consumer tag for reply queue
               }).

%% -- Callbacks --

%% We use xreq and xrep, because we want to be asynchronous, and to
%% strip the routing information from the messages (and apply it when
%% sending back).  This means we have to recv and send multipart
%% messages.  For receiving, we have a little state machine.  For
%% sending, we just do it all in one go.

create_in_socket() ->
    {ok, In} = zmq:socket(xrep, [{active, true}]),
    In.

create_out_socket() ->
    {ok, Out} = zmq:socket(xreq, [{active, true}]),
    Out.

init(Options, Connection, ConsumeChannel) ->
    %% TODO Assume default exchange for requests and replies, and
    %% only look for the shared request queue
    ReqQueueName = case r0mq_util:get_option(request_queue, Options) of
                       missing -> r0mq_util:private_name();
                       Name    -> Name
                   end,
    %% We don't actually care about the type, but we supply one
    %% in case it gets created.
    ReqExchange = <<"">>,
    RepExchange = <<"">>,
    r0mq_util:ensure_shared_queue(ReqQueueName, ReqExchange, queue, Connection),
    {ok, RepQueueName} = r0mq_util:create_bind_private_queue(
                           RepExchange, queue, ConsumeChannel),
    {ok, #state{ req_exchange = ReqExchange,
                 rep_exchange = RepExchange,
                 req_queue = ReqQueueName,
                 rep_queue = RepQueueName}}.

start_listening(Channel, State = #state{req_queue = ReqQueue,
                                        rep_queue = RepQueue}) ->
    ConsumeRep = #'basic.consume'{ queue = RepQueue,
                                   no_ack = true,
                                   exclusive = true },
    #'basic.consume_ok'{consumer_tag = RepTag } =
        amqp_channel:subscribe(Channel, ConsumeRep, self()),
    ConsumeReq = #'basic.consume'{ queue = ReqQueue,
                                   no_ack = true,
                                   exclusive = false },
    #'basic.consume_ok'{consumer_tag = ReqTag } =
        amqp_channel:subscribe(Channel, ConsumeReq, self()),
    {ok, State#state{ request_tag = ReqTag, reply_tag = RepTag }}.

%% If we get a zero-length payload, it means we've got the path, and
%% the next is the request payload. (FIXME: request payloads may be
%% multipart as well)
zmq_message(<<>>, in, _Channel, State = #state{incoming_state = path}) ->
    {ok, State#state{incoming_state = payload}};
zmq_message(Data, in, _Channel, State = #state{incoming_state = path,
                                               incoming_path = Path}) ->
    {ok, State#state{incoming_path = [Data | Path]}};
zmq_message(Data, in, Channel, State = #state{incoming_state = payload,
                                              incoming_path = Path,
                                              req_exchange = Exchange,
                                              req_queue = Queue,
                                              rep_queue = ReplyQueue}) ->
    CorrelationId = encode_path(Path),
    Msg = #amqp_msg{payload = Data,
                    props = #'P_basic'{
                      reply_to = ReplyQueue,
                      correlation_id = CorrelationId}},
    Pub = #'basic.publish'{ exchange = Exchange,
                            routing_key = Queue },
    amqp_channel:cast(Channel, Pub, Msg),
    {ok, State#state{incoming_state = path, incoming_path = []}};
zmq_message(<<>>, out, _Channel, State = #state{outgoing_state = path}) ->
    {ok, State#state{outgoing_state = payload}};
zmq_message(Data, out, _Channel, State = #state{outgoing_state = path,
                                                outgoing_path = Path}) ->
    {ok, State#state{outgoing_path = [Data | Path]}};
zmq_message(Data, out, Channel, State = #state{outgoing_state = payload,
                                               outgoing_path = Path,
                                               req_exchange = Exchange,
                                               rep_queue = Queue}) ->
    [ CorrelationId | [ReplyTo] ] = Path, %% NB reverse of what we send
    Msg = #amqp_msg{payload = Data,
                    props = #'P_basic'{
                      reply_to = ReplyTo,
                      correlation_id = CorrelationId}},
    Pub = #'basic.publish'{ exchange = Exchange,
                            routing_key = Queue },
    amqp_channel:cast(Channel, Pub, Msg),
    {ok, State#state{outgoing_state = path, outgoing_path = []}}.

amqp_message(#'basic.deliver'{consumer_tag = Tag},
             #amqp_msg{ payload = Payload, props = Props },
             InSock, OutSock,
             _Channel,
             State = #state{request_tag = ReqTag,
                            reply_tag = RepTag}) ->
    case Tag of
        ReqTag ->
            %% A request, either from an AMQP client, or us
            io:format("Request received ~p", [Payload]),
            #'P_basic'{correlation_id = CorrelationId,
                       reply_to = ReplyTo} = Props,
            zmq:send(OutSock, ReplyTo, [sndmore]),
            zmq:send(OutSock, CorrelationId, [sndmore]),
            zmq:send(OutSock, <<>>, [sndmore]),
            zmq:send(OutSock, Payload);
        RepTag ->
            %% A reply. Since it's for us, the correlation id will be
            %% our encoded correlation id
            #'P_basic'{correlation_id = CorrelationId} = Props,
            io:format("Reply received ~p", [Payload]),
            Path = decode_path(CorrelationId),
            lists:foreach(fun (PathElement) ->
                                  zmq:send(InSock, PathElement, [sndmore])
                          end, Path),
            zmq:send(InSock, <<>>, [sndmore]),
            zmq:send(InSock, Payload)
    end,
    {ok, State}.

%% FIXME only deal with one for the minute
encode_path([Id]) ->
    Id.

decode_path(CorrId) ->
    [CorrId].

