-module(r0mq_req).

%% The request service. The socket accepts
%% requests and sends responses (i.e., it is connected to by req
%% sockets.
%%
%% See http://wiki.github.com/rabbitmq/rmq-0mq/reqrep

%% Callbacks
-export([init/3, create_socket/0,
        start_listening/2, zmq_message/3, amqp_message/5]).

-include_lib("amqp_client/include/amqp_client.hrl").

-record(state, {req_queue, % used as routing key for requests
                req_exchange, % exchange to which to send requests
                rep_exchange, % (probably default) exchange to send replies
                rep_queue, % (private) queue from which to collect replies
                incoming_state = path, % state of multipart request message
                incoming_path = [],
                reply_tag = none % consumer tag for reply queue
               }).

%% -- Callbacks --

%% We use xrep, because we want to be asynchronous, and to
%% strip the routing information from the messages (and apply it when
%% sending back).  This means we have to recv and send multipart
%% messages.  For receiving, we have a little state machine.  For
%% sending, we just do it all in one go.

create_socket() ->
    {ok, In} = zmq:socket(xrep, [{active, true}]),
    In.

init(Options, Connection, ConsumeChannel) ->
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
    {ok, #state{ req_queue = ReqQueueName,
                 req_exchange = ReqExchange,
                 rep_exchange = RepExchange,
                 rep_queue = RepQueueName}}.

start_listening(Channel, State = #state{rep_queue = RepQueue}) ->
    ConsumeRep = #'basic.consume'{ queue = RepQueue,
                                   no_ack = true,
                                   exclusive = true },
    #'basic.consume_ok'{consumer_tag = RepTag } =
        amqp_channel:subscribe(Channel, ConsumeRep, self()),
    {ok, State#state{ reply_tag = RepTag }}.

%% If we get a zero-length payload, it means we've got the path, and
%% the next is the request payload.
zmq_message(<<>>, _Channel, State = #state{incoming_state = path}) ->
    {ok, State#state{incoming_state = payload}};
zmq_message(Data, _Channel, State = #state{incoming_state = path,
                                           incoming_path = Path}) ->
    {ok, State#state{incoming_path = [Data | Path]}};
zmq_message(Data, Channel, State = #state{incoming_state = payload,
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
    {ok, State#state{incoming_state = path, incoming_path = []}}.

amqp_message(#'basic.deliver'{consumer_tag = Tag},
             #amqp_msg{ payload = Payload, props = Props },
             Sock, _Channel, State) ->
    %% A reply. Since it's for us, the correlation id will be
    %% our encoded correlation id
    #'P_basic'{correlation_id = CorrelationId} = Props,
    io:format("Reply received ~p corr id ~p", [Payload, CorrelationId]),
    Path = decode_path(CorrelationId),
    lists:foreach(fun (PathElement) ->
                          zmq:send(Sock, PathElement, [sndmore])
                  end, Path),
    zmq:send(Sock, <<>>, [sndmore]),
    zmq:send(Sock, Payload),
    {ok, State}.

%% FIXME only deal with one for the minute
encode_path([Id]) ->
    Id.

decode_path(undefined) ->
    [];
decode_path(CorrId) ->
    [CorrId].
