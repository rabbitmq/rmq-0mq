-module(r0mq_rep).

%% The reply service. The socket is sent requests and
%% accepts replies (i.e., it is connected to by rep
%% sockets.
%%
%% See http://wiki.github.com/rabbitmq/rmq-0mq/reqrep

%% Callbacks
-export([init/3, create_socket/0,
        start_listening/2, zmq_message/3, amqp_message/5]).

-include_lib("amqp_client/include/amqp_client.hrl").

-record(state, {req_exchange, % (probably default) exchange from which we get requests
                req_queue, % (probably shared) queue from which we retrieve requests
                rep_exchange, % (probably default) exchange to send replies
                outgoing_state = path, % state of multipart reply message
                outgoing_path = [],
                request_tag = none % consumer tag for request queue
               }).

%% -- Callbacks --

%% We use xreq, because we want to be asynchronous, and to
%% strip the routing information from the messages (and apply it when
%% sending back).  This means we have to recv and send multipart
%% messages.  For receiving, we have a little state machine.  For
%% sending, we just do it all in one go.

create_socket() ->
    {ok, Out} = zmq:socket(xreq, [{active, true}]),
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
    {ok, #state{ req_exchange = ReqExchange,
                 rep_exchange = RepExchange,
                 req_queue = ReqQueueName}}.

start_listening(Channel, State = #state{req_queue = ReqQueue}) ->
    ConsumeReq = #'basic.consume'{ queue = ReqQueue,
                                   no_ack = true,
                                   exclusive = false },
    #'basic.consume_ok'{consumer_tag = ReqTag } =
        amqp_channel:subscribe(Channel, ConsumeReq, self()),
    {ok, State#state{ request_tag = ReqTag }}.

zmq_message(<<>>, _Channel, State = #state{outgoing_state = path}) ->
    {ok, State#state{outgoing_state = payload}};
zmq_message(Data, _Channel, State = #state{outgoing_state = path,
                                           outgoing_path = Path}) ->
    {ok, State#state{outgoing_path = [Data | Path]}};
zmq_message(Data, Channel, State = #state{outgoing_state = payload,
                                          outgoing_path = Path,
                                          req_exchange = Exchange}) ->
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
    {ok, State#state{outgoing_state = path, outgoing_path = []}}.

amqp_message(#'basic.deliver'{consumer_tag = Tag},
             #amqp_msg{ payload = Payload, props = Props },
             Sock,
             _Channel,
             State = #state{request_tag = ReqTag}) ->
    %% A request, either from an AMQP client, or us
    io:format("Request received ~p (corr id ~p)~n", [Payload]),
    #'P_basic'{correlation_id = CorrelationId,
               reply_to = ReplyTo} = Props,
    case CorrelationId of
        undefined -> no_send;
        Id        -> zmq:send(Sock, Id, [sndmore])
    end,
    io:format("Sending: ~p~n", [ReplyTo]),
    zmq:send(Sock, ReplyTo, [sndmore]),
    io:format("Sending: <<>>~n", []),
    zmq:send(Sock, <<>>, [sndmore]),
    io:format("Sending: ~p~n", [Payload]),
    zmq:send(Sock, Payload),
    {ok, State}.
