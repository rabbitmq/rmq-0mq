# 0MQ plugin for RabbitMQ

The purpose of this plugin is to provision ZeroMQ sockets that relay
messages into RabbitMQ. Each ZeroMQ socket type is given an idiomatic
encoding in the AMQP broker model, which means AMQP clients can
interoperate.

# Building

This plugin uses erlzmq, which requires ZeroMQ to be built and
installed.  Currently erlzmq only works with ZeroMQ as
at http://github.com/sustrik/zeromq2 or in the wip-shutdown branch of
http://github.com/zeromq/zeromq2. The latter is soon to be merged
into master.

You'll need to build and install libzmq.so from the zeromq2
repository. Then the directory needs to be in /etc/ld.so.conf or given
in LD_LIBRARY_PATH when running RabbitMQ. Using /usr/local as the
prefix when building libzmq (which it is by default) should work.

The make file should fetch and build erlzmq for you. It assumes that
libzmq has been put in /usr/local; change ZMQ_PATH in
deps/erlzmq/Makefile if you put it somewhere else.

At the minute RabbitMQ plugins expect to be built from a directory in
rabbitmq-public-umbrella/, and this one is no exception.

    cd rmq-0mq
    make

Either copy dist/*.ez into $rabbitmq-server/plugins/; or, copy
dist/erlzmq.ez (the internal dependency) and symlink the working
directory into $rabbitmq-server/plugins/.

# Using

The mapping between ZeroMQ sockets and exchanges and queues is managed
by configuration. Each instance of a mapping is called a "service".

Here is an example, given as a complete RabbitMQ config file:

    [{r0mq,
      [{services,
        [{pubsub,
          [{bind, "tcp://127.0.0.1:5555"}],
          [{bind, "tcp://127.0.0.1:5556"}],
          [{exchange, <<"amq.fanout">>}]}]}]}].

The general pattern for the R0MQ section is

    {r0mq,
      [{services,
        [{ServiceType,
          [{BindOrConnect, InAddress}, ...],
          [{BindOrConnect, OutAddress}, ...],
          [{OptionKey, OptionValue}, ...]},
         ...]}]}

where

    ServiceType = pubsub
                | pipeline
                | reqrep

    BindOrConnect = bind
                  | connect

and OptionKey will depend on the service type; in the example, it is
the exchange to which messages arriving at the inbound socket are
published, and from which published messages are send over the
outbound socket.

(NB: only pubsub and pipeline are implemented so far).
