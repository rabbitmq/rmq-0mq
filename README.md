# 0MQ plugin for RabbitMQ

The purpose of this plugin is to provision ZeroMQ sockets that relay
messages into RabbitMQ. Each ZeroMQ socket type is given an encoding
in the AMQP broker model, which means AMQP clients can interoperate.

# Building

This plugin uses erlzmq, which requires ZeroMQ to be built and
installed.  Currently erlzmq only works with ZeroMQ as
at http://github.com/sustrik/zeromq2.

You'll need to build and install libzmq.so from that repository. Then
the directory needs to be in /etc/ld.so.conf or given in
LD_LIBRARY_PATH when running RabbitMQ.

At the minute the plugin expects to be built from a directory inside
rabbitmq-public-umbrella/.

