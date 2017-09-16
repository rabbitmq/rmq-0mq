PROJECT = rmq_0mq
PROJECT_DESCRIPTION = RabbitMQ ZeroMQ Support

DEPS = rabbit_common rabbit amqp_client

include rabbitmq-components.mk
include erlang.mk
