{application, r0mq,
 [{description, "RabbitMQ -- ZeroMQ bridge"},
  {vsn, "0.0.0"},
  {modules, [
             r0mq,
             r0mq_pubsub
  ]},
  {registered, []},
  {env, []},
  {mod, {r0mq, []}},
  {applications, [kernel, stdlib, rabbit]}]}.
