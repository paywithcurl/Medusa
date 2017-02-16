# This file is responsible for configuring your application
# and its dependencies with the aid of the Mix.Config module.
use Mix.Config

config :medusa, Medusa,
  retry_publish_backoff: 500,
  retry_publish_max: 1,
  retry_consume_pow_base: 0,
  RabbitMQ: %{
    admin: [
      protocol: System.get_env("RABBITMQ_ADMIN_PROTOCOL") || "http",
      port: String.to_integer(System.get_env("RABBITMQ_ADMIN_PORT") || "15672"),
    ],
    connection: [
      host: System.get_env("RABBITMQ_HOST") || "127.0.0.1",
      username: System.get_env("RABBITMQ_USERNAME") || "guest",
      password: System.get_env("RABBITMQ_PASSWORD") || "guest",
      port: String.to_integer(System.get_env("RABBITMQ_PORT") || "5672"),
      virtual_host: System.get_env("RABBITMQ_VIRTUAL_HOST") || "/",
      heartbeat: 10,
    ]
  }

config :logger, :console,
  format: "\n$message\n"

import_config "#{Mix.env}.exs"
