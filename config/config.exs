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
  metadata: [:message_id, :topic]

# This configuration is loaded before any dependency and is restricted
# to this project. If another project depends on this project, this
# file won't be loaded nor affect the parent project. For this reason,
# if you want to provide default values for your application for
# 3rd-party users, it should be done in your "mix.exs" file.

# You can configure for your application as:
#
#     config :medusa, key: :value
#
# And access this configuration in your application as:
#
#     Application.get_env(:medusa, :key)
#
# Or configure a 3rd-party app:
#
#     config :logger, level: :info
#

# It is also possible to import configuration files, relative to this
# directory. For example, you can emulate configuration per environment
# by uncommenting the line below and defining dev.exs, test.exs and such.
# Configuration from the imported file will override the ones defined
# here (which is why it is important to import them last).
#
import_config "#{Mix.env}.exs"
