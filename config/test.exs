use Mix.Config

config :logger, level: :warn,
  json_logger: [metadata: 56]

config :medusa, Medusa,
  adapter: Medusa.Adapter.RabbitMQ,
  group: System.get_env("GROUP")
