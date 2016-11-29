use Mix.Config

config :medusa, Medusa,
  adapter: Medusa.Adapter.RabbitMQ,
  group: System.get_env("GROUP")
