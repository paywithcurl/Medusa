defmodule Medusa.Adapter.RabbitMQTest do
  use ExUnit.Case, async: true

  setup do
    {:ok, conn} = AMQP.Connection.open()
    {:ok, chan} = AMQP.Channel.open(conn)
    queue_name = "test_queue_name"
    config = [
      adapter: Medusa.Adapter.RabbitMQ,
      RabbitMQ: [
        connection: [
          username: "guest",
          password: "guest",
          virtual_host: "/",
          host: "localhost",
          port: :undefined
        ],
        queue_name: queue_name
      ]
    ]

    on_exit fn ->
      Application.stop(:medusa)
      AMQP.Queue.delete(chan, queue_name)
      Application.ensure_all_started(:medusa)
    end

    {:ok, chan: chan, queue_name: queue_name, config: config}
  end

  test "config queue_name should crate on RabbitMQ server",
        %{chan: chan, queue_name: queue_name, config: config} do
    Application.put_env(:medusa, Medusa, config)
    assert {:ok, _} = AMQP.Queue.declare(chan, queue_name, passive: true)
  end

end
