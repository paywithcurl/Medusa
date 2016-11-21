defmodule Medusa.Adapter.RabbitMQTest do
  use ExUnit.Case, async: true
  import Medusa.TestHelper
  alias Medusa.Broker.Message
  alias Medusa.Adapter.RabbitMQ

  setup do
    Process.register(self, :self)
    :ok
  end

  describe "RabbitMQ" do

    setup do
      put_adapter_config(Medusa.Adapter.RabbitMQ)
      :ok
    end

    @tag :rabbitmq
    test "Send events" do
      Medusa.consume("foo.bar", &MyModule.echo/1, queue_name: "echo")
      Medusa.consume("foo.*", &MyModule.echo/1)
      Medusa.consume("foo.baz", &MyModule.echo/1)
      Process.sleep(100) # wait RabbitMQ connection
      Medusa.publish("foo.bar", "foobar", %{"optional_field" => "nice_to_have"})
      assert_receive %Message{body: "foobar", metadata: %{"optional_field" => "nice_to_have"}}
      assert_receive %Message{body: "foobar", metadata: %{"optional_field" => "nice_to_have"}}
      refute_receive %Message{body: "foobar", metadata: %{"optional_field" => "nice_to_have"}}
    end

    @tag :rabbitmq
    test "Send non-match events" do
      Medusa.consume("ping.pong", &MyModule.echo/1)
      Process.sleep(100) # wait RabbitMQ connection
      Medusa.publish("ping", "ping")
      refute_receive %Message{body: "ping"}
    end

    @tag :rabbitmq
    test "Send event to consumer with bind_once: true.
          consumer and producer should die" do
      assert consumer_children() == []
      assert producer_children() == []
      {:ok, _} =  Medusa.consume("rabbit.bind1", &MyModule.echo/1, bind_once: true)
      [{_, consumer, _, _}] = consumer_children()
      [{_, producer, _, _}] = producer_children()
      Process.sleep(100) # wait RabbitMQ connection
      assert Process.alive?(consumer)
      assert Process.alive?(producer)
      ref_consumer = Process.monitor(consumer)
      ref_producer = Process.monitor(producer)
      Medusa.publish("rabbit.bind1", "die both")
      assert_receive %Message{body: "die both"}
      assert_receive {:DOWN, ^ref_consumer, :process, _, :normal}
      assert_receive {:DOWN, ^ref_producer, :process, _, :normal}
    end

    @tag :rabbitmq
    test "Send event to consumer with bind_once: true should not kill other producer-consumer" do
      assert consumer_children() == []
      assert producer_children() == []
      {:ok, _} = Medusa.consume("rabbit.bind2", &MyModule.echo/1)
      {:ok, _} = Medusa.consume("rabbit.bind2", &MyModule.echo/1, bind_once: true)
      Process.sleep(100) # wait RabbitMQ connection
      assert length(consumer_children()) == 2
      assert length(producer_children()) == 2
      Medusa.publish("rabbit.bind2", "only con2, prod2 die")
      assert_receive %Message{body: "only con2, prod2 die"}
      assert_receive %Message{body: "only con2, prod2 die"}
      Process.sleep(10)
      assert length(consumer_children()) == 1
      assert length(producer_children()) == 1
    end

    @tag :rabbitmq
    test "publish when no connection is queue and resend when re-connected" do
      Medusa.consume("publish.queue", &MyModule.echo/1, queue_name: "test_publish_queue")
      adapter = RabbitMQ |> Process.whereis
      path = [ Access.key(:mod_state), Access.key(:channel) ]
      :sys.replace_state(adapter, &put_in(&1, path, nil))
      assert Medusa.publish("publish.queue", "foo") == :error  # can't publish right now
      assert Medusa.publish("publish.queue", "bar") == :error  # can't publish right now
      assert Medusa.publish("publish.queue", "baz") == :error  # can't publish right now
      send(adapter, {:DOWN, make_ref(), :process, self, :test})
      assert_receive %Message{body: "foo"}, 1_000
      assert_receive %Message{body: "bar"}, 1_000
      assert_receive %Message{body: "baz"}, 1_000
    end

  end

end
