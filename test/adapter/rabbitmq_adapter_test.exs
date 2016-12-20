defmodule Medusa.Adapter.RabbitMQTest do
  use ExUnit.Case, async: false
  import Medusa.TestHelper
  alias Medusa.Message
  alias Medusa.Adapter.RabbitMQ

  setup_all do
    put_adapter_config(Medusa.Adapter.RabbitMQ)
    {:ok, conn} = AMQP.Connection.open()
    {:ok, chan} = AMQP.Channel.open(conn)
    {:ok, _} = Agent.start(fn -> MapSet.new() end, name: :queues)
    on_exit fn ->
      :queues
      |> Agent.get(&(&1))
      |> Enum.each(&AMQP.Queue.delete(chan, "test-rabbitmq.#{&1}"))
    end
  end

  setup do
    MedusaConfig.set_message_validator(:medusa_config, nil)
    Process.register(self, :self)
    :ok
  end

  describe "RabbitMQ basic" do
    @tag :rabbitmq
    test "Send events" do
      Medusa.consume("foo.bar", &MyModule.echo/1, queue_name: "echo")
      Medusa.consume("foo.*", &MyModule.echo/1)
      Medusa.consume("foo.baz", &MyModule.echo/1)
      Process.sleep(1_000) # wait RabbitMQ connection
      Medusa.publish("foo.bar", "foobar", %{"optional_field" => "nice_to_have"})
      assert_receive %Message{body: "foobar", metadata: %{"optional_field" => "nice_to_have"}}, 1_000
      assert_receive %Message{body: "foobar", metadata: %{"optional_field" => "nice_to_have"}}, 1_000
      refute_receive %Message{body: "foobar", metadata: %{"optional_field" => "nice_to_have"}}, 1_000
    end

    @tag :rabbitmq
    test "Send non-match events" do
      Medusa.consume("ping.pong", &MyModule.echo/1)
      Process.sleep(1_000) # wait RabbitMQ connection
      Medusa.publish("ping", "ping")
      refute_receive %Message{body: "ping"}, 1_000
    end

    @tag :rabbitmq
    @tag :skip
    test "Send event to consumer with bind_once: true.
          consumer and producer should die" do
      assert consumer_children() == []
      assert producer_children() == []
      {:ok, _} =  Medusa.consume("rabbit.bind1", &MyModule.echo/1, bind_once: true)
      [{_, consumer, _, _}] = consumer_children()
      [{_, producer, _, _}] = producer_children()
      Process.sleep(1_000) # wait RabbitMQ connection
      assert Process.alive?(consumer)
      assert Process.alive?(producer)
      ref_consumer = Process.monitor(consumer)
      ref_producer = Process.monitor(producer)
      Medusa.publish("rabbit.bind1", "die both")
      assert_receive %Message{body: "die both"}, 1_000
      assert_receive {:DOWN, ^ref_consumer, :process, _, :normal}, 1_000
      assert_receive {:DOWN, ^ref_producer, :process, _, :normal}, 1_000
    end

    @tag :rabbitmq
    @tag :skip
    test "Send event to consumer with bind_once: true should not kill other producer-consumer" do
      assert consumer_children() == []
      assert producer_children() == []
      {:ok, _} = Medusa.consume("rabbit.bind2", &MyModule.echo/1)
      {:ok, _} = Medusa.consume("rabbit.bind2", &MyModule.echo/1, bind_once: true, queue_name: "test_rabbit_bind2")
      Process.sleep(1_000) # wait RabbitMQ connection
      assert length(consumer_children()) == 2
      assert length(producer_children()) == 2
      Medusa.publish("rabbit.bind2", "only con2, prod2 die")
      assert_receive %Message{body: "only con2, prod2 die"}, 1_000
      assert_receive %Message{body: "only con2, prod2 die"}, 1_000
      Process.sleep(10)
      assert length(consumer_children()) == 1
      assert length(producer_children()) == 1
    end
  end

  describe "RabbitMQ re-publish" do
    @tag :rabbitmq
    test "publish when no connection is queue and resend when re-connected" do
      Medusa.consume("publish.queue", &MyModule.echo/1, queue_name: "test_publish_queue")
      adapter = RabbitMQ |> Process.whereis
      path = [ Access.key(:mod_state), Access.key(:channel) ]
      :sys.replace_state(adapter, &put_in(&1, path, nil))
      assert Medusa.publish("publish.queue", "foo") == {:error, "cannot connect rabbitmq"}  # can't publish right now
      assert Medusa.publish("publish.queue", "bar") == {:error, "cannot connect rabbitmq"}  # can't publish right now
      assert Medusa.publish("publish.queue", "baz") == {:error, "cannot connect rabbitmq"}  # can't publish right now
      send(adapter, {:DOWN, make_ref(), :process, self, :test})
      assert_receive %Message{body: "foo"}, 1_000
      assert_receive %Message{body: "bar"}, 1_000
      assert_receive %Message{body: "baz"}, 1_000
    end
  end


  describe "Retry on failrue within max_retries" do
    @tag :rabbitmq
    test "{:error, reason} will retry" do
      %{body: body} = publish_consume(&MyModule.state/1, %{times: 1}, max_retries: 1)
      assert_receive %Message{body: ^body}, 1_000
    end

    @tag :rabbitmq
    test "raise will retry" do
      %{body: body} = publish_consume(&MyModule.state/1, %{times: 2, raise: true}, max_retries: 5)
      assert_receive %Message{body: ^body}, 1_000
    end

    @tag :rabbitmq
    test "throw will retry" do
      %{body: body} = publish_consume(&MyModule.state/1, %{times: 5, throw: true}, max_retries: 10)
      assert_receive %Message{body: ^body}, 1_000
    end

    @tag :rabbitmq
    test "exit will retry" do
      %{body: body} = publish_consume(&MyModule.state/1, %{times: 2, http_error: true}, max_retries: 5)
      assert_receive %Message{body: ^body}, 1_000
    end
  end

  describe "Drop message when reach max_retries" do
    @tag :rabbitmq
    test "setting drop_on_failure retry until reach maximum before drop" do
      %{body: _} = publish_consume(&MyModule.state/1, %{times: 10}, drop_on_failure: true)
      refute_receive %Message{}, 1_000
    end
  end

  describe "Requeue message when reach max_retries" do
    @tag :rabbitmq
    test "retry until reach maximum before requeue" do
      %{body: body} = publish_consume(&MyModule.state/1, %{times: 3}, drop_on_failure: false)
    assert_receive %Message{body: ^body}, 1_000
    end
  end

  describe "Wrong return value" do
    @tag :rabbitmq
    test "not return :ok, :error, {:error, reason} will drop message immediately" do
      %{body: _} = publish_consume(&MyModule.state/1,
                           %{times: 2, bad_return: true},
                           drop_on_failure: false)
      refute_receive %Message{}, 1_000
    end
  end

  describe "Multi functions in consume Success" do
    @tag :rabbitmq
    test "consume many functions consumer do it in sequence" do
      %{body: body} = publish_consume([&MyModule.reverse/1, &MyModule.echo/1], %{}, drop_on_failure: true)
      body = String.reverse(body)
      assert_receive %Message{body: ^body}, 1_000
    end
  end

  describe "Multi functions in consume wrong failrue in the middle" do
    @tag :rabbitmq
    test "not return %Message{} with drop_on_failure should drop immediately" do
      %{body: _} = publish_consume([&MyModule.error/1, &MyModule.echo/1], %{}, drop_on_failure: true)
      refute_receive %Message{}, 1_000
    end

    @tag :rabbitmq
    test "not return %Message{} with no drop_on_failure should requeue" do
      %{body: body} = publish_consume([&MyModule.state/1, &MyModule.echo/1],
                              %{times: 2, middleware: true},
                              drop_on_failure: false)
      assert_receive %Message{body: ^body}, 1_000
    end

    @tag :rabbitmq
    test "raise with no drop_on_failure should requeue" do
      %{body: body} = publish_consume([&MyModule.state/1, &MyModule.echo/1],
                              %{times: 2, middleware: true, raise: true})
      assert_receive %Message{body: ^body}, 1_000
    end

    @tag :rabbitmq
    test "throw with no drop_on_failure should requeue" do
      %{body: body} = publish_consume([&MyModule.state/1, &MyModule.echo/1],
                              %{times: 2, middleware: true, throw: true})
      assert_receive %Message{body: ^body}, 1_000
    end
  end

  describe "Multi functions in consume failure in last function" do
    @tag :rabbitmq
    test "not return :ok, :error or {:error, reason} should drop it immediately" do
      %{body: _} = publish_consume([&MyModule.reverse/1, &MyModule.reverse/1], %{}, drop_on_failure: false)
      refute_receive %Message{}, 1_000
    end

    @tag :rabbitmq
    test ":error with drop_on_failure should drop immediately" do
      %{body: _} = publish_consume([&MyModule.reverse/1, &MyModule.error/1], %{}, drop_on_failure: true)
      refute_receive %Message{}, 1_000
    end

    @tag :rabbitmq
    test ":error with no drop_on_failure should requeue" do
      %{body: body} = publish_consume([&MyModule.reverse/1, &MyModule.state/1], %{times: 2})
      body = String.reverse(body)
      assert_receive %Message{body: ^body}, 1_000
    end

    @tag :rabbitmq
    test "{:error, reason} with drop_on_failure should drop immediately after retries" do
      %{body: _} = publish_consume([&MyModule.reverse/1, &MyModule.state/1],
                           %{times: 100},
                           drop_on_failure: true)
      refute_receive %Message{}, 1_000
    end

    @tag :rabbitmq
    test "{:error, reason} with no drop_on_failure should requeue after retries" do
      %{body: body} = publish_consume([&MyModule.reverse/1, &MyModule.state/1],
                              %{times: 5},
                              max_retries: 3)
      body = String.reverse(body)
      assert_receive %Message{body: ^body}, 1_000
    end

    @tag :rabbitmq
    test "raise with drop_on_failure should drop immediately after retries" do
      %{body: _} = publish_consume([&MyModule.reverse/1, &MyModule.state/1],
                              %{times: 5, raise: true},
                              max_retries: 3, drop_on_failure: true)
      refute_receive %Message{}, 1_000
    end

    @tag :rabbitmq
    test "raise with no drop_on_failure should requeue after retries" do
      %{body: body} = publish_consume([&MyModule.reverse/1, &MyModule.state/1],
                              %{times: 5, raise: true},
                              max_retries: 3, drop_on_failure: false)
      body = String.reverse(body)
      assert_receive %Message{body: ^body}, 1_000
    end

    @tag :rabbitmq
    test "throw with drop_on_failure should drop immediately after retries" do
      %{body: _} = publish_consume([&MyModule.reverse/1, &MyModule.state/1],
                              %{times: 5, throw: true},
                              max_retries: 3, drop_on_failure: true)
      refute_receive %Message{}, 1_000
    end

    @tag :rabbitmq
    test "throw with no drop_on_failure should nack message" do
      %{body: _} = publish_consume([&MyModule.reverse/1, &MyModule.state/1],
                              %{times: 5, throw: true},
                              max_retries: 3, drop_on_failure: true)
      refute_receive %Message{}, 1_000
    end
  end

  describe "Validators" do
    @tag :rabbitmq
    test "Only global validator and :ok" do
      MedusaConfig.set_message_validator(:medusa_config, &always_ok/1)
      %{body: body} = publish_consume(&MyModule.echo/1)
      assert_receive %Message{body: ^body}, 1_000
    end

    @tag :rabbitmq
    test "Only global validator and {:error, reason}" do
      MedusaConfig.set_message_validator(:medusa_config, &always_error/1)
      %{body: _} = publish_consume(&MyModule.echo/1)
      refute_receive %Message{}, 1_000
    end

    @tag :rabbitmq
    test "With 1 extra validator and both: ok" do
      MedusaConfig.set_message_validator(:medusa_config, &always_ok/1)
      %{body: body} = publish_consume(&MyModule.echo/1, %{}, message_validators: &always_ok/1)
      assert_receive %Message{body: ^body}, 1_000
    end

    @tag :rabbitmq
    test "With only extra validator and :ok" do
      %{body: body} = publish_consume(&MyModule.echo/1, %{}, message_validators: &always_ok/1)
      assert_receive %Message{body: ^body}, 1_000
    end

    @tag :rabbitmq
    test "With list of extra validators and all :ok" do
      MedusaConfig.set_message_validator(:medusa_config, &always_ok/1)
      %{body: body} = publish_consume(&MyModule.echo/1, %{}, message_validators: [&always_ok/1, &always_ok/1])
      assert_receive %Message{body: ^body}, 1_000
    end

    @tag :rabbitmq
    test "With error extra validator in the middle" do
      %{body: _} = publish_consume(&MyModule.echo/1, %{}, message_validators: [&always_error/1, &always_ok/1])
      refute_receive %Message{}, 1_000
    end

    @tag :rabbitmq
    test "With error extra validator in the end" do
      %{body: _} = publish_consume(&MyModule.echo/1, %{}, message_validators: [&always_ok/1, &always_error/1])
      refute_receive %Message{}, 1_000
    end

    @tag :rabbitmq
    test "With error extra validator in the global" do
      MedusaConfig.set_message_validator(:medusa_config, &always_error/1)
      %{body: _} = publish_consume(&MyModule.echo/1, %{}, message_validators: [&always_ok/1, &always_ok/1])
      refute_receive %Message{}, 1_000
    end
  end

  defp always_ok(_message), do: :ok

  defp always_error(_message), do: :error

  defp random_string do
    32 |> :crypto.strong_rand_bytes |> Base.encode64
  end

  defp publish_consume(functions, metadata \\ %{}, opts \\ []) do
    agent_name = random_string() |> String.to_atom
    body = random_string()
    topic = random_string()
    queue_name = random_string()
    Agent.start(fn -> 0 end, name: agent_name)
    Agent.update(:queues, &MapSet.put(&1, queue_name))
    opts = Keyword.merge(opts, queue_name: queue_name)
    :ok = Medusa.consume(topic, functions, opts)
    Process.sleep(1_000)
    Medusa.publish(topic, body, Map.merge(metadata, %{agent: agent_name}))
    %{agent: agent_name, body: body}
  end

end
