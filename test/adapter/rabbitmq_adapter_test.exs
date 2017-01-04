defmodule Medusa.Adapter.RabbitMQTest do
  use ExUnit.Case, async: false
  import Medusa.TestHelper
  import Medusa
  alias Medusa.Message
  alias Medusa.Adapter.RabbitMQ

  setup_all do
    opts = put_rabbitmq_adapter_config()[:RabbitMQ][:connection]
    {:ok, conn} = AMQP.Connection.open(opts)
    {:ok, chan} = AMQP.Channel.open(conn)
    {:ok, _} = Agent.start(fn -> MapSet.new() end, name: :queues)
    on_exit fn ->
      :queues
      |> Agent.get(&(&1))
      |> MapSet.union(MapSet.new(["foo1", "foo2", "foo3", "bind1", "bind2",
                                  "republish", "multi_consumer"]))
      |> Enum.each(&AMQP.Queue.delete(chan, "test-rabbitmq.#{&1}"))
    end

    # pre-define consume
    consume("foo.bar", &MyModule.echo/1, queue_name: "foo1")
    consume("foo.*", &MyModule.echo/1, queue_name: "foo2")
    consume("foo.baz", &MyModule.echo/1, queue_name: "foo3")
    consume("rabbit.bind1", &MyModule.echo/1, bind_once: true, queue_name: "bind1")
    consume("rabbit.bind2", &MyModule.echo/1, queue_name: "bind2")
    consume("rabbit.bind2", &MyModule.echo/1, bind_once: true, queue_name: "bind2")
    consume("rabbit.republish", &MyModule.echo/1, queue_name: "republish")
    Process.sleep(1_000)
  end

  setup do
    MedusaConfig.set_message_validator(:medusa_config, nil)
    :ok
  end

  describe "RabbitMQ basic" do
    @tag :rabbitmq
    test "Send events" do
      myself = pid_to_list(self)
      publish("foo.bar", "foobar", %{"from" => myself})
      assert_receive %Message{body: "foobar", metadata: %{"event" => "foo.bar"}}, 1_000
      assert_receive %Message{body: "foobar", metadata: %{"event" => "foo.bar"}}, 1_000
      refute_receive %Message{body: "foobar", metadata: %{"event" => "foo.bar"}}, 1_000
    end

    @tag :rabbitmq
    test "Send non-match events" do
      publish("ping", "ping")
      refute_receive %Message{body: "ping"}, 1_000
    end

    @tag :rabbitmq
    test "Send event to consumer with bind_once: true.
          consumer and producer should die" do
      myself = pid_to_list(self)
      producer = Process.whereis(:"test-rabbitmq.bind1")
      assert producer
      consumers = :sys.get_state(producer).consumers
      assert Map.keys(consumers) |> length == 1
      ref = Process.monitor(producer)
      publish("rabbit.bind1", "die both", %{"from" => myself})
      assert_receive %Message{body: "die both"}, 1_000
      assert_receive {:DOWN, ^ref, :process, _, :normal}, 1_000
      refute Process.whereis(:"test-rabbit.rabbit_bind1")
    end

    @tag :rabbitmq
    test "Send event to consumer with bind_once: true should not kill other producer-consumer" do
      myself = pid_to_list(self)
      producer = Process.whereis(:"test-rabbitmq.bind2")
      assert producer
      consumers = :sys.get_state(producer).consumers
      assert Map.keys(consumers) |> length == 2
      publish("rabbit.bind2", "die 1 consumer", %{"from" => myself})
      publish("rabbit.bind2", "die 1 consumer", %{"from" => myself})
      assert_receive %Message{body: "die 1 consumer"}, 1_000
      assert_receive %Message{body: "die 1 consumer"}, 1_000
      Process.sleep(100)
      consumers = :sys.get_state(producer).consumers
      assert Map.keys(consumers) |> length == 1
    end

    @tag :rabbitmq
    test "consume with multiple consumers" do
      consume("rabbit.multi_consumer", &MyModule.echo/1, queue_name: "multi_consumer", consumers: 10)
      Process.sleep(1_000)
      producer = Process.whereis(:"test-rabbitmq.multi_consumer")
      consumers = :sys.get_state(producer).consumers
      assert Map.keys(consumers) |> length == 10
      consume("rabbit.multi_consumer", &MyModule.echo/1, queue_name: "multi_consumer", consumers: 5)
      Process.sleep(1_000)
      producer = Process.whereis(:"test-rabbitmq.multi_consumer")
      consumers = :sys.get_state(producer).consumers
      assert Map.keys(consumers) |> length == 15
    end

    test "Test connectivity with alive?" do
      assert Medusa.alive?
    end

    test "Test connectivity to host timing out with alive?" do
      config = Application.get_env(:medusa, Medusa)

      # TEST-NET ip from RFC 5737, shouldn't be routable
      Application.put_env(:medusa, Medusa, invalid_config "192.0.2.0")
      assert not Medusa.alive?
      Application.put_env(:medusa, Medusa, config)
    end

    test "Test connectivity to invalid host with alive?" do
      config = Application.get_env(:medusa, Medusa)

      # Invalid TLD from RFC 2606
      Application.put_env(:medusa, Medusa, invalid_config "rabbitmq.invalid")
      assert not Medusa.alive?
      Application.put_env(:medusa, Medusa, config)
    end

  end

  describe "RabbitMQ re-publish" do
    @tag :rabbitmq
    test "publish when no connection is queue and resend when re-connected" do
      myself = pid_to_list(self)
      adapter = Process.whereis(RabbitMQ)
      path = [ Access.key(:mod_state), Access.key(:channel) ]
      :sys.replace_state(adapter, &put_in(&1, path, nil))
      assert publish("rabbit.republish", "foo", %{"from" => myself}) ==
        {:error, "cannot connect rabbitmq"}
      assert publish("rabbit.republish", "bar", %{"from" => myself}) ==
        {:error, "cannot connect rabbitmq"}
      send(adapter, {:DOWN, make_ref(), :process, self, :test})
      assert_receive %Message{body: "foo"}, 1_000
      assert_receive %Message{body: "bar"}, 1_000
    end
  end


  describe "Retry on failure within max_retries" do
    @tag :rabbitmq
    test "{:error, reason} will retry" do
      myself = pid_to_list(self)
      %{body: body} = publish_consume(&MyModule.state/1,
                                      %{times: 1, from: myself},
                                      max_retries: 1)
      assert_receive %Message{body: ^body}, 1_000
    end

    @tag :rabbitmq
    test "raise will retry" do
      myself = pid_to_list(self)
      %{body: body} = publish_consume(&MyModule.state/1,
                                      %{times: 2, raise: true, from: myself},
                                      max_retries: 5)
      assert_receive %Message{body: ^body}, 1_000
    end

    @tag :rabbitmq
    test "throw will retry" do
      myself = pid_to_list(self)
      %{body: body} = publish_consume(&MyModule.state/1,
                                      %{times: 5, throw: true, from: myself},
                                      max_retries: 10)
      assert_receive %Message{body: ^body}, 1_000
    end

    @tag :rabbitmq
    test "exit will retry" do
      myself = pid_to_list(self)
      %{body: body} = publish_consume(&MyModule.state/1,
                                      %{times: 2, http_error: true, from: myself},
                                      max_retries: 5)
      assert_receive %Message{body: ^body}, 1_000
    end
  end

  describe "Drop message when reach max_retries" do
    @tag :rabbitmq
    test "setting drop_on_failure retry until reach maximum before drop" do
      myself = pid_to_list(self)
      %{body: _} = publish_consume(&MyModule.state/1,
                                   %{times: 10, from: myself},
                                   drop_on_failure: true)
      refute_receive %Message{}, 1_000
    end
  end

  describe "Requeue message when reach max_retries" do
    @tag :rabbitmq
    test "retry until reach maximum before requeue" do
      myself = pid_to_list(self)
      %{body: body} = publish_consume(&MyModule.state/1,
                                      %{times: 3, from: myself},
                                      drop_on_failure: false)
    assert_receive %Message{body: ^body}, 1_000
    end
  end

  describe "Wrong return value" do
    @tag :rabbitmq
    test "not return :ok, :error, {:error, reason} will drop message immediately" do
      myself = pid_to_list(self)
      %{body: _} = publish_consume(&MyModule.state/1,
                           %{times: 2, bad_return: true, from: myself},
                           drop_on_failure: false)
      refute_receive %Message{}, 1_000
    end
  end

  describe "Multi functions in consume Success" do
    @tag :rabbitmq
    test "consume many functions consumer do it in sequence" do
      myself = pid_to_list(self)
      %{body: body} = publish_consume([&MyModule.reverse/1, &MyModule.echo/1],
                                      %{ from: myself},
                                      drop_on_failure: true)
      body = String.reverse(body)
      assert_receive %Message{body: ^body}, 1_000
    end
  end

  describe "Multi functions in consume wrong failure in the middle" do
    @tag :rabbitmq
    test "not return %Message{} with drop_on_failure should drop immediately" do
      myself = pid_to_list(self)
      %{body: _} = publish_consume([&MyModule.error/1, &MyModule.echo/1],
                                   %{from: myself},
                                   drop_on_failure: true)
      refute_receive %Message{}, 1_000
    end

    @tag :rabbitmq
    test "not return %Message{} with no drop_on_failure should requeue" do
      myself = pid_to_list(self)
      %{body: body} = publish_consume([&MyModule.state/1, &MyModule.echo/1],
                                      %{times: 2, middleware: true, from: myself},
                              drop_on_failure: false)
      assert_receive %Message{body: ^body}, 1_000
    end

    @tag :rabbitmq
    test "raise with no drop_on_failure should requeue" do
      myself = pid_to_list(self)
      %{body: body} = publish_consume([&MyModule.state/1, &MyModule.echo/1],
                                      %{times: 2, middleware: true, raise: true, from: myself})
      assert_receive %Message{body: ^body}, 1_000
    end

    @tag :rabbitmq
    test "throw with no drop_on_failure should requeue" do
      myself = pid_to_list(self)
      %{body: body} = publish_consume([&MyModule.state/1, &MyModule.echo/1],
                                      %{times: 2, middleware: true, throw: true, from: myself})
      assert_receive %Message{body: ^body}, 1_000
    end
  end

  describe "Multi functions in consume failure in last function" do
    @tag :rabbitmq
    test "not return :ok, :error or {:error, reason} should drop it immediately" do
      myself = pid_to_list(self)
      %{body: _} = publish_consume([&MyModule.reverse/1, &MyModule.reverse/1],
                                   %{from: myself},
                                   drop_on_failure: false)
      refute_receive %Message{}, 1_000
    end

    @tag :rabbitmq
    test ":error with drop_on_failure should drop immediately" do
      myself = pid_to_list(self)
      %{body: _} = publish_consume([&MyModule.reverse/1, &MyModule.error/1],
                                   %{from: myself},
                                   drop_on_failure: true)
      refute_receive %Message{}, 1_000
    end

    @tag :rabbitmq
    test ":error with no drop_on_failure should requeue" do
      myself = pid_to_list(self)
      %{body: body} = publish_consume([&MyModule.reverse/1, &MyModule.state/1],
                                      %{times: 2, from: myself})
      body = String.reverse(body)
      assert_receive %Message{body: ^body}, 1_000
    end

    @tag :rabbitmq
    test "{:error, reason} with drop_on_failure should drop immediately after retries" do
      myself = pid_to_list(self)
      %{body: _} = publish_consume([&MyModule.reverse/1, &MyModule.state/1],
                                   %{times: 100, from: myself},
                                   drop_on_failure: true)
      refute_receive %Message{}, 1_000
    end

    @tag :rabbitmq
    test "{:error, reason} with no drop_on_failure should requeue after retries" do
      myself = pid_to_list(self)
      %{body: body} = publish_consume([&MyModule.reverse/1, &MyModule.state/1],
                                      %{times: 5, from: myself},
                                      max_retries: 3)
      body = String.reverse(body)
      assert_receive %Message{body: ^body}, 1_000
    end

    @tag :rabbitmq
    test "raise with drop_on_failure should drop immediately after retries" do
      myself = pid_to_list(self)
      %{body: _} = publish_consume([&MyModule.reverse/1, &MyModule.state/1],
                                   %{times: 5, raise: true, from: myself},
                                   max_retries: 3, drop_on_failure: true)
      refute_receive %Message{}, 1_000
    end

    @tag :rabbitmq
    test "raise with no drop_on_failure should requeue after retries" do
      myself = pid_to_list(self)
      %{body: body} = publish_consume([&MyModule.reverse/1, &MyModule.state/1],
                                      %{times: 5, raise: true, from: myself},
                                      max_retries: 3, drop_on_failure: false)
      body = String.reverse(body)
      assert_receive %Message{body: ^body}, 1_000
    end

    @tag :rabbitmq
    test "throw with drop_on_failure should drop immediately after retries" do
      myself = pid_to_list(self)
      %{body: _} = publish_consume([&MyModule.reverse/1, &MyModule.state/1],
                                   %{times: 5, throw: true, from: myself},
                                   max_retries: 3, drop_on_failure: true)
      refute_receive %Message{}, 1_000
    end

    @tag :rabbitmq
    test "throw with no drop_on_failure should nack message" do
      myself = pid_to_list(self)
      %{body: _} = publish_consume([&MyModule.reverse/1, &MyModule.state/1],
                                   %{times: 5, throw: true, from: myself},
                                   max_retries: 3, drop_on_failure: true)
      refute_receive %Message{}, 1_000
    end
  end

  describe "Validators" do
    @tag :rabbitmq
    test "Only global validator and :ok" do
      myself = pid_to_list(self)
      MedusaConfig.set_message_validator(:medusa_config, &always_ok/1)
      %{body: body} = publish_consume(&MyModule.echo/1, %{from: myself})
      assert_receive %Message{body: ^body}, 1_000
    end

    @tag :rabbitmq
    test "Only global validator and {:error, reason}" do
      myself = pid_to_list(self)
      MedusaConfig.set_message_validator(:medusa_config, &always_error/1)
      %{body: _} = publish_consume(&MyModule.echo/1, %{from: myself})
      refute_receive %Message{}, 1_000
    end

    @tag :rabbitmq
    test "With 1 extra validator and both: ok" do
      myself = pid_to_list(self)
      MedusaConfig.set_message_validator(:medusa_config, &always_ok/1)
      %{body: body} = publish_consume(&MyModule.echo/1,
                                      %{from: myself},
                                      message_validators: &always_ok/1)
      assert_receive %Message{body: ^body}, 1_000
    end

    @tag :rabbitmq
    test "With only extra validator and :ok" do
      myself = pid_to_list(self)
      %{body: body} = publish_consume(&MyModule.echo/1,
                                      %{from: myself},
                                      message_validators: &always_ok/1)
      assert_receive %Message{body: ^body}, 1_000
    end

    @tag :rabbitmq
    test "With list of extra validators and all :ok" do
      MedusaConfig.set_message_validator(:medusa_config, &always_ok/1)
      myself = pid_to_list(self)
      %{body: body} = publish_consume(&MyModule.echo/1,
                                      %{from: myself},
                                      message_validators: [&always_ok/1, &always_ok/1])
      assert_receive %Message{body: ^body}, 1_000
    end

    @tag :rabbitmq
    test "With error extra validator in the middle" do
      myself = pid_to_list(self)
      %{body: _} = publish_consume(&MyModule.echo/1,
                                   %{from: myself},
                                   message_validators: [&always_error/1, &always_ok/1])
      refute_receive %Message{}, 1_000
    end

    @tag :rabbitmq
    test "With error extra validator in the end" do
      myself = pid_to_list(self)
      %{body: _} = publish_consume(&MyModule.echo/1,
                                   %{from: myself}, message_validators: [&always_ok/1, &always_error/1])
      refute_receive %Message{}, 1_000
    end

    @tag :rabbitmq
    test "With error extra validator in the global" do
      myself = pid_to_list(self)
      MedusaConfig.set_message_validator(:medusa_config, &always_error/1)
      %{body: _} = publish_consume(&MyModule.echo/1,
                                   %{from: myself},
                                   message_validators: [&always_ok/1, &always_ok/1])
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
    :ok = consume(topic, functions, opts)
    Process.sleep(1_000)
    publish(topic, body, Map.merge(metadata, %{agent: agent_name}))
    %{agent: agent_name, body: body}
  end

  defp invalid_config(host) do
    [
      adapter: Medusa.Adapter.RabbitMQ,
      RabbitMQ: %{
	admin: [
	  protocol: "http",
	  port: 15672,
	],
	connection: [
	  host: host,
	  username: "donald",
	  password: "boss",
	  virtual_host: "/"
	]
      }
    ]
  end

end
