defmodule Medusa.Adapter.RabbitMQTest do
  use ExUnit.Case, async: false
  import Medusa.TestHelper
  alias Medusa.Message
  alias Medusa.Adapter.RabbitMQ

  setup_all do
    put_rabbitmq_adapter_config()
    :ok = Medusa.consume("rabbit.basic1",
                         &forward_message_to_test/1,
                         queue_name: "rabbit_basic_test_1")
    :ok = Medusa.consume("*.basic1",
                         &forward_message_to_test/1,
                         queue_name: "rabbit_basic_test_*")
    :ok = Medusa.consume("rabbit.bind1",
                         &forward_message_to_test/1,
                         queue_name: "rabbit_bind_test_1",
                         bind_once: true)
    :ok = Medusa.consume("rabbit.bind2",
                         &forward_message_to_test/1,
                         queue_name: "rabbit_bind_test_2")
    :ok = Medusa.consume("rabbit.bind2",
                         &forward_message_to_test/1,
                         queue_name: "rabbit_bind_test_2",
                         bind_once: true)
    :ok = Medusa.consume("rabbit.republish",
                         &forward_message_to_test/1,
                         queue_name: "rabbit_republish_test")
    :ok = Medusa.consume("rabbit.retry1",
                         &message_to_test/1,
                         queue_name: "rabbit_retry_test_1",
                         max_retries: 1)
    :ok = Medusa.consume("rabbit.retry2",
                         &message_to_test/1,
                         queue_name: "rabbit_retry_test_2",
                         max_retries: 5)
    :ok = Medusa.consume("rabbit.retry3",
                         &message_to_test/1,
                         queue_name: "rabbit_retry_test_3",
                         max_retries: 2)
    :ok = Medusa.consume("rabbit.retry4",
                         &message_to_test/1,
                         queue_name: "rabbit_retry_test_4",
                         max_retries: 2)
    :ok = Medusa.consume("rabbit.retry.failed1",
                         &message_to_test/1,
                         queue_name: "rabbit_retry_failed_test_1",
                         max_retries: 1,
                         on_failure: :drop)
    :ok = Medusa.consume("rabbit.retry.failed2",
                         &message_to_test/1,
                         queue_name: "rabbit_retry_failed_test_2",
                         max_retries: 1,
                         on_failure: &always_drop/1)
    :ok = Medusa.consume("rabbit.retry.failed3",
                         &message_to_test/1,
                         queue_name: "rabbit_retry_failed_test_3",
                         max_retries: 1,
                         on_failure: &always_ok/1)
    :ok = Medusa.consume("rabbit.retry.requeue1",
                         &message_to_test/1,
                         queue_name: "rabbit_retry_requeue_test_1",
                         max_retries: 1,
                         on_failure: :keep)
    :ok = Medusa.consume("rabbit.retry.requeue2",
                         &message_to_test/1,
                         queue_name: "rabbit_retry_requeue_test_2",
                         max_retries: 1,
                         on_failure: &always_keep/1)
    :ok = Medusa.consume("rabbit.retry.requeue3",
                         &message_to_test/1,
                         queue_name: "rabbit_retry_requeue_test_3",
                         max_retries: 1,
                         on_failure: &always_ok/1)
    :ok = Medusa.consume("rabbit.wrong1",
                         &message_to_test/1,
                         queue_name: "rabbit_wrong_test_1",
                         max_retries: 1,
                         on_failure: :keep)
    :ok = Medusa.consume("rabbit.multi1",
                         [&reverse_message/1, &forward_message_to_test/1],
                         queue_name: "rabbit_multi_test_1")
    :ok = Medusa.consume("rabbit.multi.requeue2",
                         [&reverse_message/1, &message_to_test/1],
                         queue_name: "rabbit_multi_requeue_test_2",
                         on_failure: :keep)
    :ok = Medusa.consume("rabbit.multi.failed1",
                         [&error_message/1, &forward_message_to_test/1],
                         queue_name: "rabbit_multi_failed_test_1",
                         on_failure: :drop)
    :ok = Medusa.consume("rabbit.multi.failed3",
                         [&reverse_message/1, &reverse_message/1],
                         queue_name: "rabbit_multi_failed_test_3",
                         on_failure: :keep)
    :ok = Medusa.consume("rabbit.multi.failed4",
                         [&reverse_message/1, &error_message/1],
                         queue_name: "rabbit_multi_failed_test_4",
                         on_failure: :drop)
    :ok = Medusa.consume("rabbit.multi.failed5",
                         [&reverse_message/1, &message_to_test/1],
                         queue_name: "rabbit_multi_failed_test_5",
                         on_failure: :keep)
    :ok = Medusa.consume("rabbit.multi.failed6",
                         [&reverse_message/1, &message_to_test/1],
                         queue_name: "rabbit_multi_failed_test_6",
                         on_failure: :drop)
    :ok = Medusa.consume("rabbit.validator1",
                         &forward_message_to_test/1,
                         queue_name: "rabbit_validator_test_1")
    :ok = Medusa.consume("rabbit.validator2",
                         &forward_message_to_test/1,
                         queue_name: "rabbit_validator_test_2")
    :ok = Medusa.consume("rabbit.validator3",
                         &forward_message_to_test/1,
                         queue_name: "rabbit_validator_test_3",
                         message_validators: &always_ok/1)
    :ok = Medusa.consume("rabbit.validator4",
                         &forward_message_to_test/1,
                         queue_name: "rabbit_validator_test_4",
                         message_validators: &always_ok/1)
    :ok = Medusa.consume("rabbit.validator5",
                         &forward_message_to_test/1,
                         queue_name: "rabbit_validator_test_5",
                         message_validators: [&always_ok/1, &always_ok/1])
    :ok = Medusa.consume("rabbit.validator6",
                         &forward_message_to_test/1,
                         queue_name: "rabbit_validator_test_6",
                         message_validators: [&always_error/1, &always_ok/1])
    :ok = Medusa.consume("rabbit.validator7",
                         &forward_message_to_test/1,
                         queue_name: "rabbit_validator_test_7",
                         message_validators: [&always_ok/1, &always_error/1])
    :ok = Medusa.consume("rabbit.validator8",
                         &forward_message_to_test/1,
                         queue_name: "rabbit_validator_test_8",
                         message_validators: [&always_ok/1, &always_ok/1])
    Process.sleep(1_000)
    :ok
  end

  setup do
    MedusaConfig.set_message_validator(:medusa_config, nil)
    :ok
  end

  describe "send messages" do
    test "matched event should received message" do
      body = "foobar"
      publish_test_message("rabbit.basic1", body, %{"extra" => "good"})
      assert_receive %Message{body: ^body, metadata: %{"event" => "rabbit.basic1", "extra" => "good"}}, 1_000
      assert_receive %Message{body: ^body, metadata: %{"event" => "rabbit.basic1", "extra" => "good"}}, 1_000
    end

    test "non-matched event should not received message" do
      publish_test_message("ping", "ping")
      refute_receive %Message{body: "ping"}, 1_000
    end
  end

  describe "bind_once" do
    test "Send event to consumer with bind_once: true.
          consumer and producer should die" do
      producer = Process.whereis(:"test-rabbitmq.rabbit_bind_test_1")
      assert producer
      consumers = :sys.get_state(producer).consumers
      assert consumers |> Map.keys() |> length == 1
      ref = Process.monitor(producer)
      publish_test_message("rabbit.bind1", "die both")
      assert_receive %Message{body: "die both", metadata: %{"event" => "rabbit.bind1"}}, 1_000
      assert_receive {:DOWN, ^ref, :process, _, :normal}, 1_000
      refute Process.whereis(:"test-rabbitmq.rabbit_bind_test_1")
    end

    test "Send event to consumer with bind_once: true should not kill other producer-consumer" do
      producer = Process.whereis(:"test-rabbitmq.rabbit_bind_test_2")
      assert producer
      consumers = :sys.get_state(producer).consumers
      assert consumers |> Map.keys() |> length == 2
      publish_test_message("rabbit.bind2", "die 1 consumer")
      publish_test_message("rabbit.bind2", "die 1 consumer")
      assert_receive %Message{body: "die 1 consumer", metadata: %{"event" => "rabbit.bind2"}}, 1_000
      assert_receive %Message{body: "die 1 consumer", metadata: %{"event" => "rabbit.bind2"}}, 1_000
      Process.sleep(100)
      consumers = :sys.get_state(producer).consumers
      assert consumers |> Map.keys() |> length == 1
    end
  end

  describe "muliple consumers" do
    test "spawn many consumers with consumers options" do
      Medusa.consume("rabbit.multi_consumer",
                     &forward_message_to_test/1,
                     queue_name: "rabbit_multi_consumer_test", consumers: 10)
      Process.sleep(1_000)
      producer = Process.whereis(:"test-rabbitmq.rabbit_multi_consumer_test")
      consumers = :sys.get_state(producer).consumers
      assert consumers |> Map.keys() |> length == 10
      Medusa.consume("rabbit.multi_consumer",
                     &forward_message_to_test/1,
                     queue_name: "rabbit_multi_consumer_test", consumers: 5)
      Process.sleep(1_000)
      producer = Process.whereis(:"test-rabbitmq.rabbit_multi_consumer_test")
      consumers = :sys.get_state(producer).consumers
      assert consumers |> Map.keys() |> length == 15
    end
  end

  describe "alive?" do
    test "connectivity with alive?" do
      assert Medusa.alive?()
    end

    test "connectivity to host timing out with alive?" do
      config = Application.get_env(:medusa, Medusa)
      # TEST-NET ip from RFC 5737, shouldn't be routable
      Application.put_env(:medusa, Medusa, invalid_config "192.0.2.0")
      refute Medusa.alive?()
      Application.put_env(:medusa, Medusa, config)
    end

    test "connectivity to invalid host with alive?" do
      config = Application.get_env(:medusa, Medusa)
      # Invalid TLD from RFC 2606
      Application.put_env(:medusa, Medusa, invalid_config "rabbitmq.invalid")
      refute Medusa.alive?()
      Application.put_env(:medusa, Medusa, config)
    end
  end

  describe "re-publish" do
    test "publish when no connection is queue and resend when re-connected" do
      adapter = Process.whereis(RabbitMQ)
      path = [ Access.key!(:mod_state), Access.key!(:channel) ]
      :sys.replace_state(adapter, &put_in(&1, path, nil))
      assert publish_test_message("rabbit.republish", "foo") ==
        {:error, "cannot connect rabbitmq"}
      assert publish_test_message("rabbit.republish", "bar") ==
        {:error, "cannot connect rabbitmq"}
      send(adapter, {:DOWN, make_ref(), :process, self(), :test})
      assert_receive %Message{body: "foo", metadata: %{"event" => "rabbit.republish"}}, 1_000
      assert_receive %Message{body: "bar", metadata: %{"event" => "rabbit.republish"}}, 1_000
    end
  end

  describe "Retry on failure" do
    test "{:error, reason}" do
      body = random_string()
      publish_test_message("rabbit.retry1", body, %{"times" => 1})
      assert_receive %Message{body: ^body, metadata: %{"event" => "rabbit.retry1"}}, 1_000
    end

    test "raise" do
      body = random_string()
      publish_test_message("rabbit.retry2", body, %{"times" => 2, raise: true})
      assert_receive %Message{body: ^body, metadata: %{"event" => "rabbit.retry2"}}, 1_000
    end

    test "throw" do
      body = random_string()
      publish_test_message("rabbit.retry3", body, %{"times" => 1, throw: true})
      assert_receive %Message{body: ^body, metadata: %{"event" => "rabbit.retry3"}}, 1_000
    end

    test "exit" do
      body = random_string()
      publish_test_message("rabbit.retry4", body, %{"times" => 1, http_error: true})
      assert_receive %Message{body: ^body, metadata: %{"event" => "rabbit.retry4"}}, 1_000
    end
  end

  describe "When retry reach max_retries" do
    test "setting on_failure to :drop should drop" do
      body = random_string()
      publish_test_message("rabbit.retry.failed1", body, %{"times" => 2, "agent" => false})
      refute_receive %Message{body: ^body, metadata: %{"event" => "rabbit.retry.failed1"}}, 1_000
    end

    test "setting on_failure to :keep should requeue" do
      body = random_string()
      publish_test_message("rabbit.retry.requeue1", body, %{"times" => 2})
      assert_receive %Message{body: ^body, metadata: %{"event" => "rabbit.retry.requeue1"}}, 1_000
    end

    test "setting on_failure to function/1 which return :drop should drop" do
      body = random_string()
      publish_test_message("rabbit.retry.failed2", body, %{"times" => 2, "agent" => false})
      refute_receive %Message{body: ^body, metadata: %{"event" => "rabbit.retry.failed2"}}, 1_000
    end

    test "setting on_failure to function/1 which return :keep should requeue" do
      body = random_string()
      publish_test_message("rabbit.retry.requeue2", body, %{"times" => 2})
      assert_receive %Message{body: ^body, metadata: %{"event" => "rabbit.retry.requeue2"}}, 1_000
    end

    test "setting on failure to function/1 which return others should logged and requeue" do
      body = random_string()
      publish_test_message("rabbit.retry.requeue3", body, %{"times" => 2})
      assert_receive %Message{body: ^body, metadata: %{"event" => "rabbit.retry.requeue3"}}, 1_000
    end
  end

  describe "Wrong return value" do
    test "not return :ok, :error, {:error, reason} will drop message immediately" do
      body = random_string()
      publish_test_message("rabbit.wrong1", body, %{"times" => 1, "agent" => false, bad_return: true})
      refute_receive %Message{body: ^body, metadata: %{"event" => "rabbit.wrong1"}}, 1_000
    end
  end

  describe "Multi functions in consume Success" do
    test "do it in sequence" do
      body = random_string()
      publish_test_message("rabbit.multi1", body)
      expected_body = String.reverse(body)
      assert_receive %Message{body: ^expected_body, metadata: %{"event" => "rabbit.multi1"}}, 1_000
    end
  end

  describe "Multi functions in consume wrong failure in the middle" do
    test "not return %Message{} with on_failure to :drop should drop immediately" do
      body = random_string()
      publish_test_message("rabbit.multi.failed1", body, %{"agent" => false})
      refute_receive %Message{body: ^body, metadata: %{"event" => "rabbit.multi.failed1"}}, 1_000
    end

    # DON'T KNOW HOW TO TEST IT
    test "not return %Message{} with on_failure to :keep should requeue" do
    end
  end

  describe "Multi functions in consume failure in last function" do
    test "not return :ok, :error or {:error, reason} should drop it immediately" do
      body = random_string()
      publish_test_message("rabbit.multi.failed3", body, %{"agent" => false})
      refute_receive %Message{body: ^body, metadata: %{"event" => "rabbit.multi.failed3"}}, 1_000
    end

    test ":error with on_failure to :drop should drop immediately" do
      body = random_string()
      publish_test_message("rabbit.multi.failed4", body, %{"agent" => false})
      refute_receive %Message{body: ^body, metadata: %{"event" => "rabbit.multi.failed4"}}, 1_000
    end

    test ":error with on_failure to :keep should requeue" do
      body = random_string()
      expected_body = String.reverse(body)
      publish_test_message("rabbit.multi.requeue2", body, %{"times" => 2})
      assert_receive %Message{body: ^expected_body, metadata: %{"event" => "rabbit.multi.requeue2"}}, 1_000
    end

    test ":error with on_failure to :drop should drop immediately after reach max retries" do
      body = random_string()
      publish_test_message("rabbit.multi.failed6", body, %{"times" => 100, "agent" => false})
      refute_receive %Message{body: ^body, metadata: %{"event" => "rabbit.multi.failed6"}}, 1_000
    end
  end

  describe "Validators" do
    test "Only global validator and :ok" do
      body = random_string()
      MedusaConfig.set_message_validator(:medusa_config, &always_ok/1)
      publish_test_message("rabbit.validator1", body)
      assert_receive %Message{body: ^body, metadata: %{"event" => "rabbit.validator1"}}, 1_000
    end

    test "Only global validator and {:error, reason}" do
      body = random_string()
      MedusaConfig.set_message_validator(:medusa_config, &always_error/1)
      publish_test_message("rabbit.validator2", body)
      refute_receive %Message{body: ^body, metadata: %{"event" => "rabbit.validator2"}}, 1_000
    end

    test "With 1 extra validator and both: ok" do
      body = random_string()
      MedusaConfig.set_message_validator(:medusa_config, &always_ok/1)
      publish_test_message("rabbit.validator3", body)
      assert_receive %Message{body: ^body, metadata: %{"event" => "rabbit.validator3"}}, 1_000
    end

    test "With only extra validator and :ok" do
      body = random_string()
      publish_test_message("rabbit.validator4", body)
      assert_receive %Message{body: ^body, metadata: %{"event" => "rabbit.validator4"}}, 1_000
    end

    test "With list of extra validators and all :ok" do
      body = random_string()
      MedusaConfig.set_message_validator(:medusa_config, &always_ok/1)
      publish_test_message("rabbit.validator5", body)
      assert_receive %Message{body: ^body, metadata: %{"event" => "rabbit.validator5"}}, 1_000
    end

    test "With error extra validator in the middle" do
      body = random_string()
      publish_test_message("rabbit.validator6", body)
      refute_receive %Message{body: ^body, metadata: %{"event" => "rabbit.validator6"}}, 1_000
    end

    test "With error extra validator in the end" do
      body = random_string()
      publish_test_message("rabbit.validator7", body)
      refute_receive %Message{body: ^body, metadata: %{"event" => "rabbit.validator7"}}, 1_000
    end

    test "With error extra validator in the global" do
      body = random_string()
      MedusaConfig.set_message_validator(:medusa_config, &always_error/1)
      publish_test_message("rabbit.validator8", body)
      refute_receive %Message{body: ^body, metadata: %{"event" => "rabbit.validator8"}}, 1_000
    end
  end

  defp always_ok(_message), do: :ok

  defp always_error(_message), do: :error

  defp always_drop(_message), do: :drop

  defp always_keep(_message), do: :keep

  defp random_string do
    32 |> :crypto.strong_rand_bytes |> Base.encode64
  end

  defp invalid_config(host) do
    [adapter: Medusa.Adapter.RabbitMQ,
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
