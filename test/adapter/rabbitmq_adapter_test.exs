defmodule Medusa.Adapter.RabbitMQTest do
  use ExUnit.Case, async: false
  import Medusa.TestHelper
  import ExUnit.CaptureLog
  alias Medusa.Message
  alias Medusa.Adapter.RabbitMQ

  setup_all do
    put_rabbitmq_adapter_config()
    :ok
  end

  setup do
    MedusaConfig.set_message_validator(:medusa_config, nil)
    delete_all_queues()
    :ok
  end

  describe "send messages" do
    test "matched event should received message" do
      :ok = Medusa.consume("rabbit.basic1",
                           &forward_message_to_test/1,
                           queue_name: "rabbit_basic_test_1")
      :ok = Medusa.consume("*.basic1",
                           &forward_message_to_test/1,
                           queue_name: "rabbit_basic_test_*")
      Process.sleep(1_000)
      body = "foobar"
      publish_test_message("rabbit.basic1", body, %{"extra" => "good"})
      assert_receive %Message{
        body: ^body,
        topic: "rabbit.basic1",
        metadata: %{"extra" => "good"}}, 1_000
      assert_receive %Message{
        body: ^body,
        topic: "rabbit.basic1",
        metadata: %{"extra" => "good"}}, 1_000
    end

    test "non-matched event should not received message" do
      publish_test_message("ping", "ping")
      refute_receive %Message{body: "ping"}, 1_000
    end
  end

  describe "bind_once" do
    test "Send event to consumer with bind_once: true.
          consumer and producer should die" do
      :ok = Medusa.consume("rabbit.bind1",
                           &forward_message_to_test/1,
                           queue_name: "rabbit_bind_test_1",
                           bind_once: true)
      Process.sleep(1_000)
      producer = Process.whereis(:"test-rabbitmq.rabbit_bind_test_1")
      assert producer
      consumers = :sys.get_state(producer).consumers
      assert consumers |> Map.keys() |> length == 1
      ref = Process.monitor(producer)
      publish_test_message("rabbit.bind1", "die both")
      assert_receive %Message{body: "die both", topic: "rabbit.bind1"}, 1_000
      assert_receive {:DOWN, ^ref, :process, _, :normal}, 1_000
      refute Process.whereis(:"test-rabbitmq.rabbit_bind_test_1")
    end

    test "Send event to consumer with bind_once: true should not kill other producer-consumer" do
      :ok = Medusa.consume("rabbit.bind2",
                           &forward_message_to_test/1,
                           queue_name: "rabbit_bind_test_2")
      :ok = Medusa.consume("rabbit.bind2",
                           &forward_message_to_test/1,
                           queue_name: "rabbit_bind_test_2",
                           bind_once: true)
      Process.sleep(1_000)
      producer = Process.whereis(:"test-rabbitmq.rabbit_bind_test_2")
      assert producer
      consumers = :sys.get_state(producer).consumers
      assert consumers |> Map.keys() |> length == 2
      publish_test_message("rabbit.bind2", "die 1 consumer")
      publish_test_message("rabbit.bind2", "die 1 consumer")
      assert_receive %Message{body: "die 1 consumer", topic: "rabbit.bind2"}, 1_000
      assert_receive %Message{body: "die 1 consumer", topic: "rabbit.bind2"}, 1_000
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
      :ok = Medusa.consume("rabbit.republish",
                           &forward_message_to_test/1,
                           queue_name: "rabbit_republish_test")
      Process.sleep(1_000)
      adapter = Process.whereis(RabbitMQ)
      path = [ Access.key!(:mod_state), Access.key!(:channel) ]
      :sys.replace_state(adapter, &put_in(&1, path, nil))
      assert publish_test_message("rabbit.republish", "foo") ==
        {:error, "cannot connect rabbitmq"}
      assert publish_test_message("rabbit.republish", "bar") ==
        {:error, "cannot connect rabbitmq"}
      send(adapter, {:DOWN, make_ref(), :process, self(), :test})
      assert_receive %Message{body: "foo", topic: "rabbit.republish"}, 1_000
      assert_receive %Message{body: "bar", topic: "rabbit.republish"}, 1_000
    end
  end

  describe "Retry on failure" do
    test "{:error, reason}" do
      :ok = Medusa.consume("rabbit.retry1",
                           &message_to_test/1,
                           queue_name: "rabbit_retry_test_1",
                           max_retries: 1)
      Process.sleep(1_000)
      body = random_string()
      publish_test_message("rabbit.retry1", body, %{"times" => 1})
      assert_receive %Message{body: ^body, topic: "rabbit.retry1"}, 1_000
    end

    test "raise" do
      :ok = Medusa.consume("rabbit.retry2",
                           &message_to_test/1,
                           queue_name: "rabbit_retry_test_2",
                           max_retries: 5)
      Process.sleep(1_000)
      body = random_string()
      publish_test_message("rabbit.retry2", body, %{"times" => 2, raise: true})
      assert_receive %Message{body: ^body, topic: "rabbit.retry2"}, 1_000
    end

    test "throw" do
      :ok = Medusa.consume("rabbit.retry3",
                           &message_to_test/1,
                           queue_name: "rabbit_retry_test_3",
                           max_retries: 2)
      Process.sleep(1_000)
      body = random_string()
      publish_test_message("rabbit.retry3", body, %{"times" => 1, throw: true})
      assert_receive %Message{body: ^body, topic: "rabbit.retry3"}, 1_000
    end

    test "exit" do
      :ok = Medusa.consume("rabbit.retry4",
                           &message_to_test/1,
                           queue_name: "rabbit_retry_test_4",
                           max_retries: 2)
      Process.sleep(1_000)
      body = random_string()
      publish_test_message("rabbit.retry4", body, %{"times" => 1, http_error: true})
      assert_receive %Message{body: ^body, topic: "rabbit.retry4"}, 1_000
    end
  end

  describe "When retry reach max_retries" do
    test "setting on_failure to :drop should drop" do
      :ok = Medusa.consume("rabbit.retry.failed1",
                           &message_to_test/1,
                           queue_name: "rabbit_retry_failed_test_1",
                           max_retries: 1,
                           on_failure: :drop)
      Process.sleep(1_000)
      body = random_string()
      publish_test_message("rabbit.retry.failed1", body, %{"times" => 2, "agent" => false})
      refute_receive %Message{body: ^body, topic: "rabbit.retry.failed1"}, 1_000
    end

    test "setting on_failure to :keep should requeue" do
      :ok = Medusa.consume("rabbit.retry.requeue1",
                           &message_to_test/1,
                           queue_name: "rabbit_retry_requeue_test_1",
                           max_retries: 1,
                           on_failure: :keep)
      Process.sleep(1_000)
      body = random_string()
      publish_test_message("rabbit.retry.requeue1", body, %{"times" => 2})
      assert_receive %Message{body: ^body, topic: "rabbit.retry.requeue1"}, 1_000
    end

    test "setting on_failure to function/1 which return :drop should drop" do
      :ok = Medusa.consume("rabbit.retry.failed2",
                           &message_to_test/1,
                           queue_name: "rabbit_retry_failed_test_2",
                           max_retries: 1,
                           on_failure: &always_drop/1)
      Process.sleep(1_000)
      body = random_string()
      publish_test_message("rabbit.retry.failed2", body, %{"times" => 2, "agent" => false})
      refute_receive %Message{body: ^body, topic: "rabbit.retry.failed2"}, 1_000
    end

    test "setting on_failure to function/1 which return :keep should requeue" do
      :ok = Medusa.consume("rabbit.retry.requeue2",
                           &message_to_test/1,
                           queue_name: "rabbit_retry_requeue_test_2",
                           max_retries: 1,
                           on_failure: &always_keep/1)
      Process.sleep(1_000)
      body = random_string()
      publish_test_message("rabbit.retry.requeue2", body, %{"times" => 2})
      assert_receive %Message{body: ^body, topic: "rabbit.retry.requeue2"}, 1_000
    end

    test "setting on failure to function/1 which return others should logged and requeue" do
      :ok = Medusa.consume("rabbit.retry.requeue3",
                           &message_to_test/1,
                           queue_name: "rabbit_retry_requeue_test_3",
                           max_retries: 1,
                           on_failure: &always_ok/1)
      Process.sleep(1_000)
      body = random_string()
      publish_test_message("rabbit.retry.requeue3", body, %{"times" => 2})
      assert_receive %Message{body: ^body, topic: "rabbit.retry.requeue3"}, 1_000
    end
  end

  describe "Wrong return value" do
    test "not return :ok, :error, {:error, reason} will drop message immediately" do
      :ok = Medusa.consume("rabbit.wrong1",
                           &message_to_test/1,
                           queue_name: "rabbit_wrong_test_1",
                           max_retries: 1,
                           on_failure: :keep)
      Process.sleep(1_000)
      body = random_string()
      publish_test_message("rabbit.wrong1", body, %{"times" => 1, "agent" => false, bad_return: true})
      refute_receive %Message{body: ^body, topic: "rabbit.wrong1"}, 1_000
    end
  end

  describe "Multi functions in consume Success" do
    test "do it in sequence" do
      :ok = Medusa.consume("rabbit.multi1",
                           [&reverse_message/1, &forward_message_to_test/1],
                           queue_name: "rabbit_multi_test_1")
      Process.sleep(1_000)
      body = random_string()
      publish_test_message("rabbit.multi1", body)
      expected_body = String.reverse(body)
      assert_receive %Message{body: ^expected_body, topic: "rabbit.multi1"}, 1_000
    end
  end

  describe "Multi functions in consume wrong failure in the middle" do
    test "not return %Message{} with on_failure to :drop should drop immediately" do
      :ok = Medusa.consume("rabbit.multi.failed1",
                           [&error_message/1, &forward_message_to_test/1],
                           queue_name: "rabbit_multi_failed_test_1",
                           on_failure: :drop)
      Process.sleep(1_000)
      body = random_string()
      publish_test_message("rabbit.multi.failed1", body, %{"agent" => false})
      refute_receive %Message{body: ^body, topic: "rabbit.multi.failed1"}, 1_000
    end

    # DON'T KNOW HOW TO TEST IT
    test "not return %Message{} with on_failure to :keep should requeue" do
    end
  end

  describe "Multi functions in consume failure in last function" do
    test "not return :ok, :error or {:error, reason} should drop it immediately" do
      :ok = Medusa.consume("rabbit.multi.failed3",
                           [&reverse_message/1, &reverse_message/1],
                           queue_name: "rabbit_multi_failed_test_3",
                           on_failure: :keep)
      Process.sleep(1_000)
      body = random_string()
      publish_test_message("rabbit.multi.failed3", body, %{"agent" => false})
      refute_receive %Message{body: ^body, topic: "rabbit.multi.failed3"}, 1_000
    end

    test ":error with on_failure to :drop should drop immediately" do
      :ok = Medusa.consume("rabbit.multi.failed4",
                           [&reverse_message/1, &error_message/1],
                           queue_name: "rabbit_multi_failed_test_4",
                           on_failure: :drop)
      Process.sleep(1_000)
      body = random_string()
      publish_test_message("rabbit.multi.failed4", body, %{"agent" => false})
      refute_receive %Message{body: ^body, topic: "rabbit.multi.failed4"}, 1_000
    end

    test ":error with on_failure to :keep should requeue" do
      :ok = Medusa.consume("rabbit.multi.requeue2",
                           [&reverse_message/1, &message_to_test/1],
                           queue_name: "rabbit_multi_requeue_test_2",
                           on_failure: :keep)
      Process.sleep(1_000)
      body = random_string()
      expected_body = String.reverse(body)
      publish_test_message("rabbit.multi.requeue2", body, %{"times" => 2})
      assert_receive %Message{body: ^expected_body, topic: "rabbit.multi.requeue2"}, 1_000
    end

    test ":error with on_failure to :drop should drop immediately after reach max retries" do
      :ok = Medusa.consume("rabbit.multi.failed6",
                           [&reverse_message/1, &message_to_test/1],
                           queue_name: "rabbit_multi_failed_test_6",
                           on_failure: :drop)
      Process.sleep(1_000)
      body = random_string()
      publish_test_message("rabbit.multi.failed6", body, %{"times" => 100, "agent" => false})
      refute_receive %Message{body: ^body, topic: "rabbit.multi.failed6"}, 1_000
    end
  end

  describe "Validators" do
    test "Only global validator and :ok" do
      :ok = Medusa.consume("rabbit.validator1",
                           &forward_message_to_test/1,
                           queue_name: "rabbit_validator_test_1")
      Process.sleep(1_000)
      body = random_string()
      MedusaConfig.set_message_validator(:medusa_config, &always_ok/1)
      publish_test_message("rabbit.validator1", body)
      assert_receive %Message{body: ^body, topic: "rabbit.validator1"}, 1_000
    end

    test "Only global validator and {:error, reason}" do
      :ok = Medusa.consume("rabbit.validator2",
                           &forward_message_to_test/1,
                           queue_name: "rabbit_validator_test_2")
      Process.sleep(1_000)
      body = random_string()
      MedusaConfig.set_message_validator(:medusa_config, &always_error/1)
      publish_test_message("rabbit.validator2", body)
      refute_receive %Message{body: ^body, topic: "rabbit.validator2"}, 1_000
    end

    test "With 1 extra validator and both: ok" do
      :ok = Medusa.consume("rabbit.validator3",
                           &forward_message_to_test/1,
                           queue_name: "rabbit_validator_test_3",
                           message_validators: &always_ok/1)
      Process.sleep(1_000)
      body = random_string()
      MedusaConfig.set_message_validator(:medusa_config, &always_ok/1)
      publish_test_message("rabbit.validator3", body)
      assert_receive %Message{body: ^body, topic: "rabbit.validator3"}, 1_000
    end

    test "With only extra validator and :ok" do
      :ok = Medusa.consume("rabbit.validator4",
                           &forward_message_to_test/1,
                           queue_name: "rabbit_validator_test_4",
                           message_validators: &always_ok/1)
      Process.sleep(1_000)
      body = random_string()
      publish_test_message("rabbit.validator4", body)
      assert_receive %Message{body: ^body, topic: "rabbit.validator4"}, 1_000
    end

    test "With list of extra validators and all :ok" do
      :ok = Medusa.consume("rabbit.validator5",
                           &forward_message_to_test/1,
                           queue_name: "rabbit_validator_test_5",
                           message_validators: [&always_ok/1, &always_ok/1])
      Process.sleep(1_000)
      body = random_string()
      MedusaConfig.set_message_validator(:medusa_config, &always_ok/1)
      publish_test_message("rabbit.validator5", body)
      assert_receive %Message{body: ^body, topic: "rabbit.validator5"}, 1_000
    end

    test "With error extra validator in the middle" do
      :ok = Medusa.consume("rabbit.validator6",
                           &forward_message_to_test/1,
                           queue_name: "rabbit_validator_test_6",
                           message_validators: [&always_error/1, &always_ok/1])
      Process.sleep(1_000)
      body = random_string()
      publish_test_message("rabbit.validator6", body)
      refute_receive %Message{body: ^body, topic: "rabbit.validator6"}, 1_000
    end

    test "With error extra validator in the end" do
      :ok = Medusa.consume("rabbit.validator7",
                           &forward_message_to_test/1,
                           queue_name: "rabbit_validator_test_7",
                           message_validators: [&always_ok/1, &always_error/1])
      Process.sleep(1_000)
      body = random_string()
      publish_test_message("rabbit.validator7", body)
      refute_receive %Message{body: ^body, topic: "rabbit.validator7"}, 1_000
    end

    test "With error extra validator in the global" do
      :ok = Medusa.consume("rabbit.validator8",
                           &forward_message_to_test/1,
                           queue_name: "rabbit_validator_test_8",
                           message_validators: [&always_ok/1, &always_ok/1])
      Process.sleep(1_000)
      body = random_string()
      MedusaConfig.set_message_validator(:medusa_config, &always_error/1)
      publish_test_message("rabbit.validator8", body)
      refute_receive %Message{body: ^body, topic: "rabbit.validator8"}, 1_000
    end
  end

  # describe "Logging" do
  #   test "publish cannot connect rabbitmq" do
  #     adapter = Process.whereis(RabbitMQ)
  #     path = [ Access.key!(:mod_state), Access.key!(:channel) ]
  #     :sys.replace_state(adapter, &put_in(&1, path, nil))
  #     error_message = capture_log(fn ->
  #       publish_test_message("rabbit.logging.publish1", "HELLO",
  #                            %{"request_id" => "1", "origin" => "medusa"})
  #     end)
  #     assert error_message =~ "\"timestamp\""
  #     assert error_message =~ "\"level\":\"error\""
  #     assert error_message =~ "\"routing_key\":\"rabbit.logging.publish1\""
  #     assert error_message =~ "\"reason\":\"cannot connect rabbitmq\""
  #     assert error_message =~ "\"rabbitmq_host\":\"#{rabbitmq_connection()[:host]}\""
  #     assert error_message =~ "\"rabbitmq_port\":#{rabbitmq_connection()[:port]}"
  #     assert error_message =~ "\"request_id\":\"1\""
  #     assert error_message =~ "\"origin\":\"medusa\""
  #   end
  #
  #   test "publish success" do
  #     info_message = capture_log(fn ->
  #       publish_test_message("rabbit.logging.publish2", "HELLO", %{"request_id" => "2", "origin" => "medusa"})
  #     end)
  #     assert info_message =~ "\"level\":\"info\""
  #     assert info_message =~ "\"routing_key\":\"rabbit.logging.publish2\""
  #     assert info_message =~ "\"rabbitmq_host\":\"#{rabbitmq_connection()[:host]}\""
  #     assert info_message =~ "\"rabbitmq_port\":#{rabbitmq_connection()[:port]}"
  #     assert info_message =~ "\"body\":\"HELLO\""
  #     assert info_message =~ "\"request_id\":\"2\""
  #     assert info_message =~ "\"origin\":\"medusa\""
  #   end
  # end



  defp always_ok(_message), do: :ok

  defp always_error(_message), do: :error

  defp always_drop(_message), do: :drop

  defp always_keep(_message), do: :keep

  defp random_string do
    32 |> :crypto.strong_rand_bytes |> Base.encode64
  end

  defp rabbitmq_connection do
    Application.get_env(:medusa, Medusa)[:RabbitMQ][:connection]
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
