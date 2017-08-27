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
      topic = "rabbit.basic1"
      body = "foobar"
      capture_log(fn ->
        :ok = Medusa.consume(topic,
                             &forward_message_to_test/1,
                             queue_name: "rabbit_basic_test_1")
        :ok = Medusa.consume("*.basic1",
                             &forward_message_to_test/1,
                             queue_name: "rabbit_basic_test_*")
        Process.sleep(1_000)
        publish_test_message(topic, body, %{"extra" => "good"})
        assert_receive %Message{
          body: ^body,
          topic: ^topic,
          metadata: %{"extra" => "good"}}, 1_000
        assert_receive %Message{
          body: ^body,
          topic: ^topic,
          metadata: %{"extra" => "good"}}, 1_000
      end)
    end

    test "non-matched event should not received message" do
      topic = "ping"
      body = "pong"
      capture_log(fn ->
        publish_test_message(topic, body)
        refute_receive %Message{body: ^body}, 1_000
      end)
    end
  end

  describe "bind_once" do
    test "Send event to consumer with bind_once: true.
          consumer and producer should die" do
      topic = "rabbit.bind1"
      body = UUID.uuid4()
      capture_log(fn ->
        :ok = Medusa.consume(topic,
                             &forward_message_to_test/1,
                             queue_name: "rabbit_bind_test_1",
                             bind_once: true)
        Process.sleep(1_000)
        producer = Process.whereis(:"test-rabbitmq.rabbit_bind_test_1")
        assert producer
        consumers = :sys.get_state(producer).consumers
        assert consumers |> Map.keys() |> length == 1
        ref = Process.monitor(producer)
        publish_test_message(topic, body)
        assert_receive %Message{body: ^body, topic: ^topic}, 1_000
        assert_receive {:DOWN, ^ref, :process, _, :normal}, 1_000
        refute Process.whereis(:"test-rabbitmq.rabbit_bind_test_1")
      end)
    end

    test "Send event to consumer with bind_once: true should not kill other producer-consumer" do
      topic = "rabbit.bind2"
      body = UUID.uuid4()
      capture_log(fn ->
        :ok = Medusa.consume(topic,
                             &forward_message_to_test/1,
                             queue_name: "rabbit_bind_test_2")
        :ok = Medusa.consume(topic,
                             &forward_message_to_test/1,
                             queue_name: "rabbit_bind_test_2",
                             bind_once: true)
        Process.sleep(1_000)
        producer = Process.whereis(:"test-rabbitmq.rabbit_bind_test_2")
        assert producer
        consumers = :sys.get_state(producer).consumers
        assert consumers |> Map.keys() |> length == 2
        publish_test_message(topic, body)
        publish_test_message(topic, body)
        assert_receive %Message{body: ^body, topic: ^topic}, 1_000
        assert_receive %Message{body: ^body, topic: ^topic}, 1_000
        Process.sleep(100)
        consumers = :sys.get_state(producer).consumers
        assert consumers |> Map.keys() |> length == 1
      end)
    end
  end

  describe "muliple consumers" do
    test "spawn many consumers with consumers options" do
      topic = "rabbit.multi_consumer"
      Medusa.consume(topic,
                     &forward_message_to_test/1,
                     queue_name: "rabbit_multi_consumer_test", consumers: 10)
      Process.sleep(1_000)
      producer = Process.whereis(:"test-rabbitmq.rabbit_multi_consumer_test")
      consumers = :sys.get_state(producer).consumers
      assert consumers |> Map.keys() |> length == 10
      Medusa.consume(topic,
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
      put_rabbitmq_adapter_config(invalid_config "192.0.2.0")
      :timer.sleep 1_000
      refute Medusa.alive?()
      put_rabbitmq_adapter_config()
    end

    test "connectivity to invalid host with alive?" do
      put_rabbitmq_adapter_config(invalid_config "rabbitmq.invalid")
      :timer.sleep 1_000
      refute Medusa.alive?()
      put_rabbitmq_adapter_config()
    end
  end

  describe "re-publish" do
    test "publish when no connection is queue and resend when re-connected" do
      topic = "rabbit.republish"
      body = UUID.uuid4()
      adapter = Process.whereis(RabbitMQ)
      capture_log(fn ->
        :ok = Medusa.consume(topic,
                             &forward_message_to_test/1,
                             queue_name: "rabbit_republish_test")
        Process.sleep(1_000)
        path = [ Access.key!(:mod_state), Access.key!(:channel) ]
        :sys.replace_state(adapter, &put_in(&1, path, nil))
        assert publish_test_message(topic, body) ==
          {:error, "cannot connect rabbitmq"}
          assert publish_test_message(topic, body) ==
            {:error, "cannot connect rabbitmq"}
        send(adapter, {:DOWN, make_ref(), :process, self(), :test})
        assert_receive %Message{body: ^body, topic: ^topic}, 1_000
        assert_receive %Message{body: ^body, topic: ^topic}, 1_000
      end)
    end
  end

  describe "Retry on failure" do
    test "{:error, reason}" do
      topic = "rabbit.retry1"
      body = UUID.uuid4()
      capture_log(fn ->
        :ok = Medusa.consume(topic,
                             &message_to_test/1,
                             queue_name: "rabbit_retry_test_1",
                             max_retries: 1)
        Process.sleep(1_000)
        publish_test_message(topic, body, %{"times" => 1})
        assert_receive %Message{body: ^body, topic: ^topic}, 1_000
      end)
    end

    test "raise" do
      topic = "rabbit.retry2"
      body = UUID.uuid4()
      capture_log(fn ->
        :ok = Medusa.consume(topic,
                             &message_to_test/1,
                             queue_name: "rabbit_retry_test_2",
                             max_retries: 5)
        Process.sleep(1_000)
        publish_test_message(topic, body, %{"times" => 2, raise: true})
        assert_receive %Message{body: ^body, topic: ^topic}, 1_000
      end)
    end

    test "throw" do
      topic = "rabbit.retry3"
      body = UUID.uuid4()
      capture_log(fn ->
        :ok = Medusa.consume(topic,
                             &message_to_test/1,
                             queue_name: "rabbit_retry_test_3",
                             max_retries: 2)
        Process.sleep(1_000)
        publish_test_message(topic, body, %{"times" => 1, throw: true})
        assert_receive %Message{body: ^body, topic: ^topic}, 1_000
      end)
    end

    test "exit" do
      topic = "rabbit.retry4"
      body = UUID.uuid4()
      capture_log(fn ->
        :ok = Medusa.consume(topic,
                             &message_to_test/1,
                             queue_name: "rabbit_retry_test_4",
                             max_retries: 2)
        Process.sleep(1_000)
        publish_test_message(topic, body, %{"times" => 1, http_error: true})
        assert_receive %Message{body: ^body, topic: ^topic}, 1_000
      end)
    end
  end

  describe "When retry reach max_retries" do
    test "setting on_failure to :drop should drop" do
      topic = "rabbit.retry.failed1"
      body = UUID.uuid4()
      capture_log(fn ->
        :ok = Medusa.consume(topic,
                             &message_to_test/1,
                             queue_name: "rabbit_retry_failed_test_1",
                             max_retries: 1,
                             on_failure: :drop)
        Process.sleep(1_000)
        publish_test_message(topic, body, %{"times" => 2, "agent" => false})
        refute_receive %Message{body: ^body, topic: ^topic}, 1_000
      end)
    end

    test "setting on_failure to :keep should requeue" do
      topic = "rabbit.retry.requeue1"
      body = UUID.uuid4()
      capture_log(fn ->
        :ok = Medusa.consume("rabbit.retry.requeue1",
                             &message_to_test/1,
                             queue_name: "rabbit_retry_requeue_test_1",
                             max_retries: 1,
                             on_failure: :keep)
        Process.sleep(1_000)
        publish_test_message(topic, body, %{"times" => 2})
        assert_receive %Message{body: ^body, topic: ^topic}, 1_000
      end)
    end

    test "setting on_failure to function/2 which return :drop should drop" do
      topic = "rabbit.retry.failed2"
      body = UUID.uuid4()
      capture_log(fn ->
        :ok = Medusa.consume(topic,
                             &message_to_test/1,
                             queue_name: "rabbit_retry_failed_test_2",
                             max_retries: 1,
                             on_failure: &always_drop/2)
        Process.sleep(1_000)
        publish_test_message(topic, body, %{"times" => 2, "agent" => false})
        refute_receive %Message{body: ^body, topic: ^topic}, 1_000
      end)
    end

    test "setting on_failure to function/2 which return :keep should requeue" do
      topic = UUID.uuid4()
      body = UUID.uuid4()
      capture_log(fn ->
        :ok = Medusa.consume(topic,
                             &message_to_test/1,
                             queue_name: UUID.uuid4(),
                             max_retries: 1,
                             on_failure: &always_keep/2)
        Process.sleep(1_000)
        publish_test_message(topic, body, %{"times" => 2})
        assert_receive %Message{body: ^body, topic: ^topic}, 1_000
      end)
    end

    test "setting on failure to function/2 which return others should logged and requeue" do
      topic = UUID.uuid4()
      body = UUID.uuid4()
      capture_log(fn ->
        :ok = Medusa.consume(topic,
                             &message_to_test/1,
                             queue_name: UUID.uuid4(),
                             max_retries: 1,
                             on_failure: &always_ok/2)
        Process.sleep(1_000)
        publish_test_message(topic, body, %{"times" => 2})
        assert_receive %Message{body: ^body, topic: ^topic}, 1_000
      end)
    end
  end

  describe "Wrong return value" do
    test "not return :ok, :error, {:error, reason} will drop message immediately" do
      topic = UUID.uuid4()
      body = UUID.uuid4()
      capture_log(fn ->
        :ok = Medusa.consume(topic,
                             &message_to_test/1,
                             queue_name: UUID.uuid4(),
                             max_retries: 1,
                             on_failure: :keep)
        Process.sleep(1_000)
        publish_test_message(topic, body, %{"times" => 1, "agent" => false, bad_return: true})
        refute_receive %Message{body: ^body, topic: ^topic}, 1_000
      end)
    end
  end

  describe "Multi functions in consume Success" do
    test "do it in sequence" do
      topic = UUID.uuid4()
      body = UUID.uuid4()
      capture_log(fn ->
        :ok = Medusa.consume(topic,
                             [&reverse_message/1, &forward_message_to_test/1],
                             queue_name: UUID.uuid4())
        Process.sleep(1_000)
        publish_test_message(topic, body)
        expected_body = String.reverse(body)
        assert_receive %Message{body: ^expected_body, topic: ^topic}, 1_000
      end)
    end
  end

  describe "Multi functions in consume wrong failure in the middle" do
    test "not return %Message{} should retry" do
      topic = UUID.uuid4()
      body = UUID.uuid4()
      capture_log(fn ->
        :ok = Medusa.consume(topic,
                             [&message_to_test/1, &forward_message_to_test/1],
                             queue_name: UUID.uuid4(),
                             on_failure: :drop)
        Process.sleep(1_000)
        publish_test_message(topic, body, %{"times" => 1, "middleware" => true})
        assert_receive %Message{body: ^body, topic: ^topic}, 1_000
      end)
    end

    test "not return %Message{} with on_failure to :drop should drop after retries" do
      topic = UUID.uuid4()
      body = UUID.uuid4()
      capture_log(fn ->
        :ok = Medusa.consume(topic,
                             [&message_to_test/1, &forward_message_to_test/1],
                             queue_name: UUID.uuid4(),
                             max_retries: 1,
                             on_failure: :drop)
        Process.sleep(1_000)
        publish_test_message(topic, body, %{"times" => 2, "middleware" => true})
        refute_receive %Message{body: ^body, topic: ^topic}, 1_000
      end)
    end

    test "not return %Message{} with on_failure to :keep should requeue" do
      topic = UUID.uuid4()
      body = UUID.uuid4()
      capture_log(fn ->
        :ok = Medusa.consume(topic,
                             [&message_to_test/1, &forward_message_to_test/1],
                             queue_name: UUID.uuid4(),
                             max_retries: 1,
                             on_failure: :keep)
        Process.sleep(1_000)
        publish_test_message(topic, body, %{"times" => 2, "middleware" => true})
        assert_receive %Message{body: ^body, topic: ^topic}, 1_000
      end)
    end
  end

  describe "Multi functions in consume failure in last function" do
    test "not return :ok, :error or {:error, reason} should drop it immediately" do
      topic = UUID.uuid4()
      body = UUID.uuid4()
      capture_log(fn ->
        :ok = Medusa.consume(topic,
                             [&reverse_message/1, &reverse_message/1],
                             queue_name: UUID.uuid4(),
                             on_failure: :keep)
        Process.sleep(1_000)
        publish_test_message(topic, body, %{"agent" => false})
        refute_receive %Message{body: ^body, topic: ^topic}, 1_000
      end)
    end

    test ":error with on_failure to :drop should drop immediately" do
      topic = UUID.uuid4()
      body = UUID.uuid4()
      capture_log(fn ->
        :ok = Medusa.consume(topic,
                             [&reverse_message/1, &error_message/1],
                             queue_name: UUID.uuid4(),
                             on_failure: :drop)
        Process.sleep(1_000)
        publish_test_message(topic, body, %{"agent" => false})
        refute_receive %Message{body: ^body, topic: ^topic}, 1_000
      end)
    end

    test ":error with on_failure to :keep should requeue" do
      topic = UUID.uuid4()
      body = UUID.uuid4()
      capture_log(fn ->
        :ok = Medusa.consume(topic,
                             [&reverse_message/1, &message_to_test/1],
                             queue_name: UUID.uuid4(),
                             on_failure: :keep)
        Process.sleep(1_000)
        expected_body = String.reverse(body)
        publish_test_message(topic, body, %{"times" => 2})
        assert_receive %Message{body: ^expected_body, topic: ^topic}, 1_000
      end)
    end

    test ":error with on_failure to :drop should drop immediately after reach max retries" do
      topic = UUID.uuid4()
      body = UUID.uuid4()
      capture_log(fn ->
        :ok = Medusa.consume(topic,
                             [&reverse_message/1, &message_to_test/1],
                             queue_name: UUID.uuid4(),
                             on_failure: :drop)
        Process.sleep(1_000)
        publish_test_message(topic, body, %{"times" => 100, "agent" => false})
        refute_receive %Message{body: ^body, topic: ^topic}, 1_000
      end)
    end
  end

  describe "Validators" do
    test "Only global validator and :ok" do
      topic = UUID.uuid4()
      body = UUID.uuid4()
      capture_log(fn ->
        :ok = Medusa.consume(topic,
                             &forward_message_to_test/1,
                             queue_name: UUID.uuid4())
        Process.sleep(1_000)
        MedusaConfig.set_message_validator(:medusa_config, &always_ok/1)
        publish_test_message(topic, body)
        assert_receive %Message{body: ^body, topic: ^topic}, 1_000
      end)
    end

    test "Only global validator and {:error, reason}" do
      topic = UUID.uuid4()
      body = UUID.uuid4()
      capture_log(fn ->
        :ok = Medusa.consume(topic,
                             &forward_message_to_test/1,
                             queue_name: UUID.uuid4())
        Process.sleep(1_000)
        MedusaConfig.set_message_validator(:medusa_config, &always_error/1)
        publish_test_message(topic, body)
        refute_receive %Message{body: ^body, topic: ^topic}, 1_000
      end)
    end

    test "With 1 extra validator and both: ok" do
      topic = UUID.uuid4()
      body = UUID.uuid4()
      capture_log(fn ->
        :ok = Medusa.consume(topic,
                             &forward_message_to_test/1,
                             queue_name: UUID.uuid4(),
                             message_validators: &always_ok/1)
        Process.sleep(1_000)
        MedusaConfig.set_message_validator(:medusa_config, &always_ok/1)
        publish_test_message(topic, body)
        assert_receive %Message{body: ^body, topic: ^topic}, 1_000
      end)
    end

    test "With only extra validator and :ok" do
      topic = UUID.uuid4()
      body = UUID.uuid4()
      capture_log(fn ->
        :ok = Medusa.consume(topic,
                             &forward_message_to_test/1,
                             queue_name: UUID.uuid4(),
                             message_validators: &always_ok/1)
        Process.sleep(1_000)
        publish_test_message(topic, body)
        assert_receive %Message{body: ^body, topic: ^topic}, 1_000
      end)
    end

    test "With list of extra validators and all :ok" do
      topic = UUID.uuid4()
      body = UUID.uuid4()
      capture_log(fn ->
        :ok = Medusa.consume(topic,
                             &forward_message_to_test/1,
                             queue_name: UUID.uuid4(),
                             message_validators: [&always_ok/1, &always_ok/1])
        Process.sleep(1_000)
        MedusaConfig.set_message_validator(:medusa_config, &always_ok/1)
        publish_test_message(topic, body)
        assert_receive %Message{body: ^body, topic: ^topic}, 1_000
      end)
    end

    test "With error extra validator in the middle" do
      topic = UUID.uuid4()
      body = UUID.uuid4()
      capture_log(fn ->
        :ok = Medusa.consume(topic,
                             &forward_message_to_test/1,
                             queue_name: UUID.uuid4(),
                             message_validators: [&always_error/1, &always_ok/1])
        Process.sleep(1_000)
        publish_test_message(topic, body)
        refute_receive %Message{body: ^body, topic: ^topic}, 1_000
      end)
    end

    test "With error extra validator in the end" do
      topic = UUID.uuid4()
      body = UUID.uuid4()
      capture_log(fn ->
        :ok = Medusa.consume(topic,
                             &forward_message_to_test/1,
                             queue_name: UUID.uuid4(),
                             message_validators: [&always_ok/1, &always_error/1])
        Process.sleep(1_000)
        publish_test_message(topic, body)
        refute_receive %Message{body: ^body, topic: ^topic}, 1_000
      end)
    end

    test "With error extra validator in the global" do
      topic = UUID.uuid4()
      body = UUID.uuid4()
      capture_log(fn ->
        :ok = Medusa.consume(topic,
                             &forward_message_to_test/1,
                             queue_name: UUID.uuid4(),
                             message_validators: [&always_ok/1, &always_ok/1])
        Process.sleep(1_000)
        MedusaConfig.set_message_validator(:medusa_config, &always_error/1)
        publish_test_message(topic, body)
        refute_receive %Message{body: ^body, topic: ^topic}, 1_000
      end)
    end
  end

  describe "Logging" do
    test "publish success" do
      topic = UUID.uuid4()
      id = UUID.uuid4()
      request_id = UUID.uuid4()
      origin = UUID.uuid4()
      body = UUID.uuid4()
      assert [%{
        "message_id" => ^id,
        "origin" => ^origin,
        "request_id" => ^request_id,
        "routing_key" => nil,
        "topic" => ^topic
      }] = capture_log(fn ->
        metadata = %{
          "id" => id,
          "request_id" => request_id,
          "origin" => origin
        }
        publish_test_message(topic, body, metadata)
      end) |> decode_logger_message
    end
  end

    test "consume success" do
      topic = UUID.uuid4()
      id = UUID.uuid4()
      request_id = UUID.uuid4()
      origin = UUID.uuid4()
      body = UUID.uuid4()
      messages = capture_log(fn ->
        :ok = Medusa.consume(topic,
                             &forward_message_to_test/1,
                             queue_name: UUID.uuid4())
        Process.sleep(1_000)
        metadata = %{
          "id" => id,
          "request_id" => request_id,
          "origin" => origin
        }
        publish_test_message(topic, body, metadata)
        assert_receive %Message{
          body: ^body,
          topic: ^topic}, 1_000
      end) |> decode_logger_message

      messages = Enum.filter(messages, & &1["routing_key"]) # only consume message
      assert [%{
        "message_id" => ^id,
        "origin" => ^origin,
        "processing_time" => _,
        "request_id" => ^request_id,
        "routing_key" => ^topic,
        "topic" => ^topic
      }] = messages
    end

  describe "exception hook" do
    test "callback raise exception" do
      topic = UUID.uuid4()
      capture_log(fn ->
        :ok = Medusa.consume(topic,
                             &message_to_test/1,
                             queue_name: UUID.uuid4(),
                             on_exception: &on_exception/3,
                             max_retries: 0,
                             on_failure: :drop)
        Process.sleep(1_000)
        publish_test_message(topic, %{}, %{"times" => 1, raise: true})
      end)
      assert_receive %{"error" => _, "topic" => ^topic}
    end

    test "callback throw exception" do
      topic = UUID.uuid4()
      capture_log(fn ->
        :ok = Medusa.consume(topic,
                             &message_to_test/1,
                             queue_name: UUID.uuid4(),
                             on_exception: &on_exception/3,
                             max_retries: 0,
                             on_failure: :drop)
        Process.sleep(1_000)
        publish_test_message(topic, %{}, %{"times" => 1, throw: true})
      end)
      assert_receive %{"error" => _, "topic" => ^topic}
    end
  end

  defp always_ok(_message), do: :ok

  defp always_ok(_message, _reason), do: :ok

  defp always_error(_message), do: :error

  defp always_drop(_message, _reason), do: :drop

  defp always_keep(_message, _reason), do: :keep

  defp on_exception(message, reason, _stacktrace) do
    from = message.metadata["from"] |> :erlang.list_to_pid
    send(from, %{
      "topic" => message.topic,
      "error" => reason,
      "id" => message.metadata["id"]
    })
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
