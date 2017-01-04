defmodule Medusa.TestHelper do
  @moduledoc false

  def restart_app do
    Application.stop(:medusa)
    Application.ensure_all_started(:medusa)
  end

  def put_adapter_config(adapter) do
    import Supervisor.Spec, warn: false
    opts = [
      adapter: adapter,
      group: "test-rabbitmq",
      retry_publish_backoff: 500,
      retry_publish_max: 1,
      retry_consume_pow_base: 0,
    ]
    Application.put_env(:medusa, Medusa, opts, persistent: true)
    restart_app()
    opts
  end

  def put_rabbitmq_adapter_config do
    import Supervisor.Spec, warn: false
    opts = [
      adapter: Medusa.Adapter.RabbitMQ,
      group: "test-rabbitmq",
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
    ]
    Application.put_env(:medusa, Medusa, opts, persistent: true)
    restart_app()
    opts
  end

  def consumer_children do
    Supervisor.which_children(Medusa.ConsumerSupervisor)
  end

  def producer_children do
    Supervisor.which_children(Medusa.ProducerSupervisor)
  end

  def pid_to_list(pid) when is_pid(pid) do
    :erlang.pid_to_list(pid)
  end

  def list_to_pid(list) when is_list(list) do
    :erlang.list_to_pid(list)
  end

end

defmodule MyModule do
  alias Medusa.Message
  import Medusa.TestHelper

  def echo(message) do
    message.metadata["from"] |> send_message_back(message)
    :ok
  end

  def error(_) do
    :error
  end

  def reverse(%Message{body: body} = message) do
    %{message | body: String.reverse(body)}
  end

  def state(%{metadata: %{"agent" => agent,
                          "times" => times,
                          "from" => from} = metadata} = message) do
    val = agent |> String.to_atom |> Agent.get_and_update(&({&1, &1+1}))
    cond do
      metadata["bad_return"] ->
        :bad_return
      val == times && metadata["middleware"] ->
        message
      val == times ->
        send_message_back(from, message)
        :ok
      metadata["raise"] ->
        raise "Boom!"
      metadata["throw"] ->
        throw "Bamm!"
      metadata["http_error"] ->
        :gen_tcp.connect('bogus url', 80, [])
      true ->
        {:error, val}
    end
  end

  defp send_message_back(list, message) when is_list(list) do
    pid = list_to_pid(list)
    if Process.alive?(pid), do: send(pid, message)
  end
end
