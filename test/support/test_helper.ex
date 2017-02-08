defmodule Medusa.TestHelper do
  @moduledoc false
  require Logger

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
      ]}]
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

  def publish_test_message(event, body, metadata \\ %{}) do
    new_metadata = Map.put(metadata, "from", :erlang.pid_to_list(self()))
    Medusa.publish(event, body, new_metadata)
  end

  def error_message(_) do
    :error
  end

  def reverse_message(%Medusa.Message{body: body} = message) do
    %{message | body: String.reverse(body)}
  end

  def forward_message_to_test(%{metadata: %{"from" => from}} = message) when is_list(from) do
    pid = :erlang.list_to_pid(from)
    case Process.alive?(pid) do
      true -> send(pid, message)
      false -> Logger.warn("CONSUMER PROCESS NOT ALIVE!")
    end
    :ok
  end

  def message_to_test(%{metadata: %{
                          "times" => times,
                          "from" => from} = metadata} = message) do
    retry = metadata["retry"] || 0
    cond do
      metadata["bad_return"] ->
        :bad_return
      retry == times && metadata["middleware"] ->
        message
      retry == times ->
        forward_message_to_test(message)
      metadata["raise"] ->
        raise "Boom!"
      metadata["throw"] ->
        throw "Bamm!"
      metadata["http_error"] ->
        :gen_tcp.connect('bogus url', 80, [])
      true ->
        {:error, retry}
    end
  end

    def delete_all_queues() do
    rabbit_conf = Application.get_env(:medusa, Medusa)[:"RabbitMQ"]
    admin_conf = rabbit_conf[:admin]
    conn_conf = rabbit_conf[:connection]
    hackney = [basic_auth: {conn_conf[:username], conn_conf[:password]}]
    vhost = URI.encode_www_form(conn_conf[:virtual_host])
    url = "#{admin_conf[:protocol]}://#{conn_conf[:host]}:#{admin_conf[:port]}/api/queues/#{vhost}"
    %{body: body} = HTTPoison.get!(url, [], hackney: hackney)
    Poison.decode!(body)
    |> Enum.map(&HTTPoison.delete!("#{url}/#{&1["name"]}", [], hackney: hackney))
  end
end
