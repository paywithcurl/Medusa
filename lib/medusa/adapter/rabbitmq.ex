defmodule Medusa.Adapter.RabbitMQ do
  @moduledoc false
  @behaviour Medusa.Adapter
  use Connection
  require Logger
  alias Medusa.Message
  alias Medusa.ProducerSupervisor, as: Producer
  alias Medusa.ConsumerSupervisor, as: Consumer

  defstruct channel: nil, connection: nil, messages: :queue.new

  @exchange_name "medusa"

  def start_link do
    Connection.start_link(__MODULE__, [], name: __MODULE__)
  end

  @doc """
  Get current rabbitmq connction
  """
  def connection do
    Connection.call(__MODULE__, :rabbitmq_connection)
  end

  @doc """
  Get current rabbitmq exchange name
  """
  def exchange do
    Connection.call(__MODULE__, :rabbitmq_exchange)
  end

  @doc """
  Create a new Producer/Consumer and listen to given topic
  Producer will only create once for queue_name in `opts`
  """
  def new_route(topic, function, opts) do
    Connection.call(__MODULE__, {:new_route, topic, function, opts})
  end

  @doc """
  Publish message
  """
  def publish(%Message{} = message) do
    Connection.call(__MODULE__, {:publish, message})
  end

  def alive? do
    Connection.call(__MODULE__, {:alive})
  end

  def init([]) do
    timeout = Medusa.config |> Keyword.get(:retry_publish_backoff, 5_000)
    :timer.send_interval(timeout, self(), :republish)
    {:connect, :init, %__MODULE__{}}
  end

  def connect(_, state) do
    Logger.debug("#{__MODULE__} connecting")
    opts = connection_opts()
    ensure_channel_closed(state.channel)
    case AMQP.Connection.open(opts) do
      {:ok, conn} ->
        Process.monitor(conn.pid)
        {:ok, %{state | connection: conn, channel: setup_channel(conn)}}
      {:error, error} ->
        Logger.debug("#{__MODULE__} connect: #{inspect error}")
        {:backoff, 1_000, %{state | connection: nil}}
    end
  end

  def handle_call(:rabbitmq_connection, _from, state) do
    {:reply, state.connection, state}
  end

  def handle_call(:rabbitmq_exchange, _from, state) do
    {:reply, @exchange_name, state}
  end

  def handle_call({:new_route, topic, function, opts}, _from, state) do
    Logger.debug("#{__MODULE__}: new route #{inspect topic}")

    queue_name =
      opts |> Keyword.get(:queue_name) |> queue_name(topic, function)
    consumers = Keyword.get(opts, :consumers, 1)
    new_opts = Keyword.put(opts, :queue_name, queue_name)

    with {:ok, p} <- start_producer(topic, new_opts),
         {:ok, _} <- start_consumer(function, p, new_opts, consumers) do
      {:reply, :ok, state}
    else
      {:error, error} ->
        Logger.debug("#{__MODULE__} new_route: #{inspect error}")
        {:reply, {:error, error}, state}
    end
  end

  def handle_call({:publish, %Message{} = message}, _from, state) do
    topic = message.topic
    Logger.debug("#{__MODULE__}: publish #{inspect message}")
    with {:ok, message_bin} <- Poison.encode(message),
         {:ok, new_state} <- do_publish(topic, message_bin, 0, state) do
      Medusa.Logger.info(message, belongs: "publishing")
      {:reply, :ok, new_state}
    else
      {:error, {:invalid, _}} ->
        Medusa.Logger.error(message,
                            reason: "malformed message",
                            belongs: "publishing")
        {:reply, {:error, "malformed message"}, state}
      {{:error, reason}, new_state} ->
        Medusa.Logger.error(message,
                            reason: inspect(reason),
                            belongs: "publishing")
        {:reply, {:error, reason}, new_state}
    end
  end

  def handle_call({:alive}, _from, state) do
    # The vhost needs to be url encoded in the path as it can contain /
    # and it needs to be separated from the path itself.
    # The default path is /
    # See https://lists.rabbitmq.com/pipermail/rabbitmq-discuss/2012-March/019161.html
    vhost = URI.encode_www_form(connection_opts()[:virtual_host])
    protocol = admin_opts()[:protocol]
    port = admin_opts()[:port]
    username = connection_opts()[:username]
    password = connection_opts()[:password]
    host = connection_opts()[:host]
    url = "#{protocol}://#{username}:#{password}@#{host}:#{port}/api/aliveness-test/#{vhost}"

    try do
      alive = case HTTPoison.get(url, %{}, [timeout: 1_000]) do
		{:ok, %{status_code: 200, body: "{\"status\":\"ok\"}"}} -> true
		_ -> false
	      end
      {:reply, alive, state}
    rescue
      _ -> {:reply, false, state}
    catch
      _, _ -> {:reply, false, state}
    end
  end

  def handle_info(:republish, %{messages: {[], []}} = state) do
    {:noreply, state}
  end

  def handle_info(:republish, %{messages: messages} = state) do
    retry_publish_max = Medusa.config |> Keyword.get(:retry_publish_max, 1)
    range = Range.new(0, :queue.len(messages))
    new_state = Enum.reduce(range, state, fn (_, acc) ->
      case :queue.out(acc.messages) do
        {{:value, {topic, message, times}}, new_messages}
        when times < retry_publish_max ->
          acc = %{acc | messages: new_messages}
          {_, acc} = do_publish(topic, message, times, acc)
          acc

        {{:value, {_topic, message, _}}, new_messages} ->
          Medusa.Logger.error(message,
                              reason: "reach max retries",
                              belongs: "publishing")
          %{acc | messages: new_messages}

        {:empty, new_messages} ->
          %{acc | messages: new_messages}
      end
    end)
    {:noreply, new_state}
  end

  def handle_info({:basic_consume_ok, meta}, state) do
    Logger.debug("#{__MODULE__} basic_consume_ok: #{inspect meta}")
    {:noreply, state}
  end

  def handle_info({:basic_cancel, meta}, state) do
    Logger.debug("#{__MODULE__} basic_cancel: #{inspect meta}")
    {:noreply, state}
  end

  def handle_info({:basic_cancel_ok, meta}, state) do
    Logger.debug("#{__MODULE__} basic_cancel_ok: #{inspect meta}")
    {:stop, :normal, state}
  end

  def handle_info({:basic_deliver, payload, _meta}, state) do
    Logger.debug("#{__MODULE__} basic_deliver: #{inspect payload}")
    {:noreply, state}
  end

  def handle_info({:DOWN, _, _, _, _}, state) do
    with %AMQP.Channel{} = chan <- state.channel,
         true <- Process.alive?(chan.conn.pid) do
      ensure_channel_closed(state.channel)
      {:noreply, %{state | channel: setup_channel(chan.conn)}}
    else
      _ ->
        {:connect, :reconnect, %{state | connection: nil}}
    end
  end

  def handle_info(msg, state) do
    Logger.debug("Got unexpected message #{inspect msg} state #{inspect state} from #{inspect self()}")
    {:noreply, state}
  end

  def terminate(reason, state) do
    ensure_channel_closed(state.channel)
    Logger.debug("""
      #{__MODULE__}
      state: #{inspect state}
      die: #{inspect reason}
    """)
  end

  defp setup_channel(conn) do
    {:ok, chan} = AMQP.Channel.open(conn)
    Process.monitor(chan.pid)
    :ok = AMQP.Exchange.topic(chan, @exchange_name, durable: true)
    chan
  end

  defp ensure_channel_closed(%AMQP.Channel{} = chan) do
    if Process.alive?(chan.pid) do
      AMQP.Channel.close(chan)
    end
  end

  defp ensure_channel_closed(_) do
    :ok
  end

  defp admin_opts do
    Medusa.config
    |> get_in([:RabbitMQ, :admin])
  end

  defp connection_opts do
    Medusa.config
    |> get_in([:RabbitMQ, :connection])
  end

  defp group_name, do: Medusa.config |> Keyword.get(:group)

  defp queue_name(name, topic, function) do
    group = group_name() || random_name()
    "#{group}.#{do_queue_name(name, group, topic, function)}"
  end

  def do_queue_name(name, _, _, _) when is_binary(name) do
    name
  end

  def do_queue_name(_, group, topic, function) do
    {group, topic, function} |> :erlang.phash2
  end

  defp do_publish(topic, message, times,
  %{channel: chan, messages: messages} = state) when not is_nil(chan) do
    try do
      message_id = Poison.decode!(message)["metadata"]["id"]
      AMQP.Basic.publish(chan,
                         @exchange_name,
                         topic,
                         message,
                         message_id: message_id,
                         persistent: true)
      {:ok, state}
    rescue
      reason ->
        new_messages = :queue.in({topic, message, times + 1}, messages)
        {{:error, reason}, %{state | messages: new_messages}}
    end
  end

  defp do_publish(topic, message, times, %{messages: messages} = state) do
    new_messages = :queue.in({topic, message, times}, messages)
    {{:error, "cannot connect rabbitmq"}, %{state | messages: new_messages}}
  end

  defp random_name(len \\ 8) do
    len
    |> :crypto.strong_rand_bytes
    |> Base.url_encode64
    |> binary_part(0, len)
  end

  defp start_producer(topic, opts) do
    case Producer.start_child(topic, opts) do
      {:ok, pid} -> {:ok, pid}
      {:error, {:already_started, pid}} -> {:ok, pid}
      {:error, error} -> {:error, error}
    end
  end

  defp start_consumer(function, producer, opts, times) do
    range = Range.new(1, times)
    Enum.map(range, fn _ -> Consumer.start_child(function, producer, opts) end)
    |> Enum.all?(&elem(&1, 0) == :ok)
    |> case do
      true -> {:ok, times}
      false -> {:error, producer}
    end
  end

end
