defmodule Medusa.Adapter.RabbitMQ do
  @moduledoc false
  @behaviour Medusa.Adapter
  use Connection
  require Logger
  alias Medusa.Broker.Message
  alias Medusa.ProducerSupervisor, as: Producer
  alias Medusa.ConsumerSupervisor, as: Consumer

  defstruct channel: nil, connection: nil, messages: :queue.new

  @exchange_name "medusa"

  def start_link do
    Connection.start_link(__MODULE__, [], name: __MODULE__)
  end

  def connection do
    Connection.call(__MODULE__, :rabbitmq_connection)
  end

  def exchange do
    Connection.call(__MODULE__, :rabbitmq_exchange)
  end

  def new_route(topic, function, opts) do
    Connection.call(__MODULE__, {:new_route, topic, function, opts})
  end

  def publish(%Message{} = message) do
    Connection.call(__MODULE__, {:publish, message})
  end

  def init([]) do
    timeout = Medusa.config |> Keyword.get(:retry_publish_backoff, 5_000)
    :timer.send_interval(timeout, self, :republish)
    {:connect, :init, %__MODULE__{}}
  end

  def connect(_, state) do
    Logger.debug("#{__MODULE__} connecting")
    opts = connection_opts
    ensure_channel_closed(state.channel)
    case AMQP.Connection.open(opts) do
      {:ok, conn} ->
        Process.monitor(conn.pid)
        {:ok, %{state | connection: conn, channel: setup_channel(conn)}}
      {:error, error} ->
        Logger.warn("#{__MODULE__} connect: #{inspect error}")
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
      opts
      |> Keyword.get(:queue_name)
      |> queue_name(topic, function)
    opts = Keyword.put(opts, :queue_name, queue_name)
    with {:ok, p} <- Producer.start_child(topic, opts),
         {:ok, _} <- Consumer.start_child(function, p, opts) do
      {:reply, :ok, state}
    else
      {:error, {:already_started, _}} ->
        {:reply, :ok, state}
      error ->
        Logger.error("#{__MODULE__} new_route: #{inspect error}")
        {:reply, {:error, error}, state}
    end
  end

  def handle_call({:publish, %Message{} = message}, _from, state) do
    topic = message.topic
    Logger.debug("#{__MODULE__}: publish #{inspect message}")
    case Poison.encode(message) do
      {:ok, message} ->
        {reply, new_state} = do_publish(topic, message, 0, state)
        {:reply, reply, new_state}
      {:error, reason} ->
        Logger.warn("#{__MODULE__}: publish malformed message #{inspect message}")
        {:reply, {:error, reason}, state}
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

        {{:value, {topic, message, _}}, new_messages} ->
          Logger.warn("#{__MODULE__} republish failed for #{inspect {topic, message}}")
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
    Logger.warn("Got unexpected message #{inspect msg} state #{inspect state} from #{inspect self}")
    {:noreply, state}
  end

  def terminate(reason, state) do
    ensure_channel_closed(state.channel)
    Logger.error("""
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

  defp connection_opts do
    Medusa.config
    |> get_in([:RabbitMQ, :connection])
    |> Kernel.||([])
    |> Keyword.put_new(:heartbeat, 10)
  end

  defp group_name, do: Medusa.config |> Keyword.get(:group)

  defp queue_name(name, topic, function) do
    group = group_name || random_name
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
      AMQP.Basic.publish(chan,
                         @exchange_name,
                         topic,
                         message,
                         persistent: true)
      {:ok, state}
    rescue
      _ ->
        new_messages = :queue.in({topic, message, times + 1}, messages)
        {:error, %{state | messages: new_messages}}
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

end
