defmodule Medusa.Adapter.RabbitMQ do
  @moduledoc false
  @behaviour Medusa.Adapter
  use Connection
  require Logger
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

  def new_route(event, function, opts) do
    Connection.call(__MODULE__, {:new_route, event, function, opts})
  end

  def publish(event, message) do
    Connection.call(__MODULE__, {:publish, event, message})
  end

  def init([]) do
    timeout = Keyword.get(config, :retry_publish_backoff, 5_000)
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

  def handle_call({:new_route, event, function, opts}, _from, state) do
    Logger.debug("#{__MODULE__}: new route #{inspect event}")
    queue_name =
      opts
      |> Keyword.get(:queue_name)
      |> queue_name(event, function)
    opts = Keyword.put(opts, :queue_name, queue_name)
    with {:ok, p} <- Producer.start_child(event, opts),
         {:ok, _} <- Consumer.start_child(function, p, opts) do
      {:reply, {:ok, p}, state}
    else
      {:error, {:already_started, p}} ->
        {:reply, {:ok, p}, state}
      error ->
        Logger.error("#{__MODULE__} new_route: #{inspect error}")
        {:reply, {:error, error}, state}
    end
  end

  def handle_call({:publish, event, payload}, _from, %{channel: chan} = state) do
    Logger.debug("#{__MODULE__}: publish #{inspect event}: #{inspect payload}")
    case Poison.encode(payload) do
      {:ok, message} ->
        {reply, new_state} = do_publish(event, message, 0, state)
        {:reply, reply, new_state}
      {:error, _} ->
        Logger.warn("#{__MODULE__}: publish malformed message #{inspect payload}")
        {:reply, :error, state}
    end
  end

  def handle_call({:publish, _, _}, _from, state) do
    {:reply, :error, state}
  end

  def handle_info(:republish, %{messages: {[], []}} = state) do
    {:noreply, state}
  end

  def handle_info(:republish, %{messages: messages} = state) do
    retry_publish_max = Keyword.get(config, :retry_publish_max, 1)
    range = Range.new(0, :queue.len(messages))
    new_state = Enum.reduce(range, state, fn (_, acc) ->
      case :queue.out(acc.messages) do
        {{:value, {event, message, times}}, new_messages}
        when times < retry_publish_max ->
          acc = %{acc | messages: new_messages}
          {_, acc} = do_publish(event, message, times, acc)
          acc

        {{:value, {event, message, _}}, new_messages} ->
          Logger.warn("#{__MODULE__} republish failed for #{inspect {event, message}}")
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

  defp config, do: Application.get_env(:medusa, Medusa)

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
    config
    |> get_in([:RabbitMQ, :connection])
    |> Kernel.||([])
    |> Keyword.put_new(:heartbeat, 10)
  end

  defp group_name, do: Keyword.get(config, :group)

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

  defp do_publish(event, message, times, %{channel: chan, messages: messages} = state) do
    try do
      AMQP.Basic.publish(chan,
                         @exchange_name,
                         event,
                         message,
                         persistent: true)
      {:ok, state}
    rescue
      _ ->
        new_messages = :queue.in({event, message, times + 1}, messages)
        {:error, %{state | messages: new_messages}}
    end
  end

  defp random_name(len \\ 8) do
    len
    |> :crypto.strong_rand_bytes
    |> Base.url_encode64
    |> binary_part(0, len)
  end

end
