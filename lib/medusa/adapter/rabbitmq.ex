defmodule Medusa.Adapter.RabbitMQ do
  @moduledoc false
  @behaviour Medusa.Adapter
  use Connection
  require Logger
  alias Medusa.ProducerSupervisor, as: Producer
  alias Medusa.ConsumerSupervisor, as: Consumer

  defstruct channel: nil, connection: nil

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
    {:connect, :init, %__MODULE__{}}
  end

  def connect(_, state) do
    Logger("#{__MODULE__} connecting")
    opts = connection_opts || []
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
    producer_opts = Keyword.put(opts, :function, function)
    with {:ok, p} <- Producer.start_child(event, producer_opts),
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

  def handle_call({:publish, event, payload}, _from, %{channel: chan} = state)
  when not is_nil(chan) do
    Logger.debug("#{__MODULE__}: publish #{inspect event}: #{inspect payload}")
    case Poison.encode(payload) do
      {:ok, message} ->
        AMQP.Basic.publish(state.channel,
                           @exchange_name,
                           event,
                           message,
                           persistent: true)
        {:reply, :ok, state}
      _ ->
        {:reply, :error, state}
    end
  end

  def handle_call({:publish, _, _}, _from, state) do
    {:reply, :error, state}
  end

  def handle_info({:basic_consume_ok, meta}, state) do
    Logger.debug("#{__MODULE__} basic_consume_ok: #{inspect meta}")
    {:noreply, state}
  end

  def handle_info({:basic_cancel, meta}, state) do
    Logger.debug("#{__MODULE__} basic_cancel: #{inspect meta}")
    {:noreply, state}
  end

  def handle_info({:basic_cancel_ok, _meta}, state) do
    Logger.debug("#{__MODULE__} basic_cancel_ok: #{inspect meta}")
    {:stop, :normal, state}
  end

  def handle_info({:basic_deliver, payload, _meta}, state) do
    Logger.debug("#{__MODULE__} basic_deliver: #{inspect payload}")
    {:noreply, state}
  end

  def handle_info({:DOWN, _, _, _, _}, %__MODULE__{channel: %{conn: conn}} = state) do
    case Process.alive?(conn.pid) do
      true ->
        {:noreply, %{state | channel: setup_channel(conn)}}
      false ->
        {:connect, :reconnect, %{state | connection: nil}}
    end
  end

  def handle_info(msg, state) do
    Logger.warn("Got unexpected message #{inspect msg} state #{inspect state} from #{inspect self}")
    {:noreply, state}
  end

  def terminate(reason, state) do
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

  def connection_opts do
    :medusa
    |> Application.get_env(Medusa)
    |> get_in([:RabbitMQ, :connection])
    |> Kernel.||([])
  end

end
