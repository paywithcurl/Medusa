defmodule Medusa.Adapter.RabbitMQ do
  @moduledoc false
  @behaviour Medusa.Adapter
  use GenServer
  use AMQP
  require Logger
  alias Medusa.Broker

  defstruct channel: nil, routes: MapSet.new

  defmodule Message do
    defstruct event: nil, message: nil
  end

  @exchange_name "medusa"

  def start_link do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  def new_route(event) do
    GenServer.call(__MODULE__, {:new_route, event})
  end

  def publish(event, message) do
    GenServer.call(__MODULE__, {:publish, event, message})
  end

  def init([]) do
    connection = init_connection()
    {:ok, connection}
  end

  def handle_call({:new_route, event}, _from, state) do
    Logger.debug "#{inspect __MODULE__}: [#{inspect event}]"
    new_state = Map.update(state,
                           :routes,
                           MapSet.new([event]),
                           &(MapSet.put(&1, event)))
    {:reply, :ok, new_state}
  end

  def handle_call({:publish, event, payload}, _from, state) do
    Logger.debug "#{inspect __MODULE__}: [#{inspect event}]: #{inspect payload}"
    message = %Message{event: event, message: payload} |> Poison.encode!
    AMQP.Basic.publish(state.channel, @exchange_name, "", message)
    {:noreply, state}
  end

  def handle_info({:basic_consume_ok, %{consumer_tag: _consumer_tag}}, state) do
    {:noreply, state}
  end

  def handle_info({:basic_cancel, %{consumer_tag: _consumer_tag}}, state) do
    {:stop, :normal, state}
  end

  def handle_info({:basic_cancel_ok, %{consumer_tag: _consumer_tag}}, state) do
    {:noreply, state}
  end

  def handle_info({:basic_deliver, payload, %{delivery_tag: tag}}, state) do
    msg = Poison.decode!(payload)
    event = Map.fetch!(msg, "event")
    message = Map.fetch!(msg, "message")
    message = %Broker.Message{body: message["body"], metadata: message["metadata"]}
    Enum.each(state.routes, &Broker.maybe_route({&1, event, message}))
    Basic.ack(state.channel, tag)
    {:noreply, state}
  end

  def handle_info({:DOWN, _, :process, pid, _reason}, %{channel: %{pid: pid, conn: conn}}) do
     state =
       case Process.alive?(conn.pid) do
         true -> init_channel(conn)
         false -> init_connection()
       end
    {:noreply, state}
  end

  def handle_info(msg, state) do
    Logger.warn("Got unexpected message #{msg} state #{state} from #{self}")
    {:noreply, state}
  end

  defp init_connection do
    connection_opts = config(:connection, [])
    case Connection.open(connection_opts) do
      {:ok, conn} ->
        init_channel(conn)
      {:error, _error} ->
        Process.sleep(10_000)
        init_connection()
    end
  end

  defp init_channel(conn) do
    {:ok, chan} = Channel.open(conn)
    Process.monitor(chan.pid)
    :ok = Basic.qos(chan, prefetch_count: 1)
    queue_name = config(:queue_name, "")
    queue_opts = queue_opts(queue_name)
    {:ok, _queue} = Queue.declare(chan, queue_name, queue_opts)
    :ok = Exchange.fanout(chan, @exchange_name, durable: true)
    :ok = Queue.bind(chan, queue_name, @exchange_name)
    {:ok, _consume} = Basic.consume(chan, queue_name)
    %__MODULE__{channel: chan}
  end

  defp config(name, default) do
    :medusa
    |> Application.get_env(Medusa)
    |> get_in([:RabbitMQ, name])
    |> Kernel.||(default)
  end

  defp queue_opts(queue_name) do
    case queue_name do
      "" -> [exclusive: true]
      _ -> [durable: true]
    end
  end

end
