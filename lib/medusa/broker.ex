defmodule Medusa.Broker do
  @moduledoc false
  alias Experimental.GenStage
  use GenServer
  require Logger

  defmodule Message do
    defstruct body: %{}, metadata: %{}
  end

  # API
  def start_link() do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  @doc """
  Adds a new route to the broker. If there is an existing route, 
  it just ignores it. 
  """  
  def new_route(event) do
    GenServer.call(__MODULE__, {:new_route, event})
  end

  @doc """
  Sends to the matching routes the event, using the configured adapter.

  TODO: Make this async.
  """
  def publish(event, payload, metadata \\ %{}) do
    message = %Message{body: payload, metadata: metadata}
    GenServer.cast(__MODULE__, {:publish, event, message})
  end

  @doc """
  Send exit signal to producer route name.
  """
  def exit_producer(route) do
    case Process.whereis(route) do
      nil -> :ok
      pid when is_pid(pid) -> GenStage.cast pid, :exit
    end
  end

  # Callbacks
  def init(_opts) do
    {:ok, []}
  end

  def handle_call({:new_route, event}, _from, state) do
    Logger.debug "#{inspect __MODULE__}: [#{inspect event}]"
    if Enum.find(state, fn(e) -> e == event end) do
      {:reply, :ok, state}
    else
      state = [event | state]
      {:reply, :ok, state}
    end

  end

  @adapter Keyword.get(Application.get_env(:medusa, Medusa), :adapter)

  def handle_cast({:publish, event, payload}, state) do
    Logger.debug "#{inspect __MODULE__}: [#{inspect event}]: #{inspect payload}"
    Enum.each(state, &maybe_route {&1, event, payload})
    {:noreply, state}
  end

  defp maybe_route({route, event, payload}) do
    f = fn -> maybe_route route, event, payload end
    Task.Supervisor.start_child Broker.Supervisor, f
  end

  defp maybe_route(route, event, payload) do
    if route_match?(route, event) do
      enqueue route, payload
      trigger_producer route
    end
  end

  defp enqueue(route, payload) do
    @adapter.insert route, payload
  end

  defp trigger_producer(route) do
    route
    |> String.to_atom
    |> GenServer.cast({:trigger})
  end

  defp route_match?(route, incoming)
  when is_binary(route) and is_binary(incoming) do
    route = String.split route, "."
    incoming = String.split incoming, "."
    route_match? route, incoming
  end
  defp route_match?([], []), do: true
  defp route_match?(["*"|t1], [_|t2]), do: route_match?(t1, t2)
  defp route_match?([h|t1], [h|t2]), do: route_match?(t1, t2)
  defp route_match?(_, _), do: false
end
