defmodule Medusa.Broker do
  @moduledoc false
  use GenServer
  require Logger

  @adapter Keyword.get(Application.get_env(:medusa, Medusa), :adapter)

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
    broadcast get_members, {:new_route, event}
  end

  @doc """
  Sends to the matching routes the event, using the configured adapter.
  """
  def publish(event, payload, metadata \\ %{}) do
    message = %Message{body: payload, metadata: metadata}
    broadcast get_members, {:publish, event, message}
  end

  # Callbacks
  def init(_opts) do
    :ok = :pg2.create pg2_namespace
    :ok = :pg2.join pg2_namespace, self
    {:ok, MapSet.new}
  end

  def handle_call({:new_route, event}, _from, state) do
    Logger.debug "#{inspect __MODULE__}: [#{inspect event}]"
    {:reply, :ok, MapSet.put(state, event)}
  end

  def handle_call({:publish, event, payload}, _from, state) do
    Logger.debug "#{inspect __MODULE__}: [#{inspect event}]: #{inspect payload}"
    Enum.each(state, &maybe_route {&1, event, payload})
    {:reply, :ok, state}
  end

  def handle_info({:forward_to_local, msg}, state) do
    handle_call(msg, self, state)
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

  defp pg2_namespace, do: {:medusa, __MODULE__}

  defp get_members, do: :pg2.get_members pg2_namespace

  defp broadcast(pids, msg) when is_list(pids) do
    Enum.each pids, fn
      pid when is_pid(pid) and node(pid) == node() ->
        GenServer.call __MODULE__, msg
      pid ->
        send pid, {:forward_to_local, msg}
    end
  end
end
