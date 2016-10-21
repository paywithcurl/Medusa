defmodule Medusa.Adapter.Local do
  @moduledoc false
  use GenServer
  require Logger
  alias Medusa.{Broker.Message, Queue}

  # API
  def start_link() do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  def new_route(event) do
    broadcast(get_all_members, {:new_route, event})
  end

  def publish(event, message) do
    broadcast(get_members, {:publish, event, message})
  end

  # Callbacks

  @doc """
  Register self into pg2 group. see `pg2_namespace/0`
  """
  def init(_opts) do
    group = pg2_namespace
    :ok = :pg2.create group
    :ok = :pg2.join group, self
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
    Queue.insert route, payload
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

  # process group name
  defp pg2_namespace do
    name =
      :medusa
      |> Application.get_env(Medusa)
      |> Keyword.get(:group, random_name)
    {:medusa, name}
  end

  # random name
  defp random_name(len \\ 32) do
    len
    |> :crypto.strong_rand_bytes
    |> Base.url_encode64
    |> binary_part(0, len)
  end

  defp broadcast(pids, msg) when is_list(pids) do
    pids
    |> List.flatten
    |> Enum.each(fn
      pid when is_pid(pid) and node(pid) == node() ->
        GenServer.call __MODULE__, msg
      pid ->
        send pid, {:forward_to_local, msg}
    end)
  end

  # get all nodes in medusa
  defp get_all_members do
    :pg2.which_groups
    |> Enum.filter_map(&elem(&1, 0) == :medusa, &:pg2.get_members/1)
  end

  # get all medusa groups and random one member from each group
  defp get_members do
    get_all_members
    |> Enum.map(&random_member/1)
  end

  defp random_member([]), do: []
  defp random_member([pid]), do: pid
  defp random_member(pids) when is_list(pids), do: Enum.random pids

end
