defmodule Medusa.Adapter.PG2 do
  @moduledoc false
  @behaviour Medusa.Adapter
  use GenServer
  alias Medusa.{Broker, Message}

  def start_link do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  def new_route(event, function, opts) do
    GenServer.call(__MODULE__, {:new_route, event, function, opts})
  end

  def publish(%Message{} = message) do
    broadcast(get_members(), {:publish, message})
  end

  # behaviour
  def alive? do
    true
  end

  @doc """
  Register self into pg2 group. see `pg2_namespace/0`
  """
  def init(_opts) do
    group = pg2_namespace()
    :ok = :pg2.create(group)
    :ok = :pg2.join(group, self())
    {:ok, MapSet.new}
  end

  def handle_call({:new_route, event, function, opts}, _from, state) do
    {_producer, _consumer} = Broker.start_producer_consumer(event, function, opts)
    {:reply, :ok, MapSet.put(state, event)}
  end

  def handle_call({:publish, %Message{} = message}, _from, state) do
    Enum.each(state, &Broker.maybe_route({&1, message.topic, message}))
    {:reply, :ok, state}
  end

  def handle_info({:forward_to_local, msg}, state) do
    spawn(fn -> GenServer.call(__MODULE__, msg) end)
    {:noreply, state}
  end

  # process group name
  defp pg2_namespace do
    name =
      :medusa
      |> Application.get_env(Medusa)
      |> Keyword.get(:group, random_name())
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
        GenServer.call(__MODULE__, msg)
      pid ->
        send(pid, {:forward_to_local, msg})
    end)
  end

  # get all nodes in medusa
  defp get_all_members do
    :pg2.which_groups
    |> Enum.filter_map(&elem(&1, 0) == :medusa, &:pg2.get_members/1)
  end

  # get all medusa groups and random one member from each group
  defp get_members do
    get_all_members()
    |> Enum.map(&random_member/1)
  end

  defp random_member([]), do: []
  defp random_member([pid]), do: pid
  defp random_member(pids) when is_list(pids), do: Enum.random(pids)

end
