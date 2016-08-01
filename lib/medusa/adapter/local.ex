defmodule Medusa.Adapter.Local do
  use GenServer
  @behaviour Medusa.Adapter
  require Logger

  def start_link() do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  def insert(type, payload) do
    Logger.debug "#{inspect __MODULE__}: insert #{inspect payload} on #{inspect type}"
    GenServer.cast __MODULE__, {:insert, type, payload}
  end

  def next(type) do
    Logger.debug "#{inspect __MODULE__}: next from #{inspect type}"
    GenServer.call __MODULE__, {:next, type}
  end

  def init(_args) do
    {:ok, %{}}
  end

  def handle_call({:next, type}, _from, state) do
    case Map.get(state, type) do
      :nil -> {:reply, [], state}
      q ->
        {{:value, i}, q} = :queue.out q
        {:ok, [i], Map.put(state, type, q)}
    end
  end

  def handle_cast({:insert, type, payload}, state) do
    {:noreply, Map.update(state, type, :queue.in(payload, :queue.new), fn q -> :queue.in(payload, q) end)}
  end

end