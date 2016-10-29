defmodule Medusa.Queue do
  use GenServer
  require Logger

  def start_link() do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  def insert(type, payload) do
    Logger.debug("#{__MODULE__}: insert #{inspect payload} on #{inspect type}")
    GenServer.cast(__MODULE__, {:insert, type, payload})
  end

  def next(type) do
    Logger.debug("#{inspect __MODULE__}: next from #{inspect type}")
    GenServer.call(__MODULE__, {:next, type})
  end

  def init(_args) do
    {:ok, %{}}
  end

  def handle_call({:next, type}, _from, state) do
    with q when is_tuple(q) <- Map.get(state, type) do
      case :queue.out(q) do
        {{:value, i}, q} -> {:reply, i, Map.put(state, type, q)}
        {:empty, q} -> {:reply, [], Map.put(state, type, q)}
      end
    else
      nil -> {:reply, [], state}
    end
  end

  def handle_cast({:insert, type, payload}, state) do
    fun = Map.update(state,
                     type,
                     :queue.in(payload, :queue.new),
                     &:queue.in(payload, &1))
    {:noreply, fun}
  end

end
