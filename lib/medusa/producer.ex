defmodule Medusa.Producer do
  alias Experimental.GenStage
  use GenStage
  require Logger
  alias Medusa.Queue

  defstruct id: nil, demand: 0, consumers: MapSet.new

  def start_link({:name, name}) do
    Logger.debug "Starting Producer #{inspect name} for: #{inspect name}"
    state = %__MODULE__{id: name}
    GenStage.start_link __MODULE__, state, name: String.to_atom(name)
  end

  def init(state) do
    {:producer, state, dispatcher: GenStage.BroadcastDispatcher}
  end

  def handle_demand(demand, state) do
    Logger.debug "Current demand: #{inspect demand}."
    get_next(%{state | demand: demand})
  end

  def handle_cast({:trigger}, state) do
    Logger.debug "Triggered"
    get_next(state)
  end

  def handle_subscribe(:consumer, _opts, {pid, _ref}, %{consumers: consumers} = state) do
    new_state = Map.put(state, :consumers, MapSet.put(consumers, pid))
    {:automatic, new_state}
  end

  def handle_cancel({:down, _reason, _process}, state) do
     {:noreply, [], state}
  end

  def handle_cancel(_reason, {pid, _ref}, %{consumers: consumers} = state) do
    cond do
      MapSet.member?(consumers, pid) && MapSet.size(consumers) == 1 ->
        {:stop, :normal, state}
      MapSet.member?(consumers, pid) ->
        new_state = Map.put(state, :consumers, MapSet.delete(consumers, pid))
        {:noreply, [], new_state}
      true ->
        {:noreply, [], state}
    end
  end

  def handle_cancel(_reason, _from, state) do
    {:noreply, [], state}
  end

  defp get_next(state) do
    Logger.debug "#{inspect __MODULE__}: #{inspect state}"
    if state.demand > 0 do
      event = Queue.next(state.id)
      Logger.debug "#{inspect __MODULE__}: event #{inspect event}"
      d = state.demand
      {:noreply, [event], %{state | demand: d - 1}}
    else {:noreply, [], state} end
  end

end
