alias Experimental.GenStage

defmodule Medusa.Producer.PG2 do
  use GenStage
  require Logger
  alias Medusa.Queue

  defstruct id: nil, demand: 0, consumers: MapSet.new

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    GenStage.start_link __MODULE__, opts, name: String.to_atom(name)
  end

  def init(opts) do
    name= opts[:name]
    Logger.debug("Starting Producer #{__MODULE__} for: #{inspect name}")
    state = %__MODULE__{id: name}
    {:producer, state, dispatcher: GenStage.BroadcastDispatcher}
  end

  def handle_demand(demand, state) do
    Logger.debug("#{state.id} Current demand: #{inspect demand}.")
    get_next_event(%{state | demand: demand})
  end

  # TODO Change this!
  def handle_cast({:trigger}, state) do
    Logger.debug("#{state.id} triggered")
    get_next_event(state)
  end

  def handle_subscribe(:consumer, _opts, {pid, _ref}, state) do
    new_state =
      Map.update(state,
                 :consumers,
                 MapSet.new([pid]),
                 &MapSet.put(&1, pid))
    {:automatic, new_state}
  end

  def handle_cancel({:down, _reason, _process}, _from, state) do
     {:noreply, [], state}
  end

  def handle_cancel(_reason, {pid, _ref}, %{consumers: consumers} = state) do
    member? = MapSet.member?(consumers, pid)
    member_size = MapSet.size(consumers)
    cond do
      member? && member_size == 1 ->
        {:stop, :normal, state}
      member? ->
        new_state =
          Map.update(state,
                     :consumers,
                     MapSet.new(),
                     &MapSet.delete(&1, pid))
        {:noreply, [], new_state}
      true ->
        {:noreply, [], state}
    end
  end

  def handle_cancel(_reason, _from, state) do
    {:noreply, [], state}
  end

  defp get_next_event(%__MODULE__{id: id, demand: demand} = state) do
    Logger.debug("#{__MODULE__} getting next event: #{inspect state}")
    if demand > 0 do
      event = Queue.next(id)
      Logger.debug("#{__MODULE__}: got event #{inspect event}")
      {:noreply, [event], %{state | demand: demand - 1}}
    else
      {:noreply, [], state}
    end
  end

end
