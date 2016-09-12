defmodule Medusa.Producer do
  alias Experimental.GenStage
  use GenStage
  require Logger

  @adapter Keyword.get(Application.get_env(:medusa, Medusa), :adapter)

  def start_link({:name, name}) do
    name = Medusa.Broker.base64_encode_name name
    Logger.debug "Starting Producer #{inspect name} for: #{inspect name}"

    GenStage.start_link __MODULE__, %{id: name, demand: 0}, name: String.to_atom(name)
  end

  def init(state) do
    {:producer, state}
  end

  def handle_demand(demand, state) do
    Logger.debug "Current demand: #{inspect demand}."
    get_next(%{state | demand: demand})
  end

  def handle_cast({:trigger}, state) do
    Logger.debug "Triggered"
    get_next(state)
  end

  defp get_next(state) do
    Logger.debug "#{inspect __MODULE__}: #{inspect state}"
    if state.demand > 0 do
      event = @adapter.next(state.id)
      Logger.debug "#{inspect __MODULE__}: event #{inspect event}"
      d = state.demand
      {:noreply, [event], %{state | demand: d - 1}}
    else {:noreply, [], state} end
  end

end
