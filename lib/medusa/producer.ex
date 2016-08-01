defmodule Medusa.Producer do
  alias Experimental.GenStage
  use GenStage
  require Logger

  @adapter Keyword.get(Application.get_env(:medusa, Medusa), :adapter)

  def start_link({:name, regex}) do
    Logger.debug "Starting Producer for: #{inspect regex}"
    name = Medusa.Broker.base64_encode_regex(regex) |> String.to_atom

    GenStage.start_link __MODULE__, %{id: regex, demand: 0}, name: name
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
    if state.demand > 0 do
      event = @adapter.next state.id
      d = state.demand
      {:noreply, [event], %{state | demand: d - 1}}
    else {:noreply, [], state} end
  end

end