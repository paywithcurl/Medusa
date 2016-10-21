defmodule Medusa.Adapter.Local do
  @moduledoc false
  @behaviour Medusa.Adapter
  use GenServer
  require Logger
  alias Medusa.Broker

  def start_link do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  def new_route(event) do
    GenServer.call(__MODULE__, {:new_route, event})
  end

  def publish(event, message) do
    GenServer.cast(__MODULE__, {:publish, event, message})
  end

  def init(_opts) do
    {:ok, MapSet.new}
  end

  def handle_call({:new_route, event}, _from, state) do
    Logger.debug "#{inspect __MODULE__}: [#{inspect event}]"
    {:reply, :ok, MapSet.put(state, event)}
  end

  def handle_cast({:publish, event, payload}, state) do
    Logger.debug "#{inspect __MODULE__}: [#{inspect event}]: #{inspect payload}"
    Enum.each(state, &Broker.maybe_route({&1, event, payload}))
    {:noreply, state}
  end
end
