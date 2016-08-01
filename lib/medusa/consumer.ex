defmodule Medusa.Consumer do
  alias Experimental.GenStage
  use GenStage
  require Logger

  def start_link(args1, args2) do
    Logger.debug "Starting Consumer for: #{inspect args1}. #{inspect args2}"
    GenStage.start_link __MODULE__, [args1, args2]
  end

  def init([{:function, f}, {:to_link, to_link}]) do
    {:consumer, f, subscribe_to: [to_link]}
  end

  @doc """
  Process the event passing the argument to the function.
  """
  def handle_events(event, _from, state) do
    Logger.debug "Handling event: #{inspect event}"
    result = state.(event)
    Logger.debug "Result of computation: #{result}"
    # We are a consumer, so we never emit events.
    {:noreply, [], state}
  end

end