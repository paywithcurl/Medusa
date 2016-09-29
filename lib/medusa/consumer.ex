defmodule Medusa.Consumer do
  alias Experimental.GenStage
  alias Medusa.Broker
  use GenStage
  require Logger

  def start_link(args) do
    Logger.debug "Starting Consumer for: #{inspect args}"
    params = %{
      function: Keyword.fetch!(args, :function),
      to_link: Keyword.fetch!(args, :to_link),
      opts: Keyword.get(args, :opts, [])
   }
    GenStage.start_link __MODULE__, params
  end

  def init(%{function: f, to_link: link, opts: opts} = params) do
    {:consumer, params, subscribe_to: [link]}
  end

  @doc """
  Process the event passing the argument to the function.
  """
  def handle_events(events, _from, state) do
    Logger.debug "Received event: #{inspect events}"
    events
    |> List.flatten
    |> do_handle_events(state)
  end

  defp do_handle_events([], state) do
    {:noreply, [], state}
  end
  defp do_handle_events(events, %{function: f, opts: opts} = state) do
    Enum.each(events, &f.(&1))
    case opts[:bind_once] do
      :full ->
        Broker.exit_producer state.to_link
        {:stop, :bind_once, state}
      :only_consumer ->
        {:stop, :bind_once, state}
      _ ->
        {:noreply, [], state}
    end
  end

end
