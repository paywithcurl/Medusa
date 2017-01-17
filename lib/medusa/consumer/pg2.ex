alias Experimental.GenStage

defmodule Medusa.Consumer.PG2 do
  use GenStage
  require Logger

  def start_link(args) do
    params = %{
      function: Keyword.fetch!(args, :function),
      to_link: Keyword.fetch!(args, :to_link),
      opts: Keyword.get(args, :opts, [])
   }
    GenStage.start_link __MODULE__, params
  end

  def init(%{to_link: link} = params) do
    {:consumer, params, subscribe_to: [link]}
  end

  @doc """
  Process the event passing the argument to the function.
  """
  def handle_events(events, _from, state) do
    events
    |> List.flatten
    |> do_handle_events(state)
  end

  defp do_handle_events([], state) do
    {:noreply, [], state}
  end

  defp do_handle_events(events, %{function: f, opts: opts} = state) do
    Enum.each(events, &f.(&1))
    if opts[:bind_once] do
      {:stop, :normal, state}
    else
      {:noreply, [], state}
    end
  end

end
